use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::device::{DeviceDeclaration, DeviceInstance, DeviceLayout, DeviceResources};
use crate::dma::{DmaMemory, DmaRange};
use crate::error::{DeviceDownReason, DeviceError};
use crate::interrupt::Interrupt;
use crate::queue::{DescriptorChain, QueueState, VirtQueue};

pub const VIRTIO_F_VERSION_1: u64 = 1 << 32;
pub const VSOCK_HOST_CID: u64 = 2;
pub const VSOCK_TYPE_STREAM: u16 = 1;
pub const VSOCK_OP_REQUEST: u16 = 1;
pub const VSOCK_OP_RESPONSE: u16 = 2;
pub const VSOCK_OP_RST: u16 = 3;
pub const VSOCK_OP_SHUTDOWN: u16 = 4;
pub const VSOCK_OP_RW: u16 = 5;
pub const VSOCK_OP_CREDIT_UPDATE: u16 = 6;
pub const VSOCK_OP_CREDIT_REQUEST: u16 = 7;

const QUEUE_RX: usize = 0;
const QUEUE_TX: usize = 1;
const QUEUE_EVENT: usize = 2;
const QUEUE_COUNT: usize = 3;
const HEADER_SIZE: usize = 44;
const MAXIMUM_PACKET_SIZE: usize = 64 * 1024;
const VIRTQ_DESC_F_WRITE: u16 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VsockHeader {
    pub source_cid: u64,
    pub destination_cid: u64,
    pub source_port: u32,
    pub destination_port: u32,
    pub length: u32,
    pub packet_type: u16,
    pub operation: u16,
    pub flags: u32,
    pub buffer_allocation: u32,
    pub forward_count: u32,
}

impl VsockHeader {
    pub fn decode(bytes: &[u8]) -> Result<Self, DeviceError> {
        if bytes.len() != HEADER_SIZE {
            return Err(DeviceError::Descriptor(
                "invalid virtio-vsock header length",
            ));
        }
        Ok(Self {
            source_cid: u64::from_le_bytes(bytes[0..8].try_into().expect("fixed header")),
            destination_cid: u64::from_le_bytes(bytes[8..16].try_into().expect("fixed header")),
            source_port: u32::from_le_bytes(bytes[16..20].try_into().expect("fixed header")),
            destination_port: u32::from_le_bytes(bytes[20..24].try_into().expect("fixed header")),
            length: u32::from_le_bytes(bytes[24..28].try_into().expect("fixed header")),
            packet_type: u16::from_le_bytes(bytes[28..30].try_into().expect("fixed header")),
            operation: u16::from_le_bytes(bytes[30..32].try_into().expect("fixed header")),
            flags: u32::from_le_bytes(bytes[32..36].try_into().expect("fixed header")),
            buffer_allocation: u32::from_le_bytes(bytes[36..40].try_into().expect("fixed header")),
            forward_count: u32::from_le_bytes(bytes[40..44].try_into().expect("fixed header")),
        })
    }

    pub fn encode(self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0; HEADER_SIZE];
        bytes[0..8].copy_from_slice(&self.source_cid.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.destination_cid.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.source_port.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.destination_port.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.length.to_le_bytes());
        bytes[28..30].copy_from_slice(&self.packet_type.to_le_bytes());
        bytes[30..32].copy_from_slice(&self.operation.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.flags.to_le_bytes());
        bytes[36..40].copy_from_slice(&self.buffer_allocation.to_le_bytes());
        bytes[40..44].copy_from_slice(&self.forward_count.to_le_bytes());
        bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VsockPacket {
    pub header: VsockHeader,
    pub payload: Vec<u8>,
}

impl VsockPacket {
    pub fn validate(&self) -> Result<(), DeviceError> {
        if self.header.packet_type != VSOCK_TYPE_STREAM
            || usize::try_from(self.header.length)
                .map_err(|_| DeviceError::Descriptor("vsock length overflows usize"))?
                != self.payload.len()
            || self.payload.len() > MAXIMUM_PACKET_SIZE
        {
            return Err(DeviceError::Descriptor("invalid virtio-vsock packet"));
        }
        Ok(())
    }
}

#[async_trait]
pub trait VsockBackend: Send + Sync {
    async fn receive_packet(&self, packet: VsockPacket) -> Result<(), DeviceError>;
    fn has_packet(&self) -> bool;
    fn take_packet(&self) -> Option<VsockPacket>;
}

pub struct VsockDeclaration {
    imm_guest_cid: u64,
    imm_maximum_queue_size: u16,
    own_imm_backend: Arc<dyn VsockBackend>,
}

impl VsockDeclaration {
    pub fn new(
        guest_cid: u64,
        maximum_queue_size: u16,
        backend: Arc<dyn VsockBackend>,
    ) -> Result<Self, DeviceError> {
        if guest_cid <= VSOCK_HOST_CID
            || maximum_queue_size == 0
            || !maximum_queue_size.is_power_of_two()
        {
            return Err(DeviceError::InvalidLayout(
                "invalid virtio-vsock declaration",
            ));
        }
        Ok(Self {
            imm_guest_cid: guest_cid,
            imm_maximum_queue_size: maximum_queue_size,
            own_imm_backend: backend,
        })
    }

    pub const fn guest_cid(&self) -> u64 {
        self.imm_guest_cid
    }
}

struct VsockDevice {
    own_imm_resources: DeviceResources,
    own_imm_backend: Arc<dyn VsockBackend>,
    own_mut_queue_states: Mutex<Vec<QueueState>>,
    atomic_mut_kicked: AtomicBool,
    atomic_mut_down: AtomicBool,
}

#[async_trait]
impl DeviceDeclaration for VsockDeclaration {
    fn layout(&self) -> DeviceLayout {
        DeviceLayout {
            queue_count: QUEUE_COUNT,
            maximum_queue_size: self.imm_maximum_queue_size,
            notifier_count: QUEUE_COUNT + 1,
            required_features: VIRTIO_F_VERSION_1,
            optional_features: 0,
        }
    }

    async fn activate(
        &self,
        resources: DeviceResources,
    ) -> Result<Box<dyn DeviceInstance>, DeviceError> {
        resources.validate(&self.layout())?;
        Ok(Box::new(VsockDevice {
            own_mut_queue_states: Mutex::new(vec![QueueState::new(); QUEUE_COUNT]),
            own_imm_resources: resources,
            own_imm_backend: Arc::clone(&self.own_imm_backend),
            atomic_mut_kicked: AtomicBool::new(false),
            atomic_mut_down: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl DeviceInstance for VsockDevice {
    fn kick(&self) {
        self.atomic_mut_kicked.store(true, Ordering::Release);
    }

    async fn process_kick(&self) -> Result<(), DeviceError> {
        if self.atomic_mut_down.load(Ordering::Acquire) {
            return Err(DeviceError::Down(DeviceDownReason::Stop));
        }
        if !self.atomic_mut_kicked.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        self.process_tx().await?;
        self.process_rx().await?;
        self.process_event().await
    }

    async fn shutdown(&self, _reason: DeviceDownReason) -> Result<(), DeviceError> {
        self.atomic_mut_down.store(true, Ordering::Release);
        self.own_imm_resources.dma.revoke();
        self.own_imm_resources.dma.wait_for_drain().await;
        Ok(())
    }
}

impl VsockDevice {
    async fn process_tx(&self) -> Result<(), DeviceError> {
        loop {
            let chain = {
                let mut states = self.own_mut_queue_states.lock().await;
                let queue = VirtQueue::new(
                    self.own_imm_resources.queues[QUEUE_TX],
                    &self.own_imm_resources.dma,
                )?;
                let Some(chain) = queue.pop(&self.own_imm_resources.dma, &mut states[QUEUE_TX])?
                else {
                    return Ok(());
                };
                chain
            };
            let packet = read_packet(&self.own_imm_resources.dma, &chain)?;
            let used_length = packet.header.length.saturating_add(HEADER_SIZE as u32);
            self.own_imm_backend.receive_packet(packet).await?;
            let mut states = self.own_mut_queue_states.lock().await;
            let queue = VirtQueue::new(
                self.own_imm_resources.queues[QUEUE_TX],
                &self.own_imm_resources.dma,
            )?;
            queue.complete(
                &self.own_imm_resources.dma,
                &mut states[QUEUE_TX],
                &chain,
                used_length,
            )?;
            drop(states);
            self.notify_queue(QUEUE_TX).await?;
        }
    }

    async fn process_rx(&self) -> Result<(), DeviceError> {
        loop {
            if !self.own_imm_backend.has_packet() {
                return Ok(());
            }
            let mut states = self.own_mut_queue_states.lock().await;
            let queue = VirtQueue::new(
                self.own_imm_resources.queues[QUEUE_RX],
                &self.own_imm_resources.dma,
            )?;
            let Some(chain) = queue.pop(&self.own_imm_resources.dma, &mut states[QUEUE_RX])? else {
                return Ok(());
            };
            let packet = self
                .own_imm_backend
                .take_packet()
                .ok_or(DeviceError::Descriptor("vsock packet disappeared"))?;
            let bytes = packet_bytes(&packet)?;
            write_packet(&self.own_imm_resources.dma, &chain, &bytes)?;
            queue.complete(
                &self.own_imm_resources.dma,
                &mut states[QUEUE_RX],
                &chain,
                u32::try_from(bytes.len())
                    .map_err(|_| DeviceError::Descriptor("vsock packet too large"))?,
            )?;
            drop(states);
            self.notify_queue(QUEUE_RX).await?;
        }
    }

    async fn process_event(&self) -> Result<(), DeviceError> {
        let mut states = self.own_mut_queue_states.lock().await;
        let queue = VirtQueue::new(
            self.own_imm_resources.queues[QUEUE_EVENT],
            &self.own_imm_resources.dma,
        )?;
        while let Some(chain) = queue.pop(&self.own_imm_resources.dma, &mut states[QUEUE_EVENT])? {
            queue.complete(
                &self.own_imm_resources.dma,
                &mut states[QUEUE_EVENT],
                &chain,
                0,
            )?;
        }
        Ok(())
    }

    async fn notify_queue(&self, queue: usize) -> Result<(), DeviceError> {
        self.own_imm_resources.interrupts[queue]
            .notify(Interrupt::Queue {
                queue_index: u16::try_from(queue).expect("fixed queue index"),
                vector: u16::try_from(queue).expect("fixed queue vector"),
            })
            .await
    }
}

fn read_packet(memory: &DmaMemory, chain: &DescriptorChain) -> Result<VsockPacket, DeviceError> {
    if chain
        .descriptors
        .iter()
        .any(|descriptor| descriptor.flags & VIRTQ_DESC_F_WRITE != 0)
    {
        return Err(DeviceError::Descriptor("vsock TX chain is writable"));
    }
    let bytes = read_chain(memory, chain)?;
    if bytes.len() < HEADER_SIZE {
        return Err(DeviceError::Descriptor("short virtio-vsock TX chain"));
    }
    let header = VsockHeader::decode(&bytes[..HEADER_SIZE])?;
    let length = usize::try_from(header.length)
        .map_err(|_| DeviceError::Descriptor("vsock length overflows usize"))?;
    if bytes.len() != HEADER_SIZE + length {
        return Err(DeviceError::Descriptor(
            "virtio-vsock TX chain length mismatch",
        ));
    }
    let packet = VsockPacket {
        header,
        payload: bytes[HEADER_SIZE..].to_vec(),
    };
    packet.validate()?;
    Ok(packet)
}

fn packet_bytes(packet: &VsockPacket) -> Result<Vec<u8>, DeviceError> {
    packet.validate()?;
    let mut bytes = Vec::with_capacity(HEADER_SIZE + packet.payload.len());
    bytes.extend_from_slice(&packet.header.encode());
    bytes.extend_from_slice(&packet.payload);
    Ok(bytes)
}

fn read_chain(memory: &DmaMemory, chain: &DescriptorChain) -> Result<Vec<u8>, DeviceError> {
    let mut bytes = Vec::new();
    for descriptor in &chain.descriptors {
        let length = usize::try_from(descriptor.length)
            .map_err(|_| DeviceError::Descriptor("descriptor length overflows usize"))?;
        let lease = memory.lease(DmaRange::new(descriptor.address, length))?;
        for part in lease.parts() {
            bytes.extend_from_slice(unsafe { part.read_slice() });
        }
    }
    Ok(bytes)
}

fn write_packet(
    memory: &DmaMemory,
    chain: &DescriptorChain,
    bytes: &[u8],
) -> Result<(), DeviceError> {
    if chain
        .descriptors
        .iter()
        .any(|descriptor| descriptor.flags & VIRTQ_DESC_F_WRITE == 0)
    {
        return Err(DeviceError::Descriptor("vsock RX chain is readable"));
    }
    let mut capacity = 0usize;
    for descriptor in &chain.descriptors {
        let length = usize::try_from(descriptor.length)
            .map_err(|_| DeviceError::Descriptor("descriptor length overflows usize"))?;
        capacity = capacity
            .checked_add(length)
            .ok_or(DeviceError::Descriptor("vsock RX capacity overflows usize"))?;
    }
    if capacity < bytes.len() {
        return Err(DeviceError::Descriptor(
            "virtio-vsock RX chain is too short",
        ));
    }
    let mut offset = 0;
    for descriptor in &chain.descriptors {
        if offset == bytes.len() {
            break;
        }
        let length = usize::try_from(descriptor.length)
            .map_err(|_| DeviceError::Descriptor("descriptor length overflows usize"))?;
        let mut lease = memory.lease(DmaRange::new(descriptor.address, length))?;
        for part in lease.parts_mut() {
            if offset == bytes.len() {
                break;
            }
            let target = unsafe { part.write_slice() };
            let count = target.len().min(bytes.len() - offset);
            target[..count].copy_from_slice(&bytes[offset..offset + count]);
            offset += count;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{VSOCK_OP_REQUEST, VSOCK_TYPE_STREAM, VsockHeader, VsockPacket};
    #[test]
    fn header_round_trip_preserves_stream_packet() {
        let header = VsockHeader {
            source_cid: 3,
            destination_cid: 2,
            source_port: 1024,
            destination_port: 1025,
            length: 3,
            packet_type: VSOCK_TYPE_STREAM,
            operation: VSOCK_OP_REQUEST,
            flags: 0,
            buffer_allocation: 65536,
            forward_count: 0,
        };
        assert_eq!(
            VsockHeader::decode(&header.encode()).expect("header"),
            header
        );
        VsockPacket {
            header,
            payload: b"abc".to_vec(),
        }
        .validate()
        .expect("packet");
    }
}
