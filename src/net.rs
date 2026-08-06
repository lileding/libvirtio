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
pub const VIRTIO_NET_F_MAC: u64 = 1 << 5;
pub const VIRTIO_NET_F_STATUS: u64 = 1 << 16;
pub const VIRTIO_NET_S_LINK_UP: u16 = 1;

const QUEUE_RX: usize = 0;
const QUEUE_TX: usize = 1;
const QUEUE_COUNT: usize = 2;
const NOTIFIER_COUNT: usize = QUEUE_COUNT + 1;
const VIRTQ_DESC_F_WRITE: u16 = 2;
const HEADER_SIZE: usize = 12;
const MAXIMUM_FRAME_SIZE: usize = 65_536;

#[async_trait]
pub trait NetBackend: Send + Sync {
    async fn transmit(&self, frame: Vec<u8>) -> Result<(), DeviceError>;

    fn has_frame(&self) -> bool;

    fn take_frame(&self) -> Option<Vec<u8>>;

    fn shutdown(&self);
}

#[derive(Clone)]
pub struct NetDeclaration {
    imm_mac: [u8; 6],
    imm_maximum_queue_size: u16,
    own_imm_backend: Arc<dyn NetBackend>,
}

impl NetDeclaration {
    pub fn new(
        mac: [u8; 6],
        maximum_queue_size: u16,
        backend: Arc<dyn NetBackend>,
    ) -> Result<Self, DeviceError> {
        if maximum_queue_size == 0 || !maximum_queue_size.is_power_of_two() {
            return Err(DeviceError::InvalidLayout("invalid virtio-net queue size"));
        }
        if mac == [0; 6] || mac[0] & 1 != 0 {
            return Err(DeviceError::InvalidLayout("invalid virtio-net MAC address"));
        }
        Ok(Self {
            imm_mac: mac,
            imm_maximum_queue_size: maximum_queue_size,
            own_imm_backend: backend,
        })
    }

    pub const fn mac(&self) -> [u8; 6] {
        self.imm_mac
    }

    pub fn config_bytes(&self) -> [u8; 8] {
        let mut bytes = [0u8; 8];
        bytes[..6].copy_from_slice(&self.imm_mac);
        bytes[6..].copy_from_slice(&VIRTIO_NET_S_LINK_UP.to_le_bytes());
        bytes
    }
}

struct NetDevice {
    own_imm_resources: DeviceResources,
    own_imm_backend: Arc<dyn NetBackend>,
    own_mut_queue_states: Mutex<Vec<QueueState>>,
    atomic_mut_kicked: AtomicBool,
    atomic_mut_down: AtomicBool,
}

#[async_trait]
impl DeviceDeclaration for NetDeclaration {
    fn layout(&self) -> DeviceLayout {
        DeviceLayout {
            queue_count: QUEUE_COUNT,
            maximum_queue_size: self.imm_maximum_queue_size,
            notifier_count: NOTIFIER_COUNT,
            required_features: VIRTIO_F_VERSION_1,
            optional_features: VIRTIO_NET_F_MAC | VIRTIO_NET_F_STATUS,
        }
    }

    async fn activate(
        &self,
        resources: DeviceResources,
    ) -> Result<Box<dyn DeviceInstance>, DeviceError> {
        resources.validate(&self.layout())?;
        Ok(Box::new(NetDevice {
            own_imm_resources: resources,
            own_imm_backend: Arc::clone(&self.own_imm_backend),
            own_mut_queue_states: Mutex::new(vec![QueueState::new(); QUEUE_COUNT]),
            atomic_mut_kicked: AtomicBool::new(false),
            atomic_mut_down: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl DeviceInstance for NetDevice {
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
        self.process_rx().await
    }

    async fn shutdown(&self, _reason: DeviceDownReason) -> Result<(), DeviceError> {
        self.atomic_mut_down.store(true, Ordering::Release);
        self.own_imm_backend.shutdown();
        self.own_imm_resources.dma.revoke();
        self.own_imm_resources.dma.wait_for_drain().await;
        Ok(())
    }
}

impl NetDevice {
    async fn process_tx(&self) -> Result<(), DeviceError> {
        loop {
            let chain = self.pop(QUEUE_TX).await?;
            let Some(chain) = chain else {
                return Ok(());
            };
            let frame = read_tx_frame(&self.own_imm_resources.dma, &chain)?;
            self.own_imm_backend.transmit(frame).await?;
            self.complete(QUEUE_TX, &chain, 0).await?;
        }
    }

    async fn process_rx(&self) -> Result<(), DeviceError> {
        while self.own_imm_backend.has_frame() {
            let Some(chain) = self.pop(QUEUE_RX).await? else {
                return Ok(());
            };
            let frame = self
                .own_imm_backend
                .take_frame()
                .ok_or(DeviceError::Descriptor("virtio-net frame disappeared"))?;
            let used_length = write_rx_frame(&self.own_imm_resources.dma, &chain, &frame)?;
            self.complete(QUEUE_RX, &chain, used_length).await?;
        }
        Ok(())
    }

    async fn pop(&self, index: usize) -> Result<Option<DescriptorChain>, DeviceError> {
        let queue = VirtQueue::new(
            self.own_imm_resources.queues[index],
            &self.own_imm_resources.dma,
        )?;
        let mut states = self.own_mut_queue_states.lock().await;
        queue.pop(&self.own_imm_resources.dma, &mut states[index])
    }

    async fn complete(
        &self,
        index: usize,
        chain: &DescriptorChain,
        used_length: u32,
    ) -> Result<(), DeviceError> {
        let queue = VirtQueue::new(
            self.own_imm_resources.queues[index],
            &self.own_imm_resources.dma,
        )?;
        let mut states = self.own_mut_queue_states.lock().await;
        queue.complete(
            &self.own_imm_resources.dma,
            &mut states[index],
            chain,
            used_length,
        )?;
        drop(states);
        self.own_imm_resources.interrupts[index]
            .notify(Interrupt::Queue {
                queue_index: u16::try_from(index).expect("fixed queue index"),
                vector: u16::try_from(index).expect("fixed vector"),
            })
            .await
    }
}

fn read_tx_frame(memory: &DmaMemory, chain: &DescriptorChain) -> Result<Vec<u8>, DeviceError> {
    if chain
        .descriptors
        .iter()
        .any(|descriptor| descriptor.flags & VIRTQ_DESC_F_WRITE != 0)
    {
        return Err(DeviceError::Descriptor("virtio-net TX chain is writable"));
    }
    let bytes = read_chain(memory, chain)?;
    if bytes.len() <= HEADER_SIZE || bytes.len() - HEADER_SIZE > MAXIMUM_FRAME_SIZE {
        return Err(DeviceError::Descriptor(
            "invalid virtio-net TX frame length",
        ));
    }
    Ok(bytes[HEADER_SIZE..].to_vec())
}

fn write_rx_frame(
    memory: &DmaMemory,
    chain: &DescriptorChain,
    frame: &[u8],
) -> Result<u32, DeviceError> {
    if frame.is_empty() || frame.len() > MAXIMUM_FRAME_SIZE {
        return Err(DeviceError::Descriptor(
            "invalid virtio-net RX frame length",
        ));
    }
    if chain
        .descriptors
        .iter()
        .any(|descriptor| descriptor.flags & VIRTQ_DESC_F_WRITE == 0)
    {
        return Err(DeviceError::Descriptor("virtio-net RX chain is readable"));
    }
    let mut bytes = [0u8; HEADER_SIZE].to_vec();
    bytes.extend_from_slice(frame);
    write_chain(memory, chain, &bytes)?;
    u32::try_from(bytes.len())
        .map_err(|_| DeviceError::Descriptor("virtio-net used length overflow"))
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

fn write_chain(
    memory: &DmaMemory,
    chain: &DescriptorChain,
    bytes: &[u8],
) -> Result<(), DeviceError> {
    let capacity = chain
        .descriptors
        .iter()
        .try_fold(0usize, |total, descriptor| {
            total
                .checked_add(usize::try_from(descriptor.length).expect("u32 fits usize"))
                .ok_or(DeviceError::Descriptor(
                    "virtio-net RX capacity overflows usize",
                ))
        })?;
    if capacity < bytes.len() {
        return Err(DeviceError::Descriptor("virtio-net RX chain is too short"));
    }
    let mut offset = 0usize;
    for descriptor in &chain.descriptors {
        if offset == bytes.len() {
            break;
        }
        let length = usize::try_from(descriptor.length).expect("u32 fits usize");
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
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::{
        NetBackend, NetDeclaration, VIRTIO_F_VERSION_1, VIRTIO_NET_F_MAC, VIRTIO_NET_F_STATUS,
        VIRTIO_NET_S_LINK_UP,
    };
    use crate::device::DeviceDeclaration;
    use crate::error::DeviceError;

    struct TestBackend;

    #[async_trait]
    impl NetBackend for TestBackend {
        async fn transmit(&self, _frame: Vec<u8>) -> Result<(), DeviceError> {
            Ok(())
        }

        fn has_frame(&self) -> bool {
            false
        }

        fn take_frame(&self) -> Option<Vec<u8>> {
            None
        }

        fn shutdown(&self) {}
    }

    #[test]
    fn declaration_exposes_modern_link_up_config() {
        let declaration = NetDeclaration::new([0x02, 0, 0, 0, 0, 1], 128, Arc::new(TestBackend))
            .expect("valid declaration");
        let layout = declaration.layout();
        assert_eq!(layout.queue_count, 2);
        assert_eq!(layout.notifier_count, 3);
        assert_eq!(layout.required_features, VIRTIO_F_VERSION_1);
        assert_eq!(
            layout.optional_features,
            VIRTIO_NET_F_MAC | VIRTIO_NET_F_STATUS
        );
        let config = declaration.config_bytes();
        assert_eq!(&config[..6], &[0x02, 0, 0, 0, 0, 1]);
        assert_eq!(
            u16::from_le_bytes(config[6..].try_into().expect("status")),
            VIRTIO_NET_S_LINK_UP
        );
        assert!(NetDeclaration::new([0; 6], 128, Arc::new(TestBackend)).is_err());
        assert!(NetDeclaration::new([1, 0, 0, 0, 0, 1], 128, Arc::new(TestBackend)).is_err());
    }
}
