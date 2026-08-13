use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify};

use crate::device::{DeviceInstance, DeviceLayout, DeviceResources, DeviceSpec};
use crate::dma::{DmaMemory, DmaRange};
use crate::error::{DeviceDownReason, DeviceError};
use crate::interrupt::Interrupt;
use crate::queue::{DescriptorChain, QueueState, VirtQueue};

pub const VIRTIO_F_VERSION_1: u64 = 1 << 32;
pub const VIRTIO_NET_F_MAC: u64 = 1 << 5;
pub const VIRTIO_NET_F_STATUS: u64 = 1 << 16;
pub const VIRTIO_NET_F_CTRL_VQ: u64 = 1 << 17;
pub const VIRTIO_NET_F_MQ: u64 = 1 << 22;
pub const VIRTIO_NET_S_LINK_UP: u16 = 1;

const VIRTIO_NET_CTRL_MQ: u8 = 4;
const VIRTIO_NET_CTRL_MQ_VQ_PAIRS_SET: u8 = 0;
const VIRTIO_NET_OK: u8 = 0;
const VIRTIO_NET_ERR: u8 = 1;
const VIRTQ_DESC_F_WRITE: u16 = 2;
const HEADER_SIZE: usize = 12;
const MAXIMUM_FRAME_SIZE: usize = 65_536;

#[async_trait]
pub trait NetworkBackend: Send + Sync {
    async fn transmit(&self, frame: Vec<u8>) -> Result<(), DeviceError>;

    fn has_frame(&self) -> bool;

    fn take_frame(&self) -> Option<Vec<u8>>;

    fn shutdown(&self);
}

#[derive(Clone)]
pub struct NetworkSpec {
    mac: [u8; 6],
    queue_pairs: usize,
    maximum_queue_size: u16,
    backend: Arc<dyn NetworkBackend>,
}

impl NetworkSpec {
    pub fn new(
        mac: [u8; 6],
        queue_pairs: usize,
        maximum_queue_size: u16,
        backend: Arc<dyn NetworkBackend>,
    ) -> Result<Self, DeviceError> {
        if queue_pairs == 0 || queue_pairs > usize::from(u16::MAX) {
            return Err(DeviceError::InvalidLayout(
                "invalid virtio-net queue pair count",
            ));
        }
        if maximum_queue_size == 0 || !maximum_queue_size.is_power_of_two() {
            return Err(DeviceError::InvalidLayout("invalid virtio-net queue size"));
        }
        if mac == [0; 6] || mac[0] & 1 != 0 {
            return Err(DeviceError::InvalidLayout("invalid virtio-net MAC address"));
        }
        Ok(Self {
            mac,
            queue_pairs,
            maximum_queue_size,
            backend,
        })
    }

    pub const fn mac(&self) -> [u8; 6] {
        self.mac
    }

    pub fn config_bytes(&self) -> [u8; 10] {
        let mut bytes = [0u8; 10];
        bytes[..6].copy_from_slice(&self.mac);
        bytes[6..8].copy_from_slice(&VIRTIO_NET_S_LINK_UP.to_le_bytes());
        bytes[8..].copy_from_slice(
            &u16::try_from(self.queue_pairs)
                .expect("validated queue pair count")
                .to_le_bytes(),
        );
        bytes
    }
}

struct NetworkDevice {
    resources: DeviceResources,
    backend: Arc<dyn NetworkBackend>,
    queue_states: Mutex<Vec<QueueState>>,
    wake: Notify,
    kicked: AtomicBool,
    down: AtomicBool,
    active_queue_pairs: AtomicUsize,
    next_rx_pair: AtomicUsize,
}

#[async_trait]
impl DeviceSpec for NetworkSpec {
    fn layout(&self) -> DeviceLayout {
        DeviceLayout {
            queue_count: self.queue_pairs * 2 + 1,
            maximum_queue_size: self.maximum_queue_size,
            notifier_count: self.queue_pairs * 2 + 2,
            required_features: VIRTIO_F_VERSION_1,
            optional_features: VIRTIO_NET_F_MAC
                | VIRTIO_NET_F_STATUS
                | VIRTIO_NET_F_CTRL_VQ
                | VIRTIO_NET_F_MQ,
        }
    }

    async fn activate(
        &self,
        resources: DeviceResources,
    ) -> Result<Arc<dyn DeviceInstance>, DeviceError> {
        resources.validate(&self.layout())?;
        let queue_count = resources.queues.len();
        Ok(Arc::new(NetworkDevice {
            resources,
            backend: Arc::clone(&self.backend),
            queue_states: Mutex::new(vec![QueueState::new(); queue_count]),
            wake: Notify::new(),
            kicked: AtomicBool::new(false),
            down: AtomicBool::new(false),
            active_queue_pairs: AtomicUsize::new(1),
            next_rx_pair: AtomicUsize::new(0),
        }))
    }
}

#[async_trait]
impl DeviceInstance for NetworkDevice {
    fn kick(&self) {
        self.kicked.store(true, Ordering::Release);
        self.wake.notify_one();
    }

    fn stop(&self, _reason: DeviceDownReason) {
        self.down.store(true, Ordering::Release);
        self.backend.shutdown();
        self.resources.dma.revoke();
        self.wake.notify_waiters();
    }

    async fn run(&self) -> Result<(), DeviceError> {
        loop {
            let notified = self.wake.notified();
            if self.down.load(Ordering::Acquire) {
                self.resources.dma.wait_for_drain().await;
                return Ok(());
            }
            notified.await;
            if self.down.load(Ordering::Acquire) {
                self.resources.dma.wait_for_drain().await;
                return Ok(());
            }
            if !self.kicked.swap(false, Ordering::AcqRel) {
                continue;
            }
            self.process_control().await?;
            self.process_tx().await?;
            self.process_rx().await?;
        }
    }
}

impl NetworkDevice {
    async fn process_tx(&self) -> Result<(), DeviceError> {
        for pair in 0..self.active_queue_pairs() {
            loop {
                let queue_index = tx_queue(pair);
                let chain = self.pop(queue_index).await?;
                let Some(chain) = chain else {
                    break;
                };
                let frame = read_tx_frame(&self.resources.dma, &chain)?;
                self.backend.transmit(frame).await?;
                self.complete(queue_index, &chain, 0).await?;
            }
        }
        Ok(())
    }

    async fn process_rx(&self) -> Result<(), DeviceError> {
        while self.backend.has_frame() {
            let pair = self.next_rx_pair.fetch_add(1, Ordering::AcqRel) % self.active_queue_pairs();
            let queue_index = rx_queue(pair);
            let Some(chain) = self.pop(queue_index).await? else {
                return Ok(());
            };
            let frame = self
                .backend
                .take_frame()
                .ok_or(DeviceError::Descriptor("virtio-net frame disappeared"))?;
            let used_length = write_rx_frame(&self.resources.dma, &chain, &frame)?;
            self.complete(queue_index, &chain, used_length).await?;
        }
        Ok(())
    }

    async fn process_control(&self) -> Result<(), DeviceError> {
        let queue_index = control_queue(self.maximum_queue_pairs());
        loop {
            let Some(chain) = self.pop(queue_index).await? else {
                return Ok(());
            };
            let status = self.handle_control(&chain)?;
            write_control_status(&self.resources.dma, &chain, status)?;
            self.complete(queue_index, &chain, 1).await?;
        }
    }

    fn handle_control(&self, chain: &DescriptorChain) -> Result<u8, DeviceError> {
        if chain.descriptors.len() < 2 {
            return Err(DeviceError::Descriptor(
                "virtio-net control chain is too short",
            ));
        }
        let header = chain.descriptors[0];
        if header.flags & VIRTQ_DESC_F_WRITE != 0 || header.length != 2 {
            return Err(DeviceError::Descriptor("invalid virtio-net control header"));
        }
        let header = read_chain_range(&self.resources.dma, header.address, 2)?;
        if header[0] != VIRTIO_NET_CTRL_MQ || header[1] != VIRTIO_NET_CTRL_MQ_VQ_PAIRS_SET {
            return Ok(VIRTIO_NET_ERR);
        }
        let data = chain.descriptors[1];
        if data.flags & VIRTQ_DESC_F_WRITE != 0 || data.length != 2 {
            return Err(DeviceError::Descriptor(
                "invalid virtio-net queue-pair request",
            ));
        }
        let data = read_chain_range(&self.resources.dma, data.address, 2)?;
        let pairs = usize::from(u16::from_le_bytes(
            data.as_slice().try_into().expect("two bytes"),
        ));
        if pairs == 0 || pairs > self.maximum_queue_pairs() {
            return Ok(VIRTIO_NET_ERR);
        }
        self.active_queue_pairs.store(pairs, Ordering::Release);
        Ok(VIRTIO_NET_OK)
    }

    fn maximum_queue_pairs(&self) -> usize {
        (self.resources.queues.len() - 1) / 2
    }

    fn active_queue_pairs(&self) -> usize {
        self.active_queue_pairs.load(Ordering::Acquire)
    }

    async fn pop(&self, index: usize) -> Result<Option<DescriptorChain>, DeviceError> {
        let queue = VirtQueue::new(self.resources.queues[index], &self.resources.dma)?;
        let mut states = self.queue_states.lock().await;
        queue.pop(&self.resources.dma, &mut states[index])
    }

    async fn complete(
        &self,
        index: usize,
        chain: &DescriptorChain,
        used_length: u32,
    ) -> Result<(), DeviceError> {
        let queue = VirtQueue::new(self.resources.queues[index], &self.resources.dma)?;
        let mut states = self.queue_states.lock().await;
        queue.complete(&self.resources.dma, &mut states[index], chain, used_length)?;
        drop(states);
        self.resources.interrupts[index]
            .notify(Interrupt::Queue {
                queue_index: u16::try_from(index).expect("fixed queue index"),
                vector: u16::try_from(index).expect("fixed vector"),
            })
            .await
    }
}

const fn rx_queue(pair: usize) -> usize {
    pair * 2
}
const fn tx_queue(pair: usize) -> usize {
    pair * 2 + 1
}
const fn control_queue(queue_pairs: usize) -> usize {
    queue_pairs * 2
}

fn read_chain_range(
    memory: &DmaMemory,
    address: u64,
    length: usize,
) -> Result<Vec<u8>, DeviceError> {
    let lease = memory.lease(DmaRange::new(address, length))?;
    let mut bytes = Vec::with_capacity(length);
    for part in lease.parts() {
        bytes.extend_from_slice(unsafe { part.read_slice() });
    }
    if bytes.len() != length {
        return Err(DeviceError::Descriptor("short virtio-net control read"));
    }
    Ok(bytes)
}

fn write_control_status(
    memory: &DmaMemory,
    chain: &DescriptorChain,
    status: u8,
) -> Result<(), DeviceError> {
    let descriptor = chain
        .descriptors
        .last()
        .expect("checked control chain length");
    if descriptor.flags & VIRTQ_DESC_F_WRITE == 0 || descriptor.length != 1 {
        return Err(DeviceError::Descriptor("invalid virtio-net control status"));
    }
    let mut lease = memory.lease(DmaRange::new(descriptor.address, 1))?;
    let bytes = unsafe { lease.parts_mut()[0].write_slice() };
    bytes[0] = status;
    Ok(())
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
        NetworkBackend, NetworkSpec, VIRTIO_F_VERSION_1, VIRTIO_NET_F_CTRL_VQ, VIRTIO_NET_F_MAC,
        VIRTIO_NET_F_MQ, VIRTIO_NET_F_STATUS, VIRTIO_NET_S_LINK_UP,
    };
    use crate::device::DeviceSpec;
    use crate::error::DeviceError;

    struct TestBackend;

    #[async_trait]
    impl NetworkBackend for TestBackend {
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
    fn spec_exposes_modern_link_up_config() {
        let spec = NetworkSpec::new([0x02, 0, 0, 0, 0, 1], 4, 128, Arc::new(TestBackend))
            .expect("valid spec");
        let layout = spec.layout();
        assert_eq!(layout.queue_count, 9);
        assert_eq!(layout.notifier_count, 10);
        assert_eq!(layout.required_features, VIRTIO_F_VERSION_1);
        assert_eq!(
            layout.optional_features,
            VIRTIO_NET_F_MAC | VIRTIO_NET_F_STATUS | VIRTIO_NET_F_CTRL_VQ | VIRTIO_NET_F_MQ
        );
        let config = spec.config_bytes();
        assert_eq!(&config[..6], &[0x02, 0, 0, 0, 0, 1]);
        assert_eq!(
            u16::from_le_bytes(config[6..8].try_into().expect("status")),
            VIRTIO_NET_S_LINK_UP
        );
        assert_eq!(
            u16::from_le_bytes(config[8..10].try_into().expect("pairs")),
            4
        );
        assert!(NetworkSpec::new([0; 6], 1, 128, Arc::new(TestBackend)).is_err());
        assert!(NetworkSpec::new([1, 0, 0, 0, 0, 1], 1, 128, Arc::new(TestBackend)).is_err());
    }
}
