//! Single-port virtio-console implementation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use tokio::sync::{Mutex, Notify};

use crate::device::{DeviceInstance, DeviceLayout, DeviceResources, DeviceSpec};
use crate::dma::{DmaMemory, DmaRange};
use crate::error::{DeviceDownReason, DeviceError};
use crate::interrupt::Interrupt;
use crate::queue::{DescriptorChain, QueueState, VirtQueue};

pub const VIRTIO_F_VERSION_1: u64 = 1 << 32;
pub const VIRTIO_CONSOLE_F_SIZE: u64 = 1;
pub const VIRTIO_CONSOLE_F_MULTIPORT: u64 = 1 << 1;
pub const VIRTIO_CONSOLE_F_EMERG_WRITE: u64 = 1 << 2;

const QUEUE_RECEIVE: usize = 0;
const QUEUE_TRANSMIT: usize = 1;
const QUEUE_COUNT: usize = 2;
const VIRTQ_DESC_F_WRITE: u16 = 2;

#[async_trait]
pub trait ConsoleBackend: Send + Sync {
    fn has_input(&self) -> bool;
    fn read_input(&self, maximum: usize) -> Option<Vec<u8>>;
    async fn write_output(&self, bytes: Vec<u8>) -> Result<(), DeviceError>;
    fn shutdown(&self);
}

pub struct ConsoleSpec {
    maximum_queue_size: u16,
    backend: Arc<dyn ConsoleBackend>,
}

impl ConsoleSpec {
    pub fn new(
        maximum_queue_size: u16,
        backend: Arc<dyn ConsoleBackend>,
    ) -> Result<Self, DeviceError> {
        if maximum_queue_size == 0 || !maximum_queue_size.is_power_of_two() {
            return Err(DeviceError::InvalidLayout(
                "invalid virtio-console queue size",
            ));
        }
        Ok(Self {
            maximum_queue_size,
            backend,
        })
    }
}

struct ConsoleDevice {
    resources: DeviceResources,
    backend: Arc<dyn ConsoleBackend>,
    queue_states: Mutex<[QueueState; QUEUE_COUNT]>,
    wake: Notify,
    down: AtomicBool,
}

#[async_trait]
impl DeviceSpec for ConsoleSpec {
    fn layout(&self) -> DeviceLayout {
        DeviceLayout {
            queue_count: QUEUE_COUNT,
            maximum_queue_size: self.maximum_queue_size,
            notifier_count: QUEUE_COUNT,
            required_features: VIRTIO_F_VERSION_1,
            optional_features: 0,
        }
    }

    async fn activate(
        &self,
        resources: DeviceResources,
    ) -> Result<Arc<dyn DeviceInstance>, DeviceError> {
        resources.validate(&self.layout())?;
        Ok(Arc::new(ConsoleDevice {
            resources,
            backend: Arc::clone(&self.backend),
            queue_states: Mutex::new([QueueState::new(); QUEUE_COUNT]),
            wake: Notify::new(),
            down: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl DeviceInstance for ConsoleDevice {
    fn kick(&self) {
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
            while self.process_transmit().await? {}
            while self.process_receive().await? {}
        }
    }
}

impl ConsoleDevice {
    async fn process_transmit(&self) -> Result<bool, DeviceError> {
        let queue = VirtQueue::new(self.resources.queues[QUEUE_TRANSMIT], &self.resources.dma)?;
        let chain = {
            let mut states = self.queue_states.lock().await;
            queue.pop(&self.resources.dma, &mut states[QUEUE_TRANSMIT])?
        };
        let Some(chain) = chain else { return Ok(false) };
        let bytes = read_chain(&self.resources.dma, &chain)?;
        self.backend.write_output(bytes).await?;
        {
            let mut states = self.queue_states.lock().await;
            queue.complete(&self.resources.dma, &mut states[QUEUE_TRANSMIT], &chain, 0)?;
        }
        self.resources.interrupts[QUEUE_TRANSMIT]
            .notify(Interrupt::Queue {
                queue_index: 1,
                vector: 1,
            })
            .await?;
        Ok(true)
    }

    async fn process_receive(&self) -> Result<bool, DeviceError> {
        if !self.backend.has_input() {
            return Ok(false);
        }
        let queue = VirtQueue::new(self.resources.queues[QUEUE_RECEIVE], &self.resources.dma)?;
        let chain = {
            let mut states = self.queue_states.lock().await;
            queue.pop(&self.resources.dma, &mut states[QUEUE_RECEIVE])?
        };
        let Some(chain) = chain else { return Ok(false) };
        let capacity = writable_capacity(&chain)?;
        let bytes = self
            .backend
            .read_input(capacity)
            .ok_or(DeviceError::Down(DeviceDownReason::Stop))?;
        write_chain(&self.resources.dma, &chain, &bytes)?;
        {
            let mut states = self.queue_states.lock().await;
            queue.complete(
                &self.resources.dma,
                &mut states[QUEUE_RECEIVE],
                &chain,
                u32::try_from(bytes.len())
                    .map_err(|_| DeviceError::Descriptor("console input too large"))?,
            )?;
        }
        self.resources.interrupts[QUEUE_RECEIVE]
            .notify(Interrupt::Queue {
                queue_index: 0,
                vector: 0,
            })
            .await?;
        Ok(true)
    }
}

fn read_chain(memory: &DmaMemory, chain: &DescriptorChain) -> Result<Vec<u8>, DeviceError> {
    if chain
        .descriptors
        .iter()
        .any(|descriptor| descriptor.flags & VIRTQ_DESC_F_WRITE != 0)
    {
        return Err(DeviceError::Descriptor(
            "console transmit chain is writable",
        ));
    }
    let mut bytes = Vec::new();
    for descriptor in &chain.descriptors {
        let length = usize::try_from(descriptor.length)
            .map_err(|_| DeviceError::Descriptor("console descriptor length overflows usize"))?;
        let lease = memory.lease(DmaRange::new(descriptor.address, length))?;
        for part in lease.parts() {
            bytes.extend_from_slice(unsafe { part.read_slice() });
        }
    }
    Ok(bytes)
}

fn writable_capacity(chain: &DescriptorChain) -> Result<usize, DeviceError> {
    let mut capacity: usize = 0;
    for descriptor in &chain.descriptors {
        if descriptor.flags & VIRTQ_DESC_F_WRITE == 0 {
            return Err(DeviceError::Descriptor("console receive chain is readable"));
        }
        capacity = capacity
            .checked_add(usize::try_from(descriptor.length).map_err(|_| {
                DeviceError::Descriptor("console descriptor length overflows usize")
            })?)
            .ok_or(DeviceError::Descriptor(
                "console receive capacity overflows usize",
            ))?;
    }
    Ok(capacity)
}

fn write_chain(
    memory: &DmaMemory,
    chain: &DescriptorChain,
    bytes: &[u8],
) -> Result<(), DeviceError> {
    let capacity = writable_capacity(chain)?;
    if bytes.len() > capacity {
        return Err(DeviceError::Descriptor(
            "console receive chain is too short",
        ));
    }
    let mut offset = 0;
    for descriptor in &chain.descriptors {
        if offset == bytes.len() {
            break;
        }
        let length = usize::try_from(descriptor.length)
            .map_err(|_| DeviceError::Descriptor("console descriptor length overflows usize"))?;
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
    use super::{ConsoleBackend, ConsoleSpec, VIRTIO_CONSOLE_F_MULTIPORT, VIRTIO_F_VERSION_1};
    use crate::device::DeviceSpec;
    use std::sync::Arc;

    struct NullBackend;
    #[async_trait::async_trait]
    impl ConsoleBackend for NullBackend {
        fn has_input(&self) -> bool {
            false
        }
        fn read_input(&self, _maximum: usize) -> Option<Vec<u8>> {
            None
        }
        async fn write_output(&self, _bytes: Vec<u8>) -> Result<(), crate::error::DeviceError> {
            Ok(())
        }
        fn shutdown(&self) {}
    }

    #[test]
    fn single_port_layout_is_two_queues() {
        let spec = ConsoleSpec::new(128, Arc::new(NullBackend)).expect("spec");
        let layout = spec.layout();
        assert_eq!(layout.queue_count, 2);
        assert_eq!(layout.required_features, VIRTIO_F_VERSION_1);
        assert_eq!(layout.optional_features & VIRTIO_CONSOLE_F_MULTIPORT, 0);
    }
}
