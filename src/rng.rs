//! Host-CSPRNG implementation of virtio-rng.
//!
//! This module is compiled only for targets whose `libc` binding declares
//! `arc4random_buf(3)`. It deliberately has no fallback entropy provider.

#![cfg(any(
    target_vendor = "apple",
    target_os = "android",
    target_os = "cygwin",
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "haiku",
    target_os = "illumos",
    target_os = "netbsd",
    target_os = "nuttx",
    target_os = "openbsd",
    target_os = "rtems",
    target_os = "solaris",
    target_os = "wasi",
))]

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

const QUEUE_RNG: usize = 0;
const QUEUE_COUNT: usize = 1;
const VIRTQ_DESC_F_WRITE: u16 = 2;

#[derive(Clone, Debug)]
pub struct RngSpec {
    maximum_queue_size: u16,
}

impl RngSpec {
    pub fn new(maximum_queue_size: u16) -> Result<Self, DeviceError> {
        if maximum_queue_size == 0 || !maximum_queue_size.is_power_of_two() {
            return Err(DeviceError::InvalidLayout("invalid virtio-rng queue size"));
        }
        Ok(Self { maximum_queue_size })
    }
}

struct RngDevice {
    resources: DeviceResources,
    queue_state: Mutex<QueueState>,
    wake: Notify,
    down: AtomicBool,
}

#[async_trait]
impl DeviceSpec for RngSpec {
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
        Ok(Arc::new(RngDevice {
            resources,
            queue_state: Mutex::new(QueueState::new()),
            wake: Notify::new(),
            down: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl DeviceInstance for RngDevice {
    fn kick(&self) {
        self.wake.notify_one();
    }

    fn stop(&self, _reason: DeviceDownReason) {
        self.down.store(true, Ordering::Release);
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
            while self.process_queue().await? {}
        }
    }
}

impl RngDevice {
    async fn process_queue(&self) -> Result<bool, DeviceError> {
        let queue = VirtQueue::new(self.resources.queues[QUEUE_RNG], &self.resources.dma)?;
        let chain = {
            let mut state = self.queue_state.lock().await;
            queue.pop(&self.resources.dma, &mut state)?
        };
        let Some(chain) = chain else {
            return Ok(false);
        };
        let length = fill_chain(&self.resources.dma, &chain)?;
        {
            let mut state = self.queue_state.lock().await;
            queue.complete(&self.resources.dma, &mut state, &chain, length)?;
        }
        self.resources.interrupts[QUEUE_RNG]
            .notify(Interrupt::Queue {
                queue_index: QUEUE_RNG as u16,
                vector: QUEUE_RNG as u16,
            })
            .await?;
        Ok(true)
    }
}

fn fill_chain(memory: &DmaMemory, chain: &DescriptorChain) -> Result<u32, DeviceError> {
    let mut length = 0usize;
    for descriptor in &chain.descriptors {
        if descriptor.flags & VIRTQ_DESC_F_WRITE == 0 || descriptor.length == 0 {
            return Err(DeviceError::Descriptor(
                "virtio-rng descriptor is not writable",
            ));
        }
        let range = DmaRange::new(
            descriptor.address,
            usize::try_from(descriptor.length).expect("u32 fits usize"),
        );
        let mut lease = memory.lease(range)?;
        for part in lease.parts_mut() {
            let bytes = unsafe { part.write_slice() };
            unsafe { libc::arc4random_buf(bytes.as_mut_ptr().cast(), bytes.len()) };
            length = length
                .checked_add(bytes.len())
                .ok_or(DeviceError::Descriptor("virtio-rng length overflow"))?;
        }
    }
    u32::try_from(length).map_err(|_| DeviceError::Descriptor("virtio-rng length exceeds u32"))
}

#[cfg(test)]
mod tests {
    use super::{RngSpec, VIRTIO_F_VERSION_1};
    use crate::device::DeviceSpec;

    #[test]
    fn spec_requires_modern_virtio() {
        let spec = RngSpec::new(128).expect("spec");
        let layout = spec.layout();
        assert_eq!(layout.queue_count, 1);
        assert_eq!(layout.required_features, VIRTIO_F_VERSION_1);
    }
}
