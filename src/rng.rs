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

use crate::device::{DeviceDeclaration, DeviceInstance, DeviceLayout, DeviceResources};
use crate::dma::{DmaMemory, DmaRange};
use crate::error::{DeviceDownReason, DeviceError};
use crate::interrupt::Interrupt;
use crate::queue::{DescriptorChain, QueueState, VirtQueue};

pub const VIRTIO_F_VERSION_1: u64 = 1 << 32;

const QUEUE_RNG: usize = 0;
const QUEUE_COUNT: usize = 1;
const VIRTQ_DESC_F_WRITE: u16 = 2;

#[derive(Clone, Debug)]
pub struct RngDeclaration {
    imm_maximum_queue_size: u16,
}

impl RngDeclaration {
    pub fn new(maximum_queue_size: u16) -> Result<Self, DeviceError> {
        if maximum_queue_size == 0 || !maximum_queue_size.is_power_of_two() {
            return Err(DeviceError::InvalidLayout("invalid virtio-rng queue size"));
        }
        Ok(Self {
            imm_maximum_queue_size: maximum_queue_size,
        })
    }
}

struct RngDevice {
    own_imm_resources: DeviceResources,
    own_mut_queue_state: Mutex<QueueState>,
    own_imm_wake: Notify,
    atomic_mut_down: AtomicBool,
}

#[async_trait]
impl DeviceDeclaration for RngDeclaration {
    fn layout(&self) -> DeviceLayout {
        DeviceLayout {
            queue_count: QUEUE_COUNT,
            maximum_queue_size: self.imm_maximum_queue_size,
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
            own_imm_resources: resources,
            own_mut_queue_state: Mutex::new(QueueState::new()),
            own_imm_wake: Notify::new(),
            atomic_mut_down: AtomicBool::new(false),
        }))
    }
}

#[async_trait]
impl DeviceInstance for RngDevice {
    fn kick(&self) {
        self.own_imm_wake.notify_one();
    }

    fn stop(&self, _reason: DeviceDownReason) {
        self.atomic_mut_down.store(true, Ordering::Release);
        self.own_imm_resources.dma.revoke();
        self.own_imm_wake.notify_waiters();
    }

    async fn run(&self) -> Result<(), DeviceError> {
        loop {
            let notified = self.own_imm_wake.notified();
            if self.atomic_mut_down.load(Ordering::Acquire) {
                self.own_imm_resources.dma.wait_for_drain().await;
                return Ok(());
            }
            notified.await;
            if self.atomic_mut_down.load(Ordering::Acquire) {
                self.own_imm_resources.dma.wait_for_drain().await;
                return Ok(());
            }
            while self.process_queue().await? {}
        }
    }
}

impl RngDevice {
    async fn process_queue(&self) -> Result<bool, DeviceError> {
        let queue = VirtQueue::new(
            self.own_imm_resources.queues[QUEUE_RNG],
            &self.own_imm_resources.dma,
        )?;
        let chain = {
            let mut state = self.own_mut_queue_state.lock().await;
            queue.pop(&self.own_imm_resources.dma, &mut state)?
        };
        let Some(chain) = chain else {
            return Ok(false);
        };
        let length = fill_chain(&self.own_imm_resources.dma, &chain)?;
        {
            let mut state = self.own_mut_queue_state.lock().await;
            queue.complete(&self.own_imm_resources.dma, &mut state, &chain, length)?;
        }
        self.own_imm_resources.interrupts[QUEUE_RNG]
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
    use super::{RngDeclaration, VIRTIO_F_VERSION_1};
    use crate::device::DeviceDeclaration;

    #[test]
    fn declaration_requires_modern_virtio() {
        let declaration = RngDeclaration::new(128).expect("declaration");
        let layout = declaration.layout();
        assert_eq!(layout.queue_count, 1);
        assert_eq!(layout.required_features, VIRTIO_F_VERSION_1);
    }
}
