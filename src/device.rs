use std::sync::Arc;

use async_trait::async_trait;

use crate::dma::DmaMemory;
use crate::error::{DeviceDownReason, DeviceError};
use crate::interrupt::InterruptNotifier;
use crate::queue::QueueLayout;

pub trait DeviceConfig: Send + Sync {
    fn size(&self) -> usize;
    fn read(&self, offset: usize, bytes: &mut [u8]) -> Result<(), DeviceError>;
    fn subscribe(&self) -> tokio::sync::watch::Receiver<u64>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeviceLayout {
    pub queue_count: usize,
    pub maximum_queue_size: u16,
    pub notifier_count: usize,
    pub required_features: u64,
    pub optional_features: u64,
}

pub struct DeviceResources {
    pub queues: Vec<QueueLayout>,
    pub dma: DmaMemory,
    pub interrupts: Vec<Arc<dyn InterruptNotifier>>,
    pub negotiated_features: u64,
}

impl DeviceResources {
    pub fn validate(&self, layout: &DeviceLayout) -> Result<(), DeviceError> {
        if self.queues.len() != layout.queue_count {
            return Err(DeviceError::InvalidLayout("unexpected queue count"));
        }
        if self.interrupts.len() < layout.notifier_count {
            return Err(DeviceError::InvalidLayout("missing interrupt notifier"));
        }
        if self.negotiated_features & layout.required_features != layout.required_features {
            return Err(DeviceError::InvalidLayout(
                "required feature not negotiated",
            ));
        }
        for queue in &self.queues {
            if queue.size > layout.maximum_queue_size {
                return Err(DeviceError::InvalidQueue {
                    queue: queue.index,
                    reason: "queue size exceeds device limit",
                });
            }
            queue.validate(&self.dma)?;
        }
        Ok(())
    }
}

#[async_trait]
pub trait DeviceSpec: Send + Sync {
    fn layout(&self) -> DeviceLayout;

    async fn activate(
        &self,
        resources: DeviceResources,
    ) -> Result<Arc<dyn DeviceInstance>, DeviceError>;
}

pub(crate) fn closes_host_backend(reason: DeviceDownReason) -> bool {
    reason == DeviceDownReason::SurpriseRemoval
}

#[async_trait]
pub trait DeviceInstance: Send + Sync {
    fn kick(&self);

    fn stop(&self, reason: DeviceDownReason);

    fn config(&self) -> Option<Arc<dyn DeviceConfig>> {
        None
    }

    /// Runs until `stop()` has revoked this device's DMA generation.
    ///
    /// The transport owns the task which polls this future. `kick()` is
    /// deliberately non-blocking so a transport can continue to receive
    /// power, reset, and removal messages while I/O is in flight.
    async fn run(&self) -> Result<(), DeviceError>;
}

#[cfg(test)]
mod tests {
    use super::closes_host_backend;
    use crate::error::DeviceDownReason;

    #[test]
    fn only_surprise_removal_closes_host_backend() {
        assert!(!closes_host_backend(DeviceDownReason::Stop));
        assert!(!closes_host_backend(DeviceDownReason::Reset));
        assert!(!closes_host_backend(DeviceDownReason::Revoked));
        assert!(closes_host_backend(DeviceDownReason::SurpriseRemoval));
    }
}
