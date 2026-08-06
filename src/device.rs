use std::sync::Arc;

use async_trait::async_trait;

use crate::dma::DmaMemory;
use crate::error::{DeviceDownReason, DeviceError};
use crate::interrupt::InterruptNotifier;
use crate::queue::QueueLayout;

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
pub trait DeviceDeclaration: Send + Sync {
    fn layout(&self) -> DeviceLayout;

    async fn activate(
        &self,
        resources: DeviceResources,
    ) -> Result<Box<dyn DeviceInstance>, DeviceError>;
}

#[async_trait]
pub trait DeviceInstance: Send + Sync {
    fn kick(&self);

    async fn process_kick(&self) -> Result<(), DeviceError>;

    async fn shutdown(&self, reason: DeviceDownReason) -> Result<(), DeviceError>;
}
