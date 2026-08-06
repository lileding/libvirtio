use async_trait::async_trait;

use crate::error::DeviceError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Interrupt {
    Queue { queue_index: u16, vector: u16 },
    Config { vector: u16 },
}

#[async_trait]
pub trait InterruptNotifier: Send + Sync {
    async fn notify(&self, interrupt: Interrupt) -> Result<(), DeviceError>;
}
