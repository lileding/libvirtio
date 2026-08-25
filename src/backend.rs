use async_trait::async_trait;

use crate::DeviceError;

#[async_trait]
pub trait ConsoleBackend: Send + Sync {
    fn has_input(&self) -> bool;

    fn read_input(&self, maximum: usize) -> Option<Vec<u8>>;

    async fn write_output(&self, bytes: Vec<u8>) -> Result<(), DeviceError>;

    fn shutdown(&self);
}

#[async_trait]
pub trait NetworkBackend: Send + Sync {
    async fn transmit(&self, frame: Vec<u8>) -> Result<(), DeviceError>;

    fn has_frame(&self) -> bool;

    fn take_frame(&self) -> Option<Vec<u8>>;

    fn shutdown(&self);
}
