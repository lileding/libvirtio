//! Transport-independent asynchronous virtio device implementations.
//!
//! The embedding monitor owns PCIe/MMIO transport, DMA mapping, the async
//! runtime, and device lifecycle.  This crate owns only virtio device logic.

pub mod block;
pub mod device;
pub mod dma;
pub mod error;
pub mod interrupt;
pub mod queue;

pub use block::{BlockDeclaration, BlockDevice};
pub use device::{DeviceDeclaration, DeviceInstance, DeviceLayout, DeviceResources};
pub use dma::{DmaLease, DmaMemory, DmaPart, DmaRange, DmaSegment};
pub use error::{DeviceDownReason, DeviceError};
pub use interrupt::{Interrupt, InterruptNotifier};
pub use queue::{QueueLayout, QueueState, VirtQueue};
