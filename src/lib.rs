//! Transport-independent asynchronous virtio device implementations.
//!
//! The embedding monitor owns PCIe/MMIO transport, DMA mapping, the async
//! runtime, and device lifecycle.  This crate owns only virtio device logic.

pub mod block;
pub mod device;
pub mod dma;
pub mod error;
pub mod interrupt;
pub mod net;
pub mod queue;
pub mod vsock;

pub use block::{BlockConfig, BlockDeclaration, BlockDevice};
pub use device::{DeviceDeclaration, DeviceInstance, DeviceLayout, DeviceResources};
pub use dma::{DmaLease, DmaMemory, DmaPart, DmaRange, DmaSegment};
pub use error::{DeviceDownReason, DeviceError};
pub use interrupt::{Interrupt, InterruptNotifier};
pub use net::{
    NetBackend, NetDeclaration, VIRTIO_F_VERSION_1 as NET_F_VERSION_1, VIRTIO_NET_F_MAC,
    VIRTIO_NET_F_STATUS, VIRTIO_NET_S_LINK_UP,
};
pub use queue::{QueueLayout, QueueState, VirtQueue};
pub use vsock::{
    VIRTIO_F_VERSION_1 as VSOCK_F_VERSION_1, VSOCK_HOST_CID, VSOCK_OP_CREDIT_UPDATE,
    VSOCK_OP_REQUEST, VSOCK_OP_RESPONSE, VSOCK_OP_RST, VSOCK_OP_RW, VSOCK_OP_SHUTDOWN,
    VSOCK_TYPE_STREAM, VsockBackend, VsockDeclaration, VsockHeader, VsockPacket,
};
