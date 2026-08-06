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
#[cfg(any(
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
pub mod rng;
pub mod vsock;

pub use block::{BlockConfig, BlockDeclaration, BlockDevice};
pub use device::{DeviceDeclaration, DeviceInstance, DeviceLayout, DeviceResources};
pub use dma::{DmaLease, DmaMemory, DmaPart, DmaRange, DmaSegment};
pub use error::{DeviceDownReason, DeviceError};
pub use interrupt::{Interrupt, InterruptNotifier};
pub use net::{
    NetBackend, NetDeclaration, VIRTIO_F_VERSION_1 as NET_F_VERSION_1, VIRTIO_NET_F_CTRL_VQ,
    VIRTIO_NET_F_MAC, VIRTIO_NET_F_MQ, VIRTIO_NET_F_STATUS, VIRTIO_NET_S_LINK_UP,
};
pub use queue::{QueueLayout, QueueState, VirtQueue};
#[cfg(any(
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
pub use rng::{RngDeclaration, VIRTIO_F_VERSION_1 as RNG_F_VERSION_1};
pub use vsock::{
    VIRTIO_F_VERSION_1 as VSOCK_F_VERSION_1, VIRTIO_VSOCK_F_SEQPACKET, VIRTIO_VSOCK_F_STREAM,
    VSOCK_HOST_CID, VSOCK_OP_CREDIT_REQUEST, VSOCK_OP_CREDIT_UPDATE, VSOCK_OP_REQUEST,
    VSOCK_OP_RESPONSE, VSOCK_OP_RST, VSOCK_OP_RW, VSOCK_OP_SHUTDOWN, VSOCK_TYPE_SEQPACKET,
    VSOCK_TYPE_STREAM, VsockBackend, VsockDeclaration, VsockHeader, VsockPacket,
};
