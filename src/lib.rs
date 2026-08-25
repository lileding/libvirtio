//! Transport-independent asynchronous virtio device implementations.
//!
//! The embedding monitor owns PCIe/MMIO transport, DMA mapping, the async
//! runtime, and device lifecycle.  This crate owns only virtio device logic.

mod backend;
#[cfg(feature = "block")]
pub mod block;
#[cfg(feature = "console")]
pub mod console;
pub mod device;
pub mod dma;
pub mod error;
#[cfg(feature = "fs")]
pub mod fs;
pub mod interrupt;
#[cfg(feature = "memory")]
pub mod memory;
#[cfg(feature = "network")]
pub mod network;
pub mod queue;
#[cfg(all(
    feature = "rng",
    any(
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
    ),
))]
pub mod rng;
#[cfg(feature = "vsock")]
pub mod vsock;

pub use backend::{ConsoleBackend, NetworkBackend};
#[cfg(feature = "block")]
pub use block::{BlockConfig, BlockDevice, BlockSpec};
#[cfg(feature = "console")]
pub use console::ConsoleSpec;
pub use device::{DeviceConfig, DeviceInstance, DeviceLayout, DeviceResources, DeviceSpec};
pub use dma::{DmaLease, DmaMemory, DmaPart, DmaRange, DmaSegment};
pub use error::{DeviceDownReason, DeviceError};
#[cfg(feature = "fs")]
pub use fs::{FsConfig, FsSpec, VIRTIO_F_VERSION_1 as FS_F_VERSION_1, VIRTIO_FS_TAG_SIZE};
pub use interrupt::{Interrupt, InterruptNotifier};
#[cfg(feature = "memory")]
pub use memory::{
    MemoryBackend, MemoryConfig, MemoryConfigState, MemoryRequest, MemoryResponse, MemorySpec,
};
#[cfg(feature = "network")]
pub use network::{
    NetworkSpec, VIRTIO_F_VERSION_1 as NETWORK_F_VERSION_1, VIRTIO_NET_F_CTRL_VQ, VIRTIO_NET_F_MAC,
    VIRTIO_NET_F_MQ, VIRTIO_NET_F_STATUS, VIRTIO_NET_S_LINK_UP,
};
pub use queue::{QueueLayout, QueueState, VirtQueue};
#[cfg(all(
    feature = "rng",
    any(
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
    ),
))]
pub use rng::{RngSpec, VIRTIO_F_VERSION_1 as RNG_F_VERSION_1};
#[cfg(feature = "vsock")]
pub use vsock::{
    VIRTIO_F_VERSION_1 as VSOCK_F_VERSION_1, VIRTIO_VSOCK_F_SEQPACKET, VIRTIO_VSOCK_F_STREAM,
    VSOCK_HOST_CID, VSOCK_OP_CREDIT_REQUEST, VSOCK_OP_CREDIT_UPDATE, VSOCK_OP_REQUEST,
    VSOCK_OP_RESPONSE, VSOCK_OP_RST, VSOCK_OP_RW, VSOCK_OP_SHUTDOWN, VSOCK_TYPE_SEQPACKET,
    VSOCK_TYPE_STREAM, VsockBackend, VsockHeader, VsockPacket, VsockSpec,
};
