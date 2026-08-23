# libvirtio

`libvirtio` is a reusable, transport-independent Rust library implementing
modern virtio devices.  It is intentionally not a daemon.  An embedding
monitor owns the transport, its reactor, the DMA mapping, and the device
lifecycle; this crate owns only virtio device behaviour.

Current consumers are `runv` and `vmon`, which bind the library to the
DragonFlyBSD vmmfs modern virtio-PCI transport.  It remains embeddable by a
monitor using another transport, such as PCI or MMIO.

## Architecture

The embedding monitor follows this sequence:

1. Observe transport power-on and obtain the DMA mapping, queue layouts, and
   interrupt notifiers.
2. Construct a device spec and inspect `DeviceSpec::layout()`.
3. Negotiate the declared virtio features with the guest transport.
4. Call `activate()` with `DeviceResources` to obtain a `DeviceInstance`.
5. Call `kick()` when the transport reports a guest doorbell. The monitor owns
   the task that awaits `DeviceInstance::run()`.
6. Call `stop()` on transport power-off, disconnect, or teardown, then await
   `run()` before reclaiming the DMA mapping.

`DeviceResources` carries a revocable DMA mapping, queue layouts, negotiated
features, and interrupt notifiers.  Device implementations never assume a
specific file-descriptor, kqueue, PCI, MMIO, or hypervisor ABI.

## Implemented Devices

| Device | Current scope |
| --- | --- |
| `BlockSpec` | Raw sector-aligned regular-file virtio-blk; one to four request queues with standard MQ negotiation, read, write, and flush; asynchronous blocking-file work through Tokio. |
| `NetworkSpec` | TAP-backed virtio-net; up to four RX/TX queue pairs, control queue, MAC address, and link-up status. No checksum or GSO offload is advertised. |
| `VsockSpec` | Modern virtio-vsock STREAM and SEQPACKET transport, packet validation, negotiated type enforcement, and revocable lifecycle hooks. |
| `RngSpec` | Modern virtio-rng filled by the platform `arc4random_buf` entropy API on targets where Rust libc declares it. |
| `FsSpec` | Read-only virtio-fs export of one host directory. It implements the FUSE session handshake plus lookup, attributes, open, read, directory listing, and statfs over a hiprio queue plus one or more request queues. |

The virtio-fs implementation does not offer DAX, FUSE notifications, writes,
xattrs, locks, caching guarantees, or symlink traversal. The following are
also deliberately not claimed yet: full virtio-net offloads,
guest-initiated vsock listeners, and SOCK_DGRAM.

## Build

```sh
cargo test
cargo build --release
```

The crate can be imported by Rust callers as:

```rust
use virtio::{BlockSpec, DeviceSpec, DeviceInstance};
```

Release builds emit both a Rust `rlib` and `target/release/libvirtio.so`.
The shared object currently has no published C ABI or stable exported API.
That boundary will be designed separately once a C monitor has a concrete
transport/reactor contract; Rust traits and Tokio objects are not exposed as
a premature C ABI.

## Safety and Ownership

DMA memory is intentionally an explicit capability.  The embedding monitor
must revoke it before releasing the underlying mapping, and must await
`shutdown()` before reclaiming transport resources.  Device code may hold DMA
leases while processing a request; revocation waits for those leases to drain.

The library does not start global workers or own a global runtime.  Its async
operations run in the runtime selected by the embedding monitor.
