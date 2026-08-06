# virtiod

`virtiod` is a reusable, transport-independent Rust library implementing
modern virtio devices.  It is intentionally not a daemon.  An embedding
monitor owns the transport, its reactor, the DMA mapping, and the device
lifecycle; this crate owns only virtio device behaviour.

The current primary consumer is `runv`, which binds the library to DragonFly
vmm's vPCIe provider ABI.  The library is also intended to be embeddable by a
future monitor using a different transport, such as PCI or MMIO.

## Architecture

The embedding monitor follows this sequence:

1. Observe transport power-on and obtain the DMA mapping, queue layouts, and
   interrupt notifiers.
2. Construct a device declaration and inspect `DeviceDeclaration::layout()`.
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
| `BlockDeclaration` | Raw sector-aligned regular-file virtio-blk; read, write, and flush; asynchronous blocking-file work through Tokio. |
| `NetDeclaration` | TAP-backed virtio-net; up to four RX/TX queue pairs, control queue, MAC address, and link-up status. No checksum or GSO offload is advertised. |
| `VsockDeclaration` | Modern virtio-vsock STREAM and SEQPACKET transport, packet validation, negotiated type enforcement, and revocable lifecycle hooks. |
| `RngDeclaration` | Modern virtio-rng filled by the platform `arc4random_buf` entropy API on targets where Rust libc declares it. |

The following are deliberately not claimed yet: full virtio-net offloads,
guest-initiated vsock listeners, SOCK_DGRAM, and virtio-fs.

## Build

```sh
cargo test
cargo build --release
```

The crate can be imported by Rust callers as:

```rust
use virtiod::{BlockDeclaration, DeviceDeclaration, DeviceInstance};
```

Release builds emit both a Rust `rlib` and `target/release/libvirtiod.so`.
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
