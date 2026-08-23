//! Deprecated compatibility facade for [`libvirtio`](https://crates.io/crates/libvirtio).
//!
//! New code should depend on `libvirtio` and import its Rust crate as `virtio`.

#[deprecated(
    since = "0.1.1",
    note = "the virtiod package was renamed to libvirtio; depend on libvirtio and use virtio instead"
)]
pub use virtio::*;
