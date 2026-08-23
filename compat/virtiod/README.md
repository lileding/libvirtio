# virtiod (deprecated)

`virtiod` was renamed to [`libvirtio`](https://crates.io/crates/libvirtio).
This compatibility release contains no device implementation of its own; it
re-exports `libvirtio 0.1.0` so existing applications can migrate without an
immediate source rewrite.

New code should use:

```toml
[dependencies]
libvirtio = "0.1"
```

```rust
use virtio::BlockSpec;
```

The `virtiod` package will receive compatibility fixes only. New features are
developed in `libvirtio`.
