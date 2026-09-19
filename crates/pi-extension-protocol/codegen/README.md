# Check generated wire DTOs

From the repository root:

```sh
cargo run --manifest-path crates/pi-extension-protocol/codegen/Cargo.toml --locked --offline
```

This isolated package pins `prost-build` 0.14.4. It reads each checked tool
`FileDescriptorSet`, regenerates the combined common/tool Rust module in a
temporary directory, and byte-compares it with `src/wire/generated/v1_0.rs`
and `v1_1.rs`. It then runs `schema/verify.sh`, which requires `protoc` 3.20.3,
Python 3 with the schema verifier's dependencies, and Node. That verifier
rebuilds and compares canonical descriptor bytes and checks descriptor-derived
method metadata and fixtures. Neither check overwrites canonical files.

Normal protocol crate builds use the checked-in modules and do not run this
package or require `protoc`.
