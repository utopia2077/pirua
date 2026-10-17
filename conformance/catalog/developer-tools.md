# Catalog developer tool

`catalog_gen` is the explicit `pi-ai` binary gated by the default-off
`dev-catalog-tools` feature. It validates and exports the release-bound chat
projection for OpenAI, Google, Anthropic and DeepSeek. There is no image catalog
binary or live feed refresh in this scope.

```sh
cargo run -p pi-ai --features dev-catalog-tools --bin catalog_gen --locked --offline -- --check
cargo run -p pi-ai --features dev-catalog-tools --bin catalog_gen --locked --offline -- --output selected-models.json
```

Run from a checkout containing the committed catalog evidence and Python 3.11 or
newer at `/usr/bin/python3`. The binary passes exact arguments through a cleared
child environment. Help works without workspace assets. The release checker
verifies archive integrity, source association, member hashes and all four chat
projections before an export is published; no network request is made.

The artifact contains `schemaVersion`, `family`, `provenance` and `catalogs`.
Only the `text` family is supported. Providers are ordered openai, google,
anthropic, deepseek; each preserves the release model order. The four projections
contain 85 chat models. [Catalog provenance](README.md) describes their evidence.

Export writes and closes a private temporary file in the destination filesystem,
then publishes it atomically with a non-replacing hard link. Existing files,
symlinks and racing creators are preserved. Failure cleanup owns only the private
temporary directory and never removes the final destination. The destination's
parent must exist and support hard links. There is no overwrite fallback or
power-loss durability guarantee.

Rust binary tests and Python publication controls check argument/error handling,
exact release projection, existing-file refusal, cleanup, failed writes/flushes,
failed publication and competing creators. A validation failure must leave no
partial final artifact. The complete gate is [scripts/verify.sh](../../scripts/verify.sh).
