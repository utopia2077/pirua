# Exact-release AI evidence

The active differential oracle executes complete released Pi AI 1.4.0 adapters
and exact declared SDK dependencies. The catalog archive is associated with
source commit `4a080252f06caf63a525d7873c5d94d3c23887e0` by its retained
integrity and SLSA statement. OAuth evidence executes complete unchanged
Anthropic/ChatGPT source bodies from that exact tag with deterministic host
capabilities.

Reproduction, dependency setup, case counts, normalizations, and evidence limits
are documented in [the release corpus](../../../conformance/provider-traces/v1.4.0/README.md).
The narrowed locked suite is:

```sh
cargo test -p pi-ai-core -p pi-ai-transport -p pi-ai-providers -p pi-ai -p pi-telemetry --all-features --locked
```

All provider calls in these tests use fake peers or native loopback servers.
Catalog validation is offline. No live authentication or paid inference is
required. Older protocol corpora remain useful regression inputs; new release
request expectations are generated separately, and removed contracts are
explicitly retired rather than admitted as compatibility aliases.
