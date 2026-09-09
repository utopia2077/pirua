# Invocation-owned Smithy retry and native error facts

`source.mjs` executes installed Smithy core 3.24.3 in five sanitized processes:
absent/false/TRUE/1/true `SMITHY_NEW_RETRIES_2026`. Only exact `true` selects the
2026 policy; changing the environment after import leaves the captured policy
unchanged. Legacy delay/throttle bases are 100/500 ms with transient cost 10 and
other cost 5; 2026 bases are 50/1000 ms with throttling cost 5 and other cost 14.
Both start with quota 500. Standard/adaptive class eligibility, quota exhaustion,
refunds, delay caps, hints and adaptive cubic send waits are retained.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/bedrock-converse-stream/check-source.mjs
cargo test -p pi-ai-transport --features native --test smithy_retry --test smithy_error --locked --offline
```

Retry owns quota, backoff and adaptive rate state; the provider owns signing and
first-modeled-event priming. Adaptive failure observation precedes the separate
maximum-attempts leaf and its send-token wait precedes cooldown. Virtual source
Date.now remains integer milliseconds; fractional waits are retained, then rounded
up by the virtual timer. Rust compares those waits within 0.00001 ms and exact
clock stages. Cancellation drops noncooperative cooldown/adaptive futures.
Explicit headerEntries retain first matching own-key behavior regardless of a
fixture reader's JSON map feature selection. No generic Fetch retry is stacked.

`native-errors.mjs` runs actual Node/Smithy handlers against loopback peers. Its
15 observations cover refused/reset/malformed head, truncated/malformed H1 body,
untrusted certificate, H2 refusal/protocol, and seven fixed refused ports. The
PEM files are newly generated synthetic loopback fixtures, never trusted account
material. Ports 40001, 42901, 50001, 50201, 50301, 50401 and 52401 must be free.
The source checker compares every fixed-port classifier fact. The first original
refused case uses an ephemeral port; its derived assistant flag may vary with that
port, while its normalized diagnostic and SDK class are checked.

TransportError's optional Smithy disposition is populated only at explicit
Smithy native stages before losing typed causes. No annotation means unknown.
Generic Fetch/HTTP/WS constructors and Debug output are unchanged. A separate
optional boolean preserves a proven final assistant retry fact; it never carries
endpoint or library text. Refused single-destination socket data can contain
numeric retry patterns in the source message. The safe fact retains that source
observation without exposing the socket address. SDK refusal eligibility stays
transient for both ordinary and matching ports.

An admitted Node H1 body emits ECONNRESET `aborted` on malformed chunk syntax,
although malformed heads are nonretryable parser failures. Hyper retains a typed
InvalidInput chunk-decoder cause, translated only at the admitted H1 body stage.
Socket-hang-up at head acquisition, body `aborted`, and H2 refused-stream wording
have distinct final assistant classification. Native default-profile controls keep
both new fields absent; provider tests prove request retry and final assistant
classification consume their separate facts.

Single-address loopback causes and the listed typed parser/certificate/protocol
conditions are established. Node multi-address AggregateError selection/wording,
unobserved platform-specific exception text and arbitrary TLS alert prose are not
reconstructed; missing safe final facts remain unknown. This does not change DNS
address iteration, byte parsing, TLS verification, connection policy or old kinds.
Full acceptance remains exclusively `./scripts/verify.sh`.
