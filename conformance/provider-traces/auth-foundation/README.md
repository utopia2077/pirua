# Authentication foundation evidence

Authority: pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`.
This unit supplies helper/auth host composition, not the three complete OAuth
flows or selected catalog/factory assembly.

`source.mjs` reads Git objects and executes complete pinned helper, context and
PKCE modules, and the complete Anthropic factory body with configured inert
catalog/adapter/createProvider dependencies. It records each real source hash.
It never loads credentials or calls provider endpoints. The Rust source test
executes it under an empty environment plus PATH/NODE_NO_WARNINGS, compares
all hashes and observations to `source-observations.json`, and fails on drift.
The fixture's PKCE input is deterministic synthetic bytes 0 through 31.

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/auth-foundation/source.mjs
```

Provider tests bind source observations to API-key truthiness/Anthropic priority,
ECMAScript default-context whitespace, selected legacy discovery, memoized lazy
load success/failure and cancellation. Failed/dropped task admission settles a
memoized safe error; cancelling one waiter does not cancel shared module loading.
Native context accepts explicit host operations and configured home expansion;
there is no provider-specific credential discovery or import-time host access.

The transport `auth-native` feature is separate from existing `native`. Its
explicit Tokio-handle service owns bounded listener/connection tasks. A callback
request transfers a one-shot reply capability to the flow; retaining it delays
the HTTP response. `close().await` acknowledges listener and accepted-connection
release, including retained replies; Drop initiates cleanup asynchronously.
Receive cancellation/drop leaves the listener available. Native tests use actual
loopback TCP, physical EOF, refused/rebound listeners, delayed response controls,
capacity/response admission and a synthetic invalid-header control. Native
entropy/SHA256 reuses exact locked ring 0.17.14; deterministic PKCE matches the
complete pinned WebCrypto program, and two real entropy requests differ.

Finite host domain: 1–1024 simultaneous connections, request head 16 bytes–1 MiB,
response body 1 byte–16 MiB configured limit (defaults 32/16 KiB/64 KiB). Request
heads must be UTF-8 with HTTP/1.0 or HTTP/1.1 and an uppercase ASCII method. One
request per connection is served with explicit Content-Length and close; bodies,
keep-alive, chunked requests and exact browser HTML styling are not host protocol
parity claims. Flow-owned validation sees method/target without normalization.
Connection-limit excess is closed. No callback path/token policy lives in host.

Numeric normalization is inherited S02 admission: finite nonnegative safe integer
event intervals and final credential milliseconds only. Fractional seconds are
accepted when the resulting source f64 expiry is integral and safe; fractional
interval events are explicitly rejected, never rounded. Complete OAuth flows
will bind this helper to their token-response stages in the next unit.

Four-way variance: planned helper/native ports implemented; callback close is
acknowledged asynchronously as contract-defined; no actual OAuth flow body or
catalog/factory completion claimed; no extra provider-specific ambient behavior.
Unverified: live accounts, browser interoperability on other platforms, TLS
provider token requests, exact Node server wire formatting, hostile allocation
exhaustion, and future flow-specific claim/manual/timer behavior. Real native
resource tests are not universal Node server parity.
