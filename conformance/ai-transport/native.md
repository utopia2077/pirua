# Native transport evidence

The optional `pi-ai-transport/native` feature implements the accepted portable
ports using real TCP/TLS. It does not enable any built-in adapter or provider.

## Construction and lifetime

`NativeHttpClient::new(tokio::runtime::Handle)` implements `RawHttpClient`.
`NativeWebSocketConnector::new(Handle)` implements `WebSocketConnector`.
`TokioClock::new(Handle)` implements the existing core clock port and
`NativeRetryJitter` obtains entropy from the TLS backend's secure random source.
The host keeps its Tokio runtime alive; the consumer need not enter that runtime.
Each pending I/O task is owned by an abort-on-drop guard. Body/socket resources
are moved into the pending task and dropped when abandoned. No detached retry,
session cache, middleware registration or global cleanup is installed.

HTTP returns status, repeated raw headers and an incremental body. Opaque response
header octets are projected one-to-one to Unicode U+0000–U+00FF, not silently
discarded by UTF-8 validation. Outgoing HTTP/WS header values use the inverse
checked projection: `é` sends `e9`, and a scalar above U+00FF fails with redacted
`InvalidRequest` before connection I/O. Sensitive flags remain set; family-level
Fetch whitespace trimming is outside this raw port. Error bodies remain available to adapter-owned
bounded classification. Native errors expose only typed categories; backend error
messages, URLs, proxy credentials and raw payloads are not formatted by this crate.

Reqwest retries, redirects, ambient proxy resolution and idle connection pooling
are disabled explicitly. No implicit total/read/connect timeout is installed.
Reqwest 0.13.4 additionally defaults TCP keepalive idle/interval to 15 seconds,
probe count to three, and TCP_USER_TIMEOUT to 30 seconds on Linux/Android/Fuchsia
(`src/async_impl/client.rs:303-307`). These are socket liveness policies, not
response/read deadlines: TCP_USER_TIMEOUT concerns unacknowledged transmitted
bytes (`:1783`). The backend now explicitly sets keepalive/interval/retries to
None on every platform and TCP_USER_TIMEOUT to None on its supported platforms,
leaving operating-system TCP liveness/retransmission defaults authoritative.
This is source-confirmed configuration scope, not a measured packet-drop timeout
experiment. The direct Tokio WS connector installs no such override. Other
timeout profiles and unrelated socket settings are unchanged.
A Linux `strace -f -e trace=setsockopt` run of the real HTTP/SSE loopback test
observed only TCP_NODELAY on the client connection, with no SO_KEEPALIVE,
TCP_KEEPIDLE/TCP_KEEPINTVL/TCP_KEEPCNT or TCP_USER_TIMEOUT setting. The separate
listener reuse option and server-side test read timeout were visible as expected.
The policy wrapper owns selected timeouts and retry attempts. A no-redirect raw
port does not prove SDK redirect equivalence: family owners must supply explicit
redirect semantics and credential-forwarding tests before claiming that parity.

HTTP(S) proxies are explicitly resolved from host snapshots. WS/WSS uses CONNECT
through HTTP or HTTPS proxies, bounds the CONNECT response head at 16 KiB, and
keeps proxy authentication out of the origin upgrade request. A secure origin
through a secure proxy has two independently verified TLS layers. Incoming frame
and message limits come from `WebSocketRequest`; the message limit also bounds
outbound frame payloads. Ping/Pong additionally enforce 125 bytes; Close with a
status permits at most 123 UTF-8 reason bytes. The pinned protocol library's
allowed-code set is checked, and reason-without-code is rejected. Local invalid
input never writes bytes or retires a useful connection. Received peer Close
flushes its queued protocol reply inside the existing cancellation/idle scope
before physical release. `send(Close)` writes a close frame; `close()` and drop
release the physical connection. Codex session reuse, continuation, sticky
fallback and graceful session-cleanup policy remain adapter/composition work.

The default HTTP trust store uses the platform verifier. Default WebSocket trust
uses WebPKI roots. Explicit private-root/configuration constructors are provided
for host-owned PKI. Neither default disables certificate or hostname verification.

## Reviewed dependencies

Exact direct versions were obtained from crates.io Cargo metadata, then API
signatures checked against the downloaded source for those exact versions:

| Dependency | Version | Purpose |
| --- | --- | --- |
| reqwest | 0.13.4 | HTTP client, streaming response chunk, explicit proxy |
| Tokio | 1.53.1 | Explicit runtime handle, TCP, timer/task cancellation |
| tokio-tungstenite | 0.30.0 | WebSocket handshake and bounded frames |
| tokio-rustls | 0.26.5 | TLS on proxy and origin streams |
| webpki-roots | 1.0.9 | Default WebSocket public roots |
| futures-util | 0.3.34 | Native socket sink/stream extensions |
| base64 | 0.23.1 | CONNECT Basic authentication encoding; SIMD-unsafe feature off |
| rcgen (test only) | 0.14.10 | Ephemeral test certificates; no live/private host keys |
| log (test only) | 0.4.34 | Synthetic privileged-host logging/filtering witness |

Reqwest source `src/async_impl/client.rs:1399-1407` documents default NACK retries;
`src/retry.rs:84-90` supplies `retry::never()`, which the implementation selects.
`client.rs:1430-1434` disables system proxies, and `:1438-1469` documents absent
default timeouts. tokio-tungstenite `src/tls.rs:189-238` verifies that
`client_async_tls_with_config` accepts the supplied stream and explicit Rustls
connector, enabling TLS over a previously established proxy tunnel.

Context7 lookup used `/snapview/tokio-tungstenite` (official repository) for TLS
connector guidance; exact pinned crate sources supplied the final signatures.
Controller separately verified the official Reqwest retry documentation.
Native dependencies are default-off and do not add any internal workspace edge.

## Executed production-backend tests

`crates/pi-ai-transport/tests/native.rs` runs only loopback listeners, synthetic
headers, generated ephemeral test certificates and an explicitly created runtime.
It polls consumer futures outside Tokio to establish the executor boundary.

The 19 integration tests cover incremental HTTP/SSE and EOF release, absolute-form HTTP proxy
requests/authentication, suppressed redirects and retries, an actual 429-to-200
policy retry, cancellation of a stalled body, bidirectional WS frames/close,
HTTP CONNECT proxy frames/authentication, WS idle and handshake timeouts, incoming
size-limit failure/release and outgoing local rejection with retained connection,
TLS rejection plus explicit private roots, WSS through HTTPS proxy with both TLS
layers, opaque response headers, exact control boundaries and invalid-control
rejection, peer-Close replies, Latin1 request bytes and pre-connect rejection
above U+00FF. A native unit test separately stalls the Close-reply writer and
proves cancellation and deadline terminate that flush phase and release I/O.
Protocol constraints follow [RFC 6455](https://www.rfc-editor.org/rfc/rfc6455.html#section-5.5).
The opaque-header regression first failed because the field was dropped and
passed after byte-preserving projection. The untrusted-certificate, overlimit,
cancelled-body and pending-handshake witnesses are causal failure cases, not
happy-path substitutes.

Default test execution uses no public network endpoints, real provider tokens or
mutable host credentials. Dependency hydration is an explicit online setup step;
the canonical verification gate remains offline.

## Dependency logging custody

Controller clarification: owned public Debug/errors/diagnostics/telemetry are
redacted. An embedding host enabling dependency wire tracing has selected
privileged raw-wire instrumentation, equivalent in custody to explicit wire
accessors. It is not sanitized telemetry. This crate installs no global logger,
subscriber, process-wide suppression or logging configuration. Request header
values are marked sensitive in both Reqwest and Tungstenite HTTP HeaderValue
objects. This protects ordinary header Debug and compatible compression paths;
it cannot redact serialized handshake bytes that a dependency explicitly traces.

The embedding host must filter dependency targets before enabling library logging
or forwarding it to observability. S13/S15 default observability must not enable
payload-bearing dependency traces. The conservative demonstrated policy suppresses
all levels for `tungstenite`, `tokio_tungstenite`, `reqwest`, `hyper`, `hyper_util`,
`h2`, `rustls`, `tokio_rustls`, `hyper_rustls`, and `rustls_platform_verifier`,
including their `::` descendants, in both log and tracing sinks. Other host
control/application diagnostics remain available.

Source audit searched log/trace/debug/info/warn/error macro call sites under the
pinned downloaded dependency source trees. Relevant examples (not an exhaustive
zero-leakage proof under arbitrary features/loggers) are:

| Target / level | Exact dependency source | Observed content |
| --- | --- | --- |
| `tungstenite::handshake::client` / TRACE | 0.30.0 `src/handshake/client.rs:197` | Serialized upgrade request, including wire header bytes |
| `tungstenite::protocol` / TRACE, DEBUG | 0.30.0 `src/protocol/mod.rs:475,719,768` | Message/frame payload and close reason |
| `tokio_tungstenite` / DEBUG | 0.30.0 `src/lib.rs:371,406` | Native send/close error display |
| `reqwest::connect` / DEBUG, TRACE | 0.13.4 `src/connect.rs:784,929,1996,2019` | Proxy/target metadata; verbose read/write bytes (verbose mode remains off) |
| `hyper::proto::h1::io` / DEBUG, TRACE | 1.11.1 `src/proto/h1/io.rs:199,210,245` | Header count and read/buffer metadata in inspected sites |
| `h2::codec::framed_write`, `h2::codec::framed_read` / DEBUG | 0.4.19 `src/codec/framed_write.rs:229`, `framed_read.rs:418` | Frame Debug records |
| `rustls::client::hs` / TRACE, DEBUG | 0.23.43 `src/client/hs.rs:481,682,1142` | Handshake structures and server identity (when logging feature is active) |
| `rustls::client::tls13` / TRACE | 0.23.43 `src/client/tls13.rs:1159` | Peer certificate metadata (when logging feature is active) |

`tests/log_custody.rs` runs in a separate test process. It proves construction
does not change the default quiet logger state, deliberately enables a synthetic
unfiltered host logger to demonstrate the raw-wire marker, then applies the
documented target filter and proves the marker disappears while a safe host
control marker remains. Only in-memory synthetic records are used. The witness
does not claim every future dependency version, feature or arbitrary host sink
automatically sanitizes its own logs.
