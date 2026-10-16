# Target evidence

Authority is [`packages/mcp` at Pi v1.4.0](https://github.com/earendil-works/pi/tree/4a080252f06caf63a525d7873c5d94d3c23887e0/packages/mcp).
The implementation was written in Rust against those source files and tests,
without embedding or executing the upstream implementation in production.
Expected wires in Rust tests are literal JSON/HTTP fixtures independent of the
client's protocol construction.

| Pinned upstream authority | Rust ownership and executable evidence |
| --- | --- |
| `src/client.ts`, `src/protocol/jsonrpc.ts`, `test/client.test.ts` | `src/client.rs`, `src/protocol.rs`; `tests/client.rs` covers initialization, accepted old protocol, paginated tool/resource results, structured arrays/null/booleans, JSON-RPC errors, correlation, progress renewal, timeout/abort/drop, late replies, server ping/roots/unknown methods, diagnostic errors and terminal closure. |
| `src/protocol/types.ts`, `src/protocol/content.ts` | `src/protocol.rs`; detached MCP DTOs preserve content fields and annotations. Runtime content conversion belongs to Coding integration. |
| `src/transports/transport.ts`, `src/transports/in-memory.ts`, `src/testing/index.ts` | `Transport`, `TransportEvent`, `memory_transport_pair`; client tests use the in-memory peer. Rust uses asynchronous channels in place of JS callbacks and structured clones. |
| `src/transports/stdio.ts`, `test/stdio.test.ts`, `test/fixtures/stdio-server.mjs`, `test/fixtures/stubborn-server.mjs` | `src/transport/stdio.rs`; `tests/stdio.rs` launches the Rust-owned Python peer, splits UTF-8/CRLF frames, observes stderr/progress/tools, verifies normal exit and forced process-group shutdown. No Node process is part of production. |
| `src/transports/streamable-http.ts`, `test/streamable-http.test.ts` | `src/transport/http.rs`, `sse.rs`; `tests/http.rs` uses real loopback HTTP for JSON/SSE responses, session/protocol headers and DELETE, ID-only priming/resumption, GET reconnection and server request replies, per-request response failure, expired sessions, accepted-without-response errors, size limits and rejected batches. Incremental SSE tests split every byte. |
| `src/auth-provider.ts`, `src/oauth/{flow,provider,discovery,types,errors}.ts`, `test/oauth.test.ts` | `src/oauth/*`; `tests/oauth.rs` observes discovery, registration, PKCE, code exchange, scope retention, a single rotating-token refresh for concurrent 401s, 403 scope step-up, cancellation during refresh, exact server binding, issuer/resource boundaries, configured metadata and aborted flows. |
| `src/oauth/callback.ts`, callback tests in `test/oauth.test.ts` | Host-owned browser and callback HTTP utility. `FlowOptions` supplies the callback code and issuer after host verification of stored state. This crate does not implement or claim an OAuth callback HTTP server. |
| `toLlmContent`, `test/content.test.ts` | Host/runtime adapter ownership. Raw `CallToolResult`, `ContentBlock` fields and structured JSON are available without introducing AI or Agent dependencies. |

Additional regressions cover JSON numeric IDs written as `2` versus `2.0`, final
stdio frames arriving immediately before process exit, and rejecting an OAuth
provider attached to a different server before token dispatch.

Independent review findings have dedicated production-boundary regressions:

| Finding and target behavior | Regression evidence |
| --- | --- |
| MCP-R1: `client.ts::close()` stops new server request delivery. | `tests/client.rs::queued_server_request_must_not_start_after_client_close` queues a request before synchronous close on a current-thread runtime and verifies that its async host effect never runs. Admission and shutdown share the incoming-request lock; task startup checks terminal ownership before constructing/polling a handler. |
| MCP-R2: `oauth/flow.ts::startAuthorization` sets generated query parameters. | `tests/oauth.rs::authorization_parameters_replace_stale_endpoint_values_and_preserve_unrelated_query` discovers a loopback endpoint containing stale/duplicate OAuth parameters. State, PKCE, redirect URI and supplied optional fields each have exactly one generated value; unrelated pairs and optional fields not supplied by the flow survive. |
| MCP-R3: `consumeSseStream` uses a streaming TextDecoder that strips one initial BOM. | `tests/http.rs::sse_initial_bom_survives_split_bytes_without_stripping_later_boms` checks splits across every BOM byte, ID-only priming, later BOM preservation and byte limits. `bom_prefixed_http_initialization_and_response_deliver_their_results` verifies both initialization and a later reply through real loopback HTTP. |

Rust API differences are deliberate: an initialized client is returned by the
async `connect` constructor, root handlers can be replaced with dynamic host
handlers, and channel subscriptions replace synchronous listeners. A dropped
request future cancels the call. Response-stream bodies stop once the matching
response arrives, even if the server keeps the body open. Malformed or oversized
messages are rejected at framing/DTO boundaries, and HTTP JSON batches remain
outside the advertised surface. Windows `taskkill` support is implemented but
has not been executed in the Linux test environment.
