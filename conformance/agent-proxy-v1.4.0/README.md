# Agent proxy v1.4.0 source observations

This subtree executes unchanged `packages/agent/src/proxy.ts` from release
`4a080252f06caf63a525d7873c5d94d3c23887e0` against an injected Fetch implementation.
It records eight deterministic request/event/result cases. No provider or proxy
server is contacted. The public Rust test
`crates/pi-agent-loop/tests/proxy.rs::target_source_proxy_vectors_match_wire_events_and_full_result`
replays these inputs through the actual optional Rust proxy client.

From repository root, first install the exact release dependencies used by the
existing Agent oracle, then verify these observations:

```sh
export PIRUA_UPSTREAM_ROOT=/path/to/pi-mono
node conformance/agent-v1.4.0/setup.mjs --install
node conformance/agent-proxy-v1.4.0/oracle.mjs --check
```

The existing Agent setup verifies the AI/telemetry release tarball hashes and
installs locked dependencies with lifecycle scripts disabled. This oracle checks
the pinned source and source-test hashes, installed AI version, and executed AI
event-stream/JSON parser helper hashes. An alternate existing dependency directory
can be selected with `PIRUA_AGENT_ORACLE_DEPENDENCIES`; it must contain the same
exact published AI 1.4.0 helpers. The generated source and dependency symlink live
only under ignored `.runtime/`. A changed dependency directory requires that
ignored symlink to match; the script rejects a mismatch.

To intentionally regenerate, use `oracle.mjs --write`, inspect the result, and
rerun `--check`. Tested with Node 26.8.1. The target source's warning for the
intentionally unknown event appears on stderr; its event is ignored as expected.

`observations.json` contains the exact raw response bytes as UTF-8 `input.wire`,
byte-fragmentation choice, source options, captured POST request, invariant event
payload sequence, and full terminal message. Cases cover all content types,
tool metadata at end, body option filtering, a BOM and ignored/non-data lines,
final unterminated terminal, EOF with partial content, reported aborted terminal,
wrong-content error, JSON HTTP error detail, and HTTP status fallback.

Clock noise is limited to `durationMs`, replaced with `<durationMs>` while its
presence is retained. `Date.now()` is fixed to 7. Rust normalizes integral numeric
representations such as `0.0` to the same JSON number as `0`. Request header names
are lowercased because HTTP field names are case-insensitive. No request body
field, auth value, event order/payload, final content/signature/usage, provider
thinking value, or deterministic error message is removed.

The source's partial event objects alias one mutable message. Native Rust events
own snapshots, so the source oracle omits only each event's `partial`, `message`,
and `error` message envelope, then compares the full terminal message separately.
Top-level delta/content/tool-call fields are preserved. Native tests additionally
assert progressive partial arguments and the end metadata. The source's internal
`partialJson` parser buffer has no field in Rust's typed ToolCall DTO; it remains
private state in the Rust parser.

The native client is enabled by `pi-agent-loop/proxy`. It takes explicit
`ProxyOptions`, `RawHttpClient`, and `RequestRuntime`; `proxy_stream_fn` adapts it
to Agent while discarding the provider API key. The body allowlist is exactly
temperature, samplingParams, maxTokens, reasoning, cacheRetention, sessionId,
headers, metadata, transport, thinkingBudgets, maxRetryDelayMs. Proxy auth is only
the HTTP Bearer header. Explicit provider headers remain an allowlisted body
option; implicit provider credentials and identity are not added.

The native domain uses HTTP(S) base URLs without userinfo/query/fragment,
normalizes the joining slash, requires contiguous content indexes and typed
object tool arguments, and uses the transport's existing 1 MiB input-buffer and
256 KiB line/tool-JSON bounds. Malformed JSON/fields use stable native errors.
Core cancellation owns a single aborted terminal with its fixed cancellation
message and retains the most recent published partial snapshot. A reported error
without a nonempty message receives `Proxy server returned an error` to satisfy
the core terminal invariant. These invalid/native cases are tested separately
and are not claimed as arbitrary JavaScript-runtime equivalence.

The first terminal is immutable. The native client closes its response at that
terminal instead of draining an irrelevant trailing body, and also closes on
EOF, failure, cancellation or task drop. Dedicated injected transport tests
cover pending-read cancellation, cancellation during acquisition and the exact
acquisition race, body failure, malformed input, parser bounds and spawn failure.
The source oracle checks the proxy protocol; independent review and final Agent
integration remain controller responsibilities.
