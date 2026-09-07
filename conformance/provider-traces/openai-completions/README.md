# OpenAI Completions evidence

Authority: pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`,
`packages/ai/src/api/openai-completions.ts` (all 1707 lines), its imported
private/pure helpers, and installed OpenAI SDK 6.40.0. No current working-tree
provider implementation supplies expected values.

`oracle.mjs` loads pinned Git objects, strips TypeScript, executes the original
wrapper with the installed SDK and an injected fetch, and captures payloads,
requests, and complete push-time event snapshots. `trace.json` has 101 cases:
100 compare requests and full snapshots; the source's invalid final missing-id
case is checked separately against the strict Rust identity boundary.
The helpers' full source hashes are retained in the fixture. The pinned
Completions-specific test files were inspected, including cache-control,
empty-tools, prompt-cache, raw-stop-reason, reasoning-details, response-model,
retry, thinking-as-text, thinking-token-budget, tool-choice and tool-result-images.
Built-in catalog/factory setup in those tests remains S05.

`retry-cap-oracle.mjs` exercises the installed SDK with pinned retry/error/
classifier helpers. Its 36 normal/cap cases include no-body 400/413, billing and
quota vetoes, code-only quota, late billing, overflow overlap, and failed body
reads. Six additional vectors establish the approved classifier-only cap prefix
`Server requested retry delay. ` against the pinned numeric display prefix.
The original SDK message is classified independently for cap failures; ordinary
Completions errors use `formatProviderError` without a provider prefix and append
`metadata.raw` according to JavaScript string coercion.

Reproduce both immutable expectations from this repository root:

```sh
env -i PATH=/usr/bin:/bin:/usr/local/bin node --disable-warning=ExperimentalWarning conformance/provider-traces/openai-completions/oracle.mjs --check
env -i PATH=/usr/bin:/bin:/usr/local/bin node --disable-warning=ExperimentalWarning conformance/provider-traces/openai-completions/retry-cap-oracle.mjs --check
```

No public endpoint is contacted; global fetch is disabled in both oracles.
All credentials and request content are synthetic. Oracle execution is optional;
ordinary verification consumes the frozen files offline.

## Declared normalization and boundaries

- Timestamp is injected as 7. Pi user-agent is `pi-ai-rust`; SDK platform identity
  headers are omitted. Protocol headers and nullable default suppression remain.
- Core costs use finite f64 and counts use mathematical safe integer admission,
  so complete snapshots compare through the core DTO. Opaque signatures retain
  exact JSON.stringify bytes and object enumeration order.
- Raw provider/error text is transiently classified and replaced with reviewed
  semantic text. Structured diagnostics retain status and independent retry/
  overflow observations. Tests normalize error prose only after checking both
  observations; successful and partial message fields remain compared.
- Middleware and transport error ports are trusted, non-Serde, category-only
  capabilities. Arbitrary external JavaScript exception messages cannot be
  reconstructed. Non-2xx body Connection/Closed failures project to `terminated`,
  Timeout to `Request timed out.` before SDK status/header retry classification.
  Caller cancellation and configured resource limits retain their separate path.
- Accepted pure constrained-sampling/transform helper failures retain their
  typed/redacted conversion boundary and become nonretry/nonoverflow errors.
  Arbitrary tool/property names that accidentally match JavaScript error-string
  classifiers are explicitly outside equivalence, as are arbitrary callback
  thrown messages. This exception does not apply to actual provider HTTP/SSE
  failures, whose normal and delay-cap observations are source-classified.
- Source lines 485–557 and 627–643 may create a tool with empty id/name before later deltas
  fill them. Core identities stay nonempty. The reducer reserves the original
  content index and queues progress until all referenced identities exist, then
  fills only missing id/name in historical snapshots. Arguments, event order,
  usage and stop reasons stay at their original snapshot values. This changes
  publication timing and early identity fields; it is not literal moment-by-
  moment JavaScript equivalence. `split-identities` includes interleaved text
  and a second complete tool. EOF with incomplete identity fails; cancellation
  and error retain only the representable prefix before that tool, never a
  fabricated identity. No invalid or unresolved tool is silently successful.
- Default intake limits: 16 MiB aggregate stream (`max_stream_bytes`), shared
  configurable SSE limits of 1 MiB buffered bytes/256 KiB frame/1024 frames per
  feed, 256 KiB error body (`AdapterServices`), 256 KiB tool arguments/custom
  input, and 1 MiB serialized event snapshot/queued snapshots. Wire JSON retains
  its shared depth-128/1-MiB bounds. Limits produce failure, never truncation.
- Deferred tool loading uses only Kimi's source system-message tools extension.
  Deferred response fetch/cancel capabilities remain unsupported.

Rust tests also exercise blocked send/body/hooks, usage-only cancellation,
failed-body EOF before retry, one retry owner, explicit/default timeout,
malformed JSON/shapes/counts, split identity EOF/cancel, argument/queue/intake
bounds, and core-safe redaction. Three native tests use real loopback sockets:
fragmented SSE and cancellation release; 307/302 redirects with discarded-body
release and final error veto; truncated non-2xx bodies with status/header retry
eligibility. The native backend's transport-owned TLS/proxy configuration and
host-owned dependency tracing boundaries remain unchanged.

Full acceptance is `./scripts/verify.sh`; family-only iterations use
`cargo test -p pi-ai-providers --features wire-openai-completions,native --test openai_completions`.

## Independent-review corrections

`correction-oracle.mjs` / `corrections.json` retain the independent review's
source-derived cancellation/zero-body/replay witnesses plus parser controls.
The original 101-case and 36+6-case fixtures remain unchanged. Run the correction
oracle with the same sanitized command and `--check` to reproduce all 15 rows.
Cancellation snapshots now update independently of queued identity publication:
usage-only and model/response-id-only chunks preserve every representable global
field while unresolved content still projects to its representable prefix.
OpenAI6.40.0 `internal/parse.ts` selects streaming first, skips non-streaming 204,
and skips JSON with Fetch-projected Content-Length exactly `0` before reading.
Controls cover whitespace, duplicate zero headers, `00`, absent length, text
content type and streaming. Noncooperative source-skipped bodies are never polled;
Completions still calls onResponse, publishes Start, and reports the subsequent
undefined/null iteration error through safe terminal text.
