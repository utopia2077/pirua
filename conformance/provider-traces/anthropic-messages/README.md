# Anthropic Messages conformance

Authority is pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`, read through
Git objects, plus installed `@anthropic-ai/sdk` 0.91.1. The complete pinned
`packages/ai/src/api/anthropic-messages.ts` was inspected; source SHA256 is
`c63b3cbe8afe2504bb5f60f2e7f7b8f773de68486a03cc0cc266c3c76f73f7f6`.
SDK `src/client.ts` SHA256 is
`7836d3870ad4816d01f98c060f6068705381748c28742e75086c0c166634dc59`, matching
the SDK evidence note. Installed-file inspection is not registry-archive proof.

## Executed evidence

Run each oracle with a sanitized child environment:

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/anthropic-messages/oracle.mjs
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/anthropic-messages/request-oracle.mjs
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/anthropic-messages/error-oracle.mjs
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/anthropic-messages/header-oracle.mjs
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/anthropic-messages/parse-error-oracle.mjs
```

These execute pinned source functions and the actual installed SDK with synthetic
Fetch responses, not the Rust implementation. No real provider calls or credential
environment are used. The scripts rely on the inspected upstream installation,
including its `partial-json` dependency; they are not build dependencies.

- `trace.json`: complete request/logical payload and 14 owned event snapshots;
  model fallback and pricing, initial text, thinking and signature deltas,
  redacted thinking, tool input fragments, 1h cache writes and reasoning counts.
- `requests.json`: 26 source/SDK request cases for authentication, exact OAuth
  identity, header-owned auth and null placeholders, Copilot, beta selection,
  caching/TTL/affinity, legacy/adaptive/disabled thinking, display, metadata,
  schema/grammar handling, header overrides and SDK URL/query reconstruction.
  Two actual SDK deprecation warnings are explicitly captured by the child
  oracle; production installs no logger and suppresses no host logging.
- `errors.json`: 17 SDK error-body cases and pinned overflow/retry classifiers,
  both before and after the provider retry-delay-cap prefix. Negative 429 billing
  and 503 quota cases remain non-retryable. The cap prefix removes the anchored
  400/413 empty-body overflow match; it does not preserve original overflow by
  assumption. Raw source SDK error strings are synthetic fixture inputs only.
- `headers.json`: executed pinned `headersToRecord` and Node Headers entries;
  ordinary duplicate joining, semicolon Cookie joining, last Set-Cookie value,
  numeric-key own-record order, empty/Latin1 values and own `__proto__` omission.
  Shared `AdapterResponseHead::from_http` supplies this exact callback projection
  without changing raw response headers used by retry and transport.

The source parser is **not** the SDK SSE iterator: `.asResponse()` feeds the
adapter's private `iterateSseMessages`. It consumes a CR immediately at a read
boundary and flushes a trailing event at EOF. The accepted transport-owned
`SseProfile::Anthropic` captures these quirks; adapter tests do not copy a parser.
The framing oracle and 2,308 stage observations live in
`conformance/ai-transport/anthropic-sse`. Missing JSON type is never inferred from
the SSE name, and unnamed typed JSON is ignored. `message_stop` does not end
consumption: EOF is still required, and later named error events still fail.

Malformed-event classification consumes the accepted carrier's exact raw lines,
including comments, ignored fields and overwritten event lines. Source interpolation
uses `raw.join("\\n")`, a literal backslash-n separator. The adapter classifies this
closed provider parser error before discarding raw context; transport does not classify
and raw lines never enter public error text, diagnostics or telemetry. A successfully
repaired event, ignored event or named error follows its own source path rather than
unconditionally classifying comments. Parsed null also fails the source's `event.type`
access inside that parser catch, so its raw context is retained for classification.

`parse-review.json` retains the independent review's exact source observations for
four raw-comment controls and eight SDK-cap cases. Three original reviewer probes
are reproduced in the adapter test with only the fixture include path changed.
`parse-errors.json` adds fifteen executed full-adapter cases covering ignored/repeated
fields, empty-flush carry, null, repair, EOF/CR boundaries, partial model/content/usage,
and named-error/ignored-event isolation. Test-only expectations decode Usage to the
accepted core type rather than treating JavaScript zero and Rust finite-f64 zero as
different numbers. All original26 tests and four oracle outputs remain unchanged.

The source V8/JSON-helper exception description remains the approved fixed typed
error-detail boundary; arbitrary engine exception prose is not reimplemented. All
actual event/data/raw fields in the provider's own parse-error composition are retained.
Resource rejection before parsing remains a typed local failure; it does not classify
rejected comments. Cancellation during incomplete raw context retains the prior snapshot
and closes the body without leaking or classifying unfinished source lines.

Causal witness: removing only raw context from the malformed-event composition makes
the original reviewer probe fail exactly billing-comment and overflow-comment while
both controls still pass. Restoring it makes those probes and the expanded suite pass.

## Source-derived tests and normalizations

The focused Rust tests additionally derive assertions from the pinned
`anthropic-sse-parsing`, `anthropic-auth-token`, `anthropic-cache-write-1h-cost`,
`anthropic-eager-tool-input-compat`, `anthropic-empty-thinking-signature-compat`,
`anthropic-force-adaptive-thinking`, `anthropic-temperature-compat`,
`anthropic-thinking-disable`, `anthropic-tool-name-normalization`,
`github-copilot-anthropic` and Anthropic sections of `deferred-tools` tests.
The complete test-path inventory was searched at the pinned revision; sibling
provider/catalog/OAuth-login and live E2E responsibilities are not claimed here.

Declared projections:

- Clock is fixed to 7 in fixtures. Private block `index` and `partialJson`
  scratch properties are omitted from core content; caller tool arguments are
  not renamed or normalized as provider count fields.
- Normal runtime user-agent is `pi-ai-rust`; SDK platform/language/package/runtime
  identity headers are omitted. Exact OAuth `claude-cli/2.1.75`, `x-app` and betas
  remain. SDK retry-count zero, timeout seconds and Anthropic protocol headers
  are preserved. The source's exact-key spread/case-variant behavior is tested.
- Query-bearing source endpoints enter through `RequestEndpoint`, never a
  durable model base URL. Query ordering follows SDK `Object.fromEntries` then
  JavaScript numeric-key enumeration and `encodeURIComponent`.
- Middleware's numeric storage variants are compared as JavaScript Number
  values. Final wire JSON, array order, string content and opaque input fragments
  remain source-bound, not regenerated from Rust.
- Missing `message_start.message.model` keeps the current valid model, narrowly
  approved because pinned fixtures omit it while source assigns undefined.
  This is source-invalid-output/partial-fixture normalization, not evidence of a
  reported model. Present null/empty/nonstring values reject. Valid fallback
  model identity is retained and matching allowed fallback pricing is used.
- Error body/status and refusal detail are classified transiently before safe
  rendering. Safe numeric status and flags live in structured diagnostics, so
  a status literal cannot defeat a source retry veto. No raw URL, body, auth,
  stack or unknown error detail is copied to default error text.
- Core safe-count, object-only argument, UTF-8, bounded JSON/SSE/tool-input and
  model/header custody remain authoritative. Raw mathematically integral counts
  accept decimal/exponent spellings; durable core Serde is unchanged.

Source `options.client` becomes a non-Serde trusted construction-time
`AnthropicClient` capability. Each call is one complete SDK-equivalent attempt
with explicit lower retry zero and original cancellation. It bypasses internal
auth/header/URL construction and uses non-OAuth payload shaping. Body reads,
including non-cooperative supplied bodies, are raced against caller cancellation.
No arbitrary JavaScript client object identity or alternative backend factory is
claimed. Provider factories, OAuth login/refresh, images and deferred-response
implementations remain outside this wire-family unit.

Default requests use accepted `send_fetch` with lower retries disabled, a
600,000 ms header-acquisition timeout, and the transport-owned complete-attempt
provider retry helper. Error-body consumption precedes retry; successful SSE
bodies never re-enter retry. Caller abort retains owned content, usage, signatures,
response identity and reported fallback model; timeout remains an error.
Native loopback tests prove real fragmented HTTP/UTF-8, EOF and cancellation
connection release. Defaults remain side-effect-free and the family feature is off.
