# Standard OpenAI Responses wrapper

Authority is pi-mono `853a80d26c90a14c1886f0ebb8ffaae133ca2185`,
`packages/ai/src/api/openai-responses.ts` in full, and its exact shared/helper
imports. `oracle.mjs` reads these Git objects, fingerprints each complete source,
and executes the wrapper with installed OpenAI 6.40.0. Models imports are reduced
to their exact calculateCost and thinking-level helper section; the selected
functions are unchanged. Runtime callbacks use a copied-at-push event recorder;
all conversion, reduction, pricing, SDK request construction and SDK SSE parsing
are real pinned source. Partial JSON uses the installed upstream dependency.
No expected fixture originates in Rust output.

Run in a clean environment:
`env -i PATH=/usr/bin:/bin node conformance/provider-traces/openai-responses/oracle.mjs --check`.
The script rejects provider environment variables, disables global fetch and
injects a local response-returning fetch into every SDK invocation. The 52 cases
retain complete request objects, 122 complete event snapshots and pinned retry/
overflow observations. Synthetic credentials and `example.invalid` endpoints are
fixture data, not host metadata. Clock is fixed at 7; Pi user-agent becomes
`pi-ai-rust`; SDK platform identity headers are omitted. Scratch fields
`index`/`partialJson`/`customInput` are excluded from copied event snapshots.
Text/thinking signature strings remain byte-exact. Finite cost numbers compare
by DTO value because JavaScript has one Number representation.

Request witnesses cover nullable summary/tier, mapped reasoning and off/null,
Copilot defaults, xAI include, cache/affinity precedence and truncation, strict/
grammar/deferred definitions, token truthiness/minimum16, sampling-last,
payload replacement, simple normalization, SDK query reserialization, complete
SDK tool-choice shapes, body-header precedence and explicit timeout header.
SDK defaults preserve a 600000ms fetch-to-head timer; `X-Stainless-Timeout` is
absent unless timeoutMs is supplied and nonzero. Shared retries default to zero;
every retry remains SDK retry-count zero. SDK organization/project/custom-header
environment values come only from explicitly injected `AdapterServices.env`.
Scoped request environment applies to Pi cache-retention lookup.
Two additional `oracle.mjs --header-stage` cases retain SDK constructor timing:
custom-header environment triggers eager default-header validation, while the
ordinary path validates during request construction after payload middleware.
Null deletion still validates header names. Neither path emits a wire request
for invalid names; SDK auth/organization headers remain request-stage fields.
URL-dependent affinity inference uses the transient resolved endpoint, matching
Models.applyAuth's effective request URL while keeping the public model intact.

The wrapper consumes the shared Responses processor and SDK-exact `OpenAI` SSE
profile. It continues to EOF after response.completed, as the pinned SDK does.
DONE-prefix data suppresses later data; it does not prematurely settle or leak
a body. Missing terminal is retryable; malformed framing fails. Callback ordering
is payload, HTTP success, response observer, Start, then body. HTTP failures never
call onResponse. Non-streaming payload replacements preserve SDK array iteration
and the complete `_thenUnwrap`/`addOutputText` failure timing. For non-streaming
responses, bounded body parsing and output-text processing finish inside the
complete attempt, before onResponse and Start; decode failures are non-provider
failures and cannot retry. Streaming bodies remain outside that attempt.
The response observer uses the accepted own-record constructor: lowercase
ordinary duplicate joining, Cookie semicolon joining, last Set-Cookie value,
numeric own-key enumeration and no own `__proto__` entry. Its full values/order
are verified against the separately executed pinned header-helper fixture.

HTTP/provider raw text is classified before replacement with reviewed retry/
overflow messages and safe diagnostics. The fixture comparison explicitly
normalizes error text/diagnostics while separately requiring both pinned
classifier results and every other snapshot field. Setup/invalid-wire/resource
failures are closed safe categories. Catch preserves content, usage, response id,
and raw stop reason; cancellation uses the shared owned snapshot. The nominal
trait converts the upstream synchronous simple-auth throw into the required
in-band setup error. No model URL mutation occurs.

Focused tests also prove cancelled non-cooperative payload/response/send/body
waits, usage-only cancellation snapshots, exact aggregate byte bounds, single
retry ownership, mismatch/spawn errors, and default adapter dispatch. The native
loopback test captures the actual HTTP request and three-byte fragmented chunked
SSE, checks success/abort, and requires peer-observed socket EOF/reset. Its server
sends a chunk terminator for success but keeps the abort body open; timeouts are
not accepted as resource-release evidence.

Framing limits are configured in `AdapterServices.sse_limits`; HTTP error and
non-streaming body collection uses `max_error_body_bytes`; aggregate streaming
input defaults to 16MiB. Shared JSON/signature/tool bounds remain unchanged.
No family is enabled by default. The complete SDK-equivalent attempt runs through
`retry_provider_request`: each attempt calls accepted `send_fetch` with lower
RetryNone and the SDK header-only timeout, then reads/classifies non-success
bodies before returning Provider failure metadata. Payload middleware runs once;
successful SSE bodies never reenter retry. A blocked failed body cannot trigger
another attempt before EOF, and cancellation closes it without a successor.

Non-success response.text() rejection still becomes a status/headers-bearing
provider failure, as SDK client.ts:858 requires. The closed native port maps
Connection/Closed body errors to `terminated` and Timeout to `Request timed out.`
for classification. Caller cancellation and local resource limits remain typed
boundaries. Arbitrary external err.message text cannot be reconstructed from
TransportErrorKind; no raw-string error port or raw diagnostic rendering was added.

`retry-cap-oracle.mjs` retains the original cap witness plus 36 executed SDK/pinned
formatter/classifier cap/plain rows, covering 400/413 empty/falsy/empty-object
bodies, forced 403 retries, billing/quota/overflow combinations, read rejection,
code-only quota and billing beyond normal body truncation. Both classifier paths
are evaluated before discarding the raw SDK message. Cap uses the original SDK
message, not its separately normalized/truncated normal HTTP display. Thus a
code-only quota veto can disappear in source cap rendering, while late billing
can veto a cap even if normal display truncates it away. Final text contains only
the selected semantic category; HTTP status remains structured diagnostic data.

For cap classification only, the approved fixed prefix
`Server requested retry delay. ` is equivalent to the real numeric prefix for
the pinned classifier set. It is not a reconstruction of actual cap display.
The retry-delay phrase already supplies a positive retry match; neither prefix
adds overflow words, and both defeat anchored no-body and anchored non-overflow
prefixes. Six additional executed SDK-error/pinned-helper cases verify those
anchors and billing/quota vetoes, and a Rust unit consumes those expectations.

New native coverage traverses relative307 then cross-origin302, checks POST
preservation and GET/body/header rewrite, and compares the complete original
success trace. Final HTTP429 billing bodies remain non-retryable after real
fragmented body reads. Caller cancellation releases an unfinished final error
body; all intermediate and final sockets require peer-observed EOF/reset.

`decode-stage-oracle.mjs --check` separately retains 68 actual SDK and complete
pinned-wrapper observations without changing the original 52/122 corpus. It
covers primitive roots, missing/null/non-iterable output and nested content,
string iterables, null elements, valid response objects, JSON event arrays,
Array.join text coercion, existing output_text overwrite, own non-callable
toString failures and JSON __proto__ data properties. SDK _request_id descriptors
are recorded as evidence, not exposed in the Rust adapter: no data callback sees
the non-streaming response object, and JSON event arrays have no such property.
Public error text remains redacted; event/hook timing and both classifiers are
asserted separately. A unit compares SDK-produced output_text values directly.
The SDK's JSON content-length zero and status204 paths skip body reads and fail
the primitive guard; cancellation during a non-streaming body read drops the
pending operation, closes once and invokes no response hook or retry successor.

The original 42 decode observations are unchanged; 26 appended operand/control
cases cover HTTP200 JSON primitives and non-JSON decoded text. The SDK's failed
`'object' in rsp` guard echoes the decoded primitive in its TypeError. Classification
uses the private fixed prefix `Cannot use 'in' operator to search for 'object' in `
plus the primitive's JS string value, before discarding that text. This preserves
both classifiers, including billing/quota vetoes and the changed anchored no-body/
throttling behavior; classifying the unprefixed operand would not. It does not scan
object fields or arbitrary raw JSON, emulate general V8 exception prose, expose a
new error-string port, or restore raw operands to public diagnostics. These decode
errors remain Other to request-level retry, despite possibly being retryable to an
outer assistant caller. All cases assert one attempt, one close and exact hook/
event timing. Malformed object/nested-content controls with classifier-looking
unused fields confirm those fields never enter the primitive error projection.
