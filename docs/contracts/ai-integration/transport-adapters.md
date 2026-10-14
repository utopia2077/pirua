# Transport and canonical wire adapters

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Ownership and request path

The existing `ApiAdapter` and nominal resolved request roots remain intact.
Adapters capture trusted process-local transport and middleware at construction.
An additive inherent `stream_with_options(TextStreamRequest, FamilyOptions)`
preserves per-invocation raw family options; trait dispatch uses adapter defaults,
and `stream_simple` translates the neutral simple options. Family-specific DTOs
preserve all upstream fields and omission/null/false distinctions. An untyped
options bag is not a replacement for the mapped family types. Endpoint selection
must end in the existing transient `RequestEndpoint`, never a mutated model URL.
Where upstream's auth path rewrites the request model URL, URL-dependent
compatibility and routing heuristics consume that effective transient endpoint,
not the unchanged public model URL. Derived flags may outlive the calculation;
credential-bearing URLs must not enter model records or diagnostics.

`pi-ai-transport` owns a single-attempt HTTP client port, owned redacted request,
response head and incremental body, request retry/timing execution, proxy
resolution, SSE framing profiles, and WebSocket connection/frame resources.
`pi-ai-providers` owns request conversion, provider event reducers, semantic
terminal/error interpretation, protocol-specific fallback, and Codex continuation.
The first transport implementation owns the concrete port signatures; subsequent
adapters consume its exported Rust types rather than duplicate
them. All dynamic methods use explicit boxed futures compatible with S03.

One adapter invocation builds its family payload, invokes trusted payload
middleware at the upstream-observable stage, converts to final wire bytes,
executes the selected request profile, observes the response, and reduces the
stream through the existing validated producer. For example, Mistral's payload
observation precedes camelCase-to-snake_case wire conversion. Construction-time
middleware is non-Serde and cannot be supplied by a foreign extension; later
protocol hook mediation remains a separate capability boundary.

## Ordered middleware payload custody

`PayloadMiddleware` consumes `&mut WirePayload` and returns
`Option<WirePayload>` in its existing boxed-future/result shape. `WirePayload`
is a provider-owned wrapper over ordered `serde_json::Value`, with `new`,
`as_value`, `as_value_mut` and `into_value`; Clone is permitted, Serde is not, and
Debug is redacted. It is transient trusted JSON, not a durable core DTO or a new
foreign callback capability. Construction and mutation retain ordinary insertion
order; JavaScript numeric-key enumeration/Number projection remain explicit wire
operations. A core DTO supplied explicitly has only its existing deterministic
order; no lost pre-construction history is invented.

Each family retains ordered logical payloads through construction, the single
middleware observation/mutation/replacement, SDK conversion and final wire
serialization. It must not pass through core `JsonValue`/`JsonObject` merely to
cross this transient seam. Supplied Anthropic client payload consumption uses the
same wire carrier. Core DTOs, their strict number/duplicate decoding and S03 proof
remain unchanged; compatibility/model/context fields are not globally rewritten.

This corrects an observed value change: reversing case-distinct headers inside a
Google SDK httpOptions replacement changes joined header values upstream, while
the old BTreeMap callback boundary erased the difference. Correct the shared
custody path, not one family's last header builder. Existing family fixtures,
event/cancellation expectations and effect stages remain intact. Accessor/type
migration in tests is mechanical, never a changed source expectation.

The wrapper does not invent a new pre-callback serialization/size failure stage.
Keep existing family/SDK validation stages and checked wire depth/byte/finite
number bounds at their consuming boundaries; a callback may still replace or
reduce a logical payload where it previously could. Trusted mutation is not a
physical-memory or secure-erasure guarantee. Values and raw errors never gain
durable/debug/telemetry permission through this interface refinement.

Pi's prior final serialization did not impose the shared formatter's fixed
one-MiB/depth128 limits. Preserve that admission domain: the JS projection mechanism
may share an explicit internal limit policy, while the existing public checked
formatter and its six other consumers retain their exact bounds. Do not add a
new Pi rejection merely while fixing order/Number spelling, and do not relax other
families' guards. Pi's final projection still follows the middleware stage; this
is not a new serialized limit setting or foreign unbounded-JSON capability.

## Ordered strict-schema wire projection

The existing JsonSchema/Cow-returning helpers retain their canonical DTO contract.
Add `make_strict_wire_json_schema(&JsonSchema)` and
`get_wire_json_schema_tool_parameters(&Tool, Option<bool>)`, returning
`Result<WirePayload, ConstrainedSamplingError>`, for source wire observation.
They start from the admitted canonical input and retain source mutation order
through items/anyOf/properties/required/additionalProperties, including original
versus appended key slots and JavaScript numeric-key enumeration where relevant.
No lost pre-input object history or executable object identity is invented.
Existing supported subset, error categories and exact depth/byte bounds remain;
nonstrict projection gains no new early validation/serialization cap.

All provider wire tool converters consume this ordered projection before their
payload hooks/SDK serialization; canonical DTO consumers keep the old helpers.
The source strict-schema exports map to these wire counterparts in cumulative
S04 evidence. No Codex-only switch makes other wire consumers keep canonicalized
output: the same source order affects middleware, binding queries and signatures.
The actual source witness starts with canonical properties/required/type and
appends additionalProperties last; an explicitly existing first slot is distinct.
Codex continuation must distinguish those resulting request bytes. Preserve old
value/error tests and add exact source wire-order/continuation controls, including
nested schema mutations and supplied-key positions. This is shared wire custody,
not a broader schema language or a change to core DTO ordering.

## Typed Bedrock payload specialization

`PayloadMiddleware<P = WirePayload>` and `AdapterServices<P = WirePayload>`
make the existing trusted payload seam generic with its unchanged JSON default.
Existing implementations, constructors and other families retain WirePayload and
its exact ordered-Value, non-Serde/redacted behavior. No core DTO or foreign
capability changes. Clone/Debug/service fields retain their established custody;
mechanical explicit default-type annotations are allowed where Rust inference
requires them, with no change in previous callback expectations.

Bedrock uses an explicitly typed `BedrockPayload` specialization containing an
owned ordered finite value graph with distinct binary leaves. JSON null, booleans,
finite numbers, strings, arrays and ordered own-key objects remain distinct from
bytes. Object construction/replacement preserves exact-key uniqueness and source
own-key order; representation details and accessor names are local to this single
owner. It is non-Serde and Debug-redacted. There is no magic JSON `$bytes` marker
or path-based restoration of lost type provenance.

The original command reaches its middleware with actual typed binary leaves.
Unchanged bytes, changed bytes, JSON-array/object replacement, string replacement
and JSON cloning remain distinguishable until the SDK schema consumes them.
For the observed blob fields, typed bytes are base64 encoded, a string is encoded
from its UTF8 bytes (not treated as existing base64), and JSON arrays/objects retain
the SDK's actual serializer behavior. A JSON observation/export may deliberately
lose binary type just as source JSON cloning does; it must never silently recreate
it. Source observation markers are fixture notation only, not executable tags.
The typed specialization is explicit at Bedrock construction, with one middleware
invocation at the original stage and no new pre-hook serialization/admission cap.

## Typed lazy setup and forwarding

The Rust lazy helper admits setup returning a canonical validated `AssistantStream`
whose cancellation is wired to the supplied original request token. This is the
existing S03 stream domain, not arbitrary JavaScript AsyncIterable: result-only
without a terminal event, bare EOF with a forever-pending result, arbitrary throws
and independently wired cancellation are not claimed interchangeable. Do not
synthesize a source-equivalent event for an input outside that domain or relax
S03 terminal/result invariants.

`LazyApiLoader::load(self: Arc<Self>)` returns an owned
`BoxFuture<'static, Result<Arc<dyn ApiAdapter>, ModelsError>>`; the loader is
Send+Sync, trusted and non-Serde. Invoke the bounded load/setup function before
returning the synchronous outer stream, then drive its owned future through the
supplied task spawner. Typed setup errors become safe in-band zero-usage failures;
panic parity is not an alternative error channel. Loader invocation occurs on
every operation. Module/import caching, including cached failures, belongs to the
host loader, never an implicit memoization added by the lazy wrapper.

The outer stream uses a private cancellation owner. While setup is pending, race
the original caller token, drop a losing setup future and explicitly settle the
outer. Once the child is owned, forward its validated events/terminal unchanged,
including usage-only cancellation snapshots; do not eagerly settle an empty outer
or return early merely because that original token is cancelled. The setup/child
handoff, queued prefix and spawn/drop paths have one settlement owner and require
controlled race evidence. No snapshot getter or new core stream capability follows.

`LazyApiCapabilities` explicitly advertises fetch/cancel deferred booleans with
default false. False rejects before loading; true fetch with loaded Unsupported
fails in-band, and true cancel delegates its fallible future. Canonical text family
factories advertise neither capability unless the pinned factory does. They use
the real lazy helper and explicitly supplied modules/services; no registration,
credential discovery, native fallback, retry or default timeout is introduced.

Bedrock's trusted module override remains source-global and is sampled on each
load, including wrappers constructed before replacement. An in-progress fallback
load retains its already selected choice; later calls observe replacement. The
setter does not register a provider, serialize a module or grant a foreign
capability. The ordinary host fallback loader owns compiled module availability
and caching; no JavaScript file importer is invented in Rust.

The current ModelsRegistry outer cancellation is a separate facade-composition
boundary: the measured wrapper returns usage0 while child/correct lazy forwarding
retain17. S04 lazy implementation must not alter immutable S03 registry defaults.
S05 facade integration owns an explicit compatible forwarding correction and must
add source/current-registry cancellation evidence before claiming assembled
snapshot preservation. Direct canonical adapter/lazy proofs do not certify that
existing registry path.

## Cancellation and timing

S03 default cancellation remains unchanged. S04 adds an explicit producer
capability to publish an owned assistant-message cancellation snapshot. When
enabled, cancellation uses that snapshot's partial content and usage, replacing
only terminal/error fields needed for a valid sanitized aborted result. Snapshot
publication, cancellation and final settlement share the same synchronization
owner; a settled result cannot change. No foreign callback or host code runs
under its lock. Before any snapshot, existing empty/zero fallback applies.
Adapters publish snapshots at every relevant upstream content/usage update,
including usage-only updates with no emitted event. pi-messages caught caller
cancellation intentionally uses its existing empty fallback. Snapshot input may
carry a provisional successful stop reason, as Responses progress does upstream;
the cancellation result replaces that reason with aborted.

Caller cancellation must end blocked send, body read, backoff and socket waits,
including non-cooperative injected ports. Request completion does not mean the
remote effect was undone. A body cannot be silently replayed after stream
progress. Transport timeout and caller cancellation remain distinguishable;
which one maps to an aborted assistant follows the family contract.

Retry and timing are explicitly selected profiles, not universal defaults:

- Shared provider retry defaults to zero; status/header eligibility, server
  delays, delay cap and injected jitter follow `utils/provider-retry.ts`.
  Lower-level HTTP/SDK automatic retries are disabled when this owner is active.
- Codex retains its separate delay parsing and broad-catch request behavior;
  fallback/continuation stays in the adapter and cannot replay after progress.
- pi-messages uses no retry or timeout even when those neutral fields are set.
- Mistral uses its combined caller/timeout lifetime and default 60-second timeout.
- OpenAI/Anthropic/Google/Bedrock SDK-owned defaults must be checked against
  pinned SDK source before each respective adapter is accepted. Unverified SDK
  defaults must not silently become generic transport defaults.

Clock, sleep, entropy/jitter and environment inputs have deterministic injection
seams. Production defaults may use real host facilities solely inside effectful
crates. Ports must expose explicit cleanup/drop behavior; Codex's instance-owned
resource tracks cached/busy/idle/expired connections and separate sticky fallback
state. S04 exposes resources, S05 registers them, S15 owns host cleanup calls.

Fetch-backed adapters select an additive `HttpTransport::send_fetch` operation;
`send` and `RawHttpClient` retain their single-hop behavior. All redirect hops
belong to one request attempt and share its acquisition deadline. A retry starts
again from the original request, with a fresh hop count and attempt deadline;
body failures after final headers still cannot replay it. The Fetch layer closes
discarded intermediate bodies and races the original cancellation throughout.
It follows HTTP redirect status/method/body/header rules and the 20-hop limit of
the inspected Node Fetch implementation, with version-labelled local oracles.
Its response headers expose Fetch joining for policy decisions without losing
the raw port's repeated fields. It does not turn every custom API-key header
into an Authorization header or invent cross-origin forwarding rules. Explicit
proxy selection remains an injected host choice; no new ambient lookup or
foreign network capability is introduced. URL, method, forbidden-body and
credential validation at this layer must match the actual finite provider
request domain. Non-HTTP schemes, redirect cycles, malformed locations and
credential-bearing redirect targets fail without exposing their raw values.
This is a derived composition boundary, not a general browser Fetch API.
A construction-time redirect opt-out preserves trusted custom transports such
as the Cloudflare binding, whose source returns `binding.run` responses directly;
native Fetch redirect behavior must not create an unrequested HTTPS fallback.
Network Fetch composition also owns its finite request defaults and incremental
response content decoding; the versioned source profile is in the SDK evidence
note. Raw single-hop clients remain raw. A construction-time distinction must
separate raw-network exchanges from trusted preconstructed responses: the latter
must not gain network default headers or be decoded a second time. This is not
the same decision as whether redirects are followed. Binding-specific request
conversion remains with the binding adapter, within the existing finite request
domain, and never creates an automatic network fallback.
The additive construction options are `FetchResponseSource::{RawNetwork,
Preconstructed}` and `FetchContentLimits`. `RawHttpClient::send_fetch_hop` has
the same arguments/result as `send` and defaults to delegating to it. Only the
raw-network Fetch path selects this single-hop hook. A native override may add
protocol-dependent absent defaults after HTTP version negotiation; raw send and
preconstructed exchanges remain unchanged. Header storage carries headers, not
hidden request-origin metadata; the hop hook owns no retries or redirect loop.

Content decoding preserves response metadata and follows the source's codec,
stack-order, skipped-body and acquisition-versus-body failure boundaries. It
must deliver incremental bounded chunks without collecting a whole response,
honor downstream demand and the original cancellation/timeout lifetime, and
release the underlying body plus codec state on close, failure or drop. Codec
implementation and documented local buffering defaults are transport-owned;
logical buffered/decoded limits are checked before unbounded growth. A long
valid stream is not rejected solely because its cumulative output exceeds a
per-buffer budget. No codec task survives its owning body or adds a retry owner.

SDK request attempts include the SDK's non-success response processing, not just
receipt of HTTP headers. OpenAI/Anthropic-equivalent callers therefore execute
their complete attempt through one generic transport-owned provider-retry helper:
the attempt acquires the response with lower retries disabled, reads/classifies
an unsuccessful response at the family boundary, and returns typed failure
metadata. Transport applies the pinned status/header/backoff rules without
knowing provider payload schemas or error formatting. A delay-cap/policy failure
retains the preceding family failure value, so safe rendering can preserve both
retry and context-overflow observations. Failed-body reads complete or cancel
before a subsequent attempt starts; successful SSE bodies remain outside retry.
The status-only `send` convenience is not sufficient to reproduce a SDK promise
that rejects only after reading its error body. All such operations still race
the original cancellation, including non-cooperative attempts and backoff.
Normal and delay-cap error paths must be classified separately before discarding
raw SDK text. A cap prefix can change both retry eligibility and anchored
overflow/veto patterns. Classification-only normalization may use
`Server requested retry delay. `: for the pinned classifier sets this preserves
the real prefix's keyword and anchor effects, without claiming its numeric
display. Verify anchored no-body errors, anchored throttling/service-unavailable
vetoes, code-only errors and text truncated differently by the normal formatter.
The complete SDK attempt also includes resource-specific post-decode validation
before its promise resolves. For standard/Azure Responses, non-streaming
`addOutputText` failure precedes onResponse and Start; safe exception rendering
does not authorize moving those observable effects earlier.

Google's inspected SDK stream is not a generic SSE consumer: each decoded
transport-read chunk first undergoes its own whole-chunk JSON error probe, before
delimiter extraction and event conversion. A private provider-owned Google SDK
decoder may preserve this coupled order for both Google families. It must retain
the same explicit bounded intake/frame/queue and cancellation/drop obligations;
using this seam does not authorize a second generic unbounded SSE implementation.
The SDK's exact delimiters, leading-data filtering, UTF-8 state and incomplete
EOF behavior require source-chunk-stage evidence, not aggregate body equality.

Google GenAI preserves SDK cloud-flag resolution from explicitly captured ambient
inputs, including ENTERPRISE precedence over VERTEXAI. Because the pinned GenAI
wrapper supplies an explicit nonempty API key, the enabled branch uses SDK
Express routing/conversion with that same key; it does not discover ADC or enable
the separate Vertex family. The source module-global generated-tool-ID counter
remains shared by default. A non-Serde construction-time counter owner may be
injected for deterministic execution, with the existing request clock; supplied
unique IDs consume no counter and exhaustion fails before wrapping the safe range.

SDK string admission follows its actual JavaScript operations. In particular,
OpenAI media selection uses ECMAScript trim, and SDK `readEnv` trims its captured
configuration strings then collapses empty to absent. These operations differ
from Rust Unicode trim, HTTP header OWS normalization, and pi's truthy scoped
`getProviderEnvValue` fallback. Preserve the corresponding header values and
eager/lazy validation stage without globally normalizing all environment inputs.

## Wire and credential boundaries

Provider wire values are not durable DTO encodings. Parse raw JSON into ordered
wire values; project mathematically integral finite safe numbers into core count
types regardless of decimal/exponent spelling. Core durable decoders keep their
accepted strict encoding. Preserve JS object enumeration and number/string
formatting when constructing opaque JSON signatures; use the shared wire JSON
projection rather than treating parsed-JSON equality as byte equality.

SSE framing must support the actual family differences: first versus joined data
lines, line endings, trailing frames, named events and DONE handling. Repair of
malformed Anthropic JSON and partial tool arguments follows pinned behavior;
unrecoverable framing/JSON, missing required terminal and illegal event sequences
produce typed failures. Framing and queued transport data have documented,
configurable resource limits with exact boundary tests, not silent truncation.
The shared decoder uses bounded `push`, demand-driven `next_frame`, and
`end_input`; an adapter stops at its terminal without interpreting trailing
frames. Intake bounds and frame-content bounds are distinct.
Select framing from the actual pinned caller, not its imported SDK. OpenAI
Responses consumes the SDK iterator; Anthropic calls `.asResponse()` and uses
pi's own `iterateSseMessages`, including its push-boundary-sensitive CR behavior
and trailing-event EOF flush. Equal SDK iterator fingerprints do not make those
adapter paths interchangeable.
Anthropic's raw SSE lines, including comments and ignored fields, are required
transient context when its own JSON parser fails: source error composition can
make them affect retry/overflow classifiers. The framing result therefore must
retain bounded, redacted raw-line context for the trusted adapter, which performs
source-equivalent classification before discarding it. The framer does not own
provider classifiers. Raw context must never enter public error text, diagnostics
or telemetry. Private buffering may retain raw lines once and derive event/data
at emission, preserving the existing logical framing/intake limits and cleanup
guarantees. Event/data framing evidence alone cannot prove complete parse-error
classification at the provider boundary.

Anthropic's supplied client is an explicit construction-time, non-Serde trusted
single-attempt capability, corresponding to the source `options.client`. It
skips internal client/auth construction and forces the non-OAuth payload path;
outer retry remains the sole owner and lower retries are explicitly zero. A
missing reported message model retains the valid requested/current model as a
declared normalization of incomplete source fixtures; a present nonempty model
replaces it, while present malformed identifiers fail. Do not claim that an
omitted field actually reported the retained model or alter core durable types.

Raw `ProviderHeaders` retains exact-key lookup and source own-key enumeration as
distinct responsibilities. Its existing BTreeMap constructor and `as_map` lookup
remain available; an additive ordered-entry construction/dispatch projection
and raw-object decoding preserve input order without collapsing case variants.
The shared read accessor is `iter_for_dispatch`, yielding borrowed exact names
and nullable values in that own-key order; it introduces no header mutation.
Exact duplicate keys remain rejected, and nullable values/wire field shapes stay
unchanged. Clone/serialization retain that order, including JavaScript's numeric
own-key enumeration. Equality must not hide an order difference that changes
an override. A BTreeMap supplied explicitly has only its existing deterministic
order; no lost pre-construction history is invented.

The registry's ASCII-insensitive merge and each header-sensitive adapter consume
the ordered projection. Header membership/lookups may keep the map view. Preserve
each family's established spread/set/append and null policy; an ordered carrier
is not authority to replace those policies with a universal merge. No new
dependency, serialized field, foreign capability or durable-header permission is
introduced. Existing S03 behavior rows and assertions remain intact;
source-derived S04 permutation witnesses establish the additional custody path.

Headers are case-insensitive, but nullable override semantics remain family
specific. pi-messages filters null overrides before spreading over defaults;
Mistral explicitly deletes defaults. Proxy selection honors scoped/ambient
precedence and NO_PROXY from the pinned resolver. Ambient values are captured
by an explicit host configuration boundary, not consulted by pure core.
Response middleware observes the pinned `headersToRecord` projection through
`AdapterResponseHead::from_http(status, &raw_headers)`: lowercase own keys,
ordinary/Cookie Fetch joining, last Set-Cookie value and JavaScript own-key
ordering. The plain-object source does not create an own `__proto__` entry;
the Rust projection does not emulate prototype values. Preserve repeated raw
transport headers for retry and wire duties; normalize only at the callback seam.

Native Fetch response fields require an explicit semantic header-data view beside
the unchanged raw repeated fields. Node's native parser removes leading field
OWS but its Fetch HeadersList retains trailing SP/HTAB, unlike public Headers
construction. An additive `HttpHeaders::with_fetch_response_fields` constructor
and `iter_for_fetch` accessor may carry those actual admitted field values;
`get_for_fetch` and callback Set-Cookie projection consume that view. This is
header data, not a request-origin flag, fake header or mutable networking mode.
Existing constructors retain their established constructed-value projection;
set/append/remove keep both views coherent, without re-normalizing existing
observed values. Raw send and `iter_for_wire` remain unchanged. Equality includes
the semantic data when present, rather than hiding observable differences.

The native Fetch hop captures source-observable HTTP/1 fields on the existing
post-TLS connection under the unchanged cumulative response-head input limit.
Only a head accepted by Hyper can publish the capture. Informational fields are
discarded, but their input charge remains. Names/cardinality must agree with the
accepted head; leading OWS, Latin-1, trailing bytes and physical-form handling
require source evidence. HTTP/2 admission is checked separately, not inferred
from HTTP/1. The native Fetch hop omits only independently source-confirmed
invalid-OWS HTTP/2 fields from both of its admitted data views; raw send still
exposes its unchanged Hyper fields. This is protocol admission, not a general
header sanitizer or permission to discard other fields. Copies of Fetch policy
fields retain their semantic values, avoiding a second constructed-value trim.
Storage for raw and semantic data is bounded proportionally to the
head limit with its metadata/encoding overhead declared; no fixed-RSS claim or
new reader/retry owner is introduced. The `HttpResponse` literal and existing
hop signatures need not change. Callback records must retain admitted values
without a second whitespace trim.

Azure's unparsed raw family input may retain its exact optional base-URL string
in a strict, Debug-redacted input DTO, like the existing transient raw options.
Serde on that input is not durable-state permission or foreign authorization.
Resolution produces only a non-Serde `RequestEndpoint`; it never writes the
effective credential-bearing URL back to raw options or the public model.

Requests, effective URLs, raw response bodies, credentials, proxy authentication
and third-party errors remain transient and redacted in Debug, telemetry and
durable fixtures. Preserve safe status, typed failure and retry/overflow control
observations by classifying raw provider failures before rendering. Public
diagnostics may project reviewed semantic categories and safe metadata, never
arbitrary raw bodies/stacks/URLs. Any display normalization relative to upstream
must be named in fixture expectations; a generic error must not erase retry or
overflow distinctions. This derives from the already accepted credential and
diagnostic custody constraints. Fixtures use synthetic credentials and payloads.
Already-redacted local helper/callback errors remain typed nonretry/nonoverflow
failures; arbitrary tool/property names in JavaScript exception prose are not
reconstructed. That closed error boundary does not remove classification or
effect-order obligations for the provider's own HTTP/SSE/SDK processing paths.
Native libraries install no global logger and change no global log levels.
Dependency wire tracing enabled by the embedding host is privileged raw-wire
instrumentation, outside the owned diagnostic projection. Hosts must filter
payload-bearing dependency log/tracing targets; S13/S15 default observability
must preserve that boundary. Exact targets and synthetic evidence are recorded
with native backend conformance, not claimed as unconditional protection from
an arbitrarily configured host logger.

## Smithy request profile and provider response records

Bedrock's default native SDK path explicitly selects HTTP/2 (including cleartext
prior knowledge), while proxy/FORCE_HTTP1 selects HTTP/1. Neither is negotiated
Fetch. Transport adds a single-attempt `RawHttpClient::send_smithy(request,
runtime, profile)` with the existing `HttpRequest` and `HttpResponse` custody.
Its default implementation fails with a typed unsupported-profile error; it must
not silently delegate to generic send. `HttpTransport::send_smithy` wraps that
operation with original caller cancellation and no default timeout or retry.
Generic send and Fetch retain all existing behavior.

`SmithyProtocol::{Http1, Http2}`, `SmithyH2PseudoHeaders { authority: String,
scheme: Option<String> }`, `SmithyBodyAdmission::{Immediate, WaitForContinue}`
and `SmithyH1Connection::{KeepAlive, Close}` make independent source policies
explicit. `SmithyRequestProfile::Http1` carries `body_admission` and `connection`;
`Http2` carries `pseudo_headers` and `body_admission`.
Profiles are non-Serde, redacted and request-owned. Physical `HttpHeaders` remains
strict: colon fields never enter it. Provider-owned ordered SDK header construction
and signing happen before the physical profile; the ordinary ordered field values
then receive Node's H1 replacement versus H2 joining/admission. Method/path are
handler-owned; illegal pseudoheaders are typed failures at the corresponding
source stage. No Accept/Fetch defaults, redirect or content decoding is added.
The finite ConverseStream serializer domain does not forward endpoint query/hash;
this profile does not introduce a general fragment-preserving request-target API.

Body admission is derived from the final exact SDK record's `Expect || expect`
and external-agent condition before physical names are lowercased. H1's default
connection policy is independent: its agent decision uses a nullish selector,
so `{Expect: "", expect: "100-continue"}` can wait while retaining keep-alive.
Proxy/external-agent paths require their actual source policy. Connection controls
only the missing physical Connection default, not explicit field overrides or a
new connection-pool owner. WaitForContinue retains the inspected at-least6000ms
grace and source100/final/error/caller-cancel ordering using injected timing.
The profile cannot reconstruct these independent decisions from lowercase fields.

The new operation returns source-admitted ordinary fields in `HttpHeaders`,
retaining first-own-key order for later grouping. H1 observation/OWS/singletons
and H2 admission/order require actual source and native evidence. Existing header
storage can represent this sequence; raw HeaderMap iteration is not proof that
acquisition preserved it. Local buffering and head/field limits remain explicit;
no origin flag or hidden pseudoheader is encoded in a physical header. H1 forced
protocol cannot negotiate H2; H2 cannot fall back to H1. Current source-configured
handler timing is none, independently of neutral options. Expect/continue/final
response body-admission and cancellation are finite request-profile obligations,
not another retry owner. The body owns the connection/driver until close/drop;
acquisition must not collect a successful stream.

The already locked `h2 =0.4.19` is approved as a native-only direct transport
dependency for this profile. Its explicit informational-response polling is
required for H2 Expect/continue admission, which the current Hyper client path
does not expose. The Smithy-H2 branch may use that existing protocol driver under
the same connection/body ownership; generic and Fetch Hyper paths remain unchanged.
No new package version, internal edge, default activation or raw HPACK side reader
is introduced. Continue/body flow-control and response observation must progress
without waiting on each other indefinitely.

`AdapterResponseHead.headers` becomes a non-Serde, redacted
`ResponseHeaderRecord`, with ordered own entries and values
`ResponseHeaderValue::{Text(String), Number(FiniteF64)}`. Explicit `get`,
`get_str` and `iter` do not stringify numeric observations. This is a private S04
interface refinement, not a new durable/foreign DTO or a physical-header type.
Existing callback assertions may migrate accessor spelling, but their string
values/order/expectations must remain identical. Exact own-key replacement and
JavaScript numeric-key enumeration belong to this record; no executable prototype
semantics are introduced.

`AdapterResponseHead::from_http` retains its accepted Fetch projection and
admission domain, including already constructed trusted headers. New checked
record/Smithy limits must not impose an additional aggregate byte/count rejection
on that existing callback path. Existing network/header guards remain intact.
`from_smithy(status, admitted_headers, protocol)` instead preserves the actual
Node/Smithy record: source singleton handling, ordinary/Cookie joining, Set-Cookie
array joined with a bare comma, and numeric `:status` for H2 only. The status is
actual admitted metadata, not a string smuggled into an HTTP field. Plain-object
projection and source own-key order require retained witnesses. Raw wire fields
remain available to their existing retry/signing owners and are not replaced by
this callback record. Bedrock auth, signing, retry and binary-event interpretation
are separate consumers and remain outside this profile boundary.

## Persistent Node WebSocket profile

Codex uses an explicit `Node26WebSocketConnector` implementing a separate
`PersistentWebSocketConnector`; existing generic WebSocket ports and native
behavior remain unchanged. A continuously driven transport owner retains the
socket, bounded framing/inflater state and partial IO while no request is active.
It exposes `WebSocketState::{Open, Closing, Closed}` and one receiver epoch at a
time. The existing `WebSocketRequest` remains the acquisition input. No upgrade
HTTP head is exposed: actual Node delivers only error/close on failed upgrade.

The new application-level types are `WebSocketMessage::{Text(String),
Binary(Vec<u8>)}` and `WebSocketEvent::{Message(WebSocketMessage), Close { code:
u16, reason: String }}`. Close codes are event observations, including1005, not
permission to emit reserved wire codes. Ping/Pong remain physical control duties
and never become application queue activity or reset a Codex parser idle timer.
Transport errors are typed/redacted; all new raw-bearing values have redacted
Debug and no Serde.

`PersistentWebSocket` supplies `state`, `start_exchange(message, runtime)` returning
an owned `Box<dyn WebSocketExchange>`, `shutdown(code, reason, runtime,
close_grace: Duration)` returning an owned-completion future, and synchronous
`abort`. All async methods use the existing boxed-future convention. Exchange
`next(runtime, idle_timeout: Option<Duration>)` returns
`Result<Option<WebSocketEvent>, TransportError>`. Send admission and receiver
attachment share one transition; overlap is rejected without IO. Dropping a
pending next removes only its waiter; dropping a receiver detaches its epoch and
queued application data. Abandoned uncertain send admission retires the socket.

Idle messages are decoded/validated to maintain compression history but are not
replayed to a later receiver. A fragmented message routes when it completes,
matching Node message dispatch. Admitted messages precede a terminal observation;
caller cancellation still wins before queued delivery. State changes when the
driver observes close/error, independently of receiver polling. The connection
must not retain its first request's token as a lifetime authority. Drop/abort,
runtime loss and failed operations release all driver/IO/codec/queue ownership.

The profile negotiates actual Node26/Undici8.10 permessage-deflate semantics,
including inbound fragments, window8–15 and persistent inflater history even when
the server declares no-context-takeover. Client messages remain masked and
uncompressed. Handshake validation and same-read upgraded tail custody require
source evidence; generic tungstenite message parsing cannot claim this profile.
The exact already locked `zlib-rs =0.6.7`, default-features=false, features=std,
is approved as a native-only direct transport dependency for its safe bounded
8-bit inflater API. No new package version, internal edge or default feature is
authorized. Existing Fetch flate2 behavior remains untouched.

Peer Close moves state to Closing immediately, sends the source code-only reply,
and emits Close at physical closure. Graceful local close and abort are distinct:
shutdown validates the wire code/reason, admits and flushes a reserved Close frame,
then waits under its explicit caller cancellation and close grace. Grace expiry
retires IO with a typed timeout; zero means immediate expiry. No unbounded or
invented Node-default closing timer is implied. A finite construction-time peer
close grace bounds a peer that never physically closes; this is the accepted
local resource bound, not source timing equivalence outside that bound. Codex's
Rust cleanup must own/await graceful completion when it promises1000/debug_close;
enqueue-then-drop cannot certify a flushed frame.

Construction limits separately bound handshake/read-ahead, physical frame,
decoded message, fragments, application bytes/count, pending command bytes/count,
outbound bytes and control reserve. Document codec working storage separately.
Queue exhaustion retires rather than blocking receive indefinitely; stalled writes
must not starve reads or shutdown. Work per poll is bounded, and no IO, codec work,
foreign destructor or waker runs under a shared-state lock. Long valid sessions
are not rejected by cumulative byte count. Provider cache, continuation, timers,
statistics and fallback remain outside this transport unit.

## Codex source signal presence

Codex's inspected abort combiner preserves a timeout through the body only when
the source call omits its caller signal. With caller plus timeout it removes
forwarding listeners at headers; its parser still observes caller cancellation.
The mandatory Rust runtime token cannot encode that source omission implicitly.

A non-Serde adapter construction selector
`CodexSourceSignalPresence::{Present, Absent}` preserves both timing profiles.
Present is the Rust default because invocation supplies an explicit runtime
cancellation token. Absent changes only the source-combiner timeout lifetime;
it never disables Rust caller cancellation or synthesizes a different token.
The family raw DTO and existing core runtime remain unchanged. Both raw/simple
dispatch and explicit family-option calls use the configured selector. S05 must
choose this source-compatibility projection explicitly when adapting omitted
source signals; no absence is inferred from whether a token has cancelled yet.
Both variants require actual source and delayed native-body witnesses; a universal
headers-only or whole-response timeout is not equivalent.

## Codex request compression primitive

Transport supplies `compress_zstd_request(body: &[u8], limits:
ZstdRequestLimits) -> Result<Vec<u8>, TransportError>`. The non-Serde, redacted
construction limits contain explicit `max_input_bytes` and `max_output_bytes`;
there is no implicit unlimited mode. This synchronous, no-I/O primitive uses the
already locked zstd-safe implementation at level 3, admits input before codec
work, and bounds output allocation before compression. Empty input is valid;
insufficient capacity is a typed failure, not truncated compressed output.
Implementation-owned working storage must be documented separately from output.

The primitive does not select headers, retry, environment, cancellation or an
HTTP backend. Codex compresses at its source stage once before the SSE attempt
loop, reuses admitted bytes, and retains the source plain-JSON fallback on encoder
failure. WebSocket request payloads remain uncompressed. Actual pinned helper and
Node built-in compression/cross-decoding fixtures establish wire compatibility;
this utility alone does not establish Codex invocation behavior.

## Codex adapter composition and resource cleanup

Codex composes the accepted Responses converter/reducer, direct Fetch profile,
request zstd utility and explicit persistent Node connector. It does not call
the OpenAI SDK request client. Missing configured WebSocket capability follows
the source pre-output SSE fallback; an existing failed upgrade exposes no HTTP
head or onResponse event. Ordered logical payload observation occurs once before
wire serialization/compression/transport selection. Preserve all seven direct
Codex exports and the separate text lazy factory with typed family options.

The shared Responses reducer adds opt-in `preserve_end_turn: bool` to its
process-local stream controls, default false. Codex enables it after its mapper
normalizes done/completed/incomplete to completion. A boolean response.end_turn
then updates output.endTurn at completion interpretation before finalization and
its cancellation snapshots; absent/nonboolean leaves the prior value unchanged.
Other families keep false and their existing source observations. Mechanical
struct-literal migration may make that default explicit; it changes no source
expectation. This narrow metadata projection avoids a generic output-mutation
callback and keeps the shared output/snapshot owner coherent.

WebSocket first-event publication also follows the mapper's earlier stage: if
the first yielded event is a boolean end_turn completion, Start already contains
that value. A preceding created event and the HTTP path retain their existing
Start timing. Completion aliases and absent/nonboolean controls must preserve
this distinction without moving all Start events or changing old shared callers.

The adapter owns one instance-scoped cache/statistics/sticky-fallback resource,
as already established by S04 timing ownership. Cache keys retain raw session
and account identity; clamped affinity headers are a different projection. Busy
entries use source one-shot behavior. Reuse, idle/age expiry, continuation byte
comparison and replacement cleanup follow the pinned module, with identity-safe
leases and no host callbacks or IO under cache locks. Failed/abandoned leases
cannot become reusable. Stats, sticky fallback, connection retirement and explicit
reset/close have separate source effects. Detached debug observations use the
existing safe metadata/error normalization and grant no durable payload permission.

Explicit graceful close returns an owned async completion, consumes the caller's
runtime and construction-supplied finite close grace, and awaits the transport
shutdown that sends1000/debug_close. Local removal/timer cancellation precedes
that await; close does not reset statistics or sticky fallback. Resource drop
and synchronous emergency cleanup abort physical ownership and do not claim a
flushed graceful frame. The source's synchronous cleanup call is not represented
as a synchronous Rust promise of completed IO. S04 exposes the owned graceful
operation and abort resource; S05 registration/S15 host cleanup must use that
distinction without changing the immutable synchronous core registry in S04.

A detached connection retains its operation charge while held by an explicit
close future, even when that future is never polled. Session removal and reaping
cancelled idle tasks do not discharge that retained ownership. Selected-session
and all-session close use the same per-retained-slot accounting boundary as
internal retirement; grouping slots into one future cannot remove their charges.
Drop, completion and bounded admission failure release their corresponding
ownership, without invoking host code under cache locks.

The configured resource limits bound retained sessions/stats, continuation data
and timer/lease ownership independently of transport's frame/queue caps. A long
valid connection does not fail from cumulative bytes. Limit exhaustion is a typed
local outcome with cleanup, never silent cache corruption or truncated context.
No global singleton, ambient credential discovery or provider registration follows.
Configured token parsing retains the pinned truthy finite-JSON claim domain:
header coercion and cache-key type are separate, and object/array claims receive
fresh per-parse identity. Falsy claims and source coercion failures preserve their
stages. This is supplied-token consumption, not JWT signature validation or an
invitation to execute arbitrary host object methods. Claims remain transient and
redacted, under the same cache/resource ownership.

Transport owns the selected Codex request-attempt/backoff mechanism; the provider
classifies its status/body/hook failures and supplies typed retry observations.
The source broad-catch path, server-delay parsing/cap, callback-before-status
stage and no-replay-after-successful-body boundary must remain distinct from the
shared SDK retry policy. Original cancellation races every noncooperative stage.
Source signal presence selects the already frozen timeout lifetime; it is not
inferred from the mandatory runtime token. Source/native traces must cover cached
and busy/closed reuse, full/delta requests, both one-time reconnect cases, pre/post
output fallback, stats/reset/close, delayed body timing and actual graceful frames.

## Configured Vertex credential consumption

S04 consumes configured Vertex credentials through a trusted, non-Serde provider
that creates one bounded, synchronous, no-I/O credential session per ADC stream
invocation. Its only per-invocation auth configuration is the optional raw
keyFilename; cloud-platform scope is fixed. The session's asynchronous leaf takes
the final initial `RequestEndpoint` and original `RequestRuntime`, returning owned
transient headers. Project/location, request body and existing headers are not
credential-leaf inputs. Session reuse spans outer retries of that invocation,
not unrelated calls; no global credential-session cache is invented.

The Rust names are `GoogleVertexAuthConfig<'a> { key_filename: Option<&'a str> }`,
`GoogleVertexAuthProvider::create_session(config)` and
`GoogleVertexAuthSession::get_request_headers(endpoint, runtime)`. Both trusted
traits are Send+Sync; creation returns `Result<Box<dyn GoogleVertexAuthSession>,
ModelsError>`, and the asynchronous session method returns an existing boxed
future of `Result<HttpHeaders, GoogleVertexAuthError>`. The error variants are
`Other(ModelsError)` and `Provider { status: Option<u16>, failure: ModelsError }`.
All auth-bearing types are non-Serde and Debug-redacted; the fixed scope is
documented at this provider boundary rather than a caller-selected string.

API-key selection precedes ADC project/location resolution and skips the ADC
provider entirely. Missing project/location keeps its source pre-payload stage.
A missing configured credential consumer is a typed configuration failure before
payload, not a claim that the host lacks ADC credentials and not empty-header or
network fallback. S05 supplies concrete discovery/factory policy; S04 opens no
credential file and contacts no metadata or token service implicitly.

The credential leaf runs at each complete SDK attempt's source header stage,
after onPayload, even when a caller Authorization header is already present.
Returned names only fill missing fields. Errors distinguish Other from a provider
Error with present status (`None` or a concrete HTTP status); preserve the latter's
outer retry eligibility without an inner generation retry owner. Arbitrary
external auth exception prose follows the existing typed-helper normalization,
not a new raw-string inlet. Later concrete S05 error semantics require a reviewed
typed projection if they need finer classifier observations.

Original caller cancellation preempts a blocked credential future under the
already accepted Rust cancellation contract, even though the inspected SDK does
not. Drop the loser/session and prevent a subsequent send; this does not undo a
remote refresh. In contrast, an explicit SDK timeout starts before auth but does
not preempt that await in the source: preserve its elapsed deadline for subsequent
Fetch observation after auth returns. Do not silently turn it into a hard auth
deadline. Neutral timeout remains ignored and no default timer is added.

## Configured Bedrock invocation and binary response

The Bedrock family consumes configured values and trusted asynchronous leaves;
it performs no implicit profile/file/process/metadata/token discovery. S05 owns
those concrete configuration providers. This is a private S04 interface decision
under the existing configured-service authority, informed by the pinned Bedrock
module and the installed SDK sources recorded in the SDK evidence note.

One synchronous, bounded, no-I/O construction operation creates an invocation
configuration/session owner. The optional raw profile is its configuration key,
not a credential or a serialized SDK client. Region, credentials and bearer-token
leaves remain separate: resolving one cannot eagerly invoke the others. The
provider itself retains pi's explicit/scoped/ambient option precedence, endpoint
pin predicate, ARN region precedence, static-pair/profile asymmetry, bearer and
exact skip-auth selection. All environment inputs are explicitly captured.
An ambient-only profile does not universally suppress a configured static pair;
a captured SDK bearer key's presence differs from pi's truthy bearer selection.

The finite SDK configuration projection covers max attempts, standard/adaptive
retry mode, ordered auth-scheme preference, FIPS/dualstack endpoint selection,
service/global endpoint overrides and user-agent app ID. Defaults-mode and profile
lookup supply those effective settings through configured values/leaves; they
are not hidden process lookups. Preserve each field's actual lazy consumption
stage and failures. Grouping pure already-resolved values is local freedom;
awaiting an unrelated region/credential/retry leaf earlier is not. A missing
required configured leaf produces a typed configuration error at its consuming
stage, including after onPayload when that is the source stage. It must not
silently choose empty credentials or an external discovery path.

Configuration, credential and token capabilities are Send+Sync, process-local,
non-Serde and Debug-redacted, with existing boxed-future/result conventions.
Credential results own access key, secret key, optional session token and optional
expiration; token results own token plus optional expiration. Existing SecretString
custody applies. One invocation owns identity memoization/coalescing/expiration
state across its retries; independently configured leaf calls supply refresh
results. No cross-invocation credential cache is introduced. Static identity
selection skips the unrelated configured leaf. S04 must reproduce the SDK's
initial identity-before-serialization/build stage and later signing-time cache
consumption rather than treating every error as a retriable HTTP failure.

The ordered logical ConverseStream input crosses the typed Bedrock payload hook
once. The actual SDK serializer then produces the final JSON POST path/body;
unknown/optional/null/numeric fields follow that schema, not direct serialization
of the logical command. Custom option headers are applied at build/low with the
source reserved-name rules; model headers do not gain a new forwarding path.
SigV4 signs the exact final SDK header record, path, query and body before their
Smithy physical projection. Bearer selection uses its source auth-scheme path.
Sign again inside every request attempt, including clock-offset correction;
identity/configuration failures before retry stay outside the attempt owner.
No supplied arbitrary-header auth callback substitutes for this signing proof.

The transport error adds optional closed Smithy retry disposition, populated only
by the explicit Smithy native profile before library error identity is discarded.
It distinguishes source-retriable connection/DNS/timeout/refused-stream outcomes
from source nonretriable parser/certificate/protocol/configuration failures;
finer closed categories are allowed only where source policy consumes them.
Existing error kinds/constructors and generic HTTP/Fetch/WS behavior remain
unchanged. No raw library text enters the annotation. Missing disposition on an
opaque configured-port failure is unknown, not evidence of a retryable network
failure. Native head and body paths retain their actual source classification;
provider HTTP/modeled-event classification remains provider-owned. Actual source
and Rust refused/reset versus malformed-head/certificate controls, plus default
profile preservation, must prove that the retry owner consumes this distinction.

Modeled-event JSON failures also retain their SDK stage. While the first event
is being primed inside send, SDK serde attaches HTTP response/body fallback;
later iterator JSON failures carry the parser diagnostic without that fallback.
The same invalid bytes can therefore have different final assistant retry facts.
Preserve this distinction through redacted failure projection; classifying the
entire raw event body after priming loses the source parser's preview/category
behavior. Complete-source malformed-event neighbors before/after messageStart
must remain separate from malformed HTTP error-body proof.

Transport owns one explicit Smithy retry policy and bounded binary event-stream
framing; providers own Bedrock serialization, modeled errors, event unions and
assistant reduction. No generic Fetch defaults, redirects, decompression or
shared OpenAI retry policy is stacked onto Smithy. Default standard retry has
three total attempts, with configured overrides and source quota/backoff rules;
neutral timeout/maxRetries/maxRetryDelayMs remain ignored. Adaptive mode, when
selected by configured inputs, must use its source policy rather than be relabeled
standard. A non-Serde construction selector captures the SDK module-initialization
`SMITHY_NEW_RETRIES_2026 === "true"` policy switch. Absent/other strings select
legacy; it is not reread from per-request scoped or ambient environment. Both
source retry generations retain their own delays, quotas and class costs.
Original Rust caller cancellation preempts noncooperative configured
leaves, acquisition, backoff and body reads under the existing S04 normalization;
losing futures and invocation resources are released. The SDK's initial event-stream priming is part of the attempt: after raw head
observation it reads the first modeled event before send resolves. A retriable
priming failure may produce another signed request/head callback before Start.
After that first event is admitted, remaining body reduction stays outside retry;
successful stream progress cannot be replayed. Source before/after-first-event
controls must establish the boundary rather than treating HTTP200 alone as send
completion.

The binary framer incrementally validates prelude/total/header lengths, prelude
and message CRC32, and all admitted Smithy header types under explicit intake,
message/header/count/queue limits. Header iteration and payload splitting use
the declared header section; source-decoded value extents may cross that section
within the same bounded owned message. Preserve those source observations without
reading outside the owned message. Source duplicate replacement, own-key handling
and UTF8 replacement remain explicit. Its 1–3-byte trailing length-prefix EOF
behavior is distinct from provider missing-terminal classification; framing
completion alone never certifies a successful assistant. It returns transient redacted header/payload
values to the provider and is independent of modeled Bedrock event names. It does
not collect the complete successful response or impose a cumulative valid-stream
byte cap. The provider interprets event/error/exception records, unknown modeled
events, terminal requirements, tool/reasoning assembly, usage-only snapshots and
request diagnostics at their source stages. Callback metadata uses the accepted
Smithy response record; Start still comes from assistant messageStart.

The already locked ring 0.17.14 may be used as an optional dependency enabled
only by the Bedrock wire feature for its safe SHA256/HMAC operations; no handwritten
cryptographic primitive or new version is required. Transport CRC/framing helpers
remain provider-independent. Exact synthetic fixed-date signatures against the
installed SDK, complete configured-session effect traces, binary split/checksum
controls and native H1/H2 body/retry/cancellation observations are obligations.
Live account access, concrete discovery and host credential refresh policy remain
S05 responsibilities and are not certified by those offline proofs.

## Cloudflare configured gateway binding

The four cloudflare.ts URL constants retain their literal source bytes. The
binding module exposes typed trusted AiGatewayBinding/AiGatewayBindingGateway,
GatewayBindingFetchOptions and universal-request construction counterparts plus
the exact source sentinel and configured binding-fetch constructor. Binding and
response capabilities are non-Serde/Debug-redacted and supplied explicitly; no
Workers runtime, provider registration or implicit network fallback is created.

Transport adds a transient logical `FetchRequest` carrying the existing endpoint,
method and proxy plus explicit header/body input values. Headers distinguish an
ordered ordinary record from already constructed Headers data; body distinguishes
text from owned bytes. These are actual source value forms, not hidden origin
headers or a guessed split of comma-joined values. Logical records preserve exact
own-key order/replacement and UTF8 strings until the chosen consumer validates
them; strict physical HttpHeaders and its existing APIs remain unchanged.

An explicit construction-time custom-Fetch capability receives that request before
network Fetch URL/method/header/body conversion. It returns a preconstructed
response and never implicitly redirects, decodes again or falls back to network.
Existing `send_fetch(HttpRequest, ...)` remains source-compatible by representing
its already selected physical/constructed header and byte inputs; a new logical
send operation admits the source input form directly. Without a configured custom
capability, logical inputs undergo the same network Fetch preparation/defaults,
redirects, timeout/retry/cancellation and single-hop backend behavior as before.
A raw send or Smithy operation cannot silently become custom Fetch.

A rejected custom-Fetch promise retains an explicit closed rejection-stage
annotation beside its original typed failure kind. It is not universally a
connection error: an SDK may wrap that rejection into its own Connection failure,
whereas direct Pi retains the typed local-helper outcome. Source-specific SDK
acquisition mappers consume the annotation only where actual source wrapping is
proved. Constructor/pre-call failures, original cancellation/timeouts and later
body-read failures remain separate; generic native/raw profiles are unchanged.

Pi switches only its Fetch input construction: retain the exact spread header
record and JSON text body through the logical operation; native Fetch performs
the prior validation/trimming/alias joining at its own consuming stage. Do not
change Pi payload effects, source null policy or other family algorithms. Other
canonical SDK/direct-Fetch forms must be proved at their actual input stage;
any additional conversion migration needs the same source evidence and bounded
ownership. The Completions payload builder reserves its messages own-key slot
between model and stream before later message assignment, preserving the pinned
SDK/binding query order without changing conversion, values or callback stages.
Complete source SDK+binding observation provides this narrow correction's basis. Arbitrary JS Request/RequestInit bodies, getters and override unions
remain outside this finite logical input type.

Normalize the configured prefix and request URL before origin/path comparison,
require an in-prefix POST with provider/endpoint path and parsed JSON body,
retain query on the endpoint and omit fragment. JSON null and other valid JSON
values are not a missing-body error; transient query values use ordered wire
JSON, not durable core safe-number admission. Body decoding follows the selected
finite input form, including UTF8 replacement/BOM behavior. The binding feature
may enable existing serde_json/unbounded_depth and disable recursion checking
only on its own parser to preserve previously admitted deep Pi JSON. Other
parsers retain their default checks even under feature unification; no arbitrary
stack/allocator guarantee follows. Filter only the source
content-length, host and cf-aig-authorization fields; other provider credentials
retain source behavior and transient custody. Call binding.gateway only after
validation/body/header conversion, then its single run with original cancellation.
Typed configuration failures expose no raw URL/body/header values.

The returned configured transport uses Preconstructed response semantics and
redirect opt-out, preserving streaming body and metadata without double decoding
or network defaults. Source non-success/redirect responses are returned to the
family, not automatically retried or redirected by this shim. Cancellation/drop
must release a noncooperative binding future/body through the existing owner.
Source/native-Rust proofs cover literal exports, URL/method/body/header selection,
call ordering, rejected requests with zero binding/network effects and successful
canonical-provider composition. A mock binding supplies the source-authorized
host capability; no real Cloudflare account access is needed or claimed.

## Final shared input observations

Complete Codex deferred-tool requests establish that function-tool `defer_loading`
is inserted before `strict` (pinned shared converter lines388–392). Preserve that
source slot order in the shared converter; grammar and nondeferred tools retain
their existing order. This is a source-order correction, with unchanged schema
policy and values, and requires exact deferred-wire evidence alongside old tests.

Fetch response-body absence is distinct from a present stream yielding immediate
EOF. Pinned Codex459, Pi400, Anthropic464 and Mistral312 inspect body presence;
the construction-time fact must remain observable through response wrappers.
Use an additive closed `ResponseBodyPresence` observation on `ResponseBody`,
defaulting to Present for all existing implementations, and an explicit absent
body capability for configured/preconstructed responses. The immutable presence
does not change on EOF/close; ownership still uses the existing body close/drop
contract. Policy wrappers forward the fact, and native Fetch null-body method/
status normalization marks absence where its existing body suppression occurs.
No hidden header, payload sentinel or inference from zero bytes replaces presence.
Keep raw/Smithy behavior and existing present-body callers unchanged. Consumers
observe absence at their actual source stage, preserving Start/retry/error order;
Complete-source SDK controls authorize only the existing after-Start `no_body`
gates in Completions and shared Responses/Azure to consume Absent; their failure
mapping and nonstreaming paths stay unchanged. Complete GenAI/Vertex source
controls establish the same missing condition at the existing after-Start Google
`Response body is empty` gate; only that condition changes, with its current
failure mapping and configured-auth/request behavior retained.
The shared prerequisite includes presence transport proof and affected direct
family evidence; Codex consumes it in its separately reviewed family unit.

## Features, dependencies and proof

Keep exactly 15 production crates and all accepted internal dependency edges.
Concrete HTTP/TLS/runtime/WebSocket dependencies may be added to effectful crates
with exact reviewed versions. Portable core dependency prohibitions still apply
under isolated and workspace feature-unified builds. No built-in wire/provider
family is enabled by default. Owning-crate feature probes establish S04 exports;
S05 owns full provider/facade composition.

Each family requires source-bound request, complete event-snapshot, terminal,
error, cancellation and resource evidence. Expected values originate in pinned
source/tests or executed upstream offline traces, never new Rust output.
Record which evidence class applies. A real loopback HTTP/proxy/WebSocket test
must exercise the production backend; mocks alone cannot prove working I/O.
Bedrock must prove binary framing/signing at the wire boundary as applicable.

Codex source fixture diagnostic error stacks retain their original capture. For
source regeneration comparison only, normalize the known local execution-root
file-URL prefix within captured assistant snapshots' `diagnostics[*].error.stack`
(recorded event partial/message/error and result values, including run phases).
Do not match request, payload, transcript or tool-argument data merely because
it has a diagnostic-shaped property. Only the two known roots are equivalent;
an unrelated raw URL cannot equal an invented internal comparison marker.
Preserve relative filenames,
function names, line/column positions, unrelated URLs and every non-stack field.
This permits the same source check from an isolated tree and main without
rewriting original fixtures or normalizing request/event/classifier observations.
Changed non-location stack content and any other observable must still fail.

Retain S03's 327 behavior rows and all 699 export identities/provenance. Exactly
two original mapped-S04 occurrences are image responsibilities retained in S05:
`./api/openrouter-images:generateImages` and
`./api/openrouter-images.lazy:openrouterImagesApi`. All other 161 occurrences
remain S04 responsibilities, including family types and shared/lazy helpers.
The cumulative final partition is 146 S02 DTO, 284 implemented, 22 existing
Rust-nonapplicable, 241 S05, and six external projections. Historical S03 evidence
remains immutable; no missing implementation is relabeled nonapplicable.

Private algorithms, networking library choice, module internals and test layout
remain implementer choices. Cross-family port changes, core additions and
evidence-contract changes require explicit review.
