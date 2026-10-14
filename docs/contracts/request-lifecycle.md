# Request lifecycle

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Portable contract

Add one optional borrowed capability to ApiAdapter, following the existing
optional-capability pattern used by provider refresh. The ordinary five methods
retain their signatures and behavior; the new method defaults to None:

```rust
fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> { None }
```

The object-safe capability has raw/simple entry points taking the existing owned
request and one owned non-Serde RequestLifecycleInvocation, returning the existing
AssistantStream. No new fetch/cancel/image capability is included in this unit.

```rust
pub trait RequestLifecycleAdapter: Send + Sync {
    fn stream_with_lifecycle(&self, request: TextStreamRequest,
        lifecycle: RequestLifecycleInvocation) -> AssistantStream;
    fn stream_simple_with_lifecycle(&self, request: SimpleStreamRequest,
        lifecycle: RequestLifecycleInvocation) -> AssistantStream;
}
```

Add ModelsRegistry::stream_with_lifecycle and stream_simple_with_lifecycle with
the existing model/context/overrides/options arguments plus RequestLifecycleOptions.
Return ObservedAssistantStream containing the ordinary AssistantStream and a
ResponseMetadataReader. Each registry call allocates an independent invocation
and metadata cell. The same caller-supplied payload handler may be reused across
requests without sharing collected response state. Reader retention must not
retain the payload handler or adapter resources after their normal request lifetime.

The registry uses its existing assembly/spawn/pump machinery. With the new route,
an absent capability produces the existing safe Adapter setup-error terminal
before invoking an adapter stream or transport; auth assembly retains its current
ordering. Do not silently fall back to an ordinary stream that skips the requested
payload interception. Existing registry methods do not query or activate this
capability. RequestRuntime, its constructor, existing options/request roots and
ModelsRequestOverrides retain their current fields and signatures.

## Payload transformation and response observation are different operations

RequestLifecycleOptions holds an optional Arc to this trusted, process-local port:

```rust
pub trait RequestPayloadTransform: Send + Sync {
    fn transform<'a>(&'a self, model: &'a Model,
        payload: &'a mut serde_json::Value)
        -> BoxFuture<'a, Result<Option<serde_json::Value>, ModelsError>>;
}
```

Use raw serde_json::Value at this boundary, preserving the existing WirePayload
consuming/ordering rules. Converting to the stricter durable JsonValue before
interception would change the opportunity to replace a payload before consumption.
None keeps the possibly mutated payload; Some replaces it. No change to signal,
auth, endpoint, headers or stream options is exposed through this port. Existing
captured payload middleware runs first, then the request-specific transform, then
the family's existing consuming/normalization path. Callback failure and cancellation
use the same in-band/race behavior as existing payload middleware, with fixed/redacted
error context. The later Harness binding owns its close/fault propagation separately.

RequestLifecycleInvocation also has a passive response recorder. It captures an
owned RequestResponseHead with status:u16 and ordered header pairs. Header values
have exactly Text(String) and Number(FiniteF64) forms, matching existing provider
response-record observations without prematurely stringifying SDK numbers. These
new observation values, invocation and reader are non-Serde and Debug-redacted.
The recorder executes no host callback or async work. It stores/replaces one complete
head atomically; snapshot returns a detached optional value.

Response recording is a distinct optional service path; its presence must not
set the captured ResponseMiddleware slot to Some and thereby enable that slot's
previously skipped, fallible header projection. At the existing family observation
point, a small shared/invocation helper applies these cases:

| Captured response middleware | Request recorder | Projection/callback result |
|---|---|---|
| absent | absent | Preserve the existing skipped projection/callback path. |
| present | absent or present | Preserve the original projection and callback, including their error/retry behavior; only after success may the recorder store that same head. |
| absent | present | Attempt projection for observation only; success records it, failure records nothing and leaves body reduction, retry classification and the assistant result unchanged. |

On observation-only projection failure, metadata remains None if none was recorded,
or retains the last complete successful observation. No partial head, provider error,
retry, hook error or new effect is manufactured. The implementation may add a
request-local optional service field/helper with passive None construction and clone
propagation; changing existing transport/header admission or captured middleware
semantics is outside scope. A wrapper entered after from_http(...)? is insufficient.

The recorded value is the latest successful response observation for this logical
request, not a claim that a later transport failure had a response. Do not invent a
websocket handshake status or activate Google's currently absent response callback.
Existing family observation placement/retry timing is retained; this is not a new
all-HTTP-attempt transport observation API.

The metadata reader is passive process-local observation. It is not a durable state
input, a new Effects boundary or the after_response hook. The Harness later awaits
the validated terminal assistant, reads available metadata, applies its existing
sanitized-header/string projection and runs one eligible after_response pipeline
before message_end and atomic settlement. The inherited serialized start rule applies:
abort-first skips a not-yet-started hook, hook-first may finish, and synthetic
settlement invokes no response hook. A mock privately invoking callbacks cannot
substitute for the actual registry/adapter bridge in this unit's proof.

## Selected adapter binding

OpenAI Responses, OpenAI Completions (DeepSeek/OpenRouter text), Google GenAI,
Anthropic Messages and OpenAI Codex Responses expose the capability. Each binds
one invocation to a request-local clone of its service configuration while retaining
shared transport/resource owners and the original captured middleware. Payload
composition may use the existing slot; passive response recording needs its separate
optional field/helper so it cannot newly enable captured-middleware failure rules. Existing
ordinary stream paths stay the implementation used underneath this scoped binding;
no duplicate provider protocol or reducer is introduced. Mutable global callback
replacement, thread-local request maps and model/header keys used as hidden routing
channels are forbidden.

All selected logical payload boundaries remain where report589 located them:
OpenAI Completions wire build; Responses before SDK execute; Google before SDK
prepare; Anthropic before force_stream; Codex before transport selection and WS
framing. Standard Responses is HTTP-only at this baseline. Codex resources/leases,
retry/fallback policy, parent/child cancellation and active-resource cleanup retain
their existing behavior. Request-scoped binding must not outlive or redirect a
subsequent exchange on the same persistent websocket resource.

Only adapters implementing the optional capability participate in the new route.
Unselected adapters, image/auth factories, normal requests and existing source
counts retain their previous scope. No pi-ai-core -> providers/transport/schema
edge is allowed. New portable contracts live in pi-ai-core; scoped service binding
lives in pi-ai-providers; Harness consumer binding comes later in pi-harness-runtime.
No dependency/version/feature change is currently proposed.

## Configured selected factories preserve optional lifecycle forwarding

This is the revision3 correction. Actual bridge596 focused2 executes four failing
factory-path tests: registry sees no capability because providers/text.rs::compose
wraps every selected configured adapter in LazyApi. Ordinary streams succeed.
The root read the failure log and both complete source wrappers; the existing
LazyApi implements only ordinary/deferred methods and defaults the new borrowed
capability to None. Direct create_provider composition does not prove the selected
factory path. The original failing witness remains part of this unit's evidence.

The private selected text compose layer may install a configured wrapper retaining
both its existing LazyApi ordinary path and the same explicitly supplied concrete
Arc<dyn ApiAdapter>. Its five ordinary methods delegate through the exact existing
lazy path, and construction/ordinary calls do not query lifecycle capability or
load modules. Only the registry's opt-in query asks the configured concrete adapter
for capability; absence remains None and reaches the existing setup failure before
adapter stream or transport work. No name/header/model key supplies routing.

The wrapper's lifecycle methods forward one owned invocation through existing
lazy_stream setup/pump semantics to that same concrete adapter. Preserve original
token cancellation, setup/spawn rejection, ownership transfer on completing poll,
one validated child terminal and consumer-drop behavior. Recheck capability at
actual child setup; a capability that is no longer available produces safe setup
failure, never fallback to an ordinary stream. One logical invocation and metadata
cell survive this wrapper; no extra handler invocation or response cell is allocated.
Concrete service/resource custody and selected provider/module ordering stay fixed.

The configured forwarding helper may remain
private in that module; the LazyApi/LazyApiCapabilities types and public lazy family
factory signatures/defaults remain unchanged. No auth/catalog/model mutation,
provider entry addition or images/unselected-factory capability is granted. Existing
selected factory ordinary behavior and all old regressions remain protected.
Owning proof must pass the preserved actual selected-factory raw/simple paths and
separately configured custom adapters without lifecycle capability; prove ordinary
calls never query that capability, and observed lazy setup preserves cancellation,
spawner rejection, raw/simple once-only forwarding and handler/reader lifetime.

## Proof obligations

- An actual ModelsRegistry with a real configured provider reaches each selected
  adapter's existing payload construction, invokes the request transform before
  transport, and emits an unchanged validated stream/result except for the
  intentional payload effect. Fake Fetch/WS/client boundaries are permitted; a
  faux callback-only registry or replacement provider implementation is not proof.
- Mutate-and-None versus Some replacement, callback error, cancellation before/
  during callback, simple-to-raw forwarding, and no transformation on ordinary
  streams have distinct observations. Global middleware retains its own timing
  and runs before scoped middleware with the prior transformed payload.
- Two concurrent requests reusing one handler have different payload bindings and
  independent metadata readers. Headers/status are detached, redacted in Debug,
  and never copied into model/auth/options or durable state. Reader lifetime does
  not retain a handler/adapter/resource owner.
- Actual configured AnthropicClient with valid body and an admitted x-observed
  header containing U+0100: ordinary no-middleware and recorder-only requests both
  succeed identically, with no new metadata on projection failure. A separately
  configured preexisting response middleware retains its original projection error
  behavior in both ordinary and observed routes. This source-derived witness
  must be covered by an executed regression.
- Existing HTTP response middleware observations are faithfully captured, including
  Codex retry timing. Body-reduction failure after a recorded head, received-error
  then no-response retry, and captured middleware failure preserve the latest
  complete observation without changing the original result/retry policy. Google and websocket-only metadata remain absent. Codex WS
  callback precedes delta/envelope creation and is not repeated on internal retries;
  HTTP fallback/cleanup/cancellation still follow the original resource rules.
- A provider without the optional adapter capability rejects the observed route
  through the existing Adapter setup terminal and performs no adapter/transport
  work. Ordinary routes remain available and unchanged.
- Existing selected/nonselected provider, source/export, header/body/cancellation,
  request-runtime and resource conformance remain passing without weakening old
  assertions or inventories. New counts identify bridge behaviors, not R2 runtime.

## Scope and deferrals

The intended code scope is the portable adapter/registry boundary plus one new
lifecycle module and exports; selected adapter capability bindings and a small shared
service-composition/invocation helper and any required passive optional service
field/clone/default wiring; new owning tests. An implementer must identify any
additional constructor/test adaptation before changing it. RequestRuntime fields,
wire DTOs, transport implementations and existing production dependency edges are
outside the lifecycle bridge.
