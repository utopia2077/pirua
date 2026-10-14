# Stream and request runtime

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## State owner

The pinned root generic stream export remains available as a consumer-only Rust
type:

```rust
pub struct EventStream<E, R = E> { /* private Arc state + unique consumer */ }
pub type AssistantStream = EventStream<AssistantMessageEvent, AssistantMessage>;
pub type AssistantMessageEventStream = AssistantStream;
```

`EventStream` is non-Clone and exposes only `next(&mut self)` and
`result(&self)`. It has no public `push`, `end`, generic producer, or constructor
that can skip assistant terminal validation. The canonical public
`create_assistant_message_event_stream` projection returns an
`AssistantMessageEventStream` plus a cloneable `AssistantStreamProducer` whose
`push` validates every terminal. `AssistantStream` is only an additional
Rust-convenience alias, not a claimed upstream export. This preserves the three
pinned root symbols without preserving JavaScript's unrestricted producer
authority.

`result()` returns owned `R` and requires `R: Clone` only on that method and its
future; the stream type and `next()` impose no `Clone` bound on `E`. Multiple
result waiters each receive a logically equal clone. The result is never a
one-time take and does not expose `Arc<R>`.

The factory input is frozen:

```rust
pub struct AssistantStreamInit {
    pub api: ApiId,
    pub provider: ProviderId,
    pub model: ModelId,
    pub clock: Arc<dyn Clock>,
    pub cancellation: CancellationToken,
}

pub fn create_assistant_message_event_stream(
    init: AssistantStreamInit,
) -> (AssistantMessageEventStream, AssistantStreamProducer);
```

`AssistantStreamInit` is non-Serde and Debug-redacted. The resulting state
retains only the three ids, clock, and a weak cancellation registration; it does
not retain auth, header, endpoint, telemetry, spawner, or arbitrary cause text.

`AssistantStream` and its producer share one private state machine:

```text
Open { FIFO events, event waiters, result waiters }
  -- validated terminal event -->
Settled { FIFO through terminal, final message }
```

The transition is atomic under one state lock. The terminal event's nested
message becomes the result in the same transition. There is no generic `end()`
that can close the queue without fixing a result.

The consumer is non-cloneable and `Send`, but need not be `Sync`. Its exclusive
`next(&mut self)` borrow statically permits one in-flight event waiter. Result
waiters use stable waiter ids and may coexist. The producer is cloneable only if
all clones target the same atomic settlement owner. Awaiting `result()` never
drains events. Waiting before a push and pushing before a wait must both avoid
lost wake-ups. Producer misuse returns a typed protocol error and cannot
synthesize a second terminal.
Duplicate terminals and progress after settlement return `AlreadySettled`.

Request cancellation owns a weak settlement capability and invokes the same
atomic `try_settle` path with an aborted terminal. It remains effective when a
non-cooperative setup task keeps a producer alive. Whichever valid provider
terminal or cancellation transition wins is final.

Wakers are registered/deduplicated under the state lock, detached with the state
change, and invoked only after unlock. A dropped `next()`, `result()`, sleep, or
cancellation wait unregisters its waiter so open streams/tokens cannot retain an
unbounded abandoned-waker list. Reentrant-waker tests are mandatory.

Producer clones share one producer-group count. If the final producer is
dropped while still open, the group settles once to a sanitized setup error
rather than leaving `result()` pending forever. The factory captures only
`ApiId`, `ProviderId`, `ModelId`, and the injected clock needed to construct the
fallback; it captures no credential, header, endpoint, or source text.

## Terminal contract

Accepted terminal pairs are:

```text
done:  stop | length | toolUse | deferred
error: error | aborted
```

The nested assistant message must carry the same stop reason. `pending` is
never terminal. Error and aborted messages carry a nonempty sanitized
`errorMessage`; both missing and empty strings are invalid in programmatic and
Serde paths. Cancellation is distinct from provider failure. Setup/auth/
adapter errors terminate in-band with zero usage unless independently known.

## Executor-neutral ports

Core defines its own explicit future alias and object-safe process-local ports:

```rust
pub type BoxFuture<'a, T> =
    Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait TaskSpawner: Send + Sync {
    fn spawn(&self, task: BoxFuture<'static, ()>)
        -> Result<(), SpawnError>;
}

pub trait Clock: Send + Sync {
    fn now_unix_ms(&self) -> u64;
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>>;
}
```

The reference clock/spawner may use standard-library threads; tests use
deterministic fakes and barriers. No S03 behavior relies on elapsed wall time.
Spawning is used to drive synchronous-return stream setup, not retry policy.

`CancellationToken` is cloneable, idempotent, executor-neutral, and provides
both `is_cancelled()` and a cancellation future. Combining cancellation sources
creates weak child links owned by an RAII registration, introduces no strong
cycle, settles once, and releases registered children/wakers when the combined
token is dropped.

Its complete public API is `new`, `cancel`, `is_cancelled`, `cancelled`, and
`linked(sources: &[CancellationToken]) -> CancellationToken`. The parent-link
registration type is private. An empty source slice creates an uncancelled token;
any already-cancelled source makes the linked token immediately cancelled.

## Telemetry prerequisite

`pi-telemetry` provides only the minimum object-safe context/span contract,
attribute/status DTOs, and no-op implementation needed to carry an explicit
parent context:

```rust
pub enum AttributeValue {
    String(String),
    Number(FiniteTelemetryNumber),
    Boolean(bool),
    Strings(Vec<String>),
    Numbers(Vec<FiniteTelemetryNumber>),
    Booleans(Vec<bool>),
}

pub type SpanAttributes = BTreeMap<String, AttributeValue>;
pub struct SpanOptions { pub name: String, pub attributes: SpanAttributes }
pub enum SpanStatus { Ok, Error { error: Option<TelemetryErrorInfo> } }

pub trait TelemetryContext: Send + Sync {
    fn start_span(&self, options: SpanOptions) -> SpanGuard;
}

pub trait TelemetrySpan: Send + Sync {
    fn start_child(&self, options: SpanOptions) -> SpanGuard;
    fn add_event(&self, name: &str, attributes: &SpanAttributes);
    fn set_attributes(&self, attributes: &SpanAttributes);
    fn set_status(&self, status: SpanStatus);
    fn finish(&self);
}

pub struct SpanGuard { /* non-Clone, Send, explicit-or-Drop settlement */ }
```

`SpanGuard` delegates child spans and recording methods, settles exactly once,
and calls an idempotent backend `finish` on explicit completion or drop. The
no-op context/span implement the same ABI. `RequestRuntime` stores
`Arc<dyn TelemetryContext>`, making it `Send + Sync` and movable into a spawned
`'static` setup task. S03 does not implement schemas, exporters, files, network,
or the full memory conformance suite reserved for S13.

The Rust span API uses an explicit span handle and settlement guard; its helper
may accept a trusted in-process closure, but no closure is serializable or
available to an extension peer.

## Endpoint custody

`RequestEndpoint` is non-Serde and wholly redacted in `Debug`. It can be created
only by resolving:

1. a transient credential endpoint override;
2. otherwise the model's classified public endpoint;
3. otherwise an explicit Azure runtime-sentinel resolver.

An unresolved sentinel is a typed setup error. Resolution never changes the
input `Model`, `ImagesModel`, or `ModelsStoreEntry`.

`RequestEndpoint::expose_for_transport()` is the one public read boundary. It
returns a borrow only to code already holding the resolved request capability;
its name and documentation make exposure explicit. No `Display`, `AsRef<str>`,
`Deref`, clone-to-model helper, or Serde implementation is provided.

Every outbound signature uses one of these non-Serde request roots:

- `TextStreamRequest`;
- `SimpleStreamRequest`;
- `DeferredFetchRequest`;
- `DeferredCancelRequest`;
- `ImageGenerationRequest`.

Each root contains `ResolvedRequest<M> { model, endpoint }`, resolved transient
auth/header data, and runtime options. No overload accepts a raw model.

The nominal roots have these minimum fields:

```rust
pub struct ResolvedAuth {
    pub api_key: Option<SecretString>,
    pub headers: ProviderHeaders,
    pub env: ProviderEnv,
}

pub struct TextStreamRequest {
    pub resolved: ResolvedRequest<Model>,
    pub context: Context,
    pub auth: ResolvedAuth,
    pub options: StreamOptions,
}

pub struct SimpleStreamRequest {
    pub resolved: ResolvedRequest<Model>,
    pub context: Context,
    pub auth: ResolvedAuth,
    pub options: SimpleStreamOptions,
}
pub struct DeferredFetchRequest { /* resolved, handle, auth, options */ }
pub struct DeferredCancelRequest { /* resolved, handle, auth, options */ }
pub struct ImageGenerationRequest { /* resolved image model, context, auth, options */ }
```

`ResolvedAuth` is explicitly non-Serde even though its nested transient types
have compatibility serialization. Its manual `Debug` prints only API-key
presence and header/environment keys. Direct compile-fail and secret-sentinel
tests cover the standalone type as well as every containing request root.

`ResolvedAuth` never retains `ModelAuth.baseUrl`; the later auth/registry unit
consumes that value while constructing `RequestEndpoint`. This stream unit owns
the opaque endpoint and explicit endpoint constructor for an already resolved
nonempty URL. The contract freezes an already-resolved per-call runtime endpoint,
not an Azure resolver callback: text/deferred precedence belongs to
`ModelsRegistry`, while S05 resolves Azure/image-provider inputs before using
the same custody root.

Serializable request option DTOs preserve the pinned two-layer stream shape and
all provider-neutral fields:

- `StreamOptionsWire` contains only common provider-request plus raw-stream
  fields and rejects `toolChoice`, `reasoning`, `deferred`, and
  `thinkingBudgets`;
- `SimpleStreamOptionsWire` flattens the raw-stream wire shape and adds exactly
  those four simple-only fields;
- `DeferredFetchOptionsWire` has timeout, max retries, max retry delay, and
  `wait`;
- `DeferredCancelOptionsWire` has timeout, max retries, and max retry delay;
- `ImagesOptionsWire` adds the common timeout/retry fields to metadata.

The non-Serde `StreamOptions`, `SimpleStreamOptions`,
`DeferredFetchOptions`, `DeferredCancelOptions`, and `ImagesOptions` each
contain their corresponding wire DTO plus `RequestRuntime`.
`SimpleStreamRequest.options` is `SimpleStreamOptions`; it is not the raw stream
wrapper. Provider-specific fields remain S04/S05. Positive/negative Serde tests
prove that each of the four simple-only fields is rejected by raw stream decode
and accepted by simple decode.

## Adapter boundary

`ApiAdapter` and `ImagesApiAdapter` are object-safe. Async methods return
explicit borrowed `BoxFuture`s. Stream methods return `AssistantStream`
synchronously and drive any asynchronous setup through the supplied spawner.

Adapters are constructed by S04/S05 with their HTTP transport and middleware.
Core never imports those types. Foreign providers use the S12 protocol adapter;
they are not Rust trait objects crossing a process boundary.

## Error and secrecy rules

- Protocol misuse, spawn failure, endpoint failure, and cancellation have
  separate typed categories.
- Runtime errors are non-Serde and non-exhaustive.
- `Display` is bounded and sanitized; `Debug` adds no source detail.
- Optional sources remain reachable through `Error::source()` but source text
  is never automatically copied into terminals or telemetry.
- Compile-fail probes prove endpoint, resolved request, runtime options, stream,
  `ResolvedAuth`, and cancellation handles do not implement Serde.
- `Usage::zero()` and an internal assistant-runtime-error constructor create
  complete fallback messages without panics. Clock values above the JavaScript
  safe-integer maximum saturate at that maximum.
