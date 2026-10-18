# Request runtime and assistant streams

The current boundary targets Pi `v1.4.0` at
`4a080252f06caf63a525d7873c5d94d3c23887e0`.

`TextStreamRequest` and `SimpleStreamRequest` are non-serializable resolved
capabilities containing a validated model, transient `RequestEndpoint`,
normalized `TranscriptContext`, explicit resolved auth, and wire/runtime
options. `DeferredFetchRequest` and `DeferredCancelRequest` retain the same
custody separation for provider-native deferred handles. Image-generation
request roots and the Azure-only runtime endpoint sentinel are removed.

Caller-facing `Context` shorthand becomes a leading system message exactly
once at registry dispatch. Provider conversions use the normalized transcript
and replay current tools/sections or retain anchored changes according to model
capabilities. Registry default client identity merges field by field with the
request identity for all four request roots, including explicit-null attribution
suppression; total absence stays absent. Authentication,
provider selection, header precedence, captured registration identity, and
request-scoped lifecycle hooks retain one authority and existing cancellation
ownership.

`RequestRuntime` holds an explicit cancellation token, `Arc<dyn TelemetrySpan>`,
injected clock/spawner, and an optional `ProviderStreamEventObserver`. The span
can parent child spans and carry causal links; propagation is an opaque adapter
contract. The provider event observer receives the adapter's parsed SDK-equivalent
event before assistant reduction. Google includes response-header metadata in
its SDK-equivalent event. Observer failure settles the request once.

`AssistantMessageEventStream` has one canonical terminal result and owned
snapshot events. `result()` observes settlement without consuming event history.
Cancellation can preserve the last published usage/content/signature snapshot;
a producer drop or setup failure still settles in-band. Child-owned cancellation
at registry/lazy handoff retains the child terminal instead of replacing its
accounting with an empty outer result. Dropping only the consumer does not
cancel detached producer work.

Terminal settlement fills an absent duration only for a message whose timestamp
belongs to the new stream. Existing durations and older forwarded/deferred
messages retain their timing. The event terminal and stored result carry the
same completed message. Duration is measured by the native monotonic clock;
Unix timestamps remain injected.

Finished tool JSON must form an object. Non-object or invalid completed input
becomes empty arguments plus `arguments_error`, retaining the complete text;
partial streaming values never execute as tool calls. Validation rejects this
diagnostic before schema validation. Bounded JSON, schema, identity, and resource
limits remain enforced, with redacted process-local errors.

The existing adversarial cancellation, race, custody, byte-split, UTF-8, retry,
header, response-body presence, and loopback suites cover the retained protocols.
The exact-release suite separately compares request payloads, wires, provider
and assistant events, malformed argument completion, and terminal messages.
