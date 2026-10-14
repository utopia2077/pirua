# Portable domain behavior

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Message transformation

`transform_messages` accepts strict `Message` values, a target `Model`, an
injected synthetic-result timestamp, and an optional trusted in-process
`ToolCallIdNormalizer` policy. The policy is invoked synchronously and is never
stored. Foreign extensions cannot supply it as a function pointer.

The transformation preserves pinned behavior:

- downgrade unsupported user/tool images to distinct text placeholders,
  suppressing insertion only when the preceding emitted block already equals
  the role-specific placeholder;
- preserve same-model redacted thinking and nonempty-signed empty thinking;
- drop foreign redacted thinking, turn foreign nonempty thinking into text, and
  remove every foreign text signature and only a nonempty foreign tool-call
  thought signature; an empty tool-call signature remains by pinned JavaScript
  truthiness;
- normalize assistant tool-call ids and following matching results;
- reject two different source ids that normalize to one target id;
- drop error/aborted assistant turns;
- synthesize missing tool failures at assistant, user, and end boundaries in
  source tool-call order.

Missing/null content repair belongs to a separate raw compatibility decoder in
`pi-ai`, because strict S02 DTOs cannot represent invalid content.
The normalizer is a borrowed synchronous trusted-Rust policy, never stored or
serialized. Synthetic result time is injected once per transform call.

## Tool validation

Validation clones input arguments before coercion. It supports the JSON Schema
features exercised by pinned tools, including primitive/union coercion
(required boolean null becomes false),
optional-null normalization, local references, arrays, objects, required,
properties, additional properties, `allOf`, `anyOf`, and `oneOf`. The selected
schema evaluator supplies the remaining structural keywords used by provider
tools.

External references and network retrieval are forbidden. A root without
`$schema` is compiled explicitly as Draft 7; a root with `$schema` uses
jsonschema draft autodetection. Both branches always install the accepted
`PatternOptions` limits and offline retriever. Invalid schemas and
invalid arguments return different typed errors. Issues contain a bounded path,
keyword/category, and sanitized message. Full arguments are available only by
explicit caller access to the original input, not error formatting.
One borrowed structural preflight runs before owned conversion and caps schema/
argument physical depth at 128 and bytes at 256 KiB/one MiB. On a canonical
cache miss, a fail-only jsonschema registry computes the schema graph from the
root, applicators recognized by the selected draft, and the transitive targets
of ref kinds active in that draft. Later-draft keywords remain data unless an
active ref promotes their value. Pinned registry preparation may nonetheless
fail closed on an external `$ref` found by its over-broad Draft 4 crawler under
a later-draft location; direct jsonschema 0.52.1 construction does the same,
without activating patterns there. A referenced value becomes a schema even when
stored below default or another data-valued location; unreferenced data and
property/definition/regex names remain data. JSON Pointer percent decoding,
anchors, nested resource ids, draft, base URI, and dynamic scope follow pinned
jsonschema 0.52.1 semantics. The lookup-returned resolver is authoritative: a
recognized-path target is already positioned and is not advanced twice, while
a target promoted from a data-valued path retains the returned containing base
and does not retroactively apply its own id. The graph is capped at
65,536 resolver contexts and dynamic-scope depth 128. Effective URI strings and
complete scope sequences are interned to integer ids; distinct effective URI
text is capped at one MiB before context retention. The same physical pattern
occurrence is counted once across repeated references. The canonical-content cache has 16 entries;
at most 32 compiled pattern strings and 8 KiB aggregate pattern source are
accepted. Explicit compile/DFA/backtrack limits apply. Because jsonschema
0.52.1 treats a patternProperties match error as no match, the wrapper
conservatively checks every bounded argument object key against companion
top-level pattern validators collected from that same closed schema graph and
built with the same draft/offline/options. An
ordinary mismatch is ignored; a runtime/backtrack failure becomes the fixed
root `patternProperties` issue `pattern evaluation limit exceeded`. Direct references do not skip sibling
coercion; a helper probe that cannot resolve the root's local definitions stays
unknown until full-root validation. Paths reconstruct TypeBox's raw
slash-to-dot behavior from decoded segments, including empty segments.
`iter_errors` internally collects an eager vector;
the published report is capped at 16 issues, but only the schema/argument input
bounds constrain that internal allocation. Every validator and schema registry
is fail-only/offline, and non-local references anywhere in the closed graph are
rejected. Arguments are cloned only after an exact canonical cache entry has
been accepted. Internal randomized hash order is never observable.

## Classification

Overflow classification ports every pinned positive pattern and exclusions.
Usage-based detection uses `input + cacheRead`, strict greater-than for silent
overflow, and the 99-percent zero-output length rule. Recoverable length is
strictly below the desired output count.

Retry classification ports every pinned transient and nonretryable quota/
billing marker. Nonretryable markers win. Only assistant messages with
`stopReason:error` and a nonempty message can classify retryable. S03's overlap
row proves only that both pure classifiers return true; the actual caller
precedence belongs to the S15 coding-runtime slice that projects pinned
`AgentSession._isRetryableError`. No backoff calculation, sleep, or callback
loop exists in S03; those helper lines belong to the selector-partitioned S05
compatibility surface.

## Context estimation

All string lengths use JavaScript UTF-16 code-unit semantics. The estimator
uses four code units per token and 4,800 code units per image. It selects only a
recent valid assistant usage block that still describes the current prefix,
then estimates trailing messages. System prompt and full tool schemas are added
only without prior usage; after usage, only tools named by later
`addedToolNames` are added.
Public counts and indexes are checked `SafeUnsigned` values; arithmetic overflow
is a typed error rather than a panic or saturating underestimate.

## JSON and text helpers

- `short_hash` reproduces JavaScript UTF-16 code units, 32-bit `Math.imul`,
  unsigned shifts, and base-36 output.
- JSON repair handles raw control characters and invalid escapes without
  changing valid JSON. Partial parsing is bounded and never fetches or executes
  code.
- `content_text` concatenates only text blocks according to pinned behavior.
- JSON serialization used for estimates is deterministic; ordering differences
  may not change counted length.

The streaming entry checks its 256-KiB byte limit before complete or partial
parsing. At or below the limit the depth-128 partial parser makes linear
progress, preserves completed collection members across a malformed follower
or missing comma like pinned partial-json 0.1.7, rejects duplicate keys and
nonfinite/unsafe numbers, and returns an empty object after its
complete/raw-partial/repaired-partial fallback sequence fails. Any over-limit
input returns the empty object.

## UUIDv7

UUID generation is an instance-owned monotonic generator receiving clock and
16-byte entropy ports. It preserves the pinned RFC 9562 layout, clock rollback
behavior, 32-bit sequence rollover, lexicographic monotonicity, and lowercase
format. There is no hidden process-global clock/random authority in core.
Entropy is obtained and the clock is read before the state lock. A 48-bit
timestamp overflow fails without mutating generator state.

## Deferred tools

Deferred-tool splitting normalizes names, deduplicates tools in source order,
and distinguishes tools already present in the transcript from newly added
tools. No provider request occurs in this helper.

## Session resources

`SessionResourceRegistry` stores trusted process-local `SessionResource`
objects behind unique monotonic tokens. A non-cloneable handle removes exactly
one token explicitly or on drop.

Cleanup snapshots active registrations in insertion order, marks cleanup in
progress, releases the registry lock, and invokes each snapshot member. A
resource registered or removed during callbacks affects only the next cleanup.
Nested cleanup returns `CleanupInProgress`. One failure does not stop later
resources; failures are returned as an ordered aggregate with redacted sources.
After the snapshot finishes, a failed resource remains registered only if the
same token is still active. Explicit removal or handle drop wins over failure:
the current-pass failure is reported but the token is not reinserted. A
successful callback removes the same still-active token. No callback executes
under a lock. `SessionResource::cleanup` is synchronous, matching pinned
cleanup. Successful resources are removed after one cleanup pass. Failure order
is callback error/panic in snapshot order, followed by disposal panics in the
fixed disposal order. All callbacks finish before state finalization; detached
callback panic payloads are then disposed in snapshot order, followed by
removed resource owners in snapshot order. Each disposal has its own
catch-unwind guard and occurs while cleanup still reads in-progress. Reentrant
destructors therefore cannot acquire a nested pass; destructor panic is
sanitized and cannot skip another callback/drop.

This is a trusted in-process lifecycle port, not the extension mechanism. The
language-neutral protocol may stream frames but never transports a Rust trait
object, JavaScript function, or language stream object/handle. Foreign peers do
not enter this registry. Accepted S12 `resource.v1` only lists and reads
resource values; it has no session-cleanup lifecycle, so foreign cleanup parity
is a deliberate future compatibility gap rather than current v1 behavior.

Owner wiring is cross-slice and explicit: S04 exposes the transport resource
object for session-scoped adapter state; S05 registers it and retains the
non-clone handle for the provider composition lifetime; S15 owns the registry
instance and calls cleanup from coding-session disposal. S03 defines registry,
token, handle, and ordering semantics only.

## Error discipline

All helper errors are typed, non-exhaustive where public, and bounded. Debug
output must not include credential, endpoint, raw header, tool-argument, or
provider-cause sentinel values. User-controlled input never causes a panic.
