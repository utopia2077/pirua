# Portable domain runtime

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## Callback and extension boundary

S03 permits three trusted, process-local policies:

- a borrowed synchronous tool-call-id normalizer;
- a stored object-safe dynamic-model fetch capability and model filter;
- a borrowed synchronous tool-name normalizer.

They are Rust traits, not protocol messages. The message and tool-name
normalizers are invoked only during one call and are never stored. Provider
fetch/filter capabilities are stored only inside the trusted Rust provider
composition object. None implements Serde; none may be supplied by a foreign
extension.

Cross-language providers use the versioned provider.v1 protocol. That protocol
transports values and may use streaming frames, but never transports a
function, credential, trait object, process handle, or language stream
object/handle. Protocol v1 remains auth-mode none. S04 owns
trusted transport adapters; S05 owns concrete provider factories and catalog
fetchers. This separation is the answer to the JavaScript-callback concern:
language-neutral extensions implement protocol capabilities, while trusted
in-process Rust composition uses object-safe traits.

## Message transformation

### Public API

~~~rust
pub trait ToolCallIdNormalizer {
    fn normalize_tool_call_id(
        &mut self,
        source_id: &ToolCallId,
        target_model: &Model,
        source_message: &AssistantMessage,
    ) -> ToolCallId;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum MessageTransformError {
    NormalizedToolCallIdCollision,
}

pub fn transform_messages(
    messages: &[Message],
    target_model: &Model,
    synthetic_result_timestamp: SafeUnsigned,
    normalizer: Option<&mut dyn ToolCallIdNormalizer>,
) -> Result<Vec<Message>, MessageTransformError>;
~~~

The trait is deliberately not Send, Sync, Clone, Serde, or static: it is a
borrowed request-local policy. The error has fixed bounded Display and Debug
text and contains no message or tool-call id. Collision failure returns no
partial output. A stateful normalizer is not rolled back.

The pi-ai facade separately exposes:

~~~rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct LaxMessageDecodeError {
    /* private safe index */
}

impl LaxMessageDecodeError {
    pub const fn index(&self) -> SafeUnsigned;
}

pub fn decode_lax_messages(
    messages: &[serde_json::Value],
) -> Result<Vec<Message>, LaxMessageDecodeError>;
~~~

The decoder clones each raw value, replaces missing or null content on user,
assistant, and tool-result messages with an empty array, then invokes strict
Message deserialization. Every other schema violation remains an error. The
input and error formatting never expose raw JSON.

### Exact behavior

The target supports images when Model.input contains Image. Otherwise user and
tool-result images become different fixed text placeholders. The source loop
suppresses an inserted placeholder when the immediately preceding emitted text
already equals that placeholder. It does not remove a same-text block that
follows an image; this asymmetric source behavior is normative.

A source assistant is from the same model only when provider, API, and model id
all match the target. Same-model redacted content remains replayable. Only a
nonempty thinking signature protects same-model empty/blank thinking; an empty
string is JavaScript-falsy and does not. Foreign redacted thinking is dropped;
foreign nonempty thinking becomes plain text; foreign text loses any text
signature. A foreign tool call loses a nonempty thought signature but preserves
an explicitly present empty string, matching the pinned truthiness branch; all
other tool-call fields remain.

The normalizer is skipped for same-model calls. For foreign calls it runs in
content order with the complete source assistant and target model. Only changed
ids enter the source-to-target result map. A preceding result is never
retroactively rewritten. A reverse final-id owner rejects two distinct source
ids that resolve to the same final id, including changed-id versus unchanged-id
collisions. Repeated occurrences of the same source id remain legal.

The second pass flushes pending tool calls at assistant, user, and end
boundaries. Error and aborted assistant messages are removed; such a message is
still a boundary for the preceding turn, but its own calls never become
pending. Synthetic results are emitted in source call order with exact text
No result provided, is-error true, missing optional fields, and the injected
timestamp.

### Exact 31-row inventory

| Stable id | Test binding | Pinned source | Expected observation |
|---|---|---|---|
| transform.lax.null-missing-content | lax_decoder_repairs_null_and_missing_content_without_mutation | lax-message-content.test.ts:31 | three roles repair missing/null content and raw values stay unchanged |
| transform.images.capable-pass-through | image_capable_model_preserves_input_blocks | transform-messages.ts:35 | image-capable target preserves blocks |
| transform.images.user-placeholder | text_only_model_uses_user_image_placeholder | transform-messages.ts:41 | user images use the user placeholder |
| transform.images.tool-placeholder | text_only_model_uses_tool_image_placeholder | transform-messages.ts:48 | tool images use the tool placeholder |
| transform.images.adjacent-collapse | adjacent_unsupported_images_collapse_to_one_placeholder | transform-messages.ts:15 | consecutive source images emit one placeholder |
| transform.images.existing-placeholder-collapse | existing_placeholder_before_image_suppresses_duplicate | transform-messages.ts:29 | preceding exact placeholder suppresses insertion |
| transform.thinking.same-redacted-preserved | same_model_preserves_redacted_thinking | transform-messages.ts:101 | same-model redacted block is unchanged |
| transform.thinking.foreign-redacted-dropped | foreign_model_drops_redacted_thinking | transform-messages.ts:101 | foreign redacted block is removed |
| transform.thinking.same-signed-empty-preserved | same_model_preserves_signed_empty_thinking | transform-messages.ts:107 | nonempty-signed empty/blank thinking remains |
| transform.thinking.unsigned-empty-dropped | unsigned_empty_thinking_is_dropped | transform-messages.ts:110 | missing/empty-signed empty/blank thinking is removed |
| transform.thinking.same-nonempty-preserved | same_model_preserves_nonempty_thinking | transform-messages.ts:112 | same-model nonempty thinking is unchanged |
| transform.thinking.foreign-nonempty-to-text | foreign_nonempty_thinking_becomes_unsigned_text | transform-messages-copilot-openai-to-anthropic.test.ts:50 | foreign thinking becomes plain text |
| transform.text.same-signature-preserved | same_model_preserves_text_signature | transform-messages.ts:119 | same-model text signature remains |
| transform.text.foreign-signature-removed | foreign_model_removes_text_signature | transform-messages.ts:119 | foreign text retains text only |
| transform.tool.same-thought-signature-preserved | same_model_preserves_tool_thought_signature | transform-messages.ts:127 | same-model thought signature remains |
| transform.tool.foreign-thought-signature-removed | foreign_model_removes_only_tool_thought_signature | transform-messages.ts:131 | foreign call removes nonempty thought signature, preserves empty, and changes nothing else |
| transform.tool-id.same-model-bypass | same_model_bypasses_tool_call_id_normalizer | transform-messages.ts:95 | exact same model makes zero normalizer calls |
| transform.tool-id.foreign-normalized | foreign_tool_calls_receive_target_and_source_normalization | transform-messages.ts:136 | foreign calls normalize in content order |
| transform.tool-id.following-result-remapped | following_tool_results_use_normalized_ids | transform-messages.ts:83 | later matching results use normalized ids |
| transform.tool-id.preceding-result-unchanged | tool_results_preceding_their_calls_remain_unchanged | transform-messages.ts:83 | preceding result stays unchanged |
| transform.tool-id.collision-rejected | normalization_collision_is_rejected_without_partial_output | upstream-map.toml:861 | two source owners of one final id fail atomically |
| transform.assistant.error-dropped | errored_assistant_turn_is_dropped | transform-messages.ts:189 | error assistant is removed |
| transform.assistant.aborted-dropped | aborted_assistant_turn_is_dropped | transform-messages.ts:189 | aborted assistant is removed |
| transform.orphan.assistant-boundary | assistant_boundary_repairs_previous_orphans | transform-messages.ts:185 | assistant boundary flushes prior missing results |
| transform.orphan.user-boundary | user_boundary_repairs_previous_orphans | transform-messages.ts:210 | user boundary flushes prior missing results |
| transform.orphan.end-boundary | end_boundary_repairs_trailing_orphans | transform-messages-copilot-openai-to-anthropic.test.ts:136 | transcript end flushes pending calls |
| transform.orphan.partial-result-source-order | partial_results_synthesize_only_missing_calls_in_source_order | transform-messages-copilot-openai-to-anthropic.test.ts:162 | only missing results synthesize in source order |
| transform.orphan.complete-results-no-synthesis | complete_tool_results_do_not_synthesize_failures | transform-messages.ts:163 | complete results add no synthetic result |
| transform.orphan.unrelated-result-preserved | unrelated_tool_result_is_preserved_and_does_not_settle_pending_call | transform-messages.ts:207 | unrelated result remains and pending call still flushes |
| transform.orphan.failed-assistant-boundary | failed_assistant_boundary_repairs_previous_but_not_failed_calls | transform-messages.ts:185 | failed boundary flushes prior calls, not its own |
| transform.orphan.injected-timestamp | synthetic_results_use_injected_timestamp_and_exact_shape | transform-messages.ts:163 | every synthetic result has exact shape and injected time |

## Tool validation

### Public API

~~~rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolValidationIssue {
    /* private bounded path, keyword, and fixed message */
}

impl ToolValidationIssue {
    pub fn path(&self) -> &str;
    pub fn keyword(&self) -> &str;
    pub fn message(&self) -> &str;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolValidationReport {
    /* private bounded issues and omitted count */
}

impl ToolValidationReport {
    pub fn issues(&self) -> &[ToolValidationIssue];
    pub const fn omitted_issue_count(&self) -> SafeUnsigned;
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolValidationError {
    ToolNotFound,
    InvalidSchema,
    InvalidArguments(ToolValidationReport),
}

pub fn validate_tool_call(
    tools: &[Tool],
    tool_call: &ToolCall,
) -> Result<JsonObject, ToolValidationError>;

pub fn validate_tool_arguments(
    tool: &Tool,
    tool_call: &ToolCall,
) -> Result<JsonObject, ToolValidationError>;
~~~

Errors, issues, and reports are non-Serde. Display and Debug expose only
bounded structured fields and fixed messages. They do not retain a raw
jsonschema error because it may retain or format argument data.

validate-tool-call selects the first exact case-sensitive ToolName. The
arguments function does not impose an additional equality check between the
tool declaration and call name. It deep-clones arguments only after borrowed
structural preflight succeeds and an exact canonical schema cache entry is
accepted.

### Coercion and validation order

The exact order is:

1. iteratively preflight the complete borrowed schema and arguments for
   physical depth and exact encoded size before owned conversion;
2. reject over-depth or over-size values;
3. canonicalize and convert only the bounded schema;
4. on a cache miss, compute its fail-only reference-closed schema graph, reject
   non-local references and excessive pattern strings/source bytes, then build
   the validator and companion guards; an exact canonical cache hit reuses that
   accepted result;
5. clone arguments;
6. recursively delete optional null properties whose direct schema does not
   accept null;
7. apply serialized-schema coercion;
8. run the companion regex guard and cached offline validator, then return an
   owned JsonObject or a typed error.

Primitive coercion follows JavaScript finite Number and String behavior:

- number: null to zero, finite nonempty numeric string to number, bool to zero
  or one;
- integer: the same, but only integral results;
- boolean: null to false, exact true/false strings, and numeric zero/one;
- string: null to empty and number/bool to ECMAScript string form;
- null: empty string, numeric zero, and false to null.

The numeric parser recognizes the finite ECMAScript ToNumber subset, including
ECMAScript whitespace and unsigned hexadecimal, binary, and octal forms.
Signed non-decimal forms, infinity, NaN, and overflow do not coerce. ryu-js
1.0.3 supplies ECMAScript number-to-string formatting.

allOf applies in source order. anyOf and oneOf preserve an already matching
arm; otherwise they clone and test coerced candidates in source order. A type
array preserves an already matching type and otherwise tries primitive
coercions in declared order. Object properties run before schema-valued
additionalProperties. Tuple items use their indexes; one item schema applies
to every array member. Defaults are not materialized.

A required null is never deleted. A nullable optional null remains. A direct
reference property remains for final validation. Encountering a direct `$ref`
does not return early: sibling coercion keywords on that same schema are still
processed. An isolated helper validator used to probe a coercion arm may be
unable to resolve a reference into the root's `$defs`; that probe is
nonmatching/unknown and selection continues, rather than classifying the root
schema. Only construction and validation of the complete root can return
`InvalidSchema` for such a reference.

### Schema and cache boundary

The only accepted schema is the serialized JsonSchema value. TypeBox symbols,
object identity, and generated code are non-authoritative. A local reference
begins with #. A schema URI selecting a built-in meta-schema is not retrieval.
On a cache miss, a registry with an explicit fail-only retriever computes a
fixed-point schema graph. Its seeds are the root and values below applicators
recognized by the selected draft: properties, the draft's definitions form,
patternProperties,
dependentSchemas, schema-valued dependencies, schema arrays, and direct
schema-valued keywords. Only reference keywords recognized by that draft are
active; later-draft keywords remain data. Each active schema-frame `$ref`,
`$dynamicRef`, or `$recursiveRef` is resolved using pinned jsonschema 0.52.1
semantics, and its target is promoted to a schema frame even when physically stored below
default, an unknown extension, or another otherwise data-valued location.
Transitive targets are processed until closure; every reached non-local ref is
`InvalidSchema`.

Registry preparation and graph interpretation are distinct. The pinned Draft 4
registry crawler over-enumerates some later-draft child locations while the
validator compiler ignores them. An external `$ref` encountered only by that
preparatory crawl therefore makes both direct offline jsonschema construction
and S03 fail closed as `InvalidSchema`; pattern and applicator accounting still
uses the selected-draft table and does not reinterpret the location as schema.

Resolver context carries the detected draft, effective embedded-resource base,
and dynamic scope. Fragment JSON Pointers use the dependency's UTF-8 percent
decoding, `~0`/`~1` object-key decoding, array-index rules, and nested id scope;
named, dynamic, legacy, and recursive anchors use the selected draft. Repeated
and cyclic references terminate through physical-node plus resolver-context
identity. At most 65,536 distinct contexts and dynamic-scope depth 128 are
accepted. Each distinct effective URI string and complete dynamic-scope
sequence is interned once to a small integer id before context insertion; the
aggregate distinct URI text is capped at one MiB. Context keys therefore retain
four fixed-size values rather than copied URI/scope strings. The resolver
returned by pinned reference lookup is authoritative. When a pointer follows a
recognized schema path, its target resource is already positioned and the
target id is not applied twice. When a pointer promotes a target from a
data-valued path, jsonschema 0.52.1 retains the containing resource base; the
target's own id is not retroactively applied and inner fragment refs resolve in
that returned containing scope. A direct active external ref on the promoted
target still fails; a physically nested ref that the returned scope and
selected-draft applicators do not reach remains data. Ordinary applicator
children enter their subresource exactly once.
Property/definition/regex map keys are names, not keywords. Objects
below const, enum, default, examples, and other instance-valued keywords remain
data unless reached by a reference; keyword-looking values in unreferenced data
are never reinterpreted.

Draft selection is root-sensitive. A schema root without `$schema` uses
explicit `Draft::Draft7`, matching the TypeBox-era input convention. A root
with `$schema` does not receive `with_draft`; jsonschema autodetects the
declared built-in draft, and malformed, unknown, or unavailable declarations
fail as `InvalidSchema`. Offline retrieval and the accepted pattern engine
limits are installed in both branches:

~~~rust
let options = jsonschema::options()
    .offline()
    .with_pattern_options(
        jsonschema::PatternOptions::fancy_regex()
            .backtrack_limit(100_000)
            .size_limit(131_072)
            .dfa_size_limit(65_536),
    );
let validator = if root_has_schema_keyword {
    options.build(&schema)
} else {
    options.with_draft(jsonschema::Draft::Draft7).build(&schema)
};
~~~

jsonschema is pinned to 0.52.1 with default features disabled. HTTP, file,
async resolution, macros, IDNA, Tokio, Reqwest, and Rustls remain disabled.

The borrowed structural preflight caps physical container depth at 128 and
computes exact deterministic encoded length without recursive Clone or Serde.
Canonical schema bytes are capped at 256 KiB and canonical argument bytes at
one MiB. A schema may compile at most 32 pattern strings with at most 8 KiB of
aggregate UTF-8 pattern source. One compiled string is each string-valued
pattern keyword in a schema frame and each key inside a schema-frame
patternProperties map; the latter cannot hide an unbounded number of patterns
behind one keyword. The same physical pattern occurrence reached through
multiple references counts once; two distinct occurrences with equal text
count twice. Property/definition names and unreferenced pattern-looking
instance data do not consume this budget. Every validator additionally uses:

~~~rust
jsonschema::PatternOptions::fancy_regex()
    .backtrack_limit(100_000)
    .size_limit(131_072)
    .dfa_size_limit(65_536)
~~~

Exceeding any schema, argument, pattern-count, pattern-byte, compiled/NFA-size,
or backtracking limit returns a fixed typed failure. jsonschema 0.52.1's
`patternProperties` implementation turns a matcher execution error into a
non-match. On a cache miss S03 therefore builds, with the selected root draft
and the same offline/PatternOptions configuration, one companion top-level
`pattern` validator for each patternProperties key discovered in that same
reference-closed schema graph and stores it inside the bounded cache entry.
After coercion and before final validation, the wrapper
conservatively walks every bounded argument object key against every cached
companion through its error-returning `validate` method; it never uses the
boolean matcher path. Ordinary pattern mismatches are ignored. This guard is
intentionally broader than schema-branch reachability so a dependency-swallowed
resource failure cannot become acceptance. A regex-engine/backtrack error
returns `InvalidArguments` with root path, keyword
`patternProperties`, and fixed message `pattern evaluation limit exceeded`;
it never falls through as a valid non-match and never includes the property
name or pattern. DFA size is an
optimization cap: exceeding it falls back to another bounded regex engine and
must preserve the correct validation result. Boundary, typed-failure, and DFA
fallback cases are deterministic tests; no elapsed-time assertion is used.

The process-wide compiled cache has at most 16 entries. Its deterministic
fixed-width digest is confirmed by canonical bytes before reuse. Cache miss
compilation occurs outside its lock; the second admission check coalesces
concurrent equal misses. Different bytes under one digest remain distinct.
FIFO insertion/eviction linearizes at the admission lock; hits do not reorder.
Invalid schemas are not cached. Poisoned locks recover, and displaced
validators/canonical bytes are dropped after unlock.

At most 16 issues are returned. Path is capped at 256 UTF-8 bytes, keyword at
64 bytes, and omitted issues are counted safely. All validator errors are
scanned under the one-MiB argument bound while retaining only the
lexicographically smallest 16 tuples of path, keyword, and fixed category.
In jsonschema 0.52.1 `iter_errors` first fills an internal eager `Vec`; the
16-issue rule bounds only the S03 report, not validator working memory. That
internal vector is constrained indirectly by the accepted one-MiB argument,
256-KiB schema, depth, and pattern bounds; S03 makes no 16-error memory claim.
The retained set is sorted before publication, so jsonschema's internally
random-seeded hash maps cannot affect issue order. Root is root. Required
errors append the first missing property to a dotted instance path. To match
TypeBox, S03 reads jsonschema's decoded instance-path segments, joins them with
raw slash separators, removes exactly the first slash, then replaces every
remaining slash with a dot. It neither emits nor preserves RFC 6901 `~0`/`~1`
escapes, and it preserves empty segments: properties `/` and `/a` format as
`.` and `.a`, while an empty property below `outer` formats as `outer.`. An
ordinary root issue is `root`; a root required issue appends its raw first
missing name, so missing `foo` formats as `foo`; a nested missing `inner` under
`outer` formats as `outer.inner`. Messages are selected from fixed keyword
categories and never use jsonschema error Display.

### Exact 30-row inventory

| Stable id | Test binding | Pinned source | Expected observation |
|---|---|---|---|
| validation.runtime.offline-no-code-generation | offline_validator_requires_no_code_generation_and_bounds_cache | validation.test.ts:37 | no eval/network; local ref works; external/invalid schema differ; cache stays bounded |
| validation.coerce.number-string | plain_number_coerces_finite_numeric_string | validation.test.ts:70 | numeric string becomes number |
| validation.coerce.number-true | plain_number_coerces_true_to_one | validation.test.ts:71 | true becomes one |
| validation.coerce.number-null | plain_number_coerces_null_to_zero | validation.test.ts:72 | required null becomes zero |
| validation.coerce.integer-string | plain_integer_coerces_integral_string | validation.test.ts:73 | integral numeric string becomes integer |
| validation.coerce.boolean-string-true | plain_boolean_coerces_true_string | validation.test.ts:74 | true string becomes true |
| validation.coerce.boolean-string-false | plain_boolean_coerces_false_string | validation.test.ts:75 | false string becomes false |
| validation.coerce.boolean-one | plain_boolean_coerces_one | validation.test.ts:76 | numeric one becomes true |
| validation.coerce.boolean-zero | plain_boolean_coerces_zero | validation.test.ts:77 | numeric zero becomes false |
| validation.coerce.string-null | plain_string_coerces_null_to_empty | validation.test.ts:78 | required null becomes empty string |
| validation.coerce.string-primitive | plain_string_coerces_boolean | validation.test.ts:79 | bool/number use ECMAScript string formatting |
| validation.coerce.null-empty-string | plain_null_coerces_empty_string | validation.test.ts:80 | empty string becomes null |
| validation.coerce.null-zero | plain_null_coerces_zero | validation.test.ts:81 | numeric zero becomes null |
| validation.coerce.null-false | plain_null_coerces_false | validation.test.ts:82 | false becomes null |
| validation.type-union.preserve-matching | type_union_preserves_existing_matching_arm | validation.test.ts:83 | matching string arm prevents number coercion |
| validation.type-union.coerce-later-arm | type_union_tries_later_coercible_arm | validation.test.ts:88 | later number arm accepts after boolean fails |
| validation.optional-null.object-recursion | optional_null_normalization_recurses | validation.test.ts:101 | optional nonnullable nulls disappear recursively |
| validation.optional-null.local-ref-nullable | local_nullable_reference_preserves_optional_null | validation.test.ts:126 | local nullable reference preserves null |
| validation.union.anyof-preserve-null | any_of_preserves_existing_null_arm | validation.test.ts:146 | matching null anyOf arm remains |
| validation.union.oneof-preserve-null | one_of_preserves_existing_null_arm | validation.test.ts:164 | matching null oneOf arm remains |
| validation.union.anyof-coerce-number | any_of_coerces_to_first_valid_arm | validation.test.ts:173 | first successful coerced arm wins |
| validation.type-union.nullable-array | nullable_array_type_preserves_null | validation.test.ts:182 | array/null union preserves null |
| validation.reject.boolean-string-one | boolean_rejects_numeric_one_string | validation.test.ts:199 | string one does not become boolean |
| validation.reject.boolean-string-zero | boolean_rejects_numeric_zero_string | validation.test.ts:200 | string zero does not become boolean |
| validation.reject.null-string | null_rejects_null_string | validation.test.ts:201 | null string does not become null |
| validation.reject.fractional-integer | integer_rejects_fractional_string | validation.test.ts:202 | fractional string does not become integer |
| validation.tool.known | validate_tool_call_selects_known_exact_name | validation.ts:302 | first exact tool returns owned validated arguments |
| validation.tool.unknown | validate_tool_call_rejects_unknown_name | validation.ts:303 | unknown tool returns fixed ToolNotFound |
| validation.issue.nested-required-path | nested_required_issue_has_bounded_dotted_path | validation.ts:282 | required issue names bounded nested dotted path |
| validation.arguments.nonmutation | validation_never_mutates_arguments_or_schema | validation.ts:318 | success and every failure leave inputs unchanged |

The runtime, string, and optional-null rows contain parameterized witnesses for
external references, invalid schemas, schema/argument/pattern boundaries,
structural-depth and pre-clone oversize rejection, compiled-size/backtrack
failures, DFA-over-limit fallback, ECMAScript numeric forms, additional
properties, tuple and homogeneous arrays, allOf, and error redaction. The
parameterized witnesses include depth 128/129 and one patternProperties object
with 32/33 keys, keyword-looking properties/definition names/regex keys and
const/enum/default/examples data that remain data in their containing frame,
local pointer targets below data-valued keys that promote 32/33-pattern and
8-KiB/one-byte-over boundaries, repeated-target physical deduplication,
distinct-target aggregation, transitive external-ref rejection, ref cycles,
percent-decoded and escaped object/array pointers, recognized embedded-resource
scope, trailing-slash relative ids without double entry, pinned containing-base
behavior for targets promoted from data-valued paths, Draft 4/7/2019/2020
keyword gating, reference promotion from an otherwise unknown keyword, and
dynamic-scope cycle termination. A 4,096-reference graph proves URI/scope reuse,
and exact unit boundaries cover 65,536 contexts, scope depth 128, and one MiB
of distinct effective URI text,
required boolean null coercing to false, `$ref` plus coercion
siblings, an isolated probe unable to resolve the root's local `$defs`, root
ordinary path `root`, root required path `foo`, nested required path
`outer.inner`, raw slash/empty path segments (`.`, `.a`, and `outer.`), and a
patternProperties property-name match that exceeds the backtrack limit and is
rejected by the wrapper. The runtime row also forces Draft 7 when root
`$schema` is absent, declared-draft autodetection when it is present, digest
collision, same-schema concurrent misses,
racing different-schema admission/eviction, poisoned-lock recovery, and
reentrant displaced-entry destruction after unlock. These witnesses do not
create extra stable rows.

## Failure classifiers

### Public API and dependency

~~~rust
pub fn is_context_overflow(
    message: &AssistantMessage,
    context_window: Option<SafeUnsigned>,
) -> bool;

pub fn is_recoverable_length(
    message: &AssistantMessage,
    desired_max_output: SafeUnsigned,
) -> bool;

pub struct OverflowPattern { /* private compiled pattern */ }

impl OverflowPattern {
    pub fn is_match(&self, message: &str) -> bool;
}

pub fn get_overflow_patterns() -> Vec<OverflowPattern>;

pub fn is_retryable_assistant_error(message: &AssistantMessage) -> bool;
~~~

The direct classifier dependency pins regex 1.13.1 with its defaults disabled
and std requested. jsonschema itself declares regex with upstream defaults, so
Cargo feature unification enables that transitive feature set in the assembled
crate; revision 36 records this unavoidable, non-I/O dependency fact rather
than claiming a false std-only graph. Private LazyLock RegexSet values own the
25 overflow, three exclusion, 41 transient, and eight nonretryable patterns.
The builders do not delegate compatibility to Rust regex Unicode or global
case-insensitive modes. Inputs are Rust strings, so lone UTF-16 surrogates are
outside this API. An audited translator expands ASCII letters to explicit
two-case classes, `\\d` to ASCII digits, and `\\s`/`\\S` to the full legacy
ECMAScript whitespace complement. It treats dot as legacy non-`u` JavaScript:
the frozen `.?` gap accepts at most one BMP non-line-terminator and therefore
does not consume one astral scalar as one character, while the frozen `.*`
form accepts any sequence of non-line-terminator Unicode scalars. Dot excludes
LF, CR, U+2028, and U+2029. Unsupported constructs, quantifier forms, or flags
fail construction. This is exact only for the frozen patterns and their ASCII,
whitespace, BMP/astral-gap, and line-terminator witnesses; it is not a general
RegExp compatibility promise.

Overflow first checks a nonempty error message, then exclusions, then positive
patterns. Silent stop overflow is input plus cache-read strictly greater than a
nonzero context. Length overflow requires zero output and input plus cache-read
at least 99 percent of a nonzero context. Each SafeUnsigned is converted before
the f64 operations so rounding order matches JavaScript Number. Recoverable
length is a separate strict output-below-desired predicate.

Retry requires Error and a present nonempty message. Nonretryable quota and
billing markers win over transient markers. No retry policy, delay, sleep,
callback, transport, or loop enters S03. The overlap fixture intentionally
makes both predicates true but asserts no caller choice. Pinned caller
precedence is a separate S15 coding-runtime obligation at
`packages/coding-agent/src/core/agent-session.ts:2846-2849`: its caller must
evaluate overflow first and suppress retry when both classifiers return true.

### Exact 43 overflow ids

Every row below fixes the fragment fields; tests may add parameterized
witnesses but may not choose another id, source, test binding, polarity, owner,
or expected result. `overflow.ts` means
`packages/ai/src/utils/overflow.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185`;
`retry.ts` has the analogous pinned prefix.

| Stable id | Source | Test binding | Polarity | Owner | Expected observation |
|---|---|---|---|---|---|
| overflow.error.anthropic-prompt-too-long | overflow.ts:38 | overflow_error_pattern_rows | positive | S03-overflow | case-insensitive `prompt is too long` matches |
| overflow.error.anthropic-request-too-large | overflow.ts:39 | overflow_error_pattern_rows | positive | S03-overflow | case-insensitive `request_too_large` matches |
| overflow.error.bedrock-input-too-long | overflow.ts:40 | overflow_error_pattern_rows | positive | S03-overflow | case-insensitive `input is too long for requested model` matches |
| overflow.error.openai-context-window | overflow.ts:41 | overflow_error_pattern_rows | positive | S03-overflow | case-insensitive `exceeds the context window` matches |
| overflow.error.openai-maximum-context-length | overflow.ts:42 | overflow_error_pattern_rows | positive | S03-overflow | optional `the ` and `models `/`model's ` prefixes plus comma-digit token and parenthesized maximum forms match |
| overflow.error.google-token-count | overflow.ts:43 | overflow_error_pattern_rows | positive | S03-overflow | `input token count` followed on one line by `exceeds the maximum` matches |
| overflow.error.xai-prompt-length | overflow.ts:44 | overflow_error_pattern_rows | positive | S03-overflow | `maximum prompt length is` plus ASCII digits matches |
| overflow.error.groq-reduce-messages | overflow.ts:45 | overflow_error_pattern_rows | positive | S03-overflow | `reduce the length of the messages` matches |
| overflow.error.openrouter-context-length | overflow.ts:46 | overflow_error_pattern_rows | positive | S03-overflow | `maximum context length is` plus ASCII digits and `tokens` matches |
| overflow.error.poolside-input-length | overflow.ts:47 | overflow_error_pattern_rows | positive | S03-overflow | optional `the `, comma digits, and `token` with optional `s` match |
| overflow.error.together-context-length | overflow.ts:48 | overflow_error_pattern_rows | positive | S03-overflow | both parenthesized ASCII token counts and `models`/`model's` forms match |
| overflow.error.copilot-token-limit | overflow.ts:49 | overflow_error_pattern_rows | positive | S03-overflow | `exceeds the limit of` plus ASCII digits matches |
| overflow.error.llama-context-size | overflow.ts:50 | overflow_error_pattern_rows | positive | S03-overflow | `exceeds the available context size` matches |
| overflow.error.lm-studio-context-length | overflow.ts:51 | overflow_error_pattern_rows | positive | S03-overflow | `greater than the context length` matches |
| overflow.error.minimax-context-window | overflow.ts:52 | overflow_error_pattern_rows | positive | S03-overflow | `context window exceeds limit` matches |
| overflow.error.kimi-model-token-limit | overflow.ts:53 | overflow_error_pattern_rows | positive | S03-overflow | `exceeded model token limit` matches |
| overflow.error.mistral-maximum-context | overflow.ts:54 | overflow_error_pattern_rows | positive | S03-overflow | `too large for model with` plus ASCII digits and `maximum context length` matches |
| overflow.error.ds4-configured-context | overflow.ts:55 | overflow_error_pattern_rows | positive | S03-overflow | both counts accept ASCII comma digits and `token` with optional `s` |
| overflow.error.zai-context-window | overflow.ts:56 | overflow_error_pattern_rows | positive | S03-overflow | `model_context_window_exceeded` matches |
| overflow.error.ollama-prompt-too-long | overflow.ts:57 | overflow_error_pattern_rows | positive | S03-overflow | `prompt too long; exceeded context length` accepts optional `max` |
| overflow.error.dashscope-input-range | overflow.ts:58 | overflow_error_pattern_rows | positive | S03-overflow | `range of input length should be` matches |
| overflow.error.generic-context-length | overflow.ts:59 | overflow_error_pattern_rows | positive | S03-overflow | each separator in `context length exceeded` independently accepts space or underscore |
| overflow.error.generic-too-many-tokens | overflow.ts:60 | overflow_error_pattern_rows | positive | S03-overflow | `too many tokens` matches |
| overflow.error.generic-token-limit | overflow.ts:61 | overflow_error_pattern_rows | positive | S03-overflow | `token limit exceeded` matches |
| overflow.error.cerebras-no-body | overflow.ts:62 | overflow_error_pattern_rows | boundary | S03-overflow | anchored 400 and 413 with optional `status code`/whitespace before `(no body)` are true; the same 429 form is overflow false and retry true |
| overflow.exclusion.bedrock-throttling-service | overflow.ts:75 | overflow_exclusion_rows | negative | S03-overflow | both line-start service prefixes beat an overlapping token marker |
| overflow.exclusion.rate-limit | overflow.ts:76 | overflow_exclusion_rows | negative | S03-overflow | rate limit beats an overlapping overflow marker |
| overflow.exclusion.too-many-requests | overflow.ts:77 | overflow_exclusion_rows | negative | S03-overflow | too-many-requests beats an overlapping token marker |
| overflow.structure.stop-reason | overflow.ts:134 | overflow_structural_negative_rows | negative | S03-overflow | every non-error reason with a marker is false absent a usage signal |
| overflow.structure.message-absent | overflow.ts:136 | overflow_structural_negative_rows | negative | S03-overflow | Error without message is false |
| overflow.structure.message-empty | overflow.ts:136 | overflow_structural_negative_rows | negative | S03-overflow | Error with empty message is false |
| overflow.structure.unmatched-message | overflow.ts:139 | overflow_structural_negative_rows | negative | S03-overflow | unknown nonempty error is false |
| overflow.silent.input-over | overflow.ts:145-148 | silent_overflow_rows | positive | S03-overflow | Stop input strictly over context is true |
| overflow.silent.cache-read-contributes | overflow.ts:146 | silent_overflow_rows | boundary | S03-overflow | only input and cache-read contribute |
| overflow.silent.strict-threshold | overflow.ts:147 | silent_overflow_rows | boundary | S03-overflow | equal and below are false |
| overflow.silent.context-and-reason-gate | overflow.ts:145 | silent_overflow_rows | negative | S03-overflow | absent/zero context and non-Stop reasons do not enter the branch |
| overflow.length.above-99-percent | overflow.ts:155-158 | length_overflow_rows | positive | S03-overflow | zero-output Length above 99 percent is true |
| overflow.length.exact-99-percent | overflow.ts:157 | length_overflow_rows | positive | S03-overflow | exact 99 percent is true |
| overflow.length.below-99-percent | overflow.ts:157 | length_overflow_rows | negative | S03-overflow | below 99 percent is false |
| overflow.length.nonzero-output | overflow.ts:155 | length_overflow_rows | negative | S03-overflow | nonzero output is false |
| overflow.length.context-and-reason-gate | overflow.ts:155 | length_overflow_rows | negative | S03-overflow | absent/zero context and non-Length reasons do not enter the branch |
| overflow.recoverable.boundary-matrix | overflow.ts:171-172 | recoverable_length_rows | boundary | S03-overflow | below desired output is true; equal/above/zero desired and non-Length are false |
| overflow.precedence.retry-overlap | final-domain-runtime.md#failure-classifiers | overflow_retry_precedence_row | boundary | S03-overflow | both pure classifiers return true from their pinned definitions; caller precedence is not claimed by this S03 row |

get-overflow-patterns returns a fresh vector of opaque, non-Serde compiled
patterns with an is-match method. It preserves the public testing capability
without exposing regex crate types or making pattern objects part of the
extension protocol. The 25 pattern rows prove that projection.

### Exact 54 retry ids

Every row below fixes the fragment fields under the same no-invention rule.
`retry.ts` means
`packages/ai/src/utils/retry.ts@853a80d26c90a14c1886f0ebb8ffaae133ca2185`.

| Stable id | Source | Test binding | Polarity | Owner | Expected observation |
|---|---|---|---|---|---|
| retry.transient.overloaded | retry.ts:28 | retryable_marker_rows | positive | S03-retry | Error plus nonempty case-insensitive `overloaded` is true |
| retry.transient.rate-limit | retry.ts:29 | retryable_marker_rows | positive | S03-retry | `rate` plus zero or one BMP non-line-terminator (one legacy UTF-16 code unit) plus `limit` is true |
| retry.transient.too-many-requests | retry.ts:30 | retryable_marker_rows | positive | S03-retry | `too many requests` is true |
| retry.transient.http-429 | retry.ts:31 | retryable_marker_rows | positive | S03-retry | marker `429` is true, including the Cerebras no-body negative-overflow witness |
| retry.transient.http-500 | retry.ts:32 | retryable_marker_rows | positive | S03-retry | marker `500` is true |
| retry.transient.http-502 | retry.ts:33 | retryable_marker_rows | positive | S03-retry | marker `502` is true |
| retry.transient.http-503 | retry.ts:34 | retryable_marker_rows | positive | S03-retry | marker `503` is true |
| retry.transient.http-504 | retry.ts:35 | retryable_marker_rows | positive | S03-retry | marker `504` is true |
| retry.transient.http-524 | retry.ts:36 | retryable_marker_rows | positive | S03-retry | marker `524` is true |
| retry.transient.service-unavailable | retry.ts:37 | retryable_marker_rows | positive | S03-retry | the gap in `service unavailable` accepts zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.server-error | retry.ts:38 | retryable_marker_rows | positive | S03-retry | the gap in `server error` accepts zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.internal-error | retry.ts:39 | retryable_marker_rows | positive | S03-retry | the gap in `internal error` accepts zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.provider-returned-error | retry.ts:43 | retryable_marker_rows | positive | S03-retry | both gaps in `provider returned error` accept zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.request-buffer-limit | retry.ts:44 | retryable_marker_rows | positive | S03-retry | exact request-buffer exhaustion wording is true |
| retry.transient.network-error | retry.ts:49 | retryable_marker_rows | positive | S03-retry | the gap in `network error` accepts zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.connection-error | retry.ts:50 | retryable_marker_rows | positive | S03-retry | the gap in `connection error` accepts zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.connection-refused | retry.ts:51 | retryable_marker_rows | positive | S03-retry | the gap in `connection refused` accepts zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.connection-lost | retry.ts:52 | retryable_marker_rows | positive | S03-retry | the gap in `connection lost` accepts zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.other-side-closed | retry.ts:53 | retryable_marker_rows | positive | S03-retry | `other side closed` is true |
| retry.transient.fetch-failed | retry.ts:54 | retryable_marker_rows | positive | S03-retry | `fetch failed` is true |
| retry.transient.dns-getaddrinfo | retry.ts:55 | retryable_marker_rows | positive | S03-retry | `getaddrinfo` is true |
| retry.transient.dns-enotfound | retry.ts:56 | retryable_marker_rows | positive | S03-retry | `ENOTFOUND` is true case-insensitively |
| retry.transient.dns-eai-again | retry.ts:57 | retryable_marker_rows | positive | S03-retry | `EAI_AGAIN` is true case-insensitively |
| retry.transient.upstream-connect | retry.ts:58 | retryable_marker_rows | positive | S03-retry | the gap in `upstream connect` accepts zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.reset-before-headers | retry.ts:59 | retryable_marker_rows | positive | S03-retry | `reset before headers` is true |
| retry.transient.socket-hang-up | retry.ts:60 | retryable_marker_rows | positive | S03-retry | `socket hang up` is true |
| retry.transient.socket-connection-closed | retry.ts:61 | retryable_marker_rows | positive | S03-retry | `socket connection was closed` is true |
| retry.transient.timed-out | retry.ts:62 | retryable_marker_rows | positive | S03-retry | both `time out` and `timed out` are true |
| retry.transient.timeout | retry.ts:63 | retryable_marker_rows | positive | S03-retry | `timeout` is true |
| retry.transient.terminated | retry.ts:64 | retryable_marker_rows | positive | S03-retry | `terminated` is true |
| retry.transient.websocket-closed | retry.ts:67 | retryable_marker_rows | positive | S03-retry | the gap in `websocket closed` accepts zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.websocket-error | retry.ts:68 | retryable_marker_rows | positive | S03-retry | the gap in `websocket error` accepts zero or one BMP non-line-terminator UTF-16 code unit |
| retry.transient.ended-without | retry.ts:73 | retryable_marker_rows | positive | S03-retry | `ended without` is true |
| retry.transient.anthropic-message-stop | retry.ts:74 | retryable_marker_rows | positive | S03-retry | `stream ended before message_stop` is true |
| retry.transient.responses-terminal-event | retry.ts:75 | retryable_marker_rows | positive | S03-retry | `stream ended before a terminal response event` is true |
| retry.transient.http2-no-response | retry.ts:76 | retryable_marker_rows | positive | S03-retry | `http2 request did not get a response` is true |
| retry.transient.retry-delay | retry.ts:80 | retryable_marker_rows | positive | S03-retry | `retry delay` is true |
| retry.transient.guidance-you-can-retry | retry.ts:84 | retryable_marker_rows | positive | S03-retry | `you can retry your request` is true |
| retry.transient.guidance-try-again | retry.ts:85 | retryable_marker_rows | positive | S03-retry | `try your request again` is true |
| retry.transient.guidance-please-retry | retry.ts:86 | retryable_marker_rows | positive | S03-retry | `please retry your request` is true |
| retry.transient.grpc-resource-exhausted | retry.ts:89 | retryable_marker_rows | positive | S03-retry | `ResourceExhausted` is true case-insensitively |
| retry.nonretryable.opencode-go-limit | retry.ts:10 | non_retryable_precedence_rows | negative | S03-retry | `GoUsageLimitError` plus a transient marker remains false |
| retry.nonretryable.opencode-free-limit | retry.ts:11 | non_retryable_precedence_rows | negative | S03-retry | `FreeUsageLimitError` plus a transient marker remains false |
| retry.nonretryable.monthly-usage-limit | retry.ts:15 | non_retryable_precedence_rows | negative | S03-retry | `Monthly usage limit reached` plus a transient marker remains false |
| retry.nonretryable.available-balance | retry.ts:16 | non_retryable_precedence_rows | negative | S03-retry | `available balance` plus a transient marker remains false |
| retry.nonretryable.insufficient-quota | retry.ts:20 | non_retryable_precedence_rows | negative | S03-retry | `insufficient_quota` plus a transient marker remains false |
| retry.nonretryable.out-of-budget | retry.ts:21 | non_retryable_precedence_rows | negative | S03-retry | `out of budget` plus a transient marker remains false |
| retry.nonretryable.quota-exceeded | retry.ts:22 | non_retryable_precedence_rows | negative | S03-retry | `quota exceeded` plus a transient marker remains false |
| retry.nonretryable.billing | retry.ts:23 | non_retryable_precedence_rows | negative | S03-retry | `billing` plus a transient marker remains false |
| retry.structure.non-error-reasons | retry.ts:224 | retry_structural_negative_rows | negative | S03-retry | all six non-error reasons remain false |
| retry.structure.message-absent | retry.ts:224 | retry_structural_negative_rows | negative | S03-retry | Error without a message is false |
| retry.structure.message-empty | retry.ts:224 | retry_structural_negative_rows | negative | S03-retry | Error with an empty message is false |
| retry.structure.unmatched-message | retry.ts:227 | retry_structural_negative_rows | negative | S03-retry | unknown nonempty message is false |
| retry.structure.whitespace-message | retry.ts:224-227 | retry_structural_negative_rows | negative | S03-retry | whitespace is nonempty but unmatched and false |

## Provider composition and model helpers

### Public API

~~~rust
pub struct ProviderModelsFetchContext<'a> {
    pub credential: Option<&'a Credential>,
    pub stored: Option<&'a ModelsStoreEntry>,
    pub force: Option<bool>,
    pub cancellation: &'a CancellationToken,
}

pub trait ProviderModelsFetcher: Send + Sync {
    fn fetch_models<'a>(
        &'a self,
        context: ProviderModelsFetchContext<'a>,
    ) -> BoxFuture<'a, Result<Vec<Model>, ModelsError>>;
}

pub trait ProviderModelFilter: Send + Sync {
    fn filter_models(
        &self,
        models: &[Model],
        credential: Option<&Credential>,
    ) -> Vec<Model>;
}

pub struct DynamicProviderModels {
    pub fetcher: Arc<dyn ProviderModelsFetcher>,
    pub clock: Arc<dyn Clock>,
}

pub struct CreateProviderOptions {
    pub id: ProviderId,
    pub name: Option<String>,
    pub auth: ProviderAuth,
    pub models: Vec<Model>,
    pub dynamic_models: Option<DynamicProviderModels>,
    pub model_filter: Option<Arc<dyn ProviderModelFilter>>,
    pub adapters: Vec<Arc<dyn ApiAdapter>>,
}

#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum CreateProviderError {
    DuplicateAdapter(ApiId),
}

impl CreateProviderError {
    pub fn duplicate_api(&self) -> Option<&ApiId>;
}

pub fn create_provider(
    options: CreateProviderOptions,
) -> Result<Arc<dyn Provider>, CreateProviderError>;

pub fn has_api(model: &Model, api: &ApiId) -> bool;
pub fn calculate_cost<'a>(
    model: &Model,
    usage: &'a mut Usage,
) -> Result<&'a UsageCost, NumericError>;
pub fn get_supported_thinking_levels(
    model: &Model,
) -> Vec<ModelThinkingLevel>;
pub fn clamp_thinking_level(
    model: &Model,
    level: ModelThinkingLevel,
) -> ModelThinkingLevel;
pub fn models_are_equal(a: Option<&Model>, b: Option<&Model>) -> bool;
~~~

Construction consumes all options. Options, dynamic configuration, fetch
context, and the private provider are non-Clone and non-Serde. Manual Debug for
options/providers shows identities, counts, adapter ids, and capability
presence only. CreateProviderError uses fixed bounded Display and Debug text;
the duplicate id is available only through pattern matching or duplicate-api.
The returned provider is Send and Sync.

Name defaults to id. Baseline model values are not normalized or rejected for a
provider mismatch because pinned createProvider does neither. The registry
remains responsible for validation at its accepted boundaries.

Adapters are normalized into a private ApiId map by each adapter's declared
identity, never by vector position or a caller-supplied side key. Duplicate
declared identities fail construction with `DuplicateAdapter`; an absent
identity returns None at selection. Unlike the untyped upstream
single-stream object, one Rust adapter does not silently serve a different API;
this preserves the accepted ApiAdapter identity invariant and is a registered
compatibility divergence. Missing APIs return None and the accepted registry
creates its fixed in-band setup error.

Provider models are baseline-only. The registry is the sole owner of dynamic
memory. Cache restore filters only cached models whose provider equals the
provider id and publishes memory Set with persistence Keep. Superseded restore
stops. Offline or cancelled phases do not fetch. Network fetch receives the
full detached stored snapshot, credential, force, and cancellation. A
post-fetch cancellation stops publication. Final publication sets identical
memory and persisted model values with a checked injected-clock timestamp.
After fetch and the post-fetch cancellation check, the clock value must convert
to `SafeUnsigned` before `publish` is called. An over-bound clock returns
`ModelsErrorCode::ModelSource` and submits neither persistence nor dynamic-memory
intent for the fetched candidate. `ModelSource` is the existing category for a
catalog-source result that cannot be materialized; `ModelValidation` would
misclassify valid fetched models, while `Provider` is reserved for provider
selection/capability failure. An earlier successful cache restore is not rolled
back.
Fetched values are not provider-filtered, matching the pin.

Provider base URL and raw headers are absent from CreateProviderOptions. Pinned
createProvider merely copies them as inert provider-object metadata. Rust puts
public endpoints on Model, transient credential endpoints/headers in auth, and
transport middleware in constructed adapters. No builder Debug or error can
expose endpoint/header bytes.

Cost selection uses input plus cache-read plus cache-write and the highest
exclusive matching threshold; first equal threshold wins. One-hour cache
writes use twice the selected input rate and may make the short component
negative. All five finite results are built before usage.cost is replaced.

Thinking order is Off, Minimal, Low, Medium, High, ExtraHigh, Max. A
non-reasoning model returns Off. Missing base mappings are supported; explicit
null disables; extended levels are opt-in. Clamping searches upward before
downward, then returns Off for an empty set. Model equality requires two present
values and compares provider plus id only.

### Exact 20-row inventory

| Stable id | Test binding | Source | Expected observation |
|---|---|---|---|
| helper.provider-builder.identity-baseline | provider_builder_preserves_identity_defaults_and_baseline_values | models.ts:763 | name defaults and baseline/auth stay detached and unnormalized |
| helper.provider-builder.overlay-by-id | provider_builder_overlay_replaces_first_id_and_appends | models.ts:769 | registry-owned overlay replaces first id or appends with stable order |
| helper.provider-builder.static-capability | provider_builder_fetcher_controls_refresher_presence | models.ts:801 | only dynamic options expose a refresher |
| helper.provider-builder.cache-restore | provider_builder_restores_only_owned_cached_models | models.ts:805 | present cache filters provider; empty clears; absent keeps |
| helper.provider-builder.restore-superseded | provider_builder_stops_when_restore_is_superseded | models.ts:808 | superseded restore prevents fetch and final publish |
| helper.provider-builder.network-publication | provider_builder_fetches_and_publishes_dynamic_catalog | models.ts:820 | full context and deterministic checked-at publish; an over-SafeUnsigned clock returns ModelSource before submitting either final intent |
| helper.provider-builder.network-stop-matrix | provider_builder_network_stop_matrix | models.ts:817 | offline, cancellation, fetch, clock overflow, and publish outcomes differ exactly |
| helper.provider-builder.adapter-selection | provider_builder_selects_declared_api_adapters | upstream-map.toml:991 | adapters self-index by declared ApiId, a duplicate fails construction, and a missing id returns None |
| helper.provider-builder.filter-forwarding | provider_builder_forwards_filter_without_mutating_inputs | models.ts:828 | detached models and exact optional credential reach filter |
| helper.has-api.exact | has_api_is_exact_and_case_sensitive | models.ts:875 | exact open API equality only |
| helper.cost.threshold-exclusive | calculate_cost_uses_exclusive_request_wide_threshold | models.ts:883 | threshold itself stays base; one above tiers whole request |
| helper.cost.highest-tier | calculate_cost_selects_highest_matching_tier | models.ts:884 | unsorted highest threshold and first duplicate win |
| helper.cost.cache-write-split | calculate_cost_prices_short_and_one_hour_cache_writes | models.ts:890 | exact short/long formula including excess long write |
| helper.cost.mutation | calculate_cost_overwrites_only_usage_cost | models.ts:892 | cost replaced; token fields unchanged |
| helper.cost.finite-atomic | calculate_cost_rejects_nonfinite_results_atomically | models.ts:896 | overflow errors leave old cost unchanged |
| helper.thinking.nonreasoning | supported_thinking_levels_for_nonreasoning_are_off_only | models.ts:903 | map is ignored and only Off returns |
| helper.thinking.mapping | supported_thinking_levels_apply_null_defaults_and_extended_opt_in | models.ts:906 | null/default/extended rules and fixed order |
| helper.thinking.clamp-pass | clamp_thinking_level_preserves_supported_requests | models.ts:917 | every supported request passes through |
| helper.thinking.clamp-search | clamp_thinking_level_searches_up_then_down | models.ts:923 | upward winner, downward fallback, Max hole, empty Off |
| helper.model-equality | models_are_equal_compares_only_present_provider_and_id | models.ts:942 | absence and provider/id matrix, all other fields ignored |

## Portable utilities

### Public API

All token results use SafeUnsigned. Arithmetic overflow returns NumericError;
no public usize or saturating undercount crosses the boundary.

~~~rust
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContextUsageEstimate {
    pub tokens: SafeUnsigned,
    pub usage_tokens: SafeUnsigned,
    pub trailing_tokens: SafeUnsigned,
    pub last_usage_index: Option<SafeUnsigned>,
}

pub enum TextAndImageContentRef<'a> {
    Text(&'a str),
    Blocks(&'a [InputContent]),
}

pub enum ContextEstimateInput<'a> {
    Context(&'a Context),
    Messages(&'a [Message]),
}

pub fn calculate_context_tokens(
    usage: &Usage,
) -> Result<SafeUnsigned, NumericError>;
pub fn estimate_text_tokens(
    text: &str,
) -> Result<SafeUnsigned, NumericError>;
pub fn estimate_text_and_image_content_tokens(
    content: TextAndImageContentRef<'_>,
) -> Result<SafeUnsigned, NumericError>;
pub fn estimate_message_tokens(
    message: &Message,
) -> Result<SafeUnsigned, NumericError>;
pub fn estimate_context_tokens(
    input: ContextEstimateInput<'_>,
) -> Result<ContextUsageEstimate, NumericError>;
~~~

String length is JavaScript UTF-16 code units, four per estimated token. Each
image is 4,800 code units. Nonzero total-tokens overrides components; zero
falls back to their sum. A valid assistant checkpoint is non-error,
non-aborted, nonzero usage and not older than a later preceding message
timestamp. Equal timestamps remain valid. Without a checkpoint, system and all
tool definitions are counted. After one, only trailing messages and definitions
named by later added-tool markers are counted. Tool JSON length uses
deterministic key order and ryu-js ECMAScript number formatting.

~~~rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JsonParseErrorCode {
    InvalidJson,
}

pub struct JsonParseError { /* private code */ }

impl JsonParseError {
    pub const fn code(&self) -> JsonParseErrorCode;
}

pub fn repair_json(json: &str) -> String;
pub fn parse_json_with_repair<T: serde::de::DeserializeOwned>(
    json: &str,
) -> Result<T, JsonParseError>;
pub fn parse_streaming_json(partial_json: Option<&str>) -> JsonValue;
~~~

Repair first preserves valid escapes, escapes raw string controls, and doubles
invalid or trailing backslashes. Complete parsing tries original then repaired
input. Error formatting contains no raw input. Streaming parsing tries complete
repaired parsing, raw partial parsing, repaired partial parsing, then an empty
object.

parse-streaming-json checks its 256-KiB byte limit before every complete or
partial attempt. At or below the limit it uses the documented fallback order;
above the limit even otherwise-complete JSON returns an empty object. The local
partial parser is depth-capped at 128, linear-progress, duplicate-key
rejecting, and finite/safe-number only. It recovers partial strings,
collections, booleans, null, and valid partial numbers. Like pinned
partial-json 0.1.7, a completed collection member remains in the recovered
prefix when a following token is malformed, and a missing comma may separate
the next recoverable member. Upstream partial NaN
and infinity are intentionally rejected by the accepted S02 JSON invariant.
Exact limit and limit-plus-one witnesses cover complete and incomplete input.

~~~rust
pub enum ContentTextInput<'a> {
    Text(&'a str),
    Input(&'a [InputContent]),
    Assistant(&'a [AssistantContent]),
}

pub fn content_text(content: ContentTextInput<'_>) -> String;
pub fn content_text_with_separator(
    content: ContentTextInput<'_>,
    separator: &str,
) -> String;
pub fn short_hash(text: &str) -> String;
~~~

Text helpers retain only text blocks in source order. short-hash iterates UTF-16
code units, reproduces 32-bit Math.imul and unsigned shifts, and emits lowercase
base-36 h2 then h1.

~~~rust
pub trait UuidV7Entropy: Send + Sync {
    fn random_bytes(&self) -> Result<[u8; 16], UuidV7EntropyError>;
}

pub struct UuidV7EntropyError { /* fixed text and redacted source */ }

impl UuidV7EntropyError {
    pub const fn new() -> Self;
    pub fn with_source(
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum UuidV7ErrorCode {
    Entropy,
    TimestampOutOfRange,
    TimestampExhausted,
}

pub struct UuidV7Error { /* code and redacted source */ }
pub struct UuidV7Generator { /* process-local clock, entropy, state */ }

impl UuidV7Error {
    pub const fn code(&self) -> UuidV7ErrorCode;
}

impl UuidV7Generator {
    pub fn new(
        clock: Arc<dyn Clock>,
        entropy: Arc<dyn UuidV7Entropy>,
    ) -> Self;
    pub fn generate(&self) -> Result<String, UuidV7Error>;
}
~~~

UuidV7EntropyError and UuidV7Error implement std::error::Error. with-source
retains the supplied error only through Error::source; Display and Debug are
fixed and redacted. Entropy maps to the Entropy code without copying source
text.

Generation obtains 16 entropy bytes, reads the clock, then locks only the
timestamp/sequence allocation. No host capability runs under the lock. A newer
clock seeds the 32-bit sequence from entropy bytes 6-9. Equal/rollback clocks
increment it. Sequence wrap advances synthetic time. RFC 9562 48-bit overflow
is typed and leaves state unchanged. Formatting is lowercase and monotonic in
mutex allocation order. There is no free zero-argument uuid function or hidden
global authority.

~~~rust
pub trait ToolNameNormalizer {
    fn normalize_tool_name(&self, name: &ToolName) -> ToolName;
}

pub struct DeferredTool {
    pub normalized_name: ToolName,
    pub tool: Tool,
}

pub struct DeferredToolsSplit {
    pub immediate: Vec<Tool>,
    pub deferred: Vec<DeferredTool>,
}

pub fn split_deferred_tools(
    context: &Context,
    enabled: bool,
    normalizer: Option<&dyn ToolNameNormalizer>,
) -> DeferredToolsSplit;
~~~

Normalization applies to definitions, calls, and added-tool markers. Duplicate
normalized definitions retain the first slot and last value. Disabled mode
still normalizes and deduplicates. A marker defers a known tool only when no
earlier call used that normalized name; a later use does not undo an already
recorded deferred marker. The borrowed normalizer is not stored or serialized.

### Exact 27-row inventory

| Stable id | Test binding | Pinned source | Expected observation |
|---|---|---|---|
| helper.context.usage-total-fallback | context_usage_prefers_nonzero_total_and_falls_back_to_components | estimate.ts:17 | nonzero total wins; zero sums four components |
| helper.context.utf16-text-image | text_and_image_estimates_use_utf16_units_and_fixed_image_cost | estimate.ts:29 | UTF-16 ceil-by-four and 4,800 image units |
| helper.context.message-roles | message_estimates_cover_each_role_and_assistant_block | estimate.ts:45 | exact user/tool/assistant block accounting |
| helper.context.no-checkpoint-prefix | context_without_checkpoint_adds_system_tools_and_all_messages | estimate.ts:134-135 | all messages, system, and tools count |
| helper.context.checkpoint-trailing | context_checkpoint_reuses_usage_and_estimates_only_trailing_content | estimate.ts:89 | usage plus trailing estimate |
| helper.context.invalid-checkpoint-matrix | stale_failed_aborted_and_zero_usage_are_not_checkpoints | estimate.ts:63 | all invalid checkpoint dimensions are excluded |
| helper.context.newer-checkpoint | newer_assistant_usage_restores_checkpoint | context-estimate.test.ts:63 | later valid response restores checkpoint |
| helper.context.added-tools-after-checkpoint | post_checkpoint_markers_add_only_named_available_tool_definitions | estimate.ts:119 | only named later available tools count |
| helper.json.repair-valid | repair_json_preserves_valid_json_and_escape_sequences | json-parse.ts:32 | valid JSON remains byte-equal |
| helper.json.repair-controls | repair_json_escapes_only_raw_string_control_characters | json-parse.ts:79 | controls inside strings repair; outside remain |
| helper.json.repair-invalid-escapes | repair_json_doubles_invalid_and_trailing_backslashes | json-parse.ts:53 | invalid/incomplete/trailing escapes repair |
| helper.json.parse-repair-order | parse_json_uses_original_then_repaired_input_without_mutation | json-parse.ts:85 | original precedes repaired parse |
| helper.json.parse-error-redaction | invalid_json_returns_typed_bounded_error_without_input | json-parse.ts:88 | error has no input sentinel |
| helper.json.streaming-fallback | streaming_json_empty_null_and_malformed_fallback_matrix | json-parse.ts:104 | empty/malformed become object; complete null remains null |
| helper.json.streaming-partial | streaming_json_recovers_partial_collections_atoms_and_invalid_escapes | json-parse.ts:112 | bounded raw/repaired order with pinned completed-prefix recovery |
| helper.text.string-separator | content_text_preserves_strings_and_supports_default_and_custom_separators | text.ts:6 | string pass-through and newline/custom join |
| helper.text.block-filtering | content_text_selects_only_text_blocks_from_input_and_assistant_content | text.ts:8 | non-text blocks are ignored |
| helper.hash.wrapping-base36 | short_hash_matches_ascii_wrapping_and_base36_vectors | hash.ts:2 | exact wrapping/base36 vectors |
| helper.hash.utf16 | short_hash_hashes_utf16_code_units_for_non_bmp_text | hash.ts:5 | non-BMP input hashes surrogate units |
| helper.uuid.rfc-layout | uuid_v7_matches_rfc_layout_version_variant_and_lowercase_vector | uuid.test.ts:16 | exact layout/version/variant/vector |
| helper.uuid.sequence-rollover | same_millisecond_sequence_rollover_advances_synthetic_timestamp | uuid.ts:25 | sequence wrap advances time |
| helper.uuid.clock-rollback | clock_rollback_preserves_lexicographic_monotonicity | uuid.ts:23 | rollback increments sequence |
| helper.uuid.instance-concurrency-error | instances_are_explicit_and_concurrent_generation_is_unique_and_bounded | domain.md#uuidv7 | object safety, concurrency, entropy and clock errors |
| helper.deferred-tools.disabled-dedup-order | disabled_split_deduplicates_last_value_in_first_source_slot | deferred-tools.ts:13 | disabled still deduplicates with first slot/last value |
| helper.deferred-tools.marker-use-order | markers_defer_only_tools_not_used_before_their_load_point | deferred-tools.ts:17 | prior use prevents deferral; unknown marker ignored |
| helper.deferred-tools.normalization-collision | normalizer_applies_consistently_and_collisions_keep_first_slot_last_value | deferred-tools.ts:14 | all names normalize; collision order is stable |
| helper.deferred-tools.empty-unknown-nonmutation | split_empty_unknown_and_nonmutation_matrix | domain.md#deferred-tools | empty/unknown cases and source immutability |

## Session resources

### Public API

~~~rust
pub trait SessionResource: Send + Sync {
    fn cleanup(
        &self,
        session_id: Option<&str>,
    ) -> Result<(), SessionResourceError>;
}

pub struct SessionResourceError { /* fixed text and redacted source */ }

impl SessionResourceError {
    pub const fn new() -> Self;
    pub fn with_source(
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SessionResourceRegistryErrorCode {
    TokenExhausted,
    CleanupInProgress,
    CleanupFailed,
}

pub struct SessionResourceRegistryError {
    /* code plus ordered errors */
}

impl SessionResourceRegistryError {
    pub fn code(&self) -> SessionResourceRegistryErrorCode;
    pub fn failures(&self) -> &[SessionResourceError];
}

pub struct SessionResourceRegistry { /* private Arc-owned state */ }

impl SessionResourceRegistry {
    pub fn new() -> Self;
    pub fn register(
        &self,
        resource: Arc<dyn SessionResource>,
    ) -> Result<SessionResourceHandle, SessionResourceRegistryError>;
    pub fn cleanup(
        &self,
        session_id: Option<&str>,
    ) -> Result<(), SessionResourceRegistryError>;
}

pub struct SessionResourceHandle { /* weak owner plus one token */ }

impl SessionResourceHandle {
    pub fn remove(&mut self) -> bool;
}
~~~

SessionResourceError and SessionResourceRegistryError implement
std::error::Error. with-source retains a resource cause only through
Error::source; all ordinary Display and Debug text is fixed and redacted.
The exact formatting contract is:

| Value | Display | Debug shape |
|---|---|---|
| SessionResourceError | `session resource cleanup failed` | `SessionResourceError { source_present: <bool> }` |
| TokenExhausted | `session resource token exhausted` | `SessionResourceRegistryError { code: TokenExhausted, failure_count: 0 }` |
| CleanupInProgress | `session resource cleanup in progress` | `SessionResourceRegistryError { code: CleanupInProgress, failure_count: 0 }` |
| CleanupFailed | `session resource cleanup failed` | `SessionResourceRegistryError { code: CleanupFailed, failure_count: <bounded count> }` |

No source Display/Debug text is delegated through either wrapper.

The registry and handle are non-Serde; the handle is non-Clone. Tokens are
checked monotonic u128 values and registrations preserve insertion order.

Cleanup marks one pass active, takes a detached ordered snapshot, releases the
lock, then invokes every snapshot member before disposing any panic payload or
final resource owner. Nested or concurrent cleanup returns CleanupInProgress.
Registration/removal during callbacks affects only later passes; a removed
snapshot member is still invoked once. Success removes the same token if still
active. Failure retains it only if it is still active. Explicit removal
therefore wins over failure without reinsertion.

A callback-body panic is caught as one sanitized failure in that callback's
snapshot position. After every callback has run, state finalization moves
removed resources out of the lock. Disposal then has one fixed sequence while
cleanup-in-progress remains true: callback panic payloads in snapshot order,
then detached snapshot/resource owners in snapshot order. Each item has its own
catch-unwind guard. The returned failure list contains callback Err/panic
failures in snapshot order followed by destructor-panic failures in that fixed
disposal order. Reentrant destructors therefore observe CleanupInProgress. A
destructor panic cannot skip another callback or resource drop; a second panic
payload whose own destructor panics is intentionally forgotten to avoid a
double-panic abort. Only after these detached disposals finish does the registry
clear cleanup-in-progress. Error sources returned to the caller are likewise
never dropped under the registry lock. Tests cover reentrant final-resource and
panic-payload destructors plus a panicking panic-payload destructor.

This intentionally makes successful cleanup one-shot. Failed active resources
remain for retry. Foreign peers never become SessionResource trait objects and
never register in S03. S12 `resource.v1` only lists and reads resource values;
it currently has no session-cleanup lifecycle. That missing foreign projection
is a recorded future compatibility gap, not an implicit lifecycle-message
contract.

S04 exposes the transport resource object for session-scoped adapter state.
S05 registers that object and retains its non-clone SessionResourceHandle for
the provider-composition lifetime. S15 owns the registry instance and invokes
cleanup during coding-session disposal. Dropping S05's handle unregisters only
that token; it does not invoke cleanup.

### Exact 12-row inventory

| Stable id | Test binding | Source | Expected observation |
|---|---|---|---|
| session-resource.registration.duplicate-object | duplicate_object_registrations_receive_independent_tokens | upstream-map.toml:849 | same Arc receives two independent tokens |
| session-resource.cleanup.insertion-order | cleanup_uses_insertion_ordered_snapshot | upstream-map.toml:849 | exact snapshot order and successful removal |
| session-resource.cleanup.session-id-none | cleanup_forwards_absent_session_id | session-resources.ts:1 | callback observes None |
| session-resource.cleanup.session-id-exact | cleanup_forwards_exact_session_id | session-resources.ts:16 | session id bytes remain exact |
| session-resource.handle.explicit-removal | explicit_removal_is_idempotent | final-domain-runtime.md#session-resources | first removal wins; second is false |
| session-resource.handle.drop | dropping_unique_handle_unregisters_exactly_one_token | domain.md#session-resources | drop removes only its token |
| session-resource.cleanup.registration-next-pass | registration_during_cleanup_waits_for_next_pass | domain.md#session-resources | new token is absent from current snapshot |
| session-resource.cleanup.removal-next-pass | removal_during_cleanup_keeps_current_snapshot_but_skips_next_pass | domain.md#session-resources | removed member runs current snapshot only |
| session-resource.cleanup.reentrancy | nested_and_concurrent_cleanup_return_cleanup_in_progress | final-domain-runtime.md#session-resources | deterministic rejection and outer continuation |
| session-resource.cleanup.failure-continuation | cleanup_continues_after_error_and_panic | final-domain-runtime.md#session-resources | later resources run after Err/panic and failures follow the fixed callback/disposal order |
| session-resource.cleanup.ordered-aggregate | cleanup_aggregates_failures_in_order_and_retries_active_failures | upstream-map.toml:849 | ordered redacted aggregate and retained active failures only |
| session-resource.cleanup.removal-wins | removal_during_failing_callback_wins_without_reinsertion | domain.md#session-resources | failure reports but removed token stays absent |

## Dependency contract

S03 final domain work may add only these exact normal dependencies:

~~~toml
regex = { version = "=1.13.1", default-features = false, features = ["std"] }
jsonschema = { version = "=0.52.1", default-features = false }
ryu-js = { version = "=1.0.3", default-features = false }
~~~

regex owns pure private classifiers. jsonschema owns offline structural schema
evaluation. ryu-js owns ECMAScript finite-number formatting. ryu-js has no
default feature and no mandatory transitive dependency; its documented MSRV is
1.71. No additional direct partial-JSON, UUID, random, index-map,
async/runtime, HTTP, TLS, provider, or SDK dependency is admitted.

The lockfile and cargo tree must prove that jsonschema resolver, file, async,
macro, IDNA, Tokio, Reqwest, and Rustls features are absent. They must also
record jsonschema's mandatory uuid-simd and getrandom dependencies, default
ahash runtime-rng/getrandom, fancy-regex, and default regex contributions
separately from the three direct dependencies. uuid-simd is used for schema
format validation; random seeding is internal performance state, never UUID
entropy or observable ordering authority. Sorted validation issues remove its
semantic effect. Later workspace feature unification cannot enable retrieval:
validator construction uses offline, graph construction installs an explicit
fail-only retriever, and every non-local reference reached by the closed graph
fails.

## Symbol and facade closure

The final symbol inventory contains exactly all 699 pinned occurrences, not
only unique names. Root and compat re-exports remain distinct rows pointing at
one Rust implementation. Every row has one of:

~~~text
implemented
s02-dto
mapped-s04
mapped-s05
rust-nonapplicable
external-schema-projection
~~~

The inventory accounts for the 30 source-level projections in this contract,
the accepted DTO/domain surfaces, and every deferred
occurrence. At minimum these special cases are explicit:

- getOverflowPatterns: implemented as opaque compiled pattern values and proved
  by 25 pattern rows;
- StringEnum: Rust-nonapplicable TypeBox construction helper;
- Type, Static, and TSchema: external-schema-projection;
- diagnostic error-to-message helpers: Rust-nonapplicable under the redacted
  diagnostic boundary; callers construct sanitized diagnostic DTOs explicitly;
- RetryCallbacks and retryAssistantCall: mapped-S05;
- non-exported estimate, hash, and deferred-tool helpers: implemented supporting
  contracts, not invented upstream exports.

pi-ai exact-import tests prove facade access. Wildcard re-export is not by
itself completeness evidence.

## Deterministic aggregation

Unit fragments are:

~~~text
conformance/ai-domain/stream-inventory.json                    22
conformance/ai-models/auth-store-inventory.json                58
conformance/ai-models/registry-inventory.json                  30
conformance/ai-domain/tool-validation-inventory.json           30
conformance/ai-domain/message-transform-inventory.json         31
conformance/ai-domain/overflow-inventory.json                  43
conformance/ai-domain/retry-inventory.json                     54
conformance/ai-domain/models-helpers-inventory.json             20
conformance/ai-domain/portable-helpers-inventory.json           27
conformance/ai-domain/session-resources-inventory.json          12
                                                               ---
                                                               327
~~~

A checked-in generator reads fragments in the exact order above and writes
conformance/ai-domain/behavior-inventory.json as a flat 327-row document.
Validation rejects a wrong fragment count, duplicate stable id, missing
required field, wrong owner prefix, unexpected order, stale generated output,
or total other than 327. The existing stream inventory moves losslessly into
its fragment; auth and registry fragments remain their accepted unit evidence.

The same generator/checker reads the pinned AI export manifest and a
independently checked disposition table to write the 699-row symbol inventory.
The exact rule groups, canonical names, origin-line selector requirement, and
causal checks are frozen in export-inventory.md. The full workspace gate runs
it in check mode.

## Security and causal acceptance

In addition to the 217 rows:

- compile probes freeze every function signature and every stored dynamic trait
  object;
- compile-fail probes reject Serde for process-local policies, providers,
  validation reports, UUID ports/generator, resources, and handles;
- unique secret-bearing values, rejected tool-call ids, raw JSON/arguments,
  endpoints, headers, provider/resource/entropy sources, and panic payloads do
  not enter default error formatting, terminal messages, fixtures, or
  telemetry; intentionally public provider identities and bounded validation
  paths remain observable where their API declares them;
- schema cache collision/concurrency/poison/eviction/drop probes prove its
  bound and lock discipline; external-reference tests prove the fail-only
  retriever performs no external retrieval or I/O; reference-closure and
  pattern resource-limit probes are deterministic;
- classifier tests initialize every static RegexSet;
- UUID and cleanup concurrency use barriers/manual synchronization, never
  elapsed sleeps;
- one causal mutation per unit proves the named row fails;
- cargo tree proves only the three admitted direct dependencies, the recorded
  transitive graph, and the allowed feature set;
- final scripts/verify.sh passes from the assembled candidate.
