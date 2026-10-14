# Shared OpenAI Responses evidence

Authority: pi-mono Git objects at
`853a80d26c90a14c1886f0ebb8ffaae133ca2185`, complete
`packages/ai/src/api/openai-responses-shared.ts`. Exact installed OpenAI 6.40.0
types were checked in `src/resources/responses/responses.ts`, including service
tiers, message phase, reasoning fields and optional tool-call item ids.

Most evidence is **source-confirmed** and independently transcribed.
`signature-bytes.json` is the separately identified executed pinned-processor
signature oracle, described below. `interleaved.json` supplies fourteen wire events,
ten complete progress snapshots (base identity/usage supplied independently by
the test fixture), final content and final usage. That matrix compares encoded
JSON signatures structurally; the dedicated signature oracle additionally checks
exact string bytes and nonalphabetic nested object order. Content
order, event order, index mapping, omission/null and numeric observations are not
normalized. No expected value is produced by the new Rust implementation.

The executed oracle ran on Node v26.8.1 using the exact extracted
encodeTextSignatureV1/processResponsesStream/mapStopReason bodies from the pinned
Git blob (SHA-256 `e86649ae0ccc20478df9919dc57e7bb3d8f2fc02e343cc1f87db16d8ca1ca2bb`).
Only calculateCost was replaced by a no-op because that run observes signatures,
not prices. It executes no SDK, auth discovery or provider call. Its literal output
is retained in `signature-bytes.json`; the reproduction script is
`signature-oracle.mjs`. Run it with Node from any directory to print the oracle.
The root pinned source path is read with git show, not from the upstream worktree.

Provider serde_json enables preserve_order (same pinned version 1.0.151, adding
indexmap 2.14.2 transitively). Event sources must parse raw wire JSON directly into
that ordered Value without first rebuilding it through core JsonObject/BTreeMap.
The EventSource ABI is unchanged. Text signatures use the core ordered
TextSignatureV1 struct (v/id/phase). Ordered Value alone is not JSON.stringify:
`api::wire_json::stringify_wire_json` recursively enumerates array-index keys
numerically before insertion-ordered non-index keys and converts all Number
variants to f64 before the existing pinned ryu-js 1.0.3 formatter renders them.
It normalizes negative zero and preserves JS string escaping over valid Rust
Unicode. Reasoning signatures, encryption backfill and function-argument strings
use this checked projection. No broad V8, nonfinite, BigInt, undefined, toJSON or
lone-surrogate representation is claimed. Existing core JsonObject canonical
ordering is unchanged. The full workspace gate checks
the feature-unified core profile with every existing S03 assertion intact.

`api::wire_json::project_wire_safe_unsigned` is the separate checked admission
boundary for slot indexes and usage counts. It accepts finite mathematical
nonnegative safe integers whether the ordered Value stores an integer or a float:
1, 1.0 and 1e0 are equivalent, and negative zero becomes zero. Fractional,
negative nonzero, nonfinite and above-safe values remain invalid. Missing index
remains distinct from zero, null index remains invalid, and missing/null usage
fields retain their source-specific defaults. Core durable SafeUnsigned decoding
is unchanged (including its deliberate rejection of a 1.0 spelling).

`numeric-wire.json` records executed pinned processor cases for decimal slots and
exponent/decimal/negative-zero usage. Inputs retain their raw JSON spellings.
Reproduce with `node numeric-wire-oracle.mjs --check`; it runs exact extracted
processor/stop-mapper/text-signature bodies with calculateCost stubbed and costs
excluded from observations. This is not an exhaustive raw IEEE-754 parser audit.

`wire-json.json` records executed native JSON.stringify oracle cases, including
nested numeric keys, integer Number variants outside the JS safe range, f64
spelling thresholds, negative zero, subnormals and Unicode escapes. Reproduce
the exact expected strings with `node wire-json-oracle.mjs --check`.

## Public integration contract

All exports are in `pi_ai_providers::api::openai_responses_shared`:

- `convert_responses_tools(&[Tool], &ConvertResponsesToolsOptions)` returns
  `Result<Vec<serde_json::Value>, ResponsesError>`.
- `convert_responses_messages(&Model, &Context, &BTreeSet<String>,
  &ConvertResponsesMessagesOptions<'_>)` returns that same wire-item result.
- `ResponsesEventSource::next(&mut self)` returns
  `BoxFuture<'_, Result<Option<serde_json::Value>, ResponsesError>>`.
- `process_responses_stream(&mut dyn ResponsesEventSource,
  &mut AssistantMessage, &AssistantStreamProducer, &Model,
  &OpenAIResponsesStreamOptions<'_>, &CancellationToken)` is asynchronous and
  returns `Result<(), ResponsesError>`. Output is an owned DTO borrowed mutably
  so partial state survives errors. The processor awaits one event at a time and
  races non-cooperative source waits against caller cancellation. The caller
  owns source/transport cleanup after return.

The wrapper owns the initial Start and final Done/Error. The processor never
publishes a terminal event. It publishes cancellation snapshots after content,
signature, response-id and usage updates, including updates without emitted
progress; inherited response_model/end_turn are preserved because the pinned
shared processor does not assign them. Snapshot identity must match the producer.

Tool strictness uses `Presence<bool>` (missing defaults false, explicit null
retained); the other optional flags remain `Option<bool>`. Grammar/deferred maps
are borrowed typed maps. `ResponsesServiceTierPolicy` supplies optional pricing
and a default nullish resolver. A resolver without pricing is never invoked;
pricing without a custom resolver uses response tier, otherwise request tier.
Both ports are borrowed trusted Rust capabilities, not Serde or foreign callbacks.

Pure message conversion uses injected `synthetic_result_timestamp` (default
zero) for S03 missing-tool-result repair. Timestamps never enter Responses wire
items; a test proves changing the injected timestamp leaves the wire identical.
No ambient time, HTTP behavior or provider registration is introduced.

## Live witnesses

`crates/pi-ai-providers/tests/openai_responses_shared.rs` binds these cases:

- `interleaved_slots_preserve_complete_progress_snapshots_and_terminal_backfill`:
  nonadjacent output slots 10/4/7 map to content indexes 0/1/2, missing/wrong slot
  deltas are ignored, provisional final-answer Stop survives progress, terminal
  usage/cost replaces it with ToolUse, and reasoning encryption is backfilled.
  Source processor and `openai-responses-terminal-event.test.ts`,
  `azure-openai-responses-reasoning-replay.test.ts`.
- `terminal_required_eof_statuses_and_redacted_provider_classification`: EOF
  requires a terminal; incomplete reasons retain distinct length/error outcomes
  and future protocol identifiers; raw provider error prose is classified then
  discarded. Source `openai-responses-terminal-event.test.ts`.
- `pending_reads_cancel_with_usage_and_backfilled_signatures_and_drop_the_loser`:
  a genuinely Pending next-event future is dropped when cancellation wins;
  the independently awaitable result retains usage-only terminal metadata and
  backfilled signatures. Accepted S04 cancellation snapshot contract.
- `custom_grammar_and_function_done_preserve_replacement_and_cleanup`: exact
  escaped custom input deltas, repeated custom close, ordinary function argument
  replacement without a false delta, and no scratch fields in persisted DTOs.
  Source `constrained-sampling.test.ts`,
  `openai-responses-partial-json-cleanup.test.ts` and namespace tests.
- `tools_preserve_default_false_explicit_null_unsupported_omission_and_grammar`:
  strict controls, omitted false defer_loading and custom Lark wire tools.
- `replay_signature_fallback_ids_multimodal_outputs_and_namespace_rules`:
  text-signature phase/fallback ids, joined tool text plus image blocks and
  different-model namespace/item-id removal. Source message-id, namespace and
  tool-result-images tests.
- `deferred_definitions_load_once_after_markers_and_tools_search_is_paired`:
  additional_tools and paired client search-output modes, marker deduplication,
  missing-definition exclusion and defer_loading. Source deferred-tools tests.
- `foreign_ids_use_pinned_hash_and_collisions_fail_without_changing_input`:
  foreign item hash, synthetic tool-result pairing and inherited S03 collision
  rejection. The shortHash("hello") expected vector was independently accepted
  in S03; the new implementation is not used as an oracle.
- `omitted_item_id_summary_separator_refusal_and_existing_encryption_follow_source`:
  optional SDK item id becomes the pinned literal `undefined` in the composite
  tool-call id; existing encrypted_content wins; summary separators/refusal and
  done-without-added slots follow source. Missing output_index is its own map key,
  distinct from zero, matching the pinned synthetic cleanup test.
- `malformed_protocol_values_fail_without_copying_free_form_diagnostics`:
  negative/unsafe indexes and usage, malformed status/reason and oversized wire
  data fail with closed errors, without copying diagnostic prose into output.
- The two tier-policy tests separately witness absent pricing, custom resolver,
  and default nullish resolution with pricing.
- `policy_failure_preserves_loaded_usage_and_cancellation_sees_the_prior_publication`:
  loaded usage survives a failing pricing hook; cancellation inside that hook
  retains the pre-hook usage snapshot. Source assigns usage before pricing.
- `opaque_signature_bytes_match_executed_pinned_processor_with_nested_wire_order`:
  exact thinking_end, text_end and backfilled signature bytes, including nested
  summary type/text order, are compared to executed pinned source output.
- `checked_wire_stringify_matches_executed_number_key_and_escape_oracles` and
  `reasoning_signature_and_encryption_backfill_apply_js_number_and_key_rules`:
  cover the independent numeric-key/number-spelling signature defect and the
  checked serializer's recorded JS oracle vectors.
- `unknown_status_and_rejected_reason_preserve_prior_accounting_and_pricing`:
  valid response id, usage and price adjustments survive terminal status/reason
  rejection, in output and a later cancellation snapshot. Unsafe rejected
  identifiers themselves are not published.
- `checked_wire_stringify_bounds_are_exact_and_errors_are_redacted` and
  `replayed_function_arguments_use_the_same_js_stringify_projection`: check
  output byte/depth bounds, immutable input, redacted errors and ordinary replay
  argument strings at the same JSON.stringify boundary.
- `wire_unsigned_checks_mathematical_values_without_changing_durable_decoders`
  and `numeric_slot_and_usage_spellings_match_executed_pinned_processor`:
  retain the rereview's two raw-number failures and safe-range/negative-zero
  witnesses, using executed pinned expectations for both processor paths.
- `invalid_wire_numbers_fail_at_both_slot_and_usage_boundaries` and
  `missing_null_usage_and_negative_zero_index_keep_their_distinct_semantics`:
  reject malformed counts/indexes while distinguishing missing slots, zero and
  null, and preserving omission/null usage behavior.

## Custody and limits

ResponsesError contains only a closed kind and independent retry/overflow flags.
Provider error messages are used transiently for those classifiers and never
copied to output or snapshots. Incomplete reasons are protocol identifiers, not
diagnostic prose: bounded alphanumeric/underscore/hyphen/dot/colon identifiers
(up to 256 UTF-8 bytes) retain their exact spelling and the pinned incomplete
prefix. Free-form or oversized reasons fail typed validation. Error rendering
uses safe category text, except the pinned incomplete template containing that
validated identifier. Failed/cancelled status errors receive a fixed nonempty
message so the outer wrapper can form a valid S03 terminal. Validation of terminal
status/reason occurs after safe reasoning backfill, response id, usage and pricing
observations, matching source finalization order rather than discarding valid
accounting because a later protocol status is unknown.

The event source/outer adapter owns actual request-aware credential sanitization;
the shared layer has no credential custody and cannot identify a secret from an
arbitrary valid protocol id. Model text, tool arguments and reasoning signatures
are intentional content/replay data, not generic diagnostic dumps.

Wire events are bounded to depth 128 and 1 MiB compact JSON before reduction;
ordinary accumulated tool JSON is bounded to the inherited 256 KiB parser size.
These are typed failures, not silent truncation. They do not claim to bound the
total assistant output over an arbitrarily long stream; the outer request/model
and consumer/transport limits own that lifetime. Core JSON values enforce safe
numeric projections; malformed wire numbers do not become unchecked DTO fields.

This unit supplies the six shared source exports. Standard Responses, Codex and
Azure HTTP wrappers, signing/auth, retries/timeouts, request hooks and cumulative
export proof bindings remain their separately assigned implementation work.
