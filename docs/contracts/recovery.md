# First attachment and generation recovery

Authority: pinned upstream `853a80d26c90a14c1886f0ebb8ffaae133ca2185` and the accepted architecture.

## First attachment has an explicit concrete entrypoint

Keep AgentHarnessFactory::create and RuntimeFactory::create_configured strict restore
entrypoints. Add RuntimeFactory::create_first_attachment with the same options and
result/error types. No option field, frozen trait inventory, durable marker or format
changes. The explicit method carries caller intent; header version, empty history,
model registration or absence of a configuration never automatically proves first use.

Both routes share validated options, one backend runtime lease, immutable seed capture
and the normal configured restore/owner construction. Capture the same model provider/
model ID, thinking (default off) and active tool names (default initial descriptors)
once, outside locks. Existing configuration stays authoritative; constructor options
remain only the seed for a permitted initial main configuration and future new lanes.
No provider request, hook, prompt/context computation or ID allocation occurs at create.

First attachment may seed only an absent main LaneConfig. Require actual main LaneState
and LaneLeaf roots of the correct kind; a configuration-less active operation is corrupt.
Do not fabricate missing lane roots or seed other lanes. Existing Session repositories
already create an idle unconfigured main with leaf/state (repo/forks::lane_writes).
Read these roots under the existing stamp bracket. An existing config is never changed;
then use the same strict restore, including full semantic validation of all lanes.

Initialization is one checked conditional Session commit: LaneConfig Absent plus
observed main state/leaf sequence predicates, writing only that LaneConfig. NotApplied
reloads; a competing existing config wins without overwrite. Use the explicit options
telemetry context for the existing single write observation. Storage/codec failures
retain the existing HarnessRuntimeError channel and release the runtime lease. No
history/usage scan, new storage capability or durable no-op is introduced. Admitted
seed work retains its runtime lease independently of the initiating waiter until the
publication/construction reaches a definitive outcome; pre-admission drop writes nothing.

As pinned H2221 explicitly says, the seed commit precedes restore. Once that commit
applies, a later construction/restore failure does not roll it back. A subsequent
attempt observes the existing seed rather than replacing it. Missing/wrong basic roots,
a configuration-less active operation or invalid options reject before seeding.
The caller's explicit first-use assertion is the added Rust trust boundary; this
method cannot infer historical first attachment without an unavailable durable marker.

## Bounded recovery leaves usage conflicts to publication

Retain the configured restorer's bounded roots and named entry/register hydration;
do not scan history or the usage ledger to validate reserved usage IDs. Document this
as the selected narrower interpretation of the conflicting pinned requirements.
Only storage commit is authoritative for an already-used usage ID. A collision must
reject the complete response/usage/state transaction, preserving every existing row
and the still-pending operation; no preflight reservation or conflict repair is added.
Storage's internal transactional validation is not a Harness recovery ledger query.

## Unknown ordinary pending work can always be settled

Generation remains the durable counter and existing pure core owns classification,
reserved IDs, ordered writes and cleanup. The runtime supplies synthetic facts for
ordinary EffectPending work with no live owned effect when cancelled or at its captured
attempt cap. Unknown below-cap work retains the existing next-attempt transition;
current options never replace captured budgets/backoff/identity. A live effect remains
a live effect rather than being reclassified as unknown.

Construct an empty settled assistant using the captured provider/model, original
reserved response/usage IDs, the already captured PlannerInputs.now timestamp and
Usage::zero(). Pass that timestamp through the private planning call sites rather
than adding a clock callback while a mutation line is held. Use a matching already-
captured in-process binding's API when available; otherwise use ApiId unknown. Do not
query providers/models, project history or compute system/tool context merely to recover
this missing API. Do not use the reopened options model. No API field is added durably.
The same identity fallback closes a cancelled Assistant NotStarted settlement when
no matching in-process binding exists. An uncancelled at-cap unknown result is Error; cancelled pending work is Aborted even
below cap. Fixed bounded diagnostic text may explain the outcome; it is not payload.
Cancellation also precedes unnecessary identity lookup in Ready and RetryWait branches.

Use existing PlanningFacts::Generation.synthetic and ReconciledOutput through actual
commit_settlement. No fabricated live effect, provider request, before_request/payload/
after_response invocation or provider-request span accompanies synthetic settlement.
Existing before_resume/lifecycle/terminal behavior and run/step/write observation stay.
Synthetic failures do not retry past the captured cap. Malformed current state still
faults; genuine provider Aborted, Stop, Length and initial Deferred classification stay.
DeferredHandle already has API provenance and must not receive this ordinary fallback.

## Distinguishing obligations and local discretion

Actual public tests must distinguish explicit fresh initialization from strict missing-
config restore, preserve existing configuration and forked/normalized main leaf data,
show one conditional seed/no extra IDs, exercise competing config publication and
reject missing roots/active corruption without seeding. Repeat first attachment must
not overwrite; an applied seed survives later restore failure as specified above.

Recovery tests must cover cap/cancel/below-cap after reopening, captured budget despite
new options, absent or unusable provider registration, no synthetic external callbacks,
reserved identity/zero usage and durable manual/automatic agreement. A valid unrelated
usage record with a colliding reserved ID must allow bounded restore then reject actual
settlement atomically. Keep existing wrong-key, cancellation, admitted-commit and
telemetry guards. Small private helpers and test hosts remain implementer choices;
no dependency, schema, accepted assertion or later-slice change is authorized.
