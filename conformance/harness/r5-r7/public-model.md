# Bounded independent public S10 histories

Model689 drives actual exported RuntimeFactory/AgentLane, actual ModelsRegistry with a
capable deterministic adapter, and real MemorySessionRepo create/close/open. Driving
never writes fabricated operation state or queued payloads. The small reference tracks
publicly returned queue IDs, membership and consumed order; it does not implement core
phase recipes. Existing S09 model251 remains separate and unchanged.

Frozen finite case IDs (no selector; every final target run executes all cases):

- `R5-NEXT-auto`, `R5-NEXT-manual`: two nextRun inputs, cancel second, repeated cancel,
  prompt consumes first; no enqueue-triggered provider request.
- `R5-ABORT-{all,one}-{abort-first,consume-first}`: four histories, two steer inputs,
  one follow-up, one nextRun; cancel-before-consume or cancel-after-placement,
  first/repeated abort, retained drained payloads and terminal nextRun preservation.
- `R5-DRAIN-all`, `R5-DRAIN-one`: steer precedes follow-up, cancelled follow-up never
  places, lane nextRun stays pending and provider request count reflects selected mode.
- `R5-SKIP-reopen`: first one-at-a-time steer consumption is an actual completed
  transaction prefix; close/reopen that same repository, then resume a generation
  before the second steer input is consumed and complete the suffix.
- `R7-PENDING-{0,1,2}-{ready,abort}`: six histories; each resume can call actual fetch
  at most once, copied complete handle persists and source IDs advance; cancellation
  calls the same complete newest-persisted handle through a nonsignalled cancel token.
- `R7-PREFIX-direct`, `R7-PREFIX-reopen`: two comparable histories. The second physically
  closes/reopens its actual completed first-poll prefix; both then finish and compare
  exact publicly observable entries/registers/Usage observations/stats.
- `R7-UNKNOWN-reopen`: stop manually after durable poll intent before dispatch, reopen
  that same prefix, replace reservations with fresh response/usage IDs at the same
  poll, complete once, and assert abandoned IDs never materialize.

Total:18 finite histories in7 Rust test functions. Branches share prefixes; this is not
exhaustive scheduling, broad state coverage or replacement for owning R5–R7 tests.

Observation boundary: MemoryRepo does not expose its backend or ledger scanner. Tests
read complete public entry/register inventories and stats, correlate actual Usage event
rows, check pending/placed exclusivity at every deterministic pump/action boundary and
record definitive pi.session.write spans. These are committed-state observations, NOT
raw ConditionalCommit request/ordered-write logs. Root explicitly assigns exact ordered
transaction spy coverage to owning runtime687 tests. The public cancellation arguments
cannot distinguish equal handles across sources alone; this model jointly checks durable
source advancement and actual handle cancellation, while internal target-registration
race evidence belongs to owning runtime tests. No runtime history/ledger read is added.

Baseline must compile and fail on cdfd481 public stubs; that expected red is retained.
Final candidate imports are root-owned and assertions remain unchanged. Candidate identity,
commands, initial fixture compile issues, expected baseline-red and final results live
under E/model689. No source mutation or full-S10 acceptance is supplied by this test unit.
