# Public runtime R5–R7 ownership and finite coverage

The runtime consumes the pure inbox, control and deferred plans through the existing
Session publication boundary. Pending payload predicates remain part of the actual
checked transaction. Custom projection uses Session-prepared metadata and rejects a
stale preparation before publishing any row. Callback code runs outside lane/settings
mutation lines. Tool cancellation uses `PlanningFacts::ToolsCancelled` to distinguish
planned-aborted from unknown-interrupted and live results.

An accepted drive owns a generation as well as an operation ID. Reservation and active
control publication share the lane/activity boundary; cleanup checks that generation.
Suspension rechecks the durable token and releases control under the same boundary.
Operation abort and cancellation-task retirement have separate signals. A deferred
resume consumes its one-poll allowance only after its intent commits. Cancellation
captures the latest source before signaling the live fetch, and terminal completion
joins its receipt. External completion retires both a started cancellation and an
unreleased cancellation gate.

These new owning targets use exported RuntimeFactory/Lane/Session APIs, actual Memory
storage, real Models adapters, deterministic host/clock and manual execution gates.
They do not call pure planners as substitutes for public execution.

| Target | Tests | Concrete coverage |
| --- | ---: | --- |
| `runtime_r5_r7` | 9 | queue/cancel/usage; steer/follow-up/write; marker idempotence; three one-poll resumes; held fetch/cancel join; abort inside clock poll; cancel getter panic; started and gated cancel retirement |
| `runtime_r5_r7_boundaries` | 9 | both before-run capture orders; expected errors and no writes; six failure-drain write kinds; cancelled custom callback count zero; original/effective request options; missing-identity admission; idle callback close; both configuration capture orders; both cancel/consume orders |
| `runtime_r5_r7_projection` | 4 | actual seq/time with other-lane publication and setter reentry; exact checked write order; physical reopen after consumed one-item prefix; pending payload CAS loss; close during held projector |
| `runtime_r5_r7_deferred` | 5 | physical unknown-poll reopen with fresh reserved IDs; seven handle mismatch axes plus error/length/aborted; ready tools in automatic/manual mode with real span parents; response hook and newest source cancellation; poll hook step parent |
| `runtime_r5_r7_handoff` | 6 | abort during suspension planning; source API without registry; external terminal during identity precheck; same-operation resume from suspension listener; both missing-identity rejection/abort handoff orders |
| `runtime_r5_r7_observation` | 4 | immutable snapshots and fault-first preadmitted watch; typed facts/config events; close-first lane and session watches |
| private driver activity tests | 4 | blocked old cleanup, old-operation cleanup, same-operation generation cleanup, atomic control publication |

The projection fixture decorates actual RuntimeStorage and forwards the real prepared
API; it records successful transaction contents and forces one pending-payload change
before checked publication. It is not an alternative storage/commit implementation.
Physical-reopen tests use actual MemoryBacking close/open, including a consumed prefix.

Sibling targets retain independent ownership and unchanged assertions: public model
689 (7 tests / 18 histories), R6 control matrix 695 (5 tests / 23 cases), retirement
700 (2 cases). The R6 matrix enumerates existing R1–R7 phases and mixed planned/live/
unknown/completed tools. R8–R10 remain outside this unit. Original runtime tests and
251 generated histories remain unchanged.

The frozen authority distinguishes raw abort-first `terminate:false` from hook-first
complete transformed results, including `terminate:true`. The manual after-tool
gate case distinguishes entry into finalization from actual hook start. No
production blanket clamp is introduced to satisfy the fixture.

Verification varies runtime behavior, execution mode, persistence boundary and oracle:
public histories exercise actual implementation; selected cases compare automatic
and manually released execution; physical reopen starts after real durable prefixes;
independent model/control/retirement tests and old regression tests supplement owning
transaction/event assertions. This is finite deterministic coverage, not exhaustive
thread-schedule enumeration. No live provider, native integration run, or whole S10
acceptance is claimed by this unit.

The causal controls remove runtime enforcement separately: omitting pending-payload
Seq predicates admits a stale public consumer transaction; retaining a consumed
poll allowance starts three fetches in one resume. Both controls must fail the
unchanged public assertions, and exact restoration must pass.
