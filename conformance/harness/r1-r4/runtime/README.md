# Public R1-R4 runtime witnesses

`public-traces.json` fixes the configured, ordinary no-tool transaction boundaries
and deterministic identities. `tests/runtime_r1_r4.rs` executes that exact trace
through the public AgentHarnessFactory/AgentHarness/AgentLane, RuntimeSession over
real Memory storage, the actual ModelsRegistry/faux lifecycle adapter, and a host
pump that retains and polls tasks. Both automatic and manually gated runs must
match the same committed transaction sequence, not merely one another's end state.

The same owning test target covers stamp-bounded restore, real publication during
hydration/CAS, retained tasks/close/fault and callback ownership, request/context
transports, retry/reopen, tools/replay, ordered finalization, usage and terminal
cleanup. The separate `operation_state_model` target owns the deterministic
generated-history property suite.

Configuration-less initialization, reserved-usage/no-ledger interpretation and
ordinary synthetic recovery API provenance are defined by the
[recovery contract](../../../../docs/contracts/recovery.md). This fixture does not
claim queue, deferred or structural runtime coverage.
