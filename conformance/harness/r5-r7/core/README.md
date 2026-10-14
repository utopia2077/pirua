# Pure R5–R7 recipe witnesses

Authority: pinned Harness v3 853a80d26c90a14c1886f0ebb8ffaae133ca2185,
H1089–1115, H1425–1507, H1718 and H1922–1927; accepted S10 runtime revision 2.

Production entrypoints are `pi_harness_core::{inbox,control,deferred}` and existing
`planner::{next_action,settlement_recipe}`, `transition::transition_recipe`.
`PlanningFacts::Inbox` supplies checked placement payloads/Seq and actual projection
facts; core selects IDs and produces the entry/delete prefix, leaf and total state.
Custom metadata preparation/projector execution is a later runtime/backend witness,
not evidence supplied by pure tests. Message write filtering is production pure code.
`PlanningFacts::ToolsCancelled` distinguishes planned-aborted from started-interrupted;
the old Tools shape remains compatibility-only for old runtime consumers.

`crates/pi-harness-core/tests/r5_r7_planner.rs` exercises queue precedence/modes,
durable skip roundtrip, all existing run phase/control selection pairs, explicit queue
versus deferred-write projection, failure revival and structural failure preservation,
abort repeat, cancelled safe-phase write drains, single poll permit, fresh replacement
IDs/same poll, exact live state, source drift, full handle equality including metadata
presence, response/error/cancelled classification, original usage and ordered tool IDs.
`crates/pi-agent-schema/tests/r5_r7_transports.rs` rejects invalid reconciliation
reason/value/key/poll pairings without changing any durable DTO.

`crates/pi-harness-runtime/tests/r5_r7_core_publication.rs` applies real exported core
recipes to actual Memory RuntimeSession. It distinguishes enqueue/control/payload-first
NotApplied from consume-first durable entry/delete, preserves remaining drained payloads,
and rereads serialized skip. It does not execute a RuntimeHarness queue call or claim a
physical repository reopen: those belong to the S10 runtime consumer's proofs.

Existing R1–R4 targets and the finite operation-state model remain unchanged.
Both feature profiles, strict Clippy, formatting, workspace checks and compiled
causal rejection/restoration remain verification obligations. Whole-runtime
acceptance also requires independent review and the complete `verify.sh` run
after actual runtime/backend composition.
