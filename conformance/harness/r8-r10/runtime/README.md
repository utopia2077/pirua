# Public R8–R10 runtime evidence

The `runtime_r8_r10*` integration targets execute the public Harness API against
actual MemoryStorage, RuntimeSession, and ModelsRegistry. The forwarding storage
spy preserves actual checked/prepared authority; it records complete transactions.
These tests do not invoke a private planner as a substitute for the runtime.

- `runtime_r8_r10`: generated and supplied compaction, unsummarized root navigation,
  source-exclusive LCA and empty-side navigation, both overflow predicates and
  bounded threshold decline.
- `runtime_r8_r10_control`: lifecycle cancellation and physical recovery, known
  between-request identity suspension and resume rejection, four unknown/cancelled
  cap states, live NotStarted versus post-intent missing, and whole-attempt retry
  with ledger usage 51 versus final display usage 40.
- `runtime_r8_r10_publication`: exact ordered hook usage/entry/moves/label/cleanup,
  backend metadata equality in returned and emitted rows, and stale terminal token
  re-preparation without hook replay. One WatchHandle observes cross-kind order.
- `runtime_r8_r10_preparation`: provisional-kind reservation, configuration and
  nextRun changes, blocked idle writes, close retirement, exact custom row metadata,
  and no stale threshold preparation after configuration or cross-lane publication.
- `runtime_r8_r10_deferred`: zero, below, equal and above the captured output limit,
  plus two opposite late registry changes. Each settles actual usage in one poll
  and never enters compaction.
- `runtime_r8_r10_retirement`: another Session writer commits a valid structural
  terminal while a noncooperative converter is pending; discovery returns the
  exact result and releases that callback before global close.
- `runtime_r8_r10_cas`: a queued nextRun changes the lane sequence while the
  nested intent gate is parked; the same captured request completes with one
  provider call under a single permitted attempt.
- `runtime_r8_r10_navigation_line`: an invalid unsummarized target read holds
  the lane mutation line without a provisional drive; a competing prompt waits
  and then accepts after the no-write navigation rejection.

Twenty-two functions contain 34 bounded top-level loop scenarios. Sequential actions
within one scenario are not counted as separate histories. Independent public model,
control and SQLite composition targets remain separately owned conformance evidence.

The only prior test-file change is the source-derived genuine-Length input
in `runtime_r5_r7_deferred`: output is set to the captured model limit and total usage
remains consistent. Existing assertions and other inputs remain intact. The new
zero-output test retains the old input as the required overflow failure control.

Full repository acceptance requires the complete workspace gate; these finite
package tests do not substitute for it.
