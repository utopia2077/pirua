# Pure structural recipes and checked publication

This unit supplies R8–R10 pure choices and process-local facts. It does not implement
public RuntimeFactory compact/navigation execution. Runtime still owns asynchronous
preparation, source projection, captured request helpers, manual gates, request spans,
prepared tokens and complete drive retirement. Core depends only on Agent schema.

The outer summary key binds Session/lane/operation/task/attempt and captured context.
Nested request/usage fields and operation-state seq may change without retiring that
key. Nested intent and settlement use the latest exact request tuple and current
checked publication predicates. A new non-Serde suspension receipt conveys nonempty
missing identities without manufacturing a provider failure or cancelled NotStarted.
Suspension returns no writes and uses the owning Run/Compaction/Navigation result.

Summary ready intents return the precise Summary dispatch in their recipe. A consumer
must install that dispatch after Applied; an already observed pending state without
its live owner is unknown and advances the complete attempt under captured policy.
At cap/cancellation an outstanding unknown request gets its reserved zero usage before
terminal reconciliation; live NotStarted clears its intent without a row. Already
settled usage is never read or rewritten. No partial summary text is durable.

Semantic success is checked against a caller-supplied complete prepared Entry. Core
never guesses seq or timestamp. The runtime keeps the matching PreparedEntries token
through its terminal gate, publishes with checked prepared commit, and on NotApplied
rebuilds metadata/results without rerunning the completed semantic effect. Standalone
finalization and summarized navigation combine entry/leaf/label and cleanup in one
recipe; in-run success restores its captured checkpoint. Terminal cleanup preserves
lane nextRun and all ledger rows. Hook usage is commit-minted with no entryId because
its ordered write precedes the result entry; it still contributes to prepared row
ordinals and the result retains its display usage (authority713/structural3).

Finite new owning tests cover:

- three owning operation kinds; seven wrong nested identity/config/index axes;
  two sequential requests and live outer identity across their state-seq changes;
- suspension owning-result projection/cancellation refusal; unknown recovery matrix
  over cancellation, cap and outstanding request; live NotStarted versus actual usage;
- hook continue/decline and captured config/settings/preparation predicates; empty
  acceptance and structural abort repeat; threshold boundary comparison/mark;
- matching Error and pre-clamp Length, substantive Stop, first/second overflow with
  empty/nonempty preparation and cancellation; distinct deferred error table;
- actual Session/Memory nested usage and stale recipe rejection; exact prepared
  compaction row with stale reprepare; generated navigation target/summary/label and
  cleanup order with no duplicated generated usage.

The actual-storage fixture forwards ordinary RuntimeSession/Memory APIs and validates
current roots through the original core validator. It does not reimplement a commit.
The nested-usage causal saves an exact source mutant before compilation, removes its
request-tuple guard, then the unchanged test observes an actual wrong usage row/state
published through Session. Exact restored source passes.

This finite component coverage is not a public manual-gate/crash/reopen acceptance
claim. Independent public histories, runtime integration, SQLite composition and the
normal main whole-workspace gate remain separately owned by the controller.
