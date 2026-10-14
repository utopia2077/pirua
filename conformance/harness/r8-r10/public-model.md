# Independent public structural model

The target r8_r10_public_model has7 Rust test functions and20 finite histories.
All use actual public RuntimeFactory, AgentLane, ModelsRegistry/adapters and MemorySessionRepo.
The reference stores visible leaf, entry count and pending-nextRun IDs. It does not
copy core recipes or predict hidden operation-state transitions. Durable states are
read only to select documented crash boundaries.

| Histories | Count | Observable oracle |
|---|---:|---|
| Empty compaction |1| Expected rejection and complete public snapshot unchanged |
| Manual decline/hook/generated |3| Leaf/count, exact full result/stored/event row, usage/source and queued preservation |
| Invalid navigation |7| Current root, root label, summarized root source, current nonroot, summarized null target, absent/foreign-only target; no writes |
| Navigation omitted/root/hook/decline/generated |5| Source/target/summary identity, final leaf/label, exact row and usage |
| Empty abandoned descendant branch |1| Decision hook still called, generated fallback with no provider request |
| Threshold decline/supply actual reopen |2| Marked trigger reached before request, physical close/open, no duplicate hook on same trigger |
| Split summary after request0 usage |1| Physical completed-prefix reopen; next whole attempt starts history again, then prefix; old usage11 retained, new17+23, final display40 |

The split fixture has old user+assistant history followed by a new user and a25-token
assistant tail; keepRecentTokens20 puts the cut inside that new turn. Golden fixture
reasoning follows pinned compaction.ts:374–422, not a copied Rust algorithm.
Unknown whole-attempt recovery preserves durable preparation and never reruns its decision hook.

Usage observations combine actual Usage events across the same listener log and maintained
SessionStats. Public entries/registers are inventoried; these are not a raw ledger scan
or a ConditionalCommit ordered-write trace. Runtime owning tests cover that trace,
the remaining nested gates/races, backend fencing, numeric goldens and classifier cases.
No exhaustive scheduler claim, forged accepted operation, sleeps or live provider is used.
Baseline public stubs must compile then fail behaviorally before a candidate is imported;
fixture compile errors are separately retained and are not causal proof.
