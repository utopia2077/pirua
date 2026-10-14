# Independent public SQLite structural histories

Pinned Harness v3 H1269/1278/1289/1345–1362/1408–1423 and the structural
contract define expected behavior. These are public tests/support with actual
production state algorithms and commits.

Six named finite histories:

- MC-P / MC-L: manual hook compaction, usage, full returned/end/entry_added/stored row
  equality, ordinal including preceding usage, allowed fact/clock drift at terminal
  gate; same schedule with actual SQLite lease expiry refuses the entire prefix.
- NV-P / NV-L: summarized hook navigation with target label, exact source/target/
  summary linkage and all-write offset; identical expiry schedule refuses entry,
  usage, movement, label, lastResult and cleanup together.
- OF-P: actual first overflow response at assistant-pending commit_settlement gate;
  one release publishes normalized response/usage/preparation/deciding together.
  Error remains billed history but is absent from summary and retained tail. Hook
  compaction resumes NeedAssistant(true) with correct trigger and response parent.
  Physical Harness/Session close and SqliteSessionRepo reopen preserve complete
  checkpoint and ledger; explicit public resume completes with one later request.
- OF-L: matching response-settlement boundary with expired real writer lease leaves
  every nonlease table byte/logical-row identical, including no partial R/U/P/state.

Snapshots use Python standard-library sqlite3 on each test's unique private DB.
They read every nonlease table in one read transaction. The sole test mutation is
writer_lease.expires_at_ms=0; no application entry/register/usage is written directly.
Clock is injected; no sleeps, native HTTP, real credentials or shared session files.

The source-derived action names are existing commit_terminal/commit_settlement,
qualified by actual durable phases. No candidate-specific step count, weakened old
assertion, fake accepted operation or synthetic pending phase is used. Full Session
rows are read through actual public APIs and matching passive events. The SQLite
nonlease snapshot detects any partial publication; reopen comparisons intentionally
compare logical checkpoint/usage because a new writer lease may advance fencing.

These are component public/backend histories, not an exhaustive scheduler, all-backend
or complete nested-summary/recovery matrix. Other independent units own those axes.
The foundation compiled baseline is expected to fail unfinished public structural
behavior; compile diagnostics are recorded separately. Test-only commit waits for an
explicitly supplied valid production parent and final grant.
