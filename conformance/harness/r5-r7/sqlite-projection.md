# Actual Harness custom projection with SQLite ownership

The `pi-agent` integration target `runtime_sqlite_projection` uses the existing `sqlite`
feature and direct dependencies. It drives the actual RuntimeFactory, SqliteSessionRepo,
ModelsRegistry and public prompt/SessionTree append operations. It calls neither storage
preparation nor synthetic operation-state publication directly.

Two finite histories share an explicitly held first provider request. A public custom
append is durably queued, then the request completes and the actual EntryProjector
records its full prepared CustomEntry and remains pending on a release token.

- **Success control:** while projection is pending, change the injected application row
  clock from7 to9000. Release the projector, finish the public run, and compare the actual
  committed entry against every callback-visible field via exact CustomEntry equality.
  All later context projections must receive the same complete row. Exactly two provider
  requests show that returned projected context causes its required continuation.
- **Expired owner:** only after positive projector entry and pending public completion,
  set `writer_lease.expires_at_ms=0` in the test's own temporary database. Release the
  projector; the public operation must reject through Fault/Closed, issue no second
  provider request, and leave every nonlease SQLite table exactly equal to the committed
  pre-expiry prefix. The comparison includes session header/revision/allocator, canonical
  entries/registers/usage and derived tables; it is not merely absence of one entry.

A fixed Python standard-library sqlite3 program is invoked with the private database path
as a separate argument. It changes only that one lease field; readout uses a read-only
connection/transaction. No shell interpolation, sleeps, network, new dependencies,
production changes or user database accesses are involved. Temporary files are local to
the fixture and cleaned after closing its owners.

Heartbeat detection may seal the writer before the final commit checks. Either path must
reject and retain the prefix. This test does not claim the final SQL transaction was the
first expiry detector; the backend independently proves its fresh post-validator lease
check. These two actual public histories close that backend proof's missing asynchronous
Harness-projector composition boundary; they do not claim all SQLite/runtime schedules.

Command shape: `cargo test --offline --locked -p pi-agent --features sqlite --test
runtime_sqlite_projection`; also run this target with `--all-features`. These finite
public histories do not prove complete runtime assembly.
