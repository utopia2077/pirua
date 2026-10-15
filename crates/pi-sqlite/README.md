# Shared SQLite infrastructure

`Database` is a cloneable handle to one mutex-protected SQLite connection. File
databases use WAL, foreign keys and a five-second busy timeout. `snapshot` reads
multiple queries under one deferred transaction; `transaction` reserves a writer
before invoking the callback and commits only after it succeeds. Callback errors
roll back before returning. Callbacks must use the supplied connection or
transaction and must not re-enter the same handle.

Components own their tables and initialize their current schema with
`initialize(component, version, schema)`. The schema and version record are
created atomically. A known component at a different version is rejected; this
crate does not migrate or downgrade it. Independent components can share a
database without sharing their session or execution semantics.

The callbacks are synchronous. Async hosts schedule blocking work explicitly.
`read` locks the connection but does not create a database snapshot; use
`snapshot` when multiple reads must see the same committed state across other
connections.
