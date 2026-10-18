# Session storage

`pi-coding-session` owns current session records, tree projection, selection,
labels, branching, forks and compaction boundaries. Runtime-specific typed
message admission remains in CodingSession; the storage crate consumes detached
record envelopes. The [crate README](../../crates/pi-coding-session/README.md)
describes the public manager and store APIs.

Memory is an in-process backend useful for temporary sessions and tests. SQLite
persists the same logical records and supports reopening. `pi-sqlite` supplies
connection/transaction infrastructure with WAL, foreign keys, a busy timeout,
stable read snapshots and immediate write transactions.

A SessionManager adopts a candidate state only after a successful transaction.
Version conflicts are explicit, and failed batches leave storage and the manager
unchanged. A branch's visible context is projected from its path and compaction
records; runtime caches do not become another record authority.

There is no JSONL backend, import path, old schema conversion or tool replay.
Unknown persisted schema versions are rejected.

`pi-durable-session` independently owns Durable's current records, documents,
transactions, publications, history and forks. Its SQLite backend reuses
`pi-sqlite`, with indexed record and document revision tables. One Session serializes
admitted mutations; only successful storage commits publish immutable changes.
Ambiguous backend failures close that Session, and reopening reads durable truth.
Document observation binds an incarnation rather than following a replacement at
the same address. The [Session contract](../contracts/durable-session.md) defines
the native APIs and comparison limits. This crate stores task state; dispatch and
recovery policy belong to the forthcoming Durable scheduler.
