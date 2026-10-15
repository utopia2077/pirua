# Coding sessions

`pi-coding-session` saves ordinary Coding conversation trees independently of
the Agent executor. `SessionManager` selects a branch and projects its messages,
model and thinking level. SQLite and Memory implement the same logical store
contract. Memory keeps data only for the lifetime of its store instance.

```rust
use std::sync::Arc;
use pi_coding_session::{MemoryStore, SessionManager};
use serde_json::json;

let store = Arc::new(MemoryStore::new());
let mut session = SessionManager::create(store.clone(), "/project")?;
let first = session.append_message(json!({
    "role": "user", "content": "Explain this project", "timestamp": 1
}))?;
session.set_label(&first, Some("starting point".into()))?;
let fork = session.fork(&first)?;
assert_eq!(fork.projection().messages.len(), 1);
# Ok::<(), pi_coding_session::Error>(())
```

Use `SqliteStore::open(path)` for persistence. Reopen with
`SessionManager::open(store, session_id)`. A store commit atomically appends its
records and selects the new leaf. Each manager carries a revision: a competing
writer causes `Error::Conflict`, leaving the stale manager unchanged. Call
`reload()` and decide how to apply the operation to the new state. The store does
not retry an operation against a different branch implicitly.

Records include messages, model and thinking selections, usage, compaction,
branch summaries, labels, names, custom entries and context edits. Global labels
survive forks; content edits affect their selected branch. Compaction callers
supply the normalized system checkpoint from the AI layer. Stored messages are
open JSON envelopes; the runtime adapter validates provider and Agent types.

The logical projection follows Pi v1.4.0 at
`4a080252f06caf63a525d7873c5d94d3c23887e0`. The
[source oracle](../../conformance/coding-session/README.md) verifies selected
projection cases. Physical storage is SQLite with Unix millisecond timestamps,
explicit session identifiers and current schema version 1. Opening a different
schema version fails. JSONL, historical import and schema migration are excluded.
Reopening a conversation does not execute recorded tools or resume an interrupted
task; Durable execution has a separate owner.

SQLite operations are synchronous and hold a connection lock. Async hosts should
run potentially large operations on their blocking executor. Session snapshots
and projections are detached values, so callers can inspect them without holding
the database lock.
