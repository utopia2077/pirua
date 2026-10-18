# pi-durable-session

Native committed-state authority for Durable: detached records, decoded Chord operations,
Memory and indexed SQLite storage, atomic Session transactions, history/forks and
incarnation-bound committed observation. There is no scheduler or provider dependency.

The public contract is [docs/contracts/durable-session.md](../../docs/contracts/durable-session.md).
The implementation follows selected Pi 1.4.0 commit
`4a080252f06caf63a525d7873c5d94d3c23887e0`; it does not read mutable upstream main.

```rust,no_run
use std::sync::Arc;
use pi_durable_session::*;
use serde_json::{Value, json};

async fn example() -> Result<()> {
    let session = Session::open(Arc::new(SqliteStorage::open("session.sqlite")?))?;
    // The higher runtime owns root initialization and execution policy.
    session.commit(|tx| tx.ensure_root_conversation()).await?;

    let counter = DocDefinition::new(
        "example.counter", 1, DocumentSemantics::Session, false,
        |_| Ok(json!({"count": 0})),
        |_, _, info| Ok(info.deltas_since_base >= 50),
    )?;
    let address = counter.address(Scope::Session, None)?;
    let definition = counter.clone();
    let target = address.clone();
    session.commit(move |tx| {
        let id = tx.doc(&definition, target, Value::Null)?;
        let count = tx.document_value(id)?["count"].as_u64().unwrap();
        tx.mutate(id, Op::Set(vec!["count".into()], json!(count + 1)))
    }).await?;
    assert_eq!(session.snapshot(counter, address).await?.unwrap()["count"], 1);
    session.close().await
}
```

`Session` async operations run on a Tokio runtime. The scoped callback is synchronous,
may borrow the transaction only during its call, and may return detached owned values.
Use `mutate` for native draft edits and `apply`/`apply_all` for exact decoded batches.
Document JSON never has a public mutable reference. No callback should reenter Session
or perform external effects while holding its transaction.

Tests consume immutable independent release fixtures under
`conformance/durable-v1.4.0`; their expected values are owned separately. The backend
and fault tests are local Rust tests and require no providers. SQLite supports only
this crate's current schema; unsupported versions reject without migration.
