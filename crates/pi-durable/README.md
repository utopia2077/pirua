# pi-durable

Durable conversations and persistent task execution over `pi-durable-session`.
`Durable` owns admission, agent configuration, model generation, ordinary and keyed
nested tools, committed progress, accounting, compaction and agent event streams.
One authoritative Session stores every document, entry, task and submission.
The lower-level `Scheduler` owns definition snapshots, invocation leases, recovery,
joins, sleeps, ownership and cancellation.

Run the complete offline model/tool/receipt example:

```sh
cargo run -p pi-durable --example offline_durable --all-features --locked --offline
```

Register native AI models and a `ToolRegistration` in `DurableOptions`, open Durable,
configure the root conversation and submit `SubmissionDraft::input`. The returned
Submission reads/waits/aborts its durable receipt. Closing joins admitted work and
keeps recovery state in the Session. The [runtime contract](../../docs/contracts/durable-runtime.md)
describes exact tool/progress/nested semantics, read events and native boundaries.
Ordinary Coding/MCP/native-extension bindings have their own module; persistent
CodeMode, the public task graph watch and Durable schema telemetry are not claimed
by this core.

The generic Scheduler is available independently:

Create a Session backed by real MemoryStorage or SqliteStorage, register immutable
TaskDefinitions, and open a Scheduler. Opening reconciles persisted running tasks
and returns paused. Inspection and committed reads never start handlers.
`resume()`, `wait_for_task()` and `wait_for_idle()` enable execution.

```rust,ignore
let task = TaskDefinition::new("example.counter", 1, |_| {
    Ok(serde_json::json!({"phase": "count", "n": 0}))
})?.phase("count", |record, invocation| async move {
    invocation.commit(|_, current| {
        let TaskState::Running { checkpoint } = &current.state else {
            unreachable!()
        };
        let n = checkpoint["n"].as_u64().unwrap() + 1;
        Ok(Some(if n == 3 {
            TaskState::Terminal {
                outcome: TaskOutcome::Completed { result: serde_json::json!(n) },
            }
        } else {
            TaskState::Running {
                checkpoint: serde_json::json!({"phase": "count", "n": n}),
            }
        }))
    }).await
});
let registry = Registry::new();
registry.install("application", vec![task.clone()])?;
let scheduler = Scheduler::open(session, registry, SchedulerOptions::default()).await?;
let id = scheduler.create_task(&task, serde_json::Value::Null, TaskOptions {
    conversation_id: Some(conversation_id),
    ..Default::default()
}).await?;
let receipt = scheduler.wait_for_task(id).await?;
scheduler.close().await?;
```

Handlers receive detached accepted Session records. `Invocation::commit` atomically
stages documents, entries, child work and the next task state. `transact<T>` also
returns an arbitrary detached receipt; returning `None` for the next state does
not count as durable checkpoint progress. A phase that returns with an unchanged
running checkpoint faults, even after document or memo writes.

Persisted TaskRecord, TaskState, TaskOutcome and IDs remain the Session DTOs. Missing
or mismatched definitions block without migration. Registry replacements never
change an active phase's snapshot. An outcome holds as `completing` until ordinary
owned task and conversation work drains. Abort intent commits before cancellation;
normal close seals the entire Session, joins handlers and privately resets surviving
running tasks to pending. Task documents retire only with the terminal Session batch.

The native [scheduler contract](../../docs/contracts/durable-scheduler.md) describes
callback ordering, lifecycle and public seams. Tests consume immutable published
1.4.0 scheduler fixtures on Memory and SQLite and add interruption, cancellation,
close finalization, initialization and stale-callback cases.
