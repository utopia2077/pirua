#![allow(dead_code)]
use pi_durable::*;
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
pub fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
pub struct Temp(pub PathBuf);
impl Temp {
    pub fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "pi-durable-scheduler-{}-{}.sqlite",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        )))
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        for path in [
            self.0.clone(),
            self.0.with_extension("sqlite-wal"),
            self.0.with_extension("sqlite-shm"),
        ] {
            let _ = std::fs::remove_file(path);
        }
    }
}
pub fn stores() -> Vec<(&'static str, Arc<dyn session::Storage>, Option<Temp>)> {
    let temp = Temp::new();
    vec![
        ("memory", Arc::new(session::MemoryStorage::new()), None),
        (
            "sqlite",
            Arc::new(session::SqliteStorage::open(&temp.0).unwrap()),
            Some(temp),
        ),
    ]
}
pub async fn scheduler(
    store: Arc<dyn session::Storage>,
    registry: Registry,
    options: SchedulerOptions,
) -> Scheduler {
    let now = options.now.clone();
    let session = Session::with_clock(store, move || now()).unwrap();
    session
        .commit(|tx| tx.ensure_root_conversation())
        .await
        .unwrap();
    Scheduler::open(session, registry, options).await.unwrap()
}
pub fn task_options() -> TaskOptions {
    TaskOptions {
        conversation_id: Some(session::ROOT_CONVERSATION_ID),
        ..Default::default()
    }
}
pub fn complete(result: serde_json::Value) -> TaskState {
    TaskState::Terminal {
        outcome: TaskOutcome::Completed { result },
    }
}
pub async fn until(scheduler: &Scheduler, id: TaskId, status: &str) -> TaskRecord {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let task = scheduler.get_task(id).await.unwrap().unwrap();
            if task.state.status() == status {
                return task;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("task {id} did not reach {status}"))
}
pub async fn wait(scheduler: &Scheduler, id: TaskId) -> TaskRecord {
    tokio::time::timeout(Duration::from_secs(5), scheduler.wait_for_task(id))
        .await
        .expect("task wait timeout")
        .unwrap()
}
pub async fn close(scheduler: &Scheduler) {
    tokio::time::timeout(Duration::from_secs(5), scheduler.close())
        .await
        .expect("close timeout")
        .unwrap();
}
