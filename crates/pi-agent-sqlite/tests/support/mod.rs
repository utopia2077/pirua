#![allow(dead_code)]
#[path = "../../../../conformance/session/backend.rs"]
pub mod backend;
pub use backend::*;
use pi_agent_schema::*;
use pi_agent_session::{codec::SessionCodec, memory::StorageClock, storage::*};
use pi_agent_sqlite::{SqliteOptions, SqliteStorage};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
pub struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<pi_agent_schema::session_values::SafeUnsigned, StorageError> {
        Ok(n(1234))
    }
}
pub struct Factory {
    path: std::path::PathBuf,
    next: AtomicU64,
}
impl Default for Factory {
    fn default() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pi-sqlite-contract-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self {
            path,
            next: AtomicU64::new(0),
        }
    }
}
impl Drop for Factory {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.path).unwrap();
    }
}
impl BackendFactory for Factory {
    fn create(&self) -> Arc<dyn AtomicStorage> {
        SqliteStorage::create(self.path.join(self.next.fetch_add(1,Ordering::SeqCst).to_string()),serde_json::from_value(serde_json::json!({"id":"contract","createdAt":1,"storageVersion":1,"storeGeneration":0})).unwrap(),Arc::new(SessionCodec::new(SessionCodecOptions::default(),ProviderHeaderPolicy::default()).unwrap()),Arc::new(Clock),SqliteOptions::default()).unwrap()
    }
}
#[derive(Default)]
pub struct Observations(pub Mutex<Vec<&'static str>>);
impl Observer for Observations {
    fn observed(&self, stage: &'static str, _: &StorageSnapshot) {
        self.0.lock().unwrap().push(stage);
    }
}
