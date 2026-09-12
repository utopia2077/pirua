#![allow(dead_code)]
use pi_agent_schema::StorageError;
use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_session::{
    memory::{MemoryBacking, MemoryStorage, StorageClock},
    storage::*,
};
use std::sync::{Arc, Mutex};
#[path = "../../../../conformance/session/backend.rs"]
pub mod backend;
pub use backend::*;
pub struct Clock;
impl StorageClock for Clock {
    fn now(&self) -> Result<SafeUnsigned, StorageError> {
        Ok(n(1234))
    }
}
pub fn memory() -> (Arc<MemoryBacking>, Arc<MemoryStorage>) {
    let backing = Arc::new(MemoryBacking::new());
    let handle = backing.open(Arc::new(Clock)).unwrap();
    (backing, handle)
}
pub struct Factory;
impl BackendFactory for Factory {
    fn create(&self) -> Arc<dyn AtomicStorage> {
        memory().1
    }
}
#[derive(Default)]
pub struct Observations(pub Mutex<Vec<&'static str>>);
impl Observer for Observations {
    fn observed(&self, stage: &'static str, _: &StorageSnapshot) {
        self.0.lock().unwrap().push(stage);
    }
}
