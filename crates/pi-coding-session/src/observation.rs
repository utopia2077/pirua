//! Process-local canonical history observation. Publication only sends detached values.
use crate::{SessionEntry, SessionSnapshot};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel};

#[derive(Clone, Debug)]
pub enum HistoryChange {
    Committed {
        session_id: String,
        base_revision: u64,
        revision: u64,
        appended: Vec<SessionEntry>,
        leaf_id: Option<String>,
    },
    Replaced {
        snapshot: SessionSnapshot,
    },
}
#[derive(Default)]
struct Observers {
    next: u64,
    senders: BTreeMap<u64, UnboundedSender<HistoryChange>>,
}
#[derive(Clone, Default)]
pub(crate) struct HistoryObservers(Arc<Mutex<Observers>>);
impl HistoryObservers {
    pub fn subscribe(&self, snapshot: SessionSnapshot) -> HistorySubscription {
        let (sender, changes) = unbounded_channel();
        let mut observers = self.0.lock().unwrap_or_else(|e| e.into_inner());
        let id = observers.next;
        observers.next += 1;
        observers.senders.insert(id, sender);
        HistorySubscription {
            snapshot,
            changes,
            id,
            observers: Arc::downgrade(&self.0),
        }
    }
    pub fn publish(&self, change: HistoryChange) {
        self.0
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .senders
            .retain(|_, sender| sender.send(change.clone()).is_ok());
    }
}
/// Snapshot and future history commits share the SessionManager's ownership boundary.
/// Dropping the subscription unregisters it. Consumers must keep this owner while
/// receiving from `changes` (the receiver must not be detached from its registration).
pub struct HistorySubscription {
    pub snapshot: SessionSnapshot,
    pub changes: UnboundedReceiver<HistoryChange>,
    id: u64,
    observers: Weak<Mutex<Observers>>,
}
impl Drop for HistorySubscription {
    fn drop(&mut self) {
        if let Some(observers) = self.observers.upgrade() {
            observers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .senders
                .remove(&self.id);
        }
    }
}
