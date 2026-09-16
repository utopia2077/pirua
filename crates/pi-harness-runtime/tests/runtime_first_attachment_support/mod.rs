use pi_agent_schema::*;
use pi_agent_session::{memory::MemoryStorage, storage::*};
use pi_ai_core::CancellationToken;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
#[derive(Default)]
pub struct CountingIds(pub AtomicUsize);
impl IdGenerator for CountingIds {
    fn next(&self, timestamp: Option<pi_ai_core::SafeUnsigned>) -> String {
        let ts = timestamp.map_or(7, |v| v.get());
        format!(
            "{:08x}-{:04x}-7000-8000-{:012x}",
            ts >> 16,
            ts & 65535,
            self.0.fetch_add(1, Ordering::SeqCst)
        )
    }
}
pub struct ProbeStorage {
    pub inner: Arc<MemoryStorage>,
    pub reads: Mutex<Vec<&'static str>>,
    pub commits: Mutex<Vec<ConditionalCommit>>,
    pub applied: AtomicUsize,
    pub race: AtomicUsize,
    pub competitor: Mutex<Option<LaneConfiguration>>,
    pub pause: AtomicBool,
    pub entered: CancellationToken,
    pub release: CancellationToken,
    pub fail_commit: AtomicBool,
    pub fail_discovery: AtomicBool,
    pub wrong_state: AtomicBool,
}
impl ProbeStorage {
    pub fn new(inner: Arc<MemoryStorage>) -> Self {
        Self {
            inner,
            reads: Mutex::new(vec![]),
            commits: Mutex::new(vec![]),
            applied: AtomicUsize::new(0),
            race: AtomicUsize::new(0),
            competitor: Mutex::new(None),
            pause: AtomicBool::new(false),
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
            fail_commit: AtomicBool::new(false),
            fail_discovery: AtomicBool::new(false),
            wrong_state: AtomicBool::new(false),
        }
    }
    fn read(&self, name: &'static str) {
        self.reads.lock().unwrap().push(name);
    }
    async fn compete(&self) -> Result<(), StorageError> {
        let value = self.competitor.lock().unwrap().clone().unwrap();
        self.inner
            .commit(Transaction {
                writes: vec![Write::SetRegister(Box::new(RegisterSet::LaneConfig {
                    key: "main".into(),
                    value,
                }))],
            })
            .await?;
        Ok(())
    }
}
fn injected() -> StorageError {
    StorageError::Io(ErrorContext::new("first-attachment fixture error").unwrap())
}
impl Storage for ProbeStorage {
    fn commit(
        &self,
        transaction: Transaction,
    ) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
        self.inner.commit(transaction)
    }
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
        self.read("entries");
        self.inner.get_entries(ids)
    }
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        Box::pin(async move {
            self.read("register");
            if namespace == RegisterNamespace::LaneState && self.wrong_state.load(Ordering::SeqCst)
            {
                return self
                    .inner
                    .get_register(RegisterNamespace::LaneLeaf, key)
                    .await;
            }
            let value = self.inner.get_register(namespace, key).await?;
            if namespace == RegisterNamespace::LaneConfig
                && key == "main"
                && self
                    .race
                    .compare_exchange(1, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                self.compete().await?;
            }
            Ok(value)
        })
    }
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        Box::pin(async move {
            self.read("list_registers");
            if self.fail_discovery.load(Ordering::SeqCst) {
                return Err(injected());
            }
            self.inner.list_registers(namespace, prefix).await
        })
    }
    fn scan_branch(&self, q: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.read("scan_branch");
        self.inner.scan_branch(q)
    }
    fn scan_branch_structure(
        &self,
        q: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        self.read("scan_branch_structure");
        self.inner.scan_branch_structure(q)
    }
    fn scan_entries(&self, q: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.read("scan_entries");
        self.inner.scan_entries(q)
    }
    fn scan_usage(&self, q: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        self.read("scan_usage");
        self.inner.scan_usage(q)
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        self.read("stats");
        self.inner.stats()
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.close()
    }
}
impl AtomicStorage for ProbeStorage {
    fn commit_conditional(
        &self,
        r: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        self.inner.commit_conditional(r)
    }
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        self.read("snapshot");
        self.inner.snapshot()
    }
}
impl CheckedStorage for ProbeStorage {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move {
            self.commits.lock().unwrap().push(request.clone());
            if self.pause.swap(false, Ordering::SeqCst) {
                self.entered.cancel();
                self.release.cancelled().await;
            }
            if self.fail_commit.load(Ordering::SeqCst) {
                return Err(CheckedCommitError::Storage(injected()));
            }
            if self
                .race
                .compare_exchange(2, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.compete().await.map_err(CheckedCommitError::Storage)?;
            }
            if self
                .race
                .compare_exchange(4, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.inner
                    .commit(Transaction {
                        writes: vec![Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
                            key: "main".into(),
                            value: None,
                        }))],
                    })
                    .await
                    .map_err(CheckedCommitError::Storage)?;
            }
            let result = self.inner.commit_checked(request, validator).await?;
            if matches!(result, ConditionalOutcome::Applied(_)) {
                self.applied.fetch_add(1, Ordering::SeqCst);
            }
            if self
                .race
                .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.compete().await.map_err(CheckedCommitError::Storage)?;
            }
            Ok(result)
        })
    }
}
impl RuntimeStorage for ProbeStorage {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        self.inner.runtime_attachment()
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        self.read("stamp");
        self.inner.read_stamp()
    }
}
