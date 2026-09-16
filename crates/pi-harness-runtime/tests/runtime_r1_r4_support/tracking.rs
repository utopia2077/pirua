//! Instrumented forwarding around actual Memory authority.
use pi_agent_schema::*;
use pi_agent_session::{memory::MemoryStorage, storage::*};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
pub struct TrackedStorage {
    pub inner: Arc<MemoryStorage>,
    pub reads: Mutex<Vec<String>>,
    pub change_and_omit: AtomicBool,
    pub stable_omit: AtomicBool,
    pub late_pending: AtomicBool,
    pub fail_reads: AtomicBool,
    pub late_result: AtomicBool,
    pub commit_trace: Arc<Mutex<Vec<String>>>,
    pub transactions: Mutex<Vec<Transaction>>,
    pub pause_entry_commit: AtomicBool,
    pub commit_entered: pi_ai_core::CancellationToken,
    pub release_commit: pi_ai_core::CancellationToken,
}
impl TrackedStorage {
    pub fn new(inner: Arc<MemoryStorage>) -> Self {
        Self {
            inner,
            reads: Mutex::new(vec![]),
            change_and_omit: AtomicBool::new(false),
            stable_omit: AtomicBool::new(false),
            late_pending: AtomicBool::new(false),
            fail_reads: AtomicBool::new(false),
            late_result: AtomicBool::new(false),
            commit_trace: Arc::new(Mutex::new(vec![])),
            transactions: Mutex::new(vec![]),
            pause_entry_commit: AtomicBool::new(false),
            commit_entered: pi_ai_core::CancellationToken::new(),
            release_commit: pi_ai_core::CancellationToken::new(),
        }
    }
    fn record(&self, name: &str) {
        self.reads.lock().unwrap().push(name.into());
    }
}
impl Storage for TrackedStorage {
    fn commit(
        &self,
        transaction: Transaction,
    ) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
        Box::pin(async move {
            self.record("commit");
            self.inner.commit(transaction).await
        })
    }
    fn get_entries<'a>(
        &'a self,
        ids: &'a [EntryId],
    ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
        Box::pin(async move {
            self.record("get_entries");
            self.inner.get_entries(ids).await
        })
    }
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        Box::pin(async move {
            self.record("get_register");
            if self.fail_reads.load(Ordering::SeqCst) {
                return Err(StorageError::Io(
                    ErrorContext::new("injected stable read failure").unwrap(),
                ));
            }
            let result = self.inner.get_register(namespace, key).await?;
            if namespace == RegisterNamespace::LaneConfig {
                if self.change_and_omit.swap(false, Ordering::SeqCst) {
                    if let Some(Register::LaneConfig { value, .. }) = &result {
                        self.inner
                            .commit(Transaction {
                                writes: vec![Write::SetRegister(Box::new(
                                    RegisterSet::LaneConfig {
                                        key: key.into(),
                                        value: value.clone(),
                                    },
                                ))],
                            })
                            .await?;
                    }
                    return Ok(None);
                }
                if self.stable_omit.load(Ordering::SeqCst) {
                    return Ok(None);
                }
            }
            Ok(result)
        })
    }
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key_prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        Box::pin(async move {
            self.record("list_registers");
            self.inner.list_registers(namespace, key_prefix).await
        })
    }
    fn scan_branch(&self, query: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        Box::pin(async move {
            self.record("scan_branch");
            self.inner.scan_branch(query).await
        })
    }
    fn scan_branch_structure(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        Box::pin(async move {
            self.record("scan_branch_structure");
            self.inner.scan_branch_structure(query).await
        })
    }
    fn scan_entries(&self, query: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        Box::pin(async move {
            self.record("scan_entries");
            self.inner.scan_entries(query).await
        })
    }
    fn scan_usage(&self, query: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        Box::pin(async move {
            self.record("scan_usage");
            self.inner.scan_usage(query).await
        })
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        Box::pin(async move {
            self.record("stats");
            self.inner.stats().await
        })
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        Box::pin(async move {
            self.record("close");
            self.inner.close().await
        })
    }
}
impl AtomicStorage for TrackedStorage {
    fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        self.inner.commit_conditional(request)
    }
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        self.record("snapshot");
        self.inner.snapshot()
    }
}
impl CheckedStorage for TrackedStorage {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move {
            if request
                .transaction
                .writes
                .iter()
                .any(|write| matches!(write, Write::Entry(_)))
                && self.pause_entry_commit.swap(false, Ordering::SeqCst)
            {
                self.commit_entered.cancel();
                self.release_commit.cancelled().await;
            }
            let tool_result=request.transaction.writes.iter().any(|write|matches!(write,Write::Entry(NewEntry::Message(entry)) if matches!(&entry.message,AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_)))));
            if tool_result {
                self.commit_trace.lock().unwrap().push("commit".into());
            }
            if (request.transaction.writes.iter().any(|write|matches!(write,Write::SetRegister(value) if matches!(value.as_ref(),RegisterSet::OperationMeta {..}))) && self.late_pending.swap(false,Ordering::SeqCst)) || (tool_result&&self.late_result.swap(false,Ordering::SeqCst)) {
        let Some(Register::LaneState {mut value,..})=self.inner.get_register(RegisterNamespace::LaneState,"main").await.map_err(CheckedCommitError::Storage)? else {panic!("fixture lane");};
        value.pending_next_run.push(EntryId::new("late-next").unwrap());
        let message=pi_ai_core::JsonValue::Object(pi_ai_core::JsonObject::new([("role".into(),pi_ai_core::JsonValue::String("user".into())),("content".into(),pi_ai_core::JsonValue::String("late next run".into())),("timestamp".into(),serde_json::from_value(serde_json::json!(7)).unwrap())].into()));
        self.inner.commit(Transaction {writes:vec![Write::SetRegister(Box::new(RegisterSet::PendingEntry {key:"late-next".into(),value:PendingEntry {entry_type:PendingEntryType::Message,custom_type:None,payload:pi_ai_core::Presence::Value(message)}})),Write::SetRegister(Box::new(RegisterSet::LaneState {key:"main".into(),value}))]}).await.map_err(CheckedCommitError::Storage)?;
    }
            let transaction = request.transaction.clone();
            let result = self.inner.commit_checked(request, validator).await?;
            if matches!(result, ConditionalOutcome::Applied(_)) {
                self.transactions.lock().unwrap().push(transaction);
            }
            Ok(result)
        })
    }
}
impl RuntimeStorage for TrackedStorage {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        self.inner.runtime_attachment()
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        self.record("read_stamp");
        self.inner.read_stamp()
    }
}
