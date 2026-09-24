//! Test-only gate around the real SQLite writer. It never supplies a CAS result.
use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_schema::*;
use pi_agent_session::storage::*;
use pi_agent_sqlite::SqliteStorage;
use pi_ai_core::CancellationToken;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

pub struct File(pub std::path::PathBuf);
impl File {
    pub fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "pi-runtime-sqlite-tool-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path.join("session.db"))
    }
}
impl Drop for File {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.0.parent().unwrap()).unwrap();
    }
}

pub struct CheckedSqlite {
    pub inner: Arc<SqliteStorage>,
    pub attempts: AtomicUsize,
    pub refused: AtomicUsize,
    pub applied: AtomicUsize,
    pub first_entered: CancellationToken,
    pub first_release: CancellationToken,
    pub second_entered: CancellationToken,
    pub second_release: CancellationToken,
    pub second_timestamp: Mutex<Option<SafeUnsigned>>,
    pub application_clock_calls: Arc<dyn Fn() -> u64 + Send + Sync>,
    pub clock_calls_during_commit: AtomicUsize,
    pub preparing_result: AtomicBool,
}
impl CheckedSqlite {
    pub fn new(
        inner: Arc<SqliteStorage>,
        application_clock_calls: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        Self {
            inner,
            attempts: AtomicUsize::new(0),
            refused: AtomicUsize::new(0),
            applied: AtomicUsize::new(0),
            first_entered: CancellationToken::new(),
            first_release: CancellationToken::new(),
            second_entered: CancellationToken::new(),
            second_release: CancellationToken::new(),
            second_timestamp: Mutex::new(None),
            application_clock_calls,
            clock_calls_during_commit: AtomicUsize::new(0),
            preparing_result: AtomicBool::new(false),
        }
    }
}
impl Storage for CheckedSqlite {
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
        self.inner.get_entries(ids)
    }
    fn get_register<'a>(
        &'a self,
        namespace: RegisterNamespace,
        key: &'a str,
    ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
        self.inner.get_register(namespace, key)
    }
    fn list_registers<'a>(
        &'a self,
        namespace: RegisterNamespace,
        prefix: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
        self.inner.list_registers(namespace, prefix)
    }
    fn scan_branch(&self, query: BranchScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.inner.scan_branch(query)
    }
    fn scan_branch_structure(
        &self,
        query: BranchScan,
    ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
        self.inner.scan_branch_structure(query)
    }
    fn scan_entries(&self, query: EntryScan) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
        self.inner.scan_entries(query)
    }
    fn scan_usage(&self, query: UsageScan) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
        self.inner.scan_usage(query)
    }
    fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
        self.inner.stats()
    }
    fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
        self.inner.close()
    }
}
impl AtomicStorage for CheckedSqlite {
    fn commit_conditional(
        &self,
        request: ConditionalCommit,
    ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
        self.inner.commit_conditional(request)
    }
    fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
        self.inner.snapshot()
    }
}
impl CheckedStorage for CheckedSqlite {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        self.inner.commit_checked(request, validator)
    }
}
impl RuntimeStorage for CheckedSqlite {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        self.inner.runtime_attachment()
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        self.inner.read_stamp()
    }
    fn prepare_entries(
        &self,
        stamp: ReadStamp,
        slots: Vec<PreparedEntrySlot>,
    ) -> BoxFuture<'_, Result<Option<PreparedEntries>, StorageError>> {
        Box::pin(async move {
            let result = slots.iter().any(|slot| matches!(&slot.entry, NewEntry::Message(message)
                if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_)))));
            if result {
                self.preparing_result.store(true, Ordering::SeqCst);
            }
            let prepared = self.inner.prepare_entries(stamp, slots).await;
            if result {
                self.preparing_result.store(false, Ordering::SeqCst);
            }
            prepared
        })
    }
    fn commit_prepared_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        prepared: PreparedEntries,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move {
            let attempt = self.attempts.fetch_add(1, Ordering::SeqCst);
            assert!(prepared.entries().iter().any(|entry| matches!(entry, Entry::Message(message) if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))));
            match attempt {
                0 => {
                    self.first_entered.cancel();
                    self.first_release.cancelled().await;
                }
                1 => {
                    let Entry::Message(message) = prepared.entries().iter().find(|entry| matches!(entry, Entry::Message(message) if matches!(&message.message, AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(_))))).unwrap() else { unreachable!() };
                    *self.second_timestamp.lock().unwrap() = Some(message.timestamp);
                    self.second_entered.cancel();
                    self.second_release.cancelled().await;
                }
                _ => panic!("unexpected extra ToolResult CAS attempt"),
            }
            let calls_before = (self.application_clock_calls)();
            let outcome = self
                .inner
                .commit_prepared_checked(prepared, request, validator)
                .await?;
            let calls_after = (self.application_clock_calls)();
            self.clock_calls_during_commit
                .fetch_add((calls_after - calls_before) as usize, Ordering::SeqCst);
            match &outcome {
                ConditionalOutcome::NotApplied => {
                    self.refused.fetch_add(1, Ordering::SeqCst);
                }
                ConditionalOutcome::Applied(_) => {
                    self.applied.fetch_add(1, Ordering::SeqCst);
                }
            }
            Ok(outcome)
        })
    }
}
