use pi_agent_schema::*;
use pi_agent_session::storage::*;
use pi_ai_core::CancellationToken;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
pub struct GateStorage<S> {
    pub inner: Arc<S>,
    pub arm_read: AtomicBool,
    pub arm_cas: AtomicBool,
    pub entered: CancellationToken,
    pub release: CancellationToken,
    pub refused: AtomicUsize,
}
impl<S> GateStorage<S> {
    pub fn new(inner: Arc<S>) -> Self {
        Self {
            inner,
            arm_read: AtomicBool::new(false),
            arm_cas: AtomicBool::new(false),
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
            refused: AtomicUsize::new(0),
        }
    }
}
impl<S: RuntimeStorage> Storage for GateStorage<S> {
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
impl<S: RuntimeStorage> AtomicStorage for GateStorage<S> {
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
impl<S: RuntimeStorage> CheckedStorage for GateStorage<S> {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move {
            if request.transaction.writes.iter().any(|write|matches!(write,Write::SetRegister(register) if matches!(register.as_ref(),RegisterSet::PendingEntry{..})))&&self.arm_cas.swap(false,Ordering::SeqCst){self.entered.cancel();self.release.cancelled().await;}
            let result = self.inner.commit_checked(request, validator).await?;
            if matches!(result, ConditionalOutcome::NotApplied) {
                self.refused.fetch_add(1, Ordering::SeqCst);
            }
            Ok(result)
        })
    }
}
impl<S: RuntimeStorage> RuntimeStorage for GateStorage<S> {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        self.inner.runtime_attachment()
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        Box::pin(async move {
            if self.arm_read.swap(false, Ordering::SeqCst) {
                self.entered.cancel();
                self.release.cancelled().await;
            }
            self.inner.read_stamp().await
        })
    }
    fn prepare_entries(
        &self,
        stamp: ReadStamp,
        slots: Vec<PreparedEntrySlot>,
    ) -> BoxFuture<'_, Result<Option<PreparedEntries>, StorageError>> {
        self.inner.prepare_entries(stamp, slots)
    }
    fn commit_prepared_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        prepared: PreparedEntries,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move {
            if request.transaction.writes.iter().any(|write|matches!(write,Write::SetRegister(register) if matches!(register.as_ref(),RegisterSet::PendingEntry{..})))&&self.arm_cas.swap(false,Ordering::SeqCst){self.entered.cancel();self.release.cancelled().await;}
            let result = self
                .inner
                .commit_prepared_checked(prepared, request, validator)
                .await?;
            if matches!(result, ConditionalOutcome::NotApplied) {
                self.refused.fetch_add(1, Ordering::SeqCst);
            }
            Ok(result)
        })
    }
}
pub struct CountingIds {
    pub inner: Arc<dyn IdGenerator>,
    pub count: AtomicUsize,
}
impl IdGenerator for CountingIds {
    fn next(&self, hint: Option<pi_ai_core::SafeUnsigned>) -> String {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.next(hint)
    }
}
