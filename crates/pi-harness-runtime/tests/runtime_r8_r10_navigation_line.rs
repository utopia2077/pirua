//! Invalid unsummarized navigation holds one lane job through its target read.
mod runtime_r1_r4_support;
mod runtime_r5_r7_support;

use pi_agent_schema::*;
use pi_agent_session::{
    codec::SessionCodec,
    memory::MemoryStorage,
    session::RuntimeSession,
    storage::{
        AtomicStorage, CheckedCommitError, CheckedStorage, ConditionalCommit, ConditionalOutcome,
        PreparedEntries, PreparedEntrySlot, PublicationValidator, ReadStamp, RuntimeAttachment,
        RuntimeStorage, StorageSnapshot,
    },
};
use pi_ai_core::CancellationToken;
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::user;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct ParkTarget {
    inner: Arc<MemoryStorage>,
    target: Mutex<Option<EntryId>>,
    armed: AtomicBool,
    stamp_reads: AtomicUsize,
    entered: CancellationToken,
    release: CancellationToken,
}
impl ParkTarget {
    fn new(inner: Arc<MemoryStorage>) -> Self {
        Self {
            inner,
            target: Mutex::new(None),
            armed: AtomicBool::new(false),
            stamp_reads: AtomicUsize::new(0),
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
        }
    }
}
impl Storage for ParkTarget {
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
        Box::pin(async move {
            let target = self.target.lock().unwrap().clone();
            if target
                .as_ref()
                .is_some_and(|target| ids == std::slice::from_ref(target))
                // Hydration reads sit between its two stamps; the direct
                // navigation target point-read follows the completed pair.
                && self.stamp_reads.load(Ordering::SeqCst).is_multiple_of(2)
                && self.armed.swap(false, Ordering::SeqCst)
            {
                self.entered.cancel();
                self.release.cancelled().await;
            }
            self.inner.get_entries(ids).await
        })
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
impl AtomicStorage for ParkTarget {
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
impl CheckedStorage for ParkTarget {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        self.inner.commit_checked(request, validator)
    }
}
impl RuntimeStorage for ParkTarget {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        self.inner.runtime_attachment()
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        self.stamp_reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_stamp()
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
        self.inner
            .commit_prepared_checked(prepared, request, validator)
    }
}

#[test]
fn invalid_unsummarized_target_holds_lane_line_without_provisional_busy() {
    let s = Setup::new();
    let storage = Arc::new(ParkTarget::new(s.store.clone()));
    let session = Arc::new(
        s.pump
            .drive(RuntimeSession::new(
                storage.clone(),
                s.session.metadata(),
                Arc::new(
                    SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::default(),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    let factory = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let h = s
        .pump
        .drive(factory.create(s.options_for(session, DriveMode::Automatic)))
        .unwrap()
        .harness;
    let leaf = s
        .pump
        .drive(h.session().append_message(user("current leaf")))
        .unwrap();
    let before = s.pump.drive(storage.snapshot()).unwrap();
    *storage.target.lock().unwrap() = Some(leaf.clone());
    storage.stamp_reads.store(0, Ordering::SeqCst);
    storage.armed.store(true, Ordering::SeqCst);
    let mut navigation = Box::pin(h.navigate_tree(
        Some(leaf.clone()),
        NavigateOptions {
            summarize: Some(false),
            ..Default::default()
        },
    ));
    assert!(poll(navigation.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
        if storage.entered.is_cancelled() {
            break;
        }
    }
    assert!(
        storage.entered.is_cancelled(),
        "actual target get_entries must be parked"
    );
    let mut prompt = Box::pin(h.prompt(PromptInput::Text {
        text: "competing prompt".into(),
        images: vec![],
    }));
    for _ in 0..20 {
        assert!(
            poll(prompt.as_mut()).is_pending(),
            "prompt must wait for navigation's lane job"
        );
        s.pump.tick();
    }
    assert_eq!(
        s.pump.drive(storage.snapshot()).unwrap(),
        before,
        "the invalid navigation has no writes while target is parked"
    );
    storage.release.cancel();
    let navigation_result = s.pump.drive(navigation).unwrap();
    assert!(
        matches!(
            navigation_result,
            Err(NavigationExpectedError::InvalidNavigation(_))
        ),
        "{navigation_result:?}"
    );
    let after_rejection = s.pump.drive(storage.snapshot()).unwrap();
    assert!(
        after_rejection
            .entries
            .iter()
            .all(|entry| matches!(entry, Entry::Message(_)))
    );
    assert!(!after_rejection.registers.iter().any(|register| matches!(
        register,
        Register::OperationMeta { value, .. } if matches!(&value.intent, OperationIntent::Navigation(_))
    ) || matches!(
        register,
        Register::OperationState { value, .. } if matches!(value.as_ref(), OperationState::Navigation(_))
    ) || matches!(
        register,
        Register::LaneLastResult { value, .. } if matches!(value, LaneLastResult::NavigationCompleted(_) | LaneLastResult::NavigationAborted(_) | LaneLastResult::NavigationFailed(_) | LaneLastResult::NavigationDeclined(_))
    )));
    let prompt_result = s.pump.drive(prompt).unwrap();
    assert!(
        prompt_result.is_ok(),
        "prompt should accept after rejection: {prompt_result:?}"
    );
    assert!(matches!(
        s.pump.drive(h.get_last_result()).unwrap(),
        Some(LaneLastResult::RunCompletedAssistant(_))
    ));
}
