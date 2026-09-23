//! Test-only deterministic application callbacks; all runtime/storage paths are real.
use crate::runtime_r1_r4_support::{Setup, Summary, assistant, n};
use pi_agent_schema::*;
use pi_agent_session::{memory::MemoryStorage, storage::*};
use pi_ai_core::{
    AssistantContent, CancellationToken, JsonObject, JsonValue, Presence, StopReason, TextContent,
    ToolCall, ToolCallId, ToolName, Usage,
};
use pi_harness_runtime::owner::RuntimeFactory;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
pub fn factory(s: &Setup) -> RuntimeFactory<(), pi_agent_session::memory::MemoryStorage> {
    RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    )
}
pub fn prompt() -> PromptInput {
    PromptInput::Text {
        text: "retirement probe".into(),
        images: vec![],
    }
}
pub struct Projector {
    pub entered: AtomicUsize,
    pub dropped: Arc<AtomicUsize>,
    pub release: CancellationToken,
}
impl Default for Projector {
    fn default() -> Self {
        Self {
            entered: AtomicUsize::new(0),
            dropped: Arc::new(AtomicUsize::new(0)),
            release: CancellationToken::new(),
        }
    }
}
struct DropMark(Arc<AtomicUsize>);
impl Drop for DropMark {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl EntryProjector for Projector {
    fn project(
        &self,
        _: CustomEntry,
    ) -> BoxFuture<'_, Result<Option<Vec<AgentMessage>>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let _mark = DropMark(self.dropped.clone());
            self.release.cancelled().await;
            Ok(Some(vec![
                serde_json::from_value(
                    serde_json::json!({"role":"user","content":"projected context","timestamp":7}),
                )
                .unwrap(),
            ]))
        })
    }
}
#[derive(Default)]
pub struct FaultPrompt {
    pub fail: AtomicBool,
    pub calls: AtomicUsize,
}
impl SystemPromptProvider<()> for FaultPrompt {
    fn resolve<'a>(&'a self, _: &'a ()) -> BoxFuture<'a, Result<String, HarnessRuntimeError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail.load(Ordering::SeqCst) {
                Err(HarnessRuntimeError::Fault(HarnessFault::new(
                    ErrorContext::new("explicit retirement probe Fault").unwrap(),
                )))
            } else {
                Ok("normal prompt".into())
            }
        })
    }
}
pub struct Tool {
    descriptor: AgentTool,
    pub started: AtomicUsize,
    pub raw_done: AtomicUsize,
    pub release: std::sync::Mutex<Option<CancellationToken>>,
}
impl Tool {
    pub fn new() -> Arc<Self> {
        Arc::new(Self{descriptor:AgentTool{name:ToolName::new("retire-tool").unwrap(),label:"retire".into(),description:"deterministic tool".into(),parameters:serde_json::from_value(serde_json::json!({"type":"object","properties":{},"additionalProperties":false})).unwrap(),constrained_sampling:None,execution_mode:None,replay:Some(ToolReplay::Safe)},started:AtomicUsize::new(0),raw_done:AtomicUsize::new(0),release:std::sync::Mutex::new(None)})
    }
}
pub fn raw_result() -> AgentToolResult {
    let mut usage = Usage::zero();
    usage.input = n(5);
    usage.total_tokens = n(5);
    AgentToolResult {
        content: vec![pi_ai_core::InputContent::Text(TextContent {
            content_type: Default::default(),
            text: "actual raw result".into(),
            text_signature: None,
        })],
        details: JsonValue::String("actual raw details".into()),
        usage: Some(usage),
        added_tool_names: None,
        terminate: Some(false),
    }
}
impl AgentHarnessTool<()> for Tool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, args: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        let JsonValue::Object(args) = args else {
            panic!("object fixture arguments")
        };
        Ok(args)
    }
    fn execute<'a>(
        &'a self,
        _: ToolCallId,
        _: JsonObject,
        _: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        _: &'a (),
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        self.started.fetch_add(1, Ordering::SeqCst);
        let release = self.release.lock().unwrap().clone();
        Box::pin(async move {
            if let Some(release) = release {
                release.cancelled().await;
            }
            self.raw_done.fetch_add(1, Ordering::SeqCst);
            Ok(raw_result())
        })
    }
}
#[derive(Default)]
pub struct AfterToolCount(pub AtomicUsize);
impl HookHandler<AfterTool> for AfterToolCount {
    fn call(
        &self,
        _: AfterToolEvent,
    ) -> BoxFuture<'_, Result<Option<AfterToolResult>, HandlerError>> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(None) })
    }
}
pub fn tool_response(s: &Setup) -> pi_ai_core::AssistantMessage {
    let mut response = assistant(&s.faux.model, StopReason::ToolUse);
    response.content = vec![AssistantContent::ToolCall(ToolCall {
        content_type: Default::default(),
        id: ToolCallId::new("retire-call").unwrap(),
        name: ToolName::new("retire-tool").unwrap(),
        arguments: JsonObject::default(),
        thought_signature: None,
        namespace: None,
    })];
    response
}
pub fn delete(namespace: RegisterNamespace, key: String) -> Write {
    Write::DeleteRegister(RegisterDelete { namespace, key })
}
pub fn external_terminal(s: &Setup) -> (OperationId, EntryId, LaneLastResult) {
    let Some(Register::LaneState { mut value, .. }) = s
        .pump
        .drive(s.session.get_register(RegisterNamespace::LaneState, "main"))
        .unwrap()
    else {
        panic!()
    };
    let operation = value.current_operation_id.take().unwrap();
    let Some(Register::OperationState { value: state, .. }) = s
        .pump
        .drive(
            s.session
                .get_register(RegisterNamespace::OperationState, operation.as_str()),
        )
        .unwrap()
    else {
        panic!()
    };
    let OperationState::Run(run) = state.as_ref() else {
        panic!()
    };
    let RunPhase::Tools(phase) = &run.phase else {
        panic!()
    };
    assert_eq!(phase.batch.calls.len(), 1);
    let ToolCallState::EffectPending {
        result_entry_id, ..
    } = &phase.batch.calls[0]
    else {
        panic!()
    };
    let result_id = result_entry_id.clone();
    let assistant = phase.batch.assistant_entry_id.clone();
    let result = raw_result();
    let usage = result.usage.clone().unwrap();
    let message = pi_ai_core::ToolResultMessage {
        role: Default::default(),
        tool_call_id: ToolCallId::new("retire-call").unwrap(),
        tool_name: ToolName::new("retire-tool").unwrap(),
        content: result.content,
        details: Presence::Value(result.details),
        usage: Some(usage.clone()),
        added_tool_names: None,
        is_error: false,
        timestamp: n(7),
    };
    let terminal = LaneLastResult::RunAborted(RunAbortedLastResult {
        operation_id: operation.clone(),
        kind: Default::default(),
        leaf_id: Some(result_id.clone()),
        final_assistant_entry_id: Some(assistant),
        outcome: Default::default(),
    });
    let mut writes = vec![
        Write::Entry(NewEntry::Message(NewMessageEntry {
            id: result_id.clone(),
            parent_id: s.pump.drive(s.session.get_leaf_id()).unwrap(),
            entry_type: Default::default(),
            message: AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(Box::new(message))),
            terminate: None,
        })),
        Write::Usage(NewUsageRow {
            id: UsageId::new(s.ids.next(None)).unwrap(),
            usage,
            entry_id: Some(result_id.clone()),
            adjustment: false,
            details: Presence::Missing,
        }),
        Write::SetRegister(Box::new(RegisterSet::LaneLeaf {
            key: "main".into(),
            value: Some(result_id.clone()),
        })),
        delete(RegisterNamespace::OperationState, operation.as_str().into()),
        delete(RegisterNamespace::OperationMeta, operation.as_str().into()),
    ];
    for namespace in [
        RegisterNamespace::OperationToolArguments,
        RegisterNamespace::OperationPreparation,
    ] {
        let prefix = format!("{}:", operation.as_str());
        for r in s
            .pump
            .drive(s.session.list_registers(namespace, Some(&prefix)))
            .unwrap()
        {
            let key = match r {
                Register::OperationToolArguments { key, .. }
                | Register::OperationPreparation { key, .. } => key,
                _ => panic!(),
            };
            writes.push(delete(namespace, key));
        }
    }
    assert!(
        run.inbox.steer.is_empty() && run.inbox.follow_up.is_empty() && run.inbox.writes.is_empty()
    );
    writes.push(Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
        key: "main".into(),
        value: terminal.clone(),
    })));
    writes.push(Write::SetRegister(Box::new(RegisterSet::LaneState {
        key: "main".into(),
        value,
    })));
    s.pump
        .drive(s.session.commit(Transaction { writes }))
        .expect("legitimate full external terminal must pass actual Session validation");
    (operation, result_id, terminal)
}

/// Parks the second post-raw read stamp after observing its value. Hydration
/// has then captured one complete old state, while a raw Session writer can
/// publish a valid terminal before the runtime consumes that observation.
pub struct AfterRawStamp {
    inner: Arc<MemoryStorage>,
    tool: Arc<Tool>,
    park_post_raw: bool,
    post_raw_stamps: AtomicUsize,
    pub entered: CancellationToken,
    pub release: CancellationToken,
    pub settle_armed: AtomicBool,
    pub settle_entered: CancellationToken,
    pub settle_release: CancellationToken,
    pub settle_not_applied: AtomicUsize,
}
impl AfterRawStamp {
    pub fn new(inner: Arc<MemoryStorage>, tool: Arc<Tool>) -> Self {
        Self {
            inner,
            tool,
            park_post_raw: true,
            post_raw_stamps: AtomicUsize::new(0),
            entered: CancellationToken::new(),
            release: CancellationToken::new(),
            settle_armed: AtomicBool::new(false),
            settle_entered: CancellationToken::new(),
            settle_release: CancellationToken::new(),
            settle_not_applied: AtomicUsize::new(0),
        }
    }
    pub fn for_checked_settlement(inner: Arc<MemoryStorage>, tool: Arc<Tool>) -> Self {
        let mut storage = Self::new(inner, tool);
        storage.park_post_raw = false;
        storage
    }
}
impl Storage for AfterRawStamp {
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
impl AtomicStorage for AfterRawStamp {
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
impl CheckedStorage for AfterRawStamp {
    fn commit_checked<'a, V: PublicationValidator + ?Sized>(
        &'a self,
        request: ConditionalCommit,
        validator: &'a V,
    ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>> {
        Box::pin(async move {
            if self.settle_armed.swap(false, Ordering::SeqCst) {
                self.settle_entered.cancel();
                self.settle_release.cancelled().await;
            }
            let outcome = self.inner.commit_checked(request, validator).await?;
            if matches!(outcome, ConditionalOutcome::NotApplied) {
                self.settle_not_applied.fetch_add(1, Ordering::SeqCst);
            }
            Ok(outcome)
        })
    }
}
impl RuntimeStorage for AfterRawStamp {
    fn runtime_attachment(&self) -> &RuntimeAttachment {
        self.inner.runtime_attachment()
    }
    fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
        Box::pin(async move {
            let stamp = self.inner.read_stamp().await?;
            if self.park_post_raw
                && self.tool.raw_done.load(Ordering::SeqCst) == 1
                && self.post_raw_stamps.fetch_add(1, Ordering::SeqCst) == 1
            {
                self.entered.cancel();
                self.release.cancelled().await;
            }
            Ok(stamp)
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
            if self.settle_armed.swap(false, Ordering::SeqCst) {
                self.settle_entered.cancel();
                self.settle_release.cancelled().await;
            }
            let outcome = self
                .inner
                .commit_prepared_checked(prepared, request, validator)
                .await?;
            if matches!(outcome, ConditionalOutcome::NotApplied) {
                self.settle_not_applied.fetch_add(1, Ordering::SeqCst);
            }
            Ok(outcome)
        })
    }
}
