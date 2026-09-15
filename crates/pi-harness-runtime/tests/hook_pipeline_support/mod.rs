use pi_agent_schema::*;
use pi_agent_session::codec::SessionCodec;
use pi_ai_core::*;
use pi_harness_runtime::hooks::*;
use pi_telemetry::*;
use std::{
    future::Future,
    marker::PhantomData,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
};

pub fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
pub fn ready<F: Future>(future: F) -> F::Output {
    match poll(Box::pin(future).as_mut()) {
        Poll::Ready(r) => r,
        Poll::Pending => panic!("expected ready"),
    }
}
pub fn obj<const N: usize>(entries: [(&str, JsonValue); N]) -> JsonObject {
    JsonObject::new(entries.into_iter().map(|(k, v)| (k.into(), v)).collect())
}
pub fn s(v: &str) -> JsonValue {
    JsonValue::String(v.into())
}
pub fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
pub fn id(v: &str) -> HandlerId {
    HandlerId::new(v).unwrap()
}
pub fn lane() -> LaneId {
    LaneId::new("main").unwrap()
}
pub fn run() -> OperationId {
    OperationId::new("run").unwrap()
}
pub fn err() -> HandlerError {
    HandlerError::new(ErrorContext::new("ordinary bounded error").unwrap())
}
pub fn model() -> Model {
    let z = FiniteF64::new(0.0).unwrap();
    RawModel {
        id: ModelId::new("model").unwrap(),
        name: "model".into(),
        api: ApiId::new("test").unwrap(),
        provider: ProviderId::new("test").unwrap(),
        base_url: "https://example.invalid".into(),
        reasoning: false,
        thinking_level_map: None,
        input: vec![ModelInput::Text],
        cost: ModelCost {
            input: z,
            output: z,
            cache_read: z,
            cache_write: z,
            tiers: None,
        },
        context_window: n(100),
        max_tokens: n(10),
        sampling_params: None,
        headers: None,
        compat: None,
    }
    .into_model(&Default::default())
    .unwrap()
}
pub fn assistant(reason: StopReason) -> AssistantMessage {
    AssistantMessage {
        role: Default::default(),
        content: vec![],
        api: ApiId::new("test").unwrap(),
        provider: ProviderId::new("test").unwrap(),
        model: ModelId::new("model").unwrap(),
        response_model: None,
        response_id: None,
        diagnostics: None,
        usage: Usage::zero(),
        stop_reason: reason,
        deferred: None,
        error_message: matches!(reason, StopReason::Aborted | StopReason::Error)
            .then(|| "terminal".into()),
        raw_stop_reason: None,
        end_turn: None,
        timestamp: n(0),
    }
}
pub fn custom(value: JsonValue) -> AgentMessage {
    AgentMessage::Custom(
        CustomAgentMessage::new(CustomRole::new("note").unwrap(), obj([("value", value)])).unwrap(),
    )
}
pub fn invalid_native() -> AgentMessage {
    AgentMessage::BuiltIn(Message::Assistant(Box::new(assistant(StopReason::Pending))))
}
pub fn before_run() -> HookEvent {
    HookEvent::BeforeRun(BeforeRunEvent {
        lane: lane(),
        run_id: run(),
        prompt: vec![],
        system_prompt: "base".into(),
        resources: Default::default(),
    })
}
pub fn request(options: AgentHarnessStreamOptions) -> HookEvent {
    HookEvent::BeforeRequest(Box::new(BeforeRequestEvent {
        lane: lane(),
        run_id: run(),
        model: model(),
        step: HookRequestStep::Assistant,
        attempt: OneBasedSafeUnsigned::new(1).unwrap(),
        stream_options: options,
    }))
}
pub fn before_tool() -> HookEvent {
    HookEvent::BeforeTool(BeforeToolEvent {
        lane: lane(),
        run_id: run(),
        tool_call_id: ToolCallId::new("call").unwrap(),
        tool_name: ToolName::new("tool").unwrap(),
        args: obj([("count", s("1"))]),
    })
}
pub fn preparation() -> CompactionPreparation {
    CompactionPreparation {
        messages_to_summarize: vec![],
        turn_prefix_messages: vec![],
        retained_tail: vec![],
        is_split_turn: false,
        tokens_before: n(0),
        previous_summary: None,
        file_ops: Default::default(),
        settings: CompactionSettings {
            enabled: true,
            reserve_tokens: n(0),
            keep_recent_tokens: n(0),
        },
    }
}
pub fn compact(tail: Vec<AgentMessage>) -> BeforeCompactionResult {
    BeforeCompactionResult::Compaction(Box::new(BeforeCompactionSupply {
        decline: None,
        compaction: CompactResult {
            summary: "".into(),
            tokens_before: n(0),
            retained_tail: tail,
            usage: None,
            details: Presence::Null,
        },
    }))
}

struct Handler<S, F>(F, PhantomData<fn() -> S>);
impl<S: HookSpec, F: Fn(S::Event) -> Result<S::Result, HandlerError> + Send + Sync> HookHandler<S>
    for Handler<S, F>
{
    fn call(
        &self,
        event: S::Event,
    ) -> pi_agent_schema::surface::BoxFuture<'_, Result<S::Result, HandlerError>> {
        let result = (self.0)(event);
        Box::pin(async move { result })
    }
}
pub fn handler<S: HookSpec>(
    f: impl Fn(S::Event) -> Result<S::Result, HandlerError> + Send + Sync + 'static,
) -> Arc<dyn HookHandler<S>> {
    Arc::new(Handler(f, PhantomData))
}
struct AsyncHandler<S, F>(F, PhantomData<fn() -> S>);
impl<
    S: HookSpec,
    F: Fn(
            S::Event,
        ) -> pi_agent_schema::surface::BoxFuture<'static, Result<S::Result, HandlerError>>
        + Send
        + Sync,
> HookHandler<S> for AsyncHandler<S, F>
{
    fn call(
        &self,
        event: S::Event,
    ) -> pi_agent_schema::surface::BoxFuture<'_, Result<S::Result, HandlerError>> {
        (self.0)(event)
    }
}
pub fn async_handler<S: HookSpec>(
    f: impl Fn(
        S::Event,
    ) -> pi_agent_schema::surface::BoxFuture<'static, Result<S::Result, HandlerError>>
    + Send
    + Sync
    + 'static,
) -> Arc<dyn HookHandler<S>> {
    Arc::new(AsyncHandler(f, PhantomData))
}
#[derive(Default)]
pub struct Signal(pub AtomicBool);
impl Cancellation for Signal {
    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
    fn cancelled(&self) -> pi_agent_schema::surface::BoxFuture<'_, ()> {
        Box::pin(std::future::pending())
    }
}
#[derive(Default)]
pub struct Observations {
    pub trace: Mutex<Vec<String>>,
    pub errors: Mutex<Vec<LaneHookHandlerErrorEvent>>,
    pub starts: Mutex<Vec<SpanOptions>>,
    pub ends: Mutex<Vec<SpanAttributes>>,
    pub statuses: Mutex<Vec<SpanStatus>>,
}
impl Observations {
    pub fn mark(&self, s: &str) {
        self.trace.lock().unwrap().push(s.into());
    }
}
impl HookErrorSink for Observations {
    fn report(&self, event: LaneHookHandlerErrorEvent) {
        self.mark("error");
        self.errors.lock().unwrap().push(event);
    }
}
pub struct TraceContext(Arc<Observations>);
impl TelemetryContext for TraceContext {
    fn start_span(&self, o: SpanOptions) -> SpanGuard {
        self.0.mark("start");
        self.0.starts.lock().unwrap().push(o);
        SpanGuard::new(Span(self.0.clone()))
    }
}
struct Span(Arc<Observations>);
impl TelemetrySpan for Span {
    fn start_child(&self, _: SpanOptions) -> SpanGuard {
        panic!("unexpected child")
    }
    fn add_event(&self, _: &str, _: &SpanAttributes) {
        panic!("unexpected event")
    }
    fn set_attributes(&self, a: &SpanAttributes) {
        self.0.ends.lock().unwrap().push(a.clone());
    }
    fn set_status(&self, s: SpanStatus) {
        self.0.statuses.lock().unwrap().push(s);
    }
    fn finish(&self) {
        self.0.mark("finish");
    }
}
pub struct Env {
    pub codec: SessionCodec,
    pub policy: ProviderHeaderPolicy,
    pub tool: Tool,
    pub signal: Arc<Signal>,
    pub seen: Arc<Observations>,
    pub telemetry: TraceContext,
    pub saved: JsonObject,
}
impl Env {
    pub fn new() -> Self {
        let policy = ProviderHeaderPolicy::from_allowlist(["x-demo".into()]);
        let schema = JsonSchema(obj([
            ("type", s("object")),
            ("required", JsonValue::Array(vec![s("value")])),
            (
                "properties",
                JsonValue::Object(obj([(
                    "value",
                    JsonValue::Object(obj([("type", s("string"))])),
                )])),
            ),
        ]));
        let codec = SessionCodec::new(
            SessionCodecOptions::new([(CustomRole::new("note").unwrap(), schema)].into()),
            policy.clone(),
        )
        .unwrap();
        let tool = Tool {
            name: ToolName::new("tool").unwrap(),
            description: "".into(),
            parameters: JsonSchema(obj([
                ("type", s("object")),
                ("required", JsonValue::Array(vec![s("count")])),
                (
                    "properties",
                    JsonValue::Object(obj([
                        ("count", JsonValue::Object(obj([("type", s("integer"))]))),
                        ("optional", JsonValue::Object(obj([("type", s("string"))]))),
                    ])),
                ),
            ])),
            constrained_sampling: None,
        };
        let seen = Arc::new(Observations::default());
        Self {
            codec,
            policy,
            tool,
            signal: Arc::new(Signal::default()),
            telemetry: TraceContext(seen.clone()),
            seen,
            saved: Default::default(),
        }
    }
    pub fn context(&self) -> StartedHookContext<'_> {
        StartedHookContext {
            codec: &self.codec,
            header_policy: &self.policy,
            tool: Some(&self.tool),
            cancellation: self.signal.as_ref(),
            telemetry: &self.telemetry,
            operation_accepted: true,
            recovery: false,
            resume_data: &self.saved,
            errors: self.seen.as_ref(),
        }
    }
    pub fn run(&self, r: &HookRegistry, e: HookEvent) -> HookPipelineOutput {
        ready(r.capture(e.name()).run_started(e, &self.context())).unwrap()
    }
}
