#![allow(dead_code)]
use crate::runtime_r1_r4_support::*;
use pi_agent_schema::*;
use pi_agent_session::memory::MemoryStorage;
use pi_ai_core::*;
use pi_harness_runtime::owner::{RuntimeFactory, RuntimeHarness};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};
pub mod tracking;
pub type Harness = RuntimeHarness<(), MemoryStorage, SessionMetadata>;
pub fn factory(s: &Setup) -> RuntimeFactory<(), MemoryStorage> {
    RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    )
}
pub fn user(text: &str) -> AgentMessage {
    AgentMessage::BuiltIn(Message::User(UserMessage {
        role: Default::default(),
        content: UserContent::Text(text.into()),
        timestamp: n(7),
    }))
}
pub fn prompt() -> PromptInput {
    PromptInput::Text {
        text: "start".into(),
        images: vec![],
    }
}
pub fn tick_until(s: &Setup, mut condition: impl FnMut() -> bool) {
    for _ in 0..2000 {
        if condition() {
            return;
        }
        s.pump.tick();
    }
    panic!("condition did not occur");
}
pub fn current(s: &Setup) -> Option<OperationState> {
    let rows = s
        .pump
        .drive(
            s.session
                .list_registers(RegisterNamespace::OperationState, None),
        )
        .unwrap();
    rows.into_iter().find_map(|r| match r {
        Register::OperationState { value, .. } => Some(*value),
        _ => None,
    })
}
pub struct Listener(pub Mutex<Vec<HarnessEvent>>);
impl EventListener for Listener {
    fn on_event(
        &self,
        event: HarnessEvent,
    ) -> pi_agent_schema::BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event);
            Ok(())
        })
    }
}
pub struct DeferredAdapter {
    pub simple: Arc<Faux>,
    pub replies: Mutex<VecDeque<AssistantMessage>>,
    pub polls: Mutex<Vec<(DeferredHandle, DeferredFetchOptionsWire)>>,
    pub cancels: Mutex<Vec<(DeferredHandle, CancellationToken)>>,
    pub hold_fetch: Mutex<Option<CancellationToken>>,
    pub hold_cancel: Mutex<Option<CancellationToken>>,
    pub models_action: Mutex<Option<Box<dyn Fn() + Send + Sync>>>,
    pub missing_models: std::sync::atomic::AtomicBool,
    pub panic_models: std::sync::atomic::AtomicBool,
    pub cancel_fails: std::sync::atomic::AtomicBool,
}
impl DeferredAdapter {
    pub fn install(s: &Setup) -> Arc<Self> {
        let adapter = Arc::new(Self {
            simple: s.faux.clone(),
            replies: Mutex::new(VecDeque::new()),
            polls: Mutex::new(vec![]),
            cancels: Mutex::new(vec![]),
            hold_fetch: Mutex::new(None),
            hold_cancel: Mutex::new(None),
            models_action: Mutex::new(None),
            missing_models: std::sync::atomic::AtomicBool::new(false),
            panic_models: std::sync::atomic::AtomicBool::new(false),
            cancel_fails: std::sync::atomic::AtomicBool::new(false),
        });
        s.pump
            .drive(s.registry.set_provider(Arc::new(DeferredProduct {
                adapter: adapter.clone(),
                auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
            })))
            .unwrap();
        adapter
    }
    pub fn handle(&self, name: &str) -> DeferredHandle {
        DeferredHandle {
            provider: self.simple.model.provider.clone(),
            model_id: self.simple.model.id.clone(),
            api: self.simple.model.api.clone(),
            id: DeferredId::new(name).unwrap(),
            expires_at: Some(n(400)),
            poll_after_ms: Some(n(3)),
            data: Presence::Null,
        }
    }
    pub fn response(&self, reason: StopReason) -> AssistantMessage {
        let mut m = assistant(&self.simple.model, reason);
        m.usage.input = n(9);
        m.usage.total_tokens = n(9);
        if reason == StopReason::Deferred {
            m.deferred = Some(self.handle("handle"));
        }
        m
    }
    pub fn begin(&self) {
        self.simple.responses.lock().unwrap().clear();
        self.simple
            .responses
            .lock()
            .unwrap()
            .push_back(self.response(StopReason::Deferred));
    }
}
impl ApiAdapter for DeferredAdapter {
    fn api_id(&self) -> &ApiId {
        &self.simple.model.api
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        Some(self)
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        self.simple.stream(r)
    }
    fn stream_simple(&self, r: SimpleStreamRequest) -> AssistantStream {
        self.simple.stream_simple(r)
    }
    fn fetch_deferred(&self, r: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        self.fetch_deferred_with_lifecycle(r, RequestLifecycleInvocation::new(Default::default()).0)
    }
    fn cancel_deferred<'a>(
        &'a self,
        r: DeferredCancelRequest,
    ) -> pi_ai_core::BoxFuture<'a, Result<(), ModelsError>> {
        self.cancels
            .lock()
            .unwrap()
            .push((r.handle, r.options.runtime.cancellation.clone()));
        let hold = self.hold_cancel.lock().unwrap().clone();
        Box::pin(async move {
            if let Some(hold) = hold {
                hold.cancelled().await;
            }
            if self.cancel_fails.load(std::sync::atomic::Ordering::Acquire) {
                Err(ModelsError::new(ModelsErrorCode::Provider))
            } else {
                Ok(())
            }
        })
    }
}
impl RequestLifecycleAdapter for DeferredAdapter {
    fn deferred_lifecycle(&self) -> Option<&dyn DeferredRequestLifecycleAdapter> {
        Some(self)
    }
    fn stream_with_lifecycle(
        &self,
        r: TextStreamRequest,
        l: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.simple.stream_with_lifecycle(r, l)
    }
    fn stream_simple_with_lifecycle(
        &self,
        r: SimpleStreamRequest,
        l: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.simple.stream_simple_with_lifecycle(r, l)
    }
}
impl DeferredRequestLifecycleAdapter for DeferredAdapter {
    fn fetch_deferred_with_lifecycle(
        &self,
        r: DeferredFetchRequest,
        l: RequestLifecycleInvocation,
    ) -> Result<AssistantStream, CapabilityError> {
        self.polls
            .lock()
            .unwrap()
            .push((r.handle.clone(), r.options.wire.clone()));
        let response = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| self.response(StopReason::Stop));
        let hold = self.hold_fetch.lock().unwrap().clone();
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.simple.model.api.clone(),
            provider: self.simple.model.provider.clone(),
            model: self.simple.model.id.clone(),
            clock: r.options.runtime.clock.clone(),
            cancellation: r.options.runtime.cancellation.clone(),
        });
        let model = self.simple.model.clone();
        r.options
            .runtime
            .spawner
            .spawn(Box::pin(async move {
                let mut payload = serde_json::json!({"poll":true});
                if let Some(transform) = l.payload
                    && transform.transform(&model, &mut payload).await.is_err()
                {
                    let _ = producer.fail_setup(StreamSetupFailure::Adapter);
                    return;
                }
                let mut partial = response.clone();
                partial.stop_reason = StopReason::Pending;
                partial.error_message = None;
                partial.deferred = None;
                let _ = producer.push(AssistantMessageEvent::Start {
                    partial: Box::new(partial),
                });
                if let Some(hold) = hold {
                    hold.cancelled().await;
                }
                l.recorder.record(RequestResponseHead {
                    status: 200,
                    headers: vec![],
                });
                let event = match response.stop_reason {
                    StopReason::Error => AssistantMessageEvent::Error {
                        reason: ErrorReason::Error,
                        error: Box::new(response),
                    },
                    StopReason::Aborted => AssistantMessageEvent::Error {
                        reason: ErrorReason::Aborted,
                        error: Box::new(response),
                    },
                    r => AssistantMessageEvent::Done {
                        reason: match r {
                            StopReason::Deferred => DoneReason::Deferred,
                            StopReason::ToolUse => DoneReason::ToolUse,
                            StopReason::Length => DoneReason::Length,
                            _ => DoneReason::Stop,
                        },
                        message: Box::new(response),
                    },
                };
                let _ = producer.push(event);
            }))
            .unwrap();
        Ok(stream)
    }
}
struct DeferredProduct {
    adapter: Arc<DeferredAdapter>,
    auth: ProviderAuth,
}
impl Provider for DeferredProduct {
    fn id(&self) -> &ProviderId {
        &self.adapter.simple.model.provider
    }
    fn name(&self) -> &str {
        "deferred-offline"
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        assert!(
            !self
                .adapter
                .panic_models
                .load(std::sync::atomic::Ordering::Acquire),
            "cancel model fixture panic"
        );
        let action = self.adapter.models_action.lock().unwrap().take();
        if let Some(action) = action {
            action();
        }
        if self
            .adapter
            .missing_models
            .load(std::sync::atomic::Ordering::Acquire)
        {
            Ok(Vec::<Model>::new().into())
        } else {
            Ok(vec![self.adapter.simple.model.clone()].into())
        }
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        (api == self.adapter.api_id()).then(|| self.adapter.clone() as Arc<dyn ApiAdapter>)
    }
}
pub fn tracked_session(
    s: &Setup,
    tracked: Arc<tracking::TrackedStorage>,
) -> Arc<pi_agent_session::session::RuntimeSession<tracking::TrackedStorage>> {
    Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                tracked,
                s.session.metadata(),
                Arc::new(
                    pi_agent_session::codec::SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::default(),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    )
}
pub fn reopen(s: &Setup) -> Arc<pi_agent_session::session::RuntimeSession<MemoryStorage>> {
    let storage = s.backing.open(s.time.clone()).unwrap();
    Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                storage,
                s.session.metadata(),
                Arc::new(
                    pi_agent_session::codec::SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::default(),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    )
}
pub fn manual_until(s: &Setup, h: &dyn AgentLane, mut condition: impl FnMut() -> bool) {
    for _ in 0..500 {
        if condition() {
            return;
        }
        s.pump.drive(h.execute_action()).unwrap();
        s.pump.tick();
    }
    panic!("manual condition did not occur");
}
