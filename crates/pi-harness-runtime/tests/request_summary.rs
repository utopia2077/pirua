mod request_summary_support;
use pi_agent_schema::{
    AfterResponseEvent, AgentHarnessStreamOptions, AgentThinkingLevel, BeforePayloadEvent,
    HarnessClosed, HarnessRuntimeError, LaneId, OperationId, SessionId, SettledAssistantMessage,
};
use pi_ai_core::*;
use pi_harness_runtime::request::*;
use request_summary_support::*;
use std::sync::{Arc, Mutex};
#[derive(Clone)]
struct Observed {
    wire: SimpleStreamOptionsWire,
    context: Context,
    model: Model,
}
struct Adapter {
    model: Model,
    calls: Arc<Mutex<Vec<Observed>>>,
    panic: bool,
}
impl ApiAdapter for Adapter {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        Some(self)
    }
    fn stream(&self, _: TextStreamRequest) -> AssistantStream {
        panic!("raw route")
    }
    fn stream_simple(&self, _: SimpleStreamRequest) -> AssistantStream {
        panic!("unobserved route")
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        panic!("summary deferred")
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { panic!("summary cancel") })
    }
}
impl RequestLifecycleAdapter for Adapter {
    fn stream_with_lifecycle(
        &self,
        _: TextStreamRequest,
        _: RequestLifecycleInvocation,
    ) -> AssistantStream {
        panic!("raw lifecycle")
    }
    fn stream_simple_with_lifecycle(
        &self,
        r: SimpleStreamRequest,
        l: RequestLifecycleInvocation,
    ) -> AssistantStream {
        assert!(!self.panic, "fixture provider panic");
        self.calls.lock().unwrap().push(Observed {
            wire: r.options.wire.clone(),
            context: r.context.clone(),
            model: r.resolved.model.clone(),
        });
        let (stream, p) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            clock: r.options.runtime.clock.clone(),
            cancellation: r.options.runtime.cancellation.clone(),
        });
        let task = Box::pin(async move {
            if let Some(payload) = l.payload {
                let mut value = serde_json::json!({"actual":true});
                if payload
                    .transform(&r.resolved.model, &mut value)
                    .await
                    .is_err()
                {
                    p.fail_setup(StreamSetupFailure::Adapter).unwrap();
                    return;
                }
            }
            l.recorder.record(RequestResponseHead {
                status: 201,
                headers: vec![(
                    "x-summary".into(),
                    RequestResponseHeaderValue::Text("observed".into()),
                )],
            });
            p.push(AssistantMessageEvent::Done {
                reason: DoneReason::Stop,
                message: Box::new(response(&r.resolved.model)),
            })
            .unwrap();
        });
        r.options.runtime.spawner.spawn(task).unwrap();
        stream
    }
}
fn response(m: &Model) -> AssistantMessage {
    serde_json::from_value(serde_json::json!({"role":"assistant","content":[{"type":"text","text":"summary"}],"api":m.api,"provider":m.provider,"model":m.id,"stopReason":"stop","timestamp":7,"usage":Usage::zero()})).unwrap()
}
struct Auth;
impl ApiKeyAuth for Auth {
    fn name(&self) -> &str {
        "none"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async {
            Ok(Some(AuthResult {
                auth: ModelAuth {
                    api_key: Some(SecretString::new("offline-summary-fixture")),
                    headers: None,
                    base_url: None,
                },
                env: None,
                source: None,
            }))
        })
    }
}
struct Product {
    adapter: Arc<Adapter>,
    auth: ProviderAuth,
}
impl Provider for Product {
    fn id(&self) -> &ProviderId {
        &self.adapter.model.provider
    }
    fn name(&self) -> &str {
        "summary fixture"
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(vec![self.adapter.model.clone()].into())
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        Some(self.adapter.clone())
    }
}
#[derive(Default)]
struct Callbacks {
    payload: Mutex<usize>,
    responses: Mutex<Vec<AfterResponseEvent>>,
    closed: bool,
}
impl RequestCallbacks for Callbacks {
    fn has_payload_handlers(&self) -> bool {
        true
    }
    fn before_payload(
        &self,
        _: BeforePayloadEvent,
    ) -> BoxFuture<'_, Result<Option<JsonValue>, HarnessRuntimeError>> {
        Box::pin(async {
            *self.payload.lock().unwrap() += 1;
            if self.closed {
                Err(HarnessRuntimeError::Closed(HarnessClosed))
            } else {
                Ok(Some(JsonValue::Bool(true)))
            }
        })
    }
    fn after_response(
        &self,
        e: AfterResponseEvent,
    ) -> BoxFuture<'_, Result<SettledAssistantMessage, HarnessRuntimeError>> {
        Box::pin(async move {
            self.responses.lock().unwrap().push(e.clone());
            let mut m = e.message.into_message();
            m.response_id = Some(ResponseId::new("after-summary").unwrap());
            Ok(SettledAssistantMessage::new(m).unwrap())
        })
    }
    fn stream_event(
        &self,
        _: AssistantMessageEvent,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        panic!("public summary stream event")
    }
}
fn fixture_model() -> Model {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["model"].clone()).unwrap()
}
fn setup(
    panic: bool,
    callbacks: Arc<Callbacks>,
) -> (
    Arc<Executor>,
    RequestInput,
    Arc<Mutex<Vec<Observed>>>,
    CapturedTextDispatch,
) {
    setup_with_model(panic, callbacks, fixture_model())
}
fn setup_with_model(
    panic: bool,
    callbacks: Arc<Callbacks>,
    model: Model,
) -> (
    Arc<Executor>,
    RequestInput,
    Arc<Mutex<Vec<Observed>>>,
    CapturedTextDispatch,
) {
    let executor = Arc::new(Executor::default());
    let calls = Arc::new(Mutex::new(vec![]));
    let adapter = Arc::new(Adapter {
        model: model.clone(),
        calls: calls.clone(),
        panic,
    });
    let models = Arc::new(ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(InMemoryModelsStore::new()),
        auth_context: Arc::new(Host),
        clock: Arc::new(TestClock),
        spawner: executor.clone(),
        model_header_policy: Default::default(),
    }));
    executor
        .drive(models.set_provider(Arc::new(Product {
            adapter,
            auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
        })))
        .unwrap();
    let captured = models
        .capture_text_dispatch(
            &models.provider_registration(&model.provider).unwrap(),
            model.clone(),
        )
        .unwrap();
    let input=RequestInput{models,model,context:serde_json::from_value(serde_json::json!({"systemPrompt":"summary system","messages":[{"role":"user","content":"summary prompt","timestamp":7}],"tools":[{"name":"must-not-reach","description":"tool","parameters":{"type":"object"}}]})).unwrap(),thinking:AgentThinkingLevel::Off,options:AgentHarnessStreamOptions{deferred:Some(DeferredRequest::Enabled(true)),..Default::default()},runtime:RequestRuntime::with_noop_telemetry(CancellationToken::new(),Arc::new(TestClock),executor.clone()),lane:LaneId::new("main").unwrap(),run_id:OperationId::new("op-summary").unwrap(),session_id:SessionId::new("ordinary-session").unwrap(),callbacks};
    (executor, input, calls, captured)
}
#[test]
fn actual_captured_summary_has_cap_routing_hooks_and_no_public_messages() {
    let callbacks = Arc::new(Callbacks::default());
    let (executor, mut input, calls, captured) = setup(false, callbacks.clone());
    let original = input.model.clone();
    input.options = seed_summary_options(input.options);
    assert_eq!(input.options.cache_retention, Some(CacheRetention::None));
    input.options.cache_retention = Some(CacheRetention::Long);
    input.options.timeout_ms = Some(SafeUnsigned::new(23).unwrap());
    input.options.metadata = Some(JsonObject::new(
        [("summary".into(), JsonValue::Bool(true))].into(),
    ));
    executor
        .drive(input.models.delete_provider(&input.model.provider))
        .unwrap();
    let result = executor
        .drive(run_summary_request(
            input,
            captured,
            SummaryRequestSettings {
                routing_key: "task-1-request-0".into(),
                max_tokens: SafeUnsigned::new(19).unwrap(),
                logical_prompt_utf16_units: None,
            },
        ))
        .unwrap();
    assert_eq!(
        result.as_message().response_id.as_ref().unwrap().as_str(),
        "after-summary"
    );
    let values = calls.lock().unwrap();
    assert_eq!(values.len(), 1);
    let call = &values[0];
    assert_eq!(call.model, original);
    assert_eq!(
        call.wire.stream.max_tokens,
        Some(SafeUnsigned::new(19).unwrap())
    );
    assert_eq!(call.wire.deferred, Some(DeferredRequest::Enabled(false)));
    assert_eq!(
        call.wire.stream.session_id.as_deref(),
        Some("task-1-request-0")
    );
    assert_eq!(call.wire.stream.cache_retention, Some(CacheRetention::Long));
    assert_eq!(
        call.wire.stream.timeout_ms,
        Some(SafeUnsigned::new(23).unwrap())
    );
    assert!(call.wire.stream.metadata.is_some());
    assert_eq!(
        call.context.system_prompt.as_deref(),
        Some("summary system")
    );
    assert_eq!(call.context.tools, None);
    assert_eq!(*callbacks.payload.lock().unwrap(), 1);
    let responses = callbacks.responses.lock().unwrap();
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].status, Some(SafeUnsigned::new(201).unwrap()));
}
#[test]
fn summary_provider_panic_is_inband_and_trusted_close_is_not() {
    for panic in [true, false] {
        let callbacks = Arc::new(Callbacks {
            closed: !panic,
            ..Default::default()
        });
        let (e, input, calls, captured) = setup(panic, callbacks);
        let result = e.drive(run_summary_request(
            input,
            captured,
            SummaryRequestSettings {
                routing_key: "failure".into(),
                max_tokens: SafeUnsigned::new(12).unwrap(),
                logical_prompt_utf16_units: None,
            },
        ));
        if panic {
            assert_eq!(result.unwrap().as_message().stop_reason, StopReason::Error);
            assert!(calls.lock().unwrap().is_empty())
        } else {
            assert!(matches!(result, Err(HarnessRuntimeError::Closed(_))));
            assert_eq!(calls.lock().unwrap().len(), 1)
        }
    }
}

fn n(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).unwrap()
}
fn content_golden() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../conformance/harness/r8-r10/content/golden.json"
    ))
    .unwrap()
}
#[test]
fn full_generated_unicode_prompt_binds_original_cap_on_actual_dispatch() {
    let g = content_golden();
    let u = &g["unicodeRequest"];
    let mut model = fixture_model();
    model.api = ApiId::new("openai-responses").unwrap();
    model.compat = None;
    model.max_tokens = n(u["model"]["maxTokens"].as_u64().unwrap());
    model.context_window = n(u["model"]["contextWindow"].as_u64().unwrap());
    let original = model.clone();
    let (executor, mut input, calls, captured) =
        setup_with_model(false, Arc::new(Callbacks::default()), model);
    input.context = serde_json::from_value(u["context"].clone()).unwrap();
    executor
        .drive(run_summary_request(
            input,
            captured,
            SummaryRequestSettings {
                routing_key: "unicode-full-prompt".into(),
                max_tokens: n(u["desired"].as_u64().unwrap()),
                logical_prompt_utf16_units: Some(n(u["logicalUnits"].as_u64().unwrap())),
            },
        ))
        .unwrap();
    let calls = calls.lock().unwrap();
    assert_eq!(calls[0].model, original);
    assert_eq!(
        calls[0].wire.stream.max_tokens,
        Some(n(u["rawCap"].as_u64().unwrap()))
    );
    assert_ne!(
        calls[0].wire.stream.max_tokens,
        Some(n(u["cleanCap"].as_u64().unwrap()))
    );
    assert_eq!(calls[0].wire.thinking_budgets, None);
}
#[test]
fn captured_anthropic_budget_compensation_matches_complete_source_helpers() {
    let g = content_golden();
    let cases = g["wireCases"].as_array().unwrap();
    assert_eq!(cases.len(), 32);
    for case in cases {
        let mut model = fixture_model();
        model.api = ApiId::new("anthropic-messages").unwrap();
        model.compat = None;
        model.reasoning = true;
        model.max_tokens = n(case["model"]["maxTokens"].as_u64().unwrap());
        model.context_window = n(case["model"]["contextWindow"].as_u64().unwrap());
        let original = model.clone();
        let (executor, mut input, calls, captured) =
            setup_with_model(false, Arc::new(Callbacks::default()), model);
        input.context=serde_json::from_value(serde_json::json!({"messages":[{"role":"user","content":"x".repeat(2052),"timestamp":7}]})).unwrap();
        input.thinking = match case["level"].as_str().unwrap() {
            "minimal" => AgentThinkingLevel::Minimal,
            "low" => AgentThinkingLevel::Low,
            "xhigh" => AgentThinkingLevel::ExtraHigh,
            "max" => AgentThinkingLevel::Max,
            _ => panic!(),
        };
        let requested = input.thinking;
        executor
            .drive(run_summary_request(
                input,
                captured,
                SummaryRequestSettings {
                    routing_key: "unicode-thinking".into(),
                    max_tokens: n(case["desired"].as_u64().unwrap()),
                    logical_prompt_utf16_units: Some(n(2053)),
                },
            ))
            .unwrap();
        let calls = calls.lock().unwrap();
        let wire = &calls[0].wire;
        assert_eq!(calls[0].model, original);
        assert_eq!(
            wire.stream.max_tokens,
            Some(n(case["wireBase"].as_u64().unwrap())),
            "{case}"
        );
        let actual = serde_json::to_value(wire.thinking_budgets.as_ref().unwrap()).unwrap();
        let mut expected = serde_json::Map::new();
        expected.insert(
            case["bucket"].as_str().unwrap().into(),
            case["effectiveBudget"].clone(),
        );
        assert_eq!(actual, serde_json::Value::Object(expected), "{case}");
        assert_eq!(
            wire.reasoning,
            Some(match requested {
                AgentThinkingLevel::Minimal => ThinkingLevel::Minimal,
                AgentThinkingLevel::Low => ThinkingLevel::Low,
                AgentThinkingLevel::ExtraHigh => ThinkingLevel::ExtraHigh,
                AgentThinkingLevel::Max => ThinkingLevel::Max,
                _ => panic!(),
            })
        );
    }
}
#[test]
fn adaptive_off_other_api_and_nonreasoning_do_not_receive_inverse_budget() {
    for mode in ["adaptive", "off", "other", "unsupported-reasoning"] {
        let mut model = fixture_model();
        model.api = ApiId::new(if mode == "other" {
            "openai-responses"
        } else {
            "anthropic-messages"
        })
        .unwrap();
        model.compat = if mode == "adaptive" {
            Some(ModelCompat(
                serde_json::from_value(serde_json::json!({"forceAdaptiveThinking":true})).unwrap(),
            ))
        } else {
            None
        };
        model.max_tokens = n(8192);
        model.context_window = n(4700);
        model.reasoning = mode != "unsupported-reasoning";
        let (executor, mut input, calls, captured) =
            setup_with_model(false, Arc::new(Callbacks::default()), model);
        input.context=serde_json::from_value(serde_json::json!({"messages":[{"role":"user","content":"x".repeat(2052),"timestamp":7}]})).unwrap();
        input.thinking = if mode == "off" {
            AgentThinkingLevel::Off
        } else {
            AgentThinkingLevel::Low
        };
        executor
            .drive(run_summary_request(
                input,
                captured,
                SummaryRequestSettings {
                    routing_key: mode.into(),
                    max_tokens: n(1000),
                    logical_prompt_utf16_units: Some(n(2053)),
                },
            ))
            .unwrap();
        let calls = calls.lock().unwrap();
        assert_eq!(calls[0].wire.stream.max_tokens, Some(n(90)));
        assert_eq!(calls[0].wire.thinking_budgets, None);
        if mode == "unsupported-reasoning" || mode == "off" {
            assert_eq!(calls[0].wire.reasoning, None)
        }
    }
}

#[test]
fn logical_prompt_binding_rejects_wrong_shape_before_provider_call() {
    for multiple in [false, true] {
        let (executor, mut input, calls, captured) = setup(false, Arc::new(Callbacks::default()));
        if multiple {
            input
                .context
                .messages
                .push(input.context.messages[0].clone());
        }
        let result = executor.drive(run_summary_request(
            input,
            captured,
            SummaryRequestSettings {
                routing_key: "invalid-owner-binding".into(),
                max_tokens: n(1000),
                logical_prompt_utf16_units: Some(n(if multiple { 100 } else { 0 })),
            },
        ));
        assert!(matches!(result, Err(HarnessRuntimeError::Fault(_))));
        assert!(calls.lock().unwrap().is_empty());
    }
}
