use pi_ai::*;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
pub struct TestClock(pub AtomicU64);
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            tokio::select! {_=tokio::time::sleep(d)=>Ok(()),_=c.cancelled()=>Err(Cancelled)}
        })
    }
}
pub struct Spawner;
impl TaskSpawner for Spawner {
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        tokio::spawn(f);
        Ok(())
    }
}
struct Host;
impl AuthContext for Host {
    fn env<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async { Ok(None) })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}
#[derive(Default)]
pub struct ScriptState {
    pub responses: Mutex<VecDeque<Value>>,
    pub requests: Mutex<Vec<Value>>,
    pub released: Mutex<BTreeSet<String>>,
    pub gates: Mutex<BTreeMap<String, Arc<tokio::sync::Notify>>>,
    pub changed: tokio::sync::Notify,
}
impl ScriptState {
    pub async fn gate(&self, name: &str, c: &CancellationToken) {
        let notify = self
            .gates
            .lock()
            .unwrap()
            .entry(name.into())
            .or_default()
            .clone();
        loop {
            let n = notify.notified();
            if self.released.lock().unwrap().contains(name) {
                return;
            }
            tokio::select! {_=n=>(),_=c.cancelled()=>return}
        }
    }
}
struct EmptyAuth;
impl ApiKeyAuth for EmptyAuth {
    fn name(&self) -> &str {
        "Scripted local provider"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async {
            Ok(Some(AuthResult {
                auth: ModelAuth::default(),
                env: None,
                source: None,
            }))
        })
    }
}
pub struct ScriptProvider {
    id: ProviderId,
    auth: ProviderAuth,
    models: Vec<Model>,
    adapter: Arc<ScriptAdapter>,
}
pub struct ScriptAdapter {
    state: Arc<ScriptState>,
    api: ApiId,
}
impl ScriptAdapter {
    fn start(
        &self,
        model: Model,
        context: Option<Value>,
        options: SimpleStreamOptions,
        operation: (&str, Option<DeferredHandle>),
        headers: ProviderHeaders,
        captured_options: Option<Value>,
    ) -> AssistantStream {
        let (op, handle) = operation;
        let mut record = json!({"kind":op,"model":model,"options":captured_options.unwrap_or_else(||json!(options.wire))});
        if !headers.as_map().is_empty() {
            record["options"]["headers"] = json!(headers.as_map());
        }
        record["context"] = context.unwrap_or_else(|| json!({"$undefined":true}));
        if let Some(handle) = handle {
            record["handle"] = json!(handle);
        }
        self.state.requests.lock().unwrap().push(record);
        self.state.changed.notify_waiters();
        let step = self
            .state
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted provider response");
        assert_eq!(
            step["op"].as_str().unwrap_or("stream"),
            op,
            "scripted provider operation"
        );
        let clock = options.runtime.clock.clone();
        let cancellation = options.runtime.cancellation.clone();
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: model.api.clone(),
            provider: model.provider.clone(),
            model: model.id.clone(),
            clock: clock.clone(),
            cancellation: cancellation.clone(),
        });
        let state = self.state.clone();
        tokio::spawn(async move {
            if let Some(gate) = step["gateBefore"].as_str() {
                state.gate(gate, &cancellation).await;
            }
            if let Some(partial) = step["partial"].as_str() {
                let mut p = message(&model, &json!({"text":partial}), clock.now_unix_ms());
                p.stop_reason = StopReason::Pending;
                let _ = producer.push(AssistantMessageEvent::TextDelta {
                    content_index: SafeUnsigned::new(0).unwrap(),
                    delta: partial.into(),
                    partial: Box::new(p),
                });
            }
            if let Some(gate) = step["gate"].as_str() {
                state.gate(gate, &cancellation).await;
            }
            if cancellation.is_cancelled() {
                return;
            }
            let message = message(&model, &step, clock.now_unix_ms());
            let event = match message.stop_reason {
                StopReason::Error => AssistantMessageEvent::Error {
                    reason: ErrorReason::Error,
                    error: Box::new(message),
                },
                StopReason::Aborted => AssistantMessageEvent::Error {
                    reason: ErrorReason::Aborted,
                    error: Box::new(message),
                },
                other => AssistantMessageEvent::Done {
                    reason: match other {
                        StopReason::Length => DoneReason::Length,
                        StopReason::ToolUse => DoneReason::ToolUse,
                        StopReason::Deferred => DoneReason::Deferred,
                        _ => DoneReason::Stop,
                    },
                    message: Box::new(message),
                },
            };
            producer.push(event).unwrap();
        });
        stream
    }
}
impl ApiAdapter for ScriptAdapter {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        self.start(
            r.resolved.model,
            Some(json!(r.context)),
            SimpleStreamOptions {
                wire: SimpleStreamOptionsWire {
                    stream: r.options.wire,
                    ..Default::default()
                },
                runtime: r.options.runtime,
            },
            ("stream", None),
            r.auth.headers,
            None,
        )
    }
    fn stream_simple(&self, r: SimpleStreamRequest) -> AssistantStream {
        self.start(
            r.resolved.model,
            Some(json!(r.context)),
            r.options,
            ("stream", None),
            r.auth.headers,
            None,
        )
    }
    fn fetch_deferred(&self, r: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Ok(self.start(
            r.resolved.model,
            None,
            SimpleStreamOptions {
                wire: Default::default(),
                runtime: r.options.runtime,
            },
            ("fetch", Some(r.handle)),
            r.auth.headers,
            Some(json!(r.options.wire)),
        ))
    }
    fn cancel_deferred<'a>(
        &'a self,
        r: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.state
            .requests
            .lock()
            .unwrap()
            .push(json!({"kind":"cancel","handle":r.handle}));
        self.state.changed.notify_waiters();
        Box::pin(async { Ok(()) })
    }
}
impl Provider for ScriptProvider {
    fn id(&self) -> &ProviderId {
        &self.id
    }
    fn name(&self) -> &str {
        "Offline scripted provider"
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(self.models.clone().into())
    }
    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        (api == &self.adapter.api).then(|| self.adapter.clone() as Arc<dyn ApiAdapter>)
    }
}
pub fn message(model: &Model, step: &Value, now: u64) -> AssistantMessage {
    let mut content = step.get("content").cloned().unwrap_or_else(|| {
        let mut items = vec![];
        if let Some(t) = step["text"].as_str() {
            items.push(json!({"type":"text","text":t}));
        }
        for c in step["calls"].as_array().into_iter().flatten() {
            let mut c = c.clone();
            c["type"] = json!("toolCall");
            items.push(c)
        }
        json!(items)
    });
    if content.is_null() {
        content = json!([])
    }
    let stop = step["stopReason"]
        .as_str()
        .unwrap_or(if step.get("calls").is_some() {
            "toolUse"
        } else {
            "stop"
        });
    let mut v = json!({"role":"assistant","content":content,"api":model.api,"provider":model.provider,"model":model.id,"usage":step.get("usage").cloned().unwrap_or(serde_json::to_value(Usage::zero()).unwrap()),"stopReason":stop,"timestamp":now,"durationMs":0});
    for name in ["deferred", "errorMessage"] {
        if let Some(x) = step.get(name) {
            v[name] = x.clone()
        }
    }
    serde_json::from_value(v).unwrap()
}
pub async fn models(
    input: &Value,
    clock: Arc<TestClock>,
) -> (Arc<ModelsRegistry>, Arc<ScriptState>) {
    let state = Arc::new(ScriptState::default());
    *state.responses.lock().unwrap() = input["responses"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into();
    let templates = input["models"]
        .as_array()
        .cloned()
        .unwrap_or(vec![json!({"id":"scripted-1"})]);
    let mut models = vec![];
    for t in templates {
        models.push(serde_json::from_value(json!({"id":t["id"],"name":t["id"],"api":"oracle-script","provider":"oracle","baseUrl":"http://localhost:0","reasoning":t["reasoning"].as_bool().unwrap_or(true),"input":["text","image"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":t["contextWindow"].as_u64().unwrap_or(100000),"maxTokens":t["maxTokens"].as_u64().unwrap_or(1000)})).unwrap());
    }
    let registry=Arc::new(ModelsRegistry::new(ModelsRegistryOptions{identity:Some(serde_json::from_value(json!({"attribution":{"name":"pi","url":"https://pi.dev","categories":["cli-agent"]}})).unwrap()),credentials:Arc::new(InMemoryCredentialStore::new()),models_store:Box::new(InMemoryModelsStore::new()),auth_context:Arc::new(Host),clock,spawner:Arc::new(Spawner),model_header_policy:ModelHeaderPolicy::from_allowlist(["x-fixture".into()])}));
    let adapter = Arc::new(ScriptAdapter {
        state: state.clone(),
        api: ApiId::new("oracle-script").unwrap(),
    });
    registry
        .set_provider(Arc::new(ScriptProvider {
            id: ProviderId::new("oracle").unwrap(),
            auth: ProviderAuth::new(Some(Arc::new(EmptyAuth)), None).unwrap(),
            models,
            adapter,
        }))
        .await
        .unwrap();
    (registry, state)
}
