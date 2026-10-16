use std::future::{Future, pending};
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use pi_ai_core::*;
use pi_telemetry::NOOP_TELEMETRY_SPAN;

fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut TaskContext::from_waker(Waker::noop()))
}
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match poll(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected completed future"),
    }
}
#[derive(Default)]
struct Executor {
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    reject: AtomicBool,
}
impl TaskSpawner for Executor {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        if self.reject.load(Ordering::Relaxed) {
            return Err(SpawnError::new());
        }
        self.tasks.lock().unwrap().push(task);
        Ok(())
    }
}
impl Executor {
    fn tick(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for mut task in tasks {
            if poll(task.as_mut()).is_pending() {
                self.tasks.lock().unwrap().push(task);
            }
        }
    }
    fn clear(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        drop(tasks);
    }
    fn drive<F: Future>(&self, future: F) -> F::Output {
        let mut future = pin!(future);
        for _ in 0..20 {
            if let Poll::Ready(value) = poll(future.as_mut()) {
                return value;
            }
            self.tick();
        }
        panic!("future did not complete");
    }
}
struct TestClock;
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(pending())
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
struct Auth {
    pending: bool,
    dropped: Arc<AtomicBool>,
}
struct DropFlag(Arc<AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}
impl ApiKeyAuth for Auth {
    fn name(&self) -> &str {
        "synthetic"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async move {
            let _drop = DropFlag(self.dropped.clone());
            if self.pending {
                pending::<()>().await;
            }
            Ok(Some(AuthResult {
                auth: ModelAuth {
                    api_key: Some(SecretString::new("synthetic")),
                    headers: None,
                    base_url: None,
                },
                env: None,
                source: None,
            }))
        })
    }
}
fn model() -> Model {
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    serde_json::from_value(value["model"].clone()).unwrap()
}
fn message(model: &Model, usage: u64) -> AssistantMessage {
    serde_json::from_value(serde_json::json!({
        "role":"assistant", "content":[{"type":"text","text":"retained"}],
        "api":model.api, "provider":model.provider, "model":model.id,
        "usage":{"input":usage,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":usage,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},
        "stopReason":"pending", "timestamp":7
    })).unwrap()
}
struct Child {
    model: Model,
    producer: Mutex<Option<AssistantStreamProducer>>,
    calls: AtomicUsize,
    handoff_cancel: bool,
    unsupported: bool,
}
impl Child {
    fn start(&self, runtime: RequestRuntime) -> AssistantStream {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            clock: runtime.clock,
            cancellation: runtime.cancellation.clone(),
        });
        producer
            .push(AssistantMessageEvent::Start {
                partial: Box::new(message(&self.model, 0)),
            })
            .unwrap();
        // The final usage is deliberately absent from every emitted progress event.
        producer
            .publish_cancellation_snapshot(message(&self.model, 17))
            .unwrap();
        *self.producer.lock().unwrap() = Some(producer);
        if self.handoff_cancel {
            runtime.cancellation.cancel();
        }
        stream
    }
}
impl ApiAdapter for Child {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        self.start(r.options.runtime)
    }
    fn stream_simple(&self, r: SimpleStreamRequest) -> AssistantStream {
        self.start(r.options.runtime)
    }
    fn fetch_deferred(&self, r: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        if self.unsupported {
            Err(CapabilityError::Unsupported)
        } else {
            Ok(self.start(r.options.runtime))
        }
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { Ok(()) })
    }
}
struct Product {
    model: Model,
    auth: ProviderAuth,
    child: Arc<Child>,
}
impl Provider for Product {
    fn id(&self) -> &ProviderId {
        &self.model.provider
    }
    fn name(&self) -> &str {
        "synthetic"
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(vec![self.model.clone()].into())
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        Some(self.child.clone())
    }
}
struct Fixture {
    registry: ModelsRegistry,
    executor: Arc<Executor>,
    child: Arc<Child>,
    token: CancellationToken,
    auth_dropped: Arc<AtomicBool>,
}
impl Fixture {
    fn new(opt_in: bool, pending_auth: bool, handoff_cancel: bool, unsupported: bool) -> Self {
        let executor = Arc::new(Executor::default());
        let options = ModelsRegistryOptions {
            identity: None,
            credentials: Arc::new(InMemoryCredentialStore::new()),
            models_store: Box::new(InMemoryModelsStore::new()),
            auth_context: Arc::new(Host),
            clock: Arc::new(TestClock),
            spawner: executor.clone(),
            model_header_policy: ModelHeaderPolicy::default(),
        };
        let registry = if opt_in {
            ModelsRegistry::new_with_child_cancellation(options)
        } else {
            ModelsRegistry::new(options)
        };
        let child = Arc::new(Child {
            model: model(),
            producer: Mutex::new(None),
            calls: AtomicUsize::new(0),
            handoff_cancel,
            unsupported,
        });
        let auth_dropped = Arc::new(AtomicBool::new(false));
        let product = Arc::new(Product {
            model: model(),
            auth: ProviderAuth::new(
                Some(Arc::new(Auth {
                    pending: pending_auth,
                    dropped: auth_dropped.clone(),
                })),
                None,
            )
            .unwrap(),
            child: child.clone(),
        });
        executor.drive(registry.set_provider(product)).unwrap();
        Self {
            registry,
            executor,
            child,
            token: CancellationToken::new(),
            auth_dropped,
        }
    }
    fn runtime(&self) -> RequestRuntime {
        RequestRuntime {
            cancellation: self.token.clone(),
            telemetry: Arc::new(NOOP_TELEMETRY_SPAN),
            provider_event_observer: None,
            clock: Arc::new(TestClock),
            spawner: self.executor.clone(),
        }
    }
    fn stream(&self, simple: bool) -> AssistantStream {
        if simple {
            self.registry.stream_simple(
                model(),
                Context {
                    system_prompt: None,
                    messages: vec![],
                    tools: None,
                },
                ModelsRequestOverrides::default(),
                SimpleStreamOptions {
                    wire: SimpleStreamOptionsWire::default(),
                    runtime: self.runtime(),
                },
            )
        } else {
            self.registry.stream(
                model(),
                Context {
                    system_prompt: None,
                    messages: vec![],
                    tools: None,
                },
                ModelsRequestOverrides::default(),
                StreamOptions {
                    wire: StreamOptionsWire::default(),
                    runtime: self.runtime(),
                },
            )
        }
    }
    fn fetch(&self) -> BoxFuture<'static, AssistantMessage> {
        let model = model();
        let handle = DeferredHandle {
            provider: model.provider.clone(),
            model_id: model.id.clone(),
            api: model.api.clone(),
            id: DeferredId::new("d").unwrap(),
            expires_at: None,
            poll_after_ms: None,
            data: Presence::Missing,
        };
        self.registry.fetch_deferred(
            model,
            handle,
            ModelsRequestOverrides::default(),
            DeferredFetchOptions {
                wire: DeferredFetchOptionsWire::default(),
                runtime: self.runtime(),
            },
        )
    }
}

fn source_oracle(extra: &[&str]) -> std::process::Output {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::process::Command::new("/usr/bin/node")
        .current_dir(root)
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .args([
            "conformance/ai-compat/registry-cancellation/oracle.mjs",
            "--check",
        ])
        .args(extra)
        .output()
        .unwrap()
}

#[test]
fn full_pinned_models_source_cancellation_witness() {
    let output = source_oracle(&[]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn source_setup_proof_rejects_wrong_store_method_and_missing_abort() {
    for (control, diagnostic) in [
        ("--control=wrong-method", "setup must enter credential read"),
        ("--control=no-setup-abort", "setup abort must settle outer"),
    ] {
        let output = source_oracle(&[control]);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(1), "{control}: {stderr}");
        assert!(stderr.contains("AssertionError"), "{control}: {stderr}");
        assert!(stderr.contains(diagnostic), "{control}: {stderr}");
    }
}

#[test]
fn raw_simple_and_deferred_preserve_source_usage_in_both_handoff_orders() {
    let trace: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-compat/registry-cancellation/trace.json"
    ))
    .unwrap();
    for case in trace["results"].as_array().unwrap().iter().take(6) {
        let f = Fixture::new(true, false, case["duringHandoff"].as_bool().unwrap(), false);
        let operation = case["operation"].as_str().unwrap();
        let mut stream = (operation != "deferred").then(|| f.stream(operation == "simple"));
        let fetch = (operation == "deferred").then(|| f.fetch());
        f.executor.tick();
        f.token.cancel();
        f.executor.tick();
        let result = match &stream {
            Some(s) => ready(s.result()),
            None => ready(fetch.unwrap()),
        };
        assert_eq!(result.usage.input.get(), case["usage"].as_u64().unwrap());
        assert_eq!(result.stop_reason, StopReason::Aborted);
        assert_eq!(
            serde_json::to_value(&result).unwrap()["content"][0]["text"],
            case["content"]
        );
        assert_eq!(f.child.calls.load(Ordering::Relaxed), 1);
        if let Some(s) = &mut stream {
            let first = ready(s.next()).unwrap();
            assert!(
                matches!(first, AssistantMessageEvent::Start { partial } if partial.usage.input.get() == 0)
            );
            assert!(
                matches!(ready(s.next()), Some(AssistantMessageEvent::Error { error, .. }) if *error == result)
            );
            assert!(ready(s.next()).is_none());
        }
        let producer = f.child.producer.lock().unwrap().take().unwrap();
        assert!(producer.fail_setup(StreamSetupFailure::Adapter).is_err());
        f.token.cancel();
        assert!(f.executor.tasks.lock().unwrap().is_empty());
    }
}

#[test]
fn default_constructor_still_uses_empty_s03_cancellation_fallback() {
    for simple in [false, true] {
        let f = Fixture::new(false, false, false, false);
        let stream = f.stream(simple);
        f.executor.tick();
        f.token.cancel();
        f.executor.tick();
        let result = ready(stream.result());
        assert_eq!(result.usage.input.get(), 0);
        assert!(result.content.is_empty());
        assert_eq!(result.stop_reason, StopReason::Aborted);
    }
    let f = Fixture::new(false, false, false, false);
    let result = f.fetch();
    f.executor.tick();
    f.token.cancel();
    f.executor.tick();
    assert_eq!(ready(result).usage.input.get(), 0);
}

#[test]
fn setup_cancellation_drops_noncooperative_auth_and_never_dispatches_child() {
    for before_poll in [false, true] {
        let f = Fixture::new(true, true, false, false);
        let stream = f.stream(false);
        if !before_poll {
            f.executor.tick();
        }
        f.token.cancel();
        f.executor.tick();
        let result = ready(stream.result());
        assert_eq!(result.stop_reason, StopReason::Aborted);
        assert_eq!(result.usage.input.get(), 0);
        assert_eq!(f.child.calls.load(Ordering::Relaxed), 0);
        if !before_poll {
            assert!(f.auth_dropped.load(Ordering::Relaxed));
        }
        assert!(f.executor.tasks.lock().unwrap().is_empty());
    }
}

#[test]
fn rejected_or_dropped_dispatch_and_unsupported_fetch_settle_once() {
    for reject in [true, false] {
        let f = Fixture::new(true, false, false, false);
        f.executor.reject.store(reject, Ordering::Relaxed);
        let mut stream = f.stream(false);
        f.executor.clear();
        let result = ready(stream.result());
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(result.usage.input.get(), 0);
        assert!(matches!(
            ready(stream.next()),
            Some(AssistantMessageEvent::Error { .. })
        ));
        assert!(ready(stream.next()).is_none());
        assert_eq!(f.child.calls.load(Ordering::Relaxed), 0);
    }
    let f = Fixture::new(true, false, false, true);
    let result = f.executor.drive(f.fetch());
    assert_eq!(result.stop_reason, StopReason::Error);
    assert_eq!(result.usage.input.get(), 0);
}

#[test]
fn dropping_consumer_does_not_cancel_owned_dispatch_and_child_is_released() {
    let f = Fixture::new(true, false, false, false);
    let stream = f.stream(false);
    f.executor.tick();
    drop(stream);
    assert!(!f.token.is_cancelled());
    assert_eq!(f.executor.tasks.lock().unwrap().len(), 1);
    f.token.cancel();
    f.executor.tick();
    assert!(f.executor.tasks.lock().unwrap().is_empty());
    let producer = f.child.producer.lock().unwrap().take().unwrap();
    assert!(
        producer
            .publish_cancellation_snapshot(message(&model(), 99))
            .is_err()
    );
}

#[test]
fn executor_drop_during_setup_or_child_forwarding_releases_owned_task() {
    for pending_auth in [true, false] {
        let f = Fixture::new(true, pending_auth, false, false);
        let stream = f.stream(false);
        f.executor.tick();
        assert_eq!(f.executor.tasks.lock().unwrap().len(), 1);
        f.executor.clear();
        let result = ready(stream.result());
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(result.usage.input.get(), 0);
        assert!(f.auth_dropped.load(Ordering::Relaxed));
        assert!(!f.token.is_cancelled());
    }
}

#[test]
fn ready_success_terminal_wins_later_cancellation_before_forwarding() {
    let f = Fixture::new(true, false, false, false);
    let mut stream = f.stream(false);
    f.executor.tick();
    let mut terminal = message(&model(), 23);
    terminal.stop_reason = StopReason::Stop;
    terminal.duration_ms = Some(SafeUnsigned::new(0).unwrap());
    f.child
        .producer
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .push(AssistantMessageEvent::Done {
            reason: DoneReason::Stop,
            message: Box::new(terminal.clone()),
        })
        .unwrap();
    f.token.cancel();
    f.executor.tick();
    assert_eq!(ready(stream.result()), terminal);
    assert!(matches!(
        ready(stream.next()),
        Some(AssistantMessageEvent::Start { .. })
    ));
    assert!(
        matches!(ready(stream.next()), Some(AssistantMessageEvent::Done { message, .. }) if *message == terminal)
    );
    assert!(ready(stream.next()).is_none());
}
