use pi_ai_core::*;
use pi_ai_providers::api::lazy::*;
use serde_json::json;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context as Cx, Poll, Waker},
    time::Duration,
};
fn poll<F: Future + ?Sized>(f: Pin<&mut F>) -> Poll<F::Output> {
    f.poll(&mut Cx::from_waker(Waker::noop()))
}
fn ready<F: Future>(f: F) -> F::Output {
    match poll(Box::pin(f).as_mut()) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("pending"),
    }
}
struct Tick;
impl Clock for Tick {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(std::future::pending())
    }
}
#[derive(Default)]
struct Spawner(Mutex<Vec<BoxFuture<'static, ()>>>);
impl TaskSpawner for Spawner {
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(f);
        Ok(())
    }
}
impl Spawner {
    fn drive(&self) {
        let tasks = std::mem::take(&mut *self.0.lock().unwrap());
        for mut t in tasks {
            if poll(t.as_mut()).is_pending() {
                self.0.lock().unwrap().push(t);
            }
        }
    }
}

fn msg(stop: &str, input: u64) -> AssistantMessage {
    serde_json::from_value(json!({"role":"assistant","content":[],"api":"a","provider":"p","model":"m","usage":{"input":input,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":input,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":stop,"timestamp":7})).unwrap()
}
fn factory(token: CancellationToken) -> (AssistantStream, AssistantStreamProducer) {
    create_assistant_message_event_stream(AssistantStreamInit {
        api: "a".parse().unwrap(),
        provider: "p".parse().unwrap(),
        model: "m".parse().unwrap(),
        clock: Arc::new(Tick),
        cancellation: token,
    })
}
fn model() -> Model {
    serde_json::from_value(json!({"id":"m","name":"Synthetic","api":"a","provider":"p","baseUrl":"https://example.invalid","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":4096,"maxTokens":128})).unwrap()
}
fn request(token: CancellationToken, s: Arc<Spawner>) -> TextStreamRequest {
    TextStreamRequest {
        resolved: ResolvedRequest {
            model: model(),
            endpoint: RequestEndpoint::new_resolved("https://example.invalid").unwrap(),
        },
        context: serde_json::from_value(json!({"messages":[]})).unwrap(),
        auth: ResolvedAuth::default(),
        options: StreamOptions {
            wire: Default::default(),
            runtime: RequestRuntime::with_noop_telemetry(token, Arc::new(Tick), s),
        },
    }
}
struct Adapter {
    api: ApiId,
    producer: Arc<Mutex<Option<AssistantStreamProducer>>>,
    complete: bool,
}
impl ApiAdapter for Adapter {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        let (s, p) = factory(r.options.runtime.cancellation);
        if self.complete {
            p.push(AssistantMessageEvent::Done {
                reason: DoneReason::Stop,
                message: Box::new(msg("stop", 3)),
            })
            .unwrap();
        } else {
            p.push(AssistantMessageEvent::Start {
                partial: Box::new(msg("pending", 0)),
            })
            .unwrap();
            *self.producer.lock().unwrap() = Some(p);
        }
        s
    }
    fn stream_simple(&self, r: SimpleStreamRequest) -> AssistantStream {
        self.stream(TextStreamRequest {
            resolved: r.resolved,
            context: r.context,
            auth: r.auth,
            options: StreamOptions {
                wire: r.options.wire.stream,
                runtime: r.options.runtime,
            },
        })
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { Err(ModelsError::new(ModelsErrorCode::Provider)) })
    }
}
fn init(token: CancellationToken) -> AssistantStreamInit {
    AssistantStreamInit {
        api: "a".parse().unwrap(),
        provider: "p".parse().unwrap(),
        model: "m".parse().unwrap(),
        clock: Arc::new(Tick),
        cancellation: token,
    }
}
fn complete(usage: u64, token: CancellationToken) -> AssistantStream {
    let (s, p) = factory(token);
    p.push(AssistantMessageEvent::Done {
        reason: DoneReason::Stop,
        message: Box::new(msg("stop", usage)),
    })
    .unwrap();
    s
}
struct Load {
    calls: AtomicUsize,
    module: Arc<dyn ApiAdapter>,
    fail: bool,
}
impl LazyApiLoader for Load {
    fn load(self: Arc<Self>) -> BoxFuture<'static, Result<Arc<dyn ApiAdapter>, ModelsError>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.fail {
                Err(ModelsError::with_source(
                    ModelsErrorCode::Provider,
                    std::io::Error::other("raw-private-sentinel"),
                ))
            } else {
                Ok(self.module.clone())
            }
        })
    }
}
fn load(complete: bool, fail: bool) -> Arc<Load> {
    Arc::new(Load {
        calls: AtomicUsize::new(0),
        module: Arc::new(Adapter {
            api: "a".parse().unwrap(),
            producer: Arc::new(Mutex::new(None)),
            complete,
        }),
        fail,
    })
}
fn simple(token: CancellationToken, s: Arc<Spawner>) -> SimpleStreamRequest {
    let r = request(token, s);
    SimpleStreamRequest {
        resolved: r.resolved,
        context: r.context,
        auth: r.auth,
        options: SimpleStreamOptions {
            wire: Default::default(),
            runtime: r.options.runtime,
        },
    }
}
fn fetch(token: CancellationToken, s: Arc<Spawner>) -> DeferredFetchRequest {
    let r = request(token, s);
    DeferredFetchRequest {
        resolved: r.resolved,
        auth: r.auth,
        handle: serde_json::from_value(
            json!({"provider":"p","modelId":"m","api":"a","id":"deferred"}),
        )
        .unwrap(),
        options: DeferredFetchOptions {
            wire: Default::default(),
            runtime: r.options.runtime,
        },
    }
}
fn cancel(token: CancellationToken, s: Arc<Spawner>) -> DeferredCancelRequest {
    let r = fetch(token, s);
    DeferredCancelRequest {
        resolved: r.resolved,
        auth: r.auth,
        handle: r.handle,
        options: DeferredCancelOptions {
            wire: Default::default(),
            runtime: r.options.runtime,
        },
    }
}

#[test]
fn child_usage_only_cancellation_and_queued_prefix_are_forwarded_unchanged() {
    let s = Arc::new(Spawner::default());
    let token = CancellationToken::new();
    let capture = Arc::new(Mutex::new(None));
    let loader = Arc::new(Load {
        calls: AtomicUsize::new(0),
        fail: false,
        module: Arc::new(Adapter {
            api: "a".parse().unwrap(),
            producer: capture.clone(),
            complete: false,
        }),
    });
    let api = lazy_api(
        "a".parse().unwrap(),
        loader.clone(),
        LazyApiCapabilities::default(),
    );
    let mut outer = api.stream(request(token.clone(), s.clone()));
    assert_eq!(loader.calls.load(Ordering::SeqCst), 1);
    s.drive();
    capture
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .publish_cancellation_snapshot(msg("pending", 17))
        .unwrap();
    token.cancel();
    s.drive();
    assert!(
        matches!(ready(outer.next()),Some(AssistantMessageEvent::Start{partial}) if partial.usage.input.get()==0)
    );
    assert!(
        matches!(ready(outer.next()),Some(AssistantMessageEvent::Error{error,..}) if error.usage.input.get()==17)
    );
    assert_eq!(ready(outer.result()).usage.input.get(), 17);
    assert!(ready(outer.next()).is_none());
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/lazy/trace.json"
    ))
    .unwrap();
    assert_eq!(fixture["cancellation"]["terminalUsage"], 17);
}

struct PendingDrop(Arc<AtomicUsize>);
impl Future for PendingDrop {
    type Output = Result<AssistantStream, ModelsError>;
    fn poll(self: Pin<&mut Self>, _: &mut Cx<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for PendingDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn pending_setup_cancellation_drops_noncooperative_future_and_pre_cancel_still_invokes() {
    for pre in [false, true] {
        let s = Spawner::default();
        let token = CancellationToken::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let calls = AtomicUsize::new(0);
        if pre {
            token.cancel();
        }
        let outer = lazy_stream(init(token.clone()), &s, || {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(PendingDrop(drops.clone()))
        });
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        s.drive();
        token.cancel();
        s.drive();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(ready(outer.result()).stop_reason, StopReason::Aborted);
        assert_eq!(ready(outer.result()).usage.input.get(), 0);
    }
}
#[test]
fn setup_ready_child_owns_cancel_inside_completing_poll_and_before_forwarding() {
    let s = Spawner::default();
    let token = CancellationToken::new();
    let child_token = token.clone();
    let mut outer = lazy_stream(init(token), &s, || {
        Box::pin(async move {
            let (child, p) = factory(child_token.clone());
            p.push(AssistantMessageEvent::Start {
                partial: Box::new(msg("pending", 0)),
            })
            .unwrap();
            p.publish_cancellation_snapshot(msg("pending", 17)).unwrap();
            child_token.cancel();
            Ok(child)
        })
    });
    s.drive();
    assert!(matches!(
        ready(outer.next()),
        Some(AssistantMessageEvent::Start { .. })
    ));
    assert_eq!(ready(outer.result()).usage.input.get(), 17);
}
struct Reject;
impl TaskSpawner for Reject {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
#[test]
fn spawn_rejection_task_drop_and_consumer_drop_keep_one_settlement_and_release_setup() {
    let drops = Arc::new(AtomicUsize::new(0));
    let outer = lazy_stream(init(CancellationToken::new()), &Reject, || {
        Box::pin(PendingDrop(drops.clone()))
    });
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        ready(outer.result()).error_message.as_deref(),
        Some("stream setup task could not be started")
    );
    let s = Spawner::default();
    let outer = lazy_stream(init(CancellationToken::new()), &s, || {
        Box::pin(PendingDrop(drops.clone()))
    });
    drop(std::mem::take(&mut *s.0.lock().unwrap()));
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_eq!(ready(outer.result()).stop_reason, StopReason::Error);
    let token = CancellationToken::new();
    let outer = lazy_stream(init(token.clone()), &s, || {
        Box::pin(PendingDrop(drops.clone()))
    });
    drop(outer);
    s.drive();
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    token.cancel();
    s.drive();
    assert_eq!(drops.load(Ordering::SeqCst), 3);
}
#[test]
fn every_operation_invokes_load_before_return_and_advertisement_precedes_loading() {
    let s = Arc::new(Spawner::default());
    let l = load(true, false);
    let api = lazy_api(
        "a".parse().unwrap(),
        l.clone(),
        LazyApiCapabilities::default(),
    );
    assert_eq!(l.calls.load(Ordering::SeqCst), 0);
    let a = api.stream(request(CancellationToken::new(), s.clone()));
    assert_eq!(l.calls.load(Ordering::SeqCst), 1);
    let b = api.stream_simple(simple(CancellationToken::new(), s.clone()));
    assert_eq!(l.calls.load(Ordering::SeqCst), 2);
    assert!(matches!(
        api.fetch_deferred(fetch(CancellationToken::new(), s.clone())),
        Err(CapabilityError::Unsupported)
    ));
    assert!(ready(api.cancel_deferred(cancel(CancellationToken::new(), s.clone()))).is_err());
    assert_eq!(l.calls.load(Ordering::SeqCst), 2);
    s.drive();
    assert_eq!(ready(a.result()).usage.input.get(), 3);
    assert_eq!(ready(b.result()).usage.input.get(), 3);
    let api = lazy_api(
        "a".parse().unwrap(),
        l.clone(),
        LazyApiCapabilities {
            fetch_deferred: true,
            cancel_deferred: true,
        },
    );
    let f = api
        .fetch_deferred(fetch(CancellationToken::new(), s.clone()))
        .unwrap();
    assert_eq!(l.calls.load(Ordering::SeqCst), 3);
    let c = api.cancel_deferred(cancel(CancellationToken::new(), s.clone()));
    assert_eq!(l.calls.load(Ordering::SeqCst), 4);
    assert!(ready(c).is_err());
    s.drive();
    assert_eq!(ready(f.result()).stop_reason, StopReason::Error);
    assert_eq!(ready(f.result()).usage.input.get(), 0);
}
#[test]
fn repeated_load_errors_are_uncached_and_redacted() {
    let s = Arc::new(Spawner::default());
    let l = load(true, true);
    let api = lazy_api(
        "a".parse().unwrap(),
        l.clone(),
        LazyApiCapabilities::default(),
    );
    for _ in 0..2 {
        let mut outer = api.stream(request(CancellationToken::new(), s.clone()));
        s.drive();
        let result = ready(outer.result());
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(result.usage.input.get(), 0);
        assert!(!format!("{result:?}").contains("raw-private-sentinel"));
        assert!(matches!(
            ready(outer.next()),
            Some(AssistantMessageEvent::Error { .. })
        ));
        assert!(ready(outer.next()).is_none());
    }
    assert_eq!(l.calls.load(Ordering::SeqCst), 2);
}

type Family = fn(Arc<dyn LazyApiLoader>) -> LazyApi;
#[test]
fn all_four_text_factories_compile_and_delegate_raw_and_simple_without_discovery() {
    let _lock = FACTORY_TEST.lock().unwrap();
    let factories: [(&str, Family); 4] = [
        ("anthropic-messages", anthropic_messages_api),
        ("google-generative-ai", google_generative_ai_api),
        ("openai-completions", openai_completions_api),
        ("openai-responses", openai_responses_api),
    ];
    let s = Arc::new(Spawner::default());
    let source: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/lazy/trace.json"
    ))
    .unwrap();
    assert_eq!(
        source["families"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|family| factories.iter().any(|(name, _)| family["id"] == *name))
            .count(),
        4
    );
    for (id, factory) in factories {
        let observation = source["families"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["id"] == id)
            .unwrap();
        assert_eq!(observation["calls"], 2);
        assert_eq!(observation["keys"], json!(["stream", "streamSimple"]));
        let l = load(true, false);
        let api = factory(l.clone());
        assert_eq!(api.api_id().as_str(), id);
        assert_eq!(api.capabilities(), LazyApiCapabilities::default());
        assert_eq!(l.calls.load(Ordering::SeqCst), 0);
        let a = api.stream(request(CancellationToken::new(), s.clone()));
        let b = api.stream_simple(simple(CancellationToken::new(), s.clone()));
        assert_eq!(l.calls.load(Ordering::SeqCst), 2);
        s.drive();
        assert_eq!(ready(a.result()).usage.input.get(), 3);
        assert_eq!(ready(b.result()).usage.input.get(), 3);
    }
}

static FACTORY_TEST: Mutex<()> = Mutex::new(());
struct Supported {
    api: ApiId,
    value: u64,
    operations: Arc<Mutex<Vec<&'static str>>>,
}
impl ApiAdapter for Supported {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        self.operations.lock().unwrap().push("raw");
        complete(self.value, r.options.runtime.cancellation)
    }
    fn stream_simple(&self, r: SimpleStreamRequest) -> AssistantStream {
        self.operations.lock().unwrap().push("simple");
        complete(self.value, r.options.runtime.cancellation)
    }
    fn fetch_deferred(&self, r: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        self.operations.lock().unwrap().push("fetch");
        Ok(complete(self.value, r.options.runtime.cancellation))
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        self.operations.lock().unwrap().push("cancel");
        Box::pin(async { Ok(()) })
    }
}
#[test]
fn advertised_supported_operations_reach_exact_loaded_methods() {
    let s = Arc::new(Spawner::default());
    let operations = Arc::new(Mutex::new(vec![]));
    let l = Arc::new(Load {
        calls: AtomicUsize::new(0),
        fail: false,
        module: Arc::new(Supported {
            api: "a".parse().unwrap(),
            value: 23,
            operations: operations.clone(),
        }),
    });
    let api = lazy_api(
        "a".parse().unwrap(),
        l.clone(),
        LazyApiCapabilities {
            fetch_deferred: true,
            cancel_deferred: true,
        },
    );
    let a = api.stream(request(CancellationToken::new(), s.clone()));
    s.drive();
    let b = api.stream_simple(simple(CancellationToken::new(), s.clone()));
    s.drive();
    let f = api
        .fetch_deferred(fetch(CancellationToken::new(), s.clone()))
        .unwrap();
    s.drive();
    let c = api.cancel_deferred(cancel(CancellationToken::new(), s));
    assert_eq!(l.calls.load(Ordering::SeqCst), 4);
    ready(c).unwrap();
    for stream in [a, b, f] {
        assert_eq!(ready(stream.result()).usage.input.get(), 23);
    }
    assert_eq!(
        *operations.lock().unwrap(),
        ["raw", "simple", "fetch", "cancel"]
    );
}

#[test]
fn cancellation_before_ready_poll_wins_setup_but_forwarder_drop_retains_queued_prefix() {
    let s = Spawner::default();
    let token = CancellationToken::new();
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let outer = lazy_stream(init(token.clone()), &s, || {
        Box::pin(std::future::poll_fn(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(complete(23, CancellationToken::new())))
        }))
    });
    token.cancel();
    s.drive();
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(ready(outer.result()).stop_reason, StopReason::Aborted);

    let token = CancellationToken::new();
    let (child, producer) = factory(token.clone());
    producer
        .push(AssistantMessageEvent::Start {
            partial: Box::new(msg("pending", 99)),
        })
        .unwrap();
    let mut outer = lazy_stream(init(token), &s, || Box::pin(async move { Ok(child) }));
    s.drive();
    drop(std::mem::take(&mut *s.0.lock().unwrap()));
    assert!(
        matches!(ready(outer.next()), Some(AssistantMessageEvent::Start{partial}) if partial.usage.input.get()==99)
    );
    assert!(
        matches!(ready(outer.next()), Some(AssistantMessageEvent::Error{error,..}) if error.usage.input.get()==0)
    );
    assert!(ready(outer.next()).is_none());
    assert_eq!(ready(outer.result()).stop_reason, StopReason::Error);
    drop(producer);
}
