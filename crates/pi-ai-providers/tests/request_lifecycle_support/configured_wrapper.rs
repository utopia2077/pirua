use super::request_lifecycle_support::{configured_wrapper, context, overrides};
use pi_ai_core::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
#[path = "executor.rs"]
mod executor;
use executor::*;

struct Probe {
    api: ApiId,
    mode: usize,
    queries: AtomicUsize,
    calls: AtomicUsize,
    complete_cancel: bool,
}
impl Probe {
    fn new(mode: usize, complete_cancel: bool) -> Arc<Self> {
        Arc::new(Self {
            api: ApiId::new("openai-responses").unwrap(),
            mode,
            queries: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            complete_cancel,
        })
    }
    fn child(&self, r: TextStreamRequest) -> AssistantStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: r.resolved.model.api.clone(),
            provider: r.resolved.model.provider.clone(),
            model: r.resolved.model.id.clone(),
            clock: r.options.runtime.clock,
            cancellation: r.options.runtime.cancellation.clone(),
        });
        if self.complete_cancel {
            let output:AssistantMessage=serde_json::from_value(serde_json::json!({"role":"assistant","content":[],"api":r.resolved.model.api,"provider":r.resolved.model.provider,"model":r.resolved.model.id,"stopReason":"pending","timestamp":7,"usage":{"input":17,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":17,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}}})).unwrap();
            producer.publish_cancellation_snapshot(output).unwrap();
            r.options.runtime.cancellation.cancel();
        } else {
            producer.fail_setup(StreamSetupFailure::Adapter).unwrap();
        }
        stream
    }
}
fn raw(r: SimpleStreamRequest) -> TextStreamRequest {
    TextStreamRequest {
        resolved: r.resolved,
        context: r.context,
        auth: r.auth,
        options: StreamOptions {
            wire: r.options.wire.stream,
            runtime: r.options.runtime,
        },
    }
}
impl ApiAdapter for Probe {
    fn api_id(&self) -> &ApiId {
        &self.api
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        let n = self.queries.fetch_add(1, Ordering::SeqCst);
        if self.mode == 1 || (self.mode == 2 && n == 0) {
            Some(self)
        } else {
            None
        }
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        self.child(r)
    }
    fn stream_simple(&self, r: SimpleStreamRequest) -> AssistantStream {
        self.child(raw(r))
    }
    fn fetch_deferred(&self, _: DeferredFetchRequest) -> Result<AssistantStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }
    fn cancel_deferred<'a>(
        &'a self,
        _: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { Ok(()) })
    }
}
impl RequestLifecycleAdapter for Probe {
    fn stream_with_lifecycle(
        &self,
        r: TextStreamRequest,
        invocation: RequestLifecycleInvocation,
    ) -> AssistantStream {
        invocation.recorder.record(RequestResponseHead {
            status: 202,
            headers: vec![],
        });
        self.child(r)
    }
    fn stream_simple_with_lifecycle(
        &self,
        r: SimpleStreamRequest,
        invocation: RequestLifecycleInvocation,
    ) -> AssistantStream {
        self.stream_with_lifecycle(raw(r), invocation)
    }
}
struct Handler;
impl RequestPayloadTransform for Handler {
    fn transform<'a>(
        &'a self,
        _: &'a Model,
        _: &'a mut serde_json::Value,
    ) -> BoxFuture<'a, Result<Option<serde_json::Value>, ModelsError>> {
        Box::pin(async { Ok(None) })
    }
}
fn rt(executor: Arc<Executor>, cancellation: CancellationToken) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(cancellation, Arc::new(TestClock), executor)
}
fn registry(executor: Arc<Executor>, provider: Arc<dyn Provider>) -> ModelsRegistry {
    let registry = ModelsRegistry::new_with_child_cancellation(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(InMemoryModelsStore::new()),
        auth_context: Arc::new(Host),
        clock: Arc::new(TestClock),
        spawner: executor.clone(),
        model_header_policy: Default::default(),
    });
    executor.drive(registry.set_provider(provider)).unwrap();
    registry
}
#[test]
fn configured_factory_ordinary_calls_do_not_query_and_missing_or_flipping_capability_never_falls_back()
 {
    for mode in [0, 1, 2] {
        let executor = Arc::new(Executor::default());
        let probe = Probe::new(mode, false);
        let provider = configured_wrapper(probe.clone());
        assert_eq!(probe.queries.load(Ordering::SeqCst), 0);
        let model = provider.models().unwrap()[0].clone();
        let registry = registry(executor.clone(), provider);
        for simple in [false, true] {
            let runtime = rt(executor.clone(), CancellationToken::new());
            let stream = if simple {
                registry.stream_simple(
                    model.clone(),
                    context(),
                    overrides(),
                    SimpleStreamOptions {
                        wire: Default::default(),
                        runtime,
                    },
                )
            } else {
                registry.stream(
                    model.clone(),
                    context(),
                    overrides(),
                    StreamOptions {
                        wire: Default::default(),
                        runtime,
                    },
                )
            };
            assert_eq!(
                executor.drive(stream.result()).stop_reason,
                StopReason::Error
            );
        }
        assert_eq!(probe.queries.load(Ordering::SeqCst), 0);
        assert_eq!(probe.calls.load(Ordering::SeqCst), 2);
        let handler = Arc::new(Handler);
        let weak = Arc::downgrade(&handler);
        let observed = registry.stream_with_lifecycle(
            model,
            context(),
            overrides(),
            StreamOptions {
                wire: Default::default(),
                runtime: rt(executor.clone(), CancellationToken::new()),
            },
            RequestLifecycleOptions {
                payload: Some(handler),
            },
        );
        assert_eq!(
            executor.drive(observed.stream.result()).stop_reason,
            StopReason::Error
        );
        executor.tick();
        assert_eq!(
            probe.queries.load(Ordering::SeqCst),
            if mode == 0 { 1 } else { 2 }
        );
        assert_eq!(
            probe.calls.load(Ordering::SeqCst),
            if mode == 1 { 3 } else { 2 }
        );
        assert_eq!(observed.metadata.snapshot().is_some(), mode == 1);
        assert!(weak.upgrade().is_none());
    }
}
#[test]
fn configured_lazy_setup_cancel_spawn_reject_consumer_drop_and_completing_poll_transfer() {
    for case in ["pre-cancel", "reject", "consumer-drop", "completing-cancel"] {
        for simple in [false, true] {
            let executor = Arc::new(Executor::default());
            let probe = Probe::new(1, case == "completing-cancel");
            let weak_adapter = Arc::downgrade(&probe);
            let provider = configured_wrapper(probe.clone());
            let model = provider.models().unwrap()[0].clone();
            let adapter = provider.adapter(&model.api).unwrap();
            let cap = adapter.lifecycle_adapter().unwrap();
            let token = CancellationToken::new();
            let runtime = rt(executor.clone(), token.clone());
            let handler = Arc::new(Handler);
            let weak_handler = Arc::downgrade(&handler);
            let (invocation, reader) = RequestLifecycleInvocation::new(RequestLifecycleOptions {
                payload: Some(handler),
            });
            if case == "reject" {
                executor.reject.store(true, Ordering::SeqCst);
            }
            let resolved = ResolvedRequest {
                model,
                endpoint: RequestEndpoint::new_resolved("https://example.invalid").unwrap(),
            };
            let stream = if simple {
                cap.stream_simple_with_lifecycle(
                    SimpleStreamRequest {
                        resolved,
                        context: context(),
                        auth: Default::default(),
                        options: SimpleStreamOptions {
                            wire: Default::default(),
                            runtime,
                        },
                    },
                    invocation,
                )
            } else {
                cap.stream_with_lifecycle(
                    TextStreamRequest {
                        resolved,
                        context: context(),
                        auth: Default::default(),
                        options: StreamOptions {
                            wire: Default::default(),
                            runtime,
                        },
                    },
                    invocation,
                )
            };
            assert_eq!(probe.calls.load(Ordering::SeqCst), 0);
            if case == "pre-cancel" {
                token.cancel();
            }
            if case == "consumer-drop" {
                drop(stream);
                executor.tick();
                executor.tick();
                assert_eq!(probe.calls.load(Ordering::SeqCst), 1);
            } else {
                let result = executor.drive(stream.result());
                assert_eq!(
                    result.stop_reason,
                    if case == "pre-cancel" || case == "completing-cancel" {
                        StopReason::Aborted
                    } else {
                        StopReason::Error
                    }
                );
                assert_eq!(
                    result.usage.input.get(),
                    if case == "completing-cancel" { 17 } else { 0 }
                );
                executor.tick();
            }
            assert_eq!(
                probe.calls.load(Ordering::SeqCst),
                usize::from(case == "consumer-drop" || case == "completing-cancel")
            );
            assert_eq!(
                reader.snapshot().is_some(),
                case == "consumer-drop" || case == "completing-cancel"
            );
            assert!(weak_handler.upgrade().is_none());
            drop(adapter);
            drop(provider);
            drop(probe);
            executor.tick();
            assert!(weak_adapter.upgrade().is_none());
        }
    }
}
