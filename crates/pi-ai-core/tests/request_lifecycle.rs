mod request_lifecycle_support;
use pi_ai_core::*;
use request_lifecycle_support::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Unsupported {
    model: Model,
    queries: AtomicUsize,
    calls: AtomicUsize,
}
impl ApiAdapter for Unsupported {
    fn api_id(&self) -> &ApiId {
        &self.model.api
    }
    fn lifecycle_adapter(&self) -> Option<&dyn RequestLifecycleAdapter> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        None
    }
    fn stream(&self, r: TextStreamRequest) -> AssistantStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: self.model.api.clone(),
            provider: self.model.provider.clone(),
            model: self.model.id.clone(),
            clock: r.options.runtime.clock,
            cancellation: r.options.runtime.cancellation,
        });
        producer.fail_setup(StreamSetupFailure::Adapter).unwrap();
        stream
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
        Box::pin(async { Ok(()) })
    }
}
struct EmptyAuth;
impl ApiKeyAuth for EmptyAuth {
    fn name(&self) -> &str {
        "fixture"
    }
    fn resolve<'a>(
        &'a self,
        _: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async {
            Ok(Some(AuthResult {
                auth: ModelAuth {
                    api_key: Some(SecretString::new("fixture")),
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
    model: Model,
    adapter: Arc<Unsupported>,
    auth: ProviderAuth,
}
impl Provider for Product {
    fn id(&self) -> &ProviderId {
        &self.model.provider
    }
    fn name(&self) -> &str {
        "fixture"
    }
    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }
    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(vec![self.model.clone()].into())
    }
    fn adapter(&self, _: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        Some(self.adapter.clone())
    }
}
#[test]
fn unsupported_observed_dispatch_fails_before_adapter_but_ordinary_never_queries_capability() {
    let executor = Arc::new(Executor::default());
    let value: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    let model: Model = serde_json::from_value(value["model"].clone()).unwrap();
    let adapter = Arc::new(Unsupported {
        model: model.clone(),
        queries: AtomicUsize::new(0),
        calls: AtomicUsize::new(0),
    });
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(InMemoryModelsStore::new()),
        auth_context: Arc::new(Host),
        clock: Arc::new(TestClock),
        spawner: executor.clone(),
        model_header_policy: Default::default(),
    });
    executor
        .drive(registry.set_provider(Arc::new(Product {
            model: model.clone(),
            adapter: adapter.clone(),
            auth: ProviderAuth::new(Some(Arc::new(EmptyAuth)), None).unwrap(),
        })))
        .unwrap();
    let runtime = || {
        RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            Arc::new(TestClock),
            executor.clone(),
        )
    };
    let context = || serde_json::from_value(serde_json::json!({"messages":[]})).unwrap();
    for simple in [false, true] {
        let stream = if simple {
            registry.stream_simple(
                model.clone(),
                context(),
                Default::default(),
                SimpleStreamOptions {
                    wire: Default::default(),
                    runtime: runtime(),
                },
            )
        } else {
            registry.stream(
                model.clone(),
                context(),
                Default::default(),
                StreamOptions {
                    wire: Default::default(),
                    runtime: runtime(),
                },
            )
        };
        assert_eq!(
            executor.drive(stream.result()).stop_reason,
            StopReason::Error
        );
    }
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 2);
    assert_eq!(adapter.queries.load(Ordering::SeqCst), 0);
    for simple in [false, true] {
        let observed = if simple {
            registry.stream_simple_with_lifecycle(
                model.clone(),
                context(),
                Default::default(),
                SimpleStreamOptions {
                    wire: Default::default(),
                    runtime: runtime(),
                },
                Default::default(),
            )
        } else {
            registry.stream_with_lifecycle(
                model.clone(),
                context(),
                Default::default(),
                StreamOptions {
                    wire: Default::default(),
                    runtime: runtime(),
                },
                Default::default(),
            )
        };
        assert_eq!(
            executor.drive(observed.stream.result()).stop_reason,
            StopReason::Error
        );
        assert!(observed.metadata.snapshot().is_none());
    }
    assert_eq!(adapter.calls.load(Ordering::SeqCst), 2);
    assert_eq!(adapter.queries.load(Ordering::SeqCst), 2);
}
#[test]
fn recorder_preserves_numbers_order_detachment_and_redaction_without_retaining_handler() {
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
    let handler = Arc::new(Handler);
    let weak = Arc::downgrade(&handler);
    let (invocation, reader) = RequestLifecycleInvocation::new(RequestLifecycleOptions {
        payload: Some(handler.clone()),
    });
    let (other, other_reader) = RequestLifecycleInvocation::new(Default::default());
    let head = RequestResponseHead {
        status: 201,
        headers: vec![
            (
                "private-header".into(),
                RequestResponseHeaderValue::Text("private-value".into()),
            ),
            (
                "count".into(),
                RequestResponseHeaderValue::Number(FiniteF64::new(7.5).unwrap()),
            ),
        ],
    };
    invocation.recorder.record(head.clone());
    assert_eq!(reader.snapshot(), Some(head.clone()));
    assert!(other_reader.snapshot().is_none());
    let mut detached = reader.snapshot().unwrap();
    detached.headers.reverse();
    assert_eq!(reader.snapshot(), Some(head));
    for debug in [
        format!("{invocation:?}"),
        format!("{reader:?}"),
        format!("{detached:?}"),
        format!("{:?}", detached.headers[1].1),
    ] {
        assert!(!debug.contains("private"));
    }
    drop(invocation);
    drop(handler);
    drop(other);
    assert!(weak.upgrade().is_none());
    assert!(reader.snapshot().is_some());
}
