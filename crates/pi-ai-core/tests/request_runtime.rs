use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use pi_ai_core::{
    ApiAdapter, ApiId, AssistantMessageEventStream, AssistantStreamInit, BoxFuture,
    CancellationToken, Cancelled, CapabilityError, Clock, Context, DeferredCancelOptions,
    DeferredCancelOptionsWire, DeferredCancelRequest, DeferredFetchOptions,
    DeferredFetchOptionsWire, DeferredFetchRequest, DeferredHandle, DeferredId, Model, ModelsError,
    ModelsErrorCode, ProviderEnv, ProviderHeaders, RequestEndpoint, RequestRuntime, ResolvedAuth,
    ResolvedRequest, SecretString, SimpleStreamOptions, SimpleStreamOptionsWire,
    SimpleStreamRequest, SpawnError, StreamOptions, StreamOptionsWire, StreamSetupFailure,
    TaskSpawner, TextStreamRequest, create_assistant_message_event_stream,
};
use pi_telemetry::{NOOP_TELEMETRY_SPAN, TelemetrySpan};

const MODELS_OPTIONS: &str =
    include_str!("../../../conformance/schema/ai/models-options.golden.json");

struct TestClock;

impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        1
    }

    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            if cancellation.is_cancelled() {
                Err(Cancelled)
            } else {
                Ok(())
            }
        })
    }
}

struct TestSpawner;

impl TaskSpawner for TestSpawner {
    fn spawn(&self, _task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Ok(())
    }
}

fn runtime() -> (RequestRuntime, Arc<dyn Clock>) {
    let clock: Arc<dyn Clock> = Arc::new(TestClock);
    let telemetry: Arc<dyn TelemetrySpan> = Arc::new(NOOP_TELEMETRY_SPAN);
    (
        RequestRuntime {
            cancellation: CancellationToken::new(),
            telemetry,
            provider_event_observer: None,
            clock: Arc::clone(&clock),
            spawner: Arc::new(TestSpawner),
        },
        clock,
    )
}

fn models() -> Model {
    let value: serde_json::Value = serde_json::from_str(MODELS_OPTIONS).unwrap();
    serde_json::from_value(value["model"].clone()).unwrap()
}

fn endpoint() -> RequestEndpoint {
    RequestEndpoint::new_resolved("https://request-endpoint-sentinel.invalid")
        .expect("nonempty resolved endpoint")
}

fn auth() -> ResolvedAuth {
    ResolvedAuth {
        api_key: Some(SecretString::new("api-key-secret-sentinel")),
        headers: ProviderHeaders::new(BTreeMap::from([
            (
                "authorization".to_owned(),
                Some("header-secret-sentinel".to_owned()),
            ),
            ("x-suppress".to_owned(), None),
        ])),
        env: ProviderEnv::new(BTreeMap::from([(
            "TOKEN".to_owned(),
            "env-secret-sentinel".to_owned(),
        )])),
    }
}

fn handle(model: &Model) -> DeferredHandle {
    DeferredHandle {
        provider: model.provider.clone(),
        model_id: model.id.clone(),
        api: model.api.clone(),
        id: DeferredId::new("deferred-1").expect("nonempty deferred id"),
        expires_at: None,
        poll_after_ms: None,
        data: pi_ai_core::Presence::Missing,
    }
}

#[test]
fn endpoint_is_explicit_nonempty_and_wholly_redacted() {
    assert!(RequestEndpoint::new_resolved("").is_err());
    let endpoint = endpoint();
    assert_eq!(
        endpoint.expose_for_transport(),
        "https://request-endpoint-sentinel.invalid"
    );
    assert!(!format!("{endpoint:?}").contains("request-endpoint-sentinel"));

    let source = include_str!("../src/request.rs");
    assert!(!source.contains("impl fmt::Display for RequestEndpoint"));
    assert!(!source.contains("impl AsRef<str> for RequestEndpoint"));
    assert!(!source.contains("impl std::ops::Deref for RequestEndpoint"));
}

#[test]
fn resolved_auth_debug_lists_only_presence_and_keys() {
    let rendered = format!("{:?}", auth());
    assert!(rendered.contains("api_key_present: true"));
    assert!(rendered.contains("authorization"));
    assert!(rendered.contains("x-suppress"));
    assert!(rendered.contains("TOKEN"));
    for secret in [
        "api-key-secret-sentinel",
        "header-secret-sentinel",
        "env-secret-sentinel",
    ] {
        assert!(!rendered.contains(secret));
    }
}

#[test]
fn all_five_nominal_roots_carry_their_exact_runtime_wrapper() {
    let model = models();
    let request_handle = handle(&model);
    let (runtime, clock) = runtime();
    let text = TextStreamRequest {
        resolved: ResolvedRequest {
            model: model.clone(),
            endpoint: endpoint(),
        },
        context: pi_ai_core::normalize_context(Context {
            system_prompt: None,
            messages: Vec::new(),
            tools: None,
        }),
        auth: auth(),
        options: StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: runtime.clone(),
        },
    };
    assert!(Arc::ptr_eq(&text.options.runtime.clock, &clock));
    assert!(!format!("{text:?}").contains("secret-sentinel"));

    let simple = SimpleStreamRequest {
        resolved: ResolvedRequest {
            model: model.clone(),
            endpoint: endpoint(),
        },
        context: pi_ai_core::normalize_context(Context {
            system_prompt: None,
            messages: Vec::new(),
            tools: None,
        }),
        auth: auth(),
        options: SimpleStreamOptions {
            wire: SimpleStreamOptionsWire::default(),
            runtime: runtime.clone(),
        },
    };
    assert!(Arc::ptr_eq(&simple.options.runtime.clock, &clock));

    let fetch = DeferredFetchRequest {
        resolved: ResolvedRequest {
            model: model.clone(),
            endpoint: endpoint(),
        },
        handle: request_handle.clone(),
        auth: auth(),
        options: DeferredFetchOptions {
            wire: DeferredFetchOptionsWire::default(),
            runtime: runtime.clone(),
        },
    };
    assert!(Arc::ptr_eq(&fetch.options.runtime.clock, &clock));

    let cancel = DeferredCancelRequest {
        resolved: ResolvedRequest {
            model,
            endpoint: endpoint(),
        },
        handle: request_handle,
        auth: auth(),
        options: DeferredCancelOptions {
            wire: DeferredCancelOptionsWire::default(),
            runtime: runtime.clone(),
        },
    };
    assert!(Arc::ptr_eq(&cancel.options.runtime.clock, &clock));
}

#[test]
fn deferred_wire_fields_are_exact_and_non_null() {
    let fetch: DeferredFetchOptionsWire = serde_json::from_value(serde_json::json!({
        "timeoutMs": 1,
        "maxRetries": 2,
        "maxRetryDelayMs": 3,
        "wait": 4
    }))
    .expect("complete deferred-fetch shape");
    assert_eq!(fetch.wait.map(pi_ai_core::SafeUnsigned::get), Some(4));

    let cancel: DeferredCancelOptionsWire = serde_json::from_value(serde_json::json!({
        "timeoutMs": 1,
        "maxRetries": 2,
        "maxRetryDelayMs": 3
    }))
    .expect("complete deferred-cancel shape");
    assert_eq!(
        cancel.max_retry_delay_ms.map(pi_ai_core::SafeUnsigned::get),
        Some(3)
    );

    for invalid in [
        serde_json::json!({"wait": null}),
        serde_json::json!({"timeoutMs": null}),
        serde_json::json!({"maxRetries": null}),
        serde_json::json!({"maxRetryDelayMs": null}),
        serde_json::json!({"unknown": 1}),
    ] {
        assert!(serde_json::from_value::<DeferredFetchOptionsWire>(invalid).is_err());
    }
    assert!(
        serde_json::from_value::<DeferredCancelOptionsWire>(serde_json::json!({"wait": 1}))
            .is_err()
    );
}

struct AdapterProbe {
    api: ApiId,
}

impl ApiAdapter for AdapterProbe {
    fn api_id(&self) -> &ApiId {
        &self.api
    }

    fn stream(&self, request: TextStreamRequest) -> AssistantMessageEventStream {
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: request.resolved.model.api,
            provider: request.resolved.model.provider,
            model: request.resolved.model.id,
            clock: request.options.runtime.clock,
            cancellation: request.options.runtime.cancellation,
        });
        producer
            .fail_setup(StreamSetupFailure::Adapter)
            .expect("adapter failure is in-band");
        stream
    }

    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantMessageEventStream {
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: request.resolved.model.api,
            provider: request.resolved.model.provider,
            model: request.resolved.model.id,
            clock: request.options.runtime.clock,
            cancellation: request.options.runtime.cancellation,
        });
        producer
            .fail_setup(StreamSetupFailure::Adapter)
            .expect("adapter failure is in-band");
        stream
    }

    fn fetch_deferred(
        &self,
        _request: DeferredFetchRequest,
    ) -> Result<AssistantMessageEventStream, CapabilityError> {
        Err(CapabilityError::Unsupported)
    }

    fn cancel_deferred<'a>(
        &'a self,
        _request: DeferredCancelRequest,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn adapter_ports_are_object_safe_and_factory_signature_is_exact() {
    fn accepts_api(_adapter: &dyn ApiAdapter) {}
    let adapter = AdapterProbe {
        api: ApiId::new("probe").expect("nonempty API"),
    };
    accepts_api(&adapter);
    let _factory: fn(
        AssistantStreamInit,
    ) -> (
        AssistantMessageEventStream,
        pi_ai_core::AssistantStreamProducer,
    ) = create_assistant_message_event_stream;
}

#[test]
fn runtime_errors_are_fixed_redacted_and_retain_explicit_sources() {
    #[derive(Debug)]
    struct SentinelSource;

    impl fmt::Display for SentinelSource {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("runtime-source-secret-sentinel")
        }
    }

    impl Error for SentinelSource {}

    let spawn = SpawnError::with_source(SentinelSource);
    assert_eq!(spawn.to_string(), "task could not be spawned");
    assert!(!format!("{spawn:?}").contains("runtime-source-secret-sentinel"));
    assert!(spawn.source().is_some_and(|source| {
        source
            .to_string()
            .contains("runtime-source-secret-sentinel")
    }));

    let models = ModelsError::with_source(ModelsErrorCode::Provider, SentinelSource);
    assert_eq!(models.to_string(), "provider operation failed");
    assert!(!format!("{models:?}").contains("runtime-source-secret-sentinel"));
    assert!(models.source().is_some());
}
