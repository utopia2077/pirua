mod captured_dispatch_support;

use captured_dispatch_support::{Executor, Host, TestClock};
use pi_ai_core::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

fn model() -> Model {
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    serde_json::from_value(fixture["model"].clone()).unwrap()
}

#[derive(Default)]
struct CountingCredentials {
    reads: AtomicUsize,
}

impl CredentialStore for CountingCredentials {
    fn read<'a>(
        &'a self,
        _: &'a ProviderId,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Err(CredentialStoreError::new(CredentialStoreErrorCode::Failed)) })
    }

    fn list<'a>(
        &'a self,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        panic!("no-auth provider must not enumerate credentials")
    }

    fn begin_mutation<'a>(
        &'a self,
        _: &'a ProviderId,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation>, CredentialStoreError>> {
        panic!("no-auth provider must not mutate credentials")
    }

    fn reconcile<'a>(
        &'a self,
        _: &'a ProviderId,
        _: CredentialReconciliation,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        panic!("no-auth provider must not reconcile credentials")
    }
}

struct CaptureAdapter {
    api: ApiId,
    calls: AtomicUsize,
    auth: Mutex<Vec<ResolvedAuth>>,
    endpoints: Mutex<Vec<String>>,
}

impl ApiAdapter for CaptureAdapter {
    fn api_id(&self) -> &ApiId {
        &self.api
    }

    fn stream(&self, _: TextStreamRequest) -> AssistantStream {
        panic!("only simple stream is exercised")
    }

    fn stream_simple(&self, request: SimpleStreamRequest) -> AssistantStream {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.auth.lock().unwrap().push(request.auth);
        self.endpoints
            .lock()
            .unwrap()
            .push(request.resolved.endpoint.expose_for_transport().to_owned());
        let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
            api: request.resolved.model.api,
            provider: request.resolved.model.provider,
            model: request.resolved.model.id,
            clock: request.options.runtime.clock,
            cancellation: request.options.runtime.cancellation,
        });
        producer.fail_setup(StreamSetupFailure::Adapter).unwrap();
        stream
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

struct NoAuthProvider {
    model: Model,
    auth: ProviderAuth,
    adapter: Arc<CaptureAdapter>,
    filters: Mutex<Vec<bool>>,
}

impl Provider for NoAuthProvider {
    fn id(&self) -> &ProviderId {
        &self.model.provider
    }

    fn name(&self) -> &str {
        "no-auth fixture"
    }

    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }

    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(vec![self.model.clone()].into())
    }

    fn filter_models(&self, models: &[Model], credential: Option<&Credential>) -> Vec<Model> {
        self.filters.lock().unwrap().push(credential.is_some());
        models.to_vec()
    }

    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        (api == &self.adapter.api).then(|| self.adapter.clone() as Arc<dyn ApiAdapter>)
    }
}

fn fixture() -> (
    ModelsRegistry,
    Arc<Executor>,
    Arc<CountingCredentials>,
    Arc<NoAuthProvider>,
) {
    let executor = Arc::new(Executor::default());
    let credentials = Arc::new(CountingCredentials::default());
    let model = model();
    let adapter = Arc::new(CaptureAdapter {
        api: model.api.clone(),
        calls: AtomicUsize::new(0),
        auth: Mutex::new(Vec::new()),
        endpoints: Mutex::new(Vec::new()),
    });
    let provider = Arc::new(NoAuthProvider {
        model,
        auth: ProviderAuth::no_auth(),
        adapter,
        filters: Mutex::new(Vec::new()),
    });
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        credentials: credentials.clone(),
        models_store: Box::new(InMemoryModelsStore::new()),
        auth_context: Arc::new(Host),
        clock: Arc::new(TestClock),
        spawner: executor.clone(),
        model_header_policy: ModelHeaderPolicy::from_allowlist(["x-public".to_owned()]),
    });
    executor
        .drive(registry.set_provider(provider.clone()))
        .unwrap();
    (registry, executor, credentials, provider)
}

fn dispatch(
    registry: &ModelsRegistry,
    executor: &Arc<Executor>,
    model: Model,
    overrides: ModelsRequestOverrides,
) -> AssistantMessage {
    let stream = registry.stream_simple(
        model,
        Context {
            system_prompt: None,
            messages: Vec::new(),
            tools: None,
        },
        overrides,
        SimpleStreamOptions {
            wire: Default::default(),
            runtime: RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                Arc::new(TestClock),
                executor.clone(),
            ),
        },
    );
    executor.drive(stream.result())
}

fn auth_overrides() -> [AuthResolutionOverrides; 3] {
    [
        AuthResolutionOverrides {
            api_key: Some(SecretString::new("never-dispatch")),
            ..Default::default()
        },
        AuthResolutionOverrides {
            env: Some(ProviderEnv::new(BTreeMap::new())),
            ..Default::default()
        },
        AuthResolutionOverrides {
            min_oauth_validity_ms: Some(SafeUnsigned::new(0).unwrap()),
            ..Default::default()
        },
    ]
}

fn assert_auth_error(result: Result<Option<AuthResult>, AuthResolutionError>) {
    assert!(matches!(
        result,
        Err(AuthResolutionError::Models(error)) if error.code() == ModelsErrorCode::Auth
    ));
}

#[test]
fn old_credential_constructor_still_requires_a_capability() {
    assert!(matches!(
        ProviderAuth::new(None, None),
        Err(ProviderAuthError::NoCapabilities)
    ));
}

#[test]
fn no_auth_catalog_is_available_without_credential_reads() {
    let (registry, executor, credentials, provider) = fixture();
    let token = CancellationToken::new();
    let available = executor
        .drive(registry.get_available(Some(provider.id()), &token))
        .unwrap();
    assert_eq!(available, vec![provider.model.clone()]);
    assert_eq!(provider.filters.lock().unwrap().as_slice(), &[false]);
    // check_auth reports credential availability; a no-auth model is still available.
    assert!(
        executor
            .drive(registry.check_auth(provider.id(), &token))
            .unwrap()
            .is_none()
    );
    assert_eq!(credentials.reads.load(Ordering::SeqCst), 0);
}

#[test]
fn direct_no_auth_resolution_rejects_overrides_without_reading_credentials() {
    let credentials = CountingCredentials::default();
    let provider = ProviderId::new("custom-provider").unwrap();
    let auth = ProviderAuth::no_auth();
    let token = CancellationToken::new();
    let host = Host;
    let clock = TestClock;
    let executor = Executor::default();
    let resolve = |overrides| {
        resolve_provider_auth(
            &provider,
            &auth,
            &credentials,
            &host,
            &clock,
            overrides,
            &token,
        )
    };
    assert!(
        executor
            .drive(resolve(Default::default()))
            .unwrap()
            .is_none()
    );
    for overrides in auth_overrides() {
        assert_auth_error(executor.drive(resolve(overrides)));
    }
    assert_eq!(credentials.reads.load(Ordering::SeqCst), 0);
    token.cancel();
    assert!(matches!(
        executor.drive(resolve(auth_overrides().into_iter().next().unwrap())),
        Err(AuthResolutionError::Cancelled)
    ));
    assert_eq!(credentials.reads.load(Ordering::SeqCst), 0);
}

#[test]
fn no_auth_registry_queries_reject_overrides_and_model_headers() {
    let (registry, executor, credentials, provider) = fixture();
    let token = CancellationToken::new();
    assert!(
        executor
            .drive(registry.get_auth(provider.id(), Default::default(), &token))
            .unwrap()
            .is_none()
    );
    assert!(
        executor
            .drive(registry.get_model_auth(&provider.model, Default::default(), &token))
            .unwrap()
            .is_none()
    );
    for overrides in auth_overrides() {
        assert_auth_error(executor.drive(registry.get_auth(provider.id(), overrides, &token)));
    }
    for overrides in auth_overrides() {
        assert_auth_error(executor.drive(registry.get_model_auth(
            &provider.model,
            overrides,
            &token,
        )));
    }
    let mut model_headers = provider.model.clone();
    model_headers.headers = Some(
        RawModelHeaders::new(BTreeMap::from([(
            "x-public".to_owned(),
            "possibly-secret".to_owned(),
        )]))
        .unwrap()
        .classify(&ModelHeaderPolicy::from_allowlist(["x-public".to_owned()]))
        .unwrap(),
    );
    assert_auth_error(executor.drive(registry.get_model_auth(
        &model_headers,
        Default::default(),
        &token,
    )));
    assert_eq!(credentials.reads.load(Ordering::SeqCst), 0);
    token.cancel();
    assert!(matches!(
        executor.drive(registry.get_auth(
            provider.id(),
            auth_overrides().into_iter().next().unwrap(),
            &token,
        )),
        Err(AuthResolutionError::Cancelled)
    ));
    assert!(matches!(
        executor.drive(registry.get_model_auth(&model_headers, Default::default(), &token)),
        Err(AuthResolutionError::Cancelled)
    ));
}

#[test]
fn no_auth_request_has_empty_auth_and_rejects_credential_inputs_before_adapter() {
    let (registry, executor, credentials, provider) = fixture();
    let default = dispatch(
        &registry,
        &executor,
        provider.model.clone(),
        Default::default(),
    );
    assert_eq!(
        default.error_message.as_deref(),
        Some("stream adapter setup failed")
    );
    assert_eq!(provider.adapter.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        provider.adapter.auth.lock().unwrap().as_slice(),
        &[ResolvedAuth::default()]
    );
    assert_eq!(
        provider.adapter.endpoints.lock().unwrap().as_slice(),
        &["https://example.invalid"]
    );
    assert_eq!(credentials.reads.load(Ordering::SeqCst), 0);

    let [key, env, minimum] = auth_overrides().map(|auth| ModelsRequestOverrides {
        auth,
        ..Default::default()
    });
    let headers = ModelsRequestOverrides {
        headers: ProviderHeaders::new(BTreeMap::from([(
            "x-public".to_owned(),
            Some("possibly-secret".to_owned()),
        )])),
        ..Default::default()
    };
    let mut model_headers = provider.model.clone();
    model_headers.headers = Some(
        RawModelHeaders::new(BTreeMap::from([(
            "x-public".to_owned(),
            "possibly-secret".to_owned(),
        )]))
        .unwrap()
        .classify(&ModelHeaderPolicy::from_allowlist(["x-public".to_owned()]))
        .unwrap(),
    );
    for (model, overrides) in [
        (provider.model.clone(), key),
        (provider.model.clone(), env),
        (provider.model.clone(), minimum),
        (provider.model.clone(), headers),
        (model_headers, ModelsRequestOverrides::default()),
    ] {
        let result = dispatch(&registry, &executor, model, overrides);
        assert_eq!(
            result.error_message.as_deref(),
            Some("stream authentication failed")
        );
        assert_eq!(provider.adapter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(credentials.reads.load(Ordering::SeqCst), 0);
    }
}
