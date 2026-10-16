use pi_ai::{
    ApiAdapter, ApiId, ApiKeyAuth, ApiKeyAuthInput, AuthContext, AuthContextError,
    AuthProviderError, AuthResult, BoxFuture, CancellationToken, CredentialInfo,
    CredentialMutation, CredentialReconciliation, CredentialStore, CredentialStoreError, Model,
    ModelBaseUrl, ModelCost, ModelId, ModelInput, ModelThinkingLevel, ModelsError, Provider,
    ProviderAuth, ProviderId, PublicModelBaseUrl, SafeUnsigned, VersionedCredential,
};
use pi_coding_runtime::{
    CodingRuntimeError, ConfiguredModelOverlay, CredentialInputs, ModelAvailability,
    ProviderComposition, ProviderCompositionInputs, builtin_models, selected_entrances,
};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

struct NoopApiKey;

struct PanicAuthContext;

struct PanicCredentialStore;

impl CredentialStore for PanicCredentialStore {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        panic!("the composition facade must not read credentials");
    }

    fn list<'a>(
        &'a self,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        panic!("the composition facade must not list credentials");
    }

    fn begin_mutation<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation + 'static>, CredentialStoreError>> {
        panic!("the composition facade must not mutate credentials");
    }

    fn reconcile<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _resolution: CredentialReconciliation,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        panic!("the composition facade must not reconcile credentials");
    }
}

impl AuthContext for PanicAuthContext {
    fn env<'a>(
        &'a self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        panic!("the composition facade must not read ambient auth");
    }

    fn file_exists<'a>(
        &'a self,
        _path: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        panic!("the composition facade must not inspect auth files");
    }
}

impl ApiKeyAuth for NoopApiKey {
    fn name(&self) -> &str {
        "test"
    }

    fn resolve<'a>(
        &'a self,
        _input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        Box::pin(async { Ok(None) })
    }
}

struct CountingProvider {
    id: ProviderId,
    model_calls: Arc<AtomicUsize>,
    auth: ProviderAuth,
}

impl CountingProvider {
    fn new(id: &str, model_calls: Arc<AtomicUsize>) -> Self {
        Self {
            id: ProviderId::new(id).expect("provider id"),
            model_calls,
            auth: ProviderAuth::new(Some(Arc::new(NoopApiKey)), None).expect("auth"),
        }
    }
}

impl Provider for CountingProvider {
    fn id(&self) -> &ProviderId {
        &self.id
    }

    fn name(&self) -> &str {
        self.id.as_str()
    }

    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }

    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        self.model_calls.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::from([]))
    }

    fn adapter(&self, _api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        None
    }
}

fn model(provider: &str, id: &str) -> Model {
    Model {
        id: ModelId::new(id).expect("model id"),
        name: id.to_owned(),
        api: ApiId::new(ApiId::OPENAI_COMPLETIONS).expect("api"),
        provider: ProviderId::new(provider).expect("provider"),
        base_url: ModelBaseUrl::Public(
            PublicModelBaseUrl::new("https://example.invalid").expect("url"),
        ),
        model_type: None,
        sampling_params_by_thinking_level: None,
        prompt_cache: None,
        input_limits: None,
        reasoning: false,
        thinking_level_map: None,
        input: vec![ModelInput::Text],
        cost: ModelCost {
            input: 0.0.try_into().expect("finite"),
            output: 0.0.try_into().expect("finite"),
            cache_read: 0.0.try_into().expect("finite"),
            cache_write: 0.0.try_into().expect("finite"),
            tiers: None,
        },
        context_window: SafeUnsigned::new(1).expect("safe"),
        max_tokens: SafeUnsigned::new(1).expect("safe"),
        sampling_params: None,
        headers: None,
        compat: None,
    }
}

#[test]
fn selected_scope_is_exactly_four_builtin_provider_entrances() {
    assert_eq!(
        selected_entrances()
            .iter()
            .map(|entry| entry.id)
            .collect::<Vec<_>>(),
        ["openai", "google", "anthropic", "deepseek"]
    );
}

#[test]
fn catalog_wrappers_are_feature_selected_and_offline() {
    for provider in ["openai", "google", "anthropic", "deepseek"] {
        let models = builtin_models(provider).unwrap();
        assert!(
            models
                .iter()
                .all(|model| model.provider.as_str() == provider)
        );
    }
    for provider in ["openrouter", "openai-codex", "azure", "custom-provider"] {
        assert!(builtin_models(provider).unwrap().is_empty());
    }
}

#[test]
fn exact_resolution_preserves_custom_model_colons_and_fails_stably() {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(CountingProvider::new("custom-provider", calls.clone()));
    let selected = model("custom-provider", "vendor/model:variant");
    let facade = ProviderComposition::new(ProviderCompositionInputs {
        providers: vec![provider],
        models: vec![selected.clone()],
        credentials: CredentialInputs::empty(),
    })
    .unwrap();
    assert_eq!(
        facade
            .resolve_reference("custom-provider/vendor/model:variant")
            .unwrap(),
        &selected
    );
    assert!(
        facade
            .resolve_reference("custom-provider/vendor/missing")
            .is_err()
    );
    assert!(facade.resolve_reference("missing/model").is_err());
    assert!(facade.resolve_selection("not-present").is_err());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn capability_failure_is_before_request_and_has_no_provider_side_effects() {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(CountingProvider::new("openai", calls.clone()));
    let facade = ProviderComposition::new(ProviderCompositionInputs {
        providers: vec![provider],
        models: Vec::new(),
        credentials: CredentialInputs::empty(),
    })
    .expect("composition");

    assert!(!facade.has_capability("models.resolve"));
    assert_eq!(
        facade.require_capability("models.resolve"),
        Err(CodingRuntimeError::MissingCapability {
            capability: "models.resolve"
        })
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn credentials_are_injected_without_auth_or_storage_calls() {
    let store: Arc<dyn CredentialStore> = Arc::new(PanicCredentialStore);
    let auth_context: Arc<dyn AuthContext> = Arc::new(PanicAuthContext);
    let facade = ProviderComposition::new(ProviderCompositionInputs {
        credentials: CredentialInputs::new(Some(store.clone()), Some(auth_context.clone())),
        ..ProviderCompositionInputs::default()
    })
    .expect("empty");

    assert!(Arc::ptr_eq(
        facade.credentials().store().expect("store retained"),
        &store
    ));
    assert!(Arc::ptr_eq(
        facade
            .credentials()
            .auth_context()
            .expect("auth context retained"),
        &auth_context
    ));
}

#[test]
fn overlays_have_precedence_and_availability_is_host_supplied() {
    let mut selected = model("openai", "base-model");
    selected.name = "Friendly Runtime".into();
    let original = model("openai", "base-model");
    let composition = ProviderComposition::new(ProviderCompositionInputs {
        models: vec![original],
        ..Default::default()
    })
    .unwrap()
    .with_model_overlays([ConfiguredModelOverlay::new(selected.clone()).with_alias("friendly")])
    .unwrap()
    .with_availability([ModelAvailability {
        provider: selected.provider.clone(),
        model_id: selected.id.clone(),
        available: false,
    }]);
    for reference in ["friendly", "openai/base-model", "Runtime"] {
        assert!(matches!(
            composition.resolve_selection(reference),
            Err(CodingRuntimeError::MissingModel { .. })
        ));
    }
    let available = composition.with_availability([ModelAvailability {
        provider: selected.provider.clone(),
        model_id: selected.id.clone(),
        available: true,
    }]);
    for reference in ["friendly", "openai/base-model", "Runtime"] {
        assert_eq!(
            available.resolve_selection(reference).unwrap().model,
            &selected
        );
    }
    assert_eq!(
        available
            .resolve_selection("openai/base-model:high")
            .unwrap()
            .thinking_level,
        ModelThinkingLevel::High
    );
}

#[test]
fn unregistered_fuzzy_selection_honors_availability_snapshot() {
    let mut selected = model("openai", "base-model");
    selected.name = "Friendly Runtime".into();
    selected.reasoning = true;
    let composition = ProviderComposition::new(ProviderCompositionInputs::default())
        .expect("empty")
        .with_model_overlays([ConfiguredModelOverlay::new(selected.clone()).with_alias("friendly")])
        .expect("overlay")
        .with_availability([ModelAvailability {
            provider: selected.provider.clone(),
            model_id: selected.id.clone(),
            available: false,
        }]);

    // These substrings match the name without using the configured alias.
    for reference in ["  RUNtime  ", "Runtime:high"] {
        assert_eq!(
            composition.resolve_selection(reference).unwrap_err(),
            CodingRuntimeError::MissingModel {
                provider: selected.provider.clone(),
                model_id: selected.id.clone(),
            }
        );
    }

    let available = composition.with_availability([ModelAvailability {
        provider: selected.provider.clone(),
        model_id: selected.id.clone(),
        available: true,
    }]);
    for (reference, thinking_level) in [
        ("  RUNtime  ", ModelThinkingLevel::Off),
        ("Runtime:high", ModelThinkingLevel::High),
    ] {
        let resolved = available
            .resolve_selection(reference)
            .expect("availability-restored unregistered substring");
        assert_eq!(resolved.model.provider, selected.provider);
        assert_eq!(resolved.model.id, selected.id);
        assert_eq!(resolved.thinking_level, thinking_level);
    }
}

#[test]
fn cross_provider_equal_ids_remain_ambiguous_until_one_provider_is_usable() {
    let first = model("openai", "shared");
    let second = model("custom-provider", "shared");
    for (first_available, second_available) in
        [(false, true), (true, false), (false, false), (true, true)]
    {
        let composition = ProviderComposition::new(ProviderCompositionInputs {
            models: vec![first.clone(), second.clone()],
            ..Default::default()
        })
        .unwrap()
        .with_availability([
            ModelAvailability {
                provider: first.provider.clone(),
                model_id: first.id.clone(),
                available: first_available,
            },
            ModelAvailability {
                provider: second.provider.clone(),
                model_id: second.id.clone(),
                available: second_available,
            },
        ]);
        if first_available != second_available {
            assert_eq!(
                composition.resolve_selection("shared").unwrap().model,
                if first_available { &first } else { &second }
            );
        } else {
            assert!(matches!(
                composition.resolve_selection("shared"),
                Err(CodingRuntimeError::InvalidModelReference { .. })
            ));
        }
    }
}
