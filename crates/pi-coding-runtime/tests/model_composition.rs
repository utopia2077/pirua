use pi_ai::{
    ApiAdapter, ApiId, ApiKeyAuth, ApiKeyAuthInput, AuthContext, AuthContextError,
    AuthProviderError, AuthResult, BoxFuture, CancellationToken, CredentialInfo,
    CredentialMutation, CredentialReconciliation, CredentialStore, CredentialStoreError, Model,
    ModelBaseUrl, ModelCost, ModelId, ModelInput, ModelThinkingLevel, ModelsError, Provider,
    ProviderAuth, ProviderId, PublicModelBaseUrl, SafeUnsigned, ThinkingLevelMap,
    VersionedCredential,
};
use pi_coding_runtime::{
    CodingRuntimeError, ConfiguredModelOverlay, CredentialInputs, EntranceKind, ModelAvailability,
    ProviderComposition, ProviderCompositionInputs, builtin_image_models, builtin_models,
    selected_entrances,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{collections::BTreeMap, sync::Arc};

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
fn selected_scope_is_exactly_six_text_plus_one_image() {
    let entrances = selected_entrances();
    assert_eq!(entrances.len(), 7);
    assert_eq!(
        entrances
            .iter()
            .filter(|entry| entry.kind == EntranceKind::Text)
            .count(),
        6
    );
    assert_eq!(entrances.last().expect("image").id, "openrouter-images");
    assert!(entrances.iter().all(|entry| {
        matches!(
            entry.id,
            "openai"
                | "google"
                | "anthropic"
                | "deepseek"
                | "openai-codex"
                | "openrouter"
                | "openrouter-images"
        )
    }));
}

#[test]
fn catalog_wrappers_are_feature_selected_and_offline() {
    let text = builtin_models("openrouter").expect("catalog lookup");
    let images = builtin_image_models("openrouter-images").expect("image lookup");
    // The default feature set intentionally has no captured catalog shards;
    // all-feature builds may return the immutable captured rows.
    assert!(
        text.is_empty()
            || text
                .iter()
                .all(|entry| entry.provider.as_str() == "openrouter")
    );
    assert!(
        images.is_empty()
            || images
                .iter()
                .all(|entry| entry.provider.as_str() == "openrouter")
    );
}

#[test]
fn exact_resolution_preserves_openrouter_colons_and_fails_stably() {
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = Arc::new(CountingProvider::new("openrouter", calls.clone()));
    let selected = model("openrouter", "vendor/model:variant");
    let facade = ProviderComposition::new(ProviderCompositionInputs {
        providers: vec![provider],
        models: vec![selected.clone()],
        credentials: CredentialInputs::empty(),
    })
    .expect("composition");

    assert_eq!(
        facade
            .resolve_reference("openrouter:vendor/model:variant")
            .expect("exact model")
            .id
            .as_str(),
        "vendor/model:variant"
    );
    assert!(matches!(
        facade.resolve_reference("openrouter:vendor/missing"),
        Err(CodingRuntimeError::MissingModel { .. })
    ));
    assert!(matches!(
        facade.resolve_reference("missing:model"),
        Err(CodingRuntimeError::MissingProvider { .. })
    ));
    assert!(matches!(
        facade.resolve_reference("openrouter"),
        Err(CodingRuntimeError::InvalidModelReference { .. })
    ));
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
    selected.reasoning = true;
    selected.thinking_level_map = Some(ThinkingLevelMap::new(BTreeMap::from([
        (ModelThinkingLevel::Off, Some("off".into())),
        (ModelThinkingLevel::High, Some("high".into())),
        (ModelThinkingLevel::Max, None),
    ])));

    let composition = ProviderComposition::new(ProviderCompositionInputs::default())
        .expect("empty")
        .with_model_overlays([ConfiguredModelOverlay::new(selected.clone()).with_alias("friendly")])
        .expect("overlay")
        .with_availability([ModelAvailability {
            provider: selected.provider.clone(),
            model_id: selected.id.clone(),
            available: false,
        }]);
    assert!(matches!(
        composition.resolve_selection("friendly"),
        Err(CodingRuntimeError::MissingModel { .. })
    ));

    let available = composition.with_availability([ModelAvailability {
        provider: selected.provider.clone(),
        model_id: selected.id.clone(),
        available: true,
    }]);
    let resolved = available
        .resolve_selection("friendly@high")
        .expect("fuzzy alias and thinking suffix");
    assert_eq!(resolved.model.id.as_str(), "base-model");
    assert_eq!(resolved.thinking_level, ModelThinkingLevel::High);
    assert!(matches!(
        available.resolve_selection("friendly@max"),
        Err(CodingRuntimeError::InvalidModelReference { .. })
    ));
}
