use pi_ai::{
    ApiAdapter, ApiId, ApiKeyAuth, ApiKeyAuthInput, AuthContext, AuthContextError,
    AuthProviderError, AuthResult, BoxFuture, CancellationToken, CredentialInfo,
    CredentialMutation, CredentialReconciliation, CredentialStore, CredentialStoreError,
    MaxRetries, Model, ModelBaseUrl, ModelCost, ModelId, ModelInput, ModelThinkingLevel,
    ModelsError, Provider, ProviderAuth, ProviderId, PublicModelBaseUrl, RetryPolicy, SafeUnsigned,
    StreamOptionsWire, ThinkingLevelMap, VersionedCredential,
};
use pi_coding_runtime::{
    CodingRuntimeError, CodingSettingsSnapshot, CompactionSettings, ConfiguredModelOverlay,
    CredentialInputs, ModelAvailability, ModelRef, ProviderComposition, ProviderCompositionInputs,
    QueueMode, default_thinking_level,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

// Independent expectations from pinned pi-mono model-resolver.ts:19-60.
const PINNED_DEFAULTS: [(&str, &str); 6] = [
    ("anthropic", "claude-opus-4-8"),
    ("openai", "gpt-5.5"),
    ("openai-codex", "gpt-5.5"),
    ("deepseek", "deepseek-v4-pro"),
    ("google", "gemini-3.1-pro-preview"),
    ("openrouter", "moonshotai/kimi-k2.6"),
];

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

fn composition(models: Vec<Model>) -> ProviderComposition {
    ProviderComposition::new(ProviderCompositionInputs {
        models,
        ..ProviderCompositionInputs::default()
    })
    .expect("detached composition")
}

fn availability(model: &Model, available: bool) -> ModelAvailability {
    ModelAvailability {
        provider: model.provider.clone(),
        model_id: model.id.clone(),
        available,
    }
}

fn identity(model: &Model) -> (&str, &str) {
    (model.provider.as_str(), model.id.as_str())
}

#[test]
fn each_pinned_default_beats_earlier_nonpreferred_models() {
    for (provider, id) in PINNED_DEFAULTS {
        let facade = composition(vec![
            model(provider, "000-nonpreferred"),
            model(provider, id),
            model("anthropic", "001-fallback"),
        ]);
        assert_eq!(
            identity(facade.select_default_model().expect("pinned default")),
            (provider, id),
        );
    }
}

#[test]
fn selected_defaults_preserve_pinned_relative_provider_priority() {
    for first in 0..PINNED_DEFAULTS.len() {
        let mut models: Vec<_> = PINNED_DEFAULTS[first..]
            .iter()
            .rev()
            .map(|(provider, id)| model(provider, id))
            .collect();
        models.push(model("anthropic", "000-fallback"));
        let facade = composition(models);
        assert_eq!(
            identity(facade.select_default_model().expect("preferred default")),
            PINNED_DEFAULTS[first],
        );
    }
}

#[test]
fn explicit_unavailability_skips_defaults_then_applies_to_fallback() {
    let defaults: Vec<_> = PINNED_DEFAULTS
        .iter()
        .map(|(provider, id)| model(provider, id))
        .collect();
    let first_fallback = model("anthropic", "000-fallback");
    let second_fallback = model("deepseek", "000-fallback");
    let mut models = defaults.clone();
    models.extend([first_fallback.clone(), second_fallback.clone()]);
    let facade = composition(models).with_availability(
        defaults
            .iter()
            .enumerate()
            .map(|(index, value)| availability(value, index == defaults.len() - 1)),
    );
    assert_eq!(
        identity(facade.select_default_model().expect("last usable default")),
        PINNED_DEFAULTS[5],
    );

    let facade = facade.with_availability(defaults.iter().map(|value| availability(value, false)));
    assert_eq!(
        identity(facade.select_default_model().expect("usable fallback")),
        identity(&first_fallback),
    );
    let facade = facade.with_availability(
        defaults
            .iter()
            .chain([&first_fallback])
            .map(|value| availability(value, false)),
    );
    assert_eq!(
        identity(facade.select_default_model().expect("next usable fallback")),
        identity(&second_fallback),
    );
}

#[test]
fn fallback_uses_existing_order_across_model_and_snapshot_permutations() {
    let models = [
        model("openrouter", "a-model"),
        model("anthropic", "z-model"),
        model("google", "a-model"),
        model("anthropic", "a-model"),
    ];
    for rotation in 0..models.len() {
        for reverse in [false, true] {
            let mut permuted = models.to_vec();
            permuted.rotate_left(rotation);
            if reverse {
                permuted.reverse();
            }
            let snapshot: Vec<_> = permuted
                .iter()
                .map(|value| availability(value, value.id.as_str() != "z-model"))
                .collect();
            let facade = composition(permuted).with_availability(snapshot.into_iter().rev());
            let selected = facade
                .select_default_model()
                .expect("deterministic fallback");
            assert_eq!(identity(selected), ("anthropic", "a-model"));
            assert!(std::ptr::eq(
                selected,
                facade.models().next().expect("first model")
            ));
        }
    }
}

#[test]
fn empty_and_all_unavailable_snapshots_return_none() {
    let unknown = model("openai", "absent-model");
    let empty = composition(Vec::new()).with_availability([availability(&unknown, true)]);
    assert!(empty.select_default_model().is_none());

    let mut models: Vec<_> = PINNED_DEFAULTS
        .iter()
        .map(|(provider, id)| model(provider, id))
        .collect();
    models.push(model("anthropic", "000-fallback"));
    let snapshot: Vec<_> = models
        .iter()
        .map(|value| availability(value, false))
        .collect();
    let facade = composition(models).with_availability(snapshot);
    assert!(facade.select_default_model().is_none());
}

#[test]
fn defaults_keep_the_selected_text_entrance_admission_boundary() {
    for provider in [
        "amazon-bedrock",
        "ant-ling",
        "azure-openai-responses",
        "google-vertex",
        "github-copilot",
        "openrouter-images",
        "custom-provider",
    ] {
        let outside = model(provider, "gpt-5.5");
        let expected = CodingRuntimeError::UnsupportedProvider {
            provider: outside.provider.clone(),
        };
        assert_eq!(
            ProviderComposition::new(ProviderCompositionInputs {
                models: vec![outside.clone()],
                ..ProviderCompositionInputs::default()
            })
            .expect_err("outside selected text entrances"),
            expected,
        );
        assert_eq!(
            composition(Vec::new())
                .with_model_overlays([ConfiguredModelOverlay::new(outside)])
                .expect_err("overlay outside selected text entrances"),
            expected,
        );
    }
}

#[test]
fn default_selection_uses_the_detached_overlay_and_current_host_snapshot() {
    let mut overlay = model("openai", "gpt-5.5");
    overlay.name = "Host supplied default overlay".into();
    let fallback = model("anthropic", "fallback");
    let facade = composition(vec![model("openai", "gpt-5.5"), fallback.clone()])
        .with_model_overlays([ConfiguredModelOverlay::new(overlay.clone()).with_alias("host")])
        .expect("overlay");
    assert_eq!(
        facade.select_default_model().expect("overlay default").name,
        overlay.name,
    );
    let facade = facade.with_availability([availability(&overlay, false)]);
    assert_eq!(
        identity(facade.select_default_model().expect("fallback")),
        identity(&fallback),
    );
    let facade = facade.with_availability([availability(&overlay, true)]);
    assert_eq!(
        identity(facade.select_default_model().expect("restored default")),
        identity(&overlay),
    );
}

#[test]
fn default_thinking_is_separate_from_explicit_host_model_and_suffixes() {
    assert_eq!(default_thinking_level(), ModelThinkingLevel::Medium);
    assert_eq!(
        pi_coding_runtime::models::default_thinking_level(),
        default_thinking_level()
    );
    let preferred = model("anthropic", "claude-opus-4-8");
    let mut explicit = model("openrouter", "vendor/host:model");
    explicit.reasoning = true;
    explicit.thinking_level_map = Some(ThinkingLevelMap::new(BTreeMap::from([
        (ModelThinkingLevel::Off, Some("off".into())),
        (ModelThinkingLevel::High, Some("high".into())),
        (ModelThinkingLevel::Max, None),
    ])));
    let facade = composition(vec![preferred.clone(), explicit.clone()])
        .with_model_overlays([ConfiguredModelOverlay::new(explicit.clone()).with_alias("host")])
        .expect("alias");
    let settings = CodingSettingsSnapshot::new(
        ModelRef::new(explicit.provider.clone(), explicit.id.clone()),
        ModelThinkingLevel::High,
        Vec::new(),
        RetryPolicy {
            enabled: false,
            max_retries: MaxRetries::new(0).expect("retry count"),
            base_delay_ms: SafeUnsigned::new(0).expect("delay"),
        },
        CompactionSettings {
            enabled: false,
            reserve_tokens: SafeUnsigned::new(0).expect("reserve"),
            keep_recent_tokens: SafeUnsigned::new(0).expect("tail"),
        },
        StreamOptionsWire::default(),
        QueueMode::All,
        QueueMode::All,
    );
    let original = settings.clone();
    assert_eq!(
        identity(facade.select_default_model().expect("default")),
        identity(&preferred)
    );
    assert_eq!(
        identity(facade.resolve_settings(&settings).expect("host model")),
        identity(&explicit)
    );
    assert_eq!(settings, original);

    for reference in ["openrouter:vendor/host:model", "host", "host:model"] {
        // The qualified substring contains the provider separator and stays exact.
        if reference == "host:model" {
            assert!(matches!(
                facade.resolve_selection(reference),
                Err(CodingRuntimeError::MissingProvider { .. })
            ));
            continue;
        }
        let resolved = facade
            .resolve_selection(reference)
            .expect("explicit selection");
        assert_eq!(identity(resolved.model), identity(&explicit));
        assert_eq!(resolved.thinking_level, ModelThinkingLevel::Off);
    }
    for reference in [
        "openrouter:vendor/host:model@HIGH",
        "host@high",
        "vendor@high",
    ] {
        let resolved = facade
            .resolve_selection(reference)
            .expect("explicit thinking suffix");
        assert_eq!(identity(resolved.model), identity(&explicit));
        assert_eq!(resolved.thinking_level, ModelThinkingLevel::High);
    }
    for reference in [
        "host@max",
        "host@unsupported",
        "anthropic:claude-opus-4-8@medium",
    ] {
        assert!(matches!(
            facade.resolve_selection(reference),
            Err(CodingRuntimeError::InvalidModelReference { .. })
        ));
    }
    assert_eq!(
        identity(facade.select_default_model().expect("unchanged default")),
        identity(&preferred)
    );
}

struct CallbackGuard {
    id: ProviderId,
    id_calls: AtomicUsize,
}

impl Provider for CallbackGuard {
    fn id(&self) -> &ProviderId {
        self.id_calls.fetch_add(1, Ordering::SeqCst);
        &self.id
    }

    fn name(&self) -> &str {
        panic!("default selection must not query provider names");
    }

    fn auth(&self) -> &ProviderAuth {
        panic!("default selection must not query provider auth");
    }

    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        panic!("default selection must not query provider models");
    }

    fn adapter(&self, _api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        panic!("default selection must not construct provider adapters");
    }
}

impl ApiKeyAuth for CallbackGuard {
    fn name(&self) -> &str {
        panic!("default selection must not query credential names");
    }

    fn resolve<'a>(
        &'a self,
        _input: ApiKeyAuthInput<'a>,
    ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
        panic!("default selection must not resolve credentials");
    }
}

impl AuthContext for CallbackGuard {
    fn env<'a>(
        &'a self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        panic!("default selection must not read ambient auth");
    }

    fn file_exists<'a>(
        &'a self,
        _path: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        panic!("default selection must not inspect auth files");
    }
}

impl CredentialStore for CallbackGuard {
    fn read<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        panic!("default selection must not read credentials");
    }

    fn list<'a>(
        &'a self,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Vec<CredentialInfo>, CredentialStoreError>> {
        panic!("default selection must not list credentials");
    }

    fn begin_mutation<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CredentialMutation + 'static>, CredentialStoreError>> {
        panic!("default selection must not mutate credentials");
    }

    fn reconcile<'a>(
        &'a self,
        _provider: &'a ProviderId,
        _resolution: CredentialReconciliation,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<VersionedCredential>, CredentialStoreError>> {
        panic!("default selection must not reconcile credentials");
    }
}

#[test]
fn default_selection_never_invokes_provider_or_credential_callbacks() {
    let guard = Arc::new(CallbackGuard {
        id: ProviderId::new("anthropic").expect("provider"),
        id_calls: AtomicUsize::new(0),
    });
    let preferred = model("anthropic", "claude-opus-4-8");
    let fallback = model("openai", "fallback");
    let facade = ProviderComposition::new(ProviderCompositionInputs {
        providers: vec![guard.clone()],
        models: vec![preferred.clone(), fallback.clone()],
        credentials: CredentialInputs::new(Some(guard.clone()), Some(guard.clone())),
    })
    .expect("host capability handles");
    assert_eq!(guard.id_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        identity(facade.select_default_model().expect("preferred")),
        identity(&preferred)
    );
    let facade = facade.with_availability([availability(&preferred, false)]);
    assert_eq!(
        identity(facade.select_default_model().expect("fallback")),
        identity(&fallback)
    );
    let facade = facade.with_availability([
        availability(&preferred, false),
        availability(&fallback, false),
    ]);
    assert!(facade.select_default_model().is_none());
    assert_eq!(guard.id_calls.load(Ordering::SeqCst), 1);
}
