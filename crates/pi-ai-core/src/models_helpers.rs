//! Provider composition, model pricing, and thinking-level helpers.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::{
    ApiAdapter, ApiId, BoxFuture, CancellationToken, Clock, Credential, FiniteF64, Model,
    ModelThinkingLevel, ModelsError, ModelsErrorCode, ModelsMemoryIntent, ModelsPersistenceIntent,
    ModelsPublication, ModelsPublisher, ModelsStoreEntry, NumericError, Provider, ProviderAuth,
    ProviderId, ProviderModelsRefresher, PublicationOutcome, RefreshModelsContext, SafeUnsigned,
    Usage, UsageCost,
};

const THINKING_LEVELS: [ModelThinkingLevel; 7] = [
    ModelThinkingLevel::Off,
    ModelThinkingLevel::Minimal,
    ModelThinkingLevel::Low,
    ModelThinkingLevel::Medium,
    ModelThinkingLevel::High,
    ModelThinkingLevel::ExtraHigh,
    ModelThinkingLevel::Max,
];

/// Borrowed context supplied to one provider catalog fetch.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<pi_ai_core::ProviderModelsFetchContext<'static>>();
/// ```
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ProviderModelsFetchContext<'static>>();
/// ```
pub struct ProviderModelsFetchContext<'a> {
    /// Effective credential for the network phase.
    pub credential: Option<&'a Credential>,
    /// Complete detached provider cache snapshot.
    pub stored: Option<&'a ModelsStoreEntry>,
    /// Optional caller freshness override.
    pub force: Option<bool>,
    /// Phase cancellation token.
    pub cancellation: &'a CancellationToken,
}

impl fmt::Debug for ProviderModelsFetchContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderModelsFetchContext")
            .field("credential_present", &self.credential.is_some())
            .field("stored_present", &self.stored.is_some())
            .field("force", &self.force)
            .field("cancellation", &self.cancellation)
            .finish()
    }
}

/// Object-safe capability for fetching a provider's dynamic model catalog.
pub trait ProviderModelsFetcher: Send + Sync {
    /// Fetches a detached catalog using one borrow-scoped context.
    fn fetch_models<'a>(
        &'a self,
        context: ProviderModelsFetchContext<'a>,
    ) -> BoxFuture<'a, Result<Vec<Model>, ModelsError>>;
}

/// Object-safe provider-specific model availability filter.
pub trait ProviderModelFilter: Send + Sync {
    /// Returns a detached filtered model list.
    fn filter_models(&self, models: &[Model], credential: Option<&Credential>) -> Vec<Model>;
}

/// Process-local dynamic catalog capabilities consumed by provider creation.
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<pi_ai_core::DynamicProviderModels>();
/// ```
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::DynamicProviderModels>();
/// ```
pub struct DynamicProviderModels {
    /// Network catalog fetch capability.
    pub fetcher: Arc<dyn ProviderModelsFetcher>,
    /// Timestamp authority used only after a successful fetch.
    pub clock: Arc<dyn Clock>,
}

impl fmt::Debug for DynamicProviderModels {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DynamicProviderModels")
            .field("fetcher_present", &true)
            .field("clock_present", &true)
            .finish()
    }
}

/// Process-local values and capabilities consumed by [`create_provider`].
///
/// ```compile_fail
/// fn requires_clone<T: Clone>() {}
/// requires_clone::<pi_ai_core::CreateProviderOptions>();
/// ```
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::CreateProviderOptions>();
/// ```
pub struct CreateProviderOptions {
    /// Provider identity.
    pub id: ProviderId,
    /// Optional display name; absence defaults to the identity.
    pub name: Option<String>,
    /// Validated authentication capabilities.
    pub auth: ProviderAuth,
    /// Static baseline models, preserved without normalization.
    pub models: Vec<Model>,
    /// Optional dynamic catalog capability.
    pub dynamic_models: Option<DynamicProviderModels>,
    /// Optional availability filter.
    pub model_filter: Option<Arc<dyn ProviderModelFilter>>,
    /// Self-identifying API adapters.
    pub adapters: Vec<Arc<dyn ApiAdapter>>,
}

impl fmt::Debug for CreateProviderOptions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateProviderOptions")
            .field("id", &self.id)
            .field("name_present", &self.name.is_some())
            .field("auth", &self.auth)
            .field("model_count", &self.models.len())
            .field("dynamic_models_present", &self.dynamic_models.is_some())
            .field("model_filter_present", &self.model_filter.is_some())
            .field(
                "adapter_ids",
                &self
                    .adapters
                    .iter()
                    .map(|adapter| adapter.api_id().as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Invalid provider composition.
///
/// The duplicate identity remains available through pattern matching or
/// [`CreateProviderError::duplicate_api`], but default formatting is fixed.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::CreateProviderError>();
/// ```
#[derive(Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum CreateProviderError {
    /// Two adapters declared the same API identity.
    DuplicateAdapter(ApiId),
}

impl CreateProviderError {
    /// Returns the duplicate API identity when present.
    pub fn duplicate_api(&self) -> Option<&ApiId> {
        match self {
            Self::DuplicateAdapter(api) => Some(api),
        }
    }
}

impl fmt::Debug for CreateProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("CreateProviderError::DuplicateAdapter")
    }
}

impl fmt::Display for CreateProviderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("provider has a duplicate API adapter")
    }
}

impl std::error::Error for CreateProviderError {}

struct ComposedProvider {
    id: ProviderId,
    name: String,
    auth: ProviderAuth,
    baseline: Arc<[Model]>,
    dynamic_models: Option<DynamicProviderModels>,
    model_filter: Option<Arc<dyn ProviderModelFilter>>,
    adapters: BTreeMap<ApiId, Arc<dyn ApiAdapter>>,
}

trait PublicationPort: Send {
    fn publish<'a>(
        &'a mut self,
        publication: ModelsPublication,
    ) -> BoxFuture<'a, Result<PublicationOutcome, ModelsError>>;
}

impl PublicationPort for ModelsPublisher {
    fn publish<'a>(
        &'a mut self,
        publication: ModelsPublication,
    ) -> BoxFuture<'a, Result<PublicationOutcome, ModelsError>> {
        ModelsPublisher::publish(self, publication)
    }
}

impl fmt::Debug for ComposedProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Provider")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("auth", &self.auth)
            .field("baseline_count", &self.baseline.len())
            .field("dynamic_models_present", &self.dynamic_models.is_some())
            .field("model_filter_present", &self.model_filter.is_some())
            .field("adapter_ids", &self.adapters.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl Provider for ComposedProvider {
    fn id(&self) -> &ProviderId {
        &self.id
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn auth(&self) -> &ProviderAuth {
        &self.auth
    }

    fn models(&self) -> Result<Arc<[Model]>, ModelsError> {
        Ok(Arc::clone(&self.baseline))
    }

    fn models_refresher(&self) -> Option<&(dyn ProviderModelsRefresher + '_)> {
        self.dynamic_models
            .as_ref()
            .map(|_| self as &dyn ProviderModelsRefresher)
    }

    fn filter_models(&self, models: &[Model], credential: Option<&Credential>) -> Vec<Model> {
        self.model_filter.as_ref().map_or_else(
            || models.to_vec(),
            |filter| filter.filter_models(models, credential),
        )
    }

    fn adapter(&self, api: &ApiId) -> Option<Arc<dyn ApiAdapter>> {
        self.adapters.get(api).cloned()
    }
}

impl ProviderModelsRefresher for ComposedProvider {
    fn refresh_models<'a>(
        &'a self,
        context: RefreshModelsContext<'a>,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        let RefreshModelsContext {
            publisher,
            credential,
            stored,
            allow_network,
            force,
            cancellation,
        } = context;
        refresh_composed_provider(
            self,
            publisher,
            credential,
            stored,
            allow_network,
            force,
            cancellation,
        )
    }
}

#[allow(clippy::too_many_arguments)] // Mirrors the frozen refresh capability without hidden state.
fn refresh_composed_provider<'a>(
    provider: &'a ComposedProvider,
    publisher: &'a mut dyn PublicationPort,
    credential: Option<&'a Credential>,
    stored: Option<&'a ModelsStoreEntry>,
    allow_network: bool,
    force: Option<bool>,
    cancellation: &'a CancellationToken,
) -> BoxFuture<'a, Result<(), ModelsError>> {
    Box::pin(async move {
        if let Some(stored) = stored {
            let restored: Arc<[Model]> = stored
                .models
                .iter()
                .filter(|model| model.provider == provider.id)
                .cloned()
                .collect::<Vec<_>>()
                .into();
            if publisher
                .publish(ModelsPublication {
                    memory: ModelsMemoryIntent::Set(restored),
                    persistence: ModelsPersistenceIntent::Keep,
                })
                .await?
                == PublicationOutcome::Superseded
            {
                return Ok(());
            }
        }

        if !allow_network || cancellation.is_cancelled() {
            return Ok(());
        }
        let Some(dynamic) = provider.dynamic_models.as_ref() else {
            return Ok(());
        };
        let refreshed = dynamic
            .fetcher
            .fetch_models(ProviderModelsFetchContext {
                credential,
                stored,
                force,
                cancellation,
            })
            .await?;
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let checked_at = SafeUnsigned::new(dynamic.clock.now_unix_ms())
            .map_err(|_| ModelsError::new(ModelsErrorCode::ModelSource))?;
        let memory: Arc<[Model]> = refreshed.clone().into();
        let _ = publisher
            .publish(ModelsPublication {
                memory: ModelsMemoryIntent::Set(memory),
                persistence: ModelsPersistenceIntent::Set(ModelsStoreEntry {
                    models: refreshed,
                    last_modified: None,
                    checked_at: Some(checked_at),
                    etag: None,
                }),
            })
            .await?;
        Ok(())
    })
}

/// Consumes provider parts and returns a provider with self-keyed adapters.
pub fn create_provider(
    options: CreateProviderOptions,
) -> Result<Arc<dyn Provider>, CreateProviderError> {
    let mut adapters = BTreeMap::new();
    for adapter in options.adapters {
        let api = adapter.api_id().clone();
        if adapters.insert(api.clone(), adapter).is_some() {
            return Err(CreateProviderError::DuplicateAdapter(api));
        }
    }
    let name = options
        .name
        .unwrap_or_else(|| options.id.as_str().to_owned());
    Ok(Arc::new(ComposedProvider {
        id: options.id,
        name,
        auth: options.auth,
        baseline: options.models.into(),
        dynamic_models: options.dynamic_models,
        model_filter: options.model_filter,
        adapters,
    }))
}

/// Returns whether a model has the exact, case-sensitive API identity.
pub fn has_api(model: &Model, api: &ApiId) -> bool {
    &model.api == api
}

/// Calculates and atomically replaces all five monetary usage fields.
pub fn calculate_cost<'a>(
    model: &Model,
    usage: &'a mut Usage,
) -> Result<&'a UsageCost, NumericError> {
    let input_tokens =
        (usage.input.get() as f64 + usage.cache_read.get() as f64) + usage.cache_write.get() as f64;
    let mut rates = (
        model.cost.input.get(),
        model.cost.output.get(),
        model.cost.cache_read.get(),
        model.cost.cache_write.get(),
    );
    let mut matched_threshold = -1.0_f64;
    if let Some(tiers) = &model.cost.tiers {
        for tier in tiers {
            let threshold = tier.input_tokens_above.get() as f64;
            if input_tokens > threshold && threshold > matched_threshold {
                rates = (
                    tier.input.get(),
                    tier.output.get(),
                    tier.cache_read.get(),
                    tier.cache_write.get(),
                );
                matched_threshold = threshold;
            }
        }
    }

    let long_write = usage.cache_write_1h.map_or(0.0, |value| value.get() as f64);
    let short_write = usage.cache_write.get() as f64 - long_write;
    let input = FiniteF64::new((rates.0 / 1_000_000.0) * usage.input.get() as f64)?;
    let output = FiniteF64::new((rates.1 / 1_000_000.0) * usage.output.get() as f64)?;
    let cache_read = FiniteF64::new((rates.2 / 1_000_000.0) * usage.cache_read.get() as f64)?;
    let cache_write =
        FiniteF64::new((rates.3 * short_write + rates.0 * 2.0 * long_write) / 1_000_000.0)?;
    let total =
        FiniteF64::new(((input.get() + output.get()) + cache_read.get()) + cache_write.get())?;

    usage.cost = UsageCost {
        input,
        output,
        cache_read,
        cache_write,
        total,
    };
    Ok(&usage.cost)
}

/// Returns supported thinking levels in the fixed public order.
pub fn get_supported_thinking_levels(model: &Model) -> Vec<ModelThinkingLevel> {
    if !model.reasoning {
        return vec![ModelThinkingLevel::Off];
    }
    THINKING_LEVELS
        .into_iter()
        .filter(|level| {
            let mapped = model
                .thinking_level_map
                .as_ref()
                .and_then(|mapping| mapping.as_map().get(level));
            if mapped == Some(&None) {
                return false;
            }
            if matches!(
                level,
                ModelThinkingLevel::ExtraHigh | ModelThinkingLevel::Max
            ) {
                return mapped.is_some_and(Option::is_some);
            }
            true
        })
        .collect()
}

/// Clamps an unsupported request upward first, then downward.
pub fn clamp_thinking_level(model: &Model, level: ModelThinkingLevel) -> ModelThinkingLevel {
    let available = get_supported_thinking_levels(model);
    if available.contains(&level) {
        return level;
    }
    let requested = THINKING_LEVELS
        .iter()
        .position(|candidate| *candidate == level)
        .expect("closed thinking level belongs to the fixed order");
    for candidate in THINKING_LEVELS.iter().skip(requested) {
        if available.contains(candidate) {
            return *candidate;
        }
    }
    for candidate in THINKING_LEVELS[..requested].iter().rev() {
        if available.contains(candidate) {
            return *candidate;
        }
    }
    available
        .first()
        .copied()
        .unwrap_or(ModelThinkingLevel::Off)
}

/// Compares two present models by provider and model identity only.
pub fn models_are_equal(a: Option<&Model>, b: Option<&Model>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.id == b.id && a.provider == b.provider,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::future::Future;
    use std::pin::pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use crate::{
        ApiKeyAuth, ApiKeyAuthInput, ApiKeyCredential, ApiKeyCredentialType, AuthProviderError,
        AuthResult, BoxFuture, Cancelled, ModelAuth, ModelsError, ModelsErrorCode,
        ModelsMemoryIntent, ModelsPersistenceIntent, ProviderAuth, ProviderEnv, SecretString,
    };

    use super::*;

    fn ready<F: Future>(future: F) -> F::Output {
        let mut future = pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(value) => value,
            Poll::Pending => panic!("fixture future unexpectedly pending"),
        }
    }

    fn model(id: &str, provider: &str) -> Model {
        serde_json::from_value(serde_json::json!({
            "id": id,
            "name": id,
            "api": "test-api",
            "provider": provider,
            "baseUrl": "https://models.invalid",
            "reasoning": false,
            "input": ["text"],
            "cost": {"input": 1.0, "output": 1.0, "cacheRead": 1.0, "cacheWrite": 1.0},
            "contextWindow": 1000,
            "maxTokens": 100
        }))
        .expect("valid model")
    }

    struct DummyApiKey;

    impl ApiKeyAuth for DummyApiKey {
        fn name(&self) -> &str {
            "dummy"
        }

        fn resolve<'a>(
            &'a self,
            _input: ApiKeyAuthInput<'a>,
        ) -> BoxFuture<'a, Result<Option<AuthResult>, AuthProviderError>> {
            Box::pin(async {
                Ok(Some(AuthResult {
                    auth: ModelAuth {
                        api_key: None,
                        headers: None,
                        base_url: None,
                    },
                    env: None,
                    source: None,
                }))
            })
        }
    }

    struct FixedClock(u64);

    impl Clock for FixedClock {
        fn now_unix_ms(&self) -> u64 {
            self.0
        }

        fn sleep<'a>(
            &'a self,
            _duration: Duration,
            _cancellation: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            Box::pin(std::future::pending())
        }
    }

    struct PanicClock;

    impl Clock for PanicClock {
        fn now_unix_ms(&self) -> u64 {
            panic!("clock must not run after post-fetch cancellation")
        }

        fn sleep<'a>(
            &'a self,
            _duration: Duration,
            _cancellation: &'a CancellationToken,
        ) -> BoxFuture<'a, Result<(), Cancelled>> {
            Box::pin(std::future::pending())
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct FetchObservation {
        credential: Option<Credential>,
        stored: Option<ModelsStoreEntry>,
        force: Option<bool>,
        cancelled: bool,
    }

    struct FakeFetcher {
        models: Vec<Model>,
        observations: Arc<Mutex<Vec<FetchObservation>>>,
        calls: Arc<AtomicUsize>,
        cancel: bool,
        fail: bool,
    }

    type FetcherFixture = (
        Arc<dyn ProviderModelsFetcher>,
        Arc<AtomicUsize>,
        Arc<Mutex<Vec<FetchObservation>>>,
    );

    impl ProviderModelsFetcher for FakeFetcher {
        fn fetch_models<'a>(
            &'a self,
            context: ProviderModelsFetchContext<'a>,
        ) -> BoxFuture<'a, Result<Vec<Model>, ModelsError>> {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.observations
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(FetchObservation {
                    credential: context.credential.cloned(),
                    stored: context.stored.cloned(),
                    force: context.force,
                    cancelled: context.cancellation.is_cancelled(),
                });
            if self.cancel {
                context.cancellation.cancel();
            }
            let result = if self.fail {
                Err(ModelsError::new(ModelsErrorCode::Stream))
            } else {
                Ok(self.models.clone())
            };
            Box::pin(async move { result })
        }
    }

    struct FakePublisher {
        publications: Vec<ModelsPublication>,
        outcomes: VecDeque<PublicationOutcome>,
        fail_at: Option<usize>,
    }

    impl FakePublisher {
        fn published() -> Self {
            Self {
                publications: Vec::new(),
                outcomes: VecDeque::new(),
                fail_at: None,
            }
        }
    }

    impl PublicationPort for FakePublisher {
        fn publish<'a>(
            &'a mut self,
            publication: ModelsPublication,
        ) -> BoxFuture<'a, Result<PublicationOutcome, ModelsError>> {
            self.publications.push(publication);
            let call = self.publications.len();
            let result = if self.fail_at == Some(call) {
                Err(ModelsError::new(ModelsErrorCode::ModelSource))
            } else {
                Ok(self
                    .outcomes
                    .pop_front()
                    .unwrap_or(PublicationOutcome::Published))
            };
            Box::pin(async move { result })
        }
    }

    fn provider(
        fetcher: Arc<dyn ProviderModelsFetcher>,
        clock: Arc<dyn Clock>,
    ) -> ComposedProvider {
        ComposedProvider {
            id: ProviderId::new("owned").expect("valid id"),
            name: "owned".to_owned(),
            auth: ProviderAuth::new(Some(Arc::new(DummyApiKey)), None).expect("auth"),
            baseline: Arc::from([]),
            dynamic_models: Some(DynamicProviderModels { fetcher, clock }),
            model_filter: None,
            adapters: BTreeMap::new(),
        }
    }

    fn fetcher(models: Vec<Model>, cancel: bool, fail: bool) -> FetcherFixture {
        let calls = Arc::new(AtomicUsize::new(0));
        let observations = Arc::new(Mutex::new(Vec::new()));
        (
            Arc::new(FakeFetcher {
                models,
                observations: Arc::clone(&observations),
                calls: Arc::clone(&calls),
                cancel,
                fail,
            }),
            calls,
            observations,
        )
    }

    fn stored(models: Vec<Model>) -> ModelsStoreEntry {
        ModelsStoreEntry {
            models,
            last_modified: Some(SafeUnsigned::new(7).expect("safe")),
            checked_at: Some(SafeUnsigned::new(8).expect("safe")),
            etag: Some("etag".to_owned()),
        }
    }

    fn credential() -> Credential {
        Credential::ApiKey(ApiKeyCredential {
            credential_type: ApiKeyCredentialType::ApiKey,
            key: Some(SecretString::new("secret")),
            env: Some(ProviderEnv::new(BTreeMap::new())),
        })
    }

    #[test]
    fn provider_builder_restores_only_owned_cached_models() {
        let (fetcher, calls, _) = fetcher(Vec::new(), false, false);
        let provider = provider(fetcher, Arc::new(FixedClock(1)));
        let cancellation = CancellationToken::new();
        let cache = stored(vec![model("own", "owned"), model("foreign", "other")]);
        let mut publisher = FakePublisher::published();
        ready(refresh_composed_provider(
            &provider,
            &mut publisher,
            None,
            Some(&cache),
            false,
            None,
            &cancellation,
        ))
        .expect("restore succeeds");
        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert_eq!(publisher.publications.len(), 1);
        let ModelsMemoryIntent::Set(restored) = &publisher.publications[0].memory else {
            panic!("restore sets memory");
        };
        assert_eq!(restored.as_ref(), &[model("own", "owned")]);
        assert_eq!(
            publisher.publications[0].persistence,
            ModelsPersistenceIntent::Keep
        );

        let mut empty_publisher = FakePublisher::published();
        ready(refresh_composed_provider(
            &provider,
            &mut empty_publisher,
            None,
            Some(&stored(Vec::new())),
            false,
            None,
            &cancellation,
        ))
        .expect("empty restore succeeds");
        let ModelsMemoryIntent::Set(empty) = &empty_publisher.publications[0].memory else {
            panic!("empty cache clears memory");
        };
        assert!(empty.is_empty());

        let mut absent_publisher = FakePublisher::published();
        ready(refresh_composed_provider(
            &provider,
            &mut absent_publisher,
            None,
            None,
            false,
            None,
            &cancellation,
        ))
        .expect("absent restore succeeds");
        assert!(absent_publisher.publications.is_empty());
    }

    #[test]
    fn composed_provider_debug_omits_model_and_capability_data() {
        let (fetcher, _, _) = fetcher(Vec::new(), false, false);
        let mut provider = provider(fetcher, Arc::new(FixedClock(1)));
        let mut secret_model = model("model", "owned");
        secret_model.name = "PRIVATE_MODEL_SENTINEL".to_owned();
        provider.baseline = vec![secret_model].into();
        let debug = format!("{provider:?}");
        assert!(!debug.contains("PRIVATE_MODEL_SENTINEL"));
        assert!(debug.contains("baseline_count: 1"));
        assert!(debug.contains("dynamic_models_present: true"));
    }

    #[test]
    fn provider_builder_stops_when_restore_is_superseded() {
        let (fetcher, calls, _) = fetcher(vec![model("fetched", "other")], false, false);
        let provider = provider(fetcher, Arc::new(FixedClock(1)));
        let mut publisher = FakePublisher::published();
        publisher.outcomes.push_back(PublicationOutcome::Superseded);
        ready(refresh_composed_provider(
            &provider,
            &mut publisher,
            Some(&credential()),
            Some(&stored(vec![model("cached", "owned")])),
            true,
            Some(true),
            &CancellationToken::new(),
        ))
        .expect("superseded restore is a normal stop");
        assert_eq!(publisher.publications.len(), 1);
        assert_eq!(calls.load(Ordering::Acquire), 0);
    }

    #[test]
    fn provider_builder_fetches_and_publishes_dynamic_catalog() {
        let refreshed = vec![model("replacement", "other"), model("append", "owned")];
        let (fetcher, calls, observations) = fetcher(refreshed.clone(), false, false);
        let provider = provider(fetcher, Arc::new(FixedClock(123)));
        let cache = stored(vec![model("cached", "owned"), model("foreign", "other")]);
        let credential = credential();
        let cancellation = CancellationToken::new();
        let mut publisher = FakePublisher::published();
        ready(refresh_composed_provider(
            &provider,
            &mut publisher,
            Some(&credential),
            Some(&cache),
            true,
            Some(true),
            &cancellation,
        ))
        .expect("network refresh succeeds");

        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(
            observations
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .as_slice(),
            &[FetchObservation {
                credential: Some(credential.clone()),
                stored: Some(cache),
                force: Some(true),
                cancelled: false,
            }]
        );
        assert_eq!(publisher.publications.len(), 2);
        let final_publication = &publisher.publications[1];
        let ModelsMemoryIntent::Set(memory) = &final_publication.memory else {
            panic!("network result sets memory");
        };
        assert_eq!(memory.as_ref(), refreshed.as_slice());
        let ModelsPersistenceIntent::Set(persisted) = &final_publication.persistence else {
            panic!("network result persists");
        };
        assert_eq!(persisted.models, refreshed);
        assert_eq!(
            persisted.checked_at,
            Some(SafeUnsigned::new(123).expect("safe"))
        );
        assert_eq!(persisted.last_modified, None);
        assert_eq!(persisted.etag, None);
    }

    #[test]
    fn provider_builder_network_stop_matrix() {
        let cancellation = CancellationToken::new();
        let (offline_fetcher, offline_calls, _) = fetcher(Vec::new(), false, false);
        let offline = provider(offline_fetcher, Arc::new(FixedClock(1)));
        let mut publisher = FakePublisher::published();
        ready(refresh_composed_provider(
            &offline,
            &mut publisher,
            None,
            None,
            false,
            None,
            &cancellation,
        ))
        .expect("offline stops");
        assert_eq!(offline_calls.load(Ordering::Acquire), 0);

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let (cancelled_fetcher, cancelled_calls, _) = fetcher(Vec::new(), false, false);
        let cancelled_provider = provider(cancelled_fetcher, Arc::new(FixedClock(1)));
        ready(refresh_composed_provider(
            &cancelled_provider,
            &mut FakePublisher::published(),
            None,
            None,
            true,
            None,
            &cancelled,
        ))
        .expect("pre-fetch cancellation stops");
        assert_eq!(cancelled_calls.load(Ordering::Acquire), 0);

        let (failing_fetcher, _, _) = fetcher(Vec::new(), false, true);
        let failing = provider(failing_fetcher, Arc::new(FixedClock(1)));
        let error = ready(refresh_composed_provider(
            &failing,
            &mut FakePublisher::published(),
            None,
            None,
            true,
            None,
            &CancellationToken::new(),
        ))
        .expect_err("fetch failure propagates");
        assert_eq!(error.code(), ModelsErrorCode::Stream);

        let (cancelling_fetcher, _, _) = fetcher(vec![model("ignored", "owned")], true, false);
        let cancelling = provider(cancelling_fetcher, Arc::new(PanicClock));
        let mut cancelled_publications = FakePublisher::published();
        ready(refresh_composed_provider(
            &cancelling,
            &mut cancelled_publications,
            None,
            None,
            true,
            None,
            &CancellationToken::new(),
        ))
        .expect("post-fetch cancellation stops");
        assert!(cancelled_publications.publications.is_empty());

        let (overflow_fetcher, _, _) = fetcher(vec![model("ignored", "owned")], false, false);
        let overflow = provider(overflow_fetcher, Arc::new(FixedClock(u64::MAX)));
        let mut overflow_publications = FakePublisher::published();
        let restored = stored(vec![model("cached", "owned")]);
        let error = ready(refresh_composed_provider(
            &overflow,
            &mut overflow_publications,
            None,
            Some(&restored),
            true,
            None,
            &CancellationToken::new(),
        ))
        .expect_err("clock overflow is typed");
        assert_eq!(error.code(), ModelsErrorCode::ModelSource);
        assert_eq!(overflow_publications.publications.len(), 1);
        let ModelsMemoryIntent::Set(cached) = &overflow_publications.publications[0].memory else {
            panic!("successful cache restore remains published");
        };
        assert_eq!(cached.as_ref(), restored.models.as_slice());

        let (publish_fetcher, _, _) = fetcher(vec![model("ignored", "owned")], false, false);
        let publish = provider(publish_fetcher, Arc::new(FixedClock(1)));
        let mut failing_publisher = FakePublisher::published();
        failing_publisher.fail_at = Some(1);
        let error = ready(refresh_composed_provider(
            &publish,
            &mut failing_publisher,
            None,
            None,
            true,
            None,
            &CancellationToken::new(),
        ))
        .expect_err("publication failure propagates");
        assert_eq!(error.code(), ModelsErrorCode::ModelSource);
    }
}
