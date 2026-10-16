use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pi_ai_core::{
    ApiAdapter, ApiId, ApiKeyAuth, ApiKeyAuthInput, AuthContext, AuthContextError,
    AuthProviderError, AuthResult, BoxFuture, CancellationToken, Cancelled, CapabilityError, Clock,
    CreateProviderError, CreateProviderOptions, Credential, DeferredCancelRequest,
    DeferredFetchRequest, DynamicProviderModels, FiniteF64, InMemoryCredentialStore,
    InMemoryModelsStore, Model, ModelAuth, ModelCostTier, ModelHeaderPolicy, ModelId,
    ModelThinkingLevel, ModelsError, ModelsRefreshOptions, ModelsRegistry, ModelsRegistryOptions,
    ModelsStore, ModelsStoreEntry, ProviderAuth, ProviderId, ProviderModelFilter,
    ProviderModelsFetchContext, ProviderModelsFetcher, SafeUnsigned, SimpleStreamRequest,
    SpawnError, TaskSpawner, TextStreamRequest, ThinkingLevelMap, Usage, UsageCost, calculate_cost,
    clamp_thinking_level, create_provider, get_supported_thinking_levels, has_api,
    models_are_equal,
};

fn safe(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).expect("fixture is JavaScript-safe")
}

fn finite(value: f64) -> FiniteF64 {
    FiniteF64::new(value).expect("fixture is finite")
}

fn model(id: &str, provider: &str, api: &str) -> Model {
    serde_json::from_value(serde_json::json!({
        "id": id,
        "name": id,
        "api": api,
        "provider": provider,
        "baseUrl": "https://models.invalid",
        "reasoning": false,
        "input": ["text"],
        "cost": {"input": 1.0, "output": 2.0, "cacheRead": 0.5, "cacheWrite": 1.5},
        "contextWindow": 1000,
        "maxTokens": 100
    }))
    .expect("valid model")
}

fn usage(input: u64, output: u64, cache_read: u64, cache_write: u64) -> Usage {
    Usage {
        input: safe(input),
        output: safe(output),
        cache_read: safe(cache_read),
        cache_write: safe(cache_write),
        cache_write_1h: None,
        reasoning: None,
        total_tokens: safe(0),
        cost: UsageCost {
            input: finite(91.0),
            output: finite(92.0),
            cache_read: finite(93.0),
            cache_write: finite(94.0),
            total: finite(95.0),
        },
    }
}

fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match poll(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("fixture future unexpectedly pending"),
    }
}

#[derive(Default)]
struct ManualSpawner {
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
}

impl ManualSpawner {
    fn drive_once(&self) {
        let tasks = std::mem::take(
            &mut *self
                .tasks
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()),
        );
        let mut pending = Vec::new();
        for mut task in tasks {
            if poll(task.as_mut()).is_pending() {
                pending.push(task);
            }
        }
        self.tasks
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .extend(pending);
    }
}

impl TaskSpawner for ManualSpawner {
    fn spawn(&self, mut task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        if poll(task.as_mut()).is_pending() {
            self.tasks
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
                .push(task);
        }
        Ok(())
    }
}

fn drive<F: Future>(future: F, spawner: &ManualSpawner) -> F::Output {
    let mut future = pin!(future);
    for _ in 0..256 {
        if let Poll::Ready(value) = poll(future.as_mut()) {
            return value;
        }
        spawner.drive_once();
    }
    panic!("manual executor did not settle")
}

struct DummyAuthContext;

impl AuthContext for DummyAuthContext {
    fn env<'a>(
        &'a self,
        _name: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async { Ok(None) })
    }

    fn file_exists<'a>(
        &'a self,
        _path: &'a str,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
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

fn auth() -> ProviderAuth {
    ProviderAuth::new(Some(Arc::new(DummyApiKey)), None).expect("valid auth")
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

struct EmptyFetcher;

impl ProviderModelsFetcher for EmptyFetcher {
    fn fetch_models<'a>(
        &'a self,
        _context: ProviderModelsFetchContext<'a>,
    ) -> BoxFuture<'a, Result<Vec<Model>, ModelsError>> {
        Box::pin(async { Ok(Vec::new()) })
    }
}

fn provider_options(models: Vec<Model>) -> CreateProviderOptions {
    CreateProviderOptions {
        id: ProviderId::new("owned").expect("valid id"),
        name: None,
        auth: auth(),
        models,
        dynamic_models: None,
        model_filter: None,
        adapters: Vec::new(),
    }
}

struct DummyAdapter {
    api: ApiId,
}

impl ApiAdapter for DummyAdapter {
    fn api_id(&self) -> &ApiId {
        &self.api
    }

    fn stream(&self, _request: TextStreamRequest) -> pi_ai_core::AssistantStream {
        panic!("adapter is not invoked by composition tests")
    }

    fn stream_simple(&self, _request: SimpleStreamRequest) -> pi_ai_core::AssistantStream {
        panic!("adapter is not invoked by composition tests")
    }

    fn fetch_deferred(
        &self,
        _request: DeferredFetchRequest,
    ) -> Result<pi_ai_core::AssistantStream, CapabilityError> {
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
fn provider_builder_preserves_identity_defaults_and_baseline_values() {
    let mut foreign = model("foreign", "another-provider", "test-api");
    foreign.name = "MODEL_SECRET_SENTINEL".to_owned();
    let before = foreign.clone();
    let options = provider_options(vec![foreign]);
    assert!(!format!("{options:?}").contains("MODEL_SECRET_SENTINEL"));
    let provider = create_provider(options).expect("provider builds");
    assert_eq!(provider.id().as_str(), "owned");
    assert_eq!(provider.name(), "owned");
    assert!(provider.auth().api_key().is_some());
    assert_eq!(provider.models().expect("baseline").as_ref(), &[before]);

    let mut named = provider_options(Vec::new());
    named.name = Some("Display Name".to_owned());
    assert_eq!(
        create_provider(named).expect("provider").name(),
        "Display Name"
    );
}

#[test]
fn provider_builder_overlay_replaces_first_id_and_appends() {
    let mut first = model("duplicate", "owned", "test-api");
    first.name = "first".to_owned();
    let mut second = first.clone();
    second.name = "second".to_owned();
    let keep = model("keep", "owned", "test-api");
    let mut replacement = first.clone();
    replacement.name = "replacement".to_owned();
    let appended = model("append", "owned", "test-api");

    let store = InMemoryModelsStore::new();
    ready(store.write(
        &ProviderId::new("owned").expect("id"),
        ModelsStoreEntry {
            models: vec![replacement.clone(), appended.clone()],
            last_modified: None,
            checked_at: None,
            etag: None,
        },
        &CancellationToken::new(),
    ))
    .expect("preload store");
    let spawner = Arc::new(ManualSpawner::default());
    let registry = ModelsRegistry::new(ModelsRegistryOptions {
        identity: None,
        credentials: Arc::new(InMemoryCredentialStore::new()),
        models_store: Box::new(store),
        auth_context: Arc::new(DummyAuthContext),
        clock: Arc::new(FixedClock(1)),
        spawner: spawner.clone(),
        model_header_policy: ModelHeaderPolicy::default(),
    });
    let mut options = provider_options(vec![first, second.clone(), keep.clone()]);
    options.dynamic_models = Some(DynamicProviderModels {
        fetcher: Arc::new(EmptyFetcher),
        clock: Arc::new(FixedClock(1)),
    });
    drive(
        registry.set_provider(create_provider(options).expect("provider")),
        &spawner,
    )
    .expect("set provider");
    let result = drive(
        registry.refresh(
            ModelsRefreshOptions {
                allow_network: false,
                providers: None,
                force: None,
            },
            &CancellationToken::new(),
        ),
        &spawner,
    );
    assert!(!result.aborted);
    assert!(result.errors.is_empty());
    assert_eq!(
        registry.get_models(None),
        vec![replacement, second, keep, appended]
    );
}

#[test]
fn provider_builder_fetcher_controls_refresher_presence() {
    let static_provider = create_provider(provider_options(Vec::new())).expect("provider");
    assert!(static_provider.models_refresher().is_none());
    let mut dynamic = provider_options(Vec::new());
    dynamic.dynamic_models = Some(DynamicProviderModels {
        fetcher: Arc::new(EmptyFetcher),
        clock: Arc::new(FixedClock(1)),
    });
    assert!(
        create_provider(dynamic)
            .expect("provider")
            .models_refresher()
            .is_some()
    );
}

#[test]
fn provider_builder_selects_declared_api_adapters() {
    let first: Arc<dyn ApiAdapter> = Arc::new(DummyAdapter {
        api: ApiId::new("api-a").expect("id"),
    });
    let second: Arc<dyn ApiAdapter> = Arc::new(DummyAdapter {
        api: ApiId::new("api-b").expect("id"),
    });
    let mut options = provider_options(Vec::new());
    options.adapters = vec![Arc::clone(&first), Arc::clone(&second)];
    let provider = create_provider(options).expect("provider");
    assert!(Arc::ptr_eq(
        &provider
            .adapter(&ApiId::new("api-a").expect("id"))
            .expect("adapter"),
        &first
    ));
    assert!(Arc::ptr_eq(
        &provider
            .adapter(&ApiId::new("api-b").expect("id"))
            .expect("adapter"),
        &second
    ));
    assert!(
        provider
            .adapter(&ApiId::new("API-A").expect("id"))
            .is_none()
    );

    let duplicate_id = ApiId::new("duplicate-secret-id").expect("id");
    let mut duplicate = provider_options(Vec::new());
    duplicate.adapters = vec![
        Arc::new(DummyAdapter {
            api: duplicate_id.clone(),
        }),
        Arc::new(DummyAdapter {
            api: duplicate_id.clone(),
        }),
    ];
    let error = match create_provider(duplicate) {
        Ok(_) => panic!("duplicate must fail"),
        Err(error) => error,
    };
    assert_eq!(
        error,
        CreateProviderError::DuplicateAdapter(duplicate_id.clone())
    );
    assert_eq!(error.duplicate_api(), Some(&duplicate_id));
    assert!(!format!("{error:?} {error}").contains(duplicate_id.as_str()));
}

#[derive(Default)]
struct RecordingFilter {
    calls: AtomicUsize,
    observed_ids: Mutex<Vec<ModelId>>,
    credential: Mutex<Option<Credential>>,
}

impl ProviderModelFilter for RecordingFilter {
    fn filter_models(&self, models: &[Model], credential: Option<&Credential>) -> Vec<Model> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        *self
            .observed_ids
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) =
            models.iter().map(|model| model.id.clone()).collect();
        *self
            .credential
            .lock()
            .unwrap_or_else(|poison| poison.into_inner()) = credential.cloned();
        models.iter().rev().cloned().collect()
    }
}

#[test]
fn provider_builder_forwards_filter_without_mutating_inputs() {
    let filter = Arc::new(RecordingFilter::default());
    let mut options = provider_options(Vec::new());
    options.model_filter = Some(filter.clone());
    let provider = create_provider(options).expect("provider");
    let models = vec![
        model("first", "owned", "test-api"),
        model("second", "owned", "test-api"),
    ];
    let before = models.clone();
    let credential: Credential = serde_json::from_value(serde_json::json!({
        "type": "api_key",
        "key": "FILTER_SECRET"
    }))
    .expect("credential");
    let output = provider.filter_models(&models, Some(&credential));
    assert_eq!(models, before);
    assert_eq!(output, vec![before[1].clone(), before[0].clone()]);
    assert_eq!(filter.calls.load(Ordering::Acquire), 1);
    assert_eq!(
        filter
            .observed_ids
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_slice(),
        &[
            ModelId::new("first").expect("id"),
            ModelId::new("second").expect("id")
        ]
    );
    assert_eq!(
        filter
            .credential
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .as_ref(),
        Some(&credential)
    );
}

#[test]
fn has_api_is_exact_and_case_sensitive() {
    let model = model("id", "owned", "api-a");
    assert!(has_api(&model, &ApiId::new("api-a").expect("id")));
    assert!(!has_api(&model, &ApiId::new("API-A").expect("id")));
}

fn tier(threshold: u64, rate: f64) -> ModelCostTier {
    ModelCostTier {
        input: finite(rate),
        output: finite(rate),
        cache_read: finite(rate),
        cache_write: finite(rate),
        input_tokens_above: safe(threshold),
    }
}

#[test]
fn calculate_cost_uses_exclusive_request_wide_threshold() {
    let mut model = model("id", "owned", "api");
    model.cost.tiers = Some(vec![tier(100, 10.0)]);
    let mut boundary = usage(50, 2, 25, 25);
    calculate_cost(&model, &mut boundary).expect("finite");
    assert_eq!(boundary.cost.input.get(), (1.0_f64 / 1_000_000.0) * 50.0);
    let mut above = usage(51, 2, 25, 25);
    calculate_cost(&model, &mut above).expect("finite");
    assert_eq!(above.cost.input.get(), (10.0_f64 / 1_000_000.0) * 51.0);
}

#[test]
fn calculate_cost_selects_highest_matching_tier() {
    let mut model = model("id", "owned", "api");
    model.cost.tiers = Some(vec![
        tier(100, 2.0),
        tier(200, 4.0),
        tier(100, 99.0),
        tier(50, 3.0),
    ]);
    let mut highest = usage(201, 0, 0, 0);
    calculate_cost(&model, &mut highest).expect("finite");
    assert_eq!(highest.cost.input.get(), (4.0_f64 / 1_000_000.0) * 201.0);
    let mut duplicate = usage(150, 0, 0, 0);
    calculate_cost(&model, &mut duplicate).expect("finite");
    assert_eq!(duplicate.cost.input.get(), (2.0_f64 / 1_000_000.0) * 150.0);
}

#[test]
fn calculate_cost_prices_short_and_one_hour_cache_writes() {
    let mut model = model("id", "owned", "api");
    model.cost.input = finite(10.0);
    model.cost.cache_write = finite(4.0);
    let mut usage = usage(0, 0, 0, 10);
    usage.cache_write_1h = Some(safe(15));
    calculate_cost(&model, &mut usage).expect("finite");
    assert_eq!(usage.cost.cache_write.get(), 0.000_28);
}

#[test]
fn calculate_cost_overwrites_only_usage_cost() {
    let model = model("id", "owned", "api");
    let mut usage = usage(10, 20, 30, 40);
    usage.cache_write_1h = Some(safe(5));
    usage.reasoning = Some(safe(7));
    let before = usage.clone();
    let returned = calculate_cost(&model, &mut usage).expect("finite").clone();
    assert_eq!(returned, usage.cost);
    assert_eq!(usage.input, before.input);
    assert_eq!(usage.output, before.output);
    assert_eq!(usage.cache_read, before.cache_read);
    assert_eq!(usage.cache_write, before.cache_write);
    assert_eq!(usage.cache_write_1h, before.cache_write_1h);
    assert_eq!(usage.reasoning, before.reasoning);
    assert_eq!(usage.total_tokens, before.total_tokens);
    assert_ne!(usage.cost, before.cost);
}

#[test]
fn calculate_cost_rejects_nonfinite_results_atomically() {
    let mut model = model("id", "owned", "api");
    model.cost.output = finite(f64::MAX);
    let mut usage = usage(1, SafeUnsigned::MAX.get(), 0, 0);
    let before = usage.clone();
    assert!(calculate_cost(&model, &mut usage).is_err());
    assert_eq!(usage, before);
}

fn thinking_model(
    reasoning: bool,
    entries: impl IntoIterator<Item = (ModelThinkingLevel, Option<&'static str>)>,
) -> Model {
    let mut model = model("id", "owned", "api");
    model.reasoning = reasoning;
    model.thinking_level_map = Some(ThinkingLevelMap::new(
        entries
            .into_iter()
            .map(|(level, value)| (level, value.map(str::to_owned)))
            .collect(),
    ));
    model
}

#[test]
fn supported_thinking_levels_for_nonreasoning_are_off_only() {
    let mapped_model = thinking_model(
        false,
        [
            (ModelThinkingLevel::Off, None),
            (ModelThinkingLevel::Max, Some("max")),
        ],
    );
    assert_eq!(
        get_supported_thinking_levels(&mapped_model),
        vec![ModelThinkingLevel::Off]
    );
}

#[test]
fn supported_thinking_levels_apply_null_defaults_and_extended_opt_in() {
    let mapped_model = thinking_model(
        true,
        [
            (ModelThinkingLevel::Low, None),
            (ModelThinkingLevel::ExtraHigh, Some("")),
            (ModelThinkingLevel::Max, None),
        ],
    );
    assert_eq!(
        get_supported_thinking_levels(&mapped_model),
        vec![
            ModelThinkingLevel::Off,
            ModelThinkingLevel::Minimal,
            ModelThinkingLevel::Medium,
            ModelThinkingLevel::High,
            ModelThinkingLevel::ExtraHigh,
        ]
    );

    let mut no_map = model("id", "owned", "api");
    no_map.reasoning = true;
    assert_eq!(
        get_supported_thinking_levels(&no_map),
        vec![
            ModelThinkingLevel::Off,
            ModelThinkingLevel::Minimal,
            ModelThinkingLevel::Low,
            ModelThinkingLevel::Medium,
            ModelThinkingLevel::High,
        ]
    );
}

#[test]
fn clamp_thinking_level_preserves_supported_requests() {
    let model = thinking_model(
        true,
        [
            (ModelThinkingLevel::ExtraHigh, Some("xhigh")),
            (ModelThinkingLevel::Max, Some("max")),
        ],
    );
    for level in [
        ModelThinkingLevel::Off,
        ModelThinkingLevel::Minimal,
        ModelThinkingLevel::Low,
        ModelThinkingLevel::Medium,
        ModelThinkingLevel::High,
        ModelThinkingLevel::ExtraHigh,
        ModelThinkingLevel::Max,
    ] {
        assert_eq!(clamp_thinking_level(&model, level), level);
    }
}

#[test]
fn clamp_thinking_level_searches_up_then_down() {
    let upward = thinking_model(
        true,
        [
            (ModelThinkingLevel::Low, None),
            (ModelThinkingLevel::High, None),
            (ModelThinkingLevel::ExtraHigh, Some("xhigh")),
        ],
    );
    assert_eq!(
        clamp_thinking_level(&upward, ModelThinkingLevel::Low),
        ModelThinkingLevel::Medium
    );
    assert_eq!(
        clamp_thinking_level(&upward, ModelThinkingLevel::Max),
        ModelThinkingLevel::ExtraHigh
    );

    let empty = thinking_model(
        true,
        [
            (ModelThinkingLevel::Off, None),
            (ModelThinkingLevel::Minimal, None),
            (ModelThinkingLevel::Low, None),
            (ModelThinkingLevel::Medium, None),
            (ModelThinkingLevel::High, None),
        ],
    );
    assert_eq!(
        clamp_thinking_level(&empty, ModelThinkingLevel::Medium),
        ModelThinkingLevel::Off
    );
}

#[test]
fn models_are_equal_compares_only_present_provider_and_id() {
    let first = model("id", "owned", "api-a");
    let mut same = first.clone();
    same.name = "different".to_owned();
    same.api = ApiId::new("api-b").expect("id");
    same.reasoning = true;
    assert!(models_are_equal(Some(&first), Some(&same)));
    assert!(!models_are_equal(None, None));
    assert!(!models_are_equal(Some(&first), None));
    assert!(!models_are_equal(None, Some(&same)));
    assert!(!models_are_equal(
        Some(&first),
        Some(&model("other", "owned", "api-a"))
    ));
    assert!(!models_are_equal(
        Some(&first),
        Some(&model("id", "other", "api-a"))
    ));
}
