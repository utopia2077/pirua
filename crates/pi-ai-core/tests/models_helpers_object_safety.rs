use std::collections::BTreeSet;
use std::sync::Arc;

use pi_ai_core::{
    ApiId, BoxFuture, CancellationToken, CreateProviderError, CreateProviderOptions, Credential,
    DynamicProviderModels, Model, ModelThinkingLevel, ModelsError, ModelsStoreEntry, NumericError,
    Provider, ProviderModelFilter, ProviderModelsFetchContext, ProviderModelsFetcher, Usage,
    UsageCost, calculate_cost, clamp_thinking_level, create_provider,
    get_supported_thinking_levels, has_api, models_are_equal,
};

const INVENTORY: &str =
    include_str!("../../../conformance/ai-domain/models-helpers-inventory.json");
const PRODUCTION: &str = include_str!("../src/models_helpers.rs");
const BEHAVIOR_TESTS: &str = include_str!("models_helpers.rs");
const CONTRACT_TESTS: &str = include_str!("models_helpers_object_safety.rs");
const FACADE_TESTS: &str = include_str!("../../pi-ai/tests/models_helpers_facade.rs");

const EXPECTED_IDS: [&str; 20] = [
    "helper.provider-builder.identity-baseline",
    "helper.provider-builder.overlay-by-id",
    "helper.provider-builder.static-capability",
    "helper.provider-builder.cache-restore",
    "helper.provider-builder.restore-superseded",
    "helper.provider-builder.network-publication",
    "helper.provider-builder.network-stop-matrix",
    "helper.provider-builder.adapter-selection",
    "helper.provider-builder.filter-forwarding",
    "helper.has-api.exact",
    "helper.cost.threshold-exclusive",
    "helper.cost.highest-tier",
    "helper.cost.cache-write-split",
    "helper.cost.mutation",
    "helper.cost.finite-atomic",
    "helper.thinking.nonreasoning",
    "helper.thinking.mapping",
    "helper.thinking.clamp-pass",
    "helper.thinking.clamp-search",
    "helper.model-equality",
];

fn assert_value<T: std::fmt::Debug + Send + Sync + 'static>() {}
fn assert_error<T: Clone + Eq + std::error::Error + Send + Sync + 'static>() {}

type FetchFn = for<'a> fn(
    &'a dyn ProviderModelsFetcher,
    ProviderModelsFetchContext<'a>,
) -> BoxFuture<'a, Result<Vec<Model>, ModelsError>>;

fn call_fetch<'a>(
    fetcher: &'a dyn ProviderModelsFetcher,
    context: ProviderModelsFetchContext<'a>,
) -> BoxFuture<'a, Result<Vec<Model>, ModelsError>> {
    fetcher.fetch_models(context)
}

fn call_filter(
    filter: &dyn ProviderModelFilter,
    models: &[Model],
    credential: Option<&Credential>,
) -> Vec<Model> {
    filter.filter_models(models, credential)
}

#[test]
fn provider_helper_public_api_and_traits_are_exact_and_object_safe() {
    type CreateProvider =
        fn(CreateProviderOptions) -> Result<Arc<dyn Provider>, CreateProviderError>;
    type HasApi = fn(&Model, &ApiId) -> bool;
    type CalculateCost = for<'a> fn(&Model, &'a mut Usage) -> Result<&'a UsageCost, NumericError>;
    type Supported = fn(&Model) -> Vec<ModelThinkingLevel>;
    type Clamp = fn(&Model, ModelThinkingLevel) -> ModelThinkingLevel;
    type Equal = fn(Option<&Model>, Option<&Model>) -> bool;
    let _create: CreateProvider = create_provider;
    let _has_api: HasApi = has_api;
    let _cost: CalculateCost = calculate_cost;
    let _supported: Supported = get_supported_thinking_levels;
    let _clamp: Clamp = clamp_thinking_level;
    let _equal: Equal = models_are_equal;

    fn accepts_fetcher(_: &dyn ProviderModelsFetcher) {}
    fn accepts_filter(_: &dyn ProviderModelFilter) {}
    let _fetcher: fn(&dyn ProviderModelsFetcher) = accepts_fetcher;
    let _filter: fn(&dyn ProviderModelFilter) = accepts_filter;
    let _fetch: FetchFn = call_fetch;
    let _apply_filter: for<'a, 'b, 'c> fn(
        &'a dyn ProviderModelFilter,
        &'b [Model],
        Option<&'c Credential>,
    ) -> Vec<Model> = call_filter;

    assert_value::<CreateProviderOptions>();
    assert_value::<DynamicProviderModels>();
    assert_error::<CreateProviderError>();
    let duplicate =
        CreateProviderError::DuplicateAdapter(ApiId::new("API_SECRET_SENTINEL").expect("valid id"));
    assert_eq!(
        duplicate.to_string(),
        "provider has a duplicate API adapter"
    );
    assert_eq!(
        format!("{duplicate:?}"),
        "CreateProviderError::DuplicateAdapter"
    );
    assert!(!format!("{duplicate:?} {duplicate}").contains("API_SECRET_SENTINEL"));

    let credential: Credential = serde_json::from_value(serde_json::json!({
        "type": "api_key",
        "key": "FETCH_CONTEXT_SECRET_SENTINEL"
    }))
    .expect("credential");
    let stored = ModelsStoreEntry {
        models: Vec::new(),
        last_modified: None,
        checked_at: None,
        etag: Some("STORE_METADATA_SENTINEL".to_owned()),
    };
    let cancellation = CancellationToken::new();
    let context = ProviderModelsFetchContext {
        credential: Some(&credential),
        stored: Some(&stored),
        force: Some(true),
        cancellation: &cancellation,
    };
    let debug = format!("{context:?}");
    assert!(!debug.contains("FETCH_CONTEXT_SECRET_SENTINEL"));
    assert!(!debug.contains("STORE_METADATA_SENTINEL"));
}

#[test]
fn models_helpers_inventory_has_exact_rows_and_live_bindings() {
    let document: serde_json::Value = serde_json::from_str(INVENTORY).expect("valid inventory");
    assert_eq!(document["suite"], "models-helpers");
    assert_eq!(document["expectedRows"], 20);
    let rows = document["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), EXPECTED_IDS.len());
    let expected_keys = ["expected", "id", "owner", "polarity", "source", "test"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    for (row, expected_id) in rows.iter().zip(EXPECTED_IDS) {
        let row = row.as_object().expect("row object");
        assert_eq!(
            row.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            expected_keys
        );
        assert_eq!(row["id"], expected_id);
        assert_eq!(row["owner"], "S03-models-helpers");
        assert!(matches!(
            row["polarity"].as_str(),
            Some("positive" | "negative" | "boundary" | "race")
        ));
        let source = row["source"].as_str().expect("source");
        assert!(
            source.contains("@853a80d26c90a14c1886f0ebb8ffaae133ca2185")
                || source == "upstream-map.toml:991"
        );
        let binding = row["test"].as_str().expect("binding");
        let declaration = format!("fn {binding}(");
        assert!(
            [PRODUCTION, BEHAVIOR_TESTS, CONTRACT_TESTS, FACADE_TESTS]
                .iter()
                .any(|source| source.contains(&declaration)),
            "missing live binding {binding}"
        );
        assert!(!row["expected"].as_str().expect("expected").is_empty());
    }
}
