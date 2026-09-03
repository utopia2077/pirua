use std::sync::Arc;

use pi_ai::{
    ApiId, CreateProviderError, CreateProviderOptions, Model, ModelThinkingLevel, NumericError,
    Provider, ProviderModelFilter, ProviderModelsFetcher, Usage, UsageCost, calculate_cost,
    clamp_thinking_level, create_provider, get_supported_thinking_levels, has_api,
    models_are_equal,
};

#[test]
fn models_helpers_facade_reexports_exact_surface() {
    let _create: fn(CreateProviderOptions) -> Result<Arc<dyn Provider>, CreateProviderError> =
        create_provider;
    let _has_api: fn(&Model, &ApiId) -> bool = has_api;
    let _cost: for<'a> fn(&Model, &'a mut Usage) -> Result<&'a UsageCost, NumericError> =
        calculate_cost;
    let _supported: fn(&Model) -> Vec<ModelThinkingLevel> = get_supported_thinking_levels;
    let _clamp: fn(&Model, ModelThinkingLevel) -> ModelThinkingLevel = clamp_thinking_level;
    let _equal: fn(Option<&Model>, Option<&Model>) -> bool = models_are_equal;

    fn accepts_fetcher(_: &dyn ProviderModelsFetcher) {}
    fn accepts_filter(_: &dyn ProviderModelFilter) {}
    let _fetcher: fn(&dyn ProviderModelsFetcher) = accepts_fetcher;
    let _filter: fn(&dyn ProviderModelFilter) = accepts_filter;

    let model: Model = serde_json::from_value(serde_json::json!({
        "id": "model",
        "name": "Model",
        "api": "api-a",
        "provider": "provider",
        "baseUrl": "https://models.invalid",
        "reasoning": false,
        "input": ["text"],
        "cost": {"input": 1.0, "output": 1.0, "cacheRead": 1.0, "cacheWrite": 1.0},
        "contextWindow": 1000,
        "maxTokens": 100
    }))
    .expect("model fixture");
    assert!(has_api(&model, &ApiId::new("api-a").expect("id")));
    assert_eq!(
        get_supported_thinking_levels(&model),
        vec![ModelThinkingLevel::Off]
    );
}
