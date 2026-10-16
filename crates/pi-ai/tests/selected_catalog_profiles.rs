#![cfg(any(
    feature = "catalog-anthropic",
    feature = "catalog-deepseek",
    feature = "catalog-google",
    feature = "catalog-openai",
))]
use pi_ai::catalog::*;
#[test]
fn actual_facade_catalog_selection_order_unknowns_and_capture_timestamp() {
    let selected = [
        ("anthropic", cfg!(feature = "catalog-anthropic")),
        ("deepseek", cfg!(feature = "catalog-deepseek")),
        ("google", cfg!(feature = "catalog-google")),
        ("openai", cfg!(feature = "catalog-openai")),
    ];
    assert_eq!(
        get_builtin_providers()
            .iter()
            .map(|p| p.as_str())
            .collect::<Vec<_>>(),
        selected
            .iter()
            .filter(|(_, enabled)| *enabled)
            .map(|(id, _)| *id)
            .collect::<Vec<_>>()
    );
    for (id, enabled) in selected {
        let models = get_builtin_models(id).unwrap();
        assert_eq!(!models.is_empty(), enabled);
        for model in models {
            assert_eq!(model.provider.as_str(), id);
            assert_eq!(
                get_builtin_model(id, model.id.as_str()).unwrap(),
                Some(model)
            );
        }
    }
    assert!(get_builtin_models("synthetic-unknown").unwrap().is_empty());
    assert!(
        get_builtin_model("synthetic-unknown", "x")
            .unwrap()
            .is_none()
    );
    assert!(
        get_builtin_model("openai", "synthetic-unknown")
            .unwrap()
            .is_none()
    );
    for excluded in [
        "openai-codex",
        "openrouter",
        "azure",
        "google-vertex",
        "amazon-bedrock",
    ] {
        assert!(get_builtin_models(excluded).unwrap().is_empty());
    }
    assert!(get_builtin_model_data_generated_at().is_none());
}
