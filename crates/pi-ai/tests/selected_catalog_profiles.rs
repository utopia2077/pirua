#![cfg(any(
    feature = "catalog-anthropic",
    feature = "catalog-deepseek",
    feature = "catalog-google",
    feature = "catalog-openai",
    feature = "catalog-openai-codex",
    feature = "catalog-openrouter",
    feature = "catalog-openrouter-images",
    feature = "images-runtime"
))]
use pi_ai::catalog::*;
#[test]
fn actual_facade_catalog_selection_order_unknowns_and_capture_timestamp() {
    let selected = [
        ("anthropic", cfg!(feature = "catalog-anthropic")),
        ("deepseek", cfg!(feature = "catalog-deepseek")),
        ("google", cfg!(feature = "catalog-google")),
        ("openai", cfg!(feature = "catalog-openai")),
        ("openai-codex", cfg!(feature = "catalog-openai-codex")),
        ("openrouter", cfg!(feature = "catalog-openrouter")),
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
    assert_eq!(
        get_builtin_images_providers(),
        if cfg!(feature = "catalog-openrouter-images") {
            vec!["openrouter"]
        } else {
            vec![]
        }
    );
    let images = get_builtin_images_models("openrouter").unwrap();
    assert_eq!(
        images.len(),
        if cfg!(feature = "catalog-openrouter-images") {
            52
        } else {
            0
        }
    );
    for model in images {
        assert_eq!(
            get_builtin_images_model("openrouter", model.id.as_str()).unwrap(),
            Some(model)
        );
    }
    assert!(
        get_builtin_images_models("synthetic-unknown")
            .unwrap()
            .is_empty()
    );
    assert!(
        get_builtin_images_model("synthetic-unknown", "x")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        get_builtin_model_data_generated_at().unwrap().get(),
        1788764386000
    );
}
#[cfg(feature = "images-runtime")]
#[test]
fn actual_generic_image_runtime_exports_need_no_selected_wire() {
    fn accepts(
        _: Option<pi_ai::images::ImagesModels>,
        _: Option<pi_ai::images::ImagesApiRegistry>,
    ) {
    }
    accepts(None, None);
}
