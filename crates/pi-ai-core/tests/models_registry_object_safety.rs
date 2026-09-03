use std::sync::Arc;

use pi_ai_core::{
    ModelsPublisher, ModelsRegistry, ModelsRegistryOptions, ModelsStore, Provider,
    ProviderModelsRefresher,
};

const INVENTORY: &str = include_str!("../../../conformance/ai-models/registry-inventory.json");
const REGISTRY_TESTS: &str = include_str!("models_registry.rs");
const CONCURRENCY_TESTS: &str = include_str!("models_registry_concurrency.rs");
const OBJECT_TESTS: &str = include_str!("models_registry_object_safety.rs");

fn assert_send<T: Send>() {}
fn assert_send_sync<T: Send + Sync>() {}
fn accept_provider(_: Arc<dyn Provider>) {}
fn accept_refresher(_: &(dyn ProviderModelsRefresher + '_)) {}
fn consume_options(_: ModelsRegistryOptions) {}
fn consume_store(_: Box<dyn ModelsStore>) {}

#[test]
fn registry_ports_are_object_safe_and_model_store_is_exclusive() {
    assert_send::<ModelsPublisher>();
    assert_send_sync::<ModelsRegistry>();
    let provider: Option<Arc<dyn Provider>> = None;
    let refresher: Option<&dyn ProviderModelsRefresher> = None;
    if let Some(provider) = provider {
        accept_provider(provider);
    }
    if let Some(refresher) = refresher {
        accept_refresher(refresher);
    }
    let _consume_options: fn(ModelsRegistryOptions) = consume_options;
    let _consume_store: fn(Box<dyn ModelsStore>) = consume_store;
}

#[test]
fn registry_inventory_has_exact_rows_and_live_bindings() {
    let document: serde_json::Value = serde_json::from_str(INVENTORY).expect("valid inventory");
    assert_eq!(document["suite"], "registry");
    assert_eq!(document["expectedRows"], 30);
    let rows = document["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 30);
    let mut ids = std::collections::BTreeSet::new();
    let mut tests = std::collections::BTreeSet::new();
    for row in rows {
        let id = row["id"].as_str().expect("id");
        let test = row["test"].as_str().expect("test");
        assert!(ids.insert(id), "duplicate id {id}");
        assert!(tests.insert(test), "duplicate test {test}");
        assert_eq!(row["owner"], "S03-models-registry");
        assert!(row["source"].as_str().is_some_and(|value| {
            value
                .rsplit_once(':')
                .is_some_and(|(_, line)| line.parse::<usize>().is_ok_and(|line| line > 0))
        }));
        assert!(
            row["polarity"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        assert!(
            row["expected"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
        let declaration = format!("fn {test}(");
        assert!(
            [REGISTRY_TESTS, CONCURRENCY_TESTS, OBJECT_TESTS]
                .iter()
                .any(|source| source.contains(&declaration)),
            "missing test binding {test}"
        );
    }
}
