use pi_ai_core::{ImagesProviderId, ModelHeaderPolicy, ProviderId};
use pi_ai_providers::catalog::*;
use serde_json::{Value, json};

fn deepseek() -> Value {
    serde_json::from_slice(include_bytes!(
        "../../../conformance/catalog/generated/text/deepseek.json"
    ))
    .unwrap()
}

#[test]
fn catalog_intake_rejects_duplicates_mismatched_identity_and_unclassified_headers() {
    let provider = ProviderId::new("deepseek").unwrap();
    let policy = ModelHeaderPolicy::default();
    let input = deepseek();
    let mut duplicate = input.clone();
    duplicate.as_array_mut().unwrap().push(input[0].clone());
    assert_eq!(
        decode_text_catalog(&serde_json::to_vec(&duplicate).unwrap(), &provider, &policy)
            .unwrap_err()
            .kind(),
        CatalogErrorKind::Identity
    );
    let other = ProviderId::new("openai").unwrap();
    assert_eq!(
        decode_text_catalog(&serde_json::to_vec(&input).unwrap(), &other, &policy)
            .unwrap_err()
            .kind(),
        CatalogErrorKind::Identity
    );
    let mut header = input.clone();
    header[0]["headers"] = json!({"X-Public-Catalog": "visible"});
    assert_eq!(
        decode_text_catalog(&serde_json::to_vec(&header).unwrap(), &provider, &policy)
            .unwrap_err()
            .kind(),
        CatalogErrorKind::Validation
    );
    let allow = ModelHeaderPolicy::from_allowlist(["X-Public-Catalog".to_owned()]);
    let accepted =
        decode_text_catalog(&serde_json::to_vec(&header).unwrap(), &provider, &allow).unwrap();
    assert_eq!(
        serde_json::to_value(&accepted).unwrap()[0]["headers"]["X-Public-Catalog"],
        "visible"
    );
    header[0]["headers"] = json!({"authorization": "SYNTHETIC-CATALOG-SECRET"});
    let even_allowed = ModelHeaderPolicy::from_allowlist(["authorization".to_owned()]);
    let error = decode_text_catalog(
        &serde_json::to_vec(&header).unwrap(),
        &provider,
        &even_allowed,
    )
    .unwrap_err();
    assert_eq!(error.kind(), CatalogErrorKind::Validation);
    assert!(!format!("{error:?} {error}").contains("SYNTHETIC-CATALOG-SECRET"));
}

#[test]
fn catalog_intake_reuses_core_endpoint_numeric_and_unknown_field_admission() {
    let provider = ProviderId::new("deepseek").unwrap();
    for (field, value, kind) in [
        (
            "baseUrl",
            json!("https://example.invalid?secret=SYNTHETIC-CATALOG-SECRET"),
            CatalogErrorKind::Validation,
        ),
        ("contextWindow", json!(1.5), CatalogErrorKind::Decode),
        ("unknownField", json!(true), CatalogErrorKind::Decode),
    ] {
        let mut input = deepseek();
        input[0][field] = value;
        let error = decode_text_catalog(
            &serde_json::to_vec(&input).unwrap(),
            &provider,
            &ModelHeaderPolicy::default(),
        )
        .unwrap_err();
        assert_eq!(error.kind(), kind);
        assert!(!format!("{error:?} {error}").contains("SYNTHETIC-CATALOG-SECRET"));
    }
    let duplicate_field = br#"[{"id":"first","id":"second"}]"#;
    assert_eq!(
        decode_text_catalog(duplicate_field, &provider, &ModelHeaderPolicy::default())
            .unwrap_err()
            .kind(),
        CatalogErrorKind::Decode
    );
}

#[test]
fn image_catalog_intake_reuses_header_endpoint_and_identity_admission() {
    let original: Value = serde_json::from_slice(include_bytes!(
        "../../../conformance/catalog/generated/images/openrouter.json"
    ))
    .unwrap();
    let provider = ImagesProviderId::new("openrouter").unwrap();
    let mut changed = original.clone();
    changed[0]["baseUrl"] = json!("");
    assert_eq!(
        decode_image_catalog(
            &serde_json::to_vec(&changed).unwrap(),
            &provider,
            &ModelHeaderPolicy::default()
        )
        .unwrap_err()
        .kind(),
        CatalogErrorKind::Validation
    );
    changed = original.clone();
    changed[0]["headers"] = json!({"x-api-key":"SYNTHETIC-CATALOG-SECRET"});
    let error = decode_image_catalog(
        &serde_json::to_vec(&changed).unwrap(),
        &provider,
        &ModelHeaderPolicy::default(),
    )
    .unwrap_err();
    assert_eq!(error.kind(), CatalogErrorKind::Validation);
    assert!(!format!("{error:?} {error}").contains("SYNTHETIC-CATALOG-SECRET"));
    changed = original.clone();
    changed.as_array_mut().unwrap().push(original[0].clone());
    assert_eq!(
        decode_image_catalog(
            &serde_json::to_vec(&changed).unwrap(),
            &provider,
            &ModelHeaderPolicy::default()
        )
        .unwrap_err()
        .kind(),
        CatalogErrorKind::Identity
    );
    let other = ImagesProviderId::new("other").unwrap();
    assert_eq!(
        decode_image_catalog(
            &serde_json::to_vec(&original).unwrap(),
            &other,
            &ModelHeaderPolicy::default()
        )
        .unwrap_err()
        .kind(),
        CatalogErrorKind::Identity
    );
}

// Core numeric DTOs serialize finite rates as floating-point JSON numbers, so
// compare numeric values rather than spelling 0 versus 0.0. Arrays stay ordered;
// no model field, null, modality, routing price or compatibility entry is omitted.
#[allow(dead_code)]
fn assert_source_values(actual: &Value, expected: &Value) {
    match (actual, expected) {
        (Value::Number(a), Value::Number(b)) => assert_eq!(a.as_f64(), b.as_f64()),
        (Value::Array(a), Value::Array(b)) => {
            assert_eq!(a.len(), b.len());
            for (a, b) in a.iter().zip(b) {
                assert_source_values(a, b);
            }
        }
        (Value::Object(a), Value::Object(b)) => {
            assert_eq!(a.len(), b.len());
            for (key, b) in b {
                assert_source_values(a.get(key).unwrap(), b);
            }
        }
        _ => assert_eq!(actual, expected),
    }
}

macro_rules! catalog_test {
    ($feature:literal, $name:ident, $function:ident, $file:literal, $count:literal) => {
        #[cfg(feature = $feature)]
        #[test]
        fn $name() {
            let models = $function().unwrap();
            assert_eq!(models.len(), $count);
            let expected: Value = serde_json::from_slice(include_bytes!($file)).unwrap();
            assert_source_values(&serde_json::to_value(models).unwrap(), &expected);
        }
    };
}

catalog_test!(
    "catalog-openai",
    captured_openai_matches_complete_source,
    openai_models,
    "../../../conformance/catalog/generated/text/openai.json",
    39
);
catalog_test!(
    "catalog-google",
    captured_google_matches_complete_source,
    google_models,
    "../../../conformance/catalog/generated/text/google.json",
    22
);
catalog_test!(
    "catalog-anthropic",
    captured_anthropic_matches_complete_source,
    anthropic_models,
    "../../../conformance/catalog/generated/text/anthropic.json",
    14
);
catalog_test!(
    "catalog-deepseek",
    captured_deepseek_matches_complete_source,
    deepseek_models,
    "../../../conformance/catalog/generated/text/deepseek.json",
    3
);
catalog_test!(
    "catalog-openai-codex",
    captured_codex_matches_complete_source,
    openai_codex_models,
    "../../../conformance/catalog/generated/text/openai-codex.json",
    7
);
catalog_test!(
    "catalog-openrouter",
    captured_openrouter_matches_complete_source,
    openrouter_models,
    "../../../conformance/catalog/generated/text/openrouter.json",
    365
);
catalog_test!(
    "catalog-openrouter-images",
    captured_images_match_complete_source,
    openrouter_image_models,
    "../../../conformance/catalog/generated/images/openrouter.json",
    52
);

#[cfg(feature = "catalog-openrouter-images")]
#[test]
fn captured_image_routing_prices_remain_negative_and_historical_projection_is_separate() {
    let models = serde_json::to_value(openrouter_image_models().unwrap()).unwrap();
    let automatic = models
        .as_array()
        .unwrap()
        .iter()
        .find(|model| model["id"] == "openrouter/auto")
        .unwrap();
    assert_eq!(automatic["cost"]["input"].as_f64(), Some(-1_000_000.0));
    assert_eq!(automatic["cost"]["output"].as_f64(), Some(-1_000_000.0));
    let historical: Value = serde_json::from_slice(include_bytes!(
        "../../../conformance/catalog/generated/historical-images.json"
    ))
    .unwrap();
    assert_eq!(historical["openrouter"].as_object().unwrap().len(), 50);
    assert_eq!(models.as_array().unwrap().len(), 52);
}
