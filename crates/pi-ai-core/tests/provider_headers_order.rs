use pi_ai_core::ProviderHeaders;
use serde_json::Value;
use std::collections::BTreeMap;

#[test]
fn pinned_own_key_permutations_null_and_numeric_keys_round_trip() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/provider-header-order/trace.json"
    ))
    .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let headers: ProviderHeaders = serde_json::from_str(case["raw"].as_str().unwrap()).unwrap();
        let expected: Vec<(String, Option<String>)> =
            serde_json::from_value(case["ownEntries"].clone()).unwrap();
        assert_eq!(
            headers
                .iter_for_dispatch()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>(),
            expected
        );
        let serialized = serde_json::to_string(&headers).unwrap();
        assert_eq!(serialized, case["serialized"].as_str().unwrap());
        assert_eq!(
            headers,
            serde_json::from_str::<ProviderHeaders>(&serialized).unwrap()
        );
        assert_eq!(headers, headers.clone());
    }
}

#[test]
fn exact_duplicates_and_invalid_values_remain_rejected() {
    for raw in [
        r#"{"x":null,"x":"secret"}"#,
        r#"{"x":"secret","x":null}"#,
        r#"{"x":null,"x":null}"#,
        r#"{"x":1}"#,
        r#"{"x":false}"#,
        r#"{"x":{}}"#,
        r#"{"x":[]}"#,
        "null",
        "[]",
    ] {
        assert!(
            serde_json::from_str::<ProviderHeaders>(raw).is_err(),
            "{raw}"
        );
    }
    let headers: ProviderHeaders =
        serde_json::from_str(r#"{"x":"private-value","X":null,"__proto__":"own"}"#).unwrap();
    assert_eq!(headers.iter_for_dispatch().count(), 3);
    assert!(!format!("{headers:?}").contains("private-value"));
}

#[test]
fn map_constructor_keeps_its_known_order_and_own_numeric_projection() {
    let headers = ProviderHeaders::new(BTreeMap::from([
        ("x".into(), None),
        ("X".into(), Some("upper".into())),
        ("10".into(), None),
        ("2".into(), None),
        ("01".into(), None),
    ]));
    assert_eq!(
        headers
            .iter_for_dispatch()
            .map(|(k, _)| k.as_str())
            .collect::<Vec<_>>(),
        ["2", "10", "01", "X", "x"]
    );
    assert_eq!(headers.as_map().get("X"), Some(&Some("upper".into())));
}

#[test]
fn raw_header_order_survives_serialization_and_is_observable_in_equality() {
    let lower_first = r#"{"x-order":"lower","X-Order":"upper"}"#;
    let upper_first = r#"{"X-Order":"upper","x-order":"lower"}"#;
    let first: ProviderHeaders = serde_json::from_str(lower_first).unwrap();
    let second: ProviderHeaders = serde_json::from_str(upper_first).unwrap();
    assert_eq!(
        first.as_map(),
        second.as_map(),
        "lookup alone cannot distinguish the source inputs"
    );
    assert_eq!(serde_json::to_string(&first).unwrap(), lower_first);
    assert_eq!(serde_json::to_string(&second).unwrap(), upper_first);
    assert_ne!(first, second, "SDK override outcomes differ");
    assert_eq!(first, first.clone());
}
