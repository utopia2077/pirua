use pi_ai_providers::api::runtime::AdapterResponseHead;
use pi_ai_transport::HttpHeaders;
use serde_json::Value;

#[test]
fn own_record_projection_matches_executed_headers_helper_without_mutating_raw() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-helpers/response-head/headers.json"
    ))
    .unwrap();
    let raw_entries: Vec<(String, String)> =
        serde_json::from_value(fixture["raw"].clone()).unwrap();
    let expected: Vec<(String, String)> =
        serde_json::from_value(fixture["ownRecordEntries"].clone()).unwrap();
    let mut raw = HttpHeaders::new();
    for (name, value) in raw_entries {
        raw.append(name, value).unwrap();
    }
    let before = raw.clone();
    let head = AdapterResponseHead::from_http(207, &raw).unwrap();
    assert_eq!(head.status, 207);
    assert_eq!(raw, before);
    assert_eq!(
        head.headers
            .iter()
            .map(|(key, value)| (key, value.as_str().expect("Fetch string record")))
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        raw.iter_for_wire()
            .filter(|(name, _)| *name == "set-cookie")
            .count(),
        2
    );
}

#[test]
fn empty_head_has_no_invented_entries_and_debug_redacts_values() {
    let head = AdapterResponseHead::from_http(204, &HttpHeaders::new()).unwrap();
    assert_eq!(head.status, 204);
    assert_eq!(head.headers.iter().count(), 0);
    let mut raw = HttpHeaders::new();
    raw.append("authorization", "private-value").unwrap();
    let head = AdapterResponseHead::from_http(200, &raw).unwrap();
    assert!(!format!("{head:?}").contains("private-value"));
}

#[test]
fn observed_network_fields_keep_trailing_whitespace_and_last_cookie() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/native-fetch-headers/headers.json"
    ))
    .unwrap();
    for row in fixture["rows"].as_array().unwrap() {
        let mut raw = HttpHeaders::new();
        let mut semantic = HttpHeaders::new();
        for name in ["x-test", "cookie", "set-cookie"] {
            for value in row["values"].as_array().unwrap() {
                let value = value.as_str().unwrap();
                raw.append(name, value.trim_matches([' ', '\t'])).unwrap();
                semantic
                    .append(name, value.trim_start_matches([' ', '\t']))
                    .unwrap();
            }
        }
        let raw = raw.with_fetch_response_fields(semantic).unwrap();
        let before = raw.clone();
        let head = AdapterResponseHead::from_http(200, &raw).unwrap();
        assert_eq!(raw, before);
        assert_eq!(
            serde_json::json!(
                head.headers
                    .iter()
                    .map(|(key, value)| (key, value.as_str().expect("Fetch string record")))
                    .collect::<Vec<_>>()
            ),
            row["network"]["own"]
        );
        for pair in row["network"]["own"].as_array().unwrap() {
            assert_eq!(
                serde_json::json!(head.headers.get_str(pair[0].as_str().unwrap())),
                pair[1]
            );
        }
    }
}

#[test]
fn accepted_large_constructed_response_survives_record_migration() {
    use pi_ai_providers::api::runtime::ResponseHeaderValue;
    use pi_ai_transport::{SmithyProtocol, TransportErrorKind};
    let source: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/smithy-http/fetch-record-admission.json"
    ))
    .unwrap();
    assert_eq!(source["largeValueBytes"], 1_048_576);
    let value = "a".repeat(1_048_576);
    let mut raw = HttpHeaders::new();
    raw.append("x-large", value.clone()).unwrap();
    let head = AdapterResponseHead::from_http(200, &raw)
        .expect("existing >one-MiB logical record admitted");
    assert_eq!(head.headers.get_str("x-large"), Some(value.as_str()));
    assert_eq!(head.headers.iter().count(), 1);
    for protocol in [SmithyProtocol::Http1, SmithyProtocol::Http2] {
        assert_eq!(
            AdapterResponseHead::from_smithy(200, &raw, protocol)
                .unwrap_err()
                .kind(),
            TransportErrorKind::LimitExceeded
        );
    }
    let mut changed = head.headers.clone();
    changed
        .insert(
            "x-large".into(),
            ResponseHeaderValue::Text("b".repeat(value.len())),
        )
        .unwrap();
    let before = changed.clone();
    for (name, text) in [
        ("x-large", "b".repeat(value.len() + 1)),
        ("x-extra", String::new()),
    ] {
        assert_eq!(
            changed
                .insert(name.into(), ResponseHeaderValue::Text(text))
                .unwrap_err()
                .kind(),
            TransportErrorKind::LimitExceeded
        );
        assert_eq!(changed, before, "failed inherited growth is atomic");
    }
    changed
        .insert(
            "x-large".into(),
            ResponseHeaderValue::Text("smaller".into()),
        )
        .unwrap();
    assert_eq!(changed.get_str("x-large"), Some("smaller"));
    let smaller = changed.clone();
    assert_eq!(
        changed
            .insert("x-large".into(), ResponseHeaderValue::Text(value.clone()))
            .unwrap_err()
            .kind(),
        TransportErrorKind::LimitExceeded
    );
    assert_eq!(
        changed, smaller,
        "shrinking does not preserve a larger growth allowance"
    );
    assert_eq!(
        head.headers.get_str("x-large"),
        Some(value.as_str()),
        "clone edits do not change the admitted source record"
    );
}

#[test]
fn accepted_many_constructed_fields_survive_record_migration() {
    use pi_ai_providers::api::runtime::ResponseHeaderValue;
    use pi_ai_transport::{SmithyProtocol, TransportErrorKind};
    let source: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/smithy-http/fetch-record-admission.json"
    ))
    .unwrap();
    assert_eq!(source["entryCount"], 8193);
    let mut raw = HttpHeaders::new();
    let mut names = Vec::new();
    for n in 0..8193 {
        let name = format!("x-{n}");
        raw.append(&name, "a").unwrap();
        names.push(name);
    }
    let head = AdapterResponseHead::from_http(200, &raw).expect("existing8193 fields admitted");
    names.sort();
    assert_eq!(
        head.headers
            .iter()
            .map(|(name, _)| name)
            .collect::<Vec<_>>(),
        names
    );
    assert!(
        head.headers
            .iter()
            .all(|(_, value)| value.as_str() == Some("a"))
    );
    for protocol in [SmithyProtocol::Http1, SmithyProtocol::Http2] {
        assert_eq!(
            AdapterResponseHead::from_smithy(200, &raw, protocol)
                .unwrap_err()
                .kind(),
            TransportErrorKind::LimitExceeded
        );
    }
    let mut changed = head.headers.clone();
    changed
        .insert("x-0".into(), ResponseHeaderValue::Text("b".into()))
        .unwrap();
    assert_eq!(changed.get_str("x-0"), Some("b"));
    let before = changed.clone();
    assert_eq!(
        changed
            .insert("x-new".into(), ResponseHeaderValue::Text(String::new()))
            .unwrap_err()
            .kind(),
        TransportErrorKind::LimitExceeded
    );
    assert_eq!(changed, before, "above-limit inherited count cannot grow");
    assert_eq!(head.headers.get_str("x-0"), Some("a"));
}
