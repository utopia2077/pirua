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
            .iter_for_wire()
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
    assert_eq!(head.headers.iter_for_wire().count(), 0);
    let mut raw = HttpHeaders::new();
    raw.append("authorization", "private-value").unwrap();
    let head = AdapterResponseHead::from_http(200, &raw).unwrap();
    assert!(!format!("{head:?}").contains("private-value"));
}
