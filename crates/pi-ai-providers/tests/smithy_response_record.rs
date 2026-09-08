use pi_ai_core::FiniteF64;
use pi_ai_providers::api::runtime::{
    AdapterResponseHead, ResponseHeaderRecord, ResponseHeaderValue,
};
use pi_ai_transport::{HttpHeaders, SmithyProtocol, TransportErrorKind};

#[test]
fn complete_records_match_actual_smithy_handlers_and_pinned_provider_projection() {
    let source: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/smithy-http/trace.json"
    ))
    .unwrap();
    for case in source["cases"].as_array().unwrap() {
        let mut raw = HttpHeaders::new();
        for pair in case["raw"].as_array().unwrap() {
            raw.append(pair[0].as_str().unwrap(), pair[1].as_str().unwrap())
                .unwrap();
        }
        let protocol = if case["protocol"] == "h2" {
            SmithyProtocol::Http2
        } else {
            SmithyProtocol::Http1
        };
        let head = AdapterResponseHead::from_smithy(
            case["status"].as_u64().unwrap() as u16,
            &raw,
            protocol,
        )
        .unwrap();
        let expected = case["entries"].as_array().unwrap();
        assert_eq!(head.headers.iter().count(), expected.len());
        for ((key, value), pair) in head.headers.iter().zip(expected) {
            assert_eq!(key, pair[0].as_str().unwrap());
            match value {
                ResponseHeaderValue::Text(v) => assert_eq!(Some(v.as_str()), pair[1].as_str()),
                ResponseHeaderValue::Number(v) => assert_eq!(Some(v.get()), pair[1].as_f64()),
            }
        }
    }
}

#[test]
fn smithy_records_preserve_typed_status_singletons_cookies_and_own_order() {
    let mut raw = HttpHeaders::new();
    for (k, v) in [
        ("x-z", "first"),
        ("set-cookie", "a=1"),
        ("content-type", "one"),
        ("x-a", "next"),
        ("set-cookie", "b=2"),
        ("content-type", "two"),
        ("cookie", "a=1"),
        ("cookie", "b=2"),
        ("x-z", "last"),
        ("__proto__", "discard"),
        ("constructor", "ordinary"),
        ("2", "number-key"),
        ("01", "string-key"),
    ] {
        raw.append(k, v).unwrap();
    }
    for protocol in [SmithyProtocol::Http1, SmithyProtocol::Http2] {
        let before = raw.clone();
        let head = AdapterResponseHead::from_smithy(200, &raw, protocol).unwrap();
        assert_eq!(before, raw);
        assert_eq!(head.headers.get_str("set-cookie"), Some("a=1,b=2"));
        assert_eq!(head.headers.get_str("cookie"), Some("a=1; b=2"));
        assert_eq!(head.headers.get_str("content-type"), Some("one"));
        assert_eq!(head.headers.get_str("x-z"), Some("first, last"));
        assert_eq!(head.headers.get_str("constructor"), Some("ordinary"));
        assert_eq!(head.headers.get_str(":status"), None);
        let status_value = ResponseHeaderValue::Number(FiniteF64::new(200.0).unwrap());
        assert_eq!(
            head.headers.get(":status"),
            if protocol == SmithyProtocol::Http2 {
                Some(&status_value)
            } else {
                None
            }
        );
        let mut expected = vec!["2"];
        if protocol == SmithyProtocol::Http2 {
            expected.push(":status");
        }
        expected.extend([
            "x-z",
            "set-cookie",
            "content-type",
            "x-a",
            "cookie",
            "constructor",
            "01",
        ]);
        assert_eq!(
            head.headers.iter().map(|(key, _)| key).collect::<Vec<_>>(),
            expected
        );
        assert!(!format!("{head:?}").contains("first"));
    }
}
#[test]
fn record_exact_replacement_is_atomic_bounded_and_never_coerces_number() {
    let mut record = ResponseHeaderRecord::new();
    record
        .insert("name".into(), ResponseHeaderValue::Text("secret".into()))
        .unwrap();
    record
        .insert(
            "2".into(),
            ResponseHeaderValue::Number(FiniteF64::new(2.0).unwrap()),
        )
        .unwrap();
    record
        .insert(
            "name".into(),
            ResponseHeaderValue::Text("replacement".into()),
        )
        .unwrap();
    assert_eq!(
        record.iter().map(|(k, _)| k).collect::<Vec<_>>(),
        ["2", "name"]
    );
    assert_eq!(record.get_str("2"), None);
    let before = record.clone();
    assert_eq!(
        record
            .insert(
                "name".into(),
                ResponseHeaderValue::Text("x".repeat(ResponseHeaderRecord::MAX_BYTES))
            )
            .unwrap_err()
            .kind(),
        TransportErrorKind::LimitExceeded
    );
    assert_eq!(record, before);
    assert!(!format!("{record:?}").contains("replacement"));
    assert!(HttpHeaders::new().append(":status", "200").is_err());
}
