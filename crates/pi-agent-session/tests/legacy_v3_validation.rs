mod legacy_v3_support;
use legacy_v3_support::*;

#[test]
fn source_envelopes_versions_and_original_ancestry_fail_closed() {
    for version in [Value::Null, json!(1), json!(2), json!(4)] {
        let mut h = header();
        h["version"] = version;
        assert!(matches!(
            normalize(source(vec![h]), &codec(), None),
            Err(LegacyError::InvalidData)
        ));
    }
    for raw in [
        b"".as_slice(),
        b"{\"type\":\"session\"}",
        b"{\"type\":\"session\",\"version\":3,\"version\":3}",
    ] {
        assert!(normalize(raw.to_vec(), &codec(), None).is_err());
    }
    let mut duplicate_nested = user("a", None);
    duplicate_nested["message"]["content"] = json!("nested");
    let valid = source(vec![header(), duplicate_nested]);
    let raw = String::from_utf8(valid).unwrap().replace(
        "\"content\":\"nested\"",
        "\"content\":\"one\",\"content\":\"two\"",
    );
    assert!(normalize(raw.into_bytes(), &codec(), None).is_err());
    for rows in [
        vec![user("a", Some("missing"))],
        vec![user("a", Some("a"))],
        vec![user("a", Some("b")), user("b", None)],
        vec![user("a", None), user("a", None)],
        vec![node("discard", Some("missing"), "active_tools_change")],
    ] {
        let mut all = vec![header()];
        all.extend(rows);
        assert!(matches!(
            normalize(source(all), &codec(), None),
            Err(LegacyError::Ancestry)
        ));
    }
    assert!(matches!(
        normalize(
            source(vec![header(), node("a", None, "future")]),
            &codec(),
            None
        ),
        Err(LegacyError::InvalidData)
    ));
    assert!(
        normalize(
            source(vec![header()]),
            &codec(),
            Some(SessionId::new("unrelated").unwrap())
        )
        .is_err()
    );
}
#[test]
fn invalid_own_branch_compaction_bounds_never_drop_history() {
    for boundary in ["missing", "cp", "later", "off"] {
        let mut cp = node("cp", Some("a"), "compaction");
        cp["firstKeptEntryId"] = json!(boundary);
        cp["summary"] = json!("");
        cp["tokensBefore"] = json!(0);
        let rows = vec![
            header(),
            user("a", None),
            user("off", None),
            cp,
            user("later", Some("cp")),
        ];
        assert!(
            matches!(
                normalize(source(rows), &codec(), None),
                Err(LegacyError::CompactionBoundary)
            ),
            "{boundary}"
        );
    }
}
#[test]
fn omitted_and_null_details_differ_but_null_usage_and_from_hook_reject() {
    let mut a = node("a", None, "custom");
    a["customType"] = json!("state");
    let mut b = node("b", Some("a"), "custom");
    b["customType"] = json!("state");
    b["data"] = Value::Null;
    let out = normalize(source(vec![header(), a, b]), &codec(), None).unwrap();
    let values = serde_json::to_value(&out.snapshot().entries).unwrap();
    assert!(values[0].get("data").is_none());
    assert!(values[1]["data"].is_null());
    for field in ["usage", "fromHook"] {
        let mut s = node("s", None, "branch_summary");
        s["summary"] = json!("");
        s["fromId"] = json!("root");
        s[field] = Value::Null;
        assert!(normalize(source(vec![header(), s]), &codec(), None).is_err());
    }
}
#[test]
fn usage_overflow_and_optional_zero_are_not_recomputed() {
    let mut a = assistant("a", None);
    a["message"]["usage"]["input"] = json!(SafeUnsigned::MAX.get());
    let b = assistant("b", Some("a"));
    assert!(matches!(
        normalize(source(vec![header(), a, b]), &codec(), None),
        Err(LegacyError::Overflow)
    ));
    let mut a = assistant("a", None);
    a["message"]["usage"]["cost"]["total"] = json!(1e308);
    let mut b = assistant("b", Some("a"));
    b["message"]["usage"]["cost"]["total"] = json!(1e308);
    assert!(matches!(
        normalize(source(vec![header(), a, b]), &codec(), None),
        Err(LegacyError::Overflow)
    ));
    let mut a = assistant("a", None);
    a["message"]["usage"]["reasoning"] = json!(0);
    let b = assistant("b", Some("a"));
    let result = normalize(source(vec![header(), a, b]), &codec(), None).unwrap();
    assert_eq!(result.snapshot().stats.usage.reasoning, Some(n(0)));
    assert_eq!(result.snapshot().stats.usage.cache_write_1h, None);
    assert_eq!(result.snapshot().stats.usage.total_tokens, n(198));
}
#[test]
fn custom_schema_admission_is_explicit_and_errors_are_content_free() {
    let mut row = node("a", None, "custom_message");
    row["customType"] = json!("note");
    row["content"] = json!("private-synthetic-marker");
    row["display"] = json!(false);
    let default = SessionCodec::new(
        SessionCodecOptions::default(),
        ProviderHeaderPolicy::default(),
    )
    .unwrap();
    let error = normalize(source(vec![header(), row.clone()]), &default, None).unwrap_err();
    assert!(matches!(&error,LegacyError::Codec(error)if error.category()=="unknown_custom_role"));
    assert!(!format!("{error:?} {error}").contains("private-synthetic-marker"));
    row["display"] = json!("invalid");
    assert!(normalize(source(vec![header(), row]), &codec(), None).is_err());
    let mut schemas = coding_message_options().custom_message_schemas().clone();
    schemas.insert(CustomRole::new("future").unwrap(),serde_json::from_value(json!({"type":"object","required":["role","payload"],"properties":{"role":{"const":"future"},"payload":{"type":"string"}},"additionalProperties":false})).unwrap());
    let explicit = SessionCodec::new(
        SessionCodecOptions::new(schemas),
        ProviderHeaderPolicy::default(),
    )
    .unwrap();
    let mut row = node("a", None, "message");
    row["message"] = json!({"role":"future","payload":"kept"});
    assert!(normalize(source(vec![header(), row]), &explicit, None).is_ok());
}
#[test]
fn custom_message_requires_source_fields_even_with_a_permissive_role_schema() {
    let options = SessionCodecOptions::from_role_schemas(
        [(
            "custom".to_owned(),
            serde_json::from_value(json!({"type":"object"})).unwrap(),
        )]
        .into(),
    )
    .unwrap();
    let permissive = SessionCodec::new(options, ProviderHeaderPolicy::default()).unwrap();
    let row = node("a", None, "custom_message");
    assert!(normalize(source(vec![header(), row]), &permissive, None).is_err());
}
