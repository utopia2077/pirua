mod session_support;
use session_support::*;
use std::sync::Arc;
#[test]
fn registered_roles_validate_before_publication_and_on_reopen() {
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let s = session(raw.clone(), registered());
    ready(s.append_message(custom("note", "valid"))).unwrap();
    for (message, category) in [
        (custom("unknown", "hidden-payload"), "unknown_custom_role"),
        (custom("note", ""), "schema"),
    ] {
        let before = bytes(raw.as_ref());
        let error = ready(s.append_message(message)).unwrap_err();
        assert_eq!(error.category(), category);
        assert!(!format!("{error:?} {error}").contains("hidden-payload"));
        assert_eq!(before, bytes(raw.as_ref()));
    }
    assert!(
        ready(RuntimeSession::new(
            raw.clone(),
            metadata(),
            codec(),
            Arc::new(Ids::default()),
            Arc::new(pi_telemetry::NoopTelemetryContext)
        ))
        .is_err()
    );
    let again = session(raw.clone(), registered());
    assert_eq!(ready(again.get_stats()).unwrap().message_count, n(1));
    ready(s.append_custom_entry(
        ct("opaque"),
        Presence::Value(serde_json::from_value(json!({"role":"unknown"})).unwrap()),
    ))
    .unwrap();
}
#[test]
fn pending_and_all_preparation_message_slots_are_checked() {
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let s = session(raw.clone(), registered());
    let fixtures: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .unwrap();
    for (index, field) in [
        (0, "messagesToSummarize"),
        (0, "turnPrefixMessages"),
        (0, "retainedTail"),
        (1, "messages"),
    ] {
        let mut value = fixtures["preparations"][index].clone();
        value[field] = json!([{"role":"unknown","text":"private"}]);
        let prep: DurableStructuralPreparation = serde_json::from_value(value).unwrap();
        let before = bytes(raw.as_ref());
        assert_eq!(
            ready(s.commit(tx(vec![set(RegisterSet::OperationPreparation {
                key: "op:task".into(),
                value: prep
            })])))
            .unwrap_err()
            .category(),
            "unknown_custom_role"
        );
        assert_eq!(before, bytes(raw.as_ref()));
    }
    for value in [
        json!({"type":"message"}),
        json!({"type":"message","payload":null}),
        json!({"type":"message","customType":"bad","payload":{"role":"user","content":"x","timestamp":0}}),
        json!({"type":"custom"}),
        json!({"type":"message","payload":{"role":"unknown"}}),
    ] {
        let pending: PendingEntry = serde_json::from_value(value).unwrap();
        let before = bytes(raw.as_ref());
        assert!(
            ready(s.commit(tx(vec![set(RegisterSet::PendingEntry {
                key: "queued".into(),
                value: pending
            })])))
            .is_err()
        );
        assert_eq!(before, bytes(raw.as_ref()));
    }
    let compaction:NewEntry=serde_json::from_value(json!({"id":"cp","parentId":null,"type":"compaction","summary":"s","retainedTail":[{"role":"unknown"}],"tokensBefore":0,"fromHook":false})).unwrap();
    assert_eq!(
        ready(s.commit(tx(vec![Write::Entry(compaction)])))
            .unwrap_err()
            .category(),
        "unknown_custom_role"
    );
}
#[test]
fn policy_aware_roots_reopen_and_changed_policy_refuses() {
    let states: serde_json::Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/states-v1.json"
    ))
    .unwrap();
    let mut state = states
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["name"] == "run.assistant.effect_pending")
        .unwrap()["value"]
        .clone();
    state["phase"]["generation"]["context"]["streamOptions"]["headers"] =
        json!({"X-Public":"visible"});
    let encoded=serde_json::to_vec(&json!({"writes":[{"kind":"register","op":"set","namespace":"op.state","key":"op","value":state}]})).unwrap();
    let policy = ProviderHeaderPolicy::from_allowlist(["x-public".into()]);
    let c = Arc::new(SessionCodec::new(SessionCodecOptions::default(), policy).unwrap());
    let tx = c.decode_transaction(&encoded).unwrap();
    assert!(codec().decode_transaction(&encoded).is_err());
    let (_, raw) = memory();
    seed(raw.as_ref(), "main", None);
    let s = session(raw.clone(), c.clone());
    ready(s.commit(tx)).unwrap();
    let reg = ready(s.get_register(RegisterNamespace::OperationState, "op"))
        .unwrap()
        .unwrap();
    let bytes = serde_json::to_vec(&reg).unwrap();
    assert_eq!(c.decode_register(&bytes).unwrap(), reg);
    assert!(codec().decode_register(&bytes).is_err());
    assert!(
        ready(RuntimeSession::new(
            raw.clone(),
            metadata(),
            codec(),
            Arc::new(Ids::default()),
            Arc::new(pi_telemetry::NoopTelemetryContext)
        ))
        .is_err()
    );
    session(raw, c);
}
#[test]
fn offline_schema_and_invalid_durable_decode_fail_closed() {
    let options = SessionCodecOptions::from_role_schemas(
        [(
            "note".into(),
            serde_json::from_value(json!({"$ref":"https://unavailable.invalid/schema"})).unwrap(),
        )]
        .into(),
    )
    .unwrap();
    assert!(SessionCodec::new(options, ProviderHeaderPolicy::default()).is_err());
    let c = codec();
    for bytes in [
        br#"{"writes":[],"extra":1}"#.as_slice(),
        br#"{"writes":[],"writes":[]}"#.as_slice(),
    ] {
        assert!(c.decode_transaction(bytes).is_err());
    }
    let raw=br#"{"id":"e","parentId":null,"seq":0,"timestamp":0,"type":"message","message":{"role":"assistant","content":[],"api":"x","provider":"x","model":"x","usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"pending","timestamp":0}}"#;
    assert!(c.decode_entry(raw).is_err());
}
