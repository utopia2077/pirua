mod legacy_v3_support;
use legacy_v3_support::*;
use pi_agent_session::session::RuntimeSession;
use std::sync::Arc;

#[test]
fn sealed_preparation_consumes_actual_memory_and_session_api() {
    let mut h = header();
    h["parentSession"] = json!("../parent.jsonl");
    let bytes = source(vec![h, user("old", None)]);
    let prepared = normalize(bytes.clone(), &codec(), None).unwrap();
    assert_eq!(prepared.source_bytes(), bytes);
    assert!(prepared.pending_physical_conversion());
    assert_eq!(
        prepared.metadata().legacy_parent_session_path.as_deref(),
        Some("../parent.jsonl")
    );
    let original = prepared.snapshot().clone();
    let raw = Arc::new(MemoryBacking::from_snapshot(original.clone()).unwrap())
        .open(Arc::new(Clock))
        .unwrap();
    let session = ready(RuntimeSession::new(
        raw.clone(),
        prepared.metadata().clone(),
        Arc::new(codec()),
        Arc::new(Ids),
        Arc::new(pi_telemetry::NoopTelemetryContext),
    ))
    .unwrap();
    assert_eq!(ready(session.get_stats()).unwrap(), original.stats);
    assert_eq!(
        ready(raw.scan_usage(UsageScan::default())).unwrap().len(),
        1
    );
    assert_eq!(original.usage[0].usage, Usage::zero());
    assert_eq!(original.usage[0].entry_id, None);
    assert!(original.usage[0].adjustment);
    assert_eq!(prepared.snapshot(), &original);
    assert_eq!(
        prepared.id_map().get("old"),
        prepared.retained_resolution()["old"].as_ref()
    );
    ready(session.close()).unwrap();
    assert_eq!(prepared.source_bytes(), bytes);
}

#[test]
fn complete_source_shaped_golden_covers_all_nodes_roles_tails_facts_and_usage() {
    let source = include_bytes!("../../../conformance/session/legacy-v3/all-v3.jsonl").to_vec();
    let prepared = normalize(source.clone(), &codec(), None).unwrap();
    let expected: Value = serde_json::from_str(include_str!(
        "../../../conformance/session/legacy-v3/all-v3.expected.json"
    ))
    .unwrap();
    let observed = json!({"metadata":prepared.metadata(),"snapshot":prepared.snapshot(),"idMap":prepared.id_map(),"retainedResolution":prepared.retained_resolution()});
    fn difference(a: &Value, b: &Value, path: String) -> Option<String> {
        match (a, b) {
            (Value::Object(a), Value::Object(b)) => {
                for key in a.keys().chain(b.keys()) {
                    if let Some(found) = difference(
                        a.get(key).unwrap_or(&Value::Null),
                        b.get(key).unwrap_or(&Value::Null),
                        format!("{path}/{key}"),
                    ) {
                        return Some(found);
                    }
                }
                None
            }
            (Value::Array(a), Value::Array(b)) if a.len() == b.len() => {
                for (i, (a, b)) in a.iter().zip(b).enumerate() {
                    if let Some(found) = difference(a, b, format!("{path}/{i}")) {
                        return Some(found);
                    }
                }
                None
            }
            _ if a == b => None,
            _ => Some(format!("{path}: {a:?} != {b:?}")),
        }
    }
    assert_eq!(difference(&observed, &expected, String::new()), None);
    assert_eq!(observed, expected);
    assert_eq!(prepared.source_bytes(), source);
    assert_eq!(prepared.snapshot().usage.len(), 1);
    assert_eq!(prepared.snapshot().stats.usage.total_tokens, n(338));
    assert_eq!(prepared.snapshot().stats.usage.cost.total.get(), -0.5);
    let resolved = normalize(
        source,
        &codec(),
        Some(SessionId::new("parent-header").unwrap()),
    )
    .unwrap();
    assert_eq!(
        resolved
            .metadata()
            .parent_session_id
            .as_ref()
            .unwrap()
            .as_str(),
        "parent-header"
    );
    assert!(resolved.metadata().legacy_parent_session_path.is_none());
    assert_eq!(resolved.snapshot(), prepared.snapshot());
}

#[test]
fn repeated_source_and_append_preserve_ids_but_bind_exact_bytes() {
    let first = source(vec![header(), user("a", None)]);
    let mut second_rows = vec![header(), user("a", None), user("b", Some("a"))];
    second_rows[1]["message"]["content"] =
        json!("payload changed without changing source identity");
    let second = source(second_rows);
    let a = normalize(first.clone(), &codec(), None).unwrap();
    let again = normalize(first, &codec(), None).unwrap();
    let b = normalize(second.clone(), &codec(), None).unwrap();
    assert_eq!(a.id_map(), again.id_map());
    assert_eq!(a.id_map()["a"], b.id_map()["a"]);
    assert_eq!(a.snapshot().usage[0].id, b.snapshot().usage[0].id);
    assert_ne!(a.source_bytes(), b.source_bytes());
    assert_eq!(b.source_bytes(), second);
    // No source path is an input to the pure API, so relocation cannot influence IDs.
}

#[test]
fn physical_fact_clears_root_labels_and_ecmascript_trim_are_exact() {
    let mut name = node("name", None, "session_info");
    name["name"] = json!("\u{feff}\u{a0} kept \u{2028}");
    let mut label = node("label", Some("a"), "label");
    label["targetId"] = json!("not-the-parent");
    label["label"] = json!("space ");
    let a = normalize(
        source(vec![header(), user("a", None), name.clone(), label.clone()]),
        &codec(),
        None,
    )
    .unwrap();
    assert!(
        a.snapshot()
            .registers
            .iter()
            .any(|r| matches!(r,Register::FactName{value,..}if value=="kept"))
    );
    assert!(
        a.snapshot()
            .registers
            .iter()
            .any(|r| matches!(r,Register::FactLabel{value,..}if value=="space "))
    );
    let mut clear_name = node("clear-name", None, "session_info");
    clear_name["name"] = json!("\u{feff} \n");
    let mut clear_label = node("clear-label", Some("a"), "label");
    clear_label["targetId"] = json!("other");
    let mut root_label = node("root-label", None, "label");
    root_label["targetId"] = json!("a");
    root_label["label"] = json!("ignored");
    let cleared = normalize(
        source(vec![
            header(),
            user("a", None),
            name,
            label,
            clear_name,
            clear_label,
            root_label,
        ]),
        &codec(),
        None,
    )
    .unwrap();
    assert_eq!(cleared.snapshot().registers.len(), 2);
    assert!(matches!(
        &cleared.snapshot().registers[0],
        Register::LaneLeaf { value: None, .. }
    ));
    let mut name = node("name", None, "session_info");
    name["name"] = json!("\u{85}kept\u{85}");
    let preserved = normalize(source(vec![header(), name]), &codec(), None).unwrap();
    assert!(
        preserved
            .snapshot()
            .registers
            .iter()
            .any(|r| matches!(r,Register::FactName{value,..}if value=="\u{85}kept\u{85}"))
    );
}

#[test]
fn builtin_missing_content_repairs_only_at_legacy_boundary() {
    for role in ["user", "assistant", "toolResult"] {
        for absent in [false, true] {
            let mut row = match role {
                "assistant" => assistant("a", None),
                "toolResult" => {
                    let mut r = node("a", None, "message");
                    r["message"] = json!({"role":"toolResult","toolCallId":"call","toolName":"tool","content":[],"isError":false,"timestamp":78});
                    r
                }
                _ => user("a", None),
            };
            if absent {
                row["message"].as_object_mut().unwrap().remove("content");
            } else {
                row["message"]["content"] = Value::Null;
            }
            let out = normalize(source(vec![header(), row]), &codec(), None).unwrap();
            assert_eq!(
                serde_json::to_value(&out.snapshot().entries[0]).unwrap()["message"]["content"],
                json!([])
            );
        }
    }
    let mut row = assistant("a", None);
    row["message"]["stopReason"] = json!("pending");
    assert!(normalize(source(vec![header(), row]), &codec(), None).is_err());
}

#[test]
fn known_unavailable_root_and_discarded_root_provenance_remain_open_ids() {
    let discard = node("discarded-root", None, "model_change");
    for from in ["off-path", "root", "discarded-root"] {
        let mut summary = node("summary", None, "branch_summary");
        summary["fromId"] = json!(from);
        summary["summary"] = json!("");
        let out = normalize(
            source(vec![header(), discard.clone(), summary]),
            &codec(),
            None,
        )
        .unwrap();
        assert!(
            matches!(&out.snapshot().entries[0],Entry::BranchSummary(row)if row.from_id.as_str()==from)
        );
    }
}

#[test]
fn empty_sources_and_backwards_entry_times_keep_virtual_state_and_physical_order() {
    let empty = normalize(source(vec![header()]), &codec(), None).unwrap();
    assert!(empty.snapshot().entries.is_empty());
    assert_eq!(empty.snapshot().registers.len(), 2);
    assert_eq!(empty.snapshot().usage.len(), 1);
    assert_eq!(empty.snapshot().usage[0].seq, n(2));
    assert_eq!(empty.snapshot().next_seq, Some(n(3)));
    assert_eq!(empty.snapshot().stats.usage, Usage::zero());
    let a = user("a", None);
    let mut b = user("b", Some("a"));
    b["timestamp"] = json!("1970-01-01T00:00:00Z");
    let result = normalize(source(vec![header(), a, b]), &codec(), None).unwrap();
    let values = serde_json::to_value(&result.snapshot().entries).unwrap();
    assert_eq!(values[0]["timestamp"], 1000);
    assert_eq!(values[1]["timestamp"], 0);
    assert_eq!(values[1]["parentId"], values[0]["id"]);
    assert_eq!(
        pi_agent_session::id::uuid_timestamp(result.id_map()["b"].as_str()).unwrap(),
        n(0)
    );
    let left = normalize(
        source(vec![header(), user("a", None), user("b", None)]),
        &codec(),
        None,
    )
    .unwrap();
    let right = normalize(
        source(vec![header(), user("b", None), user("a", None)]),
        &codec(),
        None,
    )
    .unwrap();
    assert_eq!(
        left.id_map(),
        right.id_map(),
        "physical position must not enter deterministic tails"
    );
    let mut detached = right.snapshot().clone();
    detached.entries.clear();
    assert_eq!(
        right.snapshot().entries.len(),
        2,
        "a detached copy cannot mutate prepared source binding"
    );
}

#[test]
fn builtin_deferred_tokens_signatures_and_custom_image_content_are_preserved() {
    let mut row = assistant("a", None);
    row["message"]["stopReason"] = json!("deferred");
    row["message"]["deferred"] = json!({"provider":"p","modelId":"m","api":"a","id":"a","expiresAt":123,"pollAfterMs":0,"data":{"entryId":"a","timestamp":"1970-01-01T00:00:01Z"}});
    row["message"]["content"] = json!([{"type":"thinking","thinking":"a","thinkingSignature":"a","redacted":true},{"type":"toolCall","id":"a","name":"tool","arguments":{"fromId":"a"},"thoughtSignature":"a","namespace":"a"},{"type":"text","text":"a","textSignature":"a"}]);
    let expected: AgentMessage = serde_json::from_value(row["message"].clone()).unwrap();
    let mut custom = node("b", Some("a"), "custom_message");
    custom["customType"] = json!("image");
    custom["display"] = json!(true);
    custom["content"] =
        json!([{"type":"text","text":"a"},{"type":"image","data":"YQ==","mimeType":"image/png"}]);
    let out = normalize(source(vec![header(), row, custom.clone()]), &codec(), None).unwrap();
    assert!(matches!(&out.snapshot().entries[0],Entry::Message(entry)if entry.message==expected));
    let value = serde_json::to_value(&out.snapshot().entries[1]).unwrap();
    assert_eq!(value["message"]["content"], custom["content"]);
    assert_eq!(value["message"]["timestamp"], 1000);
}
