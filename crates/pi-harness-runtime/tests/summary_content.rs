use pi_agent_schema::*;
use pi_ai_core::{Message, SafeUnsigned, Usage};
use pi_harness_runtime::summary_content::*;
use serde_json::{Value, json};
use std::collections::BTreeMap;
fn golden() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/harness/r8-r10/content/golden.json"
    ))
    .unwrap()
}
fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
fn messages(v: &Value) -> Vec<AgentMessage> {
    serde_json::from_value(v.clone()).unwrap()
}
fn rows(v: &Value) -> Vec<Entry> {
    v.as_array()
        .unwrap()
        .iter()
        .map(|v| {
            let mut v = v.clone();
            if v["type"] == "compaction" || v["type"] == "branch_summary" {
                v["fromHook"] = false.into()
            }
            serde_json::from_value(v).unwrap()
        })
        .collect()
}
fn detached(rows: &[Entry]) -> DetachedBranch {
    let mut summaries = BTreeMap::new();
    for e in rows {
        if let Entry::Compaction(e) = e {
            summaries.insert(e.id.clone(),serde_json::from_value(json!({"role":"compactionSummary","summary":e.summary,"tokensBefore":e.tokens_before,"timestamp":e.timestamp})).unwrap());
        }
        if let Entry::BranchSummary(e) = e {
            summaries.insert(e.id.clone(),serde_json::from_value(json!({"role":"branchSummary","summary":e.summary,"fromId":e.from_id,"timestamp":e.timestamp})).unwrap());
        }
    }
    project_branch(rows, &BTreeMap::new(), &summaries, None).unwrap()
}
fn model() -> pi_ai_core::Model {
    let v: Value = serde_json::from_str(include_str!(
        "../../../conformance/schema/ai/models-options.golden.json"
    ))
    .unwrap();
    let mut m: pi_ai_core::Model = serde_json::from_value(v["model"].clone()).unwrap();
    m.max_tokens = n(999);
    m.reasoning = true;
    m
}
fn response(text: &str) -> SettledAssistantMessage {
    serde_json::from_value(json!({"role":"assistant","content":[{"type":"text","text":text}],"api":"synthetic","provider":"synthetic","model":"fixture","usage":{"input":2,"output":3,"cacheRead":0,"cacheWrite":0,"totalTokens":5,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":2})).unwrap()
}
#[test]
fn complete_pinned_preparation_and_request_goldens() {
    let g = golden();
    for (s, generated) in g["observations"]
        .as_array()
        .unwrap()
        .iter()
        .zip(g["generator"].as_array().unwrap())
    {
        let prep = prepare_compaction(
            &detached(&rows(&s["entries"])),
            serde_json::from_value(s["settings"].clone()).unwrap(),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            prep,
            serde_json::from_value::<CompactionPreparation>(s["preparation"].clone()).unwrap(),
            "{}",
            s["name"]
        );
        let history: Vec<Message> =
            serde_json::from_value(generated["convertedHistory"].clone()).unwrap();
        let prefix: Vec<Message> =
            serde_json::from_value(generated["convertedPrefix"].clone()).unwrap();
        let specs = compaction_requests(
            &prep,
            &history,
            &prefix,
            &model(),
            Some("focus"),
            AgentThinkingLevel::High,
        )
        .unwrap();
        let calls = generated["calls"].as_array().unwrap();
        assert_eq!(specs.len(), calls.len());
        for (spec, call) in specs.iter().zip(calls) {
            assert_eq!(spec.system_prompt, call["context"]["systemPrompt"]);
            assert_eq!(
                spec.prompt,
                call["context"]["messages"][0]["content"][0]["text"]
            );
            assert_eq!(
                spec.max_tokens.get(),
                call["options"]["maxTokens"].as_u64().unwrap()
            );
        }
        let result = finish_compaction(&prep, &vec![response("summary"); specs.len()]).unwrap();
        assert_eq!(
            result,
            serde_json::from_value::<CompactResult>(generated["result"].clone()).unwrap()
        );
    }
}
#[test]
fn pinned_branch_budgets_transcript_utf16_and_json() {
    let g = golden();
    for case in g["branch"].as_array().unwrap() {
        let p = prepare_branch(
            &detached(&rows(&case["entries"])).positions,
            i128::from(case["budget"].as_i64().unwrap()),
        )
        .unwrap();
        assert_eq!(
            p,
            serde_json::from_value::<BranchPreparation>(case["preparation"].clone()).unwrap()
        )
    }
    let transcript: Vec<Message> = serde_json::from_value(g["transcriptMessages"].clone()).unwrap();
    assert_eq!(
        serialize_conversation(&transcript).unwrap(),
        g["transcript"]
    );
    for (m, t) in messages(&g["transcriptMessages"])
        .iter()
        .zip(g["tokens"].as_array().unwrap())
    {
        assert_eq!(estimate_tokens(m).unwrap().get(), t.as_u64().unwrap());
    }
}
#[test]
fn pinned_branch_prompt_and_empty_generated_fallback() {
    let g = golden();
    let entries = rows(&g["observations"][0]["entries"]);
    let p = prepare_branch(&detached(&entries).positions, 1000).unwrap();
    let converted: Vec<_> = p
        .messages
        .iter()
        .map(|m| match m {
            AgentMessage::BuiltIn(m) => m.clone(),
            _ => panic!(),
        })
        .collect();
    let spec = branch_request(&p, &converted, None, false)
        .unwrap()
        .unwrap();
    assert_eq!(
        spec.prompt,
        g["branchCalls"][0]["context"]["messages"][0]["content"][0]["text"]
    );
    let mut r = response("branch text").into_message();
    r.usage = Usage::zero();
    let result = finish_branch(&p, Some(&SettledAssistantMessage::new(r).unwrap())).unwrap();
    assert_eq!(
        result,
        serde_json::from_value::<BranchSummaryResult>(g["branchResult"]["value"].clone()).unwrap()
    );
    let empty = prepare_branch(&[], 0).unwrap();
    assert!(branch_request(&empty, &[], None, false).unwrap().is_none());
    assert_eq!(
        serde_json::to_value(finish_branch(&empty, None).unwrap()).unwrap(),
        g["empty"]["value"]
    );
}
#[test]
fn v3_projection_filters_stored_failures_but_retains_projector_outputs_and_prospective_boundary() {
    let g = golden();
    let mut entries = rows(&g["observations"][2]["entries"]);
    entries.truncate(1);
    let b = detached(&entries);
    assert!(
        prepare_compaction(
            &b,
            CompactionSettings {
                enabled: true,
                reserve_tokens: n(10),
                keep_recent_tokens: n(3)
            }
        )
        .unwrap()
        .is_none()
    );
    let id = EntryId::new("future-overflow-response").unwrap();
    let mut summaries = BTreeMap::new();
    summaries.insert(
        EntryId::new("e0").unwrap(),
        b.positions[0].message.clone().unwrap(),
    );
    let projected =
        project_branch(&entries, &BTreeMap::new(), &summaries, Some(id.clone())).unwrap();
    assert!(!projected.ends_at_compaction);
    assert!(
        matches!(projected.positions.last().unwrap().origin,ProjectionOrigin::Prospective(ref actual)if actual==&id)
    );
    assert!(
        prepare_compaction(
            &projected,
            CompactionSettings {
                enabled: true,
                reserve_tokens: n(10),
                keep_recent_tokens: n(3)
            }
        )
        .unwrap()
        .is_some()
    );
    let mut error = serde_json::to_value(response("exclude")).unwrap();
    error["stopReason"] = "error".into();
    error["errorMessage"] = "overflow".into();
    let row: Entry = serde_json::from_value(
        json!({"type":"message","id":"err","parentId":"e0","seq":2,"timestamp":2,"message":error}),
    )
    .unwrap();
    entries.push(row);
    let custom:Entry=serde_json::from_value(json!({"type":"custom","customType":"note","id":"custom","parentId":"err","seq":3,"timestamp":3})).unwrap();
    entries.push(custom);
    let outputs = BTreeMap::from([(
        EntryId::new("custom").unwrap(),
        vec![serde_json::from_value(error).unwrap()],
    )]);
    let p = project_branch(&entries, &outputs, &summaries, None).unwrap();
    assert!(
        p.positions
            .iter()
            .find(|p| p.origin == ProjectionOrigin::Stored(EntryId::new("err").unwrap()))
            .unwrap()
            .message
            .is_none()
    );
    assert!(p.positions.last().unwrap().message.is_some());
}
#[test]
fn thresholds_usage_arithmetic_and_timestamp_independence() {
    let settings = CompactionSettings {
        enabled: true,
        reserve_tokens: n(10),
        keep_recent_tokens: n(1),
    };
    assert!(!should_compact(n(10), n(20), &settings));
    assert!(should_compact(n(11), n(20), &settings));
    assert!(should_compact(n(0), n(9), &settings));
    let mut disabled = settings.clone();
    disabled.enabled = false;
    assert!(!should_compact(n(100), n(9), &disabled));
    let mut a = response("a").into_message();
    a.timestamp = n(1);
    a.usage.cache_write_1h = Some(n(0));
    let before: AgentMessage =
        serde_json::from_value(json!({"role":"user","content":"longer","timestamp":100})).unwrap();
    let e = estimate_context(&[
        before,
        AgentMessage::BuiltIn(Message::Assistant(Box::new(a.clone()))),
    ])
    .unwrap();
    assert_eq!(e.tokens, n(5));
    assert_eq!(e.last_usage_index, Some(n(1)));
    let sum = combine_usage(&a.usage, &a.usage).unwrap();
    assert_eq!(sum.total_tokens, n(10));
    assert_eq!(sum.cache_write_1h, Some(n(0)));
    assert_eq!(sum.reasoning, None);
    a.usage.input = SafeUnsigned::MAX;
    assert_eq!(
        combine_usage(&a.usage, &a.usage),
        Err(ContentError::NumberRange)
    );
}
#[test]
fn utf16_slice_removes_only_cut_half_and_preserves_original_count() {
    let message:Message=serde_json::from_value(json!({"role":"toolResult","toolCallId":"c","toolName":"read","content":[{"type":"text","text":format!("{}😀end","x".repeat(1999))}],"isError":false,"timestamp":1})).unwrap();
    let transcript = serialize_conversation_text(&[message]).unwrap();
    assert!(!transcript.text.contains('😀'));
    assert!(!transcript.text.contains('\u{fffd}'));
    assert!(
        transcript
            .text
            .ends_with("[... 4 more characters truncated]")
    );
    assert_eq!(
        transcript.logical_utf16_units.get(),
        transcript.text.encode_utf16().count() as u64 + 1
    );
}

#[test]
fn numeric_key_order_and_file_list_order_are_js_compatible() {
    assert_eq!(
        js_json(&json!({"a":{"z":1},"z":2,"10":10,"2":2})),
        r#"{"2":2,"10":10,"a":{"z":1},"z":2}"#
    );
    let ops =
        DurableFileOperations::new(vec!["\u{e000}".into(), "😀".into()], vec![], vec![]).unwrap();
    assert_eq!(file_lists(&ops).0, vec!["😀", "\u{e000}"]);
}

#[test]
fn failed_receipts_do_not_become_generated_results_or_drop_optional_usage() {
    let g = golden();
    let prep: CompactionPreparation =
        serde_json::from_value(g["observations"][0]["preparation"].clone()).unwrap();
    for stop in [
        pi_ai_core::StopReason::Error,
        pi_ai_core::StopReason::Aborted,
    ] {
        let mut raw = response("partial").into_message();
        raw.stop_reason = stop;
        raw.error_message = Some("reported failure".into());
        let receipt = SettledAssistantMessage::new(raw).unwrap();
        assert_eq!(
            finish_compaction(&prep, std::slice::from_ref(&receipt)),
            Err(ContentError::ProviderResponse)
        );
        assert_eq!(receipt.as_message().usage.total_tokens, n(5));
    }
    let ops = DurableFileOperations::new(
        vec!["both".into(), "read-only".into()],
        vec!["both".into()],
        vec!["edited".into()],
    )
    .unwrap();
    assert_eq!(
        file_lists(&ops),
        (
            vec!["read-only".into()],
            vec!["both".into(), "edited".into()]
        )
    );
    let mut first = Usage::zero();
    first.reasoning = Some(n(3));
    let second = Usage::zero();
    assert_eq!(
        combine_usage(&first, &second).unwrap().reasoning,
        Some(n(3))
    );
}

#[test]
fn complete_source_sanitizer_boundary_goldens_keep_raw_length() {
    let g = golden();
    for case in g["scalarCases"].as_array().unwrap() {
        let message: Message = serde_json::from_value(case["message"].clone()).unwrap();
        let text = serialize_conversation_text(&[message]).unwrap();
        assert_eq!(text.text, case["clean"]);
        assert_eq!(
            text.logical_utf16_units.get(),
            case["logicalUnits"].as_u64().unwrap()
        );
    }
}

#[test]
fn full_pinned_generator_prompt_keeps_pre_sanitization_budget_observation() {
    let g = golden();
    let u = &g["unicodeRequest"];
    let prep: CompactionPreparation = serde_json::from_value(u["preparation"].clone()).unwrap();
    let converted: Vec<Message> = serde_json::from_value(u["convertedHistory"].clone()).unwrap();
    let spec = compaction_requests(
        &prep,
        &converted,
        &[],
        &model(),
        u["focus"].as_str(),
        AgentThinkingLevel::Off,
    )
    .unwrap()
    .remove(0);
    assert_eq!(
        spec.prompt,
        u["context"]["messages"][0]["content"][0]["text"]
    );
    assert_eq!(
        spec.prompt_utf16_units.get(),
        u["logicalUnits"].as_u64().unwrap()
    );
    assert_eq!(
        spec.prompt_utf16_units.get(),
        spec.prompt.encode_utf16().count() as u64 + 1
    );
    assert_eq!(
        u["rawEstimate"].as_u64().unwrap(),
        u["cleanEstimate"].as_u64().unwrap() + 1
    );
    assert_eq!(u["rawCap"], 90);
    assert_eq!(u["cleanCap"], 91);
}
