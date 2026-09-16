use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use std::collections::BTreeMap;
fn n(v: u64) -> SafeUnsigned {
    SafeUnsigned::new(v).unwrap()
}
fn key(kind: EffectKind, index: Option<SafeUnsigned>) -> EffectKey {
    EffectKey {
        session_id: SessionId::new("session").unwrap(),
        lane: LaneId::new("main").unwrap(),
        operation_id: OperationId::new("op").unwrap(),
        kind,
        step_or_task_id: EffectStepId::new("step").unwrap(),
        logical_attempt: n(if index.is_some() { 0 } else { 1 }),
        item_index: index,
    }
}
fn request() -> HookEffectPlan<&'static str> {
    HookEffectPlan{key:key(EffectKind::Hook,None),event:HookEvent::BeforeRequest(Box::new(serde_json::from_value(serde_json::json!({"lane":"main","runId":"op","step":"assistant","attempt":1,"streamOptions":{},"model":{"id":"model","name":"test","api":"api","provider":"provider","baseUrl":"https://example.invalid","reasoning":false,"input":["text"],"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":100,"maxTokens":20}})).unwrap())),telemetry_context:"explicit context"}
}
#[test]
fn complete_effective_options_preserve_header_recasing_and_all_fields() {
    let plan = request();
    let policy = ProviderHeaderPolicy::from_allowlist(vec!["x-a".into()]);
    let options = AgentHarnessStreamOptions {
        headers: Some(
            RawProviderHeaders::new(BTreeMap::from([("x-a".into(), "new".into())]))
                .unwrap()
                .classify(&policy)
                .unwrap(),
        ),
        timeout_ms: Some(n(42)),
        ..Default::default()
    };
    let output = RequestOptionsOutput::new(&plan, options.clone()).unwrap();
    assert_eq!(output.options(), &options);
    assert_eq!(output.key(), &plan.key);
    assert!(matches!(
        EffectOutput::RequestOptions(output.clone()),
        EffectOutput::RequestOptions(_)
    ));
    assert!(matches!(
        SettlementOutput::RequestOptions(output),
        SettlementOutput::RequestOptions(_)
    ));
    let mut wrong = plan.clone();
    wrong.key.logical_attempt = n(2);
    assert!(RequestOptionsOutput::new(&wrong, options).is_err());
    let mut wrong = plan;
    wrong.event = HookEvent::BeforeRunEnd(BeforeRunEndEvent {
        lane: LaneId::new("main").unwrap(),
        run_id: OperationId::new("op").unwrap(),
        messages: vec![],
    });
    assert!(RequestOptionsOutput::new(&wrong, Default::default()).is_err());
}
#[test]
fn clearance_receipt_carries_args_replay_and_all_original_index_bits() {
    let h = request();
    let HookEvent::BeforeRequest(event) = h.event else {
        panic!()
    };
    let index = n(4_294_967_301);
    let plan = ToolClearancePlan {
        key: key(EffectKind::Hook, Some(index)),
        assistant_entry_id: EntryId::new("assistant").unwrap(),
        source_index: index,
        turn_id: StepId::new("step").unwrap(),
        result_entry_id: EntryId::new("result").unwrap(),
        producing_configuration: LaneConfiguration {
            model: LaneModel {
                provider: event.model.provider,
                model_id: event.model.id,
            },
            thinking_level: AgentThinkingLevel::Off,
            active_tool_names: vec![],
        },
        telemetry_context: "explicit context",
    };
    let outcome = ToolClearanceOutcome::Prepared {
        effective_args: JsonObject::new(BTreeMap::from([(
            "changed".into(),
            JsonValue::Bool(true),
        )])),
        replay: ToolReplay::Safe,
    };
    let output = ToolClearanceOutput::new(&plan, outcome.clone()).unwrap();
    assert_eq!(output.source_index(), index);
    assert_eq!(output.outcome(), &outcome);
    let mut wrong = plan.clone();
    wrong.key.item_index = Some(n(5));
    assert!(ToolClearanceOutput::new(&wrong, outcome).is_err());
    let result = AgentToolResult {
        content: vec![],
        details: JsonValue::Null,
        usage: None,
        added_tool_names: None,
        terminate: None,
    };
    let value = ReconciledValue::Tool {
        result,
        is_error: true,
        terminate: false,
    };
    assert!(
        ReconciledOutput::new(
            &EffectPlan::ToolClearance(plan.clone()),
            ReconcileReason::InterruptedTool,
            value.clone()
        )
        .is_ok()
    );
    assert!(
        ReconciledOutput::new(
            &EffectPlan::ToolClearance(plan),
            ReconcileReason::UnknownAtCap,
            value
        )
        .is_err()
    );
    assert_eq!(EFFECTS_METHODS.len(), 8);
}
#[test]
fn pending_placement_constructor_proves_payload_and_retains_token() {
    let message = serde_json::json!({"role":"user","content":"queued","timestamp":1});
    let entry: NewEntry = serde_json::from_value(
        serde_json::json!({"id":"queued","parentId":null,"type":"message","message":message}),
    )
    .unwrap();
    let pending: PendingEntry =
        serde_json::from_value(serde_json::json!({"type":"message","payload":message})).unwrap();
    let placement = PendingPlacement::new(entry.clone(), pending.clone(), n(19)).unwrap();
    assert_eq!(placement.entry(), &entry);
    assert_eq!(placement.pending(), &pending);
    assert_eq!(placement.seq(), n(19));
    let mut wrong = pending;
    wrong.payload = Presence::Null;
    assert!(PendingPlacement::new(entry, wrong, n(19)).is_err());
    let entry:NewEntry=serde_json::from_value(serde_json::json!({"id":"custom","parentId":null,"type":"custom","customType":"note","data":null})).unwrap();
    let pending: PendingEntry = serde_json::from_value(
        serde_json::json!({"type":"custom","customType":"note","payload":null}),
    )
    .unwrap();
    assert!(PendingPlacement::new(entry.clone(), pending.clone(), n(20)).is_ok());
    let mut missing = pending;
    missing.payload = Presence::Missing;
    assert!(PendingPlacement::new(entry, missing, n(20)).is_err());
}
