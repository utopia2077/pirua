use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use serde_json::json;
fn plan() -> EffectPlan<()> {
    let deferred:Deferred=serde_json::from_value(json!({"status":"effect_pending","stepId":"step","sourceEntryId":"source","poll":3,"responseEntryId":"r","usageId":"u","configuration":{"model":{"provider":"p","modelId":"m"},"thinkingLevel":"off","activeToolNames":[]},"streamOptions":{}})).unwrap();
    EffectPlan::Deferred(DeferredEffectPlan {
        key: EffectKey {
            session_id: SessionId::new("s").unwrap(),
            lane: LaneId::new("main").unwrap(),
            operation_id: OperationId::new("o").unwrap(),
            kind: EffectKind::Deferred,
            step_or_task_id: EffectStepId::new("step").unwrap(),
            logical_attempt: SafeUnsigned::new(3).unwrap(),
            item_index: None,
        },
        deferred: PendingDeferred::try_from(deferred).unwrap(),
        stream_options: Default::default(),
        telemetry_context: (),
    })
}
fn value() -> ReconciledValue {
    ReconciledValue::Assistant(Box::new(serde_json::from_value(json!({"role":"assistant","content":[],"api":"unknown","provider":"p","model":"m","usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"totalTokens":0,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"aborted","errorMessage":"Operation cancelled","timestamp":1})).unwrap()))
}
#[test]
fn deferred_reconciliation_accepts_only_cancelled_matching_projection() {
    let p = plan();
    let o = ReconciledOutput::new(&p, ReconcileReason::CancelledNotStarted, value()).unwrap();
    assert_eq!(o.key(), p.key());
    for reason in [
        ReconcileReason::UnknownAtCap,
        ReconcileReason::InterruptedTool,
    ] {
        assert!(ReconciledOutput::new(&p, reason, value()).is_err());
    }
    for mutation in 0..4 {
        let mut p = plan();
        let EffectPlan::Deferred(d) = &mut p else {
            panic!()
        };
        match mutation {
            0 => d.key.logical_attempt = SafeUnsigned::new(2).unwrap(),
            1 => d.key.kind = EffectKind::Assistant,
            2 => d.key.item_index = Some(SafeUnsigned::new(0).unwrap()),
            _ => d.key.step_or_task_id = EffectStepId::new("wrong").unwrap(),
        };
        assert!(ReconciledOutput::new(&p, ReconcileReason::CancelledNotStarted, value()).is_err());
    }
    let tool = ReconciledValue::Tool {
        result: AgentToolResult {
            content: vec![],
            details: JsonValue::Null,
            usage: None,
            added_tool_names: None,
            terminate: None,
        },
        is_error: true,
        terminate: false,
    };
    assert!(ReconciledOutput::new(&p, ReconcileReason::CancelledNotStarted, tool).is_err());
}
