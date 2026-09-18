use pi_agent_schema::*;
use pi_ai_core::SafeUnsigned;
use serde_json::json;
#[test]
fn suspension_receipt_requires_nonempty_missing_and_matching_summary_identity() {
    let context:SummaryContext=serde_json::from_value(json!({"taskId":"task","resultEntryId":"result","kind":"compaction","configuration":{"model":{"provider":"p","modelId":"m"},"thinkingLevel":"off","activeToolNames":[]},"streamOptions":{},"retryPolicy":{"maxAttempts":3,"baseDelayMs":0},"reason":"manual"})).unwrap();
    let p = SummaryEffectPlan {
        key: EffectKey {
            session_id: SessionId::new("s").unwrap(),
            lane: LaneId::new("main").unwrap(),
            operation_id: OperationId::new("op").unwrap(),
            kind: EffectKind::Summary,
            step_or_task_id: EffectStepId::new("task").unwrap(),
            logical_attempt: SafeUnsigned::new(1).unwrap(),
            item_index: None,
        },
        generation: PendingSummaryGeneration {
            context,
            attempt: OneBasedSafeUnsigned::new(1).unwrap(),
            request: None,
            usage_ids: vec![],
        },
        telemetry_context: (),
    };
    let missing = MissingIdentities {
        tools: vec![],
        models: vec!["p/m".into()],
    };
    let o = SummarySuspendedOutput::new(&p, missing.clone()).unwrap();
    assert_eq!(o.key(), &p.key);
    assert_eq!(o.missing(), &missing);
    let _: EffectOutput = EffectOutput::SummarySuspended(o.clone());
    let _: SettlementOutput = SettlementOutput::SummarySuspended(o);
    assert!(
        SummarySuspendedOutput::new(
            &p,
            MissingIdentities {
                tools: vec![],
                models: vec![]
            }
        )
        .is_none()
    );
    for axis in 0..4 {
        let mut changed = p.clone();
        match axis {
            0 => changed.key.kind = EffectKind::Assistant,
            1 => changed.key.step_or_task_id = EffectStepId::new("other").unwrap(),
            2 => changed.key.logical_attempt = SafeUnsigned::new(2).unwrap(),
            _ => changed.key.item_index = Some(SafeUnsigned::new(0).unwrap()),
        };
        assert!(SummarySuspendedOutput::new(&changed, missing.clone()).is_none());
    }
}
