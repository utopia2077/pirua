//! A lane queue update must not consume a summary attempt before its request starts.
mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::{current, factory, user};

#[test]
fn next_run_during_nested_intent_replans_same_request() {
    let s = Setup::new();
    let h = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Manual)))
        .unwrap()
        .harness;
    s.pump
        .drive(h.set_retry_policy(RetryPolicy {
            enabled: false,
            max_retries: pi_ai_core::MaxRetries::new(0).unwrap(),
            base_delay_ms: n(0),
        }))
        .unwrap();
    s.pump
        .drive(
            h.session()
                .append_message(user("source for one summary request")),
        )
        .unwrap();
    let mut call = Box::pin(h.compact(CompactOptions::default()));
    assert!(poll(call.as_mut()).is_pending());
    for _ in 0..500 {
        s.pump.tick();
        let pending_without_request = matches!(
            current(&s),
            Some(OperationState::Compaction(CompactionState {
                structural: StructuralDecision::Generating { generation, .. },
                ..
            })) if matches!(generation.as_ref(), SummaryGeneration::EffectPending {
                request: None,
                usage_ids,
                ..
            } if usage_ids.is_empty())
        );
        let action = s.pump.drive(h.peek_action()).unwrap();
        if pending_without_request
            && action
                .as_ref()
                .is_some_and(|a| a.kind == "commit_transition")
        {
            break;
        }
        if action.is_some() {
            s.pump.drive(h.execute_action()).unwrap();
        }
    }
    assert!(matches!(
        current(&s),
        Some(OperationState::Compaction(CompactionState {
            structural: StructuralDecision::Generating { generation, .. },
            ..
        })) if matches!(generation.as_ref(), SummaryGeneration::EffectPending {
            request: None,
            usage_ids,
            ..
        } if usage_ids.is_empty())
    ));
    assert_eq!(
        s.pump.drive(h.peek_action()).unwrap().unwrap().kind,
        "commit_transition"
    );
    assert!(s.faux.requests.lock().unwrap().is_empty());
    let queued = s
        .pump
        .drive(h.next_run(user("queued concurrently")))
        .unwrap()
        .unwrap();
    s.pump.drive(h.run_to_completion()).unwrap();
    let result = s.pump.drive(call).unwrap().unwrap();
    assert!(
        matches!(result.outcome, CompactionOutcome::Completed(_)),
        "harmless lane sequence change must not burn the only attempt: {result:?}"
    );
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert_eq!(
        s.pump.drive(h.watch()).unwrap().snapshot().queues.next_run[0].entry_id,
        queued.entry_id
    );
}
