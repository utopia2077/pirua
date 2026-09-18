//! R9 poll classification uses the actual captured pre-clamp Model limit.
mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use pi_ai_core::{CancellationToken, ProviderAuth, StopReason};
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::{DeferredAdapter, factory, prompt, tick_until};
use std::sync::Arc;
#[test]
fn deferred_length_zero_below_equal_above_preserves_usage_and_never_compacts() {
    for output in [0, 1, 31_999, 32_000, 32_001] {
        run_length(output, None);
    }
}
#[test]
fn deferred_length_uses_captured_limit_after_registry_model_changes() {
    run_length(16_000, Some(8_000));
    run_length(32_000, Some(64_000));
}
fn run_length(output: u64, replacement_limit: Option<u64>) {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    assert_eq!(adapter.simple.model.max_tokens, n(32_000));
    let mut response = adapter.response(StopReason::Length);
    response.usage.output = n(output);
    response.usage.total_tokens = n(response.usage.input.get() + output);
    adapter.replies.lock().unwrap().push_back(response);
    let h = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap()
        .harness;
    s.pump.drive(h.prompt(prompt())).unwrap().unwrap();
    let hold = CancellationToken::new();
    if replacement_limit.is_some() {
        *adapter.hold_fetch.lock().unwrap() = Some(hold.clone());
    }
    let mut call = Box::pin(h.resume());
    assert!(poll(call.as_mut()).is_pending());
    tick_until(&s, || adapter.polls.lock().unwrap().len() == 1);
    if let Some(limit) = replacement_limit {
        let mut model = s.faux.model.clone();
        model.max_tokens = n(limit);
        let replacement = Arc::new(Faux::new(model));
        s.pump
            .drive(s.registry.set_provider(Arc::new(Product {
                faux: replacement.clone(),
                auth: ProviderAuth::new(Some(Arc::new(Auth)), None).unwrap(),
            })))
            .unwrap();
        hold.cancel();
        assert!(replacement.requests.lock().unwrap().is_empty());
    }
    let result = s.pump.drive(call).unwrap().unwrap();
    let failed = output < 32_000;
    assert_eq!(
        matches!(result, ResumeOutcome::RunFailedWithAssistant(_)),
        failed,
        "output {output} replacement {replacement_limit:?}"
    );
    let snapshot = s.pump.drive(s.store.snapshot()).unwrap();
    assert_eq!(adapter.polls.lock().unwrap().len(), 1);
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert_eq!(snapshot.usage.len(), 2);
    let usage = snapshot.usage.last().unwrap();
    assert_eq!(usage.usage.output, n(output));
    assert_eq!(usage.usage.total_tokens, n(output + 9));
    assert!(
        !snapshot
            .entries
            .iter()
            .any(|e| matches!(e, Entry::Compaction(_)))
    );
    let last = snapshot.entries.last().unwrap();
    let Entry::Message(entry) = last else {
        panic!("poll response absent")
    };
    let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(response)) = &entry.message else {
        panic!("poll response kind")
    };
    assert_eq!(
        response.stop_reason,
        if failed {
            StopReason::Error
        } else {
            StopReason::Length
        }
    );
    assert_eq!(response.usage.output, n(output));
    assert!(
        matches!(
            s.pump.drive(h.get_last_result()).unwrap(),
            Some(LaneLastResult::RunFailed(_))
        ) == failed
    );
    assert!(s.pump.drive(h.peek_action()).unwrap().is_none());
}
