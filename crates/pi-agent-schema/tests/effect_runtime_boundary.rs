//! Public ABI witnesses only: this fixture has no runtime, storage, or effect executor.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use pi_agent_schema::*;
use pi_ai_core::SafeUnsigned;
use serde::de::DeserializeOwned;
use serde_json::json;

fn number(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).expect("safe witness number")
}

fn key(attempt: SafeUnsigned, index: Option<SafeUnsigned>) -> EffectKey {
    EffectKey {
        session_id: SessionId::new("session").unwrap(),
        lane: LaneId::new("main").unwrap(),
        operation_id: OperationId::new("operation").unwrap(),
        kind: EffectKind::Tool,
        step_or_task_id: EffectStepId::new("step").unwrap(),
        logical_attempt: attempt,
        item_index: index,
    }
}

#[test]
fn effect_keys_preserve_the_complete_safe_number_domain() {
    let above_u32 = u64::from(u32::MAX) + 1;
    let attempt = OneBasedSafeUnsigned::new(above_u32).unwrap();
    let first = key(number(attempt.get()), None);
    let second = key(number(above_u32 + 1), None);
    let maximum = key(SafeUnsigned::MAX, Some(SafeUnsigned::MAX));
    let zero_index = key(SafeUnsigned::MAX, Some(number(0)));
    let no_index = key(SafeUnsigned::MAX, None);
    assert_eq!(first.logical_attempt.get(), above_u32);
    assert_ne!(first, second);
    assert!(first < second);
    assert_ne!(maximum, zero_index);
    assert_ne!(zero_index, no_index);
    assert_eq!(
        maximum.logical_attempt.get(),
        OneBasedSafeUnsigned::MAX.get()
    );
    assert_eq!(maximum.item_index.unwrap().get(), SafeUnsigned::MAX.get());
    let ordered = BTreeMap::from([(first.clone(), "first"), (second.clone(), "second")]);
    assert_eq!(ordered.get(&first), Some(&"first"));
    assert_eq!(ordered.get(&second), Some(&"second"));
    assert_eq!(ordered.keys().collect::<Vec<_>>(), vec![&first, &second]);
    let hashed = HashMap::from([(maximum.clone(), "maximum"), (zero_index.clone(), "zero")]);
    assert_eq!(hashed.get(&maximum), Some(&"maximum"));
    assert_eq!(hashed.get(&zero_index), Some(&"zero"));
    assert_eq!(key(number(0), None).logical_attempt.get(), 0);
}

// Deliberately no Serde, Debug or equality bound on the telemetry context.
#[derive(Clone)]
struct OpaqueContext(Arc<()>);

fn telemetry() -> OpaqueContext {
    OpaqueContext(Arc::new(()))
}

fn decode<T: DeserializeOwned>(value: serde_json::Value) -> T {
    serde_json::from_value(value).expect("typed ABI input")
}

fn current() -> CurrentOperation {
    CurrentOperation {
        operation: decode(json!({"operationId":"operation","lane":"main",
            "sourceLeafId":"entry","startedAt":0,"intent":{"kind":"compaction"}})),
        state: decode(json!({"kind":"compaction","control":{"status":"running"},
            "structural":{"status":"deciding","taskId":"task"}})),
        operation_state_seq: number(2),
        lane_state: LaneState {
            current_operation_id: Some(OperationId::new("operation").unwrap()),
            pending_next_run: vec![],
        },
        lane_state_seq: number(3),
        leaf_id: Some(EntryId::new("entry").unwrap()),
        configuration: decode(json!({"model":{"provider":"provider","modelId":"model"},
            "thinkingLevel":"off","activeToolNames":[]})),
        configuration_seq: number(1),
    }
}

fn tool_plan() -> ToolEffectPlan<OpaqueContext> {
    ToolEffectPlan {
        key: key(number(0), Some(number(0))),
        assistant_entry_id: EntryId::new("assistant").unwrap(),
        source_index: number(0),
        args_key: "operation:step:0".to_owned(),
        telemetry_context: telemetry(),
    }
}

fn raw_tool() -> ToolRawOutput {
    ToolRawOutput {
        key: tool_plan().key,
        result: decode(json!({"content":[],"details":null})),
        is_error: true,
    }
}

fn transition() -> TransitionCommit<OpaqueContext> {
    let current = current();
    TransitionCommit {
        next: current.state.clone(),
        current,
        telemetry_context: telemetry(),
        expected_configuration_seq: None,
        expected_settings_revision: None,
    }
}

fn settlement() -> SettlementCommit<OpaqueContext> {
    let plan = tool_plan();
    SettlementCommit {
        current: current(),
        output: SettlementOutput::NotStarted {
            key: plan.key.clone(),
        },
        plan: EffectPlan::Tool(plan),
        telemetry_context: telemetry(),
    }
}

fn terminal() -> TerminalCommit<OpaqueContext> {
    TerminalCommit {
        current: current(),
        result: OperationResult::Compaction(CompactionOutcome::Aborted(CompactionAborted {
            kind: AbortedPublicOutcome::Aborted,
            leaf_id: EntryId::new("entry").unwrap(),
        })),
        telemetry_context: telemetry(),
    }
}

fn summary_request() -> SummaryRequestPlan<OpaqueContext> {
    SummaryRequestPlan {
        task_id: TaskId::new("task").unwrap(),
        attempt: OneBasedSafeUnsigned::new(1).unwrap(),
        request_index: number(0),
        usage_id: UsageId::new("usage").unwrap(),
        configuration: current().configuration,
        messages: vec![],
        telemetry_context: telemetry(),
    }
}

fn summary_settlement() -> SummaryRequestSettlement<OpaqueContext> {
    SummaryRequestSettlement {
        current: current(),
        task_id: TaskId::new("task").unwrap(),
        attempt: OneBasedSafeUnsigned::new(1).unwrap(),
        request_index: number(0),
        usage_id: UsageId::new("usage").unwrap(),
        response: decode(json!({"role":"assistant","content":[],"api":"api",
            "provider":"provider","model":"model","usage":{"input":0,"output":0,
            "cacheRead":0,"cacheWrite":0,"totalTokens":0,
            "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},
            "stopReason":"stop","timestamp":0})),
        telemetry_context: telemetry(),
    }
}

#[derive(Clone, Copy)]
enum Reply {
    Closed,
    Fault,
    NoCurrent,
    Value,
}

// This object supplies canned values to exercise the public dynamic return ABI.
// It does not model admission, settlement, cancellation, or any Harness behavior.
struct BoundaryWitness(Reply);

impl BoundaryWitness {
    fn reply<T: Send + 'static>(&self, value: T) -> BoxFuture<'_, Result<T, HarnessRuntimeError>> {
        Box::pin(std::future::ready(match self.0 {
            Reply::Closed => Err(HarnessRuntimeError::Closed(HarnessClosed)),
            Reply::Fault => Err(HarnessRuntimeError::Fault(HarnessFault::new(
                ErrorContext::new("ABI fault witness").unwrap(),
            ))),
            Reply::NoCurrent | Reply::Value => Ok(value),
        }))
    }

    fn conditional<T>(&self, value: T) -> Option<T> {
        if matches!(self.0, Reply::NoCurrent) {
            None
        } else {
            Some(value)
        }
    }
}

impl Effects<OpaqueContext> for BoundaryWitness {
    fn commit_transition(
        &self,
        plan: TransitionCommit<OpaqueContext>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>> {
        self.reply(self.conditional(plan.current))
    }
    fn commit_settlement(
        &self,
        plan: SettlementCommit<OpaqueContext>,
    ) -> BoxFuture<'_, Result<Option<SettlementResult<OpaqueContext>>, HarnessRuntimeError>> {
        self.reply(self.conditional(SettlementResult {
            current: plan.current,
            dispatch: None,
            suspend: None,
            consume_deferred_poll: false,
        }))
    }
    fn commit_terminal(
        &self,
        plan: TerminalCommit<OpaqueContext>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>> {
        self.reply(self.conditional(plan.current))
    }
    fn run(
        &self,
        plan: EffectPlan<OpaqueContext>,
    ) -> BoxFuture<'_, Result<EffectOutput, HarnessRuntimeError>> {
        let mut output = raw_tool();
        output.key = plan.key().clone();
        self.reply(EffectOutput::ToolRaw(output))
    }
    fn finalize_tool(
        &self,
        _plan: ToolEffectPlan<OpaqueContext>,
        output: ToolRawOutput,
    ) -> BoxFuture<'_, Result<ToolSettlementOutput, HarnessRuntimeError>> {
        self.reply(ToolSettlementOutput {
            key: output.key,
            result: output.result,
            is_error: output.is_error,
            terminate: false,
        })
    }
    fn run_summary_request(
        &self,
        _plan: SummaryRequestPlan<OpaqueContext>,
    ) -> BoxFuture<'_, Result<SummaryRequestOutput, HarnessRuntimeError>> {
        self.reply(SummaryRequestOutput::NotStarted)
    }
    fn settle_summary_request(
        &self,
        plan: SummaryRequestSettlement<OpaqueContext>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>> {
        self.reply(self.conditional(plan.current))
    }
    fn sleep(
        &self,
        _delay: Duration,
        context: OpaqueContext,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        // A process-local owned context remains usable without extra trait bounds.
        let _owner = context.0;
        self.reply(())
    }
}

// All fixture futures are ready. This is one poll, not a task executor.
fn ready<T>(future: impl Future<Output = T>) -> T {
    match std::pin::pin!(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("ABI witness unexpectedly parked"),
    }
}

#[test]
fn dynamic_effects_expose_closed_and_fault_on_all_eight_methods() {
    for reply in [Reply::Closed, Reply::Fault] {
        let witness = BoundaryWitness(reply);
        let fx: &dyn Effects<OpaqueContext> = &witness;
        let results = [
            (
                "commit_transition",
                ready(fx.commit_transition(transition())).map(|_| ()),
            ),
            (
                "commit_settlement",
                ready(fx.commit_settlement(settlement())).map(|_| ()),
            ),
            (
                "commit_terminal",
                ready(fx.commit_terminal(terminal())).map(|_| ()),
            ),
            (
                "run",
                ready(fx.run(EffectPlan::Tool(tool_plan()))).map(|_| ()),
            ),
            (
                "finalize_tool",
                ready(fx.finalize_tool(tool_plan(), raw_tool())).map(|_| ()),
            ),
            (
                "run_summary_request",
                ready(fx.run_summary_request(summary_request())).map(|_| ()),
            ),
            (
                "settle_summary_request",
                ready(fx.settle_summary_request(summary_settlement())).map(|_| ()),
            ),
            ("sleep", ready(fx.sleep(Duration::ZERO, telemetry()))),
        ];
        assert_eq!(results.each_ref().map(|(name, _)| *name), EFFECTS_METHODS);
        for (name, result) in results {
            assert!(
                matches!(
                    (reply, result),
                    (Reply::Closed, Err(HarnessRuntimeError::Closed(_)))
                        | (Reply::Fault, Err(HarnessRuntimeError::Fault(_)))
                ),
                "{name}"
            );
        }
    }
}

#[test]
fn dynamic_conditional_absence_is_distinct_from_successful_current() {
    for reply in [Reply::NoCurrent, Reply::Value] {
        let witness = BoundaryWitness(reply);
        let fx: &dyn Effects<OpaqueContext> = &witness;
        let expected = matches!(reply, Reply::Value).then(current);
        assert_eq!(ready(fx.commit_transition(transition())).unwrap(), expected);
        assert_eq!(ready(fx.commit_terminal(terminal())).unwrap(), expected);
        assert_eq!(
            ready(fx.settle_summary_request(summary_settlement())).unwrap(),
            expected
        );
        let output = ready(fx.commit_settlement(settlement())).unwrap();
        match (output, expected) {
            (None, None) => (),
            (Some(output), Some(current)) => {
                assert_eq!(output.current, current);
                assert!(output.dispatch.is_none());
                assert!(output.suspend.is_none());
                assert!(!output.consume_deferred_poll);
            }
            _ => panic!("conditional absence and successful current were conflated"),
        }
    }
}

#[test]
fn dynamic_in_band_error_output_is_not_a_lifecycle_rejection() {
    let fx: &dyn Effects<OpaqueContext> = &BoundaryWitness(Reply::Value);
    let output = ready(fx.run(EffectPlan::Tool(tool_plan()))).unwrap();
    let EffectOutput::ToolRaw(raw) = output else {
        panic!("expected ordinary tool failure")
    };
    assert_eq!(raw.key, tool_plan().key);
    assert!(raw.is_error);
    let finalized = ready(fx.finalize_tool(tool_plan(), raw)).unwrap();
    assert!(finalized.is_error);
    assert!(!finalized.terminate);
    assert!(matches!(
        ready(fx.run_summary_request(summary_request())).unwrap(),
        SummaryRequestOutput::NotStarted
    ));
    assert!(ready(fx.sleep(Duration::ZERO, telemetry())).is_ok());
}
