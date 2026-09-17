//! Explicit process-local span ownership; no durable identities or ambient context.
use crate::owner::Telemetry;
use pi_agent_schema::*;
use pi_telemetry::{
    AttributeValue, FiniteTelemetryNumber, SpanAttributes, SpanGuard, SpanOptions, SpanStatus,
    TelemetryContext,
};
use std::sync::{Arc, Mutex};

pub(crate) fn string(value: impl Into<String>) -> AttributeValue {
    AttributeValue::String(value.into())
}
pub(crate) fn number(value: u64) -> AttributeValue {
    AttributeValue::Number(FiniteTelemetryNumber::new(value as f64).expect("finite counter"))
}
pub(crate) struct Span {
    guard: SpanGuard,
    // A delayed child keeps its explicit parent alive until the child finishes.
    _parent: Telemetry,
}
impl TelemetryContext for Span {
    fn start_span(&self, options: SpanOptions) -> SpanGuard {
        self.guard.start_child(options)
    }
}
impl Span {
    pub(crate) fn start(parent: Telemetry, name: &str, attributes: SpanAttributes) -> Arc<Self> {
        let guard = parent.start_span(SpanOptions {
            name: name.into(),
            attributes,
        });
        guard.set_status(SpanStatus::Error { error: None });
        Arc::new(Self {
            guard,
            _parent: parent,
        })
    }
    pub(crate) fn end(&self, attrs: SpanAttributes, failed: bool) {
        self.guard.set_attributes(&attrs);
        self.guard.set_status(if failed {
            SpanStatus::Error { error: None }
        } else {
            SpanStatus::Ok
        });
    }
    pub(crate) fn outcome(&self, key: &str, value: &str, failed: bool) {
        self.end(SpanAttributes::from([(key.into(), string(value))]), failed);
    }
}
#[derive(Clone, PartialEq)]
enum Phase {
    Checkpoint(String, &'static str),
    Turn(String),
}
struct State {
    run: Option<Arc<Span>>,
    phase: Option<(Phase, Arc<Span>)>,
    step: Option<((String, u64), Arc<Span>)>,
}
pub(crate) struct Trace {
    root: Telemetry,
    state: Mutex<State>,
}
impl Trace {
    pub(crate) fn new(root: Telemetry) -> Self {
        Self {
            root,
            state: Mutex::new(State {
                run: None,
                phase: None,
                step: None,
            }),
        }
    }
    pub(crate) fn begin(
        &self,
        session: &SessionId,
        lane: &LaneId,
        id: &OperationId,
        recovery: bool,
    ) {
        let mut attrs = correlation(lane, id);
        attrs.insert("pi.session.id".into(), string(session.as_str()));
        attrs.insert("pi.operation.kind".into(), string("run"));
        attrs.insert(
            "pi.operation.recovery".into(),
            AttributeValue::Boolean(recovery),
        );
        let run = Span::start(self.root.clone(), "pi.harness.run", attrs);
        run.outcome("pi.operation.outcome", "failed", true);
        let old = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .run
            .replace(run);
        drop(old);
    }
    pub(crate) fn context(&self) -> Telemetry {
        let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        s.step
            .as_ref()
            .map(|(_, s)| s.clone() as Telemetry)
            .or_else(|| s.phase.as_ref().map(|(_, s)| s.clone() as Telemetry))
            .or_else(|| s.run.as_ref().map(|s| s.clone() as Telemetry))
            .unwrap_or_else(|| self.root.clone())
    }
    pub(crate) fn sync(&self, current: &CurrentOperation) {
        let OperationState::Run(run) = &current.state else {
            return;
        };
        let (phase, step) = match &run.phase {
            RunPhase::Checkpoint(c) => (
                Phase::Checkpoint(
                    c.trigger_entry_id.as_str().into(),
                    if matches!(run.control, Control::CancelRequested(_)) {
                        "abort_reconcile"
                    } else {
                        "normal"
                    },
                ),
                None,
            ),
            RunPhase::Assistant(a) => {
                let (context, attempt) = match &a.generation {
                    Generation::Ready {
                        context,
                        next_attempt,
                    } => (context, Some(next_attempt.get())),
                    Generation::EffectPending {
                        context, attempt, ..
                    } => (context, Some(attempt.get())),
                    Generation::RetryWait { context, .. } => (context, None),
                };
                (
                    Phase::Turn(context.step_id.as_str().into()),
                    attempt.map(|a| (context.step_id.as_str().into(), a)),
                )
            }
            RunPhase::Tools(t) => (Phase::Turn(t.batch.turn_id.as_str().into()), None),
            RunPhase::Deferred(DeferredPhase {
                deferred: Deferred::EffectPending { step_id, poll, .. },
                ..
            }) => (
                Phase::Turn(format!("{}:poll:{}", step_id.as_str(), poll.get())),
                Some((step_id.as_str().into(), poll.get())),
            ),
            RunPhase::FailureDrain(_) => (
                Phase::Checkpoint("failure_drain".into(), "failure_drain"),
                None,
            ),
            _ => return,
        };
        self.sync_phase(
            current,
            phase,
            step,
            if matches!(run.phase, RunPhase::Deferred(_)) {
                "deferred"
            } else {
                "assistant"
            },
        );
    }
    pub(crate) fn poll_start(&self, current: &CurrentOperation, step: &str, poll: u64) {
        self.sync_phase(
            current,
            Phase::Turn(format!("{step}:poll:{poll}")),
            Some((step.into(), poll)),
            "deferred",
        );
    }
    fn sync_phase(
        &self,
        current: &CurrentOperation,
        phase: Phase,
        step: Option<(String, u64)>,
        step_kind: &str,
    ) {
        // Only the driver updates this state. Detach before observer callbacks or
        // guard destruction; concurrent effects only clone the current context.
        let (root, mut old_phase, mut old_step) = {
            let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            (s.run.clone(), s.phase.take(), s.step.take())
        };
        let Some(root) = root else { return };
        if old_step.as_ref().map(|(k, _)| k) != step.as_ref() {
            drop(old_step.take());
        }
        if old_phase.as_ref().map(|(k, _)| k) != Some(&phase) {
            if let Some((_, span)) = old_phase.take() {
                span.end(Default::default(), false);
            }
            let mut attrs = correlation(&current.operation.lane, &current.operation.operation_id);
            let name = match &phase {
                Phase::Checkpoint(_, kind) => {
                    attrs.insert("pi.checkpoint.kind".into(), string(*kind));
                    "pi.harness.checkpoint"
                }
                Phase::Turn(turn) => {
                    attrs.insert("pi.turn.id".into(), string(turn));
                    "pi.harness.turn"
                }
            };
            old_phase = Some((phase, Span::start(root, name, attrs)));
        }
        if let Some(key) = step
            && old_step.is_none()
        {
            let mut attrs = correlation(&current.operation.lane, &current.operation.operation_id);
            attrs.insert("pi.step.kind".into(), string(step_kind));
            attrs.insert("pi.step.attempt".into(), number(key.1));
            let span = Span::start(
                old_phase.as_ref().expect("phase").1.clone(),
                "pi.harness.step",
                attrs,
            );
            span.outcome("pi.step.outcome", "failed", true);
            old_step = Some((key, span));
        }
        let mut s = self.state.lock().unwrap_or_else(|p| p.into_inner());
        s.phase = old_phase;
        s.step = old_step;
    }
    pub(crate) fn step_outcome(&self, value: &str) {
        let span = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .step
            .as_ref()
            .map(|(_, s)| s.clone());
        if let Some(span) = span {
            span.outcome(
                "pi.step.outcome",
                value,
                matches!(value, "retry" | "failed"),
            );
        }
    }
    pub(crate) fn finish(&self, result: &OperationResult) {
        let OperationResult::Run(outcome) = result else {
            return;
        };
        let value = match outcome.as_ref() {
            RunOutcome::CompletedWithAssistant(_) | RunOutcome::CompletedWithoutAssistant(_) => {
                "completed"
            }
            RunOutcome::AbortedWithAssistant(_) | RunOutcome::AbortedWithoutAssistant(_) => {
                "aborted"
            }
            RunOutcome::FailedWithAssistant(_) | RunOutcome::FailedWithoutAssistant(_) => "failed",
            _ => "suspended",
        };
        let (run, phase) = {
            let s = self.state.lock().unwrap_or_else(|p| p.into_inner());
            (s.run.clone(), s.phase.as_ref().map(|(_, s)| s.clone()))
        };
        if let Some(run) = run {
            run.outcome("pi.operation.outcome", value, value == "failed");
        }
        if let Some(phase) = phase {
            phase.end(Default::default(), value == "failed");
        }
    }
}
pub(crate) fn correlation(lane: &LaneId, operation: &OperationId) -> SpanAttributes {
    SpanAttributes::from([
        ("pi.lane.name".into(), string(lane.as_str())),
        ("pi.operation.id".into(), string(operation.as_str())),
    ])
}

pub(crate) fn event_name(kind: HarnessEventType) -> &'static str {
    match kind {
        HarnessEventType::RunStart => "run_start",
        HarnessEventType::RunResume => "run_resume",
        HarnessEventType::RunSuspend => "run_suspend",
        HarnessEventType::RunAbort => "run_abort",
        HarnessEventType::RunEnd => "run_end",
        HarnessEventType::Fault => "fault",
        HarnessEventType::HandlerError => "handler_error",
        HarnessEventType::TurnStart => "turn_start",
        HarnessEventType::TurnEnd => "turn_end",
        HarnessEventType::RetryScheduled => "retry_scheduled",
        HarnessEventType::RetryStart => "retry_start",
        HarnessEventType::RetryEnd => "retry_end",
        HarnessEventType::MessageStart => "message_start",
        HarnessEventType::MessageUpdate => "message_update",
        HarnessEventType::MessageEnd => "message_end",
        HarnessEventType::ToolStart => "tool_start",
        HarnessEventType::ToolUpdate => "tool_update",
        HarnessEventType::ToolEnd => "tool_end",
        HarnessEventType::EntryAdded => "entry_added",
        HarnessEventType::WritePending => "write_pending",
        HarnessEventType::QueueUpdate => "queue_update",
        HarnessEventType::FactUpdate => "fact_update",
        HarnessEventType::ConfigUpdate => "config_update",
        HarnessEventType::CompactionStart => "compaction_start",
        HarnessEventType::CompactionEnd => "compaction_end",
        HarnessEventType::NavigationStart => "navigation_start",
        HarnessEventType::NavigationEnd => "navigation_end",
        HarnessEventType::LaneCreated => "lane_created",
        HarnessEventType::Usage => "usage",
    }
}
