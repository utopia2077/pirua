use pi_agent_loop::*;
use pi_agent_schema::*;
use pi_ai_core::{AssistantMessage, JsonObject, JsonValue, Usage};
use serde_json::json;
use std::future::{Future, poll_fn};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll, Wake, Waker};

fn block_on<F: Future>(future: F) -> F::Output {
    struct WakeThread(std::thread::Thread);
    impl Wake for WakeThread {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(WakeThread(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::park(),
        }
    }
}
fn error() -> ToolExecutionError {
    ToolExecutionError::new(ErrorContext::new("fixture tool error").unwrap())
}
fn object(value: serde_json::Value) -> JsonObject {
    serde_json::from_value(value).unwrap()
}
fn call(id: &str) -> AgentToolCall {
    serde_json::from_value(json!({"type":"toolCall","id":id,"name":"work","arguments":{"n":"1"}}))
        .unwrap()
}
fn assistant() -> AssistantMessage {
    serde_json::from_value(json!({
    "role":"assistant","content":[call("a"),call("b")],"api":"test","provider":"test","model":"test",
    "usage":Usage::zero(),"stopReason":"toolUse","timestamp":1
})).unwrap()
}
#[derive(Default)]
struct Signal(AtomicBool);
impl Cancellation for Signal {
    fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(std::future::pending())
    }
}
#[derive(Default)]
struct Trace(Mutex<Vec<String>>);
impl Trace {
    fn push(&self, value: impl Into<String>) {
        self.0.lock().unwrap().push(value.into());
    }
    fn get(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}
impl AgentEventSink for Trace {
    fn emit(&self, event: AgentEvent) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            match event {
                AgentEvent::ToolExecutionStart { tool_call_id, .. } => {
                    self.push(format!("start:{tool_call_id}"))
                }
                AgentEvent::ToolExecutionUpdate {
                    tool_call_id, args, ..
                } => {
                    assert_eq!(args, JsonValue::Object(call("x").arguments));
                    self.push(format!("update:{tool_call_id}"));
                }
                AgentEvent::ToolExecutionEnd { tool_call_id, .. } => {
                    self.push(format!("end:{tool_call_id}"))
                }
                AgentEvent::MessageEnd {
                    message: AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result)),
                } => self.push(format!("message:{}", result.tool_call_id)),
                _ => (),
            }
            Ok(())
        })
    }
}
struct Validator;
impl ToolResultValidator for Validator {
    fn validate(&self, result: &AgentToolResult) -> Result<(), ToolExecutionError> {
        let bytes = serde_json::to_vec(result).map_err(|_| error())?;
        serde_json::from_slice::<AgentToolResult>(&bytes)
            .map(|_| ())
            .map_err(|_| error())
    }
    fn validate_usage(&self, usage: &Usage) -> Result<(), ToolExecutionError> {
        serde_json::from_value::<Usage>(serde_json::to_value(usage).unwrap())
            .map(|_| ())
            .map_err(|_| error())
    }
}
struct ContextValue {
    expected: i32,
}
struct ToolImpl {
    descriptor: AgentTool,
    trace: Arc<Trace>,
    mode: &'static str,
    reverse: bool,
    second_done: AtomicBool,
}
impl ToolImpl {
    fn new(trace: Arc<Trace>, mode: &'static str, reverse: bool) -> Self {
        Self { descriptor: serde_json::from_value(json!({"name":"work","label":"Work","description":"context","parameters":{"type":"object","properties":{"n":{"type":"integer"}},"required":["n"],"additionalProperties":false},"replay":"safe"})).unwrap(), trace, mode, reverse, second_done: AtomicBool::new(false) }
    }
}
impl AgentHarnessTool<ContextValue> for ToolImpl {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(&self, _: JsonValue) -> Result<JsonObject, ToolExecutionError> {
        self.trace.push("prepare");
        if self.mode == "prepare_panic" {
            panic!("fixture")
        }
        if self.mode == "prepare_error" {
            return Err(error());
        }
        Ok(object(json!({"n":"7"})))
    }
    fn execute<'a>(
        &'a self,
        id: pi_ai_core::ToolCallId,
        params: JsonObject,
        _: &'a dyn Cancellation,
        progress: &'a dyn ToolProgressSink,
        context: &'a ContextValue,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async move {
            self.trace.push(format!("execute:{id}"));
            assert_eq!(params, object(json!({"n":context.expected})));
            if self.reverse && id.as_str() == "a" {
                poll_fn(|cx| {
                    if self.second_done.load(Ordering::SeqCst) {
                        Poll::Ready(())
                    } else {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
            }
            if self.mode == "wait_after" && id.as_str() == "b" {
                let mut polls = 0;
                poll_fn(|cx| {
                    polls += 1;
                    assert!(
                        polls < 100,
                        "first source finalization was held behind later execution"
                    );
                    if self.trace.get().iter().any(|v| v == "after:a") {
                        Poll::Ready(())
                    } else {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                })
                .await;
            }
            if self.mode == "execute_panic" {
                panic!("fixture")
            }
            if self.mode == "execute_error" {
                return Err(error());
            }
            let mut result = tool_error("success");
            result.terminate = Some(true);
            result.usage = Some(Usage::zero());
            progress.update(result.clone()).await.unwrap();
            if self.mode == "invalid" {
                result.details = nested();
            }
            self.trace.push(format!("complete:{id}"));
            self.second_done.store(true, Ordering::SeqCst);
            Ok(result)
        })
    }
}
fn nested() -> JsonValue {
    (0..140).fold(JsonValue::Null, |value, _| JsonValue::Array(vec![value]))
}
struct Hooks {
    trace: Arc<Trace>,
    before: Option<BeforeToolResult>,
    after: Option<AfterToolResult>,
    mode: &'static str,
}
impl ToolPhaseHooks<ContextValue> for Hooks {
    fn before<'a>(
        &'a self,
        call: &'a AgentToolCall,
        args: &'a JsonObject,
        _: &'a ContextValue,
        _: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<BeforeToolResult>, ToolPhaseError>> {
        Box::pin(async move {
            self.trace.push(format!("before:{}", call.id));
            assert_eq!(*args, object(json!({"n":7})));
            if self.mode == "before_outer" {
                return Err(ToolPhaseError::Runtime(HarnessRuntimeError::Closed(
                    HarnessClosed,
                )));
            }
            if self.mode == "before_panic" {
                panic!("fixture")
            }
            if self.mode == "before_error" {
                return Err(ToolPhaseError::Ordinary(error()));
            }
            if self.mode == "block_b" && call.id.as_str() == "b" {
                return Ok(Some(BeforeToolResult {
                    args: None,
                    block: Some(BeforeToolBlock {
                        reason: "blocked".into(),
                        terminate: Some(true),
                    }),
                }));
            }
            Ok(self.before.clone())
        })
    }
    fn after<'a>(
        &'a self,
        call: &'a AgentToolCall,
        _: &'a JsonObject,
        _: &'a ExecutedToolOutcome,
        _: &'a ContextValue,
        _: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<AfterToolResult>, ToolPhaseError>> {
        Box::pin(async move {
            self.trace.push(format!("after:{}", call.id));
            if self.mode == "after_outer" {
                return Err(ToolPhaseError::Runtime(HarnessRuntimeError::Closed(
                    HarnessClosed,
                )));
            }
            if self.mode == "after_panic" {
                panic!("fixture")
            }
            if self.mode == "after_error" {
                return Err(ToolPhaseError::Ordinary(error()));
            }
            Ok(self.after.clone())
        })
    }
}
struct Phases {
    binding: AgentHarnessToolHandle<ContextValue>,
    hooks: Hooks,
    signal: Signal,
    trace: Arc<Trace>,
    context: ContextValue,
}
impl Phases {
    fn new(mode: &'static str, reverse: bool) -> Self {
        let trace = Arc::new(Trace::default());
        Self {
            binding: Arc::new(ToolImpl::new(trace.clone(), mode, reverse)),
            hooks: Hooks {
                trace: trace.clone(),
                before: None,
                after: None,
                mode,
            },
            signal: Signal::default(),
            trace,
            context: ContextValue { expected: 7 },
        }
    }
}
impl ToolBatchPhases<ContextValue> for Phases {
    type Binding = AgentHarnessToolHandle<ContextValue>;
    type Error = HarnessRuntimeError;
    fn execution_mode(&self, _: &str) -> Option<ToolExecutionMode> {
        self.binding.descriptor().execution_mode
    }
    fn is_cancelled(&self) -> bool {
        self.signal.is_cancelled()
    }
    fn prepare<'a>(
        &'a self,
        context: &'a ContextValue,
        _: &'a AssistantMessage,
        call: &'a AgentToolCall,
    ) -> BoxFuture<'a, Result<ToolPreparation<Self::Binding>, Self::Error>> {
        Box::pin(prepare_tool_call(
            call,
            Some(self.binding.clone()),
            context,
            &self.signal,
            &self.hooks,
            false,
        ))
    }
    fn execute<'a>(
        &'a self,
        prepared: &'a PreparedToolCall<Self::Binding>,
    ) -> BoxFuture<'a, Result<ExecutedToolOutcome, Self::Error>> {
        Box::pin(execute_tool_call(
            prepared,
            &self.context,
            &self.signal,
            self.trace.as_ref(),
            &Validator,
        ))
    }
    fn finalize<'a>(
        &'a self,
        context: &'a ContextValue,
        _: &'a AssistantMessage,
        prepared: &'a PreparedToolCall<Self::Binding>,
        raw: ExecutedToolOutcome,
    ) -> BoxFuture<'a, Result<FinalizedToolCall, Self::Error>> {
        Box::pin(finalize_tool_call(
            prepared,
            raw,
            context,
            &self.signal,
            &self.hooks,
            &Validator,
            true,
        ))
    }
    fn emit(&self, event: AgentEvent) -> BoxFuture<'_, Result<(), Self::Error>> {
        self.trace.emit(event)
    }
}
fn prepared(phases: &Phases) -> PreparedToolCall<AgentHarnessToolHandle<ContextValue>> {
    match block_on(prepare_tool_call(
        &call("a"),
        Some(phases.binding.clone()),
        &phases.context,
        &phases.signal,
        &phases.hooks,
        false,
    ))
    .unwrap()
    {
        ToolPreparation::Prepared(value) => value,
        _ => panic!("expected prepared"),
    }
}
#[test]
fn actual_rewrite_replacement_context_and_explicit_patch_values() {
    let mut phases = Phases::new("ok", false);
    phases.hooks.before = Some(BeforeToolResult {
        args: Some(object(json!({"n":"9"}))),
        block: None,
    });
    phases.context.expected = 9;
    phases.hooks.after = Some(AfterToolResult {
        content: Some(vec![]),
        details: pi_ai_core::Presence::Null,
        is_error: Some(false),
        usage: Some(Usage::zero()),
        terminate: Some(false),
    });
    let prepared = prepared(&phases);
    assert_eq!(prepared.args, object(json!({"n":9})));
    assert_eq!(prepared.tool_call.arguments, call("a").arguments);
    let raw = block_on(phases.execute(&prepared)).unwrap();
    let value = block_on(phases.finalize(&phases.context, &assistant(), &prepared, raw)).unwrap();
    assert!(value.result.content.is_empty());
    assert_eq!(value.result.details, JsonValue::Null);
    assert_eq!(value.result.usage, Some(Usage::zero()));
    assert_eq!(value.result.terminate, Some(false));
    assert!(!value.terminate);
    assert!(!value.is_error);
}
#[test]
fn unknown_invalid_blocked_cancelled_and_length_do_not_execute_or_finalize() {
    for mode in [
        "unknown",
        "invalid_args",
        "block",
        "cancel",
        "length",
        "prepare_error",
        "prepare_panic",
        "before_error",
        "before_panic",
    ] {
        let mut p = Phases::new(mode, false);
        if mode == "invalid_args" {
            p.hooks.before = Some(BeforeToolResult {
                args: Some(object(json!({"bad":1}))),
                block: None,
            });
        }
        if mode == "block" {
            p.hooks.before = Some(BeforeToolResult {
                args: None,
                block: Some(BeforeToolBlock {
                    reason: "blocked".into(),
                    terminate: Some(true),
                }),
            });
        }
        if mode == "cancel" {
            p.signal.0.store(true, Ordering::SeqCst);
        }
        let result = block_on(prepare_tool_call(
            &call("a"),
            if mode == "unknown" {
                None
            } else {
                Some(p.binding.clone())
            },
            &p.context,
            &p.signal,
            &p.hooks,
            mode == "length",
        ))
        .unwrap();
        let ToolPreparation::Immediate(outcome) = result else {
            panic!("{mode}")
        };
        assert!(outcome.is_error);
        assert_eq!(outcome.terminate, mode == "block");
        assert!(
            !p.trace
                .get()
                .iter()
                .any(|s| s.starts_with("execute:") || s.starts_with("after:"))
        );
        if matches!(mode, "length" | "cancel" | "unknown") {
            assert!(p.trace.get().is_empty());
        }
    }
}
#[test]
fn raw_final_validation_and_exceptions_retain_valid_usage() {
    for mode in [
        "invalid",
        "execute_error",
        "execute_panic",
        "after_error",
        "after_panic",
        "invalid_patch",
    ] {
        let mut p = Phases::new(mode, false);
        if mode == "invalid_patch" {
            p.hooks.after = Some(AfterToolResult {
                details: pi_ai_core::Presence::Value(nested()),
                ..Default::default()
            });
        }
        let prepared = prepared(&p);
        let raw = block_on(p.execute(&prepared)).unwrap();
        if mode == "invalid" {
            assert!(raw.is_error);
            assert_eq!(raw.result.usage, Some(Usage::zero()));
        }
        let result = block_on(p.finalize(&p.context, &assistant(), &prepared, raw)).unwrap();
        assert!(result.is_error, "{mode}");
        assert!(!result.terminate);
        if !mode.starts_with("execute") {
            assert_eq!(result.result.usage, Some(Usage::zero()));
        }
    }
}
#[test]
fn persisted_replay_skips_preflight_and_abort_first_skips_after() {
    let mut p = Phases::new("prepare_panic", false);
    p.context.expected = 13;
    let saved = PreparedToolCall {
        tool_call: call("a"),
        args: object(json!({"n":13})),
        binding: p.binding.clone(),
    };
    let raw = block_on(p.execute(&saved)).unwrap();
    let result = block_on(finalize_tool_call(
        &saved, raw, &p.context, &p.signal, &p.hooks, &Validator, false,
    ))
    .unwrap();
    assert!(!result.terminate);
    let trace = p.trace.get();
    assert!(
        !trace
            .iter()
            .any(|v| v == "prepare" || v.starts_with("before:") || v.starts_with("after:"))
    );
}
#[test]
fn production_schedulers_preserve_each_order_and_nonempty_termination() {
    for legacy in [false, true] {
        let p = Phases::new("ok", true);
        let assistant = assistant();
        let input = ToolBatchInput {
            context: &p.context,
            assistant_message: &assistant,
            default_mode: ToolExecutionMode::Parallel,
        };
        let result = block_on(if legacy {
            execute_legacy_tool_batch(input, &p)
        } else {
            execute_tool_batch(input, &p)
        })
        .unwrap();
        assert!(result.terminate);
        assert_eq!(
            result
                .messages
                .iter()
                .map(|m| m.tool_call_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b"]
        );
        let trace = p.trace.get();
        let pos = |s: &str| trace.iter().position(|v| v == s).unwrap();
        assert!(pos("before:b") < pos("execute:a"));
        assert!(pos("complete:b") < pos("complete:a"));
        if legacy {
            assert!(pos("after:b") < pos("after:a"));
            assert!(pos("end:b") < pos("end:a"));
        } else {
            assert!(pos("after:a") < pos("after:b"));
            assert!(pos("end:a") < pos("end:b"));
        }
        assert!(pos("message:a") < pos("message:b"));
        let mut empty = assistant.clone();
        empty.content.clear();
        let result = block_on(execute_tool_batch(
            ToolBatchInput {
                context: &p.context,
                assistant_message: &empty,
                default_mode: ToolExecutionMode::Parallel,
            },
            &p,
        ))
        .unwrap();
        assert!(!result.terminate);
    }
}
#[test]
fn production_length_and_sequential_paths() {
    for length in [false, true] {
        let p = Phases::new("ok", false);
        let mut assistant = assistant();
        if length {
            assistant.stop_reason = pi_ai_core::StopReason::Length;
        }
        let result = block_on(execute_tool_batch(
            ToolBatchInput {
                context: &p.context,
                assistant_message: &assistant,
                default_mode: ToolExecutionMode::Sequential,
            },
            &p,
        ))
        .unwrap();
        assert_eq!(result.messages.len(), 2);
        let trace = p.trace.get();
        if length {
            assert!(!result.terminate);
            assert!(
                !trace.iter().any(|v| v == "prepare"
                    || v.starts_with("execute:")
                    || v.starts_with("after:"))
            );
        } else {
            assert!(
                trace.iter().position(|v| v == "after:a").unwrap()
                    < trace.iter().position(|v| v == "before:b").unwrap()
            );
        }
    }
}

#[test]
fn owner_failures_stay_outer_even_when_tool_ignores_progress_error() {
    let p = Phases::new("before_outer", false);
    assert!(matches!(
        block_on(prepare_tool_call(
            &call("a"),
            Some(p.binding.clone()),
            &p.context,
            &p.signal,
            &p.hooks,
            false
        )),
        Err(HarnessRuntimeError::Closed(_))
    ));
    let p = Phases::new("after_outer", false);
    let prepared = prepared(&p);
    let raw = block_on(p.execute(&prepared)).unwrap();
    assert!(matches!(
        block_on(p.finalize(&p.context, &assistant(), &prepared, raw)),
        Err(HarnessRuntimeError::Closed(_))
    ));
    struct ClosedEvents;
    impl AgentEventSink for ClosedEvents {
        fn emit(&self, _: AgentEvent) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
            Box::pin(async { Err(HarnessRuntimeError::Closed(HarnessClosed)) })
        }
    }
    let p = Phases::new("ok", false);
    let prepared = crate::prepared(&p);
    assert!(matches!(
        block_on(execute_tool_call(
            &prepared,
            &p.context,
            &p.signal,
            &ClosedEvents,
            &Validator
        )),
        Err(HarnessRuntimeError::Closed(_))
    ));
}
#[test]
fn dropped_execution_retires_pending_progress_delivery() {
    struct PendingEvents(AtomicBool);
    struct Delivery<'a>(&'a AtomicBool);
    impl Drop for Delivery<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    impl AgentEventSink for PendingEvents {
        fn emit(&self, _: AgentEvent) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
            Box::pin(async {
                let _delivery = Delivery(&self.0);
                std::future::pending().await
            })
        }
    }
    let p = Phases::new("ok", false);
    let prepared = prepared(&p);
    let events = PendingEvents(AtomicBool::new(false));
    let mut future = Box::pin(execute_tool_call(
        &prepared, &p.context, &p.signal, &events, &Validator,
    ));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(future.as_mut().poll(&mut cx).is_pending());
    drop(future);
    assert!(events.0.load(Ordering::SeqCst));
    assert!(!p.trace.get().iter().any(|v| v.starts_with("complete:")));
}
#[test]
fn cancellation_before_execute_never_calls_bound_tool() {
    let p = Phases::new("ok", false);
    let prepared = prepared(&p);
    p.signal.0.store(true, Ordering::SeqCst);
    let raw = block_on(p.execute(&prepared)).unwrap();
    assert!(raw.is_error);
    assert!(!p.trace.get().iter().any(|v| v.starts_with("execute:")));
}

#[test]
fn immediate_end_events_follow_each_scheduler_and_preserve_block_termination() {
    for legacy in [false, true] {
        let p = Phases::new("block_b", false);
        let assistant = assistant();
        let input = ToolBatchInput {
            context: &p.context,
            assistant_message: &assistant,
            default_mode: ToolExecutionMode::Parallel,
        };
        let batch = block_on(if legacy {
            execute_legacy_tool_batch(input, &p)
        } else {
            execute_tool_batch(input, &p)
        })
        .unwrap();
        assert!(batch.terminate);
        let trace = p.trace.get();
        assert!(!trace.iter().any(|v| v == "execute:b" || v == "after:b"));
        let a = trace.iter().position(|v| v == "end:a").unwrap();
        let b = trace.iter().position(|v| v == "end:b").unwrap();
        assert_eq!(b < a, legacy);
    }
}

#[test]
fn target_finalizes_first_ready_source_while_later_execution_is_pending() {
    let p = Phases::new("wait_after", false);
    let assistant = assistant();
    let result = block_on(execute_tool_batch(
        ToolBatchInput {
            context: &p.context,
            assistant_message: &assistant,
            default_mode: ToolExecutionMode::Parallel,
        },
        &p,
    ))
    .unwrap();
    assert!(result.messages.iter().all(|m| !m.is_error));
    let trace = p.trace.get();
    assert!(
        trace.iter().position(|v| v == "after:a").unwrap()
            < trace.iter().position(|v| v == "complete:b").unwrap()
    );
}
#[test]
fn sequential_tool_override_controls_an_otherwise_parallel_batch() {
    let mut p = Phases::new("ok", false);
    let mut tool = ToolImpl::new(p.trace.clone(), "ok", false);
    tool.descriptor.execution_mode = Some(ToolExecutionMode::Sequential);
    p.binding = Arc::new(tool);
    let assistant = assistant();
    block_on(execute_legacy_tool_batch(
        ToolBatchInput {
            context: &p.context,
            assistant_message: &assistant,
            default_mode: ToolExecutionMode::Parallel,
        },
        &p,
    ))
    .unwrap();
    let trace = p.trace.get();
    assert!(
        trace.iter().position(|v| v == "after:a").unwrap()
            < trace.iter().position(|v| v == "before:b").unwrap()
    );
}

#[test]
fn legacy_ready_executions_start_before_post_hooks() {
    let p = Phases::new("ok", false);
    let assistant = assistant();
    block_on(execute_legacy_tool_batch(
        ToolBatchInput {
            context: &p.context,
            assistant_message: &assistant,
            default_mode: ToolExecutionMode::Parallel,
        },
        &p,
    ))
    .unwrap();
    let trace = p.trace.get();
    assert!(
        trace.iter().position(|v| v == "execute:b").unwrap()
            < trace.iter().position(|v| v == "after:a").unwrap(),
        "{trace:?}"
    );
}

#[test]
fn legacy_ready_peer_starts_before_after_cancellation() {
    struct CancelAfter(Phases);
    impl ToolBatchPhases<ContextValue> for CancelAfter {
        type Binding = AgentHarnessToolHandle<ContextValue>;
        type Error = HarnessRuntimeError;
        fn execution_mode(&self, name: &str) -> Option<ToolExecutionMode> {
            self.0.execution_mode(name)
        }
        fn is_cancelled(&self) -> bool {
            self.0.is_cancelled()
        }
        fn prepare<'a>(
            &'a self,
            context: &'a ContextValue,
            assistant: &'a AssistantMessage,
            call: &'a AgentToolCall,
        ) -> BoxFuture<'a, Result<ToolPreparation<Self::Binding>, Self::Error>> {
            self.0.prepare(context, assistant, call)
        }
        fn execute<'a>(
            &'a self,
            prepared: &'a PreparedToolCall<Self::Binding>,
        ) -> BoxFuture<'a, Result<ExecutedToolOutcome, Self::Error>> {
            self.0.execute(prepared)
        }
        fn finalize<'a>(
            &'a self,
            context: &'a ContextValue,
            assistant: &'a AssistantMessage,
            prepared: &'a PreparedToolCall<Self::Binding>,
            raw: ExecutedToolOutcome,
        ) -> BoxFuture<'a, Result<FinalizedToolCall, Self::Error>> {
            Box::pin(async move {
                let result = self.0.finalize(context, assistant, prepared, raw).await?;
                if prepared.tool_call.id.as_str() == "a" {
                    self.0.signal.0.store(true, Ordering::SeqCst);
                }
                Ok(result)
            })
        }
        fn emit(&self, event: AgentEvent) -> BoxFuture<'_, Result<(), Self::Error>> {
            self.0.emit(event)
        }
    }
    let p = CancelAfter(Phases::new("ok", false));
    let assistant = assistant();
    let result = block_on(execute_legacy_tool_batch(
        ToolBatchInput {
            context: &p.0.context,
            assistant_message: &assistant,
            default_mode: ToolExecutionMode::Parallel,
        },
        &p,
    ))
    .unwrap();
    assert!(p.0.signal.is_cancelled());
    assert!(
        p.0.trace.get().iter().any(|v| v == "execute:b"),
        "{:?}",
        p.0.trace.get()
    );
    assert!(result.messages.iter().all(|m| !m.is_error));
}
