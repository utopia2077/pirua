use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

use pi_agent_schema::*;
use pi_ai_core::{JsonObject, JsonValue, Presence, Tool, Usage, validate_tool_arguments};

/// An ordinary isolated callback failure or an owner lifecycle failure.
pub enum ToolPhaseError {
    /// Converted to an in-band tool error.
    Ordinary(ToolExecutionError),
    /// Propagated without manufacturing a successful settlement.
    Runtime(HarnessRuntimeError),
}

/// Process-local typed hook adapter. Invocation/start authorization belongs to the caller.
pub trait ToolPhaseHooks<C>: Send + Sync {
    /// Applies before-tool behavior to validated effective arguments.
    fn before<'a>(
        &'a self,
        call: &'a AgentToolCall,
        args: &'a JsonObject,
        context: &'a C,
        cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<BeforeToolResult>, ToolPhaseError>>;
    /// Applies after-tool behavior to a validated raw outcome.
    fn after<'a>(
        &'a self,
        call: &'a AgentToolCall,
        args: &'a JsonObject,
        outcome: &'a ExecutedToolOutcome,
        context: &'a C,
        cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<AfterToolResult>, ToolPhaseError>>;
}

/// Validation adapter for the caller's actual durable result boundary.
/// JSON, content tags, IDs and numeric domains are already constrained by the DTOs.
pub trait ToolResultValidator: Send + Sync {
    /// Validates the complete candidate before progress or settlement.
    fn validate(&self, result: &AgentToolResult) -> Result<(), ToolExecutionError>;
    /// Independently validates usage so malformed content cannot erase accounting.
    fn validate_usage(&self, usage: &Usage) -> Result<(), ToolExecutionError>;
}

/// Constructs the source-compatible synthetic error shape.
pub fn tool_error(message: impl Into<String>) -> AgentToolResult {
    AgentToolResult {
        content: vec![pi_ai_core::InputContent::Text(pi_ai_core::TextContent {
            content_type: Default::default(),
            text: message.into(),
            text_signature: None,
        })],
        details: JsonValue::Object(JsonObject::default()),
        usage: None,
        added_tool_names: None,
        terminate: None,
    }
}

fn immediate(message: impl Into<String>) -> ImmediateToolOutcome {
    ImmediateToolOutcome {
        result: tool_error(message),
        is_error: true,
        terminate: false,
    }
}

async fn isolated<F: Future>(future: F) -> Result<F::Output, ()> {
    let mut future = Box::pin(future);
    poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(_) => Poll::Ready(Err(())),
        },
    )
    .await
}

fn arguments<C>(
    binding: &dyn AgentHarnessTool<C>,
    call: &AgentToolCall,
    args: JsonObject,
) -> Result<JsonObject, String> {
    let descriptor = binding.descriptor();
    let tool = Tool {
        name: descriptor.name.clone(),
        description: descriptor.description.clone(),
        parameters: descriptor.parameters.clone(),
        constrained_sampling: descriptor.constrained_sampling.clone(),
    };
    let mut candidate = call.clone();
    candidate.arguments = args;
    validate_tool_arguments(&tool, &candidate).map_err(|error| error.to_string())
}

/// Phase one. Genuine-length calls and immediate outcomes never execute a tool.
/// The returned args are the complete effective value to persist, even if unchanged.
pub async fn prepare_tool_call<C: Sync>(
    call: &AgentToolCall,
    binding: Option<AgentHarnessToolHandle<C>>,
    context: &C,
    cancellation: &dyn Cancellation,
    hooks: &dyn ToolPhaseHooks<C>,
    genuine_length: bool,
) -> Result<ToolPreparation<AgentHarnessToolHandle<C>>, HarnessRuntimeError> {
    if genuine_length {
        return Ok(ToolPreparation::Immediate(immediate(format!(
            "Tool call \"{}\" was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.",
            call.name
        ))));
    }
    if cancellation.is_cancelled() {
        return Ok(ToolPreparation::Immediate(immediate("Operation aborted")));
    }
    let Some(binding) = binding.filter(|b| b.descriptor().name == call.name) else {
        return Ok(ToolPreparation::Immediate(immediate(format!(
            "Tool {} not found",
            call.name
        ))));
    };
    let prepared = catch_unwind(AssertUnwindSafe(|| {
        binding.prepare_arguments(JsonValue::Object(call.arguments.clone()))
    }));
    let args = match prepared {
        Ok(Ok(args)) => match arguments(binding.as_ref(), call, args) {
            Ok(args) => args,
            Err(error) => return Ok(ToolPreparation::Immediate(immediate(error))),
        },
        Ok(Err(error)) => return Ok(ToolPreparation::Immediate(immediate(error.to_string()))),
        Err(_) => {
            return Ok(ToolPreparation::Immediate(immediate(
                "Tool preparation panicked",
            )));
        }
    };
    // Calling a trait method may itself panic before it returns its future.
    let before = isolated(async { hooks.before(call, &args, context, cancellation).await }).await;
    let patch = match before {
        Ok(Ok(patch)) => patch,
        Ok(Err(ToolPhaseError::Runtime(error))) => return Err(error),
        Ok(Err(ToolPhaseError::Ordinary(error))) => {
            return Ok(ToolPreparation::Immediate(immediate(error.to_string())));
        }
        Err(()) => {
            return Ok(ToolPreparation::Immediate(immediate(
                "Before-tool callback panicked",
            )));
        }
    };
    if cancellation.is_cancelled() {
        return Ok(ToolPreparation::Immediate(immediate("Operation aborted")));
    }
    let mut args = args;
    if let Some(patch) = patch {
        if let Some(block) = patch.block {
            let mut outcome = immediate(if block.reason.is_empty() {
                "Tool execution was blocked".to_owned()
            } else {
                block.reason
            });
            outcome.terminate = block.terminate == Some(true);
            if outcome.terminate {
                outcome.result.terminate = Some(true);
            }
            return Ok(ToolPreparation::Immediate(outcome));
        }
        if let Some(replacement) = patch.args {
            args = match arguments(binding.as_ref(), call, replacement) {
                Ok(args) => args,
                Err(error) => return Ok(ToolPreparation::Immediate(immediate(error))),
            };
        }
    }
    Ok(ToolPreparation::Prepared(PreparedToolCall {
        tool_call: call.clone(),
        args,
        binding,
    }))
}

fn checked(
    mut outcome: ExecutedToolOutcome,
    validator: &dyn ToolResultValidator,
) -> ExecutedToolOutcome {
    if let Err(error) = validator.validate(&outcome.result) {
        let usage = outcome
            .result
            .usage
            .take()
            .filter(|usage| validator.validate_usage(usage).is_ok());
        outcome = ExecutedToolOutcome {
            result: tool_error(error.to_string()),
            is_error: true,
        };
        outcome.result.usage = usage.or_else(|| Some(Usage::zero()));
    }
    outcome
}

struct Progress<'a> {
    active: AtomicBool,
    call: &'a AgentToolCall,
    events: &'a dyn AgentEventSink,
    validator: &'a dyn ToolResultValidator,
    failure: Mutex<Option<HarnessRuntimeError>>,
}
impl ToolProgressSink for Progress<'_> {
    fn update(&self, partial: AgentToolResult) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            if !self.active.load(Ordering::Acquire) {
                return Ok(());
            }
            let result = isolated(async {
                let partial = checked(
                    ExecutedToolOutcome {
                        result: partial,
                        is_error: false,
                    },
                    self.validator,
                )
                .result;
                self.events
                    .emit(AgentEvent::ToolExecutionUpdate {
                        tool_call_id: self.call.id.clone(),
                        tool_name: self.call.name.clone(),
                        // The legacy update event carries original provider args.
                        args: JsonValue::Object(self.call.arguments.clone()),
                        partial_result: partial,
                    })
                    .await
            })
            .await
            .unwrap_or_else(|()| {
                Err(HarnessRuntimeError::Fault(HarnessFault::new(
                    ErrorContext::new("tool progress delivery panicked")
                        .expect("bounded static error"),
                )))
            });
            if let Err(error) = result {
                let mut failure = self.failure.lock().unwrap_or_else(|e| e.into_inner());
                if failure.is_none() {
                    *failure = Some(error);
                }
            }
            Ok(())
        })
    }
}
struct Retire<'a>(&'a AtomicBool);
impl Drop for Retire<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Phase two. Saved safe-replay args can be placed directly in `PreparedToolCall`;
/// this function never calls prepareArguments or before_tool again.
pub async fn execute_tool_call<C: Sync>(
    prepared: &PreparedToolCall<AgentHarnessToolHandle<C>>,
    context: &C,
    cancellation: &dyn Cancellation,
    events: &dyn AgentEventSink,
    validator: &dyn ToolResultValidator,
) -> Result<ExecutedToolOutcome, HarnessRuntimeError> {
    if cancellation.is_cancelled() {
        return Ok(ExecutedToolOutcome {
            result: tool_error("Operation aborted"),
            is_error: true,
        });
    }
    let progress = Progress {
        active: AtomicBool::new(true),
        call: &prepared.tool_call,
        events,
        validator,
        failure: Mutex::new(None),
    };
    let retire = Retire(&progress.active);
    let result = isolated(async {
        prepared
            .binding
            .execute(
                prepared.tool_call.id.clone(),
                prepared.args.clone(),
                cancellation,
                &progress,
                context,
            )
            .await
    })
    .await;
    drop(retire);
    if let Some(error) = progress
        .failure
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .take()
    {
        return Err(error);
    }
    let outcome = match result {
        Ok(Ok(result)) => ExecutedToolOutcome {
            result,
            is_error: false,
        },
        Ok(Err(error)) => ExecutedToolOutcome {
            result: tool_error(error.to_string()),
            is_error: true,
        },
        Err(()) => ExecutedToolOutcome {
            result: tool_error("Tool execution panicked"),
            is_error: true,
        },
    };
    Ok(checked(outcome, validator))
}

/// Phase three. Caller invokes this in Harness source order or legacy completion order.
/// Pass `run_after=false` when abort won the caller's serialized hook-start check.
pub async fn finalize_tool_call<C: Sync>(
    prepared: &PreparedToolCall<AgentHarnessToolHandle<C>>,
    mut outcome: ExecutedToolOutcome,
    context: &C,
    cancellation: &dyn Cancellation,
    hooks: &dyn ToolPhaseHooks<C>,
    validator: &dyn ToolResultValidator,
    run_after: bool,
) -> Result<FinalizedToolCall, HarnessRuntimeError> {
    if run_after {
        let patch = isolated(async {
            hooks
                .after(
                    &prepared.tool_call,
                    &prepared.args,
                    &outcome,
                    context,
                    cancellation,
                )
                .await
        })
        .await;
        match patch {
            Ok(Ok(Some(patch))) => {
                if let Some(value) = patch.content {
                    outcome.result.content = value;
                }
                match patch.details {
                    Presence::Missing => (),
                    Presence::Null => outcome.result.details = JsonValue::Null,
                    Presence::Value(value) => outcome.result.details = value,
                }
                if let Some(value) = patch.usage {
                    outcome.result.usage = Some(value);
                }
                if let Some(value) = patch.terminate {
                    outcome.result.terminate = Some(value);
                }
                if let Some(value) = patch.is_error {
                    outcome.is_error = value;
                }
            }
            Ok(Ok(None)) => (),
            Ok(Err(ToolPhaseError::Runtime(error))) => return Err(error),
            error => {
                let usage = outcome.result.usage.take();
                outcome = ExecutedToolOutcome {
                    result: tool_error(match error {
                        Ok(Err(ToolPhaseError::Ordinary(error))) => error.to_string(),
                        _ => "After-tool callback panicked".to_owned(),
                    }),
                    is_error: true,
                };
                outcome.result.usage = usage;
            }
        }
    } else {
        outcome.result.terminate = Some(false);
    }
    let outcome = checked(outcome, validator);
    Ok(FinalizedToolCall {
        tool_call: prepared.tool_call.clone(),
        terminate: outcome.result.terminate == Some(true),
        result: outcome.result,
        is_error: outcome.is_error,
    })
}
