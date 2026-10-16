use futures_util::{FutureExt, StreamExt, future::try_join_all, stream::FuturesUnordered};
use pi_agent_schema::*;
use pi_ai_core::{
    AssistantMessage, InputContent, JsonObject, JsonValue, Message, Presence, TextContent,
    ToolCall, ToolResultMessage, validate_tool_arguments,
};
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Instant,
};

pub type ToolUpdateSink =
    Arc<dyn Fn(AgentToolResult) -> BoxFuture<'static, Result<(), AgentError>> + Send + Sync>;

pub struct RunToolCallOptions {
    pub tools: Vec<AgentTool>,
    pub assistant_message: AssistantMessage,
    pub context: AgentContext,
    pub cancellation: CancellationToken,
    pub hooks: ToolCallHooks,
    pub on_update: Option<ToolUpdateSink>,
    pub monotonic_clock: Option<crate::MonotonicClock>,
}

pub(crate) async fn isolated<T>(
    future: impl Future<Output = Result<T, AgentError>>,
) -> Result<T, AgentError> {
    AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(AgentError::Callback("Agent callback panicked".into())))
}

pub(crate) fn error_result(message: impl Into<String>) -> AgentToolResult {
    AgentToolResult {
        content: vec![InputContent::Text(TextContent {
            content_type: Default::default(),
            text: message.into(),
            text_signature: None,
        })],
        details: Some(JsonValue::Object(JsonObject::default())),
        ..Default::default()
    }
}

fn failed(call: ToolCall, message: impl Into<String>) -> AgentToolCallOutcome {
    AgentToolCallOutcome {
        tool_call: call,
        result: error_result(message),
        is_error: true,
        duration_ms: None,
    }
}

struct Prepared {
    tool: AgentTool,
    before: BeforeToolCallContext,
}
enum Preparation {
    Immediate(Box<AgentToolCallOutcome>),
    Prepared(Box<Prepared>),
}

async fn prepare(call: ToolCall, options: &RunToolCallOptions) -> Preparation {
    let Some(tool) = options
        .tools
        .iter()
        .find(|tool| tool.declaration.name == call.name)
        .cloned()
    else {
        let message = format!("Tool {} not found", call.name);
        return Preparation::Immediate(Box::new(failed(call, message)));
    };
    let candidate = isolated(async {
        let mut candidate = call.clone();
        if let Some(prepare) = &tool.prepare_arguments {
            candidate.arguments = prepare(candidate.arguments)?;
        }
        let args = validate_tool_arguments(&tool.declaration, &candidate).map_err(|error| {
            AgentError::Tool(pi_ai_core::format_tool_validation_feedback(
                &candidate, &error,
            ))
        })?;
        let before = BeforeToolCallContext {
            assistant_message: options.assistant_message.clone(),
            tool_call: call.clone(),
            args,
            context: options.context.clone(),
        };
        let patch = if let Some(hook) = &options.hooks.before {
            hook(before.clone(), options.cancellation.clone()).await?
        } else {
            None
        };
        if options.cancellation.is_cancelled() {
            return Err(AgentError::Tool("Operation aborted".into()));
        }
        Ok((before, patch))
    })
    .await;
    match candidate {
        Err(error) => Preparation::Immediate(Box::new(failed(call, error.to_string()))),
        Ok((_, Some(patch))) if patch.block => {
            let mut outcome = failed(
                call,
                patch
                    .reason
                    .filter(|reason| !reason.is_empty())
                    .unwrap_or_else(|| "Tool execution was blocked".into()),
            );
            if patch.terminate {
                outcome.result.terminate = Some(true);
            }
            Preparation::Immediate(Box::new(outcome))
        }
        Ok((mut before, patch)) => {
            if let Some(args) = patch.and_then(|patch| patch.args) {
                before.args = args;
            }
            Preparation::Prepared(Box::new(Prepared { tool, before }))
        }
    }
}

// Retire callbacks even when the owning tool future is dropped.
struct UpdateAdmission(Arc<Mutex<bool>>);
impl Drop for UpdateAdmission {
    fn drop(&mut self) {
        *self.0.lock().unwrap_or_else(|error| error.into_inner()) = false;
    }
}

async fn execute(
    prepared: Prepared,
    options: &RunToolCallOptions,
) -> Result<AgentToolCallOutcome, AgentError> {
    let (sender, mut receiver) =
        tokio::sync::mpsc::unbounded_channel::<BoxFuture<'static, Result<(), AgentError>>>();
    let accepting = Arc::new(Mutex::new(true));
    let admission = UpdateAdmission(accepting.clone());
    let waker = std::future::poll_fn(|cx| Poll::Ready(cx.waker().clone())).await;
    let on_update: ToolUpdate = {
        let accepting = accepting.clone();
        let sink = options.on_update.clone();
        Arc::new(move |partial| {
            let Some(sink) = sink.clone() else {
                return;
            };
            let (deliver, receive) =
                tokio::sync::oneshot::channel::<BoxFuture<'static, Result<(), AgentError>>>();
            {
                let accepting = accepting.lock().unwrap_or_else(|error| error.into_inner());
                if !*accepting {
                    return;
                }
                // Register the acknowledgement before invoking foreign code. This also
                // accounts for updates admitted on another thread as execution settles.
                let acknowledgement = Box::pin(async move {
                    match receive.await {
                        Ok(future) => future.await,
                        Err(_) => Err(AgentError::Callback("Tool update was abandoned".into())),
                    }
                });
                if sender.send(acknowledgement).is_err() {
                    return;
                }
            }
            // JavaScript starts an async callback immediately, up to its first await.
            // Preserve that ordering even when a sibling tool finishes in the same poll.
            let mut future: BoxFuture<'static, Result<(), AgentError>> =
                Box::pin(isolated(async move { sink(partial).await }));
            if let Poll::Ready(result) = future.as_mut().poll(&mut Context::from_waker(&waker)) {
                future = Box::pin(std::future::ready(result));
            }
            let _ = deliver.send(future);
        })
    };
    let started = Instant::now();
    let clock_start = options.monotonic_clock.as_ref().map(|clock| clock());
    let invocation = ToolInvocation {
        call_id: prepared.before.tool_call.id.to_string(),
        arguments: prepared.before.args.clone(),
        cancellation: options.cancellation.clone(),
        on_update,
    };
    let mut future = Box::pin(async {
        let result = isolated(async { (prepared.tool.execute)(invocation).await }).await;
        let elapsed = if let (Some(start), Some(clock)) = (clock_start, &options.monotonic_clock) {
            clock().saturating_sub(start)
        } else {
            started.elapsed()
        };
        (result, (elapsed.as_secs_f64() * 1000.0).round() as u64)
    });
    let mut update_error = None;
    let mut updates: FuturesUnordered<BoxFuture<'static, Result<(), AgentError>>> =
        FuturesUnordered::new();
    let mut receiving = true;
    let (executed, duration_ms) = loop {
        tokio::select! {
            result = &mut future => break result,
            update = receiver.recv(), if receiving => {
                if let Some(update) = update { updates.push(update); }
                else { receiving = false; }
            },
            result = updates.next(), if !updates.is_empty() => {
                if let Some(Err(error)) = result { update_error.get_or_insert(error); }
            }
        }
    };
    drop(admission);
    while let Ok(update) = receiver.try_recv() {
        updates.push(update);
    }
    // A rejected progress acknowledgement is a broken event boundary, not a
    // tool-domain error. It rejects the invocation before after-hooks/publication.
    if let Some(error) = update_error {
        return Err(error);
    }
    while let Some(result) = updates.next().await {
        result?;
    }
    let (mut result, mut is_error) = match executed {
        Ok(result) => {
            let is_error = result.is_error == Some(true);
            (result, is_error)
        }
        Err(error) => (error_result(error.to_string()), true),
    };
    if let Some(hook) = &options.hooks.after {
        let context = AfterToolCallContext {
            before: prepared.before.clone(),
            result: result.clone(),
            is_error,
        };
        match isolated(async { hook(context, options.cancellation.clone()).await }).await {
            Ok(Some(patch)) => {
                // Target uses nullish fallbacks; a changed content array invalidates old structured data.
                let structured = patch
                    .structured_content
                    .filter(|value| !matches!(value, JsonValue::Null));
                if let Some(content) = patch.content {
                    result.content = content;
                    result.structured_content = structured;
                } else if structured.is_some() {
                    result.structured_content = structured;
                }
                if let Some(details) = patch
                    .details
                    .filter(|value| !matches!(value, JsonValue::Null))
                {
                    result.details = Some(details);
                }
                if let Some(usage) = patch.usage {
                    result.usage = Some(usage);
                }
                if let Some(terminate) = patch.terminate {
                    result.terminate = Some(terminate);
                }
                if let Some(patch) = patch.is_error {
                    is_error = patch;
                }
            }
            Err(error) => {
                result = error_result(error.to_string());
                is_error = true;
            }
            Ok(None) => {}
        }
    }
    Ok(AgentToolCallOutcome {
        tool_call: prepared.before.tool_call,
        result,
        is_error,
        duration_ms: Some(duration_ms),
    })
}

/// Invokes a nested or direct tool through the same argument/hook boundary as the loop.
/// Emits no Agent lifecycle events and appends no transcript messages.
/// Tool-domain failures are outcomes; rejected progress acknowledgements propagate.
pub async fn run_tool_call(
    call: ToolCall,
    options: RunToolCallOptions,
) -> Result<AgentToolCallOutcome, AgentError> {
    match prepare(call, &options).await {
        Preparation::Immediate(outcome) => Ok(*outcome),
        Preparation::Prepared(prepared) => execute(*prepared, &options).await,
    }
}

async fn emit_end(
    outcome: &AgentToolCallOutcome,
    sink: &AgentEventSink,
    cancellation: &CancellationToken,
) -> Result<(), AgentError> {
    sink(
        AgentEvent::ToolExecutionEnd {
            tool_call_id: outcome.tool_call.id.to_string(),
            tool_name: outcome.tool_call.name.to_string(),
            result: outcome.result.clone(),
            is_error: outcome.is_error,
            duration_ms: outcome.duration_ms,
        },
        cancellation.clone(),
    )
    .await
}

async fn emit_result(
    outcome: &AgentToolCallOutcome,
    sink: &AgentEventSink,
    config: &crate::AgentLoopConfig,
) -> Result<ToolResultMessage, AgentError> {
    let message = ToolResultMessage {
        role: Default::default(),
        tool_call_id: outcome.tool_call.id.clone(),
        tool_name: outcome.tool_call.name.clone(),
        content: outcome.result.content.clone(),
        details: match outcome.result.details.clone() {
            None => Presence::Missing,
            Some(JsonValue::Null) => Presence::Null,
            Some(value) => Presence::Value(value),
        },
        usage: outcome.result.usage.clone(),
        nested_calls: None,
        duration_ms: outcome.duration_ms.map(crate::agent::safe_ms),
        is_error: outcome.is_error,
        timestamp: crate::agent::now(),
    };
    let event_message = config
        .message_hooks
        .start(
            AgentMessage::BuiltIn(Message::ToolResult(Box::new(message))),
            config.cancellation.clone(),
        )
        .await?;
    sink(
        AgentEvent::MessageStart {
            message: event_message.clone(),
        },
        config.cancellation.clone(),
    )
    .await?;
    let event_message = config
        .message_hooks
        .end(event_message, config.cancellation.clone())
        .await?;
    sink(
        AgentEvent::MessageEnd {
            message: event_message.clone(),
        },
        config.cancellation.clone(),
    )
    .await?;
    let AgentMessage::BuiltIn(Message::ToolResult(message)) = event_message else {
        unreachable!("message hooks retain role")
    };
    Ok(*message)
}

pub(crate) async fn execute_batch(
    context: &AgentContext,
    assistant: &AssistantMessage,
    config: &crate::AgentLoopConfig,
    sink: &AgentEventSink,
) -> Result<(Vec<ToolResultMessage>, bool), AgentError> {
    let calls = assistant
        .content
        .iter()
        .filter_map(|content| match content {
            pi_ai_core::AssistantContent::ToolCall(call) => Some(call.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    let truncated = assistant.stop_reason == pi_ai_core::StopReason::Length;
    let sequential = truncated
        || config.tool_execution == ToolExecutionMode::Sequential
        || calls.iter().any(|call| {
            context.tools.iter().any(|tool| {
                tool.declaration.name == call.name
                    && tool.execution_mode == Some(ToolExecutionMode::Sequential)
            })
        });
    let mut ready = Vec::new();
    let mut ordered = Vec::new();
    let mut messages = Vec::new();
    for call in calls {
        sink(
            AgentEvent::ToolExecutionStart {
                tool_call_id: call.id.to_string(),
                tool_name: call.name.to_string(),
                args: call.arguments.clone(),
            },
            config.cancellation.clone(),
        )
        .await?;
        let update_sink: ToolUpdateSink = {
            let (sink, cancellation, call) =
                (sink.clone(), config.cancellation.clone(), call.clone());
            Arc::new(move |partial_result| {
                let (sink, cancellation, call) = (sink.clone(), cancellation.clone(), call.clone());
                Box::pin(async move {
                    sink(
                        AgentEvent::ToolExecutionUpdate {
                            tool_call_id: call.id.to_string(),
                            tool_name: call.name.to_string(),
                            args: call.arguments,
                            partial_result,
                        },
                        cancellation,
                    )
                    .await
                })
            })
        };
        let options = RunToolCallOptions {
            tools: context.tools.clone(),
            context: context.clone(),
            assistant_message: assistant.clone(),
            cancellation: config.cancellation.clone(),
            hooks: config.tool_hooks.clone(),
            on_update: Some(update_sink),
            monotonic_clock: config.monotonic_clock.clone(),
        };
        let preparation = if truncated {
            let message = format!(
                "Tool call \"{}\" was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.",
                call.name
            );
            Preparation::Immediate(Box::new(failed(call, message)))
        } else {
            prepare(call, &options).await
        };
        match preparation {
            Preparation::Immediate(outcome) => {
                emit_end(&outcome, sink, &config.cancellation).await?;
                if sequential {
                    messages.push(emit_result(&outcome, sink, config).await?);
                }
                ordered.push(Some(*outcome));
            }
            Preparation::Prepared(prepared) if sequential => {
                let outcome = execute(*prepared, &options).await?;
                emit_end(&outcome, sink, &config.cancellation).await?;
                messages.push(emit_result(&outcome, sink, config).await?);
                ordered.push(Some(outcome));
            }
            Preparation::Prepared(prepared) => {
                let index = ordered.len();
                ordered.push(None);
                ready.push((index, prepared, options));
            }
        }
        if !truncated && config.cancellation.is_cancelled() {
            break;
        }
    }
    if !sequential {
        let completed = try_join_all(ready.into_iter().map(
            |(index, prepared, options)| async move {
                let outcome = if options.cancellation.is_cancelled() {
                    failed(prepared.before.tool_call, "Operation aborted")
                } else {
                    execute(*prepared, &options).await?
                };
                emit_end(&outcome, sink, &config.cancellation).await?;
                Ok::<_, AgentError>((index, outcome))
            },
        ))
        .await?;
        for (index, outcome) in completed {
            ordered[index] = Some(outcome);
        }
        for outcome in ordered.iter().flatten() {
            messages.push(emit_result(outcome, sink, config).await?);
        }
    }
    let terminate = !ordered.is_empty()
        && ordered
            .iter()
            .flatten()
            .all(|outcome| outcome.result.terminate == Some(true));
    Ok((messages, terminate))
}
