use std::future::{Future, poll_fn};
use std::task::{Context, Poll};

use pi_agent_schema::*;
use pi_ai_core::{
    AssistantContent, Message, Presence, SafeUnsigned, StopReason, ToolResultMessage,
};

/// Executes a target batch with source-ordered finalization and result artifacts.
/// This coordinator does not own persistence or Harness admission.
pub fn execute_tool_batch<'a, C, P>(
    input: ToolBatchInput<'a, C>,
    phases: &'a P,
) -> BoxFuture<'a, Result<ExecutedToolBatch, P::Error>>
where
    C: Sync + 'a,
    P: ToolBatchPhases<C> + ?Sized + 'a,
{
    Box::pin(batch(input, phases, false))
}

/// Compatibility coordinator preserving pinned legacy completion-order hooks/end
/// events and source-ordered messages after all preparation has finished.
pub fn execute_legacy_tool_batch<'a, C, P>(
    input: ToolBatchInput<'a, C>,
    phases: &'a P,
) -> BoxFuture<'a, Result<ExecutedToolBatch, P::Error>>
where
    C: Sync + 'a,
    P: ToolBatchPhases<C> + ?Sized + 'a,
{
    Box::pin(batch(input, phases, true))
}

type Finalization<'a, E> = (usize, BoxFuture<'a, Result<FinalizedToolCall, E>>);

async fn legacy_parallel<C: Sync, P: ToolBatchPhases<C> + ?Sized>(
    input: ToolBatchInput<'_, C>,
    prepared: &[(&AgentToolCall, ToolPreparation<P::Binding>)],
    phases: &P,
) -> Result<Vec<FinalizedToolCall>, P::Error> {
    let mut running: Vec<_> = prepared
        .iter()
        .map(|(_, preparation)| match preparation {
            ToolPreparation::Prepared(prepared) => Some(phases.execute(prepared)),
            ToolPreparation::Immediate(_) => None,
        })
        .collect();
    let mut values: Vec<_> = prepared
        .iter()
        .map(|(call, preparation)| match preparation {
            ToolPreparation::Immediate(outcome) => Some(finish_immediate(call, outcome.clone())),
            ToolPreparation::Prepared(_) => None,
        })
        .collect();
    let mut finalizing: Vec<Finalization<'_, P::Error>> = Vec::new();
    poll_fn(|cx| {
        // Promise.all's map invokes every prepared execution before any await
        // continuation. A ready A must not run its after hook before B starts.
        for (index, execution) in running.iter_mut().enumerate() {
            let Some(future) = execution else {
                continue;
            };
            match future.as_mut().poll(cx) {
                Poll::Pending => (),
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(raw)) => {
                    *execution = None;
                    let ToolPreparation::Prepared(prepared) = &prepared[index].1 else {
                        unreachable!("only prepared slots execute")
                    };
                    finalizing.push((
                        index,
                        Box::pin(async move {
                            let result = phases
                                .finalize(input.context, input.assistant_message, prepared, raw)
                                .await?;
                            end(phases, &result).await?;
                            Ok(result)
                        }),
                    ));
                }
            }
        }
        // Queue order records observed raw completion; a pending hook does not
        // prevent later hooks/end events or later execution polls from progressing.
        for (index, future) in &mut finalizing {
            if values[*index].is_none() {
                match future.as_mut().poll(cx) {
                    Poll::Pending => (),
                    Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                    Poll::Ready(Ok(value)) => values[*index] = Some(value),
                }
            }
        }
        if values.iter().all(Option::is_some) {
            Poll::Ready(Ok(values
                .iter_mut()
                .map(|value| value.take().expect("all complete"))
                .collect()))
        } else {
            Poll::Pending
        }
    })
    .await
}
// Continue polling later effects while the first source slot or its hook waits.
fn pump<T, E>(
    running: &mut [BoxFuture<'_, Result<T, E>>],
    completed: &mut [Option<T>],
    from: usize,
    cx: &mut Context<'_>,
) -> Result<(), E> {
    for index in from..running.len() {
        if completed[index].is_none() {
            match running[index].as_mut().poll(cx) {
                Poll::Ready(Ok(value)) => completed[index] = Some(value),
                Poll::Ready(Err(error)) => return Err(error),
                Poll::Pending => (),
            }
        }
    }
    Ok(())
}
fn finish_immediate(call: &AgentToolCall, outcome: ImmediateToolOutcome) -> FinalizedToolCall {
    FinalizedToolCall {
        tool_call: call.clone(),
        result: outcome.result,
        is_error: outcome.is_error,
        terminate: outcome.terminate,
    }
}
async fn end<C, P: ToolBatchPhases<C> + ?Sized>(
    phases: &P,
    value: &FinalizedToolCall,
) -> Result<(), P::Error> {
    phases
        .emit(AgentEvent::ToolExecutionEnd {
            tool_call_id: value.tool_call.id.clone(),
            tool_name: value.tool_call.name.clone(),
            result: value.result.clone(),
            is_error: value.is_error,
        })
        .await
}
fn message(value: FinalizedToolCall) -> ToolResultMessage {
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("tool result clock precedes Unix epoch")
        .as_millis();
    let millis = u64::try_from(millis).expect("tool result clock exceeds timestamp domain");
    ToolResultMessage {
        role: Default::default(),
        tool_call_id: value.tool_call.id,
        tool_name: value.tool_call.name,
        content: value.result.content,
        details: Presence::Value(value.result.details),
        usage: value.result.usage,
        added_tool_names: value
            .result
            .added_tool_names
            .filter(|names| !names.is_empty()),
        is_error: value.is_error,
        timestamp: SafeUnsigned::new(millis)
            .expect("tool result clock exceeds safe timestamp domain"),
    }
}
async fn publish<C, P: ToolBatchPhases<C> + ?Sized>(
    phases: &P,
    value: &ToolResultMessage,
) -> Result<(), P::Error> {
    let message = AgentMessage::BuiltIn(Message::ToolResult(Box::new(value.clone())));
    phases
        .emit(AgentEvent::MessageStart {
            message: LiveAgentMessage::Settled(message.clone()),
        })
        .await?;
    phases.emit(AgentEvent::MessageEnd { message }).await
}
async fn batch<C: Sync, P: ToolBatchPhases<C> + ?Sized>(
    input: ToolBatchInput<'_, C>,
    phases: &P,
    legacy: bool,
) -> Result<ExecutedToolBatch, P::Error> {
    let calls: Vec<_> = input
        .assistant_message
        .content
        .iter()
        .filter_map(|content| match content {
            AssistantContent::ToolCall(call) => Some(call),
            _ => None,
        })
        .collect();
    let sequential = input.default_mode == ToolExecutionMode::Sequential
        || calls.iter().any(|call| {
            phases.execution_mode(call.name.as_str()) == Some(ToolExecutionMode::Sequential)
        });
    let truncated = input.assistant_message.stop_reason == StopReason::Length;
    let mut prepared = Vec::new();
    let mut final_calls = Vec::new();
    let mut messages = Vec::new();
    for call in calls {
        phases
            .emit(AgentEvent::ToolExecutionStart {
                tool_call_id: call.id.clone(),
                tool_name: call.name.clone(),
                args: pi_ai_core::JsonValue::Object(call.arguments.clone()),
            })
            .await?;
        let preparation = if truncated {
            ToolPreparation::Immediate(ImmediateToolOutcome {
                result: crate::tool_error(format!(
                    "Tool call \"{}\" was not executed: the response hit the output token limit, so its arguments may be truncated. Re-issue the tool call with complete arguments.",
                    call.name
                )),
                is_error: true,
                terminate: false,
            })
        } else {
            phases
                .prepare(input.context, input.assistant_message, call)
                .await?
        };
        if sequential || truncated {
            let finalized = match preparation {
                ToolPreparation::Immediate(outcome) => finish_immediate(call, outcome),
                ToolPreparation::Prepared(prepared) => {
                    let raw = phases.execute(&prepared).await?;
                    phases
                        .finalize(input.context, input.assistant_message, &prepared, raw)
                        .await?
                }
            };
            end(phases, &finalized).await?;
            let result = message(finalized.clone());
            publish(phases, &result).await?;
            messages.push(result);
            final_calls.push(finalized);
        } else {
            if legacy && let ToolPreparation::Immediate(outcome) = &preparation {
                end(phases, &finish_immediate(call, outcome.clone())).await?;
            }
            prepared.push((call, preparation));
        }
        if !truncated && phases.is_cancelled() {
            break;
        }
    }
    if !sequential && !truncated {
        if legacy {
            final_calls = legacy_parallel(input, &prepared, phases).await?;
        } else {
            let futures: Vec<_> = prepared.iter().map(|(_, preparation)| -> BoxFuture<'_, Result<Option<ExecutedToolOutcome>, P::Error>> {
                Box::pin(async { match preparation {
                    ToolPreparation::Immediate(_) => Ok(None),
                    ToolPreparation::Prepared(prepared) => phases.execute(prepared).await.map(Some),
                } })
            }).collect();
            let mut running = futures;
            let mut completed: Vec<Option<Option<ExecutedToolOutcome>>> =
                (0..prepared.len()).map(|_| None).collect();
            for (index, (call, preparation)) in prepared.iter().enumerate() {
                let raw = poll_fn(|cx| {
                    if let Err(error) = pump(&mut running, &mut completed, index, cx) {
                        return Poll::Ready(Err(error));
                    }
                    match completed[index].take() {
                        Some(value) => Poll::Ready(Ok(value)),
                        None => Poll::Pending,
                    }
                })
                .await?;
                let finalized = match (preparation, raw) {
                    (ToolPreparation::Immediate(outcome), _) => {
                        let value = finish_immediate(call, outcome.clone());
                        end(phases, &value).await?;
                        value
                    }
                    (ToolPreparation::Prepared(prepared), Some(raw)) => {
                        let mut finalizing = Box::pin(async {
                            let value = phases
                                .finalize(input.context, input.assistant_message, prepared, raw)
                                .await?;
                            end(phases, &value).await?;
                            Ok(value)
                        });
                        poll_fn(|cx| {
                            if let Err(error) = pump(&mut running, &mut completed, index + 1, cx) {
                                return Poll::Ready(Err(error));
                            }
                            finalizing.as_mut().poll(cx)
                        })
                        .await?
                    }
                    _ => unreachable!("execution slots preserve preparation kind"),
                };
                final_calls.push(finalized);
            }
        }
        for finalized in &final_calls {
            let result = message(finalized.clone());
            publish(phases, &result).await?;
            messages.push(result);
        }
    }
    Ok(ExecutedToolBatch {
        terminate: !final_calls.is_empty() && final_calls.iter().all(|value| value.terminate),
        messages,
    })
}
