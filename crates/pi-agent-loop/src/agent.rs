use pi_agent_schema::*;
use pi_ai_core::{
    AssistantContent, AssistantMessage, AssistantMessageEvent, ClientIdentity, Context, Message,
    Model, SafeUnsigned, StopReason, SystemContent, SystemMessage, get_current_tools,
    get_tool_state_changes, normalize_context,
};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{
    AgentLoopConfig, AgentRequest, AgentRequestUpdate, AgentTurnContext, AgentTurnDecision,
    PrepareRequestContext, tools::execute_batch,
};

pub fn default_agent_identity() -> ClientIdentity {
    // Independent of the host's provider keys and explicit request overrides.
    serde_json::from_value(serde_json::json!({"attribution":{"name":"pi","url":"https://pi.dev","categories":["cli-agent"]}})).expect("static identity is valid")
}

pub(crate) fn safe_ms(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value.min(pi_ai_core::JS_SAFE_INTEGER_MAX)).expect("saturated timestamp")
}
pub(crate) fn now() -> SafeUnsigned {
    safe_ms(
        u64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(u64::MAX),
    )
}
pub fn requested_thinking(reasoning: Option<pi_ai_core::ThinkingLevel>) -> ThinkingLevel {
    match reasoning {
        None => ThinkingLevel::Off,
        Some(pi_ai_core::ThinkingLevel::Minimal) => ThinkingLevel::Minimal,
        Some(pi_ai_core::ThinkingLevel::Low) => ThinkingLevel::Low,
        Some(pi_ai_core::ThinkingLevel::Medium) => ThinkingLevel::Medium,
        Some(pi_ai_core::ThinkingLevel::High) => ThinkingLevel::High,
        Some(pi_ai_core::ThinkingLevel::ExtraHigh) => ThinkingLevel::ExtraHigh,
        Some(pi_ai_core::ThinkingLevel::Max) => ThinkingLevel::Max,
    }
}
pub fn provider_thinking(level: ThinkingLevel) -> Option<pi_ai_core::ThinkingLevel> {
    match level {
        ThinkingLevel::Off => None,
        ThinkingLevel::Minimal => Some(pi_ai_core::ThinkingLevel::Minimal),
        ThinkingLevel::Low => Some(pi_ai_core::ThinkingLevel::Low),
        ThinkingLevel::Medium => Some(pi_ai_core::ThinkingLevel::Medium),
        ThinkingLevel::High => Some(pi_ai_core::ThinkingLevel::High),
        ThinkingLevel::ExtraHigh => Some(pi_ai_core::ThinkingLevel::ExtraHigh),
        ThinkingLevel::Max => Some(pi_ai_core::ThinkingLevel::Max),
    }
}

fn apply(update: AgentRequestUpdate, context: &mut AgentContext, config: &mut AgentLoopConfig) {
    if let Some(value) = update.context {
        *context = value;
    }
    if let Some(value) = update.model {
        config.model = value;
    }
    if let Some(value) = update.thinking_level {
        config.options.reasoning = provider_thinking(value);
    }
}

fn declare_tools(
    context: &AgentContext,
    mut pending: Vec<AdmittedMessage>,
) -> Vec<AdmittedMessage> {
    let system_index = pending
        .iter()
        .rposition(|message| message.message.role() == "system");
    let mut baseline = pending.clone();
    if let Some(index) = system_index
        && let AgentMessage::BuiltIn(Message::System(message)) = &mut baseline[index].message
    {
        message.tools_added = None;
        message.tools_removed = None;
    }
    let committed = context
        .messages
        .iter()
        .chain(baseline.iter().map(|m| &m.message))
        .filter_map(|message| message.as_builtin().cloned())
        .collect::<Vec<_>>();
    let executable = context
        .tools
        .iter()
        .map(|tool| tool.declaration.clone())
        .collect::<Vec<_>>();
    let changes = get_tool_state_changes(&get_current_tools(&committed), &executable);
    let unchanged = changes.tools_added.is_empty() && changes.tools_removed.is_empty();
    let mut system = if let Some(index) = system_index {
        match &pending[index].message {
            AgentMessage::BuiltIn(Message::System(message)) => message.clone(),
            _ => unreachable!("system roles are typed"),
        }
    } else {
        if unchanged {
            return pending;
        }
        SystemMessage {
            role: Default::default(),
            content: SystemContent::Text(String::new()),
            sections: None,
            tools_added: None,
            tools_removed: None,
            timestamp: now(),
        }
    };
    system.tools_added = (!changes.tools_added.is_empty()).then_some(changes.tools_added);
    system.tools_removed = (!changes.tools_removed.is_empty()).then_some(changes.tools_removed);
    let message = AgentMessage::BuiltIn(Message::System(system));
    if let Some(index) = system_index {
        pending[index] = message.into();
    } else {
        let index = pending
            .iter()
            .position(|message| message.message.role() != "system")
            .unwrap_or(pending.len());
        pending.insert(index, message.into());
    }
    pending
}

async fn emit_message(
    admitted: AdmittedMessage,
    config: &AgentLoopConfig,
    sink: &AgentEventSink,
    inputs: &crate::InputLoopOptions,
) -> Result<Option<AgentMessage>, AgentError> {
    let on_rejected = admitted.input.as_ref().and_then(|i| i.on_rejected.clone());
    let result = emit_admitted_message(admitted, config, sink, inputs).await;
    if let Err(error) = &result
        && let Some(rejected) = on_rejected
    {
        rejected(error.clone());
    }
    result
}
async fn emit_admitted_message(
    admitted: AdmittedMessage,
    config: &AgentLoopConfig,
    sink: &AgentEventSink,
    inputs: &crate::InputLoopOptions,
) -> Result<Option<AgentMessage>, AgentError> {
    if admitted.is_cancelled() {
        return Ok(None);
    }
    let input_id = admitted.input.as_ref().map(|i| i.id);
    let cancelled = || {
        admitted
            .input
            .as_ref()
            .is_some_and(|i| i.cancellation.is_cancelled() || config.cancellation.is_cancelled())
    };
    let emit = |event| {
        if let Some(events) = &inputs.events {
            events(event, input_id, config.cancellation.clone())
        } else {
            sink(event, config.cancellation.clone())
        }
    };
    let message = config
        .message_hooks
        .start(admitted.message, config.cancellation.clone())
        .await?;
    if cancelled() {
        return Ok(None);
    }
    emit(AgentEvent::MessageStart {
        message: message.clone(),
    })
    .await?;
    if cancelled() {
        return Ok(None);
    }
    let message = config
        .message_hooks
        .end(message, config.cancellation.clone())
        .await?;
    if cancelled() {
        return Ok(None);
    }
    emit(AgentEvent::MessageEnd {
        message: message.clone(),
    })
    .await?;
    if cancelled() {
        return Ok(None);
    }
    Ok(Some(message))
}

pub async fn run_agent_loop(
    prompts: Vec<AgentMessage>,
    context: AgentContext,
    config: AgentLoopConfig,
    sink: AgentEventSink,
) -> Result<Vec<AgentMessage>, AgentError> {
    run_agent_loop_with_inputs(
        prompts.into_iter().map(Into::into).collect(),
        context,
        config,
        sink,
        Default::default(),
    )
    .await
}

pub async fn run_agent_loop_with_inputs(
    prompts: Vec<AdmittedMessage>,
    mut context: AgentContext,
    config: AgentLoopConfig,
    sink: AgentEventSink,
    inputs: crate::InputLoopOptions,
) -> Result<Vec<AgentMessage>, AgentError> {
    let tracked_initial = prompts.iter().any(|m| m.input.is_some());
    let initial = declare_tools(&context, prompts);
    sink(AgentEvent::AgentStart, config.cancellation.clone()).await?;
    sink(AgentEvent::TurnStart, config.cancellation.clone()).await?;
    let mut emitted = Vec::new();
    let mut emitted_tracked = false;
    for message in initial {
        let tracked = message.input.is_some();
        if let Some(message) = emit_message(message, &config, &sink, &inputs).await? {
            emitted_tracked |= tracked;
            context.messages.push(message.clone());
            emitted.push(message);
        }
    }
    if tracked_initial && !emitted_tracked {
        sink(
            AgentEvent::AgentEnd {
                messages: emitted.clone(),
            },
            config.cancellation.clone(),
        )
        .await?;
        return Ok(emitted);
    }
    run(context, emitted, config, sink, inputs).await
}

pub async fn run_agent_loop_continue(
    context: AgentContext,
    config: AgentLoopConfig,
    sink: AgentEventSink,
) -> Result<Vec<AgentMessage>, AgentError> {
    run_agent_loop_continue_with_inputs(context, config, sink, Default::default()).await
}

pub async fn run_agent_loop_continue_with_inputs(
    context: AgentContext,
    config: AgentLoopConfig,
    sink: AgentEventSink,
    inputs: crate::InputLoopOptions,
) -> Result<Vec<AgentMessage>, AgentError> {
    if context.messages.is_empty() {
        return Err(AgentError::InvalidContext(
            "Cannot continue: no messages in context".into(),
        ));
    }
    if context
        .messages
        .last()
        .is_some_and(|message| message.role() == "assistant")
    {
        return Err(AgentError::InvalidContext(
            "Cannot continue from message role: assistant".into(),
        ));
    }
    sink(AgentEvent::AgentStart, config.cancellation.clone()).await?;
    sink(AgentEvent::TurnStart, config.cancellation.clone()).await?;
    run(context, Vec::new(), config, sink, inputs).await
}

async fn poll(
    queue: &Option<crate::PollMessages>,
    admitted: &Option<crate::PollInputs>,
) -> Result<Vec<AdmittedMessage>, AgentError> {
    if let Some(queue) = admitted {
        return queue().await;
    }
    match queue {
        Some(queue) => Ok(queue().await?.into_iter().map(Into::into).collect()),
        None => Ok(Vec::new()),
    }
}

async fn run(
    mut context: AgentContext,
    mut messages: Vec<AgentMessage>,
    mut config: AgentLoopConfig,
    sink: AgentEventSink,
    inputs: crate::InputLoopOptions,
) -> Result<Vec<AgentMessage>, AgentError> {
    let mut last_turn: Option<AgentTurnContext> = None;
    let mut pending = poll(&config.get_steering_messages, &inputs.steering).await?;
    loop {
        let mut prepared = Vec::new();
        if let Some(last) = last_turn.take() {
            if let Some(prepare) = &config.prepare_next_turn
                && let Some(update) = prepare(last, config.cancellation.clone()).await?
            {
                prepared = update.messages.into_iter().map(Into::into).collect();
                apply(update.request, &mut context, &mut config);
            }
            if pending.is_empty() {
                pending = poll(&config.get_steering_messages, &inputs.steering).await?;
            }
            sink(AgentEvent::TurnStart, config.cancellation.clone()).await?;
        }
        prepared.append(&mut pending);
        for message in declare_tools(&context, prepared) {
            if let Some(message) = emit_message(message, &config, &sink, &inputs).await? {
                context.messages.push(message.clone());
                messages.push(message);
            }
        }
        if let Some(prepare) = &config.prepare_request {
            let request = PrepareRequestContext {
                context: context.clone(),
                model: config.model.clone(),
                thinking_level: requested_thinking(config.options.reasoning),
            };
            if let Some(update) = prepare(request, config.cancellation.clone()).await? {
                apply(update, &mut context, &mut config);
            }
        }
        let assistant = response(&mut context, &config, &sink).await?;
        messages.push(AgentMessage::BuiltIn(Message::Assistant(Box::new(
            assistant.clone(),
        ))));
        let hard_exit = matches!(
            assistant.stop_reason,
            StopReason::Error | StopReason::Aborted
        );
        let has_calls = assistant
            .content
            .iter()
            .any(|content| matches!(content, AssistantContent::ToolCall(_)));
        let (tool_results, terminate) = if has_calls && !hard_exit {
            execute_batch(&context, &assistant, &config, &sink).await?
        } else {
            (Vec::new(), false)
        };
        for result in &tool_results {
            let message = AgentMessage::BuiltIn(Message::ToolResult(Box::new(result.clone())));
            context.messages.push(message.clone());
            messages.push(message);
        }
        let turn = AgentTurnContext {
            message: assistant.clone(),
            tool_results: tool_results.clone(),
            context: context.clone(),
            new_messages: messages.clone(),
        };
        let decision = if let Some(finish) = &config.finish_turn {
            finish(turn.clone(), config.cancellation.clone()).await?
        } else {
            None
        };
        sink(
            AgentEvent::TurnEnd {
                message: AgentMessage::BuiltIn(Message::Assistant(Box::new(assistant))),
                tool_results,
            },
            config.cancellation.clone(),
        )
        .await?;
        if hard_exit || decision == Some(AgentTurnDecision::End) {
            break;
        }
        last_turn = Some(turn);
        pending = poll(&config.get_steering_messages, &inputs.steering).await?;
        if (has_calls && !terminate) || !pending.is_empty() {
            continue;
        }
        pending = poll(&config.get_follow_up_messages, &inputs.follow_up).await?;
        if !pending.is_empty() || decision == Some(AgentTurnDecision::Continue) {
            continue;
        }
        break;
    }
    sink(
        AgentEvent::AgentEnd {
            messages: messages.clone(),
        },
        config.cancellation.clone(),
    )
    .await?;
    Ok(messages)
}

async fn response(
    context: &mut AgentContext,
    config: &AgentLoopConfig,
    sink: &AgentEventSink,
) -> Result<AssistantMessage, AgentError> {
    let messages = if let Some(transform) = &config.transform_context {
        transform(context.messages.clone(), config.cancellation.clone()).await?
    } else {
        context.messages.clone()
    };
    let messages = (config.convert_to_llm)(messages).await?;
    let context_request = normalize_context(Context {
        messages,
        system_prompt: None,
        tools: None,
    });
    let mut api_key = config.api_key.clone();
    if let Some(resolve) = &config.get_api_key
        && let Some(resolved) = resolve(config.model.provider.to_string()).await?
        && !resolved.expose_secret().is_empty()
    {
        api_key = Some(resolved);
    }
    let mut options = config.options.clone();
    options.stream.identity = Some(pi_ai_core::merge_client_identity(
        Some(&default_agent_identity()),
        options.stream.identity.as_ref(),
    ));
    let request = AgentRequest {
        model: config.model.clone(),
        context: context_request,
        options,
        api_key,
        cancellation: config.cancellation.clone(),
    };
    let mut stream = (config.stream)(request).await?;
    let mut added = false;
    while let Some(event) = stream.next().await {
        let (partial, start) = match &event {
            AssistantMessageEvent::Start { partial } => (Some(partial.as_ref()), true),
            AssistantMessageEvent::TextStart { partial, .. }
            | AssistantMessageEvent::TextDelta { partial, .. }
            | AssistantMessageEvent::TextEnd { partial, .. }
            | AssistantMessageEvent::ThinkingStart { partial, .. }
            | AssistantMessageEvent::ThinkingDelta { partial, .. }
            | AssistantMessageEvent::ThinkingEnd { partial, .. }
            | AssistantMessageEvent::ToolCallStart { partial, .. }
            | AssistantMessageEvent::ToolCallDelta { partial, .. }
            | AssistantMessageEvent::ToolCallEnd { partial, .. } => (Some(partial.as_ref()), false),
            AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. } => break,
        };
        if let Some(partial) = partial {
            let message = AgentMessage::BuiltIn(Message::Assistant(Box::new(partial.clone())));
            if start {
                let message = config
                    .message_hooks
                    .start(message, config.cancellation.clone())
                    .await?;
                context.messages.push(message.clone());
                added = true;
                sink(
                    AgentEvent::MessageStart { message },
                    config.cancellation.clone(),
                )
                .await?;
            } else if added {
                *context.messages.last_mut().expect("started assistant") = message.clone();
                sink(
                    AgentEvent::MessageUpdate {
                        message,
                        assistant_message_event: event,
                    },
                    config.cancellation.clone(),
                )
                .await?;
            }
        }
    }
    let mut result = stream.result().await;
    result.thinking_level = Some(requested_thinking(config.options.reasoning));
    let mut message = AgentMessage::BuiltIn(Message::Assistant(Box::new(result)));
    if added {
        *context.messages.last_mut().expect("started assistant") = message.clone();
    } else {
        message = config
            .message_hooks
            .start(message, config.cancellation.clone())
            .await?;
        context.messages.push(message.clone());
        sink(
            AgentEvent::MessageStart {
                message: message.clone(),
            },
            config.cancellation.clone(),
        )
        .await?;
    }
    let message = config
        .message_hooks
        .end(message, config.cancellation.clone())
        .await?;
    *context.messages.last_mut().expect("finalized assistant") = message.clone();
    sink(
        AgentEvent::MessageEnd {
            message: message.clone(),
        },
        config.cancellation.clone(),
    )
    .await?;
    let AgentMessage::BuiltIn(Message::Assistant(result)) = message else {
        unreachable!("message hooks retain role")
    };
    Ok(*result)
}

pub fn failure_message(model: &Model, error: &AgentError, aborted: bool) -> AssistantMessage {
    AssistantMessage {
        role: Default::default(),
        content: vec![AssistantContent::Text(pi_ai_core::TextContent {
            content_type: Default::default(),
            text: String::new(),
            text_signature: None,
        })],
        api: model.api.clone(),
        provider: model.provider.clone(),
        model: model.id.clone(),
        response_model: None,
        response_id: None,
        provider_thinking_level: None,
        thinking_level: None,
        duration_ms: None,
        diagnostics: None,
        usage: pi_ai_core::Usage::zero(),
        stop_reason: if aborted {
            StopReason::Aborted
        } else {
            StopReason::Error
        },
        deferred: None,
        error_message: Some(error.to_string()),
        raw_stop_reason: None,
        end_turn: None,
        timestamp: now(),
    }
}
