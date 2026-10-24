use pi_agent::*;
use pi_ai_core::{
    AssistantMessageEvent, AssistantStreamInit, Cancelled, Clock, DoneReason, InputContent,
    JsonValue, Model, StopReason, TextContent, create_assistant_message_event_stream,
};
use serde_json::json;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
fn model() -> Model {
    serde_json::from_value(json!({"id":"synthetic","name":"Synthetic","api":"openai-responses","provider":"openai",
        "baseUrl":"https://example.invalid/v1","reasoning":false,"input":["text"],
        "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0},"contextWindow":10000,"maxTokens":1000})).unwrap()
}
struct TestClock;
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async { Ok(()) })
    }
}
fn answer(request: AgentRequest) -> pi_ai_core::AssistantStream {
    let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
        api: request.model.api.clone(),
        provider: request.model.provider.clone(),
        model: request.model.id.clone(),
        clock: Arc::new(TestClock),
        cancellation: request.cancellation,
    });
    let mut result = failure_message(&request.model, &AgentError::Callback(String::new()), false);
    result.stop_reason = StopReason::Stop;
    result.error_message = None;
    producer
        .push(AssistantMessageEvent::Done {
            reason: DoneReason::Stop,
            message: Box::new(result),
        })
        .unwrap();
    stream
}

fn admitted_system(input: InputAdmission) -> AdmittedMessage {
    AdmittedMessage {
        message: AgentMessage::BuiltIn(pi_ai_core::Message::System(
            pi_ai_core::create_initial_system_message(Some("admitted system"), None).unwrap(),
        )),
        input: Some(input),
    }
}

fn declaration_tool() -> AgentTool {
    AgentTool::new(
        serde_json::from_value(json!({
            "name":"declared", "description":"Admission declaration fixture",
            "parameters":{"type":"object"}
        }))
        .unwrap(),
        Arc::new(|_| Box::pin(async { panic!("fixture tool must not execute") })),
    )
}

#[test]
fn cancelled_system_admissions_emit_no_message_or_history_after_tool_normalization() {
    runtime().block_on(async {
        for with_tools in [false, true] {
            let agent = Agent::new(AgentOptions::new(
                model(),
                Arc::new(|_| Box::pin(async { panic!("cancelled input must not reach provider") })),
            ));
            if with_tools {
                agent.set_tools(vec![declaration_tool()]);
            }
            let events = Arc::new(Mutex::new(Vec::new()));
            agent.subscribe_inputs(Arc::new({
                let events = events.clone();
                move |event, input, _| {
                    if matches!(
                        event,
                        AgentEvent::MessageStart { .. } | AgentEvent::MessageEnd { .. }
                    ) {
                        events.lock().unwrap().push((event, input));
                    }
                    Box::pin(async { Ok(()) })
                }
            }));
            let cancellation = CancellationToken::new();
            cancellation.cancel();
            let result = agent
                .prompt_inputs(vec![admitted_system(InputAdmission {
                    id: InputId::new(),
                    cancellation,
                    on_rejected: None,
                })])
                .await
                .unwrap();
            assert!(
                events.lock().unwrap().is_empty(),
                "cancelled system input was emitted"
            );
            assert!(result.is_empty());
            assert!(agent.state().messages.is_empty());
        }
    });
}

#[test]
fn active_system_admissions_keep_identity_and_initial_tool_declarations() {
    runtime().block_on(async {
        for with_tools in [false, true] {
            let requests = Arc::new(Mutex::new(Vec::new()));
            let agent = Agent::new(AgentOptions::new(model(), Arc::new({
                let requests = requests.clone();
                move |request| {
                    requests.lock().unwrap().push(request.clone());
                    Box::pin(async move { Ok(answer(request)) })
                }
            })));
            if with_tools {
                agent.set_tools(vec![declaration_tool()]);
            }
            let events = Arc::new(Mutex::new(Vec::new()));
            agent.subscribe_inputs(Arc::new({
                let events = events.clone();
                move |event, input, _| {
                    if matches!(&event, AgentEvent::MessageStart { message } | AgentEvent::MessageEnd { message }
                        if message.role() == "system")
                    {
                        events.lock().unwrap().push((event, input));
                    }
                    Box::pin(async { Ok(()) })
                }
            }));
            let id = InputId::new();
            agent.prompt_inputs(vec![admitted_system(InputAdmission {
                id, cancellation: CancellationToken::new(), on_rejected: None,
            })]).await.unwrap();
            let requests = requests.lock().unwrap();
            assert_eq!(requests.len(), 1, "active system admission must reach provider");
            let expected_tools = if with_tools { vec!["declared"] } else { vec![] };
            let declarations = pi_ai_core::get_current_tools(requests[0].context.messages());
            assert_eq!(declarations.iter().map(|tool| tool.name.as_str()).collect::<Vec<_>>(), expected_tools);
            let events = events.lock().unwrap();
            assert_eq!(events.len(), 2);
            assert!(events.iter().all(|(_, input)| *input == Some(id)));
            let history = agent.state().messages;
            let systems = history.iter().filter(|message| message.role() == "system").collect::<Vec<_>>();
            assert_eq!(systems.len(), 1, "normalize declarations in the admitted system record");
            assert_eq!(serde_json::to_value(systems[0]).unwrap()["content"], "admitted system");
            assert_eq!(history.last().unwrap().role(), "assistant");
        }
    });
}

#[test]
fn rejected_system_admissions_keep_their_rejection_callback() {
    runtime().block_on(async {
        let agent = Agent::new(AgentOptions::new(
            model(),
            Arc::new(|_| Box::pin(async { panic!("rejected input must not reach provider") })),
        ));
        let rejected = Arc::new(Mutex::new(Vec::new()));
        let id = InputId::new();
        agent.subscribe_inputs(Arc::new(move |event, _, _| Box::pin(async move {
            if matches!(event, AgentEvent::MessageStart { message } if message.role() == "system") {
                return Err(AgentError::Callback("system start rejected".into()));
            }
            Ok(())
        })));
        agent
            .prompt_inputs(vec![admitted_system(InputAdmission {
                id,
                cancellation: CancellationToken::new(),
                on_rejected: Some(Arc::new({
                    let rejected = rejected.clone();
                    move |error| rejected.lock().unwrap().push(error)
                })),
            })])
            .await
            .unwrap();
        assert_eq!(
            rejected.lock().unwrap().as_slice(),
            &[AgentError::Callback("system start rejected".into())]
        );
        assert!(
            agent
                .state()
                .messages
                .iter()
                .all(|message| message.role() != "system")
        );
    });
}

#[test]
fn explicit_continuation_drives_real_queues_from_empty_or_system_only_context() {
    runtime().block_on(async {
        for system_only in [false, true] {
            for steering in [false, true] {
                for mode in [QueueMode::OneAtATime, QueueMode::All] {
                    let requests = Arc::new(Mutex::new(Vec::new()));
                    let captured = requests.clone();
                    let mut options = AgentOptions::new(
                        model(),
                        Arc::new(move |request| {
                            captured.lock().unwrap().push(request.clone());
                            Box::pin(async move { Ok(answer(request)) })
                        }),
                    );
                    if system_only {
                        options.initial_state.messages.push(AgentMessage::BuiltIn(
                            pi_ai_core::Message::System(
                                pi_ai_core::create_initial_system_message(
                                    Some("system only"),
                                    None,
                                )
                                .unwrap(),
                            ),
                        ));
                    }
                    options.steering_mode = mode;
                    options.follow_up_mode = mode;
                    let agent = Agent::new(options);
                    assert_eq!(
                        agent.continue_run().await,
                        Err(AgentError::InvalidContext(
                            "No messages to continue from".into()
                        ))
                    );
                    for text in ["first", "second"] {
                        let message = serde_json::from_value(
                            json!({"role":"user","content":text,"timestamp":7}),
                        )
                        .unwrap();
                        if steering {
                            agent.steer(message);
                        } else {
                            agent.follow_up(message);
                        }
                    }
                    assert!(!agent.state().is_streaming);
                    assert!(requests.lock().unwrap().is_empty());
                    agent.continue_run().await.unwrap();
                    assert_eq!(
                        requests.lock().unwrap().len(),
                        if mode == QueueMode::All { 1 } else { 2 }
                    );
                    let users = agent
                        .state()
                        .messages
                        .iter()
                        .filter(|message| message.role() == "user")
                        .map(|message| serde_json::to_value(message).unwrap()["content"].clone())
                        .collect::<Vec<_>>();
                    assert_eq!(users, vec![json!("first"), json!("second")]);
                    assert!(!agent.has_queued_messages());
                }
            }
        }
    });
}

#[test]
fn awaited_end_listener_owns_busy_and_idle_until_acknowledged() {
    runtime().block_on(async {
        let agent = Agent::new(AgentOptions::new(
            model(),
            Arc::new(|request| Box::pin(async move { Ok(answer(request)) })),
        ));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let signal = Arc::new(Mutex::new(None));
        let subscription = agent.subscribe({
            let (entered, release, signal, agent) = (
                entered.clone(),
                release.clone(),
                signal.clone(),
                agent.clone(),
            );
            Arc::new(move |event, token| {
                let (entered, release, signal, agent) = (
                    entered.clone(),
                    release.clone(),
                    signal.clone(),
                    agent.clone(),
                );
                Box::pin(async move {
                    if matches!(event, AgentEvent::AgentEnd { .. }) {
                        assert!(agent.state().is_streaming);
                        *signal.lock().unwrap() = Some(token);
                        entered.notify_one();
                        release.notified().await;
                    }
                    Ok(())
                })
            })
        });
        let running = tokio::spawn({
            let agent = agent.clone();
            async move { agent.prompt_text("hello", Vec::new()).await }
        });
        entered.notified().await;
        assert_eq!(agent.prompt(Vec::new()).await, Err(AgentError::Busy));
        assert_eq!(agent.reset(), Err(AgentError::Busy));
        assert!(
            tokio::time::timeout(Duration::from_millis(5), agent.wait_for_idle())
                .await
                .is_err()
        );
        release.notify_one();
        running.await.unwrap().unwrap();
        agent.wait_for_idle().await;
        assert!(!agent.state().is_streaming);
        assert!(agent.signal().is_none());
        assert!(
            !signal.lock().unwrap().as_ref().unwrap().is_cancelled(),
            "ordinary settlement is not cancellation"
        );
        agent.unsubscribe(subscription);
    });
}

#[test]
fn dropping_a_prompt_future_cancels_and_releases_native_run_ownership() {
    runtime().block_on(async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let captured = Arc::new(Mutex::new(None));
        let stream: StreamFn = {
            let (entered, captured) = (entered.clone(), captured.clone());
            Arc::new(move |request| {
                let (entered, captured) = (entered.clone(), captured.clone());
                Box::pin(async move {
                    *captured.lock().unwrap() = Some(request.cancellation);
                    entered.notify_one();
                    std::future::pending().await
                })
            })
        };
        let agent = Agent::new(AgentOptions::new(model(), stream));
        let running = tokio::spawn({
            let agent = agent.clone();
            async move { agent.prompt_text("hello", Vec::new()).await }
        });
        entered.notified().await;
        let waiting = tokio::spawn({
            let agent = agent.clone();
            async move { agent.wait_for_idle().await }
        });
        running.abort();
        assert!(running.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        assert!(captured.lock().unwrap().as_ref().unwrap().is_cancelled());
        assert!(!agent.state().is_streaming);
        agent.reset().unwrap();
    });
}

#[test]
fn tool_duration_excludes_pending_progress_ack_and_hooks_and_late_updates() {
    runtime().block_on(async {
        let ticks = Arc::new(AtomicU64::new(0));
        let progress_started = Arc::new(tokio::sync::Notify::new());
        let release_progress = Arc::new(tokio::sync::Notify::new());
        let tool_returned = Arc::new(tokio::sync::Notify::new());
        let retained_update = Arc::new(Mutex::new(None));
        let updates = Arc::new(AtomicU64::new(0));
        let tool = AgentTool::new(
            serde_json::from_value(
                json!({"name":"check","description":"check","parameters":{"type":"object"}}),
            )
            .unwrap(),
            {
                let (ticks, started, returned, retained) = (
                    ticks.clone(),
                    progress_started.clone(),
                    tool_returned.clone(),
                    retained_update.clone(),
                );
                Arc::new(move |call| {
                    let (ticks, started, returned, retained) = (
                        ticks.clone(),
                        started.clone(),
                        returned.clone(),
                        retained.clone(),
                    );
                    Box::pin(async move {
                        *retained.lock().unwrap() = Some(call.on_update.clone());
                        (call.on_update)(AgentToolResult::default());
                        started.notified().await;
                        ticks.fetch_add(7, Ordering::SeqCst);
                        returned.notify_one();
                        Ok(AgentToolResult {
                            content: vec![InputContent::Text(TextContent {
                                content_type: Default::default(),
                                text: "done".into(),
                                text_signature: None,
                            })],
                            details: Some(JsonValue::Null),
                            ..Default::default()
                        })
                    })
                })
            },
        );
        let hooks = ToolCallHooks {
            before: Some({
                let ticks = ticks.clone();
                Arc::new(move |_, _| {
                    ticks.fetch_add(200, Ordering::SeqCst);
                    Box::pin(async { Ok(None) })
                })
            }),
            after: Some({
                let ticks = ticks.clone();
                Arc::new(move |_, _| {
                    ticks.fetch_add(400, Ordering::SeqCst);
                    Box::pin(async { Ok(None) })
                })
            }),
        };
        let on_update: ToolUpdateSink = {
            let (ticks, entered, release, updates) = (
                ticks.clone(),
                progress_started,
                release_progress.clone(),
                updates.clone(),
            );
            Arc::new(move |_| {
                let (ticks, entered, release, updates) = (
                    ticks.clone(),
                    entered.clone(),
                    release.clone(),
                    updates.clone(),
                );
                Box::pin(async move {
                    updates.fetch_add(1, Ordering::SeqCst);
                    entered.notify_one();
                    release.notified().await;
                    ticks.fetch_add(1000, Ordering::SeqCst);
                    Ok(())
                })
            })
        };
        let call = serde_json::from_value(
            json!({"type":"toolCall","id":"c1","name":"check","arguments":{}}),
        )
        .unwrap();
        let mut assistant = failure_message(&model(), &AgentError::Callback(String::new()), false);
        assistant.stop_reason = StopReason::ToolUse;
        assistant.error_message = None;
        let options = RunToolCallOptions {
            tools: vec![tool.clone()],
            assistant_message: assistant,
            context: AgentContext {
                messages: Vec::new(),
                tools: vec![tool],
            },
            cancellation: CancellationToken::new(),
            hooks,
            on_update: Some(on_update),
            monotonic_clock: Some({
                let ticks = ticks.clone();
                Arc::new(move || Duration::from_millis(ticks.load(Ordering::SeqCst)))
            }),
        };
        let running = tokio::spawn(run_tool_call(call, options));
        tool_returned.notified().await;
        assert!(
            !running.is_finished(),
            "progress acknowledgement must settle before tool finalization"
        );
        release_progress.notify_one();
        let result = running.await.unwrap().unwrap();
        assert!(!result.is_error);
        assert_eq!(result.duration_ms, Some(7));
        assert_eq!(ticks.load(Ordering::SeqCst), 1607);
        retained_update.lock().unwrap().as_ref().unwrap()(AgentToolResult::default());
        tokio::task::yield_now().await;
        assert_eq!(updates.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn configuration_changes_apply_to_future_runs_without_replacing_active_requests() {
    runtime().block_on(async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let original: StreamFn = {
            let (entered, release, seen) = (entered.clone(), release.clone(), seen.clone());
            Arc::new(move |request| {
                let (entered, release, seen) = (entered.clone(), release.clone(), seen.clone());
                Box::pin(async move {
                    seen.lock()
                        .unwrap()
                        .push(("original", request.options.reasoning));
                    entered.notify_one();
                    release.notified().await;
                    Ok(answer(request))
                })
            })
        };
        let agent = Agent::new(AgentOptions::new(model(), original));
        let running = tokio::spawn({
            let agent = agent.clone();
            async move { agent.prompt_text("first", Vec::new()).await }
        });
        entered.notified().await;
        let mut config = agent.configuration();
        config.options.reasoning = Some(pi_ai_core::ThinkingLevel::High);
        config.stream = {
            let seen = seen.clone();
            Arc::new(move |request| {
                let seen = seen.clone();
                Box::pin(async move {
                    seen.lock()
                        .unwrap()
                        .push(("replacement", request.options.reasoning));
                    Ok(answer(request))
                })
            })
        };
        agent.set_configuration(config);
        release.notify_one();
        running.await.unwrap().unwrap();
        agent.prompt_text("second", Vec::new()).await.unwrap();
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            &[
                ("original", None),
                ("replacement", Some(pi_ai_core::ThinkingLevel::High))
            ]
        );
        assert_eq!(agent.state().thinking_level, ThinkingLevel::High);
    });
}

#[test]
fn dropping_tool_execution_retires_its_update_callback() {
    runtime().block_on(async {
        let entered = Arc::new(tokio::sync::Notify::new());
        let retained = Arc::new(Mutex::new(None));
        let seen = Arc::new(AtomicU64::new(0));
        let declaration = serde_json::from_value(
            json!({"name":"hold","description":"hold","parameters":{"type":"object"}}),
        )
        .unwrap();
        let tool = AgentTool::new(declaration, {
            let (entered, retained) = (entered.clone(), retained.clone());
            Arc::new(move |call| {
                let (entered, retained) = (entered.clone(), retained.clone());
                Box::pin(async move {
                    *retained.lock().unwrap() = Some(call.on_update);
                    entered.notify_one();
                    std::future::pending().await
                })
            })
        });
        let call = serde_json::from_value(
            json!({"type":"toolCall","id":"hold-1","name":"hold","arguments":{}}),
        )
        .unwrap();
        let running = tokio::spawn(run_tool_call(
            call,
            RunToolCallOptions {
                tools: vec![tool],
                assistant_message: failure_message(
                    &model(),
                    &AgentError::Tool(String::new()),
                    false,
                ),
                context: AgentContext::default(),
                cancellation: CancellationToken::new(),
                hooks: ToolCallHooks::default(),
                monotonic_clock: None,
                on_update: Some({
                    let seen = seen.clone();
                    Arc::new(move |_| {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async { Ok(()) })
                    })
                }),
            },
        ));
        entered.notified().await;
        running.abort();
        assert!(running.await.unwrap_err().is_cancelled());
        retained.lock().unwrap().as_ref().unwrap()(AgentToolResult::default());
        assert_eq!(seen.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn transformed_messages_drive_dispatch_events_and_subsequent_context() {
    runtime().block_on(async {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let calls = Arc::new(AtomicU64::new(0));
        let events = Arc::new(Mutex::new(Vec::new()));
        let stream: StreamFn = {
            let requests = requests.clone();
            Arc::new(move |request| {
                let requests = requests.clone();
                Box::pin(async move {
                    let mut captured = requests.lock().unwrap();
                    let first = captured.is_empty();
                    captured.push(serde_json::to_value(&request.context.messages).unwrap());
                    drop(captured);
                    let (stream, producer) = create_assistant_message_event_stream(AssistantStreamInit {
                        api: request.model.api.clone(), provider: request.model.provider.clone(),
                        model: request.model.id.clone(), clock: Arc::new(TestClock), cancellation: request.cancellation,
                    });
                    let mut message = failure_message(&request.model, &AgentError::Tool(String::new()), false);
                    message.stop_reason = StopReason::Stop;
                    message.error_message = None;
                    message.content = vec![pi_ai_core::AssistantContent::Text(TextContent {
                        content_type: Default::default(), text: if first { "draft" } else { "done" }.into(), text_signature: None,
                    })];
                    producer.push(AssistantMessageEvent::Done { reason: DoneReason::Stop, message: Box::new(message) }).unwrap();
                    Ok(stream)
                })
            })
        };
        let mut options = AgentOptions::new(model(), stream);
        options.initial_state.tools.push(AgentTool::new(
            serde_json::from_value(json!({"name":"selected","description":"Selected by a message hook","parameters":{"type":"object"}})).unwrap(),
            {
                let calls = calls.clone();
                Arc::new(move |call| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(call.call_id, "rewritten-1");
                    Box::pin(async { Ok(AgentToolResult::default()) })
                })
            },
        ));
        options.loop_config.message_hooks.start = Some(Arc::new(|message, _| Box::pin(async move {
            let mut value = serde_json::to_value(message).unwrap();
            if value["role"] == "toolResult" { value["content"] = json!([{"type":"text","text":"start result"}]); }
            Ok(serde_json::from_value(value).unwrap())
        })));
        options.loop_config.message_hooks.end = Some(Arc::new(|message, _| Box::pin(async move {
            let mut value = serde_json::to_value(message).unwrap();
            match value["role"].as_str() {
                Some("user") => value["content"] = json!("edited user"),
                Some("toolResult") => value["content"] = json!([{"type":"text","text":"edited result"}]),
                Some("assistant") if value["content"][0]["text"] == "draft" => {
                    value["content"] = json!([{"type":"toolCall","id":"rewritten-1","name":"selected","arguments":{}}]);
                    value["stopReason"] = json!("toolUse");
                }
                _ => {}
            }
            Ok(serde_json::from_value(value).unwrap())
        })));
        let agent = Agent::new(options);
        agent.subscribe({
            let events = events.clone();
            Arc::new(move |event, _| {
                events.lock().unwrap().push(serde_json::to_value(event).unwrap());
                Box::pin(async { Ok(()) })
            })
        });
        agent.prompt_text("original user", Vec::new()).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 2, "the finalized assistant controls actual scheduling");
        let first = requests[0].as_array().unwrap();
        assert_eq!(first.iter().find(|value| value["role"] == "user").unwrap()["content"], "edited user");
        let second = requests[1].as_array().unwrap();
        assert_eq!(second.iter().find(|value| value["role"] == "assistant").unwrap()["content"][0]["name"], "selected");
        assert_eq!(second.last().unwrap()["content"][0]["text"], "edited result");
        let events = events.lock().unwrap();
        let starts = events.iter().find(|event| event["type"] == "message_start" && event["message"]["role"] == "toolResult").unwrap();
        let ends = events.iter().find(|event| event["type"] == "message_end" && event["message"]["role"] == "toolResult").unwrap();
        assert_eq!(starts["message"]["content"][0]["text"], "start result");
        assert_eq!(ends["message"]["content"][0]["text"], "edited result");
        let state = serde_json::to_value(agent.state().messages).unwrap();
        assert!(state.as_array().unwrap().iter().any(|message| message == &ends["message"]));
    });
}

#[test]
fn message_transformation_rejects_role_changes_before_request() {
    runtime().block_on(async {
        let mut config = AgentLoopConfig::new(
            model(),
            Arc::new(|_| Box::pin(async { panic!("invalid replacement must not reach provider") })),
        );
        config.message_hooks.end = Some(Arc::new(|_, _| {
            Box::pin(async {
                Ok(AgentMessage::Custom(
                    CustomMessage::new("replacement", Default::default()).unwrap(),
                ))
            })
        }));
        let prompt =
            serde_json::from_value(json!({"role":"user","content":"hello","timestamp":1})).unwrap();
        let error = run_agent_loop(
            vec![prompt],
            AgentContext::default(),
            config,
            Arc::new(|_, _| Box::pin(async { Ok(()) })),
        )
        .await
        .unwrap_err();
        assert_eq!(
            error,
            AgentError::InvalidContext("Message replacement must retain its role".into())
        );
    });
}

#[test]
fn rejected_progress_acknowledgement_rejects_call_before_after_hook() {
    runtime().block_on(async {
        let after = Arc::new(AtomicU64::new(0));
        let tool = AgentTool::new(
            serde_json::from_value(
                json!({"name":"progress","description":"progress","parameters":{"type":"object"}}),
            )
            .unwrap(),
            Arc::new(|call| {
                Box::pin(async move {
                    (call.on_update)(AgentToolResult::default());
                    (call.on_update)(AgentToolResult::default());
                    Ok(AgentToolResult::default())
                })
            }),
        );
        let options = RunToolCallOptions {
            tools: vec![tool],
            assistant_message: failure_message(&model(), &AgentError::Tool(String::new()), false),
            context: AgentContext::default(),
            cancellation: CancellationToken::new(),
            monotonic_clock: None,
            hooks: ToolCallHooks {
                before: None,
                after: Some({
                    let after = after.clone();
                    Arc::new(move |_, _| {
                        after.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async { Ok(None) })
                    })
                }),
            },
            on_update: Some({
                let index = AtomicU64::new(0);
                Arc::new(move |_| {
                    if index.fetch_add(1, Ordering::SeqCst) == 0 {
                        Box::pin(std::future::pending())
                    } else {
                        Box::pin(async { Err(AgentError::Event("progress rejected".into())) })
                    }
                })
            }),
        };
        let call = serde_json::from_value(
            json!({"type":"toolCall","id":"p1","name":"progress","arguments":{}}),
        )
        .unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), run_tool_call(call, options))
            .await
            .expect("a rejected acknowledgement must not wait for another pending acknowledgement");
        assert!(
            matches!(result, Err(AgentError::Event(message)) if message == "progress rejected")
        );
        assert_eq!(after.load(Ordering::SeqCst), 0);
    });
}

#[test]
fn rejected_progress_acknowledgement_ends_loop_before_tool_result_or_next_request() {
    runtime().block_on(async {
        let requests = Arc::new(AtomicU64::new(0));
        let after = Arc::new(AtomicU64::new(0));
        let events = Arc::new(Mutex::new(Vec::new()));
        let tool = AgentTool::new(
            serde_json::from_value(
                json!({"name":"progress","description":"progress","parameters":{"type":"object"}}),
            )
            .unwrap(),
            Arc::new(|call| {
                Box::pin(async move {
                    (call.on_update)(AgentToolResult::default());
                    Ok(AgentToolResult::default())
                })
            }),
        );
        let stream: StreamFn = {
            let requests = requests.clone();
            Arc::new(move |request| {
                requests.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    let (stream, producer) =
                        create_assistant_message_event_stream(AssistantStreamInit {
                            api: request.model.api.clone(),
                            provider: request.model.provider.clone(),
                            model: request.model.id.clone(),
                            clock: Arc::new(TestClock),
                            cancellation: request.cancellation,
                        });
                    let mut message =
                        failure_message(&request.model, &AgentError::Tool(String::new()), false);
                    message.stop_reason = StopReason::ToolUse;
                    message.error_message = None;
                    message.content = vec![pi_ai_core::AssistantContent::ToolCall(
                        serde_json::from_value(
                            json!({"type":"toolCall","id":"p1","name":"progress","arguments":{}}),
                        )
                        .unwrap(),
                    )];
                    producer
                        .push(AssistantMessageEvent::Done {
                            reason: DoneReason::ToolUse,
                            message: Box::new(message),
                        })
                        .unwrap();
                    Ok(stream)
                })
            })
        };
        let mut config = AgentLoopConfig::new(model(), stream);
        config.tool_hooks.after = Some({
            let after = after.clone();
            Arc::new(move |_, _| {
                after.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(None) })
            })
        });
        let sink: AgentEventSink = {
            let events = events.clone();
            Arc::new(move |event, _| {
                let fail = matches!(event, AgentEvent::ToolExecutionUpdate { .. });
                events
                    .lock()
                    .unwrap()
                    .push(serde_json::to_value(event).unwrap());
                Box::pin(async move {
                    if fail {
                        Err(AgentError::Event("progress rejected".into()))
                    } else {
                        Ok(())
                    }
                })
            })
        };
        let prompt =
            serde_json::from_value(json!({"role":"user","content":"hello","timestamp":1})).unwrap();
        let result = run_agent_loop(
            vec![prompt],
            AgentContext {
                messages: Vec::new(),
                tools: vec![tool],
            },
            config,
            sink,
        )
        .await;
        assert_eq!(result, Err(AgentError::Event("progress rejected".into())));
        assert_eq!(requests.load(Ordering::SeqCst), 1);
        assert_eq!(after.load(Ordering::SeqCst), 0);
        let events = events.lock().unwrap();
        assert_eq!(events.last().unwrap()["type"], "tool_execution_update");
        assert!(
            !events
                .iter()
                .any(|event| event["type"] == "tool_execution_end"
                    || event["message"]["role"] == "toolResult")
        );
    });
}

#[test]
fn empty_dynamic_api_key_keeps_configured_fallback_without_trimming_other_values() {
    runtime().block_on(async {
        for (resolved, expected) in [
            (None, "fallback-key"),
            (Some(""), "fallback-key"),
            (Some("replacement-key"), "replacement-key"),
            (Some(" "), " "),
        ] {
            let seen = Arc::new(Mutex::new(None));
            let stream: StreamFn = {
                let seen = seen.clone();
                Arc::new(move |request| {
                    *seen.lock().unwrap() = request
                        .api_key
                        .as_ref()
                        .map(|key| key.expose_secret().to_owned());
                    Box::pin(async move { Ok(answer(request)) })
                })
            };
            let mut config = AgentLoopConfig::new(model(), stream);
            config.api_key = Some(pi_ai_core::SecretString::new("fallback-key"));
            config.get_api_key = Some(Arc::new(move |_| {
                Box::pin(async move { Ok(resolved.map(pi_ai_core::SecretString::new)) })
            }));
            let prompt =
                serde_json::from_value(json!({"role":"user","content":"hello","timestamp":1}))
                    .unwrap();
            run_agent_loop(
                vec![prompt],
                AgentContext::default(),
                config,
                Arc::new(|_, _| Box::pin(async { Ok(()) })),
            )
            .await
            .unwrap();
            assert_eq!(seen.lock().unwrap().as_deref(), Some(expected));
        }
    });
}
