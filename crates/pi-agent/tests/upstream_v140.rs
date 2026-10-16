//! Differential scenarios from the unchanged Pi v1.4.0 source oracle.
//! Expected observations are frozen under conformance/agent-v1.4.0.
mod upstream_v140_support;
use pi_agent::*;
use pi_ai_core::{self as ai, Message, ModelThinkingLevel};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
};
use std::time::Duration;
use upstream_v140_support::*;

async fn scenario(id: &str) -> Value {
    let rec = Record::new();
    if let Some(natural) = id.strip_prefix("finish-continue-") {
        let natural = natural.to_owned();
        let tools = if natural == "tool" {
            vec![echo("echo", "echo")]
        } else {
            vec![]
        };
        let responses = vec![
            if natural == "tool" {
                calls(&[call("a", "echo", json!({}))])
            } else {
                done()
            },
            done(),
        ];
        let mut cfg = config(&rec, responses, false, false, None);
        let r = rec.clone();
        let count = AtomicUsize::new(0);
        cfg.finish_turn = Some(Arc::new(move |turn, _| {
            let r = r.clone();
            let n = count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                r.mark("finishTurn",json!({"turn":turn_value(&turn),"decision":if n==0{json!("continue")}else{Value::Null}}));
                Ok((n == 0).then_some(AgentTurnDecision::Continue))
            })
        }));
        let r = rec.clone();
        let n = natural.clone();
        let count = AtomicUsize::new(0);
        cfg.get_steering_messages = Some(Arc::new(move || {
            let r = r.clone();
            let delivered = if n == "steering" && count.fetch_add(1, Ordering::SeqCst) == 1 {
                vec![user("steering")]
            } else {
                vec![]
            };
            Box::pin(async move {
                r.mark("steeringPoll", json!({"delivered":value!(&delivered)}));
                Ok(delivered)
            })
        }));
        let r = rec.clone();
        let count = AtomicUsize::new(0);
        cfg.get_follow_up_messages = Some(Arc::new(move || {
            let r = r.clone();
            let delivered = if natural == "follow-up" && count.fetch_add(1, Ordering::SeqCst) == 0 {
                vec![user("follow-up")]
            } else {
                vec![]
            };
            Box::pin(async move {
                r.mark("followUpPoll", json!({"delivered":value!(&delivered)}));
                Ok(delivered)
            })
        }));
        let r = rec.clone();
        cfg.prepare_next_turn = Some(Arc::new(move |turn, _| {
            let r = r.clone();
            Box::pin(async move {
                r.mark("prepareNextTurn", json!({"turn":turn_value(&turn)}));
                Ok(None)
            })
        }));
        run_loop(
            &rec,
            cfg,
            AgentContext {
                messages: vec![],
                tools,
            },
            vec![user("run")],
            None,
        )
        .await;
    } else if let Some(reason) = id.strip_prefix("hard-exit-") {
        let end = reason == "normal-end";
        let mut response = calls(&[call("a", "echo", json!({}))]);
        if !end {
            response.stop_reason = serde_json::from_value(json!(reason)).unwrap();
            response.error_message = Some(format!("provider {reason}"));
        }
        let mut cfg = config(&rec, vec![response], false, false, None);
        let r = rec.clone();
        cfg.finish_turn = Some(Arc::new(move |turn, _| {
            let r = r.clone();
            Box::pin(async move {
                r.mark("finishTurn", json!({"turn":turn_value(&turn)}));
                Ok(Some(if end {
                    AgentTurnDecision::End
                } else {
                    AgentTurnDecision::Continue
                }))
            })
        }));
        let r = rec.clone();
        cfg.get_steering_messages = Some(Arc::new(move || {
            let r = r.clone();
            Box::pin(async move {
                r.mark("steeringPoll", json!({}));
                Ok(vec![])
            })
        }));
        cfg.get_follow_up_messages = Some(Arc::new(|| {
            Box::pin(async { panic!("hard exit must not poll follow-up") })
        }));
        cfg.prepare_next_turn = Some(Arc::new(|_, _| {
            Box::pin(async { panic!("hard exit must not prepare next turn") })
        }));
        run_loop(
            &rec,
            cfg,
            AgentContext {
                messages: vec![],
                tools: vec![echo("echo", "executed")],
            },
            vec![user("run")],
            None,
        )
        .await;
    } else if id.starts_with("preparation-") {
        let pending = id.ends_with("pending");
        let queued = Arc::new(Mutex::new(vec![user("startup")]));
        let preparations = Arc::new(AtomicUsize::new(0));
        let mut cfg = config(
            &rec,
            vec![calls(&[call("a", "echo", json!({}))]), done()],
            false,
            false,
            None,
        );
        let r = rec.clone();
        let q = queued.clone();
        let n = preparations.clone();
        cfg.prepare_request = Some(Arc::new(move |request, _| {
            let r = r.clone();
            let q = q.clone();
            let n = n.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                r.mark("prepareRequest", json!({"request":request_value(&request)}));
                Ok(Some(if n == 0 {
                    if pending {
                        q.lock().unwrap().push(user("request-late"));
                    }
                    AgentRequestUpdate {
                        context: Some(AgentContext {
                            messages: vec![system("canonical"), user("canonical user")],
                            tools: request.context.tools,
                        }),
                        model: Some(model("replacement")),
                        thinking_level: Some(ModelThinkingLevel::High),
                    }
                } else {
                    AgentRequestUpdate {
                        thinking_level: Some(ModelThinkingLevel::Off),
                        ..Default::default()
                    }
                }))
            })
        }));
        let r = rec.clone();
        let q = queued.clone();
        cfg.get_steering_messages = Some(Arc::new(move || {
            let r = r.clone();
            let q = q.clone();
            Box::pin(async move {
                let mut q = q.lock().unwrap();
                let delivered = if q.is_empty() {
                    vec![]
                } else {
                    vec![q.remove(0)]
                };
                r.mark("steeringPoll", json!({"delivered":value!(&delivered)}));
                Ok(delivered)
            })
        }));
        let r = rec.clone();
        let q = queued.clone();
        cfg.prepare_next_turn = Some(Arc::new(move |turn, _| {
            let r = r.clone();
            let q = q.clone();
            Box::pin(async move {
                r.mark("prepareNextTurn", json!({"turn":turn_value(&turn)}));
                q.lock().unwrap().push(user("preparation-late"));
                Ok(Some(AgentLoopTurnUpdate {
                    messages: vec![system("prepared")],
                    ..Default::default()
                }))
            })
        }));
        cfg.finish_turn = Some(Arc::new(move |_, _| {
            let n = preparations.load(Ordering::SeqCst);
            Box::pin(async move { Ok((n == 2).then_some(AgentTurnDecision::End)) })
        }));
        run_loop(
            &rec,
            cfg,
            AgentContext {
                messages: vec![],
                tools: vec![echo("echo", "executed")],
            },
            vec![user("run")],
            None,
        )
        .await;
    } else if id == "transcript-tool-delta" {
        let old = echo("old", "old");
        let mut current = echo("current", "current");
        current.declaration.parameters = serde_json::from_value(schema()).unwrap();
        let baseline=serde_json::from_value(json!({"role":"system","content":"baseline","timestamp":12,"toolsAdded":[value!(&old.declaration)]})).unwrap();
        let anchor=serde_json::from_value(json!({"role":"system","content":"anchor","timestamp":12,"sections":{"note":"<note>retained</note>"},"toolsAdded":[value!(&old.declaration)],"toolsRemoved":[{"name":"invented"}]})).unwrap();
        let mut cfg = config(&rec, vec![done(), done()], false, false, None);
        let count = AtomicUsize::new(0);
        cfg.finish_turn = Some(Arc::new(move |_, _| {
            let first = count.fetch_add(1, Ordering::SeqCst) == 0;
            Box::pin(async move { Ok(first.then_some(AgentTurnDecision::Continue)) })
        }));
        cfg.prepare_next_turn = Some(Arc::new(|turn, _| {
            Box::pin(async move {
                Ok(Some(AgentLoopTurnUpdate {
                    request: AgentRequestUpdate {
                        context: Some(AgentContext {
                            messages: turn.context.messages,
                            tools: vec![],
                        }),
                        ..Default::default()
                    },
                    messages: vec![user("after removal")],
                }))
            })
        }));
        run_loop(
            &rec,
            cfg,
            AgentContext {
                messages: vec![baseline],
                tools: vec![current],
            },
            vec![system("first"), user("between"), anchor, user("last")],
            None,
        )
        .await;
    } else if id == "stream-request-boundary" {
        let mut cfg = config(&rec, vec![done()], true, false, None);
        cfg.options.reasoning = Some(ai::ThinkingLevel::Max);
        cfg.api_key = Some(ai::SecretString::new("static-fallback"));
        cfg.options.stream.identity = Some(
            serde_json::from_value(
                json!({"attribution":null,"userAgent":{"custom":"oracle app"},"version":"1.0"}),
            )
            .unwrap(),
        );
        let r = rec.clone();
        cfg.transform_context = Some(Arc::new(move |messages, _| {
            let r = r.clone();
            Box::pin(async move {
                r.mark("transformContext", json!({"messages":value!(&messages)}));
                let mut messages = messages
                    .into_iter()
                    .filter(|m| m.role() != "custom")
                    .collect::<Vec<_>>();
                messages.push(user("transformed"));
                Ok(messages)
            })
        }));
        let r = rec.clone();
        cfg.convert_to_llm = Arc::new(move |messages| {
            let r = r.clone();
            Box::pin(async move {
                r.mark("convertToLlm", json!({"messages":value!(&messages)}));
                let mut messages = messages
                    .into_iter()
                    .filter_map(|m| m.as_builtin().cloned())
                    .collect::<Vec<_>>();
                messages.push(user("converted").as_builtin().unwrap().clone());
                Ok(messages)
            })
        });
        let r = rec.clone();
        cfg.get_api_key = Some(Arc::new(move |provider| {
            let r = r.clone();
            Box::pin(async move {
                r.mark("getApiKey", json!({"provider":provider}));
                Ok(Some(ai::SecretString::new("rotated")))
            })
        }));
        let custom =
            serde_json::from_value(json!({"role":"custom","value":"host only","timestamp":14}))
                .unwrap();
        run_loop(
            &rec,
            cfg,
            AgentContext::default(),
            vec![user("original"), custom],
            None,
        )
        .await;
    } else if let Some(mode) = id.strip_prefix("tool-order-") {
        let parallel = mode == "parallel";
        let execution = if mode == "sequential" {
            ToolExecutionMode::Sequential
        } else {
            ToolExecutionMode::Parallel
        };
        let release = Gate::new();
        let saved_update: Arc<Mutex<Option<ToolUpdate>>> = Arc::new(Mutex::new(None));
        let mut tools = vec![];
        for name in ["a", "b"] {
            let r = rec.clone();
            let release = release.clone();
            let saved = saved_update.clone();
            let mut item = tool(
                name,
                Arc::new(move |invocation| {
                    let r = r.clone();
                    let release = release.clone();
                    let saved = saved.clone();
                    Box::pin(async move {
                        r.mark("execute",json!({"id":invocation.call_id,"args":value!(&invocation.arguments),"aborted":invocation.cancellation.is_cancelled()}));
                        if invocation.call_id == "a" {
                            *saved.lock().unwrap() = Some(invocation.on_update.clone());
                            (invocation.on_update)(text_result("working"));
                            if parallel {
                                release.wait().await;
                            }
                        }
                        let mut result = text_result(&invocation.call_id);
                        result.terminate = Some(true);
                        Ok(result)
                    })
                }),
            );
            if mode == "override" && name == "b" {
                item.execution_mode = Some(ToolExecutionMode::Sequential);
            }
            tools.push(item);
        }
        let mut cfg = config(
            &rec,
            vec![calls(&[
                call("a", "a", json!({})),
                call("b", "b", json!({})),
            ])],
            false,
            false,
            Some(execution),
        );
        cfg.tool_execution = execution;
        let r = rec.clone();
        cfg.tool_hooks.before = Some(Arc::new(move |context, _| {
            let r = r.clone();
            Box::pin(async move {
                r.mark(
                    "beforeToolCall",
                    json!({"id":context.tool_call.id,"args":value!(&context.args)}),
                );
                Ok(None)
            })
        }));
        let r = rec.clone();
        cfg.tool_hooks.after = Some(Arc::new(move |context, _| {
            let r = r.clone();
            Box::pin(async move {
                r.mark(
                    "afterToolCall",
                    json!({"id":context.before.tool_call.id,"result":value!(&context.result)}),
                );
                Ok(None)
            })
        }));
        let r = rec.clone();
        cfg.finish_turn = Some(Arc::new(move |turn, _| {
            let r = r.clone();
            Box::pin(async move {
                r.mark("finishTurn", json!({"turn":turn_value(&turn)}));
                Ok(None)
            })
        }));
        let r = rec.clone();
        let sink: AgentEventSink = Arc::new(move |event, _| {
            let r = r.clone();
            let release = release.clone();
            Box::pin(async move {
                r.event(&event);
                if matches!(&event,AgentEvent::ToolExecutionEnd{tool_call_id,..} if tool_call_id=="b")
                {
                    release.release();
                }
                Ok(())
            })
        });
        run_loop(
            &rec,
            cfg,
            AgentContext {
                messages: vec![],
                tools,
            },
            vec![user("run")],
            Some(sink),
        )
        .await;
        let before = rec.event_count();
        saved_update.lock().unwrap().as_ref().unwrap()(text_result("late after run"));
        tokio::task::yield_now().await;
        rec.set(
            "lateUpdate",
            json!({"before":before,"after":rec.event_count()}),
        );
    } else if id == "length-truncation" {
        let mut invalid = call("arguments-error", "echo", json!({}));
        invalid.arguments_error = Some("bad JSON".into());
        let response = assistant(
            value!([
                call("schema-valid", "echo", json!({"value":"salvaged"})),
                invalid
            ]),
            "length",
        );
        let mut cfg = config(&rec, vec![response, done()], false, false, None);
        cfg.tool_hooks.before = Some(Arc::new(|_, _| {
            Box::pin(async { panic!("truncation must skip hooks") })
        }));
        let mut t = tool(
            "echo",
            Arc::new(|_| Box::pin(async { panic!("truncated calls must not execute") })),
        );
        t.declaration.parameters = serde_json::from_value(schema()).unwrap();
        run_loop(
            &rec,
            cfg,
            AgentContext {
                messages: vec![],
                tools: vec![t],
            },
            vec![user("run")],
            None,
        )
        .await;
    } else if id.starts_with("termination-") {
        let unanimous = id.ends_with("unanimous");
        let mut responses = vec![calls(&[
            call("blocked", "echo", json!({})),
            call("executed", "echo", json!({})),
        ])];
        if !unanimous {
            responses.push(done());
        }
        let mut cfg = config(&rec, responses, false, false, None);
        cfg.tool_hooks.before = Some(Arc::new(|context, _| {
            Box::pin(async move {
                Ok(
                    (context.tool_call.id.as_str() == "blocked").then(|| BeforeToolCallResult {
                        block: true,
                        reason: Some("blocked".into()),
                        terminate: true,
                        ..Default::default()
                    }),
                )
            })
        }));
        cfg.tool_hooks.after = Some(Arc::new(move |_, _| {
            Box::pin(async move {
                Ok(Some(AfterToolCallResult {
                    terminate: Some(unanimous),
                    ..Default::default()
                }))
            })
        }));
        run_loop(
            &rec,
            cfg,
            AgentContext {
                messages: vec![],
                tools: vec![echo("echo", "ok")],
            },
            vec![user("run")],
            None,
        )
        .await;
    } else if id == "tool-validation-and-hooks" {
        validation(&rec).await;
    } else if id == "structured-content-overrides" {
        structured(&rec).await;
    } else if let Some(abort_at) = id.strip_prefix("tool-abort-") {
        let token = CancellationToken::new();
        if abort_at == "before-call" {
            token.cancel();
        }
        let during = abort_at == "before-hook";
        let r = rec.clone();
        let hooks = ToolCallHooks {
            before: Some(Arc::new(move |_, token| {
                let r = r.clone();
                Box::pin(async move {
                    r.mark("beforeToolCall", json!({"aborted":token.is_cancelled()}));
                    if during {
                        token.cancel();
                    }
                    Ok(None)
                })
            })),
            after: Some(Arc::new(|_, _| {
                Box::pin(async { panic!("immediate aborted call must skip after hook") })
            })),
        };
        let outcome = run_tool_call(
            call("a", "echo", json!({})),
            RunToolCallOptions {
                tools: vec![tool(
                    "echo",
                    Arc::new(|_| Box::pin(async { panic!("aborted tool must not execute") })),
                )],
                assistant_message: done(),
                context: AgentContext::default(),
                cancellation: token,
                hooks,
                on_update: None,
                monotonic_clock: None,
            },
        )
        .await
        .unwrap();
        rec.set("outcome", outcome_value(&outcome));
    } else if id == "progress-await-and-late" {
        progress(&rec).await;
    } else if id == "duration-excludes-hooks" {
        duration(&rec).await;
    } else if id.starts_with("agent-assistant-tail-") {
        queues(&rec, id.ends_with("-all")).await;
    } else if id == "agent-end-listener-idle-barrier" {
        idle(&rec).await;
    } else {
        panic!("unimplemented fixture driver: {id}");
    }
    rec.finish()
}

async fn validation(rec: &Record) {
    let r = rec.clone();
    let mut t = tool(
        "echo",
        Arc::new(move |invocation| {
            let r = r.clone();
            Box::pin(async move {
                r.mark(
                    "execute",
                    json!({"id":invocation.call_id,"args":value!(&invocation.arguments)}),
                );
                (invocation.on_update)(text_result("progress"));
                if invocation.call_id == "throw" {
                    return Err(AgentError::Tool("execute threw".into()));
                }
                let args = value!(&invocation.arguments);
                let arg = &args["value"];
                let mut result = text_result(
                    arg.as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| arg.to_string())
                        .as_str(),
                );
                result.structured_content =
                    Some(serde_json::from_value(json!({"value":arg})).unwrap());
                result.is_error = Some(invocation.call_id == "reported-error");
                Ok(result)
            })
        }),
    );
    t.declaration.parameters = serde_json::from_value(schema()).unwrap();
    let r = rec.clone();
    t.prepare_arguments = Some(Arc::new(move |args| {
        r.mark("prepareArguments", json!({"args":value!(&args)}));
        let raw = value!(&args);
        Ok(if raw["legacyValue"].is_string() {
            serde_json::from_value(json!({"value":raw["legacyValue"]})).unwrap()
        } else {
            args
        })
    }));
    let mut invalid = call("arguments-error", "echo", json!({"value":"valid"}));
    invalid.arguments_error = Some("unterminated JSON".into());
    let mut inputs = vec![
        call(
            "prepare-coerce-mutate",
            "echo",
            json!({"legacyValue":"prepared"}),
        ),
        call("schema-coerce", "echo", json!({"value":42})),
        call("schema-invalid", "echo", json!({"value":{}})),
        invalid,
        call("missing", "missing", json!({})),
    ];
    inputs.extend(
        ["blocked", "throw", "reported-error", "after-throw"]
            .map(|id| call(id, "echo", json!({"value":id}))),
    );
    let mut outcomes = vec![];
    for tc in inputs {
        let r = rec.clone();
        let before: BeforeToolCall = Arc::new(move |context, _| {
            let r = r.clone();
            Box::pin(async move {
                r.mark(
                    "beforeToolCall",
                    json!({"id":context.tool_call.id,"args":value!(&context.args)}),
                );
                if context.tool_call.id.as_str() == "blocked" {
                    Ok(Some(BeforeToolCallResult {
                        block: true,
                        reason: Some("policy blocked".into()),
                        terminate: true,
                        ..Default::default()
                    }))
                } else if context.tool_call.id.as_str() == "prepare-coerce-mutate" {
                    Ok(Some(BeforeToolCallResult {
                        args: Some(
                            serde_json::from_value(
                                json!({"value":{"mutatedAfterValidation":true}}),
                            )
                            .unwrap(),
                        ),
                        ..Default::default()
                    }))
                } else {
                    Ok(None)
                }
            })
        });
        let r = rec.clone();
        let after: AfterToolCall = Arc::new(move |context, _| {
            let r = r.clone();
            Box::pin(async move {
                r.mark("afterToolCall",json!({"id":context.before.tool_call.id,"args":value!(&context.before.args),"result":value!(&context.result),"isError":context.is_error}));
                if context.before.tool_call.id.as_str() == "after-throw" {
                    Err(AgentError::Tool("after hook threw".into()))
                } else {
                    Ok(None)
                }
            })
        });
        let r = rec.clone();
        let update: ToolUpdateSink = Arc::new(move |result| {
            let r = r.clone();
            Box::pin(async move {
                r.mark("update", json!({"partialResult":value!(result)}));
                Ok(())
            })
        });
        let outcome = run_tool_call(
            tc.clone(),
            RunToolCallOptions {
                tools: vec![t.clone()],
                assistant_message: calls(&[tc]),
                context: AgentContext::default(),
                cancellation: CancellationToken::new(),
                hooks: ToolCallHooks {
                    before: Some(before),
                    after: Some(after),
                },
                on_update: Some(update),
                monotonic_clock: None,
            },
        )
        .await
        .unwrap();
        let mut observed = outcome_value(&outcome);
        if outcome.tool_call.id.as_str() == "schema-invalid" {
            normalize_native_schema_invalid_diagnostic(&mut observed);
        }
        outcomes.push(observed);
    }
    rec.set("outcomes", json!(outcomes));
}
async fn structured(rec: &Record) {
    let variants = vec![
        json!({"content":[{"type":"text","text":"redacted"}]}),
        json!({"structuredContent":{"replaced":true}}),
        json!({"content":[{"type":"text","text":"redacted"}],"structuredContent":{"both":true}}),
        json!({"details":{"replacement":true}}),
        json!({"isError":false,"terminate":false}),
    ];
    let mut outcomes = vec![];
    for patch in variants {
        let t = tool(
            "echo",
            Arc::new(|_| {
                Box::pin(async {
                    Ok(serde_json::from_value(json!({"content":[{"type":"text","text":"original"}],"details":{"old":true},"structuredContent":{"original":true},"isError":true,"terminate":true,"usage":usage()})).unwrap())
                })
            }),
        );
        let after: AfterToolCall = Arc::new(move |_, _| {
            let patch = patch.clone();
            Box::pin(async move {
                Ok(Some(AfterToolCallResult {
                    content: patch
                        .get("content")
                        .map(|v| serde_json::from_value(v.clone()).unwrap()),
                    details: patch
                        .get("details")
                        .map(|v| serde_json::from_value(v.clone()).unwrap()),
                    structured_content: patch
                        .get("structuredContent")
                        .map(|v| serde_json::from_value(v.clone()).unwrap()),
                    is_error: patch["isError"].as_bool(),
                    terminate: patch["terminate"].as_bool(),
                    ..Default::default()
                }))
            })
        });
        let tc = call("a", "echo", json!({}));
        let outcome = run_tool_call(
            tc.clone(),
            RunToolCallOptions {
                tools: vec![t],
                assistant_message: calls(&[tc]),
                context: AgentContext::default(),
                cancellation: CancellationToken::new(),
                hooks: ToolCallHooks {
                    before: None,
                    after: Some(after),
                },
                on_update: None,
                monotonic_clock: None,
            },
        )
        .await
        .unwrap();
        outcomes.push(outcome_value(&outcome));
    }
    rec.set("outcomes", json!(outcomes));
}

async fn progress(rec: &Record) {
    let entered = Gate::new();
    let release_update = Gate::new();
    let b_started = Gate::new();
    let release_b = Gate::new();
    let a_ended = Gate::new();
    let saved: Arc<Mutex<Option<ToolUpdate>>> = Arc::new(Mutex::new(None));
    let s = saved.clone();
    let a = tool(
        "a",
        Arc::new(move |invocation| {
            let s = s.clone();
            Box::pin(async move {
                *s.lock().unwrap() = Some(invocation.on_update.clone());
                (invocation.on_update)(text_result("progress"));
                let mut result = text_result("a");
                result.terminate = Some(true);
                Ok(result)
            })
        }),
    );
    let start = b_started.clone();
    let release = release_b.clone();
    let b = tool(
        "b",
        Arc::new(move |_| {
            let start = start.clone();
            let release = release.clone();
            Box::pin(async move {
                start.release();
                release.wait().await;
                let mut result = text_result("b");
                result.terminate = Some(true);
                Ok(result)
            })
        }),
    );
    let r = rec.clone();
    let enter = entered.clone();
    let update = release_update.clone();
    let ended = a_ended.clone();
    let sink: AgentEventSink = Arc::new(move |event, _| {
        let r = r.clone();
        let enter = enter.clone();
        let update = update.clone();
        let ended = ended.clone();
        Box::pin(async move {
            r.event(&event);
            if matches!(event, AgentEvent::ToolExecutionUpdate { .. }) {
                enter.release();
                update.wait().await;
                r.mark("updateListenerReleased", json!({}));
            }
            if matches!(&event,AgentEvent::ToolExecutionEnd{tool_call_id,..} if tool_call_id=="a") {
                ended.release();
            }
            Ok(())
        })
    });
    let cfg = config(
        rec,
        vec![calls(&[
            call("a", "a", json!({})),
            call("b", "b", json!({})),
        ])],
        false,
        false,
        None,
    );
    let r = rec.clone();
    let running = tokio::spawn(async move {
        run_loop(
            &r,
            cfg,
            AgentContext {
                messages: vec![],
                tools: vec![a, b],
            },
            vec![user("run")],
            Some(sink),
        )
        .await;
    });
    futures_util::future::join(entered.wait(), b_started.wait()).await;
    rec.mark("whileUpdateListenerBlocked",json!({"toolEnds":rec.events().iter().filter(|event|event["type"]=="tool_execution_end").count()}));
    release_update.release();
    a_ended.wait().await;
    let before = rec.event_count();
    saved.lock().unwrap().as_ref().unwrap()(text_result("late while b active"));
    tokio::task::yield_now().await;
    rec.mark(
        "lateWhileActive",
        json!({"before":before,"after":rec.event_count()}),
    );
    release_b.release();
    running.await.unwrap();
    saved.lock().unwrap().as_ref().unwrap()(text_result("late after run"));
    tokio::task::yield_now().await;
}
async fn duration(rec: &Record) {
    let clock = Arc::new(AtomicU64::new(0));
    let release = Gate::new();
    let c = clock.clone();
    let g = release.clone();
    let t = tool(
        "echo",
        Arc::new(move |invocation| {
            let c = c.clone();
            let g = g.clone();
            Box::pin(async move {
                (invocation.on_update)(text_result("progress"));
                c.fetch_add(7, Ordering::SeqCst);
                tokio::spawn(async move {
                    tokio::task::yield_now().await;
                    tokio::task::yield_now().await;
                    g.release();
                });
                Ok(text_result("done"))
            })
        }),
    );
    let c = clock.clone();
    let before: BeforeToolCall = Arc::new(move |_, _| {
        let c = c.clone();
        Box::pin(async move {
            c.fetch_add(100, Ordering::SeqCst);
            Ok(None)
        })
    });
    let c = clock.clone();
    let after: AfterToolCall = Arc::new(move |_, _| {
        let c = c.clone();
        Box::pin(async move {
            c.fetch_add(200, Ordering::SeqCst);
            Ok(None)
        })
    });
    let c = clock.clone();
    let update: ToolUpdateSink = Arc::new(move |_| {
        let c = c.clone();
        let release = release.clone();
        Box::pin(async move {
            release.wait().await;
            c.fetch_add(300, Ordering::SeqCst);
            Ok(())
        })
    });
    let c = clock.clone();
    let outcome = run_tool_call(
        call("a", "echo", json!({})),
        RunToolCallOptions {
            tools: vec![t],
            assistant_message: done(),
            context: AgentContext::default(),
            cancellation: CancellationToken::new(),
            hooks: ToolCallHooks {
                before: Some(before),
                after: Some(after),
            },
            on_update: Some(update),
            monotonic_clock: Some(Arc::new(move || {
                Duration::from_millis(c.load(Ordering::SeqCst))
            })),
        },
    )
    .await
    .unwrap();
    rec.set("outcome", outcome_value(&outcome));
    rec.set(
        "measuredExecuteMilliseconds",
        json!(outcome.duration_ms.unwrap()),
    );
    rec.set(
        "totalSyntheticMilliseconds",
        json!(clock.load(Ordering::SeqCst)),
    );
}
async fn queues(rec: &Record, all: bool) {
    let cfg = config(
        rec,
        vec![done(); if all { 2 } else { 4 }],
        false,
        true,
        Some(ToolExecutionMode::Parallel),
    );
    rec.register_config(&cfg);
    let mut options = AgentOptions::new(model("oracle-model"), cfg.stream.clone());
    options.loop_config = cfg;
    options.initial_state = AgentInitialState {
        system_prompt: Some("baseline".into()),
        messages: vec![
            user("initial user"),
            AgentMessage::BuiltIn(Message::Assistant(Box::new(assistant(
                json!([{"type":"text","text":"initial assistant"}]),
                "stop",
            )))),
        ],
        ..Default::default()
    };
    let mode = if all {
        QueueMode::All
    } else {
        QueueMode::OneAtATime
    };
    options.steering_mode = mode;
    options.follow_up_mode = mode;
    let agent = Agent::new(options);
    let subscription = agent.subscribe(rec.sink());
    agent.steer(user("s1"));
    agent.steer(user("s2"));
    agent.follow_up(user("f1"));
    agent.follow_up(user("f2"));
    rec.mark("peek",json!({"first":value!(agent.peek_queued_messages()),"second":value!(agent.peek_queued_messages()),"has":agent.has_queued_messages()}));
    agent.continue_run().await.unwrap();
    rec.set("state", state_value(&agent));
    rec.set("queued", json!(agent.has_queued_messages()));
    agent.unsubscribe(subscription);
}
async fn idle(rec: &Record) {
    let entered = Gate::new();
    let release = Gate::new();
    let prompt_settled = Arc::new(AtomicBool::new(false));
    let idle_settled = Arc::new(AtomicBool::new(false));
    let mut cfg = config(
        rec,
        vec![done()],
        false,
        true,
        Some(ToolExecutionMode::Parallel),
    );
    cfg.finish_turn = Some(Arc::new(|_, _| {
        Box::pin(async { Ok(Some(AgentTurnDecision::End)) })
    }));
    rec.register_config(&cfg);
    let mut options = AgentOptions::new(model("oracle-model"), cfg.stream.clone());
    options.loop_config = cfg;
    options.initial_state.system_prompt = Some("baseline".into());
    let agent = Agent::new(options);
    agent.follow_up(user("f1"));
    let saved_token = Arc::new(Mutex::new(None));
    let token = saved_token.clone();
    let a = agent.clone();
    let r = rec.clone();
    let enter = entered.clone();
    let gate = release.clone();
    let first=agent.subscribe(Arc::new(move|event,signal|{let a=a.clone();let r=r.clone();let enter=enter.clone();let gate=gate.clone();let token=token.clone();Box::pin(async move{r.event(&event);
        if matches!(&event,AgentEvent::MessageEnd{message} if message.role()=="assistant"){a.steer(user("s1"));a.steer(user("s2"));}
        if matches!(event,AgentEvent::AgentEnd{..}){let state=a.state();r.mark("firstListenerEntered",json!({"streaming":state.is_streaming,"signalActive":a.signal().is_some(),"streamingMessage":state.streaming_message.map(|m|value!(m)),"pending":state.pending_tool_calls}));*token.lock().unwrap()=Some(signal);enter.release();gate.wait().await;r.mark("firstListenerReleased",json!({}));}Ok(())
    })}));
    let a = agent.clone();
    let r = rec.clone();
    let second = agent.subscribe(Arc::new(move |event, _| {
        let a = a.clone();
        let r = r.clone();
        Box::pin(async move {
            if matches!(event, AgentEvent::AgentEnd { .. }) {
                r.mark(
                    "secondListener",
                    json!({"streaming":a.state().is_streaming}),
                );
            }
            Ok(())
        })
    }));
    let a = agent.clone();
    let settled = prompt_settled.clone();
    let running = tokio::spawn(async move {
        a.prompt(vec![user("run")]).await.unwrap();
        settled.store(true, Ordering::SeqCst);
    });
    entered.wait().await;
    let a = agent.clone();
    let settled = idle_settled.clone();
    let waiting = tokio::spawn(async move {
        a.wait_for_idle().await;
        settled.store(true, Ordering::SeqCst);
    });
    tokio::task::yield_now().await;
    for result in [
        agent.prompt(vec![user("blocked")]).await.map(|_| ()),
        agent.continue_run().await.map(|_| ()),
        agent.reset(),
    ] {
        assert!(
            matches!(result, Err(AgentError::Busy)),
            "busy is a typed native rejection"
        );
    }
    rec.mark("blockedAtAgentEnd",json!({"promptSettled":prompt_settled.load(Ordering::SeqCst),"idleSettled":idle_settled.load(Ordering::SeqCst),"streaming":agent.state().is_streaming,
        "rejections":{"prompt":"Busy","continue":"Busy","reset":"Busy"},"queued":agent.has_queued_messages(),"peek":value!(agent.peek_queued_messages())}));
    // Native token handles have no public identity comparator. After capturing
    // the trace, functionally prove the listener token shares active cancellation.
    saved_token.lock().unwrap().as_ref().unwrap().cancel();
    assert!(agent.signal().unwrap().is_cancelled());
    release.release();
    running.await.unwrap();
    waiting.await.unwrap();
    rec.mark("idle",json!({"promptSettled":prompt_settled.load(Ordering::SeqCst),"idleSettled":idle_settled.load(Ordering::SeqCst),"streaming":agent.state().is_streaming,
        "signal":agent.signal().map(|s|json!({"aborted":s.is_cancelled()})),"pending":agent.state().pending_tool_calls,"peek":value!(agent.peek_queued_messages())}));
    agent.clear_steering_queue();
    rec.mark(
        "afterClearSteering",
        json!({"peek":value!(agent.peek_queued_messages())}),
    );
    rec.set("state", state_value(&agent));
    agent.unsubscribe(first);
    agent.unsubscribe(second);
}

macro_rules! source_case {
    ($name:ident,$id:literal) => {
        #[test]
        fn $name() {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let actual = runtime.block_on(async {
                tokio::time::timeout(Duration::from_secs(10), scenario($id))
                    .await
                    .expect("source scenario must settle")
            });
            compare($id, actual);
        }
    };
}
source_case!(finish_continue_none, "finish-continue-none");
source_case!(finish_continue_tool, "finish-continue-tool");
source_case!(finish_continue_steering, "finish-continue-steering");
source_case!(finish_continue_follow_up, "finish-continue-follow-up");
source_case!(hard_exit_end, "hard-exit-normal-end");
source_case!(hard_exit_error, "hard-exit-error");
source_case!(hard_exit_aborted, "hard-exit-aborted");
source_case!(preparation_repoll, "preparation-repoll");
source_case!(preparation_pending, "preparation-pending");
source_case!(transcript_tool_delta, "transcript-tool-delta");
source_case!(stream_request_boundary, "stream-request-boundary");
source_case!(tool_order_parallel, "tool-order-parallel");
source_case!(tool_order_sequential, "tool-order-sequential");
source_case!(tool_order_override, "tool-order-override");
source_case!(progress_await_and_late, "progress-await-and-late");
source_case!(tool_validation_and_hooks, "tool-validation-and-hooks");
source_case!(structured_content_overrides, "structured-content-overrides");
source_case!(length_truncation, "length-truncation");
source_case!(termination_unanimous, "termination-unanimous");
source_case!(termination_mixed, "termination-mixed");
source_case!(duration_excludes_hooks, "duration-excludes-hooks");
source_case!(tool_abort_before_call, "tool-abort-before-call");
source_case!(tool_abort_before_hook, "tool-abort-before-hook");
source_case!(
    agent_assistant_tail_one,
    "agent-assistant-tail-one-at-a-time"
);
source_case!(agent_assistant_tail_all, "agent-assistant-tail-all");
source_case!(
    agent_end_listener_idle_barrier,
    "agent-end-listener-idle-barrier"
);
