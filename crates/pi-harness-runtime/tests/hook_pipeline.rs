mod hook_pipeline_support;
use hook_pipeline_support::*;
use pi_agent_schema::*;
use pi_ai_core::*;
use pi_harness_runtime::{hooks::*, manual_gate::ManualGate, scheduler::Admission};
use pi_telemetry::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[test]
fn before_run_atomic_native_custom_validation_and_id_aggregate() {
    let env = Env::new();
    let r = HookRegistry::new();
    r.before_run(
        id("a"),
        handler::<BeforeRun>(|e| {
            assert!(e.prompt.is_empty());
            Ok(Some(BeforeRunResult {
                messages: Some(vec![custom(s("ok"))]),
                system_prompt: Some("".into()),
                resume_data: Presence::Null,
            }))
        }),
    )
    .unwrap();
    for (key, msg) in [
        ("native", invalid_native()),
        ("custom", custom(JsonValue::Bool(false))),
    ] {
        r.before_run(
            id(key),
            handler::<BeforeRun>(move |_| {
                Ok(Some(BeforeRunResult {
                    messages: Some(vec![custom(s("must not leak")), msg.clone()]),
                    system_prompt: Some("bad".into()),
                    resume_data: Presence::Value(s("bad")),
                }))
            }),
        )
        .unwrap();
    }
    r.before_run(
        id("b"),
        handler::<BeforeRun>(|e| {
            assert_eq!(e.prompt, vec![custom(s("ok"))]);
            assert_eq!(e.system_prompt, "");
            Ok(Some(BeforeRunResult {
                messages: Some(vec![]),
                system_prompt: None,
                resume_data: Presence::Value(JsonValue::Bool(false)),
            }))
        }),
    )
    .unwrap();
    let out = env.run(&r, before_run());
    let HookResult::BeforeRun(Some(a)) = out.result().unwrap() else {
        panic!()
    };
    assert_eq!(a.messages, Some(vec![custom(s("ok"))]));
    assert_eq!(a.system_prompt, Some("".into()));
    assert_eq!(
        a.resume_data,
        Presence::Value(JsonValue::Object(obj([
            ("a", JsonValue::Null),
            ("b", JsonValue::Bool(false))
        ])))
    );
    assert_eq!(env.seen.errors.lock().unwrap().len(), 2);
}

#[test]
fn before_resume_projects_each_id_for_all_three_event_variants() {
    let mut env = Env::new();
    env.saved = obj([
        ("a", JsonValue::Null),
        ("b", JsonValue::Bool(false)),
        ("other", s("private")),
    ]);
    let r = HookRegistry::new();
    let calls = Arc::new(AtomicUsize::new(0));
    for (key, want) in [
        ("a", Presence::Null),
        ("b", Presence::Value(JsonValue::Bool(false))),
        ("missing", Presence::Missing),
    ] {
        let calls = calls.clone();
        r.before_resume(
            id(key),
            handler::<BeforeResume>(move |e| {
                let got = match e {
                    BeforeResumeEvent::Run(e) => e.resume_data,
                    BeforeResumeEvent::Compaction(e) => e.resume_data,
                    BeforeResumeEvent::Navigation(e) => e.resume_data,
                };
                assert_eq!(got, want);
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }),
        )
        .unwrap();
    }
    r.before_run(id("a"), handler::<BeforeRun>(|_| Ok(None)))
        .unwrap();
    let events = [
        BeforeResumeEvent::Run(BeforeResumeRunEvent {
            lane: lane(),
            run_id: run(),
            kind: Default::default(),
            prompt: vec![],
            system_prompt_override: None,
            resume_data: Presence::Value(s("ignored")),
        }),
        BeforeResumeEvent::Compaction(BeforeResumeCompactionEvent {
            lane: lane(),
            run_id: run(),
            kind: Default::default(),
            source_leaf_id: None,
            custom_instructions: None,
            resume_data: Presence::Value(s("ignored")),
        }),
        BeforeResumeEvent::Navigation(BeforeResumeNavigationEvent {
            lane: lane(),
            run_id: run(),
            kind: Default::default(),
            source_leaf_id: None,
            target_id: None,
            summarize: true,
            label: None,
            custom_instructions: None,
            resume_data: Presence::Value(s("ignored")),
        }),
    ];
    for e in events {
        assert_eq!(
            env.run(&r, HookEvent::BeforeResume(e)).result(),
            Some(&HookResult::BeforeResume)
        );
    }
    assert_eq!(calls.load(Ordering::SeqCst), 9);
}

#[test]
fn request_full_options_clear_then_recase_and_all_eight_fields() {
    let env = Env::new();
    let r = HookRegistry::new();
    let base = AgentHarnessStreamOptions {
        headers: Some(
            RawProviderHeaders::new([("X-Demo".into(), "old".into())].into())
                .unwrap()
                .classify(&env.policy)
                .unwrap(),
        ),
        metadata: Some(obj([("old", s("old"))])),
        ..Default::default()
    };
    r.before_request(
        None,
        handler::<BeforeRequest>(|e| {
            assert!(
                e.stream_options
                    .headers
                    .unwrap()
                    .as_map()
                    .contains_key("X-Demo")
            );
            Ok(Some(BeforeRequestResult {
                stream_options: Some(AgentHarnessStreamOptionsPatch {
                    headers: Presence::Null,
                    metadata: Presence::Null,
                    ..Default::default()
                }),
            }))
        }),
    )
    .unwrap();
    let patch = RawProviderHeaderPatch::new([("x-demo".into(), Some("new".into()))].into())
        .unwrap()
        .classify(&env.policy)
        .unwrap();
    r.before_request(
        None,
        handler::<BeforeRequest>(move |e| {
            assert!(e.stream_options.headers.is_none());
            assert!(e.stream_options.metadata.is_none());
            Ok(Some(BeforeRequestResult {
                stream_options: Some(AgentHarnessStreamOptionsPatch {
                    transport: Some(Transport::Sse),
                    timeout_ms: Some(n(0)),
                    max_retries: Some(n(0)),
                    max_retry_delay_ms: Some(n(0)),
                    headers: Presence::Value(patch.clone()),
                    metadata: Presence::Value(
                        MetadataPatch::new(obj([("null", JsonValue::Null)]), vec![]).unwrap(),
                    ),
                    cache_retention: Some(CacheRetention::None),
                    deferred: Some(DeferredRequest::Enabled(false)),
                }),
            }))
        }),
    )
    .unwrap();
    let out = env.run(&r, request(base));
    let o = out.request_options().unwrap();
    assert!(out.result().is_none());
    assert_eq!(
        o.headers.as_ref().unwrap().as_map(),
        &[("x-demo".into(), "new".into())].into()
    );
    assert_eq!(o.metadata, Some(obj([("null", JsonValue::Null)])));
    assert_eq!(o.transport, Some(Transport::Sse));
    assert_eq!(o.timeout_ms, Some(n(0)));
    assert_eq!(o.max_retries, Some(n(0)));
    assert_eq!(o.max_retry_delay_ms, Some(n(0)));
    assert_eq!(o.cache_retention, Some(CacheRetention::None));
    assert_eq!(o.deferred, Some(DeferredRequest::Enabled(false)));
    assert!(
        RawProviderHeaderPatch::from_entries([
            ("X-Demo".into(), None),
            ("x-demo".into(), Some("new".into()))
        ])
        .is_err()
    );
}

#[test]
fn request_rejects_absent_cross_policy_deletion_atomically_and_duplicate_effective_map() {
    let env = Env::new();
    let r = HookRegistry::new();
    let foreign = ProviderHeaderPolicy::from_allowlist(["x-denied".into()]);
    let deletion = RawProviderHeaderPatch::new([("x-denied".into(), None)].into())
        .unwrap()
        .classify(&foreign)
        .unwrap();
    r.before_request(
        None,
        handler::<BeforeRequest>(move |_| {
            Ok(Some(BeforeRequestResult {
                stream_options: Some(AgentHarnessStreamOptionsPatch {
                    timeout_ms: Some(n(42)),
                    metadata: Presence::Null,
                    headers: Presence::Value(deletion.clone()),
                    ..Default::default()
                }),
            }))
        }),
    )
    .unwrap();
    let base = AgentHarnessStreamOptions {
        metadata: Some(obj([("keep", JsonValue::Bool(false))])),
        ..Default::default()
    };
    let expected = base.clone();
    r.before_request(
        None,
        handler::<BeforeRequest>(move |e| {
            assert_eq!(e.stream_options, expected);
            Ok(None)
        }),
    )
    .unwrap();
    assert_eq!(
        env.run(&r, request(base.clone())).request_options(),
        Some(&base)
    );
    assert_eq!(env.seen.errors.lock().unwrap().len(), 1);
    let old = AgentHarnessStreamOptions {
        headers: Some(
            RawProviderHeaders::new([("X-Demo".into(), "old".into())].into())
                .unwrap()
                .classify(&env.policy)
                .unwrap(),
        ),
        ..Default::default()
    };
    let p = AgentHarnessStreamOptionsPatch {
        headers: Presence::Value(
            RawProviderHeaderPatch::new([("x-demo".into(), Some("new".into()))].into())
                .unwrap()
                .classify(&env.policy)
                .unwrap(),
        ),
        ..Default::default()
    };
    assert!(apply_stream_options_patch(&old, &p, &env.policy).is_err());
    assert_eq!(
        env.run(&HookRegistry::new(), request(old.clone()))
            .request_options(),
        Some(&old)
    );
}

#[test]
fn request_exact_delete_keep_empty_and_metadata_null_data() {
    let env = Env::new();
    let base = AgentHarnessStreamOptions {
        metadata: Some(obj([("delete", s("x")), ("keep", s("y"))])),
        headers: Some(
            RawProviderHeaders::new([("X-Demo".into(), "old".into())].into())
                .unwrap()
                .classify(&env.policy)
                .unwrap(),
        ),
        ..Default::default()
    };
    let p = AgentHarnessStreamOptionsPatch {
        headers: Presence::Value(
            RawProviderHeaderPatch::new([("X-Demo".into(), None)].into())
                .unwrap()
                .classify(&env.policy)
                .unwrap(),
        ),
        metadata: Presence::Value(
            MetadataPatch::new(obj([("null", JsonValue::Null)]), vec!["delete".into()]).unwrap(),
        ),
        ..Default::default()
    };
    let o = apply_stream_options_patch(&base, &p, &env.policy).unwrap();
    assert!(o.headers.as_ref().unwrap().as_map().is_empty());
    assert_eq!(
        o.metadata,
        Some(obj([("keep", s("y")), ("null", JsonValue::Null)]))
    );
    assert_eq!(
        apply_stream_options_patch(&o, &Default::default(), &env.policy).unwrap(),
        o
    );
}

#[test]
fn transform_context_replaces_valid_messages_and_isolates_invalid_replacement() {
    let env = Env::new();
    let r = HookRegistry::new();
    r.transform_context(
        None,
        handler::<TransformContext>(|_| {
            Ok(Some(TransformContextResult {
                messages: vec![custom(s("ok"))],
            }))
        }),
    )
    .unwrap();
    r.transform_context(
        None,
        handler::<TransformContext>(|_| {
            Ok(Some(TransformContextResult {
                messages: vec![invalid_native()],
            }))
        }),
    )
    .unwrap();
    r.transform_context(
        None,
        handler::<TransformContext>(|e| {
            assert_eq!(e.messages, vec![custom(s("ok"))]);
            Ok(Some(TransformContextResult { messages: vec![] }))
        }),
    )
    .unwrap();
    let out = env.run(
        &r,
        HookEvent::TransformContext(TransformContextEvent {
            lane: lane(),
            run_id: run(),
            messages: vec![],
        }),
    );
    assert_eq!(
        out.result(),
        Some(&HookResult::TransformContext(Some(
            TransformContextResult { messages: vec![] }
        )))
    );
    assert_eq!(env.seen.errors.lock().unwrap().len(), 1);
}

#[test]
fn before_run_end_latest_defined_empty_and_payload_scalars() {
    let env = Env::new();
    let r = HookRegistry::new();
    for follow in [Some("first"), None, Some("")] {
        r.before_run_end(
            None,
            handler::<BeforeRunEnd>(move |_| {
                Ok(Some(BeforeRunEndResult {
                    follow_up: follow.map(str::to_owned),
                }))
            }),
        )
        .unwrap();
    }
    let out = env.run(
        &r,
        HookEvent::BeforeRunEnd(BeforeRunEndEvent {
            lane: lane(),
            run_id: run(),
            messages: vec![],
        }),
    );
    assert_eq!(
        out.result(),
        Some(&HookResult::BeforeRunEnd(Some(BeforeRunEndResult {
            follow_up: Some("".into())
        })))
    );
    for (prior, next) in [
        (s("start"), JsonValue::Bool(false)),
        (JsonValue::Bool(false), JsonValue::Null),
        (JsonValue::Null, s("")),
    ] {
        r.before_payload(
            None,
            handler::<BeforePayload>(move |e| {
                assert_eq!(e.payload, prior);
                assert_eq!(e.model, model());
                Ok(Some(BeforePayloadResult {
                    payload: next.clone(),
                }))
            }),
        )
        .unwrap();
    }
    assert_eq!(
        env.run(
            &r,
            HookEvent::BeforePayload(Box::new(BeforePayloadEvent {
                lane: lane(),
                run_id: run(),
                model: model(),
                payload: s("start")
            }))
        )
        .result(),
        Some(&HookResult::BeforePayload(Some(BeforePayloadResult {
            payload: s("")
        })))
    );
}

#[test]
fn before_tool_real_normalization_first_block_and_failure_stop() {
    for failed in [false, true] {
        let env = Env::new();
        let r = HookRegistry::new();
        r.before_tool(
            None,
            handler::<BeforeTool>(|e| {
                assert_eq!(e.args.as_map()["count"], s("1"));
                Ok(Some(BeforeToolResult {
                    args: Some(obj([("count", s("2")), ("optional", JsonValue::Null)])),
                    block: None,
                }))
            }),
        )
        .unwrap();
        r.before_tool(
            None,
            handler::<BeforeTool>(move |e| {
                assert!(!e.args.as_map().contains_key("optional"));
                assert_eq!(
                    e.args.as_map()["count"],
                    JsonValue::Number(JsonNumber::Unsigned(n(2)))
                );
                if failed {
                    Err(err())
                } else {
                    Ok(Some(BeforeToolResult {
                        args: None,
                        block: Some(BeforeToolBlock {
                            reason: "blocked".into(),
                            terminate: Some(false),
                        }),
                    }))
                }
            }),
        )
        .unwrap();
        let later = Arc::new(AtomicUsize::new(0));
        let c = later.clone();
        r.before_tool(
            None,
            handler::<BeforeTool>(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(Some(BeforeToolResult {
                    args: Some(obj([("count", s("999"))])),
                    block: None,
                }))
            }),
        )
        .unwrap();
        let out = env.run(&r, before_tool());
        let HookResult::BeforeTool(Some(r)) = out.result().unwrap() else {
            panic!()
        };
        assert_eq!(later.load(Ordering::SeqCst), 0);
        assert_eq!(
            r.block.as_ref().unwrap().terminate,
            if failed { None } else { Some(false) }
        );
        assert_eq!(env.seen.errors.lock().unwrap().len(), usize::from(failed));
        assert!(r.args.is_some());
    }
}

#[test]
fn before_tool_invalid_args_and_block_is_atomic_failure() {
    let env = Env::new();
    let r = HookRegistry::new();
    r.before_tool(
        None,
        handler::<BeforeTool>(|_| {
            Ok(Some(BeforeToolResult {
                args: Some(obj([("count", s("bad"))])),
                block: Some(BeforeToolBlock {
                    reason: "unvalidated".into(),
                    terminate: Some(true),
                }),
            }))
        }),
    )
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let c = calls.clone();
    r.before_tool(
        None,
        handler::<BeforeTool>(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }),
    )
    .unwrap();
    let out = env.run(&r, before_tool());
    let HookResult::BeforeTool(Some(r)) = out.result().unwrap() else {
        panic!()
    };
    assert_eq!(r.args, None);
    assert_eq!(r.block.as_ref().unwrap().terminate, None);
    assert_ne!(r.block.as_ref().unwrap().reason, "unvalidated");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(env.seen.errors.lock().unwrap().len(), 1);
}

#[test]
fn after_tool_chains_false_empty_null_usage_and_output_only_terminate() {
    let env = Env::new();
    let r = HookRegistry::new();
    r.after_tool(
        None,
        handler::<AfterTool>(|_| {
            Ok(Some(AfterToolResult {
                content: Some(vec![]),
                details: Presence::Null,
                is_error: Some(false),
                usage: Some(Usage::zero()),
                terminate: Some(true),
            }))
        }),
    )
    .unwrap();
    r.after_tool(
        None,
        handler::<AfterTool>(|e| {
            assert!(e.content.is_empty());
            assert_eq!(e.details, Presence::Null);
            assert!(!e.is_error);
            assert_eq!(e.usage, Some(Usage::zero()));
            Ok(Some(AfterToolResult {
                terminate: Some(false),
                ..Default::default()
            }))
        }),
    )
    .unwrap();
    let out = env.run(
        &r,
        HookEvent::AfterTool(AfterToolEvent {
            lane: lane(),
            run_id: run(),
            tool_call_id: ToolCallId::new("c").unwrap(),
            tool_name: ToolName::new("tool").unwrap(),
            args: Default::default(),
            content: vec![],
            details: Presence::Value(s("old")),
            is_error: true,
            usage: None,
        }),
    );
    let HookResult::AfterTool(Some(r)) = out.result().unwrap() else {
        panic!()
    };
    assert_eq!(r.terminate, Some(false));
    assert_eq!(r.details, Presence::Null);
    assert_eq!(r.content, Some(vec![]));
    assert_eq!(r.is_error, Some(false));
    assert_eq!(r.usage, Some(Usage::zero()));
}

#[test]
fn structural_continue_invalid_then_first_valid_supply_stops() {
    let env = Env::new();
    let r = HookRegistry::new();
    for result in [
        None,
        Some(BeforeCompactionResult::Continue(Default::default())),
        Some(BeforeCompactionResult::DeclineFalse(Default::default())),
        Some(compact(vec![invalid_native()])),
        Some(compact(vec![custom(s("ok"))])),
    ] {
        r.before_compaction(
            None,
            handler::<BeforeCompaction>(move |_| Ok(result.clone())),
        )
        .unwrap();
    }
    let later = Arc::new(AtomicUsize::new(0));
    let c = later.clone();
    r.before_compaction(
        None,
        handler::<BeforeCompaction>(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(Some(
                BeforeCompactionResult::DeclineTrue(Default::default()),
            ))
        }),
    )
    .unwrap();
    let out = env.run(
        &r,
        HookEvent::BeforeCompaction(Box::new(BeforeCompactionEvent {
            lane: lane(),
            run_id: run(),
            reason: HookCompactionReason::Manual,
            preparation: preparation(),
            custom_instructions: None,
        })),
    );
    assert_eq!(
        out.result(),
        Some(&HookResult::BeforeCompaction(Some(Box::new(compact(
            vec![custom(s("ok"))]
        )))))
    );
    assert_eq!(later.load(Ordering::SeqCst), 0);
    assert_eq!(env.seen.errors.lock().unwrap().len(), 1);
    for result in [
        None,
        Some(BeforeNavigationResult::Continue(Default::default())),
        Some(BeforeNavigationResult::DeclineFalse(Default::default())),
        Some(BeforeNavigationResult::Summary(BeforeNavigationSupply {
            decline: None,
            summary: BranchSummaryResult {
                summary: "".into(),
                usage: None,
                read_files: vec![],
                modified_files: vec![],
            },
        })),
    ] {
        r.before_navigation(
            None,
            handler::<BeforeNavigation>(move |_| Ok(result.clone())),
        )
        .unwrap();
    }
    let c = later.clone();
    r.before_navigation(
        None,
        handler::<BeforeNavigation>(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }),
    )
    .unwrap();
    let out = env.run(
        &r,
        HookEvent::BeforeNavigation(Box::new(BeforeNavigationEvent {
            lane: lane(),
            run_id: run(),
            target_id: EntryId::new("target").unwrap(),
            preparation: BranchPreparation {
                messages: vec![],
                file_ops: Default::default(),
                total_tokens: n(0),
            },
            custom_instructions: None,
        })),
    );
    assert!(matches!(
        out.result(),
        Some(HookResult::BeforeNavigation(Some(
            BeforeNavigationResult::Summary(_)
        )))
    ));
    assert_eq!(later.load(Ordering::SeqCst), 0);
}

#[test]
fn after_response_cancellation_legality_preserves_metadata_and_started_abort_finishes() {
    for abort in [false, true] {
        let env = Env::new();
        let r = HookRegistry::new();
        let signal = env.signal.clone();
        r.after_response(
            None,
            handler::<AfterResponse>(move |_| {
                signal.0.store(abort, Ordering::SeqCst);
                Ok(Some(AfterResponseResult {
                    message: Some(
                        SettledAssistantMessage::new(assistant(StopReason::Aborted)).unwrap(),
                    ),
                }))
            }),
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        r.after_response(
            None,
            handler::<AfterResponse>(move |e| {
                assert_eq!(
                    e.message.as_message().stop_reason,
                    if abort {
                        StopReason::Aborted
                    } else {
                        StopReason::Stop
                    }
                );
                assert_eq!(e.status, Some(n(201)));
                assert_eq!(e.headers.as_ref().unwrap().as_map()["x-response"], "value");
                c.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }),
        )
        .unwrap();
        let out = env.run(
            &r,
            HookEvent::AfterResponse(Box::new(AfterResponseEvent {
                lane: lane(),
                run_id: run(),
                status: Some(n(201)),
                headers: Some(
                    ResponseHeaders::new([("x-response".into(), "value".into())].into()).unwrap(),
                ),
                message: SettledAssistantMessage::new(assistant(StopReason::Stop)).unwrap(),
            })),
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(env.seen.errors.lock().unwrap().len(), usize::from(!abort));
        assert!(matches!(out.result(), Some(HookResult::AfterResponse(Some(_)))) == abort);
    }
}

#[test]
fn capture_reentry_stale_unsubscribe_and_drop_handle_preserves_registration() {
    let env = Env::new();
    let r = HookRegistry::new();
    let history = Arc::new(Mutex::new(vec![]));
    let old = r
        .before_run(
            id("reuse"),
            handler::<BeforeRun>({
                let h = history.clone();
                move |_| {
                    h.lock().unwrap().push("old");
                    Ok(None)
                }
            }),
        )
        .unwrap();
    assert!(
        r.before_run(id("reuse"), handler::<BeforeRun>(|_| Ok(None)))
            .is_err()
    );
    let capture = r.capture(HookName::BeforeRun);
    assert_eq!(
        capture.revision(),
        r.capture(HookName::BeforeRun).revision()
    );
    old.unsubscribe();
    old.unsubscribe();
    let new = r
        .before_run(
            id("reuse"),
            handler::<BeforeRun>({
                let h = history.clone();
                move |_| {
                    h.lock().unwrap().push("new");
                    Ok(None)
                }
            }),
        )
        .unwrap();
    old.unsubscribe();
    drop(new);
    assert_eq!(r.capture(HookName::BeforeRun).len(), 1);
    assert_ne!(
        capture.revision(),
        r.capture(HookName::BeforeRun).revision()
    );
    ready(capture.run_started(before_run(), &env.context())).unwrap();
    env.run(&r, before_run());
    assert_eq!(*history.lock().unwrap(), vec!["old", "new"]);
    let holder: Arc<Mutex<Option<Arc<dyn HookSubscription>>>> = Arc::new(Mutex::new(None));
    let r2 = r.clone();
    let holder2 = holder.clone();
    *holder.lock().unwrap() = Some(
        r.before_run(
            id("reenter"),
            handler::<BeforeRun>(move |_| {
                holder2.lock().unwrap().as_ref().unwrap().unsubscribe();
                r2.before_run(id("nested"), handler::<BeforeRun>(|_| Ok(None)))
                    .unwrap();
                assert_eq!(r2.capture(HookName::BeforeRun).len(), 2);
                Ok(None)
            }),
        )
        .unwrap(),
    );
    let captured = r.capture(HookName::BeforeRun);
    env.run(&r, before_run());
    assert_eq!(captured.len(), 2);
    assert_eq!(r.capture(HookName::BeforeRun).len(), 2);
}

#[test]
fn captured_callback_destructor_reenters_outside_lock_and_no_subscription_cycle() {
    struct Retired {
        r: HookRegistry,
        drops: Arc<AtomicUsize>,
    }
    impl Drop for Retired {
        fn drop(&mut self) {
            let _ = self.r.capture(HookName::BeforeRun);
            self.r
                .before_run(id("from-drop"), handler::<BeforeRun>(|_| Ok(None)))
                .unwrap();
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    let r = HookRegistry::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let retired = Retired {
        r: r.clone(),
        drops: drops.clone(),
    };
    let h = handler::<BeforeRun>(move |_| {
        let _ = &retired;
        Ok(None)
    });
    let weak = Arc::downgrade(&h);
    let sub = r.before_run(id("old"), h).unwrap();
    let captured = r.capture(HookName::BeforeRun);
    sub.unsubscribe();
    assert!(weak.upgrade().is_some());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(captured);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(weak.upgrade().is_none());
    assert_eq!(r.capture(HookName::BeforeRun).len(), 1);
    let r = HookRegistry::new();
    let h = handler::<BeforePayload>(|_| Ok(None));
    let weak = Arc::downgrade(&h);
    let retained_sub = r.before_payload(None, h).unwrap();
    drop(r);
    assert!(weak.upgrade().is_none());
    retained_sub.unsubscribe();
}

#[test]
fn ordinary_errors_construction_poll_panics_order_and_passive_telemetry() {
    let env = Env::new();
    let r = HookRegistry::new();
    let seen = env.seen.clone();
    r.before_run_end(
        Some(id("ordinary")),
        handler::<BeforeRunEnd>(move |_| {
            seen.mark("ordinary");
            Err(err())
        }),
    )
    .unwrap();
    r.before_run_end(
        None,
        handler::<BeforeRunEnd>(|_| panic!("private panic payload")),
    )
    .unwrap();
    r.before_run_end(
        None,
        async_handler::<BeforeRunEnd>(|_| Box::pin(async { panic!("private polling payload") })),
    )
    .unwrap();
    let seen = env.seen.clone();
    r.before_run_end(
        None,
        handler::<BeforeRunEnd>(move |_| {
            seen.mark("survivor");
            Ok(None)
        }),
    )
    .unwrap();
    let mut context = env.context();
    context.operation_accepted = false;
    context.recovery = true;
    ready(r.capture(HookName::BeforeRunEnd).run_started(
        HookEvent::BeforeRunEnd(BeforeRunEndEvent {
            lane: lane(),
            run_id: run(),
            messages: vec![],
        }),
        &context,
    ))
    .unwrap();
    let errors = env.seen.errors.lock().unwrap();
    assert_eq!(errors.len(), 3);
    assert_eq!(errors[0].error, "ordinary bounded error");
    assert_eq!(errors[1].error, "hook handler panicked");
    assert_eq!(errors[2].error, "hook handler panicked");
    assert!(errors.iter().all(|e| e.recovery.is_some()
        && e.stack.is_none()
        && e.lane == lane()
        && e.hook == "before_run_end"));
    assert_eq!(
        *env.seen.trace.lock().unwrap(),
        vec![
            "start", "ordinary", "error", "finish", "start", "error", "finish", "start", "error",
            "finish", "start", "survivor", "finish"
        ]
    );
    let starts = env.seen.starts.lock().unwrap();
    assert_eq!(starts.len(), 4);
    for s in starts.iter() {
        assert_eq!(s.name, "pi.harness.hook");
        assert!(!s.attributes.contains_key("pi.operation.id"));
        assert!(s.attributes.keys().all(|k| {
            ["pi.lane.name", "pi.hook.name", "pi.hook.registration_id"].contains(&k.as_str())
        }));
    }
    assert_eq!(
        starts[0].attributes["pi.hook.registration_id"],
        AttributeValue::String("ordinary".into())
    );
    let statuses = env.seen.statuses.lock().unwrap();
    assert!(
        statuses[..3]
            .iter()
            .all(|s| matches!(s, SpanStatus::Error { error: None }))
    );
    assert_eq!(statuses[3], SpanStatus::Ok);
    let ends = env.seen.ends.lock().unwrap();
    for end in &ends[..3] {
        assert_eq!(
            end["pi.hook.outcome"],
            AttributeValue::String("failed".into())
        );
    }
    assert_eq!(
        ends[3]["pi.hook.outcome"],
        AttributeValue::String("skipped".into())
    );
}

#[test]
fn pending_started_hook_survives_abort_and_drop_finishes_span_without_receipt() {
    for abandoned in [false, true] {
        let env = Env::new();
        let r = HookRegistry::new();
        let released = Arc::new(AtomicBool::new(false));
        let release = released.clone();
        r.before_run_end(
            None,
            async_handler::<BeforeRunEnd>(move |_| {
                let release = release.clone();
                Box::pin(async move {
                    std::future::poll_fn(|_| {
                        if release.load(Ordering::SeqCst) {
                            std::task::Poll::Ready(())
                        } else {
                            std::task::Poll::Pending
                        }
                    })
                    .await;
                    Ok(Some(BeforeRunEndResult {
                        follow_up: Some("done".into()),
                    }))
                })
            }),
        )
        .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        r.before_run_end(
            None,
            handler::<BeforeRunEnd>(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }),
        )
        .unwrap();
        let captured = r.capture(HookName::BeforeRunEnd);
        let context = env.context();
        let mut future = Box::pin(captured.run_started(
            HookEvent::BeforeRunEnd(BeforeRunEndEvent {
                lane: lane(),
                run_id: run(),
                messages: vec![],
            }),
            &context,
        ));
        assert!(poll(future.as_mut()).is_pending());
        env.signal.0.store(true, Ordering::SeqCst);
        assert!(poll(future.as_mut()).is_pending());
        if abandoned {
            drop(future);
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            assert_eq!(*env.seen.trace.lock().unwrap(), vec!["start", "finish"]);
            assert!(env.seen.ends.lock().unwrap().is_empty());
        } else {
            released.store(true, Ordering::SeqCst);
            let out = match poll(future.as_mut()) {
                std::task::Poll::Ready(r) => r.unwrap(),
                _ => panic!(),
            };
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(
                out.result(),
                Some(&HookResult::BeforeRunEnd(Some(BeforeRunEndResult {
                    follow_up: Some("done".into())
                })))
            );
            assert_eq!(
                env.seen.starts.lock().unwrap()[0].attributes["pi.operation.id"],
                AttributeValue::String("run".into())
            );
        }
    }
}

#[test]
fn concurrent_captured_invocations_own_independent_state_and_cross_thread_polling() {
    let r = HookRegistry::new();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let b = barrier.clone();
    r.before_run(
        id("append"),
        handler::<BeforeRun>(move |e| {
            b.wait();
            Ok(Some(BeforeRunResult {
                system_prompt: Some(e.system_prompt.clone()),
                resume_data: Presence::Value(s(&e.system_prompt)),
                messages: Some(vec![custom(s(&e.system_prompt))]),
            }))
        }),
    )
    .unwrap();
    let captured = r.capture(HookName::BeforeRun);
    std::thread::scope(|scope| {
        let threads: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|tag| {
                let captured = captured.clone();
                scope.spawn(move || {
                    let env = Env::new();
                    let event = before_run_with_prompt(tag);
                    let out = ready(captured.run_started(event, &env.context())).unwrap();
                    let HookResult::BeforeRun(Some(a)) = out.result().unwrap() else {
                        panic!()
                    };
                    assert_eq!(a.messages, Some(vec![custom(s(tag))]));
                    assert_eq!(a.system_prompt, Some(tag.into()));
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
    });
}
fn before_run_with_prompt(prompt: &str) -> HookEvent {
    let mut e = before_run();
    let HookEvent::BeforeRun(v) = &mut e else {
        panic!()
    };
    v.system_prompt = prompt.into();
    e
}

#[test]
fn one_manual_gate_parks_entire_pipeline_with_zero_callbacks_or_spans() {
    let env = Env::new();
    let r = HookRegistry::new();
    let calls = Arc::new(AtomicUsize::new(0));
    for key in ["a", "b"] {
        let c = calls.clone();
        r.before_run(
            id(key),
            handler::<BeforeRun>(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }),
        )
        .unwrap();
    }
    let gate = ManualGate::new(Admission::new(), DriveMode::Manual);
    let captured = r.capture(HookName::BeforeRun);
    let context = env.context();
    let mut task = Box::pin(async {
        let _permission = gate
            .park(ActionInfo {
                kind: "hook".into(),
                description: "whole pipeline".into(),
                details: Presence::Missing,
            })
            .await
            .unwrap();
        captured.run_started(before_run(), &context).await
    });
    assert!(poll(task.as_mut()).is_pending());
    assert!(gate.peek().is_some());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(env.seen.starts.lock().unwrap().is_empty());
    assert!(gate.release_next().is_some());
    assert!(matches!(poll(task.as_mut()), std::task::Poll::Ready(Ok(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(gate.peek().is_none());
}

#[test]
fn invalid_initial_context_and_output_family_mismatch_call_nobody() {
    let env = Env::new();
    let r = HookRegistry::new();
    let called = Arc::new(AtomicUsize::new(0));
    let c = called.clone();
    r.before_tool(
        None,
        handler::<BeforeTool>(move |_| {
            c.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }),
    )
    .unwrap();
    let cap = r.capture(HookName::BeforeTool);
    let mut ctx = env.context();
    ctx.tool = None;
    assert!(matches!(
        ready(cap.run_started(before_tool(), &ctx)),
        Err(HarnessRuntimeError::Fault(_))
    ));
    ctx.tool = Some(&env.tool);
    assert!(ready(cap.run_started(before_run(), &ctx)).is_err());
    let mut bad = before_tool();
    if let HookEvent::BeforeTool(e) = &mut bad {
        e.tool_name = ToolName::new("other").unwrap();
    }
    assert!(ready(cap.run_started(bad, &ctx)).is_err());
    let mut bad = before_run();
    if let HookEvent::BeforeRun(e) = &mut bad {
        e.prompt.push(invalid_native());
    }
    assert!(ready(r.capture(HookName::BeforeRun).run_started(bad, &ctx)).is_err());
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert!(env.seen.starts.lock().unwrap().is_empty());
    assert!(
        HookPipelineOutput::from_result(HookName::BeforeRun, HookResult::BeforeTool(None)).is_err()
    );
    assert!(
        HookPipelineOutput::from_result(HookName::BeforeRequest, HookResult::BeforeRequest(None))
            .is_err()
    );
}

#[test]
fn stable_resume_duplicates_and_optional_diagnostic_ids_have_distinct_rules() {
    let r = HookRegistry::new();
    let sub = r
        .before_resume(id("same"), handler::<BeforeResume>(|_| Ok(())))
        .unwrap();
    assert!(
        r.before_resume(id("same"), handler::<BeforeResume>(|_| Ok(())))
            .is_err()
    );
    sub.unsubscribe();
    let replacement = r
        .before_resume(id("same"), handler::<BeforeResume>(|_| Ok(())))
        .unwrap();
    sub.unsubscribe();
    drop(replacement);
    assert_eq!(r.capture(HookName::BeforeResume).len(), 1);
    for _ in 0..2 {
        r.before_run_end(Some(id("same")), handler::<BeforeRunEnd>(|_| Ok(None)))
            .unwrap();
    }
    let env = Env::new();
    env.run(
        &r,
        HookEvent::BeforeRunEnd(BeforeRunEndEvent {
            lane: lane(),
            run_id: run(),
            messages: vec![],
        }),
    );
    assert_eq!(env.seen.starts.lock().unwrap().len(), 2);
}

#[test]
fn structural_true_decline_stops_and_continue_shapes_survive() {
    for decline in [false, true] {
        let env = Env::new();
        let r = HookRegistry::new();
        let compaction = if decline {
            BeforeCompactionResult::DeclineTrue(Default::default())
        } else {
            BeforeCompactionResult::DeclineFalse(Default::default())
        };
        let navigation = if decline {
            BeforeNavigationResult::DeclineTrue(Default::default())
        } else {
            BeforeNavigationResult::Continue(Default::default())
        };
        let c = compaction.clone();
        r.before_compaction(
            None,
            handler::<BeforeCompaction>(move |_| Ok(Some(c.clone()))),
        )
        .unwrap();
        let selection = navigation.clone();
        r.before_navigation(
            None,
            handler::<BeforeNavigation>(move |_| Ok(Some(selection.clone()))),
        )
        .unwrap();
        let later = Arc::new(AtomicUsize::new(0));
        let c = later.clone();
        r.before_compaction(
            None,
            handler::<BeforeCompaction>(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }),
        )
        .unwrap();
        let c = later.clone();
        r.before_navigation(
            None,
            handler::<BeforeNavigation>(move |_| {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(None)
            }),
        )
        .unwrap();
        let out = env.run(
            &r,
            HookEvent::BeforeCompaction(Box::new(BeforeCompactionEvent {
                lane: lane(),
                run_id: run(),
                reason: HookCompactionReason::Manual,
                preparation: preparation(),
                custom_instructions: None,
            })),
        );
        assert_eq!(
            out.result(),
            Some(&HookResult::BeforeCompaction(Some(Box::new(compaction))))
        );
        let out = env.run(
            &r,
            HookEvent::BeforeNavigation(Box::new(BeforeNavigationEvent {
                lane: lane(),
                run_id: run(),
                target_id: EntryId::new("target").unwrap(),
                preparation: BranchPreparation {
                    messages: vec![],
                    file_ops: Default::default(),
                    total_tokens: n(0),
                },
                custom_instructions: None,
            })),
        );
        assert_eq!(
            out.result(),
            Some(&HookResult::BeforeNavigation(Some(navigation)))
        );
        assert_eq!(later.load(Ordering::SeqCst), if decline { 0 } else { 2 });
    }
}

#[test]
fn before_run_provisional_id_and_empty_aggregate_stay_distinct() {
    let env = Env::new();
    let r = HookRegistry::new();
    assert_eq!(
        env.run(&r, before_run()).result(),
        Some(&HookResult::BeforeRun(None))
    );
    r.before_run(
        id("empty"),
        handler::<BeforeRun>(|_| Ok(Some(BeforeRunResult::default()))),
    )
    .unwrap();
    let mut context = env.context();
    context.operation_accepted = false;
    let out = ready(
        r.capture(HookName::BeforeRun)
            .run_started(before_run(), &context),
    )
    .unwrap();
    assert_eq!(
        out.result(),
        Some(&HookResult::BeforeRun(Some(BeforeRunResult::default())))
    );
    assert!(
        !env.seen.starts.lock().unwrap()[0]
            .attributes
            .contains_key("pi.operation.id")
    );
}

#[test]
fn user_future_poll_and_waker_can_reenter_registry() {
    use std::task::{Context, Poll, Wake, Waker};
    struct Reenter(HookRegistry, AtomicUsize);
    impl Wake for Reenter {
        fn wake(self: Arc<Self>) {
            self.wake_by_ref();
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0
                .before_run_end(None, handler::<BeforeRunEnd>(|_| Ok(None)))
                .unwrap();
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }
    let env = Env::new();
    let r = HookRegistry::new();
    let registry = r.clone();
    r.before_run_end(
        None,
        async_handler::<BeforeRunEnd>(move |_| {
            let registry = registry.clone();
            let mut first = true;
            Box::pin(std::future::poll_fn(move |cx| {
                assert!(!registry.capture(HookName::BeforeRunEnd).is_empty());
                if first {
                    first = false;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(None))
                }
            }))
        }),
    )
    .unwrap();
    let wake = Arc::new(Reenter(r.clone(), AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let cap = r.capture(HookName::BeforeRunEnd);
    let context = env.context();
    let mut future = Box::pin(cap.run_started(
        HookEvent::BeforeRunEnd(BeforeRunEndEvent {
            lane: lane(),
            run_id: run(),
            messages: vec![],
        }),
        &context,
    ));
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(wake.1.load(Ordering::SeqCst), 1);
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_ready()
    );
    assert_eq!(cap.len(), 1);
    assert_eq!(r.capture(HookName::BeforeRunEnd).len(), 2);
    assert_eq!(env.seen.starts.lock().unwrap().len(), 1);
}
