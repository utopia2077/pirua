mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
mod runtime_telemetry_support;
use pi_agent_schema::*;
use pi_ai_core::StopReason;
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::*;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
#[test]
fn physical_unknown_poll_reopen_preserves_poll_and_abandons_old_reserved_pair() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    adapter
        .replies
        .lock()
        .unwrap()
        .push_back(adapter.response(StopReason::Deferred));
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Manual)))
        .unwrap();
    let mut initial = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(initial.as_mut()).is_pending());
    {
        s.pump.tick();
        s.pump.drive(c.harness.run_to_completion())
    }
    .unwrap();
    s.pump.drive(initial).unwrap().unwrap();
    let mut resume = Box::pin(c.harness.resume());
    assert!(poll(resume.as_mut()).is_pending());
    manual_until(
        &s,
        c.harness.as_ref(),
        || matches!(current(&s),Some(OperationState::Run(r))if matches!(r.phase,RunPhase::Deferred(DeferredPhase{deferred:Deferred::EffectPending{..},..}))),
    );
    let Some(OperationState::Run(r)) = current(&s) else {
        panic!()
    };
    let RunPhase::Deferred(DeferredPhase {
        deferred:
            Deferred::EffectPending {
                poll: old_poll,
                response_entry_id: old_response,
                usage_id: old_usage,
                ..
            },
        ..
    }) = r.phase
    else {
        panic!()
    };
    assert!(adapter.polls.lock().unwrap().is_empty());
    s.pump.drive(c.harness.close()).unwrap();
    assert!(s.pump.drive(resume).is_err());
    let store = s.backing.open(s.time.clone()).unwrap();
    let session = Arc::new(
        s.pump
            .drive(pi_agent_session::session::RuntimeSession::new(
                store.clone(),
                s.session.metadata(),
                Arc::new(
                    pi_agent_session::codec::SessionCodec::new(
                        Default::default(),
                        Default::default(),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    let restored = s
        .pump
        .drive(factory(&s).create(s.options_for(session.clone(), DriveMode::Automatic)))
        .unwrap();
    let events = Arc::new(Listener(Mutex::new(vec![])));
    let watch = s.pump.drive(restored.harness.watch()).unwrap();
    watch.start(events.clone());
    s.pump.drive(restored.harness.resume()).unwrap().unwrap();
    assert_eq!(adapter.polls.lock().unwrap().len(), 1);
    use pi_agent_session::storage::AtomicStorage;
    let snapshot = s.pump.drive(store.snapshot()).unwrap();
    assert!(
        !snapshot
            .entries
            .iter()
            .any(|e| matches!(e,Entry::Message(e)if e.id==old_response))
    );
    assert!(!snapshot.usage.iter().any(|u| u.id == old_usage));
    let state = s
        .pump
        .drive(session.list_registers(RegisterNamespace::OperationState, None))
        .unwrap()
        .pop()
        .unwrap();
    let Register::OperationState { value, .. } = state else {
        panic!()
    };
    let OperationState::Run(run) = *value else {
        panic!()
    };
    let RunPhase::Deferred(DeferredPhase {
        deferred:
            Deferred::Suspended {
                poll,
                source_entry_id,
                ..
            },
        ..
    }) = run.phase
    else {
        panic!()
    };
    assert_eq!(poll.get(), old_poll.get());
    assert_ne!(source_entry_id, old_response);
    assert_eq!(snapshot.usage.len(), 2);
    assert_eq!(
        events
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|e| matches!(e, HarnessEvent::TurnStart(_)))
            .count(),
        1,
        "unknown replacement is one poll turn"
    );
}
#[test]
fn actual_pending_handle_equality_checks_every_axis_and_error_never_retries() {
    for field in [
        "provider",
        "modelId",
        "api",
        "id",
        "expiresAt",
        "pollAfterMs",
        "data",
    ] {
        let s = Setup::new();
        let adapter = DeferredAdapter::install(&s);
        adapter.begin();
        let mut response = adapter.response(StopReason::Deferred);
        let mut handle = serde_json::to_value(response.deferred.as_ref().unwrap()).unwrap();
        if field == "data" {
            handle.as_object_mut().unwrap().remove("data");
        } else {
            handle[field] = match field {
                "expiresAt" | "pollAfterMs" => serde_json::json!(900),
                _ => serde_json::json!("different"),
            };
        }
        response.deferred = Some(serde_json::from_value(handle).unwrap());
        adapter.replies.lock().unwrap().push_back(response);
        let mut options = s.options(DriveMode::Automatic);
        options.retry.enabled = true;
        options.retry.max_retries = pi_ai_core::MaxRetries::new(20).unwrap();
        let c = s.pump.drive(factory(&s).create(options)).unwrap();
        s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
        s.pump.drive(c.harness.resume()).unwrap().unwrap();
        assert_eq!(adapter.polls.lock().unwrap().len(), 1, "{field}");
        assert!(
            matches!(
                s.pump.drive(c.harness.get_last_result()).unwrap(),
                Some(LaneLastResult::RunFailed(_))
            ),
            "{field}"
        );
    }
    for reason in [StopReason::Error, StopReason::Length, StopReason::Aborted] {
        let s = Setup::new();
        let adapter = DeferredAdapter::install(&s);
        adapter.begin();
        let mut response = adapter.response(reason);
        if reason == StopReason::Error {
            response.error_message = Some("429 rate limit exceeded".into());
        }
        adapter.replies.lock().unwrap().push_back(response);
        let c = s
            .pump
            .drive(factory(&s).create(s.options(DriveMode::Automatic)))
            .unwrap();
        s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
        let result = s.pump.drive(c.harness.resume());
        assert_eq!(adapter.polls.lock().unwrap().len(), 1);
        assert_eq!(result.is_err(), reason == StopReason::Aborted);
        if reason == StopReason::Length {
            assert!(matches!(
                s.pump.drive(c.harness.get_last_result()).unwrap(),
                Some(LaneLastResult::RunCompletedAssistant(_))
            ));
        }
    }
}
struct AutoTool {
    descriptor: AgentTool,
    calls: Arc<AtomicUsize>,
}
impl AgentHarnessTool<()> for AutoTool {
    fn descriptor(&self) -> &AgentTool {
        &self.descriptor
    }
    fn prepare_arguments(
        &self,
        args: pi_ai_core::JsonValue,
    ) -> Result<pi_ai_core::JsonObject, ToolExecutionError> {
        let pi_ai_core::JsonValue::Object(args) = args else {
            panic!()
        };
        Ok(args)
    }
    fn execute<'a>(
        &'a self,
        _: pi_ai_core::ToolCallId,
        _: pi_ai_core::JsonObject,
        _: &'a dyn Cancellation,
        _: &'a dyn ToolProgressSink,
        _: &'a (),
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let mut usage = pi_ai_core::Usage::zero();
            usage.input = n(3);
            usage.total_tokens = n(3);
            Ok(AgentToolResult {
                content: vec![],
                details: pi_ai_core::JsonValue::String("redeemed".into()),
                usage: Some(usage),
                added_tool_names: None,
                terminate: Some(true),
            })
        })
    }
}
#[test]
fn deferred_ready_tools_use_poll_turn_and_real_request_tool_telemetry() {
    for mode in [DriveMode::Automatic, DriveMode::Manual] {
        let s = Setup::new();
        let adapter = DeferredAdapter::install(&s);
        adapter.begin();
        let mut response = adapter.response(StopReason::ToolUse);
        response.content = vec![pi_ai_core::AssistantContent::ToolCall(
            pi_ai_core::ToolCall {
                content_type: Default::default(),
                id: pi_ai_core::ToolCallId::new("redeem").unwrap(),
                name: pi_ai_core::ToolName::new("tool").unwrap(),
                arguments: Default::default(),
                thought_signature: None,
                namespace: None,
            },
        )];
        adapter.replies.lock().unwrap().push_back(response);
        let calls = Arc::new(AtomicUsize::new(0));
        let recorder = Arc::new(runtime_telemetry_support::Recorder::default());
        let mut options = s.options(mode);
        options.telemetry_context = recorder.clone();
        options.tools = vec![Arc::new(AutoTool {
            descriptor: AgentTool {
                name: pi_ai_core::ToolName::new("tool").unwrap(),
                label: "tool".into(),
                description: "tool".into(),
                parameters: serde_json::from_value(serde_json::json!({"type":"object"})).unwrap(),
                constrained_sampling: None,
                execution_mode: None,
                replay: Some(ToolReplay::Safe),
            },
            calls: calls.clone(),
        })];
        let c = s.pump.drive(factory(&s).create(options)).unwrap();
        s.pump
            .drive(
                c.harness
                    .set_active_tools(vec![pi_ai_core::ToolName::new("tool").unwrap()]),
            )
            .unwrap();
        let mut initial = Box::pin(c.harness.prompt(prompt()));
        assert!(poll(initial.as_mut()).is_pending());
        if mode == DriveMode::Manual {
            {
                s.pump.tick();
                s.pump.drive(c.harness.run_to_completion())
            }
            .unwrap();
        }
        s.pump.drive(initial).unwrap().unwrap();
        let mut resume = Box::pin(c.harness.resume());
        assert!(poll(resume.as_mut()).is_pending());
        if mode == DriveMode::Manual {
            {
                s.pump.tick();
                s.pump.drive(c.harness.run_to_completion())
            }
            .unwrap();
        }
        s.pump.drive(resume).unwrap().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(matches!(
            s.pump.drive(c.harness.get_last_result()).unwrap(),
            Some(LaneLastResult::RunCompletedTerminatedTools(_))
        ));
        for _ in 0..8 {
            s.pump.tick();
        }
        recorder.assert_finished();
        let rows = recorder.rows();
        assert!(rows.iter().any(|r|r.name=="pi.harness.turn"&&matches!(r.attrs.get("pi.turn.id"),Some(pi_telemetry::AttributeValue::String(turn))if turn.ends_with(":poll:1"))));
        assert_eq!(rows.iter().filter(|r| r.name == "pi.ai.request").count(), 2);
        assert_eq!(
            rows.iter().filter(|r| r.name == "pi.harness.tool").count(),
            1
        );
        let fetch = rows
            .iter()
            .find(|r| {
                r.name == "pi.ai.request"
                    && runtime_telemetry_support::text(r, "pi.ai.operation").as_deref()
                        == Some("fetch_deferred")
            })
            .unwrap();
        assert_eq!(rows[fetch.parent.unwrap()].name, "pi.harness.step");
        let tool = rows.iter().find(|r| r.name == "pi.harness.tool").unwrap();
        assert_eq!(rows[tool.parent.unwrap()].name, "pi.harness.turn");
    }
}
struct DeferredResponseHook(Arc<AtomicUsize>);
impl HookHandler<AfterResponse> for DeferredResponseHook {
    fn call(
        &self,
        e: AfterResponseEvent,
    ) -> BoxFuture<'_, Result<Option<AfterResponseResult>, HandlerError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(Some(AfterResponseResult {
                message: Some(e.message),
            }))
        })
    }
}
#[test]
fn deferred_response_hook_reaches_actual_helper_and_cancel_uses_latest_pending_source() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    adapter
        .replies
        .lock()
        .unwrap()
        .push_back(adapter.response(StopReason::Deferred));
    let c = s
        .pump
        .drive(factory(&s).create(s.options(DriveMode::Automatic)))
        .unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let _r = c
        .harness
        .hooks()
        .after_response(None, Arc::new(DeferredResponseHook(count.clone())))
        .unwrap();
    s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    s.pump.drive(c.harness.resume()).unwrap().unwrap();
    let Some(OperationState::Run(run)) = current(&s) else {
        panic!()
    };
    let RunPhase::Deferred(DeferredPhase {
        deferred:
            Deferred::Suspended {
                source_entry_id,
                poll,
                ..
            },
        ..
    }) = run.phase
    else {
        panic!()
    };
    assert_eq!(poll.get(), 1);
    let Entry::Message(e) = s
        .pump
        .drive(c.harness.session().get_entry(source_entry_id))
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(m)) = e.message else {
        panic!()
    };
    let expected = m.deferred.unwrap();
    s.pump.drive(c.harness.abort()).unwrap().unwrap();
    s.pump.drive(c.harness.wait_for_idle()).unwrap();
    assert_eq!(adapter.cancels.lock().unwrap()[0].0, expected);
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

struct BeforePollProbe;
impl HookHandler<BeforeRequest> for BeforePollProbe {
    fn call(
        &self,
        _: BeforeRequestEvent,
    ) -> BoxFuture<'_, Result<Option<BeforeRequestResult>, HandlerError>> {
        Box::pin(async { Ok(None) })
    }
}
#[test]
fn deferred_before_request_hook_has_its_poll_step_parent() {
    let s = Setup::new();
    let adapter = DeferredAdapter::install(&s);
    adapter.begin();
    let recorder = Arc::new(runtime_telemetry_support::Recorder::default());
    let mut options = s.options(DriveMode::Automatic);
    options.telemetry_context = recorder.clone();
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    let _r = c
        .harness
        .hooks()
        .before_request(None, Arc::new(BeforePollProbe))
        .unwrap();
    s.pump.drive(c.harness.prompt(prompt())).unwrap().unwrap();
    s.pump.drive(c.harness.resume()).unwrap().unwrap();
    let rows = recorder.rows();
    let hook = rows
        .iter()
        .rfind(|r| {
            r.name == "pi.harness.hook"
                && runtime_telemetry_support::text(r, "pi.hook.name").as_deref()
                    == Some("before_request")
        })
        .unwrap();
    assert_eq!(rows[hook.parent.unwrap()].name, "pi.harness.step");
    assert_eq!(
        runtime_telemetry_support::text(&rows[hook.parent.unwrap()], "pi.step.kind").as_deref(),
        Some("deferred")
    );
}
