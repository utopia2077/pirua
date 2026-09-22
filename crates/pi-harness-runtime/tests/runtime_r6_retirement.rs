//! Two ownership-retirement boundaries, driven through actual public runtime.
mod runtime_r1_r4_support;
mod runtime_r6_retirement_support;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use pi_agent_session::{codec::SessionCodec, session::RuntimeSession};
use pi_ai_core::{CancellationToken, Presence};
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_r1_r4_support::{Setup, Summary, poll};
use runtime_r6_retirement_support::*;
use std::{
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

#[test]
fn actual_public_fault_retires_pending_custom_projector_and_accepted_prompt() {
    let s = Setup::new();
    let held = CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(held.clone());
    let projector = Arc::new(Projector::default());
    let fault_prompt = Arc::new(FaultPrompt::default());
    let mut options = s.options::<()>(DriveMode::Automatic);
    options.entry_projectors.insert(
        CustomType::new("pending-projection").unwrap(),
        projector.clone(),
    );
    options.system_prompt = Some(SystemPromptSource::Provider(fault_prompt.clone()));
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    let mut accepted = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(accepted.as_mut()).is_pending());
    for _ in 0..64 {
        s.pump.tick();
        if s.faux.requests.lock().unwrap().len() == 1 {
            break;
        }
    }
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    let custom = s
        .pump
        .drive(c.harness.session().append_custom_entry(
            CustomType::new("pending-projection").unwrap(),
            Presence::Missing,
        ))
        .unwrap();
    held.cancel();
    for _ in 0..64 {
        s.pump.tick();
        if projector.entered.load(Ordering::SeqCst) == 1 {
            break;
        }
    }
    assert_eq!(projector.entered.load(Ordering::SeqCst), 1);
    assert_eq!(projector.dropped.load(Ordering::SeqCst), 0);
    assert!(poll(accepted.as_mut()).is_pending());
    assert!(
        s.pump
            .drive(s.session.get_entries(std::slice::from_ref(&custom)))
            .unwrap()
            .is_empty()
    );
    assert!(
        s.pump
            .drive(
                s.session
                    .get_register(RegisterNamespace::PendingEntry, custom.as_str())
            )
            .unwrap()
            .is_some()
    );
    let other = s
        .pump
        .drive(
            c.harness
                .create_lane(LaneId::new("fault-source").unwrap(), None),
        )
        .unwrap()
        .unwrap();
    let calls = fault_prompt.calls.load(Ordering::SeqCst);
    fault_prompt.fail.store(true, Ordering::SeqCst);
    let trigger = s.pump.drive(other.prompt(prompt()));
    assert!(
        matches!(trigger, Err(HarnessRuntimeError::Fault(_))),
        "positive actual public Fault trigger"
    );
    assert_eq!(fault_prompt.calls.load(Ordering::SeqCst), calls + 1);
    assert!(
        matches!(
            s.pump.drive(c.harness.get_last_result()),
            Err(HarnessRuntimeError::Fault(_))
        ),
        "owner admission really sealed with Fault"
    );
    let sealed = s.pump.drive(s.store.snapshot()).unwrap();
    let mut completion = None;
    for _ in 0..64 {
        s.pump.tick();
        if completion.is_none()
            && let Poll::Ready(result) = poll(accepted.as_mut())
        {
            completion = Some(result);
        }
    }
    assert_eq!(
        s.pump.drive(s.store.snapshot()).unwrap(),
        sealed,
        "no writes after settled fault boundary"
    );
    assert!(
        s.pump
            .drive(s.session.get_entries(std::slice::from_ref(&custom)))
            .unwrap()
            .is_empty()
    );
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    eprintln!(
        "accepted completion after real Fault: {completion:?}; projector drops={}",
        projector.dropped.load(Ordering::SeqCst)
    );
    assert_eq!(
        projector.dropped.load(Ordering::SeqCst),
        1,
        "Fault must drop the actually-started pending projector future"
    );
    assert!(
        matches!(completion, Some(Err(HarnessRuntimeError::Fault(_)))),
        "accepted prompt must reject Fault without projector cooperation"
    );
}

#[test]
fn external_terminal_after_tool_hydration_stamp_keeps_original_result() {
    let s = Setup::new();
    let tool = Tool::new();
    let after = Arc::new(AfterToolCount::default());
    let storage = Arc::new(AfterRawStamp::new(s.store.clone(), tool.clone()));
    let session = Arc::new(
        s.pump
            .drive(RuntimeSession::new(
                storage.clone(),
                s.session.metadata(),
                Arc::new(
                    SessionCodec::new(
                        SessionCodecOptions::default(),
                        ProviderHeaderPolicy::default(),
                    )
                    .unwrap(),
                ),
                s.ids.clone(),
                Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
            ))
            .unwrap(),
    );
    *s.faux.responses.lock().unwrap() = vec![tool_response(&s)].into();
    let mut options = s.options_for::<(), _>(session, DriveMode::Automatic);
    options.tools = vec![tool.clone()];
    options.active_tool_names = Some(vec![pi_ai_core::ToolName::new("retire-tool").unwrap()]);
    options.tool_context = Some(AgentHarnessToolContextSource::Static(()));
    let factory = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s.pump.drive(factory.create(options)).unwrap();
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("retire-tool").unwrap()]),
        )
        .unwrap();
    let _hook = c.harness.hooks().after_tool(None, after.clone()).unwrap();
    let mut accepted = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(accepted.as_mut()).is_pending());
    for _ in 0..512 {
        s.pump.tick();
        if storage.entered.is_cancelled() {
            break;
        }
    }
    assert!(
        storage.entered.is_cancelled(),
        "post-raw hydration stamp must park"
    );
    assert_eq!(tool.started.load(Ordering::SeqCst), 1);
    assert_eq!(tool.raw_done.load(Ordering::SeqCst), 1);
    assert_eq!(after.0.load(Ordering::SeqCst), 0);
    let (operation, result_id, last) = external_terminal(&s);
    let terminal = s.pump.drive(s.store.snapshot()).unwrap();
    storage.release.cancel();
    let completion = s.pump.drive(accepted);
    assert!(
        matches!(&completion, Ok(Ok(RunSuccess { run_id, outcome: RunOutcome::AbortedWithAssistant(_), .. })) if run_id == &operation),
        "valid external terminal must resolve the original run: {completion:?}"
    );
    assert_eq!(s.pump.drive(s.store.snapshot()).unwrap(), terminal);
    assert!(
        s.pump
            .drive(s.session.get_entries(std::slice::from_ref(&result_id)))
            .unwrap()
            .contains_key(&result_id)
    );
    assert_eq!(after.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        s.pump.drive(c.harness.get_last_result()).unwrap(),
        Some(last)
    );
}

#[test]
fn stable_changed_saved_tool_arguments_fault_before_raw_settlement() {
    let s = Setup::new();
    let tool = Tool::new();
    let release = CancellationToken::new();
    *tool.release.lock().unwrap() = Some(release.clone());
    *s.faux.responses.lock().unwrap() = vec![tool_response(&s)].into();
    let mut options = s.options::<()>(DriveMode::Automatic);
    options.tools = vec![tool.clone()];
    options.active_tool_names = Some(vec![pi_ai_core::ToolName::new("retire-tool").unwrap()]);
    options.tool_context = Some(AgentHarnessToolContextSource::Static(()));
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("retire-tool").unwrap()]),
        )
        .unwrap();
    let mut accepted = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(accepted.as_mut()).is_pending());
    for _ in 0..512 {
        s.pump.tick();
        if tool.started.load(Ordering::SeqCst) == 1 {
            break;
        }
    }
    assert_eq!(tool.started.load(Ordering::SeqCst), 1);
    assert_eq!(tool.raw_done.load(Ordering::SeqCst), 0);
    let mut args = s
        .pump
        .drive(
            s.session
                .list_registers(RegisterNamespace::OperationToolArguments, None),
        )
        .unwrap();
    assert_eq!(args.len(), 1);
    let Register::OperationToolArguments { key, .. } = args.pop().unwrap() else {
        panic!()
    };
    let changed =
        pi_ai_core::JsonObject::new([("changed".into(), pi_ai_core::JsonValue::Bool(true))].into());
    s.pump
        .drive(s.session.commit(Transaction {
            writes: vec![Write::SetRegister(Box::new(
                RegisterSet::OperationToolArguments {
                    key,
                    value: changed,
                },
            ))],
        }))
        .expect("stable argument replacement is a valid Session write");
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    release.cancel();
    let completion = s.pump.drive(accepted);
    assert!(
        matches!(completion, Err(HarnessRuntimeError::Fault(_))),
        "{completion:?}"
    );
    assert_eq!(tool.raw_done.load(Ordering::SeqCst), 1);
    assert_eq!(
        s.pump.drive(s.store.snapshot()).unwrap(),
        before,
        "no raw tool result or usage may settle"
    );
}

#[test]
fn external_terminal_after_raw_tool_before_finalize_resolves_original_result() {
    let s = Setup::new();
    let tool = Tool::new();
    let after = Arc::new(AfterToolCount::default());
    *s.faux.responses.lock().unwrap() = vec![tool_response(&s)].into();
    let mut options = s.options::<()>(DriveMode::Manual);
    options.tools = vec![tool.clone()];
    options.active_tool_names = Some(vec![pi_ai_core::ToolName::new("retire-tool").unwrap()]);
    options.tool_context = Some(AgentHarnessToolContextSource::Static(()));
    let c = s.pump.drive(factory(&s).create(options)).unwrap();
    // Setup opens an already-configured Session; constructor options correctly
    // do not overwrite its persisted active-tool list. Use the public setter.
    s.pump
        .drive(
            c.harness
                .set_active_tools(vec![pi_ai_core::ToolName::new("retire-tool").unwrap()]),
        )
        .unwrap();
    let _hook = c.harness.hooks().after_tool(None, after.clone()).unwrap();
    let mut accepted = Box::pin(c.harness.prompt(prompt()));
    assert!(poll(accepted.as_mut()).is_pending());
    let mut parked = false;
    for _ in 0..512 {
        s.pump.tick();
        if tool.raw_done.load(Ordering::SeqCst) == 1
            && s.pump
                .drive(c.harness.peek_action())
                .unwrap()
                .is_some_and(|a| a.kind == "finalize_tool")
        {
            parked = true;
            break;
        }
        s.pump.drive(c.harness.execute_action()).unwrap();
    }
    assert!(
        parked,
        "actual raw tool completed and finalization is parked"
    );
    assert_eq!(tool.started.load(Ordering::SeqCst), 1);
    assert_eq!(tool.raw_done.load(Ordering::SeqCst), 1);
    assert_eq!(after.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        s.pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationToolArguments, None)
            )
            .unwrap()
            .len(),
        1
    );
    let (operation, result_id, last) = external_terminal(&s);
    let terminal = s.pump.drive(s.store.snapshot()).unwrap();
    s.session
        .codec()
        .validate_snapshot(&terminal)
        .expect("external terminal remains fully codec-valid");
    assert!(
        s.pump
            .drive(
                s.session
                    .get_register(RegisterNamespace::OperationMeta, operation.as_str())
            )
            .unwrap()
            .is_none()
    );
    assert!(
        s.pump
            .drive(
                s.session
                    .get_register(RegisterNamespace::OperationState, operation.as_str())
            )
            .unwrap()
            .is_none()
    );
    assert!(
        s.pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationToolArguments, None)
            )
            .unwrap()
            .is_empty()
    );
    assert!(
        s.pump
            .drive(s.session.get_entries(std::slice::from_ref(&result_id)))
            .unwrap()
            .contains_key(&result_id)
    );
    assert_eq!(
        s.pump.drive(c.harness.get_last_result()).unwrap(),
        Some(last.clone())
    );
    assert!(s.pump.drive(c.harness.execute_action()).unwrap().is_some());
    let mut completion = None;
    for _ in 0..128 {
        s.pump.tick();
        if let Poll::Ready(result) = poll(accepted.as_mut()) {
            completion = Some(result);
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action());
    }
    assert_eq!(
        s.pump.drive(s.store.snapshot()).unwrap(),
        terminal,
        "stale finalization cannot publish or recreate saved args"
    );
    assert_eq!(tool.started.load(Ordering::SeqCst), 1);
    assert_eq!(tool.raw_done.load(Ordering::SeqCst), 1);
    assert_eq!(after.0.load(Ordering::SeqCst), 0);
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    eprintln!("original accepted completion after valid external terminal: {completion:?}");
    assert!(
        matches!(completion,Some(Ok(Ok(RunSuccess{run_id,outcome:RunOutcome::AbortedWithAssistant(_),..}))) if run_id==operation),
        "legitimate external terminal must resolve original result, not HarnessFault"
    );
    assert_eq!(
        s.pump.drive(c.harness.get_last_result()).unwrap(),
        Some(last)
    );
    s.pump.drive(c.harness.close()).unwrap();
}
