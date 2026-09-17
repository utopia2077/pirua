//! Two ownership-retirement boundaries, driven through actual public runtime.
mod runtime_r1_r4_support;
mod runtime_r6_retirement_support;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use pi_ai_core::{CancellationToken, Presence};
use runtime_r1_r4_support::{Setup, poll};
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
