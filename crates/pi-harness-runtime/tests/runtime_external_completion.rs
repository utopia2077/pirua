//! External terminal interpretation through actual public Memory/Models runtime.
mod runtime_r1_r4_support;
use pi_agent_schema::*;
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_r1_r4_support::*;
use std::sync::Arc;

#[test]
fn valid_external_terminal_returns_last_result_without_faulting_owner() {
    use pi_agent_session::storage::AtomicStorage;
    let s = Setup::new();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Manual)))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "external terminal".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let mut captured = None;
    for _ in 0..400 {
        s.pump.tick();
        let states = s
            .pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationState, None),
            )
            .unwrap();
        for state in states {
            if let Register::OperationState { key, value, .. } = state
                && let OperationState::Run(run) = value.as_ref()
                && let RunPhase::Checkpoint(checkpoint) = &run.phase
                && matches!(checkpoint.continuation, Continuation::MayFinish { .. })
            {
                captured = Some((key, checkpoint.trigger_entry_id.clone()));
            }
        }
        if captured.is_some() {
            break;
        }
        s.pump.drive(c.harness.execute_action()).unwrap();
    }
    let (id, assistant_id) =
        captured.expect("actual assistant must be durably committed before external terminal");
    // Reproduce a complete external terminal transaction with a valid lastResult,
    // not the earlier missing-register-only stale-output scenario.
    s.pump
        .drive(s.session.commit(Transaction {
            writes: vec![
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationMeta,
                    key: id.clone(),
                }),
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationState,
                    key: id.clone(),
                }),
                Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
                    key: "main".into(),
                    value: LaneLastResult::RunCompletedAssistant(RunCompletedAssistantLastResult {
                        operation_id: OperationId::new(id).unwrap(),
                        kind: Default::default(),
                        leaf_id: Some(assistant_id.clone()),
                        final_assistant_entry_id: assistant_id,
                        outcome: Default::default(),
                        run_completion: Default::default(),
                    }),
                })),
                Write::SetRegister(Box::new(RegisterSet::LaneState {
                    key: "main".into(),
                    value: LaneState {
                        current_operation_id: None,
                        pending_next_run: vec![],
                    },
                })),
            ],
        }))
        .unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let mut result = None;
    for _ in 0..100 {
        s.pump.tick();
        if let std::task::Poll::Ready(value) = poll(prompt.as_mut()) {
            result = Some(value);
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action());
    }
    let result = result.expect("public driver settles after observing external terminal");
    assert_eq!(
        s.pump.drive(s.store.snapshot()).unwrap(),
        before,
        "external terminal must not be rewritten"
    );
    println!("external terminal public result={result:?}");
    assert!(
        matches!(
            result,
            Ok(Ok(RunSuccess {
                outcome: RunOutcome::CompletedWithAssistant(_),
                ..
            }))
        ),
        "valid external terminal is ordinary completion, not whole-owner corruption"
    );
}

fn check_external_terminal(mode: &str) {
    use pi_agent_session::storage::AtomicStorage;
    let s = Setup::new();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Manual)))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "external terminal".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    let mut captured = None;
    for _ in 0..400 {
        s.pump.tick();
        let states = s
            .pump
            .drive(
                s.session
                    .list_registers(RegisterNamespace::OperationState, None),
            )
            .unwrap();
        for state in states {
            if let Register::OperationState { key, value, .. } = state
                && let OperationState::Run(run) = value.as_ref()
                && let RunPhase::Checkpoint(checkpoint) = &run.phase
                && matches!(checkpoint.continuation, Continuation::MayFinish { .. })
            {
                captured = Some((key, checkpoint.trigger_entry_id.clone()));
            }
        }
        if captured.is_some() {
            break;
        }
        s.pump.drive(c.harness.execute_action()).unwrap();
    }
    let (id, assistant_id) =
        captured.expect("actual assistant must be durably committed before external terminal");
    let meta = s
        .pump
        .drive(
            s.session
                .get_register(RegisterNamespace::OperationMeta, &id),
        )
        .unwrap()
        .unwrap();
    let state = s
        .pump
        .drive(
            s.session
                .get_register(RegisterNamespace::OperationState, &id),
        )
        .unwrap()
        .unwrap();
    let original = OperationId::new(id.clone()).unwrap();
    let final_id = assistant_id.clone();
    let unrelated = s
        .pump
        .drive(
            c.harness
                .create_lane(LaneId::new("unrelated").unwrap(), None),
        )
        .unwrap()
        .unwrap();
    // Reproduce a complete external terminal transaction with a valid lastResult,
    // not the earlier missing-register-only stale-output scenario.
    let mut transaction = Transaction {
        writes: vec![
            Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::OperationMeta,
                key: id.clone(),
            }),
            Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::OperationState,
                key: id.clone(),
            }),
            Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
                key: "main".into(),
                value: LaneLastResult::RunCompletedAssistant(RunCompletedAssistantLastResult {
                    operation_id: OperationId::new(id).unwrap(),
                    kind: Default::default(),
                    leaf_id: Some(assistant_id.clone()),
                    final_assistant_entry_id: assistant_id,
                    outcome: Default::default(),
                    run_completion: Default::default(),
                }),
            })),
            Write::SetRegister(Box::new(RegisterSet::LaneState {
                key: "main".into(),
                value: LaneState {
                    current_operation_id: None,
                    pending_next_run: vec![],
                },
            })),
        ],
    };
    let Write::SetRegister(last) = &mut transaction.writes[2] else {
        unreachable!()
    };
    let RegisterSet::LaneLastResult { value, .. } = last.as_mut() else {
        unreachable!()
    };
    match mode {
        "missing" => {
            transaction.writes.remove(2);
        }
        "mismatch" => {
            let LaneLastResult::RunCompletedAssistant(v) = value else {
                unreachable!()
            };
            v.operation_id = OperationId::new("another-result").unwrap();
        }
        "missing-assistant" => {
            let LaneLastResult::RunCompletedAssistant(v) = value else {
                unreachable!()
            };
            v.final_assistant_entry_id = EntryId::new("absent-assistant").unwrap();
        }
        "non-assistant" => {
            let Register::OperationMeta {
                value: operation, ..
            } = &meta
            else {
                unreachable!()
            };
            let OperationIntent::Run(intent) = &operation.intent else {
                unreachable!()
            };
            let LaneLastResult::RunCompletedAssistant(v) = value else {
                unreachable!()
            };
            v.final_assistant_entry_id = intent.prompt_entry_ids[0].clone();
        }
        "aborted" => {
            *value = LaneLastResult::RunAborted(RunAbortedLastResult {
                operation_id: original.clone(),
                kind: Default::default(),
                leaf_id: Some(final_id.clone()),
                final_assistant_entry_id: Some(final_id.clone()),
                outcome: Default::default(),
            })
        }
        "new-operation" => {
            let Register::OperationMeta { mut value, .. } = meta else {
                unreachable!()
            };
            let Register::OperationState {
                value: next_state, ..
            } = state
            else {
                unreachable!()
            };
            let new_id = OperationId::new("new-operation").unwrap();
            value.operation_id = new_id.clone();
            transaction
                .writes
                .push(Write::SetRegister(Box::new(RegisterSet::OperationMeta {
                    key: new_id.as_str().into(),
                    value,
                })));
            transaction
                .writes
                .push(Write::SetRegister(Box::new(RegisterSet::OperationState {
                    key: new_id.as_str().into(),
                    value: next_state,
                })));
            let Write::SetRegister(last) = &mut transaction.writes[3] else {
                unreachable!()
            };
            let RegisterSet::LaneState { value, .. } = last.as_mut() else {
                unreachable!()
            };
            value.current_operation_id = Some(new_id);
        }
        "valid" => {}
        _ => unreachable!(),
    }
    s.pump.drive(s.session.commit(transaction)).unwrap();
    let before = s.pump.drive(s.store.snapshot()).unwrap();
    let watch = s.pump.drive(c.harness.watch()).unwrap();
    let events = Arc::new(TerminalEvents(std::sync::Mutex::new(vec![])));
    watch.start(events.clone());
    let mut result = None;
    for _ in 0..100 {
        s.pump.tick();
        if let std::task::Poll::Ready(value) = poll(prompt.as_mut()) {
            result = Some(value);
            break;
        }
        let _ = s.pump.drive(c.harness.execute_action());
    }
    let result = result.expect("public driver settles after observing external terminal");
    assert_eq!(
        s.pump.drive(s.store.snapshot()).unwrap(),
        before,
        "external terminal must not be rewritten"
    );
    if matches!(
        mode,
        "missing" | "mismatch" | "missing-assistant" | "non-assistant"
    ) {
        assert!(
            matches!(result, Err(HarnessRuntimeError::Fault(_))),
            "invalid terminal must fault: {mode}: {result:?}"
        );
        assert!(matches!(
            s.pump.drive(c.harness.get_stream_options()),
            Err(HarnessRuntimeError::Fault(_))
        ));
    } else {
        let delivered = events.0.lock().unwrap();
        assert_eq!(
            delivered.len(),
            1,
            "external completion emits only one operation end: {delivered:?}"
        );
        match &delivered[0] {
            HarnessEvent::RunEndCompletedWithFinal(event) if mode != "aborted" => {
                assert_eq!(event.run_id, original)
            }
            HarnessEvent::RunEndAbortedWithFinal(event) if mode == "aborted" => {
                assert_eq!(event.run_id, original)
            }
            other => panic!("wrong external end lifecycle: {other:?}"),
        }
        drop(delivered);
        let success = result.unwrap().unwrap();
        assert_eq!(
            success.run_id, original,
            "drive must retain original identity"
        );
        let returned = match success.outcome {
            RunOutcome::CompletedWithAssistant(v) if mode != "aborted" => {
                (v.final_entry_id, v.final_message)
            }
            RunOutcome::AbortedWithAssistant(v) if mode == "aborted" => {
                (v.final_entry_id, v.final_message)
            }
            other => panic!("wrong external outcome: {mode}: {other:?}"),
        };
        assert_eq!(returned.0, final_id);
        let entries = s.pump.drive(s.session.get_entries(&[final_id])).unwrap();
        let Entry::Message(entry) = entries.values().next().unwrap() else {
            unreachable!()
        };
        assert_eq!(
            AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(Box::new(
                returned.1.into_message()
            ))),
            entry.message
        );
        assert!(s.pump.drive(c.harness.get_stream_options()).is_ok());
        assert!(
            s.pump.drive(unrelated.get_model()).is_ok(),
            "other lanes must remain live"
        );
        assert_eq!(
            s.pump.drive(s.store.snapshot()).unwrap(),
            before,
            "observing completion cannot publish"
        );
    }
}
#[test]
fn external_terminal_preserves_original_identity_and_reconstructs_durable_result() {
    for mode in ["valid", "aborted", "new-operation"] {
        check_external_terminal(mode);
    }
}
#[test]
fn malformed_external_terminal_remains_an_owner_fault() {
    for mode in ["missing", "mismatch", "missing-assistant", "non-assistant"] {
        check_external_terminal(mode);
    }
}

struct TerminalEvents(std::sync::Mutex<Vec<HarnessEvent>>);
impl EventListener for TerminalEvents {
    fn on_event(&self, event: HarnessEvent) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(async move {
            self.0.lock().unwrap().push(event);
            Ok(())
        })
    }
}
