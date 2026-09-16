//! Single bounded review661 probe; Setup/Pump/Faux borrowed read-only from runtime_r1_r4_support.
mod runtime_r1_r4_support;
use pi_agent_schema::*;
use pi_harness_runtime::owner::RuntimeFactory;
use runtime_r1_r4_support::*;
use std::sync::Arc;
#[rustfmt::skip]
#[test]
fn valid_external_terminal_during_live_request_preserves_owner() {
    use pi_agent_session::storage::AtomicStorage;
    let s = Setup::new();
    let hold = pi_ai_core::CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(hold.clone());
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "stale model".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    let Some(Register::LaneState { value, .. }) = s
        .pump
        .drive(s.session.get_register(RegisterNamespace::LaneState, "main"))
        .unwrap()
    else {
        panic!("lane")
    };
    let id = value.current_operation_id.unwrap();
    let Some(Register::LaneLeaf { value: Some(leaf), .. }) = s.pump.drive(s.session.get_register(RegisterNamespace::LaneLeaf, "main")).unwrap() else { panic!("accepted prompt leaf") };
    let unrelated = s.pump.drive(c.harness.create_lane(LaneId::new("unrelated").unwrap(), None)).unwrap().unwrap();
    s.pump
        .drive(s.session.commit(Transaction {
            writes: vec![
                Write::SetRegister(Box::new(RegisterSet::LaneLastResult { key: "main".into(), value: LaneLastResult::RunAborted(RunAbortedLastResult { operation_id: id.clone(), kind: Default::default(), leaf_id: Some(leaf.clone()), final_assistant_entry_id: None, outcome: Default::default() }) })),
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationMeta,
                    key: id.as_str().into(),
                }),
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationState,
                    key: id.as_str().into(),
                }),
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
    hold.cancel();
    let result = s.pump.drive(prompt);
    let owner_health = s.pump.drive(c.harness.get_stream_options());
    let other_health = s.pump.drive(unrelated.get_model());
    println!("live terminal result={result:?}; owner={owner_health:?}; other_lane={other_health:?}");
    assert_eq!(s.pump.drive(s.store.snapshot()).unwrap(), before);
    assert!(before.usage.is_empty());
    assert!(matches!(result, Ok(Ok(RunSuccess { run_id, outcome: RunOutcome::AbortedWithoutAssistant(_), .. })) if run_id == id), "valid external terminal while a real provider was live must resolve its durable aborted result");
    assert!(owner_health.is_ok());
    assert!(other_health.is_ok());
}

use std::sync::atomic::{AtomicUsize, Ordering};
struct ResponseProbe {
    entered: Arc<AtomicUsize>,
    release: Option<pi_ai_core::CancellationToken>,
}
impl HookHandler<AfterResponse> for ResponseProbe {
    fn call(
        &self,
        _: AfterResponseEvent,
    ) -> BoxFuture<'_, Result<Option<AfterResponseResult>, HandlerError>> {
        Box::pin(async move {
            self.entered.fetch_add(1, Ordering::SeqCst);
            if let Some(release) = &self.release {
                release.cancelled().await;
            }
            Ok(None)
        })
    }
}

fn late_response_history(mode: &str, started_hook: bool) {
    use pi_agent_session::storage::AtomicStorage;
    let s = Setup::new();
    let provider_release = pi_ai_core::CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(provider_release.clone());
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_release = pi_ai_core::CancellationToken::new();
    let _subscription = c
        .harness
        .hooks()
        .after_response(
            None,
            Arc::new(ResponseProbe {
                entered: calls.clone(),
                release: started_hook.then(|| hook_release.clone()),
            }),
        )
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "late callback authority".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    assert_eq!(
        s.faux.requests.lock().unwrap().len(),
        1,
        "a real request must already be started"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    if started_hook {
        provider_release.cancel();
        for _ in 0..100 {
            s.pump.tick();
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "hook must have started before external terminal"
        );
    }
    let Some(Register::LaneState { value, .. }) = s
        .pump
        .drive(s.session.get_register(RegisterNamespace::LaneState, "main"))
        .unwrap()
    else {
        panic!()
    };
    let original_id = value.current_operation_id.unwrap();
    let Some(Register::LaneLeaf {
        value: Some(leaf), ..
    }) = s
        .pump
        .drive(s.session.get_register(RegisterNamespace::LaneLeaf, "main"))
        .unwrap()
    else {
        panic!()
    };
    let unrelated = s
        .pump
        .drive(c.harness.create_lane(LaneId::new("other").unwrap(), None))
        .unwrap()
        .unwrap();
    let mut writes = vec![
        Write::DeleteRegister(RegisterDelete {
            namespace: RegisterNamespace::OperationMeta,
            key: original_id.as_str().into(),
        }),
        Write::DeleteRegister(RegisterDelete {
            namespace: RegisterNamespace::OperationState,
            key: original_id.as_str().into(),
        }),
        Write::SetRegister(Box::new(RegisterSet::LaneState {
            key: "main".into(),
            value: LaneState {
                current_operation_id: None,
                pending_next_run: vec![],
            },
        })),
    ];
    if mode != "missing-result" {
        let last_id = if mode == "wrong-id" {
            OperationId::new("wrong-result-owner").unwrap()
        } else {
            original_id.clone()
        };
        writes.push(Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
            key: "main".into(),
            value: LaneLastResult::RunAborted(RunAbortedLastResult {
                operation_id: last_id,
                kind: Default::default(),
                leaf_id: Some(leaf.clone()),
                final_assistant_entry_id: (mode == "wrong-assistant").then(|| leaf.clone()),
                outcome: Default::default(),
            }),
        })));
    }
    if mode == "new-operation" {
        let Some(Register::OperationMeta { mut value, .. }) = s
            .pump
            .drive(
                s.session
                    .get_register(RegisterNamespace::OperationMeta, original_id.as_str()),
            )
            .unwrap()
        else {
            panic!()
        };
        let Some(Register::OperationState {
            value: next_state, ..
        }) = s
            .pump
            .drive(
                s.session
                    .get_register(RegisterNamespace::OperationState, original_id.as_str()),
            )
            .unwrap()
        else {
            panic!()
        };
        let next_id = OperationId::new("replacement-operation").unwrap();
        value.operation_id = next_id.clone();
        writes.push(Write::SetRegister(Box::new(RegisterSet::OperationMeta {
            key: next_id.as_str().into(),
            value,
        })));
        writes.push(Write::SetRegister(Box::new(RegisterSet::OperationState {
            key: next_id.as_str().into(),
            value: next_state,
        })));
        writes.push(Write::SetRegister(Box::new(RegisterSet::LaneState {
            key: "main".into(),
            value: LaneState {
                current_operation_id: Some(next_id),
                pending_next_run: vec![],
            },
        })));
    }
    s.pump
        .drive(s.session.commit(Transaction { writes }))
        .unwrap();
    let terminal = s.pump.drive(s.store.snapshot()).unwrap();
    provider_release.cancel();
    hook_release.cancel();
    let result = s.pump.drive(prompt);
    assert_eq!(
        calls.load(Ordering::SeqCst),
        usize::from(started_hook),
        "retired operation must not start a late response hook"
    );
    assert_eq!(
        s.pump.drive(s.store.snapshot()).unwrap(),
        terminal,
        "late output must not commit entries, usage, or replace the terminal/new operation"
    );
    assert!(terminal.usage.is_empty());
    if matches!(mode, "valid" | "new-operation") {
        assert!(
            matches!(result, Ok(Ok(RunSuccess { run_id, outcome: RunOutcome::AbortedWithoutAssistant(_), .. })) if run_id == original_id)
        );
        assert!(s.pump.drive(c.harness.get_stream_options()).is_ok());
        assert!(s.pump.drive(unrelated.get_model()).is_ok());
    } else {
        assert!(
            matches!(result, Err(HarnessRuntimeError::Fault(_))),
            "invalid terminal must still fault: {mode}"
        );
        assert!(matches!(
            s.pump.drive(c.harness.get_stream_options()),
            Err(HarnessRuntimeError::Fault(_))
        ));
    }
}

#[test]
fn retired_unstarted_response_handler_is_skipped_without_new_writes() {
    late_response_history("valid", false);
}
#[test]
fn old_response_handler_cannot_run_for_the_replacement_operation() {
    late_response_history("new-operation", false);
}
#[test]
fn an_already_started_response_handler_can_finish_after_external_terminal() {
    late_response_history("valid", true);
}
#[test]
fn late_output_does_not_hide_missing_mismatched_or_invalid_terminal_data() {
    for mode in ["missing-result", "wrong-id", "wrong-assistant"] {
        late_response_history(mode, false);
    }
}

fn publish_external_terminal(s: &Setup) -> OperationId {
    let Some(Register::LaneState { value, .. }) = s
        .pump
        .drive(s.session.get_register(RegisterNamespace::LaneState, "main"))
        .unwrap()
    else {
        panic!()
    };
    let id = value.current_operation_id.unwrap();
    let Some(Register::LaneLeaf {
        value: Some(leaf), ..
    }) = s
        .pump
        .drive(s.session.get_register(RegisterNamespace::LaneLeaf, "main"))
        .unwrap()
    else {
        panic!()
    };
    s.pump
        .drive(s.session.commit(Transaction {
            writes: vec![
                Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
                    key: "main".into(),
                    value: LaneLastResult::RunAborted(RunAbortedLastResult {
                        operation_id: id.clone(),
                        kind: Default::default(),
                        leaf_id: Some(leaf),
                        final_assistant_entry_id: None,
                        outcome: Default::default(),
                    }),
                })),
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationMeta,
                    key: id.as_str().into(),
                }),
                Write::DeleteRegister(RegisterDelete {
                    namespace: RegisterNamespace::OperationState,
                    key: id.as_str().into(),
                }),
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
    id
}

struct TransformProbe {
    entered: Arc<AtomicUsize>,
    release: pi_ai_core::CancellationToken,
}
impl HookHandler<TransformContext> for TransformProbe {
    fn call(
        &self,
        _: TransformContextEvent,
    ) -> BoxFuture<'_, Result<Option<TransformContextResult>, HandlerError>> {
        Box::pin(async move {
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.release.cancelled().await;
            Ok(None)
        })
    }
}
struct PendingConverter(Arc<AtomicUsize>);
impl ProviderMessageConverter for PendingConverter {
    fn convert(
        &self,
        _: Vec<AgentMessage>,
    ) -> BoxFuture<'_, Result<Vec<pi_ai_core::Message>, HarnessRuntimeError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        })
    }
}
#[test]
fn retired_transform_continuation_does_not_start_pending_converter() {
    use pi_agent_session::storage::AtomicStorage;
    let s = Setup::new();
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let mut options = s.options(DriveMode::Automatic);
    options.to_provider_messages = Some(Arc::new(PendingConverter(calls.clone())));
    let c = s.pump.drive(f.create(options)).unwrap();
    let entered = Arc::new(AtomicUsize::new(0));
    let release = pi_ai_core::CancellationToken::new();
    let _subscription = c
        .harness
        .hooks()
        .transform_context(
            None,
            Arc::new(TransformProbe {
                entered: entered.clone(),
                release: release.clone(),
            }),
        )
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "retired transform".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        s.pump.tick();
    }
    assert_eq!(entered.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(s.faux.requests.lock().unwrap().is_empty());
    let id = publish_external_terminal(&s);
    let terminal = s.pump.drive(s.store.snapshot()).unwrap();
    release.cancel();
    let result = s.pump.drive(prompt);
    assert!(
        matches!(result, Ok(Ok(RunSuccess { run_id, outcome: RunOutcome::AbortedWithoutAssistant(_), .. })) if run_id == id)
    );
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no newly started converter after observed retirement"
    );
    assert_eq!(s.pump.drive(s.store.snapshot()).unwrap(), terminal);
    assert!(s.pump.drive(c.harness.get_stream_options()).is_ok());
}

struct PayloadProbe(Arc<AtomicUsize>);
impl HookHandler<BeforePayload> for PayloadProbe {
    fn call(
        &self,
        _: BeforePayloadEvent,
    ) -> BoxFuture<'_, Result<Option<BeforePayloadResult>, HandlerError>> {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        })
    }
}
#[test]
fn retired_payload_gate_completes_without_waiting_for_live_adapter() {
    use pi_agent_session::storage::AtomicStorage;
    let s = Setup::new();
    let hold = pi_ai_core::CancellationToken::new();
    *s.faux.hold.lock().unwrap() = Some(hold.clone());
    let f = RuntimeFactory::new(
        s.time.clone(),
        s.pump.clone(),
        s.ids.clone(),
        Arc::new(()),
        Arc::new(Summary),
    );
    let c = s
        .pump
        .drive(f.create(s.options(DriveMode::Automatic)))
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let _subscription = c
        .harness
        .hooks()
        .before_payload(None, Arc::new(PayloadProbe(calls.clone())))
        .unwrap();
    let mut prompt = Box::pin(c.harness.prompt(PromptInput::Text {
        text: "retired payload".into(),
        images: vec![],
    }));
    assert!(poll(prompt.as_mut()).is_pending());
    for _ in 0..100 {
        if !s.faux.requests.lock().unwrap().is_empty() {
            break;
        }
        s.pump.tick();
    }
    assert_eq!(s.faux.requests.lock().unwrap().len(), 1);
    assert!(
        s.faux.payloads.lock().unwrap().is_empty(),
        "payload host task has not started yet"
    );
    let id = publish_external_terminal(&s);
    let terminal = s.pump.drive(s.store.snapshot()).unwrap();
    let result = s.pump.drive(prompt);
    assert!(
        !hold.is_cancelled(),
        "adapter must remain paused to prove forward progress"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(
        matches!(result, Ok(Ok(RunSuccess { run_id, outcome: RunOutcome::AbortedWithoutAssistant(_), .. })) if run_id == id)
    );
    assert_eq!(s.pump.drive(s.store.snapshot()).unwrap(), terminal);
    assert!(s.pump.drive(c.harness.get_stream_options()).is_ok());
    // Keep the adapter paused: the validated stream has already settled by
    // cancellation, independently of this deliberately uncooperative producer.
}
