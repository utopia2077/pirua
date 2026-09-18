//! Discovery of another Session writer's terminal result retires the old summary.
mod runtime_r1_r4_support;
mod runtime_r5_r7_support;
use pi_agent_schema::*;
use pi_agent_session::storage::AtomicStorage;
use runtime_r1_r4_support::*;
use runtime_r5_r7_support::{factory, user};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};
#[derive(Default)]
struct Converter {
    entered: AtomicUsize,
    drops: Arc<AtomicUsize>,
}
struct Live(Arc<AtomicUsize>);
impl Drop for Live {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl ProviderMessageConverter for Converter {
    fn convert(
        &self,
        _: Vec<AgentMessage>,
    ) -> BoxFuture<'_, Result<Vec<pi_ai_core::Message>, HarnessRuntimeError>> {
        Box::pin(async move {
            let _live = Live(self.drops.clone());
            self.entered.fetch_add(1, Ordering::SeqCst);
            std::future::pending().await
        })
    }
}
fn one_task(s: &Setup) {
    let task = {
        let mut tasks = s.pump.tasks.lock().unwrap();
        (!tasks.is_empty()).then(|| tasks.remove(0))
    };
    if let Some(mut task) = task
        && poll(task.as_mut()).is_pending()
    {
        s.pump.tasks.lock().unwrap().push(task);
    }
}
#[test]
fn external_terminal_discovery_retires_converter_without_fault_or_global_close() {
    let s = Setup::new();
    let converter = Arc::new(Converter::default());
    let mut options = s.options(DriveMode::Automatic);
    options.to_provider_messages = Some(converter.clone());
    let h = s.pump.drive(factory(&s).create(options)).unwrap().harness;
    let leaf = s
        .pump
        .drive(h.session().append_message(user("summary selected history")))
        .unwrap();
    let mut call = Box::pin(h.compact(CompactOptions::default()));
    assert!(poll(call.as_mut()).is_pending());
    for _ in 0..1000 {
        one_task(&s);
        if converter.entered.load(Ordering::SeqCst) == 1 {
            break;
        }
    }
    assert_eq!(converter.entered.load(Ordering::SeqCst), 1);
    assert_eq!(converter.drops.load(Ordering::SeqCst), 0);
    let Some(Register::LaneState { mut value, .. }) = s
        .pump
        .drive(s.session.get_register(RegisterNamespace::LaneState, "main"))
        .unwrap()
    else {
        panic!("lane state")
    };
    let operation = value.current_operation_id.take().unwrap();
    let Some(Register::OperationState { value: state, .. }) = s
        .pump
        .drive(
            s.session
                .get_register(RegisterNamespace::OperationState, operation.as_str()),
        )
        .unwrap()
    else {
        panic!("operation state")
    };
    assert!(
        matches!(state.as_ref(),OperationState::Compaction(c)if matches!(&c.structural,StructuralDecision::Generating{generation,..}if matches!(generation.as_ref(),SummaryGeneration::EffectPending{request:None,usage_ids,..}if usage_ids.is_empty())))
    );
    let error = OperationError {
        code: OperationErrorCode::new("external_done").unwrap(),
        message: "independent Session terminal".into(),
        details: pi_ai_core::Presence::Missing,
    };
    let last = LaneLastResult::CompactionFailed(CompactionFailedLastResult {
        operation_id: operation.clone(),
        kind: Default::default(),
        leaf_id: Some(leaf.clone()),
        outcome: Default::default(),
        error: error.clone(),
    });
    let mut writes = vec![];
    for namespace in [
        RegisterNamespace::OperationMeta,
        RegisterNamespace::OperationState,
    ] {
        writes.push(Write::DeleteRegister(RegisterDelete {
            namespace,
            key: operation.as_str().into(),
        }));
    }
    for r in s
        .pump
        .drive(s.session.list_registers(
            RegisterNamespace::OperationPreparation,
            Some(&format!("{operation}:")),
        ))
        .unwrap()
    {
        let Register::OperationPreparation { key, .. } = r else {
            unreachable!()
        };
        writes.push(Write::DeleteRegister(RegisterDelete {
            namespace: RegisterNamespace::OperationPreparation,
            key,
        }));
    }
    writes.push(Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
        key: "main".into(),
        value: last.clone(),
    })));
    writes.push(Write::SetRegister(Box::new(RegisterSet::LaneState {
        key: "main".into(),
        value,
    })));
    s.pump
        .drive(s.session.commit(Transaction { writes }))
        .unwrap();
    let terminal = s.pump.drive(s.store.snapshot()).unwrap();
    s.session.codec().validate_snapshot(&terminal).unwrap();
    let mut result = None;
    for _ in 0..128 {
        one_task(&s);
        if result.is_none()
            && let Poll::Ready(value) = poll(call.as_mut())
        {
            result = Some(value);
        }
    }
    let result = result
        .expect("the parent must actually discover terminal")
        .unwrap()
        .unwrap();
    assert_eq!(result.run_id, operation);
    assert!(
        matches!(result.outcome,CompactionOutcome::Failed(f)if f.leaf_id==leaf&&f.error==error)
    );
    assert_eq!(s.pump.drive(h.get_last_result()).unwrap(), Some(last));
    assert_eq!(s.pump.drive(s.store.snapshot()).unwrap(), terminal);
    assert!(s.faux.requests.lock().unwrap().is_empty());
    assert!(!s.pump.drive(h.watch()).unwrap().snapshot().faulted);
    let dropped_before_close = converter.drops.load(Ordering::SeqCst);
    s.pump.drive(h.close()).unwrap();
    for _ in 0..32 {
        one_task(&s);
    }
    assert!(s.pump.tasks.lock().unwrap().is_empty());
    assert_eq!(
        dropped_before_close, 1,
        "external discovery must release old converter before global close"
    );
}
