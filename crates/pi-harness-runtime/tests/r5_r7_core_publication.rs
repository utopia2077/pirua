#[allow(dead_code)]
mod operation_state_model_support;
mod r5_r7_core_support;
use operation_state_model_support::{Model, ready};
use pi_agent_schema::*;
use pi_agent_session::storage::ConditionalOutcome;
use pi_harness_core::inbox;
use r5_r7_core_support::*;
#[test]
fn real_memory_consume_deletes_payload_and_reopened_state_keeps_skip() {
    let m = Model::new();
    enqueue(&m, &["a", "b"]);
    let r = selected_recipe(&m);
    assert!(matches!(publish(&m, r), ConditionalOutcome::Applied(_)));
    assert!(m.register(RegisterNamespace::PendingEntry, "a").is_none());
    assert!(m.register(RegisterNamespace::PendingEntry, "b").is_some());
    assert!(
        ready(m.session.get_entries(&[id("a")]))
            .unwrap()
            .contains_key(&id("a"))
    );
    // A fresh bounded load from actual storage, followed by durable codec roundtrip.
    let (mut c, _, _) = m.current();
    c.state = serde_json::from_value(serde_json::to_value(&c.state).unwrap()).unwrap();
    assert_eq!(inbox::select(&c).unwrap(), None);
}
#[test]
fn actual_memory_rejects_each_stale_queue_control_or_payload_recipe() {
    for change in ["enqueue", "abort", "payload"] {
        let m = Model::new();
        enqueue(&m, &["a", "b"]);
        let recipe = selected_recipe(&m);
        match change {
            "enqueue" => enqueue(&m, &["late"]),
            "abort" => cancel(&m),
            _ => {
                let Register::PendingEntry { value, .. } =
                    m.register(RegisterNamespace::PendingEntry, "a").unwrap()
                else {
                    panic!()
                };
                ready(m.session.commit(Transaction {
                    writes: vec![Write::SetRegister(Box::new(RegisterSet::PendingEntry {
                        key: "a".into(),
                        value,
                    }))],
                }))
                .unwrap();
            }
        }
        let before = m.snapshot();
        assert!(
            matches!(publish(&m, recipe), ConditionalOutcome::NotApplied),
            "{change}"
        );
        assert_eq!(m.snapshot(), before);
        assert!(ready(m.session.get_entries(&[id("a")])).unwrap().is_empty());
    }
}
#[test]
fn consume_first_then_abort_keeps_placed_entry_and_drains_only_remainder() {
    let m = Model::new();
    enqueue(&m, &["a", "b"]);
    assert!(matches!(
        publish(&m, selected_recipe(&m)),
        ConditionalOutcome::Applied(_)
    ));
    cancel(&m);
    let (c, _, _) = m.current();
    let OperationState::Run(r) = c.state else {
        panic!()
    };
    let Control::CancelRequested(control) = r.control else {
        panic!()
    };
    assert_eq!(control.drained_steer, vec![id("b")]);
    assert!(m.register(RegisterNamespace::PendingEntry, "a").is_none());
    assert!(m.register(RegisterNamespace::PendingEntry, "b").is_some());
    assert!(
        ready(m.session.get_entries(&[id("a")]))
            .unwrap()
            .contains_key(&id("a"))
    );
}
