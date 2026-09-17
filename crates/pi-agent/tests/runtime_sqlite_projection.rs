#![cfg(feature = "sqlite")]
mod runtime_sqlite_projection_support;
use pi_agent_schema::*;
use pi_ai_core::{JsonValue, Presence};
use runtime_sqlite_projection_support::*;
use std::sync::atomic::Ordering;

fn history(expire: bool) {
    let w = World::new();
    let h = w.harness.clone();
    let mut prompt = Box::pin(async move {
        h.prompt(PromptInput::Text {
            text: "actual projection boundary".into(),
            images: vec![],
        })
        .await
    });
    assert!(poll(prompt.as_mut()).is_pending());
    w.pump.until(|| w.adapter.calls.load(Ordering::SeqCst) == 1);
    let data = Presence::Value(JsonValue::String("exact-data".into()));
    let id = w
        .pump
        .drive(
            w.harness
                .session()
                .append_custom_entry(CustomType::new("metadata-sensitive").unwrap(), data.clone()),
        )
        .unwrap();
    assert!(
        w.pump
            .drive(w.session.get_entries(std::slice::from_ref(&id)))
            .unwrap()
            .is_empty(),
        "active append is pending, not placed"
    );
    w.adapter.release_first.cancel();
    w.pump
        .until(|| !w.projector.rows.lock().unwrap().is_empty());
    assert!(
        poll(prompt.as_mut()).is_pending(),
        "actual projector must hold completion"
    );
    let rows = w.projector.rows.lock().unwrap().clone();
    assert_eq!(rows.len(), 1);
    let observed = rows[0].clone();
    assert_eq!(observed.id, id);
    assert_eq!(observed.data, data);
    assert!(observed.parent_id.is_some());
    assert_eq!(observed.timestamp, n(7));
    assert_eq!(observed.entry_type, CustomEntryType::Custom);
    let path = w.session.metadata().path;
    let prefix = database(&path, false).unwrap();
    assert!(
        w.pump
            .drive(w.session.get_entries(std::slice::from_ref(&id)))
            .unwrap()
            .is_empty()
    );
    if expire {
        database(&path, true);
    }
    w.time.0.store(9000, Ordering::SeqCst);
    w.projector.release.cancel();
    let outcome = w.pump.drive(prompt);
    if expire {
        assert!(
            matches!(
                outcome,
                Err(HarnessRuntimeError::Fault(_)) | Err(HarnessRuntimeError::Closed(_))
            ),
            "lease loss cannot report a committed run: {outcome:?}"
        );
        assert_eq!(
            database(&path, false).unwrap(),
            prefix,
            "lease expiry during actual projection must preserve every nonlease table exactly"
        );
        assert_eq!(
            w.adapter.calls.load(Ordering::SeqCst),
            1,
            "no post-expiry continuation request"
        );
    } else {
        assert!(
            matches!(outcome, Ok(Ok(_))),
            "control run must complete: {outcome:?}"
        );
        let committed = w
            .pump
            .drive(w.session.get_entries(std::slice::from_ref(&id)))
            .unwrap();
        assert_eq!(
            committed[&id],
            Entry::Custom(observed.clone()),
            "all projector-visible CustomEntry fields must equal actual committed row"
        );
        assert!(
            w.projector
                .rows
                .lock()
                .unwrap()
                .iter()
                .all(|row| row == &observed)
        );
        assert_eq!(
            w.adapter.calls.load(Ordering::SeqCst),
            2,
            "projected context must drive the required continuation"
        );
        assert_ne!(database(&path, false).unwrap(), prefix);
    }
}
#[test]
fn public_sqlite_projection_commits_exact_visible_entry_metadata() {
    history(false);
}
#[test]
fn public_sqlite_expiry_during_pending_projection_preserves_durable_prefix() {
    history(true);
}
