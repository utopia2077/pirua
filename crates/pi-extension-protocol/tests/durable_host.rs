mod durable_support;
use durable_support::*;
use pi_extension_protocol::{
    host::{self},
    profiles::{ApprovedProfile, Exposure, ProfileApproval, Registry},
    wire::v1_1 as w,
};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
fn input() -> serde_json::Value {
    json!({"context":{"conversationId":1,"taskId":2},"call":{"type":"toolCall","id":"call","name":"remote","arguments":{}},"progress":{"enabled":true,"outputLimits":{"maxBytes":50000,"maxLines":2000,"retain":"head"}}})
}
#[tokio::test]
async fn closed_inventory_and_async_progress_credit_wait_for_ack() {
    let registry = Registry::durable().unwrap();
    assert_eq!(registry.roots().count(), 17);
    for m in [
        "before_request",
        "after_response",
        "on_yield",
        "after_tools",
        "before_tool",
        "after_tool",
        "before_compact",
        "tool.call",
    ] {
        assert!(registry.method("durable.v1", m).is_some());
    }
    assert!(registry.method("durable.v1", "session.snapshot").is_none());
    let (c, mut peer) = ready(&["tool:remote"], "scope").await;
    let cap = c.grant().profiles()[0].reference().clone();
    let caller = c.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let e = entered.clone();
    let r = release.clone();
    let task = tokio::spawn(async move {
        caller
            .request_with_progress_ack(
                cap,
                "tool.call".into(),
                host::bound(
                    &Registry::durable().unwrap(),
                    "durable.input:tool.call",
                    &input(),
                )
                .unwrap(),
                validator(),
                Box::pin(std::future::pending()),
                move |_| {
                    let e = e.clone();
                    let r = r.clone();
                    async move {
                        e.notify_one();
                        r.notified().await;
                        Ok(())
                    }
                },
            )
            .await
    });
    let w::envelope::Frame::Request(req) = peer.recv().await else {
        panic!()
    };
    peer.progress(req.request_id, 1, json!({"type":"details","value":null}))
        .await;
    entered.notified().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.recv())
            .await
            .is_err()
    );
    release.notify_one();
    let w::envelope::Frame::Credit(credit) = peer.recv().await else {
        panic!("missing settled credit")
    };
    assert_eq!(credit.request_id, req.request_id);
    assert!(credit.additional_bytes > 0);
    peer.answer(
        req.request_id,
        "tool.call",
        json!({"result":{"output":[],"details":null,"structuredOutput":null}}),
        true,
    )
    .await;
    let result = task.await.unwrap().unwrap();
    assert_eq!(result["result"]["details"], serde_json::Value::Null);
    c.close();
}
#[tokio::test]
async fn cancellation_finishes_admitted_ack_without_starting_buffered_callbacks() {
    let (c, mut peer) = ready(&["tool:remote"], "scope").await;
    let cap = c.grant().profiles()[0].reference().clone();
    let caller = c.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let cancelled = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let (e, r, n, k) = (
        entered.clone(),
        release.clone(),
        cancelled.clone(),
        calls.clone(),
    );
    let task = tokio::spawn(async move {
        caller
            .request_with_progress_ack(
                cap,
                "tool.call".into(),
                host::bound(
                    &Registry::durable().unwrap(),
                    "durable.input:tool.call",
                    &input(),
                )
                .unwrap(),
                validator(),
                Box::pin(async move { n.notified().await }),
                move |_| {
                    k.fetch_add(1, Ordering::SeqCst);
                    let e = e.clone();
                    let r = r.clone();
                    async move {
                        e.notify_one();
                        r.notified().await;
                        Ok(())
                    }
                },
            )
            .await
    });
    let w::envelope::Frame::Request(req) = peer.recv().await else {
        panic!()
    };
    peer.progress(req.request_id, 1, json!({"type":"details","value":1}))
        .await;
    entered.notified().await;
    peer.progress(req.request_id, 2, json!({"type":"details","value":2}))
        .await;
    cancelled.notify_one();
    let w::envelope::Frame::Cancel(cancel) = peer.recv().await else {
        panic!("cancel must run while ack waits")
    };
    assert_eq!(cancel.request_id, req.request_id);
    assert!(!task.is_finished());
    release.notify_one();
    assert_eq!(task.await.unwrap(), Err(host::Error::Cancelled));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.recv())
            .await
            .is_err()
    );
    c.close();
}
#[tokio::test]
async fn terminal_waits_for_ack_and_failure_cannot_resolve_success() {
    for failure in [false, true] {
        let (c, mut peer) = ready(&["tool:remote"], "scope").await;
        let cap = c.grant().profiles()[0].reference().clone();
        let caller = c.clone();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let (e, r) = (entered.clone(), release.clone());
        let task = tokio::spawn(async move {
            caller
                .request_with_progress_ack(
                    cap,
                    "tool.call".into(),
                    host::bound(
                        &Registry::durable().unwrap(),
                        "durable.input:tool.call",
                        &input(),
                    )
                    .unwrap(),
                    validator(),
                    Box::pin(std::future::pending()),
                    move |_| {
                        let e = e.clone();
                        let r = r.clone();
                        async move {
                            e.notify_one();
                            r.notified().await;
                            if failure {
                                Err(host::Error::HostValidation)
                            } else {
                                Ok(())
                            }
                        }
                    },
                )
                .await
        });
        let w::envelope::Frame::Request(req) = peer.recv().await else {
            panic!()
        };
        peer.progress(
            req.request_id,
            1,
            json!({"type":"diagnostic","diagnostic":{"severity":"warn","message":"running"}}),
        )
        .await;
        entered.notified().await;
        peer.answer(
            req.request_id,
            "tool.call",
            json!({"result":{"output":[]}}),
            true,
        )
        .await;
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        release.notify_one();
        let result = task.await.unwrap();
        assert_eq!(result.is_ok(), !failure);
        c.close();
    }
}
#[test]
fn host_approvals_are_closed_and_fractional_intervals_are_preserved() {
    let r = Registry::durable().unwrap();
    let mut v = input();
    v["progress"]["outputLimits"]["retain"] = json!("tail");
    v["progress"]["outputWindow"] =
        json!({"maxBytes":32,"maxLines":4,"minIntervalMs":0.125,"bytesPerSecond":100});
    let b = host::bound(&r, "durable.input:tool.call", &v).unwrap();
    let output = r
        .validate_v1_1("durable.input:tool.call", &b, Default::default(), 100000)
        .unwrap();
    assert_eq!(
        output["progress"]["outputWindow"]["minIntervalMs"],
        json!(0.125)
    );
    v["lease"] = json!(1);
    let b = host::bound(&r, "durable.input:tool.call", &v).unwrap();
    assert!(
        r.validate_v1_1("durable.input:tool.call", &b, Default::default(), 100000)
            .is_err()
    );
    for (name, execution, replay) in [
        ("session.snapshot", 1, 1),
        ("arbitrary", 2, 1),
        ("tool:remote", 2, 3),
    ] {
        assert!(
            ApprovedProfile::durable(ProfileApproval {
                principal: "peer".into(),
                kind: "durable".into(),
                name: name.into(),
                profile: "durable.v1".into(),
                handler: "h".into(),
                minors: BTreeSet::from([1]),
                replay,
                execution,
                scope: "s".into(),
                permissions: BTreeSet::new(),
                exposure: Exposure::new(BTreeMap::new(), BTreeSet::new()).unwrap()
            })
            .is_err()
        );
    }
}

#[tokio::test]
async fn durable_peer_request_cannot_reach_coding_snapshot_or_host_actions() {
    let (connection, mut peer) = ready(&["tool:remote"], "scope").await;
    connection
        .set_snapshot_handler(Arc::new(|| panic!("Durable peer reached Coding snapshot")))
        .unwrap();
    let capability = connection.grant().profiles()[0].reference().clone();
    peer.send(w::envelope::Frame::Request(w::Request {
        request_id: 2,
        parent_request_id: None,
        capability_ref: Some(capability),
        effect_key: None,
        method: "session.snapshot".into(),
        deadline_after_ms: Some(1000),
        initial_stream_credit_bytes: None,
        payload: vec![],
    }))
    .await;
    let w::envelope::Frame::Response(response) = peer.recv().await else {
        panic!()
    };
    assert_eq!(response.request_id, 2);
    let Some(w::response::Terminal::Error(error)) = response.terminal else {
        panic!()
    };
    assert_eq!(error.code, w::ErrorCode::Forbidden as i32);
    assert!(connection.current());
    connection.close();
}
