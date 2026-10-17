mod current_support;
use current_support::*;
use pi_extension_protocol::{
    host::{self},
    profiles::{NativeError, Registry, Stage},
    wire::v1_1 as w,
};
use serde_json::json;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}
#[test]
fn current_registry_grant_closed_shapes_and_real_handshake() {
    runtime().block_on(async {
        let (c, mut peer) = ready(&["input"], "s").await;
        assert!(c.current());
        assert_eq!(c.grant().profiles().len(), 1);
        assert!(
            c.grant()
                .profiles()
                .iter()
                .all(|p| p.reference().name != "manifest_only")
        );
        let cap = c.grant().profiles()[0].reference().clone();
        let call = c.clone();
        let count = Arc::new(AtomicUsize::new(0));
        let captured = count.clone();
        let task = tokio::spawn(async move {
            let payload =
                host::bound(&Registry::coding().unwrap(), "coding.input:input", &input()).unwrap();
            call.request(
                cap,
                "input".into(),
                payload,
                Arc::new(move |p| {
                    captured.fetch_add(1, Ordering::SeqCst);
                    if p.stage == Stage::Terminal && p.fields["text"] != "new" {
                        Err(NativeError::Rejected)
                    } else {
                        Ok(())
                    }
                }),
                Box::pin(std::future::pending()),
                |_| Ok(()),
            )
            .await
        });
        let w::envelope::Frame::Request(r) = peer.recv().await else {
            panic!()
        };
        assert_eq!(value(&r.payload), input());
        assert_eq!(r.effect_key, None);
        assert_eq!(r.parent_request_id, None);
        peer.answer(
            r.request_id,
            "input",
            json!({"action":"transform","text":"new"}),
            false,
        )
        .await;
        assert_eq!(task.await.unwrap().unwrap()["text"], "new");
        assert_eq!(count.load(Ordering::SeqCst), 2);
        // A duplicate terminal belongs to the retired ID and cannot settle a new call.
        peer.answer(r.request_id, "input", json!({"action":"handled"}), false)
            .await;
        let cap = c.grant().profiles()[0].reference().clone();
        let call = c.clone();
        let task = tokio::spawn(async move {
            call.request(
                cap,
                "input".into(),
                host::bound(&Registry::coding().unwrap(), "coding.input:input", &input()).unwrap(),
                validator(),
                Box::pin(std::future::pending()),
                |_| Ok(()),
            )
            .await
        });
        let w::envelope::Frame::Request(r2) = peer.recv().await else {
            panic!()
        };
        assert!(r2.request_id > r.request_id);
        peer.answer(r2.request_id, "input", json!({"action":"continue"}), false)
            .await;
        assert_eq!(task.await.unwrap().unwrap()["action"], "continue");
        c.close();
    })
}
#[test]
fn cancel_late_first_terminal_and_stale_close() {
    runtime().block_on(async {
        let (c, mut peer) = ready(&["input"], "s").await;
        let cap = c.grant().profiles()[0].reference().clone();
        let call = c.clone();
        let (cancel, signal) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            call.request(
                cap,
                "input".into(),
                host::bound(&Registry::coding().unwrap(), "coding.input:input", &input()).unwrap(),
                validator(),
                Box::pin(async {
                    let _ = signal.await;
                }),
                |_| Ok(()),
            )
            .await
        });
        let w::envelope::Frame::Request(r) = peer.recv().await else {
            panic!()
        };
        cancel.send(()).unwrap();
        assert_eq!(task.await.unwrap(), Err(host::Error::Cancelled));
        assert!(matches!(peer.recv().await, w::envelope::Frame::Cancel(_)));
        peer.answer(r.request_id, "input", json!({"action":"handled"}), false)
            .await;
        let cap = c.grant().profiles()[0].reference().clone();
        let call = c.clone();
        let task = tokio::spawn(async move {
            call.request(
                cap,
                "input".into(),
                host::bound(&Registry::coding().unwrap(), "coding.input:input", &input()).unwrap(),
                validator(),
                Box::pin(std::future::pending()),
                |_| Ok(()),
            )
            .await
        });
        let w::envelope::Frame::Request(r2) = peer.recv().await else {
            panic!()
        };
        peer.answer(r2.request_id, "input", json!({"action":"continue"}), false)
            .await;
        assert!(task.await.unwrap().is_ok());
        c.close();
        assert!(!c.current());
        let cap = c.grant().profiles()[0].reference().clone();
        assert_eq!(
            c.request(
                cap,
                "input".into(),
                host::bound(&Registry::coding().unwrap(), "coding.input:input", &input()).unwrap(),
                validator(),
                Box::pin(std::future::pending()),
                |_| Ok(())
            )
            .await,
            Err(host::Error::Closed)
        );
    })
}
#[test]
fn native_rejection_is_not_success_and_local_fault_is_distinct() {
    runtime().block_on(async {
        for (native_fault, expected) in [
            (NativeError::Rejected, host::Error::NativeRejected),
            (NativeError::LocalFault, host::Error::HostValidation),
        ] {
            let (c, mut peer) = ready(&["input"], "s").await;
            let cap = c.grant().profiles()[0].reference().clone();
            let task = tokio::spawn(async move {
                c.request(
                    cap,
                    "input".into(),
                    host::bound(&Registry::coding().unwrap(), "coding.input:input", &input())
                        .unwrap(),
                    Arc::new(move |p| {
                        if p.stage == Stage::Terminal {
                            Err(native_fault)
                        } else {
                            Ok(())
                        }
                    }),
                    Box::pin(std::future::pending()),
                    |_| Ok(()),
                )
                .await
            });
            let w::envelope::Frame::Request(r) = peer.recv().await else {
                panic!()
            };
            peer.answer(r.request_id, "input", json!({"action":"continue"}), false)
                .await;
            assert_eq!(task.await.unwrap(), Err(expected));
        }
    })
}
#[test]
fn progress_order_and_typed_schema_failure_close_generation() {
    runtime().block_on(async {
        let (c, mut peer) = ready(&["remote"], "s").await;
        let cap = c.grant().profiles()[0].reference().clone();
        let call = c.clone();
        let n = Arc::new(AtomicUsize::new(0));
        let capture = n.clone();
        let task = tokio::spawn(async move {
            call.request(
                cap,
                "tool.call".into(),
                host::bound(
                    &Registry::coding().unwrap(),
                    "coding.input:tool.call",
                    &json!({"toolCallId":"t","arguments":{}}),
                )
                .unwrap(),
                validator(),
                Box::pin(std::future::pending()),
                move |_| {
                    capture.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                },
            )
            .await
        });
        let w::envelope::Frame::Request(r) = peer.recv().await else {
            panic!()
        };
        peer.progress(r.request_id, 1, json!({"result":{"content":[]}}))
            .await;
        peer.answer(
            r.request_id,
            "tool.call",
            json!({"result":{"content":[]}}),
            true,
        )
        .await;
        assert!(task.await.unwrap().is_ok());
        assert_eq!(n.load(Ordering::SeqCst), 1);
        let cap = c.grant().profiles()[0].reference().clone();
        let call = c.clone();
        let task = tokio::spawn(async move {
            call.request(
                cap,
                "tool.call".into(),
                host::bound(
                    &Registry::coding().unwrap(),
                    "coding.input:tool.call",
                    &json!({"toolCallId":"t2","arguments":{}}),
                )
                .unwrap(),
                validator(),
                Box::pin(std::future::pending()),
                |_| Ok(()),
            )
            .await
        });
        let w::envelope::Frame::Request(r) = peer.recv().await else {
            panic!()
        };
        peer.progress(r.request_id, 2, json!({"result":{"content":[]}}))
            .await;
        assert_eq!(task.await.unwrap(), Err(host::Error::Protocol));
        assert!(!c.current());
    })
}
#[test]
fn current_registry_refuses_old_harness_projection_and_unknown_fields() {
    let r = Registry::coding().unwrap();
    assert!(r.digest("hook.input:before_run").is_none());
    let mut b = host::bound(&r, "coding.input:input", &input()).unwrap();
    assert!(
        r.validate_v1_1("coding.input:input", &b, Default::default(), 10000)
            .is_ok()
    );
    b = host::bound(
        &r,
        "coding.input:input",
        &json!({"text":"x","images":[],"source":"interactive","lane":"obsolete"}),
    )
    .unwrap();
    assert!(
        r.validate_v1_1("coding.input:input", &b, Default::default(), 10000)
            .is_err()
    );
}

#[test]
fn wrong_capability_method_is_denied_before_native_work() {
    runtime().block_on(async {
        let (c, _peer) = ready(&["input"], "s").await;
        let cap = c.grant().profiles()[0].reference().clone();
        let n = Arc::new(AtomicUsize::new(0));
        let capture = n.clone();
        let result = c
            .request(
                cap,
                "tool.call".into(),
                host::bound(
                    &Registry::coding().unwrap(),
                    "coding.input:tool.call",
                    &json!({"toolCallId":"t","arguments":{}}),
                )
                .unwrap(),
                Arc::new(move |_| {
                    capture.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
                Box::pin(std::future::pending()),
                |_| Ok(()),
            )
            .await;
        assert_eq!(result, Err(host::Error::Protocol));
        assert_eq!(n.load(Ordering::SeqCst), 0);
        assert!(c.current());
        c.close();
    })
}

#[test]
fn finite_progress_credit_is_checked_before_native_publication() {
    runtime().block_on(async {
        let (c, mut peer) = ready(&["remote"], "s").await;
        let cap = c.grant().profiles()[0].reference().clone();
        let call = c.clone();
        let n = Arc::new(AtomicUsize::new(0));
        let capture = n.clone();
        let task = tokio::spawn(async move {
            call.request(
                cap,
                "tool.call".into(),
                host::bound(
                    &Registry::coding().unwrap(),
                    "coding.input:tool.call",
                    &json!({"toolCallId":"t","arguments":{}}),
                )
                .unwrap(),
                Arc::new(move |_| {
                    capture.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
                Box::pin(std::future::pending()),
                |_| panic!("over-credit progress published"),
            )
            .await
        });
        let w::envelope::Frame::Request(r) = peer.recv().await else {
            panic!()
        };
        let credit = r.initial_stream_credit_bytes.unwrap();
        assert!(credit < 262000);
        peer.progress(
            r.request_id,
            1,
            json!({"result":{"content":[{"type":"text","text":"x".repeat(credit as usize+1)}]}}),
        )
        .await;
        assert_eq!(task.await.unwrap(), Err(host::Error::Protocol));
        assert_eq!(n.load(Ordering::SeqCst), 1);
        assert!(!c.current());
    })
}
#[test]
fn closed_current_inventory_covers_selected_hooks_without_harness_state() {
    let r = Registry::coding().unwrap();
    assert_eq!(r.roots().count(), 35);
    assert_eq!(r.method("coding.v1", "before_run"), None);
    let b = host::bound(
        &r,
        "coding.output:turn_end",
        &json!({"value":{"entries":[],"continueRun":false,"operation":"obsolete"}}),
    )
    .unwrap();
    assert!(
        r.validate_v1_1("coding.output:turn_end", &b, Default::default(), 100000)
            .is_err()
    );
}

#[test]
fn retained_progress_budget_exhaustion_keeps_terminal_cancel_and_late_ownership() {
    runtime().block_on(async {
        use pi_extension_protocol::host::{HostConnection, ProgressCredit};
        use prost::Message;
        use std::{num::NonZeroUsize, time::Duration};
        for cancel_case in [false, true] {
            let value = json!({"result":{"content":[]}});
            let bound = host::bound(
                &Registry::coding().unwrap(),
                "coding.progress:tool.call",
                &value,
            )
            .unwrap();
            let credit = pi_extension_protocol::wire::framing::encode(
                &w::Envelope {
                    frame: Some(w::envelope::Frame::StreamItem(w::StreamItem {
                        request_id: 1,
                        sequence: 1,
                        payload: bound.encode_to_vec(),
                    })),
                },
                NonZeroUsize::new(262144).unwrap(),
            )
            .unwrap()
            .body_len as u64;
            let mut limits = limits();
            limits.in_flight_requests = 1;
            limits.progress_queue_bytes = credit;
            limits.max_stream_credit_bytes = credit;
            let (channel, mut peer) = pair();
            let handshake = tokio::spawn(async move {
                peer.handshake(&["remote"], "s", false).await;
                peer
            });
            let c = HostConnection::connect(
                authority_with_limits(&["remote"], "s", limits),
                channel,
                host::ConnectionConfig {
                    io_timeout: Duration::from_secs(1),
                    request_timeout: Duration::from_secs(1),
                    lifetime: Duration::from_secs(30),
                },
            )
            .await
            .unwrap();
            let mut peer = handshake.await.unwrap();
            let cap = c.grant().profiles()[0].reference().clone();
            let call = c.clone();
            let count = Arc::new(AtomicUsize::new(0));
            let capture = count.clone();
            let (cancel, signal) = tokio::sync::oneshot::channel();
            let (seen, observed) = tokio::sync::oneshot::channel();
            let mut seen = Some(seen);
            let task = tokio::spawn(async move {
                call.request_with_progress_credit(
                    cap,
                    "tool.call".into(),
                    host::bound(
                        &Registry::coding().unwrap(),
                        "coding.input:tool.call",
                        &json!({"toolCallId":"first","arguments":{}}),
                    )
                    .unwrap(),
                    validator(),
                    Box::pin(async {
                        let _ = signal.await;
                    }),
                    ProgressCredit::Retain,
                    move |_| {
                        capture.fetch_add(1, Ordering::SeqCst);
                        if let Some(s) = seen.take() {
                            let _ = s.send(());
                        }
                        Ok(())
                    },
                )
                .await
            });
            let w::envelope::Frame::Request(first) = peer.recv().await else {
                panic!()
            };
            assert_eq!(first.initial_stream_credit_bytes, Some(credit));
            peer.progress(first.request_id, 1, value.clone()).await;
            observed.await.unwrap();
            if cancel_case {
                cancel.send(()).unwrap();
                assert_eq!(task.await.unwrap(), Err(host::Error::Cancelled));
                assert!(matches!(peer.recv().await, w::envelope::Frame::Cancel(_)));
                peer.progress(first.request_id, 2, value.clone()).await;
                peer.answer(first.request_id, "tool.call", value.clone(), true)
                    .await;
            } else {
                peer.answer(first.request_id, "tool.call", value.clone(), true)
                    .await;
                assert!(task.await.unwrap().is_ok());
                drop(cancel);
            }
            let cap = c.grant().profiles()[0].reference().clone();
            let call = c.clone();
            let task = tokio::spawn(async move {
                call.request_with_progress_credit(
                    cap,
                    "tool.call".into(),
                    host::bound(
                        &Registry::coding().unwrap(),
                        "coding.input:tool.call",
                        &json!({"toolCallId":"second","arguments":{}}),
                    )
                    .unwrap(),
                    validator(),
                    Box::pin(std::future::pending()),
                    ProgressCredit::Retain,
                    |_| panic!("late progress wrote to next invocation"),
                )
                .await
            });
            let w::envelope::Frame::Request(second) = peer.recv().await else {
                panic!("retained progress must not emit credit")
            };
            assert!(second.request_id > first.request_id);
            peer.answer(second.request_id, "tool.call", value, true)
                .await;
            assert!(task.await.unwrap().is_ok());
            assert_eq!(count.load(Ordering::SeqCst), 1);
            c.close();
        }
    })
}
