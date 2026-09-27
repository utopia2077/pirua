//! Real process codecs/static specimens and framed six-profile component ownership.
//! Logical credit tests do not certify production resources or durable settlement.
mod profile_peer_support;
use pi_extension_protocol::profiles::Direction;
use pi_extension_protocol::{
    endpoint::{ActiveFrame, SelectedEnvelope},
    profile_live::{
        Accepted, Binding, Error, EventQueue, Owner, Phase, RetireReason, Telemetry, TerminalType,
    },
    profiles::{Stage, wire_v1_1 as dto},
    wire::v1_1,
};
use profile_peer_support::transport::{Peer, frame};
use profile_peer_support::*;
use prost::Message;
use serde_json::json;

fn pairs() -> [(u32, u32); 4] {
    [(0, 0), (0, 1), (1, 0), (1, 1)]
}
fn incoming(f: &ActiveFrame) -> v1_1::Envelope {
    match &f.envelope {
        SelectedEnvelope::V1_0(e) => v1_1::Envelope::decode(e.encode_to_vec().as_slice()).unwrap(),
        SelectedEnvelope::V1_1(e) => e.clone(),
    }
}
fn request(
    peer: &mut Peer,
    o: &mut Owner,
    method: &str,
    minor: u32,
    now: u64,
    credit: Option<u64>,
    n: &mut Native,
) -> Binding {
    let mut s = start(method, minor, now, None);
    s.initial_credit = credit;
    let (b, r) = o.start(s, n).unwrap();
    descriptor_ref_check(r.capability_ref.as_ref().unwrap());
    peer.send(frame(v1_1::envelope::Frame::Request(r)));
    o.dispatched(&b).unwrap();
    b
}
#[allow(clippy::too_many_arguments)]
fn terminal(
    peer: &mut Peer,
    e: &mut pi_extension_protocol::endpoint::Endpoint,
    o: &mut Owner,
    b: &Binding,
    minor: u32,
    method: &str,
    now: &mut u64,
    n: &mut Native,
) -> Vec<u8> {
    loop {
        *now += 1;
        let raw = peer.recv();
        let f = active(e, &raw, *now);
        let len = f.body_len;
        match o.receive(7, f, *now, n).unwrap() {
            Accepted::Progress(p) => {
                assert_eq!(p, expected(method, Stage::Progress, minor));
                o.release_progress(len as u64).unwrap();
            }
            Accepted::Terminal => {
                let r = o.receipt(b).unwrap();
                assert!(r.binding().same_identity(b));
                assert_eq!(r.payload(), Some(&expected(method, Stage::Terminal, minor)));
                return raw;
            }
            _ => panic!("unexpected normal acceptance"),
        }
    }
}

#[test]
fn all_174_specimens_and_3192_controls_execute_in_real_node_python_against_rust() {
    let c = corpus();
    assert_eq!(c["probes"].as_array().unwrap().len(), 174);
    let r = registry();
    let rows=c["probes"].as_array().unwrap().iter().map(|p|json!({"id":p["id"],"results":p["checks"].as_array().unwrap().iter().map(|x|policy(&r,x)).collect::<Vec<_>>()})).collect::<Vec<_>>();
    let controls = c["controls"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| json!({"id":p["id"],"result":shape(&r,p)}))
        .collect::<Vec<_>>();
    assert_eq!(controls.len(), 3192);
    for language in ["node", "python"] {
        let result = transport::probe(language);
        for (a, b) in result["rows"].as_array().unwrap().iter().zip(&rows) {
            assert_eq!(a, b, "{language} original specimen {}", b["id"]);
        }
        assert_eq!(result["rows"].as_array().unwrap().len(), rows.len());
        for (a, b) in result["controls"].as_array().unwrap().iter().zip(&controls) {
            assert_eq!(a, b, "{language} finite control {}", b["id"]);
        }
        assert_eq!(result["controls"].as_array().unwrap().len(), controls.len());
        assert_eq!(
            result["codecs"],
            digest_rows(),
            "{language} actual canonical descriptor/field codec"
        );
    }
    // Preserve the original advisory bytes and status/execution annotations exactly.
    assert_eq!(
        sha(pi_extension_protocol::profiles::STATIC_REGISTRY),
        c["authority"]["registry_sha256"]
    );
    assert_eq!(
        sha(include_bytes!(
            "../../../conformance/extension-protocol/six-profiles-v1.json"
        )),
        c["authority"]["vectors_sha256"]
    );
}

#[test]
fn all_25_methods_and_independent_minors_use_actual_endpoint_owner_and_peers() {
    for language in ["node", "python"] {
        for (core, minor) in pairs() {
            let mut peer = Peer::spawn(language, core, minor, "all-methods");
            let (mut e, mut o) = handshake(&mut peer, core, minor, "all-methods");
            let mut n = Native::default();
            let mut now = 4;
            let mut count = 0;
            for method in corpus()["methods"].as_object().unwrap().keys().filter(|m| {
                registry().method(&profile_name(m), m).unwrap().0 == Direction::CoreToExtension
            }) {
                let b = request(
                    &mut peer,
                    &mut o,
                    method,
                    minor,
                    now,
                    if matches!(method.as_str(), "provider.invoke" | "event.subscribe") {
                        Some(1_048_576)
                    } else {
                        None
                    },
                    &mut n,
                );
                let retained = terminal(
                    &mut peer, &mut e, &mut o, &b, minor, method, &mut now, &mut n,
                );
                assert!(matches!(o.phase(&b), Some(Phase::TerminalReceived)));
                assert_eq!(
                    o.receipt(&b).unwrap().terminal_type(),
                    if matches!(method.as_str(), "provider.invoke" | "event.subscribe") {
                        TerminalType::StreamEnd
                    } else {
                        TerminalType::Response
                    }
                );
                assert!(!retained.is_empty());
                o.retire(&b, RetireReason::Consumed).unwrap();
                count += 1;
                now += 1;
            }
            assert_eq!(count, 16);
            assert_eq!(o.occupancy(), 0);
            assert_eq!(n.seen.len(), 33);
            peer.finish(&format!("{language}-{core}-{minor}-all-methods"), false);
            // These six actual peer-originated requests share a real live provider parent.
            let mut peer = Peer::spawn(language, core, minor, "host-actions");
            let (mut e, mut o) = handshake(&mut peer, core, minor, "host-actions");
            let mut n = Native::default();
            let parent = request(&mut peer, &mut o, "provider.invoke", minor, 4, None, &mut n);
            let mut now = 5;
            for method in [
                "snapshot.get",
                "resource.get",
                "queue.steer",
                "queue.follow_up",
                "queue.next_run",
                "operation.abort",
            ] {
                let raw = peer.recv();
                let f = active(&mut e, &raw, now);
                let Some(v1_1::envelope::Frame::Request(r)) = incoming(&f).frame else {
                    panic!("peer host Request")
                };
                assert_eq!(r.method, method);
                assert_eq!(r.parent_request_id, Some(parent.id()));
                let child = o
                    .accept_request(r, start(method, minor, now, Some(parent.clone())), &mut n)
                    .unwrap();
                o.dispatched(&child).unwrap();
                assert_eq!(
                    n.seen.last(),
                    Some(&expected(method, Stage::Request, minor))
                );
                let response = frame(v1_1::envelope::Frame::Response(v1_1::Response {
                    request_id: child.id(),
                    terminal: Some(v1_1::response::Terminal::Result(payload(
                        method, "terminal", minor,
                    ))),
                }));
                let f = active(&mut e, &response, now);
                assert!(matches!(
                    o.receive(7, f, now, &mut n),
                    Ok(Accepted::Terminal)
                ));
                assert_eq!(
                    o.receipt(&child).unwrap().payload(),
                    Some(&expected(method, Stage::Terminal, minor))
                );
                peer.send(response);
                o.retire(&child, RetireReason::Consumed).unwrap();
                count += 1;
                now += 1;
            }
            terminal(
                &mut peer,
                &mut e,
                &mut o,
                &parent,
                minor,
                "provider.invoke",
                &mut now,
                &mut n,
            );
            o.retire(&parent, RetireReason::Consumed).unwrap();
            assert_eq!(count, 22);
            assert_eq!(o.occupancy(), 0);
            peer.finish(&format!("{language}-{core}-{minor}-host-actions"), false);
            let mut peer = Peer::spawn(language, core, minor, "passive-origin");
            let (mut e, mut o) = handshake(&mut peer, core, minor, "passive-origin");
            let mut n = Native::default();
            count += passive_exchange(&mut peer, &mut e, &mut o, minor, false, &mut n);
            assert_eq!(count, 25);
            assert_eq!(o.occupancy(), 0);
            peer.finish(&format!("{language}-{core}-{minor}-passive-origin"), false);
        }
    }
}

#[test]
fn framed_provider_credit_cancellation_deadline_loss_and_first_receipt_histories() {
    for language in ["node", "python"] {
        for (core, minor) in pairs() {
            for scenario in [
                "credit",
                "cancel",
                "cancel-progress",
                "end-cancel",
                "duplicate",
                "sequence",
                "pending",
                "wrong-projection",
                "unknown-enum",
                "error",
                "silence",
                "overcredit",
                "loss",
                "drain",
                "relative",
            ] {
                let mut peer = Peer::spawn(language, core, minor, scenario);
                let (mut e, mut o) = handshake(&mut peer, core, minor, scenario);
                let mut n = Native::default();
                // Recompute from the actual selected Envelope, never inner payload/prefix length.
                let exact = v1_1::Envelope {
                    frame: Some(v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
                        request_id: 1,
                        sequence: 0,
                        payload: payload("provider.invoke", "progress", minor),
                    })),
                }
                .encoded_len() as u64;
                let b = if scenario == "relative" {
                    let mut s = start("provider.invoke", minor, 4, None);
                    s.requested_deadline_ms = Some(2);
                    let (b, r) = o.start(s, &mut n).unwrap();
                    peer.send(frame(v1_1::envelope::Frame::Request(r)));
                    o.dispatched(&b).unwrap();
                    b
                } else {
                    request(
                        &mut peer,
                        &mut o,
                        "provider.invoke",
                        minor,
                        4,
                        if scenario == "cancel-progress" {
                            Some(exact)
                        } else if scenario == "sequence" {
                            Some(1_048_576)
                        } else {
                            None
                        },
                        &mut n,
                    )
                };
                if scenario == "drain" {
                    o.drain(5).unwrap();
                    assert_eq!(
                        o.start(start("provider.invoke", minor, 5, None), &mut n)
                            .err(),
                        Some(Error::Generation)
                    );
                }
                if scenario == "loss" {
                    o.lose_generation(5).unwrap();
                    assert_eq!(o.cancel(&b, 1, 5), Err(Error::Stale));
                    peer.send(frame(v1_1::envelope::Frame::Cancel(v1_1::Cancel {
                        request_id: b.id(),
                        reason: 1,
                    })));
                }
                if scenario == "silence" {
                    assert!(peer.try_recv(25).is_none());
                    o.expire(5004).unwrap();
                    assert_eq!(o.phase(&b), Some(Phase::Abandoned));
                    assert_eq!(o.cancel(&b, 1, 5005), Err(Error::Stale));
                    assert!(o.receipt(&b).is_none());
                    peer.finish(&format!("{language}-{core}-{minor}-{scenario}"), true);
                    continue;
                }
                if scenario == "credit" {
                    assert!(peer.try_recv(25).is_none());
                    let credit = frame(v1_1::envelope::Frame::Credit(v1_1::Credit {
                        request_id: b.id(),
                        additional_bytes: exact,
                    }));
                    let f = active(&mut e, &credit, 5);
                    assert!(matches!(o.receive(7, f, 5, &mut n), Ok(Accepted::Credited)));
                    peer.send(credit);
                }
                if matches!(scenario, "cancel" | "cancel-progress") {
                    o.cancel(&b, 1, 5).unwrap();
                    peer.send(frame(v1_1::envelope::Frame::Cancel(v1_1::Cancel {
                        request_id: b.id(),
                        reason: 1,
                    })));
                }
                let mut now = 6;
                let mut got_progress = 0;
                let mut failure = None;
                let mut saved_raw = None;
                loop {
                    let raw = peer.recv();
                    let f = active(&mut e, &raw, now);
                    let len = f.body_len;
                    let is_item = matches!(
                        incoming(&f).frame,
                        Some(v1_1::envelope::Frame::StreamItem(_))
                    );
                    match o.receive(7, f, now, &mut n) {
                        Ok(Accepted::Progress(p)) => {
                            assert_eq!(p, expected("provider.invoke", Stage::Progress, minor));
                            assert_eq!(len as u64, exact);
                            got_progress += 1;
                            o.release_progress(len as u64).unwrap();
                        }
                        Ok(Accepted::Suppressed) => {
                            assert_eq!(scenario, "cancel-progress");
                            assert!(is_item);
                            got_progress += 1;
                        }
                        Ok(Accepted::Terminal) => {
                            saved_raw = Some(raw);
                            break;
                        }
                        Err(error) => {
                            failure = Some(error);
                            break;
                        }
                        _ => panic!("scenario acceptance"),
                    };
                    now += 1;
                }
                match scenario {
                    "overcredit" => assert_eq!(failure, Some(Error::Credit)),
                    "loss" => assert_eq!(failure, Some(Error::Stale)),
                    "relative" => assert_eq!(failure, Some(Error::Deadline)),
                    "sequence" => assert_eq!(failure, Some(Error::Sequence)),
                    "pending" => assert!(matches!(failure, Some(Error::Validation(_)))),
                    "wrong-projection" => assert_eq!(
                        failure,
                        Some(Error::Validation(
                            pi_extension_protocol::profiles::Error::Projection
                        ))
                    ),
                    "unknown-enum" => assert_eq!(failure, Some(Error::Variant)),
                    _ => {
                        assert!(failure.is_none());
                        let r = o.receipt(&b).unwrap();
                        if scenario == "error" {
                            assert_eq!(r.error(), Some(v1_1::ErrorCode::HandlerError));
                            assert!(r.payload().is_none());
                        } else {
                            assert_eq!(
                                r.payload(),
                                Some(&expected("provider.invoke", Stage::Terminal, minor))
                            );
                        }
                        assert_eq!(
                            got_progress,
                            usize::from(matches!(scenario, "credit" | "cancel-progress"))
                        );
                        let received = r.received_ms();
                        let saved = r.payload().cloned();
                        if scenario == "end-cancel" {
                            assert_eq!(o.cancel(&b, 1, now + 1), Err(Error::Stale));
                        }
                        if scenario == "duplicate" {
                            let raw = peer.recv();
                            let f = active(&mut e, &raw, now + 1);
                            assert_eq!(o.receive(7, f, now + 1, &mut n).err(), Some(Error::Stale));
                        }
                        o.drain(now + 2).unwrap();
                        o.lose_generation(now + 3).unwrap();
                        o.expire(now + 10_000).unwrap();
                        assert_eq!(o.receipt(&b).unwrap().received_ms(), received);
                        assert_eq!(o.receipt(&b).unwrap().payload(), saved.as_ref());
                        assert_eq!(o.cancel(&b, 1, now + 10_001), Err(Error::Stale));
                        assert!(saved_raw.is_some());
                        o.retire(&b, RetireReason::Consumed).unwrap();
                    }
                }
                if failure.is_some() {
                    assert!(o.receipt(&b).is_none());
                    assert_eq!(o.phase(&b), Some(Phase::Abandoned));
                }
                peer.finish(
                    &format!("{language}-{core}-{minor}-{scenario}"),
                    failure.is_some(),
                );
            }
        }
    }
}

#[test]
fn all_hooks_none_unit_typed_errors_and_real_host_denials() {
    for language in ["node", "python"] {
        for (core, minor) in pairs() {
            for scenario in ["none", "error"] {
                let mut peer = Peer::spawn(language, core, minor, scenario);
                let (mut e, mut o) = handshake(&mut peer, core, minor, scenario);
                let mut n = Native::default();
                let mut now = 4;
                for method in corpus()["methods"]
                    .as_object()
                    .unwrap()
                    .keys()
                    .filter(|m| profile_name(m) == "hook.v1")
                {
                    let b = request(&mut peer, &mut o, method, minor, now, None, &mut n);
                    now += 1;
                    let raw = peer.recv();
                    let f = active(&mut e, &raw, now);
                    assert!(matches!(
                        o.receive(7, f, now, &mut n),
                        Ok(Accepted::Terminal)
                    ));
                    let r = o.receipt(&b).unwrap();
                    if scenario == "error" {
                        assert_eq!(r.error(), Some(v1_1::ErrorCode::HandlerError));
                    } else {
                        assert!(r.payload().unwrap().projections.is_empty());
                        assert_eq!(r.payload().unwrap().method, *method);
                    }
                    o.retire(&b, RetireReason::Consumed).unwrap();
                    now += 1;
                }
                peer.finish(
                    &format!("{language}-{core}-{minor}-hooks-{scenario}"),
                    false,
                );
            }
            for (scenario, want) in [
                ("host-secret", Error::SecretBrokerUnavailable),
                ("host-ungranted", Error::Forbidden),
                ("host-unknown", Error::UnsupportedMethod),
                ("host-parent", Error::Parent),
                ("host-reference", Error::Reference),
            ] {
                let mut peer = Peer::spawn(language, core, minor, scenario);
                let (mut e, mut o) = handshake(&mut peer, core, minor, scenario);
                let mut n = Native::default();
                let parent = request(&mut peer, &mut o, "provider.invoke", minor, 4, None, &mut n);
                let before = n.seen.len();
                let raw = peer.recv();
                let f = active(&mut e, &raw, 5);
                let Some(v1_1::envelope::Frame::Request(r)) = incoming(&f).frame else {
                    panic!("host Request")
                };
                let got = o.accept_request(r, start("queue.steer", minor, 5, Some(parent)), &mut n);
                assert_eq!(got.err(), Some(want));
                assert_eq!(n.seen.len(), before);
                assert_eq!(o.occupancy(), 1);
                peer.finish(&format!("{language}-{core}-{minor}-{scenario}"), true);
            }
        }
    }
}

#[test]
fn actual_passive_gap_resync_telemetry_and_finite_owner_queue_controls() {
    for language in ["node", "python"] {
        for (core, minor) in pairs() {
            let mut peer = Peer::spawn(language, core, minor, "event-gap");
            let (mut e, mut o) = handshake(&mut peer, core, minor, "event-gap");
            let mut n = Native::default();
            assert_eq!(
                passive_exchange(&mut peer, &mut e, &mut o, minor, true, &mut n),
                2
            );
            peer.finish(&format!("{language}-{core}-{minor}-event-gap"), false);
            // Runtime-owned telemetry projects actual attributes before the peer receives a batch.
            let mut metrics = Telemetry::new(4096, 2).unwrap();
            let attrs = json!({"pi.operation.id":"SENTINEL","pi.operation.outcome":"declined","pi.checkpoint.kind":"failure_drain","pi.hook.name":"before_run","pi.hook.outcome":"SENTINEL"});
            let record=metrics.project(&registry(),json!({"kind":"span_start","spanId":1,"name":"pi.harness.run","attributes":attrs})).unwrap().unwrap();
            assert!(!format!("{:?}", record.wire()).contains("SENTINEL"));
            let seq = metrics.enqueue(vec![record]).unwrap().unwrap();
            let batch = metrics.take_batch().unwrap();
            assert_eq!(batch.sequence, seq);
            metrics
                .ack(
                    seq,
                    dto::TelemetryEmitBatchResult {
                        accepted: 0,
                        dropped: 1,
                    },
                )
                .unwrap();
            metrics.flush(seq, seq).unwrap();
            assert!(metrics.flush(seq, seq + 1).is_err());
            assert!(metrics.disabled());
            let mut tiny = Telemetry::new(1, 1).unwrap();
            let record = tiny
                .project(
                    &registry(),
                    json!({"kind":"span_start","spanId":1,"name":"pi.harness.run","attributes":{}}),
                )
                .unwrap()
                .unwrap();
            assert!(tiny.enqueue(vec![record]).unwrap().is_none());
            assert_eq!(tiny.dropped(), 1);
        }
    }
}

fn passive_exchange(
    peer: &mut Peer,
    e: &mut pi_extension_protocol::endpoint::Endpoint,
    o: &mut Owner,
    minor: u32,
    gap_case: bool,
    n: &mut Native,
) -> usize {
    let mut q = EventQueue::new("s1".into(), 7, 2, 64, 0, 0, 100).unwrap();
    if gap_case {
        for seq in 0u64..4 {
            q.publish(7, seq, seq.saturating_sub(1), vec![1], 5 + seq)
                .unwrap();
        }
        assert!(q.paused());
        assert!(q.take_event().is_none());
    }
    let methods = if gap_case {
        vec!["event.subscribe", "event.resync"]
    } else {
        vec!["resource.changed", "event.subscribe", "event.resync"]
    };
    let mut now = 4;
    for method in &methods {
        let raw = peer.recv();
        let f = active(e, &raw, now);
        let Some(v1_1::envelope::Frame::Request(r)) = incoming(&f).frame else {
            panic!("actual passive-origin request")
        };
        assert_eq!(r.method, *method);
        let b = o
            .accept_request(r, start(method, minor, now, None), n)
            .unwrap();
        o.dispatched(&b).unwrap();
        assert_eq!(
            n.seen.last(),
            Some(&expected(method, Stage::Request, minor))
        );
        now += 1;
        if *method == "event.subscribe" {
            let mut items = vec![payload(method, "progress", minor)];
            if gap_case {
                let c = corpus();
                let ev = &c["methods"][method]["event_item"]["event"];
                let root = ev["event"]["schema_id"].as_str().unwrap();
                let event = dto::EventSinkItem {
                    item: Some(dto::event_sink_item::Item::Event(dto::EventNotification {
                        subscription_id: ev["subscription_id"].as_str().unwrap().into(),
                        sequence: ev["sequence"].as_u64().unwrap(),
                        event: Some(profile_peer_support::shared_bound(
                            root,
                            &ev["event"]["value"],
                        )),
                    })),
                };
                items.push(event.encode_to_vec());
                items.push(
                    dto::EventSinkItem {
                        item: Some(dto::event_sink_item::Item::Gap(q.gap().unwrap().clone())),
                    }
                    .encode_to_vec(),
                );
            }
            for (seq, payload) in items.into_iter().enumerate() {
                let raw = frame(v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
                    request_id: b.id(),
                    sequence: seq as u64,
                    payload,
                }));
                let f = active(e, &raw, now);
                let len = f.body_len;
                let Ok(Accepted::Progress(p)) = o.receive(7, f, now, n) else {
                    panic!("actual passive item native/static admission")
                };
                if seq == 0 {
                    assert_eq!(p, expected(method, Stage::Progress, minor));
                }
                if seq == 2 {
                    assert_eq!(p.fields["first_dropped"], 0);
                    assert_eq!(p.fields["last_dropped"], 2);
                }
                o.release_progress(len as u64).unwrap();
                peer.send(raw);
                now += 1;
            }
        }
        let end = if *method == "event.subscribe" {
            v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                request_id: b.id(),
                terminal: Some(v1_1::stream_end::Terminal::Result(payload(
                    method, "terminal", minor,
                ))),
            })
        } else {
            v1_1::envelope::Frame::Response(v1_1::Response {
                request_id: b.id(),
                terminal: Some(v1_1::response::Terminal::Result(payload(
                    method, "terminal", minor,
                ))),
            })
        };
        let raw = frame(end);
        assert!(matches!(
            o.receive(7, active(e, &raw, now), now, n),
            Ok(Accepted::Terminal)
        ));
        let expected = expected(method, Stage::Terminal, minor);
        assert_eq!(o.receipt(&b).unwrap().payload(), Some(&expected));
        if *method == "event.resync" && gap_case {
            let frozen = q.gap().unwrap().clone();
            q.resync(
                7,
                expected.fields["snapshot_revision"].as_u64().unwrap(),
                expected.fields["next_event_sequence"].as_u64().unwrap(),
                now,
            )
            .unwrap();
            assert!(!q.paused());
            assert_eq!((frozen.first_dropped, frozen.last_dropped), (0, 2));
        }
        peer.send(raw);
        o.retire(&b, RetireReason::Consumed).unwrap();
        now += 1;
    }
    methods.len()
}

#[test]
fn real_peers_reject_open_sentinels_and_isolate_resource_telemetry_failures() {
    for language in ["node", "python"] {
        for (core, minor) in pairs() {
            for (scenario, method) in [
                ("sentinel", "before_tool"),
                ("bad-resource", "resource.read"),
                ("bad-ack", "telemetry.emit_batch"),
                ("bad-flush", "telemetry.flush"),
            ] {
                let mut peer = Peer::spawn(language, core, minor, scenario);
                let (mut e, mut o) = handshake(&mut peer, core, minor, scenario);
                let mut n = Native::default();
                let b = request(&mut peer, &mut o, method, minor, 4, None, &mut n);
                let raw = peer.recv();
                let f = active(&mut e, &raw, 5);
                let err = o.receive(7, f, 5, &mut n).err().unwrap();
                assert!(matches!(err, Error::Validation(_)));
                assert!(o.receipt(&b).is_none());
                if scenario == "sentinel" {
                    assert_eq!(
                        err,
                        Error::Validation(pi_extension_protocol::profiles::Error::Exposure)
                    );
                    assert_eq!(o.phase(&b), Some(Phase::Abandoned));
                    assert!(o.draining());
                    assert_eq!(n.seen.len(), 1);
                } else {
                    assert!(o.capability_disabled(b.capability()));
                    assert!(!o.draining());
                    let parent =
                        request(&mut peer, &mut o, "provider.invoke", minor, 6, None, &mut n);
                    let mut now = 6;
                    terminal(
                        &mut peer,
                        &mut e,
                        &mut o,
                        &parent,
                        minor,
                        "provider.invoke",
                        &mut now,
                        &mut n,
                    );
                    assert!(o.receipt(&parent).is_some());
                    o.retire(&parent, RetireReason::Consumed).unwrap();
                }
                peer.finish(
                    &format!("{language}-{core}-{minor}-{scenario}"),
                    scenario == "sentinel",
                );
            }
            let mut peer = Peer::spawn(language, core, minor, "telemetry-projection");
            let (mut e, mut o) = handshake(&mut peer, core, minor, "telemetry-projection");
            let mut n = Native::default();
            let mut metrics = Telemetry::new(4096, 2).unwrap();
            let record=metrics.project(&registry(),json!({"kind":"span_start","name":"pi.harness.run","attributes":{"pi.operation.id":"SENTINEL","pi.operation.outcome":"declined","pi.checkpoint.kind":"failure_drain","pi.hook.name":"before_run","pi.hook.outcome":"SENTINEL"}})).unwrap().unwrap();
            let seq = metrics.enqueue(vec![record]).unwrap().unwrap();
            let batch = metrics.take_batch().unwrap();
            let mut s = start("telemetry.emit_batch", minor, 4, None);
            s.payload = batch.encode_to_vec();
            let (b, r) = o.start(s, &mut n).unwrap();
            assert!(!format!("{:?}", r).contains("SENTINEL"));
            peer.send(frame(v1_1::envelope::Frame::Request(r)));
            o.dispatched(&b).unwrap();
            let raw = peer.recv();
            let f = active(&mut e, &raw, 5);
            let Some(v1_1::envelope::Frame::Response(response)) = incoming(&f).frame else {
                panic!("telemetry peer ack")
            };
            let Some(v1_1::response::Terminal::Result(result)) = response.terminal else {
                panic!("ack result")
            };
            metrics
                .ack(
                    seq,
                    dto::TelemetryEmitBatchResult::decode(result.as_slice()).unwrap(),
                )
                .unwrap();
            assert!(matches!(o.receive(7, f, 5, &mut n), Ok(Accepted::Terminal)));
            o.retire(&b, RetireReason::Consumed).unwrap();
            let mut s = start("telemetry.flush", minor, 6, None);
            s.payload = dto::TelemetryFlushRequest {
                through_sequence: seq,
            }
            .encode_to_vec();
            let (b, r) = o.start(s, &mut n).unwrap();
            peer.send(frame(v1_1::envelope::Frame::Request(r)));
            o.dispatched(&b).unwrap();
            let raw = peer.recv();
            let f = active(&mut e, &raw, 7);
            assert!(matches!(o.receive(7, f, 7, &mut n), Ok(Accepted::Terminal)));
            metrics
                .flush(
                    seq,
                    o.receipt(&b).unwrap().payload().unwrap().fields["flushed_through"]
                        .as_u64()
                        .unwrap(),
                )
                .unwrap();
            assert!(!metrics.disabled());
            o.retire(&b, RetireReason::Consumed).unwrap();
            peer.finish(
                &format!("{language}-{core}-{minor}-telemetry-projection"),
                false,
            );
        }
    }
}

#[test]
fn actual_frame_queues_reserve_terminal_control_and_cap_parent_deadlines() {
    for language in ["node", "python"] {
        for (core, minor) in pairs() {
            let mut peer = Peer::spawn(language, core, minor, "silence");
            let (e, _) = handshake(&mut peer, core, minor, "silence");
            let grant = e.active_grant().unwrap().clone();
            let mut n = Native::default();
            for control in [true, false] {
                let mut seats = reservations();
                if control {
                    seats.control_bytes -= 1;
                } else {
                    seats.terminal_bytes -= 1;
                }
                assert_eq!(
                    Owner::new(grant.clone(), seats, 3, 10_000).err(),
                    Some(Error::HostContract)
                );
            }
            let mut seats = reservations();
            seats.blocking_bytes = 1;
            let mut tiny = Owner::new(grant.clone(), seats, 3, 10_000).unwrap();
            assert_eq!(
                tiny.start(start("provider.invoke", minor, 4, None), &mut n)
                    .err(),
                Some(Error::Overloaded)
            );
            assert_eq!(n.seen.len(), 0);
            assert_eq!(tiny.occupancy(), 0);
            let mut o = Owner::new(grant.clone(), reservations(), 3, 10_000).unwrap();
            let mut bindings = Vec::new();
            for i in 0..32 {
                bindings.push(request(
                    &mut peer,
                    &mut o,
                    "provider.invoke",
                    minor,
                    4 + i,
                    None,
                    &mut n,
                ));
            }
            assert_eq!(o.occupancy(), 32);
            let before = n.seen.len();
            assert_eq!(
                o.start(start("provider.invoke", minor, 36, None), &mut n)
                    .err(),
                Some(Error::Overloaded)
            );
            assert_eq!(n.seen.len(), before);
            for b in &bindings {
                o.retire(b, RetireReason::NoLongerExpected).unwrap();
            }
            assert_eq!(o.occupancy(), 0);
            peer.finish(&format!("{language}-{core}-{minor}-queue-inflight"), true);
            // A real child's requested100ms is capped by the live parent's remaining2ms.
            let mut peer = Peer::spawn(language, core, minor, "host-actions");
            let (mut e, mut o) = handshake(&mut peer, core, minor, "host-actions");
            let mut n = Native::default();
            let mut s = start("provider.invoke", minor, 4, None);
            s.requested_deadline_ms = Some(3);
            let (parent, r) = o.start(s, &mut n).unwrap();
            peer.send(frame(v1_1::envelope::Frame::Request(r)));
            o.dispatched(&parent).unwrap();
            let raw = peer.recv();
            let f = active(&mut e, &raw, 5);
            let Some(v1_1::envelope::Frame::Request(r)) = incoming(&f).frame else {
                panic!("live child")
            };
            let b = o
                .accept_request(
                    r,
                    start("snapshot.get", minor, 5, Some(parent.clone())),
                    &mut n,
                )
                .unwrap();
            o.dispatched(&b).unwrap();
            o.expire(7).unwrap();
            assert_eq!(o.phase(&parent), Some(Phase::Abandoned));
            assert_eq!(o.phase(&b), Some(Phase::Abandoned));
            assert!(o.receipt(&b).is_none());
            let raw = frame(v1_1::envelope::Frame::Response(v1_1::Response {
                request_id: b.id(),
                terminal: Some(v1_1::response::Terminal::Result(payload(
                    "snapshot.get",
                    "terminal",
                    minor,
                ))),
            }));
            assert_eq!(
                o.receive(7, active(&mut e, &raw, 8), 8, &mut n).err(),
                Some(Error::Stale)
            );
            peer.finish(&format!("{language}-{core}-{minor}-parent-deadline"), true);
            // Progress overload leaves the reserved complete StreamEnd consumable.
            let mut peer = Peer::spawn(language, core, minor, "all-methods");
            let (mut e, _) = handshake(&mut peer, core, minor, "all-methods");
            let mut seats = reservations();
            seats.progress_bytes = 1;
            let mut o = Owner::new(e.active_grant().unwrap().clone(), seats, 3, 10_000).unwrap();
            let mut n = Native::default();
            let b = request(
                &mut peer,
                &mut o,
                "provider.invoke",
                minor,
                4,
                Some(1_048_576),
                &mut n,
            );
            let raw = peer.recv();
            assert_eq!(
                o.receive(7, active(&mut e, &raw, 5), 5, &mut n).err(),
                Some(Error::Overloaded)
            );
            assert_eq!(o.phase(&b), Some(Phase::Open));
            let raw = peer.recv();
            assert!(matches!(
                o.receive(7, active(&mut e, &raw, 6), 6, &mut n),
                Ok(Accepted::Terminal)
            ));
            assert_eq!(
                o.receipt(&b).unwrap().payload(),
                Some(&expected("provider.invoke", Stage::Terminal, minor))
            );
            peer.finish(&format!("{language}-{core}-{minor}-queue-progress"), false);
        }
    }
}

#[test]
fn actual_inline_ancestry_depth_cancellation_and_rate_ceilings() {
    for language in ["node", "python"] {
        for (core, minor) in pairs() {
            let mut peer = Peer::spawn(language, core, minor, "none");
            let (mut e, mut o) = handshake(&mut peer, core, minor, "none");
            let mut n = Native::default();
            let parent = request(&mut peer, &mut o, "provider.invoke", minor, 4, None, &mut n);
            let mut all = vec![parent.clone()];
            for _ in 0..7 {
                let mut s = start("before_tool", minor, 4, all.last().cloned());
                s.role = pi_extension_protocol::profile_live::Role::Inline;
                s.key = Some(key());
                let (b, r) = o.start(s, &mut n).unwrap();
                o.dispatched(&b).unwrap();
                peer.send(frame(v1_1::envelope::Frame::Request(r)));
                all.push(b);
            }
            let before = n.seen.len();
            let mut s = start("before_tool", minor, 4, all.last().cloned());
            s.role = pi_extension_protocol::profile_live::Role::Inline;
            s.key = Some(key());
            assert_eq!(o.start(s, &mut n).err(), Some(Error::Parent));
            assert_eq!(n.seen.len(), before);
            assert_eq!(all.len(), 8);
            // Same numeric parent ID from an independently owned binding is insufficient.
            let mut other =
                Owner::new(e.active_grant().unwrap().clone(), reservations(), 3, 10000).unwrap();
            let (foreign, _) = other
                .start(start("provider.invoke", minor, 4, None), &mut n)
                .unwrap();
            assert_eq!(foreign.id(), parent.id());
            let mut s = start("before_tool", minor, 4, Some(foreign));
            s.role = pi_extension_protocol::profile_live::Role::Inline;
            s.key = Some(key());
            assert_eq!(o.start(s, &mut n).err(), Some(Error::Parent));
            o.cancel(&parent, 1, 5).unwrap();
            peer.send(frame(v1_1::envelope::Frame::Cancel(v1_1::Cancel {
                request_id: parent.id(),
                reason: 1,
            })));
            for b in &all {
                assert_eq!(o.phase(b), Some(Phase::CancelRequested));
            }
            for _ in &all {
                let raw = peer.recv();
                assert!(matches!(
                    o.receive(7, active(&mut e, &raw, 6), 6, &mut n),
                    Ok(Accepted::Terminal)
                ));
            }
            for b in all {
                assert!(o.receipt(&b).is_some());
                o.retire(&b, RetireReason::Consumed).unwrap();
            }
            peer.finish(&format!("{language}-{core}-{minor}-ancestry-depth"), false);
            let mut peer = Peer::spawn(language, core, minor, "all-methods");
            let (mut e, mut o) = handshake(&mut peer, core, minor, "all-methods");
            let mut n = Native::default();
            for _ in 0..100 {
                let b = request(&mut peer, &mut o, "provider.invoke", minor, 5, None, &mut n);
                let raw = peer.recv();
                assert!(matches!(
                    o.receive(7, active(&mut e, &raw, 5), 5, &mut n),
                    Ok(Accepted::Terminal)
                ));
                o.retire(&b, RetireReason::Consumed).unwrap();
            }
            let before = n.seen.len();
            let next = o.next_id();
            assert_eq!(
                o.start(start("provider.invoke", minor, 5, None), &mut n)
                    .err(),
                Some(Error::Overloaded)
            );
            assert_eq!(o.next_id(), next);
            assert_eq!(n.seen.len(), before);
            assert_eq!(o.occupancy(), 0);
            peer.finish(&format!("{language}-{core}-{minor}-rate-ceiling"), false);
        }
    }
}

#[test]
fn completed_peer_projection_and_provider_usage_match_native_receipt() {
    for language in ["node", "python"] {
        for (core, minor) in pairs() {
            let mut peer = Peer::spawn(language, core, minor, "single-provider");
            let (mut e, mut o) = handshake(&mut peer, core, minor, "single-provider");
            let mut n = Native::default();
            let b = request(&mut peer, &mut o, "provider.invoke", minor, 4, None, &mut n);
            let raw = peer.recv();
            // The complete peer process must succeed before the native assertion.
            peer.finish(&format!("{language}-{core}-{minor}-single-provider"), false);
            let f = active(&mut e, &raw, 5);
            assert_eq!(
                o.receive(7, f, 5, &mut n).err(),
                None,
                "successful peer projection/usage must admit a native receipt"
            );
            assert_eq!(
                o.receipt(&b).unwrap().payload(),
                Some(&expected("provider.invoke", Stage::Terminal, minor))
            );
            o.retire(&b, RetireReason::Consumed).unwrap();
        }
    }
}

#[test]
fn selected_wire_map_keys_survive_closed_shapes_open_leaves_and_canonical_roundtrip() {
    use pi_extension_protocol::{jcs, profiles, wire::value};
    let r = registry();
    let c = corpus();
    let usage = c["controls"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["id"] == "Usage.specimen")
        .unwrap()["value"]
        .clone();
    let mut rows = Vec::new();
    let mut expected = Vec::new();
    for minor in 0..=1 {
        for key in ["unexpected", "__proto__", "constructor", "toString"] {
            let mut v = usage.clone();
            v.as_object_mut()
                .unwrap()
                .insert(key.into(), json!({"secret":"SENTINEL"}));
            let w = profiles::json_wire(&v).unwrap();
            let accepted = r
                .validate_type("Usage", &w, value::Limits::default(), 1_000_000)
                .is_ok();
            rows.push(key_fidelity_row(
                &format!("usage:{key}"),
                minor,
                "Usage",
                &v,
            ));
            expected.push(json!({"shape":accepted,"json":v,"canonical":std::str::from_utf8(canonical(&v).bytes()).unwrap()}));
        }
        // Legitimate open data retains all names recursively, including empty and non-ASCII keys.
        let v: serde_json::Value = serde_json::from_str(r#"{"__proto__":{"constructor":{"toString":"kept"}},"constructor":"own","toString":[{"__proto__":"nested"}],"":"empty","z/ ~":"ordinary","é":"unicode"}"#).unwrap();
        let w = profiles::json_wire(&v).unwrap();
        let native = r
            .validate_type("JsonObject", &w, value::Limits::default(), 1_000_000)
            .unwrap();
        assert_eq!(native, v);
        rows.push(key_fidelity_row("open-map", minor, "JsonObject", &v));
        expected.push(json!({"shape":true,"json":native,"canonical":std::str::from_utf8(canonical(&v).bytes()).unwrap()}));
        for key in ["unexpected", "__proto__", "constructor", "toString"] {
            let mut w = profiles::json_wire(&json!({key:"first"})).unwrap();
            let Some(v1_1::wire_value::Value::MapValue(m)) = &mut w.value else {
                panic!("map")
            };
            m.entries.push(v1_1::WireMapEntry {
                key: key.into(),
                value: Some(profiles::json_wire(&json!("second")).unwrap()),
            });
            let accepted = r
                .validate_type("JsonObject", &w, value::Limits::default(), 1_000_000)
                .is_ok();
            rows.push(json!({"id":format!("{minor}:duplicate:{key}"),"minor":minor,"type":"JsonObject","message":"WireValue","hex":hex_bytes(&w.encode_to_vec())}));
            expected.push(json!({"shape":accepted,"duplicate":true}));
        }
        // Use the frozen InvokeHookRequest input and captured args binding, not a new schema.
        for (key, text) in [
            ("", "baseline"),
            ("unexpected", "extra"),
            ("__proto__", "extra"),
            ("__proto__", "SENTINEL"),
            ("constructor", "extra"),
            ("toString", "extra"),
        ] {
            let mut request =
                dto::InvokeHookRequest::decode(payload("before_tool", "request", minor).as_slice())
                    .unwrap();
            let input = request.input.as_mut().unwrap();
            let mut v = r
                .validate_type(
                    &input.projection_id,
                    input.value.as_ref().unwrap(),
                    value::Limits::default(),
                    1_000_000,
                )
                .unwrap();
            if !key.is_empty() {
                let mut args = serde_json::Map::new();
                args.insert(key.into(), json!({"content":text}));
                v["event"]["args"] = serde_json::Value::Object(args);
            }
            input.value = Some(profiles::json_wire(&v).unwrap());
            let shape = r
                .validate_type(
                    &input.projection_id,
                    input.value.as_ref().unwrap(),
                    value::Limits::default(),
                    1_000_000,
                )
                .is_ok();
            let bytes = request.encode_to_vec();
            let approved = approved(minor)
                .into_iter()
                .find(|a| a.name() == "before_tool")
                .unwrap();
            let mut native = Native::default();
            let exposed = approved
                .validate_payload(
                    minor,
                    "before_tool",
                    Stage::Request,
                    &bytes,
                    value::Limits::default(),
                    jcs::Limits::default(),
                    &mut native,
                )
                .is_ok();
            assert_eq!(native.seen.len(), usize::from(exposed));
            rows.push(json!({"id":format!("{minor}:args:{key}:{text}"),"minor":minor,"type":"hook.input:before_tool","message":"InvokeHookRequest","hex":hex_bytes(&bytes),"leaves":c["exposure"]["open_leaves"]}));
            expected.push(json!({"shape":shape,"json":v,"canonical":std::str::from_utf8(canonical(&v).bytes()).unwrap(),"exposed":exposed}));
        }
    }
    // Both actual child processes complete successfully before any semantic assertion.
    let observations = ["python", "node"].map(|language| {
        (
            language,
            key_fidelity_probe(language, "wire-map-keys", &rows),
        )
    });
    for (language, observations) in observations {
        assert_eq!(observations.as_array().unwrap().len(), rows.len());
        for ((row, expected), actual) in rows
            .iter()
            .zip(&expected)
            .zip(observations.as_array().unwrap())
        {
            assert_eq!(actual["id"], row["id"]);
            assert_eq!(
                actual["roundtripHex"], row["hex"],
                "{language} descriptor bytes {}",
                row["id"]
            );
            assert_eq!(
                actual["shape"],
                json!({"value":expected["shape"]}),
                "{language} native key shape {}",
                row["id"]
            );
            if expected["duplicate"] == true {
                for field in ["json", "canonical", "rebuiltHex"] {
                    assert!(
                        actual[field].get("error").is_some(),
                        "{language} duplicate key {field} {}",
                        row["id"]
                    );
                }
            } else {
                assert_eq!(
                    actual["json"],
                    json!({"value":expected["json"]}),
                    "{language} own key values {}",
                    row["id"]
                );
                assert_eq!(
                    actual["canonical"],
                    json!({"value":expected["canonical"]}),
                    "{language} JCS keys {}",
                    row["id"]
                );
                let rebuilt = profiles::json_wire(&expected["json"])
                    .unwrap()
                    .encode_to_vec();
                assert_eq!(
                    actual["rebuiltHex"],
                    json!({"value":hex_bytes(&rebuilt)}),
                    "{language} rebuilt keys {}",
                    row["id"]
                );
                if let Some(exposed) = expected.get("exposed") {
                    assert_eq!(
                        actual["exposed"],
                        json!({"value":exposed}),
                        "{language} captured open-leaf exposure {}",
                        row["id"]
                    );
                }
            }
        }
    }
}

#[test]
fn selected_descriptor_case_and_portable_property_lookup_use_only_own_keys() {
    use pi_extension_protocol::{jcs, profiles, wire::value};
    let r = registry();
    let captured = corpus()["exposure"]["open_leaves"]["hook.input:before_tool.event.args"].clone();
    let portable = schema(captured.clone());
    let mut rows = Vec::new();
    let mut expected = Vec::new();
    for minor in 0..=1 {
        for key in ["unexpected", "__proto__", "constructor", "toString"] {
            let v = json!({"role":key});
            let shape = r
                .validate_type(
                    "AgentMessage",
                    &profiles::json_wire(&v).unwrap(),
                    value::Limits::default(),
                    1_000_000,
                )
                .is_ok();
            let instance = json!({key:"extra"});
            let result = portable.validate_instance(
                &serde_json::to_vec(&instance).unwrap(),
                jcs::Limits::default(),
            );
            assert_eq!(result, Err(jcs::Error::InstanceMismatch));
            let mut row = key_fidelity_row(&format!("lookup:{key}"), minor, "AgentMessage", &v);
            row["portable"] = json!({"schema":captured,"value":instance});
            rows.push(row);
            expected.push(json!({"shape":shape,"portable":result.is_ok()}));
        }
        // An explicitly declared special property remains selectable and type checked.
        for key in ["__proto__", "constructor", "toString"] {
            let mut properties = serde_json::Map::new();
            properties.insert(key.into(), json!({"type":"string"}));
            let declared = json!({"type":"object","properties":properties,"required":[key],"additionalProperties":false});
            let admitted = schema(declared.clone());
            for instance in [json!({key:"retained"}), json!({key:{}}), json!({})] {
                let result = admitted
                    .validate_instance(
                        &serde_json::to_vec(&instance).unwrap(),
                        jcs::Limits::default(),
                    )
                    .is_ok();
                let mut row = key_fidelity_row(
                    &format!("declared:{key}:{}", rows.len()),
                    minor,
                    "AgentMessage",
                    &json!({"role":"ordinary"}),
                );
                row["portable"] = json!({"schema":declared,"value":instance});
                rows.push(row);
                expected.push(json!({"shape":true,"portable":result}));
            }
        }
        // Static custom-role fallback never supplies a missing public leaf binding.
        for role in ["unexpected", "__proto__", "constructor", "toString"] {
            let v = json!({"role":role});
            let mut request =
                dto::QueueSteerRequest::decode(payload("queue.steer", "request", minor).as_slice())
                    .unwrap();
            let item = request.item.as_mut().unwrap();
            item.value = Some(profiles::json_wire(&v).unwrap());
            let shape = r
                .validate_type(
                    &item.projection_id,
                    item.value.as_ref().unwrap(),
                    value::Limits::default(),
                    1_000_000,
                )
                .is_ok();
            let bytes = request.encode_to_vec();
            let a = approved(minor)
                .into_iter()
                .find(|a| a.name() == "actions")
                .unwrap();
            let mut native = Native::default();
            let result = a.validate_payload(
                minor,
                "queue.steer",
                Stage::Request,
                &bytes,
                value::Limits::default(),
                jcs::Limits::default(),
                &mut native,
            );
            assert_eq!(result.err(), Some(profiles::Error::Exposure));
            assert!(native.seen.is_empty());
            rows.push(json!({"id":format!("{minor}:custom-exposure:{role}"),"minor":minor,"type":"host.queue_item.v1","message":"QueueSteerRequest","hex":hex_bytes(&bytes),"leaves":corpus()["exposure"]["open_leaves"]}));
            expected.push(json!({"shape":shape,"exposed":false}));
        }
    }
    let observations = ["python", "node"].map(|language| {
        (
            language,
            key_fidelity_probe(language, "descriptor-own-keys", &rows),
        )
    });
    for (language, observations) in observations {
        assert_eq!(observations.as_array().unwrap().len(), rows.len());
        for ((row, expected), actual) in rows
            .iter()
            .zip(&expected)
            .zip(observations.as_array().unwrap())
        {
            assert_eq!(actual["id"], row["id"]);
            assert_eq!(actual["roundtripHex"], row["hex"]);
            assert_eq!(
                actual["shape"],
                json!({"value":expected["shape"]}),
                "{language} native custom-role fallback {}",
                row["id"]
            );
            if let Some(portable) = expected.get("portable") {
                assert_eq!(
                    actual["portable"],
                    json!({"value":portable}),
                    "{language} admitted property lookup {}",
                    row["id"]
                );
            }
            if let Some(exposed) = expected.get("exposed") {
                assert_eq!(
                    actual["exposed"],
                    json!({"value":exposed}),
                    "{language} native custom-role exposure {}",
                    row["id"]
                );
            }
        }
    }
}

fn hex_bytes(raw: &[u8]) -> String {
    raw.iter().map(|b| format!("{b:02x}")).collect()
}
