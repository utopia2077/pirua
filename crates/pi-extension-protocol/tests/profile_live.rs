mod profile_support;
use pi_extension_protocol::{
    endpoint::{ActiveFrame, SelectedEnvelope},
    profile_live::*,
    profiles::{Direction, NativeError, wire_v1_1 as dto},
    wire::{v1_0, v1_1},
};
use profile_support::*;
use prost::Message;
use serde_json::{Value, json};
use std::collections::BTreeSet;
fn wire(method: &str, stage: &str) -> Vec<u8> {
    let v: Value =
        serde_json::from_slice(include_bytes!("../schema/projections/method-wire-v1.json"))
            .unwrap();
    let r = v["methods"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["method"] == method && r["minor"] == 1)
        .unwrap();
    let s = r["payloads"][stage]["hex"].as_str().unwrap();
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
fn setup(core: u32) -> Owner {
    let grants = [
        ("queue.steer", "main"),
        ("conversation.read", "main"),
        ("telemetry.receive", "allowed"),
        ("resource.provide", "allowed"),
    ];
    let profiles = vec![
        profile("provider.v1", "remote", "allowed", BTreeSet::new()),
        profile("hook.v1", "before_run", "allowed", BTreeSet::new()),
        profile("host-action.v1", "actions", "main", BTreeSet::new()),
        profile("telemetry-sink.v1", "metrics", "allowed", BTreeSet::new()),
        profile("resource.v1", "resources", "allowed", BTreeSet::new()),
    ];
    let grant = activate(
        core,
        profiles,
        grants
            .iter()
            .map(|(n, s)| v1_1::PermissionGrant {
                name: (*n).into(),
                scope: (*s).into(),
            })
            .collect(),
    );
    Owner::new(
        grant,
        Reservations {
            blocking_bytes: 1_048_576,
            control_bytes: 8_388_608,
            terminal_bytes: 8_388_608,
            progress_bytes: 1_048_576,
            passive_bytes: 1_048_576,
            telemetry_bytes: 1_048_576,
        },
        0,
        200,
    )
    .unwrap()
}
fn start(method: &str) -> Start {
    let (profile_name, name, role, direction, scope) = match method {
        "provider.invoke" => (
            "provider.v1",
            "remote",
            Role::Planned,
            Direction::CoreToExtension,
            "allowed",
        ),
        "before_run" => (
            "hook.v1",
            "before_run",
            Role::Preaccept,
            Direction::CoreToExtension,
            "allowed",
        ),
        "queue.steer" | "queue.follow_up" | "snapshot.get" => (
            "host-action.v1",
            "actions",
            Role::HostAction,
            Direction::ExtensionToCore,
            "main",
        ),
        "telemetry.emit_batch" | "telemetry.flush" => (
            "telemetry-sink.v1",
            "metrics",
            Role::Passive,
            Direction::CoreToExtension,
            "allowed",
        ),
        _ => (
            "resource.v1",
            "resources",
            Role::Passive,
            Direction::CoreToExtension,
            "allowed",
        ),
    };
    let p = profile(profile_name, name, scope, BTreeSet::new());
    let key = if role == Role::Planned {
        Some(v1_1::EffectKey {
            session_id: "s".into(),
            lane: "main".into(),
            operation_id: "o".into(),
            effect_kind: "assistant".into(),
            step_or_task_id: "step".into(),
            logical_attempt: 0,
            item_index: Some(0),
        })
    } else {
        None
    };
    Start {
        capability: p.reference(1).unwrap(),
        method: method.into(),
        direction,
        role,
        key,
        parent: None,
        scope: scope.into(),
        payload: wire(method, "request"),
        now_ms: 4,
        requested_deadline_ms: None,
        method_default_ms: 100,
        method_limit_ms: 150,
        initial_credit: None,
        generation: 7,
    }
}
fn frame(core: u32, arm: v1_1::envelope::Frame) -> ActiveFrame {
    let e = v1_1::Envelope { frame: Some(arm) };
    let body_len = e.encoded_len();
    ActiveFrame {
        envelope: if core == 0 {
            SelectedEnvelope::V1_0(v1_0::Envelope::decode(e.encode_to_vec().as_slice()).unwrap())
        } else {
            SelectedEnvelope::V1_1(e)
        },
        body_len,
    }
}
fn end(id: u64) -> v1_1::envelope::Frame {
    v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
        request_id: id,
        terminal: Some(v1_1::stream_end::Terminal::Result(wire(
            "provider.invoke",
            "terminal",
        ))),
    })
}

#[test]
fn first_terminal_retained_exact_identity_across_loss_drain_duplicate_and_retire() {
    for core in [0, 1] {
        let mut owner = setup(core);
        let mut n = Native::default();
        let (b, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
        owner.dispatched(&b).unwrap();
        assert!(matches!(
            owner.receive(7, frame(core, end(b.id())), 5, &mut n),
            Ok(Accepted::Terminal)
        ));
        let receipt = owner.receipt(&b).unwrap();
        assert_eq!(receipt.terminal_type(), TerminalType::StreamEnd);
        assert!(receipt.binding().same_identity(&b));
        let received = receipt.received_ms();
        let retained = receipt.payload().unwrap().clone();
        owner.drain(6).unwrap();
        owner.lose_generation(7).unwrap();
        assert_eq!(owner.receipt(&b).unwrap().payload(), Some(&retained));
        assert_eq!(
            owner
                .receive(
                    7,
                    frame(
                        core,
                        v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                            request_id: b.id(),
                            terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
                                code: v1_1::ErrorCode::HandlerError as i32,
                                safe_message: "handler failed".into()
                            }))
                        })
                    ),
                    8,
                    &mut n
                )
                .err(),
            Some(Error::Stale)
        );
        assert_eq!(owner.receipt(&b).unwrap().received_ms(), received);
        assert_eq!(owner.receipt(&b).unwrap().payload(), Some(&retained));
        assert_eq!(
            owner.start(start("provider.invoke"), &mut n).err(),
            Some(Error::Deadline)
        );
        owner.retire(&b, RetireReason::Consumed).unwrap();
        assert!(owner.receipt(&b).is_none());
        assert_eq!(owner.occupancy(), 0);
    }
    let mut a = setup(1);
    let mut b = setup(1);
    let mut n = Native::default();
    let (ba, _) = a.start(start("before_run"), &mut n).unwrap();
    let (bb, _) = b.start(start("before_run"), &mut n).unwrap();
    assert!(!ba.same_identity(&bb));
    assert!(a.receipt(&bb).is_none());
    assert_eq!(
        a.retire(&bb, RetireReason::NoLongerExpected),
        Err(Error::Stale)
    );
    a.retire(&ba, RetireReason::NoLongerExpected).unwrap();
}

#[test]
fn unary_stream_types_zero_credit_exact_body_cancel_suppression_sequence_and_deadlines() {
    for core in [0, 1] {
        for shortage in [0, 1] {
            let mut owner = setup(core);
            let mut n = Native::default();
            let mut s = start("provider.invoke");
            let item = v1_1::StreamItem {
                request_id: 1,
                sequence: 0,
                payload: wire("provider.invoke", "progress"),
            };
            let bytes =
                frame(core, v1_1::envelope::Frame::StreamItem(item.clone())).body_len as u64;
            s.initial_credit = if shortage == 0 { None } else { Some(bytes - 1) };
            let (b, _) = owner.start(s, &mut n).unwrap();
            assert_eq!(
                owner
                    .receive(
                        7,
                        frame(core, v1_1::envelope::Frame::StreamItem(item)),
                        5,
                        &mut n
                    )
                    .err(),
                Some(Error::Credit)
            );
            assert!(owner.draining());
            assert_eq!(owner.phase(&b), Some(Phase::Abandoned));
        }
        let mut owner = setup(core);
        let mut n = Native::default();
        let (b, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
        let item = v1_1::StreamItem {
            request_id: b.id(),
            sequence: 0,
            payload: wire("provider.invoke", "progress"),
        };
        let bytes = frame(core, v1_1::envelope::Frame::StreamItem(item.clone())).body_len as u64;
        owner
            .receive(
                7,
                frame(
                    core,
                    v1_1::envelope::Frame::Credit(v1_1::Credit {
                        request_id: b.id(),
                        additional_bytes: bytes,
                    }),
                ),
                5,
                &mut n,
            )
            .unwrap();
        owner.cancel(&b, 1, 6).unwrap();
        assert!(matches!(
            owner.receive(
                7,
                frame(core, v1_1::envelope::Frame::StreamItem(item)),
                7,
                &mut n
            ),
            Ok(Accepted::Suppressed)
        ));
        assert!(matches!(
            owner.receive(7, frame(core, end(b.id())), 8, &mut n),
            Ok(Accepted::Terminal)
        ));
        let mut owner = setup(core);
        let mut s = start("provider.invoke");
        s.initial_credit = Some(bytes * 2);
        let (b, _) = owner.start(s, &mut n).unwrap();
        owner
            .receive(
                7,
                frame(
                    core,
                    v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
                        request_id: b.id(),
                        sequence: 0,
                        payload: wire("provider.invoke", "progress"),
                    }),
                ),
                5,
                &mut n,
            )
            .unwrap();
        assert_eq!(
            owner
                .receive(
                    7,
                    frame(
                        core,
                        v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
                            request_id: b.id(),
                            sequence: 0,
                            payload: wire("provider.invoke", "progress")
                        })
                    ),
                    6,
                    &mut n
                )
                .err(),
            Some(Error::Sequence)
        );
        assert!(owner.draining());
        let mut owner = setup(core);
        let (b, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
        owner.cancel(&b, 1, 5).unwrap();
        assert_eq!(
            owner
                .receive(
                    7,
                    frame(
                        core,
                        v1_1::envelope::Frame::Credit(v1_1::Credit {
                            request_id: b.id(),
                            additional_bytes: 1
                        })
                    ),
                    6,
                    &mut n
                )
                .err(),
            Some(Error::Credit)
        );
    }

    let mut owner = setup(1);
    let mut n = Native::default();
    let (b, _) = owner.start(start("before_run"), &mut n).unwrap();
    assert!(b.key().is_none());
    assert_eq!(
        owner
            .receive(
                7,
                frame(
                    1,
                    v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                        request_id: b.id(),
                        terminal: Some(v1_1::stream_end::Terminal::Result(vec![]))
                    })
                ),
                5,
                &mut n
            )
            .err(),
        Some(Error::Reference)
    );
    assert!(owner.draining());
    let mut owner = setup(1);
    let (b, _) = owner.start(start("before_run"), &mut n).unwrap();
    assert!(matches!(
        owner.receive(
            7,
            frame(
                1,
                v1_1::envelope::Frame::Response(v1_1::Response {
                    request_id: b.id(),
                    terminal: Some(v1_1::response::Terminal::Result(vec![]))
                })
            ),
            6,
            &mut n
        ),
        Ok(Accepted::Terminal)
    ));
    let mut owner = setup(1);
    let mut s = start("provider.invoke");
    s.requested_deadline_ms = Some(1);
    let (b, _) = owner.start(s, &mut n).unwrap();
    assert_eq!(
        owner.receive(7, frame(1, end(b.id())), 5, &mut n).err(),
        Some(Error::Deadline)
    );
    assert_eq!(owner.phase(&b), Some(Phase::Abandoned));
    let mut owner = setup(1);
    let (b, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
    owner.cancel(&b, 1, 5).unwrap();
    assert_eq!(
        owner.receive(7, frame(1, end(b.id())), 25, &mut n).err(),
        Some(Error::Deadline)
    );
}

#[test]
fn parent_grant_scope_secret_guards_precede_native_work_and_ids() {
    let mut owner = setup(1);
    let mut n = Native::default();
    let (parent, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
    let before = n.work;
    let mut missing = start("queue.steer");
    assert_eq!(owner.start(missing, &mut n).err(), Some(Error::Parent));
    assert_eq!(n.work, before);
    let id = owner.next_id();
    missing = start("queue.follow_up");
    missing.parent = Some(parent.clone());
    assert_eq!(owner.start(missing, &mut n).err(), Some(Error::Forbidden));
    assert_eq!(n.work, before);
    assert_eq!(owner.next_id(), id);
    let mut secret = start("queue.steer");
    secret.method = "secret.resolve".into();
    assert_eq!(
        owner.start(secret, &mut n).err(),
        Some(Error::SecretBrokerUnavailable)
    );
    assert_eq!(n.work, before);
    let mut unknown = start("queue.steer");
    unknown.method = "future.method".into();
    assert_eq!(
        owner.start(unknown, &mut n).err(),
        Some(Error::UnsupportedMethod)
    );
    assert_eq!(n.work, before);
    let mut wrong_scope = start("queue.steer");
    wrong_scope.parent = Some(parent.clone());
    wrong_scope.payload = dto::QueueSteerRequest {
        lane: "other".into(),
        item: Some(bound(
            "host.queue_item.v1",
            &json!({"role":"user","content":"x","timestamp":0}),
        )),
    }
    .encode_to_vec();
    assert_eq!(
        owner.start(wrong_scope, &mut n).err(),
        Some(Error::Forbidden)
    );
    assert_eq!(n.work, before);
    let mut child = start("queue.steer");
    child.parent = Some(parent.clone());
    let (child, _) = owner.start(child, &mut n).unwrap();
    assert_eq!(n.work, before + 1);
    assert_eq!(child.parent(), Some(parent.id()));
    owner.cancel(&parent, 1, 5).unwrap();
    assert_eq!(owner.phase(&child), Some(Phase::CancelRequested));
    let mut pending = start("queue.steer");
    pending.now_ms = 6;
    pending.parent = Some(parent);
    assert_eq!(owner.start(pending, &mut n).err(), Some(Error::Parent));
}

#[test]
fn finite_reservations_overload_before_id_or_work_and_host_fault_atomicity() {
    let mut owner = setup(1);
    let mut n = Native::default();
    let mut bindings = vec![];
    for _ in 0..32 {
        bindings.push(owner.start(start("before_run"), &mut n).unwrap().0);
    }
    let id = owner.next_id();
    let before = n.work;
    assert_eq!(
        owner.start(start("before_run"), &mut n).err(),
        Some(Error::Overloaded)
    );
    assert_eq!(owner.next_id(), id);
    assert_eq!(n.work, before);
    assert_eq!(owner.occupancy(), 32);
    owner
        .retire(&bindings[0], RetireReason::NoLongerExpected)
        .unwrap();
    n.fail = Some(NativeError::LocalFault);
    assert_eq!(
        owner.start(start("before_run"), &mut n).err(),
        Some(Error::Validation(
            pi_extension_protocol::profiles::Error::HostLocalFault
        ))
    );
    assert_eq!(owner.next_id(), id);
    assert_eq!(owner.occupancy(), 31);
}

#[test]
fn reserved_gap_pauses_and_stays_immutable_until_exact_resync() {
    assert!(EventQueue::new("s".into(), 7, 2, 1, 0, 0, 50).is_err());
    let mut q = EventQueue::new("s".into(), 7, 2, 32, 0, 0, 50).unwrap();
    q.publish(7, 0, 0, vec![1], 1).unwrap();
    q.publish(7, 1, 1, vec![2], 2).unwrap();
    q.publish(7, 2, 2, vec![3], 3).unwrap();
    assert!(q.paused());
    let gap = q.gap().unwrap().clone();
    assert_eq!((gap.first_dropped, gap.last_dropped), (0, 2));
    assert!(q.take_event().is_none());
    q.publish(7, 3, 3, vec![4], 4).unwrap();
    assert_eq!(q.gap(), Some(&gap));
    assert_eq!(q.cursor(), 4);
    assert_eq!(q.dropped(), 4);
    assert_eq!(q.resync(7, 2, 3, 5), Err(Error::Sequence));
    q.resync(7, 3, 4, 6).unwrap();
    assert!(!q.paused());
    assert_eq!(gap.last_dropped, 2);
    q.publish(7, 4, 4, vec![5], 7).unwrap();
    assert_eq!(q.take_event(), Some(vec![5]));
    assert_eq!(q.publish(8, 5, 5, vec![], 8), Err(Error::Generation));
}

#[test]
fn telemetry_closed_projection_overload_ack_flush_and_disable_are_isolated() {
    let mut sink = Telemetry::new(4096, 8).unwrap();
    let r = registry();
    let record=sink.project(&r,json!({"kind":"span_start","name":"pi.harness.run","attributes":{"pi.operation.outcome":"declined","pi.checkpoint.kind":"failure_drain","secret":"SECRET_SENTINEL","pi.step.outcome":"future"},"free":"SECRET_SENTINEL"})).unwrap().unwrap();
    let admitted = r
        .validate_v1_1(
            "telemetry.record.v1",
            record.wire(),
            value_limits(),
            262_144,
        )
        .unwrap();
    assert_eq!(
        admitted["attributes"],
        json!({"pi.operation.outcome":"declined","pi.checkpoint.kind":"failure_drain"})
    );
    assert!(admitted.get("free").is_none());
    assert!(
        sink.project(&r, json!({"kind":"span_event","name":"unknown"}))
            .unwrap()
            .is_none()
    );
    let seq = sink.enqueue(vec![record.clone()]).unwrap().unwrap();
    let batch = sink.take_batch().unwrap();
    assert_eq!(batch.sequence, seq);
    sink.ack(
        seq,
        dto::TelemetryEmitBatchResult {
            accepted: 1,
            dropped: 0,
        },
    )
    .unwrap();
    sink.flush(seq, seq).unwrap();
    sink.flush(seq, seq).unwrap();
    assert_eq!(sink.flush(seq + 1, seq + 1), Err(Error::Variant));
    assert!(sink.disabled());
    let mut small = Telemetry::new(1, 2).unwrap();
    assert_eq!(small.enqueue(vec![record.clone()]).unwrap(), None);
    assert_eq!(small.dropped(), 1);
    assert!(!small.disabled());
    let mut sink = Telemetry::new(4096, 8).unwrap();
    let seq = sink.enqueue(vec![record]).unwrap().unwrap();
    sink.take_batch().unwrap();
    assert_eq!(
        sink.ack(
            seq,
            dto::TelemetryEmitBatchResult {
                accepted: u64::MAX,
                dropped: 1
            }
        ),
        Err(Error::Variant)
    );
    assert!(sink.disabled());
    let mut owner = setup(1);
    let mut n = Native::default();
    assert!(owner.start(start("before_run"), &mut n).is_ok());
}

#[test]
fn depth_eight_parent_deadline_terminal_and_cross_owner_fences() {
    let mut owner = setup(1);
    let mut n = Native::default();
    let (root, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
    let mut parent = root.clone();
    for _ in 2..=8 {
        let mut s = start("queue.steer");
        s.parent = Some(parent);
        parent = owner.start(s, &mut n).unwrap().0;
    }
    let work = n.work;
    let id = owner.next_id();
    let mut ninth = start("queue.steer");
    ninth.parent = Some(parent);
    assert_eq!(owner.start(ninth, &mut n).err(), Some(Error::Parent));
    assert_eq!(n.work, work);
    assert_eq!(owner.next_id(), id);
    let mut other = setup(1);
    let (foreign, _) = other.start(start("provider.invoke"), &mut n).unwrap();
    let mut s = start("queue.steer");
    s.parent = Some(foreign);
    assert_eq!(owner.start(s, &mut n).err(), Some(Error::Parent));
    let mut owner = setup(1);
    let mut s = start("provider.invoke");
    s.requested_deadline_ms = Some(5);
    let (p, _) = owner.start(s, &mut n).unwrap();
    let mut s = start("queue.steer");
    s.parent = Some(p.clone());
    let (child, _) = owner.start(s, &mut n).unwrap();
    owner.receive(7, frame(1, end(p.id())), 5, &mut n).unwrap();
    assert_eq!(owner.dispatched(&child), Err(Error::Parent));
    let mut s = start("queue.steer");
    s.now_ms = 6;
    s.parent = Some(p);
    assert_eq!(owner.start(s, &mut n).err(), Some(Error::Parent));
}

#[test]
fn malformed_telemetry_ack_and_resource_list_disable_only_their_capability() {
    let mut owner = setup(1);
    let mut n = Native::default();
    let (b, _) = owner.start(start("telemetry.emit_batch"), &mut n).unwrap();
    let bad = dto::TelemetryEmitBatchResult {
        accepted: 0,
        dropped: 0,
    }
    .encode_to_vec();
    assert!(
        owner
            .receive(
                7,
                frame(
                    1,
                    v1_1::envelope::Frame::Response(v1_1::Response {
                        request_id: b.id(),
                        terminal: Some(v1_1::response::Terminal::Result(bad))
                    })
                ),
                5,
                &mut n
            )
            .is_err()
    );
    assert!(owner.capability_disabled(b.capability()));
    assert!(!owner.draining());
    assert!(owner.receipt(&b).is_none());
    let mut s = start("before_run");
    s.now_ms = 6;
    assert!(owner.start(s, &mut n).is_ok());
    let mut owner = setup(1);
    let (b, _) = owner.start(start("resource.list"), &mut n).unwrap();
    assert!(
        owner
            .receive(
                7,
                frame(
                    1,
                    v1_1::envelope::Frame::Response(v1_1::Response {
                        request_id: b.id(),
                        terminal: Some(v1_1::response::Terminal::Result(vec![]))
                    })
                ),
                5,
                &mut n
            )
            .is_err()
    );
    assert!(owner.capability_disabled(b.capability()));
    assert!(!owner.draining());
}

#[test]
fn wrong_generation_minor_request_id_cancel_variant_and_native_fault_never_receipt() {
    for defect in 0..5 {
        let mut owner = setup(1);
        let mut n = Native::default();
        let (b, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
        let mut f = frame(1, end(b.id()));
        let mut generation = 7;
        match defect {
            0 => generation = 8,
            1 => f = frame(0, end(b.id())),
            2 => f = frame(1, end(b.id() + 1)),
            3 => {
                f = frame(
                    1,
                    v1_1::envelope::Frame::Cancel(v1_1::Cancel {
                        request_id: b.id(),
                        reason: 999,
                    }),
                )
            }
            _ => n.fail = Some(NativeError::LocalFault),
        }
        assert!(owner.receive(generation, f, 5, &mut n).is_err());
        assert!(owner.receipt(&b).is_none());
        if defect == 4 {
            assert!(!owner.draining());
            assert_eq!(owner.phase(&b), Some(Phase::Open));
        }
    }
}

#[test]
fn prework_authorization_count_guard() {
    let mut owner = setup(1);
    let mut n = Native::default();
    let (parent, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
    let before = n.work;
    let result = owner.start(start("queue.steer"), &mut n);
    assert_eq!(n.work, before, "missing parent invoked native work");
    assert_eq!(result.err(), Some(Error::Parent));
    let mut request = start("queue.follow_up");
    request.parent = Some(parent);
    let result = owner.start(request, &mut n);
    assert_eq!(n.work, before, "missing grant invoked native work");
    assert_eq!(result.err(), Some(Error::Forbidden));
}

#[test]
fn duplicate_terminal_value_is_opaque_and_immutable() {
    let mut owner = setup(1);
    let mut n = Native::default();
    let (b, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
    owner.receive(7, frame(1, end(b.id())), 5, &mut n).unwrap();
    let original = owner.receipt(&b).unwrap().payload().unwrap().clone();
    let time = owner.receipt(&b).unwrap().received_ms();
    let result = owner.receive(
        7,
        frame(
            1,
            v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                request_id: b.id(),
                terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
                    code: v1_1::ErrorCode::HandlerError as i32,
                    safe_message: "handler failed".into(),
                })),
            }),
        ),
        6,
        &mut n,
    );
    assert_eq!(
        owner.receipt(&b).unwrap().payload(),
        Some(&original),
        "duplicate replaced the opaque first value"
    );
    assert_eq!(owner.receipt(&b).unwrap().received_ms(), time);
    assert!(owner.receipt(&b).unwrap().binding().same_identity(&b));
    assert_eq!(result.err(), Some(Error::Stale));
}

#[test]
fn incoming_request_reference_parent_and_method_denials_precede_work() {
    let mut owner = setup(1);
    let mut n = Native::default();
    let (parent, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
    let before = n.work;
    let next = owner.next_id();
    let mut context = start("queue.steer");
    context.parent = Some(parent.clone());
    let mut bad = context.capability.clone();
    bad.schema_digest_sha256[0] ^= 1;
    let request = v1_1::Request {
        request_id: next,
        parent_request_id: Some(parent.id()),
        capability_ref: Some(bad),
        effect_key: None,
        method: "queue.steer".into(),
        deadline_after_ms: None,
        initial_stream_credit_bytes: None,
        payload: context.payload.clone(),
    };
    assert_eq!(
        owner.accept_request(request, context, &mut n).err(),
        Some(Error::Reference)
    );
    assert_eq!(n.work, before);
    assert_eq!(owner.next_id(), next);
    assert!(owner.draining());
    assert_eq!(owner.phase(&parent), Some(Phase::Abandoned));
    for method in ["secret.resolve", "future.method"] {
        let mut owner = setup(1);
        let (parent, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
        let before = n.work;
        let mut context = start("queue.steer");
        context.parent = Some(parent.clone());
        let request = v1_1::Request {
            request_id: owner.next_id(),
            parent_request_id: Some(parent.id()),
            capability_ref: Some(context.capability.clone()),
            effect_key: None,
            method: method.into(),
            deadline_after_ms: None,
            initial_stream_credit_bytes: None,
            payload: context.payload.clone(),
        };
        let result = owner.accept_request(request, context, &mut n);
        assert_eq!(n.work, before);
        assert_eq!(
            result.err(),
            Some(if method == "secret.resolve" {
                Error::SecretBrokerUnavailable
            } else {
                Error::UnsupportedMethod
            })
        );
        assert!(!owner.draining());
    }
}

#[test]
fn abandonment_lost_target_cannot_gain_first_terminal_after_cancel() {
    let mut observed = Vec::new();
    for core in [0, 1] {
        let mut owner = setup(core);
        let mut n = Native::default();
        let (b, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
        owner.lose_generation(5).unwrap();
        assert_eq!(owner.phase(&b), Some(Phase::Abandoned));
        let work = n.work;
        let cancelled = owner.cancel(&b, 1, 6);
        let phase_after_cancel = owner.phase(&b);
        let terminal = owner.receive(7, frame(core, end(b.id())), 7, &mut n);
        observed.push((
            core,
            cancelled,
            phase_after_cancel,
            terminal.err(),
            owner.receipt(&b).is_some(),
            n.work - work,
        ));
    }
    assert_eq!(
        observed,
        vec![
            (
                0,
                Err(Error::Stale),
                Some(Phase::Abandoned),
                Some(Error::Stale),
                false,
                0
            ),
            (
                1,
                Err(Error::Stale),
                Some(Phase::Abandoned),
                Some(Error::Stale),
                false,
                0
            ),
        ],
        "cancellation revived lost targets and admitted their first terminals"
    );
}

#[test]
fn abandonment_cascade_preserves_closed_descendants_and_retained_receipts() {
    for core in [0, 1] {
        let mut owner = setup(core);
        let mut n = Native::default();
        let (root, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
        let mut s = start("queue.steer");
        s.parent = Some(root.clone());
        s.requested_deadline_ms = Some(1);
        let (closed, _) = owner.start(s, &mut n).unwrap();
        let mut s = start("queue.steer");
        s.parent = Some(closed.clone());
        let (closed_grandchild, _) = owner.start(s, &mut n).unwrap();
        let mut s = start("queue.steer");
        s.parent = Some(root.clone());
        let (retained, _) = owner.start(s, &mut n).unwrap();
        let response = |id| {
            v1_1::envelope::Frame::Response(v1_1::Response {
                request_id: id,
                terminal: Some(v1_1::response::Terminal::Result(wire(
                    "queue.steer",
                    "terminal",
                ))),
            })
        };
        owner
            .receive(7, frame(core, response(retained.id())), 4, &mut n)
            .unwrap();
        let payload = owner.receipt(&retained).unwrap().payload().unwrap().clone();
        let received = owner.receipt(&retained).unwrap().received_ms();
        let mut s = start("queue.steer");
        s.parent = Some(root.clone());
        let (live, _) = owner.start(s, &mut n).unwrap();
        owner.expire(5).unwrap();
        assert_eq!(owner.phase(&root), Some(Phase::Open));
        assert_eq!(owner.phase(&closed), Some(Phase::Abandoned));
        assert_eq!(owner.phase(&closed_grandchild), Some(Phase::Abandoned));
        owner.cancel(&root, 1, 6).unwrap();
        assert_eq!(
            (owner.phase(&closed), owner.phase(&closed_grandchild)),
            (Some(Phase::Abandoned), Some(Phase::Abandoned)),
            "ancestor cancellation revived abandoned descendants"
        );
        assert_eq!(owner.phase(&root), Some(Phase::CancelRequested));
        assert_eq!(owner.phase(&live), Some(Phase::CancelRequested));
        owner.cancel(&root, 2, 7).unwrap();
        assert_eq!(owner.phase(&retained), Some(Phase::TerminalReceived));
        assert_eq!(owner.receipt(&retained).unwrap().payload(), Some(&payload));
        assert_eq!(owner.receipt(&retained).unwrap().received_ms(), received);
        assert!(
            owner
                .receipt(&retained)
                .unwrap()
                .binding()
                .same_identity(&retained)
        );
        assert!(matches!(
            owner.receive(7, frame(core, response(live.id())), 8, &mut n),
            Ok(Accepted::Terminal)
        ));
        assert!(matches!(
            owner.receive(7, frame(core, end(root.id())), 9, &mut n),
            Ok(Accepted::Terminal)
        ));
        let work = n.work;
        assert_eq!(
            owner
                .receive(7, frame(core, response(closed.id())), 10, &mut n)
                .err(),
            Some(Error::Stale)
        );
        assert_eq!(
            owner
                .receive(7, frame(core, response(closed_grandchild.id())), 11, &mut n)
                .err(),
            Some(Error::Stale)
        );
        assert_eq!(n.work, work);
        assert!(owner.receipt(&closed).is_none());
        assert!(owner.receipt(&closed_grandchild).is_none());
        owner.retire(&retained, RetireReason::Consumed).unwrap();
    }
}

#[test]
fn abandonment_deadline_expiry_malformed_close_drain_and_retirement_stay_final() {
    for core in [0, 1] {
        for transition in 0..4 {
            let mut owner = setup(core);
            let mut n = Native::default();
            let mut s = start("provider.invoke");
            if transition < 2 {
                s.requested_deadline_ms = Some(1);
            }
            let (b, _) = owner.start(s, &mut n).unwrap();
            match transition {
                0 => owner.expire(5).unwrap(),
                1 => assert_eq!(
                    owner.receive(7, frame(core, end(b.id())), 5, &mut n).err(),
                    Some(Error::Deadline)
                ),
                2 => {
                    assert_eq!(
                        owner
                            .receive(
                                7,
                                frame(
                                    core,
                                    v1_1::envelope::Frame::Credit(v1_1::Credit {
                                        request_id: b.id(),
                                        additional_bytes: u64::MAX
                                    })
                                ),
                                5,
                                &mut n
                            )
                            .err(),
                        Some(Error::Credit)
                    );
                    assert!(owner.draining());
                }
                _ => {
                    owner.drain(5).unwrap();
                    assert_eq!(owner.phase(&b), Some(Phase::Open));
                    owner.expire(104).unwrap();
                }
            }
            assert_eq!(owner.phase(&b), Some(Phase::Abandoned));
            let now = if transition == 3 { 105 } else { 6 };
            assert_eq!(
                owner.cancel(&b, 1, now),
                Err(Error::Stale),
                "abandoned transition {transition} revived in core minor {core}"
            );
            assert_eq!(owner.phase(&b), Some(Phase::Abandoned));
            assert_eq!(
                owner
                    .receive(7, frame(core, end(b.id())), now + 1, &mut n)
                    .err(),
                Some(Error::Stale)
            );
            assert!(owner.receipt(&b).is_none());
            assert_eq!(owner.retire(&b, RetireReason::Consumed), Err(Error::Stale));
            owner.retire(&b, RetireReason::NoLongerExpected).unwrap();
            assert_eq!(owner.cancel(&b, 1, now + 2), Err(Error::Stale));
            assert_eq!(owner.phase(&b), None);
        }
    }
}

#[test]
fn abandonment_controls_keep_binding_validation_grace_and_receipt_custody() {
    for core in [0, 1] {
        let mut owner = setup(core);
        let mut other = setup(core);
        let mut n = Native::default();
        let (b, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
        let (foreign, _) = other.start(start("provider.invoke"), &mut n).unwrap();
        assert_eq!(b.id(), foreign.id());
        assert_eq!(owner.cancel(&foreign, 1, 5), Err(Error::Stale));
        assert_eq!(owner.cancel(&b, 999, 5), Err(Error::Variant));
        assert_eq!(owner.phase(&b), Some(Phase::Open));
        owner.cancel(&b, 1, 5).unwrap();
        owner.cancel(&b, 2, 6).unwrap();
        assert!(matches!(
            owner.receive(7, frame(core, end(b.id())), 7, &mut n),
            Ok(Accepted::Terminal)
        ));
        let payload = owner.receipt(&b).unwrap().payload().unwrap().clone();
        let received = owner.receipt(&b).unwrap().received_ms();
        owner.drain(8).unwrap();
        owner.lose_generation(9).unwrap();
        owner.expire(200).unwrap();
        assert_eq!(owner.cancel(&b, 1, 201), Err(Error::Stale));
        assert_eq!(owner.phase(&b), Some(Phase::TerminalReceived));
        assert_eq!(owner.receipt(&b).unwrap().payload(), Some(&payload));
        assert_eq!(owner.receipt(&b).unwrap().received_ms(), received);
        assert!(owner.receipt(&b).unwrap().binding().same_identity(&b));
        owner.retire(&b, RetireReason::Consumed).unwrap();
        assert_eq!(owner.occupancy(), 0);
        let mut owner = setup(core);
        let (b, _) = owner.start(start("provider.invoke"), &mut n).unwrap();
        owner.cancel(&b, 1, 5).unwrap();
        owner.cancel(&b, 2, 6).unwrap();
        assert_eq!(
            owner.receive(7, frame(core, end(b.id())), 25, &mut n).err(),
            Some(Error::Deadline)
        );
        assert_eq!(owner.phase(&b), Some(Phase::Abandoned));
        assert!(owner.receipt(&b).is_none());
    }
}
