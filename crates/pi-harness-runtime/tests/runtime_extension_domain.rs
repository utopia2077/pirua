#![cfg(feature = "extensions")]

use std::num::NonZeroUsize;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use pi_ai_core::JsonObject;
use pi_extension_protocol::activation::{
    Activation, AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs,
};
use pi_extension_protocol::endpoint::{ActiveFrame, SelectedEnvelope};
use pi_extension_protocol::jcs::{self, Limits as JsonLimits};
use pi_extension_protocol::tool_live::{
    LiveError, LivePhase, ReceiveOutcome, SelectedArguments, Start, TerminalKind, ToolInvocation,
};
use pi_extension_protocol::wire::framing;
use pi_extension_protocol::wire::{v1_0, v1_1};
use pi_harness_runtime::extensions::domain::{
    DomainHostError, DomainReceiveError, EchoDomainBinding,
};
use pi_harness_runtime::extensions::value::encode_arguments;
use serde_json::{Value, json};

const CATALOG: &str = include_str!("../../pi-extension-protocol/schema/fixtures/host-catalog.json");
const VECTORS: &str =
    include_str!("../../pi-extension-protocol/schema/fixtures/activation-vectors.json");
const SECRET: &str = "SECRET_SENTINEL";

fn hex(value: &str) -> Vec<u8> {
    let (pairs, tail) = value.as_bytes().as_chunks::<2>();
    assert!(tail.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn limits() -> v1_1::ProtocolLimits {
    v1_1::ProtocolLimits {
        pre_handshake_frame_bytes: 4096,
        frame_bytes: 8192,
        aggregate_decoded_bytes: 8192,
        value_depth: 32,
        value_elements: 1024,
        string_bytes: 4096,
        bytes_length: 4096,
        in_flight_requests: 8,
        blocking_queue_bytes: 4096,
        progress_queue_bytes: 4096,
        passive_queue_bytes: 4096,
        telemetry_queue_bytes: 4096,
        nesting_depth: 8,
        requests_per_second: 32,
        handshake_ms: 1000,
        cancellation_grace_ms: 100,
        max_stream_credit_bytes: 4096,
    }
}

fn active(minor: u32, alter_progress: bool) -> Activation {
    active_with_decoded_limit(minor, alter_progress, 8192)
}

fn active_with_decoded_limit(minor: u32, alter_progress: bool, decoded_limit: u64) -> Activation {
    let catalog: Value = serde_json::from_str(CATALOG).unwrap();
    let entry = &catalog["entries"][0];
    let agent = &entry["agentTool"];
    let descriptor = AgentTool {
        name: "echo".into(),
        label: agent["label"].as_str().unwrap().into(),
        description: agent["description"].as_str().unwrap().into(),
        parameters: agent["parameters"].clone(),
        execution_mode: AgentToolMode::Sequential,
        replay: ToolReplay::Safe,
    };
    let mut alternate = entry["selected_minor_schema_sets"]["1.0"]["progress"]["schema"].clone();
    if alter_progress {
        alternate["maxProperties"] = json!(16);
    }
    let inputs = [0, 1]
        .into_iter()
        .map(|tool_minor| {
            let set = &entry["selected_minor_schema_sets"][format!("1.{tool_minor}")];
            ToolSchemaInputs {
                minor: tool_minor,
                arguments: serde_json::to_vec(&set["arguments"]["schema"]).unwrap(),
                progress: serde_json::to_vec(if alter_progress {
                    &alternate
                } else {
                    &set["progress"]["schema"]
                })
                .unwrap(),
                raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).unwrap(),
            }
        })
        .collect();
    let tool =
        ApprovedTool::new("peer-a".into(), descriptor, inputs, JsonLimits::default()).unwrap();
    let mut host_limits = limits();
    host_limits.aggregate_decoded_bytes = decoded_limit;
    let host = HostAuthority::new(
        "peer-a".into(),
        7,
        minor,
        minor,
        host_limits,
        vec![tool],
        vec![],
    )
    .unwrap();
    let mut activation = Activation::new(host, 100);
    // Frozen Hello offers core 1.1 and tool.v1 1.1. The host ceiling selects
    // 1.0 or 1.1 without changing peer-owned bytes.
    let hello = [
        0x0a, 0x04, 0x08, 0x01, 0x18, 0x01, 0x12, 0x0d, 0x0a, 0x07, b't', b'o', b'o', b'l', b'.',
        b'v', b'1', 0x10, 0x01, 0x20, 0x01,
    ];
    activation.receive_hello(&hello, 101).unwrap();
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let mut manifest = hex(fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
        .as_str()
        .unwrap());
    if alter_progress {
        let original = hex(
            entry["selected_minor_schema_sets"]["1.0"]["progress"]["schema_digest_sha256"]
                .as_str()
                .unwrap(),
        );
        let replacement = jcs::admit_schema(
            &serde_json::to_vec(&alternate).unwrap(),
            JsonLimits::default(),
        )
        .unwrap()
        .canonical()
        .sha256();
        let mut replaced = 0;
        for offset in 0..=manifest.len() - 32 {
            if manifest[offset..offset + 32] == original
                && manifest[..offset]
                    .windows(b"tool.progress:echo".len())
                    .rposition(|window| window == b"tool.progress:echo")
                    .is_some_and(|position| offset - position < 60)
            {
                manifest[offset..offset + 32].copy_from_slice(&replacement);
                replaced += 1;
            }
        }
        assert_eq!(replaced, 2);
    }
    activation.receive_manifest(&manifest, 102).unwrap();
    activation.receive_ready(&[], 103).unwrap();
    activation
}

fn start(
    grant: &pi_extension_protocol::activation::ActiveGrant,
    request_id: u64,
) -> ToolInvocation {
    let saved: JsonObject = serde_json::from_str(r#"{"text":"ping"}"#).unwrap();
    let arguments: SelectedArguments = encode_arguments(&saved, grant, "echo").unwrap();
    ToolInvocation::start(
        grant,
        "echo",
        Start {
            effect_key: v1_1::EffectKey {
                session_id: "s".into(),
                lane: "main".into(),
                operation_id: "op".into(),
                effect_kind: "tool".into(),
                step_or_task_id: "turn".into(),
                logical_attempt: 0,
                item_index: Some(0),
            },
            source_call_id: "call".into(),
            request_id,
            active_generation: 7,
            arguments,
            now_ms: 200,
            requested_deadline_after_ms: Some(700),
            method_limit_ms: 1000,
            connection_remaining_ms: 500,
            parent_remaining_ms: Some(300),
            initial_credit_bytes: Some(4096),
        },
    )
    .unwrap()
    .0
}

// Self-contained protobuf encoding for the small echo fixture tree. The wire
// values deliberately differ only in the usage.input integer/float oneof arm.
fn varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while value >= 128 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
    out
}
fn bytes(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![tag << 3 | 2];
    out.extend(varint(payload.len() as u64));
    out.extend(payload);
    out
}
fn integer(value: i64) -> Vec<u8> {
    let zigzag = ((value as u64) << 1) ^ ((value >> 63) as u64);
    let mut out = vec![3 << 3];
    out.extend(varint(zigzag));
    out
}
fn float_one() -> Vec<u8> {
    let mut out = vec![4 << 3 | 1];
    out.extend(1.0f64.to_le_bytes());
    out
}
fn map(entries: Vec<(&str, Vec<u8>)>) -> Vec<u8> {
    let mut map = Vec::new();
    for (key, value) in entries {
        let mut entry = bytes(1, key.as_bytes());
        entry.extend(bytes(2, &value));
        map.extend(bytes(1, &entry));
    }
    bytes(8, &map)
}
fn echo_value(float_input: bool) -> Vec<u8> {
    let cost = map(vec![
        ("input", integer(0)),
        ("output", integer(0)),
        ("cacheRead", integer(0)),
        ("cacheWrite", integer(0)),
        ("total", integer(0)),
    ]);
    let usage = map(vec![
        ("input", if float_input { float_one() } else { integer(1) }),
        ("output", integer(0)),
        ("cacheRead", integer(0)),
        ("cacheWrite", integer(0)),
        ("totalTokens", integer(0)),
        ("cost", cost),
    ]);
    map(vec![
        ("content", bytes(7, &[])),
        ("details", map(vec![])),
        ("usage", usage),
    ])
}
fn bound(kind: &str, float_input: bool) -> Vec<u8> {
    let mut out = bytes(1, format!("tool.{kind}:echo").as_bytes());
    out.extend(bytes(
        2,
        &hex("a08e48cf5321241817eefd20593ea27ef00f7effb1118796abe2da134ea00335"),
    ));
    out.extend(bytes(3, &echo_value(float_input)));
    out
}
fn frame(minor: u32, progress: bool, float_input: bool, request_id: u64) -> ActiveFrame {
    let payload = if progress {
        bytes(1, &bound("progress", float_input))
    } else {
        bytes(1, &bound("result", float_input))
    };
    let cap = NonZeroUsize::new(8192).unwrap();
    if minor == 0 {
        let frame = if progress {
            v1_0::envelope::Frame::StreamItem(v1_0::StreamItem {
                request_id,
                sequence: 0,
                payload,
            })
        } else {
            v1_0::envelope::Frame::StreamEnd(v1_0::StreamEnd {
                request_id,
                terminal: Some(v1_0::stream_end::Terminal::Result(payload)),
            })
        };
        let envelope = v1_0::Envelope { frame: Some(frame) };
        let body_len = framing::encode(&envelope, cap).unwrap().body_len;
        ActiveFrame {
            envelope: SelectedEnvelope::V1_0(envelope),
            body_len,
        }
    } else {
        let frame = if progress {
            v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
                request_id,
                sequence: 0,
                payload,
            })
        } else {
            v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                request_id,
                terminal: Some(v1_1::stream_end::Terminal::Result(payload)),
            })
        };
        let envelope = v1_1::Envelope { frame: Some(frame) };
        let body_len = framing::encode(&envelope, cap).unwrap().body_len;
        ActiveFrame {
            envelope: SelectedEnvelope::V1_1(envelope),
            body_len,
        }
    }
}

#[test]
fn selected_echo_integer_and_float_domain_boundary_both_minors() {
    for minor in [0, 1] {
        let activation = active(minor, false);
        let grant = activation.active_grant().unwrap();
        for progress in [true, false] {
            let mut accepted = start(grant, 17);
            let domain = EchoDomainBinding::new(grant, &accepted).unwrap();
            let output = domain
                .receive_checked(&mut accepted, 7, &frame(minor, progress, false, 17), 201)
                .unwrap();
            assert_eq!(output.value.unwrap().usage.unwrap().input.get(), 1);
            if progress {
                assert!(matches!(output.outcome, ReceiveOutcome::Progress(_)));
                assert_eq!(accepted.next_sequence(), 1);
                assert!(accepted.credit() < 4096);
                assert!(accepted.receipt().is_none());
            } else {
                assert!(matches!(output.outcome, ReceiveOutcome::TerminalReceived));
                assert_eq!(accepted.phase(), LivePhase::TerminalReceived);
                assert_eq!(accepted.receipt().unwrap().kind(), TerminalKind::Result);
            }

            let mut rejected = start(grant, 17);
            let domain = EchoDomainBinding::new(grant, &rejected).unwrap();
            let error = domain
                .receive_checked(&mut rejected, 7, &frame(minor, progress, true, 17), 201)
                .err()
                .unwrap();
            assert!(
                matches!(
                    error,
                    DomainReceiveError::Protocol(LiveError::InvalidProgress)
                        if progress
                ) || matches!(
                    error,
                    DomainReceiveError::Protocol(LiveError::InvalidTerminal)
                        if !progress
                )
            );
            assert_eq!(rejected.phase(), LivePhase::PeerLost);
            assert_eq!(rejected.credit(), 4096);
            assert_eq!(rejected.next_sequence(), 0);
            assert!(rejected.receipt().is_none());
            assert!(!error.to_string().contains("1.0"));
            assert!(!format!("{error:?}").contains(SECRET));
        }
    }
}

#[test]
fn peer_error_terminal_bypasses_typed_result_and_hides_message() {
    for minor in [0, 1] {
        let activation = active(minor, false);
        let grant = activation.active_grant().unwrap();
        let mut invocation = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &invocation).unwrap();
        let envelope = if minor == 0 {
            let envelope = v1_0::Envelope {
                frame: Some(v1_0::envelope::Frame::StreamEnd(v1_0::StreamEnd {
                    request_id: 17,
                    terminal: Some(v1_0::stream_end::Terminal::Error(v1_0::TypedError {
                        code: 14,
                        safe_message: SECRET.into(),
                    })),
                })),
            };
            let body_len = framing::encode(&envelope, NonZeroUsize::new(8192).unwrap())
                .unwrap()
                .body_len;
            ActiveFrame {
                envelope: SelectedEnvelope::V1_0(envelope),
                body_len,
            }
        } else {
            let envelope = v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                    request_id: 17,
                    terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
                        code: 14,
                        safe_message: SECRET.into(),
                    })),
                })),
            };
            let body_len = framing::encode(&envelope, NonZeroUsize::new(8192).unwrap())
                .unwrap()
                .body_len;
            ActiveFrame {
                envelope: SelectedEnvelope::V1_1(envelope),
                body_len,
            }
        };
        let output = domain
            .receive_checked(&mut invocation, 7, &envelope, 201)
            .unwrap();
        assert!(matches!(output.outcome, ReceiveOutcome::TerminalReceived));
        assert!(output.value.is_none());
        let receipt = invocation.receipt().unwrap();
        assert_eq!(receipt.kind(), TerminalKind::Error);
        assert!(!receipt.error().unwrap().1.contains(SECRET));
    }
}

#[test]
fn host_binding_and_selected_projection_reject_before_peer_receive() {
    let activation = active(1, false);
    let grant = activation.active_grant().unwrap();
    let first = start(grant, 17);
    let mut other = start(grant, 18);
    let binding = EchoDomainBinding::new(grant, &first).unwrap();
    let error = binding
        .receive_checked(&mut other, 7, &frame(1, false, false, 18), 201)
        .err()
        .unwrap();
    assert!(matches!(
        error,
        DomainReceiveError::Host(DomainHostError::Binding)
    ));
    assert_eq!(other.phase(), LivePhase::Open);
    assert_eq!(other.credit(), 4096);
    assert!(other.receipt().is_none());

    let altered = active(1, true);
    let altered_grant = altered.active_grant().unwrap();
    let invocation = start(altered_grant, 17);
    let error = EchoDomainBinding::new(altered_grant, &invocation)
        .err()
        .unwrap();
    assert_eq!(error, DomainHostError::Projection);
    assert!(!error.to_string().contains(SECRET));
}

#[test]
fn mismatched_host_decoded_limit_returns_host_fault_without_panicking() {
    // These grants select the same visible capability and generation, but the
    // binding from the second grant carries a tighter decoded-value ceiling.
    // A valid echo result is 172 decoded JSON bytes and reaches the callback.
    for minor in [0, 1] {
        let source = active_with_decoded_limit(minor, false, 8192);
        let tight = active_with_decoded_limit(minor, false, 64);
        let source_grant = source.active_grant().unwrap();
        let tight_grant = tight.active_grant().unwrap();
        assert_eq!(
            source_grant.wire().selected_capabilities,
            tight_grant.wire().selected_capabilities
        );
        assert_eq!(
            source_grant.wire().connection_generation,
            tight_grant.wire().connection_generation
        );

        for progress in [true, false] {
            let mut invocation = start(source_grant, 17);
            let domain = EchoDomainBinding::new(tight_grant, &invocation).unwrap();
            let observed = Arc::new(AtomicUsize::new(0));
            let observed_hook = Arc::clone(&observed);
            let test_thread = std::thread::current().id();
            let prior_hook = std::panic::take_hook();
            std::panic::set_hook(Box::new(move |_| {
                if std::thread::current().id() == test_thread {
                    observed_hook.fetch_add(1, Ordering::SeqCst);
                }
            }));
            let result =
                domain.receive_checked(&mut invocation, 7, &frame(minor, progress, false, 17), 201);
            std::panic::set_hook(prior_hook);

            let error = result.err().unwrap();
            assert!(matches!(
                error,
                DomainReceiveError::Host(DomainHostError::Binding)
            ));
            assert_eq!(observed.load(Ordering::SeqCst), 0);
            assert_eq!(invocation.phase(), LivePhase::Open);
            assert_eq!(invocation.credit(), 4096);
            assert_eq!(invocation.next_sequence(), 0);
            assert!(invocation.receipt().is_none());
            assert!(!error.to_string().contains(SECRET));
        }
    }
}

#[test]
fn cancelled_progress_is_typed_checked_before_suppression() {
    for minor in [0, 1] {
        let activation = active(minor, false);
        let grant = activation.active_grant().unwrap();
        let mut valid = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &valid).unwrap();
        valid.cancel(v1_1::CancelReason::CancelUser, 201).unwrap();
        let outcome = domain
            .receive_checked(&mut valid, 7, &frame(minor, true, false, 17), 202)
            .unwrap();
        assert!(matches!(
            outcome.outcome,
            ReceiveOutcome::IgnoredCancelledProgress
        ));
        assert!(outcome.value.is_none());
        assert_eq!(valid.next_sequence(), 1);
        assert!(valid.credit() < 4096);
        assert!(valid.receipt().is_none());
        let terminal = domain
            .receive_checked(&mut valid, 7, &frame(minor, false, false, 17), 203)
            .unwrap();
        assert!(matches!(terminal.outcome, ReceiveOutcome::TerminalReceived));
        assert!(terminal.value.is_some());
        assert!(valid.receipt().is_some());

        let mut invalid = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &invalid).unwrap();
        invalid.cancel(v1_1::CancelReason::CancelUser, 201).unwrap();
        let error = domain
            .receive_checked(&mut invalid, 7, &frame(minor, true, true, 17), 202)
            .err()
            .unwrap();
        assert!(matches!(
            error,
            DomainReceiveError::Protocol(LiveError::InvalidProgress)
        ));
        assert_eq!(invalid.phase(), LivePhase::PeerLost);
        assert_eq!(invalid.credit(), 4096);
        assert_eq!(invalid.next_sequence(), 0);
        assert!(invalid.receipt().is_none());
    }
}

fn peer_error_frame(minor: u32) -> ActiveFrame {
    let cap = NonZeroUsize::new(8192).unwrap();
    if minor == 0 {
        let envelope = v1_0::Envelope {
            frame: Some(v1_0::envelope::Frame::StreamEnd(v1_0::StreamEnd {
                request_id: 17,
                terminal: Some(v1_0::stream_end::Terminal::Error(v1_0::TypedError {
                    code: 14,
                    safe_message: SECRET.into(),
                })),
            })),
        };
        let body_len = framing::encode(&envelope, cap).unwrap().body_len;
        ActiveFrame {
            envelope: SelectedEnvelope::V1_0(envelope),
            body_len,
        }
    } else {
        let envelope = v1_1::Envelope {
            frame: Some(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                request_id: 17,
                terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
                    code: 14,
                    safe_message: SECRET.into(),
                })),
            })),
        };
        let body_len = framing::encode(&envelope, cap).unwrap().body_len;
        ActiveFrame {
            envelope: SelectedEnvelope::V1_1(envelope),
            body_len,
        }
    }
}

#[test]
fn final_echo_clock_fences_typed_progress_and_result_in_both_minors() {
    use std::cell::Cell;

    for minor in [0, 1] {
        let activation = active(minor, false);
        let grant = activation.active_grant().unwrap();
        for progress in [true, false] {
            let selected = frame(minor, progress, false, 17);
            let mut expired = start(grant, 17);
            let domain = EchoDomainBinding::new(grant, &expired).unwrap();
            let calls = Cell::new(0);
            let error = domain
                .receive_checked_with_final_time(&mut expired, 7, &selected, 499, None, || {
                    calls.set(calls.get() + 1);
                    500
                })
                .err()
                .unwrap();
            assert!(matches!(error, DomainReceiveError::PeerLost));
            assert_eq!(calls.get(), 1);
            assert_eq!(expired.phase(), LivePhase::PeerLost);
            assert_eq!(expired.credit(), 4096);
            assert_eq!(expired.next_sequence(), 0);
            assert!(expired.receipt().is_none());

            let mut accepted = start(grant, 17);
            let domain = EchoDomainBinding::new(grant, &accepted).unwrap();
            let output = domain
                .receive_checked_with_final_time(&mut accepted, 7, &selected, 499, None, || 499)
                .unwrap();
            assert_eq!(output.value.unwrap().usage.unwrap().input.get(), 1);
            if progress {
                assert!(matches!(output.outcome, ReceiveOutcome::Progress(_)));
                assert_eq!(accepted.next_sequence(), 1);
                assert!(accepted.credit() < 4096);
                assert!(accepted.receipt().is_none());
            } else {
                assert!(matches!(output.outcome, ReceiveOutcome::TerminalReceived));
                assert_eq!(accepted.receipt().unwrap().received_ms(), 499);
                assert_eq!(accepted.receipt().unwrap().kind(), TerminalKind::Result);
            }
        }
    }
}

#[test]
fn final_echo_clock_checks_peer_error_without_rust_result_construction() {
    use std::cell::Cell;

    for minor in [0, 1] {
        let activation = active(minor, false);
        let grant = activation.active_grant().unwrap();
        let selected = peer_error_frame(minor);
        let mut expired = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &expired).unwrap();
        let calls = Cell::new(0);
        let error = domain
            .receive_checked_with_final_time(&mut expired, 7, &selected, 499, Some(500), || {
                calls.set(calls.get() + 1);
                500
            })
            .err()
            .unwrap();
        assert!(matches!(error, DomainReceiveError::PeerLost));
        assert_eq!(calls.get(), 1);
        assert!(expired.receipt().is_none());

        let mut accepted = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &accepted).unwrap();
        let output = domain
            .receive_checked_with_final_time(&mut accepted, 7, &selected, 499, Some(500), || 499)
            .unwrap();
        assert!(matches!(output.outcome, ReceiveOutcome::TerminalReceived));
        assert!(output.value.is_none());
        let receipt = accepted.receipt().unwrap();
        assert_eq!(receipt.kind(), TerminalKind::Error);
        assert_eq!(receipt.received_ms(), 499);
        assert!(!receipt.error().unwrap().1.contains(SECRET));
    }
}

#[test]
fn cancelled_echo_progress_charges_only_before_final_grace_cutoff() {
    for minor in [0, 1] {
        let activation = active(minor, false);
        let grant = activation.active_grant().unwrap();
        let selected = frame(minor, true, false, 17);
        let mut accepted = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &accepted).unwrap();
        accepted
            .cancel(v1_1::CancelReason::CancelUser, 201)
            .unwrap();
        let output = domain
            .receive_checked_with_final_time(&mut accepted, 7, &selected, 249, Some(250), || 249)
            .unwrap();
        assert!(matches!(
            output.outcome,
            ReceiveOutcome::IgnoredCancelledProgress
        ));
        assert!(output.value.is_none());
        assert_eq!(accepted.next_sequence(), 1);
        assert_eq!(accepted.credit(), 4096 - selected.body_len as u64);

        let mut expired = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &expired).unwrap();
        expired.cancel(v1_1::CancelReason::CancelUser, 201).unwrap();
        let error = domain
            .receive_checked_with_final_time(&mut expired, 7, &selected, 249, Some(250), || 250)
            .err()
            .unwrap();
        assert!(matches!(error, DomainReceiveError::PeerLost));
        assert_eq!(expired.next_sequence(), 0);
        assert_eq!(expired.credit(), 4096);
        assert!(expired.receipt().is_none());
    }
}

#[test]
fn final_echo_clock_preserves_host_and_peer_error_categories() {
    for minor in [0, 1] {
        let activation = active(minor, false);
        let grant = activation.active_grant().unwrap();
        let source = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &source).unwrap();
        let mut wrong = start(grant, 18);
        let error = domain
            .receive_checked_with_final_time(
                &mut wrong,
                7,
                &frame(minor, false, false, 18),
                499,
                None,
                || panic!("wrong binding must not call final clock"),
            )
            .err()
            .unwrap();
        assert!(matches!(
            error,
            DomainReceiveError::Host(DomainHostError::Binding)
        ));
        assert_eq!(wrong.phase(), LivePhase::Open);
        assert!(wrong.receipt().is_none());

        let mut mismatch = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &mismatch).unwrap();
        let error = domain
            .receive_checked_with_final_time(
                &mut mismatch,
                7,
                &frame(minor, false, true, 17),
                499,
                None,
                || panic!("peer mismatch must not call final clock"),
            )
            .err()
            .unwrap();
        assert!(matches!(
            error,
            DomainReceiveError::Protocol(LiveError::InvalidTerminal)
        ));
        assert_eq!(mismatch.phase(), LivePhase::PeerLost);
        assert!(mismatch.receipt().is_none());

        let source = active_with_decoded_limit(minor, false, 8192);
        let tight = active_with_decoded_limit(minor, false, 64);
        let source_grant = source.active_grant().unwrap();
        let tight_grant = tight.active_grant().unwrap();
        let mut fault = start(source_grant, 17);
        let domain = EchoDomainBinding::new(tight_grant, &fault).unwrap();
        let error = domain
            .receive_checked_with_final_time(
                &mut fault,
                7,
                &frame(minor, false, false, 17),
                499,
                None,
                || panic!("host fault must not call final clock"),
            )
            .err()
            .unwrap();
        assert!(matches!(
            error,
            DomainReceiveError::Host(DomainHostError::Binding)
        ));
        assert_eq!(fault.phase(), LivePhase::Open);
        assert_eq!(fault.credit(), 4096);
        assert_eq!(fault.next_sequence(), 0);
        assert!(fault.receipt().is_none());
    }
}

#[test]
fn fixed_time_echo_receive_keeps_legacy_loss_projection() {
    for minor in [0, 1] {
        let activation = active(minor, false);
        let grant = activation.active_grant().unwrap();
        let selected = frame(minor, false, false, 17);
        let mut expired = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &expired).unwrap();
        let error = domain
            .receive_checked(&mut expired, 7, &selected, 500)
            .err()
            .unwrap();
        assert!(matches!(
            error,
            DomainReceiveError::Protocol(LiveError::PeerLost)
        ));
        assert_eq!(expired.phase(), LivePhase::PeerLost);
        assert!(expired.receipt().is_none());

        let mut accepted = start(grant, 17);
        let domain = EchoDomainBinding::new(grant, &accepted).unwrap();
        let output = domain
            .receive_checked(&mut accepted, 7, &selected, 499)
            .unwrap();
        assert!(matches!(output.outcome, ReceiveOutcome::TerminalReceived));
        assert_eq!(accepted.receipt().unwrap().received_ms(), 499);
    }
}
