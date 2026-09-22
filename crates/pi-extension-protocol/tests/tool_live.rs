use std::num::NonZeroUsize;

use pi_extension_protocol::activation::{
    Activation, AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs,
};
use pi_extension_protocol::endpoint::{ActiveFrame, SelectedEnvelope};
use pi_extension_protocol::jcs::Limits as JsonLimits;
use pi_extension_protocol::tool_live::{
    DomainDecision, DomainKind, LiveError, LivePhase, ReceiveOutcome, RetireReason,
    SelectedArguments, Start, TerminalKind, ToolInvocation,
};
use pi_extension_protocol::wire::framing::{self, FrameReader};
use pi_extension_protocol::wire::value;
use pi_extension_protocol::wire::{v1_0, v1_1};
use prost::Message;
use serde_json::Value;

const CATALOG: &str = include_str!("../schema/fixtures/host-catalog.json");
const VECTORS: &str = include_str!("../schema/fixtures/activation-vectors.json");
const RAW: &str = include_str!("../schema/fixtures/raw-vectors.json");
const LEGACY_INVALID: &str = include_str!("fixtures/raw-echo-legacy-invalid.json");

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}
fn unhex(s: &str) -> Vec<u8> {
    let (pairs, tail) = s.as_bytes().as_chunks::<2>();
    assert!(tail.is_empty());
    pairs
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn raw_frame(name: &str) -> Vec<u8> {
    let raw: Value = serde_json::from_str(RAW).unwrap();
    unhex(raw[name]["frame_hex"].as_str().unwrap())
}
fn legacy_invalid_frame(name: &str) -> Vec<u8> {
    let legacy: Value = serde_json::from_str(LEGACY_INVALID).unwrap();
    let body = unhex(legacy[name]["body_hex"].as_str().unwrap());
    assert_eq!(
        body.len(),
        legacy[name]["body_bytes"].as_u64().unwrap() as usize
    );
    let envelope = v1_1::Envelope::decode(body.as_slice()).unwrap();
    framing::encode(&envelope, nz(8192)).unwrap().bytes
}
fn raw_request() -> (v1_1::Request, v1_1::InvokeToolRequest) {
    let frame = active_frame(&raw_frame("request"), 1);
    let SelectedEnvelope::V1_1(envelope) = frame.envelope else {
        unreachable!()
    };
    let Some(v1_1::envelope::Frame::Request(req)) = envelope.frame else {
        unreachable!()
    };
    let inner = v1_1::InvokeToolRequest::decode(req.payload.as_slice()).unwrap();
    (req, inner)
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
        max_stream_credit_bytes: 256,
    }
}
fn active_with_credit(
    generation: u64,
    core_minor: u32,
    tool_minor: u32,
    max_credit: u64,
) -> Activation {
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
    let inputs = [0, 1]
        .into_iter()
        .map(|minor| {
            let set = &entry["selected_minor_schema_sets"][format!("1.{minor}")];
            ToolSchemaInputs {
                minor,
                arguments: serde_json::to_vec(&set["arguments"]["schema"]).unwrap(),
                progress: serde_json::to_vec(&set["progress"]["schema"]).unwrap(),
                raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).unwrap(),
            }
        })
        .collect();
    let tool =
        ApprovedTool::new("peer-a".into(), descriptor, inputs, JsonLimits::default()).unwrap();
    let mut host_limits = limits();
    host_limits.max_stream_credit_bytes = max_credit;
    let host = HostAuthority::new(
        "peer-a".into(),
        generation,
        core_minor,
        tool_minor,
        host_limits,
        vec![tool],
        vec![],
    )
    .unwrap();
    let mut activation = Activation::new(host, 100);
    let hello = v1_1::Hello {
        protocol: Some(v1_1::VersionRange {
            major: 1,
            min_minor: 0,
            max_minor: core_minor,
        }),
        supported_profiles: vec![v1_1::ProfileRange {
            name: "tool.v1".into(),
            major: 1,
            min_minor: 0,
            max_minor: 1,
        }],
    };
    activation
        .receive_hello(&hello.encode_to_vec(), 101)
        .unwrap();
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = unhex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    activation.receive_manifest(&raw, 102).unwrap();
    activation.receive_ready(&[], 103).unwrap();
    activation
}
fn active(generation: u64, core_minor: u32, tool_minor: u32) -> Activation {
    active_with_credit(generation, core_minor, tool_minor, 256)
}
fn arguments(tool_minor: u32) -> SelectedArguments {
    let (_, inner) = raw_request();
    let arg = inner.arguments.unwrap();
    if tool_minor == 0 {
        SelectedArguments::V1_0(
            v1_0::SchemaBoundValue::decode(arg.encode_to_vec().as_slice()).unwrap(),
        )
    } else {
        SelectedArguments::V1_1(arg)
    }
}
fn start(
    _core_minor: u32,
    tool_minor: u32,
    generation: u64,
    request_id: u64,
    credit: Option<u64>,
) -> Start {
    let (req, inner) = raw_request();
    Start {
        effect_key: req.effect_key.unwrap(),
        source_call_id: inner.tool_call_id,
        request_id,
        active_generation: generation,
        arguments: arguments(tool_minor),
        now_ms: 200,
        requested_deadline_after_ms: Some(700),
        method_limit_ms: 1000,
        connection_remaining_ms: 500,
        parent_remaining_ms: Some(300),
        initial_credit_bytes: credit,
    }
}
fn active_frame(bytes: &[u8], core_minor: u32) -> ActiveFrame {
    if core_minor == 0 {
        let mut r = FrameReader::<v1_0::Envelope>::new(nz(8192));
        let f = r.read_one(bytes).unwrap().frame.unwrap();
        ActiveFrame {
            envelope: SelectedEnvelope::V1_0(f.envelope),
            body_len: f.body_len,
        }
    } else {
        let mut r = FrameReader::<v1_1::Envelope>::new(nz(8192));
        let f = r.read_one(bytes).unwrap().frame.unwrap();
        ActiveFrame {
            envelope: SelectedEnvelope::V1_1(f.envelope),
            body_len: f.body_len,
        }
    }
}
fn frame_new(frame: v1_1::envelope::Frame) -> ActiveFrame {
    let bytes = framing::encode(&v1_1::Envelope { frame: Some(frame) }, nz(8192))
        .unwrap()
        .bytes;
    active_frame(&bytes, 1)
}
fn frame_old(frame: v1_0::envelope::Frame) -> ActiveFrame {
    let bytes = framing::encode(&v1_0::Envelope { frame: Some(frame) }, nz(8192))
        .unwrap()
        .bytes;
    active_frame(&bytes, 0)
}
fn valid_progress(core: u32) -> ActiveFrame {
    active_frame(&raw_frame("progress"), core)
}
fn edited_progress_new(edit: impl FnOnce(&mut v1_1::StreamItem)) -> ActiveFrame {
    let SelectedEnvelope::V1_1(envelope) = valid_progress(1).envelope else {
        unreachable!()
    };
    let Some(v1_1::envelope::Frame::StreamItem(mut item)) = envelope.frame else {
        unreachable!()
    };
    edit(&mut item);
    frame_new(v1_1::envelope::Frame::StreamItem(item))
}
fn edited_progress_old(edit: impl FnOnce(&mut v1_0::StreamItem)) -> ActiveFrame {
    let SelectedEnvelope::V1_0(envelope) = valid_progress(0).envelope else {
        unreachable!()
    };
    let Some(v1_0::envelope::Frame::StreamItem(mut item)) = envelope.frame else {
        unreachable!()
    };
    edit(&mut item);
    frame_old(v1_0::envelope::Frame::StreamItem(item))
}
fn valid_terminal(core: u32) -> ActiveFrame {
    active_frame(&raw_frame("terminal"), core)
}
fn started(core: u32, tool: u32, credit: Option<u64>) -> ToolInvocation {
    started_with_credit(core, tool, credit, 256)
}
fn started_with_credit(
    core: u32,
    tool: u32,
    credit: Option<u64>,
    max_credit: u64,
) -> ToolInvocation {
    let a = active_with_credit(7, core, tool, max_credit);
    ToolInvocation::start(
        a.active_grant().unwrap(),
        "echo",
        start(core, tool, 7, 17, credit),
    )
    .unwrap()
    .0
}

fn wire_integer(value: i64) -> v1_1::WireValue {
    v1_1::WireValue {
        value: Some(v1_1::wire_value::Value::IntegerValue(value)),
    }
}
fn wire_map(entries: Vec<(&str, v1_1::WireValue)>) -> v1_1::WireValue {
    v1_1::WireValue {
        value: Some(v1_1::wire_value::Value::MapValue(v1_1::WireMap {
            entries: entries
                .into_iter()
                .map(|(key, value)| v1_1::WireMapEntry {
                    key: key.into(),
                    value: Some(value),
                })
                .collect(),
        })),
    }
}
fn usage_frame(core: u32, tool: u32, kind: DomainKind, float_input: bool) -> ActiveFrame {
    let catalog: Value = serde_json::from_str(CATALOG).unwrap();
    let slot = match kind {
        DomainKind::Progress => "progress",
        DomainKind::Result => "raw_result",
    };
    let projection =
        &catalog["entries"][0]["selected_minor_schema_sets"][format!("1.{tool}")][slot];
    let id = projection["projection_id"].as_str().unwrap();
    assert_eq!(
        id,
        match kind {
            DomainKind::Progress => "tool.progress:echo",
            DomainKind::Result => "tool.result:echo",
        }
    );
    let digest = unhex(projection["schema_digest_sha256"].as_str().unwrap());
    let input = if float_input {
        v1_1::WireValue {
            value: Some(v1_1::wire_value::Value::FloatValue(1.0)),
        }
    } else {
        wire_integer(1)
    };
    let cost = wire_map(vec![
        ("input", wire_integer(0)),
        ("output", wire_integer(0)),
        ("cacheRead", wire_integer(0)),
        ("cacheWrite", wire_integer(0)),
        ("total", wire_integer(0)),
    ]);
    let usage = wire_map(vec![
        ("input", input),
        ("output", wire_integer(0)),
        ("cacheRead", wire_integer(0)),
        ("cacheWrite", wire_integer(0)),
        ("totalTokens", wire_integer(0)),
        ("cost", cost),
    ]);
    let root = wire_map(vec![
        (
            "content",
            v1_1::WireValue {
                value: Some(v1_1::wire_value::Value::ListValue(v1_1::WireList {
                    items: vec![],
                })),
            },
        ),
        ("details", wire_map(vec![])),
        ("usage", usage),
    ]);
    let bound = v1_1::SchemaBoundValue {
        projection_id: id.into(),
        schema_digest_sha256: digest,
        value: Some(root),
    };
    let envelope = match kind {
        DomainKind::Progress => v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
            request_id: 17,
            sequence: 0,
            payload: v1_1::ToolProgress {
                event: Some(bound),
                percent_complete: None,
            }
            .encode_to_vec(),
        }),
        DomainKind::Result => v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
            request_id: 17,
            terminal: Some(v1_1::stream_end::Terminal::Result(
                v1_1::InvokeToolResult {
                    raw_result: Some(bound),
                }
                .encode_to_vec(),
            )),
        }),
    };
    let encoded = framing::encode(
        &v1_1::Envelope {
            frame: Some(envelope),
        },
        nz(8192),
    )
    .unwrap();
    active_frame(&encoded.bytes, core)
}

fn assert_cancelled_malformed(
    core: u32,
    tool: u32,
    credit: u64,
    frame: &ActiveFrame,
    expected: LiveError,
) {
    let mut live = started(core, tool, Some(credit));
    live.cancel(v1_1::CancelReason::CancelUser, 200).unwrap();
    assert_eq!(live.receive(7, frame, 201).err(), Some(expected));
    assert_eq!(live.phase(), LivePhase::PeerLost);
    assert_eq!(live.credit(), credit);
    assert_eq!(live.next_sequence(), 0);
    assert!(live.receipt().is_none());
}

#[test]
fn domain_rejection_precedes_progress_mutation_and_terminal_retention() {
    let progress = valid_progress(1);
    let mut live = started(1, 1, Some(137));
    let mut calls = 0;
    let outcome = live.receive_checked(7, &progress, 201, |check| {
        calls += 1;
        assert_eq!(check.kind(), DomainKind::Progress);
        DomainDecision::PeerValueMismatch
    });
    assert_eq!(outcome.err(), Some(LiveError::InvalidProgress));
    assert_eq!(calls, 1);
    assert_eq!(live.credit(), 137);
    assert_eq!(live.next_sequence(), 0);
    assert!(live.receipt().is_none());

    let terminal = valid_terminal(1);
    let mut result_live = started(1, 1, None);
    let mut result_calls = 0;
    let outcome = result_live.receive_checked(7, &terminal, 201, |check| {
        result_calls += 1;
        assert_eq!(check.kind(), DomainKind::Result);
        DomainDecision::PeerValueMismatch
    });
    assert_eq!(outcome.err(), Some(LiveError::InvalidTerminal));
    assert_eq!(result_calls, 1);
    assert_eq!(result_live.phase(), LivePhase::PeerLost);
    assert!(result_live.receipt().is_none());
}

#[test]
fn host_domain_fault_returns_without_poisoning_or_charging_the_invocation() {
    for (core, tool) in [(0, 0), (1, 1)] {
        let progress = valid_progress(core);
        let credit = progress.body_len as u64;
        let mut live = started(core, tool, Some(credit));
        let mut calls = 0;
        let error = live.receive_checked(7, &progress, 201, |check| {
            calls += 1;
            assert_eq!(check.kind(), DomainKind::Progress);
            DomainDecision::HostFault
        });
        assert_eq!(error.err(), Some(LiveError::HostDomainFault));
        assert_eq!(calls, 1);
        assert_eq!(live.phase(), LivePhase::Open);
        assert_eq!(live.credit(), credit);
        assert_eq!(live.next_sequence(), 0);
        assert!(live.receipt().is_none());
        assert!(matches!(
            live.receive_checked(7, &progress, 202, |_| DomainDecision::Accept),
            Ok(ReceiveOutcome::Progress(_))
        ));

        let terminal = valid_terminal(core);
        let mut result_live = started(core, tool, None);
        let error = result_live.receive_checked(7, &terminal, 201, |check| {
            assert_eq!(check.kind(), DomainKind::Result);
            DomainDecision::HostFault
        });
        assert_eq!(error.err(), Some(LiveError::HostDomainFault));
        assert_eq!(result_live.phase(), LivePhase::Open);
        assert!(result_live.receipt().is_none());
        assert!(matches!(
            result_live.receive_checked(7, &terminal, 202, |_| DomainDecision::Accept),
            Ok(ReceiveOutcome::TerminalReceived)
        ));

        let mut cancelled = started(core, tool, Some(credit));
        cancelled
            .cancel(v1_1::CancelReason::CancelUser, 200)
            .unwrap();
        assert_eq!(
            cancelled
                .receive_checked(7, &progress, 201, |_| DomainDecision::HostFault)
                .err(),
            Some(LiveError::HostDomainFault)
        );
        assert_eq!(cancelled.phase(), LivePhase::CancelRequested);
        assert_eq!(cancelled.credit(), credit);
        assert_eq!(cancelled.next_sequence(), 0);
        assert!(matches!(
            cancelled.receive_checked(7, &progress, 202, |_| DomainDecision::Accept),
            Ok(ReceiveOutcome::IgnoredCancelledProgress)
        ));
    }
    assert_eq!(
        LiveError::HostDomainFault.to_string(),
        "host tool domain validator failed"
    );
}

#[test]
fn selected_usage_domain_check_distinguishes_integer_and_integral_float_in_both_minors() {
    for (core, tool) in [(0, 0), (1, 1)] {
        for kind in [DomainKind::Progress, DomainKind::Result] {
            let float_frame = usage_frame(core, tool, kind, true);
            let integer_frame = usage_frame(core, tool, kind, false);
            let credit = if kind == DomainKind::Progress {
                Some(float_frame.body_len as u64)
            } else {
                None
            };
            let mut rejected = started_with_credit(core, tool, credit, 512);
            let mut calls = 0;
            let result = rejected.receive_checked(7, &float_frame, 201, |check| {
                calls += 1;
                assert_eq!(check.kind(), kind);
                assert_eq!(check.profile_minor(), tool);
                assert_eq!(check.binding().generation(), 7);
                assert_eq!(
                    check.binding().capability_ref().authenticated_extension_id,
                    "peer-a"
                );
                assert_eq!(check.binding().capability_ref().name, "echo");
                let expected = if kind == DomainKind::Progress {
                    "tool.progress:echo"
                } else {
                    "tool.result:echo"
                };
                assert_eq!(check.projection_id(), expected);
                let catalog: Value = serde_json::from_str(CATALOG).unwrap();
                let slot = if kind == DomainKind::Progress {
                    "progress"
                } else {
                    "raw_result"
                };
                let expected_digest = unhex(
                    catalog["entries"][0]["selected_minor_schema_sets"][format!("1.{tool}")][slot]
                        ["schema_digest_sha256"]
                        .as_str()
                        .unwrap(),
                );
                assert_eq!(check.schema_digest_sha256().as_slice(), expected_digest);
                let value: Value = serde_json::from_slice(check.json()).unwrap();
                assert_eq!(value["usage"]["input"], 1.0);
                assert!(value["usage"]["input"].as_u64().is_none());
                DomainDecision::PeerValueMismatch
            });
            assert_eq!(calls, 1);
            assert_eq!(
                result.err(),
                Some(if kind == DomainKind::Progress {
                    LiveError::InvalidProgress
                } else {
                    LiveError::InvalidTerminal
                })
            );
            assert_eq!(rejected.phase(), LivePhase::PeerLost);
            assert!(rejected.receipt().is_none());
            if kind == DomainKind::Progress {
                assert_eq!(rejected.credit(), float_frame.body_len as u64);
                assert_eq!(rejected.next_sequence(), 0);
            } else {
                let mut protocol_only = started_with_credit(core, tool, None, 512);
                assert!(matches!(
                    protocol_only.receive(7, &float_frame, 201),
                    Ok(ReceiveOutcome::TerminalReceived)
                ));
                assert!(protocol_only.receipt().is_some());
            }

            let credit = if kind == DomainKind::Progress {
                Some(integer_frame.body_len as u64)
            } else {
                None
            };
            let mut accepted = started_with_credit(core, tool, credit, 512);
            let mut accepted_calls = 0;
            let result = accepted.receive_checked(7, &integer_frame, 201, |check| {
                accepted_calls += 1;
                let value: Value = serde_json::from_slice(check.json()).unwrap();
                assert_eq!(value["usage"]["input"].as_u64(), Some(1));
                DomainDecision::Accept
            });
            assert_eq!(accepted_calls, 1);
            if kind == DomainKind::Progress {
                assert!(matches!(result, Ok(ReceiveOutcome::Progress(_))));
                assert_eq!(accepted.credit(), 0);
                assert_eq!(accepted.next_sequence(), 1);
                assert!(accepted.receipt().is_none());
            } else {
                assert!(matches!(result, Ok(ReceiveOutcome::TerminalReceived)));
                let json = accepted.receipt().unwrap().result_json().unwrap();
                let value: Value = serde_json::from_slice(json).unwrap();
                assert_eq!(value["usage"]["input"].as_u64(), Some(1));
                assert!(matches!(
                    accepted.receive_checked(7, &integer_frame, 202, |_| {
                        accepted_calls += 1;
                        DomainDecision::Accept
                    }),
                    Ok(ReceiveOutcome::Stale)
                ));
                assert_eq!(accepted_calls, 1);
            }
        }
    }
}

#[test]
fn domain_callback_is_skipped_before_selected_schema_and_for_typed_error() {
    let progress = valid_progress(1);
    let mut wrong_generation = started(1, 1, Some(137));
    let mut calls = 0;
    assert_eq!(
        wrong_generation
            .receive_checked(8, &progress, 201, |_| {
                calls += 1;
                DomainDecision::Accept
            })
            .err(),
        Some(LiveError::WrongGeneration)
    );
    assert_eq!(calls, 0);
    let mut expired = started(1, 1, Some(137));
    assert_eq!(
        expired
            .receive_checked(7, &progress, 501, |_| {
                calls += 1;
                DomainDecision::Accept
            })
            .err(),
        Some(LiveError::PeerLost)
    );
    assert_eq!(calls, 0);
    let mut wrong_request = started(1, 1, Some(256));
    let frame = edited_progress_new(|item| item.request_id = 18);
    assert_eq!(
        wrong_request
            .receive_checked(7, &frame, 201, |_| {
                calls += 1;
                DomainDecision::Accept
            })
            .err(),
        Some(LiveError::WrongRequestId)
    );
    assert_eq!(calls, 0);
    let mut gap = started(1, 1, Some(256));
    let frame = edited_progress_new(|item| item.sequence = 1);
    assert_eq!(
        gap.receive_checked(7, &frame, 201, |_| {
            calls += 1;
            DomainDecision::Accept
        })
        .err(),
        Some(LiveError::InvalidSequence)
    );
    assert_eq!(calls, 0);
    let mut no_credit = started(1, 1, None);
    assert_eq!(
        no_credit
            .receive_checked(7, &progress, 201, |_| {
                calls += 1;
                DomainDecision::Accept
            })
            .err(),
        Some(LiveError::CreditViolation)
    );
    assert_eq!(calls, 0);
    let mut invalid_schema = started(1, 1, Some(256));
    let frame = edited_progress_new(|item| {
        let mut payload = v1_1::ToolProgress::decode(item.payload.as_slice()).unwrap();
        payload.event.as_mut().unwrap().projection_id = "tool.result:echo".into();
        item.payload = payload.encode_to_vec();
    });
    assert!(matches!(
        invalid_schema.receive_checked(7, &frame, 201, |_| {
            calls += 1;
            DomainDecision::Accept
        }),
        Err(LiveError::Value(value::Error::ProjectionMismatch))
    ));
    assert_eq!(calls, 0);
    let error = frame_new(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
        request_id: 17,
        terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
            code: v1_1::ErrorCode::HandlerError as i32,
            safe_message: "SECRET_SENTINEL".into(),
        })),
    }));
    let mut typed = started(1, 1, None);
    assert!(matches!(
        typed.receive_checked(7, &error, 201, |_| {
            calls += 1;
            DomainDecision::PeerValueMismatch
        }),
        Ok(ReceiveOutcome::TerminalReceived)
    ));
    assert_eq!(calls, 0);
    assert_eq!(
        typed.receipt().unwrap().error(),
        Some((v1_1::ErrorCode::HandlerError, "remote tool error"))
    );
    assert!(matches!(
        typed.receive_checked(7, &error, 202, |_| {
            calls += 1;
            DomainDecision::PeerValueMismatch
        }),
        Ok(ReceiveOutcome::Stale)
    ));
    assert_eq!(calls, 0);
    let mut wrong_arm = started(1, 1, None);
    let credit_frame = frame_new(v1_1::envelope::Frame::Credit(v1_1::Credit {
        request_id: 17,
        additional_bytes: 1,
    }));
    assert_eq!(
        wrong_arm
            .receive_checked(7, &credit_frame, 201, |_| {
                calls += 1;
                DomainDecision::Accept
            })
            .err(),
        Some(LiveError::InvalidTerminal)
    );
    assert_eq!(calls, 0);
    let mut wrong_minor = started(1, 1, Some(137));
    assert_eq!(
        wrong_minor
            .receive_checked(7, &valid_progress(0), 201, |_| {
                calls += 1;
                DomainDecision::Accept
            })
            .err(),
        Some(LiveError::WrongVersion)
    );
    assert_eq!(calls, 0);
}

#[test]
fn cancelled_progress_still_gets_domain_check_before_debit() {
    let float = usage_frame(1, 1, DomainKind::Progress, true);
    let mut rejected = started_with_credit(1, 1, Some(float.body_len as u64), 512);
    rejected
        .cancel(v1_1::CancelReason::CancelUser, 200)
        .unwrap();
    let mut calls = 0;
    assert_eq!(
        rejected
            .receive_checked(7, &float, 201, |check| {
                calls += 1;
                assert_eq!(check.kind(), DomainKind::Progress);
                DomainDecision::PeerValueMismatch
            })
            .err(),
        Some(LiveError::InvalidProgress)
    );
    assert_eq!(calls, 1);
    assert_eq!(rejected.credit(), float.body_len as u64);
    assert_eq!(rejected.next_sequence(), 0);
    assert!(rejected.receipt().is_none());
    let integer = usage_frame(1, 1, DomainKind::Progress, false);
    let mut accepted = started_with_credit(1, 1, Some(integer.body_len as u64), 512);
    accepted
        .cancel(v1_1::CancelReason::CancelUser, 200)
        .unwrap();
    assert!(matches!(
        accepted.receive_checked(7, &integer, 201, |_| DomainDecision::Accept),
        Ok(ReceiveOutcome::IgnoredCancelledProgress)
    ));
    assert_eq!(accepted.credit(), 0);
    assert_eq!(accepted.next_sequence(), 1);
}

#[test]
fn local_validator_panic_leaves_invocation_unmodified_for_host_fault_handling() {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    let progress = valid_progress(1);
    let mut live = started(1, 1, Some(137));
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let _ = live.receive_checked(7, &progress, 201, |_| panic!("local validator failure"));
    }));
    assert!(outcome.is_err());
    assert_eq!(live.phase(), LivePhase::Open);
    assert_eq!(live.credit(), 137);
    assert_eq!(live.next_sequence(), 0);
    assert!(live.receipt().is_none());
    assert!(matches!(
        live.receive(7, &progress, 202),
        Ok(ReceiveOutcome::Progress(_))
    ));

    let terminal = valid_terminal(1);
    let mut result_live = started(1, 1, None);
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        let _ =
            result_live.receive_checked(7, &terminal, 201, |_| panic!("local validator failure"));
    }));
    assert!(outcome.is_err());
    assert_eq!(result_live.phase(), LivePhase::Open);
    assert!(result_live.receipt().is_none());
    assert!(matches!(
        result_live.receive(7, &terminal, 202),
        Ok(ReceiveOutcome::TerminalReceived)
    ));
}

#[test]
fn outbound_request_uses_exact_host_binding_and_selected_dtos() {
    for (core, tool) in [(0, 1), (1, 0), (1, 1)] {
        let a = active(7, core, tool);
        let (live, encoded) = ToolInvocation::start(
            a.active_grant().unwrap(),
            "echo",
            start(core, tool, 7, 17, None),
        )
        .unwrap();
        assert_eq!(live.binding().generation(), 7);
        assert_eq!(live.binding().request_id(), 17);
        assert_eq!(live.binding().effect_key().effect_kind, "tool");
        assert_eq!(live.source_call_id(), "call-1");
        assert_eq!(live.deadline_ms(), 500);
        assert_eq!(live.credit(), 0);
        assert_eq!(live.replay(), ToolReplay::Safe);
        let frame = active_frame(&encoded.bytes, core);
        let (request_id, method, credit, payload, key, cap) = match frame.envelope {
            SelectedEnvelope::V1_0(e) => {
                let Some(v1_0::envelope::Frame::Request(r)) = e.frame else {
                    panic!()
                };
                (
                    r.request_id,
                    r.method,
                    r.initial_stream_credit_bytes,
                    r.payload,
                    r.effect_key.unwrap().encode_to_vec(),
                    r.capability_ref.unwrap().encode_to_vec(),
                )
            }
            SelectedEnvelope::V1_1(e) => {
                let Some(v1_1::envelope::Frame::Request(r)) = e.frame else {
                    panic!()
                };
                (
                    r.request_id,
                    r.method,
                    r.initial_stream_credit_bytes,
                    r.payload,
                    r.effect_key.unwrap().encode_to_vec(),
                    r.capability_ref.unwrap().encode_to_vec(),
                )
            }
        };
        assert_eq!(request_id, 17);
        assert_eq!(method, "tool.invoke");
        assert_eq!(credit, None);
        assert_eq!(
            v1_1::EffectKey::decode(key.as_slice()).unwrap(),
            *live.binding().effect_key()
        );
        assert_eq!(
            v1_1::CapabilityRef::decode(cap.as_slice()).unwrap(),
            *live.binding().capability_ref()
        );
        if tool == 0 {
            let inner = v1_0::InvokeToolRequest::decode(payload.as_slice()).unwrap();
            assert_eq!(inner.tool_id, "echo");
            assert_eq!(inner.tool_call_id, "call-1");
            assert_eq!(
                inner.arguments.unwrap().projection_id,
                "tool.arguments:echo"
            );
        } else {
            let inner = v1_1::InvokeToolRequest::decode(payload.as_slice()).unwrap();
            assert_eq!(inner.tool_id, "echo");
            assert_eq!(inner.tool_call_id, "call-1");
            assert_eq!(
                inner.arguments.unwrap().projection_id,
                "tool.arguments:echo"
            );
        }
    }
}

#[test]
fn start_rejects_peer_shaped_or_unbounded_arguments_and_old_generation() {
    let a = active(7, 1, 1);
    let grant = a.active_grant().unwrap();
    assert!(matches!(
        ToolInvocation::start(grant, "other", start(1, 1, 7, 17, None)),
        Err(LiveError::InvalidHostBinding)
    ));
    let mut wrong = start(1, 1, 7, 17, None);
    let SelectedArguments::V1_1(ref mut arg) = wrong.arguments else {
        unreachable!()
    };
    arg.projection_id = "synthetic.echo.arguments".into();
    assert!(matches!(
        ToolInvocation::start(grant, "echo", wrong),
        Err(LiveError::Value(value::Error::ProjectionMismatch))
    ));
    let mut wrong = start(1, 1, 7, 17, None);
    let SelectedArguments::V1_1(ref mut arg) = wrong.arguments else {
        unreachable!()
    };
    arg.schema_digest_sha256[0] ^= 1;
    assert!(matches!(
        ToolInvocation::start(grant, "echo", wrong),
        Err(LiveError::Value(value::Error::ProjectionMismatch))
    ));
    let mut wrong = start(1, 1, 7, 17, None);
    let SelectedArguments::V1_1(ref mut arg) = wrong.arguments else {
        unreachable!()
    };
    arg.value = Some(v1_1::WireValue {
        value: Some(v1_1::wire_value::Value::TextValue("wrong".into())),
    });
    assert!(matches!(
        ToolInvocation::start(grant, "echo", wrong),
        Err(LiveError::Value(value::Error::Schema(_)))
    ));
    let mut wrong = start(1, 1, 7, 17, None);
    wrong.active_generation = 8;
    assert!(matches!(
        ToolInvocation::start(grant, "echo", wrong),
        Err(LiveError::InvalidHostBinding)
    ));
    let wrong = start(1, 1, 7, 0, None);
    assert!(matches!(
        ToolInvocation::start(grant, "echo", wrong),
        Err(LiveError::InvalidHostBinding)
    ));
    let mut wrong = start(1, 1, 7, 17, Some(257));
    assert!(matches!(
        ToolInvocation::start(grant, "echo", wrong),
        Err(LiveError::CreditViolation)
    ));
    wrong = start(1, 1, 7, 17, None);
    wrong.now_ms = u64::MAX;
    assert!(matches!(
        ToolInvocation::start(grant, "echo", wrong),
        Err(LiveError::InvalidDeadline)
    ));
    let mut wrong = start(1, 1, 7, 17, None);
    wrong.effect_key.effect_kind = "provider".into();
    assert!(matches!(
        ToolInvocation::start(grant, "echo", wrong),
        Err(LiveError::InvalidEffectKey)
    ));
    let a0 = active(7, 1, 0);
    let wrong = start(1, 1, 7, 17, None);
    assert!(matches!(
        ToolInvocation::start(a0.active_grant().unwrap(), "echo", wrong),
        Err(LiveError::WrongVersion)
    ));
}

#[test]
fn exact_137_byte_progress_credit_and_sequence_are_enforced() {
    let progress = active_frame(&legacy_invalid_frame("progress"), 1);
    assert_eq!(progress.body_len, 132);
    let mut zero = started(1, 1, None);
    assert_eq!(
        zero.receive(7, &progress, 201).err(),
        Some(LiveError::CreditViolation)
    );
    let mut below = started(1, 1, Some(131));
    assert!(matches!(
        below.receive(7, &progress, 201),
        Err(LiveError::CreditViolation)
    ));
    assert_eq!(below.phase(), LivePhase::PeerLost);
    assert!(below.receipt().is_none());
    let mut exact = started(1, 1, Some(132));
    assert!(matches!(
        exact.receive(7, &progress, 201),
        Err(LiveError::Value(value::Error::MissingValue))
    ));
    let progress = valid_progress(1);
    assert_eq!(progress.body_len, 137);
    let mut one_short = started(1, 1, Some(136));
    assert_eq!(
        one_short.receive(7, &progress, 201).err(),
        Some(LiveError::CreditViolation)
    );
    let mut exact = started(1, 1, Some(progress.body_len as u64));
    let outcome = exact.receive(7, &progress, 201).unwrap();
    let ReceiveOutcome::Progress(value) = outcome else {
        panic!()
    };
    assert!(
        std::str::from_utf8(value.json())
            .unwrap()
            .contains("working")
    );
    assert_eq!(value.percent_complete(), None);
    assert_eq!(exact.credit(), 0);
    assert!(matches!(
        exact.receive(7, &progress, 202),
        Err(LiveError::InvalidSequence)
    ));
    assert_eq!(exact.phase(), LivePhase::PeerLost);

    let mut replenished = started(1, 1, None);
    let credit = replenished
        .grant_credit(progress.body_len as u64, 200)
        .unwrap();
    assert!(credit.body_len > 0);
    assert_eq!(replenished.credit(), progress.body_len as u64);
    assert!(matches!(
        replenished.grant_credit(u64::MAX, 200),
        Err(LiveError::CreditViolation)
    ));
    replenished.receive(7, &progress, 201).unwrap();
    assert_eq!(replenished.credit(), 0);
    replenished.grant_credit(256, 202).unwrap();
    assert_eq!(replenished.credit(), 256);
}

#[test]
fn selected_progress_minor_and_real_projection_ids_are_checked() {
    let raw = valid_progress(1);
    let mut one = started(1, 1, Some(256));
    let mut frame = raw;
    let SelectedEnvelope::V1_1(ref mut e) = frame.envelope else {
        unreachable!()
    };
    let Some(v1_1::envelope::Frame::StreamItem(ref mut item)) = e.frame else {
        unreachable!()
    };
    let mut progress = v1_1::ToolProgress::decode(item.payload.as_slice()).unwrap();
    progress.event.as_mut().unwrap().projection_id = "tool.result:echo".into();
    item.payload = progress.encode_to_vec();
    assert!(matches!(
        one.receive(7, &frame, 201),
        Err(LiveError::Value(value::Error::ProjectionMismatch))
    ));

    let mut too_high = started(1, 1, Some(256));
    let mut frame = valid_progress(1);
    let SelectedEnvelope::V1_1(ref mut e) = frame.envelope else {
        unreachable!()
    };
    let Some(v1_1::envelope::Frame::StreamItem(ref mut item)) = e.frame else {
        unreachable!()
    };
    let mut progress = v1_1::ToolProgress::decode(item.payload.as_slice()).unwrap();
    progress.percent_complete = Some(101);
    item.payload = progress.encode_to_vec();
    assert!(matches!(
        too_high.receive(7, &frame, 201),
        Err(LiveError::InvalidProgress)
    ));

    let mut old = started(1, 0, Some(256));
    let mut frame = valid_progress(1);
    let SelectedEnvelope::V1_1(ref mut e) = frame.envelope else {
        unreachable!()
    };
    let Some(v1_1::envelope::Frame::StreamItem(ref mut item)) = e.frame else {
        unreachable!()
    };
    let mut progress = v1_1::ToolProgress::decode(item.payload.as_slice()).unwrap();
    progress.percent_complete = Some(0);
    item.payload = progress.encode_to_vec();
    assert!(matches!(
        old.receive(7, &frame, 201),
        Err(LiveError::UnsupportedVariant)
    ));
    let frame = valid_progress(0);
    let mut old_ok = started(0, 0, Some(frame.body_len as u64));
    assert!(matches!(
        old_ok.receive(7, &frame, 201),
        Ok(ReceiveOutcome::Progress(_))
    ));
    let mut mixed = started(0, 1, Some(frame.body_len as u64));
    assert!(matches!(
        mixed.receive(7, &frame, 201),
        Ok(ReceiveOutcome::Progress(_))
    ));
}

#[test]
fn zero_credit_terminal_is_retained_through_loss_and_explicit_retirement() {
    let terminal = valid_terminal(1);
    assert_eq!(terminal.body_len, 131);
    let mut live = started(1, 1, None);
    let binding = live.binding().clone();
    assert!(matches!(
        live.receive(7, &terminal, 201),
        Ok(ReceiveOutcome::TerminalReceived)
    ));
    assert_eq!(live.phase(), LivePhase::TerminalReceived);
    let receipt = live.receipt().unwrap();
    assert_eq!(receipt.kind(), TerminalKind::Result);
    assert!(receipt.binding().same_live_identity(&binding));
    assert!(
        std::str::from_utf8(receipt.result_json().unwrap())
            .unwrap()
            .contains("pong")
    );
    let first = receipt.result_json().unwrap().to_vec();
    live.begin_drain(210);
    live.peer_lost();
    assert!(!live.expire(999));
    assert_eq!(live.receipt().unwrap().result_json().unwrap(), first);
    assert!(matches!(
        live.receive(7, &terminal, 1000),
        Ok(ReceiveOutcome::Stale)
    ));
    let later = active_frame(&raw_frame("progress"), 1);
    assert!(matches!(
        live.receive(7, &later, 1001),
        Ok(ReceiveOutcome::Stale)
    ));
    assert_eq!(live.receipt().unwrap().result_json().unwrap(), first);
    let other = started(1, 1, None).binding().clone();
    assert_eq!(
        live.retire(&other, RetireReason::Committed).unwrap_err(),
        LiveError::WrongBinding
    );
    assert!(live.receipt().is_some());
    live.retire(&binding, RetireReason::Committed).unwrap();
    assert_eq!(live.phase(), LivePhase::Retired);
    assert!(live.receipt().is_none());
    let mut old_tool = started(1, 0, None);
    assert!(matches!(
        old_tool.receive(7, &terminal, 201),
        Ok(ReceiveOutcome::TerminalReceived)
    ));
    let old_binding = old_tool.binding().clone();
    old_tool
        .retire(&old_binding, RetireReason::NoLongerExpected)
        .unwrap();
    assert!(old_tool.receipt().is_none());
}

#[test]
fn wrong_generation_and_key_do_not_replace_old_live_receipt() {
    let terminal = valid_terminal(1);
    let mut live = started(1, 1, None);
    assert_eq!(
        live.receive(8, &terminal, 201).err(),
        Some(LiveError::WrongGeneration)
    );
    assert_eq!(live.phase(), LivePhase::Open);
    live.begin_drain(300);
    assert!(matches!(
        live.receive(7, &terminal, 202),
        Ok(ReceiveOutcome::TerminalReceived)
    ));
    assert_eq!(live.receipt().unwrap().binding().generation(), 7);
    let other = started(1, 1, None).binding().clone();
    assert!(!live.receipt().unwrap().binding().same_live_identity(&other));
    assert_eq!(
        live.retire(&other, RetireReason::NoLongerExpected)
            .unwrap_err(),
        LiveError::WrongBinding
    );
    let mut wrong_key = live.binding().effect_key().clone();
    wrong_key.operation_id = "other".into();
    assert_ne!(live.receipt().unwrap().binding().effect_key(), &wrong_key);
    assert!(live.receipt().is_some());
}

#[test]
fn malformed_terminal_or_wrong_request_never_mints_receipt() {
    let invalid = legacy_invalid_frame("terminal");
    let mut frozen_invalid = started(1, 1, None);
    assert_eq!(
        frozen_invalid
            .receive(7, &active_frame(&invalid, 1), 201)
            .err(),
        Some(LiveError::Value(value::Error::MissingValue))
    );
    assert!(frozen_invalid.receipt().is_none());
    let raw = raw_frame("terminal");
    let mut wrong = started(1, 1, None);
    let mut frame = active_frame(&raw, 1);
    let SelectedEnvelope::V1_1(ref mut e) = frame.envelope else {
        unreachable!()
    };
    let Some(v1_1::envelope::Frame::StreamEnd(ref mut end)) = e.frame else {
        unreachable!()
    };
    end.request_id = 18;
    assert_eq!(
        wrong.receive(7, &frame, 201).err(),
        Some(LiveError::WrongRequestId)
    );
    assert!(wrong.receipt().is_none());

    let mut wrong = started(1, 1, None);
    let mut frame = active_frame(&raw, 1);
    let SelectedEnvelope::V1_1(ref mut e) = frame.envelope else {
        unreachable!()
    };
    let Some(v1_1::envelope::Frame::StreamEnd(ref mut end)) = e.frame else {
        unreachable!()
    };
    let Some(v1_1::stream_end::Terminal::Result(ref mut bytes)) = end.terminal else {
        unreachable!()
    };
    let mut result = v1_1::InvokeToolResult::decode(bytes.as_slice()).unwrap();
    result.raw_result.as_mut().unwrap().projection_id = "tool.progress:echo".into();
    *bytes = result.encode_to_vec();
    assert!(matches!(
        wrong.receive(7, &frame, 201),
        Err(LiveError::Value(value::Error::ProjectionMismatch))
    ));
    assert!(wrong.receipt().is_none());

    let mut wrong = started(1, 1, None);
    let frame = frame_new(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
        request_id: 17,
        terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
            code: 127,
            safe_message: "x".into(),
        })),
    }));
    assert_eq!(
        wrong.receive(7, &frame, 201).err(),
        Some(LiveError::UnsupportedVariant)
    );
    assert!(wrong.receipt().is_none());
}

#[test]
fn error_terminal_and_cancel_arrival_order_are_explicit() {
    let error = frame_new(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
        request_id: 17,
        terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
            code: v1_1::ErrorCode::HandlerError as i32,
            safe_message: "safe".into(),
        })),
    }));
    let mut cancelled = started(1, 1, None);
    let cancel = cancelled
        .cancel(v1_1::CancelReason::CancelUser, 200)
        .unwrap();
    assert!(cancel.body_len > 0);
    assert_eq!(cancelled.phase(), LivePhase::CancelRequested);
    assert!(matches!(
        cancelled.receive(7, &error, 201),
        Ok(ReceiveOutcome::TerminalReceived)
    ));
    let receipt = cancelled.receipt().unwrap();
    assert_eq!(receipt.kind(), TerminalKind::Error);
    assert_eq!(
        receipt.error(),
        Some((v1_1::ErrorCode::HandlerError, "remote tool error"))
    );
    let mut terminal_first = started(1, 1, None);
    terminal_first.receive(7, &error, 201).unwrap();
    assert_eq!(
        terminal_first
            .cancel(v1_1::CancelReason::CancelUser, 202)
            .unwrap_err(),
        LiveError::Stale
    );
    assert_eq!(
        terminal_first.receipt().unwrap().error(),
        Some((v1_1::ErrorCode::HandlerError, "remote tool error"))
    );
}

#[test]
fn preterminal_loss_deadline_and_safe_replay_get_fresh_identity() {
    let a = active(7, 1, 1);
    let first_start = start(1, 1, 7, 17, None);
    let key = first_start.effect_key.clone();
    let call_id = first_start.source_call_id.clone();
    let (mut first, _) =
        ToolInvocation::start(a.active_grant().unwrap(), "echo", first_start).unwrap();
    let first_binding = first.binding().clone();
    first.peer_lost();
    assert_eq!(first.phase(), LivePhase::PeerLost);
    assert!(first.receipt().is_none());
    let b = active(8, 1, 1);
    let second_start = start(1, 1, 8, 18, None);
    assert_eq!(second_start.effect_key, key);
    assert_eq!(second_start.source_call_id, call_id);
    let (second, _) =
        ToolInvocation::start(b.active_grant().unwrap(), "echo", second_start).unwrap();
    assert!(!first_binding.same_live_identity(second.binding()));
    assert_eq!(second.binding().generation(), 8);
    assert_eq!(second.binding().request_id(), 18);
    assert_eq!(second.binding().effect_key(), &key);
    assert_eq!(second.source_call_id(), call_id);
    assert!(matches!(second.replay(), ToolReplay::Safe));
    let old_start = start(1, 1, 7, 19, None);
    assert!(matches!(
        ToolInvocation::start(
            a.active_grant().unwrap(),
            "echo",
            Start {
                active_generation: 8,
                ..old_start
            }
        ),
        Err(LiveError::InvalidHostBinding)
    ));
    let mut expired = started(1, 1, None);
    assert!(expired.expire(500));
    assert_eq!(expired.phase(), LivePhase::PeerLost);
    assert!(expired.receipt().is_none());
    let mut drained = started(1, 1, None);
    drained.begin_drain(210);
    assert!(drained.expire(210));
    assert!(drained.receipt().is_none());
}

#[test]
fn inner_payload_is_capped_before_prost_and_unknown_active_arm_is_rejected() {
    let mut live = started(1, 1, Some(256));
    let frame = ActiveFrame {
        body_len: 132,
        envelope: SelectedEnvelope::V1_1(v1_1::Envelope {
            frame: Some(v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
                request_id: 17,
                sequence: 0,
                payload: vec![0; 8193],
            })),
        }),
    };
    assert_eq!(live.receive(7, &frame, 201).err(), Some(LiveError::Limit));
    assert!(live.receipt().is_none());
    let mut live = started(1, 1, None);
    let frame = frame_new(v1_1::envelope::Frame::Credit(v1_1::Credit {
        request_id: 17,
        additional_bytes: 1,
    }));
    assert_eq!(
        live.receive(7, &frame, 201).err(),
        Some(LiveError::InvalidTerminal)
    );
    let mut live0 = started(0, 0, None);
    let error = frame_old(v1_0::envelope::Frame::StreamEnd(v1_0::StreamEnd {
        request_id: 17,
        terminal: Some(v1_0::stream_end::Terminal::Error(v1_0::TypedError {
            code: v1_0::ErrorCode::HandlerError as i32,
            safe_message: "safe".into(),
        })),
    }));
    assert!(matches!(
        live0.receive(7, &error, 201),
        Ok(ReceiveOutcome::TerminalReceived)
    ));
}

#[test]
fn planner_shaped_tool_effect_key_accepts_zero_and_requires_safe_index() {
    // pi-harness-core::tool_batch::effect mints ToolEffectPlan with attempt 0
    // and Some(source_index); both numbers are pi-agent-schema SafeUnsigned.
    let a = active(7, 1, 1);
    let grant = a.active_grant().unwrap();
    let mut planned = start(1, 1, 7, 17, None);
    planned.effect_key.logical_attempt = 0;
    planned.effect_key.item_index = Some(0);
    assert!(ToolInvocation::start(grant, "echo", planned).is_ok());

    let mut max_safe = start(1, 1, 7, 17, None);
    max_safe.effect_key.logical_attempt = 9_007_199_254_740_991;
    max_safe.effect_key.item_index = Some(9_007_199_254_740_991);
    assert!(ToolInvocation::start(grant, "echo", max_safe).is_ok());

    let mut missing = start(1, 1, 7, 17, None);
    missing.effect_key.item_index = None;
    assert!(matches!(
        ToolInvocation::start(grant, "echo", missing),
        Err(LiveError::InvalidEffectKey)
    ));
    let mut unsafe_attempt = start(1, 1, 7, 17, None);
    unsafe_attempt.effect_key.logical_attempt = 9_007_199_254_740_992;
    assert!(matches!(
        ToolInvocation::start(grant, "echo", unsafe_attempt),
        Err(LiveError::InvalidEffectKey)
    ));
    let mut unsafe_index = start(1, 1, 7, 17, None);
    unsafe_index.effect_key.item_index = Some(9_007_199_254_740_992);
    assert!(matches!(
        ToolInvocation::start(grant, "echo", unsafe_index),
        Err(LiveError::InvalidEffectKey)
    ));
}

#[test]
fn peer_typed_error_text_never_enters_receipt() {
    let marker = "SECRET_SENTINEL";
    let frame = frame_new(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
        request_id: 17,
        terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
            code: v1_1::ErrorCode::HandlerError as i32,
            safe_message: marker.into(),
        })),
    }));
    let mut live = started(1, 1, None);
    assert!(matches!(
        live.receive(7, &frame, 201),
        Ok(ReceiveOutcome::TerminalReceived)
    ));
    let (code, text) = live.receipt().unwrap().error().unwrap();
    assert_eq!(code, v1_1::ErrorCode::HandlerError);
    assert_eq!(text, "remote tool error");
    assert!(!text.contains(marker));
    assert!(!format!("{:?}", live.receipt().unwrap().error()).contains(marker));
    assert!(!format!("{}", LiveError::InvalidTerminal).contains(marker));

    let oversized = "x".repeat(4097);
    let frame = frame_new(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
        request_id: 17,
        terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
            code: v1_1::ErrorCode::HandlerError as i32,
            safe_message: oversized,
        })),
    }));
    let mut limited = started(1, 1, None);
    assert_eq!(
        limited.receive(7, &frame, 201).err(),
        Some(LiveError::Limit)
    );
    assert!(limited.receipt().is_none());
}

#[test]
fn control_frames_expire_at_host_monotonic_deadline() {
    let mut credit = started(1, 1, None);
    assert_eq!(credit.deadline_ms(), 500);
    assert_eq!(credit.grant_credit(1, 501).err(), Some(LiveError::PeerLost));
    assert_eq!(credit.phase(), LivePhase::PeerLost);
    assert_eq!(credit.credit(), 0);
    assert!(credit.receipt().is_none());

    let mut cancel = started(1, 1, None);
    assert_eq!(
        cancel.cancel(v1_1::CancelReason::CancelUser, 501).err(),
        Some(LiveError::PeerLost)
    );
    assert_eq!(cancel.phase(), LivePhase::PeerLost);
    assert!(cancel.receipt().is_none());

    let mut draining = started(1, 1, None);
    draining.begin_drain(210);
    assert_eq!(
        draining.grant_credit(1, 211).err(),
        Some(LiveError::PeerLost)
    );
}

#[test]
fn cancel_validates_and_debits_later_progress_but_keeps_first_terminal() {
    let progress = valid_progress(1);
    assert_eq!(progress.body_len, 137);
    let terminal = valid_terminal(1);
    let mut live = started(1, 1, Some(137));
    live.cancel(v1_1::CancelReason::CancelUser, 200).unwrap();
    assert_eq!(live.phase(), LivePhase::CancelRequested);
    assert_eq!(live.credit(), 137);
    assert_eq!(live.next_sequence(), 0);
    assert!(matches!(
        live.receive(7, &progress, 201),
        Ok(ReceiveOutcome::IgnoredCancelledProgress)
    ));
    assert_eq!(live.credit(), 0);
    assert_eq!(live.next_sequence(), 1);
    assert_eq!(live.phase(), LivePhase::CancelRequested);
    assert!(live.receipt().is_none());
    assert!(matches!(
        live.receive(7, &terminal, 202),
        Ok(ReceiveOutcome::TerminalReceived)
    ));
    assert_eq!(live.receipt().unwrap().kind(), TerminalKind::Result);
    assert!(matches!(
        live.receive(7, &progress, 203),
        Ok(ReceiveOutcome::Stale)
    ));

    let mut prior = started(1, 1, Some(256));
    assert!(matches!(
        prior.receive(7, &progress, 199),
        Ok(ReceiveOutcome::Progress(_))
    ));
    assert_eq!(prior.credit(), 119);
    assert_eq!(prior.next_sequence(), 1);
    prior.grant_credit(137, 199).unwrap();
    assert_eq!(prior.credit(), 256);
    prior.cancel(v1_1::CancelReason::CancelUser, 200).unwrap();
    let next = edited_progress_new(|item| item.sequence = 1);
    assert!(matches!(
        prior.receive(7, &next, 201),
        Ok(ReceiveOutcome::IgnoredCancelledProgress)
    ));
    assert_eq!(prior.credit(), 256 - next.body_len as u64);
    assert_eq!(prior.next_sequence(), 2);
    assert!(matches!(
        prior.receive(7, &terminal, 202),
        Ok(ReceiveOutcome::TerminalReceived)
    ));
}

#[test]
fn cancelled_progress_uses_original_credit_and_retains_zero_credit_terminal_in_both_minors() {
    for (core, tool) in [(0, 0), (1, 0), (1, 1)] {
        let progress = valid_progress(core);
        assert_eq!(progress.body_len, 137);
        let mut live = started(core, tool, Some(137));
        live.cancel(v1_1::CancelReason::CancelUser, 200).unwrap();
        assert_eq!(live.grant_credit(1, 201).err(), Some(LiveError::Stale));
        assert!(matches!(
            live.receive(7, &progress, 201),
            Ok(ReceiveOutcome::IgnoredCancelledProgress)
        ));
        assert_eq!(live.credit(), 0);
        assert_eq!(live.next_sequence(), 1);
        assert_eq!(live.phase(), LivePhase::CancelRequested);
        assert!(live.receipt().is_none());
        assert!(matches!(
            live.receive(7, &valid_terminal(core), 202),
            Ok(ReceiveOutcome::TerminalReceived)
        ));
        assert_eq!(live.phase(), LivePhase::TerminalReceived);
        assert_eq!(live.receipt().unwrap().kind(), TerminalKind::Result);
        assert_eq!(live.credit(), 0);
    }
}

fn typed_error_frame(core: u32) -> ActiveFrame {
    if core == 0 {
        frame_old(v1_0::envelope::Frame::StreamEnd(v1_0::StreamEnd {
            request_id: 17,
            terminal: Some(v1_0::stream_end::Terminal::Error(v1_0::TypedError {
                code: v1_0::ErrorCode::HandlerError as i32,
                safe_message: "untrusted peer text".into(),
            })),
        }))
    } else {
        frame_new(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
            request_id: 17,
            terminal: Some(v1_1::stream_end::Terminal::Error(v1_1::TypedError {
                code: v1_1::ErrorCode::HandlerError as i32,
                safe_message: "untrusted peer text".into(),
            })),
        }))
    }
}

#[test]
fn final_clock_after_domain_blocks_progress_and_result_at_deadline() {
    use std::cell::{Cell, RefCell};

    for (core, tool) in [(0, 0), (1, 1)] {
        for kind in [DomainKind::Progress, DomainKind::Result] {
            let frame = match kind {
                DomainKind::Progress => valid_progress(core),
                DomainKind::Result => valid_terminal(core),
            };
            let credit = (kind == DomainKind::Progress).then_some(frame.body_len as u64);
            let mut live = started(core, tool, credit);
            let clock = Cell::new(499);
            let order = RefCell::new(Vec::new());
            let result = live.receive_checked_with_final_time(
                7,
                &frame,
                499,
                None,
                |check| {
                    assert_eq!(check.kind(), kind);
                    order.borrow_mut().push("domain");
                    clock.set(500);
                    DomainDecision::Accept
                },
                || {
                    order.borrow_mut().push("clock");
                    clock.get()
                },
            );
            assert_eq!(result.err(), Some(LiveError::PeerLost));
            assert_eq!(*order.borrow(), ["domain", "clock"]);
            assert_eq!(live.phase(), LivePhase::PeerLost);
            assert_eq!(live.credit(), credit.unwrap_or(0));
            assert_eq!(live.next_sequence(), 0);
            assert!(live.receipt().is_none());

            let mut accepted = started(core, tool, credit);
            let result = accepted.receive_checked_with_final_time(
                7,
                &frame,
                499,
                None,
                |_| DomainDecision::Accept,
                || 499,
            );
            match kind {
                DomainKind::Progress => {
                    assert!(matches!(result, Ok(ReceiveOutcome::Progress(_))));
                    assert_eq!(accepted.credit(), 0);
                    assert_eq!(accepted.next_sequence(), 1);
                }
                DomainKind::Result => {
                    assert!(matches!(result, Ok(ReceiveOutcome::TerminalReceived)));
                    assert_eq!(accepted.receipt().unwrap().received_ms(), 499);
                }
            }
        }
    }
}

#[test]
fn typed_peer_error_uses_final_clock_without_domain_callback() {
    use std::cell::Cell;

    for (core, tool) in [(0, 0), (1, 1)] {
        let frame = typed_error_frame(core);
        let calls = Cell::new(0);
        let mut expired = started(core, tool, None);
        assert_eq!(
            expired
                .receive_checked_with_final_time(
                    7,
                    &frame,
                    499,
                    None,
                    |_| {
                        calls.set(calls.get() + 1);
                        DomainDecision::Accept
                    },
                    || 500,
                )
                .err(),
            Some(LiveError::PeerLost)
        );
        assert_eq!(calls.get(), 0);
        assert_eq!(expired.phase(), LivePhase::PeerLost);
        assert!(expired.receipt().is_none());

        let mut accepted = started(core, tool, None);
        assert!(matches!(
            accepted.receive_checked_with_final_time(
                7,
                &frame,
                499,
                None,
                |_| panic!("typed error needs no domain construction"),
                || 499,
            ),
            Ok(ReceiveOutcome::TerminalReceived)
        ));
        assert_eq!(accepted.receipt().unwrap().received_ms(), 499);
        assert_eq!(
            accepted.receipt().unwrap().error(),
            Some((v1_1::ErrorCode::HandlerError, "remote tool error"))
        );
        assert!(matches!(
            accepted.receive_checked_with_final_time(
                7,
                &frame,
                500,
                None,
                |_| panic!("stale terminal needs no domain construction"),
                || panic!("stale terminal needs no final clock"),
            ),
            Ok(ReceiveOutcome::Stale)
        ));
        assert_eq!(accepted.receipt().unwrap().received_ms(), 499);
    }
}

#[test]
fn cancellation_grace_final_cutoff_precedes_credit_or_terminal_mutation() {
    for (core, tool) in [(0, 0), (1, 1)] {
        let progress = valid_progress(core);
        let credit = progress.body_len as u64;
        let mut accepted = started(core, tool, Some(credit));
        accepted
            .cancel(v1_1::CancelReason::CancelUser, 200)
            .unwrap();
        assert!(matches!(
            accepted.receive_checked_with_final_time(
                7,
                &progress,
                249,
                Some(250),
                |_| DomainDecision::Accept,
                || 249,
            ),
            Ok(ReceiveOutcome::IgnoredCancelledProgress)
        ));
        assert_eq!(accepted.credit(), 0);
        assert_eq!(accepted.next_sequence(), 1);
        assert_eq!(accepted.phase(), LivePhase::CancelRequested);

        let mut expired = started(core, tool, Some(credit));
        expired.cancel(v1_1::CancelReason::CancelUser, 200).unwrap();
        assert_eq!(
            expired
                .receive_checked_with_final_time(
                    7,
                    &progress,
                    249,
                    Some(250),
                    |_| DomainDecision::Accept,
                    || 250,
                )
                .err(),
            Some(LiveError::PeerLost)
        );
        assert_eq!(expired.phase(), LivePhase::PeerLost);
        assert_eq!(expired.credit(), credit);
        assert_eq!(expired.next_sequence(), 0);

        let mut draining = started(core, tool, Some(credit));
        draining.begin_drain(250);
        assert_eq!(
            draining
                .receive_checked_with_final_time(
                    7,
                    &progress,
                    249,
                    None,
                    |_| DomainDecision::Accept,
                    || 250,
                )
                .err(),
            Some(LiveError::PeerLost)
        );
        assert_eq!(draining.credit(), credit);
        assert_eq!(draining.next_sequence(), 0);

        let mut ingress_expired = started(core, tool, Some(credit));
        ingress_expired
            .cancel(v1_1::CancelReason::CancelUser, 200)
            .unwrap();
        assert_eq!(
            ingress_expired
                .receive_checked_with_final_time(
                    7,
                    &progress,
                    250,
                    Some(250),
                    |_| panic!("expired input must skip domain"),
                    || panic!("expired input must skip final clock"),
                )
                .err(),
            Some(LiveError::PeerLost)
        );
        assert_eq!(ingress_expired.credit(), credit);
        assert_eq!(ingress_expired.next_sequence(), 0);

        let terminal = valid_terminal(core);
        let mut terminal_accepted = started(core, tool, None);
        terminal_accepted
            .cancel(v1_1::CancelReason::CancelUser, 200)
            .unwrap();
        assert!(matches!(
            terminal_accepted.receive_checked_with_final_time(
                7,
                &terminal,
                249,
                Some(250),
                |_| DomainDecision::Accept,
                || 249,
            ),
            Ok(ReceiveOutcome::TerminalReceived)
        ));
        assert_eq!(terminal_accepted.receipt().unwrap().received_ms(), 249);
        assert!(!terminal_accepted.expire(250));
        let mut terminal_expired = started(core, tool, None);
        terminal_expired
            .cancel(v1_1::CancelReason::CancelUser, 200)
            .unwrap();
        assert_eq!(
            terminal_expired
                .receive_checked_with_final_time(
                    7,
                    &terminal,
                    249,
                    Some(250),
                    |_| DomainDecision::Accept,
                    || 250,
                )
                .err(),
            Some(LiveError::PeerLost)
        );
        assert!(terminal_expired.receipt().is_none());
    }
}

#[test]
fn final_clock_skips_rejected_input_and_preserves_domain_error_classes() {
    for (core, tool) in [(0, 0), (1, 1)] {
        let progress = valid_progress(core);
        let credit = progress.body_len as u64;
        let mut wrong_generation = started(core, tool, Some(credit));
        assert_eq!(
            wrong_generation
                .receive_checked_with_final_time(
                    8,
                    &progress,
                    499,
                    None,
                    |_| panic!("wrong generation must skip domain"),
                    || panic!("wrong generation must skip final clock"),
                )
                .err(),
            Some(LiveError::WrongGeneration)
        );
        let mut no_credit = started(core, tool, None);
        assert_eq!(
            no_credit
                .receive_checked_with_final_time(
                    7,
                    &progress,
                    499,
                    None,
                    |_| panic!("credit failure must skip domain"),
                    || panic!("credit failure must skip final clock"),
                )
                .err(),
            Some(LiveError::CreditViolation)
        );
        let wrong_request_frame = if core == 0 {
            edited_progress_old(|item| item.request_id = 18)
        } else {
            edited_progress_new(|item| item.request_id = 18)
        };
        let mut wrong_request = started(core, tool, Some(credit));
        assert_eq!(
            wrong_request
                .receive_checked_with_final_time(
                    7,
                    &wrong_request_frame,
                    499,
                    None,
                    |_| panic!("wrong request must skip domain"),
                    || panic!("wrong request must skip final clock"),
                )
                .err(),
            Some(LiveError::WrongRequestId)
        );
        let gap_frame = if core == 0 {
            edited_progress_old(|item| item.sequence = 1)
        } else {
            edited_progress_new(|item| item.sequence = 1)
        };
        let mut gap = started(core, tool, Some(credit));
        assert_eq!(
            gap.receive_checked_with_final_time(
                7,
                &gap_frame,
                499,
                None,
                |_| panic!("sequence gap must skip domain"),
                || panic!("sequence gap must skip final clock"),
            )
            .err(),
            Some(LiveError::InvalidSequence)
        );
        let wrong_arm = if core == 0 {
            frame_old(v1_0::envelope::Frame::Credit(v1_0::Credit {
                request_id: 17,
                additional_bytes: 1,
            }))
        } else {
            frame_new(v1_1::envelope::Frame::Credit(v1_1::Credit {
                request_id: 17,
                additional_bytes: 1,
            }))
        };
        let mut invalid_arm = started(core, tool, Some(credit));
        assert_eq!(
            invalid_arm
                .receive_checked_with_final_time(
                    7,
                    &wrong_arm,
                    499,
                    None,
                    |_| panic!("wrong arm must skip domain"),
                    || panic!("wrong arm must skip final clock"),
                )
                .err(),
            Some(LiveError::InvalidTerminal)
        );
        let invalid_schema_frame = if core == 0 {
            edited_progress_old(|item| {
                let mut payload = v1_0::ToolProgress::decode(item.payload.as_slice()).unwrap();
                payload.event.as_mut().unwrap().projection_id = "tool.result:echo".into();
                item.payload = payload.encode_to_vec();
            })
        } else {
            edited_progress_new(|item| {
                let mut payload = v1_1::ToolProgress::decode(item.payload.as_slice()).unwrap();
                payload.event.as_mut().unwrap().projection_id = "tool.result:echo".into();
                item.payload = payload.encode_to_vec();
            })
        };
        let mut invalid_schema = started(core, tool, Some(credit));
        assert!(matches!(
            invalid_schema.receive_checked_with_final_time(
                7,
                &invalid_schema_frame,
                499,
                None,
                |_| panic!("invalid schema must skip domain"),
                || panic!("invalid schema must skip final clock"),
            ),
            Err(LiveError::Value(value::Error::ProjectionMismatch))
        ));
        let mut host_fault = started(core, tool, Some(credit));
        assert_eq!(
            host_fault
                .receive_checked_with_final_time(
                    7,
                    &progress,
                    499,
                    None,
                    |_| DomainDecision::HostFault,
                    || panic!("host fault must skip final clock"),
                )
                .err(),
            Some(LiveError::HostDomainFault)
        );
        assert_eq!(host_fault.phase(), LivePhase::Open);
        assert_eq!(host_fault.credit(), credit);
        assert_eq!(host_fault.next_sequence(), 0);
        let mut peer_mismatch = started(core, tool, Some(credit));
        assert_eq!(
            peer_mismatch
                .receive_checked_with_final_time(
                    7,
                    &progress,
                    499,
                    None,
                    |_| DomainDecision::PeerValueMismatch,
                    || panic!("peer mismatch must skip final clock"),
                )
                .err(),
            Some(LiveError::InvalidProgress)
        );
        assert_eq!(peer_mismatch.phase(), LivePhase::PeerLost);
        assert_eq!(peer_mismatch.credit(), credit);
        assert_eq!(peer_mismatch.next_sequence(), 0);
    }
}

#[test]
fn cancelled_progress_rejects_overcredit_wrong_request_gap_and_duplicate() {
    for (core, tool) in [(0, 0), (1, 1)] {
        let frame = valid_progress(core);
        assert_cancelled_malformed(core, tool, 136, &frame, LiveError::CreditViolation);
        let wrong_request = if core == 0 {
            edited_progress_old(|item| item.request_id = 18)
        } else {
            edited_progress_new(|item| item.request_id = 18)
        };
        assert_cancelled_malformed(core, tool, 256, &wrong_request, LiveError::WrongRequestId);
        let gap = if core == 0 {
            edited_progress_old(|item| item.sequence = 1)
        } else {
            edited_progress_new(|item| item.sequence = 1)
        };
        assert_cancelled_malformed(core, tool, 256, &gap, LiveError::InvalidSequence);

        let mut duplicate = started(core, tool, Some(256));
        duplicate
            .cancel(v1_1::CancelReason::CancelUser, 200)
            .unwrap();
        assert!(matches!(
            duplicate.receive(7, &frame, 201),
            Ok(ReceiveOutcome::IgnoredCancelledProgress)
        ));
        assert_eq!(duplicate.credit(), 119);
        assert_eq!(duplicate.next_sequence(), 1);
        assert_eq!(
            duplicate.receive(7, &frame, 202).err(),
            Some(LiveError::InvalidSequence)
        );
        assert_eq!(duplicate.phase(), LivePhase::PeerLost);
        assert!(duplicate.receipt().is_none());
    }
}

#[test]
fn cancelled_progress_checks_selected_projection_instance_and_minor() {
    let old_missing_details = active_frame(&legacy_invalid_frame("progress"), 0);
    assert_cancelled_malformed(
        0,
        0,
        256,
        &old_missing_details,
        LiveError::Value(value::Error::MissingValue),
    );
    let missing_details = edited_progress_new(|item| {
        let mut progress = v1_1::ToolProgress::decode(item.payload.as_slice()).unwrap();
        let value = progress.event.as_mut().unwrap().value.as_mut().unwrap();
        let Some(v1_1::wire_value::Value::MapValue(map)) = value.value.as_mut() else {
            unreachable!()
        };
        map.entries
            .iter_mut()
            .find(|entry| entry.key == "details")
            .unwrap()
            .value = None;
        item.payload = progress.encode_to_vec();
    });
    assert_eq!(missing_details.body_len, 132);
    assert_cancelled_malformed(
        1,
        1,
        256,
        &missing_details,
        LiveError::Value(value::Error::MissingValue),
    );

    let old_wrong_projection = edited_progress_old(|item| {
        let mut progress = v1_0::ToolProgress::decode(item.payload.as_slice()).unwrap();
        progress.event.as_mut().unwrap().projection_id = "tool.result:echo".into();
        item.payload = progress.encode_to_vec();
    });
    assert_cancelled_malformed(
        0,
        0,
        256,
        &old_wrong_projection,
        LiveError::Value(value::Error::ProjectionMismatch),
    );

    let wrong_projection = edited_progress_new(|item| {
        let mut progress = v1_1::ToolProgress::decode(item.payload.as_slice()).unwrap();
        progress.event.as_mut().unwrap().projection_id = "tool.result:echo".into();
        item.payload = progress.encode_to_vec();
    });
    assert_cancelled_malformed(
        1,
        1,
        256,
        &wrong_projection,
        LiveError::Value(value::Error::ProjectionMismatch),
    );

    let wrong_instance = edited_progress_new(|item| {
        let mut progress = v1_1::ToolProgress::decode(item.payload.as_slice()).unwrap();
        let value = progress.event.as_mut().unwrap().value.as_mut().unwrap();
        let Some(v1_1::wire_value::Value::MapValue(map)) = value.value.as_mut() else {
            unreachable!()
        };
        let details = map
            .entries
            .iter_mut()
            .find(|entry| entry.key == "details")
            .unwrap();
        details.value.as_mut().unwrap().value =
            Some(v1_1::wire_value::Value::TextValue("bad".into()));
        item.payload = progress.encode_to_vec();
    });
    let mut live = started(1, 1, Some(256));
    live.cancel(v1_1::CancelReason::CancelUser, 200).unwrap();
    assert!(matches!(
        live.receive(7, &wrong_instance, 201),
        Err(LiveError::Value(value::Error::Schema(_)))
    ));
    assert_eq!(live.phase(), LivePhase::PeerLost);
    assert_eq!(live.credit(), 256);
    assert_eq!(live.next_sequence(), 0);
    assert!(live.receipt().is_none());

    let future_field = edited_progress_new(|item| {
        let mut progress = v1_1::ToolProgress::decode(item.payload.as_slice()).unwrap();
        progress.percent_complete = Some(0);
        item.payload = progress.encode_to_vec();
    });
    assert_eq!(future_field.body_len, 139);
    assert_cancelled_malformed(1, 0, 256, &future_field, LiveError::UnsupportedVariant);
}

#[test]
fn cancelled_progress_preserves_generation_and_exact_deadline_precedence() {
    for (core, tool) in [(0, 0), (1, 1)] {
        let progress = valid_progress(core);
        let mut live = started(core, tool, Some(137));
        live.cancel(v1_1::CancelReason::CancelUser, 200).unwrap();
        assert_eq!(
            live.receive(8, &progress, 500).err(),
            Some(LiveError::WrongGeneration)
        );
        assert_eq!(live.phase(), LivePhase::CancelRequested);
        assert_eq!(live.credit(), 137);
        assert_eq!(live.next_sequence(), 0);
        assert!(live.receipt().is_none());
        assert_eq!(
            live.receive(7, &progress, 500).err(),
            Some(LiveError::PeerLost)
        );
        assert_eq!(live.phase(), LivePhase::PeerLost);
        assert_eq!(live.credit(), 137);
        assert_eq!(live.next_sequence(), 0);
        assert!(live.receipt().is_none());
    }
}

#[test]
fn cancel_blocks_new_credit_without_changing_window() {
    let mut live = started(1, 1, None);
    live.cancel(v1_1::CancelReason::CancelUser, 200).unwrap();
    assert_eq!(live.grant_credit(137, 201).err(), Some(LiveError::Stale));
    assert_eq!(live.credit(), 0);
    assert_eq!(live.phase(), LivePhase::CancelRequested);
    let mut overdue = started(1, 1, None);
    overdue.cancel(v1_1::CancelReason::CancelUser, 200).unwrap();
    assert_eq!(
        overdue.grant_credit(1, 501).err(),
        Some(LiveError::PeerLost)
    );
    assert_eq!(overdue.phase(), LivePhase::PeerLost);
}
