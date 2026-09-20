use std::num::NonZeroUsize;

use pi_extension_protocol::activation::{
    AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs,
};
use pi_extension_protocol::endpoint::{
    Endpoint, EndpointError, EndpointEvent, EndpointPhase, SelectedEnvelope,
};
use pi_extension_protocol::jcs::Limits as JsonLimits;
use pi_extension_protocol::wire::framing::{self, FrameError, FrameReader};
use pi_extension_protocol::wire::{v1_0, v1_1};
use prost::Message;
use serde_json::Value;

const CATALOG: &str = include_str!("../schema/fixtures/host-catalog.json");
const VECTORS: &str = include_str!("../schema/fixtures/activation-vectors.json");
const RAW: &str = include_str!("../schema/fixtures/raw-vectors.json");

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

fn generated_outer(source: &str) -> &str {
    source
        .split_once("pub struct Envelope {")
        .unwrap()
        .1
        .split_once("pub enum MethodDirection")
        .unwrap()
        .0
}

fn unhex(hex: &str) -> Vec<u8> {
    let (pairs, tail) = hex.as_bytes().as_chunks::<2>();
    assert!(tail.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn raw_request() -> Vec<u8> {
    let raw: Value = serde_json::from_str(RAW).unwrap();
    unhex(raw["request"]["frame_hex"].as_str().unwrap())
}

fn limits(pre: u64, post: u64) -> v1_1::ProtocolLimits {
    v1_1::ProtocolLimits {
        pre_handshake_frame_bytes: pre,
        frame_bytes: post,
        aggregate_decoded_bytes: post,
        value_depth: 32,
        value_elements: 1024,
        string_bytes: 512,
        bytes_length: 512,
        in_flight_requests: 8,
        blocking_queue_bytes: 4096,
        progress_queue_bytes: 4096,
        passive_queue_bytes: 4096,
        telemetry_queue_bytes: 4096,
        nesting_depth: 8,
        requests_per_second: 32,
        handshake_ms: 100,
        cancellation_grace_ms: 10,
        max_stream_credit_bytes: 4096,
    }
}

fn host(pre: u64, post: u64, core_minor: u32, with_tool: bool) -> HostAuthority {
    let tools = if with_tool {
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
        vec![ApprovedTool::new("peer-a".into(), descriptor, inputs, JsonLimits::default()).unwrap()]
    } else {
        vec![]
    };
    HostAuthority::new(
        "peer-a".into(),
        7,
        core_minor,
        1,
        limits(pre, post),
        tools,
        vec![],
    )
    .unwrap()
}

fn hello(core_minor: u32) -> v1_1::Hello {
    v1_1::Hello {
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
    }
}

fn minimal_manifest(core_minor: u32) -> v1_1::Manifest {
    v1_1::Manifest {
        declared_extension_id: "metadata".into(),
        extension_version: "0.1".into(),
        protocol: Some(v1_1::VersionRange {
            major: 1,
            min_minor: 0,
            max_minor: core_minor,
        }),
        capabilities: vec![],
        permissions: vec![],
        subscriptions: vec![],
    }
}

fn full_manifest() -> v1_1::Manifest {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let bytes = unhex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    let manifest = v1_1::Manifest::decode(bytes.as_slice()).unwrap();
    assert_eq!(manifest.encode_to_vec(), bytes);
    manifest
}

fn framed_new(frame: v1_1::envelope::Frame) -> Vec<u8> {
    framing::encode(&v1_1::Envelope { frame: Some(frame) }, nz(8192))
        .unwrap()
        .bytes
}

fn framed_old(frame: v1_0::envelope::Frame) -> Vec<u8> {
    framing::encode(&v1_0::Envelope { frame: Some(frame) }, nz(8192))
        .unwrap()
        .bytes
}

fn decode_sent_old(bytes: &[u8]) -> v1_0::Envelope {
    let mut reader = FrameReader::<v1_0::Envelope>::new(nz(8192));
    let step = reader.read_one(bytes).unwrap();
    assert_eq!(step.consumed, bytes.len());
    step.frame.unwrap().envelope
}

fn decode_sent_new(bytes: &[u8]) -> v1_1::Envelope {
    let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
    let step = reader.read_one(bytes).unwrap();
    assert_eq!(step.consumed, bytes.len());
    step.frame.unwrap().envelope
}

fn sent(step: pi_extension_protocol::endpoint::ReadStep) -> Vec<u8> {
    match step.event.unwrap() {
        EndpointEvent::Send(frame) => frame.bytes,
        other => panic!("expected outbound frame: {other:?}"),
    }
}

fn activate_empty(endpoint: &mut Endpoint, core_minor: u32) {
    let hello = framed_new(v1_1::envelope::Frame::Hello(hello(core_minor)));
    let _ = sent(endpoint.read_one(&hello, 101).unwrap());
    let manifest = framed_new(v1_1::envelope::Frame::Manifest(minimal_manifest(
        core_minor,
    )));
    let _ = sent(endpoint.read_one(&manifest, 102).unwrap());
    assert_eq!(endpoint.phase(), EndpointPhase::AwaitReady);
    assert!(endpoint.active_grant().is_none());
}

#[test]
fn canonical_outer_envelope_shape_is_identical_across_core_minors() {
    let old = include_str!("../schema/v1_0/common.proto");
    let new = include_str!("../schema/v1_1/common.proto");
    let old_outer = old.split_once("message Envelope {").unwrap().1;
    let new_outer = new.split_once("message Envelope {").unwrap().1;
    assert_eq!(old_outer, new_outer);
    // The checked-in generated DTOs differ only in nested Manifest display_label
    // and ToolProgress percent_complete; active core Envelope arms are common.
    let old_generated = include_str!("../src/wire/generated/v1_0.rs");
    let new_generated = include_str!("../src/wire/generated/v1_1.rs");
    assert_eq!(
        generated_outer(old_generated),
        generated_outer(new_generated)
    );
}

#[test]
fn full_raw_handshake_emits_selected_dtos_and_host_identity() {
    for core_minor in [0, 1] {
        let mut endpoint = Endpoint::new(host(4096, 8192, core_minor, true), 100);
        let hello_frame = if core_minor == 0 {
            let old = v1_0::Hello::decode(hello(0).encode_to_vec().as_slice()).unwrap();
            framed_old(v1_0::envelope::Frame::Hello(old))
        } else {
            framed_new(v1_1::envelope::Frame::Hello(hello(1)))
        };
        let manifest_frame = if core_minor == 0 {
            let old = v1_0::Manifest::decode(full_manifest().encode_to_vec().as_slice()).unwrap();
            framed_old(v1_0::envelope::Frame::Manifest(old))
        } else {
            framed_new(v1_1::envelope::Frame::Manifest(full_manifest()))
        };
        let joined = [hello_frame.clone(), manifest_frame.clone()].concat();
        let first = endpoint.read_one(&joined, 101).unwrap();
        assert_eq!(first.consumed, hello_frame.len());
        let ack = sent(first);
        if core_minor == 0 {
            let Some(v1_0::envelope::Frame::HelloAck(ack)) = decode_sent_old(&ack).frame else {
                panic!("1.0 HelloAck");
            };
            assert_eq!(ack.selected_protocol.unwrap().minor, 0);
        } else {
            let Some(v1_1::envelope::Frame::HelloAck(ack)) = decode_sent_new(&ack).frame else {
                panic!("1.1 HelloAck");
            };
            assert_eq!(ack.selected_protocol.unwrap().minor, 1);
        }
        assert!(endpoint.active_grant().is_none());
        let second = endpoint
            .read_one(&joined[hello_frame.len()..], 102)
            .unwrap();
        assert_eq!(second.consumed, manifest_frame.len());
        let grant = sent(second);
        if core_minor == 0 {
            let Some(v1_0::envelope::Frame::Grant(grant)) = decode_sent_old(&grant).frame else {
                panic!("1.0 Grant");
            };
            assert_eq!(grant.selected_protocol.unwrap().minor, 0);
            assert_eq!(grant.authenticated_extension_id, "peer-a");
            assert_eq!(grant.connection_generation, 7);
            assert_eq!(
                grant.selected_capabilities[0]
                    .profile
                    .as_ref()
                    .unwrap()
                    .minor,
                1
            );
        } else {
            let Some(v1_1::envelope::Frame::Grant(grant)) = decode_sent_new(&grant).frame else {
                panic!("1.1 Grant");
            };
            assert_eq!(grant.selected_protocol.unwrap().minor, 1);
            assert_eq!(grant.authenticated_extension_id, "peer-a");
            assert_eq!(grant.connection_generation, 7);
            assert_eq!(
                grant.selected_capabilities[0]
                    .profile
                    .as_ref()
                    .unwrap()
                    .minor,
                1
            );
        }
        assert_eq!(endpoint.phase(), EndpointPhase::AwaitReady);
        assert!(endpoint.active_grant().is_none());
        let ready = if core_minor == 0 {
            include_bytes!("../schema/fixtures/ready-1.0.frame.bin").to_vec()
        } else {
            framed_new(v1_1::envelope::Frame::Ready(v1_1::Ready {}))
        };
        let step = endpoint.read_one(&ready, 103).unwrap();
        assert_eq!(step.consumed, ready.len());
        assert!(matches!(step.event, Some(EndpointEvent::Ready)));
        assert_eq!(endpoint.phase(), EndpointPhase::Active);
        let active = endpoint.active_grant().unwrap();
        assert_eq!(active.wire().authenticated_extension_id, "peer-a");
        assert_eq!(active.wire().connection_generation, 7);
        assert_eq!(active.tools().len(), 1);
        assert_eq!(
            active.tools()[0]
                .reference()
                .profile
                .as_ref()
                .unwrap()
                .minor,
            1
        );
        let request = raw_request();
        let step = endpoint.read_one(&request, 104).unwrap();
        assert_eq!(step.consumed, request.len());
        let Some(EndpointEvent::Active(frame)) = step.event else {
            panic!("active frame");
        };
        assert_eq!(frame.body_len, 219);
        match (core_minor, frame.envelope) {
            (
                0,
                SelectedEnvelope::V1_0(v1_0::Envelope {
                    frame: Some(v1_0::envelope::Frame::Request(_)),
                }),
            ) => {}
            (
                1,
                SelectedEnvelope::V1_1(v1_1::Envelope {
                    frame: Some(v1_1::envelope::Frame::Request(_)),
                }),
            ) => {}
            other => panic!("wrong selected DTO: {other:?}"),
        }
    }
}

#[test]
fn coalesced_ready_and_larger_request_switches_cap_at_boundary() {
    let request = raw_request();
    assert_eq!(&request[..2], [0xdb, 1]); // 219-byte Envelope body.
    let mut endpoint = Endpoint::new(host(120, 300, 1, false), 100);
    activate_empty(&mut endpoint, 1);
    let ready = framed_new(v1_1::envelope::Frame::Ready(v1_1::Ready {}));
    let joined = [ready.clone(), request.clone()].concat();
    let first = endpoint.read_one(&joined, 103).unwrap();
    assert_eq!(first.consumed, ready.len());
    assert!(matches!(first.event, Some(EndpointEvent::Ready)));
    let second = endpoint.read_one(&joined[first.consumed..], 104).unwrap();
    assert_eq!(second.consumed, request.len());
    let Some(EndpointEvent::Active(frame)) = second.event else {
        panic!("active request");
    };
    assert_eq!(frame.body_len, 219);
}

#[test]
fn wrong_order_and_malformed_envelopes_close_without_active_grant() {
    let ready = framed_new(v1_1::envelope::Frame::Ready(v1_1::Ready {}));
    let mut early = Endpoint::new(host(120, 300, 1, false), 100);
    assert_eq!(
        early.read_one(&ready, 101).unwrap_err(),
        EndpointError::UnexpectedFrame
    );
    assert_eq!(early.phase(), EndpointPhase::Closed);
    assert!(early.active_grant().is_none());
    assert_eq!(
        early.read_one(&ready, 102).unwrap_err(),
        EndpointError::Closed
    );

    let mut before_ready = Endpoint::new(host(120, 300, 1, false), 100);
    activate_empty(&mut before_ready, 1);
    let request = framed_new(v1_1::envelope::Frame::Request(v1_1::Request::default()));
    assert_eq!(
        before_ready.read_one(&request, 103).unwrap_err(),
        EndpointError::UnexpectedFrame
    );
    assert!(before_ready.active_grant().is_none());

    let mut malformed = Endpoint::new(host(120, 300, 1, false), 100);
    assert_eq!(
        malformed.read_one(&[2, 0x7a, 0], 101).unwrap_err(),
        EndpointError::Frame(FrameError::InvalidEnvelope)
    );
    assert_eq!(malformed.phase(), EndpointPhase::Closed);
    assert_eq!(
        malformed.read_one(&ready, 102).unwrap_err(),
        EndpointError::Closed
    );

    let mut noncanonical = Endpoint::new(host(120, 300, 1, false), 100);
    assert_eq!(
        noncanonical.read_one(&[0x80, 0], 101).unwrap_err(),
        EndpointError::Frame(FrameError::NonCanonicalLength)
    );
    assert_eq!(noncanonical.phase(), EndpointPhase::Closed);
}

#[test]
fn timeout_oversize_and_outbound_preflight_are_fatal() {
    let mut timeout = Endpoint::new(host(120, 300, 1, false), 100);
    assert_eq!(
        timeout.read_one(&[], 201).unwrap_err(),
        EndpointError::HandshakeTimeout
    );
    assert_eq!(timeout.phase(), EndpointPhase::Closed);
    let mut slow = Endpoint::new(host(120, 300, 1, false), 100);
    assert_eq!(slow.read_one(&[0x80], 101).unwrap().consumed, 1);
    assert_eq!(
        slow.read_one(&[], 201).unwrap_err(),
        EndpointError::HandshakeTimeout
    );

    let mut oversized = Endpoint::new(host(120, 300, 1, false), 100);
    assert_eq!(
        oversized.read_one(&[121], 101).unwrap_err(),
        EndpointError::Frame(FrameError::FrameTooLarge {
            length: 121,
            limit: 120
        })
    );
    assert_eq!(oversized.phase(), EndpointPhase::Closed);

    let mut outbound = Endpoint::new(host(30, 300, 1, false), 100);
    let hello_frame = framed_new(v1_1::envelope::Frame::Hello(hello(1)));
    let _ = sent(outbound.read_one(&hello_frame, 101).unwrap());
    let manifest = framed_new(v1_1::envelope::Frame::Manifest(minimal_manifest(1)));
    assert!(manifest.len() < 30);
    assert!(matches!(
        outbound.read_one(&manifest, 102),
        Err(EndpointError::Frame(FrameError::FrameTooLarge {
            limit: 30,
            ..
        }))
    ));
    assert_eq!(outbound.phase(), EndpointPhase::Closed);
}

#[test]
fn cap_minus_one_and_eof_poison_the_connection() {
    let request = raw_request();
    let mut too_small = Endpoint::new(host(120, 218, 1, false), 100);
    activate_empty(&mut too_small, 1);
    let ready = framed_new(v1_1::envelope::Frame::Ready(v1_1::Ready {}));
    too_small.read_one(&ready, 103).unwrap();
    assert_eq!(
        too_small.read_one(&request[..2], 104).unwrap_err(),
        EndpointError::Frame(FrameError::FrameTooLarge {
            length: 219,
            limit: 218
        })
    );
    assert_eq!(too_small.phase(), EndpointPhase::Closed);
    assert!(too_small.active_grant().is_none());

    let mut partial = Endpoint::new(host(120, 300, 1, false), 100);
    activate_empty(&mut partial, 1);
    partial.read_one(&ready, 103).unwrap();
    let step = partial.read_one(&request[..30], 104).unwrap();
    assert_eq!(step.consumed, 30);
    assert!(step.event.is_none());
    assert_eq!(
        partial.finish().unwrap_err(),
        EndpointError::Frame(FrameError::TruncatedBody {
            remaining: 219 - 28
        })
    );
    assert_eq!(partial.phase(), EndpointPhase::Closed);
    assert_eq!(
        partial.read_one(&request[30..], 105).unwrap_err(),
        EndpointError::Closed
    );
}

#[test]
fn complete_boundary_eof_closes_and_prevents_new_work() {
    let mut endpoint = Endpoint::new(host(120, 300, 1, false), 100);
    activate_empty(&mut endpoint, 1);
    let ready = framed_new(v1_1::envelope::Frame::Ready(v1_1::Ready {}));
    endpoint.read_one(&ready, 103).unwrap();
    assert!(endpoint.active_grant().is_some());
    endpoint.finish().unwrap();
    assert_eq!(endpoint.phase(), EndpointPhase::Closed);
    assert!(endpoint.active_grant().is_none());
    assert_eq!(
        endpoint.read_one(&raw_request(), 104).unwrap_err(),
        EndpointError::Closed
    );
    assert_eq!(endpoint.finish().unwrap_err(), EndpointError::Closed);
}
