//! Wire vectors exercise generated DTOs; semantic admission belongs to later layers.

use pi_extension_protocol::wire::{v1_0, v1_1};
use prost::Message;
use ring::digest::{SHA256, digest};
use serde_json::Value;

fn fixture() -> Value {
    serde_json::from_str(include_str!("../schema/fixtures/raw-vectors.json")).unwrap()
}

fn manifest() -> Value {
    serde_json::from_str(include_str!("../schema/fixtures/manifest.json")).unwrap()
}

fn unhex(value: &str) -> Vec<u8> {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn sha256_hex(bytes: &[u8]) -> String {
    digest(&SHA256, bytes)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn checked_envelope_bodies_round_trip_and_decode_inner_tool_dtos() {
    let vectors = fixture();
    for kind in ["request", "progress", "terminal"] {
        let vector = &vectors[kind];
        let body = unhex(vector["body_hex"].as_str().unwrap());
        assert_eq!(body.len(), vector["body_bytes"].as_u64().unwrap() as usize);
        let decoded = v1_0::Envelope::decode(body.as_slice()).unwrap();
        assert_eq!(decoded.encode_to_vec(), body, "{kind} body changed");

        match (kind, decoded.frame.unwrap()) {
            ("request", v1_0::envelope::Frame::Request(request)) => {
                assert_eq!(request.request_id, 17);
                assert_eq!(request.method, "tool.invoke");
                assert_eq!(request.effect_key.as_ref().unwrap().item_index, Some(0));
                let inner = v1_0::InvokeToolRequest::decode(request.payload.as_slice()).unwrap();
                assert_eq!(inner.tool_id, "echo");
                assert_eq!(inner.tool_call_id, "call-1");
                assert_ne!(inner.tool_call_id, request.request_id.to_string());
                assert_ne!(
                    inner.tool_call_id,
                    request.effect_key.unwrap().item_index.unwrap().to_string()
                );
                assert_eq!(
                    inner.arguments.unwrap().projection_id,
                    "tool.arguments:echo"
                );
            }
            ("progress", v1_0::envelope::Frame::StreamItem(item)) => {
                assert_eq!(item.request_id, 17);
                let inner = v1_0::ToolProgress::decode(item.payload.as_slice()).unwrap();
                assert_eq!(inner.event.unwrap().projection_id, "tool.progress:echo");
            }
            ("terminal", v1_0::envelope::Frame::StreamEnd(end)) => {
                assert_eq!(end.request_id, 17);
                let v1_0::stream_end::Terminal::Result(payload) = end.terminal.unwrap() else {
                    panic!("expected complete result")
                };
                let inner = v1_0::InvokeToolResult::decode(payload.as_slice()).unwrap();
                assert_eq!(inner.raw_result.unwrap().projection_id, "tool.result:echo");
            }
            _ => panic!("unexpected {kind} envelope role"),
        }
    }
}

#[test]
fn selected_minor_preserves_optional_zero_and_distinguishes_absence() {
    let vectors = fixture();
    let present = &vectors["selected_minor_1_0_forbids_1_1_tool_progress"]["enveloped_progress"];
    let present_body = unhex(present["body_hex"].as_str().unwrap());
    let decoded = v1_1::Envelope::decode(present_body.as_slice()).unwrap();
    assert_eq!(decoded.encode_to_vec(), present_body);
    let v1_1::envelope::Frame::StreamItem(item) = decoded.frame.unwrap() else {
        panic!("expected stream item")
    };
    let progress = v1_1::ToolProgress::decode(item.payload.as_slice()).unwrap();
    assert_eq!(progress.percent_complete, Some(0));
    assert_eq!(
        progress.encode_to_vec(),
        unhex(
            vectors["selected_minor_1_0_forbids_1_1_tool_progress"]["tool_progress_1_1_hex"]
                .as_str()
                .unwrap()
        )
    );

    let absent_body = unhex(vectors["progress"]["body_hex"].as_str().unwrap());
    let absent = v1_1::Envelope::decode(absent_body.as_slice()).unwrap();
    let v1_1::envelope::Frame::StreamItem(item) = absent.frame.unwrap() else {
        panic!("expected stream item")
    };
    assert_eq!(
        v1_1::ToolProgress::decode(item.payload.as_slice())
            .unwrap()
            .percent_complete,
        None
    );
}

#[test]
fn unknown_closed_error_numeric_remains_raw_until_explicit_rejection() {
    let body = unhex(
        fixture()["unknown_error_code"]["body_hex"]
            .as_str()
            .unwrap(),
    );
    let decoded = v1_0::Envelope::decode(body.as_slice()).unwrap();
    assert_eq!(decoded.encode_to_vec(), body);
    let v1_0::envelope::Frame::Response(response) = decoded.frame.unwrap() else {
        panic!("expected response")
    };
    let v1_0::response::Terminal::Error(error) = response.terminal.unwrap() else {
        panic!("expected error")
    };
    assert_eq!(error.code, 127);
    assert!(v1_0::ErrorCode::try_from(error.code).is_err());
    assert!(v1_1::ErrorCode::try_from(error.code).is_err());
}

#[test]
fn checked_descriptor_hashes_and_profile_digests_match_manifest() {
    let manifest = manifest();
    for (minor, common, tool) in [
        (
            "1.0",
            include_bytes!("../schema/descriptors/common-1.0.pb").as_slice(),
            include_bytes!("../schema/descriptors/tool-1.0.pb").as_slice(),
        ),
        (
            "1.1",
            include_bytes!("../schema/descriptors/common-1.1.pb").as_slice(),
            include_bytes!("../schema/descriptors/tool-1.1.pb").as_slice(),
        ),
    ] {
        assert_eq!(
            sha256_hex(common),
            manifest["descriptors"][format!("common-{minor}")]["file_sha256"]
                .as_str()
                .unwrap()
        );
        assert_eq!(
            sha256_hex(tool),
            manifest["descriptors"][format!("tool-{minor}")]["file_sha256"]
                .as_str()
                .unwrap()
        );
        let profile_preimage = [b"tool.v1\0".as_slice(), minor.as_bytes(), b"\0", tool].concat();
        assert_eq!(
            sha256_hex(&profile_preimage),
            manifest["descriptors"][format!("tool-{minor}")]["profile_digest"]
                .as_str()
                .unwrap()
        );
    }
}
