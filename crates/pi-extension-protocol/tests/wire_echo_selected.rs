use std::num::NonZeroUsize;

use pi_extension_protocol::wire::echo_bounded::{
    IndexStorage, NormalizationWorkBound, ScanError, SelectedMinor,
};
use pi_extension_protocol::wire::echo_selected::{ActiveArm, SelectError, select_active};
use pi_extension_protocol::wire::v1_1;
use prost::Message;

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

fn varint(mut n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    while n >= 128 {
        out.push(n as u8 | 0x80);
        n >>= 7;
    }
    out.push(n as u8);
    out
}

fn scalar(tag: u32, n: u64) -> Vec<u8> {
    let mut out = varint(u64::from(tag) << 3);
    out.extend(varint(n));
    out
}

fn bytes(tag: u32, data: &[u8]) -> Vec<u8> {
    let mut out = varint((u64::from(tag) << 3) | 2);
    out.extend(varint(data.len() as u64));
    out.extend_from_slice(data);
    out
}

fn body(tag: u32, data: &[u8]) -> Vec<u8> {
    bytes(tag, data)
}

fn prost_len(body: &[u8]) -> usize {
    v1_1::Envelope::decode(body).unwrap().encoded_len()
}

fn checked_size(body: &[u8], limit: usize) -> Result<(ActiveArm, usize, usize), SelectError> {
    let mut storage = IndexStorage::<4100, 4101>::new();
    let mut arena = storage.borrow(nz(4100), usize::MAX).unwrap();
    let view = arena.scan_envelope(body).unwrap();
    let selected = select_active(view, SelectedMinor::V1_0, nz(limit))?;
    Ok((
        selected.arm(),
        selected.body_len(),
        selected.normalized_len().unwrap(),
    ))
}

fn signed_enum_witness() -> Vec<u8> {
    let mut error = scalar(1, u64::from(u32::MAX)); // five-byte -1 on the wire
    error.extend(bytes(2, &vec![b'x'; 4078]));
    let mut end = scalar(1, 17);
    end.extend(bytes(3, &error));
    body(9, &end)
}

#[test]
fn signed_enum_expansion_preserves_endpoint_guard_order_and_original_length() {
    let transmitted = signed_enum_witness();
    assert_eq!(transmitted.len(), 4095);
    assert_eq!(prost_len(&transmitted), 4100);
    for limit in [4096, 4099] {
        assert_eq!(
            checked_size(&transmitted, limit),
            Err(SelectError::FrameTooLarge {
                length: 4100,
                limit
            })
        );
    }
    assert_eq!(
        checked_size(&transmitted, 4100),
        Ok((ActiveArm::StreamEnd, 4095, 4100))
    );
}

#[test]
fn selected_core_1_1_skips_normalization_guard_and_retains_transmitted_length() {
    let transmitted = signed_enum_witness();
    let mut storage = IndexStorage::<4096, 4097>::new();
    let mut arena = storage.borrow(nz(4096), usize::MAX).unwrap();
    let view = arena.scan_envelope(&transmitted).unwrap();
    let selected = select_active(view, SelectedMinor::V1_1, nz(4096)).unwrap();
    assert_eq!(selected.arm(), ActiveArm::StreamEnd);
    assert_eq!(selected.body_len(), 4095);
    assert_eq!(selected.normalized_len(), None);
}

#[test]
fn split_item_normalizes_to_137_but_span_reborrows_original_139_byte_body() {
    let regular = v1_1::Envelope::decode(hex_fixture("progress").as_slice()).unwrap();
    let Some(v1_1::envelope::Frame::StreamItem(item)) = regular.frame else {
        panic!("progress fixture must contain StreamItem")
    };
    let mut transmitted = body(8, &scalar(1, 17));
    transmitted.extend(body(8, &bytes(3, &item.payload)));
    assert_eq!(transmitted.len(), 139);
    assert_eq!(prost_len(&transmitted), 137);

    let mut storage = IndexStorage::<139, 140>::new();
    let mut arena = storage.borrow(nz(139), usize::MAX).unwrap();
    let span = {
        let view = arena.scan_envelope(&transmitted).unwrap();
        let selected = select_active(view, SelectedMinor::V1_0, nz(139)).unwrap();
        assert_eq!(selected.arm(), ActiveArm::StreamItem);
        assert_eq!(selected.body_len(), 139);
        assert_eq!(selected.normalized_len(), Some(137));
        selected.opaque_payload_span().unwrap()
    };
    assert_eq!(span.len(), item.payload.len());
    let payload = span.reborrow().unwrap();
    assert_eq!(payload, item.payload);
    let progress = arena.scan_tool_progress(payload).unwrap();
    assert!(progress.has_field(progress.root(), 1));
}

#[test]
fn outer_message_id_cannot_alias_inner_child_after_arena_reuse() {
    let mut value = bytes(1, b"projection");
    value.extend(bytes(2, b"digest"));
    value.extend(body(3, &[]));
    let payload = body(1, &value);
    let transmitted = body(8, &bytes(3, &payload));
    let mut storage = IndexStorage::<128, 129>::new();
    let mut arena = storage.borrow(nz(128), usize::MAX).unwrap();
    let (old_id, span) = {
        let outer = arena.scan_envelope(&transmitted).unwrap();
        let selected = select_active(outer, SelectedMinor::V1_0, nz(128)).unwrap();
        let old_id = selected.view().message(selected.view().root(), 8).unwrap();
        (old_id, selected.opaque_payload_span().unwrap())
    };
    let inner = arena.scan_tool_progress(span.reborrow().unwrap()).unwrap();
    let new_id = inner.message(inner.root(), 1).unwrap();
    assert_eq!(inner.text(new_id, 1), Some("projection"));
    assert_ne!(old_id, new_id);
    assert_eq!(inner.text(old_id, 1), None);
    assert_eq!(inner.bytes(old_id, 1), None);
    assert_eq!(
        inner.bytes_span(new_id, 2).unwrap().reborrow(),
        Some(b"digest".as_slice())
    );
    assert_eq!(inner.bytes_span(old_id, 2), None);
    assert_eq!(inner.message(old_id, 3), None);
    assert!(!inner.has_field(old_id, 1));
    assert_eq!(inner.repeated_messages(old_id, 3).count(), 0);

    let mut other_storage = IndexStorage::<128, 129>::new();
    let mut other_arena = other_storage.borrow(nz(128), usize::MAX).unwrap();
    let other = other_arena
        .scan_tool_progress(span.reborrow().unwrap())
        .unwrap();
    assert_eq!(other.text(new_id, 1), None);
    assert_ne!(other.message(other.root(), 1).unwrap(), new_id);
}

fn hex_fixture(name: &str) -> Vec<u8> {
    let json: serde_json::Value =
        serde_json::from_str(include_str!("../schema/fixtures/raw-vectors.json")).unwrap();
    let hex = json[name]["body_hex"].as_str().unwrap();
    let (pairs, remainder) = hex.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|chunk| u8::from_str_radix(std::str::from_utf8(chunk).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn scan_precedes_selection_and_final_arm_precedes_size() {
    let mut malformed = body(8, &bytes(1, b"bad wire for request_id"));
    malformed.extend(body(9, &[]));
    let mut storage = IndexStorage::<128, 129>::new();
    let mut arena = storage.borrow(nz(128), usize::MAX).unwrap();
    assert!(matches!(
        arena.scan_envelope(&malformed),
        Err(ScanError::Malformed)
    ));
    assert!(matches!(
        arena.scan_envelope(&[]),
        Err(ScanError::Malformed)
    ));

    let hello = body(1, &bytes(99, &[b'x'; 30]));
    let view = arena.scan_envelope(&hello).unwrap();
    assert!(matches!(
        select_active(view, SelectedMinor::V1_0, nz(1)),
        Err(SelectError::UnexpectedFrame)
    ));

    // Request is a final Active arm even when it is wrong-direction for the
    // echo receive transition. Endpoint's selected-1.0 guard runs first.
    let request = body(6, &bytes(8, &[b'x'; 30]));
    let view = arena.scan_envelope(&request).unwrap();
    assert_eq!(
        select_active(view, SelectedMinor::V1_0, nz(1)).err(),
        Some(SelectError::FrameTooLarge {
            length: prost_len(&request) as u64,
            limit: 1
        })
    );
}

#[test]
fn all_active_arms_and_generated_field_policies_match_prost() {
    // Covers every field reachable through active tags 6-12: u64, optional
    // u64/string, truncated u32, bool, i32 enums, bytes and nested messages.
    // The repository's schema verifier compares the generated DTO with the
    // canonical descriptor during the protocol all-target test pass.
    let mut profile = bytes(1, b"tool.v1");
    profile.extend(scalar(2, (1u64 << 32) + 1)); // uint32 truncation
    profile.extend(scalar(3, 1));
    let mut capability = bytes(1, b"peer");
    capability.extend(bytes(2, b"tool"));
    capability.extend(bytes(3, b"echo"));
    capability.extend(body(4, &profile));
    capability.extend(bytes(5, b"digest"));
    capability.extend(bytes(6, b"")); // optional empty remains present
    let mut effect = Vec::new();
    for tag in 1..=5 {
        effect.extend(bytes(tag, b"x"));
    }
    effect.extend(scalar(6, 2));
    effect.extend(scalar(7, 0)); // optional zero remains present
    let mut request = scalar(1, 17);
    request.extend(scalar(2, 0));
    request.extend(body(3, &capability));
    request.extend(body(4, &effect));
    request.extend(bytes(5, b"invoke"));
    request.extend(scalar(6, 0));
    request.extend(scalar(7, 0));
    request.extend(bytes(8, b"opaque"));

    let mut typed_error = scalar(1, u64::from(u32::MAX));
    typed_error.extend(bytes(2, b"error"));
    let mut response = scalar(1, 17);
    response.extend(body(3, &typed_error));
    let mut stream_item = scalar(1, 17);
    stream_item.extend(scalar(2, 0));
    stream_item.extend(bytes(3, b"opaque"));
    let mut stream_end = scalar(1, 17);
    stream_end.extend(bytes(2, b"")); // oneof empty remains present
    let mut cancel = scalar(1, 17);
    cancel.extend(scalar(2, u64::from(u32::MAX)));
    let mut credit = scalar(1, 17);
    credit.extend(scalar(2, 500));
    let mut goaway = scalar(1, u64::from(u32::MAX));
    goaway.extend(scalar(2, 2)); // protobuf bool normalizes to one
    for (tag, nested, arm) in [
        (6, request, ActiveArm::Request),
        (7, response, ActiveArm::Response),
        (8, stream_item, ActiveArm::StreamItem),
        (9, stream_end, ActiveArm::StreamEnd),
        (10, cancel, ActiveArm::Cancel),
        (11, credit, ActiveArm::Credit),
        (12, goaway, ActiveArm::GoAway),
    ] {
        let transmitted = body(tag, &nested);
        let normalized = prost_len(&transmitted);
        assert_eq!(
            checked_size(&transmitted, 4100),
            Ok((arm, transmitted.len(), normalized)),
            "active arm {tag}"
        );
        assert_eq!(
            checked_size(&transmitted, normalized - 1),
            Err(SelectError::FrameTooLarge {
                length: normalized as u64,
                limit: normalized - 1,
            }),
            "active arm {tag} must receive the guard"
        );
    }
}

#[test]
fn generated_active_field_tags_and_checked_work_formula_are_audited() {
    // `schema_authority` runs the canonical descriptor/generated comparison.
    // This table independently fails if an active generated message acquires
    // a field which the size policy and all-fields corpus have not reviewed.
    let source = include_str!("../src/wire/generated/v1_1.rs");
    for (name, expected) in [
        ("Envelope", vec![6, 7, 8, 9, 10, 11, 12]),
        ("Request", (1..=8).collect()),
        ("CapabilityRef", (1..=6).collect()),
        ("ProfileVersion", (1..=3).collect()),
        ("EffectKey", (1..=7).collect()),
        ("Response", (1..=3).collect()),
        ("StreamItem", (1..=3).collect()),
        ("StreamEnd", (1..=3).collect()),
        ("TypedError", (1..=2).collect()),
        ("Cancel", (1..=2).collect()),
        ("Credit", (1..=2).collect()),
        ("GoAway", (1..=2).collect()),
    ] {
        let marker = format!("pub struct {name} {{");
        let start = source.find(&marker).unwrap() + marker.len();
        let body = source[start..].split_once("\n}").unwrap().0;
        let mut tags = Vec::new();
        for line in body.lines().filter(|line| line.contains("#[prost(")) {
            if let Some(value) = line.split("tags = \"").nth(1) {
                let value = value.split('"').next().unwrap();
                tags.extend(
                    value
                        .split(',')
                        .map(|tag| tag.trim().parse::<u32>().unwrap()),
                );
            } else if let Some(value) = line.split("tag = \"").nth(1) {
                tags.push(value.split('"').next().unwrap().parse::<u32>().unwrap());
            }
        }
        if name == "Envelope" {
            tags.retain(|tag| *tag >= 6);
        }
        tags.sort_unstable();
        assert_eq!(tags, expected, "generated {name} tags");
    }
    let work = NormalizationWorkBound::checked(nz(4096)).unwrap();
    assert_eq!(work.index_visits, 4 * 4096 + 2);
    assert_eq!(work.varint_steps, 30 * 4096);
    assert_eq!(work.total_steps, 34 * 4096 + 2);
    assert_eq!(
        NormalizationWorkBound::checked(nz(usize::MAX)),
        Err(ScanError::Limit)
    );
}

#[test]
fn merge_replacement_unknowns_and_repeated_superseded_children_match_prost() {
    let first_request = body(6, &scalar(1, 99));
    let response = body(7, &scalar(1, 44));
    let mut final_request = body(6, &scalar(1, 17));
    final_request.extend(body(6, &scalar(2, 0))); // optional present zero
    final_request.extend(body(6, &body(3, &bytes(1, b"peer"))));
    final_request.extend(body(6, &body(3, &body(4, &scalar(2, 1)))));
    final_request.extend(bytes(99, b"ignored"));
    let mut transmitted = first_request;
    transmitted.extend(response);
    transmitted.extend(final_request);
    assert_eq!(
        checked_size(&transmitted, 4100),
        Ok((
            ActiveArm::Request,
            transmitted.len(),
            prost_len(&transmitted)
        ))
    );
    let decoded = v1_1::Envelope::decode(transmitted.as_slice()).unwrap();
    let Some(v1_1::envelope::Frame::Request(request)) = decoded.frame else {
        panic!("final Request")
    };
    assert_eq!(request.request_id, 17);
    assert_eq!(request.parent_request_id, Some(0));
    assert_eq!(request.capability_ref.unwrap().profile.unwrap().major, 1);

    // Manifest's repeated children are fully parsed before a later active
    // arm replaces it, then excluded from the active normalized size.
    let mut manifest = body(4, &body(3, &[]));
    manifest.extend(body(4, &body(3, &[])));
    let mut with_repeated = body(3, &manifest);
    with_repeated.extend(body(8, &scalar(1, 17)));
    assert_eq!(
        checked_size(&with_repeated, 4100),
        Ok((
            ActiveArm::StreamItem,
            with_repeated.len(),
            prost_len(&with_repeated)
        ))
    );
}
