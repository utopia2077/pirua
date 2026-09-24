use std::num::NonZeroUsize;

use pi_extension_protocol::activation::HostAuthority;
use pi_extension_protocol::endpoint::{Endpoint, EndpointEvent, SelectedEnvelope};
use pi_extension_protocol::wire::echo_bounded::{
    IndexArena, IndexBudget, IndexStorage, ScanError, SelectedMinor,
};
use pi_extension_protocol::wire::{v1_0, v1_1};
use prost::Message;
use serde_json::Value;

fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}
fn unhex(value: &str) -> Vec<u8> {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}
fn fixture(name: &str) -> Vec<u8> {
    let data: Value =
        serde_json::from_str(include_str!("../schema/fixtures/raw-vectors.json")).unwrap();
    unhex(data[name]["body_hex"].as_str().unwrap())
}
fn put_varint(mut n: usize, output: &mut Vec<u8>) {
    while n >= 128 {
        output.push((n as u8) | 0x80);
        n >>= 7;
    }
    output.push(n as u8);
}
fn delimited(tag: u8, payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![tag << 3 | 2];
    put_varint(payload.len(), &mut bytes);
    bytes.extend_from_slice(payload);
    bytes
}
fn stream_item_from(body: &[u8]) -> v1_1::StreamItem {
    let envelope = v1_1::Envelope::decode(body).unwrap();
    let Some(v1_1::envelope::Frame::StreamItem(item)) = envelope.frame else {
        panic!("StreamItem")
    };
    item
}

fn active_endpoint(minor: u32) -> Endpoint {
    let limits = v1_1::ProtocolLimits {
        pre_handshake_frame_bytes: 4096,
        frame_bytes: 4096,
        aggregate_decoded_bytes: 4096,
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
    };
    let authority =
        HostAuthority::new("peer-a".into(), 7, minor, 1, limits, vec![], vec![]).unwrap();
    let mut endpoint = Endpoint::new(authority, 100);
    let hello = v1_1::Envelope {
        frame: Some(v1_1::envelope::Frame::Hello(v1_1::Hello {
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: minor,
            }),
            supported_profiles: vec![],
        })),
    };
    let manifest = v1_1::Envelope {
        frame: Some(v1_1::envelope::Frame::Manifest(v1_1::Manifest {
            declared_extension_id: "metadata".into(),
            extension_version: "0.1".into(),
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: minor,
            }),
            capabilities: vec![],
            permissions: vec![],
            subscriptions: vec![],
        })),
    };
    let ready = v1_1::Envelope {
        frame: Some(v1_1::envelope::Frame::Ready(v1_1::Ready {})),
    };
    for (message, now) in [(hello, 101), (manifest, 102), (ready, 103)] {
        let body = message.encode_to_vec();
        let mut frame = Vec::new();
        put_varint(body.len(), &mut frame);
        frame.extend_from_slice(&body);
        endpoint.read_one(&frame, now).unwrap();
    }
    endpoint
}

#[test]
fn scanner_matches_active_endpoint_on_original_and_split_bodies() {
    let regular = fixture("progress");
    let item = stream_item_from(&regular);
    let mut split = delimited(8, &[0x08, 0x11]);
    split.extend(delimited(8, &delimited(3, &item.payload)));
    for minor in [0, 1] {
        let mut endpoint = active_endpoint(minor);
        for body in [&regular, &split] {
            let mut frame = Vec::new();
            put_varint(body.len(), &mut frame);
            frame.extend_from_slice(body);
            let step = endpoint.read_one(&frame, 104).unwrap();
            let Some(EndpointEvent::Active(active)) = step.event else {
                panic!("active frame")
            };
            let (id, sequence, payload) = match active.envelope {
                SelectedEnvelope::V1_0(value) => {
                    let Some(v1_0::envelope::Frame::StreamItem(item)) = value.frame else {
                        panic!("1.0 item")
                    };
                    (item.request_id, item.sequence, item.payload)
                }
                SelectedEnvelope::V1_1(value) => {
                    let Some(v1_1::envelope::Frame::StreamItem(item)) = value.frame else {
                        panic!("1.1 item")
                    };
                    (item.request_id, item.sequence, item.payload)
                }
            };
            let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
            let view = arena.scan_envelope(body).unwrap();
            let item = view.message(view.root(), 8).unwrap();
            assert_eq!(view.scalar(item, 1).unwrap_or(0), id);
            assert_eq!(view.scalar(item, 2).unwrap_or(0), sequence);
            assert_eq!(view.bytes(item, 3).unwrap_or(&[]), payload);
            assert_eq!(view.body_len(), active.body_len);
        }
    }
}

#[test]
fn original_fixture_lengths_and_selected_minors_remain_distinct() {
    for (body, expected_arm, expected_len) in
        [(fixture("progress"), 8, 137), (fixture("terminal"), 9, 131)]
    {
        let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
        let view = arena.scan_envelope(&body).unwrap();
        assert_eq!(view.body_len(), expected_len);
        assert_eq!(view.arm(), Some(expected_arm));
        let root = view.root();
        let arm = view.message(root, expected_arm).unwrap();
        assert_eq!(view.scalar(arm, 1), Some(17));
        assert_eq!(
            v1_0::Envelope::decode(body.as_slice())
                .unwrap()
                .encoded_len(),
            body.len()
        );
        assert_eq!(
            v1_1::Envelope::decode(body.as_slice())
                .unwrap()
                .encoded_len(),
            body.len()
        );
    }

    let data: Value =
        serde_json::from_str(include_str!("../schema/fixtures/raw-vectors.json")).unwrap();
    let body = unhex(
        data["selected_minor_1_0_forbids_1_1_tool_progress"]["enveloped_progress"]["body_hex"]
            .as_str()
            .unwrap(),
    );
    let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
    let payload = {
        let view = arena.scan_envelope(&body).unwrap();
        assert_eq!(view.body_len(), 139);
        view.bytes(view.message(view.root(), 8).unwrap(), 3)
            .unwrap()
            .to_vec()
    };
    {
        let progress = arena.scan_tool_progress(&payload).unwrap();
        assert!(progress.has_field(progress.root(), 2)); // present zero is still present
        assert_eq!(progress.scalar(progress.root(), 2), Some(0));
    }
    assert_eq!(
        v1_1::ToolProgress::decode(payload.as_slice())
            .unwrap()
            .percent_complete,
        Some(0)
    );
    assert!(matches!(
        arena.scan_selected_tool_progress(&payload, SelectedMinor::V1_0),
        Err(ScanError::UnsupportedVariant)
    ));
    assert!(
        arena
            .scan_selected_tool_progress(&payload, SelectedMinor::V1_1)
            .is_ok()
    );
    let mut repeated = payload.clone();
    repeated.extend([0x10, 0x00]);
    assert!(matches!(
        arena.scan_selected_tool_progress(&repeated, SelectedMinor::V1_0),
        Err(ScanError::UnsupportedVariant)
    ));
    assert!(v1_0::ToolProgress::decode(payload.as_slice()).is_ok()); // selected 1.0 probe rejects before this decode
}

#[test]
fn split_same_arm_merges_but_original_139_byte_debit_survives() {
    let original = fixture("progress");
    let item = stream_item_from(&original);
    assert_eq!(item.payload.len(), 129);
    let mut split = delimited(8, &[0x08, 0x11]);
    split.extend(delimited(8, &delimited(3, &item.payload)));
    assert_eq!(split.len(), 139);
    let prost = stream_item_from(&split);
    assert_eq!(prost.request_id, 17);
    assert_eq!(prost.payload, item.payload);
    assert_eq!(
        v1_0::Envelope::decode(split.as_slice())
            .unwrap()
            .encoded_len(),
        137
    );
    let mut arena = IndexArena::try_new(nz(split.len())).unwrap();
    let view = arena.scan_envelope(&split).unwrap();
    let item_id = view.message(view.root(), 8).unwrap();
    assert_eq!(view.scalar(item_id, 1), Some(prost.request_id));
    assert_eq!(view.bytes(item_id, 3), Some(prost.payload.as_slice()));
    assert_eq!(view.body_len(), 139);
}

#[test]
fn split_event_merges_optional_message_and_repeated_list_appends() {
    let payload = stream_item_from(&fixture("progress")).payload;
    assert_eq!(&payload[..2], &[0x0a, 0x7f]);
    let event = &payload[2..];
    let mut split = delimited(1, &event[..54]);
    split.extend(delimited(1, &event[54..]));
    let old = v1_0::ToolProgress::decode(split.as_slice()).unwrap();
    let new = v1_1::ToolProgress::decode(split.as_slice()).unwrap();
    assert_eq!(
        old.event.as_ref().unwrap().projection_id,
        "tool.progress:echo"
    );
    assert_eq!(new.event.as_ref().unwrap().schema_digest_sha256.len(), 32);
    assert!(new.event.as_ref().unwrap().value.is_some());
    let mut arena = IndexArena::try_new(nz(split.len())).unwrap();
    let view = arena.scan_tool_progress(&split).unwrap();
    let event_id = view.message(view.root(), 1).unwrap();
    assert_eq!(
        view.bytes(event_id, 1),
        Some("tool.progress:echo".as_bytes())
    );
    assert_eq!(view.bytes(event_id, 2).unwrap().len(), 32);
    assert!(view.message(event_id, 3).is_some());

    let item_a = delimited(1, &[0x2a, 0x01, b'a']);
    let item_b = delimited(1, &[0x2a, 0x01, b'b']);
    let mut value = delimited(7, &item_a);
    value.extend(delimited(7, &item_b));
    let wire = v1_1::WireValue::decode(value.as_slice()).unwrap();
    let Some(v1_1::wire_value::Value::ListValue(list)) = wire.value else {
        panic!("list")
    };
    assert_eq!(list.items.len(), 2);
    // The same index format is used for this value through ToolProgress.event.
    let progress = delimited(1, &delimited(3, &value));
    let mut arena = IndexArena::try_new(nz(progress.len())).unwrap();
    let view = arena.scan_tool_progress(&progress).unwrap();
    let event = view.message(view.root(), 1).unwrap();
    let wire = view.message(event, 3).unwrap();
    let list = view.message(wire, 7).unwrap();
    assert_eq!(view.repeated_messages(list, 1).count(), 2);
}

#[test]
fn terminal_result_is_borrowed_from_effective_stream_end() {
    let body = fixture("terminal");
    let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
    let payload = {
        let view = arena.scan_envelope(&body).unwrap();
        let end = view.message(view.root(), 9).unwrap();
        assert_eq!(view.scalar(end, 1), Some(17));
        view.bytes(end, 2).unwrap().to_vec()
    };
    let result = arena.scan_tool_result(&payload).unwrap();
    let bound = result.message(result.root(), 1).unwrap();
    assert_eq!(result.text(bound, 1), Some("tool.result:echo"));
    assert_eq!(result.bytes(bound, 2).unwrap().len(), 32);
    assert!(result.message(bound, 3).is_some());
}

#[test]
fn oneof_replacement_and_last_scalar_match_prost() {
    let mut body = delimited(8, &[0x08, 0x11, 0x10, 0x01]);
    body.extend(delimited(8, &[0x08, 0x12, 0x10, 0x02]));
    let prost = stream_item_from(&body);
    let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
    let view = arena.scan_envelope(&body).unwrap();
    let item = view.message(view.root(), 8).unwrap();
    assert_eq!(view.scalar(item, 1), Some(prost.request_id));
    assert_eq!(view.scalar(item, 2), Some(prost.sequence));
    assert_eq!((prost.request_id, prost.sequence), (18, 2));

    let mut replaced = delimited(8, &[0x08, 0x11]);
    replaced.extend(delimited(10, &[0x08, 0x11]));
    replaced.extend(delimited(8, &[0x10, 0x03]));
    let prost = stream_item_from(&replaced);
    assert_eq!(prost.request_id, 0);
    let mut arena = IndexArena::try_new(nz(replaced.len())).unwrap();
    let view = arena.scan_envelope(&replaced).unwrap();
    assert_eq!(view.arm(), Some(8));
    assert_eq!(view.scalar(view.message(view.root(), 8).unwrap(), 1), None);
    assert_eq!(
        view.scalar(view.message(view.root(), 8).unwrap(), 2),
        Some(3)
    );
}

#[test]
fn many_distinct_optional_message_parents_preserve_final_arm_and_reuse() {
    // E1178's valid-input work witness: each profile is the first optional
    // message field in a new CapabilityDeclaration. The former reverse lookup
    // searched every preceding record on each of these misses.
    let capability = delimited(3, &[]);
    let mut manifest = Vec::new();
    for _ in 0..4096 {
        manifest.extend(delimited(4, &capability));
    }
    let mut body = delimited(3, &manifest);
    body.extend(delimited(8, &[0x08, 0x11, 0x10, 0x02]));

    let prost = v1_1::Envelope::decode(body.as_slice()).unwrap();
    let Some(v1_1::envelope::Frame::StreamItem(item)) = prost.frame else {
        panic!("final StreamItem")
    };
    assert_eq!((item.request_id, item.sequence), (17, 2));

    let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
    let view = arena.scan_envelope(&body).unwrap();
    assert_eq!(view.arm(), Some(8));
    assert_eq!(view.body_len(), body.len());
    let item = view.message(view.root(), 8).unwrap();
    assert_eq!(view.scalar(item, 1), Some(17));
    assert_eq!(view.scalar(item, 2), Some(2));

    // Reusing the arena must clear the direct slots from the earlier scan.
    let short = delimited(8, &[0x10, 0x03]);
    let view = arena.scan_envelope(&short).unwrap();
    let item = view.message(view.root(), 8).unwrap();
    assert_eq!(view.scalar(item, 1), None);
    assert_eq!(view.scalar(item, 2), Some(3));
}

#[test]
fn every_earlier_arm_is_validated_before_a_later_good_item() {
    let valid = fixture("progress");
    for tag in 1..=12 {
        let mut body = delimited(tag, &[0x80]); // invalid nested key varint
        body.extend_from_slice(&valid);
        assert!(
            v1_1::Envelope::decode(body.as_slice()).is_err(),
            "arm {tag}"
        );
        let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
        assert!(
            matches!(arena.scan_envelope(&body), Err(ScanError::Malformed)),
            "arm {tag}"
        );
    }
    let mut body = delimited(6, &[0x2a, 0x01, 0xff]); // invalid UTF-8 in Request.method
    body.extend_from_slice(&valid);
    assert!(v1_1::Envelope::decode(body.as_slice()).is_err());
    let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
    assert!(matches!(
        arena.scan_envelope(&body),
        Err(ScanError::Malformed)
    ));

    // E1162's exact malformed Request prefix cannot be rescued by progress.
    let mut body = vec![0x32, 0x01, 0x80];
    body.extend_from_slice(&valid);
    let mut frame = Vec::new();
    put_varint(body.len(), &mut frame);
    frame.extend_from_slice(&body);
    for minor in [0, 1] {
        let mut endpoint = active_endpoint(minor);
        assert!(endpoint.read_one(&frame, 104).is_err());
        let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
        assert!(matches!(
            arena.scan_envelope(&body),
            Err(ScanError::Malformed)
        ));
    }
}

#[test]
fn unknown_fields_groups_and_malformed_wire_match_prost() {
    let good = fixture("progress");
    let variants = [
        [vec![0x78, 0x01], good.clone()].concat(), // unknown varint
        [vec![0x7a, 0x02, 0x01, 0x02], good.clone()].concat(), // unknown bytes
        [vec![0x7b, 0x78, 0x01, 0x7c], good.clone()].concat(), // group
        [vec![0x7b, 0x7b, 0x7c, 0x7c], good.clone()].concat(),
    ];
    for body in variants {
        let reference = v1_1::Envelope::decode(body.as_slice()).unwrap();
        let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
        let view = arena.scan_envelope(&body).unwrap();
        assert_eq!(view.arm(), Some(8));
        assert_eq!(view.body_len(), body.len());
        let Some(v1_1::envelope::Frame::StreamItem(item)) = reference.frame else {
            panic!()
        };
        assert_eq!(
            view.scalar(view.message(view.root(), 8).unwrap(), 1),
            Some(item.request_id)
        );
    }
    let bads = [
        vec![0x7b, 0x84, 0x01], // mismatched end group (tag 16)
        vec![0x7b],             // missing end group
        vec![0x7c],             // unmatched end group
        vec![0x00],             // tag zero
        vec![0x7e],             // invalid wire type
        vec![0x78, 0x80],       // truncated varint
        vec![
            0x78, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02,
        ], // overflow
        vec![0x7a, 0x03, 0x01], // truncated length
        vec![0x42, 0x02, 0x0a, 0x00], // StreamItem.request_id wrong wire type
    ];
    for body in bads {
        assert!(
            v1_1::Envelope::decode(body.as_slice()).is_err(),
            "{body:02x?}"
        );
        let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
        assert!(
            matches!(arena.scan_envelope(&body), Err(ScanError::Malformed)),
            "{body:02x?}"
        );
    }
    for count in [99, 100, 101] {
        let mut body = vec![0x7b; count];
        body.extend(vec![0x7c; count]);
        body.extend_from_slice(&good);
        let reference = v1_1::Envelope::decode(body.as_slice()).is_ok();
        let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
        assert_eq!(
            arena.scan_envelope(&body).is_ok(),
            reference,
            "depth {count}"
        );
    }
    let mut body = vec![0x7b; 100];
    body.extend([0x78, 0x01]);
    body.extend(vec![0x7c; 100]);
    body.extend_from_slice(&good);
    let reference = v1_1::Envelope::decode(body.as_slice()).is_ok();
    let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
    assert_eq!(arena.scan_envelope(&body).is_ok(), reference);

    // One known message level and unknown groups consume the same Prost depth.
    for count in [98, 99, 100] {
        let mut item = vec![0x08, 0x11];
        item.extend(vec![0x7b; count]);
        item.extend(vec![0x7c; count]);
        let body = delimited(8, &item);
        let reference = v1_1::Envelope::decode(body.as_slice()).is_ok();
        let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
        assert_eq!(
            arena.scan_envelope(&body).is_ok(),
            reference,
            "known + group depth {count}"
        );
    }
}

#[test]
fn index_capacity_overflow_and_ceiling_fail_closed() {
    assert_eq!(IndexBudget::checked(nz(usize::MAX)), Err(ScanError::Limit));
    let budget = IndexBudget::checked(nz(137)).unwrap();
    assert_eq!(
        IndexArena::try_new_with_ceiling(nz(137), budget.capacity_bytes - 1).err(),
        Some(ScanError::Limit)
    );
    let mut arena = IndexArena::try_new_with_ceiling(nz(137), budget.capacity_bytes).unwrap();
    assert!(arena.budget().capacity_bytes >= budget.capacity_bytes);
    assert_eq!(
        arena
            .scan_envelope(&fixture("progress"))
            .unwrap()
            .body_len(),
        137
    );
    let mut tiny = IndexArena::try_new(nz(1)).unwrap();
    assert!(matches!(
        tiny.scan_envelope(&fixture("progress")),
        Err(ScanError::Limit)
    ));
}

#[test]
fn single_byte_fixture_mutations_agree_with_prost_envelope_validity() {
    for base in [fixture("request"), fixture("progress"), fixture("terminal")] {
        for offset in 0..base.len() {
            for replacement in [0, 1, 0x7f, 0x80, 0xff] {
                let mut body = base.clone();
                body[offset] = replacement;
                let reference = v1_1::Envelope::decode(body.as_slice())
                    .is_ok_and(|envelope| envelope.frame.is_some());
                let mut arena = IndexArena::try_new(nz(body.len())).unwrap();
                assert_eq!(
                    arena.scan_envelope(&body).is_ok(),
                    reference,
                    "fixture length {}, byte {offset}, replacement {replacement:#x}",
                    body.len()
                );
            }
        }
    }
}

#[test]
fn typed_slot_checks_selected_layout_and_full_backing_before_borrow() {
    assert_eq!(IndexBudget::checked(nz(1)).unwrap().capacity_bytes, 360);
    assert_eq!(
        IndexBudget::checked(nz(137)).unwrap().capacity_bytes,
        27_560
    );
    assert_eq!(
        IndexBudget::checked(nz(4096)).unwrap().capacity_bytes,
        819_360
    );

    for (frame, expected) in [(1, 360), (137, 27_560), (4096, 819_360)] {
        let slot = match frame {
            1 => IndexStorage::<1, 2>::checked_slot_bytes(nz(frame), expected),
            137 => IndexStorage::<137, 138>::checked_slot_bytes(nz(frame), expected),
            _ => IndexStorage::<4096, 4097>::checked_slot_bytes(nz(frame), expected),
        };
        assert_eq!(slot, Ok(expected));
        let below = match frame {
            1 => IndexStorage::<1, 2>::checked_slot_bytes(nz(frame), expected - 1),
            137 => IndexStorage::<137, 138>::checked_slot_bytes(nz(frame), expected - 1),
            _ => IndexStorage::<4096, 4097>::checked_slot_bytes(nz(frame), expected - 1),
        };
        assert_eq!(below, Err(ScanError::Limit));
    }
    let mut small = IndexStorage::<1, 2>::new();
    let mut small_arena = small.borrow(nz(1), 360).unwrap();
    assert!(matches!(
        small_arena.scan_envelope(&[0]),
        Err(ScanError::Malformed)
    ));

    type Oversized = IndexStorage<256, 257>;
    let full = std::mem::size_of::<Oversized>();
    assert!(full > IndexBudget::checked(nz(137)).unwrap().capacity_bytes);
    assert_eq!(
        Oversized::checked_slot_bytes(nz(137), full - 1),
        Err(ScanError::Limit)
    );
    assert_eq!(Oversized::checked_slot_bytes(nz(137), full), Ok(full));
    assert_eq!(
        IndexStorage::<136, 138>::checked_slot_bytes(nz(137), usize::MAX),
        Err(ScanError::Limit)
    );
    assert_eq!(
        IndexStorage::<137, 137>::checked_slot_bytes(nz(137), usize::MAX),
        Err(ScanError::Limit)
    );
    assert_eq!(
        Oversized::checked_slot_bytes(nz(usize::MAX), usize::MAX),
        Err(ScanError::Limit)
    );

    let mut storage = Oversized::new();
    assert!(matches!(
        storage.borrow(nz(137), full - 1),
        Err(ScanError::Limit)
    ));
    let arena = storage.borrow(nz(137), full).unwrap();
    assert_eq!(
        arena.selected_budget(),
        IndexBudget::checked(nz(137)).unwrap()
    );
    assert_eq!(arena.slot_bytes(), full);
}

#[test]
fn borrowed_scanner_preserves_prost_fixtures_and_reuses_after_malformed() {
    let regular = fixture("progress");
    let item = stream_item_from(&regular);
    let mut split = delimited(8, &[0x08, 0x11]);
    split.extend(delimited(8, &delimited(3, &item.payload)));
    let mut replaced = delimited(8, &[0x08, 0x11]);
    replaced.extend(delimited(10, &[0x08, 0x11]));
    replaced.extend(delimited(8, &[0x10, 0x03]));
    let mut storage = IndexStorage::<256, 257>::new();
    let slot_bytes = std::mem::size_of_val(&storage);
    let mut arena = storage.borrow(nz(256), slot_bytes).unwrap();

    for body in [&regular, &split, &replaced] {
        let expected = stream_item_from(body);
        let view = arena.scan_envelope(body).unwrap();
        let item = view.message(view.root(), 8).unwrap();
        assert_eq!(view.arm(), Some(8));
        assert_eq!(view.body_len(), body.len());
        assert_eq!(view.scalar(item, 1).unwrap_or(0), expected.request_id);
        assert_eq!(view.scalar(item, 2).unwrap_or(0), expected.sequence);
        assert_eq!(view.bytes(item, 3).unwrap_or(&[]), expected.payload);
    }
    assert_eq!(split.len(), 139);
    assert_eq!(
        v1_0::Envelope::decode(split.as_slice())
            .unwrap()
            .encoded_len(),
        137
    );

    let grouped = [vec![0x7b, 0x78, 0x01, 0x7c], regular.clone()].concat();
    assert!(v1_1::Envelope::decode(grouped.as_slice()).is_ok());
    assert_eq!(arena.scan_envelope(&grouped).unwrap().arm(), Some(8));
    let malformed_earlier = [delimited(6, &[0x80]), regular.clone()].concat();
    assert!(v1_1::Envelope::decode(malformed_earlier.as_slice()).is_err());
    assert!(matches!(
        arena.scan_envelope(&malformed_earlier),
        Err(ScanError::Malformed)
    ));

    let malformed = [0x42, 0x01, 0x80];
    assert!(matches!(
        arena.scan_envelope(&malformed),
        Err(ScanError::Malformed)
    ));
    let view = arena.scan_envelope(&regular).unwrap();
    assert_eq!(view.arm(), Some(8));
    let payload = item.payload;
    let progress = arena.scan_tool_progress(&payload).unwrap();
    assert!(progress.has_field(progress.root(), 1));
    let mut with_zero = payload.clone();
    with_zero.extend([0x10, 0x00]);
    assert!(matches!(
        arena.scan_selected_tool_progress(&with_zero, SelectedMinor::V1_0),
        Err(ScanError::UnsupportedVariant)
    ));
    assert!(
        arena
            .scan_selected_tool_progress(&with_zero, SelectedMinor::V1_1)
            .is_ok()
    );
}

#[test]
fn borrowed_scanner_keeps_transmitted_length_for_selected_1_0_counterexample() {
    let mut error = vec![0x08, 0xff, 0xff, 0xff, 0xff, 0x0f];
    error.extend(delimited(2, &vec![b'x'; 4078]));
    let mut end = vec![0x08, 0x11];
    end.extend(delimited(3, &error));
    let body = delimited(9, &end);
    assert_eq!(body.len(), 4095);
    assert_eq!(
        v1_1::Envelope::decode(body.as_slice())
            .unwrap()
            .encoded_len(),
        4100
    );

    let mut storage = IndexStorage::<4096, 4097>::new();
    let mut arena = storage.borrow(nz(4096), 819_360).unwrap();
    let view = arena.scan_envelope(&body).unwrap();
    assert_eq!(view.arm(), Some(9));
    assert_eq!(view.body_len(), 4095);
    let end = view.message(view.root(), 9).unwrap();
    let error = view.message(end, 3).unwrap();
    assert_eq!(view.scalar(error, 1), Some(u64::from(u32::MAX)));
}
