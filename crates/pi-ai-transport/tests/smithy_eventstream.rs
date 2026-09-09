#![cfg(feature = "native")]
use base64::{Engine, engine::general_purpose::STANDARD};
use pi_ai_transport::{
    SmithyEventStreamDecoder, SmithyEventStreamLimits, SmithyEventStreamMessage, SmithyHeaderValue,
    TransportErrorKind,
};
use serde_json::{Value, json};
fn corpus() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/smithy-eventstream/source.json"
    ))
    .unwrap()
}
fn wire(row: &Value) -> Vec<u8> {
    STANDARD.decode(row["wire"].as_str().unwrap()).unwrap()
}
fn observed(message: SmithyEventStreamMessage) -> Value {
    let headers: Vec<Value> = message
        .headers
        .into_iter()
        .map(|(name, value)| {
            let (kind, value) = match value {
                SmithyHeaderValue::Boolean(v) => ("boolean", json!(v)),
                SmithyHeaderValue::Byte(v) => ("byte", json!(v)),
                SmithyHeaderValue::Short(v) => ("short", json!(v)),
                SmithyHeaderValue::Integer(v) => ("integer", json!(v)),
                SmithyHeaderValue::Long(v) => (
                    "long",
                    json!(v.iter().map(|b| format!("{b:02x}")).collect::<String>()),
                ),
                SmithyHeaderValue::Bytes(v) => ("binary", json!(STANDARD.encode(v))),
                SmithyHeaderValue::String(v) => ("string", json!(v)),
                SmithyHeaderValue::Timestamp(v) => ("timestamp", json!(i64::from_be_bytes(v))),
                SmithyHeaderValue::Uuid(v) => {
                    let h = v.iter().map(|b| format!("{b:02x}")).collect::<String>();
                    (
                        "uuid",
                        json!(format!(
                            "{}-{}-{}-{}-{}",
                            &h[..8],
                            &h[8..12],
                            &h[12..16],
                            &h[16..20],
                            &h[20..]
                        )),
                    )
                }
            };
            json!([name, kind, value])
        })
        .collect();
    json!({"headers":headers,"body":STANDARD.encode(message.payload)})
}
#[test]
fn installed_source_headers_checksums_owned_extents_and_all_chunk_splits() {
    let c = corpus();
    let rows = c["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 17);
    for row in rows {
        let bytes = wire(row);
        for split in 0..=bytes.len() {
            let mut decoder = SmithyEventStreamDecoder::new(Default::default());
            decoder.push(&bytes[..split]).unwrap();
            decoder.push(&bytes[split..]).unwrap();
            decoder.finish();
            let actual = decoder.next_frame();
            if row.get("error").is_some() {
                assert_eq!(
                    actual.unwrap_err().kind(),
                    TransportErrorKind::Protocol,
                    "{} split {split}",
                    row["id"]
                );
                assert_eq!(decoder.buffered_bytes(), 0);
                assert!(decoder.next_frame().unwrap().is_none());
            } else {
                assert_eq!(
                    observed(actual.unwrap().unwrap()),
                    row["decoded"],
                    "{} split {split}",
                    row["id"]
                );
                assert!(decoder.next_frame().unwrap().is_none());
                assert_eq!(decoder.buffered_bytes(), 0);
            }
        }
    }
    let chunks = c["chunks"].as_array().unwrap();
    assert_eq!(chunks.len(), 5);
    for row in chunks {
        let mut decoder = SmithyEventStreamDecoder::new(Default::default());
        decoder.push(&wire(row)).unwrap();
        decoder.finish();
        let actual = decoder.next_frame();
        if row.get("error").is_some() {
            assert_eq!(actual.unwrap_err().kind(), TransportErrorKind::Protocol);
        } else {
            let values: Vec<_> = actual.unwrap().into_iter().map(observed).collect();
            assert_eq!(json!(values), row["decoded"]);
        }
        assert_eq!(decoder.buffered_bytes(), 0);
    }
}
fn crc(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for b in bytes {
        crc ^= u32::from(*b);
        for _ in 0..8 {
            crc = (crc >> 1) ^ if crc & 1 != 0 { 0xedb88320 } else { 0 };
        }
    }
    !crc
}
fn frame(headers: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend_from_slice(
        &u32::try_from(16 + headers.len() + payload.len())
            .unwrap()
            .to_be_bytes(),
    );
    b.extend_from_slice(&u32::try_from(headers.len()).unwrap().to_be_bytes());
    b.extend_from_slice(&crc(&b).to_be_bytes());
    b.extend_from_slice(headers);
    b.extend_from_slice(payload);
    b.extend_from_slice(&crc(&b).to_be_bytes());
    b
}
#[test]
fn each_limit_has_exact_boundary_failure_release_and_unrelated_control() {
    let bytes = frame(&[1, b'x', 0], b"abc");
    assert_eq!(bytes.len(), 22);
    let exact = SmithyEventStreamLimits {
        max_buffer_bytes: 22,
        max_message_bytes: 22,
        max_header_bytes: 3,
        max_headers: 1,
    };
    let mut decoder = SmithyEventStreamDecoder::new(exact);
    // The bounds apply to retained intake, not cumulative ordinary messages.
    for _ in 0..10000 {
        decoder.push(&bytes).unwrap();
        assert_eq!(decoder.buffered_bytes(), 22);
        assert_eq!(decoder.next_frame().unwrap().unwrap().payload, b"abc");
        assert_eq!(decoder.buffered_bytes(), 0);
    }
    for limits in [
        SmithyEventStreamLimits {
            max_buffer_bytes: 21,
            ..exact
        },
        SmithyEventStreamLimits {
            max_message_bytes: 21,
            ..exact
        },
        SmithyEventStreamLimits {
            max_header_bytes: 2,
            ..exact
        },
        SmithyEventStreamLimits {
            max_headers: 0,
            ..exact
        },
    ] {
        let mut decoder = SmithyEventStreamDecoder::new(limits);
        let err = match decoder.push(&bytes) {
            Err(e) => e,
            Ok(()) => decoder.next_frame().unwrap_err(),
        };
        assert_eq!(err.kind(), TransportErrorKind::LimitExceeded);
        assert_eq!(decoder.buffered_bytes(), 0);
        assert!(decoder.next_frame().unwrap().is_none());
        assert!(decoder.push(&[]).is_err());
    }
    // Header count charges actual wire entries, including duplicates and discarded prototype names.
    for headers in [
        vec![1, b'x', 0, 1, b'x', 1],
        vec![9, b'_', b'_', b'p', b'r', b'o', b't', b'o', b'_', b'_', 0],
    ] {
        let mut decoder = SmithyEventStreamDecoder::new(SmithyEventStreamLimits {
            max_headers: if headers.len() == 6 { 1 } else { 0 },
            ..Default::default()
        });
        decoder.push(&frame(&headers, &[])).unwrap();
        assert_eq!(
            decoder.next_frame().unwrap_err().kind(),
            TransportErrorKind::LimitExceeded
        );
        assert_eq!(decoder.buffered_bytes(), 0);
    }
    // Lossy UTF-8 expands one wire byte to three owned text bytes.
    let bytes = frame(&[1, 255, 7, 0, 1, 255], &[]);
    for (limit, success) in [(6, true), (5, false)] {
        let mut decoder = SmithyEventStreamDecoder::new(SmithyEventStreamLimits {
            max_header_bytes: limit,
            ..Default::default()
        });
        decoder.push(&bytes).unwrap();
        assert_eq!(decoder.next_frame().is_ok(), success);
        assert_eq!(decoder.buffered_bytes(), 0);
    }
    // Successful clear releases partial data and closes intake.
    let mut decoder = SmithyEventStreamDecoder::new(exact);
    decoder.push(&[0, 0, 0]).unwrap();
    decoder.clear();
    assert_eq!(decoder.buffered_bytes(), 0);
    assert!(decoder.next_frame().unwrap().is_none());
    assert!(decoder.push(&bytes).is_err());
}
#[test]
fn queued_message_is_independent_and_short_eof_is_only_framing_success() {
    let first = frame(&[], b"a");
    let second = frame(&[], b"b");
    let mut decoder = SmithyEventStreamDecoder::new(Default::default());
    decoder.push(&[first.clone(), second].concat()).unwrap();
    assert_eq!(decoder.next_frame().unwrap().unwrap().payload, b"a");
    assert_eq!(decoder.next_frame().unwrap().unwrap().payload, b"b");
    for tail in 1..=3 {
        let mut decoder = SmithyEventStreamDecoder::new(Default::default());
        decoder.push(&first).unwrap();
        decoder.push(&[0, 0, 0][..tail]).unwrap();
        decoder.finish();
        assert!(decoder.next_frame().unwrap().is_some());
        assert!(decoder.next_frame().unwrap().is_none());
        assert_eq!(decoder.buffered_bytes(), 0);
    }
}
