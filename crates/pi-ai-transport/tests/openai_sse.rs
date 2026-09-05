use pi_ai_transport::{SseDecoder, SseLimits, SseProfile, TransportErrorKind};
use serde_json::{Value, json};

#[test]
fn sdk_decoded_growth_obeys_continuous_queue_bound() {
    // Eleven raw bytes become seventeen normalized frame bytes plus held CR.
    for cap in [11, 17, 18, 19] {
        let mut decoder = SseDecoder::new(
            SseProfile::OpenAI,
            SseLimits {
                max_buffer_bytes: cap,
                max_frame_bytes: 32,
                max_frames_per_feed: 1,
            },
        );
        decoder.push(b"data:\xff\xff\xff\xff\r\r").unwrap();
        let result = decoder.next_frame();
        if cap < 18 {
            assert_eq!(
                result.unwrap_err().kind(),
                TransportErrorKind::LimitExceeded
            );
            assert_eq!(
                decoder.push(&[]).unwrap_err().kind(),
                TransportErrorKind::Closed
            );
            assert_eq!(
                decoder.next_frame().unwrap_err().kind(),
                TransportErrorKind::Closed
            );
        } else {
            assert!(result.unwrap().is_none());
            decoder.push(&[]).unwrap();
            decoder.end_input();
            assert_eq!(
                decoder.next_frame().unwrap().unwrap().data,
                "\u{fffd}".repeat(4)
            );
            assert!(decoder.next_frame().unwrap().is_none());
        }
    }
}

#[test]
fn sdk_queue_bound_counts_existing_frame_and_waiting_raw_tail() {
    // First line charge7; separator1; replacement line charge11; raw tail2.
    let wire = b"data: a\ndata:\xff\xff\r\rX";
    assert_eq!(wire.len(), 18);
    for cap in [20, 21, 22] {
        let mut decoder = SseDecoder::new(
            SseProfile::OpenAI,
            SseLimits {
                max_buffer_bytes: cap,
                max_frame_bytes: 100,
                max_frames_per_feed: 1,
            },
        );
        decoder.push(wire).unwrap();
        let result = decoder.next_frame();
        if cap < 21 {
            assert_eq!(
                result.unwrap_err().kind(),
                TransportErrorKind::LimitExceeded
            );
        } else {
            assert!(result.unwrap().is_none());
            decoder.push(&[]).unwrap();
        }
    }
}

#[test]
fn sdk_eof_flush_has_no_extra_intake_byte_charge() {
    for wire in [
        b"data:x".as_slice(),
        b"data:x\r",
        b"data:x\r\r",
        b"data:x\ry",
    ] {
        let mut decoder = SseDecoder::new(
            SseProfile::OpenAI,
            SseLimits {
                max_buffer_bytes: wire.len(),
                max_frame_bytes: 100,
                max_frames_per_feed: 1,
            },
        );
        decoder.push(wire).unwrap();
        decoder.end_input();
        let result = decoder.next_frame().unwrap();
        assert_eq!(result.is_some(), wire.ends_with(b"\r\r"));
    }
}

fn drain(decoder: &mut SseDecoder, frames: &mut Vec<Value>) {
    while let Some(frame) = decoder.next_frame().unwrap() {
        frames.push(json!({"event":frame.event,"data":frame.data}));
    }
}

#[test]
fn sdk_chunk_gate_line_decoder_eof_and_boms_match_every_byte_split() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/openai-sse/frames.json"
    ))
    .unwrap();
    assert_eq!(fixture["evidenceClass"], "executed-installed-sdk-iterator");
    for case in fixture["cases"].as_array().unwrap() {
        let bytes: Vec<u8> = serde_json::from_value(case["bytes"].clone()).unwrap();
        assert_eq!(case["splits"].as_array().unwrap().len(), bytes.len() + 1);
        for split in case["splits"].as_array().unwrap() {
            let index = split["split"].as_u64().unwrap() as usize;
            let mut decoder = SseDecoder::new(SseProfile::OpenAI, SseLimits::default());
            let mut frames = Vec::new();
            decoder.push(&bytes[..index]).unwrap();
            drain(&mut decoder, &mut frames);
            assert_eq!(
                json!(frames),
                split["stages"][0],
                "{} split {index} first",
                case["id"]
            );
            decoder.push(&bytes[index..]).unwrap();
            drain(&mut decoder, &mut frames);
            assert_eq!(
                json!(frames),
                split["stages"][1],
                "{} split {index} second",
                case["id"]
            );
            decoder.end_input();
            drain(&mut decoder, &mut frames);
            assert_eq!(
                json!(frames),
                split["stages"][2],
                "{} split {index} EOF",
                case["id"]
            );
        }
    }
}

#[test]
fn sdk_profile_intake_normalized_frame_and_per_feed_limits_are_exact() {
    let wire = b"data: x\n\n";
    let limits = SseLimits {
        max_buffer_bytes: wire.len(),
        max_frame_bytes: 7,
        max_frames_per_feed: 1,
    };
    let mut decoder = SseDecoder::new(SseProfile::OpenAI, limits);
    decoder.push(wire).unwrap();
    assert_eq!(decoder.next_frame().unwrap().unwrap().data, "x");
    assert!(decoder.next_frame().unwrap().is_none());
    let mut decoder = SseDecoder::new(
        SseProfile::OpenAI,
        SseLimits {
            max_buffer_bytes: wire.len() - 1,
            ..limits
        },
    );
    assert_eq!(
        decoder.push(wire).unwrap_err().kind(),
        TransportErrorKind::LimitExceeded
    );
    let mut decoder = SseDecoder::new(
        SseProfile::OpenAI,
        SseLimits {
            max_frame_bytes: 6,
            ..limits
        },
    );
    decoder.push(wire).unwrap();
    assert_eq!(
        decoder.next_frame().unwrap_err().kind(),
        TransportErrorKind::LimitExceeded
    );
    assert_eq!(
        decoder.next_frame().unwrap_err().kind(),
        TransportErrorKind::Closed
    );
    let mut decoder = SseDecoder::new(
        SseProfile::OpenAI,
        SseLimits {
            max_buffer_bytes: 100,
            ..limits
        },
    );
    decoder.push(b"data: x\n\ndata: y\n\n").unwrap();
    assert!(decoder.next_frame().unwrap().is_some());
    assert_eq!(
        decoder.next_frame().unwrap_err().kind(),
        TransportErrorKind::LimitExceeded
    );
    let mut decoder = SseDecoder::new(SseProfile::OpenAI, limits);
    decoder.push(wire).unwrap();
    decoder.next_frame().unwrap();
    decoder.push(wire).unwrap();
    assert!(decoder.next_frame().unwrap().is_some());
}
