use pi_ai_transport::{SseDecoder, SseLimits, SseProfile, TransportErrorKind};
use serde_json::{Value, json};
fn drain(decoder: &mut SseDecoder, frames: &mut Vec<Value>) {
    while let Some(frame) = decoder.next_frame().unwrap() {
        frames.push(json!({"event":frame.event,"data":frame.data}));
    }
}
#[test]
fn pinned_private_parser_every_split_stage_and_bytewise() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/anthropic-sse/frames.json"
    ))
    .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let bytes: Vec<u8> = serde_json::from_value(case["bytes"].clone()).unwrap();
        for split in case["splits"].as_array().unwrap() {
            let n = split["split"].as_u64().unwrap() as usize;
            let mut d = SseDecoder::new(SseProfile::Anthropic, SseLimits::default());
            let mut frames = Vec::new();
            for (i, part) in [&bytes[..n], &bytes[n..]].into_iter().enumerate() {
                d.push(part).unwrap();
                drain(&mut d, &mut frames);
                assert_eq!(
                    json!(frames),
                    split["stages"][i],
                    "{} split{n} stage{i}",
                    case["id"]
                );
            }
            d.end_input();
            drain(&mut d, &mut frames);
            assert_eq!(
                json!(frames),
                split["stages"][2],
                "{} EOF split{n}",
                case["id"]
            );
        }
        let mut d = SseDecoder::new(SseProfile::Anthropic, SseLimits::default());
        let mut frames = Vec::new();
        for (i, b) in bytes.iter().enumerate() {
            d.push(&[*b]).unwrap();
            drain(&mut d, &mut frames);
            assert_eq!(json!(frames), case["bytewise"][i], "{} byte{i}", case["id"]);
        }
        d.end_input();
        drain(&mut d, &mut frames);
        assert_eq!(json!(frames), case["bytewise"][bytes.len()]);
    }
}
#[test]
fn queued_pushes_preserve_cr_split_without_eager_frames() {
    let mut d = SseDecoder::new(SseProfile::Anthropic, SseLimits::default());
    d.push(b"event: e\r").unwrap();
    d.push(b"\ndata: x\r\n\r\n").unwrap();
    let first = d.next_frame().unwrap().unwrap();
    assert_eq!(first.event.as_deref(), Some("e"));
    assert!(first.data.is_empty());
    let second = d.next_frame().unwrap().unwrap();
    assert_eq!(second.event, None);
    assert_eq!(second.data, "x");
}

#[test]
fn a_leading_empty_line_consumes_stream_bom_position() {
    for newline in *b"\n\r" {
        let mut d = SseDecoder::new(SseProfile::Anthropic, SseLimits::default());
        d.push(&[newline]).unwrap();
        assert!(d.next_frame().unwrap().is_none());
        d.push(b"\xef\xbb\xbfdata: ignored\n\n").unwrap();
        assert!(d.next_frame().unwrap().is_none());
    }
}

#[test]
fn exact_queue_frame_feed_and_decoded_growth_limits() {
    let wire = b"data: x\n\n";
    let charge = wire.len() + size_of::<usize>();
    for cap in [charge - 1, charge, charge + 1] {
        let mut d = SseDecoder::new(
            SseProfile::Anthropic,
            SseLimits {
                max_buffer_bytes: cap,
                max_frame_bytes: 7,
                max_frames_per_feed: 1,
            },
        );
        let result = d.push(wire);
        if cap < charge {
            assert_eq!(
                result.unwrap_err().kind(),
                TransportErrorKind::LimitExceeded
            );
        } else {
            result.unwrap();
            assert_eq!(d.next_frame().unwrap().unwrap().data, "x");
        }
    }
    for cap in [6, 7, 8] {
        let mut d = SseDecoder::new(
            SseProfile::Anthropic,
            SseLimits {
                max_frame_bytes: cap,
                ..SseLimits::default()
            },
        );
        d.push(wire).unwrap();
        if cap < 7 {
            assert_eq!(
                d.next_frame().unwrap_err().kind(),
                TransportErrorKind::LimitExceeded
            );
        } else {
            assert_eq!(d.next_frame().unwrap().unwrap().data, "x");
        }
    }
    // Thirteen source bytes plus one chunk marker fit; decoding requires29bytes.
    for cap in [28, 29, 30] {
        let mut d = SseDecoder::new(
            SseProfile::Anthropic,
            SseLimits {
                max_buffer_bytes: cap,
                max_frame_bytes: 100,
                max_frames_per_feed: 1,
            },
        );
        d.push(b"data:\xff\xff\xff\xff\xff\xff\xff\xff").unwrap();
        let result = d.next_frame();
        if cap < 29 {
            assert_eq!(
                result.unwrap_err().kind(),
                TransportErrorKind::LimitExceeded
            );
            assert_eq!(d.push(&[]).unwrap_err().kind(), TransportErrorKind::Closed);
        } else {
            assert!(result.unwrap().is_none());
            d.end_input();
            assert_eq!(d.next_frame().unwrap().unwrap().data, "\u{fffd}".repeat(8));
        }
    }
    for limit in [0, 1, 2] {
        let mut d = SseDecoder::new(
            SseProfile::Anthropic,
            SseLimits {
                max_frames_per_feed: limit,
                ..SseLimits::default()
            },
        );
        d.push(b"data: a\n\ndata: b\n\n").unwrap();
        for _ in 0..limit {
            assert!(d.next_frame().unwrap().is_some());
        }
        if limit < 2 {
            assert_eq!(
                d.next_frame().unwrap_err().kind(),
                TransportErrorKind::LimitExceeded
            );
        } else {
            assert!(d.next_frame().unwrap().is_none());
        }
    }
}
#[test]
fn terminal_precedes_uninterpreted_overlimit_tail_and_feed_resets() {
    let mut d = SseDecoder::new(
        SseProfile::Anthropic,
        SseLimits {
            max_frame_bytes: 7,
            max_buffer_bytes: 200,
            max_frames_per_feed: 1,
        },
    );
    d.push(b"data: x\n\ndata: enormous unterminated trailing frame")
        .unwrap();
    assert_eq!(d.next_frame().unwrap().unwrap().data, "x");
    assert_eq!(
        d.next_frame().unwrap_err().kind(),
        TransportErrorKind::LimitExceeded
    );
    let mut d = SseDecoder::new(
        SseProfile::Anthropic,
        SseLimits {
            max_frames_per_feed: 1,
            ..SseLimits::default()
        },
    );
    for _ in 0..3 {
        d.push(b"data: x\n\n").unwrap();
        assert!(d.next_frame().unwrap().is_some());
        assert!(d.next_frame().unwrap().is_none());
    }
}
