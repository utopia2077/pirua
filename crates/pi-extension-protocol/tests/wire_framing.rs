use std::num::NonZeroUsize;

use pi_extension_protocol::wire::framing::{
    DecodedFrame, EnvelopeMessage, FrameError, FrameReader, encode,
};
use pi_extension_protocol::wire::{v1_0, v1_1};
use prost::Message;
use serde_json::Value;

fn limit(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn unhex(value: &str) -> Vec<u8> {
    let (pairs, remainder) = value.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn vectors() -> Value {
    serde_json::from_str(include_str!("../schema/fixtures/raw-vectors.json")).unwrap()
}

fn raw_frame(kind: &str) -> Vec<u8> {
    unhex(vectors()[kind]["frame_hex"].as_str().unwrap())
}

fn ready_frame() -> &'static [u8] {
    include_bytes!("../schema/fixtures/ready-1.0.frame.bin")
}

fn long_request_frame() -> &'static [u8] {
    include_bytes!("../schema/fixtures/long-request-1.0.frame.bin")
}

// Exercise concatenated fixed-limit frames. Endpoint callers instead handle
// each `read_one` result before passing the remaining chunk to the reader.
fn read_all<E: EnvelopeMessage>(
    reader: &mut FrameReader<E>,
    mut input: &[u8],
) -> Result<Vec<DecodedFrame<E>>, FrameError> {
    let mut frames = Vec::new();
    while !input.is_empty() {
        let step = reader.read_one(input)?;
        input = &input[step.consumed..];
        if let Some(frame) = step.frame {
            frames.push(frame);
        }
    }
    Ok(frames)
}

#[test]
fn canonical_ready_and_long_request_round_trip_across_every_split() {
    assert_eq!(ready_frame(), [2, 0x2a, 0]);
    assert_eq!(&long_request_frame()[..2], [0x98, 1]);
    let echo_request = raw_frame("request");

    for (frame, expected_body_len) in [
        (ready_frame(), 2),
        (long_request_frame(), 152),
        (echo_request.as_slice(), 219),
    ] {
        for split in 0..=frame.len() {
            let mut reader = FrameReader::<v1_0::Envelope>::new(limit(219));
            assert!(
                read_all(&mut reader, &frame[..split]).unwrap().is_empty() || split == frame.len()
            );
            let decoded = read_all(&mut reader, &frame[split..]).unwrap();
            assert_eq!(decoded.len(), usize::from(split != frame.len()));
            if let Some(decoded) = decoded.first() {
                assert_eq!(decoded.body_len, expected_body_len);
                let encoded = encode(&decoded.envelope, limit(219)).unwrap();
                assert_eq!(encoded.body_len, expected_body_len);
                assert_eq!(encoded.bytes, frame);
            }
            reader.finish().unwrap();
        }
    }
}

#[test]
fn bytewise_chunks_and_concatenated_frames_preserve_each_body_length() {
    let frames = [
        ready_frame().to_vec(),
        raw_frame("request"),
        raw_frame("progress"),
        raw_frame("terminal"),
    ];
    let joined = frames.concat();

    let mut bytewise = FrameReader::<v1_0::Envelope>::new(limit(219));
    let mut lengths = Vec::new();
    for byte in joined.chunks(1) {
        lengths.extend(
            read_all(&mut bytewise, byte)
                .unwrap()
                .into_iter()
                .map(|frame| frame.body_len),
        );
    }
    bytewise.finish().unwrap();
    assert_eq!(lengths, [2, 219, 132, 126]);

    let mut together = FrameReader::<v1_0::Envelope>::new(limit(219));
    let decoded = read_all(&mut together, &joined).unwrap();
    assert_eq!(
        decoded
            .iter()
            .map(|frame| frame.body_len)
            .collect::<Vec<_>>(),
        lengths
    );
    assert!(read_all(&mut together, &[]).unwrap().is_empty());
    together.finish().unwrap();
}

#[test]
fn fixed_pre_handshake_cap_applies_before_body_and_grant_changes_only_at_boundary() {
    let mut exact = FrameReader::<v1_0::Envelope>::new(limit(2));
    assert_eq!(read_all(&mut exact, ready_frame()).unwrap()[0].body_len, 2);
    exact.finish().unwrap();

    let mut below = FrameReader::<v1_0::Envelope>::new(limit(1));
    assert_eq!(
        read_all(&mut below, &ready_frame()[..1]).unwrap_err(),
        FrameError::FrameTooLarge {
            length: 2,
            limit: 1
        }
    );
    assert_eq!(
        read_all(&mut below, &ready_frame()[1..]).unwrap_err(),
        FrameError::Poisoned
    );

    let request = raw_frame("request");
    let mut reader = FrameReader::<v1_0::Envelope>::new(limit(2));
    assert!(read_all(&mut reader, &request[..1]).unwrap().is_empty());
    assert_eq!(
        reader.use_negotiated_limit(limit(219)),
        Err(FrameError::TransitionNotAtBoundary)
    );
    assert_eq!(
        read_all(&mut reader, &request[1..2]).unwrap_err(),
        FrameError::FrameTooLarge {
            length: 219,
            limit: 2
        }
    );

    let mut granted = FrameReader::<v1_0::Envelope>::new(limit(2));
    assert_eq!(
        read_all(&mut granted, ready_frame()).unwrap()[0].body_len,
        2
    );
    granted.use_negotiated_limit(limit(219)).unwrap();
    assert_eq!(
        granted.use_negotiated_limit(limit(usize::MAX)),
        Err(FrameError::AlreadyNegotiated)
    );
    assert_eq!(read_all(&mut granted, &request).unwrap()[0].body_len, 219);
    granted.finish().unwrap();

    let mut one_below = FrameReader::<v1_0::Envelope>::new(limit(2));
    one_below.use_negotiated_limit(limit(218)).unwrap();
    assert_eq!(
        read_all(&mut one_below, &request[..2]).unwrap_err(),
        FrameError::FrameTooLarge {
            length: 219,
            limit: 218
        }
    );
}

#[test]
fn malformed_lengths_are_rejected_before_any_envelope_is_accepted() {
    let invalid = [
        (vec![0], FrameError::ZeroLength),
        (vec![0x80, 0], FrameError::NonCanonicalLength),
        (vec![0x82, 0, 0x2a, 0], FrameError::NonCanonicalLength),
        (
            vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2],
            FrameError::LengthOverflow,
        ),
        (
            vec![
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0,
            ],
            FrameError::LengthOverflow,
        ),
    ];
    for (frame, expected) in invalid {
        let mut reader = FrameReader::<v1_0::Envelope>::new(limit(219));
        assert_eq!(read_all(&mut reader, &frame).unwrap_err(), expected);
        assert_eq!(
            read_all(&mut reader, ready_frame()).unwrap_err(),
            FrameError::Poisoned
        );
    }

    let mut reader = FrameReader::<v1_0::Envelope>::new(limit(219));
    let mut mixed = ready_frame().to_vec();
    mixed.extend_from_slice(&[0x80, 0]);
    assert_eq!(
        read_all(&mut reader, &mixed).unwrap_err(),
        FrameError::NonCanonicalLength
    );
    assert_eq!(reader.finish().unwrap_err(), FrameError::Poisoned);
}

#[test]
fn truncated_prefix_body_and_invalid_envelope_close_reader() {
    let mut prefix = FrameReader::<v1_0::Envelope>::new(limit(219));
    assert!(read_all(&mut prefix, &[0xdb]).unwrap().is_empty());
    assert_eq!(prefix.finish().unwrap_err(), FrameError::TruncatedPrefix);

    let mut body = FrameReader::<v1_0::Envelope>::new(limit(219));
    assert!(
        read_all(&mut body, &long_request_frame()[..3])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        body.finish().unwrap_err(),
        FrameError::TruncatedBody { remaining: 151 }
    );

    for invalid in [&[1, 0xff][..], &[1, 0][..], &[2, 0x2a, 1][..]] {
        let mut reader = FrameReader::<v1_0::Envelope>::new(limit(219));
        assert_eq!(
            read_all(&mut reader, invalid).unwrap_err(),
            FrameError::InvalidEnvelope
        );
        assert_eq!(
            read_all(&mut reader, ready_frame()).unwrap_err(),
            FrameError::Poisoned
        );
    }
}

#[test]
fn progress_credit_charge_is_132_envelope_body_bytes_excluding_two_prefix_bytes() {
    let vectors = vectors();
    let progress = raw_frame("progress");
    assert_eq!(
        &progress[..2],
        unhex(vectors["progress"]["prefix_hex"].as_str().unwrap())
    );
    assert_eq!(progress.len(), 134);
    assert_eq!(vectors["credit_edge"]["charged_envelope_body_bytes"], 132);
    assert_eq!(vectors["credit_edge"]["prefix_bytes_excluded"], 2);

    let mut reader = FrameReader::<v1_0::Envelope>::new(limit(132));
    let frame = read_all(&mut reader, &progress).unwrap().pop().unwrap();
    assert_eq!(frame.body_len, 132);
    let v1_0::envelope::Frame::StreamItem(_) = frame.envelope.frame.unwrap() else {
        panic!("expected progress item");
    };
    let terminal = read_all(&mut reader, &raw_frame("terminal")).unwrap();
    assert_eq!(terminal[0].body_len, 126);
    assert!(matches!(
        terminal[0].envelope.frame,
        Some(v1_0::envelope::Frame::StreamEnd(_))
    ));
}

#[test]
fn selected_v1_1_envelope_uses_the_same_physical_boundary() {
    let vector = &vectors()["selected_minor_1_0_forbids_1_1_tool_progress"]["enveloped_progress"];
    let bytes = unhex(vector["frame_hex"].as_str().unwrap());
    let mut reader = FrameReader::<v1_1::Envelope>::new(limit(134));
    let frame = reader.read_one(&bytes).unwrap();
    assert_eq!(frame.consumed, bytes.len());
    let frame = frame.frame.unwrap();
    assert_eq!(frame.body_len, 134);
    assert_eq!(encode(&frame.envelope, limit(134)).unwrap().bytes, bytes);
    reader.finish().unwrap();
}

#[test]
fn encoder_checks_body_limit_and_rejects_empty_envelope() {
    let ready = v1_0::Envelope::decode(&ready_frame()[1..]).unwrap();
    assert_eq!(encode(&ready, limit(2)).unwrap().bytes, ready_frame());
    assert_eq!(
        encode(&ready, limit(1)).unwrap_err(),
        FrameError::FrameTooLarge {
            length: 2,
            limit: 1
        }
    );
    assert_eq!(
        encode(&v1_0::Envelope { frame: None }, limit(2)).unwrap_err(),
        FrameError::InvalidEnvelope
    );
}

#[test]
fn coalesced_handshake_can_switch_limit_at_frame_boundary() {
    let mut input = ready_frame().to_vec();
    input.extend_from_slice(&raw_frame("request"));
    let mut reader = FrameReader::<v1_0::Envelope>::new(limit(2));
    let ready = reader.read_one(&input).unwrap();
    assert_eq!(ready.consumed, ready_frame().len());
    assert_eq!(ready.frame.unwrap().body_len, 2);
    reader.use_negotiated_limit(limit(219)).unwrap();
    let request = reader.read_one(&input[ready.consumed..]).unwrap();
    assert_eq!(request.consumed, input.len() - ready.consumed);
    assert_eq!(request.frame.unwrap().body_len, 219);
    reader.finish().unwrap();

    let mut capped = FrameReader::<v1_0::Envelope>::new(limit(2));
    assert_eq!(capped.read_one(&input).unwrap().consumed, 3);
    assert_eq!(
        capped.read_one(&input[3..5]).unwrap_err(),
        FrameError::FrameTooLarge {
            length: 219,
            limit: 2
        }
    );
}
