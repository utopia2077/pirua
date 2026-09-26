use std::num::NonZeroUsize;

use pi_extension_protocol::wire::framing::{FrameError, FrameReader, RawFrameReader, RawReadError};
use pi_extension_protocol::wire::v1_0;
use serde_json::Value;

fn limit(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn ready() -> &'static [u8] {
    include_bytes!("../schema/fixtures/ready-1.0.frame.bin")
}

fn long_request() -> &'static [u8] {
    include_bytes!("../schema/fixtures/long-request-1.0.frame.bin")
}

fn frame_error(error: FrameError) -> RawReadError {
    RawReadError::Frame(error)
}

#[test]
fn exact_limit_body_survives_every_split_and_bytewise_input() {
    let frame = long_request();
    assert_eq!(frame.len(), 154);
    assert_eq!(&frame[..2], &[0x98, 1]);

    for split in 0..=frame.len() {
        let mut backing = [0x55; 152];
        let mut reader = RawFrameReader::new(limit(152), &mut backing).unwrap();
        let first = reader.read_one(&frame[..split]).unwrap();
        assert_eq!(first.consumed, split);
        if split == frame.len() {
            let raw = first.frame.unwrap();
            assert_eq!(raw.body_len, 152);
            assert_eq!(raw.body, &frame[2..]);
        } else {
            assert!(first.frame.is_none());
            assert_eq!(reader.read_one(&[]).unwrap().consumed, 0);
            let second = reader.read_one(&frame[split..]).unwrap();
            assert_eq!(second.consumed, frame.len() - split);
            let raw = second.frame.unwrap();
            assert_eq!(raw.body_len, 152);
            assert_eq!(raw.body, &frame[2..]);
        }
        reader.finish().unwrap();
    }

    let mut backing = [0; 152];
    let mut reader = RawFrameReader::new(limit(152), &mut backing).unwrap();
    for (index, byte) in frame.iter().enumerate() {
        let step = reader.read_one(std::slice::from_ref(byte)).unwrap();
        assert_eq!(step.consumed, 1);
        if index + 1 == frame.len() {
            assert_eq!(step.frame.unwrap().body, &frame[2..]);
        } else {
            assert!(step.frame.is_none());
        }
    }
    reader.finish().unwrap();
}

#[test]
fn coalesced_tail_waits_for_ready_borrow_and_limit_transition() {
    let mut input = ready().to_vec();
    input.extend_from_slice(long_request());
    let mut backing = [0; 152];
    let mut reader = RawFrameReader::new(limit(2), &mut backing).unwrap();

    let first_end = {
        let step = reader.read_one(&input).unwrap();
        assert_eq!(step.consumed, ready().len());
        let raw = step.frame.unwrap();
        assert_eq!(raw.body_len, 2);
        assert_eq!(raw.body, &ready()[1..]);
        step.consumed
    };
    reader.use_negotiated_limit(limit(152)).unwrap();
    let step = reader.read_one(&input[first_end..]).unwrap();
    assert_eq!(step.consumed, long_request().len());
    assert_eq!(step.frame.unwrap().body, &long_request()[2..]);
    reader.finish().unwrap();

    // A complete frame resets the reader to a reusable boundary.
    let again = reader.read_one(ready()).unwrap();
    assert_eq!(again.consumed, ready().len());
    assert_eq!(again.frame.unwrap().body, &ready()[1..]);
}

#[test]
fn local_capacity_failure_preserves_limit_and_transition_order() {
    let mut short_pre = [0; 1];
    assert!(matches!(
        RawFrameReader::new(limit(2), &mut short_pre),
        Err(RawReadError::StorageTooSmall {
            required: 2,
            capacity: 1
        })
    ));

    let mut backing = [0; 2];
    let mut reader = RawFrameReader::new(limit(2), &mut backing).unwrap();
    assert_eq!(
        reader.use_negotiated_limit(limit(3)),
        Err(RawReadError::StorageTooSmall {
            required: 3,
            capacity: 2
        })
    );
    assert_eq!(
        reader.read_one(ready()).unwrap().frame.unwrap().body,
        &ready()[1..]
    );
    assert_eq!(
        reader.read_one(&[3]).unwrap_err(),
        frame_error(FrameError::FrameTooLarge {
            length: 3,
            limit: 2
        })
    );
    assert_eq!(
        reader.use_negotiated_limit(limit(3)),
        Err(frame_error(FrameError::Poisoned))
    );

    let mut backing = [0; 2];
    let mut reader = RawFrameReader::new(limit(1), &mut backing).unwrap();
    assert_eq!(
        reader.use_negotiated_limit(limit(3)),
        Err(RawReadError::StorageTooSmall {
            required: 3,
            capacity: 2
        })
    );
    reader.use_negotiated_limit(limit(2)).unwrap();
    assert_eq!(
        reader.read_one(ready()).unwrap().frame.unwrap().body,
        &ready()[1..]
    );

    let mut backing = [0; 2];
    let mut reader = RawFrameReader::new(limit(2), &mut backing).unwrap();
    assert_eq!(reader.read_one(&[0x80]).unwrap().consumed, 1);
    assert_eq!(
        reader.use_negotiated_limit(limit(3)),
        Err(frame_error(FrameError::TransitionNotAtBoundary))
    );
    assert_eq!(
        reader.finish(),
        Err(frame_error(FrameError::TruncatedPrefix))
    );

    let mut backing = [0; 2];
    let mut reader = RawFrameReader::new(limit(2), &mut backing).unwrap();
    assert_eq!(reader.read_one(&ready()[..2]).unwrap().consumed, 2);
    assert_eq!(
        reader.use_negotiated_limit(limit(3)),
        Err(frame_error(FrameError::TransitionNotAtBoundary))
    );
    assert_eq!(
        reader.read_one(&ready()[2..]).unwrap().frame.unwrap().body,
        &ready()[1..]
    );
    reader.use_negotiated_limit(limit(2)).unwrap();
    assert_eq!(
        reader.use_negotiated_limit(limit(3)),
        Err(frame_error(FrameError::AlreadyNegotiated))
    );
}

#[test]
fn malformed_and_over_limit_prefixes_poison_before_body_copy() {
    let invalid: &[(&[u8], FrameError)] = &[
        (&[0], FrameError::ZeroLength),
        (&[0x80, 0], FrameError::NonCanonicalLength),
        (&[0x82, 0, 0x2a], FrameError::NonCanonicalLength),
        (
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 2],
            FrameError::LengthOverflow,
        ),
        (
            &[
                0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0,
            ],
            FrameError::LengthOverflow,
        ),
        (
            &[3, 0xaa, 0xbb, 0xcc],
            FrameError::FrameTooLarge {
                length: 3,
                limit: 2,
            },
        ),
    ];
    for (input, expected) in invalid {
        let mut backing = [0x55; 2];
        {
            let mut reader = RawFrameReader::new(limit(2), &mut backing).unwrap();
            assert_eq!(
                reader.read_one(input).unwrap_err(),
                frame_error(expected.clone())
            );
            assert_eq!(
                reader.read_one(ready()).unwrap_err(),
                frame_error(FrameError::Poisoned)
            );
            assert_eq!(reader.finish(), Err(frame_error(FrameError::Poisoned)));
            assert_eq!(
                reader.use_negotiated_limit(limit(3)),
                Err(frame_error(FrameError::Poisoned))
            );
        }
        assert_eq!(backing, [0x55; 2]);
    }
}

#[test]
fn eof_reports_exact_remaining_bytes_and_clean_eof_is_reusable() {
    let mut backing = [0; 152];
    let mut prefix = RawFrameReader::new(limit(152), &mut backing).unwrap();
    assert!(prefix.read_one(&[0x98]).unwrap().frame.is_none());
    assert_eq!(
        prefix.finish(),
        Err(frame_error(FrameError::TruncatedPrefix))
    );
    assert_eq!(prefix.finish(), Err(frame_error(FrameError::Poisoned)));

    let mut backing = [0; 152];
    let mut body = RawFrameReader::new(limit(152), &mut backing).unwrap();
    assert!(body.read_one(&long_request()[..3]).unwrap().frame.is_none());
    assert_eq!(
        body.finish(),
        Err(frame_error(FrameError::TruncatedBody { remaining: 151 }))
    );
    assert_eq!(
        body.read_one(&[]).unwrap_err(),
        frame_error(FrameError::Poisoned)
    );

    let mut backing = [0; 2];
    let mut clean = RawFrameReader::new(limit(2), &mut backing).unwrap();
    assert_eq!(clean.read_one(&[]).unwrap().consumed, 0);
    clean.finish().unwrap();
    assert_eq!(
        clean.read_one(ready()).unwrap().frame.unwrap().body,
        &ready()[1..]
    );
    clean.finish().unwrap();
}

#[test]
fn raw_body_can_be_an_invalid_envelope_and_retains_transmitted_length() {
    let mut backing = [0; 139];
    let mut raw = RawFrameReader::new(limit(139), &mut backing).unwrap();
    let step = raw.read_one(&[1, 0xff]).unwrap();
    assert_eq!(step.consumed, 2);
    assert_eq!(step.frame.unwrap().body, &[0xff]);
    raw.finish().unwrap();

    let mut decoded = FrameReader::<v1_0::Envelope>::new(limit(139));
    assert_eq!(
        decoded.read_one(&[1, 0xff]).unwrap_err(),
        FrameError::InvalidEnvelope
    );

    let vectors: Value =
        serde_json::from_str(include_str!("../schema/fixtures/raw-vectors.json")).unwrap();
    let hex =
        vectors["selected_minor_1_0_forbids_1_1_tool_progress"]["enveloped_progress"]["frame_hex"]
            .as_str()
            .unwrap();
    let (pairs, remainder) = hex.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    let bytes: Vec<u8> = pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect();
    assert_eq!(bytes.len(), 141);
    let step = raw.read_one(&bytes).unwrap();
    assert_eq!(step.consumed, bytes.len());
    let frame = step.frame.unwrap();
    assert_eq!(frame.body_len, 139);
    assert_eq!(frame.body, &bytes[2..]);
}
