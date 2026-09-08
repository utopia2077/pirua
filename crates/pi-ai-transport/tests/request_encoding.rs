use pi_ai_transport::{TransportErrorKind, ZstdRequestLimits, compress_zstd_request};
use serde_json::{Value, json};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/ai-transport/zstd-request/trace.json"
    ))
    .unwrap()
}

fn body(case: &Value) -> Vec<u8> {
    if let Some(count) = case["count"].as_u64() {
        return (0..count)
            .map(|i| {
                format!(
                    "{{\"id\":{i},\"text\":\"the repeated long prefix item {}\",\"group\":{}}}\n",
                    i % 17,
                    i % 13
                )
            })
            .collect::<String>()
            .into_bytes();
    }
    let text = case["text"].as_str().unwrap();
    if let Some(length) = case["length"].as_u64() {
        let length = length as usize;
        text.repeat(length.div_ceil(text.len())).as_bytes()[..length].to_vec()
    } else {
        text.as_bytes().to_vec()
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn pinned_level_three_frames_and_cross_decode_vectors() {
    let corpus = fixture();
    assert_eq!(corpus["cases"].as_array().unwrap().len(), 14);
    let mut vectors = Vec::new();
    for case in corpus["cases"].as_array().unwrap() {
        let input = body(case);
        assert_eq!(input.len() as u64, case["inputBytes"].as_u64().unwrap());
        let output = compress_zstd_request(
            &input,
            ZstdRequestLimits {
                max_input_bytes: input.len(),
                max_output_bytes: zstd_safe::compress_bound(input.len()),
            },
        )
        .unwrap();
        assert_eq!(
            hex(&output),
            case["encoded"].as_str().unwrap(),
            "{}",
            case["id"]
        );
        let encoded = case["encoded"].as_str().unwrap();
        let source_bytes: Vec<_> = (0..encoded.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&encoded[i..i + 2], 16).unwrap())
            .collect();
        let mut decoded = vec![0; input.len()];
        let written = zstd_safe::decompress(decoded.as_mut_slice(), &source_bytes).unwrap();
        assert_eq!(written, input.len());
        assert_eq!(decoded, input);
        vectors.push(json!({"id":case["id"],"encoded":hex(&output)}));
    }
    // Consumed by the separate actual-Node cross-decoder; never an expectation.
    println!("ZSTD_CROSS_DECODE={}", json!(vectors));
}

#[test]
fn exact_input_output_and_empty_capacity_boundaries() {
    for case in fixture()["cases"].as_array().unwrap() {
        let input = body(case);
        let needed = case["encoded"].as_str().unwrap().len() / 2;
        for extra in [0, 1] {
            let limits = ZstdRequestLimits {
                max_input_bytes: input.len(),
                max_output_bytes: needed + extra,
            };
            let encoded = compress_zstd_request(&input, limits).unwrap();
            assert_eq!(hex(&encoded), case["encoded"].as_str().unwrap());
        }
        for max_output_bytes in [0, needed - 1] {
            assert_eq!(
                compress_zstd_request(
                    &input,
                    ZstdRequestLimits {
                        max_input_bytes: input.len(),
                        max_output_bytes
                    }
                )
                .unwrap_err()
                .kind(),
                TransportErrorKind::LimitExceeded
            );
        }
        if !input.is_empty() {
            assert_eq!(
                compress_zstd_request(
                    &input,
                    ZstdRequestLimits {
                        max_input_bytes: input.len() - 1,
                        max_output_bytes: usize::MAX
                    }
                )
                .unwrap_err()
                .kind(),
                TransportErrorKind::LimitExceeded
            );
        }
    }
}

#[test]
fn explicit_limits_redact_and_large_allowance_does_not_require_large_output() {
    let limits = ZstdRequestLimits {
        max_input_bytes: usize::MAX,
        max_output_bytes: usize::MAX,
    };
    assert_eq!(format!("{limits:?}"), "ZstdRequestLimits([REDACTED])");
    let result = compress_zstd_request(b"{}", limits).unwrap();
    assert_eq!(result.len(), 11);
    assert!(result.capacity() <= zstd_safe::compress_bound(2));
    let error = compress_zstd_request(
        b"synthetic-private-body",
        ZstdRequestLimits {
            max_input_bytes: 0,
            max_output_bytes: 0,
        },
    )
    .unwrap_err();
    assert!(!format!("{error:?} {error}").contains("synthetic-private-body"));
}
