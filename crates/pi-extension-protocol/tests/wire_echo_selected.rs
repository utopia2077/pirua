use std::num::NonZeroUsize;

use pi_extension_protocol::jcs::{self, admit_schema};
use pi_extension_protocol::wire::echo_bounded::{
    IndexStorage, NormalizationWorkBound, ScanError, SelectedMinor,
};
use pi_extension_protocol::wire::echo_selected::{
    ActiveArm, AdmissionError, ResultTerminal, RoleError, SelectError, select_active,
};
use pi_extension_protocol::wire::{v1_0, v1_1, value};
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

fn selected_projection<'a>(id: &'a str, schema: &'a jcs::PortableSchema) -> value::Projection<'a> {
    value::Projection {
        id,
        digest_sha256: schema.canonical().sha256(),
        schema,
    }
}

fn bound_bytes(id: &str, schema: &jcs::PortableSchema, raw_value: Option<&[u8]>) -> Vec<u8> {
    let mut bound = bytes(1, id.as_bytes());
    bound.extend(bytes(2, &schema.canonical().sha256()));
    if let Some(raw_value) = raw_value {
        bound.extend(bytes(3, raw_value));
    }
    bound
}

fn indexed_progress(
    payload: &[u8],
    core: SelectedMinor,
    tool: SelectedMinor,
    expected: &value::Projection<'_>,
    value_limits: value::Limits,
    instance_limits: jcs::Limits,
) -> Result<(Vec<u8>, Option<u32>, usize), AdmissionError> {
    let transmitted = body(8, &bytes(3, payload));
    let mut storage = IndexStorage::<1024, 1025>::new();
    let mut arena = storage.borrow(nz(1024), usize::MAX).unwrap();
    let view = arena.scan_envelope(&transmitted).unwrap();
    let token = select_active(view, core, nz(1024))
        .unwrap()
        .into_progress_payload()
        .unwrap();
    let admitted = token.admit(&mut arena, tool, expected, value_limits, instance_limits)?;
    Ok((admitted.json, admitted.percent_complete, admitted.body_len))
}

fn owned_progress(
    payload: &[u8],
    tool: SelectedMinor,
    expected: &value::Projection<'_>,
    value_limits: value::Limits,
    instance_limits: jcs::Limits,
) -> Result<(Vec<u8>, Option<u32>), AdmissionError> {
    if tool == SelectedMinor::V1_0 {
        let future =
            v1_1::ToolProgress::decode(payload).map_err(|_| AdmissionError::InvalidProgress)?;
        if future.percent_complete.is_some() {
            return Err(AdmissionError::UnsupportedVariant);
        }
        let progress =
            v1_0::ToolProgress::decode(payload).map_err(|_| AdmissionError::InvalidProgress)?;
        let event = progress.event.ok_or(AdmissionError::InvalidProgress)?;
        let json = value::admit_schema_bound_v1_0(&event, expected, value_limits, instance_limits)
            .map_err(AdmissionError::Value)?;
        Ok((json, None))
    } else {
        let progress =
            v1_1::ToolProgress::decode(payload).map_err(|_| AdmissionError::InvalidProgress)?;
        if progress
            .percent_complete
            .is_some_and(|percent| percent > 100)
        {
            return Err(AdmissionError::InvalidProgress);
        }
        let event = progress.event.ok_or(AdmissionError::InvalidProgress)?;
        let json = value::admit_schema_bound_v1_1(&event, expected, value_limits, instance_limits)
            .map_err(AdmissionError::Value)?;
        Ok((json, progress.percent_complete))
    }
}

fn assert_progress_pair(
    name: &str,
    payload: &[u8],
    schema: &jcs::PortableSchema,
    id: &str,
    value_limits: value::Limits,
    instance_limits: jcs::Limits,
) -> Result<(Vec<u8>, Option<u32>), AdmissionError> {
    let expected = selected_projection(id, schema);
    for tool in [SelectedMinor::V1_0, SelectedMinor::V1_1] {
        let oracle = owned_progress(payload, tool, &expected, value_limits, instance_limits);
        for core in [SelectedMinor::V1_0, SelectedMinor::V1_1] {
            let indexed = indexed_progress(
                payload,
                core,
                tool,
                &expected,
                value_limits,
                instance_limits,
            )
            .map(|(json, percent, original_len)| {
                assert_eq!(original_len, body(8, &bytes(3, payload)).len());
                (json, percent)
            });
            assert_eq!(indexed, oracle, "{name}, core={core:?}, tool={tool:?}");
        }
    }
    owned_progress(
        payload,
        SelectedMinor::V1_1,
        &expected,
        value_limits,
        instance_limits,
    )
}

#[test]
fn indexed_value_first_error_pairs_match_prost_owned_for_both_minor_axes() {
    let array = admit_schema(
        br#"{"type":"array","items":{"type":"null"}}"#,
        jcs::Limits::default(),
    )
    .unwrap();
    let object = admit_schema(br#"{"type":"object"}"#, jcs::Limits::default()).unwrap();
    let null = admit_schema(br#"{"type":"null"}"#, jcs::Limits::default()).unwrap();
    let defaults = value::Limits::default();

    let mut items = bytes(1, &bytes(6, b"bad"));
    items.extend(bytes(1, &bytes(1, &[])));
    let list = bytes(7, &items);
    let payload = bytes(1, &bound_bytes("p", &array, Some(&list)));
    assert_eq!(
        assert_progress_pair(
            "list count before invalid child",
            &payload,
            &array,
            "p",
            value::Limits {
                elements: 1,
                ..defaults
            },
            jcs::Limits::default(),
        ),
        Err(AdmissionError::Value(value::Error::Limit))
    );
    assert_eq!(
        assert_progress_pair(
            "list child after count",
            &payload,
            &array,
            "p",
            value::Limits {
                elements: 2,
                ..defaults
            },
            jcs::Limits::default(),
        ),
        Err(AdmissionError::Value(value::Error::BytesNotJson))
    );

    let first = bytes(2, &bytes(1, &[]));
    for second_value in [Some(bytes(6, b"bad")), None] {
        let second = second_value
            .as_ref()
            .map_or_else(Vec::new, |value| bytes(2, value));
        let mut entries = bytes(1, &first);
        entries.extend(bytes(1, &second));
        let payload = bytes(1, &bound_bytes("p", &object, Some(&bytes(8, &entries))));
        assert_eq!(
            assert_progress_pair(
                "omitted duplicate key before second value",
                &payload,
                &object,
                "p",
                defaults,
                jcs::Limits::default(),
            ),
            Err(AdmissionError::Value(value::Error::DuplicateKey))
        );
    }
    let one = bytes(8, &bytes(1, &first));
    let payload = bytes(1, &bound_bytes("p", &object, Some(&one)));
    assert_eq!(
        assert_progress_pair(
            "omitted key is empty key",
            &payload,
            &object,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Ok((br#"{"":null}"#.to_vec(), None))
    );

    let w = std::mem::size_of::<v1_1::WireValue>();
    let e = std::mem::size_of::<v1_1::WireMapEntry>();
    assert_eq!(w, std::mem::size_of::<v1_0::WireValue>());
    assert_eq!(e, std::mem::size_of::<v1_0::WireMapEntry>());
    for key in ["", "é"] {
        let mut first = bytes(1, key.as_bytes());
        first.extend(bytes(2, &bytes(1, &[])));
        let second = bytes(1, key.as_bytes());
        let mut entries = bytes(1, &first);
        entries.extend(bytes(1, &second));
        let payload = bytes(1, &bound_bytes("p", &object, Some(&bytes(8, &entries))));
        let threshold = 2 * w + 2 * e + 2 * key.len();
        assert_eq!(
            assert_progress_pair(
                "second key charge minus one",
                &payload,
                &object,
                "p",
                value::Limits {
                    decoded_bytes: threshold - 1,
                    ..defaults
                },
                jcs::Limits::default(),
            ),
            Err(AdmissionError::Value(value::Error::Limit))
        );
        assert_eq!(
            assert_progress_pair(
                "second key charge exact",
                &payload,
                &object,
                "p",
                value::Limits {
                    decoded_bytes: threshold,
                    ..defaults
                },
                jcs::Limits::default(),
            ),
            Err(AdmissionError::Value(value::Error::DuplicateKey))
        );
    }

    let payload = bytes(1, &bound_bytes("p", &null, Some(&[])));
    for (depth, error) in [(0, value::Error::Limit), (1, value::Error::MissingValue)] {
        assert_eq!(
            assert_progress_pair(
                "missing arm after depth and DTO charge",
                &payload,
                &null,
                "p",
                value::Limits {
                    depth,
                    decoded_bytes: w,
                    ..defaults
                },
                jcs::Limits::default(),
            ),
            Err(AdmissionError::Value(error))
        );
    }

    let mut bad = bytes(1, &bound_bytes("p", &null, Some(&bytes(6, b"bad"))));
    bad.extend(bytes(2, b"wrong wire type"));
    assert_eq!(
        assert_progress_pair(
            "full scan before value",
            &bad,
            &null,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Err(AdmissionError::InvalidProgress)
    );
    bad.truncate(bad.len() - bytes(2, b"wrong wire type").len());
    assert_eq!(
        assert_progress_pair(
            "value without later malformed",
            &bad,
            &null,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Err(AdmissionError::Value(value::Error::BytesNotJson))
    );

    let wrong = bytes(1, &bound_bytes("wrong", &null, None));
    assert_eq!(
        assert_progress_pair(
            "identity before missing value",
            &wrong,
            &null,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Err(AdmissionError::Value(value::Error::ProjectionMismatch))
    );
    let right = bytes(1, &bound_bytes("p", &null, None));
    assert_eq!(
        assert_progress_pair(
            "missing value after identity",
            &right,
            &null,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Err(AdmissionError::Value(value::Error::MissingValue))
    );
}

#[test]
fn indexed_percent_and_presence_order_match_owned_tool_versions() {
    let null = admit_schema(br#"{"type":"null"}"#, jcs::Limits::default()).unwrap();
    let defaults = value::Limits::default();
    for raw in [0, 100, 101, (1u64 << 32) + 100] {
        let payload = scalar(2, raw);
        let expected = selected_projection("p", &null);
        for core in [SelectedMinor::V1_0, SelectedMinor::V1_1] {
            for tool in [SelectedMinor::V1_0, SelectedMinor::V1_1] {
                let oracle =
                    owned_progress(&payload, tool, &expected, defaults, jcs::Limits::default());
                let indexed = indexed_progress(
                    &payload,
                    core,
                    tool,
                    &expected,
                    defaults,
                    jcs::Limits::default(),
                )
                .map(|(json, percent, _)| (json, percent));
                assert_eq!(indexed, oracle, "raw={raw}, core={core:?}, tool={tool:?}");
            }
        }
    }
    let mismatch = bytes(1, &bound_bytes("wrong", &null, Some(&bytes(1, &[]))));
    let mut over = mismatch.clone();
    over.extend(scalar(2, 101));
    let expected = selected_projection("p", &null);
    assert_eq!(
        indexed_progress(
            &over,
            SelectedMinor::V1_0,
            SelectedMinor::V1_1,
            &expected,
            defaults,
            jcs::Limits::default()
        )
        .err(),
        Some(AdmissionError::InvalidProgress)
    );
    assert_eq!(
        indexed_progress(
            &mismatch,
            SelectedMinor::V1_0,
            SelectedMinor::V1_1,
            &expected,
            defaults,
            jcs::Limits::default()
        )
        .err(),
        Some(AdmissionError::Value(value::Error::ProjectionMismatch))
    );
    let mut malformed_after_percent = scalar(2, 0);
    malformed_after_percent.extend(scalar(1, 1));
    assert_eq!(
        assert_progress_pair(
            "1.0 percent scan first",
            &malformed_after_percent,
            &null,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Err(AdmissionError::InvalidProgress)
    );
    assert_eq!(
        indexed_progress(
            &scalar(2, 0),
            SelectedMinor::V1_0,
            SelectedMinor::V1_0,
            &expected,
            defaults,
            jcs::Limits::default()
        )
        .err(),
        Some(AdmissionError::UnsupportedVariant)
    );
}

#[test]
fn indexed_json_and_jcs_limits_match_exact_owned_bytes_and_errors() {
    let object = admit_schema(
        r#"{"type":"object","properties":{"é":{"type":"string"}},"required":["é"]}"#.as_bytes(),
        jcs::Limits::default(),
    )
    .unwrap();
    let mut entry = bytes(1, "é".as_bytes());
    entry.extend(bytes(2, &bytes(5, "line\n雪\"".as_bytes())));
    let raw = bytes(8, &bytes(1, &entry));
    let payload = bytes(1, &bound_bytes("p", &object, Some(&raw)));
    let json = r#"{"é":"line\n雪\""}"#.as_bytes().to_vec();
    assert_eq!(
        assert_progress_pair(
            "unicode and escaping",
            &payload,
            &object,
            "p",
            value::Limits::default(),
            jcs::Limits::default()
        ),
        Ok((json.clone(), None))
    );
    let base = value::Limits::default();
    for ceiling in [json.len() - 1, json.len()] {
        let result = assert_progress_pair(
            "exact JSON writer ceiling",
            &payload,
            &object,
            "p",
            value::Limits {
                json_bytes: ceiling,
                ..base
            },
            jcs::Limits::default(),
        );
        assert_eq!(result.is_ok(), ceiling == json.len());
    }
    for instance in [
        jcs::Limits {
            input_bytes: json.len() - 1,
            ..jcs::Limits::default()
        },
        jcs::Limits {
            depth: 0,
            ..jcs::Limits::default()
        },
        jcs::Limits {
            nodes: 1,
            ..jcs::Limits::default()
        },
        jcs::Limits {
            elements: 0,
            ..jcs::Limits::default()
        },
        jcs::Limits {
            decoded_bytes: 1,
            ..jcs::Limits::default()
        },
        jcs::Limits {
            instance_work: 0,
            ..jcs::Limits::default()
        },
    ] {
        assert_eq!(
            assert_progress_pair(
                "JCS parse/work ceiling",
                &payload,
                &object,
                "p",
                base,
                instance
            ),
            Err(AdmissionError::Value(value::Error::Schema(
                jcs::Error::Limit
            )))
        );
    }
}

#[test]
fn indexed_numeric_arms_and_merge_rules_match_owned() {
    let number = admit_schema(br#"{"type":"number"}"#, jcs::Limits::default()).unwrap();
    let bool_schema = admit_schema(br#"{"type":"boolean"}"#, jcs::Limits::default()).unwrap();
    let text_schema = admit_schema(br#"{"type":"string"}"#, jcs::Limits::default()).unwrap();
    let object = admit_schema(br#"{"type":"object"}"#, jcs::Limits::default()).unwrap();
    let defaults = value::Limits::default();
    for number_value in [
        i64::MIN,
        -9_007_199_254_740_991,
        -1,
        0,
        9_007_199_254_740_991,
        i64::MAX,
    ] {
        let zigzag = ((number_value as u64) << 1) ^ ((number_value >> 63) as u64);
        let payload = bytes(1, &bound_bytes("p", &number, Some(&scalar(3, zigzag))));
        let _ = assert_progress_pair(
            "sint64 zigzag",
            &payload,
            &number,
            "p",
            defaults,
            jcs::Limits::default(),
        );
    }
    for float in [-0.0, 1.0, 1e20, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let mut raw = varint((4u64 << 3) | 1);
        raw.extend(float.to_bits().to_le_bytes());
        let payload = bytes(1, &bound_bytes("p", &number, Some(&raw)));
        let _ = assert_progress_pair(
            "float bits",
            &payload,
            &number,
            "p",
            defaults,
            jcs::Limits::default(),
        );
    }
    for raw in [0, 2, u64::MAX] {
        let payload = bytes(1, &bound_bytes("p", &bool_schema, Some(&scalar(2, raw))));
        let _ = assert_progress_pair(
            "nonzero bool",
            &payload,
            &bool_schema,
            "p",
            defaults,
            jcs::Limits::default(),
        );
    }
    let mut repeated_text = bytes(5, b"first");
    repeated_text.extend(bytes(5, b""));
    let payload = bytes(1, &bound_bytes("p", &text_schema, Some(&repeated_text)));
    assert_eq!(
        assert_progress_pair(
            "repeated final empty text",
            &payload,
            &text_schema,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Ok((b"\"\"".to_vec(), None))
    );
    let mut entry = bytes(1, b"old");
    entry.extend(bytes(1, b""));
    entry.extend(bytes(2, &bytes(1, &[])));
    let payload = bytes(
        1,
        &bound_bytes("p", &object, Some(&bytes(8, &bytes(1, &entry)))),
    );
    assert_eq!(
        assert_progress_pair(
            "repeated final empty key",
            &payload,
            &object,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Ok((br#"{"":null}"#.to_vec(), None))
    );

    let first_map = bytes(
        8,
        &bytes(1, &{
            let mut entry = bytes(1, b"a");
            entry.extend(bytes(2, &bytes(1, &[])));
            entry
        }),
    );
    let second_map = bytes(
        8,
        &bytes(1, &{
            let mut entry = bytes(1, b"b");
            entry.extend(bytes(2, &bytes(1, &[])));
            entry
        }),
    );
    let mut merged = first_map.clone();
    merged.extend(second_map);
    let payload = bytes(1, &bound_bytes("p", &object, Some(&merged)));
    assert_eq!(
        assert_progress_pair(
            "same-arm map merge",
            &payload,
            &object,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Ok((br#"{"a":null,"b":null}"#.to_vec(), None))
    );
    let mut replaced = first_map;
    replaced.extend(bytes(5, b"text"));
    replaced.extend(bytes(8, &[]));
    let payload = bytes(1, &bound_bytes("p", &object, Some(&replaced)));
    assert_eq!(
        assert_progress_pair(
            "oneof replacement",
            &payload,
            &object,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Ok((b"{}".to_vec(), None))
    );

    let mut first_list = bytes(7, &bytes(1, &bytes(1, &[])));
    first_list.extend(bytes(7, &bytes(1, &bytes(1, &[]))));
    let array = admit_schema(
        br#"{"type":"array","items":{"type":"null"}}"#,
        jcs::Limits::default(),
    )
    .unwrap();
    let mut partial_bound = bytes(1, b"p");
    partial_bound.extend(bytes(2, &array.canonical().sha256()));
    let mut progress = bytes(1, &partial_bound);
    progress.extend(bytes(1, &bytes(3, &first_list)));
    assert_eq!(
        assert_progress_pair(
            "SchemaBoundValue and list same-arm merge",
            &progress,
            &array,
            "p",
            defaults,
            jcs::Limits::default()
        ),
        Ok((b"[null,null]".to_vec(), None))
    );
}

#[test]
fn ordinary_absence_and_explicit_empty_keep_owned_defaults() {
    let null = admit_schema(br#"{"type":"null"}"#, jcs::Limits::default()).unwrap();
    let raw = bytes(1, &[]);
    let mut omitted_id = bytes(2, &null.canonical().sha256());
    omitted_id.extend(bytes(3, &raw));
    let explicit_id = {
        let mut b = bytes(1, b"");
        b.extend(&omitted_id);
        b
    };
    for bound in [omitted_id, explicit_id] {
        assert_eq!(
            assert_progress_pair(
                "omitted or empty projection id",
                &bytes(1, &bound),
                &null,
                "",
                value::Limits::default(),
                jcs::Limits::default()
            ),
            Ok((b"null".to_vec(), None))
        );
    }
    let omitted_digest = {
        let mut b = bytes(1, b"");
        b.extend(bytes(3, &raw));
        b
    };
    let explicit_empty_digest = {
        let mut b = bytes(1, b"");
        b.extend(bytes(2, &[]));
        b.extend(bytes(3, &raw));
        b
    };
    for bound in [omitted_digest, explicit_empty_digest] {
        assert_eq!(
            assert_progress_pair(
                "omitted or empty digest",
                &bytes(1, &bound),
                &null,
                "",
                value::Limits::default(),
                jcs::Limits::default()
            ),
            Err(AdmissionError::Value(value::Error::ProjectionMismatch))
        );
    }
    for transmitted in [body(8, &[]), body(8, &bytes(3, &[]))] {
        let mut storage = IndexStorage::<32, 33>::new();
        let mut arena = storage.borrow(nz(32), usize::MAX).unwrap();
        let selected = select_active(
            arena.scan_envelope(&transmitted).unwrap(),
            SelectedMinor::V1_1,
            nz(32),
        )
        .unwrap();
        let token = selected.into_progress_payload().unwrap();
        assert_eq!(
            token
                .admit(
                    &mut arena,
                    SelectedMinor::V1_1,
                    &selected_projection("", &null),
                    value::Limits::default(),
                    jcs::Limits::default()
                )
                .err(),
            Some(AdmissionError::InvalidProgress)
        );
    }
}

#[test]
fn const_enum_and_required_instance_work_boundaries_match_owned() {
    let cases = [
        (br#"{"const":"x"}"#.as_slice(), bytes(5, b"x")),
        (
            br#"{"type":"string","enum":["x","y"]}"#.as_slice(),
            bytes(5, b"x"),
        ),
        (
            br#"{"type":"object","properties":{"x":{"type":"null"}},"required":["x"]}"#.as_slice(),
            bytes(
                8,
                &bytes(1, &{
                    let mut e = bytes(1, b"x");
                    e.extend(bytes(2, &bytes(1, &[])));
                    e
                }),
            ),
        ),
    ];
    for (schema_bytes, raw) in cases {
        let schema = admit_schema(schema_bytes, jcs::Limits::default()).unwrap();
        let payload = bytes(1, &bound_bytes("p", &schema, Some(&raw)));
        let first_success = (1..256)
            .find(|work| {
                owned_progress(
                    &payload,
                    SelectedMinor::V1_1,
                    &selected_projection("p", &schema),
                    value::Limits::default(),
                    jcs::Limits {
                        instance_work: *work,
                        ..jcs::Limits::default()
                    },
                )
                .is_ok()
            })
            .unwrap();
        assert_eq!(
            assert_progress_pair(
                "work just below",
                &payload,
                &schema,
                "p",
                value::Limits::default(),
                jcs::Limits {
                    instance_work: first_success - 1,
                    ..jcs::Limits::default()
                }
            ),
            Err(AdmissionError::Value(value::Error::Schema(
                jcs::Error::Limit
            )))
        );
        assert!(
            assert_progress_pair(
                "work exact",
                &payload,
                &schema,
                "p",
                value::Limits::default(),
                jcs::Limits {
                    instance_work: first_success,
                    ..jcs::Limits::default()
                }
            )
            .is_ok()
        );
    }
}

#[test]
fn consuming_roles_and_inner_resource_errors_are_distinct() {
    let null = admit_schema(br#"{"type":"null"}"#, jcs::Limits::default()).unwrap();
    let projection = selected_projection("p", &null);
    let valid = bytes(1, &bound_bytes("p", &null, Some(&bytes(1, &[]))));
    let transmitted = body(8, &bytes(3, &valid));
    let mut storage = IndexStorage::<1024, 1025>::new();
    let mut arena = storage.borrow(nz(1024), usize::MAX).unwrap();
    let token = select_active(
        arena.scan_envelope(&transmitted).unwrap(),
        SelectedMinor::V1_0,
        nz(1024),
    )
    .unwrap()
    .into_progress_payload()
    .unwrap();
    let mut small_storage = IndexStorage::<1, 2>::new();
    let mut small = small_storage.borrow(nz(1), usize::MAX).unwrap();
    assert_eq!(
        token
            .admit(
                &mut small,
                SelectedMinor::V1_1,
                &projection,
                value::Limits::default(),
                jcs::Limits::default()
            )
            .err(),
        Some(AdmissionError::LocalResource(ScanError::Limit))
    );
    let admitted = indexed_progress(
        &valid,
        SelectedMinor::V1_0,
        SelectedMinor::V1_1,
        &projection,
        value::Limits::default(),
        jcs::Limits::default(),
    )
    .unwrap();
    assert_eq!(admitted.0, b"null");

    let empty_item = body(8, &[]);
    let progress = select_active(
        arena.scan_envelope(&empty_item).unwrap(),
        SelectedMinor::V1_1,
        nz(1024),
    )
    .unwrap()
    .into_progress_payload()
    .unwrap();
    assert_eq!(
        progress
            .admit(
                &mut arena,
                SelectedMinor::V1_1,
                &projection,
                value::Limits::default(),
                jcs::Limits::default()
            )
            .err(),
        Some(AdmissionError::InvalidProgress)
    );

    for wrong_arm in [body(6, &[]), body(7, &[])] {
        let selected = select_active(
            arena.scan_envelope(&wrong_arm).unwrap(),
            SelectedMinor::V1_1,
            nz(1024),
        )
        .unwrap();
        assert_eq!(
            selected.into_progress_payload().err(),
            Some(RoleError::WrongArm)
        );
    }
    let result_empty = body(9, &bytes(2, &[]));
    let terminal = select_active(
        arena.scan_envelope(&result_empty).unwrap(),
        SelectedMinor::V1_1,
        nz(1024),
    )
    .unwrap()
    .into_result_terminal()
    .unwrap();
    let ResultTerminal::Result(token) = terminal else {
        panic!("present empty result must stay present")
    };
    assert_eq!(
        token
            .admit(
                &mut arena,
                SelectedMinor::V1_1,
                &projection,
                value::Limits::default(),
                jcs::Limits::default()
            )
            .err(),
        Some(AdmissionError::InvalidTerminal)
    );
    let malformed_result = body(9, &bytes(2, &bytes(1, b"not a bound message")));
    let selected = select_active(
        arena.scan_envelope(&malformed_result).unwrap(),
        SelectedMinor::V1_1,
        nz(1024),
    )
    .unwrap();
    let ResultTerminal::Result(token) = selected.into_result_terminal().unwrap() else {
        panic!()
    };
    assert_eq!(
        token
            .admit(
                &mut arena,
                SelectedMinor::V1_1,
                &projection,
                value::Limits::default(),
                jcs::Limits::default(),
            )
            .err(),
        Some(AdmissionError::InvalidTerminal)
    );
    let no_terminal = body(9, &[]);
    let selected = select_active(
        arena.scan_envelope(&no_terminal).unwrap(),
        SelectedMinor::V1_1,
        nz(1024),
    )
    .unwrap();
    assert_eq!(
        selected.into_result_terminal().err(),
        Some(RoleError::InvalidTerminal)
    );
    for (terminal_fields, expect_result) in [
        (
            {
                let mut end = bytes(3, &[]);
                end.extend(bytes(2, &[]));
                end
            },
            true,
        ),
        (
            {
                let mut end = bytes(2, &[]);
                end.extend(bytes(3, &[]));
                end
            },
            false,
        ),
    ] {
        let transmitted = body(9, &terminal_fields);
        let selected = select_active(
            arena.scan_envelope(&transmitted).unwrap(),
            SelectedMinor::V1_1,
            nz(1024),
        )
        .unwrap();
        let terminal = selected.into_result_terminal().unwrap();
        assert_eq!(matches!(terminal, ResultTerminal::Result(_)), expect_result);
    }

    let result = bytes(1, &bound_bytes("p", &null, Some(&bytes(1, &[]))));
    let transmitted = body(9, &bytes(2, &result));
    for tool in [SelectedMinor::V1_0, SelectedMinor::V1_1] {
        for core in [SelectedMinor::V1_0, SelectedMinor::V1_1] {
            let selected =
                select_active(arena.scan_envelope(&transmitted).unwrap(), core, nz(1024)).unwrap();
            let ResultTerminal::Result(token) = selected.into_result_terminal().unwrap() else {
                panic!()
            };
            let got = token
                .admit(
                    &mut arena,
                    tool,
                    &projection,
                    value::Limits::default(),
                    jcs::Limits::default(),
                )
                .unwrap();
            let expected = if tool == SelectedMinor::V1_0 {
                let dto = v1_0::InvokeToolResult::decode(result.as_slice()).unwrap();
                value::admit_schema_bound_v1_0(
                    dto.raw_result.as_ref().unwrap(),
                    &projection,
                    value::Limits::default(),
                    jcs::Limits::default(),
                )
                .unwrap()
            } else {
                let dto = v1_1::InvokeToolResult::decode(result.as_slice()).unwrap();
                value::admit_schema_bound_v1_1(
                    dto.raw_result.as_ref().unwrap(),
                    &projection,
                    value::Limits::default(),
                    jcs::Limits::default(),
                )
                .unwrap()
            };
            assert_eq!(got.json, expected);
            assert_eq!(got.body_len, transmitted.len());
        }
    }
}

#[test]
fn direct_sibling_enumeration_counts_matching_nonmatching_and_merged_children() {
    let mut capability = bytes(1, b"peer");
    capability.extend(bytes(9, &[]));
    capability.extend(bytes(11, &[]));
    capability.extend(bytes(9, &[]));
    let transmitted = body(3, &bytes(4, &capability));
    let mut storage = IndexStorage::<1024, 1025>::new();
    let mut arena = storage.borrow(nz(1024), usize::MAX).unwrap();
    let view = arena.scan_envelope(&transmitted).unwrap();
    let manifest = view.message(view.root(), 3).unwrap();
    let capability = view.direct_messages(manifest, 4).next().unwrap();
    let mut profiles = view.direct_messages(capability, 9);
    assert!(profiles.next().is_some());
    assert!(profiles.next().is_some());
    assert!(profiles.next().is_none());
    assert_eq!(profiles.visited_records(), 4);
    let mut schemas = view.direct_messages(capability, 11);
    assert!(schemas.next().is_some());
    assert!(schemas.next().is_none());
    assert_eq!(schemas.visited_records(), 4);

    let mut raw = bytes(7, &bytes(1, &bytes(1, &[])));
    raw.extend(bytes(7, &bytes(1, &bytes(1, &[]))));
    let payload = bytes(1, &bytes(3, &raw));
    let view = arena.scan_tool_progress(&payload).unwrap();
    let bound = view.message(view.root(), 1).unwrap();
    let root = view.message(bound, 3).unwrap();
    let list = view.message(root, 7).unwrap();
    let mut children = view.direct_messages(list, 1);
    assert!(children.next().is_some());
    assert!(children.next().is_some());
    assert!(children.next().is_none());
    assert_eq!(children.visited_records(), 2);

    let mut raw = bytes(1, &[]);
    for _ in 0..12 {
        raw = bytes(7, &bytes(1, &raw));
    }
    let wide = (0..80)
        .flat_map(|_| bytes(1, &bytes(1, &[])))
        .collect::<Vec<_>>();
    let mut top = bytes(1, &raw);
    top.extend(wide);
    let payload = bytes(1, &bytes(3, &bytes(7, &top)));
    let view = arena.scan_tool_progress(&payload).unwrap();
    let bound = view.message(view.root(), 1).unwrap();
    let root = view.message(bound, 3).unwrap();
    fn visit(
        view: &pi_extension_protocol::wire::echo_bounded::IndexedMessage<'_, '_>,
        node: pi_extension_protocol::wire::echo_bounded::MessageId,
    ) -> (usize, usize) {
        let Some(7) = view.oneof_tag(node) else {
            return (0, 0);
        };
        let list = view.message(node, 7).unwrap();
        let mut count_pass = view.direct_messages(list, 1);
        let mut count = 0;
        while count_pass.next().is_some() {
            count += 1;
        }
        let mut visits = count_pass.visited_records();
        let mut walk_pass = view.direct_messages(list, 1);
        let mut descendants = 0;
        for child in walk_pass.by_ref() {
            let (nested, nested_visits) = visit(view, child);
            descendants += nested + 1;
            visits += nested_visits;
        }
        visits += walk_pass.visited_records();
        assert_eq!(count, walk_pass.visited_records());
        (descendants, visits)
    }
    let (children, visits) = visit(&view, root);
    assert_eq!(children, 93);
    assert_eq!(visits, 2 * children);
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
