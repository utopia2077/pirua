use pi_extension_protocol::jcs::{self, admit_schema};
use pi_extension_protocol::wire::{v1_0, v1_1, value};

const ARGUMENTS: &str = include_str!("fixtures/tool.arguments_echo.schema.json");
const PROGRESS: &str = include_str!("fixtures/tool.progress_echo.schema.json");
const RESULT: &str = include_str!("fixtures/tool.result_echo.schema.json");

fn wire(arm: v1_0::wire_value::Value) -> v1_0::WireValue {
    v1_0::WireValue { value: Some(arm) }
}

fn text(value: &str) -> v1_0::WireValue {
    wire(v1_0::wire_value::Value::TextValue(value.into()))
}

fn map(entries: Vec<(&str, v1_0::WireValue)>) -> v1_0::WireValue {
    wire(v1_0::wire_value::Value::MapValue(v1_0::WireMap {
        entries: entries
            .into_iter()
            .map(|(key, value)| v1_0::WireMapEntry {
                key: key.into(),
                value: Some(value),
            })
            .collect(),
    }))
}

fn list(items: Vec<v1_0::WireValue>) -> v1_0::WireValue {
    wire(v1_0::wire_value::Value::ListValue(v1_0::WireList { items }))
}

fn bound(value: v1_0::WireValue, id: &str, schema: &jcs::PortableSchema) -> v1_0::SchemaBoundValue {
    v1_0::SchemaBoundValue {
        projection_id: id.into(),
        schema_digest_sha256: schema.canonical().sha256().to_vec(),
        value: Some(value),
    }
}

fn projection<'a>(id: &'a str, schema: &'a jcs::PortableSchema) -> value::Projection<'a> {
    value::Projection {
        id,
        digest_sha256: schema.canonical().sha256(),
        schema,
    }
}

fn check(
    input: &v1_0::SchemaBoundValue,
    expected: &value::Projection<'_>,
) -> Result<Vec<u8>, value::Error> {
    value::admit_schema_bound_v1_0(
        input,
        expected,
        value::Limits::default(),
        jcs::Limits::default(),
    )
}

fn admit_raw(input: &v1_0::WireValue, limits: value::Limits) -> Result<(), value::Error> {
    value::admit_v1_0(input, limits, value::BytePolicy::JsonOnly)
}

#[test]
fn selected_echo_arguments_progress_and_raw_result_validate() {
    let arguments = admit_schema(ARGUMENTS.as_bytes(), jcs::Limits::default()).unwrap();
    let progress = admit_schema(PROGRESS.as_bytes(), jcs::Limits::default()).unwrap();
    let result = admit_schema(RESULT.as_bytes(), jcs::Limits::default()).unwrap();
    let args = bound(map(vec![("text", text("hello"))]), "echo.args", &arguments);
    assert_eq!(
        check(&args, &projection("echo.args", &arguments)).unwrap(),
        br#"{"text":"hello"}"#
    );

    let content = list(vec![map(vec![
        ("type", text("text")),
        ("text", text("hello")),
    ])]);
    let payload = map(vec![("content", content), ("details", map(vec![]))]);
    for (id, schema) in [("echo.progress", &progress), ("echo.result", &result)] {
        let selected = bound(payload.clone(), id, schema);
        let json = check(&selected, &projection(id, schema)).unwrap();
        assert_eq!(
            schema.validate_instance(&json, jcs::Limits::default()),
            Ok(())
        );
    }
}

#[test]
fn identity_is_checked_before_value_and_digest_must_match_schema() {
    let schema = admit_schema(ARGUMENTS.as_bytes(), jcs::Limits::default()).unwrap();
    let expected = projection("echo.args", &schema);
    let mut input = bound(v1_0::WireValue { value: None }, "wrong", &schema);
    assert_eq!(
        check(&input, &expected),
        Err(value::Error::ProjectionMismatch)
    );
    input.projection_id = "echo.args".into();
    input.schema_digest_sha256[0] ^= 1;
    assert_eq!(
        check(&input, &expected),
        Err(value::Error::ProjectionMismatch)
    );
    input.schema_digest_sha256 = expected.digest_sha256.to_vec();
    assert_eq!(check(&input, &expected), Err(value::Error::MissingValue));
    let wrong_schema = admit_schema(br#"{"type":"null"}"#, jcs::Limits::default()).unwrap();
    let incoherent = value::Projection {
        schema: &wrong_schema,
        ..expected
    };
    assert_eq!(
        check(&input, &incoherent),
        Err(value::Error::ProjectionMismatch)
    );
}

#[test]
fn missing_duplicate_and_bytes_are_distinct_from_json_null() {
    let null_schema = admit_schema(br#"{"type":"null"}"#, jcs::Limits::default()).unwrap();
    let json_null = bound(
        wire(v1_0::wire_value::Value::NullValue(v1_0::NullValue {})),
        "null",
        &null_schema,
    );
    assert_eq!(
        check(&json_null, &projection("null", &null_schema)).unwrap(),
        b"null"
    );
    let absent = v1_0::WireValue { value: None };
    assert_eq!(
        admit_raw(&absent, value::Limits::default()),
        Err(value::Error::MissingValue)
    );
    let duplicate = map(vec![("a", text("1")), ("a", text("2"))]);
    assert_eq!(
        admit_raw(&duplicate, value::Limits::default()),
        Err(value::Error::DuplicateKey)
    );
    let missing_entry = wire(v1_0::wire_value::Value::MapValue(v1_0::WireMap {
        entries: vec![v1_0::WireMapEntry {
            key: "a".into(),
            value: None,
        }],
    }));
    assert_eq!(
        admit_raw(&missing_entry, value::Limits::default()),
        Err(value::Error::MissingValue)
    );
    let bytes = wire(v1_0::wire_value::Value::BytesValue(vec![0, 255]));
    assert_eq!(
        admit_raw(&bytes, value::Limits::default()),
        Err(value::Error::BytesNotJson)
    );
    assert_eq!(
        value::admit_v1_0(
            &bytes,
            value::Limits::default(),
            value::BytePolicy::ReviewedByteProfile
        ),
        Ok(())
    );
    let json_bytes = bound(bytes, "null", &null_schema);
    assert_eq!(
        check(&json_bytes, &projection("null", &null_schema)),
        Err(value::Error::BytesNotJson)
    );
}

#[test]
fn safe_integer_edges_and_nonfinite_float_reject() {
    use v1_0::wire_value::Value;
    let integer_schema = admit_schema(br#"{"type":"integer"}"#, jcs::Limits::default()).unwrap();
    for accepted in [-9_007_199_254_740_991, 9_007_199_254_740_991] {
        assert_eq!(
            admit_raw(
                &wire(Value::IntegerValue(accepted)),
                value::Limits::default()
            ),
            Ok(())
        );
        let selected = bound(
            wire(Value::IntegerValue(accepted)),
            "integer",
            &integer_schema,
        );
        assert_eq!(
            check(&selected, &projection("integer", &integer_schema)).unwrap(),
            accepted.to_string().as_bytes()
        );
    }
    for rejected in [
        i64::MIN,
        -9_007_199_254_740_992,
        9_007_199_254_740_992,
        i64::MAX,
    ] {
        assert_eq!(
            admit_raw(
                &wire(Value::IntegerValue(rejected)),
                value::Limits::default()
            ),
            Err(value::Error::UnsafeInteger)
        );
    }
    for rejected in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert_eq!(
            admit_raw(&wire(Value::FloatValue(rejected)), value::Limits::default()),
            Err(value::Error::NonFiniteFloat)
        );
    }
    assert_eq!(
        admit_raw(&wire(Value::FloatValue(-0.0)), value::Limits::default()),
        Ok(())
    );
}

#[test]
fn each_value_ceiling_is_enforced() {
    use v1_0::wire_value::Value;
    let defaults = value::Limits::default();
    let nested = list(vec![list(vec![text("x")])]);
    assert_eq!(
        admit_raw(
            &nested,
            value::Limits {
                depth: 2,
                ..defaults
            }
        ),
        Err(value::Error::Limit)
    );
    let two = list(vec![text("a"), text("b")]);
    assert_eq!(
        admit_raw(
            &two,
            value::Limits {
                elements: 1,
                ..defaults
            }
        ),
        Err(value::Error::Limit)
    );
    assert_eq!(
        admit_raw(
            &text("é"),
            value::Limits {
                string_bytes: 1,
                ..defaults
            }
        ),
        Err(value::Error::Limit)
    );
    assert_eq!(
        admit_raw(
            &map(vec![("é", text(""))]),
            value::Limits {
                string_bytes: 1,
                ..defaults
            }
        ),
        Err(value::Error::Limit)
    );
    assert_eq!(
        value::admit_v1_0(
            &wire(Value::BytesValue(vec![0, 1])),
            value::Limits {
                bytes_length: 1,
                ..defaults
            },
            value::BytePolicy::ReviewedByteProfile
        ),
        Err(value::Error::Limit)
    );
    assert_eq!(
        admit_raw(
            &text("x"),
            value::Limits {
                decoded_bytes: 1,
                ..defaults
            }
        ),
        Err(value::Error::Limit)
    );
    let one_text_bytes = std::mem::size_of::<v1_0::WireValue>() + 1;
    assert_eq!(
        admit_raw(
            &text("x"),
            value::Limits {
                decoded_bytes: one_text_bytes,
                ..defaults
            }
        ),
        Ok(())
    );
    assert_eq!(
        admit_raw(
            &text("x"),
            value::Limits {
                decoded_bytes: one_text_bytes - 1,
                ..defaults
            }
        ),
        Err(value::Error::Limit)
    );

    let schema = admit_schema(ARGUMENTS.as_bytes(), jcs::Limits::default()).unwrap();
    let input = bound(map(vec![("text", text("x"))]), "echo.args", &schema);
    assert_eq!(
        value::admit_schema_bound_v1_0(
            &input,
            &projection("echo.args", &schema),
            value::Limits {
                json_bytes: 8,
                ..defaults
            },
            jcs::Limits::default()
        ),
        Err(value::Error::Limit)
    );
}

#[test]
fn schema_mismatch_and_instance_budget_reject() {
    let schema = admit_schema(ARGUMENTS.as_bytes(), jcs::Limits::default()).unwrap();
    let input = bound(
        map(vec![(
            "text",
            wire(v1_0::wire_value::Value::IntegerValue(3)),
        )]),
        "echo.args",
        &schema,
    );
    assert_eq!(
        check(&input, &projection("echo.args", &schema)),
        Err(value::Error::Schema(jcs::Error::InstanceMismatch))
    );
    let valid = bound(map(vec![("text", text("x"))]), "echo.args", &schema);
    assert_eq!(
        value::admit_schema_bound_v1_0(
            &valid,
            &projection("echo.args", &schema),
            value::Limits::default(),
            jcs::Limits {
                instance_work: 0,
                ..jcs::Limits::default()
            }
        ),
        Err(value::Error::Schema(jcs::Error::Limit))
    );
}

#[test]
fn selected_v1_1_uses_its_own_dto() {
    let schema = admit_schema(ARGUMENTS.as_bytes(), jcs::Limits::default()).unwrap();
    let input = v1_1::SchemaBoundValue {
        projection_id: "echo.args".into(),
        schema_digest_sha256: schema.canonical().sha256().to_vec(),
        value: Some(v1_1::WireValue {
            value: Some(v1_1::wire_value::Value::MapValue(v1_1::WireMap {
                entries: vec![v1_1::WireMapEntry {
                    key: "text".into(),
                    value: Some(v1_1::WireValue {
                        value: Some(v1_1::wire_value::Value::TextValue("x".into())),
                    }),
                }],
            })),
        }),
    };
    assert_eq!(
        value::admit_v1_1(
            input.value.as_ref().unwrap(),
            value::Limits::default(),
            value::BytePolicy::JsonOnly
        ),
        Ok(())
    );
    assert_eq!(
        value::admit_schema_bound_v1_1(
            &input,
            &projection("echo.args", &schema),
            value::Limits::default(),
            jcs::Limits::default()
        )
        .unwrap(),
        br#"{"text":"x"}"#
    );
}

#[test]
fn errors_never_echo_rejected_content() {
    let input = map(vec![
        ("secret-sentinel", text("a")),
        ("secret-sentinel", text("b")),
    ]);
    let error = admit_raw(&input, value::Limits::default()).unwrap_err();
    let display = error.to_string();
    assert!(!display.contains("secret-sentinel"));
    assert!(!format!("{error:?}").contains("secret-sentinel"));
}
