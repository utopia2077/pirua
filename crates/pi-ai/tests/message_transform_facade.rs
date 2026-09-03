use pi_ai::{
    LaxMessageDecodeError, Message, SafeUnsigned, ToolCallIdNormalizer, decode_lax_messages,
    transform_messages,
};
use serde_json::{Value, json};

fn assert_error_contract<T>()
where
    T: Copy + Clone + std::fmt::Debug + Eq + std::error::Error + Send + Sync + 'static,
{
}

#[test]
fn lax_decoder_repairs_null_and_missing_content_without_mutation() {
    let raw = vec![
        json!({"role":"user","content":null,"timestamp":1}),
        json!({"role":"user","timestamp":2}),
        json!({
            "role":"assistant",
            "content":null,
            "api":"api",
            "provider":"provider",
            "model":"model",
            "usage":{
                "input":0,"output":0,"cacheRead":0,"cacheWrite":0,
                "totalTokens":0,
                "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}
            },
            "stopReason":"stop",
            "timestamp":3
        }),
        json!({
            "role":"assistant",
            "api":"api",
            "provider":"provider",
            "model":"model",
            "usage":{
                "input":0,"output":0,"cacheRead":0,"cacheWrite":0,
                "totalTokens":0,
                "cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}
            },
            "stopReason":"stop",
            "timestamp":4
        }),
        json!({
            "role":"toolResult",
            "toolCallId":"call-null",
            "toolName":"tool",
            "content":null,
            "isError":false,
            "timestamp":5
        }),
        json!({
            "role":"toolResult",
            "toolCallId":"call-missing",
            "toolName":"tool",
            "isError":false,
            "timestamp":6
        }),
    ];
    let before = raw.clone();
    let decoded = decode_lax_messages(&raw).expect("lax messages decode");
    assert_eq!(raw, before);
    assert_eq!(decoded.len(), 6);
    for message in &decoded[0..2] {
        assert!(
            matches!(message, Message::User(message) if matches!(&message.content, pi_ai::UserContent::Blocks(content) if content.is_empty()))
        );
    }
    for message in &decoded[2..4] {
        assert!(matches!(message, Message::Assistant(message) if message.content.is_empty()));
    }
    for message in &decoded[4..6] {
        assert!(matches!(message, Message::ToolResult(message) if message.content.is_empty()));
    }

    const SENTINEL: &str = "RAW_MESSAGE_SECRET_d939";
    let invalid = vec![
        json!({"role":"user","content":[],"timestamp":1}),
        Value::String(SENTINEL.to_owned()),
    ];
    let error = decode_lax_messages(&invalid).expect_err("second message is invalid");
    assert_eq!(error.index(), SafeUnsigned::new(1).expect("safe index"));
    assert!(!format!("{error:?} {error}").contains(SENTINEL));

    let strict_violation = [json!({"role":"user","timestamp":1,"unknown":true})];
    assert!(decode_lax_messages(&strict_violation).is_err());
}

#[test]
fn message_transform_runtime_is_reexported_through_facade() {
    type TransformFn =
        for<'messages, 'model, 'policy> fn(
            &'messages [Message],
            &'model pi_ai::Model,
            SafeUnsigned,
            Option<&'policy mut dyn ToolCallIdNormalizer>,
        )
            -> Result<Vec<Message>, pi_ai::MessageTransformError>;
    let _transform: TransformFn = transform_messages;
    let _decode: fn(&[Value]) -> Result<Vec<Message>, LaxMessageDecodeError> = decode_lax_messages;
    let _index: fn(&LaxMessageDecodeError) -> SafeUnsigned = LaxMessageDecodeError::index;
    assert_error_contract::<LaxMessageDecodeError>();
}
