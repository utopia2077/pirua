#![cfg(feature = "wire-bedrock-converse-stream")]
use pi_ai_providers::api::bedrock_converse_stream::*;
use serde_json::json;
#[test]
fn binary_custody_clone_order_and_plain_json_never_restore_bytes() {
    let mut object = BedrockObject::new();
    object.insert("z".into(), BedrockValue::Bytes(vec![1, 2, 3]));
    object.insert("a".into(), BedrockValue::String("private fixture".into()));
    object.insert("12".into(), BedrockValue::Null);
    object.insert("2".into(), BedrockValue::Null);
    object.insert("z".into(), BedrockValue::Bytes(vec![4, 5]));
    assert_eq!(
        object.iter().map(|(k, _)| k).collect::<Vec<_>>(),
        ["2", "12", "z", "a"]
    );
    let payload = BedrockPayload::new(BedrockValue::Object(object));
    let cloned = payload.clone();
    assert!(matches!(cloned.as_value().get("z"),Some(BedrockValue::Bytes(b))if b==&[4,5]));
    let copied = BedrockPayload::from_json(payload.to_json());
    assert!(matches!(
        copied.as_value().get("z"),
        Some(BedrockValue::Object(_))
    ));
    assert_eq!(copied.to_json()["z"], json!({"0":4.0,"1":5.0}));
    for marker in [
        json!({"$bytes":"AQID"}),
        json!({"type":"Buffer","data":[1.0,2.0,3.0]}),
        json!({"__proto__":{"$bytes":"AQID"}}),
    ] {
        let payload = BedrockPayload::from_json(marker.clone());
        assert!(matches!(payload.as_value(), BedrockValue::Object(_)));
        assert_eq!(payload.to_json(), marker);
    }
    for debug in [
        format!("{payload:?}"),
        format!("{:?}", payload.as_value()),
        format!(
            "{:?}",
            BedrockCredentials {
                access_key_id: pi_ai_core::SecretString::new("synthetic-access"),
                secret_access_key: pi_ai_core::SecretString::new("synthetic-secret"),
                session_token: None,
                expiration_unix_ms: None
            }
        ),
    ] {
        assert!(debug.contains("REDACTED"));
        assert!(!debug.contains("private fixture"));
        assert!(!debug.contains("synthetic-secret"));
        assert!(!debug.contains("synthetic-access"));
    }
}

#[test]
fn configured_session_debug_redacts_captured_platform_values() {
    let settings = BedrockSettings {
        user_agent: vec![(
            "synthetic-private-platform".into(),
            Some("synthetic-private-version".into()),
        )],
        ..Default::default()
    };
    let debug = format!("{settings:?}");
    assert!(debug.contains("REDACTED"));
    assert!(!debug.contains("synthetic-private-platform"));
    assert!(!debug.contains("synthetic-private-version"));
}
