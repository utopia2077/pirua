use serde_json::{Value, json};
#[test]
fn pinned_baseline_positive_and_negative_fixtures() {
    let schema: Value =
        serde_json::from_str(include_str!("../../../conformance/acp-v2/schema.json")).unwrap();
    for (accepted, data) in [
        (
            true,
            include_str!("../../../conformance/acp-v2/fixtures/standard.json"),
        ),
        (
            false,
            include_str!("../../../conformance/acp-v2/fixtures/invalid.json"),
        ),
    ] {
        for case in serde_json::from_str::<Vec<Value>>(data).unwrap() {
            let root = json!({"$schema":schema["$schema"],"$defs":schema["$defs"],"$ref":format!("#/$defs/{}",case["definition"].as_str().unwrap())});
            let validator = jsonschema::validator_for(&root).unwrap();
            assert_eq!(validator.is_valid(&case["value"]), accepted, "{case}");
        }
    }
}
#[test]
fn request_validation_uses_the_definition_without_envelope_constraints() {
    let contracts = pi_acp_server::protocol::Contracts::new();
    contracts
        .validate("NewSessionRequest", &json!({"cwd":"/tmp"}))
        .unwrap();
    assert!(
        contracts
            .validate(
                "PromptRequest",
                &json!({"sessionId":"s","prompt":[{"type":"text","text":12}]})
            )
            .is_err()
    );
    contracts
        .validate(
            "PromptRequest",
            &json!({"sessionId":"s","prompt":[{"type":"future","unknown":12}]}),
        )
        .unwrap();
}
#[test]
fn snapshots_whitelist_execution_settings_instead_of_mirroring_host_credentials() {
    let settings = pi_acp_server::projection::display_settings(
        &json!({"credentials":{"key":"private"},"apiKey":"private","retry":{"enabled":false,"apiKey":"private","provider":{"maxRetryDelayMs":12,"secret":"private"}}}),
    );
    assert!(!settings.to_string().contains("private"));
    assert_eq!(settings["retry"]["enabled"], false);
    assert_eq!(settings["retry"]["provider"]["maxRetryDelayMs"], 12);
}
