use pi_agent_schema::AgentToolResult;
use pi_ai_core::JsonValue;

#[test]
fn structured_null_and_details_survive_the_language_neutral_tool_boundary() {
    let wire = serde_json::json!({
        "content": [], "structuredContent": null, "details": null
    });
    let result: AgentToolResult = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(result.structured_content, Some(JsonValue::Null));
    assert_eq!(result.details, Some(JsonValue::Null));
    assert_eq!(serde_json::to_value(result).unwrap(), wire);

    let missing = serde_json::json!({"content": []});
    let result: AgentToolResult = serde_json::from_value(missing.clone()).unwrap();
    assert_eq!(result.structured_content, None);
    assert_eq!(result.details, None);
    assert_eq!(serde_json::to_value(result).unwrap(), missing);
}
