use pi_ai::{
    JsonObject, Tool, ToolCall, ToolValidationError, ToolValidationIssue, ToolValidationReport,
    validate_tool_arguments, validate_tool_call,
};

fn assert_value_contract<T>()
where
    T: Clone + std::fmt::Debug + Eq + Send + Sync + 'static,
{
}

fn assert_error_contract<T>()
where
    T: Clone + std::fmt::Debug + Eq + std::error::Error + Send + Sync + 'static,
{
}

#[test]
fn tool_validation_facade_reexports_exact_surface() {
    type ValidateCall = fn(&[Tool], &ToolCall) -> Result<JsonObject, ToolValidationError>;
    type ValidateArguments = fn(&Tool, &ToolCall) -> Result<JsonObject, ToolValidationError>;
    let _validate_call: ValidateCall = validate_tool_call;
    let _validate_arguments: ValidateArguments = validate_tool_arguments;
    let _path: for<'a> fn(&'a ToolValidationIssue) -> &'a str = ToolValidationIssue::path;
    let _issues: for<'a> fn(&'a ToolValidationReport) -> &'a [ToolValidationIssue] =
        ToolValidationReport::issues;
    assert_value_contract::<ToolValidationIssue>();
    assert_value_contract::<ToolValidationReport>();
    assert_error_contract::<ToolValidationError>();

    let tool: Tool = serde_json::from_value(serde_json::json!({
        "name":"echo",
        "description":"Echo",
        "parameters":{
            "type":"object",
            "properties":{"value":{"type":"number"}},
            "required":["value"]
        }
    }))
    .expect("tool fixture");
    let call: ToolCall = serde_json::from_value(serde_json::json!({
        "type":"toolCall",
        "id":"call-1",
        "name":"echo",
        "arguments":{"value":"42"}
    }))
    .expect("call fixture");
    assert_eq!(
        validate_tool_call(&[tool], &call).expect("facade validates"),
        serde_json::from_value(serde_json::json!({"value":42})).expect("arguments fixture")
    );
}
