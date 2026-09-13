use pi_agent_schema::SessionCodecOptions;
use serde_json::{Value, json};

/// Concrete schemas for the four pinned coding-agent message roles. Callers may
/// explicitly choose these options; normalization never merges or overrides an
/// existing caller registry and never implicitly permits unknown custom roles.
pub fn coding_message_options() -> SessionCodecOptions {
    let number = json!({"type":"integer","minimum":0,"maximum":9_007_199_254_740_991u64});
    let string = json!({"type":"string"});
    let text = json!({"type":"object","properties":{"type":{"const":"text"},"text":{"type":"string"},"textSignature":{"type":"string"}},"required":["type","text"],"additionalProperties":false});
    let image = json!({"type":"object","properties":{"type":{"const":"image"},"data":{"type":"string"},"mimeType":{"type":"string"}},"required":["type","data","mimeType"],"additionalProperties":false});
    let content =
        json!({"anyOf":[{"type":"string"},{"type":"array","items":{"anyOf":[text,image]}}]});
    let schemas = [
        (
            "custom",
            json!({"type":"object","properties":{"role":{"const":"custom"},"customType":string,"content":content,"display":{"type":"boolean"},"details":{},"timestamp":number},"required":["role","customType","content","display","timestamp"],"additionalProperties":false}),
        ),
        (
            "bashExecution",
            json!({"type":"object","properties":{"role":{"const":"bashExecution"},"command":string,"output":string,"exitCode":{"type":"integer","minimum":-9_007_199_254_740_991i64,"maximum":9_007_199_254_740_991u64},"cancelled":{"type":"boolean"},"truncated":{"type":"boolean"},"fullOutputPath":string,"timestamp":number,"excludeFromContext":{"type":"boolean"}},"required":["role","command","output","cancelled","truncated","timestamp"],"additionalProperties":false}),
        ),
        (
            "branchSummary",
            json!({"type":"object","properties":{"role":{"const":"branchSummary"},"summary":string,"fromId":{"type":"string","minLength":1},"timestamp":number},"required":["role","summary","fromId","timestamp"],"additionalProperties":false}),
        ),
        (
            "compactionSummary",
            json!({"type":"object","properties":{"role":{"const":"compactionSummary"},"summary":string,"tokensBefore":number,"timestamp":number},"required":["role","summary","tokensBefore","timestamp"],"additionalProperties":false}),
        ),
    ];
    SessionCodecOptions::from_role_schemas(
        schemas
            .into_iter()
            .map(|(role, value): (&str, Value)| {
                (
                    role.to_owned(),
                    serde_json::from_value(value).expect("static schema object"),
                )
            })
            .collect(),
    )
    .expect("static coding role names")
}
