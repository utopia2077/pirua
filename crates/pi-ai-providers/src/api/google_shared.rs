//! Pinned Google GenAI/Vertex helper projections, without client construction.
//!
//! SDK-facing content is transient and serialize-only, not a new durable message
//! format. Core model/tool ordering and strict admission remain unchanged.

mod content;
mod retry;

pub use content::{
    GoogleContent, GoogleFunctionCall, GoogleFunctionResponse, GoogleInlineData, GooglePart,
    GoogleResponseValue, GoogleRole, convert_messages,
};
pub use retry::{GoogleRequestFailure, GoogleRetryOptions, retry_google_request};

use std::fmt;

use pi_ai_core::{JsonValue, MessageTransformError, Model, ModelThinkingLevel, StopReason, Tool};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::constrained_sampling::{
    ConstrainedSamplingError, get_json_schema_tool_parameters, resolve_json_schema_strict_sampling,
};
use super::wire_json::{WireJsonError, stringify_wire_json};

/// Google SDK 1.52.0 thinking discriminants, including the unspecified value.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GoogleApiThinkingLevel {
    ThinkingLevelUnspecified,
    Minimal,
    Low,
    Medium,
    High,
}

/// Logical Google levels after applying a model's custom mapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ResolvedGoogleThinkingLevel {
    Minimal,
    Low,
    Medium,
    High,
}

/// The complete pinned SDK function-calling enum.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GoogleFunctionCallingConfigMode {
    ModeUnspecified,
    Auto,
    Any,
    None,
    Validated,
}

/// The complete pinned SDK finish enum. Unknown values fail enum admission;
/// raw string dispatch deliberately has different unknown-value behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum GoogleFinishReason {
    FinishReasonUnspecified,
    Stop,
    MaxTokens,
    Safety,
    Recitation,
    Language,
    Other,
    Blocklist,
    ProhibitedContent,
    Spii,
    MalformedFunctionCall,
    ImageSafety,
    UnexpectedToolCall,
    ImageProhibitedContent,
    NoImage,
    ImageRecitation,
    ImageOther,
}

/// Redacted, typed helper rejection; no schema, model ID or content is formatted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GoogleSharedError {
    UnsupportedThinkingLevel,
    MessageTransform(MessageTransformError),
    ConstrainedSampling(ConstrainedSamplingError),
    WireJson(WireJsonError),
    InvalidProjection,
}
impl fmt::Display for GoogleSharedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::UnsupportedThinkingLevel => "unsupported Google thinking level mapping",
            Self::MessageTransform(_) => "Google message transformation failed",
            Self::ConstrainedSampling(_) => "Google constrained sampling failed",
            Self::WireJson(_) => "Google wire JSON limit or number failure",
            Self::InvalidProjection => "invalid Google helper projection",
        })
    }
}
impl std::error::Error for GoogleSharedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::MessageTransform(error) => Some(error),
            Self::ConstrainedSampling(error) => Some(error),
            Self::WireJson(error) => Some(error),
            _ => None,
        }
    }
}
impl From<ConstrainedSamplingError> for GoogleSharedError {
    fn from(value: ConstrainedSamplingError) -> Self {
        Self::ConstrainedSampling(value)
    }
}

/// Off resolves to high before inspecting mappings. Null mappings fall back to
/// the logical level; strings are lowercased, but never whitespace-trimmed.
pub fn resolve_google_thinking_level(
    model: &Model,
    level: ModelThinkingLevel,
) -> Result<ResolvedGoogleThinkingLevel, GoogleSharedError> {
    if level == ModelThinkingLevel::Off {
        return Ok(ResolvedGoogleThinkingLevel::High);
    }
    let mapped = model
        .thinking_level_map
        .as_ref()
        .and_then(|map| map.as_map().get(&level))
        .and_then(Option::as_deref);
    let default = match level {
        ModelThinkingLevel::Minimal => "minimal",
        ModelThinkingLevel::Low => "low",
        ModelThinkingLevel::Medium => "medium",
        ModelThinkingLevel::High => "high",
        ModelThinkingLevel::ExtraHigh => "xhigh",
        ModelThinkingLevel::Max => "max",
        ModelThinkingLevel::Off => "off",
    };
    match mapped.map(str::to_lowercase).as_deref().unwrap_or(default) {
        "minimal" => Ok(ResolvedGoogleThinkingLevel::Minimal),
        "low" => Ok(ResolvedGoogleThinkingLevel::Low),
        "medium" => Ok(ResolvedGoogleThinkingLevel::Medium),
        "high" => Ok(ResolvedGoogleThinkingLevel::High),
        _ => Err(GoogleSharedError::UnsupportedThinkingLevel),
    }
}

/// A signature alone is not evidence that a part is thinking.
pub fn is_thinking_part(part: &GooglePart) -> bool {
    part.thought == Some(true)
}

/// Retains the last nonempty signature within one part, never across parts.
pub fn retain_thought_signature<'a>(
    existing: Option<&'a str>,
    incoming: Option<&'a str>,
) -> Option<&'a str> {
    incoming.filter(|value| !value.is_empty()).or(existing)
}

fn gemini_major_at_least_three(id: &str) -> Option<bool> {
    let lower = id.to_lowercase();
    let rest = lower
        .strip_prefix("gemini-live-")
        .or_else(|| lower.strip_prefix("gemini-"))?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    if digits.is_empty() {
        return None;
    }
    // Avoid overflow while matching parseInt followed solely by >= 3, including
    // hundreds of leading zeros and arbitrarily large source version numbers.
    let digits = digits.trim_start_matches('0');
    Some(
        digits.len() > 1
            || digits
                .as_bytes()
                .first()
                .is_some_and(|digit| *digit >= b'3'),
    )
}

/// Claude/gpt-oss prefixes are case-sensitive; Gemini's version regex is not.
pub fn requires_tool_call_id(model_id: &str) -> bool {
    model_id.starts_with("claude-")
        || model_id.starts_with("gpt-oss-")
        || gemini_major_at_least_three(model_id) == Some(true)
}

/// Gemini 3+ enables validated strict function calling.
pub fn supports_google_strict_tool_sampling(model_id: &str) -> bool {
    gemini_major_at_least_three(model_id) == Some(true)
}

/// Unknown strings map to AUTO, without case folding.
pub fn map_tool_choice(choice: &str) -> GoogleFunctionCallingConfigMode {
    match choice {
        "none" => GoogleFunctionCallingConfigMode::None,
        "any" => GoogleFunctionCallingConfigMode::Any,
        _ => GoogleFunctionCallingConfigMode::Auto,
    }
}

/// Strict resolution runs before explicit none/any. Like Array.some, it stops
/// at the first strict tool; later invalid strict tools are not visited here.
pub fn resolve_google_function_calling_mode(
    tools: &[Tool],
    tool_choice: Option<&str>,
    supports_strict_mode: bool,
) -> Result<Option<GoogleFunctionCallingConfigMode>, GoogleSharedError> {
    let mut strict = false;
    for tool in tools {
        if resolve_json_schema_strict_sampling(tool, supports_strict_mode)? == Some(true) {
            strict = true;
            break;
        }
    }
    Ok(match tool_choice {
        Some("none" | "any") => tool_choice.map(map_tool_choice),
        _ if strict => Some(GoogleFunctionCallingConfigMode::Validated),
        Some(choice) if !choice.is_empty() => Some(map_tool_choice(choice)),
        _ => None,
    })
}

/// Exhaustive SDK mapping; unknown strings cannot construct this enum.
pub fn map_stop_reason(reason: GoogleFinishReason) -> StopReason {
    match reason {
        GoogleFinishReason::Stop => StopReason::Stop,
        GoogleFinishReason::MaxTokens => StopReason::Length,
        GoogleFinishReason::FinishReasonUnspecified
        | GoogleFinishReason::Safety
        | GoogleFinishReason::Recitation
        | GoogleFinishReason::Language
        | GoogleFinishReason::Other
        | GoogleFinishReason::Blocklist
        | GoogleFinishReason::ProhibitedContent
        | GoogleFinishReason::Spii
        | GoogleFinishReason::MalformedFunctionCall
        | GoogleFinishReason::ImageSafety
        | GoogleFinishReason::UnexpectedToolCall
        | GoogleFinishReason::ImageProhibitedContent
        | GoogleFinishReason::NoImage
        | GoogleFinishReason::ImageRecitation
        | GoogleFinishReason::ImageOther => StopReason::Error,
    }
}

/// Raw responses map every unrecognized string to error, not an enum failure.
pub fn map_stop_reason_string(reason: &str) -> StopReason {
    match reason {
        "STOP" => StopReason::Stop,
        "MAX_TOKENS" => StopReason::Length,
        _ => StopReason::Error,
    }
}

/// Exactly one of the SDK's mutually exclusive parameter fields.
#[derive(Clone, PartialEq, Serialize)]
pub enum GoogleToolParameters {
    #[serde(rename = "parameters")]
    Parameters(Value),
    #[serde(rename = "parametersJsonSchema")]
    JsonSchema(Value),
}
impl fmt::Debug for GoogleToolParameters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GoogleToolParameters([REDACTED])")
    }
}

#[derive(Clone, PartialEq, Serialize)]
pub struct GoogleFunctionDeclaration {
    pub name: String,
    pub description: String,
    #[serde(flatten)]
    pub parameters: GoogleToolParameters,
}
impl fmt::Debug for GoogleFunctionDeclaration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("GoogleFunctionDeclaration([REDACTED])")
    }
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GoogleTool {
    pub function_declarations: Vec<GoogleFunctionDeclaration>,
}

fn check_depth(value: &JsonValue, depth: usize) -> Result<(), GoogleSharedError> {
    if depth > 128 {
        return Err(GoogleSharedError::WireJson(WireJsonError::DepthLimit));
    }
    match value {
        JsonValue::Array(values) => {
            for value in values {
                check_depth(value, depth + 1)?;
            }
        }
        JsonValue::Object(values) => {
            for value in values.as_map().values() {
                check_depth(value, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn sanitize_for_open_api(value: Value) -> Value {
    let Value::Object(values) = value else {
        // Deliberately do NOT visit arrays, including anyOf/items/default data.
        return value;
    };
    Value::Object(
        values
            .into_iter()
            .filter_map(|(key, value)| {
                if matches!(
                    key.as_str(),
                    "$schema"
                        | "$id"
                        | "$anchor"
                        | "$dynamicAnchor"
                        | "$vocabulary"
                        | "$comment"
                        | "$defs"
                        | "definitions"
                        | "__proto__"
                ) {
                    None
                } else {
                    Some((key, sanitize_for_open_api(value)))
                }
            })
            .collect(),
    )
}

/// None corresponds to source undefined for empty input. Strict schema behavior
/// and canonical property order are delegated to the accepted shared converter.
pub fn convert_tools(
    tools: &[Tool],
    use_parameters: bool,
    supports_strict_mode: bool,
) -> Result<Option<Vec<GoogleTool>>, GoogleSharedError> {
    if tools.is_empty() {
        return Ok(None);
    }
    let mut declarations = Vec::with_capacity(tools.len());
    for tool in tools {
        for value in tool.parameters.0.as_map().values() {
            check_depth(value, 1)?;
        }
        let strict = resolve_json_schema_strict_sampling(tool, supports_strict_mode)?;
        let parameters = get_json_schema_tool_parameters(tool, strict)?;
        let value = serde_json::to_value(parameters.as_ref())
            .map_err(|_| GoogleSharedError::InvalidProjection)?;
        stringify_wire_json(&value).map_err(GoogleSharedError::WireJson)?;
        declarations.push(GoogleFunctionDeclaration {
            name: tool.name.as_str().to_owned(),
            description: tool.description.clone(),
            parameters: if use_parameters {
                GoogleToolParameters::Parameters(sanitize_for_open_api(value))
            } else {
                GoogleToolParameters::JsonSchema(value)
            },
        });
    }
    Ok(Some(vec![GoogleTool {
        function_declarations: declarations,
    }]))
}
