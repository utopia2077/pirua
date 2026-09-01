//! Serializable tool declarations and constrained-sampling configuration.

use serde::{Deserialize, Serialize};

use crate::id::ToolName;
use crate::json::{JsonObject, LiteralFalse};

/// Object-only JSON Schema used for tool parameters.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JsonSchema(pub JsonObject);

/// Strictness requested for JSON-schema constrained sampling.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum JsonSchemaStrictness {
    /// Prefer strict sampling and fall back when unsupported.
    #[serde(rename = "prefer")]
    Prefer,
    /// Require strict sampling.
    #[serde(rename = "require")]
    Require,
}

/// Provider grammar encodings for the same intended input language.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrammarVariants {
    /// OpenAI Lark grammar.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub openai_lark: Option<String>,
    /// OpenAI regular-expression grammar.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub openai_regex: Option<String>,
}

/// Provider-side constrained-sampling configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConstrainedSamplingConfig {
    /// JSON-schema constrained sampling.
    JsonSchema {
        /// Requested strictness.
        strict: JsonSchemaStrictness,
    },
    /// Grammar constrained sampling.
    Grammar {
        /// Provider-specific grammar variants.
        variants: GrammarVariants,
    },
}

/// Optional tool sampling setting, including the literal `false` disable value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConstrainedSampling {
    /// Explicitly disables constrained sampling.
    Disabled(LiteralFalse),
    /// Enables a constrained-sampling configuration.
    Config(ConstrainedSamplingConfig),
}

/// A serializable tool declaration without execution behavior.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Tool<S = JsonSchema> {
    /// Stable model-visible tool name.
    pub name: ToolName,
    /// Model-visible description.
    pub description: String,
    /// Object-only parameter schema.
    pub parameters: S,
    /// Optional provider-side constrained sampling.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub constrained_sampling: Option<ConstrainedSampling>,
}
