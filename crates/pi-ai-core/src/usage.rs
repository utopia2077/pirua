//! Provider and tool usage accounting values.

use serde::{Deserialize, Serialize};

use crate::json::{FiniteF64, SafeUnsigned};

/// Monetary cost associated with one usage record.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UsageCost {
    /// Input-token cost.
    pub input: FiniteF64,
    /// Output-token cost.
    pub output: FiniteF64,
    /// Cache-read cost.
    pub cache_read: FiniteF64,
    /// Cache-write cost.
    pub cache_write: FiniteF64,
    /// Total request cost.
    pub total: FiniteF64,
}

/// Provider- or tool-reported usage.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Usage {
    /// Uncached input tokens.
    pub input: SafeUnsigned,
    /// Output tokens, including reported reasoning tokens.
    pub output: SafeUnsigned,
    /// Cache-read input tokens.
    pub cache_read: SafeUnsigned,
    /// Cache-write input tokens.
    pub cache_write: SafeUnsigned,
    /// Cache-write tokens using one-hour retention when reported.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub cache_write_1h: Option<SafeUnsigned>,
    /// Reasoning tokens when the provider exposes a breakdown.
    #[serde(
        default,
        deserialize_with = "crate::json::deserialize_optional_non_null",
        skip_serializing_if = "Option::is_none"
    )]
    pub reasoning: Option<SafeUnsigned>,
    /// Total provider-reported tokens.
    pub total_tokens: SafeUnsigned,
    /// Monetary cost fields.
    pub cost: UsageCost,
}
