//! Closed durable tool and queue configuration discriminants.

use serde::{Deserialize, Serialize};

/// Queue draining behavior.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum QueueMode {
    /// Drain all eligible queued messages.
    #[serde(rename = "all")]
    All,
    /// Drain only the oldest eligible message.
    #[serde(rename = "one-at-a-time")]
    OneAtATime,
}

/// Tool execution behavior for one batch.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ToolExecutionMode {
    /// Execute and settle calls one by one.
    #[serde(rename = "sequential")]
    Sequential,
    /// Permit effect execution in parallel while settling in source order.
    #[serde(rename = "parallel")]
    Parallel,
}

/// Captured tool replay policy.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub enum ToolReplay {
    /// Never replay an effect whose outcome is unknown.
    #[default]
    #[serde(rename = "never")]
    Never,
    /// Replay when both captured and current declarations say it is safe.
    #[serde(rename = "safe")]
    Safe,
}
