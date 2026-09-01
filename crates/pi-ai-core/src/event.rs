//! Closed assistant-message stream events with terminal consistency checks.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer, de, ser};

use crate::content::ToolCall;
use crate::json::SafeUnsigned;
use crate::message::{AssistantMessage, StopReason};

/// Successful terminal stream reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DoneReason {
    /// Normal completion.
    #[serde(rename = "stop")]
    Stop,
    /// Length-limited completion.
    #[serde(rename = "length")]
    Length,
    /// Tool-use completion.
    #[serde(rename = "toolUse")]
    ToolUse,
    /// Deferred completion with a handle.
    #[serde(rename = "deferred")]
    Deferred,
}

impl DoneReason {
    const fn stop_reason(self) -> StopReason {
        match self {
            Self::Stop => StopReason::Stop,
            Self::Length => StopReason::Length,
            Self::ToolUse => StopReason::ToolUse,
            Self::Deferred => StopReason::Deferred,
        }
    }
}

/// Error terminal stream reason.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum ErrorReason {
    /// Caller-owned cancellation.
    #[serde(rename = "aborted")]
    Aborted,
    /// Provider or runtime failure.
    #[serde(rename = "error")]
    Error,
}

impl ErrorReason {
    const fn stop_reason(self) -> StopReason {
        match self {
            Self::Aborted => StopReason::Aborted,
            Self::Error => StopReason::Error,
        }
    }
}

/// Error returned when a terminal event contradicts its nested assistant message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AssistantMessageEventError {
    /// A terminal nested message still had the `pending` stop reason.
    PendingTerminal,
    /// Event reason and nested message stop reason differed.
    ReasonMismatch,
    /// An error or aborted terminal omitted `errorMessage`.
    MissingErrorMessage,
}

impl fmt::Display for AssistantMessageEventError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PendingTerminal => {
                formatter.write_str("terminal assistant message cannot be pending")
            }
            Self::ReasonMismatch => {
                formatter.write_str("terminal event reason must equal nested message stopReason")
            }
            Self::MissingErrorMessage => {
                formatter.write_str("error or aborted terminal requires errorMessage")
            }
        }
    }
}

impl std::error::Error for AssistantMessageEventError {}

/// Progress and terminal events emitted while building an assistant message.
#[derive(Clone, Debug, PartialEq)]
pub enum AssistantMessageEvent {
    /// Starts a new assistant message.
    Start {
        /// Current partial message.
        partial: Box<AssistantMessage>,
    },
    /// Starts a text block.
    TextStart {
        /// Content-array index.
        content_index: SafeUnsigned,
        /// Current partial message.
        partial: Box<AssistantMessage>,
    },
    /// Appends text to a text block.
    TextDelta {
        /// Content-array index.
        content_index: SafeUnsigned,
        /// Text delta.
        delta: String,
        /// Current partial message.
        partial: Box<AssistantMessage>,
    },
    /// Finishes a text block.
    TextEnd {
        /// Content-array index.
        content_index: SafeUnsigned,
        /// Final text content.
        content: String,
        /// Current partial message.
        partial: Box<AssistantMessage>,
    },
    /// Starts a thinking block.
    ThinkingStart {
        /// Content-array index.
        content_index: SafeUnsigned,
        /// Current partial message.
        partial: Box<AssistantMessage>,
    },
    /// Appends text to a thinking block.
    ThinkingDelta {
        /// Content-array index.
        content_index: SafeUnsigned,
        /// Thinking delta.
        delta: String,
        /// Current partial message.
        partial: Box<AssistantMessage>,
    },
    /// Finishes a thinking block.
    ThinkingEnd {
        /// Content-array index.
        content_index: SafeUnsigned,
        /// Final thinking content.
        content: String,
        /// Current partial message.
        partial: Box<AssistantMessage>,
    },
    /// Starts a tool-call block.
    ToolCallStart {
        /// Content-array index.
        content_index: SafeUnsigned,
        /// Current partial message.
        partial: Box<AssistantMessage>,
    },
    /// Appends JSON text to a tool-call block.
    ToolCallDelta {
        /// Content-array index.
        content_index: SafeUnsigned,
        /// Tool-argument JSON delta.
        delta: String,
        /// Current partial message.
        partial: Box<AssistantMessage>,
    },
    /// Finishes a tool-call block.
    ToolCallEnd {
        /// Content-array index.
        content_index: SafeUnsigned,
        /// Final tool call.
        tool_call: ToolCall,
        /// Current partial message.
        partial: Box<AssistantMessage>,
    },
    /// Terminates successfully.
    Done {
        /// Successful terminal reason.
        reason: DoneReason,
        /// Final assistant message.
        message: Box<AssistantMessage>,
    },
    /// Terminates with an error or abort.
    Error {
        /// Error terminal reason.
        reason: ErrorReason,
        /// Final error assistant message.
        error: Box<AssistantMessage>,
    },
}

impl AssistantMessageEvent {
    /// Validates terminal reason and error-message invariants.
    pub fn validate(&self) -> Result<(), AssistantMessageEventError> {
        let (expected, message, requires_error) = match self {
            Self::Done { reason, message } => (reason.stop_reason(), message.as_ref(), false),
            Self::Error { reason, error } => (reason.stop_reason(), error.as_ref(), true),
            _ => return Ok(()),
        };
        if message.stop_reason == StopReason::Pending {
            return Err(AssistantMessageEventError::PendingTerminal);
        }
        if message.stop_reason != expected {
            return Err(AssistantMessageEventError::ReasonMismatch);
        }
        if requires_error && message.error_message.is_none() {
            return Err(AssistantMessageEventError::MissingErrorMessage);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(remote = "AssistantMessageEvent", tag = "type", deny_unknown_fields)]
enum AssistantMessageEventDef {
    #[serde(rename = "start")]
    Start { partial: Box<AssistantMessage> },
    #[serde(rename = "text_start")]
    TextStart {
        #[serde(rename = "contentIndex")]
        content_index: SafeUnsigned,
        partial: Box<AssistantMessage>,
    },
    #[serde(rename = "text_delta")]
    TextDelta {
        #[serde(rename = "contentIndex")]
        content_index: SafeUnsigned,
        delta: String,
        partial: Box<AssistantMessage>,
    },
    #[serde(rename = "text_end")]
    TextEnd {
        #[serde(rename = "contentIndex")]
        content_index: SafeUnsigned,
        content: String,
        partial: Box<AssistantMessage>,
    },
    #[serde(rename = "thinking_start")]
    ThinkingStart {
        #[serde(rename = "contentIndex")]
        content_index: SafeUnsigned,
        partial: Box<AssistantMessage>,
    },
    #[serde(rename = "thinking_delta")]
    ThinkingDelta {
        #[serde(rename = "contentIndex")]
        content_index: SafeUnsigned,
        delta: String,
        partial: Box<AssistantMessage>,
    },
    #[serde(rename = "thinking_end")]
    ThinkingEnd {
        #[serde(rename = "contentIndex")]
        content_index: SafeUnsigned,
        content: String,
        partial: Box<AssistantMessage>,
    },
    #[serde(rename = "toolcall_start")]
    ToolCallStart {
        #[serde(rename = "contentIndex")]
        content_index: SafeUnsigned,
        partial: Box<AssistantMessage>,
    },
    #[serde(rename = "toolcall_delta")]
    ToolCallDelta {
        #[serde(rename = "contentIndex")]
        content_index: SafeUnsigned,
        delta: String,
        partial: Box<AssistantMessage>,
    },
    #[serde(rename = "toolcall_end")]
    ToolCallEnd {
        #[serde(rename = "contentIndex")]
        content_index: SafeUnsigned,
        #[serde(rename = "toolCall")]
        tool_call: ToolCall,
        partial: Box<AssistantMessage>,
    },
    #[serde(rename = "done")]
    Done {
        reason: DoneReason,
        message: Box<AssistantMessage>,
    },
    #[serde(rename = "error")]
    Error {
        reason: ErrorReason,
        error: Box<AssistantMessage>,
    },
}

impl Serialize for AssistantMessageEvent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.validate().map_err(ser::Error::custom)?;
        AssistantMessageEventDef::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for AssistantMessageEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let event = AssistantMessageEventDef::deserialize(deserializer)?;
        event.validate().map_err(de::Error::custom)?;
        Ok(event)
    }
}
