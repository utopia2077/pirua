//! Detached Agent messages, tools and invocation contracts.
//! Runtime state and durable task scheduling are owned by their executors.

mod event;
mod input;
mod message;
mod tool;

pub use event::*;
pub use input::*;
pub use message::*;
pub use pi_ai_core::{BoxFuture, CancellationToken};
pub use tool::*;

use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AgentError {
    Busy,
    InvalidContext(String),
    Callback(String),
    Event(String),
    Tool(String),
}

impl fmt::Display for AgentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy => f.write_str("Agent is already processing"),
            Self::InvalidContext(message)
            | Self::Callback(message)
            | Self::Event(message)
            | Self::Tool(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for AgentError {}
