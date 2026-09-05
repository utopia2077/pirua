//! Shared wire helpers and explicitly enabled canonical provider protocols.

pub mod constrained_sampling;
pub mod github_copilot_headers;
pub mod openai_prompt_cache;
pub mod openai_responses_shared;
pub mod runtime;
pub mod simple_options;
pub mod wire_json;

#[cfg(feature = "wire-pi-messages")]
pub mod pi_messages;
