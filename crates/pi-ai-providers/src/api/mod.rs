//! Shared wire helpers and explicitly enabled canonical provider protocols.

pub mod constrained_sampling;
pub mod github_copilot_headers;
pub mod google_shared;
pub mod openai_prompt_cache;
pub mod openai_responses_shared;
pub mod runtime;
pub mod simple_options;
pub mod wire_json;

#[cfg(any(feature = "wire-openai-responses", feature = "wire-openai-completions"))]
mod openai_sdk_strings;

#[cfg(feature = "wire-openai-responses")]
pub mod openai_responses;

#[cfg(feature = "wire-openai-completions")]
pub mod openai_completions;

#[cfg(feature = "wire-pi-messages")]
pub mod pi_messages;

#[cfg(feature = "wire-mistral-conversations")]
pub mod mistral_conversations;

#[cfg(feature = "wire-anthropic-messages")]
pub mod anthropic_messages;

#[cfg(feature = "wire-google-genai")]
pub mod google_genai;
#[cfg(feature = "wire-google-genai")]
mod google_sdk;
