//! Shared wire helpers and four selected provider protocols.
#[cfg(feature = "wire-anthropic-messages")]
pub mod anthropic_messages;
pub mod constrained_sampling;
pub mod github_copilot_headers;
#[cfg(feature = "wire-google-genai")]
pub mod google_genai;
#[cfg(feature = "wire-google-genai")]
mod google_sdk;
pub mod google_shared;
#[cfg(feature = "wire-google-genai")]
mod google_stream;
pub mod lazy;
#[cfg(feature = "wire-openai-completions")]
pub mod openai_completions;
pub mod openai_prompt_cache;
#[cfg(feature = "wire-openai-responses")]
pub mod openai_responses;
pub mod openai_responses_shared;
#[cfg(feature = "wire-openai-responses")]
mod openai_sdk;
#[cfg(any(feature = "wire-openai-responses", feature = "wire-openai-completions"))]
pub(crate) mod openai_sdk_strings;
mod request_lifecycle;
pub mod runtime;
pub mod simple_options;
pub mod wire_json;
