//! Typed registrations and caller-authorized, process-local started hook pipelines.
//!
//! These values do not authorize effect start or durable settlement. The caller
//! owns admission, task retention, codec/policy association and lossless transport
//! of complete request options into its eventual Effects implementation.

mod pipeline;
mod registry;
mod request;

pub use pipeline::{HookErrorSink, HookPipelineOutput, StartedHookContext};
pub use registry::{CaptureRevision, CapturedHookPipeline, HookRegistry};
pub use request::apply_stream_options_patch;

use pi_agent_schema::{ErrorContext, HandlerError, HarnessRuntimeError};
fn context(message: &'static str) -> ErrorContext {
    ErrorContext::new(message).expect("bounded static hook context")
}
fn failure(message: &'static str) -> HandlerError {
    HandlerError::new(context(message))
}
fn fault() -> HarnessRuntimeError {
    HarnessRuntimeError::Fault(pi_agent_schema::HarnessFault::new(context(
        "invalid started hook invocation context",
    )))
}
