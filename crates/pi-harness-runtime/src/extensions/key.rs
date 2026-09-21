//! Exact, host-only correlation key for a planned remote tool effect.
//!
//! This conversion has no channel, invocation, or Session authority. A rejected
//! local plan cannot produce a wire key for `ToolInvocation::start`.

use std::fmt;

use pi_agent_schema::{EffectKind, ToolEffectPlan};
use pi_ai_core::JS_SAFE_INTEGER_MAX;
use pi_extension_protocol::wire::v1_1;

/// A fixed category that never prints any identity or saved argument.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolKeyError {
    InvalidToolPlan,
}

impl fmt::Display for ToolKeyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid planned tool effect key")
    }
}

impl std::error::Error for ToolKeyError {}

/// Copies the complete derived key of a real tool plan into the v1 wire DTO.
///
/// A tool call has logical attempt zero and a present item index equal to the
/// plan's source content index. The typed counters already enforce JS-safe
/// values; the explicit bound keeps this adapter's wire contract visible.
pub fn encode_tool_effect_key<C>(
    plan: &ToolEffectPlan<C>,
) -> Result<v1_1::EffectKey, ToolKeyError> {
    let key = &plan.key;
    if key.kind != EffectKind::Tool
        || key.logical_attempt.get() != 0
        || key.item_index != Some(plan.source_index)
        || key.logical_attempt.get() > JS_SAFE_INTEGER_MAX
        || plan.source_index.get() > JS_SAFE_INTEGER_MAX
    {
        return Err(ToolKeyError::InvalidToolPlan);
    }

    Ok(v1_1::EffectKey {
        session_id: key.session_id.as_str().to_owned(),
        lane: key.lane.as_str().to_owned(),
        operation_id: key.operation_id.as_str().to_owned(),
        effect_kind: "tool".to_owned(),
        step_or_task_id: key.step_or_task_id.as_str().to_owned(),
        logical_attempt: key.logical_attempt.get(),
        item_index: key.item_index.map(|index| index.get()),
    })
}
