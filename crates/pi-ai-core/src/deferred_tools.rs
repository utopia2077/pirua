//! Deferred-tool partitioning with borrowed name normalization.

use std::collections::{BTreeMap, BTreeSet};

use crate::{AssistantContent, Context, Message, Tool, ToolName};

/// Borrowed process-local tool-name normalization policy.
pub trait ToolNameNormalizer {
    /// Returns the normalized identity used for matching and deduplication.
    fn normalize_tool_name(&self, name: &ToolName) -> ToolName;
}

/// One transcript-loaded tool and its normalized lookup identity.
#[derive(Clone, Debug, PartialEq)]
pub struct DeferredTool {
    /// Normalized matching identity.
    pub normalized_name: ToolName,
    /// Last source definition occupying this normalized identity's first slot.
    pub tool: Tool,
}

/// Stable partition of immediate and transcript-loaded tools.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct DeferredToolsSplit {
    /// Tools available at request start.
    pub immediate: Vec<Tool>,
    /// Tools made available by transcript markers.
    pub deferred: Vec<DeferredTool>,
}

/// Splits current tool definitions according to prior calls and load markers.
pub fn split_deferred_tools(
    context: &Context,
    enabled: bool,
    normalizer: Option<&dyn ToolNameNormalizer>,
) -> DeferredToolsSplit {
    let normalize = |name: &ToolName| {
        normalizer.map_or_else(|| name.clone(), |policy| policy.normalize_tool_name(name))
    };

    let mut positions = BTreeMap::<ToolName, usize>::new();
    let mut unique = Vec::<(ToolName, Tool)>::new();
    for tool in context.tools.as_deref().unwrap_or_default() {
        let normalized = normalize(&tool.name);
        if let Some(position) = positions.get(&normalized).copied() {
            unique[position].1 = tool.clone();
        } else {
            positions.insert(normalized.clone(), unique.len());
            unique.push((normalized, tool.clone()));
        }
    }
    if !enabled {
        return DeferredToolsSplit {
            immediate: unique.into_iter().map(|(_, tool)| tool).collect(),
            deferred: Vec::new(),
        };
    }

    let mut deferred_names = BTreeSet::new();
    let mut used_names = BTreeSet::new();
    for message in &context.messages {
        match message {
            Message::Assistant(message) => {
                for block in &message.content {
                    if let AssistantContent::ToolCall(call) = block {
                        used_names.insert(normalize(&call.name));
                    }
                }
            }
            Message::ToolResult(message) => {
                for name in message.added_tool_names.as_deref().unwrap_or_default() {
                    let normalized = normalize(name);
                    if !used_names.contains(&normalized) {
                        deferred_names.insert(normalized);
                    }
                }
            }
            Message::User(_) => {}
        }
    }

    let mut split = DeferredToolsSplit::default();
    for (normalized_name, tool) in unique {
        if deferred_names.contains(&normalized_name) {
            split.deferred.push(DeferredTool {
                normalized_name,
                tool,
            });
        } else {
            split.immediate.push(tool);
        }
    }
    split
}
