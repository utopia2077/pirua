//! Pi v1.4.0 context estimates, including invalidation by edits and compaction.
use pi_coding_session::{EntryContent, SessionEntry, SessionProjection};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ContextUsage {
    pub tokens: Option<u64>,
    pub context_window: u64,
    pub percent: Option<f64>,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContextUsageEstimate {
    pub tokens: u64,
    pub usage_tokens: u64,
    pub trailing_tokens: u64,
    pub last_usage_index: Option<usize>,
}
fn js_len(text: &str) -> u64 {
    text.encode_utf16().count() as u64
}
fn content_chars(value: &Value) -> u64 {
    value.as_str().map(js_len).unwrap_or_else(|| {
        value
            .as_array()
            .map(|blocks| {
                blocks
                    .iter()
                    .map(|b| match b["type"].as_str() {
                        Some("text") => b["text"].as_str().map(js_len).unwrap_or(0),
                        Some("image") => 4800,
                        _ => 0,
                    })
                    .sum()
            })
            .unwrap_or(0)
    })
}
pub fn estimate_tokens(message: &Value) -> u64 {
    let chars = match message["role"].as_str() {
        Some("system") => {
            content_chars(&message["content"])
                + message["sections"]
                    .as_object()
                    .map(|s| {
                        s.values()
                            .filter_map(Value::as_str)
                            .map(js_len)
                            .sum::<u64>()
                    })
                    .unwrap_or(0)
                + message
                    .get("toolsAdded")
                    .map(|v| js_len(&v.to_string()))
                    .unwrap_or(0)
        }
        Some("user" | "custom" | "toolResult") => content_chars(&message["content"]),
        Some("assistant") => message["content"]
            .as_array()
            .map(|blocks| {
                blocks
                    .iter()
                    .map(|b| match b["type"].as_str() {
                        Some("text") => b["text"].as_str().map(js_len).unwrap_or(0),
                        Some("thinking") => b["thinking"].as_str().map(js_len).unwrap_or(0),
                        Some("toolCall") => {
                            b["name"].as_str().map(js_len).unwrap_or(0)
                                + js_len(&b["arguments"].to_string())
                        }
                        _ => 0,
                    })
                    .sum()
            })
            .unwrap_or(0),
        Some("bashExecution") => {
            message["command"].as_str().map(js_len).unwrap_or(0)
                + message["output"].as_str().map(js_len).unwrap_or(0)
        }
        Some("branchSummary" | "compactionSummary") => {
            message["summary"].as_str().map(js_len).unwrap_or(0)
        }
        _ => 0,
    };
    chars.div_ceil(4)
}
pub fn calculate_context_tokens(usage: &Value) -> u64 {
    usage["totalTokens"]
        .as_u64()
        .filter(|v| *v > 0)
        .unwrap_or_else(|| {
            ["input", "output", "cacheRead", "cacheWrite"]
                .iter()
                .map(|key| usage[key].as_u64().unwrap_or(0))
                .sum()
        })
}
fn valid_usage(message: &Value) -> Option<u64> {
    (message["role"] == "assistant"
        && message["stopReason"] != "error"
        && message["stopReason"] != "aborted")
        .then(|| calculate_context_tokens(&message["usage"]))
        .filter(|v| *v > 0)
}
pub fn estimate_context_tokens(messages: &[Value]) -> ContextUsageEstimate {
    let last = messages
        .iter()
        .enumerate()
        .rev()
        .find_map(|(i, m)| valid_usage(m).map(|v| (i, v)));
    let usage_tokens = last.map(|(_, v)| v).unwrap_or(0);
    let trailing_tokens = messages
        .iter()
        .skip(last.map(|(i, _)| i + 1).unwrap_or(0))
        .map(estimate_tokens)
        .sum::<u64>();
    ContextUsageEstimate {
        tokens: usage_tokens + trailing_tokens,
        usage_tokens,
        trailing_tokens,
        last_usage_index: last.map(|(i, _)| i),
    }
}
pub fn estimate_projected_context_tokens(
    projection: &SessionProjection,
    branch: &[SessionEntry],
) -> ContextUsageEstimate {
    let estimate = estimate_context_tokens(&projection.messages);
    if let Some(index) = estimate.last_usage_index {
        let mut offset = 0;
        let usage_entry = projection.entries.iter().find(|entry| {
            let end = offset + entry.messages.len();
            let found = index < end;
            offset = end;
            found
        });
        let usage_position =
            usage_entry.and_then(|e| branch.iter().position(|b| b.id == e.source_entry.id));
        let invalidating = branch.iter().rposition(|e| {
            matches!(
                e.content,
                EntryContent::Compaction { .. } | EntryContent::ContextEdit { .. }
            )
        });
        if usage_position.is_some() && (invalidating.is_none() || usage_position > invalidating) {
            return estimate;
        }
    }
    let current_system = projection.messages.iter().rfind(|m| m["role"] == "system");
    let tokens = current_system.map(estimate_tokens).unwrap_or(0)
        + projection
            .messages
            .iter()
            .filter(|m| m["role"] != "system")
            .map(estimate_tokens)
            .sum::<u64>();
    ContextUsageEstimate {
        tokens,
        usage_tokens: 0,
        trailing_tokens: tokens,
        last_usage_index: None,
    }
}
pub fn should_compact(
    tokens: u64,
    context_window: u64,
    settings: &crate::CompactionSettings,
) -> bool {
    settings.enabled
        && i128::from(tokens)
            > i128::from(context_window) - i128::from(settings.reserve_tokens.get())
}
pub(crate) fn context_usage(
    projection: &SessionProjection,
    branch: &[SessionEntry],
    window: u64,
) -> Option<ContextUsage> {
    if window == 0 {
        return None;
    }
    if let Some(index) = branch
        .iter()
        .rposition(|e| matches!(e.content, EntryContent::Compaction { .. }))
    {
        let post = branch[index + 1..].iter().any(|e| {
            projection.entries.iter().any(|p| {
                p.source_entry.id == e.id && p.messages.iter().any(|m| valid_usage(m).is_some())
            })
        });
        if !post {
            return Some(ContextUsage {
                tokens: None,
                context_window: window,
                percent: None,
            });
        }
    }
    let tokens = estimate_projected_context_tokens(projection, branch).tokens;
    Some(ContextUsage {
        tokens: Some(tokens),
        context_window: window,
        percent: Some(tokens as f64 / window as f64 * 100.0),
    })
}
