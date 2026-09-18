use super::*;
use pi_ai_core::{Message, Usage};
use serde_json::Value;

fn utf16(s: &str) -> u64 {
    s.encode_utf16().count() as u64
}
fn content_chars(v: &Value) -> u64 {
    if let Some(s) = v.as_str() {
        return utf16(s);
    }
    v.as_array()
        .into_iter()
        .flatten()
        .map(|b| match b["type"].as_str() {
            Some("text") => utf16(b["text"].as_str().unwrap_or("")),
            Some("image") => 4800,
            _ => 0,
        })
        .sum()
}
fn js_entries(o: &serde_json::Map<String, Value>) -> Vec<(&String, &Value)> {
    let mut entries: Vec<_> = o.iter().collect();
    entries.sort_by(|(a, _), (b, _)| {
        fn index(s: &str) -> Option<u32> {
            let n = s.parse::<u32>().ok()?;
            (n != u32::MAX && n.to_string() == s).then_some(n)
        }
        match (index(a), index(b)) {
            (Some(a), Some(b)) => a.cmp(&b),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            _ => a.cmp(b),
        }
    });
    entries
}
/// ECMAScript-compatible numbers and numeric-key ordering over the DTO's canonical
/// map order. Foreign insertion order already lost by JsonObject is not invented.
pub fn js_json(v: &Value) -> String {
    match v {
        Value::Number(n) => ryu_js::Buffer::new()
            .format_finite(n.as_f64().expect("JSON finite number"))
            .to_owned(),
        Value::Array(a) => format!("[{}]", a.iter().map(js_json).collect::<Vec<_>>().join(",")),
        Value::Object(o) => {
            let entries = js_entries(o);
            format!(
                "{{{}}}",
                entries
                    .into_iter()
                    .map(|(k, v)| format!(
                        "{}:{}",
                        serde_json::to_string(k).expect("string"),
                        js_json(v)
                    ))
                    .collect::<Vec<_>>()
                    .join(",")
            )
        }
        _ => v.to_string(),
    }
}
/// Pinned AgentMessage character estimator, distinct from generic AI Context estimation.
pub fn estimate_tokens(message: &AgentMessage) -> Result<SafeUnsigned, ContentError> {
    let v = value(message)?;
    let chars = match v["role"].as_str() {
        Some("user" | "custom" | "toolResult") => content_chars(&v["content"]),
        Some("assistant") => v["content"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|b| match b["type"].as_str() {
                Some("text") => utf16(b["text"].as_str().unwrap_or("")),
                Some("thinking") => utf16(b["thinking"].as_str().unwrap_or("")),
                Some("toolCall") => {
                    utf16(b["name"].as_str().unwrap_or("")) + utf16(&js_json(&b["arguments"]))
                }
                _ => 0,
            })
            .sum(),
        Some("bashExecution") => {
            utf16(v["command"].as_str().unwrap_or("")) + utf16(v["output"].as_str().unwrap_or(""))
        }
        Some("branchSummary" | "compactionSummary") => utf16(v["summary"].as_str().unwrap_or("")),
        _ => 0,
    };
    number(chars.div_ceil(4))
}
/// Source context estimate: most recent usable usage, plus trailing estimates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextEstimate {
    pub tokens: SafeUnsigned,
    pub usage_tokens: SafeUnsigned,
    pub trailing_tokens: SafeUnsigned,
    pub last_usage_index: Option<SafeUnsigned>,
}
/// Provider context usage, honoring a nonzero reported total.
pub fn context_tokens(u: &Usage) -> Result<SafeUnsigned, ContentError> {
    if u.total_tokens.get() > 0 {
        return Ok(u.total_tokens);
    }
    number(add(
        add(u.input.get(), u.output.get())?,
        add(u.cache_read.get(), u.cache_write.get())?,
    )?)
}
/// Estimate without imposing generic AI timestamp applicability rules.
pub fn estimate_context(messages: &[AgentMessage]) -> Result<ContextEstimate, ContentError> {
    let mut usage = 0;
    let mut last = None;
    for (i, m) in messages.iter().enumerate().rev() {
        if let AgentMessage::BuiltIn(Message::Assistant(a)) = m
            && !matches!(
                a.stop_reason,
                pi_ai_core::StopReason::Error | pi_ai_core::StopReason::Aborted
            )
        {
            let n = context_tokens(&a.usage)?.get();
            if n > 0 {
                usage = n;
                last = Some(i);
                break;
            }
        }
    }
    let mut trailing = 0;
    for m in &messages[last.map_or(0, |i| i + 1)..] {
        trailing = add(trailing, estimate_tokens(m)?.get())?
    }
    Ok(ContextEstimate {
        tokens: number(add(usage, trailing)?)?,
        usage_tokens: number(usage)?,
        trailing_tokens: number(trailing)?,
        last_usage_index: last.map(|i| number(i as u64)).transpose()?,
    })
}
/// Strict greater-than using signed subtraction, including reserve > window.
pub fn should_compact(
    tokens: SafeUnsigned,
    window: SafeUnsigned,
    settings: &CompactionSettings,
) -> bool {
    settings.enabled
        && i128::from(tokens.get())
            > i128::from(window.get()) - i128::from(settings.reserve_tokens.get())
}
fn text(v: &Value, separator: &str) -> String {
    if let Some(s) = v.as_str() {
        return s.into();
    }
    v.as_array()
        .into_iter()
        .flatten()
        .filter(|b| b["type"] == "text")
        .filter_map(|b| b["text"].as_str())
        .collect::<Vec<_>>()
        .join(separator)
}
/// Serialize provider-neutral messages using the pinned summary transcript format.
pub fn serialize_conversation(messages: &[Message]) -> Result<String, ContentError> {
    Ok(serialize_conversation_text(messages)?.text)
}

/// Scalar prompt text plus its logical length before provider surrogate removal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummaryText {
    pub text: String,
    pub logical_utf16_units: SafeUnsigned,
}

/// Preserve exact UTF-16 truncation/counting, then apply the selected providers'
/// removal of cut-created lone halves at Rust's scalar-string boundary.
pub fn serialize_conversation_text(messages: &[Message]) -> Result<SummaryText, ContentError> {
    let mut parts = vec![];
    let mut removed_units = 0;
    for m in messages {
        let v = serde_json::to_value(m).map_err(|_| ContentError::InvalidInput)?;
        match v["role"].as_str() {
            Some("user") => {
                let t = text(&v["content"], "");
                if !t.is_empty() {
                    parts.push(format!("[User]: {t}"))
                }
            }
            Some("assistant") => {
                let blocks = v["content"].as_array().ok_or(ContentError::InvalidInput)?;
                let thoughts: Vec<_> = blocks
                    .iter()
                    .filter(|b| b["type"] == "thinking")
                    .filter_map(|b| b["thinking"].as_str())
                    .collect();
                if !thoughts.is_empty() {
                    parts.push(format!("[Assistant thinking]: {}", thoughts.join("\n")))
                }
                if blocks.iter().any(|b| b["type"] == "text") {
                    parts.push(format!("[Assistant]: {}", text(&v["content"], "\n")))
                }
                let mut calls = vec![];
                for b in blocks.iter().filter(|b| b["type"] == "toolCall") {
                    let args = b["arguments"]
                        .as_object()
                        .ok_or(ContentError::InvalidInput)?;
                    let pairs = js_entries(args);
                    calls.push(format!(
                        "{}({})",
                        b["name"].as_str().unwrap_or(""),
                        pairs
                            .into_iter()
                            .map(|(k, v)| format!("{k}={}", js_json(v)))
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                }
                if !calls.is_empty() {
                    parts.push(format!("[Assistant tool calls]: {}", calls.join("; ")))
                }
            }
            Some("toolResult") => {
                let mut t = text(&v["content"], "");
                let units: Vec<_> = t.encode_utf16().collect();
                if units.len() > 2000 {
                    t = char::decode_utf16(units[..2000].iter().copied())
                        .filter_map(Result::ok)
                        .collect();
                    removed_units = add(removed_units, 2000 - utf16(&t))?;
                    t.push_str(&format!(
                        "\n\n[... {} more characters truncated]",
                        units.len() - 2000
                    ))
                }
                if !t.is_empty() {
                    parts.push(format!("[Tool result]: {t}"))
                }
            }
            _ => {}
        }
    }
    let text = parts.join("\n\n");
    let logical_utf16_units = number(add(utf16(&text), removed_units)?)?;
    Ok(SummaryText {
        text,
        logical_utf16_units,
    })
}
/// Append pinned file metadata tags.
pub fn format_files(read: &[String], modified: &[String]) -> String {
    let mut sections = vec![];
    if !read.is_empty() {
        sections.push(format!("<read-files>\n{}\n</read-files>", read.join("\n")))
    }
    if !modified.is_empty() {
        sections.push(format!(
            "<modified-files>\n{}\n</modified-files>",
            modified.join("\n")
        ))
    }
    if sections.is_empty() {
        String::new()
    } else {
        format!("\n\n{}", sections.join("\n\n"))
    }
}
