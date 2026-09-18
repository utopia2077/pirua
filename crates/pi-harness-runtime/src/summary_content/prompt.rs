use super::*;
use pi_ai_core::{FiniteF64, Message, Model, StopReason, Usage, UsageCost};
#[path = "prompts.rs"]
mod prompts;
use prompts::*;
/// A process-local request template; stamp its user message only at actual intent.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SummaryRequestSpec {
    pub system_prompt: String,
    pub prompt: String,
    pub max_tokens: SafeUnsigned,
    /// Complete prompt length before removal of cut-created lone surrogate units.
    pub prompt_utf16_units: SafeUnsigned,
    pub thinking: AgentThinkingLevel,
}
fn cap(reserve: SafeUnsigned, ratio: f64, model: &Model) -> Result<SafeUnsigned, ContentError> {
    let desired = (reserve.get() as f64 * ratio).floor() as u64;
    number(if model.max_tokens.get() > 0 {
        desired.min(model.max_tokens.get())
    } else {
        desired
    })
}
fn thinking(model: &Model, t: AgentThinkingLevel) -> AgentThinkingLevel {
    if model.reasoning {
        t
    } else {
        AgentThinkingLevel::Off
    }
}
fn request(
    messages: &[Message],
    instructions: &str,
    previous: Option<&str>,
    cap: SafeUnsigned,
    t: AgentThinkingLevel,
) -> Result<SummaryRequestSpec, ContentError> {
    let transcript = serialize_conversation_text(messages)?;
    let removed_units = transcript
        .logical_utf16_units
        .get()
        .checked_sub(transcript.text.encode_utf16().count() as u64)
        .ok_or(ContentError::InvalidInput)?;
    let mut prompt = format!("<conversation>\n{}\n</conversation>\n\n", transcript.text);
    if let Some(s) = previous.filter(|s| !s.is_empty()) {
        prompt.push_str(&format!("<previous-summary>\n{s}\n</previous-summary>\n\n"))
    }
    prompt.push_str(instructions);
    Ok(SummaryRequestSpec {
        system_prompt: SUMMARIZATION_SYSTEM_PROMPT.into(),
        prompt_utf16_units: number(add(prompt.encode_utf16().count() as u64, removed_units)?)?,
        prompt,
        max_tokens: cap,
        thinking: t,
    })
}
/// Build exactly one or two specifications, with caller-converted history/prefix.
/// No callback, clock, request, retry or persistence is performed.
pub fn compaction_requests(
    preparation: &CompactionPreparation,
    history: &[Message],
    prefix: &[Message],
    model: &Model,
    custom: Option<&str>,
    level: AgentThinkingLevel,
) -> Result<Vec<SummaryRequestSpec>, ContentError> {
    let mut requests = vec![];
    let split = preparation.is_split_turn && !preparation.turn_prefix_messages.is_empty();
    if !split || !preparation.messages_to_summarize.is_empty() {
        let mut instructions = if preparation
            .previous_summary
            .as_ref()
            .is_some_and(|s| !s.is_empty())
        {
            UPDATE_SUMMARIZATION_PROMPT
        } else {
            SUMMARIZATION_PROMPT
        }
        .to_owned();
        if let Some(c) = custom.filter(|s| !s.is_empty()) {
            instructions.push_str(&format!("\n\nAdditional focus: {c}"))
        }
        requests.push(request(
            history,
            &instructions,
            preparation.previous_summary.as_deref(),
            cap(preparation.settings.reserve_tokens, 0.8, model)?,
            thinking(model, level),
        )?)
    }
    if split {
        requests.push(request(
            prefix,
            TURN_PREFIX_SUMMARIZATION_PROMPT,
            None,
            cap(preparation.settings.reserve_tokens, 0.5, model)?,
            thinking(model, level),
        )?)
    }
    Ok(requests)
}
/// Empty branch yields no request; the response combiner supplies the pinned fallback.
pub fn branch_request(
    preparation: &BranchPreparation,
    messages: &[Message],
    custom: Option<&str>,
    replace: bool,
) -> Result<Option<SummaryRequestSpec>, ContentError> {
    if preparation.messages.is_empty() {
        return Ok(None);
    }
    let instructions = match custom.filter(|s| !s.is_empty()) {
        Some(c) if replace => c.into(),
        Some(c) => format!("{BRANCH_SUMMARY_PROMPT}\n\nAdditional focus: {c}"),
        None => BRANCH_SUMMARY_PROMPT.into(),
    };
    request(
        messages,
        &instructions,
        None,
        number(2048)?,
        AgentThinkingLevel::Off,
    )
    .map(Some)
}
/// Add two provider usage receipts without filling absent optional counters.
pub fn combine_usage(a: &Usage, b: &Usage) -> Result<Usage, ContentError> {
    let sum = |a: SafeUnsigned, b: SafeUnsigned| number(add(a.get(), b.get())?);
    let optional = |a: Option<SafeUnsigned>,
                    b: Option<SafeUnsigned>|
     -> Result<Option<SafeUnsigned>, ContentError> {
        if a.is_none() && b.is_none() {
            Ok(None)
        } else {
            number(add(
                a.map_or(0, SafeUnsigned::get),
                b.map_or(0, SafeUnsigned::get),
            )?)
            .map(Some)
        }
    };
    let cost = |a: FiniteF64, b: FiniteF64| {
        FiniteF64::new(a.get() + b.get()).map_err(|_| ContentError::NumberRange)
    };
    Ok(Usage {
        input: sum(a.input, b.input)?,
        output: sum(a.output, b.output)?,
        cache_read: sum(a.cache_read, b.cache_read)?,
        cache_write: sum(a.cache_write, b.cache_write)?,
        cache_write_1h: optional(a.cache_write_1h, b.cache_write_1h)?,
        reasoning: optional(a.reasoning, b.reasoning)?,
        total_tokens: sum(a.total_tokens, b.total_tokens)?,
        cost: UsageCost {
            input: cost(a.cost.input, b.cost.input)?,
            output: cost(a.cost.output, b.cost.output)?,
            cache_read: cost(a.cost.cache_read, b.cost.cache_read)?,
            cache_write: cost(a.cost.cache_write, b.cost.cache_write)?,
            total: cost(a.cost.total, b.cost.total)?,
        },
    })
}
fn response_text(r: &SettledAssistantMessage) -> Result<String, ContentError> {
    if matches!(
        r.as_message().stop_reason,
        StopReason::Error | StopReason::Aborted
    ) {
        return Err(ContentError::ProviderResponse);
    }
    Ok(r.as_message()
        .content
        .iter()
        .filter_map(|c| {
            if let pi_ai_core::AssistantContent::Text(t) = c {
                Some(t.text.as_str())
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("\n"))
}
/// Combine only the producing attempt's fully settled responses; ledger is untouched.
pub fn finish_compaction(
    p: &CompactionPreparation,
    responses: &[SettledAssistantMessage],
) -> Result<CompactResult, ContentError> {
    let split = p.is_split_turn && !p.turn_prefix_messages.is_empty();
    let expected = if split && !p.messages_to_summarize.is_empty() {
        2
    } else {
        1
    };
    if responses.len() != expected {
        return Err(ContentError::InvalidInput);
    }
    let mut summary = if split {
        let history = if expected == 2 {
            response_text(&responses[0])?
        } else {
            "No prior history.".into()
        };
        format!(
            "{history}\n\n---\n\n**Turn Context (split turn):**\n\n{}",
            response_text(&responses[expected - 1])?
        )
    } else {
        response_text(&responses[0])?
    };
    let usage = if expected == 2 {
        combine_usage(
            &responses[0].as_message().usage,
            &responses[1].as_message().usage,
        )?
    } else {
        responses[0].as_message().usage.clone()
    };
    let (read, modified) = file_lists(&p.file_ops);
    summary.push_str(&format_files(&read, &modified));
    let details =
        serde_json::from_value(serde_json::json!({"readFiles":read,"modifiedFiles":modified}))
            .map_err(|_| ContentError::InvalidInput)?;
    Ok(CompactResult {
        summary,
        tokens_before: p.tokens_before,
        retained_tail: p.retained_tail.clone(),
        usage: Some(usage),
        details: Presence::Value(details),
    })
}
/// Empty generated branch is distinct from a hook-supplied or unsummarized result.
pub fn finish_branch(
    p: &BranchPreparation,
    response: Option<&SettledAssistantMessage>,
) -> Result<BranchSummaryResult, ContentError> {
    if p.messages.is_empty() {
        if response.is_some() {
            return Err(ContentError::InvalidInput);
        }
        return Ok(BranchSummaryResult {
            summary: "No content to summarize".into(),
            usage: None,
            read_files: vec![],
            modified_files: vec![],
        });
    }
    let r = response.ok_or(ContentError::InvalidInput)?;
    let (read, modified) = file_lists(&p.file_ops);
    Ok(BranchSummaryResult {
        summary: format!(
            "{BRANCH_SUMMARY_PREAMBLE}{}{}",
            response_text(r)?,
            format_files(&read, &modified)
        ),
        usage: Some(r.as_message().usage.clone()),
        read_files: read,
        modified_files: modified,
    })
}
