//! Canonical projection based compaction and branch-summary preparation.
mod prompts;
use crate::{CompactionSettings, estimate_projected_context_tokens, estimate_tokens};
use pi_agent::{AgentError, AgentMessage};
use pi_ai::Usage;
use pi_coding_session::{EntryContent, SessionEntry, SessionProjection, SessionSnapshot};
pub use prompts::*;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc};

struct SummaryClock;
impl pi_ai::Clock for SummaryClock {
    fn now_unix_ms(&self) -> u64 {
        crate::messages::now()
    }
    fn sleep<'a>(
        &'a self,
        duration: std::time::Duration,
        cancellation: &'a pi_ai::CancellationToken,
    ) -> pi_ai::BoxFuture<'a, Result<(), pi_ai::Cancelled>> {
        Box::pin(async move {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => Err(pi_ai::Cancelled),
                _ = tokio::time::sleep(duration) => Ok(()),
            }
        })
    }
}
struct SummaryEntropy;
impl pi_ai::UuidV7Entropy for SummaryEntropy {
    fn random_bytes(&self) -> Result<[u8; 16], pi_ai::UuidV7EntropyError> {
        use ring::rand::SecureRandom;
        let mut bytes = [0; 16];
        ring::rand::SystemRandom::new()
            .fill(&mut bytes)
            .map_err(|_| pi_ai::UuidV7EntropyError::new())?;
        Ok(bytes)
    }
}
pub(crate) fn summary_ids() -> pi_ai::UuidV7Generator {
    pi_ai::UuidV7Generator::new(Arc::new(SummaryClock), Arc::new(SummaryEntropy))
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileOperations {
    pub read: BTreeSet<String>,
    pub written: BTreeSet<String>,
    pub edited: BTreeSet<String>,
}
impl FileOperations {
    pub fn extract(&mut self, message: &Value) {
        let calls = if message["role"] == "assistant" {
            message["content"].as_array().cloned().unwrap_or_default()
        } else if message["role"] == "toolResult" {
            message["nestedCalls"]["calls"]
                .as_array()
                .cloned()
                .unwrap_or_default()
        } else {
            Vec::new()
        };
        for call in calls {
            if let Some(path) = call["arguments"]["path"].as_str() {
                match call["name"].as_str() {
                    Some("read") => {
                        self.read.insert(path.into());
                    }
                    Some("write") => {
                        self.written.insert(path.into());
                    }
                    Some("edit") => {
                        self.edited.insert(path.into());
                    }
                    _ => {}
                }
            }
        }
    }
    pub fn lists(&self) -> (Vec<String>, Vec<String>) {
        let modified = self
            .edited
            .union(&self.written)
            .cloned()
            .collect::<BTreeSet<_>>();
        (
            self.read.difference(&modified).cloned().collect(),
            modified.into_iter().collect(),
        )
    }
    pub fn suffix(&self) -> String {
        let (read, modified) = self.lists();
        let mut parts = Vec::new();
        if !read.is_empty() {
            parts.push(format!("<read-files>\n{}\n</read-files>", read.join("\n")));
        }
        if !modified.is_empty() {
            parts.push(format!(
                "<modified-files>\n{}\n</modified-files>",
                modified.join("\n")
            ));
        }
        if parts.is_empty() {
            String::new()
        } else {
            format!("\n\n{}", parts.join("\n\n"))
        }
    }
    pub fn details(&self) -> Value {
        let (r, m) = self.lists();
        json!({"readFiles":r,"modifiedFiles":m})
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionPreparation {
    pub first_kept_entry_id: String,
    pub messages_to_summarize: Vec<AgentMessage>,
    pub turn_prefix_messages: Vec<AgentMessage>,
    pub is_split_turn: bool,
    pub tokens_before: u64,
    pub previous_summary: Option<String>,
    pub file_ops: FileOperations,
    pub settings: CompactionSettings,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CompactionResult {
    pub summary: String,
    pub first_kept_entry_id: Option<String>,
    pub tokens_before: u64,
    pub details: Option<Value>,
    pub usage: Option<Usage>,
}
fn turn_start(message: &Value) -> bool {
    matches!(
        message["role"].as_str(),
        Some("user" | "bashExecution" | "custom" | "branchSummary" | "compactionSummary")
    )
}
fn cut_point(message: &Value) -> bool {
    turn_start(message) || message["role"] == "assistant"
}
fn messages(
    entries: &[pi_coding_session::ProjectedEntry],
) -> Result<Vec<AgentMessage>, AgentError> {
    entries
        .iter()
        .flat_map(|e| e.messages.iter())
        .filter(|m| m["role"] != "system")
        .cloned()
        .map(crate::messages::decode)
        .collect()
}
pub fn prepare_compaction(
    projection: &SessionProjection,
    branch: &[SessionEntry],
    settings: &CompactionSettings,
) -> Result<Option<CompactionPreparation>, AgentError> {
    if branch
        .last()
        .is_some_and(|e| matches!(e.content, EntryContent::Compaction { .. }))
    {
        return Ok(None);
    }
    let entries = &projection.entries;
    let prev = entries.iter().position(|e| {
        matches!(e.source_entry.content, EntryContent::Compaction { .. }) && !e.messages.is_empty()
    });
    let start = prev.map(|i| i + 1).unwrap_or(0);
    let cuts = (start..entries.len())
        .filter(|i| {
            !matches!(
                entries[*i].source_entry.content,
                EntryContent::Compaction { .. }
            ) && entries[*i].messages.iter().any(cut_point)
        })
        .collect::<Vec<_>>();
    let Some(mut cut) = cuts.first().copied() else {
        return Ok(None);
    };
    let mut accumulated = 0;
    let mut exceeded = false;
    for i in (start..entries.len()).rev() {
        let tokens = entries[i].messages.iter().map(estimate_tokens).sum::<u64>();
        if tokens == 0 {
            continue;
        }
        accumulated += tokens;
        if accumulated >= settings.keep_recent_tokens.get() {
            cut = cuts
                .iter()
                .copied()
                .find(|v| *v >= i)
                .unwrap_or(*cuts.last().expect("nonempty"));
            exceeded = true;
            break;
        }
    }
    let intrinsic = |entry: &pi_coding_session::ProjectedEntry| {
        !matches!(entry.source_entry.content, EntryContent::ContextEdit { .. })
            && !pi_coding_session::project_entry(&entry.source_entry).is_empty()
    };
    let suffix = &entries[cut + 1..];
    let omitted = suffix
        .iter()
        .filter(|e| intrinsic(e) && e.messages.is_empty())
        .map(|e| e.source_entry.id.as_str())
        .collect::<BTreeSet<_>>();
    let external=suffix.iter().any(|e|matches!(&e.source_entry.content,EntryContent::ContextEdit{target_id,replacement:Some(_)} if !omitted.contains(target_id.as_str())));
    if exceeded&&!external&&suffix.iter().any(|e|matches!(&e.source_entry.content,EntryContent::Message{message} if message["role"]=="assistant")&&intrinsic(e)&&e.messages.is_empty())&&suffix.iter().all(|e|!matches!(e.source_entry.content,EntryContent::Compaction{..})&&(!intrinsic(e)||e.messages.is_empty())){cut+=1;}
    while cut > start {
        let prior = &entries[cut - 1];
        if matches!(prior.source_entry.content, EntryContent::Compaction { .. })
            || !prior.messages.is_empty()
        {
            break;
        }
        cut -= 1;
    }
    let Some(first) = entries.get(cut) else {
        return Ok(None);
    };
    let starts = !matches!(first.source_entry.content, EntryContent::Compaction { .. })
        && first.messages.iter().any(turn_start);
    let turn = if starts {
        None
    } else {
        (start..cut).rev().find(|i| {
            !matches!(
                entries[*i].source_entry.content,
                EntryContent::Compaction { .. }
            ) && entries[*i].messages.iter().any(turn_start)
        })
    };
    let history = messages(&entries[start..turn.unwrap_or(cut)])?;
    let prefix = if let Some(turn) = turn {
        messages(&entries[turn..cut])?
    } else {
        Vec::new()
    };
    if history.is_empty() && prefix.is_empty() {
        return Ok(None);
    }
    let previous_summary = prev.and_then(|i| {
        if let EntryContent::Compaction { summary, .. } = &entries[i].source_entry.content {
            Some(summary.clone())
        } else {
            None
        }
    });
    let mut file_ops = FileOperations::default();
    if let Some(i) = prev
        && let EntryContent::Compaction {
            details: Some(details),
            from_hook,
            ..
        } = &entries[i].source_entry.content
        && *from_hook != Some(true)
    {
        for name in details["readFiles"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            file_ops.read.insert(name.into());
        }
        for name in details["modifiedFiles"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            file_ops.edited.insert(name.into());
        }
    }
    for m in history.iter().chain(&prefix) {
        file_ops.extract(
            &serde_json::to_value(m).map_err(|e| AgentError::InvalidContext(e.to_string()))?,
        );
    }
    Ok(Some(CompactionPreparation {
        first_kept_entry_id: first.source_entry.id.clone(),
        messages_to_summarize: history,
        turn_prefix_messages: prefix,
        is_split_turn: turn.is_some(),
        tokens_before: estimate_projected_context_tokens(projection, branch).tokens,
        previous_summary,
        file_ops,
        settings: settings.clone(),
    }))
}
pub fn serialize_conversation(messages: &[pi_ai::Message]) -> String {
    let mut parts = Vec::new();
    for m in messages {
        let v = serde_json::to_value(m).expect("typed message");
        let text = crate::messages::content_text(&v["content"], "");
        match v["role"].as_str() {
            Some("user") if !text.is_empty() => parts.push(format!("[User]: {text}")),
            Some("assistant") => {
                let blocks = v["content"].as_array().expect("assistant blocks");
                if blocks.iter().any(|b| b["type"] == "text") {
                    parts.push(format!(
                        "[Assistant]: {}",
                        crate::messages::content_text(&v["content"], "\n")
                    ));
                }
                let calls = blocks
                    .iter()
                    .filter(|b| b["type"] == "toolCall")
                    .map(|b| {
                        let args = b["arguments"]
                            .as_object()
                            .map(|o| {
                                o.iter()
                                    .map(|(k, v)| format!("{k}={v}"))
                                    .collect::<Vec<_>>()
                                    .join(", ")
                            })
                            .unwrap_or_default();
                        format!("{}({args})", b["name"].as_str().unwrap_or_default())
                    })
                    .collect::<Vec<_>>();
                if !calls.is_empty() {
                    parts.push(format!("[Assistant tool calls]: {}", calls.join("; ")));
                }
            }
            Some("toolResult") if !text.is_empty() => {
                let length = text.encode_utf16().count();
                let text = if length > 2000 {
                    let prefix = String::from_utf16_lossy(
                        &text.encode_utf16().take(2000).collect::<Vec<_>>(),
                    );
                    format!(
                        "{prefix}\n\n[... {} more characters truncated]",
                        length - 2000
                    )
                } else {
                    text
                };
                parts.push(format!("[Tool result]: {text}"));
            }
            _ => {}
        }
    }
    parts.join("\n\n")
}
pub fn collect_branch_entries(
    snapshot: &SessionSnapshot,
    target_id: &str,
) -> (Vec<SessionEntry>, Option<String>) {
    let old = branch_path(snapshot, snapshot.leaf_id.as_deref());
    let target = branch_path(snapshot, Some(target_id));
    let common = target
        .iter()
        .rev()
        .find(|e| old.iter().any(|o| o.id == e.id))
        .map(|e| e.id.clone());
    let start = common
        .as_ref()
        .and_then(|id| old.iter().position(|e| e.id == *id))
        .map(|i| i + 1)
        .unwrap_or(0);
    (old[start..].to_vec(), common)
}
pub(crate) fn branch_path(snapshot: &SessionSnapshot, leaf: Option<&str>) -> Vec<SessionEntry> {
    let mut current = leaf;
    let mut entries = Vec::new();
    while let Some(entry) = current.and_then(|id| snapshot.entries.iter().find(|e| e.id == id)) {
        entries.push(entry.clone());
        current = entry.parent_id.as_deref();
    }
    entries.reverse();
    entries
}

use crate::{
    CodingSession, SessionError,
    hooks::*,
    session::{combine_usage, lock},
};
use pi_agent::{AgentRequest, CancellationToken};
use pi_ai::{Context, Message, SimpleStreamOptionsWire, StopReason};
struct SummaryCall {
    stream: pi_agent::StreamFn,
    request: AgentRequest,
}
impl pi_ai::AssistantRetryProducer for SummaryCall {
    fn produce<'a>(
        &'a self,
    ) -> pi_ai::BoxFuture<'a, Result<pi_ai::AssistantMessage, pi_ai::AssistantRetryError>> {
        Box::pin(async move {
            let stream = (self.stream)(self.request.clone())
                .await
                .map_err(pi_ai::AssistantRetryError::with_source)?;
            Ok(stream.result().await)
        })
    }
}
impl CodingSession {
    pub async fn compact(
        &self,
        custom_instructions: Option<String>,
    ) -> Result<CompactionResult, SessionError> {
        let _guard = self.idle_guard()?;
        let signal = CancellationToken::new();
        let mut operation = crate::session_lifecycle::OperationGuard::new(self, signal.clone())?;
        let result = self
            .compact_internal(custom_instructions, CompactionReason::Manual, false, signal)
            .await;
        operation.complete();
        result?.ok_or_else(|| SessionError::Invalid("Nothing to compact".into()))
    }
    pub fn abort_compaction(&self) {
        if let Some(signal) = lock(&self.inner.state).operation.clone() {
            signal.cancel();
        }
    }
    pub(crate) async fn compact_internal(
        &self,
        instructions: Option<String>,
        reason: CompactionReason,
        will_retry: bool,
        signal: CancellationToken,
    ) -> Result<Option<CompactionResult>, SessionError> {
        let preparation = {
            let manager = lock(&self.inner.manager);
            let settings = lock(&self.inner.state).compaction.clone();
            prepare_compaction(&manager.projection(), &manager.get_branch(), &settings)?
        };
        let Some(preparation) = preparation else {
            return Ok(None);
        };
        self.emit(SessionEvent::CompactionStart { reason, will_retry })
            .await?;
        let result = async {
            let hook = self
                .inner
                .hooks
                .before_compact(preparation.clone(), reason, signal.clone())
                .await?;
            if hook.cancel {
                return Ok(None);
            }
            let from_hook = hook.compaction.is_some();
            let result = if let Some(result) = hook.compaction {
                result
            } else {
                self.generate_compaction(&preparation, instructions, signal.clone())
                    .await?
            };
            if signal.is_cancelled() {
                return Ok(None);
            }
            let typed = self.messages()?;
            let llm = crate::convert_to_llm(typed)?;
            let system = pi_ai::get_current_system_message(&llm)
                .map(|m| serde_json::to_value(Message::System(m)))
                .transpose()?;
            let entry = {
                let mut manager = lock(&self.inner.manager);
                let id = manager.append_compaction(pi_coding_session::CompactionOptions {
                    summary: result.summary.clone(),
                    first_kept_entry_id: result.first_kept_entry_id.clone(),
                    tokens_before: result.tokens_before,
                    details: result.details.clone(),
                    from_hook: Some(from_hook),
                    usage: result
                        .usage
                        .as_ref()
                        .map(serde_json::to_value)
                        .transpose()?,
                    system_message: system,
                })?;
                manager
                    .get_entry(&id)
                    .expect("committed compaction")
                    .clone()
            };
            self.refresh()?;
            self.cancel_cache_warming();
            self.emit(SessionEvent::SessionCompact {
                entry,
                reason,
                from_hook,
            })
            .await?;
            Ok::<_, SessionError>(Some(result))
        }
        .await;
        self.emit(SessionEvent::CompactionEnd {
            reason,
            result: result.as_ref().ok().cloned().flatten(),
            aborted: signal.is_cancelled(),
            will_retry: will_retry && result.as_ref().is_ok_and(Option::is_some),
            error: result.as_ref().err().map(ToString::to_string),
        })
        .await?;
        result
    }
    async fn generate_compaction(
        &self,
        p: &CompactionPreparation,
        instructions: Option<String>,
        signal: CancellationToken,
    ) -> Result<CompactionResult, SessionError> {
        let mut usage = None;
        let mut history = p
            .previous_summary
            .clone()
            .unwrap_or_else(|| "No prior history.".into());
        if !p.messages_to_summarize.is_empty() || !p.is_split_turn {
            let prompt = summary_prompt(
                &p.messages_to_summarize,
                p.previous_summary.as_deref(),
                instructions.as_deref(),
            )?;
            let result = self.complete_summary(prompt, signal.clone(), false).await?;
            history = result.summary;
            usage = result.usage;
        }
        let mut summary = history;
        if p.is_split_turn && !p.turn_prefix_messages.is_empty() {
            let conversation =
                serialize_conversation(&crate::convert_to_llm(p.turn_prefix_messages.clone())?);
            let mut prompt = format!(
                "<conversation>\n{conversation}\n</conversation>\n\n{TURN_PREFIX_SUMMARIZATION_PROMPT}"
            );
            if let Some(focus) = instructions {
                prompt.push_str(&format!("\n\nAdditional focus: {focus}"));
            }
            let prefix = self.complete_summary(prompt, signal, false).await?;
            summary = format!(
                "{summary}\n\n---\n\n**Turn Context (split turn):**\n\n{}",
                prefix.summary
            );
            usage = combine_usage(usage, prefix.usage);
        }
        summary.push_str(&p.file_ops.suffix());
        Ok(CompactionResult {
            summary,
            first_kept_entry_id: Some(p.first_kept_entry_id.clone()),
            tokens_before: p.tokens_before,
            details: Some(p.file_ops.details()),
            usage,
        })
    }
    pub(crate) async fn summarize_branch(
        &self,
        entries: &[SessionEntry],
        instructions: Option<String>,
        replace: bool,
        signal: CancellationToken,
    ) -> Result<SummaryResult, SessionError> {
        let selected = self.model();
        let budget = selected.context_window.get().saturating_sub(16384);
        let preparation = prepare_branch_entries(entries, budget)?;
        let kept = preparation.messages;
        let files = preparation.file_ops;
        if kept.is_empty() {
            return Ok(SummaryResult {
                summary: "No content to summarize".into(),
                details: None,
                usage: None,
            });
        }
        let conversation = serialize_conversation(&crate::convert_to_llm(kept)?);
        let base = if replace && instructions.is_some() {
            instructions.clone().unwrap_or_default()
        } else {
            let mut base = BRANCH_SUMMARY_PROMPT.to_owned();
            if let Some(focus) = instructions {
                base.push_str(&format!("\n\nAdditional focus: {focus}"));
            }
            base
        };
        let mut result = self
            .complete_summary(
                format!("<conversation>\n{conversation}\n</conversation>\n\n{base}"),
                signal,
                true,
            )
            .await?;
        result.summary = format!(
            "{BRANCH_SUMMARY_PREAMBLE}{}{}",
            result.summary,
            files.suffix()
        );
        result.details = Some(files.details());
        Ok(result)
    }
    async fn complete_summary(
        &self,
        prompt: String,
        signal: CancellationToken,
        branch: bool,
    ) -> Result<SummaryResult, SessionError> {
        let selected = self.model();
        let thinking = self.thinking_level();
        let messages = crate::convert_to_llm(self.messages()?)?;
        let route = if self.inner.models.is_virtual(&selected) {
            self.inner
                .models
                .resolve(crate::ModelRouteRequest {
                    selected_model: selected,
                    messages: messages.clone(),
                    thinking_level: thinking,
                    reason: crate::ModelRouteReason::Direct,
                    failed: None,
                    previous: self.route_previous(&messages),
                    state: None,
                    cancellation: signal.clone(),
                })
                .await?
        } else {
            crate::ModelRoute {
                model: selected,
                thinking_level: thinking,
                state: None,
            }
        };
        if self.inner.models.is_virtual(&route.model) {
            return Err(SessionError::Invalid(
                "Summarization route must select a physical model".into(),
            ));
        }
        let options = SimpleStreamOptionsWire {
            reasoning: pi_agent::provider_thinking(route.thinking_level),
            stream: pi_ai::StreamOptionsWire {
                session_id: Some(
                    self.inner
                        .summary_ids
                        .generate()
                        .map_err(|error| SessionError::Invalid(error.to_string()))?,
                ),
                cache_retention: Some(pi_ai::CacheRetention::None),
                ..Default::default()
            },
            ..Default::default()
        };
        let context = pi_ai::normalize_context(Context {
            system_prompt: Some(SUMMARIZATION_SYSTEM_PROMPT.into()),
            messages: vec![
                crate::messages::user_message(prompt, Vec::new())
                    .as_builtin()
                    .expect("user")
                    .clone(),
            ],
            tools: None,
        });
        let api_key = self
            .inner
            .models
            .api_key(route.model.clone(), signal.clone())
            .await?;
        let policy = lock(&self.inner.state).retry.clone();
        if signal.is_cancelled() {
            return Err(SessionError::Invalid("Summarization aborted".into()));
        }
        let response = pi_ai::retry_assistant_call(
            &SummaryCall {
                stream: self.inner.stream.clone(),
                request: AgentRequest {
                    model: route.model,
                    context,
                    options,
                    api_key,
                    cancellation: signal.clone(),
                },
            },
            Some(&policy),
            &signal,
            &pi_ai::ClockRetrySleeper::new(Arc::new(SummaryClock)),
            None,
        )
        .await
        .map_err(|error| {
            std::error::Error::source(&error)
                .and_then(|source| source.downcast_ref::<AgentError>())
                .cloned()
                .map(SessionError::Agent)
                .unwrap_or_else(|| SessionError::Invalid(error.to_string()))
        })?;
        if response.stop_reason == StopReason::Aborted {
            return Err(SessionError::Invalid("Summarization aborted".into()));
        }
        if let Some(failure) = summarization_failure(
            &response,
            if branch {
                "Branch summarization"
            } else {
                "Summarization"
            },
        ) {
            return Err(SessionError::Invalid(failure));
        }
        if response
            .content
            .iter()
            .any(|b| matches!(b, pi_ai::AssistantContent::ToolCall(_)))
        {
            return Err(SessionError::Invalid(
                if branch {
                    "Branch summarization attempted to call a tool"
                } else {
                    "Summarization attempted to call a tool"
                }
                .into(),
            ));
        }
        let value = serde_json::to_value(&response)?;
        let summary = crate::messages::content_text(&value["content"], "\n");
        if summary.trim().is_empty() {
            return Err(SessionError::Invalid(
                "Summarization produced no text".into(),
            ));
        }
        Ok(SummaryResult {
            summary,
            details: None,
            usage: Some(response.usage),
        })
    }
}
fn summary_prompt(
    messages: &[AgentMessage],
    previous: Option<&str>,
    instructions: Option<&str>,
) -> Result<String, AgentError> {
    let conversation = serialize_conversation(&crate::convert_to_llm(messages.to_vec())?);
    let mut prompt = format!("<conversation>\n{conversation}\n</conversation>\n\n");
    if let Some(previous) = previous {
        prompt.push_str(&format!(
            "<previous-summary>\n{previous}\n</previous-summary>\n\n"
        ));
    }
    prompt.push_str(if previous.is_some() {
        UPDATE_SUMMARIZATION_PROMPT
    } else {
        SUMMARIZATION_PROMPT
    });
    if let Some(instructions) = instructions {
        prompt.push_str(&format!("\n\nAdditional focus: {instructions}"));
    }
    Ok(prompt)
}

pub fn summarization_failure(response: &pi_ai::AssistantMessage, label: &str) -> Option<String> {
    match response.stop_reason {
        StopReason::Error => Some(format!(
            "{label} failed: {}",
            response
                .error_message
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("Unknown error")
        )),
        StopReason::Length => Some(format!(
            "{label} failed: generation hit the token cap and the summary is incomplete"
        )),
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub struct BranchPreparation {
    pub messages: Vec<AgentMessage>,
    pub file_ops: FileOperations,
    pub total_tokens: u64,
}
pub fn prepare_branch_entries(
    entries: &[SessionEntry],
    token_budget: u64,
) -> Result<BranchPreparation, AgentError> {
    let mut file_ops = FileOperations::default();
    let mut messages = Vec::new();
    let mut total_tokens = 0;
    for entry in entries {
        if let EntryContent::BranchSummary {
            details: Some(details),
            from_hook,
            ..
        } = &entry.content
            && *from_hook != Some(true)
        {
            for name in details["readFiles"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                file_ops.read.insert(name.into());
            }
            for name in details["modifiedFiles"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                file_ops.edited.insert(name.into());
            }
        }
    }
    for entry in entries.iter().rev() {
        let value = match &entry.content {
            EntryContent::Message { message } if message["role"] != "toolResult" => {
                Some(message.clone())
            }
            EntryContent::Compaction {
                summary,
                tokens_before,
                ..
            } => Some(
                json!({"role":"compactionSummary","summary":summary,"tokensBefore":tokens_before,"timestamp":entry.timestamp}),
            ),
            EntryContent::BranchSummary {
                summary, from_id, ..
            } => Some(
                json!({"role":"branchSummary","summary":summary,"fromId":from_id,"timestamp":entry.timestamp}),
            ),
            EntryContent::CustomMessage { .. } => {
                pi_coding_session::project_entry(entry).into_iter().next()
            }
            _ => None,
        };
        let Some(value) = value else { continue };
        file_ops.extract(&value);
        let tokens = estimate_tokens(&value);
        if token_budget > 0 && total_tokens + tokens > token_budget {
            if matches!(
                entry.content,
                EntryContent::Compaction { .. } | EntryContent::BranchSummary { .. }
            ) && (total_tokens as f64) < token_budget as f64 * 0.9
            {
                messages.push(crate::messages::decode(value)?);
                total_tokens += tokens;
            }
            break;
        }
        messages.push(crate::messages::decode(value)?);
        total_tokens += tokens;
    }
    messages.reverse();
    Ok(BranchPreparation {
        messages,
        file_ops,
        total_tokens,
    })
}
