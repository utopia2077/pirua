//! Bounded detached context projection and explicit trusted materialization stages.
use crate::{session::RuntimeSession, storage::CheckedStorage};
use pi_agent_schema::session_values::*;
use pi_agent_schema::*;
use std::collections::BTreeMap;

/// A detached summary awaiting a caller-chosen message representation.
#[derive(Clone, Debug, PartialEq)]
pub struct ContextSummary {
    /// Structural kind.
    pub kind: SummaryKind,
    /// Entry that carries this summary.
    pub source_id: EntryId,
    /// Committed entry time.
    pub timestamp: SafeUnsigned,
    /// Exact stored summary text.
    pub text: String,
}
/// Ordered context input, before summary rendering and transformation.
#[derive(Clone, Debug, PartialEq)]
pub enum ContextItem {
    /// Exact summary source.
    Summary(ContextSummary),
    /// Projected or directly stored message.
    Message(AgentMessage),
}
/// Explicit trusted application policy for representing summaries.
pub trait SummaryRenderer: Send + Sync {
    /// Render one detached summary; no authority lock is held.
    fn render(
        &self,
        summary: ContextSummary,
    ) -> BoxFuture<'_, Result<AgentMessage, HarnessRuntimeError>>;
}
/// Read/schema failure versus trusted application computation failure.
#[derive(Debug)]
pub enum ContextError {
    /// Session read or codec failure.
    Session(SessionError),
    /// Caller computation failed.
    Application(HarnessRuntimeError),
}
impl std::fmt::Display for ContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Session(_) => "context session failure",
            Self::Application(_) => "context application failure",
        })
    }
}
impl std::error::Error for ContextError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Session(e) => e,
            Self::Application(e) => e,
        })
    }
}
fn retain(message: &AgentMessage) -> bool {
    !matches!(message, AgentMessage::BuiltIn(Message::Assistant(message)) if matches!(message.stop_reason,StopReason::Error|StopReason::Aborted|StopReason::Deferred))
}
impl<S: CheckedStorage + 'static, M: SessionMetadataRecord> RuntimeSession<S, M> {
    /// Read exactly the selected branch through its newest compaction, then invoke
    /// borrowed custom projectors on detached rows in oldest-first order.
    pub async fn context_items(
        &self,
        projectors: &BTreeMap<CustomType, &dyn EntryProjector>,
    ) -> Result<Vec<ContextItem>, ContextError> {
        let mut entries = self
            .find_entries_on_branch(BranchScan {
                order: Some(BranchOrder::NewestFirst),
                stop_at_type: Some(EntryType::Compaction),
                ..Default::default()
            })
            .await
            .map_err(ContextError::Session)?;
        entries.reverse();
        let mut items = Vec::new();
        for entry in entries {
            match entry {
                Entry::Message(e) => {
                    if retain(&e.message) {
                        items.push(ContextItem::Message(e.message));
                    }
                }
                Entry::Compaction(e) => {
                    items.push(ContextItem::Summary(ContextSummary {
                        kind: SummaryKind::Compaction,
                        source_id: e.id,
                        timestamp: e.timestamp,
                        text: e.summary,
                    }));
                    items.extend(
                        e.retained_tail
                            .into_iter()
                            .filter(retain)
                            .map(ContextItem::Message),
                    );
                }
                Entry::BranchSummary(e) => items.push(ContextItem::Summary(ContextSummary {
                    kind: SummaryKind::BranchSummary,
                    source_id: e.id,
                    timestamp: e.timestamp,
                    text: e.summary,
                })),
                Entry::Custom(e) => {
                    if let Some(projector) = projectors.get(&e.custom_type)
                        && let Some(messages) = projector
                            .project(e)
                            .await
                            .map_err(ContextError::Application)?
                    {
                        items.extend(
                            messages
                                .into_iter()
                                .filter(retain)
                                .map(ContextItem::Message),
                        );
                    }
                }
            }
        }
        Ok(items)
    }
}
/// Materialize summaries in source order, then transform context and convert to
/// provider-neutral messages. Hook dispatch and tool healing belong to the caller.
pub async fn materialize(
    items: Vec<ContextItem>,
    renderer: &dyn SummaryRenderer,
    transform: Option<&dyn AgentContextTransform>,
    converter: &dyn ProviderMessageConverter,
    cancellation: &dyn Cancellation,
) -> Result<Vec<Message>, ContextError> {
    let mut messages = Vec::new();
    for item in items {
        messages.push(match item {
            ContextItem::Message(message) => message,
            ContextItem::Summary(summary) => renderer
                .render(summary)
                .await
                .map_err(ContextError::Application)?,
        });
    }
    if let Some(transform) = transform {
        messages = transform
            .transform(messages, cancellation)
            .await
            .map_err(ContextError::Application)?;
    }
    converter
        .convert(messages)
        .await
        .map_err(ContextError::Application)
}
