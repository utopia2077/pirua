//! Detached structural input collection over actual committed rows.
use crate::{effects::RuntimeEffects, owner::RuntimeLane, summary_content};
use pi_agent_schema::*;
use pi_agent_session::{context::ContextSummary, storage::RuntimeStorage};
use pi_ai_core::SafeUnsigned;
use std::{collections::BTreeMap, task::Poll};

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) async fn structural_wait<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, HarnessRuntimeError>>,
    ) -> Result<T, HarnessRuntimeError> {
        let mut future = std::pin::pin!(future);
        let mut seal = Box::pin(self.lane.owner.admission.seal_signal());
        let mut retired = Box::pin(self.retired.cancelled());
        std::future::poll_fn(|cx| {
            if seal.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(self
                    .lane
                    .owner
                    .admission
                    .check()
                    .expect_err("sealed structure")));
            }
            if retired.as_mut().poll(cx).is_ready() {
                // The original operation was finalized by another Session writer.
                // This effect has no further owner; dropping its held callback here
                // must not change the lane's global admission state.
                return Poll::Ready(Err(HarnessRuntimeError::Closed(HarnessClosed)));
            }
            future.as_mut().poll(cx)
        })
        .await
    }
    pub(crate) async fn project_structural_rows(
        &self,
        rows: &[Entry],
        prospective: Option<EntryId>,
    ) -> Result<summary_content::DetachedBranch, HarnessRuntimeError> {
        let mut custom = BTreeMap::new();
        let mut summaries = BTreeMap::new();
        for row in rows {
            match row {
                Entry::Custom(e) => {
                    if let Some(projector) = self.lane.owner.projectors.get(&e.custom_type) {
                        let messages = self.structural_wait(projector.project(e.clone())).await?;
                        if let Some(messages) = messages {
                            for m in &messages {
                                self.lane
                                    .owner
                                    .session
                                    .codec()
                                    .validate_message(m)
                                    .map_err(|_| {
                                        self.lane.owner.fail("invalid structural projection")
                                    })?;
                            }
                            custom.insert(e.id.clone(), messages);
                        }
                    }
                }
                Entry::Compaction(e) => {
                    let m = self
                        .structural_wait(self.lane.owner.summaries.render(ContextSummary {
                            kind: SummaryKind::Compaction,
                            source_id: e.id.clone(),
                            timestamp: e.timestamp,
                            text: e.summary.clone(),
                        }))
                        .await?;
                    self.lane
                        .owner
                        .session
                        .codec()
                        .validate_message(&m)
                        .map_err(|_| self.lane.owner.fail("invalid structural rendering"))?;
                    summaries.insert(e.id.clone(), m);
                }
                Entry::BranchSummary(e) => {
                    let m = self
                        .structural_wait(self.lane.owner.summaries.render(ContextSummary {
                            kind: SummaryKind::BranchSummary,
                            source_id: e.id.clone(),
                            timestamp: e.timestamp,
                            text: e.summary.clone(),
                        }))
                        .await?;
                    self.lane
                        .owner
                        .session
                        .codec()
                        .validate_message(&m)
                        .map_err(|_| self.lane.owner.fail("invalid structural rendering"))?;
                    summaries.insert(e.id.clone(), m);
                }
                _ => {}
            }
        }
        summary_content::project_branch(rows, &custom, &summaries, prospective)
            .map_err(|_| self.lane.owner.fail("structural projection failed"))
    }
    pub(crate) async fn compaction_preparation(
        &self,
        leaf: Option<EntryId>,
        settings: CompactionSettings,
        prospective: Option<EntryId>,
    ) -> Result<Option<CompactionPreparation>, HarnessRuntimeError> {
        let rows = self
            .lane
            .structural_rows(leaf, Some(EntryType::Compaction))
            .await?;
        let projected = self.project_structural_rows(&rows, prospective).await?;
        summary_content::prepare_compaction(&projected, settings)
            .map_err(|_| self.lane.owner.fail("compaction preparation failed"))
    }
    pub(crate) async fn navigation_preparation(
        &self,
        source: EntryId,
        target: EntryId,
        window: SafeUnsigned,
        reserve: SafeUnsigned,
    ) -> Result<BranchPreparation, HarnessRuntimeError> {
        let target_path = self.lane.structural_rows(Some(target), None).await?;
        let targets: std::collections::BTreeSet<_> =
            target_path.iter().map(entry_id).cloned().collect();
        let mut source_path = self
            .lane
            .structural_rows(Some(source), Some(EntryType::Compaction))
            .await?;
        if let Some(index) = source_path
            .iter()
            .rposition(|e| targets.contains(entry_id(e)))
        {
            source_path.drain(..=index);
        }
        let projected = self.project_structural_rows(&source_path, None).await?;
        summary_content::prepare_branch(
            &projected.positions,
            summary_content::branch_budget(window, reserve),
        )
        .map_err(|_| self.lane.owner.fail("navigation preparation failed"))
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) async fn structural_rows(
        &self,
        start: Option<EntryId>,
        stop: Option<EntryType>,
    ) -> Result<Vec<Entry>, HarnessRuntimeError> {
        let Some(start) = start else {
            return Ok(vec![]);
        };
        self.owner
            .session
            .lane_view(self.lane.name.clone())
            .find_entries_on_branch(BranchScan {
                start: Some(start),
                order: Some(BranchOrder::OldestFirst),
                stop_at_type: stop,
                ..Default::default()
            })
            .await
            .map_err(|_| self.owner.fail("structural branch read failed"))
    }
}
pub(crate) fn entry_id(e: &Entry) -> &EntryId {
    match e {
        Entry::Message(e) => &e.id,
        Entry::Compaction(e) => &e.id,
        Entry::BranchSummary(e) => &e.id,
        Entry::Custom(e) => &e.id,
    }
}
pub(crate) fn durable_compaction(p: CompactionPreparation) -> DurableStructuralPreparation {
    DurableStructuralPreparation::Compaction {
        messages_to_summarize: p.messages_to_summarize,
        turn_prefix_messages: p.turn_prefix_messages,
        retained_tail: p.retained_tail,
        is_split_turn: p.is_split_turn,
        tokens_before: p.tokens_before,
        previous_summary: p.previous_summary,
        file_ops: p.file_ops,
        settings: p.settings,
    }
}
pub(crate) fn durable_branch(p: BranchPreparation) -> DurableStructuralPreparation {
    DurableStructuralPreparation::BranchSummary {
        messages: p.messages,
        file_ops: p.file_ops,
        total_tokens: p.total_tokens,
    }
}
