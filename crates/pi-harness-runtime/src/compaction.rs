//! Runtime materialization of threshold and prospective overflow facts.
use crate::{effects::RuntimeEffects, owner::Telemetry, restore::HydratedLane, summary_content};
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use pi_harness_core::{
    compaction::{Overflow, PreparedCompaction, Threshold},
    planner::{PlanningFacts, SettlementFacts},
};

pub(crate) struct ThresholdCandidate {
    pub observed: HydratedLane,
    pub facts: PlanningFacts<Telemetry>,
    pub stamp: pi_agent_session::storage::ReadStamp,
    pub revision: pi_ai_core::SafeUnsigned,
}
pub(crate) struct OverflowCandidate {
    pub observed: HydratedLane,
    pub facts: SettlementFacts,
    pub stamp: pi_agent_session::storage::ReadStamp,
    pub revision: pi_ai_core::SafeUnsigned,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) async fn threshold_facts(
        &self,
        h: &HydratedLane,
    ) -> Result<Option<PlanningFacts<Telemetry>>, HarnessRuntimeError> {
        let Some(current) = &h.current else {
            return Ok(None);
        };
        let OperationState::Run(run) = &current.state else {
            return Ok(None);
        };
        let RunPhase::Checkpoint(p) = &run.phase else {
            return Ok(None);
        };
        if !matches!(run.control, Control::Running(_))
            || p.threshold_checked_trigger_entry_id.as_ref() == Some(&p.trigger_entry_id)
            || !run.settings.compaction.enabled
            || p.skip_inbox_once != Some(true)
                && (!run.inbox.writes.is_empty() || !run.inbox.steer.is_empty())
        {
            return Ok(None);
        }
        let stamp = self
            .lane
            .owner
            .session
            .read_stamp()
            .await
            .map_err(|_| self.lane.owner.fail("threshold stamp failed"))?;
        let observed = self.lane.load().await?;
        if observed.current != h.current
            || observed.roots.configuration != h.roots.configuration
            || observed.roots.leaf != h.roots.leaf
        {
            return Ok(None);
        }
        let revision = self
            .lane
            .owner
            .settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .revision;
        let Some(model) = self.lane.owner.models.get_model(
            &current.configuration.model.provider,
            &current.configuration.model.model_id,
        ) else {
            return Ok(None);
        };
        let rows = self
            .lane
            .structural_rows(current.leaf_id.clone(), Some(EntryType::Compaction))
            .await?;
        let branch = self.project_structural_rows(&rows, None).await?;
        let messages: Vec<_> = branch
            .positions
            .iter()
            .filter_map(|p| p.message.clone())
            .collect();
        let tokens = summary_content::estimate_context(&messages)
            .map_err(|_| self.lane.owner.fail("threshold estimate failed"))?
            .tokens;
        let prepared = if pi_harness_core::compaction::threshold_due(
            tokens,
            model.context_window,
            &run.settings.compaction,
        ) {
            summary_content::prepare_compaction(&branch, run.settings.compaction.clone())
                .map_err(|_| self.lane.owner.fail("threshold preparation failed"))?
                .map(|p| {
                    Ok(PreparedCompaction {
                        task_id: TaskId::new(self.lane.owner.ids.next(None))
                            .map_err(|_| self.lane.owner.fail("threshold task invalid"))?,
                        preparation: crate::preparation::durable_compaction(p),
                    })
                })
                .transpose()?
        } else {
            None
        };
        let facts = PlanningFacts::Threshold(Threshold {
            telemetry_context: self.trace.context(),
            context_tokens: tokens,
            context_window: model.context_window,
            prepared,
        });
        *self
            .threshold_candidate
            .lock()
            .unwrap_or_else(|p| p.into_inner()) = Some(ThresholdCandidate {
            observed: h.clone(),
            facts: facts.clone(),
            stamp,
            revision,
        });
        Ok(Some(facts))
    }
    pub(crate) async fn overflow_facts(
        &self,
        commit: &SettlementCommit<Telemetry>,
    ) -> Result<Option<OverflowCandidate>, HarnessRuntimeError> {
        let EffectPlan::Assistant(plan) = &commit.plan else {
            return Ok(None);
        };
        let SettlementOutput::Assistant { message, .. } = &commit.output else {
            return Ok(None);
        };
        let raw = message.as_message();
        let matching_error = raw.stop_reason == pi_ai_core::StopReason::Error
            && pi_ai_core::is_context_overflow(raw, None);
        if !pi_harness_core::compaction::is_overflow(
            raw,
            plan.generation.intended_output_limit,
            matching_error,
        ) {
            return Ok(None);
        }
        let stamp = self
            .lane
            .owner
            .session
            .read_stamp()
            .await
            .map_err(|_| self.lane.owner.fail("overflow stamp failed"))?;
        let revision = self
            .lane
            .owner
            .settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .revision;
        let h = {
            let _line = self.lane.lane.line.acquire().await?;
            self.lane.load().await?
        };
        let Some(current) = h
            .current
            .clone()
            .filter(|c| c.operation.operation_id == plan.key.operation_id)
        else {
            return Ok(None);
        };
        let OperationState::Run(run) = &current.state else {
            return Ok(None);
        };
        let prepared = if matches!(run.control, Control::Running(_))
            && !plan.generation.context.overflow_recovery_used
        {
            self.compaction_preparation(
                current.leaf_id.clone(),
                run.settings.compaction.clone(),
                Some(plan.generation.response_entry_id.clone()),
            )
            .await?
            .map(|p| {
                Ok(PreparedCompaction {
                    task_id: TaskId::new(self.lane.owner.ids.next(None))
                        .map_err(|_| self.lane.owner.fail("overflow task invalid"))?,
                    preparation: crate::preparation::durable_compaction(p),
                })
            })
            .transpose()?
        } else {
            None
        };
        Ok(Some(OverflowCandidate {
            observed: h,
            facts: SettlementFacts::Overflow(Overflow {
                retryable: pi_ai_core::is_retryable_assistant_error(raw),
                matching_error,
                prepared,
                followers: vec![],
            }),
            stamp,
            revision,
        }))
    }
}
