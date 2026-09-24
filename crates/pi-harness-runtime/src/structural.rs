//! Public structural acceptance and process-local terminal candidates.
use crate::{driver::DriveGuard, effects::RuntimeEffects, owner::*, preparation};
use pi_agent_schema::*;
use pi_agent_session::{
    backend_support::register_seq,
    storage::{PreparedEntries, RuntimeStorage},
};
use pi_ai_core::SafeUnsigned;
use std::sync::Arc;

pub(crate) struct TerminalCandidate {
    pub source: SettlementCommit<Telemetry>,
    pub result_id: Option<EntryId>,
    pub hook_usage_id: Option<UsageId>,
    pub prepared: Option<PreparedEntries>,
    pub extra: pi_harness_core::structural::Settlement,
    pub current: CurrentOperation,
    pub result: OperationResult,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) fn reserved_kind(&self) -> OperationKind {
        let drive = self
            .lane
            .active
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .and_then(|(_, drive)| drive.upgrade());
        drive.map_or(OperationKind::Run, |d| d.kind())
    }
    pub(crate) async fn compact_call(&self, options: CompactOptions) -> CompactionCall {
        if let Err(e) = self.owner.admission.check() {
            return match e {
                HarnessRuntimeError::Closed(_) => {
                    Ok(Err(CompactionExpectedError::Closed(crate::inbox::closed())))
                }
                e => Err(e),
            };
        }
        let lane = self.clone();
        let result = self
            .owner
            .clone()
            .retain_runtime(Box::pin(async move {
                lane.accept_structure(OperationIntent::Compaction(CompactionIntent {
                    kind: Default::default(),
                    custom_instructions: options.custom_instructions,
                }))
                .await
            }))
            .await?;
        match result {
            Ok((op, OperationResult::Compaction(outcome))) => Ok(Ok(CompactionSuccess {
                run_id: op,
                outcome,
            })),
            Ok(_) => Err(self.owner.fail("compact result kind mismatch")),
            Err(rejection) => Ok(Err(match rejection {
                StructuralRejection::Busy(e) => CompactionExpectedError::LaneBusy(e),
                StructuralRejection::Missing(e) => CompactionExpectedError::MissingIdentities(e),
                StructuralRejection::Empty => {
                    CompactionExpectedError::NothingToCompact(NothingToCompactError {
                        tag: Default::default(),
                        message: "Nothing to compact".into(),
                        lane: self.lane.name.clone(),
                    })
                }
                _ => return Err(self.owner.fail("compact rejection kind mismatch")),
            })),
        }
    }
    pub(crate) async fn navigation_call(
        &self,
        target: Option<EntryId>,
        options: NavigateOptions,
    ) -> NavigationCall {
        if let Err(e) = self.owner.admission.check() {
            return match e {
                HarnessRuntimeError::Closed(_) => {
                    Ok(Err(NavigationExpectedError::Closed(crate::inbox::closed())))
                }
                e => Err(e),
            };
        }
        let lane = self.clone();
        let result = self
            .owner
            .clone()
            .retain_runtime(Box::pin(async move {
                lane.accept_structure(OperationIntent::Navigation(NavigationIntent {
                    kind: Default::default(),
                    target_id: target,
                    summarize: options.summarize.unwrap_or(false),
                    label: options.label,
                    custom_instructions: options.custom_instructions,
                }))
                .await
            }))
            .await?;
        match result {
            Ok((op, OperationResult::Navigation(outcome))) => Ok(Ok(NavigationSuccess {
                run_id: op,
                outcome,
            })),
            Ok(_) => Err(self.owner.fail("navigation result kind mismatch")),
            Err(e) => Ok(Err(match e {
                StructuralRejection::Busy(e) => NavigationExpectedError::LaneBusy(e),
                StructuralRejection::Missing(e) => NavigationExpectedError::MissingIdentities(e),
                StructuralRejection::Invalid => {
                    NavigationExpectedError::InvalidNavigation(InvalidNavigationError {
                        tag: Default::default(),
                        message: "Invalid navigation".into(),
                        lane: self.lane.name.clone(),
                        reason: "invalid target or source".into(),
                    })
                }
                StructuralRejection::Unknown(id) => {
                    NavigationExpectedError::UnknownTarget(UnknownTargetError {
                        tag: Default::default(),
                        message: "Unknown target".into(),
                        target_id: id,
                    })
                }
                _ => return Err(self.owner.fail("navigation rejection kind mismatch")),
            })),
        }
    }
    async fn accept_structure(
        &self,
        intent: OperationIntent,
    ) -> Result<Result<(OperationId, OperationResult), StructuralRejection>, HarnessRuntimeError>
    {
        if let OperationIntent::Navigation(n) = &intent
            && !n.summarize
        {
            return self.accept_unsummarized_navigation(n.clone()).await;
        }
        let kind = match intent {
            OperationIntent::Compaction(_) => OperationKind::Compaction,
            OperationIntent::Navigation(_) => OperationKind::Navigation,
            _ => return Err(self.owner.fail("structural intent mismatch")),
        };
        let id = OperationId::new(self.owner.ids.next(None))
            .map_err(|_| self.owner.fail("structural identity invalid"))?;
        let fx = RuntimeEffects::new(self.clone(), false);
        *fx.operation_kind.lock().unwrap_or_else(|p| p.into_inner()) = kind;
        let control: Arc<dyn crate::abort::ActiveDrive> = fx.clone();
        {
            let _line = self.lane.line.acquire().await?;
            let h = self.load().await?;
            if let Some(c) = &h.current {
                return Ok(Err(StructuralRejection::Busy(LaneBusyError {
                    tag: Default::default(),
                    message: "lane has an operation".into(),
                    lane: self.lane.name.clone(),
                    operation_id: c.operation.operation_id.clone(),
                    operation_kind: crate::snapshots::kind(c),
                })));
            }
            if !self
                .lane
                .reserve_drive(id.clone(), Arc::downgrade(&control))
            {
                let provisional = self
                    .lane
                    .reservation
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .clone()
                    .ok_or_else(|| self.owner.fail("reservation missing"))?;
                return Ok(Err(StructuralRejection::Busy(LaneBusyError {
                    tag: Default::default(),
                    message: "lane admission reserved".into(),
                    lane: self.lane.name.clone(),
                    operation_id: provisional,
                    operation_kind: self.reserved_kind(),
                })));
            }
        }
        let _guard = DriveGuard::new(self.lane.clone());
        let task = TaskId::new(self.owner.ids.next(None))
            .map_err(|_| self.owner.fail("structural task invalid"))?;
        loop {
            let (observed, settings) = {
                let _settings = self.owner.settings_line.acquire().await?;
                let _line = self.lane.line.acquire().await?;
                (
                    self.load().await?,
                    self.owner
                        .settings
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .clone(),
                )
            };
            let source = match &observed.roots.leaf {
                Register::LaneLeaf { value, .. } => value.clone(),
                _ => return Err(self.owner.fail("structural source invalid")),
            };
            let configuration = match &observed.roots.configuration {
                Register::LaneConfig { value, .. } => value.clone(),
                _ => return Err(self.owner.fail("structural config invalid")),
            };
            let missing = self.missing(&configuration);
            if !missing.models.is_empty() || !missing.tools.is_empty() {
                let line = self.lane.line.acquire().await?;
                let latest = self.load().await?;
                if latest.roots.configuration != observed.roots.configuration
                    || latest.roots.leaf != observed.roots.leaf
                {
                    drop(line);
                    continue;
                }
                fx.release_drive();
                drop(line);
                self.lane.announce();
                return Ok(Err(StructuralRejection::Missing(MissingIdentitiesError {
                    tag: Default::default(),
                    message: "configured identities are missing".into(),
                    lane: self.lane.name.clone(),
                    tools: missing.tools,
                    models: missing.models,
                })));
            }
            let target = if let OperationIntent::Navigation(n) = &intent {
                if let Some(target) = &n.target_id {
                    self.owner
                        .session
                        .get_entries(std::slice::from_ref(target))
                        .await
                        .map_err(|_| self.owner.fail("navigation target read failed"))?
                        .get(target)
                        .cloned()
                } else {
                    None
                }
            } else {
                None
            };
            if let OperationIntent::Navigation(n) = &intent
                && let Err(e) = pi_harness_core::navigation::validate(&source, n, target.as_ref())
            {
                let line = self.lane.line.acquire().await?;
                let latest = self.load().await?;
                if latest.roots.leaf != observed.roots.leaf {
                    drop(line);
                    continue;
                }
                fx.release_drive();
                drop(line);
                self.lane.announce();
                return Ok(Err(match e {
                    pi_harness_core::navigation::AdmissionError::InvalidNavigation => {
                        StructuralRejection::Invalid
                    }
                    pi_harness_core::navigation::AdmissionError::UnknownTarget => {
                        StructuralRejection::Unknown(
                            n.target_id
                                .clone()
                                .ok_or_else(|| self.owner.fail("unknown root target"))?,
                        )
                    }
                }));
            }
            let prepared = match &intent {
                OperationIntent::Compaction(_) => fx
                    .compaction_preparation(source.clone(), settings.run.compaction.clone(), None)
                    .await?
                    .map(preparation::durable_compaction),
                OperationIntent::Navigation(n) if n.summarize => {
                    let Some(model) = self
                        .owner
                        .models
                        .get_model(&configuration.model.provider, &configuration.model.model_id)
                    else {
                        continue;
                    };
                    Some(preparation::durable_branch(
                        fx.navigation_preparation(
                            source
                                .clone()
                                .ok_or_else(|| self.owner.fail("navigation root source"))?,
                            n.target_id
                                .clone()
                                .ok_or_else(|| self.owner.fail("navigation root target"))?,
                            model.context_window,
                            settings.run.compaction.reserve_tokens,
                        )
                        .await?,
                    ))
                }
                _ => None,
            };
            let settings_line = self.owner.settings_line.acquire().await?;
            let line = self.lane.line.acquire().await?;
            let latest = self.load().await?;
            if latest.roots.leaf != observed.roots.leaf
                || latest.roots.configuration != observed.roots.configuration
                || self
                    .owner
                    .settings
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .revision
                    != settings.revision
            {
                drop(line);
                drop(settings_line);
                continue;
            }
            if matches!(intent, OperationIntent::Compaction(_)) && prepared.is_none() {
                fx.release_drive();
                drop(line);
                drop(settings_line);
                self.lane.announce();
                return Ok(Err(StructuralRejection::Empty));
            }
            let Register::LaneState {
                value: lane_state,
                seq: lane_seq,
                ..
            } = latest.roots.lane_state
            else {
                return Err(self.owner.fail("structural lane state invalid"));
            };
            let operation = Operation {
                operation_id: id.clone(),
                lane: self.lane.name.clone(),
                source_leaf_id: source.clone(),
                started_at: SafeUnsigned::new(self.owner.clock.now_unix_ms())
                    .map_err(|_| self.owner.fail("structural clock range"))?,
                intent: intent.clone(),
            };
            let facts = pi_harness_core::structural::Acceptance {
                operation: operation.clone(),
                lane_state,
                lane_seq,
                leaf_seq: Some(register_seq(&latest.roots.leaf)),
                source_leaf: source,
                configuration_seq: register_seq(&latest.roots.configuration),
                settings_revision: settings.revision,
                identities_present: true,
                task_id: task.clone(),
                preparation: prepared,
                target,
                telemetry_context: self.owner.telemetry.clone(),
            };
            let recipe = pi_harness_core::structural::acceptance_recipe(&facts)
                .map_err(|_| self.owner.fail("structural acceptance rejected"))?
                .ok_or_else(|| self.owner.fail("structural acceptance unexpectedly empty"))?;
            let mut publication = crate::publication::HeldPublication::new(settings_line, line);
            if self
                .apply_prepared_scoped(recipe, None, publication.commit_slot(), false)
                .await?
                .is_none()
            {
                continue;
            }
            *fx.original_operation
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(operation);
            publication.release();
            self.owner.events.flush();
            self.lane.announce();
            fx.trace.begin_kind(
                &self.owner.session.metadata().base_metadata().id,
                &self.lane.name,
                &id,
                false,
                kind,
            );
            let result = crate::driver::drive(fx.clone(), id.clone()).await?;
            fx.trace.finish(&result);
            return Ok(Ok((id, result)));
        }
    }
    async fn accept_unsummarized_navigation(
        &self,
        intent: NavigationIntent,
    ) -> Result<Result<(OperationId, OperationResult), StructuralRejection>, HarnessRuntimeError>
    {
        let id = OperationId::new(self.owner.ids.next(None))
            .map_err(|_| self.owner.fail("navigation identity invalid"))?;
        let task = TaskId::new(self.owner.ids.next(None))
            .map_err(|_| self.owner.fail("navigation task invalid"))?;
        let fx = RuntimeEffects::new(self.clone(), false);
        *fx.operation_kind.lock().unwrap_or_else(|p| p.into_inner()) = OperationKind::Navigation;
        let control: Arc<dyn crate::abort::ActiveDrive> = fx.clone();
        loop {
            // Model resolution may invoke a provider callback; capture it before
            // entering the line that protects target validation and acceptance.
            let detached = {
                let _line = self.lane.line.acquire().await?;
                let detached = self.load().await?;
                if let Some(busy) = self.navigation_busy(&detached) {
                    return Ok(Err(busy));
                }
                detached
            };
            let configuration = match &detached.roots.configuration {
                Register::LaneConfig { value, .. } => value.clone(),
                _ => return Err(self.owner.fail("navigation config invalid")),
            };
            let missing = self.missing(&configuration);
            let settings_line = self.owner.settings_line.acquire().await?;
            let line = self.lane.line.acquire().await?;
            let observed = self.load().await?;
            if observed.roots.configuration != detached.roots.configuration {
                continue;
            }
            if let Some(busy) = self.navigation_busy(&observed) {
                return Ok(Err(busy));
            }
            let source = match &observed.roots.leaf {
                Register::LaneLeaf { value, .. } => value.clone(),
                _ => return Err(self.owner.fail("navigation source invalid")),
            };
            if !missing.models.is_empty() || !missing.tools.is_empty() {
                let latest = self.load().await?;
                if latest.roots.configuration != observed.roots.configuration
                    || latest.roots.leaf != observed.roots.leaf
                    || latest.current != observed.current
                {
                    continue;
                }
                return Ok(Err(StructuralRejection::Missing(MissingIdentitiesError {
                    tag: Default::default(),
                    message: "configured identities are missing".into(),
                    lane: self.lane.name.clone(),
                    tools: missing.tools,
                    models: missing.models,
                })));
            }
            // The target read and validation remain on this same lane line. No
            // process-local drive exists until a valid acceptance can publish.
            let target = if let Some(target) = &intent.target_id {
                self.owner
                    .session
                    .get_entries(std::slice::from_ref(target))
                    .await
                    .map_err(|_| self.owner.fail("navigation target read failed"))?
                    .get(target)
                    .cloned()
            } else {
                None
            };
            if let Err(e) = pi_harness_core::navigation::validate(&source, &intent, target.as_ref())
            {
                let latest = self.load().await?;
                if latest.roots.leaf != observed.roots.leaf
                    || latest.roots.configuration != observed.roots.configuration
                    || latest.current != observed.current
                {
                    continue;
                }
                return Ok(Err(match e {
                    pi_harness_core::navigation::AdmissionError::InvalidNavigation => {
                        StructuralRejection::Invalid
                    }
                    pi_harness_core::navigation::AdmissionError::UnknownTarget => {
                        StructuralRejection::Unknown(
                            intent
                                .target_id
                                .clone()
                                .ok_or_else(|| self.owner.fail("unknown root target"))?,
                        )
                    }
                }));
            }
            let latest = self.load().await?;
            let settings = self
                .owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            if latest.roots.leaf != observed.roots.leaf
                || latest.roots.configuration != observed.roots.configuration
                || latest.current != observed.current
            {
                continue;
            }
            let Register::LaneState {
                value: lane_state,
                seq: lane_seq,
                ..
            } = latest.roots.lane_state
            else {
                return Err(self.owner.fail("navigation lane state invalid"));
            };
            let operation = Operation {
                operation_id: id.clone(),
                lane: self.lane.name.clone(),
                source_leaf_id: source.clone(),
                started_at: SafeUnsigned::new(self.owner.clock.now_unix_ms())
                    .map_err(|_| self.owner.fail("navigation clock range"))?,
                intent: OperationIntent::Navigation(intent.clone()),
            };
            let facts = pi_harness_core::structural::Acceptance {
                operation: operation.clone(),
                lane_state,
                lane_seq,
                leaf_seq: Some(register_seq(&latest.roots.leaf)),
                source_leaf: source,
                configuration_seq: register_seq(&latest.roots.configuration),
                settings_revision: settings.revision,
                identities_present: true,
                task_id: task.clone(),
                preparation: None,
                target,
                telemetry_context: self.owner.telemetry.clone(),
            };
            let recipe = pi_harness_core::structural::acceptance_recipe(&facts)
                .map_err(|_| self.owner.fail("navigation acceptance rejected"))?
                .ok_or_else(|| self.owner.fail("navigation acceptance unexpectedly empty"))?;
            let mut publication = crate::publication::HeldPublication::new(settings_line, line);
            if self
                .apply_prepared_scoped(recipe, None, publication.commit_slot(), false)
                .await?
                .is_none()
            {
                continue;
            }
            // Acceptance and drive registration share this lane line. No
            // provisional owner exists for a rejected or refused navigation.
            if !self
                .lane
                .reserve_drive(id.clone(), Arc::downgrade(&control))
            {
                return Err(self.owner.fail("accepted navigation lost drive control"));
            }
            let guard = DriveGuard::new(self.lane.clone());
            *fx.original_operation
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(operation);
            publication.release();
            self.owner.events.flush();
            self.lane.announce();
            fx.trace.begin_kind(
                &self.owner.session.metadata().base_metadata().id,
                &self.lane.name,
                &id,
                false,
                OperationKind::Navigation,
            );
            let result = crate::driver::drive(fx.clone(), id.clone()).await?;
            fx.trace.finish(&result);
            drop(guard);
            return Ok(Ok((id, result)));
        }
    }
    fn navigation_busy(
        &self,
        observed: &crate::restore::HydratedLane,
    ) -> Option<StructuralRejection> {
        if let Some(c) = &observed.current {
            return Some(StructuralRejection::Busy(LaneBusyError {
                tag: Default::default(),
                message: "lane has an operation".into(),
                lane: self.lane.name.clone(),
                operation_id: c.operation.operation_id.clone(),
                operation_kind: crate::snapshots::kind(c),
            }));
        }
        let provisional = self
            .lane
            .reservation
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        provisional.map(|provisional| {
            StructuralRejection::Busy(LaneBusyError {
                tag: Default::default(),
                message: "lane admission reserved".into(),
                lane: self.lane.name.clone(),
                operation_id: provisional,
                operation_kind: self.reserved_kind(),
            })
        })
    }
}
enum StructuralRejection {
    Busy(LaneBusyError),
    Missing(MissingIdentitiesError),
    Empty,
    Invalid,
    Unknown(EntryId),
}

fn semantic(output: &SettlementOutput) -> Option<(SummarySuccessResult, bool)> {
    match output {
        SettlementOutput::Summary {
            outcome: SummaryAttemptOutcome::Success(s),
            ..
        } => Some((s.clone(), false)),
        SettlementOutput::Hook(o) => match o.result() {
            HookResult::BeforeCompaction(Some(r)) => match r.as_ref() {
                BeforeCompactionResult::Compaction(s) => {
                    Some((SummarySuccessResult::Compact(s.compaction.clone()), true))
                }
                _ => None,
            },
            HookResult::BeforeNavigation(Some(BeforeNavigationResult::Summary(s))) => {
                Some((SummarySuccessResult::BranchSummary(s.summary.clone()), true))
            }
            _ => None,
        },
        _ => None,
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) async fn structural_settlement(
        &self,
        plan: SettlementCommit<Telemetry>,
    ) -> Result<Option<SettlementResult<Telemetry>>, HarnessRuntimeError> {
        use pi_harness_core::planner::{RecipeDecision, SettlementFacts};
        self.permission("commit_settlement").await?;
        let old = self
            .terminal_candidate
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        let mut result_id = old.as_ref().and_then(|c| c.result_id.clone());
        let mut hook_usage_id = old.as_ref().and_then(|c| c.hook_usage_id.clone());
        drop(old);
        loop {
            let settings = self.lane.owner.settings_line.acquire().await?;
            let line = self.lane.lane.line.acquire().await?;
            let mut publication = crate::publication::HeldPublication::new(settings, line);
            let before = self
                .lane
                .owner
                .session
                .read_stamp()
                .await
                .map_err(|_| self.lane.owner.fail("structural settlement stamp failed"))?;
            let h = self.lane.load().await?;
            let after = self
                .lane
                .owner
                .session
                .read_stamp()
                .await
                .map_err(|_| self.lane.owner.fail("structural settlement stamp failed"))?;
            if before != after {
                continue;
            }
            let Some(current) = h
                .current
                .as_ref()
                .filter(|c| c.operation == plan.current.operation)
            else {
                self.retire_effect(plan.plan.key());
                return Ok(None);
            };
            let inputs = self.inputs(&h).await?;
            let mut extra = pi_harness_core::structural::Settlement::default();
            let mut prepared = None;
            if !pi_harness_core::structural::is_cancelled(current) {
                if let Some(p) = pi_harness_core::structural::pending(current) {
                    result_id = Some(p.context.result_entry_id)
                }
                if result_id.is_none() {
                    result_id =
                        Some(EntryId::new(self.lane.owner.ids.next(None)).map_err(|_| {
                            self.lane.owner.fail("structural result identity invalid")
                        })?)
                }
                extra.result_entry_id = result_id.clone();
                if let Some((value, from_hook)) = semantic(&plan.output) {
                    let used = match &value {
                        SummarySuccessResult::Compact(s) => s.usage.is_some(),
                        SummarySuccessResult::BranchSummary(s) => s.usage.is_some(),
                    };
                    if from_hook && used && hook_usage_id.is_none() {
                        hook_usage_id =
                            Some(UsageId::new(self.lane.owner.ids.next(None)).map_err(|_| {
                                self.lane
                                    .owner
                                    .fail("structural hook usage identity invalid")
                            })?)
                    }
                    extra.hook_usage_id = hook_usage_id.clone();
                    let id = result_id.clone().expect("structural result identity");
                    let entry = match value {
                        SummarySuccessResult::Compact(s) => {
                            NewEntry::Compaction(NewCompactionEntry {
                                id,
                                parent_id: current.leaf_id.clone(),
                                entry_type: Default::default(),
                                summary: s.summary,
                                retained_tail: s.retained_tail,
                                tokens_before: s.tokens_before,
                                details: s.details,
                                usage: s.usage,
                                from_hook,
                            })
                        }
                        SummarySuccessResult::BranchSummary(s) => {
                            let OperationState::Navigation(NavigationState::Summarized(n)) =
                                &current.state
                            else {
                                return Err(self
                                    .lane
                                    .owner
                                    .fail("branch publication state mismatch"));
                            };
                            let details =
                                pi_ai_core::JsonObject::new(std::collections::BTreeMap::from([
                                    (
                                        "readFiles".into(),
                                        pi_ai_core::JsonValue::Array(
                                            s.read_files
                                                .into_iter()
                                                .map(pi_ai_core::JsonValue::String)
                                                .collect(),
                                        ),
                                    ),
                                    (
                                        "modifiedFiles".into(),
                                        pi_ai_core::JsonValue::Array(
                                            s.modified_files
                                                .into_iter()
                                                .map(pi_ai_core::JsonValue::String)
                                                .collect(),
                                        ),
                                    ),
                                ]));
                            NewEntry::BranchSummary(NewBranchSummaryEntry {
                                id,
                                parent_id: Some(n.target_id.clone()),
                                entry_type: Default::default(),
                                from_id: current.operation.source_leaf_id.clone().ok_or_else(
                                    || self.lane.owner.fail("branch publication source absent"),
                                )?,
                                summary: s.summary,
                                details: pi_ai_core::Presence::Value(
                                    pi_ai_core::JsonValue::Object(details),
                                ),
                                usage: s.usage,
                                from_hook,
                            })
                        }
                    };
                    let write_index = usize::from(hook_usage_id.is_some())
                        + usize::from(matches!(entry, NewEntry::BranchSummary(_)));
                    prepared = self
                        .lane
                        .owner
                        .session
                        .prepare_entries(
                            before,
                            vec![pi_agent_session::storage::PreparedEntrySlot {
                                write_index,
                                entry,
                            }],
                        )
                        .await
                        .map_err(|_| {
                            self.lane.owner.fail("structural result preparation failed")
                        })?;
                    let Some(token) = prepared.as_ref() else {
                        continue;
                    };
                    extra.prepared_entry =
                        Some(token.entries().first().cloned().ok_or_else(|| {
                            self.lane.owner.fail("prepared structural entry absent")
                        })?);
                }
            }
            let decision = pi_harness_core::planner::settlement_recipe(
                current,
                &plan,
                &inputs,
                &self.lane.facts(&h)?,
                &SettlementFacts::Structural(extra.clone()),
            )
            .map_err(|_| self.lane.owner.fail("structural settlement rejected"))?;
            let result = match decision {
                RecipeDecision::Apply(recipe) => {
                    let dispatch = recipe.dispatch.clone();
                    let Some(applied) = self
                        .lane
                        .apply_prepared_scoped(*recipe, prepared, publication.commit_slot(), false)
                        .await?
                    else {
                        continue;
                    };
                    self.summary_settled(current, applied.current.as_ref(), &plan.output);
                    self.retire_effect(plan.plan.key());
                    if matches!(current.state, OperationState::Run(_))
                        && applied
                            .current
                            .as_ref()
                            .is_some_and(|c| pi_harness_core::structural::decision(c).is_none())
                    {
                        let outcome = if pi_harness_core::structural::is_cancelled(current) {
                            OperationResult::Compaction(CompactionOutcome::Aborted(
                                CompactionAborted {
                                    kind: Default::default(),
                                    leaf_id: current.leaf_id.clone().ok_or_else(|| {
                                        self.lane.owner.fail("compaction cancellation leaf missing")
                                    })?,
                                },
                            ))
                        } else if let Some(entry) = extra.prepared_entry.as_ref() {
                            let (value, from_hook) = semantic(&plan.output).ok_or_else(|| {
                                self.lane.owner.fail("compaction semantic missing")
                            })?;
                            pi_harness_core::structural::completed_result(
                                current, &value, entry, from_hook,
                            )
                            .map_err(|_| {
                                self.lane.owner.fail("compaction public result rejected")
                            })?
                        } else if let Some(CurrentOperation {
                            state: OperationState::Run(run),
                            ..
                        }) = &applied.current
                            && let RunPhase::FailureDrain(f) = &run.phase
                        {
                            OperationResult::Compaction(CompactionOutcome::Failed(
                                CompactionFailed {
                                    kind: Default::default(),
                                    leaf_id: current.leaf_id.clone().ok_or_else(|| {
                                        self.lane.owner.fail("compaction failure leaf missing")
                                    })?,
                                    error: f.error.clone(),
                                },
                            ))
                        } else {
                            OperationResult::Compaction(CompactionOutcome::Declined(
                                CompactionDeclined {
                                    kind: Default::default(),
                                    leaf_id: current.leaf_id.clone().ok_or_else(|| {
                                        self.lane.owner.fail("compaction decline leaf missing")
                                    })?,
                                },
                            ))
                        };
                        let (kind, event) =
                            end_event(&current.operation, reason(current), outcome, self.recovery)?;
                        self.events().enqueue(kind, &self.lane.lane.name, event);
                    }
                    applied.current.map(|current| SettlementResult {
                        current,
                        terminal: None,
                        dispatch,
                        suspend: None,
                        consume_deferred_poll: false,
                    })
                }
                RecipeDecision::Terminal(terminal) => {
                    let result = terminal.result.clone();
                    *self
                        .terminal_candidate
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = Some(TerminalCandidate {
                        source: plan.clone(),
                        result_id,
                        hook_usage_id,
                        prepared,
                        extra,
                        current: current.clone(),
                        result: result.clone(),
                    });
                    Some(SettlementResult {
                        current: current.clone(),
                        terminal: Some(result),
                        dispatch: None,
                        suspend: None,
                        consume_deferred_poll: false,
                    })
                }
                RecipeDecision::Suspend(result) => {
                    self.publish_suspension(current, &result);
                    self.this
                        .upgrade()
                        .expect("live structural owner")
                        .release_drive();
                    self.retire_effect(plan.plan.key());
                    Some(SettlementResult {
                        current: current.clone(),
                        terminal: None,
                        dispatch: None,
                        suspend: Some(*result),
                        consume_deferred_poll: false,
                    })
                }
                RecipeDecision::NoLongerExpected => {
                    self.retire_effect(plan.plan.key());
                    None
                }
            };
            publication.release();
            self.events().flush();
            self.lane.lane.announce();
            return Ok(result);
        }
    }
}

pub(crate) fn reason(current: &CurrentOperation) -> CompactionEventReason {
    match &current.state {
        OperationState::Run(r) => match &r.phase {
            RunPhase::Compaction(c) => match c.reason {
                RunCompactionReason::Threshold => CompactionEventReason::Threshold,
                RunCompactionReason::Overflow => CompactionEventReason::Overflow,
            },
            _ => CompactionEventReason::Manual,
        },
        _ => CompactionEventReason::Manual,
    }
}
pub(crate) fn end_event(
    operation: &Operation,
    reason: CompactionEventReason,
    result: OperationResult,
    recovery: bool,
) -> Result<(HarnessEventType, HarnessEvent), HarnessRuntimeError> {
    let lane = operation.lane.clone();
    let run_id = operation.operation_id.clone();
    let recovery = recovery.then_some(pi_ai_core::LiteralTrue);
    Ok(match result {
        OperationResult::Run(result) => (
            HarnessEventType::RunEnd,
            crate::publication::run_end(lane, run_id, OperationResult::Run(result))?,
        ),
        OperationResult::Compaction(result) => (
            HarnessEventType::CompactionEnd,
            match result {
                CompactionOutcome::Completed(c) => {
                    HarnessEvent::CompactionEndCompleted(Box::new(CompactionEndCompletedEvent {
                        event_type: Default::default(),
                        lane,
                        recovery,
                        run_id,
                        reason,
                        outcome: Default::default(),
                        from_hook: c.entry.from_hook,
                        entry: c.entry,
                    }))
                }
                CompactionOutcome::Declined(_) => {
                    HarnessEvent::CompactionEndDeclined(CompactionEndDeclinedEvent {
                        event_type: Default::default(),
                        lane,
                        recovery,
                        run_id,
                        reason,
                        outcome: Default::default(),
                    })
                }
                CompactionOutcome::Aborted(_) => {
                    HarnessEvent::CompactionEndAborted(CompactionEndAbortedEvent {
                        event_type: Default::default(),
                        lane,
                        recovery,
                        run_id,
                        reason,
                        outcome: Default::default(),
                    })
                }
                CompactionOutcome::Failed(c) => {
                    HarnessEvent::CompactionEndFailed(CompactionEndFailedEvent {
                        event_type: Default::default(),
                        lane,
                        recovery,
                        run_id,
                        reason,
                        outcome: Default::default(),
                        error: c.error,
                    })
                }
                _ => {
                    return Err(crate::restore::fault(
                        "suspension cannot emit structural end",
                    ));
                }
            },
        ),
        OperationResult::Navigation(result) => {
            let old_leaf_id = operation.source_leaf_id.clone();
            (
                HarnessEventType::NavigationEnd,
                match result {
                    NavigationOutcome::Completed(n) => HarnessEvent::NavigationEndCompleted(
                        Box::new(NavigationEndCompletedEvent {
                            event_type: Default::default(),
                            lane,
                            recovery,
                            run_id,
                            old_leaf_id: n.old_leaf_id,
                            new_leaf_id: n.new_leaf_id,
                            outcome: Default::default(),
                            summary_entry: n.summary_entry,
                        }),
                    ),
                    NavigationOutcome::Declined(n) => {
                        HarnessEvent::NavigationEndDeclined(NavigationEndDeclinedEvent {
                            event_type: Default::default(),
                            lane,
                            recovery,
                            run_id,
                            old_leaf_id,
                            new_leaf_id: n.leaf_id,
                            outcome: Default::default(),
                        })
                    }
                    NavigationOutcome::Aborted(n) => {
                        HarnessEvent::NavigationEndAborted(NavigationEndAbortedEvent {
                            event_type: Default::default(),
                            lane,
                            recovery,
                            run_id,
                            old_leaf_id,
                            new_leaf_id: n.leaf_id,
                            outcome: Default::default(),
                        })
                    }
                    NavigationOutcome::Failed(n) => {
                        HarnessEvent::NavigationEndFailed(NavigationEndFailedEvent {
                            event_type: Default::default(),
                            lane,
                            recovery,
                            run_id,
                            old_leaf_id,
                            new_leaf_id: n.leaf_id,
                            outcome: Default::default(),
                            error: n.error,
                        })
                    }
                    _ => {
                        return Err(crate::restore::fault(
                            "suspension cannot emit navigation end",
                        ));
                    }
                },
            )
        }
    })
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) fn structural_started(
        &self,
        previous: Option<&CurrentOperation>,
        current: &CurrentOperation,
        telemetry: Telemetry,
    ) {
        if pi_harness_core::structural::decision(current).is_some()
            && previous.is_some_and(|p| {
                p.operation.operation_id == current.operation.operation_id
                    && pi_harness_core::structural::task(p).ok()
                        == pi_harness_core::structural::task(current).ok()
            })
        {
            return;
        }
        let event = match &current.state {
            state
                if matches!(state, OperationState::Compaction(_))
                    || matches!(state,OperationState::Run(r) if matches!(r.phase,RunPhase::Compaction(_))) =>
            {
                Some((
                    HarnessEventType::CompactionStart,
                    HarnessEvent::CompactionStart(CompactionStartEvent {
                        event_type: Default::default(),
                        lane: self.lane.name.clone(),
                        recovery: None,
                        run_id: current.operation.operation_id.clone(),
                        reason: reason(current),
                    }),
                ))
            }
            OperationState::Navigation(_) => {
                if previous
                    .is_some_and(|p| p.operation.operation_id == current.operation.operation_id)
                {
                    return;
                }
                let OperationIntent::Navigation(n) = &current.operation.intent else {
                    return;
                };
                Some((
                    HarnessEventType::NavigationStart,
                    HarnessEvent::NavigationStart(NavigationStartEvent {
                        event_type: Default::default(),
                        lane: self.lane.name.clone(),
                        recovery: None,
                        run_id: current.operation.operation_id.clone(),
                        target_id: n.target_id.clone(),
                    }),
                ))
            }
            _ => None,
        };
        if let Some((kind, event)) = event {
            self.owner
                .events
                .with_context(telemetry)
                .enqueue(kind, &self.lane.name, event);
        }
    }
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) async fn structural_external(
        &self,
        original: &Operation,
        value: LaneLastResult,
    ) -> Result<OperationResult, HarnessRuntimeError> {
        let (id, leaf, outcome, error) = match value {
            LaneLastResult::CompactionCompleted(v)
                if matches!(original.intent, OperationIntent::Compaction(_)) =>
            {
                (v.operation_id, v.leaf_id, "completed", None)
            }
            LaneLastResult::CompactionDeclined(v)
                if matches!(original.intent, OperationIntent::Compaction(_)) =>
            {
                (v.operation_id, v.leaf_id, "declined", None)
            }
            LaneLastResult::CompactionAborted(v)
                if matches!(original.intent, OperationIntent::Compaction(_)) =>
            {
                (v.operation_id, v.leaf_id, "aborted", None)
            }
            LaneLastResult::CompactionFailed(v)
                if matches!(original.intent, OperationIntent::Compaction(_)) =>
            {
                (v.operation_id, v.leaf_id, "failed", Some(v.error))
            }
            LaneLastResult::NavigationCompleted(v)
                if matches!(original.intent, OperationIntent::Navigation(_)) =>
            {
                (v.operation_id, v.leaf_id, "completed", None)
            }
            LaneLastResult::NavigationDeclined(v)
                if matches!(original.intent, OperationIntent::Navigation(_)) =>
            {
                (v.operation_id, v.leaf_id, "declined", None)
            }
            LaneLastResult::NavigationAborted(v)
                if matches!(original.intent, OperationIntent::Navigation(_)) =>
            {
                (v.operation_id, v.leaf_id, "aborted", None)
            }
            LaneLastResult::NavigationFailed(v)
                if matches!(original.intent, OperationIntent::Navigation(_)) =>
            {
                (v.operation_id, v.leaf_id, "failed", Some(v.error))
            }
            _ => return Err(self.owner.fail("external structural kind mismatch")),
        };
        if id != original.operation_id {
            return Err(self.owner.fail("external structural identity mismatch"));
        }
        for namespace in [
            RegisterNamespace::OperationPreparation,
            RegisterNamespace::OperationToolArguments,
        ] {
            if !self
                .owner
                .session
                .list_registers(namespace, Some(&format!("{}:", id.as_str())))
                .await
                .map_err(|_| self.owner.fail("external structural cleanup read failed"))?
                .is_empty()
            {
                return Err(self.owner.fail("external structural cleanup incomplete"));
            }
        }
        let entry = if let Some(id) = &leaf {
            Some(
                self.owner
                    .session
                    .get_entries(std::slice::from_ref(id))
                    .await
                    .map_err(|_| self.owner.fail("external structural entry read failed"))?
                    .remove(id)
                    .ok_or_else(|| self.owner.fail("external structural entry missing"))?,
            )
        } else {
            None
        };
        Ok(match &original.intent {
            OperationIntent::Compaction(_) => {
                let leaf_id =
                    leaf.ok_or_else(|| self.owner.fail("external compaction leaf missing"))?;
                OperationResult::Compaction(match outcome {
                    "completed" => {
                        let Some(Entry::Compaction(entry)) = entry else {
                            return Err(self.owner.fail("external compaction result row mismatch"));
                        };
                        CompactionOutcome::Completed(CompactionCompleted {
                            kind: Default::default(),
                            leaf_id,
                            entry,
                        })
                    }
                    "declined" => CompactionOutcome::Declined(CompactionDeclined {
                        kind: Default::default(),
                        leaf_id,
                    }),
                    "aborted" => CompactionOutcome::Aborted(CompactionAborted {
                        kind: Default::default(),
                        leaf_id,
                    }),
                    _ => CompactionOutcome::Failed(CompactionFailed {
                        kind: Default::default(),
                        leaf_id,
                        error: error.expect("failed error"),
                    }),
                })
            }
            OperationIntent::Navigation(n) => OperationResult::Navigation(match outcome {
                "completed" => {
                    let summary_entry = if n.summarize {
                        let Some(Entry::BranchSummary(e)) = entry else {
                            return Err(self.owner.fail("external branch result row missing"));
                        };
                        if e.parent_id != n.target_id
                            || Some(&e.from_id) != original.source_leaf_id.as_ref()
                        {
                            return Err(self.owner.fail("external branch result lineage mismatch"));
                        }
                        Some(e)
                    } else {
                        if leaf != n.target_id {
                            return Err(self.owner.fail("external navigation target mismatch"));
                        }
                        None
                    };
                    NavigationOutcome::Completed(Box::new(NavigationCompleted {
                        kind: Default::default(),
                        old_leaf_id: original.source_leaf_id.clone(),
                        new_leaf_id: leaf,
                        summary_entry,
                    }))
                }
                "declined" => NavigationOutcome::Declined(NavigationDeclined {
                    kind: Default::default(),
                    leaf_id: leaf,
                }),
                "aborted" => NavigationOutcome::Aborted(NavigationAborted {
                    kind: Default::default(),
                    leaf_id: leaf,
                }),
                _ => NavigationOutcome::Failed(NavigationFailed {
                    kind: Default::default(),
                    leaf_id: leaf,
                    error: error.expect("failed error"),
                }),
            }),
            _ => return Err(self.owner.fail("external original kind mismatch")),
        })
    }
}

pub(crate) fn dispatch_configuration(current: &CurrentOperation) -> Option<&LaneConfiguration> {
    if pi_harness_core::structural::is_cancelled(current) {
        return None;
    }
    match pi_harness_core::structural::decision(current)? {
        StructuralDecision::Deciding { .. } => Some(&current.configuration),
        StructuralDecision::Generating { generation, .. } => match generation.as_ref() {
            SummaryGeneration::Ready { context, .. } => Some(&context.configuration),
            _ => None,
        },
    }
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) fn resume_structure(&self, current: &CurrentOperation) {
        let event = match &current.state {
            OperationState::Navigation(_) => {
                let OperationIntent::Navigation(n) = &current.operation.intent else {
                    return;
                };
                Some((
                    HarnessEventType::NavigationStart,
                    HarnessEvent::NavigationStart(NavigationStartEvent {
                        event_type: Default::default(),
                        lane: current.operation.lane.clone(),
                        recovery: Some(pi_ai_core::LiteralTrue),
                        run_id: current.operation.operation_id.clone(),
                        target_id: n.target_id.clone(),
                    }),
                ))
            }
            state
                if matches!(state, OperationState::Compaction(_))
                    || matches!(state,OperationState::Run(r)if matches!(r.phase,RunPhase::Compaction(_))) =>
            {
                Some((
                    HarnessEventType::CompactionStart,
                    HarnessEvent::CompactionStart(CompactionStartEvent {
                        event_type: Default::default(),
                        lane: current.operation.lane.clone(),
                        recovery: Some(pi_ai_core::LiteralTrue),
                        run_id: current.operation.operation_id.clone(),
                        reason: reason(current),
                    }),
                ))
            }
            _ => None,
        };
        if let Some((kind, event)) = event {
            self.trace.sync(current);
            self.events().enqueue(kind, &current.operation.lane, event);
        }
    }
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeLane<C, S, M>
{
    pub(crate) fn resume_configuration(
        &self,
        current: &CurrentOperation,
    ) -> Option<LaneConfiguration> {
        if pi_harness_core::structural::is_cancelled(current) {
            return None;
        }
        if let Some(configuration) = dispatch_configuration(current) {
            return Some(configuration.clone());
        }
        let pending = pi_harness_core::structural::pending(current)?;
        if pending.request.is_some() {
            return None;
        }
        let observations = self
            .owner
            .suspensions
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let observation = observations.get(&self.lane.name)?;
        (observation.operation_id == current.operation.operation_id
            && observation.summary.as_ref() == Some(&pending))
        .then_some(pending.context.configuration)
    }
}
