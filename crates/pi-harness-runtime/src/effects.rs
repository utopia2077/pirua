//! Concrete effect interpreter; procedures receive detached facts and this catalog.
use crate::owner::*;
use crate::tools::{CapturedToolCall, captured_arguments_match};
use pi_agent_schema::*;
use pi_agent_session::{
    backend_support::{register_key, register_seq},
    session::WriteObservation,
    storage::RuntimeStorage,
};
use pi_ai_core::{CancellationToken, SafeUnsigned};
use pi_harness_core::planner::*;
use std::{
    any::Any,
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    sync::{Arc, Mutex},
    time::Duration,
};

/// Owns both Harness lines and any admitted commit across Session awaits,
/// errors, cancellation and unwind. Drop releases the pair before the commit:
/// the last commit can wake arbitrary drain waiters after an admission seal.
struct SettlementPermits {
    pair: Option<crate::scheduler::PairPermit>,
    commit: Option<crate::scheduler::CommitPermit>,
    observation: Option<WriteObservation>,
    observation_parent: Option<Telemetry>,
    pending_cleanup_fault: Option<Box<dyn FnOnce() + Send>>,
}

impl SettlementPermits {
    fn new() -> Self {
        Self {
            pair: None,
            commit: None,
            observation: None,
            observation_parent: None,
            pending_cleanup_fault: None,
        }
    }

    async fn acquire(
        &mut self,
        settings_line: &crate::scheduler::MutationLine,
        lane_line: &crate::scheduler::MutationLine,
    ) -> Result<(), HarnessRuntimeError> {
        debug_assert!(self.pair.is_none());
        self.pair = Some(settings_line.acquire_pair(lane_line).await?);
        Ok(())
    }

    fn commit_slot(&mut self) -> &mut Option<crate::scheduler::CommitPermit> {
        &mut self.commit
    }

    fn observation_slot(&mut self) -> &mut Option<WriteObservation> {
        &mut self.observation
    }

    fn commit_and_observation_slots(
        &mut self,
    ) -> (
        &mut Option<crate::scheduler::CommitPermit>,
        &mut Option<WriteObservation>,
    ) {
        (&mut self.commit, &mut self.observation)
    }

    fn observation_parent(&mut self, parent: Telemetry) {
        self.observation_parent = Some(parent);
    }

    fn release_lines(&mut self) {
        if let Some(pair) = self.pair.take() {
            pair.release();
        }
    }

    #[cfg(feature = "extensions")]
    fn arm_cleanup_fault(&mut self, fault: impl FnOnce() + Send + 'static) {
        debug_assert!(self.pending_cleanup_fault.is_none());
        self.pending_cleanup_fault = Some(Box::new(fault));
    }

    #[cfg(feature = "extensions")]
    fn disarm_cleanup_fault(&mut self) {
        self.pending_cleanup_fault = None;
    }

    /// Make the earned observation visible before exact B1 cleanup callbacks,
    /// without allowing Close's last-commit drain to complete yet.
    #[cfg(feature = "extensions")]
    fn emit_before_cleanup(&mut self) {
        let mut first = None;
        Self::attempt(&mut first, || self.release_lines());
        let observation = self.observation.take();
        let parent = self.observation_parent.take();
        if let Some(observation) = observation {
            Self::attempt(&mut first, || {
                let parent = parent.expect("earned write observation retains its parent");
                observation.emit(parent.as_ref());
            });
        }
        if let Some(panic) = first {
            resume_unwind(panic);
        }
    }

    fn attempt(first: &mut Option<Box<dyn Any + Send>>, work: impl FnOnce()) {
        if let Err(panic) = catch_unwind(AssertUnwindSafe(work)) {
            if first.is_none() {
                *first = Some(panic);
            } else {
                let _ = catch_unwind(AssertUnwindSafe(|| drop(panic)));
            }
        }
    }

    fn finish_with(&mut self, observe: impl FnOnce()) {
        let already_unwinding = std::thread::panicking();
        let mut first = None;
        Self::attempt(&mut first, || self.release_lines());
        let observation = self.observation.take();
        let parent = self.observation_parent.take();
        if let Some(observation) = observation {
            Self::attempt(&mut first, || {
                let parent = parent.expect("earned write observation retains its parent");
                observation.emit(parent.as_ref());
            });
        }
        if let Some(cleanup_fault) = self.pending_cleanup_fault.take() {
            Self::attempt(&mut first, cleanup_fault);
        }
        Self::attempt(&mut first, observe);
        Self::attempt(&mut first, || drop(self.commit.take()));
        if let Some(panic) = first {
            if already_unwinding {
                let _ = catch_unwind(AssertUnwindSafe(|| drop(panic)));
            } else {
                resume_unwind(panic);
            }
        }
    }

    fn observe_fault<C, S: RuntimeStorage, M: SessionMetadataRecord>(
        &mut self,
        owner: &Owner<C, S, M>,
        error: HarnessRuntimeError,
    ) -> HarnessRuntimeError {
        let mut observed = None;
        self.finish_with(|| {
            observed = Some(
                owner
                    .observe::<()>(Err(error))
                    .expect_err("fault observation"),
            );
        });
        observed.expect("fault observed before commit drain")
    }

    fn release(&mut self) {
        // Keep this explicit; field declaration order is not the ownership rule.
        self.finish_with(|| {});
    }
}

impl Drop for SettlementPermits {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(test)]
mod settlement_permit_tests {
    use super::SettlementPermits;
    use crate::scheduler::{Admission, MutationLine};
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    fn poll<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }

    struct ReenterSettings {
        settings: MutationLine,
        acquired: Arc<AtomicBool>,
    }

    impl Wake for ReenterSettings {
        fn wake(self: Arc<Self>) {
            let mut attempt = Box::pin(self.settings.acquire());
            if let Poll::Ready(Ok(permit)) = poll(attempt.as_mut(), Waker::noop()) {
                self.acquired.store(true, Ordering::SeqCst);
                drop(permit);
            }
        }
    }

    #[test]
    fn dropping_pending_settlement_read_releases_both_lines_before_wake() {
        let admission = Admission::new();
        let settings = MutationLine::new(admission.clone());
        let lane = MutationLine::new(admission);
        let mut settlement = Box::pin({
            let settings = settings.clone();
            let lane = lane.clone();
            async move {
                let mut permits = SettlementPermits::new();
                permits.acquire(&settings, &lane).await.unwrap();
                std::future::pending::<()>().await;
            }
        });
        assert!(poll(settlement.as_mut(), Waker::noop()).is_pending());
        let acquired = Arc::new(AtomicBool::new(false));
        let wake = Waker::from(Arc::new(ReenterSettings {
            settings: settings.clone(),
            acquired: acquired.clone(),
        }));
        let mut lane_waiter = Box::pin(lane.acquire());
        assert!(poll(lane_waiter.as_mut(), &wake).is_pending());
        drop(settlement);
        assert!(
            acquired.load(Ordering::SeqCst),
            "lane waiter reentered settings only after paired Drop released both"
        );
        let Poll::Ready(Ok(lane_permit)) = poll(lane_waiter.as_mut(), Waker::noop()) else {
            panic!("lane waiter remained blocked")
        };
        drop(lane_permit);
    }

    #[test]
    fn queued_settlement_pair_cancel_preserves_next_settings_ticket() {
        let admission = Admission::new();
        let settings = MutationLine::new(admission.clone());
        let lane = MutationLine::new(admission);
        let mut occupied = Box::pin(lane.acquire());
        let lane_holder = match poll(occupied.as_mut(), Waker::noop()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("lane holder"),
        };
        let mut pair = Box::pin({
            let settings = settings.clone();
            let lane = lane.clone();
            async move {
                let mut permits = SettlementPermits::new();
                permits.acquire(&settings, &lane).await
            }
        });
        assert!(poll(pair.as_mut(), Waker::noop()).is_pending());
        assert!(
            !settings.held_for_test(),
            "queued pair reserves without holding settings"
        );
        let mut successor = Box::pin(settings.acquire());
        assert!(poll(successor.as_mut(), Waker::noop()).is_pending());
        drop(pair);
        let next = match poll(successor.as_mut(), Waker::noop()) {
            Poll::Ready(Ok(permit)) => permit,
            _ => panic!("cancelled pair must pass its settings position"),
        };
        drop(next);
        drop(lane_holder);
    }

    #[test]
    fn queued_tool_begin_pair_cancel_or_seal_without_settings_ownership() {
        use pi_agent_schema::HarnessRuntimeError;

        // The Tool branch of begin awaits this same pair acquisition before
        // hydration or registration. Neither queue exit holds settings.
        for seal in [false, true] {
            let admission = Admission::new();
            let settings = MutationLine::new(admission.clone());
            let lane = MutationLine::new(admission.clone());
            let mut occupied = Box::pin(lane.acquire());
            let Poll::Ready(Ok(lane_holder)) = poll(occupied.as_mut(), Waker::noop()) else {
                panic!("lane holder")
            };
            let mut begin_pair = Box::pin(settings.acquire_pair(&lane));
            assert!(poll(begin_pair.as_mut(), Waker::noop()).is_pending());
            assert!(!settings.held_for_test());
            let mut next_settings = Box::pin(settings.acquire());
            assert!(poll(next_settings.as_mut(), Waker::noop()).is_pending());
            if seal {
                admission.seal_close();
                assert!(matches!(
                    poll(begin_pair.as_mut(), Waker::noop()),
                    Poll::Ready(Err(HarnessRuntimeError::Closed(_)))
                ));
                assert!(matches!(
                    poll(next_settings.as_mut(), Waker::noop()),
                    Poll::Ready(Err(HarnessRuntimeError::Closed(_)))
                ));
            } else {
                drop(begin_pair);
                let Poll::Ready(Ok(successor)) = poll(next_settings.as_mut(), Waker::noop()) else {
                    panic!("cancelled begin passed its settings position")
                };
                drop(successor);
            }
            assert!(!settings.held_for_test());
            drop(lane_holder);
        }
    }

    struct CheckDrain {
        settings: MutationLine,
        lane: MutationLine,
        called: Arc<AtomicBool>,
    }
    impl Wake for CheckDrain {
        fn wake(self: Arc<Self>) {
            assert!(!self.settings.held_for_test());
            assert!(!self.lane.held_for_test());
            self.called.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn last_commit_drain_follows_pair_release_on_drop() {
        let admission = Admission::new();
        let settings = MutationLine::new(admission.clone());
        let lane = MutationLine::new(admission.clone());
        let mut scope = SettlementPermits::new();
        let mut acquisition = Box::pin(scope.acquire(&settings, &lane));
        assert!(matches!(
            poll(acquisition.as_mut(), Waker::noop()),
            Poll::Ready(Ok(()))
        ));
        drop(acquisition);
        *scope.commit_slot() = Some(admission.try_admit().unwrap());
        let called = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(CheckDrain {
            settings: settings.clone(),
            lane: lane.clone(),
            called: called.clone(),
        }));
        let mut drain = Box::pin(admission.drained());
        assert!(poll(drain.as_mut(), &waker).is_pending());
        admission.seal_close();
        assert!(!called.load(Ordering::SeqCst));
        drop(scope);
        assert!(called.load(Ordering::SeqCst));
        assert!(poll(drain.as_mut(), Waker::noop()).is_ready());
    }

    #[test]
    fn raw_fault_seal_notifies_after_pair_release() {
        let admission = Admission::new();
        let settings = MutationLine::new(admission.clone());
        let lane = MutationLine::new(admission.clone());
        let called = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(CheckDrain {
            settings: settings.clone(),
            lane: lane.clone(),
            called: called.clone(),
        }));
        let mut sealed = Box::pin(admission.seal_signal());
        assert!(poll(sealed.as_mut(), &waker).is_pending());
        let mut scope = SettlementPermits::new();
        let mut acquisition = Box::pin(scope.acquire(&settings, &lane));
        assert!(matches!(
            poll(acquisition.as_mut(), Waker::noop()),
            Poll::Ready(Ok(()))
        ));
        drop(acquisition);
        let pi_agent_schema::HarnessRuntimeError::Fault(fault) =
            crate::restore::fault("routed fault")
        else {
            unreachable!()
        };
        assert!(!called.load(Ordering::SeqCst));
        scope.release();
        admission.seal_fault(fault);
        assert!(called.load(Ordering::SeqCst));
        assert!(poll(sealed.as_mut(), Waker::noop()).is_ready());
    }

    struct PanicDropWake(Arc<AtomicBool>);
    impl Wake for PanicDropWake {
        fn wake(self: Arc<Self>) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    impl Drop for PanicDropWake {
        fn drop(&mut self) {
            panic!("deferred caller Waker drop");
        }
    }

    #[test]
    fn panicking_pair_waker_still_releases_lines_before_last_commit_drain() {
        let admission = Admission::new();
        let settings = MutationLine::new(admission.clone());
        let lane = MutationLine::new(admission.clone());
        let mut scope = SettlementPermits::new();
        let caller = Waker::from(Arc::new(PanicDropWake(Arc::new(AtomicBool::new(false)))));
        let mut acquisition = Box::pin(scope.acquire(&settings, &lane));
        assert!(matches!(
            poll(acquisition.as_mut(), &caller),
            Poll::Ready(Ok(()))
        ));
        drop(acquisition);
        drop(caller);
        *scope.commit_slot() = Some(admission.try_admit().unwrap());
        let called = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(CheckDrain {
            settings: settings.clone(),
            lane: lane.clone(),
            called: called.clone(),
        }));
        let mut drain = Box::pin(admission.drained());
        assert!(poll(drain.as_mut(), &waker).is_pending());
        admission.seal_close();
        assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(scope))).is_err());
        assert!(called.load(Ordering::SeqCst));
        assert!(poll(drain.as_mut(), Waker::noop()).is_ready());
    }

    #[cfg(feature = "extensions")]
    #[test]
    fn cancelled_wait_for_cleanup_arbiter_faults_before_last_commit_drain() {
        use crate::tools::CleanupArbiter;
        use pi_agent_schema::HarnessRuntimeError;

        struct FaultBeforeDrain {
            admission: Admission,
            settings: MutationLine,
            lane: MutationLine,
            called: Arc<AtomicBool>,
        }
        impl Wake for FaultBeforeDrain {
            fn wake(self: Arc<Self>) {
                assert!(!self.settings.held_for_test());
                assert!(!self.lane.held_for_test());
                assert!(matches!(
                    self.admission.check(),
                    Err(HarnessRuntimeError::Fault(_))
                ));
                self.called.store(true, Ordering::SeqCst);
            }
        }

        let admission = Admission::new();
        let settings = MutationLine::new(admission.clone());
        let lane = MutationLine::new(admission.clone());
        let arbiter = CleanupArbiter::new();
        let mut first = Box::pin(arbiter.enter());
        let Poll::Ready(Ok(Some(first_lease))) = poll(first.as_mut(), Waker::noop()) else {
            panic!("first cleanup owns the arbiter")
        };
        drop(first);
        let mut cleanup = Box::pin({
            let admission = admission.clone();
            let settings = settings.clone();
            let lane = lane.clone();
            let arbiter = arbiter.clone();
            async move {
                let mut permits = SettlementPermits::new();
                permits.acquire(&settings, &lane).await.unwrap();
                *permits.commit_slot() = Some(admission.try_admit().unwrap());
                let fault_admission = admission.clone();
                permits.arm_cleanup_fault(move || {
                    let HarnessRuntimeError::Fault(fault) =
                        crate::restore::fault("cancelled exact cleanup")
                    else {
                        unreachable!()
                    };
                    fault_admission.seal_fault(fault);
                });
                permits.emit_before_cleanup();
                let _ = arbiter.enter().await;
            }
        });
        assert!(poll(cleanup.as_mut(), Waker::noop()).is_pending());
        assert!(!settings.held_for_test());
        assert!(!lane.held_for_test());
        let called = Arc::new(AtomicBool::new(false));
        let waker = Waker::from(Arc::new(FaultBeforeDrain {
            admission: admission.clone(),
            settings: settings.clone(),
            lane: lane.clone(),
            called: called.clone(),
        }));
        let mut drain = Box::pin(admission.drained());
        assert!(poll(drain.as_mut(), &waker).is_pending());
        admission.seal_close();
        assert!(!called.load(Ordering::SeqCst));
        drop(cleanup);
        assert!(called.load(Ordering::SeqCst));
        assert!(poll(drain.as_mut(), Waker::noop()).is_ready());
        drop(first_lease);
    }
}

pub(crate) struct RuntimeEffects<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    pub lane: RuntimeLane<C, S, M>,
    pub operation_kind: Mutex<OperationKind>,
    pub original_operation: Mutex<Option<Operation>>,
    pub applied_dispatch: Mutex<Option<EffectPlan<Telemetry>>>,
    pub terminal_candidate: Mutex<Option<crate::structural::TerminalCandidate>>,
    pub terminal_result: Mutex<Option<OperationResult>>,
    pub threshold_candidate: Mutex<Option<crate::compaction::ThresholdCandidate>>,
    pub summary_binding: Mutex<Option<crate::summary::SummaryBinding>>,
    pub recovery: bool,
    pub trace: crate::telemetry::Trace,
    pub(crate) this: std::sync::Weak<Self>,
    pub running: Mutex<BTreeMap<EffectKey, EffectPlan<Telemetry>>>,
    pub started_turns: Mutex<BTreeSet<String>>,
    pub cancellation: CancellationToken,
    pub retired: CancellationToken,
    pub inbox_preparation: Mutex<Option<crate::checkpoint::InboxPreparation>>,
    pub poll_permit: std::sync::atomic::AtomicBool,
    pub cancel_targets: Mutex<BTreeMap<EntryId, crate::poll::CancelTarget>>,
    pub executed_tools: Mutex<BTreeSet<EffectKey>>,
    pub captured_tools: Mutex<BTreeMap<EffectKey, CapturedToolCall<C>>>,
    #[cfg(feature = "extensions")]
    pub(crate) routed_tools: Mutex<BTreeMap<EffectKey, Arc<Mutex<crate::tools::RoutedEffect>>>>,
    pub request_bindings: Mutex<BTreeMap<EffectKey, RequestBinding>>,
    pub start_signals: Mutex<BTreeMap<EffectKey, CancellationToken>>,
    pub context_line: crate::scheduler::MutationLine,
    pub contexts: Mutex<BTreeMap<StepId, Arc<C>>>,
    pub bindings: Mutex<BTreeMap<(StepId, SafeUnsigned), AgentHarnessToolHandle<C>>>,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub fn new(lane: RuntimeLane<C, S, M>, recovery: bool) -> Arc<Self> {
        Arc::new_cyclic(|this| Self {
            trace: crate::telemetry::Trace::new(lane.owner.telemetry.clone()),
            operation_kind: Mutex::new(OperationKind::Run),
            original_operation: Mutex::new(None),
            applied_dispatch: Mutex::new(None),
            terminal_candidate: Mutex::new(None),
            terminal_result: Mutex::new(None),
            threshold_candidate: Mutex::new(None),
            summary_binding: Mutex::new(None),
            recovery,
            this: this.clone(),
            context_line: crate::scheduler::MutationLine::new(lane.owner.admission.clone()),
            contexts: Mutex::new(BTreeMap::new()),
            bindings: Mutex::new(BTreeMap::new()),
            lane,
            running: Mutex::new(BTreeMap::new()),
            started_turns: Mutex::new(BTreeSet::new()),
            start_signals: Mutex::new(BTreeMap::new()),
            request_bindings: Mutex::new(BTreeMap::new()),
            executed_tools: Mutex::new(BTreeSet::new()),
            captured_tools: Mutex::new(BTreeMap::new()),
            #[cfg(feature = "extensions")]
            routed_tools: Mutex::new(BTreeMap::new()),
            cancellation: CancellationToken::new(),
            retired: CancellationToken::new(),
            inbox_preparation: Mutex::new(None),
            poll_permit: std::sync::atomic::AtomicBool::new(false),
            cancel_targets: Mutex::new(BTreeMap::new()),
        })
    }
    pub(crate) async fn permission(&self, kind: &str) -> Result<(), HarnessRuntimeError> {
        let mut park = Box::pin(self.lane.lane.gate.park(ActionInfo {
            kind: kind.into(),
            description: kind.into(),
            details: pi_ai_core::Presence::Missing,
        }));
        let mut announced = false;
        let _permission = std::future::poll_fn(|cx| {
            let result = park.as_mut().poll(cx);
            if !announced {
                announced = true;
                self.lane.lane.announce();
            }
            result
        })
        .await?;
        Ok(())
    }
    pub(crate) async fn inputs(
        &self,
        h: &crate::restore::HydratedLane,
    ) -> Result<PlannerInputs<Telemetry>, HarnessRuntimeError> {
        self.inputs_with_policy(h, false).await
    }
    pub(crate) fn line_fault(&self, raw: bool, message: &'static str) -> HarnessRuntimeError {
        if raw {
            crate::restore::fault(message)
        } else {
            self.lane.owner.fail(message)
        }
    }
    async fn inputs_with_policy(
        &self,
        h: &crate::restore::HydratedLane,
        raw: bool,
    ) -> Result<PlannerInputs<Telemetry>, HarnessRuntimeError> {
        let settings = self
            .lane
            .owner
            .settings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let attempts = if settings.retry.enabled {
            settings.retry.max_retries.max_attempts()
        } else {
            1
        };
        let mut loaded: BTreeMap<_, _> = h
            .entries
            .iter()
            .map(|(k, v)| (k.as_str().into(), PlannerLoadedValue::Entry(v.clone())))
            .chain(h.registers.iter().map(|((_, key), value)| {
                (key.clone(), PlannerLoadedValue::Register(value.clone()))
            }))
            .collect();
        if let Some(CurrentOperation {
            state: OperationState::Run(run),
            operation,
            ..
        }) = &h.current
            && let RunPhase::Tools(tools) = &run.phase
        {
            for call in &tools.batch.calls {
                // Hydration already captured each pending call's arguments with
                // the operation state. A later raw read could replace that
                // sequence after the execution-capture check but before the
                // checked settlement recipe is built.
                if matches!(call, ToolCallState::EffectPending { .. }) {
                    continue;
                }
                let index = match call {
                    ToolCallState::Planned { source_index, .. }
                    | ToolCallState::EffectPending { source_index, .. }
                    | ToolCallState::Completed { source_index, .. } => source_index,
                };
                let key = format!(
                    "{}:{}:{}",
                    operation.operation_id.as_str(),
                    tools.batch.turn_id.as_str(),
                    index.get()
                );
                if let Some(register) = self
                    .lane
                    .owner
                    .session
                    .get_register(RegisterNamespace::OperationToolArguments, &key)
                    .await
                    .map_err(|_| self.line_fault(raw, "tool cleanup arguments read failed"))?
                {
                    loaded.insert(key, PlannerLoadedValue::Register(register));
                }
            }
        }
        Ok(PlannerInputs {
            running: self
                .running
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
            deferred_poll: if self.poll_permit.load(std::sync::atomic::Ordering::Acquire) {
                DeferredPollPermit::One
            } else {
                DeferredPollPermit::None
            },
            deferred_cancellations: BTreeSet::new(),
            loaded,
            runtime: RuntimeSnapshot {
                settings_revision: settings.revision,
                stream_options: settings.stream_options,
                retry_policy: NormalizedRetryPolicy {
                    max_attempts: OneBasedSafeUnsigned::new(attempts)
                        .map_err(|_| self.line_fault(raw, "retry policy range failure"))?,
                    base_delay_ms: settings.retry.base_delay_ms,
                },
            },
            context: None,
            now: SafeUnsigned::new(self.lane.owner.clock.now_unix_ms())
                .map_err(|_| self.line_fault(raw, "runtime clock range failure"))?,
        })
    }
    pub(crate) async fn planning(
        &self,
        h: &crate::restore::HydratedLane,
        step: Option<StepId>,
        now: SafeUnsigned,
    ) -> Result<PlanningFacts<Telemetry>, HarnessRuntimeError> {
        let current = h
            .current
            .as_ref()
            .ok_or_else(|| self.lane.owner.fail("planner operation disappeared"))?;
        let session_id = self
            .lane
            .owner
            .session
            .metadata()
            .base_metadata()
            .id
            .clone();
        let telemetry_context = self.trace.context();
        Ok(
            match required_facts(current).map_err(|_| {
                self.lane
                    .owner
                    .fail("operation requires a later runtime stage")
            })? {
                RequiredFacts::Structural => {
                    PlanningFacts::Structural(pi_harness_core::structural::Planning {
                        session_id,
                        telemetry_context,
                        result_entry_id: None,
                    })
                }
                RequiredFacts::Checkpoint => PlanningFacts::Checkpoint {
                    session_id,
                    telemetry_context,
                    step_id: match step {
                        Some(step) => step,
                        None => StepId::new(self.lane.owner.ids.next(None))
                            .map_err(|_| self.lane.owner.fail("step identity failed"))?,
                    },
                },
                RequiredFacts::BeforeRequest => {
                    let OperationState::Run(run) = &current.state else {
                        return Err(self.lane.owner.fail("invalid request operation"));
                    };
                    let RunPhase::Assistant(AssistantPhase {
                        generation:
                            Generation::Ready {
                                context,
                                next_attempt,
                            },
                        ..
                    }) = &run.phase
                    else {
                        return Err(self.lane.owner.fail("invalid ready request"));
                    };
                    let missing = self.lane.missing(&context.configuration);
                    if !missing.models.is_empty() || !missing.tools.is_empty() {
                        return Ok(PlanningFacts::MissingIdentities { missing });
                    }
                    match self.lane.owner.models.get_model(
                        &context.configuration.model.provider,
                        &context.configuration.model.model_id,
                    ) {
                        Some(model) => PlanningFacts::BeforeRequest {
                            session_id,
                            telemetry_context,
                            event: Box::new(BeforeRequestEvent {
                                lane: current.operation.lane.clone(),
                                run_id: current.operation.operation_id.clone(),
                                model,
                                step: HookRequestStep::Assistant,
                                attempt: *next_attempt,
                                stream_options: context.stream_options.clone(),
                            }),
                        },
                        None => PlanningFacts::MissingIdentities {
                            missing: MissingIdentities {
                                tools: vec![],
                                models: vec![format!(
                                    "{}/{}",
                                    context.configuration.model.provider.as_str(),
                                    context.configuration.model.model_id.as_str()
                                )],
                            },
                        },
                    }
                }
                RequiredFacts::Generation => {
                    let synthetic = match &current.state {
                        OperationState::Run(run) => match &run.phase {
                            RunPhase::Assistant(AssistantPhase {
                                generation:
                                    Generation::EffectPending {
                                        context, attempt, ..
                                    },
                                ..
                            }) if !matches!(run.control, Control::Running(_))
                                || attempt.get() >= context.retry_policy.max_attempts.get() =>
                            {
                                let key = EffectKey {
                                    session_id: session_id.clone(),
                                    lane: current.operation.lane.clone(),
                                    operation_id: current.operation.operation_id.clone(),
                                    kind: EffectKind::Assistant,
                                    step_or_task_id: EffectStepId::new(context.step_id.as_str())
                                        .map_err(|_| {
                                            self.lane.owner.fail("generation step invalid")
                                        })?,
                                    logical_attempt: SafeUnsigned::new(attempt.get()).map_err(
                                        |_| self.lane.owner.fail("generation attempt invalid"),
                                    )?,
                                    item_index: None,
                                };
                                Some(Box::new(self.synthetic_generation(
                                    &key,
                                    context,
                                    now,
                                    !matches!(run.control, Control::Running(_)),
                                )?))
                            }
                            _ => None,
                        },
                        _ => None,
                    };
                    PlanningFacts::Generation {
                        session_id,
                        telemetry_context,
                        synthetic,
                    }
                }
                RequiredFacts::Finish => PlanningFacts::Finish {
                    session_id,
                    telemetry_context,
                    event: BeforeRunEndEvent {
                        lane: current.operation.lane.clone(),
                        run_id: current.operation.operation_id.clone(),
                        messages: self.run_messages(current).await?,
                    },
                },
                RequiredFacts::Terminal => PlanningFacts::Terminal,
                RequiredFacts::DeferredSource => {
                    self.deferred_facts(h, session_id, telemetry_context, now)
                        .await?
                }
                RequiredFacts::Tools => {
                    self.tool_facts(current, session_id, telemetry_context)
                        .await?
                }
            },
        )
    }
    pub(crate) async fn run_messages(
        &self,
        current: &CurrentOperation,
    ) -> Result<Vec<AgentMessage>, HarnessRuntimeError> {
        let mut entries = self
            .lane
            .owner
            .session
            .lane_view(self.lane.lane.name.clone())
            .find_entries_on_branch(BranchScan {
                order: Some(BranchOrder::NewestFirst),
                ..Default::default()
            })
            .await
            .map_err(|_| self.lane.owner.fail("run message read failed"))?;
        let mut messages = Vec::new();
        for entry in entries.drain(..) {
            if current.operation.source_leaf_id.as_ref()
                == Some(&pi_agent_session::backend_support::entry_structure(&entry).id)
            {
                break;
            }
            if let Entry::Message(e) = entry {
                messages.push(e.message);
            }
        }
        messages.reverse();
        Ok(messages)
    }
    async fn transition(
        &self,
        plan: TransitionCommit<Telemetry>,
    ) -> Result<Option<CurrentOperation>, HarnessRuntimeError> {
        self.permission("commit_transition").await?;
        let mut permits = SettlementPermits::new();
        permits
            .acquire(&self.lane.owner.settings_line, &self.lane.lane.line)
            .await?;
        let h = self.lane.load().await?;
        let Some(current) = &h.current else {
            return Ok(None);
        };
        if let Some(result) = self
            .nested_transition(&h, &plan, permits.commit_slot())
            .await?
        {
            permits.release();
            self.events().flush();
            self.lane.lane.announce();
            return Ok(result);
        }
        let threshold = self
            .threshold_candidate
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        if let Some(candidate) = threshold {
            if candidate.observed.current != h.current
                || candidate.observed.roots.configuration != h.roots.configuration
                || candidate.observed.roots.leaf != h.roots.leaf
                || candidate.revision
                    != self
                        .lane
                        .owner
                        .settings
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .revision
            {
                return Ok(None);
            }
            let inputs = self.inputs(&h).await?;
            let decision = pi_harness_core::transition::transition_recipe(
                current,
                &plan,
                &inputs,
                &self.lane.facts(&h)?,
                &candidate.facts,
            )
            .map_err(|_| self.lane.owner.fail("threshold transition rejected"))?;
            let result = if let RecipeDecision::Apply(recipe) = decision {
                let token = self
                    .lane
                    .owner
                    .session
                    .prepare_entries(candidate.stamp, vec![])
                    .await
                    .map_err(|_| self.lane.owner.fail("threshold preparation stamp failed"))?;
                if let Some(token) = token {
                    self.lane
                        .apply_prepared_scoped(*recipe, Some(token), permits.commit_slot(), false)
                        .await?
                        .and_then(|h| h.current)
                } else {
                    None
                }
            } else {
                None
            };
            permits.release();
            self.events().flush();
            self.lane.lane.announce();
            return Ok(result);
        }
        if pi_harness_core::inbox::select(current)
            .map_err(|_| self.lane.owner.fail("inbox selection failed"))?
            .is_some()
        {
            let prepared = self
                .inbox_preparation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            let Some(prepared) = prepared else {
                return Ok(None);
            };
            if current.operation != prepared.current.operation
                || current.operation_state_seq != prepared.current.operation_state_seq
                || current.lane_state_seq != prepared.current.lane_state_seq
            {
                return Ok(None);
            }
            let inputs = self.inputs(&h).await?;
            let decision = pi_harness_core::transition::transition_recipe(
                current,
                &plan,
                &inputs,
                &self.lane.facts(&h)?,
                &prepared.facts,
            )
            .map_err(|_| self.lane.owner.fail("inbox recipe rejected"))?;
            let result = match decision {
                RecipeDecision::Apply(recipe) => self
                    .lane
                    .apply_prepared_scoped(*recipe, prepared.entries, permits.commit_slot(), false)
                    .await?
                    .and_then(|h| h.current),
                _ => None,
            };
            permits.release();
            self.events().flush();
            self.lane.lane.announce();
            return Ok(result);
        }
        let step = match &plan.next {
            OperationState::Run(run) => match &run.phase {
                RunPhase::Assistant(AssistantPhase {
                    generation: Generation::Ready { context, .. },
                    ..
                }) => Some(context.step_id.clone()),
                _ => None,
            },
            _ => None,
        };
        let inputs = self.inputs(&h).await?;
        let planning = self.planning(&h, step, inputs.now).await?;
        let decision = pi_harness_core::transition::transition_recipe(
            current,
            &plan,
            &inputs,
            &self.lane.facts(&h)?,
            &planning,
        )
        .map_err(|_| self.lane.owner.fail("transition recipe rejected"))?;
        let result = match decision {
            RecipeDecision::Apply(recipe) => {
                let dispatch = recipe.dispatch.clone();
                let applied = self
                    .lane
                    .apply_prepared_scoped(*recipe, None, permits.commit_slot(), false)
                    .await?;
                if let Some(current) = applied.as_ref().and_then(|h| h.current.as_ref())
                    && dispatch.is_some()
                {
                    self.trace.sync(current);
                    let mut dispatch = dispatch;
                    if let Some(EffectPlan::Summary(plan)) = &mut dispatch {
                        plan.telemetry_context = self.trace.context();
                    }
                    *self
                        .applied_dispatch
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = dispatch;
                }
                applied.and_then(|h| h.current)
            }
            RecipeDecision::NoLongerExpected => None,
            RecipeDecision::Suspend(_) => {
                return Err(self.lane.owner.fail("unexpected suspension transition"));
            }
            RecipeDecision::Terminal(_) => {
                return Err(self.lane.owner.fail("unexpected terminal transition"));
            }
        };
        if result
            .as_ref()
            .is_some_and(|next| pi_harness_core::structural::decision(next).is_none())
            && pi_harness_core::structural::decision(current).is_some()
            && pi_harness_core::structural::is_cancelled(current)
        {
            let outcome =
                OperationResult::Compaction(CompactionOutcome::Aborted(CompactionAborted {
                    kind: Default::default(),
                    leaf_id: current
                        .leaf_id
                        .clone()
                        .ok_or_else(|| self.lane.owner.fail("cancelled compaction leaf absent"))?,
                }));
            let (kind, event) = crate::structural::end_event(
                &current.operation,
                crate::structural::reason(current),
                outcome,
                self.recovery,
            )?;
            self.events().enqueue(kind, &self.lane.lane.name, event);
        }
        if let Some(next) = &result
            && matches!(&current.state,OperationState::Run(run) if matches!(run.phase,RunPhase::Checkpoint(_)))
        {
            self.turn_start(next);
        }
        permits.release();
        self.events().flush();
        self.lane.lane.announce();
        Ok(result)
    }
    async fn settlement(
        &self,
        plan: SettlementCommit<Telemetry>,
    ) -> Result<Option<SettlementResult<Telemetry>>, HarnessRuntimeError> {
        if pi_harness_core::structural::decision(&plan.current).is_some() {
            return self.structural_settlement(plan).await;
        }
        self.permission("commit_settlement").await?;
        #[cfg(feature = "extensions")]
        let routed = matches!(
            (&plan.plan, &plan.output),
            (EffectPlan::Tool(_), SettlementOutput::Tool(_))
        ) && self
            .routed_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains_key(plan.plan.key());
        #[cfg(not(feature = "extensions"))]
        let routed = false;
        loop {
            let overflow = self.overflow_facts(&plan).await?;
            let mut retired_requests = Vec::new();
            let mut permits = SettlementPermits::new();
            permits
                .acquire(&self.lane.owner.settings_line, &self.lane.lane.line)
                .await?;
            let h = if routed {
                self.lane.load_raw().await?
            } else {
                self.lane.load().await?
            };
            let Some(current) = &h.current else {
                permits.release();
                #[cfg(feature = "extensions")]
                self.cleanup_routed(plan.plan.key(), false).await?;
                retired_requests.extend(self.retire_effect(plan.plan.key()));
                return Ok(None);
            };
            if current.operation != plan.current.operation
                || matches!(&plan.plan,EffectPlan::Deferred(p) if pi_harness_core::deferred::plan(current,&plan.plan.key().session_id,&plan.telemetry_context).map_or(true,|expected|expected.deferred!=p.deferred || expected.key!=p.key))
            {
                permits.release();
                #[cfg(feature = "extensions")]
                self.cleanup_routed(plan.plan.key(), false).await?;
                retired_requests.extend(self.retire_effect(plan.plan.key()));
                return Ok(None);
            }
            if let (EffectPlan::Tool(tool), SettlementOutput::Tool(_)) = (&plan.plan, &plan.output)
            {
                if !pi_harness_core::tool_batch::expects_effect(
                    current,
                    &self.lane.owner.session.metadata().base_metadata().id,
                    tool,
                ) {
                    permits.release();
                    #[cfg(feature = "extensions")]
                    self.cleanup_routed(plan.plan.key(), false).await?;
                    retired_requests.extend(self.retire_effect(plan.plan.key()));
                    return Ok(None);
                }
                let captured = self
                    .captured_tools
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(&tool.key)
                    .map(|capture| capture.arguments.clone())
                    .ok_or_else(|| self.line_fault(routed, "tool execution capture absent"))?;
                if !captured_arguments_match(&h, &captured) {
                    return Err(
                        self.line_fault(routed, "saved tool arguments changed before settlement")
                    );
                }
                #[cfg(feature = "extensions")]
                self.require_routed_terminal(&tool.key)?;
            }
            // The first routed tool needs a clock sample, a usage ID and
            // application tool descriptors for its recipe. Gather those with
            // both lines free, then revalidate the exact observation and
            // registry generations before using them for checked publication.
            #[cfg(feature = "extensions")]
            let mut routed_registry = None;
            let (h, routed_inputs, routed_facts) = if routed {
                let (handles_generation, route_epoch) = {
                    let tools = self
                        .lane
                        .owner
                        .tools
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    (tools.handles_generation(), tools.route_epoch())
                };
                let settings_revision = self
                    .lane
                    .owner
                    .settings
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .revision;
                permits.release_lines();
                let inputs = self.inputs_with_policy(&h, true).await?;
                let facts = if overflow.is_none() {
                    Some(self.settlement_facts(&plan, inputs.now, &h, true).await?)
                } else {
                    None
                };
                permits
                    .acquire(&self.lane.owner.settings_line, &self.lane.lane.line)
                    .await?;
                let latest = self.lane.load_raw().await?;
                let same_registry = {
                    let tools = self
                        .lane
                        .owner
                        .tools
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    Arc::ptr_eq(&handles_generation, &tools.handles_generation())
                        && route_epoch == tools.route_epoch()
                };
                if latest.roots != h.roots
                    || latest.entries != h.entries
                    || latest.registers != h.registers
                    || !same_registry
                    || settings_revision != inputs.runtime.settings_revision
                    || settings_revision
                        != self
                            .lane
                            .owner
                            .settings
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .revision
                {
                    continue;
                }
                let Some(current) = latest.current.as_ref() else {
                    continue;
                };
                let EffectPlan::Tool(tool) = &plan.plan else {
                    unreachable!("routed settlement has a tool plan")
                };
                if !pi_harness_core::tool_batch::expects_effect(
                    current,
                    &self.lane.owner.session.metadata().base_metadata().id,
                    tool,
                ) {
                    continue;
                }
                let captured = self
                    .captured_tools
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(&tool.key)
                    .map(|capture| capture.arguments.clone())
                    .ok_or_else(|| self.line_fault(true, "tool execution capture absent"))?;
                if !captured_arguments_match(&latest, &captured) {
                    return Err(
                        self.line_fault(true, "saved tool arguments changed before settlement")
                    );
                }
                #[cfg(feature = "extensions")]
                self.require_routed_terminal(&tool.key)?;
                #[cfg(feature = "extensions")]
                {
                    routed_registry = Some((handles_generation, route_epoch, settings_revision));
                }
                (latest, Some(inputs), facts)
            } else {
                (h, None, None)
            };
            let current = h.current.as_ref().expect("checked current operation");
            let inputs = match routed_inputs {
                Some(inputs) => inputs,
                None => self.inputs_with_policy(&h, false).await?,
            };
            if matches!(plan.output, SettlementOutput::NotStarted { .. })
                && (matches!(plan.plan, EffectPlan::Hook(_))
                    || matches!(&current.state,OperationState::Run(run) if matches!(run.control,Control::Running(_))))
            {
                permits.release();
                retired_requests.extend(self.retire_effect(plan.plan.key()));
                return Ok(None);
            }
            if let Some(candidate) = &overflow
                && (candidate.observed.current != h.current
                    || candidate.observed.roots.configuration != h.roots.configuration
                    || candidate.observed.roots.leaf != h.roots.leaf
                    || candidate.revision
                        != self
                            .lane
                            .owner
                            .settings
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .revision)
            {
                continue;
            }
            let facts = if let Some(candidate) = &overflow {
                candidate.facts.clone()
            } else if let Some(facts) = routed_facts {
                facts
            } else {
                self.settlement_facts(&plan, inputs.now, &h, false).await?
            };
            let decision = settlement_recipe(
                current,
                &plan,
                &inputs,
                &if routed {
                    self.lane.facts_raw(&h)?
                } else {
                    self.lane.facts(&h)?
                },
                &facts,
            )
            .map_err(|_| self.line_fault(routed, "settlement recipe rejected"))?;
            #[cfg(feature = "extensions")]
            let mut routed_committed = false;
            let result = match decision {
                RecipeDecision::Suspend(result) => Some(SettlementResult {
                    current: current.clone(),
                    terminal: None,
                    dispatch: None,
                    suspend: Some(*result),
                    consume_deferred_poll: false,
                }),
                RecipeDecision::NoLongerExpected => None,
                RecipeDecision::Terminal(terminal) => Some(SettlementResult {
                    terminal: Some(terminal.result),
                    current: terminal.current,
                    dispatch: None,
                    suspend: None,
                    consume_deferred_poll: false,
                }),
                RecipeDecision::Apply(recipe) => {
                    let mut dispatch = recipe.dispatch.clone();
                    let consume_deferred_poll = pi_harness_core::deferred::consumes_poll(&recipe);
                    let lifecycle = self.lane.precommit_messages(
                        &recipe,
                        &current.operation.operation_id,
                        self.recovery,
                    );
                    if !lifecycle.is_empty() {
                        permits.release();
                        for (kind, event) in lifecycle {
                            self.events().enqueue(kind, &self.lane.lane.name, event);
                        }
                        self.events().flush();
                        permits
                            .acquire(&self.lane.owner.settings_line, &self.lane.lane.line)
                            .await?;
                    }
                    let prepared = if routed {
                        // This admitted write remains owned across the outside-line
                        // timestamp preparation, checked CAS and final observation.
                        debug_assert!(permits.commit_slot().is_none());
                        *permits.commit_slot() = Some(self.lane.owner.admission.try_admit()?);
                        permits.observation_parent(recipe.telemetry_context.clone());
                        permits.release_lines();
                        let slots = recipe
                            .transaction
                            .writes
                            .iter()
                            .enumerate()
                            .filter_map(|(write_index, write)| match write {
                                Write::Entry(entry) => {
                                    Some(pi_agent_session::storage::PreparedEntrySlot {
                                        write_index,
                                        entry: entry.clone(),
                                    })
                                }
                                _ => None,
                            })
                            .collect();
                        let stamp = match self.lane.owner.session.read_stamp().await {
                            Ok(stamp) => stamp,
                            Err(_) => {
                                *permits.observation_slot() = self
                                    .lane
                                    .owner
                                    .session
                                    .write_observation_for_preparation_error(&recipe.transaction)
                                    .ok();
                                return Err(permits.observe_fault(
                                    self.lane.owner.as_ref(),
                                    crate::restore::fault("routed publication stamp read failed"),
                                ));
                            }
                        };
                        let token =
                            match self.lane.owner.session.prepare_entries(stamp, slots).await {
                                Ok(Some(token)) => token,
                                Ok(None) => {
                                    // Stale preparation is a retry, not a failed write.
                                    permits.release();
                                    continue;
                                }
                                Err(_) => {
                                    *permits.observation_slot() = self
                                        .lane
                                        .owner
                                        .session
                                        .write_observation_for_preparation_error(
                                            &recipe.transaction,
                                        )
                                        .ok();
                                    return Err(permits.observe_fault(
                                        self.lane.owner.as_ref(),
                                        crate::restore::fault(
                                            "routed publication preparation failed",
                                        ),
                                    ));
                                }
                            };
                        permits
                            .acquire(&self.lane.owner.settings_line, &self.lane.lane.line)
                            .await?;
                        Some(token)
                    } else if let Some(candidate) = &overflow {
                        let slots = recipe
                            .transaction
                            .writes
                            .iter()
                            .enumerate()
                            .filter_map(|(write_index, w)| {
                                if let Write::Entry(entry) = w {
                                    Some(pi_agent_session::storage::PreparedEntrySlot {
                                        write_index,
                                        entry: entry.clone(),
                                    })
                                } else {
                                    None
                                }
                            })
                            .collect();
                        let token = self
                            .lane
                            .owner
                            .session
                            .prepare_entries(candidate.stamp.clone(), slots)
                            .await
                            .map_err(|_| {
                                self.line_fault(routed, "overflow entry preparation failed")
                            })?;
                        let Some(token) = token else { continue };
                        Some(token)
                    } else {
                        None
                    };
                    #[cfg(feature = "extensions")]
                    if let (EffectPlan::Tool(tool), SettlementOutput::Tool(_)) =
                        (&plan.plan, &plan.output)
                    {
                        let latest = if routed {
                            match self.lane.load_raw().await {
                                Ok(latest) => latest,
                                Err(error) => {
                                    return Err(
                                        permits.observe_fault(self.lane.owner.as_ref(), error)
                                    );
                                }
                            }
                        } else {
                            self.lane.load().await?
                        };
                        if !latest.current.as_ref().is_some_and(|current| {
                            pi_harness_core::tool_batch::expects_effect(
                                current,
                                &self.lane.owner.session.metadata().base_metadata().id,
                                tool,
                            )
                        }) {
                            continue;
                        }
                        let captured = self
                            .captured_tools
                            .lock()
                            .unwrap_or_else(|p| p.into_inner())
                            .get(&tool.key)
                            .map(|capture| capture.arguments.clone());
                        let Some(captured) = captured else {
                            let error =
                                self.line_fault(routed, "routed tool capture absent before apply");
                            return Err(if routed {
                                permits.observe_fault(self.lane.owner.as_ref(), error)
                            } else {
                                error
                            });
                        };
                        if !captured_arguments_match(&latest, &captured) {
                            let error = self.line_fault(
                                routed,
                                "saved routed tool arguments changed before apply",
                            );
                            return Err(if routed {
                                permits.observe_fault(self.lane.owner.as_ref(), error)
                            } else {
                                error
                            });
                        }
                        if let Err(error) = self.require_routed_terminal(&tool.key) {
                            return Err(if routed {
                                permits.observe_fault(self.lane.owner.as_ref(), error)
                            } else {
                                error
                            });
                        }
                        if routed {
                            let (handles_generation, route_epoch, settings_revision) =
                                routed_registry.as_ref().expect("routed registry captured");
                            let same_registry = {
                                let tools = self
                                    .lane
                                    .owner
                                    .tools
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner());
                                Arc::ptr_eq(handles_generation, &tools.handles_generation())
                                    && *route_epoch == tools.route_epoch()
                            };
                            if latest.roots != h.roots
                                || latest.entries != h.entries
                                || latest.registers != h.registers
                                || !same_registry
                                || *settings_revision
                                    != self
                                        .lane
                                        .owner
                                        .settings
                                        .lock()
                                        .unwrap_or_else(|p| p.into_inner())
                                        .revision
                            {
                                permits.release();
                                continue;
                            }
                        }
                    }
                    let applied_result = if routed {
                        let token = prepared.expect("routed commit has prepared entries");
                        let (commit, observation) = permits.commit_and_observation_slots();
                        self.lane
                            .apply_routed_prepared(*recipe, token, commit, observation)
                            .await
                    } else {
                        self.lane
                            .apply_prepared_scoped(*recipe, prepared, permits.commit_slot(), false)
                            .await
                    };
                    let Some(applied) = (match applied_result {
                        Ok(applied) => applied,
                        Err(error) if routed => {
                            return Err(permits.observe_fault(self.lane.owner.as_ref(), error));
                        }
                        Err(error) => return Err(error),
                    }) else {
                        // Keep the actual output/binding on CAS loss. The next
                        // iteration replans it against newer inbox/control roots.
                        continue;
                    };
                    #[cfg(feature = "extensions")]
                    {
                        routed_committed = matches!(
                            (&plan.plan, &plan.output),
                            (EffectPlan::Tool(_), SettlementOutput::Tool(_))
                        );
                    }
                    if consume_deferred_poll {
                        permits.release_lines();
                        self.poll_permit
                            .store(false, std::sync::atomic::Ordering::Release);
                        if let Some(current) = &applied.current {
                            self.trace.sync(current);
                            self.turn_start(current);
                        }
                        if let Some(EffectPlan::Deferred(plan)) = &mut dispatch {
                            plan.telemetry_context = self.trace.context();
                        }
                    }
                    let assistant = match &plan.output {
                        SettlementOutput::Assistant { message, .. }
                        | SettlementOutput::Deferred { message, .. } => Some(message),
                        SettlementOutput::Reconciled(output) => match output.value() {
                            ReconciledValue::Assistant(message) => Some(message.as_ref()),
                            _ => None,
                        },
                        SettlementOutput::NotStarted { .. } => match &facts {
                            SettlementFacts::NotStarted {
                                value: ReconciledValue::Assistant(message),
                            } => Some(message.as_ref()),
                            _ => None,
                        },
                        _ => None,
                    };
                    if let Some(message) = assistant {
                        let retry = applied.current.as_ref().is_some_and(|c| matches!(&c.state,OperationState::Run(r) if matches!(&r.phase,RunPhase::Assistant(AssistantPhase {generation:Generation::RetryWait {..},..}))));
                        let outcome = if retry {
                            "retry"
                        } else {
                            match message.as_message().stop_reason {
                                pi_ai_core::StopReason::Error => "failed",
                                pi_ai_core::StopReason::Aborted => "aborted",
                                pi_ai_core::StopReason::Deferred => "deferred",
                                _ => "succeeded",
                            }
                        };
                        self.trace.step_outcome(outcome);
                    }
                    if let Err(error) = self.after_settlement(current, &applied, routed).await {
                        return Err(if routed {
                            permits.observe_fault(self.lane.owner.as_ref(), error)
                        } else {
                            error
                        });
                    }
                    let candidate = self
                        .request_bindings
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(plan.plan.key());
                    if let Some(candidate) = candidate {
                        if let Some(effect @ (EffectPlan::Assistant(_) | EffectPlan::Deferred(_))) =
                            &dispatch
                        {
                            retired_requests.extend(
                                self.request_bindings
                                    .lock()
                                    .unwrap_or_else(|p| p.into_inner())
                                    .insert(effect.key().clone(), candidate),
                            );
                        } else {
                            retired_requests.push(candidate);
                        }
                    }
                    applied.current.map(|current| SettlementResult {
                        terminal: None,
                        current,
                        dispatch,
                        suspend: None,
                        consume_deferred_poll,
                    })
                }
            };
            #[cfg(feature = "extensions")]
            if routed_committed {
                let owner = self.lane.owner.clone();
                permits.arm_cleanup_fault(move || {
                    let _ = owner.observe::<()>(Err(crate::restore::fault(
                        "routed cleanup abandoned after publication",
                    )));
                });
                permits.emit_before_cleanup();
                let cleanup = self.cleanup_routed_raw(plan.plan.key(), true).await;
                if let Err(error) = cleanup {
                    permits.disarm_cleanup_fault();
                    return Err(permits.observe_fault(self.lane.owner.as_ref(), error));
                }
            } else {
                permits.release();
                self.cleanup_routed(plan.plan.key(), false).await?;
            }
            #[cfg(not(feature = "extensions"))]
            permits.release();
            retired_requests.extend(self.retire_effect(plan.plan.key()));
            drop(retired_requests);
            #[cfg(feature = "extensions")]
            if routed_committed {
                permits.disarm_cleanup_fault();
                permits.release();
            }
            self.events().flush();
            self.lane.lane.announce();
            return Ok(result);
        }
    }
    pub(crate) fn retire_effect(&self, key: &EffectKey) -> Option<RequestBinding> {
        let old = self
            .captured_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
        drop(old);
        let old = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
        drop(old);
        let retired_request = self
            .request_bindings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
        let old = self
            .lane
            .lane
            .running_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
        drop(old);
        retired_request
    }
    async fn terminal(
        &self,
        plan: TerminalCommit<Telemetry>,
    ) -> Result<Option<CurrentOperation>, HarnessRuntimeError> {
        self.wait_cancellations().await?;
        if self.retired.is_cancelled() {
            return Ok(None);
        }
        self.permission("commit_terminal").await?;
        let mut permits = SettlementPermits::new();
        permits
            .acquire(&self.lane.owner.settings_line, &self.lane.lane.line)
            .await?;
        let h = self.lane.load().await?;
        let Some(current) = &h.current else {
            return Ok(None);
        };
        let inputs = self.inputs(&h).await?;
        let prefix = format!("{}:", current.operation.operation_id.as_str());
        let mut registers = Vec::new();
        for namespace in [
            RegisterNamespace::OperationToolArguments,
            RegisterNamespace::OperationPreparation,
        ] {
            for register in self
                .lane
                .owner
                .session
                .list_registers(namespace, Some(&prefix))
                .await
                .map_err(|_| self.lane.owner.fail("terminal cleanup inventory failed"))?
            {
                let key = register_key(&register);
                registers.push(pi_harness_core::planner::RegisterCondition {
                    namespace: key.namespace,
                    key: key.key,
                    seq: Some(register_seq(&register)),
                });
            }
        }
        let mut owned_pending = BTreeSet::new();
        if let OperationState::Run(run) = &current.state {
            owned_pending.extend(
                run.inbox
                    .steer
                    .iter()
                    .chain(&run.inbox.follow_up)
                    .chain(&run.inbox.writes)
                    .map(|id| id.as_str().to_owned()),
            );
            if let Control::CancelRequested(control) = &run.control {
                owned_pending.extend(
                    control
                        .drained_steer
                        .iter()
                        .chain(&control.drained_follow_up)
                        .map(|id| id.as_str().to_owned()),
                );
            }
        }
        for ((namespace, key), register) in &h.registers {
            if *namespace == RegisterNamespace::PendingEntry && owned_pending.contains(key) {
                registers.push(pi_harness_core::planner::RegisterCondition {
                    namespace: *namespace,
                    key: key.clone(),
                    seq: Some(register_seq(register)),
                });
            }
        }
        let mut candidate = self
            .terminal_candidate
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .take();
        let decision = if let Some(c) = candidate.as_ref() {
            pi_harness_core::structural::terminal_recipe(
                current,
                &TerminalCommit {
                    current: c.current.clone(),
                    result: c.result.clone(),
                    telemetry_context: plan.telemetry_context.clone(),
                },
                &self.lane.facts(&h)?,
                &pi_harness_core::terminal::TerminalFacts { registers },
                &c.extra,
            )
        } else {
            pi_harness_core::terminal::terminal_recipe(
                current,
                &plan,
                &inputs,
                &self.lane.facts(&h)?,
                &pi_harness_core::terminal::TerminalFacts { registers },
            )
        }
        .map_err(|_| self.lane.owner.fail("terminal recipe rejected"))?;
        let result = match decision {
            RecipeDecision::Apply(recipe) => {
                let prepared = candidate.as_mut().and_then(|c| c.prepared.take());
                if self
                    .lane
                    .apply_prepared_scoped(*recipe, prepared, permits.commit_slot(), false)
                    .await?
                    .is_some()
                {
                    let result = candidate
                        .as_ref()
                        .map_or_else(|| plan.result.clone(), |c| c.result.clone());
                    *self
                        .terminal_result
                        .lock()
                        .unwrap_or_else(|p| p.into_inner()) = Some(result);
                    if let Some(c) = candidate.take() {
                        self.summary_settled(current, None, &c.source.output);
                        self.retire_effect(c.source.plan.key());
                    }
                    Some(current.clone())
                } else {
                    None
                }
            }
            RecipeDecision::NoLongerExpected => None,
            _ => return Err(self.lane.owner.fail("invalid terminal recipe")),
        };
        if let Some(candidate) = candidate {
            *self
                .terminal_candidate
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = Some(candidate);
        }
        permits.release();
        self.events().flush();
        self.lane.lane.announce();
        Ok(result)
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    Effects<Telemetry> for RuntimeEffects<C, S, M>
{
    fn commit_transition(
        &self,
        plan: TransitionCommit<Telemetry>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_runtime(Box::pin(async move { this.transition(plan).await }))
                .await
        })
    }
    fn commit_settlement(
        &self,
        plan: SettlementCommit<Telemetry>,
    ) -> BoxFuture<'_, Result<Option<SettlementResult<Telemetry>>, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_runtime(Box::pin(async move { this.settlement(plan).await }))
                .await
        })
    }
    fn commit_terminal(
        &self,
        plan: TerminalCommit<Telemetry>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_runtime(Box::pin(async move { this.terminal(plan).await }))
                .await
        })
    }
    fn run(
        &self,
        plan: EffectPlan<Telemetry>,
    ) -> BoxFuture<'_, Result<EffectOutput, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_effect(Box::pin(async move {
                    this.lane.owner.observe(this.execute(plan).await)
                }))
                .await?
        })
    }
    fn finalize_tool(
        &self,
        plan: ToolEffectPlan<Telemetry>,
        output: ToolRawOutput,
    ) -> BoxFuture<'_, Result<ToolSettlementOutput, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_effect(Box::pin(async move {
                    this.lane.owner.observe(this.finalize(plan, output).await)
                }))
                .await?
        })
    }
    fn run_summary_request(
        &self,
        plan: SummaryRequestPlan<Telemetry>,
    ) -> BoxFuture<'_, Result<SummaryRequestOutput, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_effect(Box::pin(async move { this.nested_request(plan).await }))
                .await?
        })
    }
    fn settle_summary_request(
        &self,
        plan: SummaryRequestSettlement<Telemetry>,
    ) -> BoxFuture<'_, Result<Option<CurrentOperation>, HarnessRuntimeError>> {
        let this = self.this.upgrade().expect("live Effects owner");
        Box::pin(async move {
            this.lane
                .owner
                .clone()
                .retain_runtime(Box::pin(async move { this.nested_settlement(plan).await }))
                .await
        })
    }
    fn sleep(
        &self,
        delay: Duration,
        context: Telemetry,
    ) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            if !self.ordinary_permission("sleep").await? {
                return Ok(());
            }
            let operation = self
                .lane
                .lane
                .reservation
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
                .expect("sleep owns operation");
            let span = crate::telemetry::Span::start(
                context,
                "pi.harness.sleep",
                pi_telemetry::SpanAttributes::from([
                    (
                        "pi.operation.id".into(),
                        crate::telemetry::string(operation.as_str()),
                    ),
                    (
                        "pi.sleep.delay_ms".into(),
                        crate::telemetry::number(delay.as_millis().min(u64::MAX as u128) as u64),
                    ),
                ]),
            );
            span.outcome("pi.sleep.outcome", "aborted", true);
            let mut close = Box::pin(self.lane.owner.admission.seal_signal());
            let mut sleep = self.lane.owner.clock.sleep(delay, &self.cancellation);
            let mut aborted = Box::pin(self.cancellation.cancelled());
            let result = std::future::poll_fn(|cx| {
                if close.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(Err(self
                        .lane
                        .owner
                        .admission
                        .check()
                        .err()
                        .unwrap_or_else(|| self.lane.owner.fail("timer lost owner"))));
                }
                if aborted.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(Ok(()));
                }
                sleep.as_mut().poll(cx).map(|result| match result {
                    Ok(()) => Ok(()),
                    Err(_) => {
                        self.lane.owner.admission.check()?;
                        if self.cancellation.is_cancelled() {
                            Ok(())
                        } else {
                            Err(self
                                .lane
                                .owner
                                .fail("operation timer cancelled without cancellation"))
                        }
                    }
                })
            })
            .await;
            span.outcome(
                "pi.sleep.outcome",
                if result.is_ok() && !self.cancellation.is_cancelled() {
                    "elapsed"
                } else {
                    "aborted"
                },
                result.is_err(),
            );
            result
        })
    }
}
pub(crate) struct TokenCancellation(pub CancellationToken);
impl Cancellation for TokenCancellation {
    fn is_cancelled(&self) -> bool {
        self.0.is_cancelled()
    }
    fn cancelled(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.0.cancelled().await;
        })
    }
}
struct ErrorSink(crate::events::EventHub);
impl crate::hooks::HookErrorSink for ErrorSink {
    fn report(&self, event: LaneHookHandlerErrorEvent) {
        let lane = event.lane.clone();
        self.0.enqueue(
            HarnessEventType::HandlerError,
            &lane,
            HarnessEvent::LaneHookHandlerError(event),
        );
        self.0.flush();
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) async fn context(&self) -> Result<Arc<C>, HarnessRuntimeError> {
        match &self.lane.owner.context {
            ContextSource::Static(context) => Ok(context.clone()),
            ContextSource::Provider(provider) => provider.resolve().await.map(Arc::new),
        }
    }
    pub(crate) async fn system_prompt(
        &self,
        override_prompt: Option<&str>,
    ) -> Result<String, HarnessRuntimeError> {
        if let Some(value) = override_prompt {
            return Ok(value.to_owned());
        }
        match &self.lane.owner.prompt {
            None => Ok(String::new()),
            Some(SystemPromptSource::Static(value)) => Ok(value.clone()),
            Some(SystemPromptSource::Provider(provider)) => {
                provider.resolve(self.context().await?.as_ref()).await
            }
        }
    }
    pub(crate) async fn hook(
        &self,
        event: HookEvent,
        accepted: bool,
        tool: Option<&pi_ai_core::Tool>,
    ) -> Result<crate::hooks::HookPipelineOutput, HarnessRuntimeError> {
        self.hook_with_telemetry(event, accepted, tool, self.trace.context())
            .await
    }
    pub(crate) async fn hook_with_telemetry(
        &self,
        event: HookEvent,
        accepted: bool,
        tool: Option<&pi_ai_core::Tool>,
        telemetry: Telemetry,
    ) -> Result<crate::hooks::HookPipelineOutput, HarnessRuntimeError> {
        if accepted && !matches!(event, HookEvent::BeforeResume(_)) {
            let _ = self.ordinary_permission("hook").await?;
        } else {
            self.permission("hook").await?;
        }
        if accepted {
            let run_id = match &event {
                HookEvent::BeforeRequest(e) => Some(&e.run_id),
                HookEvent::BeforePayload(e) => Some(&e.run_id),
                HookEvent::AfterResponse(e) => Some(&e.run_id),
                HookEvent::BeforeTool(e) => Some(&e.run_id),
                HookEvent::AfterTool(e) => Some(&e.run_id),
                HookEvent::TransformContext(e) => Some(&e.run_id),
                _ => None,
            };
            if let Some(run_id) = run_id {
                let _line = self.lane.lane.line.acquire().await?;
                let h = self.lane.load().await?;
                // Hydration failures remain faults. A successfully hydrated different
                // operation is only loss of this unstarted callback's authority;
                // the owning drive validates the original operation's lastResult.
                let current = h
                    .current
                    .as_ref()
                    .filter(|current| &current.operation.operation_id == run_id);
                let cancelled = current.is_some_and(pi_harness_core::structural::is_cancelled);
                if current.is_none() || cancelled {
                    // Cancellation can invoke stream fallback callbacks: release
                    // the mutation line before cancelling only this old drive.
                    drop(_line);
                    self.cancellation.cancel();
                    let result = match event {
                        HookEvent::BeforeRequest(_) => HookResult::BeforeRequest(None),
                        HookEvent::BeforePayload(_) => HookResult::BeforePayload(None),
                        HookEvent::AfterResponse(_) => HookResult::AfterResponse(None),
                        HookEvent::BeforeTool(_) => HookResult::BeforeTool(None),
                        // This serialized boundary proves the hook never started. Keep the
                        // raw output, but do not let an earlier run_after=true preserve
                        // its termination request. Started hooks never take this branch.
                        HookEvent::AfterTool(_) => HookResult::AfterTool(Some(AfterToolResult {
                            terminate: Some(false),
                            ..Default::default()
                        })),
                        HookEvent::TransformContext(_) => HookResult::TransformContext(None),
                        HookEvent::BeforeResume(_) => HookResult::BeforeResume,
                        _ => return Err(self.lane.owner.fail("invalid skipped hook family")),
                    };
                    return crate::hooks::HookPipelineOutput::from_result(result.name(), result);
                }
            }
        }
        self.started_hook_with_telemetry(event, accepted, tool, telemetry)
            .await
    }
    async fn started_hook_with_telemetry(
        &self,
        event: HookEvent,
        accepted: bool,
        tool: Option<&pi_ai_core::Tool>,
        telemetry: Telemetry,
    ) -> Result<crate::hooks::HookPipelineOutput, HarnessRuntimeError> {
        self.lane.owner.admission.check()?;
        let saved = match &event {
            HookEvent::BeforeResume(BeforeResumeEvent::Run(e)) => match &e.resume_data {
                pi_ai_core::Presence::Value(pi_ai_core::JsonValue::Object(data)) => data.clone(),
                _ => Default::default(),
            },
            _ => Default::default(),
        };
        let cancellation = TokenCancellation(self.cancellation.clone());
        let codec = self.lane.owner.session.codec();
        let context = crate::hooks::StartedHookContext {
            codec,
            header_policy: codec.header_policy(),
            tool,
            cancellation: &cancellation,
            telemetry: telemetry.as_ref(),
            operation_accepted: accepted,
            recovery: self.recovery,
            resume_data: &saved,
            errors: &ErrorSink(self.events().clone()),
        };
        let captured = self.lane.owner.hooks.capture(event.name());
        let mut invocation = Box::pin(captured.run_started(event, &context));
        let mut close = Box::pin(self.lane.owner.admission.seal_signal());
        std::future::poll_fn(|cx| {
            if close.as_mut().poll(cx).is_ready() {
                return std::task::Poll::Ready(Err(self
                    .lane
                    .owner
                    .admission
                    .check()
                    .err()
                    .unwrap_or_else(|| self.lane.owner.fail("hook lost owner"))));
            }
            invocation.as_mut().poll(cx)
        })
        .await
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    async fn settlement_facts(
        &self,
        commit: &SettlementCommit<Telemetry>,
        now: SafeUnsigned,
        h: &crate::restore::HydratedLane,
        raw: bool,
    ) -> Result<SettlementFacts, HarnessRuntimeError> {
        Ok(match &commit.output {
            SettlementOutput::RequestOptions(_) => SettlementFacts::Request {
                response_entry_id: EntryId::new(self.lane.owner.ids.next(None))
                    .map_err(|_| self.line_fault(raw, "response identity failed"))?,
                usage_id: UsageId::new(self.lane.owner.ids.next(None))
                    .map_err(|_| self.line_fault(raw, "usage identity failed"))?,
            },
            SettlementOutput::Assistant { message, .. }
            | SettlementOutput::Deferred { message, .. } => {
                let message_raw = message.as_message();
                let mut followers = Vec::new();
                for (index, content) in message_raw.content.iter().enumerate() {
                    if matches!(content, pi_ai_core::AssistantContent::ToolCall(_)) {
                        followers.push((
                            SafeUnsigned::new(index as u64).map_err(|_| {
                                self.line_fault(raw, "tool source index range failure")
                            })?,
                            EntryId::new(self.lane.owner.ids.next(Some(message_raw.timestamp)))
                                .map_err(|_| self.line_fault(raw, "tool result identity failed"))?,
                        ));
                    }
                }
                if matches!(commit.plan, EffectPlan::Deferred(_)) {
                    SettlementFacts::Deferred {
                        overflow: {
                            let budget = self
                                .request_bindings
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .get(commit.plan.key())
                                .map(|b| b.model.max_tokens);
                            (message_raw.stop_reason == pi_ai_core::StopReason::Error
                                && pi_ai_core::is_context_overflow(message_raw, None))
                                || budget.is_some_and(|limit| {
                                    message_raw.stop_reason == pi_ai_core::StopReason::Length
                                        && message_raw.usage.output < limit
                                })
                        },
                        followers,
                    }
                } else {
                    SettlementFacts::Assistant {
                        retryable: pi_ai_core::is_retryable_assistant_error(message_raw),
                        overflow: false,
                        followers,
                    }
                }
            }
            SettlementOutput::Hook(output) => match output.result() {
                HookResult::BeforeRunEnd(result) => {
                    let follow_up = if let Some(text) =
                        result.as_ref().and_then(|r| r.follow_up.as_ref())
                    {
                        Some(NewMessageEntry {
                            id: EntryId::new(self.lane.owner.ids.next(None))
                                .map_err(|_| self.line_fault(raw, "follow-up identity failed"))?,
                            parent_id: commit.current.leaf_id.clone(),
                            entry_type: MessageEntryType::Message,
                            message: AgentMessage::BuiltIn(pi_ai_core::Message::User(
                                pi_ai_core::UserMessage {
                                    role: Default::default(),
                                    content: pi_ai_core::UserContent::Text(text.clone()),
                                    timestamp: SafeUnsigned::new(
                                        self.lane.owner.clock.now_unix_ms(),
                                    )
                                    .map_err(|_| {
                                        self.line_fault(raw, "follow-up clock range failure")
                                    })?,
                                },
                            )),
                            terminate: None,
                        })
                    } else {
                        None
                    };
                    SettlementFacts::RunEnd { follow_up }
                }
                _ => {
                    return Err(self
                        .lane
                        .owner
                        .fail("hook settlement requires a later runtime stage"));
                }
            },
            SettlementOutput::Tool(output) => SettlementFacts::Tool {
                usage_id: output
                    .result
                    .usage
                    .as_ref()
                    .map(|_| UsageId::new(self.lane.owner.ids.next(None)))
                    .transpose()
                    .map_err(|_| self.line_fault(raw, "tool usage identity failed"))?,
                declarations: self.declarations_with_policy(&commit.current, raw).await?,
            },
            SettlementOutput::ToolClearance(output) => {
                let usage = match output.outcome() {
                    ToolClearanceOutcome::Immediate { result, .. } => result.usage.as_ref(),
                    _ => None,
                };
                SettlementFacts::Tool {
                    usage_id: usage
                        .map(|_| UsageId::new(self.lane.owner.ids.next(None)))
                        .transpose()
                        .map_err(|_| self.line_fault(raw, "tool usage identity failed"))?,
                    declarations: self.declarations_with_policy(&commit.current, raw).await?,
                }
            }
            SettlementOutput::Reconciled(output) => match output.value() {
                ReconciledValue::Assistant(_) => SettlementFacts::Assistant {
                    retryable: false,
                    overflow: false,
                    followers: vec![],
                },
                ReconciledValue::Tool { result, .. } => SettlementFacts::Tool {
                    usage_id: result
                        .usage
                        .as_ref()
                        .map(|_| UsageId::new(self.lane.owner.ids.next(None)))
                        .transpose()
                        .map_err(|_| self.line_fault(raw, "tool usage identity failed"))?,
                    declarations: self.declarations_with_policy(&commit.current, raw).await?,
                },
            },
            SettlementOutput::NotStarted { .. } => match &commit.plan {
                EffectPlan::Tool(_) | EffectPlan::ToolClearance(_) => SettlementFacts::NotStarted {
                    value: ReconciledValue::Tool {
                        result: pi_agent_loop::tool_error("Operation aborted"),
                        is_error: true,
                        terminate: false,
                    },
                },
                EffectPlan::Deferred(plan) => SettlementFacts::NotStarted {
                    value: ReconciledValue::Assistant(Box::new(self.synthetic_deferred(
                        &plan.deferred,
                        now,
                        self.deferred_source_api(h, &plan.deferred.source_entry_id)?,
                    )?)),
                },
                EffectPlan::Assistant(plan) => SettlementFacts::NotStarted {
                    value: ReconciledValue::Assistant(Box::new(self.synthetic_generation(
                        &plan.key,
                        &plan.generation.context,
                        now,
                        true,
                    )?)),
                },
                _ => {
                    return Err(self
                        .lane
                        .owner
                        .fail("cancelled settlement requires a later stage"));
                }
            },
            _ => {
                return Err(self
                    .lane
                    .owner
                    .fail("settlement requires a later runtime stage"));
            }
        })
    }
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) async fn begin(
        &self,
        plan: &EffectPlan<Telemetry>,
    ) -> Result<bool, HarnessRuntimeError> {
        if !self.ordinary_permission("effect_start").await? {
            return Ok(false);
        }
        // Provider model snapshots are host callbacks, so observe them before locks.
        let candidate = if let EffectPlan::Hook(HookEffectPlan {
            event: HookEvent::BeforeRequest(event),
            ..
        }) = plan
        {
            let Some(before) = self
                .lane
                .owner
                .models
                .provider_registration(&event.model.provider)
            else {
                return Ok(false);
            };
            let Some(model) = self
                .lane
                .owner
                .models
                .get_model(&event.model.provider, &event.model.id)
            else {
                return Ok(false);
            };
            let Some(after) = self
                .lane
                .owner
                .models
                .provider_registration(&event.model.provider)
            else {
                return Ok(false);
            };
            if model != event.model || !before.same_registration(&after) {
                return Ok(false);
            }
            Some(RequestBinding {
                model,
                registration: before,
            })
        } else {
            None
        };
        // Declared before guards: even early returns drop these after the lane unlocks.
        let mut observed_registration = None;
        let mut displaced_request = None;
        let routed = matches!(plan, EffectPlan::Tool(_));
        let mut pair = None;
        let mut settings = None;
        let mut line = None;
        if routed {
            pair = Some(
                self.lane
                    .owner
                    .settings_line
                    .acquire_pair(&self.lane.lane.line)
                    .await?,
            );
        } else {
            settings = Some(self.lane.owner.settings_line.acquire().await?);
            line = Some(self.lane.lane.line.acquire().await?);
        }
        let h = if routed {
            self.lane.load_raw().await?
        } else {
            self.lane.load().await?
        };
        let Some(current) = h.current.as_ref() else {
            return Ok(false);
        };
        let key = plan.key();
        if key.session_id != self.lane.owner.session.metadata().base_metadata().id
            || key.lane != self.lane.lane.name
            || key.operation_id != current.operation.operation_id
        {
            return Ok(false);
        }
        if pi_harness_core::structural::is_cancelled(current) {
            drop(line);
            drop(settings);
            drop(pair);
            self.cancellation.cancel();
            return Ok(false);
        }
        let expected = if pi_harness_core::structural::decision(current).is_some() {
            match plan {
                EffectPlan::Summary(p) => {
                    pi_harness_core::structural::matches(current, p, &key.session_id)
                }
                EffectPlan::Hook(p) => {
                    let inputs = self.inputs(&h).await?;
                    pi_harness_core::structural::decision_event(current, &inputs)
                        .is_ok_and(|event| event == p.event)
                        && pi_harness_core::structural::task(current)
                            .is_ok_and(|task| task.as_str() == key.step_or_task_id.as_str())
                }
                _ => false,
            }
        } else if let OperationState::Run(run) = &current.state {
            match (plan,&run.phase) {
            (EffectPlan::Deferred(plan),RunPhase::Deferred(DeferredPhase{deferred,..}))=>PendingDeferred::try_from(deferred.clone()).is_ok_and(|p|p==plan.deferred),
            (EffectPlan::Hook(plan),RunPhase::Deferred(DeferredPhase{deferred,..}))=>matches!(plan.event,HookEvent::BeforeRequest(_)) && match deferred {Deferred::Suspended{step_id,poll,..}=>key.step_or_task_id.as_str()==step_id.as_str() && key.logical_attempt.get()==poll.get().saturating_add(1),Deferred::EffectPending{step_id,poll,..}=>key.step_or_task_id.as_str()==step_id.as_str() && key.logical_attempt.get()==poll.get()},
            (EffectPlan::Assistant(plan),RunPhase::Assistant(AssistantPhase {generation,..}))=>PendingGeneration::try_from(generation.clone()).is_ok_and(|pending|pending==plan.generation),
            (EffectPlan::Hook(plan),RunPhase::Assistant(AssistantPhase {generation:Generation::Ready {context,next_attempt},..}))=>matches!(plan.event,HookEvent::BeforeRequest(_))&&key.step_or_task_id.as_str()==context.step_id.as_str()&&key.logical_attempt.get()==next_attempt.get()&&key.kind==EffectKind::Hook,
            (EffectPlan::Hook(plan),RunPhase::Checkpoint(checkpoint))=>matches!(plan.event,HookEvent::BeforeRunEnd(_))&&key.step_or_task_id.as_str()==checkpoint.trigger_entry_id.as_str()&&matches!(checkpoint.continuation,Continuation::MayFinish {..}),
            (EffectPlan::Tool(plan),RunPhase::Tools(_))=>pi_harness_core::tool_batch::expects_effect(current,&key.session_id,plan),
            (EffectPlan::ToolClearance(plan),RunPhase::Tools(tools))=>plan.assistant_entry_id==tools.batch.assistant_entry_id&&plan.turn_id==tools.batch.turn_id&&tools.batch.calls.iter().any(|call|matches!(call,ToolCallState::Planned {source_index,result_entry_id} if *source_index==plan.source_index&&*result_entry_id==plan.result_entry_id)),
            _=>false,
        }
        } else {
            false
        };
        if !expected {
            return Ok(false);
        }
        if let Some(candidate) = &candidate {
            observed_registration = self
                .lane
                .owner
                .models
                .provider_registration(&candidate.model.provider);
            if observed_registration
                .as_ref()
                .is_none_or(|current| !current.same_registration(&candidate.registration))
            {
                return Ok(false);
            }
            displaced_request = self
                .request_bindings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(key.clone(), candidate.clone());
        }
        let previous = self
            .running
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key.clone(), plan.clone());
        if previous.is_some() {
            return Err(if routed {
                crate::restore::fault("duplicate actual effect start")
            } else {
                self.lane.owner.fail("duplicate actual effect start")
            });
        }
        drop(previous);
        let signal = self
            .start_signals
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(key);
        drop(line);
        drop(settings);
        drop(pair);
        drop(observed_registration);
        drop(displaced_request);
        if let Some(signal) = signal {
            signal.cancel();
        }
        Ok(true)
    }
    pub(crate) async fn execute(
        self: &Arc<Self>,
        plan: EffectPlan<Telemetry>,
    ) -> Result<EffectOutput, HarnessRuntimeError> {
        if !self.begin(&plan).await? {
            return Ok(EffectOutput::NotStarted {
                key: plan.key().clone(),
            });
        }
        match plan {
            EffectPlan::Hook(plan) => {
                let output = self
                    .started_hook_with_telemetry(
                        plan.event.clone(),
                        true,
                        None,
                        plan.telemetry_context.clone(),
                    )
                    .await?;
                if let Some(options) = output.request_options() {
                    Ok(EffectOutput::RequestOptions(
                        RequestOptionsOutput::new(&plan, options.clone()).map_err(|_| {
                            self.lane.owner.fail("request options receipt mismatch")
                        })?,
                    ))
                } else {
                    Ok(EffectOutput::Hook(
                        HookEffectOutput::new(
                            &plan,
                            output
                                .result()
                                .cloned()
                                .ok_or_else(|| self.lane.owner.fail("hook result missing"))?,
                        )
                        .map_err(|_| self.lane.owner.fail("hook result mismatch"))?,
                    ))
                }
            }
            EffectPlan::Assistant(plan) => self.request(plan).await,
            EffectPlan::Deferred(plan) => self.fetch(plan).await,
            EffectPlan::ToolClearance(plan) => self.clearance(plan).await,
            EffectPlan::Tool(plan) => self.execute_tool(plan).await,
            EffectPlan::Summary(plan) => {
                self.summary_retry_started(&plan);
                self.events().flush();
                self.summary(plan).await
            }
            EffectPlan::CancelDeferred(_) => Err(self
                .lane
                .owner
                .fail("cancel request must use its dedicated owner")),
        }
    }
}

#[derive(Clone)]
pub(crate) struct RequestBinding {
    pub model: pi_ai_core::Model,
    pub registration: pi_ai_core::ProviderRegistration,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) fn turn_start(&self, current: &CurrentOperation) {
        let turn = match &current.state {
            OperationState::Run(run) => match &run.phase {
                RunPhase::Assistant(AssistantPhase { generation, .. }) => Some(match generation {
                    Generation::Ready { context, .. }
                    | Generation::EffectPending { context, .. }
                    | Generation::RetryWait { context, .. } => context.step_id.as_str().to_owned(),
                }),
                RunPhase::Tools(tools) => Some(tools.batch.turn_id.as_str().to_owned()),
                RunPhase::Deferred(DeferredPhase {
                    deferred: Deferred::EffectPending { step_id, poll, .. },
                    ..
                }) => Some(format!("{}:poll:{}", step_id.as_str(), poll.get())),
                _ => None,
            },
            _ => None,
        };
        if let Some(turn) = turn {
            self.start_turn(&current.operation.operation_id, turn);
        }
    }
    pub(crate) fn start_turn(&self, operation: &OperationId, turn: String) {
        if !self
            .started_turns
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(turn.clone())
        {
            return;
        }
        self.events().enqueue(
            HarnessEventType::TurnStart,
            &self.lane.lane.name,
            HarnessEvent::TurnStart(TurnStartEvent {
                event_type: Default::default(),
                lane: self.lane.lane.name.clone(),
                recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                run_id: operation.clone(),
                turn_id: turn,
            }),
        );
    }
    pub(crate) fn publish_suspension(&self, current: &CurrentOperation, result: &OperationResult) {
        let missing = match result {
            OperationResult::Run(outcome) => match outcome.as_ref() {
                RunOutcome::MissingIdentities(o) => Some(o.missing.clone()),
                _ => None,
            },
            OperationResult::Compaction(CompactionOutcome::MissingIdentities(o)) => {
                Some(o.missing.clone())
            }
            OperationResult::Navigation(NavigationOutcome::MissingIdentities(o)) => {
                Some(o.missing.clone())
            }
            _ => None,
        };
        if let Some(missing) = missing {
            self.lane
                .owner
                .suspensions
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(
                    self.lane.lane.name.clone(),
                    SuspensionObservation {
                        operation_id: current.operation.operation_id.clone(),
                        summary: pi_harness_core::structural::pending(current),
                        missing,
                    },
                );
        }
        let event = match result {
            OperationResult::Run(outcome) => match outcome.as_ref() {
                RunOutcome::Deferred(outcome) => {
                    Some(HarnessEvent::RunSuspendDeferred(RunSuspendDeferredEvent {
                        event_type: Default::default(),
                        lane: self.lane.lane.name.clone(),
                        recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                        run_id: current.operation.operation_id.clone(),
                        reason: Default::default(),
                        deferred: outcome.deferred.clone(),
                    }))
                }
                RunOutcome::MissingIdentities(outcome) => {
                    Some(HarnessEvent::RunSuspendMissing(RunSuspendMissingEvent {
                        event_type: Default::default(),
                        lane: self.lane.lane.name.clone(),
                        recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                        run_id: current.operation.operation_id.clone(),
                        reason: Default::default(),
                        missing: outcome.missing.clone(),
                    }))
                }
                _ => None,
            },
            _ => None,
        };
        if let Some(event) = event {
            self.events()
                .enqueue(HarnessEventType::RunSuspend, &self.lane.lane.name, event);
        }
    }
    async fn after_settlement(
        &self,
        previous: &CurrentOperation,
        applied: &crate::restore::HydratedLane,
        raw: bool,
    ) -> Result<(), HarnessRuntimeError> {
        let Some(current) = &applied.current else {
            return Ok(());
        };
        let (OperationState::Run(before), OperationState::Run(after)) =
            (&previous.state, &current.state)
        else {
            return Ok(());
        };
        if let RunPhase::Assistant(AssistantPhase {
            generation:
                Generation::RetryWait {
                    context,
                    next_attempt,
                    not_before,
                    error_message,
                },
            ..
        }) = &after.phase
        {
            self.events().enqueue(
                HarnessEventType::RetryScheduled,
                &self.lane.lane.name,
                HarnessEvent::RetryScheduled(RetryScheduledEvent {
                    event_type: Default::default(),
                    lane: self.lane.lane.name.clone(),
                    recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: current.operation.operation_id.clone(),
                    step: context.step_id.as_str().into(),
                    attempt: *next_attempt,
                    max_attempts: context.retry_policy.max_attempts,
                    delay_ms: SafeUnsigned::new(
                        not_before
                            .get()
                            .saturating_sub(self.lane.owner.clock.now_unix_ms()),
                    )
                    .map_err(|_| self.line_fault(raw, "retry delay range failure"))?,
                    error_message: error_message.clone(),
                }),
            );
        }
        if matches!(
            after.phase,
            RunPhase::Assistant(_)
                | RunPhase::Tools(_)
                | RunPhase::Deferred(DeferredPhase {
                    deferred: Deferred::EffectPending { .. },
                    ..
                })
        ) {
            return Ok(());
        }
        let (turn, assistant_id, result_ids) = match &before.phase {
            RunPhase::Assistant(AssistantPhase {
                generation:
                    Generation::EffectPending {
                        context,
                        response_entry_id,
                        ..
                    },
                ..
            }) => (context.step_id.clone(), response_entry_id.clone(), vec![]),
            RunPhase::Deferred(DeferredPhase {
                deferred:
                    Deferred::EffectPending {
                        step_id,
                        poll,
                        response_entry_id,
                        ..
                    },
                ..
            }) => (
                StepId::new(format!("{}:poll:{}", step_id.as_str(), poll.get()))
                    .map_err(|_| self.line_fault(raw, "poll turn invalid"))?,
                response_entry_id.clone(),
                vec![],
            ),
            RunPhase::Tools(tools) => (
                tools.batch.turn_id.clone(),
                tools.batch.assistant_entry_id.clone(),
                tools
                    .batch
                    .calls
                    .iter()
                    .map(|call| pi_harness_core::tool_batch::source(call).1.clone())
                    .collect::<Vec<_>>(),
            ),
            _ => return Ok(()),
        };
        let ids = std::iter::once(assistant_id.clone())
            .chain(result_ids.iter().cloned())
            .collect::<Vec<_>>();
        let entries = self
            .lane
            .owner
            .session
            .get_entries(&ids)
            .await
            .map_err(|_| self.line_fault(raw, "turn closure hydration failed"))?;
        let Some(Entry::Message(entry)) = entries.get(&assistant_id) else {
            return Err(self.line_fault(raw, "turn assistant is absent"));
        };
        let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(message)) = &entry.message else {
            return Err(self.line_fault(raw, "turn assistant has wrong kind"));
        };
        let message = SettledAssistantMessage::new(*message.clone())
            .map_err(|_| self.line_fault(raw, "turn assistant is unsettled"))?;
        let tool_results = result_ids
            .iter()
            .map(|id| match entries.get(id) {
                Some(Entry::Message(entry)) => match &entry.message {
                    AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(result)) => {
                        Ok(*result.clone())
                    }
                    _ => Err(self.line_fault(raw, "turn tool result has wrong kind")),
                },
                _ => Err(self.line_fault(raw, "turn tool result absent")),
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.events().enqueue(
            HarnessEventType::TurnEnd,
            &self.lane.lane.name,
            HarnessEvent::TurnEnd(Box::new(TurnEndEvent {
                event_type: Default::default(),
                lane: self.lane.lane.name.clone(),
                recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                run_id: current.operation.operation_id.clone(),
                turn_id: turn.as_str().into(),
                message,
                tool_results,
            })),
        );
        Ok(())
    }
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub(crate) fn events(&self) -> crate::events::EventHub {
        self.lane.owner.events.with_context(self.trace.context())
    }
}
