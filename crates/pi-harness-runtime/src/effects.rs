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
    origin_fence: Option<Arc<dyn crate::tools::OriginFence>>,
}

impl SettlementPermits {
    fn new() -> Self {
        Self {
            pair: None,
            commit: None,
            observation: None,
            observation_parent: None,
            pending_cleanup_fault: None,
            origin_fence: None,
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
    fn arm_origin(&mut self, fence: Arc<dyn crate::tools::OriginFence>) {
        self.origin_fence = Some(fence);
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
        let origin = self.origin_fence.take();
        if let Some(origin) = &origin {
            origin.begin_failure(false);
        }
        Self::attempt(&mut first, || self.release_lines());
        let mut observe = Some(observe);
        if origin.is_some() {
            Self::attempt(&mut first, observe.take().expect("original fault observer"));
            if let Some(origin) = &origin {
                Self::attempt(&mut first, || origin.finish_failure());
            }
        }
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
        if let Some(observe) = observe {
            Self::attempt(&mut first, observe);
        }
        Self::attempt(&mut first, || drop(self.commit.take()));
        Self::attempt(&mut first, || drop(origin));
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
        let origin = self.origin_fence.take();
        self.finish_with(|| {});
        drop(origin);
    }
}

impl Drop for SettlementPermits {
    fn drop(&mut self) {
        if let Some(origin) = self.origin_fence.take() {
            origin.begin_failure(false);
            let _ = catch_unwind(AssertUnwindSafe(|| self.release_lines()));
            let observation = self.observation.take();
            let parent = self.observation_parent.take();
            let commit = self.commit.take();
            let cleanup = self.pending_cleanup_fault.take();
            origin.defer_release(Box::new(move || {
                let mut first = None;
                if let Some(observation) = observation {
                    Self::attempt(&mut first, || {
                        observation.emit(parent.expect("earned observation parent").as_ref())
                    });
                }
                if let Some(cleanup) = cleanup {
                    Self::attempt(&mut first, cleanup);
                }
                Self::attempt(&mut first, || drop(commit));
            }));
            return;
        }
        self.finish_with(|| {});
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
    #[cfg(feature = "extensions")]
    pub(crate) profile_receipts: crate::extensions::profiles::custody::Receipts,
    #[cfg(feature = "extensions")]
    pub(crate) profile_resources:
        Option<Arc<crate::extensions::profiles::resources::ResourceRevision>>,
    pub start_signals: Mutex<BTreeMap<EffectKey, CancellationToken>>,
    pub context_line: crate::scheduler::MutationLine,
    pub contexts: Mutex<BTreeMap<StepId, Arc<C>>>,
    pub bindings: Mutex<BTreeMap<(StepId, SafeUnsigned), AgentHarnessToolHandle<C>>>,
    pub(crate) selected_tools: Mutex<crate::tools::SelectedTools<C>>,
    pub(crate) origin_consumers: Mutex<BTreeMap<EffectKey, Arc<dyn crate::tools::OriginFence>>>,
    pub(crate) identity: Arc<()>,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    pub fn new(lane: RuntimeLane<C, S, M>, recovery: bool) -> Arc<Self> {
        #[cfg(feature = "extensions")]
        let captured_resources = if recovery {
            None
        } else {
            lane.owner
                .settings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .profile_resources
                .clone()
        };
        let fx = Arc::new_cyclic(|this| Self {
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
            selected_tools: Mutex::new(BTreeMap::new()),
            origin_consumers: Mutex::new(BTreeMap::new()),
            identity: Arc::new(()),
            lane,
            running: Mutex::new(BTreeMap::new()),
            started_turns: Mutex::new(BTreeSet::new()),
            start_signals: Mutex::new(BTreeMap::new()),
            request_bindings: Mutex::new(BTreeMap::new()),
            #[cfg(feature = "extensions")]
            profile_receipts: Default::default(),
            #[cfg(feature = "extensions")]
            profile_resources: captured_resources,
            executed_tools: Mutex::new(BTreeSet::new()),
            captured_tools: Mutex::new(BTreeMap::new()),
            #[cfg(feature = "extensions")]
            routed_tools: Mutex::new(BTreeMap::new()),
            cancellation: CancellationToken::new(),
            retired: CancellationToken::new(),
            inbox_preparation: Mutex::new(None),
            poll_permit: std::sync::atomic::AtomicBool::new(false),
            cancel_targets: Mutex::new(BTreeMap::new()),
        });
        #[cfg(test)]
        origin_runtime_tests::observe(&fx);
        fx
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
        let publication = self.arm_tool_publication(&plan)?;
        let consumer = self
            .origin_consumers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(plan.plan.key())
            .cloned();
        let mut consumer_ownership = consumer
            .as_ref()
            .map(|lease| crate::tools::OriginOwnership::new(lease.clone()));
        let result = self.settlement_inner(plan, &publication, &consumer).await;
        let result = if publication.is_some() || consumer.is_some() {
            self.lane.owner.observe(result)
        } else {
            result
        };
        if result.is_ok()
            && let Some(ownership) = &mut consumer_ownership
        {
            ownership.complete();
        }
        result
    }
    async fn settlement_inner(
        &self,
        plan: SettlementCommit<Telemetry>,
        publication: &Option<(
            Arc<crate::tools::ToolPublication<C>>,
            crate::tools::OriginOwnership,
        )>,
        consumer: &Option<Arc<dyn crate::tools::OriginFence>>,
    ) -> Result<Option<SettlementResult<Telemetry>>, HarnessRuntimeError> {
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
        let known_tool = if matches!(plan.plan, EffectPlan::Tool(_)) {
            self.lane
                .owner
                .tool_publications
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(plan.plan.key())
                .cloned()
        } else {
            None
        };
        let protected = routed || publication.is_some() || known_tool.is_some();
        loop {
            let overflow = self.overflow_facts(&plan).await?;
            let mut retired_requests = Vec::new();
            let mut origin_release = crate::tools::OriginRelease::default();
            let mut permits = SettlementPermits::new();
            if let Some((_, ownership)) = &publication {
                permits.arm_origin(ownership.0.clone());
            } else if let Some(consumer) = consumer {
                permits.arm_origin(consumer.clone());
            }
            permits
                .acquire(&self.lane.owner.settings_line, &self.lane.lane.line)
                .await?;
            let h = if protected {
                self.lane.load_raw().await?
            } else {
                self.lane.load().await?
            };
            let Some(current) = &h.current else {
                if let Some((cell, ownership)) = &publication {
                    ownership.0.disarm();
                    origin_release.append(self.retire_tool_publication(&cell.plan.key, true));
                }
                origin_release.append(self.retire_lost_publication(&h, plan.plan.key()));
                permits.release();
                #[cfg(feature = "extensions")]
                self.cleanup_routed(plan.plan.key(), false).await?;
                retired_requests.extend(self.retire_effect(plan.plan.key()));
                return Ok(None);
            };
            if current.operation != plan.current.operation
                || matches!(&plan.plan,EffectPlan::Deferred(p) if pi_harness_core::deferred::plan(current,&plan.plan.key().session_id,&plan.telemetry_context).map_or(true,|expected|expected.deferred!=p.deferred || expected.key!=p.key))
            {
                if let Some((cell, ownership)) = &publication {
                    ownership.0.disarm();
                    origin_release.append(self.retire_tool_publication(&cell.plan.key, true));
                }
                origin_release.append(self.retire_lost_publication(&h, plan.plan.key()));
                permits.release();
                #[cfg(feature = "extensions")]
                self.cleanup_routed(plan.plan.key(), false).await?;
                retired_requests.extend(self.retire_effect(plan.plan.key()));
                return Ok(None);
            }
            if let Some(cell) = &known_tool
                && crate::tools::publication_arguments(
                    cell,
                    &h,
                    &self.lane.owner.session.metadata().base_metadata().id,
                )?
                .is_none()
            {
                origin_release.append(self.retire_lost_publication(&h, plan.plan.key()));
                permits.release();
                return Ok(None);
            }
            if let (EffectPlan::Tool(tool), SettlementOutput::Tool(_)) = (&plan.plan, &plan.output)
            {
                if !pi_harness_core::tool_batch::expects_effect(
                    current,
                    &self.lane.owner.session.metadata().base_metadata().id,
                    tool,
                ) {
                    origin_release.append(self.retire_lost_publication(&h, plan.plan.key()));
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
                    permits.release();
                    continue;
                }
                let Some(current) = latest.current.as_ref() else {
                    permits.release();
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
                    permits.release();
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
                None => self.inputs_with_policy(&h, protected).await?,
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
                permits.release();
                continue;
            }
            let facts = if let Some(candidate) = &overflow {
                candidate.facts.clone()
            } else if let Some(facts) = routed_facts {
                facts
            } else {
                self.settlement_facts(&plan, inputs.now, &h, protected)
                    .await?
            };
            let decision = settlement_recipe(
                current,
                &plan,
                &inputs,
                &if protected {
                    self.lane.facts_raw(&h)?
                } else {
                    self.lane.facts(&h)?
                },
                &facts,
            )
            .map_err(|_| self.line_fault(protected, "settlement recipe rejected"))?;
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
                            permits.release();
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
                                permits.release();
                                continue;
                            }
                        }
                    }
                    let applied_result = if let Some((cell, _)) = &publication {
                        self.lane
                            .apply_tool_publication(*recipe, prepared, permits.commit_slot(), cell)
                            .await
                    } else if routed {
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
                        Err(error) if protected => {
                            return Err(permits.observe_fault(self.lane.owner.as_ref(), error));
                        }
                        Err(error) => return Err(error),
                    }) else {
                        // Keep the actual output/binding on CAS loss. The next
                        // iteration replans it against newer inbox/control roots.
                        permits.release();
                        permits.release();
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
                    if let Err(error) = self.after_settlement(current, &applied, protected).await {
                        return Err(if protected {
                            permits.observe_fault(self.lane.owner.as_ref(), error)
                        } else {
                            error
                        });
                    }
                    if let Some((cell, ownership)) = publication {
                        if !self.publish_tool_selection(cell, &applied)? {
                            origin_release
                                .append(self.retire_tool_publication(&cell.plan.key, true));
                        }
                        ownership.0.disarm();
                    }
                    origin_release.append(self.retire_lost_publication(&applied, plan.plan.key()));
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
            if let Some((cell, ownership)) = &publication {
                if result.is_none() && !self.publish_tool_selection(cell, &h)? {
                    origin_release.append(self.retire_tool_publication(&cell.plan.key, true));
                }
                // H NoLongerExpected does not prove K loss: the helper above
                // keeps a still-pending K protected and requires our Applied receipt.
                ownership.0.disarm();
            }
            if result.is_none() {
                origin_release.append(self.retire_lost_publication(&h, plan.plan.key()));
            }
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
            #[cfg(feature = "extensions")]
            let terminal_pending = result
                .as_ref()
                .is_some_and(|result| result.terminal.is_some());
            #[cfg(feature = "extensions")]
            if terminal_pending {
                self.profile_receipts.invalidate(plan.plan.key());
                retired_requests.extend(self.retire_effect_state(plan.plan.key()));
            } else {
                if result.is_some() {
                    self.profile_receipts.settled(plan.plan.key());
                }
                retired_requests.extend(self.retire_effect(plan.plan.key()));
            }
            #[cfg(not(feature = "extensions"))]
            retired_requests.extend(self.retire_effect(plan.plan.key()));
            drop(retired_requests);
            #[cfg(feature = "extensions")]
            if routed_committed {
                permits.disarm_cleanup_fault();
                permits.release();
            }
            self.events().flush();
            self.lane.lane.announce();
            if let Some((cell, _)) = &publication {
                cell.publication_done.cancel();
            }
            drop(origin_release);
            return Ok(result);
        }
    }
    pub(crate) fn retire_effect(&self, key: &EffectKey) -> Option<RequestBinding> {
        #[cfg(feature = "extensions")]
        self.profile_receipts.retire(key);
        self.retire_effect_state(key)
    }
    fn retire_effect_state(&self, key: &EffectKey) -> Option<RequestBinding> {
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
        #[cfg(feature = "extensions")]
        self.profile_receipts
            .check_operation(&current.operation.operation_id)?;
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
        #[cfg(feature = "extensions")]
        if result.is_some() {
            self.profile_receipts.retire_operation(
                &current.operation.operation_id,
                pi_extension_protocol::profile_live::RetireReason::Consumed,
            );
        }
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
        self.hook_with_telemetry_key(event, accepted, tool, telemetry, None)
            .await
    }
    pub(crate) async fn hook_with_key(
        &self,
        event: HookEvent,
        accepted: bool,
        tool: Option<&pi_ai_core::Tool>,
        key: &EffectKey,
    ) -> Result<crate::hooks::HookPipelineOutput, HarnessRuntimeError> {
        self.hook_with_telemetry_key(event, accepted, tool, self.trace.context(), Some(key))
            .await
    }
    pub(crate) async fn hook_with_telemetry_key(
        &self,
        event: HookEvent,
        accepted: bool,
        tool: Option<&pi_ai_core::Tool>,
        telemetry: Telemetry,
        key: Option<&EffectKey>,
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
        self.started_hook_with_telemetry(event, accepted, tool, telemetry, key)
            .await
    }
    async fn started_hook_with_telemetry(
        &self,
        event: HookEvent,
        accepted: bool,
        tool: Option<&pi_ai_core::Tool>,
        telemetry: Telemetry,
        _key: Option<&EffectKey>,
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
        #[cfg(feature = "extensions")]
        let scope = crate::extensions::profiles::custody::EffectScope {
            fx: self.this.upgrade().expect("live hook owner"),
            accepted,
            expected_key: _key.cloned(),
        };
        #[cfg(feature = "extensions")]
        let mut invocation = Box::pin(captured.run_started_scoped(event, &context, &scope));
        #[cfg(not(feature = "extensions"))]
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
        #[cfg(feature = "extensions")]
        self.profile_receipts.check(commit.plan.key())?;
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
                #[cfg(feature = "extensions")]
                remote: self
                    .lane
                    .owner
                    .profile_providers
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(&model.provider)
                    .filter(|remote| {
                        remote.registration.same_registration(&before)
                            && remote.models.contains(&model)
                    })
                    .cloned(),
                model,
                registration: before,
            })
        } else {
            None
        };
        // Declared before guards: even early returns drop these after the lane unlocks.
        let mut origin_release = crate::tools::OriginRelease::default();
        let mut observed_registration = None;
        let mut displaced_request = None;
        let routed = matches!(plan, EffectPlan::Tool(_));
        let mut pair = None;
        let mut settings = None;
        let mut line = None;
        if routed {
            pair = Some(crate::tools::PairedPermits::new(
                self.lane
                    .owner
                    .settings_line
                    .acquire_pair(&self.lane.lane.line)
                    .await?,
            ));
            pair.as_mut().expect("Tool pair").arm_origin(
                self.origin_consumers
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(plan.key())
                    .cloned(),
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
        if let EffectPlan::Tool(tool) = plan
            && !h.current.as_ref().is_some_and(|current| {
                pi_harness_core::tool_batch::expects_effect(current, &tool.key.session_id, tool)
            })
        {
            origin_release.append(self.retire_actual_lost_publication(&h, &tool.key));
            if let Some(pair) = &mut pair {
                pair.release();
            }
            return Ok(false);
        }
        let Some(current) = h.current.as_ref() else {
            if let Some(pair) = &mut pair {
                pair.release();
            }
            return Ok(false);
        };
        let key = plan.key();
        if key.session_id != self.lane.owner.session.metadata().base_metadata().id
            || key.lane != self.lane.lane.name
            || key.operation_id != current.operation.operation_id
        {
            if let Some(pair) = &mut pair {
                pair.release();
            }
            return Ok(false);
        }
        if pi_harness_core::structural::is_cancelled(current) {
            drop(line);
            drop(settings);
            if let Some(pair) = &mut pair {
                pair.release();
            }
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
            if let Some(pair) = &mut pair {
                pair.release();
            }
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
                if let Some(pair) = &mut pair {
                    pair.release();
                }
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
        if let Some(pair) = &mut pair {
            pair.release();
        }
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
        let operation = plan.key().operation_id.clone();
        let origin = if matches!(plan, EffectPlan::Tool(_)) {
            self.origin_consumers
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(plan.key())
                .cloned()
        } else {
            None
        };
        let mut ownership = origin
            .as_ref()
            .map(|origin| crate::tools::OriginOwnership::new(origin.clone()));
        let result = self.execute_inner(plan).await;
        let result = if result.is_ok()
            && origin.as_ref().is_some_and(|lease| lease.is_active())
            && self.origin_stamp(&operation).is_none()
        {
            Err(crate::restore::fault(
                "Direct consumer lost its actual drive stamp",
            ))
        } else {
            result
        };
        let result = if origin.is_some() {
            self.lane.owner.observe(result)
        } else {
            result
        };
        if result.is_ok()
            && let Some(origin) = &origin
        {
            origin.return_to_drive();
            if let Some(ownership) = &mut ownership {
                ownership.complete();
            }
        }
        result
    }
    async fn execute_inner(
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
                        Some(&plan.key),
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
    #[cfg(feature = "extensions")]
    pub remote: Option<crate::extensions::profiles::provider::RegisteredProvider>,
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

#[cfg(test)]
#[path = "../tests/runtime_r1_r4_support/mod.rs"]
mod origin_support;

#[cfg(test)]
mod origin_runtime_tests {
    use super::*;
    type Observer = dyn Fn(&dyn Any) + Send + Sync;
    static OBSERVERS: std::sync::OnceLock<Mutex<Vec<std::sync::Weak<Observer>>>> =
        std::sync::OnceLock::new();
    pub(super) fn observe(value: &dyn Any) {
        let observers = {
            let mut stored = OBSERVERS.get_or_init(Mutex::default).lock().unwrap();
            let live: Vec<_> = stored.iter().filter_map(std::sync::Weak::upgrade).collect();
            stored.retain(|observer| observer.strong_count() != 0);
            live
        };
        for observer in observers {
            observer(value);
        }
    }

    #[allow(dead_code)]
    mod actual {

        use super::*;
        use crate::effects::origin_support::poll;
        use pi_agent_session::{memory::MemoryStorage, storage::*};
        use pi_ai_core::{BoxFuture, SpawnError, TaskSpawner};
        use std::{
            future::Future,
            sync::atomic::{AtomicBool, AtomicU64, Ordering},
            task::{Context, Waker},
        };

        struct Task {
            id: u64,
            future: BoxFuture<'static, ()>,
        }
        pub struct Pump {
            tasks: Mutex<Vec<Task>>,
            next: AtomicU64,
            pub current: AtomicU64,
        }
        impl Default for Pump {
            fn default() -> Self {
                Self {
                    tasks: Mutex::new(vec![]),
                    next: AtomicU64::new(1),
                    current: AtomicU64::new(0),
                }
            }
        }
        impl TaskSpawner for Pump {
            fn spawn(&self, future: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
                let id = self.next.fetch_add(1, Ordering::SeqCst);
                self.tasks.lock().unwrap().push(Task { id, future });
                Ok(())
            }
        }
        impl Pump {
            pub fn pending_tasks(&self) -> usize {
                self.tasks.lock().unwrap().len()
            }
            pub fn tick(&self) {
                let ids: Vec<_> = self
                    .tasks
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|task| task.id)
                    .collect();
                for id in ids {
                    self.poll_task(id, Waker::noop());
                }
            }
            pub fn poll_task(&self, id: u64, waker: &Waker) -> Option<bool> {
                let mut task = {
                    let mut tasks = self.tasks.lock().unwrap();
                    let index = tasks.iter().position(|task| task.id == id)?;
                    tasks.remove(index)
                };
                let previous = self.current.swap(id, Ordering::SeqCst);
                let pending = task
                    .future
                    .as_mut()
                    .poll(&mut Context::from_waker(waker))
                    .is_pending();
                self.current.store(previous, Ordering::SeqCst);
                if pending {
                    self.tasks.lock().unwrap().push(task);
                }
                Some(pending)
            }
            pub fn drop_task(&self, id: u64) {
                let removed = {
                    let mut tasks = self.tasks.lock().unwrap();
                    let index = tasks
                        .iter()
                        .position(|task| task.id == id)
                        .expect("actual task is retained");
                    tasks.remove(index)
                };
                drop(removed);
            }
            pub fn drive<F: Future>(&self, future: F) -> F::Output {
                let mut future = std::pin::pin!(future);
                for _ in 0..20000 {
                    if let Poll::Ready(result) = poll(future.as_mut()) {
                        return result;
                    }
                    self.tick();
                }
                panic!("origin1341 actual runtime did not settle");
            }
        }

        pub struct Store {
            pub inner: Arc<MemoryStorage>,
            pub pump: Arc<Pump>,
            pub mode: AtomicU64,
            pub exit: AtomicU64,
            pub drive: AtomicU64,
            pub producer: AtomicU64,
            pub consumer: AtomicU64,
            pub suppress_park: Arc<AtomicBool>,
            pub attempts: AtomicU64,
            pub refused: AtomicU64,
            pub applied: AtomicBool,
            pub args_seq: AtomicU64,
            pub commit_entered: pi_ai_core::CancellationToken,
            pub commit_release: pi_ai_core::CancellationToken,
            pub retry_entered: pi_ai_core::CancellationToken,
            pub retry_release: pi_ai_core::CancellationToken,
            pub hydrate_entered: pi_ai_core::CancellationToken,
            pub hydrate_release: pi_ai_core::CancellationToken,
            pub consumer_entered: pi_ai_core::CancellationToken,
            pub consumer_release: pi_ai_core::CancellationToken,
            post_apply: AtomicBool,
            consumer_parked: AtomicBool,
        }
        impl Store {
            pub fn new(inner: Arc<MemoryStorage>, pump: Arc<Pump>) -> Self {
                Self {
                    inner,
                    pump,
                    mode: AtomicU64::new(0),
                    exit: AtomicU64::new(0),
                    drive: AtomicU64::new(0),
                    producer: AtomicU64::new(0),
                    consumer: AtomicU64::new(0),
                    suppress_park: Arc::new(AtomicBool::new(false)),
                    attempts: AtomicU64::new(0),
                    refused: AtomicU64::new(0),
                    applied: AtomicBool::new(false),
                    args_seq: AtomicU64::new(0),
                    commit_entered: pi_ai_core::CancellationToken::new(),
                    commit_release: pi_ai_core::CancellationToken::new(),
                    retry_entered: pi_ai_core::CancellationToken::new(),
                    retry_release: pi_ai_core::CancellationToken::new(),
                    hydrate_entered: pi_ai_core::CancellationToken::new(),
                    hydrate_release: pi_ai_core::CancellationToken::new(),
                    consumer_entered: pi_ai_core::CancellationToken::new(),
                    consumer_release: pi_ai_core::CancellationToken::new(),
                    post_apply: AtomicBool::new(false),
                    consumer_parked: AtomicBool::new(false),
                }
            }
            fn args_index(request: &ConditionalCommit) -> Option<usize> {
                request.transaction.writes.iter().position(|write| matches!(write,
                Write::SetRegister(register) if matches!(register.as_ref(), RegisterSet::OperationToolArguments { .. })))
            }
            async fn before(&self, request: &ConditionalCommit) -> Option<usize> {
                let index = Self::args_index(request)?;
                self.producer
                    .store(self.pump.current.load(Ordering::SeqCst), Ordering::SeqCst);
                let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if self.mode.load(Ordering::SeqCst) == 2 {
                    if attempt == 1 {
                        self.commit_entered.cancel();
                        self.commit_release.cancelled().await;
                    } else {
                        self.retry_entered.cancel();
                        self.retry_release.cancelled().await;
                    }
                }
                Some(index)
            }
            fn after(&self, index: Option<usize>, outcome: &ConditionalOutcome) {
                let Some(index) = index else { return };
                match outcome {
                    ConditionalOutcome::NotApplied => {
                        self.refused.fetch_add(1, Ordering::SeqCst);
                    }
                    ConditionalOutcome::Applied(commit) => {
                        self.args_seq
                            .store(commit.seqs[index].get(), Ordering::SeqCst);
                        self.applied.store(true, Ordering::SeqCst);
                        self.post_apply.store(true, Ordering::SeqCst);
                    }
                }
            }
        }
        impl Storage for Store {
            fn commit(
                &self,
                transaction: Transaction,
            ) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
                self.inner.commit(transaction)
            }
            fn get_entries<'a>(
                &'a self,
                ids: &'a [EntryId],
            ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
                self.inner.get_entries(ids)
            }
            fn get_register<'a>(
                &'a self,
                namespace: RegisterNamespace,
                key: &'a str,
            ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
                self.inner.get_register(namespace, key)
            }
            fn list_registers<'a>(
                &'a self,
                namespace: RegisterNamespace,
                prefix: Option<&'a str>,
            ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
                self.inner.list_registers(namespace, prefix)
            }
            fn scan_branch(
                &self,
                query: BranchScan,
            ) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
                self.inner.scan_branch(query)
            }
            fn scan_branch_structure(
                &self,
                query: BranchScan,
            ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
                self.inner.scan_branch_structure(query)
            }
            fn scan_entries(
                &self,
                query: EntryScan,
            ) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
                self.inner.scan_entries(query)
            }
            fn scan_usage(
                &self,
                query: UsageScan,
            ) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
                self.inner.scan_usage(query)
            }
            fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
                self.inner.stats()
            }
            fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
                self.inner.close()
            }
        }
        impl AtomicStorage for Store {
            fn commit_conditional(
                &self,
                request: ConditionalCommit,
            ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
                self.inner.commit_conditional(request)
            }
            fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
                self.inner.snapshot()
            }
        }
        impl CheckedStorage for Store {
            fn commit_checked<'a, V: PublicationValidator + ?Sized>(
                &'a self,
                request: ConditionalCommit,
                validator: &'a V,
            ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>>
            {
                Box::pin(async move {
                    let index = self.before(&request).await;
                    let outcome = self.inner.commit_checked(request, validator).await?;
                    self.after(index, &outcome);
                    Ok(outcome)
                })
            }
        }
        impl RuntimeStorage for Store {
            fn runtime_attachment(&self) -> &RuntimeAttachment {
                self.inner.runtime_attachment()
            }
            fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
                Box::pin(async move {
                    if self.post_apply.swap(false, Ordering::SeqCst)
                        && (self.mode.load(Ordering::SeqCst) == 1
                            || (self.mode.load(Ordering::SeqCst) == 5
                                && self.attempts.load(Ordering::SeqCst) == 2))
                    {
                        self.hydrate_entered.cancel();
                        self.hydrate_release.cancelled().await;
                        match self.exit.load(Ordering::SeqCst) {
                            1 => {
                                return Err(StorageError::Io(
                                    ErrorContext::new("origin1341 hydration error").unwrap(),
                                ));
                            }
                            2 => panic!("origin1341 actual hydration unwind"),
                            _ => {}
                        }
                    }
                    let current = self.pump.current.load(Ordering::SeqCst);
                    if self.mode.load(Ordering::SeqCst) == 3
                        && !self.suppress_park.load(Ordering::SeqCst)
                        && current != 0
                        && self.applied.load(Ordering::SeqCst)
                        && current != self.drive.load(Ordering::SeqCst)
                        && current != self.producer.load(Ordering::SeqCst)
                        && !self.consumer_parked.swap(true, Ordering::SeqCst)
                    {
                        self.consumer.store(current, Ordering::SeqCst);
                        self.consumer_entered.cancel();
                        self.consumer_release.cancelled().await;
                        match self.exit.load(Ordering::SeqCst) {
                            1 => {
                                return Err(StorageError::Io(
                                    ErrorContext::new("origin1341 consumer read error").unwrap(),
                                ));
                            }
                            2 => panic!("origin1341 actual consumer read unwind"),
                            _ => {}
                        }
                    }
                    self.inner.read_stamp().await
                })
            }
            fn prepare_entries(
                &self,
                stamp: ReadStamp,
                slots: Vec<PreparedEntrySlot>,
            ) -> BoxFuture<'_, Result<Option<PreparedEntries>, StorageError>> {
                self.inner.prepare_entries(stamp, slots)
            }
            fn commit_prepared_checked<'a, V: PublicationValidator + ?Sized>(
                &'a self,
                prepared: PreparedEntries,
                request: ConditionalCommit,
                validator: &'a V,
            ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>>
            {
                self.inner
                    .commit_prepared_checked(prepared, request, validator)
            }
        }
    }
    use crate::effects::origin_support::{Setup, Summary, assistant, poll};
    use pi_ai_core::{BoxFuture, JsonObject};
    use std::{
        sync::atomic::{AtomicU64, Ordering},
        task::Poll,
    };
    struct DropContext {
        id: u64,
        probe: Arc<dyn Fn(u8, u64) + Send + Sync>,
    }
    impl Drop for DropContext {
        fn drop(&mut self) {
            (self.probe)(1, self.id);
        }
    }
    struct Contexts {
        next: AtomicU64,
        probe: Arc<dyn Fn(u8, u64) + Send + Sync>,
    }
    impl ToolContextProvider<DropContext> for Contexts {
        fn resolve(&self) -> BoxFuture<'_, Result<DropContext, HarnessRuntimeError>> {
            Box::pin(async move {
                Ok(DropContext {
                    id: self.next.fetch_add(1, Ordering::SeqCst),
                    probe: self.probe.clone(),
                })
            })
        }
    }
    struct DropTool {
        descriptor: AgentTool,
        entered: CancellationToken,
        release: CancellationToken,
        probe: Arc<dyn Fn(u8, u64) + Send + Sync>,
    }
    impl Drop for DropTool {
        fn drop(&mut self) {
            (self.probe)(0, 0);
        }
    }
    impl AgentHarnessTool<DropContext> for DropTool {
        fn descriptor(&self) -> &AgentTool {
            &self.descriptor
        }
        fn prepare_arguments(
            &self,
            value: pi_ai_core::JsonValue,
        ) -> Result<JsonObject, ToolExecutionError> {
            let pi_ai_core::JsonValue::Object(args) = value else {
                panic!("actual object args")
            };
            Ok(args)
        }
        fn execute<'a>(
            &'a self,
            _: pi_ai_core::ToolCallId,
            _: JsonObject,
            _: &'a dyn Cancellation,
            _: &'a dyn ToolProgressSink,
            _: &'a DropContext,
        ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
            Box::pin(async move {
                self.entered.cancel();
                self.release.cancelled().await;
                Ok(AgentToolResult {
                    content: vec![],
                    details: pi_ai_core::JsonValue::Null,
                    usage: None,
                    added_tool_names: None,
                    terminate: None,
                })
            })
        }
    }
    struct Before(CancellationToken, CancellationToken);
    impl HookHandler<BeforeTool> for Before {
        fn call(
            &self,
            _: BeforeToolEvent,
        ) -> BoxFuture<'_, Result<Option<BeforeToolResult>, HandlerError>> {
            self.0.cancel();
            Box::pin(async move {
                self.1.cancelled().await;
                Ok(None)
            })
        }
    }
    fn until(pump: &actual::Pump, signal: &CancellationToken) {
        for _ in 0..20000 {
            if signal.is_cancelled() {
                return;
            }
            pump.tick();
        }
        panic!("actual runtime milestone did not occur");
    }
    fn external_terminal(setup: &Setup) {
        let Some(Register::LaneState { mut value, .. }) = setup
            .pump
            .drive(
                setup
                    .session
                    .get_register(RegisterNamespace::LaneState, "main"),
            )
            .unwrap()
        else {
            panic!("lane")
        };
        let operation = value.current_operation_id.take().unwrap();
        let Some(Register::OperationState { value: state, .. }) = setup
            .pump
            .drive(
                setup
                    .session
                    .get_register(RegisterNamespace::OperationState, operation.as_str()),
            )
            .unwrap()
        else {
            panic!("operation")
        };
        let OperationState::Run(run) = state.as_ref() else {
            panic!("run")
        };
        let RunPhase::Tools(tools) = &run.phase else {
            panic!("tools")
        };
        let result = LaneLastResult::RunAborted(RunAbortedLastResult {
            operation_id: operation.clone(),
            kind: Default::default(),
            leaf_id: setup.pump.drive(setup.session.get_leaf_id()).unwrap(),
            final_assistant_entry_id: Some(tools.batch.assistant_entry_id.clone()),
            outcome: Default::default(),
        });
        let mut writes = vec![
            Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::OperationState,
                key: operation.as_str().into(),
            }),
            Write::DeleteRegister(RegisterDelete {
                namespace: RegisterNamespace::OperationMeta,
                key: operation.as_str().into(),
            }),
            Write::SetRegister(Box::new(RegisterSet::LaneLastResult {
                key: "main".into(),
                value: result,
            })),
            Write::SetRegister(Box::new(RegisterSet::LaneState {
                key: "main".into(),
                value,
            })),
        ];
        for namespace in [
            RegisterNamespace::OperationToolArguments,
            RegisterNamespace::OperationPreparation,
        ] {
            for register in setup
                .pump
                .drive(
                    setup
                        .session
                        .list_registers(namespace, Some(&format!("{}:", operation.as_str()))),
                )
                .unwrap()
            {
                let key = match register {
                    Register::OperationToolArguments { key, .. }
                    | Register::OperationPreparation { key, .. } => key,
                    _ => unreachable!(),
                };
                writes.push(Write::DeleteRegister(RegisterDelete { namespace, key }));
            }
        }
        setup
            .pump
            .drive(setup.session.commit(Transaction { writes }))
            .unwrap();
    }
    #[test]
    fn actual_final_selection_payloads_reenter_unlocked_on_every_exit() {
        type Fx = RuntimeEffects<DropContext, actual::Store, SessionMetadata>;
        type Own = Owner<DropContext, actual::Store, SessionMetadata>;
        for path in [
            "success",
            "rejection",
            "retirement",
            "work-drop",
            "early-error",
            "close",
        ] {
            let setup = Setup::new();
            let mut response = assistant(&setup.faux.model, pi_ai_core::StopReason::ToolUse);
            response.content = vec![pi_ai_core::AssistantContent::ToolCall(
                pi_ai_core::ToolCall {
                    content_type: Default::default(),
                    id: pi_ai_core::ToolCallId::new("drop-call").unwrap(),
                    name: pi_ai_core::ToolName::new("echo").unwrap(),
                    arguments: serde_json::from_str(r#"{"text":"ping"}"#).unwrap(),
                    thought_signature: None,
                    namespace: None,
                },
            )];
            *setup.faux.responses.lock().unwrap() = vec![
                response,
                assistant(&setup.faux.model, pi_ai_core::StopReason::Stop),
            ]
            .into();
            let pump = Arc::new(actual::Pump::default());
            let store = Arc::new(actual::Store::new(setup.store.clone(), pump.clone()));
            let session = Arc::new(
                pump.drive(pi_agent_session::session::RuntimeSession::new(
                    store.clone(),
                    setup.session.metadata(),
                    Arc::new(
                        pi_agent_session::codec::SessionCodec::new(
                            SessionCodecOptions::default(),
                            ProviderHeaderPolicy::default(),
                        )
                        .unwrap(),
                    ),
                    setup.ids.clone(),
                    Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
                ))
                .unwrap(),
            );
            let owner_slot: Arc<Mutex<std::sync::Weak<Own>>> =
                Arc::new(Mutex::new(std::sync::Weak::new()));
            let fx_slot: Arc<Mutex<std::sync::Weak<Fx>>> =
                Arc::new(Mutex::new(std::sync::Weak::new()));
            let cells = Arc::new(Mutex::new(Vec::<
                std::sync::Weak<crate::tools::ToolPublication<DropContext>>,
            >::new()));
            let selected_context = Arc::new(AtomicU64::new(0));
            let records = Arc::new(Mutex::new(Vec::new()));
            let probe: Arc<dyn Fn(u8, u64) + Send + Sync> = {
                let owner_slot = owner_slot.clone();
                let fx_slot = fx_slot.clone();
                let cells = cells.clone();
                let records = records.clone();
                let selected_context = selected_context.clone();
                Arc::new(move |kind, id| {
                    if kind == 1 && id != selected_context.load(Ordering::SeqCst) {
                        return;
                    }
                    let owner = owner_slot.lock().unwrap().clone();
                    let fx = fx_slot.lock().unwrap().clone();
                    let cells = cells.lock().unwrap().clone();
                    let (send, receive) = std::sync::mpsc::channel();
                    let worker = std::thread::spawn(move || {
                        let Some(owner) = owner.upgrade() else {
                            let _ = send.send(false);
                            return;
                        };
                        let mut unlocked = owner.tools.try_lock().is_ok()
                            && owner.tool_publications.try_lock().is_ok()
                            && !owner.settings_line.held_for_test();
                        let lanes: Vec<_> = owner.lanes.lock().unwrap().values().cloned().collect();
                        unlocked &= lanes.iter().all(|lane| !lane.line.held_for_test());
                        for cell in cells.into_iter().filter_map(|cell| cell.upgrade()) {
                            unlocked &= cell.state.try_lock().is_ok();
                        }
                        if let Some(fx) = fx.upgrade() {
                            unlocked &= fx.selected_tools.try_lock().is_ok()
                                && fx.origin_consumers.try_lock().is_ok()
                                && fx.contexts.try_lock().is_ok()
                                && fx.bindings.try_lock().is_ok();
                        }
                        send.send(unlocked).unwrap();
                    });
                    let result = receive
                        .recv_timeout(Duration::from_secs(5))
                        .expect("real final payload reentry finishes under finite deadline");
                    worker.join().unwrap();
                    records.lock().unwrap().push((kind, id, result));
                })
            };
            let tool = Arc::new(DropTool {
                descriptor: AgentTool {
                    name: pi_ai_core::ToolName::new("echo").unwrap(),
                    label: "Echo".into(),
                    description: "actual selection final Drop".into(),
                    parameters: serde_json::from_str(r#"{"type":"object"}"#).unwrap(),
                    constrained_sampling: None,
                    execution_mode: Some(ToolExecutionMode::Sequential),
                    replay: Some(ToolReplay::Safe),
                },
                entered: CancellationToken::new(),
                release: CancellationToken::new(),
                probe: probe.clone(),
            });
            let weak_tool = Arc::downgrade(&tool);
            let entered = tool.entered.clone();
            let execution_release = tool.release.clone();
            let mut options = setup.options_for::<DropContext, _>(session, DriveMode::Automatic);
            options.tools = vec![tool.clone()];
            options.tool_context = Some(AgentHarnessToolContextSource::Provider(Arc::new(
                Contexts {
                    next: AtomicU64::new(1),
                    probe: probe.clone(),
                },
            )));
            let factory = RuntimeFactory::new(
                setup.time.clone(),
                pump.clone(),
                setup.ids.clone(),
                Arc::new(DropContext {
                    id: 0,
                    probe: Arc::new(|_, _| {}),
                }),
                Arc::new(Summary),
            );
            let harness = pump
                .drive(factory.create_configured(options))
                .unwrap()
                .harness;
            *owner_slot.lock().unwrap() = Arc::downgrade(&harness.main.owner);
            pump.drive(harness.set_active_tools(vec![pi_ai_core::ToolName::new("echo").unwrap()]))
                .unwrap();
            let observer: Arc<Observer> = {
                let owner = Arc::downgrade(&harness.main.owner);
                let fx_slot = fx_slot.clone();
                Arc::new(move |value| {
                    if let Some(fx) = value.downcast_ref::<Arc<Fx>>()
                        && owner
                            .upgrade()
                            .is_some_and(|owner| Arc::ptr_eq(&owner, &fx.lane.owner))
                    {
                        *fx_slot.lock().unwrap() = Arc::downgrade(fx);
                    }
                })
            };
            OBSERVERS
                .get_or_init(Mutex::default)
                .lock()
                .unwrap()
                .push(Arc::downgrade(&observer));
            let before = Arc::new(Before(CancellationToken::new(), CancellationToken::new()));
            let subscription = harness.hooks().before_tool(None, before.clone()).unwrap();
            if path != "success" {
                store.mode.store(1, Ordering::SeqCst);
            }
            let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
                text: "invoke echo".into(),
                images: vec![],
            }));
            assert!(poll(prompt.as_mut()).is_pending());
            until(&pump, &before.0);
            if path != "success" {
                pump.drive(harness.set_tools(vec![])).unwrap();
            }
            drop(tool);
            before.1.cancel();
            if path == "success" {
                until(&pump, &entered);
                pump.drive(harness.set_tools(vec![])).unwrap();
            }
            if path != "success" {
                until(&pump, &store.hydrate_entered);
            }
            {
                let fx = fx_slot.lock().unwrap().upgrade().unwrap();
                let selections = fx.selected_tools.lock().unwrap();
                let selection = selections
                    .values()
                    .next()
                    .expect("actual prepared selection");
                selected_context.store(selection.context.id, Ordering::SeqCst);
                *cells.lock().unwrap() = harness
                    .main
                    .owner
                    .tool_publications
                    .lock()
                    .unwrap()
                    .values()
                    .map(Arc::downgrade)
                    .collect();
            }
            let mut close = None;
            match path {
                "success" => execution_release.cancel(),
                "rejection" => store.hydrate_release.cancel(),
                "retirement" => {
                    external_terminal(&setup);
                    store.hydrate_release.cancel();
                }
                "work-drop" => pump.drop_task(store.producer.load(Ordering::SeqCst)),
                "early-error" => {
                    store.exit.store(1, Ordering::SeqCst);
                    store.hydrate_release.cancel();
                }
                "close" => {
                    let mut future = Box::pin(harness.close());
                    assert!(poll(future.as_mut()).is_pending());
                    close = Some(future);
                    store.hydrate_release.cancel();
                }
                _ => unreachable!(),
            }
            let result = pump.drive(prompt);
            for _ in 0..100 {
                pump.tick();
            }
            if let Some(close) = close {
                pump.drive(close).unwrap();
            }
            let observed = records.lock().unwrap().clone();
            println!(
                "E1341_FINAL_PAYLOAD path={path} actual_final_tool={} selected_context={} records={observed:?} result={result:?}",
                weak_tool.upgrade().is_none(),
                selected_context.load(Ordering::SeqCst)
            );
            subscription.unsubscribe();
            store.hydrate_release.cancel();
            execution_release.cancel();
            let close_result = pump.drive(harness.close());
            assert!(
                close_result.is_ok() || matches!(close_result, Err(HarnessRuntimeError::Fault(_)))
            );
            for _ in 0..20000 {
                pump.tick();
            }
            let opaque = harness
                .main
                .owner
                .tool_publications
                .lock()
                .unwrap()
                .values()
                .filter(|cell| {
                    matches!(
                        cell.state.lock().unwrap().phase,
                        crate::tools::PublicationPhase::Publishing(_)
                            | crate::tools::PublicationPhase::ConsumerDirect { .. }
                            | crate::tools::PublicationPhase::Sealing(_)
                    )
                })
                .count();
            println!(
                "E1341_FINAL_CLEANUP path={path} tasks={} unresolved_owned_cells={opaque}",
                pump.pending_tasks()
            );
            assert_eq!(pump.pending_tasks(), 0);
            assert_eq!(opaque, 0);
            assert!(weak_tool.upgrade().is_none());
            assert!(observed.iter().any(|(kind, _, _)| *kind == 0));
            assert!(observed.iter().any(|(kind, _, _)| *kind == 1));
            assert!(observed.iter().all(|(_, _, unlocked)| *unlocked));
        }
    }
    #[cfg(feature = "extensions")]
    mod wait1359 {
        use super::*;
        use pi_agent_session::storage::*;
        use std::sync::atomic::AtomicBool;
        #[allow(dead_code)]
        mod wire {
            use super::*;
            use crate::extensions::{
                tool_driver::{OwnerLimits, SingleToolOwner},
                transport::{ChannelError, MonotonicClock, OrderedByteChannel, PumpConfig},
            };
            use pi_ai_core::{SpawnError, TaskSpawner};
            use pi_extension_protocol::{
                activation::{
                    AgentToolMode, ApprovedTool, HostAuthority, ToolReplay as WireReplay,
                    ToolSchemaInputs,
                },
                jcs::Limits as JsonLimits,
                wire::{
                    framing::{self, EncodedFrame, FrameReader},
                    v1_1,
                },
            };
            use serde_json::Value;
            use std::{
                collections::VecDeque,
                num::{NonZeroU64, NonZeroUsize},
                pin::Pin,
                sync::Barrier,
                task::{Context, Poll, Waker},
            };
            const CATALOG: &str =
                include_str!("../../pi-extension-protocol/schema/fixtures/host-catalog.json");
            const VECTORS: &str =
                include_str!("../../pi-extension-protocol/schema/fixtures/activation-vectors.json");
            fn hex(value: &str) -> Vec<u8> {
                value
                    .as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
                    .collect()
            }
            use std::future::{Future, poll_fn};
            fn nz(value: usize) -> NonZeroUsize {
                NonZeroUsize::new(value).unwrap()
            }
            fn varint(mut number: usize) -> Vec<u8> {
                let mut out = Vec::new();
                loop {
                    let mut byte = (number & 127) as u8;
                    number >>= 7;
                    if number != 0 {
                        byte |= 128;
                    }
                    out.push(byte);
                    if number == 0 {
                        return out;
                    }
                }
            }
            fn field(number: u8, value: &[u8]) -> Vec<u8> {
                let mut out = vec![(number << 3) | 2];
                out.extend(varint(value.len()));
                out.extend(value);
                out
            }
            fn frame(arm: u8, body: &[u8]) -> Vec<u8> {
                let envelope = field(arm, body);
                [varint(envelope.len()), envelope].concat()
            }
            fn hello() -> Vec<u8> {
                let version = [0x08, 0x01, 0x18, 0x01];
                let profile = [field(1, b"tool.v1"), vec![0x10, 0x01, 0x20, 0x01]].concat();
                frame(1, &[field(1, &version), field(2, &profile)].concat())
            }
            fn manifest() -> Vec<u8> {
                let vectors: Value = serde_json::from_str(VECTORS).unwrap();
                frame(
                    3,
                    &hex(vectors["cases"]["valid_1_1"]["manifest_pb_hex"]
                        .as_str()
                        .unwrap()),
                )
            }
            fn manifest_with_replay(replay: WireReplay) -> Vec<u8> {
                if replay == WireReplay::Safe {
                    return manifest();
                }
                let mut reader = FrameReader::<v1_1::Envelope>::new(nz(8192));
                let mut envelope = reader
                    .read_one(&manifest())
                    .unwrap()
                    .frame
                    .unwrap()
                    .envelope;
                let Some(v1_1::envelope::Frame::Manifest(manifest)) = envelope.frame.as_mut()
                else {
                    panic!("manifest fixture")
                };
                assert_eq!(manifest.capabilities.len(), 1);
                manifest.capabilities[0].replay = v1_1::ReplayContract::ReplayNever as i32;
                framing::encode(&envelope, nz(8192)).unwrap().bytes
            }
            fn ready_frame() -> Vec<u8> {
                frame(5, &[])
            }

            fn origin1341_host(
                generation: u64,
                minor: u32,
                alter_progress: bool,
                parallel: bool,
                replay: WireReplay,
            ) -> HostAuthority {
                let catalog: Value = serde_json::from_str(CATALOG).unwrap();
                let entry = &catalog["entries"][0];
                let mut altered =
                    entry["selected_minor_schema_sets"]["1.0"]["progress"]["schema"].clone();
                if alter_progress {
                    altered["maxProperties"] = serde_json::json!(16);
                }
                let descriptor = pi_extension_protocol::activation::AgentTool {
                    name: "echo".into(),
                    label: entry["agentTool"]["label"].as_str().unwrap().into(),
                    description: entry["agentTool"]["description"].as_str().unwrap().into(),
                    parameters: entry["agentTool"]["parameters"].clone(),
                    execution_mode: if parallel {
                        AgentToolMode::Parallel
                    } else {
                        AgentToolMode::Sequential
                    },
                    replay,
                };
                let schemas = [0, 1]
                    .into_iter()
                    .map(|minor| {
                        let set = &entry["selected_minor_schema_sets"][format!("1.{minor}")];
                        ToolSchemaInputs {
                            minor,
                            arguments: serde_json::to_vec(&set["arguments"]["schema"]).unwrap(),
                            progress: serde_json::to_vec(if alter_progress {
                                &altered
                            } else {
                                &set["progress"]["schema"]
                            })
                            .unwrap(),
                            raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).unwrap(),
                        }
                    })
                    .collect();
                let tool =
                    ApprovedTool::new("peer-a".into(), descriptor, schemas, JsonLimits::default())
                        .unwrap();
                let limits = v1_1::ProtocolLimits {
                    pre_handshake_frame_bytes: 4096,
                    frame_bytes: 8192,
                    aggregate_decoded_bytes: 8192,
                    value_depth: 32,
                    value_elements: 1024,
                    string_bytes: 4096,
                    bytes_length: 4096,
                    in_flight_requests: 8,
                    blocking_queue_bytes: 4096,
                    progress_queue_bytes: 4096,
                    passive_queue_bytes: 4096,
                    telemetry_queue_bytes: 4096,
                    nesting_depth: 8,
                    requests_per_second: 32,
                    handshake_ms: 1000,
                    cancellation_grace_ms: 100,
                    max_stream_credit_bytes: 4096,
                };
                HostAuthority::new(
                    "peer-a".into(),
                    generation,
                    minor,
                    minor,
                    limits,
                    vec![tool],
                    vec![],
                )
                .unwrap()
            }
            pub(super) struct FakeChannel {
                reads: Mutex<VecDeque<Option<Vec<u8>>>>,
                writes: Mutex<Vec<Vec<u8>>>,
                closed: AtomicBool,
                stall: AtomicBool,
            }
            struct WritePause {
                entered: Barrier,
                release: Barrier,
            }
            impl WritePause {
                fn new() -> Self {
                    Self {
                        entered: Barrier::new(2),
                        release: Barrier::new(2),
                    }
                }
            }
            impl FakeChannel {
                fn new() -> Self {
                    Self {
                        reads: Mutex::new(VecDeque::new()),
                        writes: Mutex::new(Vec::new()),
                        closed: AtomicBool::new(false),
                        stall: AtomicBool::new(false),
                    }
                }
                fn push(&self, bytes: Vec<u8>) {
                    self.reads.lock().unwrap().push_back(Some(bytes));
                }
                fn eof(&self) {
                    self.reads.lock().unwrap().push_back(None);
                }
                fn writes(&self) -> Vec<Vec<u8>> {
                    self.writes.lock().unwrap().clone()
                }

                fn read_future<'a>(
                    &'a self,
                    buffer: &'a mut [u8],
                ) -> impl Future<Output = Result<Option<usize>, ChannelError>> + Send + 'a
                {
                    poll_fn(move |_| {
                        if self.closed.load(Ordering::SeqCst) {
                            return Poll::Ready(Err(ChannelError::Closed));
                        }
                        match self.reads.lock().unwrap().pop_front() {
                            Some(Some(bytes)) if bytes.len() <= buffer.len() => {
                                buffer[..bytes.len()].copy_from_slice(&bytes);
                                Poll::Ready(Ok(Some(bytes.len())))
                            }
                            Some(Some(_)) => Poll::Ready(Err(ChannelError::Failed)),
                            Some(None) => Poll::Ready(Ok(None)),
                            None => Poll::Pending,
                        }
                    })
                }
            }
            impl OrderedByteChannel for FakeChannel {
                fn max_read_scratch_bytes(&self) -> usize {
                    let mut empty = [];
                    std::mem::size_of_val(&self.read_future(&mut empty))
                }

                fn read_chunk<'a>(
                    &'a self,
                    buffer: &'a mut [u8],
                ) -> BoxFuture<'a, Result<Option<usize>, ChannelError>> {
                    Box::pin(self.read_future(buffer))
                }
                fn write_frame(
                    &self,
                    frame: EncodedFrame,
                ) -> BoxFuture<'_, Result<(), ChannelError>> {
                    Box::pin(poll_fn(move |_| {
                        if self.closed.load(Ordering::SeqCst) {
                            return Poll::Ready(Err(ChannelError::Closed));
                        }
                        if self.stall.load(Ordering::SeqCst) {
                            return Poll::Pending;
                        }
                        self.writes.lock().unwrap().push(frame.bytes.clone());
                        Poll::Ready(Ok(()))
                    }))
                }
                fn close(&self) {
                    self.closed.store(true, Ordering::SeqCst);
                }
            }

            struct FakeClock {
                now: AtomicU64,
                jump: Mutex<Option<ClockJump>>,
                pause_now: Mutex<Option<(u32, Arc<WritePause>)>>,
                on_read: Mutex<Option<Box<dyn FnOnce() + Send>>>,
            }
            struct ClockJump {
                skip_reads: u32,
                ingress: u64,
                final_time: u64,
            }
            impl FakeClock {
                fn new(now: u64) -> Self {
                    Self {
                        now: AtomicU64::new(now),
                        jump: Mutex::new(None),
                        pause_now: Mutex::new(None),
                        on_read: Mutex::new(None),
                    }
                }
                fn set(&self, now: u64) {
                    self.now.store(now, Ordering::SeqCst);
                    *self.jump.lock().unwrap() = None;
                }
                fn jump_after_reads(&self, skip_reads: u32, ingress: u64, final_time: u64) {
                    assert!(final_time >= ingress);
                    self.now.store(ingress, Ordering::SeqCst);
                    *self.jump.lock().unwrap() = Some(ClockJump {
                        skip_reads,
                        ingress,
                        final_time,
                    });
                }
                fn pause_after_reads(&self, skip_reads: u32, pause: Arc<WritePause>) {
                    *self.pause_now.lock().unwrap() = Some((skip_reads, pause));
                }
                fn on_next_read(&self, callback: impl FnOnce() + Send + 'static) {
                    *self.on_read.lock().unwrap() = Some(Box::new(callback));
                }
            }
            impl MonotonicClock for FakeClock {
                fn now_ms(&self) -> u64 {
                    let callback = self.on_read.lock().unwrap().take();
                    if let Some(callback) = callback {
                        callback();
                    }
                    let pause = {
                        let mut gate = self.pause_now.lock().unwrap();
                        match gate.as_mut() {
                            Some((remaining, _)) if *remaining > 0 => {
                                *remaining -= 1;
                                None
                            }
                            Some(_) => gate.take().map(|(_, pause)| pause),
                            None => None,
                        }
                    };
                    if let Some(pause) = pause {
                        pause.entered.wait();
                        pause.release.wait();
                    }
                    let mut jump = self.jump.lock().unwrap();
                    if let Some(armed) = jump.as_mut() {
                        if armed.skip_reads == 0 {
                            let time = armed.ingress;
                            self.now.store(armed.final_time, Ordering::SeqCst);
                            *jump = None;
                            return time;
                        }
                        armed.skip_reads -= 1;
                    }
                    self.now.load(Ordering::SeqCst)
                }
                fn sleep_until(&self, deadline: u64) -> BoxFuture<'_, ()> {
                    Box::pin(poll_fn(move |_| {
                        if self.now_ms() >= deadline {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        }
                    }))
                }
            }

            #[derive(Default)]
            pub(super) struct FakeSpawner {
                tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
                reject: AtomicBool,
                poll_reader_during_next_spawn: AtomicBool,
                poll_next_spawn_inline: AtomicBool,
                poll_then_reject_next_spawn: AtomicBool,
            }
            impl TaskSpawner for FakeSpawner {
                fn spawn(&self, mut future: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
                    if self.reject.load(Ordering::SeqCst) {
                        return Err(SpawnError::new());
                    }
                    if self
                        .poll_then_reject_next_spawn
                        .swap(false, Ordering::SeqCst)
                    {
                        let _ = poll(future.as_mut());
                        return Err(SpawnError::new());
                    }
                    if !self.poll_next_spawn_inline.swap(false, Ordering::SeqCst)
                        || poll(future.as_mut()).is_pending()
                    {
                        self.tasks.lock().unwrap().push(future);
                    }
                    if self
                        .poll_reader_during_next_spawn
                        .swap(false, Ordering::SeqCst)
                    {
                        self.tick_at(1);
                    }
                    Ok(())
                }
            }
            impl FakeSpawner {
                pub(super) fn tick(&self) {
                    let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
                    for mut task in tasks {
                        if poll(task.as_mut()).is_pending() {
                            self.tasks.lock().unwrap().push(task);
                        }
                    }
                }
                fn tick_at(&self, index: usize) {
                    let mut task = self.tasks.lock().unwrap().remove(index);
                    if poll(task.as_mut()).is_pending() {
                        self.tasks.lock().unwrap().insert(index, task);
                    }
                }
                fn drive<F: Future>(&self, future: F) -> F::Output {
                    let mut future = std::pin::pin!(future);
                    for _ in 0..1000 {
                        if let Poll::Ready(result) = poll(future.as_mut()) {
                            return result;
                        }
                        self.tick();
                    }
                    panic!("manual spawner did not settle");
                }
            }
            fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
                future.poll(&mut Context::from_waker(Waker::noop()))
            }

            pub(super) fn ready() -> (
                Arc<FakeSpawner>,
                Arc<FakeChannel>,
                Arc<crate::extensions::tool_driver::ReadyToolRoute>,
            ) {
                let spawner = Arc::new(FakeSpawner::default());
                let owner = SingleToolOwner::new(
                    spawner.clone(),
                    OwnerLimits {
                        outbound_bytes: nz(8192),
                        receipt_bytes: nz(8192),
                        write_timeout_ms: NonZeroU64::new(10).unwrap(),
                    },
                );
                let channel = Arc::new(FakeChannel::new());
                channel.push(hello());
                channel.push(manifest_with_replay(WireReplay::Safe));
                channel.push(ready_frame());
                let route = spawner
                    .drive(owner.connect(
                        origin1341_host(7, 1, false, true, WireReplay::Safe),
                        channel.clone(),
                        Arc::new(FakeClock::new(200)),
                        PumpConfig {
                            max_read_chunk: nz(8192),
                            write_timeout_ms: NonZeroU64::new(10).unwrap(),
                        },
                    ))
                    .unwrap();
                (spawner, channel, Arc::new(route))
            }
            pub(super) fn declaration() -> AgentTool {
                let c: Value = serde_json::from_str(CATALOG).unwrap();
                let d = &c["entries"][0]["agentTool"];
                AgentTool {
                    name: pi_ai_core::ToolName::new("echo").unwrap(),
                    label: d["label"].as_str().unwrap().into(),
                    description: d["description"].as_str().unwrap().into(),
                    parameters: serde_json::from_value(d["parameters"].clone()).unwrap(),
                    constrained_sampling: None,
                    execution_mode: Some(ToolExecutionMode::Parallel),
                    replay: Some(ToolReplay::Safe),
                }
            }
        }
        type Fx = RuntimeEffects<DropContext, WaitStore, SessionMetadata>;
        type Harness = RuntimeHarness<DropContext, WaitStore, SessionMetadata>;
        struct CountingSpawner {
            pump: Arc<actual::Pump>,
            next: AtomicU64,
        }
        impl pi_ai_core::TaskSpawner for CountingSpawner {
            fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), pi_ai_core::SpawnError> {
                self.next.fetch_add(1, Ordering::SeqCst);
                self.pump.spawn(f)
            }
        }
        struct WaitStore {
            base: actual::Store,
            pump: Arc<actual::Pump>,
            actor: Mutex<std::sync::Weak<Fx>>,
            target: AtomicU64,
            entered: CancellationToken,
            release: CancellationToken,
            reads: AtomicU64,
        }
        impl WaitStore {
            fn new(
                base: Arc<pi_agent_session::memory::MemoryStorage>,
                pump: Arc<actual::Pump>,
            ) -> Self {
                Self {
                    base: actual::Store::new(base, pump.clone()),
                    pump,
                    actor: Mutex::new(std::sync::Weak::new()),
                    target: AtomicU64::new(0),
                    entered: CancellationToken::new(),
                    release: CancellationToken::new(),
                    reads: AtomicU64::new(0),
                }
            }
        }
        impl pi_agent_schema::Storage for WaitStore {
            fn commit(&self, t: Transaction) -> BoxFuture<'_, Result<CommitResult, StorageError>> {
                self.base.commit(t)
            }
            fn get_entries<'a>(
                &'a self,
                ids: &'a [EntryId],
            ) -> BoxFuture<'a, Result<EntryMap, StorageError>> {
                self.base.get_entries(ids)
            }
            fn get_register<'a>(
                &'a self,
                n: RegisterNamespace,
                k: &'a str,
            ) -> BoxFuture<'a, Result<Option<Register>, StorageError>> {
                self.base.get_register(n, k)
            }
            fn list_registers<'a>(
                &'a self,
                n: RegisterNamespace,
                p: Option<&'a str>,
            ) -> BoxFuture<'a, Result<Vec<Register>, StorageError>> {
                self.base.list_registers(n, p)
            }
            fn scan_branch(
                &self,
                q: BranchScan,
            ) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
                self.base.scan_branch(q)
            }
            fn scan_branch_structure(
                &self,
                q: BranchScan,
            ) -> BoxFuture<'_, Result<Vec<EntryStructure>, StorageError>> {
                self.base.scan_branch_structure(q)
            }
            fn scan_entries(
                &self,
                q: EntryScan,
            ) -> BoxFuture<'_, Result<Vec<Entry>, StorageError>> {
                self.base.scan_entries(q)
            }
            fn scan_usage(
                &self,
                q: UsageScan,
            ) -> BoxFuture<'_, Result<Vec<UsageRow>, StorageError>> {
                self.base.scan_usage(q)
            }
            fn stats(&self) -> BoxFuture<'_, Result<SessionStats, StorageError>> {
                self.base.stats()
            }
            fn close(&self) -> BoxFuture<'_, Result<(), StorageError>> {
                self.base.close()
            }
        }
        impl pi_agent_session::storage::AtomicStorage for WaitStore {
            fn commit_conditional(
                &self,
                q: ConditionalCommit,
            ) -> BoxFuture<'_, Result<ConditionalOutcome, StorageError>> {
                self.base.commit_conditional(q)
            }
            fn snapshot(&self) -> BoxFuture<'_, Result<StorageSnapshot, StorageError>> {
                self.base.snapshot()
            }
        }
        impl pi_agent_session::storage::CheckedStorage for WaitStore {
            fn commit_checked<'a, V: PublicationValidator + ?Sized>(
                &'a self,
                q: ConditionalCommit,
                v: &'a V,
            ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>>
            {
                self.base.commit_checked(q, v)
            }
        }
        impl pi_agent_session::storage::RuntimeStorage for WaitStore {
            fn runtime_attachment(&self) -> &pi_agent_session::storage::RuntimeAttachment {
                self.base.runtime_attachment()
            }
            fn read_stamp(&self) -> BoxFuture<'_, Result<ReadStamp, StorageError>> {
                Box::pin(async move {
                    let target = self.target.load(Ordering::SeqCst);
                    if target != 0 && self.pump.current.load(Ordering::SeqCst) == target {
                        let fx = self.actor.lock().unwrap().upgrade();
                        let owned = fx.as_ref().is_some_and(|fx| {
                            fx.origin_consumers.lock().unwrap().iter().any(|(k, l)| {
                                k.item_index == Some(SafeUnsigned::new(1).unwrap()) && l.is_active()
                            })
                        });
                        if owned && self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
                            let trace = std::backtrace::Backtrace::force_capture().to_string();
                            assert!(trace.contains("wait_tool_publications"));
                            println!("E1359_NEXT_FRESH_READ target={target} trace={trace}");
                            self.entered.cancel();
                            self.release.cancelled().await;
                            return Err(StorageError::Io(
                                ErrorContext::new("origin1359 exact fresh wait read error")
                                    .unwrap(),
                            ));
                        }
                    }
                    self.base.read_stamp().await
                })
            }
            fn prepare_entries(
                &self,
                s: ReadStamp,
                p: Vec<PreparedEntrySlot>,
            ) -> BoxFuture<'_, Result<Option<PreparedEntries>, StorageError>> {
                self.base.prepare_entries(s, p)
            }
            fn commit_prepared_checked<'a, V: PublicationValidator + ?Sized>(
                &'a self,
                p: PreparedEntries,
                q: ConditionalCommit,
                v: &'a V,
            ) -> BoxFuture<'a, Result<ConditionalOutcome, CheckedCommitError<V::Error>>>
            {
                self.base.commit_prepared_checked(p, q, v)
            }
        }
        struct Gate {
            entered: CancellationToken,
            closed: Mutex<bool>,
            changed: std::sync::Condvar,
            once: AtomicBool,
            task: AtomicU64,
        }
        impl Gate {
            fn new() -> Self {
                Self {
                    entered: CancellationToken::new(),
                    closed: Mutex::new(true),
                    changed: std::sync::Condvar::new(),
                    once: AtomicBool::new(false),
                    task: AtomicU64::new(0),
                }
            }
            fn open(&self) {
                *self.closed.lock().unwrap() = false;
                self.changed.notify_all();
            }
        }
        struct WaitTool {
            descriptor: AgentTool,
            id: u64,
            gate: Option<Arc<Gate>>,
            pump: Arc<actual::Pump>,
            probe: Arc<dyn Fn(u8, u64) + Send + Sync>,
            exec: AtomicU64,
        }
        impl Drop for WaitTool {
            fn drop(&mut self) {
                (self.probe)(2, self.id);
            }
        }
        impl AgentHarnessTool<DropContext> for WaitTool {
            fn descriptor(&self) -> &AgentTool {
                if let Some(gate) = &self.gate {
                    let trace = std::backtrace::Backtrace::force_capture().to_string();
                    if trace.contains("execute_tool_inner")
                        && trace.contains("prepared")
                        && !gate.once.swap(true, Ordering::SeqCst)
                    {
                        gate.task
                            .store(self.pump.current.load(Ordering::SeqCst), Ordering::SeqCst);
                        gate.entered.cancel();
                        println!(
                            "E1359_REAL_E_CALLBACK task={} trace={trace}",
                            gate.task.load(Ordering::SeqCst)
                        );
                        let lock = gate.closed.lock().unwrap();
                        let (lock, timeout) = gate
                            .changed
                            .wait_timeout_while(lock, Duration::from_secs(20), |closed| *closed)
                            .unwrap();
                        assert!(
                            !timeout.timed_out() && !*lock,
                            "actual E callback released within finite fixture budget"
                        );
                    }
                }
                &self.descriptor
            }
            fn prepare_arguments(
                &self,
                v: pi_ai_core::JsonValue,
            ) -> Result<JsonObject, ToolExecutionError> {
                let pi_ai_core::JsonValue::Object(v) = v else {
                    panic!("object")
                };
                Ok(v)
            }
            fn execute<'a>(
                &'a self,
                _: pi_ai_core::ToolCallId,
                _: JsonObject,
                _: &'a dyn Cancellation,
                _: &'a dyn ToolProgressSink,
                _: &'a DropContext,
            ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
                self.exec.fetch_add(1, Ordering::SeqCst);
                Box::pin(async {
                    Ok(AgentToolResult {
                        content: vec![],
                        details: pi_ai_core::JsonValue::Null,
                        usage: None,
                        added_tool_names: None,
                        terminate: None,
                    })
                })
            }
        }
        struct SwapD {
            harness: Mutex<std::sync::Weak<Harness>>,
            replacements: Vec<AgentHarnessToolHandle<DropContext>>,
        }
        impl HookHandler<BeforeTool> for SwapD {
            fn call(
                &self,
                e: BeforeToolEvent,
            ) -> BoxFuture<'_, Result<Option<BeforeToolResult>, HandlerError>> {
                Box::pin(async move {
                    if e.tool_name.as_str() == "d" {
                        let harness = self.harness.lock().unwrap().upgrade().unwrap();
                        harness.set_tools(self.replacements.clone()).await.unwrap();
                    }
                    Ok(None)
                })
            }
        }
        fn wake_external_operation_terminal(setup: &Setup) {
            // A coherent real external terminal lets old E retire without a lost
            // stamp Fault; D stays armed until this new reader can check fresh state.
            external_terminal(setup);
        }
        fn history(case: u8) {
            let drop_work = case == 1;
            let check_error = case == 2;
            use crate::tools::PublicationPhase;
            let setup = Setup::new();
            let mut response = assistant(&setup.faux.model, pi_ai_core::StopReason::ToolUse);
            response.content = ["echo", "d", "e"]
                .into_iter()
                .enumerate()
                .map(|(i, n)| {
                    pi_ai_core::AssistantContent::ToolCall(pi_ai_core::ToolCall {
                        content_type: Default::default(),
                        id: pi_ai_core::ToolCallId::new(format!("wait-{i}")).unwrap(),
                        name: pi_ai_core::ToolName::new(n).unwrap(),
                        arguments: serde_json::from_str(r#"{"text":"ping"}"#).unwrap(),
                        thought_signature: None,
                        namespace: None,
                    })
                })
                .collect();
            *setup.faux.responses.lock().unwrap() = vec![
                response,
                assistant(&setup.faux.model, pi_ai_core::StopReason::Stop),
            ]
            .into();
            let pump = Arc::new(actual::Pump::default());
            let host = Arc::new(CountingSpawner {
                pump: pump.clone(),
                next: AtomicU64::new(1),
            });
            let store = Arc::new(WaitStore::new(setup.store.clone(), pump.clone()));
            let session = Arc::new(
                pump.drive(pi_agent_session::session::RuntimeSession::new(
                    store.clone(),
                    setup.session.metadata(),
                    Arc::new(
                        pi_agent_session::codec::SessionCodec::new(
                            SessionCodecOptions::default(),
                            ProviderHeaderPolicy::default(),
                        )
                        .unwrap(),
                    ),
                    setup.ids.clone(),
                    Arc::new(pi_telemetry::NOOP_TELEMETRY_CONTEXT),
                ))
                .unwrap(),
            );
            let owner_slot: Arc<
                Mutex<std::sync::Weak<Owner<DropContext, WaitStore, SessionMetadata>>>,
            > = Arc::new(Mutex::new(std::sync::Weak::new()));
            let cell_slot = Arc::new(Mutex::new(std::sync::Weak::<
                crate::tools::ToolPublication<DropContext>,
            >::new()));
            let records = Arc::new(Mutex::new(vec![]));
            let probe: Arc<dyn Fn(u8, u64) + Send + Sync> = {
                let owner_slot = owner_slot.clone();
                let cell_slot = cell_slot.clone();
                let records = records.clone();
                let store = store.clone();
                Arc::new(move |kind, id| {
                    if kind == 2 && id != 1 {
                        return;
                    }
                    if kind == 1 && id == 0 {
                        return;
                    }
                    let owner = owner_slot.lock().unwrap().clone();
                    let cell = cell_slot.lock().unwrap().clone();
                    let fx = store.actor.lock().unwrap().clone();
                    let (tx, rx) = std::sync::mpsc::channel();
                    let worker = std::thread::spawn(move || {
                        let Some(owner) = owner.upgrade() else {
                            let _ = tx.send((false, false));
                            return;
                        };
                        let cell = cell.upgrade();
                        let mut unlocked = owner.tools.try_lock().is_ok()
                            && owner.tool_publications.try_lock().is_ok()
                            && !owner.settings_line.held_for_test()
                            && owner
                                .lanes
                                .lock()
                                .unwrap()
                                .values()
                                .all(|l| !l.line.held_for_test());
                        let completed = cell.as_ref().is_none_or(|c| {
                            let state = c.state.try_lock();
                            unlocked &= state.is_ok();
                            state.is_ok_and(|state| {
                                matches!(
                                    state.phase,
                                    PublicationPhase::Sealed | PublicationPhase::Retired
                                )
                            })
                        });
                        if let Some(fx) = fx.upgrade() {
                            unlocked &= fx.selected_tools.try_lock().is_ok()
                                && fx.origin_consumers.try_lock().is_ok()
                                && fx.contexts.try_lock().is_ok()
                                && fx.bindings.try_lock().is_ok();
                        }
                        tx.send((unlocked, completed)).unwrap();
                    });
                    let observed = rx.recv_timeout(Duration::from_secs(5)).unwrap();
                    worker.join().unwrap();
                    records
                        .lock()
                        .unwrap()
                        .push((kind, id, observed.0, observed.1));
                })
            };
            let gate = Arc::new(Gate::new());
            let base = wire::declaration();
            let create = |name: &str, id, gate| {
                let mut descriptor = base.clone();
                descriptor.name = pi_ai_core::ToolName::new(name).unwrap();
                Arc::new(WaitTool {
                    descriptor,
                    id,
                    gate,
                    pump: pump.clone(),
                    probe: probe.clone(),
                    exec: AtomicU64::new(0),
                })
            };
            let echo = create("echo", 0, None);
            let d = create("d", 1, None);
            let weak_d = Arc::downgrade(&d);
            let replacement = create("d", 3, None);
            let e = create("e", 2, Some(gate.clone()));
            let factory = RuntimeFactory::new(
                setup.time.clone(),
                host.clone(),
                setup.ids.clone(),
                Arc::new(DropContext {
                    id: 0,
                    probe: Arc::new(|_, _| {}),
                }),
                Arc::new(Summary),
            );
            let mut options = setup.options_for::<DropContext, _>(session, DriveMode::Automatic);
            options.tool_execution = ToolExecutionMode::Parallel;
            options.tools = vec![echo.clone(), d.clone(), e.clone()];
            options.tool_context = Some(AgentHarnessToolContextSource::Provider(Arc::new(
                Contexts {
                    next: AtomicU64::new(1),
                    probe: probe.clone(),
                },
            )));
            let harness = pump
                .drive(factory.create_configured(options))
                .unwrap()
                .harness;
            *owner_slot.lock().unwrap() = Arc::downgrade(&harness.main.owner);
            let (wire_pump, _channel, route) = wire::ready();
            pump.drive(harness.register_ready_echo_tool(
                echo.clone(),
                route.clone(),
                crate::extensions::tool_driver::EchoRouteTiming {
                    method_limit_ms: std::num::NonZeroU64::new(1000).unwrap(),
                    connection_deadline_ms: 10000,
                },
            ))
            .unwrap()
            .unwrap();
            pump.drive(
                harness.set_active_tools(
                    ["echo", "d", "e"]
                        .map(|n| pi_ai_core::ToolName::new(n).unwrap())
                        .to_vec(),
                ),
            )
            .unwrap();
            let swap = Arc::new(SwapD {
                harness: Mutex::new(Arc::downgrade(&harness)),
                replacements: vec![echo, replacement, e.clone()],
            });
            let sub = harness.hooks().before_tool(None, swap).unwrap();
            drop(d);
            let keeper: Arc<Mutex<Option<Arc<Fx>>>> = Arc::new(Mutex::new(None));
            let observer: Arc<Observer> = {
                let keeper = keeper.clone();
                let owner = Arc::downgrade(&harness.main.owner);
                let store = store.clone();
                Arc::new(move |value| {
                    if let Some(fx) = value.downcast_ref::<Arc<Fx>>()
                        && owner
                            .upgrade()
                            .is_some_and(|o| Arc::ptr_eq(&o, &fx.lane.owner))
                    {
                        *store.actor.lock().unwrap() = Arc::downgrade(fx);
                        if store.target.load(Ordering::SeqCst) != 0 {
                            *keeper.lock().unwrap() = Some(fx.clone());
                        }
                    }
                })
            };
            OBSERVERS
                .get_or_init(Mutex::default)
                .lock()
                .unwrap()
                .push(Arc::downgrade(&observer));
            let mut prompt = Box::pin(harness.prompt(PromptInput::Text {
                text: "wait three".into(),
                images: vec![],
            }));
            assert!(poll(prompt.as_mut()).is_pending());
            let worker_pump = pump.clone();
            let worker_gate = gate.clone();
            let worker = std::thread::spawn(move || {
                for _ in 0..20000 {
                    worker_pump.tick();
                    if worker_gate.entered.is_cancelled() {
                        return;
                    }
                }
                panic!("actual E callback not reached")
            });
            for _ in 0..5000 {
                wire_pump.tick();
                if gate.entered.is_cancelled() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            if !gate.entered.is_cancelled() {
                gate.open();
                worker.join().unwrap();
                println!(
                    "E1359_SETUP_STATE prompt={:?} owner={:?} snapshot={:?}",
                    poll(prompt.as_mut()),
                    pump.drive(harness.get_tools()).map(|t| t.len()),
                    pump.drive(setup.store.snapshot())
                );
                panic!("real E outside-pair callback not reached");
            }
            route.close();
            let original = pump.drive(prompt);
            assert!(
                matches!(original, Err(HarnessRuntimeError::PeerLost)),
                "original recoverable release: {original:?}"
            );
            assert!(pump.drive(harness.get_tools()).is_ok());
            let dcell = harness
                .main
                .owner
                .tool_publications
                .lock()
                .unwrap()
                .values()
                .find(|c| c.plan.source_index.get() == 1)
                .cloned()
                .unwrap();
            let ecell = harness
                .main
                .owner
                .tool_publications
                .lock()
                .unwrap()
                .values()
                .find(|c| c.plan.source_index.get() == 2)
                .cloned()
                .unwrap();
            assert!(matches!(
                dcell.state.lock().unwrap().phase,
                PublicationPhase::ReadyDirect
            ));
            assert!(matches!(
                ecell.state.lock().unwrap().phase,
                PublicationPhase::ConsumerDirect { .. }
            ));
            *cell_slot.lock().unwrap() = Arc::downgrade(&dcell);
            if check_error {
                let key = ecell.plan.args_key.clone();
                let Some(Register::OperationToolArguments { value, .. }) = pump
                    .drive(
                        setup
                            .session
                            .get_register(RegisterNamespace::OperationToolArguments, &key),
                    )
                    .unwrap()
                else {
                    panic!("actual E args")
                };
                pump.drive(setup.session.commit(Transaction {
                    writes: vec![Write::SetRegister(Box::new(
                        RegisterSet::OperationToolArguments { key, value },
                    ))],
                }))
                .unwrap();
            }
            let task = host.next.load(Ordering::SeqCst);
            store.target.store(task, Ordering::SeqCst);
            let mut resume = Box::pin(harness.resume());
            assert!(poll(resume.as_mut()).is_pending());
            pump.tick();
            let fx = keeper.lock().unwrap().as_ref().unwrap().clone();
            if !check_error {
                assert!(matches!(
                    dcell.state.lock().unwrap().phase,
                    PublicationPhase::ConsumerDirect { .. }
                ));
                assert!(
                    fx.origin_consumers
                        .lock()
                        .unwrap()
                        .values()
                        .any(|l| !l.is_work() && l.is_active())
                );
            } else {
                assert!(
                    fx.origin_consumers
                        .lock()
                        .unwrap()
                        .keys()
                        .any(|k| k.item_index == Some(SafeUnsigned::new(1).unwrap())),
                    "actual D was claimed before E receipt check"
                );
            }
            println!(
                "E1359_D_CLAIMED_E_WAITED drop_work={drop_work} D={:?} E={:?} resume_task={task} e_task={}",
                dcell.plan.key,
                ecell.plan.key,
                gate.task.load(Ordering::SeqCst)
            );
            if !check_error {
                wake_external_operation_terminal(&setup);
            }
            gate.open();
            worker.join().unwrap();
            if !check_error {
                for _ in 0..2000 {
                    pump.tick();
                    if store.entered.is_cancelled() {
                        break;
                    }
                }
                if !store.entered.is_cancelled() {
                    println!(
                        "E1359_SECOND_READ_SETUP target={} current={} reads={} owner_error={:?} resume={:?}",
                        store.target.load(Ordering::SeqCst),
                        pump.current.load(Ordering::SeqCst),
                        store.reads.load(Ordering::SeqCst),
                        harness
                            .main
                            .owner
                            .admission
                            .check()
                            .err()
                            .map(|e| e.to_string()),
                        poll(resume.as_mut())
                    );
                    store.release.cancel();
                    sub.unsubscribe();
                    drop(observer);
                    drop(fx);
                    drop(ecell);
                    let close = pump.drive(harness.close());
                    for _ in 0..100 {
                        pump.tick();
                        wire_pump.tick();
                    }
                    println!(
                        "E1359_SETUP_CLEANUP close={close:?} tasks={}",
                        pump.pending_tasks()
                    );
                    panic!("actual second protected read not reached");
                }
            }
            let pubdone = dcell.publication_done.clone();
            let handoff = dcell.test_handoff_signal();
            let mut prior_close = None;
            if case == 3 {
                let _ = harness
                    .main
                    .owner
                    .observe::<()>(Err(crate::restore::fault("origin1359 prior fault")));
            }
            if case == 4 {
                let mut closed = Box::pin(harness.close());
                assert!(poll(closed.as_mut()).is_pending());
                prior_close = Some(closed);
            }
            if drop_work {
                pump.drop_task(task);
            } else if !check_error {
                store.release.cancel();
                pump.tick();
            }
            let result = pump.drive(resume);
            let phase = match dcell.state.lock().unwrap().phase {
                PublicationPhase::Sealed => "Sealed",
                PublicationPhase::Retired => "Retired",
                PublicationPhase::Sealing(_) => "Sealing",
                _ => "other",
            };
            let completed = phase == "Sealed" || (case == 4 && phase == "Retired");
            let signals = (pubdone.is_cancelled(), handoff.is_cancelled());
            drop(fx);
            drop(keeper.lock().unwrap().take());
            drop(ecell);
            sub.unsubscribe();
            drop(observer);
            drop(e);
            let close = if let Some(closed) = prior_close {
                pump.drive(closed)
            } else {
                pump.drive(harness.close())
            };
            for _ in 0..100 {
                pump.tick();
                wire_pump.tick();
            }
            let unresolved = harness
                .main
                .owner
                .tool_publications
                .lock()
                .unwrap()
                .values()
                .filter(|c| {
                    matches!(
                        c.state.lock().unwrap().phase,
                        PublicationPhase::Publishing(_)
                            | PublicationPhase::ConsumerDirect { .. }
                            | PublicationPhase::Sealing(_)
                    )
                })
                .count();
            let dropped = weak_d.upgrade().is_none();
            let observed = records.lock().unwrap().clone();
            println!(
                "E1359_COMPLETION message={} drop_work={drop_work} result={result:?} completed={completed} phase={phase} signals={signals:?} actual_final_d={dropped} records={observed:?} close={close:?} unresolved={unresolved} tasks={}",
                result
                    .as_ref()
                    .err()
                    .map(|e| e.to_string())
                    .unwrap_or_default(),
                pump.pending_tasks()
            );
            assert!(
                completed,
                "outer admission Fault is not exact cell completion"
            );
            assert_eq!(signals, (true, true));
            assert_eq!(unresolved, 0);
            assert_eq!(pump.pending_tasks(), 0);
            assert!(dropped);
            assert!(observed.iter().any(|r| r.0 == 2 && r.1 == 1));
            assert!(observed.iter().any(|r| r.0 == 1));
            assert!(observed.iter().all(|r| r.2 && r.3));
            assert!(matches!(result, Err(HarnessRuntimeError::Fault(_))));
            let expected = match case {
                0 => Some("runtime read stamp failed"),
                2 => Some("original committed tool arguments changed"),
                3 => Some("origin1359 prior fault"),
                4 => Some("runtime read stamp failed"),
                _ => None,
            };
            if let Some(expected) = expected {
                assert_eq!(result.unwrap_err().to_string(), expected);
            }
        }
        #[test]
        fn actual_owned_wait_next_read_error_finishes_exact_cell() {
            history(0)
        }
        #[test]
        fn actual_owned_wait_retained_drop_finishes_exact_cell() {
            history(1)
        }
        #[test]
        fn actual_owned_wait_new_claim_receipt_error_finishes_exact_cell() {
            history(2)
        }
        #[test]
        fn actual_owned_wait_prior_fault_preserves_first_error() {
            history(3)
        }
        #[test]
        fn actual_owned_wait_prior_close_keeps_admitted_error_and_completion() {
            history(4)
        }
    }
}
