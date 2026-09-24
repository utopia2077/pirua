//! Shared phase consumers with actual bindings, durable arguments and batch context.
use crate::{
    effects::{RuntimeEffects, TokenCancellation},
    owner::Telemetry,
    restore::HydratedLane,
};
use pi_agent_schema::*;
use pi_agent_session::storage::RuntimeStorage;
use pi_ai_core::{JsonObject, Presence, SafeUnsigned};
use pi_harness_core::planner::{PlanningFacts, ToolDeclaration};
use std::{collections::BTreeMap, sync::Arc};

#[cfg(feature = "extensions")]
use crate::extensions::tool_driver::{
    CallOutcome, DriverError, LocalAbortCause, ReadyToolRoute, TypedTerminal,
};
#[cfg(feature = "extensions")]
use crate::owner::EchoRouteSnapshot;
#[cfg(feature = "extensions")]
use pi_extension_protocol::tool_live::{InvocationBinding, LivePhase, RetireReason};
#[cfg(feature = "extensions")]
use std::{future::Future, pin::pin, sync::Mutex, task::Poll};

#[cfg(feature = "extensions")]
pub(crate) struct PairedPermits(Option<crate::scheduler::PairPermit>);

#[cfg(feature = "extensions")]
impl PairedPermits {
    pub(crate) fn new(pair: crate::scheduler::PairPermit) -> Self {
        Self(Some(pair))
    }
    pub(crate) async fn acquire(
        settings: &crate::scheduler::MutationLine,
        lane: &crate::scheduler::MutationLine,
    ) -> Result<Self, HarnessRuntimeError> {
        Ok(Self::new(settings.acquire_pair(lane).await?))
    }
    pub(crate) fn release(&mut self) {
        if let Some(pair) = self.0.take() {
            pair.release();
        }
    }
}

#[cfg(feature = "extensions")]
impl Drop for PairedPermits {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(all(test, feature = "extensions"))]
mod paired_permit_tests {
    use super::PairedPermits;
    use crate::scheduler::{Admission, MutationLine};
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    struct CountWake(AtomicUsize);
    impl Wake for CountWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn poll<F: Future + ?Sized>(future: Pin<&mut F>, waker: &Waker) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(waker))
    }

    #[test]
    fn dropping_pending_load_releases_both_lines_and_wakes_waiters() {
        let admission = Admission::new();
        let settings = MutationLine::new(admission.clone());
        let lane = MutationLine::new(admission);
        let mut load = Box::pin({
            let settings = settings.clone();
            let lane = lane.clone();
            async move {
                let pair = settings.acquire_pair(&lane).await.unwrap();
                let _both = PairedPermits::new(pair);
                std::future::pending::<()>().await;
            }
        });
        assert!(poll(load.as_mut(), Waker::noop()).is_pending());
        let settings_wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let lane_wake = Arc::new(CountWake(AtomicUsize::new(0)));
        let mut settings_waiter = Box::pin(settings.acquire());
        let mut lane_waiter = Box::pin(lane.acquire());
        assert!(
            poll(
                settings_waiter.as_mut(),
                &Waker::from(settings_wake.clone())
            )
            .is_pending()
        );
        assert!(poll(lane_waiter.as_mut(), &Waker::from(lane_wake.clone())).is_pending());
        drop(load);
        assert_eq!(settings_wake.0.load(Ordering::SeqCst), 1);
        assert_eq!(lane_wake.0.load(Ordering::SeqCst), 1);
        let Poll::Ready(Ok(settings_permit)) = poll(settings_waiter.as_mut(), Waker::noop()) else {
            panic!("settings released")
        };
        let Poll::Ready(Ok(lane_permit)) = poll(lane_waiter.as_mut(), Waker::noop()) else {
            panic!("lane released")
        };
        drop(settings_permit);
        drop(lane_permit);
    }

    #[test]
    fn queued_routed_precheck_and_safe_resume_acquisition_cancel_or_seal() {
        use pi_agent_schema::{ErrorContext, HarnessFault, HarnessRuntimeError};

        // Both routed precheck and Safe resume call this exact acquisition
        // helper before any state read or side effect.
        for seal in [false, true] {
            let admission = Admission::new();
            let settings = MutationLine::new(admission.clone());
            let lane = MutationLine::new(admission.clone());
            let mut occupied = Box::pin(lane.acquire());
            let Poll::Ready(Ok(lane_holder)) = poll(occupied.as_mut(), Waker::noop()) else {
                panic!("lane holder")
            };
            let notified = Arc::new(CountWake(AtomicUsize::new(0)));
            let pair_waker = Waker::from(notified.clone());
            let mut route = Box::pin(PairedPermits::acquire(&settings, &lane));
            assert!(poll(route.as_mut(), &pair_waker).is_pending());
            assert!(!settings.held_for_test());
            let mut next_settings = Box::pin(settings.acquire());
            assert!(poll(next_settings.as_mut(), Waker::noop()).is_pending());
            if seal {
                admission.seal_fault(HarnessFault::new(
                    ErrorContext::new("queued route seal").unwrap(),
                ));
                assert_eq!(notified.0.load(Ordering::SeqCst), 1);
                assert!(matches!(
                    poll(route.as_mut(), Waker::noop()),
                    Poll::Ready(Err(HarnessRuntimeError::Fault(_)))
                ));
                assert!(matches!(
                    poll(next_settings.as_mut(), Waker::noop()),
                    Poll::Ready(Err(HarnessRuntimeError::Fault(_)))
                ));
            } else {
                drop(route);
                let Poll::Ready(Ok(successor)) = poll(next_settings.as_mut(), Waker::noop()) else {
                    panic!("cancelled route passed its settings position")
                };
                drop(successor);
            }
            assert!(!settings.held_for_test());
            drop(lane_holder);
        }
    }
}

#[cfg(feature = "extensions")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RoutedDisposition {
    NoLongerExpected,
    UnadmittedLocalAbort,
    PeerLost,
    HostFault,
}

#[cfg(feature = "extensions")]
pub(crate) struct RoutedEffect {
    pub(crate) route: Arc<ReadyToolRoute>,
    pub(crate) binding: Option<InvocationBinding>,
    pub(crate) disposition: Option<RoutedDisposition>,
    pub(crate) terminal: bool,
    pub(crate) entered: bool,
    pub(crate) owner_released: bool,
    cleanup: Arc<CleanupArbiter>,
}

#[cfg(feature = "extensions")]
pub(crate) struct CleanupArbiter {
    state: Mutex<CleanupStatus>,
}

#[cfg(feature = "extensions")]
enum CleanupStatus {
    Ready,
    Running(pi_ai_core::CancellationToken),
    Complete,
    Failed,
}

#[cfg(feature = "extensions")]
impl CleanupArbiter {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(CleanupStatus::Ready),
        })
    }

    fn ready(&self) -> bool {
        matches!(
            *self.state.lock().unwrap_or_else(|p| p.into_inner()),
            CleanupStatus::Ready
        )
    }

    pub(crate) async fn enter(self: &Arc<Self>) -> Result<Option<CleanupLease>, ()> {
        loop {
            let pending = {
                let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                match &*state {
                    CleanupStatus::Ready => {
                        *state = CleanupStatus::Running(pi_ai_core::CancellationToken::new());
                        return Ok(Some(CleanupLease {
                            arbiter: self.clone(),
                            finished: false,
                        }));
                    }
                    CleanupStatus::Running(done) => Some(done.clone()),
                    CleanupStatus::Complete => return Ok(None),
                    CleanupStatus::Failed => return Err(()),
                }
            };
            pending.expect("running cleanup token").cancelled().await;
        }
    }

    fn finish(&self, success: bool) {
        let done = {
            let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
            let old = std::mem::replace(
                &mut *state,
                if success {
                    CleanupStatus::Complete
                } else {
                    CleanupStatus::Failed
                },
            );
            let CleanupStatus::Running(done) = old else {
                panic!("cleanup owner must finish only its own reservation")
            };
            done
        };
        done.cancel();
    }
}

#[cfg(feature = "extensions")]
pub(crate) struct CleanupLease {
    arbiter: Arc<CleanupArbiter>,
    finished: bool,
}

#[cfg(feature = "extensions")]
impl CleanupLease {
    fn complete(mut self) {
        self.finished = true;
        self.arbiter.finish(true);
    }
}

#[cfg(feature = "extensions")]
impl Drop for CleanupLease {
    fn drop(&mut self) {
        if !self.finished {
            self.arbiter.finish(false);
        }
    }
}

#[cfg(all(test, feature = "extensions"))]
mod cleanup_arbiter_tests {
    use super::CleanupArbiter;
    use std::{
        future::Future,
        pin::Pin,
        sync::{Arc, Barrier},
        task::{Context, Poll, Waker},
    };

    fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
        future.poll(&mut Context::from_waker(Waker::noop()))
    }

    #[test]
    fn overlapping_cleanup_waits_for_exact_owner_completion() {
        let arbiter = CleanupArbiter::new();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let first_arbiter = arbiter.clone();
        let first_entered = entered.clone();
        let first_release = release.clone();
        let first = std::thread::spawn(move || {
            let mut claim = Box::pin(first_arbiter.enter());
            let Poll::Ready(Ok(Some(lease))) = poll(claim.as_mut()) else {
                panic!("first cleanup must own the exact reservation")
            };
            first_entered.wait();
            first_release.wait();
            lease.complete();
        });
        entered.wait();
        let mut second = Box::pin(arbiter.enter());
        assert!(
            poll(second.as_mut()).is_pending(),
            "a second exit cannot report completed cleanup at entry"
        );
        release.wait();
        first.join().unwrap();
        assert!(matches!(poll(second.as_mut()), Poll::Ready(Ok(None))));

        let failed = CleanupArbiter::new();
        let mut first = Box::pin(failed.enter());
        let Poll::Ready(Ok(Some(lease))) = poll(first.as_mut()) else {
            panic!("first claim")
        };
        let mut second = Box::pin(failed.enter());
        assert!(poll(second.as_mut()).is_pending());
        drop(lease);
        assert!(
            matches!(poll(second.as_mut()), Poll::Ready(Err(()))),
            "a failed owner transition is never counted as completed"
        );
    }
}

#[cfg(feature = "extensions")]
fn route_error(message: &'static str) -> ToolExecutionError {
    ToolExecutionError::new(ErrorContext::new(message).expect("static route error"))
}

#[cfg(feature = "extensions")]
struct RoutedTool<C: Send + Sync + 'static, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: Arc<RuntimeEffects<C, S, M>>,
    plan: ToolEffectPlan<Telemetry>,
    saved: CapturedArguments,
    source_id: pi_ai_core::ToolCallId,
    original: AgentHarnessToolHandle<C>,
    snapshot: EchoRouteSnapshot<C>,
    state: Arc<Mutex<RoutedEffect>>,
}

#[cfg(feature = "extensions")]
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    AgentHarnessTool<C> for RoutedTool<C, S, M>
{
    fn descriptor(&self) -> &AgentTool {
        self.original.descriptor()
    }

    fn prepare_arguments(
        &self,
        args: pi_ai_core::JsonValue,
    ) -> Result<JsonObject, ToolExecutionError> {
        self.original.prepare_arguments(args)
    }

    fn execute<'a>(
        &'a self,
        tool_call_id: pi_ai_core::ToolCallId,
        params: JsonObject,
        cancellation: &'a dyn Cancellation,
        _progress: &'a dyn ToolProgressSink,
        _context: &'a C,
    ) -> BoxFuture<'a, Result<AgentToolResult, ToolExecutionError>> {
        self.state.lock().unwrap_or_else(|p| p.into_inner()).entered = true;
        Box::pin(async move {
            if tool_call_id != self.source_id || params != self.saved.value {
                return Err(route_error("extension tool call changed before admission"));
            }
            // This guard pairs release even if an awaited Session read is
            // cancelled and the wrapper future is dropped at that point.
            let mut permits =
                PairedPermits::acquire(&self.fx.lane.owner.settings_line, &self.fx.lane.lane.line)
                    .await
                    .map_err(|_| route_error("extension tool admission unavailable"))?;
            let h = match self.fx.lane.load_raw().await {
                Ok(h) => h,
                Err(error) => {
                    permits.release();
                    let _ = self.fx.lane.owner.observe::<()>(Err(error));
                    return Err(route_error("extension tool state unavailable"));
                }
            };
            let current_key = h.current.as_ref().is_some_and(|current| {
                pi_harness_core::tool_batch::expects_effect(
                    current,
                    &self.fx.lane.owner.session.metadata().base_metadata().id,
                    &self.plan,
                )
            });
            let running = h.current.as_ref().is_some_and(|current| {
                matches!(&current.state, OperationState::Run(run)
                    if matches!(run.control, Control::Running(_)))
            });
            let cancelled = h.current.as_ref().is_some_and(|current| {
                matches!(&current.state, OperationState::Run(run)
                    if matches!(run.control, Control::CancelRequested(_)))
            });
            let arguments_match = captured_arguments_match(&h, &self.saved);
            if !current_key || !running || !arguments_match {
                if current_key && cancelled && arguments_match {
                    self.state
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .disposition = Some(RoutedDisposition::UnadmittedLocalAbort);
                }
                return Err(route_error(
                    "extension tool effect changed before admission",
                ));
            }
            let selected = self
                .fx
                .lane
                .owner
                .tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .echo_is_current(&self.snapshot);
            if !selected {
                return Err(route_error("extension tool route was replaced"));
            }
            let prepared = self.snapshot.route.prepare_planned_with_bounds(
                &self.plan,
                &self.saved.value,
                self.source_id.as_str(),
                None,
                None,
            );
            if let Ok(prepared) = &prepared {
                self.state.lock().unwrap_or_else(|p| p.into_inner()).binding =
                    Some(prepared.binding().clone());
                self.fx
                    .lane
                    .owner
                    .routed_pending
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(self.plan.key.clone());
            }
            permits.release();
            let prepared = prepared.map_err(|error| {
                if error == DriverError::HostFault {
                    self.state
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .disposition = Some(RoutedDisposition::HostFault);
                }
                route_error("extension tool admission failed")
            })?;
            let binding = prepared.binding().clone();
            let handle = prepared.activate().map_err(|error| {
                if error == DriverError::HostFault {
                    let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    state.disposition = Some(RoutedDisposition::HostFault);
                    state.owner_released = true;
                }
                route_error("extension tool activation failed")
            })?;
            let mut outcome = pin!(handle.outcome());
            let mut cancelled = pin!(cancellation.cancelled());
            let first = std::future::poll_fn(|cx| {
                if let Poll::Ready(value) = outcome.as_mut().poll(cx) {
                    return Poll::Ready(Some(value));
                }
                if cancelled.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(None);
                }
                Poll::Pending
            })
            .await;
            if first.is_none() {
                let line = self
                    .fx
                    .lane
                    .lane
                    .line
                    .acquire()
                    .await
                    .map_err(|_| route_error("extension tool cancellation unavailable"))?;
                let h = match self.fx.lane.load_raw().await {
                    Ok(h) => h,
                    Err(error) => {
                        drop(line);
                        let _ = self.fx.lane.owner.observe::<()>(Err(error));
                        return Err(route_error("extension tool cancellation state unavailable"));
                    }
                };
                let pending = h.current.as_ref().is_some_and(|current| {
                    pi_harness_core::tool_batch::expects_effect(current,
                        &self.fx.lane.owner.session.metadata().base_metadata().id, &self.plan)
                        && matches!(&current.state, OperationState::Run(run) if matches!(run.control, Control::CancelRequested(_)))
                });
                drop(line);
                if pending {
                    let _ = self
                        .snapshot
                        .route
                        .owner()
                        .abort_local(&binding, LocalAbortCause::User);
                }
            }
            let outcome = match first {
                Some(value) => value,
                None => outcome.await,
            };
            match outcome {
                Ok(CallOutcome::Terminal(TypedTerminal::Result(result))) => {
                    self.state
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .terminal = true;
                    Ok(result)
                }
                Ok(CallOutcome::Terminal(TypedTerminal::PeerError(code))) => {
                    self.state
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .terminal = true;
                    Err(ToolExecutionError::new(
                        ErrorContext::new(format!("extension tool peer error: {code:?}"))
                            .expect("bounded peer error code"),
                    ))
                }
                Ok(CallOutcome::NoLongerExpected) | Err(DriverError::NoLongerExpected) => {
                    let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    state.disposition = Some(RoutedDisposition::NoLongerExpected);
                    state.owner_released = true;
                    Err(route_error("extension tool effect no longer expected"))
                }
                Ok(CallOutcome::UnadmittedLocalAbort) | Err(DriverError::UnadmittedLocalAbort) => {
                    let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
                    state.disposition = Some(RoutedDisposition::UnadmittedLocalAbort);
                    state.owner_released = true;
                    Err(route_error("extension tool call locally aborted"))
                }
                Err(
                    DriverError::PeerLost
                    | DriverError::MalformedPeer
                    | DriverError::Deadline
                    | DriverError::Closed,
                ) => {
                    self.state
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .disposition = Some(RoutedDisposition::PeerLost);
                    Err(route_error("extension tool peer lost"))
                }
                Err(DriverError::HostFault) => {
                    self.state
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .disposition = Some(RoutedDisposition::HostFault);
                    Err(route_error("extension tool host fault"))
                }
                Err(_) => Err(route_error("extension tool call failed")),
            }
        })
    }
}

#[cfg(test)]
fn saved_arguments_match(register: Option<&Register>, key: &str, args: &JsonObject) -> bool {
    matches!(register, Some(Register::OperationToolArguments { key: saved_key, value, .. })
        if saved_key == key && value == args)
}

#[derive(Clone)]
pub(crate) struct CapturedArguments {
    pub key: String,
    pub value: JsonObject,
    pub seq: SafeUnsigned,
}

pub(crate) struct CapturedToolCall<C> {
    pub arguments: CapturedArguments,
    pub prepared: Option<(AgentToolCall, AgentHarnessToolHandle<C>, Arc<C>)>,
}

fn hydrated_arguments(h: &HydratedLane, key: &str) -> Option<CapturedArguments> {
    let Register::OperationToolArguments {
        key: saved_key,
        value,
        seq,
    } = h
        .registers
        .get(&(RegisterNamespace::OperationToolArguments, key.to_owned()))?
    else {
        return None;
    };
    (saved_key == key).then(|| CapturedArguments {
        key: key.to_owned(),
        value: value.clone(),
        seq: *seq,
    })
}

pub(crate) fn captured_arguments_match(h: &HydratedLane, captured: &CapturedArguments) -> bool {
    h.registers
        .get(&(
            RegisterNamespace::OperationToolArguments,
            captured.key.clone(),
        ))
        .is_some_and(|register| {
            matches!(register, Register::OperationToolArguments { key, value, seq }
                if key == &captured.key && value == &captured.value && seq == &captured.seq)
        })
}

impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    RuntimeEffects<C, S, M>
{
    #[cfg(feature = "extensions")]
    pub(crate) fn require_routed_terminal(
        &self,
        key: &EffectKey,
    ) -> Result<(), HarnessRuntimeError> {
        // Release the process-local cell before entering the owner line. The
        // owner's exact opaque identity, not the local terminal latch or route
        // generation, authorizes a routed ToolRaw and checked settlement.
        let state = self
            .routed_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .cloned();
        let Some(state) = state else { return Ok(()) };
        let (route, binding, terminal, cleanup) = {
            let state = state.lock().unwrap_or_else(|p| p.into_inner());
            (
                state.route.clone(),
                state.binding.clone(),
                state.terminal,
                state.cleanup.clone(),
            )
        };
        let available = cleanup.ready();
        let Some(binding) = binding else {
            return if terminal || !available {
                Err(crate::restore::fault("routed terminal binding absent"))
            } else {
                // A rejected preclaim has no B1 and remains an ordinary
                // content-free in-band tool error.
                Ok(())
            };
        };
        if !terminal
            || !available
            || route.owner().binding_phase(&binding) != Ok(LivePhase::TerminalReceived)
        {
            return Err(crate::restore::fault(
                "routed terminal binding is no longer retained",
            ));
        }
        Ok(())
    }

    #[cfg(feature = "extensions")]
    pub(crate) async fn routed_exit(
        &self,
        plan: &ToolEffectPlan<Telemetry>,
    ) -> Result<Option<RoutedDisposition>, HarnessRuntimeError> {
        self.lane.owner.admission.check()?;
        let line = self.lane.lane.line.acquire().await?;
        let h = self.lane.load_raw().await?;
        let expected = h.current.as_ref().is_some_and(|current| {
            pi_harness_core::tool_batch::expects_effect(
                current,
                &self.lane.owner.session.metadata().base_metadata().id,
                plan,
            )
        });
        let cancelled = h.current.as_ref().is_some_and(|current| {
            matches!(&current.state, OperationState::Run(run) if matches!(run.control, Control::CancelRequested(_)))
        });
        let state = self
            .routed_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&plan.key)
            .cloned();
        let Some(state) = state else { return Ok(None) };
        let mut state = state.lock().unwrap_or_else(|p| p.into_inner());
        if !expected {
            state.disposition = Some(RoutedDisposition::NoLongerExpected);
        } else {
            let captured = self
                .captured_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&plan.key)
                .map(|call| call.arguments.clone())
                .ok_or_else(|| crate::restore::fault("routed tool capture absent"))?;
            if !captured_arguments_match(&h, &captured) {
                return Err(crate::restore::fault("saved routed tool arguments changed"));
            }
            if cancelled && !state.entered {
                state.disposition = Some(RoutedDisposition::UnadmittedLocalAbort);
            }
            if state.terminal {
                state.disposition = None;
            }
        }
        let result = state.disposition;
        let claimed = state.binding.is_some();
        drop(state);
        if result.is_none() && claimed {
            self.require_routed_terminal(&plan.key)?;
        }
        drop(line);
        Ok(result)
    }

    #[cfg(feature = "extensions")]
    pub(crate) async fn classify_skipped_routed_abort(
        &self,
        plan: &ToolEffectPlan<Telemetry>,
    ) -> Result<(), HarnessRuntimeError> {
        let mut permits =
            PairedPermits::acquire(&self.lane.owner.settings_line, &self.lane.lane.line).await?;
        let h = self.lane.load_raw().await?;
        let expected = h.current.as_ref().is_some_and(|current| {
            pi_harness_core::tool_batch::expects_effect(
                current,
                &self.lane.owner.session.metadata().base_metadata().id,
                plan,
            ) && matches!(&current.state, OperationState::Run(run)
                    if matches!(run.control, Control::CancelRequested(_)))
        });
        if !expected || hydrated_arguments(&h, &plan.args_key).is_none() {
            return Ok(());
        }
        let turn = StepId::new(plan.key.step_or_task_id.as_str())
            .map_err(|_| crate::restore::fault("routed tool turn invalid"))?;
        let candidate = self
            .bindings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(turn, plan.source_index))
            .cloned();
        let Some(candidate) = candidate else {
            return Ok(());
        };
        let snapshot = self
            .lane
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .echo_snapshot(&candidate);
        if let Some(snapshot) = snapshot {
            self.routed_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .entry(plan.key.clone())
                .or_insert_with(|| {
                    Arc::new(Mutex::new(RoutedEffect {
                        route: snapshot.route,
                        binding: None,
                        disposition: Some(RoutedDisposition::UnadmittedLocalAbort),
                        terminal: false,
                        entered: false,
                        owner_released: false,
                        cleanup: CleanupArbiter::new(),
                    }))
                });
        }
        permits.release();
        Ok(())
    }

    #[cfg(feature = "extensions")]
    pub(crate) async fn cleanup_routed(
        &self,
        key: &EffectKey,
        committed: bool,
    ) -> Result<(), HarnessRuntimeError> {
        self.cleanup_routed_with_policy(key, committed, false).await
    }

    #[cfg(feature = "extensions")]
    pub(crate) async fn cleanup_routed_raw(
        &self,
        key: &EffectKey,
        committed: bool,
    ) -> Result<(), HarnessRuntimeError> {
        self.cleanup_routed_with_policy(key, committed, true).await
    }

    #[cfg(feature = "extensions")]
    async fn cleanup_routed_with_policy(
        &self,
        key: &EffectKey,
        committed: bool,
        raw: bool,
    ) -> Result<(), HarnessRuntimeError> {
        let state = self
            .routed_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .cloned();
        let Some(state) = state else { return Ok(()) };
        let (route, binding, disposition, owner_released, cleanup) = {
            let state = state.lock().unwrap_or_else(|p| p.into_inner());
            (
                state.route.clone(),
                state.binding.clone(),
                state.disposition,
                state.owner_released,
                state.cleanup.clone(),
            )
        };
        // A competing external-completion exit must wait for the first exact
        // owner transition and local view cleanup; entering is not success.
        let Some(lease) = cleanup
            .enter()
            .await
            .map_err(|()| self.line_fault(raw, "routed tool cleanup previously failed"))?
        else {
            return Ok(());
        };
        let peer_loss = disposition == Some(RoutedDisposition::PeerLost);
        if peer_loss {
            self.executed_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(key);
            drop(self.retire_effect(key));
        }
        if let Some(binding) = &binding
            && !owner_released
        {
            let owner = route.owner();
            // The peer can retain T or lose its route between the Session
            // read and the owner transition. Reclassify only this opaque B1.
            let mut result = Err(DriverError::WrongBinding);
            for _ in 0..3 {
                result = match owner.binding_phase(binding) {
                    _ if disposition == Some(RoutedDisposition::HostFault) => {
                        owner.acknowledge_host_fault(binding)
                    }
                    Ok(LivePhase::TerminalReceived) => owner.retire(
                        binding,
                        if committed {
                            RetireReason::Committed
                        } else {
                            RetireReason::NoLongerExpected
                        },
                    ),
                    Ok(LivePhase::Open | LivePhase::CancelRequested) if !committed => {
                        owner.fence_no_longer_expected(binding)
                    }
                    Ok(LivePhase::PeerLost) if !committed => owner.acknowledge_peer_lost(binding),
                    Ok(_) => Err(DriverError::WrongBinding),
                    Err(error) => Err(error),
                };
                if result != Err(DriverError::WrongBinding) {
                    break;
                }
            }
            if result.is_err() {
                return Err(self.line_fault(raw, "routed tool binding cleanup failed"));
            }
        }
        if committed || disposition != Some(RoutedDisposition::PeerLost) {
            self.lane
                .owner
                .routed_pending
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(key);
        }
        if !peer_loss {
            self.executed_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(key);
            drop(self.retire_effect(key));
        }
        // Keep the completed cell as a drive-local tombstone. A concurrently
        // finishing tool task must not mistake an absent map entry for a native
        // call and publish ToolRaw after exact owner cleanup.
        lease.complete();
        Ok(())
    }

    #[cfg(feature = "extensions")]
    pub(crate) fn routed_disposition(&self, key: &EffectKey) -> Option<RoutedDisposition> {
        let state = self
            .routed_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(key)
            .cloned()?;
        state.lock().unwrap_or_else(|p| p.into_inner()).disposition
    }

    #[cfg(feature = "extensions")]
    pub(crate) async fn cleanup_all_routed(&self) -> Result<(), HarnessRuntimeError> {
        let keys: Vec<_> = self
            .routed_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .cloned()
            .collect();
        for key in keys {
            self.cleanup_routed(&key, false).await?;
        }
        Ok(())
    }

    pub(crate) async fn declarations(
        &self,
        current: &CurrentOperation,
    ) -> Result<BTreeMap<SafeUnsigned, ToolDeclaration>, HarnessRuntimeError> {
        self.declarations_with_policy(current, false).await
    }
    pub(crate) async fn declarations_with_policy(
        &self,
        current: &CurrentOperation,
        raw: bool,
    ) -> Result<BTreeMap<SafeUnsigned, ToolDeclaration>, HarnessRuntimeError> {
        let OperationState::Run(run) = &current.state else {
            return Err(self.line_fault(raw, "tool operation kind invalid"));
        };
        let RunPhase::Tools(tools) = &run.phase else {
            return Ok(BTreeMap::new());
        };
        let entries = self
            .lane
            .owner
            .session
            .get_entries(std::slice::from_ref(&tools.batch.assistant_entry_id))
            .await
            .map_err(|_| self.line_fault(raw, "tool source read failed"))?;
        let Some(Entry::Message(entry)) = entries.get(&tools.batch.assistant_entry_id) else {
            return Err(self.line_fault(raw, "tool source absent"));
        };
        let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(assistant)) = &entry.message
        else {
            return Err(self.line_fault(raw, "tool source kind invalid"));
        };
        let bindings = self
            .lane
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let mut declarations = BTreeMap::new();
        for call in &tools.batch.calls {
            let (index, _) = pi_harness_core::tool_batch::source(call);
            let Some(pi_ai_core::AssistantContent::ToolCall(call)) =
                assistant.content.get(index.get() as usize)
            else {
                return Err(self.line_fault(raw, "tool source index invalid"));
            };
            if let Some(binding) = bindings
                .iter()
                .find(|binding| binding.descriptor().name == call.name)
            {
                let descriptor = binding.descriptor();
                declarations.insert(
                    index,
                    ToolDeclaration {
                        replay: descriptor.replay.unwrap_or_default(),
                        execution_mode: descriptor
                            .execution_mode
                            .unwrap_or(run.settings.tool_execution),
                    },
                );
            }
        }
        Ok(declarations)
    }
    pub(crate) async fn tool_facts(
        &self,
        current: &CurrentOperation,
        session_id: SessionId,
        telemetry_context: Telemetry,
    ) -> Result<PlanningFacts<Telemetry>, HarnessRuntimeError> {
        let declarations = self.declarations(current).await?;
        if let OperationState::Run(run) = &current.state
            && let RunPhase::Tools(tools) = &run.phase
            && let Some(ToolCallState::Planned { source_index, .. }) = tools
                .batch
                .calls
                .iter()
                .find(|call| !matches!(call, ToolCallState::Completed { .. }))
            && !declarations.contains_key(source_index)
            && matches!(run.control, Control::Running(_))
        {
            let call = self
                .source_call(&tools.batch.assistant_entry_id, *source_index)
                .await?;
            return Ok(PlanningFacts::MissingIdentities {
                missing: MissingIdentities {
                    tools: vec![call.name.as_str().into()],
                    models: vec![],
                },
            });
        }
        if matches!(&current.state,OperationState::Run(run) if matches!(run.control,Control::CancelRequested(_)))
        {
            return Ok(PlanningFacts::ToolsCancelled {
                session_id,
                telemetry_context,
                declarations,
                interrupted: pi_agent_loop::tool_error("Tool execution was interrupted"),
                aborted: pi_agent_loop::tool_error("Operation aborted"),
            });
        }
        Ok(PlanningFacts::Tools {
            session_id,
            telemetry_context,
            declarations,
            interrupted: pi_agent_loop::tool_error("Tool execution was interrupted"),
        })
    }
    async fn batch_context(&self, turn: &StepId) -> Result<Arc<C>, HarnessRuntimeError> {
        let _line = self.context_line.acquire().await?;
        if let Some(context) = self
            .contexts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(turn)
            .cloned()
        {
            return Ok(context);
        }
        let context = self.context().await?;
        let old = self
            .contexts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(turn.clone(), context.clone());
        drop(old);
        Ok(context)
    }
    async fn source_call(
        &self,
        id: &EntryId,
        index: SafeUnsigned,
    ) -> Result<AgentToolCall, HarnessRuntimeError> {
        let entries = self
            .lane
            .owner
            .session
            .get_entries(std::slice::from_ref(id))
            .await
            .map_err(|_| self.lane.owner.fail("tool source read failed"))?;
        let Some(Entry::Message(entry)) = entries.get(id) else {
            return Err(self.lane.owner.fail("tool source missing"));
        };
        let AgentMessage::BuiltIn(pi_ai_core::Message::Assistant(assistant)) = &entry.message
        else {
            return Err(self.lane.owner.fail("tool source kind invalid"));
        };
        let Some(pi_ai_core::AssistantContent::ToolCall(call)) =
            assistant.content.get(index.get() as usize)
        else {
            return Err(self.lane.owner.fail("tool source index invalid"));
        };
        Ok(call.clone())
    }
    pub(crate) async fn clearance(
        self: &Arc<Self>,
        plan: ToolClearancePlan<Telemetry>,
    ) -> Result<EffectOutput, HarnessRuntimeError> {
        let call = self
            .source_call(&plan.assistant_entry_id, plan.source_index)
            .await?;
        let context = self.batch_context(&plan.turn_id).await?;
        let bindings = self
            .lane
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let binding = bindings.into_iter().find(|binding| {
            binding.descriptor().name == call.name
                && plan
                    .producing_configuration
                    .active_tool_names
                    .contains(&call.name)
        });
        let hooks = ToolHooks {
            fx: self.clone(),
            key: plan.key.clone(),
            binding: binding.clone(),
        };
        let output = pi_agent_loop::prepare_tool_call(
            &call,
            binding,
            context.as_ref(),
            &TokenCancellation(self.cancellation.clone()),
            &hooks,
            false,
        )
        .await?;
        let outcome = match output {
            ToolPreparation::Prepared(prepared) => {
                let replay = prepared.binding.descriptor().replay.unwrap_or_default();
                let old = self
                    .bindings
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert((plan.turn_id.clone(), plan.source_index), prepared.binding);
                drop(old);
                ToolClearanceOutcome::Prepared {
                    effective_args: prepared.args,
                    replay,
                }
            }
            ToolPreparation::Immediate(output) => ToolClearanceOutcome::Immediate {
                result: output.result,
                is_error: output.is_error,
                terminate: output.terminate,
            },
        };
        Ok(EffectOutput::ToolClearance(
            ToolClearanceOutput::new(&plan, outcome)
                .map_err(|_| self.lane.owner.fail("tool clearance pairing failed"))?,
        ))
    }
    async fn tool_is_current(
        &self,
        plan: &ToolEffectPlan<Telemetry>,
    ) -> Result<bool, HarnessRuntimeError> {
        let _line = self.lane.lane.line.acquire().await?;
        let h = self.lane.load_raw().await?;
        Ok(h.current.as_ref().is_some_and(|current| {
            pi_harness_core::tool_batch::expects_effect(
                current,
                &self.lane.owner.session.metadata().base_metadata().id,
                plan,
            )
        }))
    }
    async fn prepared(
        &self,
        plan: &ToolEffectPlan<Telemetry>,
        check_current: bool,
    ) -> Result<
        Option<(
            PreparedToolCall<AgentHarnessToolHandle<C>>,
            Arc<C>,
            CapturedArguments,
            bool,
        )>,
        HarnessRuntimeError,
    > {
        let call = self
            .source_call(&plan.assistant_entry_id, plan.source_index)
            .await?;
        let turn = StepId::new(plan.key.step_or_task_id.as_str())
            .map_err(|_| self.lane.owner.fail("tool turn identity invalid"))?;
        let context = self.batch_context(&turn).await?;
        let current = self
            .lane
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        let candidate = self
            .bindings
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&(turn.clone(), plan.source_index))
            .cloned();
        let actual = current
            .into_iter()
            .find(|binding| binding.descriptor().name == call.name);
        let actual = if check_current {
            actual
        } else {
            candidate.clone().or(actual)
        };
        let Some(actual) = actual else {
            return Ok(None);
        };
        let binding = if let Some(candidate) = candidate {
            if check_current && !Arc::ptr_eq(&candidate, &actual) {
                return Ok(None);
            }
            candidate
        } else {
            if actual.descriptor().replay != Some(ToolReplay::Safe) {
                return Ok(None);
            }
            actual
        };
        #[cfg(feature = "extensions")]
        let was_routed = self
            .lane
            .owner
            .tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .echo_snapshot(&binding)
            .is_some();
        #[cfg(not(feature = "extensions"))]
        let was_routed = false;
        let register = self
            .lane
            .owner
            .session
            .get_register(RegisterNamespace::OperationToolArguments, &plan.args_key)
            .await
            .map_err(|_| self.lane.owner.fail("saved tool arguments read failed"))?;
        let Some(Register::OperationToolArguments {
            key,
            value: args,
            seq,
        }) = register
        else {
            if !self.tool_is_current(plan).await? {
                return Ok(None);
            }
            return Err(self.lane.owner.fail("saved tool arguments absent"));
        };
        if key != plan.args_key {
            return Err(self.lane.owner.fail("saved tool arguments key mismatch"));
        }
        let captured = CapturedArguments {
            key: key.clone(),
            value: args.clone(),
            seq,
        };
        Ok(Some((
            PreparedToolCall {
                tool_call: call,
                args,
                binding,
            },
            context,
            captured,
            was_routed,
        )))
    }
    async fn missing_binding_output(
        &self,
        plan: ToolEffectPlan<Telemetry>,
    ) -> Result<EffectOutput, HarnessRuntimeError> {
        // A previously captured normal binding may still validate the in-band
        // missing-binding result. Its arguments are reconstructed from the
        // coherent capture below, never from this candidate read.
        let synthetic_prepared = self.prepared(&plan, false).await?;
        let _line = self.lane.lane.line.acquire().await?;
        let h = self.lane.load_raw().await?;
        if !h.current.as_ref().is_some_and(|current| {
            pi_harness_core::tool_batch::expects_effect(
                current,
                &self.lane.owner.session.metadata().base_metadata().id,
                &plan,
            )
        }) {
            return Ok(EffectOutput::NotStarted { key: plan.key });
        }
        let arguments = hydrated_arguments(&h, &plan.args_key)
            .ok_or_else(|| crate::restore::fault("saved tool arguments absent"))?;
        let old = self
            .captured_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                plan.key.clone(),
                CapturedToolCall {
                    arguments,
                    prepared: synthetic_prepared.map(|(prepared, context, _, _)| {
                        (prepared.tool_call, prepared.binding, context)
                    }),
                },
            );
        drop(old);
        Ok(EffectOutput::ToolRaw(ToolRawOutput {
            key: plan.key,
            result: pi_agent_loop::tool_error("Tool binding is missing or replaced"),
            is_error: true,
        }))
    }
    pub(crate) async fn execute_tool(
        self: &Arc<Self>,
        plan: ToolEffectPlan<Telemetry>,
    ) -> Result<EffectOutput, HarnessRuntimeError> {
        let Some((prepared, context, prepair_arguments, was_routed)) =
            self.prepared(&plan, true).await?
        else {
            return self.missing_binding_output(plan).await;
        };
        #[cfg(not(feature = "extensions"))]
        let _ = (&prepair_arguments, was_routed);
        let (replay, captured_arguments, route_snapshot) = {
            #[cfg(feature = "extensions")]
            let mut permits =
                PairedPermits::acquire(&self.lane.owner.settings_line, &self.lane.lane.line)
                    .await?;
            #[cfg(not(feature = "extensions"))]
            let _settings = self.lane.owner.settings_line.acquire().await?;
            #[cfg(not(feature = "extensions"))]
            let _line = self.lane.lane.line.acquire().await?;
            let h = self.lane.load_raw().await?;
            let current_key = h.current.as_ref().is_some_and(|current| {
                pi_harness_core::tool_batch::expects_effect(
                    current,
                    &self.lane.owner.session.metadata().base_metadata().id,
                    &plan,
                )
            });
            if !current_key {
                return Ok(EffectOutput::NotStarted { key: plan.key });
            }
            let running = h.current.as_ref().is_some_and(|current| {
                matches!(&current.state, OperationState::Run(run)
                    if matches!(run.control, Control::Running(_)))
            });
            // Telemetry uses the already-validated captured declaration; it
            // does not invoke another application descriptor callback.
            let replay = h
                .current
                .as_ref()
                .and_then(|current| match &current.state {
                    OperationState::Run(run) => match &run.phase {
                        RunPhase::Tools(tools) => {
                            tools.batch.calls.iter().find_map(|call| match call {
                                ToolCallState::EffectPending {
                                    source_index,
                                    replay,
                                    ..
                                } if *source_index == plan.source_index => Some(*replay),
                                _ => None,
                            })
                        }
                        _ => None,
                    },
                    _ => None,
                })
                .expect("validated pending tool");
            let bindings = self
                .lane
                .owner
                .tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            #[cfg(feature = "extensions")]
            let route_snapshot = self
                .lane
                .owner
                .tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .echo_snapshot(&prepared.binding);
            #[cfg(not(feature = "extensions"))]
            let route_snapshot = ();
            #[cfg(feature = "extensions")]
            if (was_routed || route_snapshot.is_some())
                && !captured_arguments_match(&h, &prepair_arguments)
            {
                return Err(crate::restore::fault(
                    "saved routed tool arguments changed before claim",
                ));
            }
            if !running {
                return Ok(EffectOutput::NotStarted { key: plan.key });
            }
            if !bindings
                .iter()
                .any(|binding| Arc::ptr_eq(binding, &prepared.binding))
            {
                let arguments = hydrated_arguments(&h, &plan.args_key)
                    .ok_or_else(|| crate::restore::fault("saved tool arguments absent"))?;
                let old = self
                    .captured_tools
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(
                        plan.key.clone(),
                        CapturedToolCall {
                            arguments,
                            prepared: Some((
                                prepared.tool_call.clone(),
                                prepared.binding.clone(),
                                context.clone(),
                            )),
                        },
                    );
                #[cfg(feature = "extensions")]
                permits.release();
                drop(old);
                return Ok(EffectOutput::ToolRaw(ToolRawOutput {
                    key: plan.key,
                    result: pi_agent_loop::tool_error("Tool binding is missing or replaced"),
                    is_error: true,
                }));
            }
            let Some(arguments) = hydrated_arguments(&h, &plan.args_key) else {
                return Err(crate::restore::fault("saved tool arguments absent"));
            };
            if arguments.value != prepared.args {
                return Ok(EffectOutput::NotStarted { key: plan.key });
            }
            let turn = StepId::new(plan.key.step_or_task_id.as_str())
                .map_err(|_| crate::restore::fault("tool turn identity invalid"))?;
            let displaced_capture = self
                .captured_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(
                    plan.key.clone(),
                    CapturedToolCall {
                        arguments: arguments.clone(),
                        prepared: Some((
                            prepared.tool_call.clone(),
                            prepared.binding.clone(),
                            context.clone(),
                        )),
                    },
                );
            let displaced_binding = self
                .bindings
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert((turn, plan.source_index), prepared.binding.clone());
            self.executed_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(plan.key.clone());
            let snapshot = RunningToolSnapshot {
                tool_call_id: prepared.tool_call.id.clone(),
                tool_name: prepared.tool_call.name.clone(),
                args: pi_ai_core::JsonValue::Object(prepared.args.clone()),
                partial_result: None,
            };
            let old = self
                .lane
                .lane
                .running_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(plan.key.clone(), snapshot);
            drop(old);
            self.events().enqueue(
                HarnessEventType::ToolStart,
                &plan.key.lane,
                HarnessEvent::ToolStart(ToolStartEvent {
                    event_type: Default::default(),
                    lane: plan.key.lane.clone(),
                    recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: plan.key.operation_id.clone(),
                    turn_id: plan.key.step_or_task_id.as_str().into(),
                    tool_call_id: prepared.tool_call.id.clone(),
                    tool_name: prepared.tool_call.name.clone(),
                    args: pi_ai_core::JsonValue::Object(prepared.args.clone()),
                }),
            );
            #[cfg(feature = "extensions")]
            permits.release();
            drop(displaced_binding);
            drop(displaced_capture);
            (replay, arguments, route_snapshot)
        };
        #[cfg(not(feature = "extensions"))]
        let _ = &route_snapshot;
        self.events().flush();
        let validator = ResultValidator {
            fx: self.as_ref(),
            call: &prepared.tool_call,
        };
        let mut attributes = crate::telemetry::correlation(&plan.key.lane, &plan.key.operation_id);
        attributes.insert(
            "pi.turn.id".into(),
            crate::telemetry::string(plan.key.step_or_task_id.as_str()),
        );
        attributes.insert(
            "pi.tool.name".into(),
            crate::telemetry::string(prepared.tool_call.name.as_str()),
        );
        attributes.insert(
            "pi.tool.call_id".into(),
            crate::telemetry::string(prepared.tool_call.id.as_str()),
        );
        attributes.insert(
            "pi.tool.replay".into(),
            crate::telemetry::string(if replay == ToolReplay::Safe {
                "safe"
            } else {
                "never"
            }),
        );
        attributes.insert(
            "pi.tool.recovery".into(),
            pi_telemetry::AttributeValue::Boolean(self.recovery),
        );
        let span = crate::telemetry::Span::start(
            plan.telemetry_context.clone(),
            "pi.harness.tool",
            attributes,
        );
        let events = ToolEvents {
            fx: self.clone(),
            plan: plan.clone(),
            arguments: captured_arguments.clone(),
            telemetry: span.clone(),
        };
        #[cfg(feature = "extensions")]
        let routed_state = route_snapshot.map(|snapshot| {
            let state = Arc::new(Mutex::new(RoutedEffect {
                route: snapshot.route.clone(),
                binding: None,
                disposition: None,
                terminal: false,
                entered: false,
                owner_released: false,
                cleanup: CleanupArbiter::new(),
            }));
            self.routed_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(plan.key.clone(), state.clone());
            (snapshot, state)
        });
        #[cfg(feature = "extensions")]
        let execution_prepared = if let Some((snapshot, state)) = &routed_state {
            let binding: AgentHarnessToolHandle<C> = Arc::new(RoutedTool {
                fx: self.clone(),
                plan: plan.clone(),
                saved: captured_arguments.clone(),
                source_id: prepared.tool_call.id.clone(),
                original: prepared.binding.clone(),
                snapshot: snapshot.clone(),
                state: state.clone(),
            });
            PreparedToolCall {
                tool_call: prepared.tool_call.clone(),
                args: prepared.args.clone(),
                binding,
            }
        } else {
            PreparedToolCall {
                tool_call: prepared.tool_call.clone(),
                args: prepared.args.clone(),
                binding: prepared.binding.clone(),
            }
        };
        #[cfg(not(feature = "extensions"))]
        let execution_prepared = PreparedToolCall {
            tool_call: prepared.tool_call.clone(),
            args: prepared.args.clone(),
            binding: prepared.binding.clone(),
        };
        let execution = pi_agent_loop::execute_tool_call(
            &execution_prepared,
            context.as_ref(),
            &TokenCancellation(self.cancellation.clone()),
            &events,
            &validator,
        )
        .await;
        #[cfg(feature = "extensions")]
        if routed_state.is_some() {
            self.lane.owner.admission.check()?;
        }
        let output = execution?;
        #[cfg(feature = "extensions")]
        if routed_state.is_some()
            && let Some(exit) = self.routed_exit(&plan).await?
        {
            return match exit {
                RoutedDisposition::PeerLost => Err(HarnessRuntimeError::PeerLost),
                RoutedDisposition::HostFault => {
                    self.cleanup_routed(&plan.key, false).await?;
                    Err(self.lane.owner.fail("extension tool host fault"))
                }
                RoutedDisposition::NoLongerExpected | RoutedDisposition::UnadmittedLocalAbort => {
                    Ok(EffectOutput::NotStarted { key: plan.key })
                }
            };
        }
        span.end(
            pi_telemetry::SpanAttributes::from([(
                "pi.tool.is_error".into(),
                pi_telemetry::AttributeValue::Boolean(output.is_error),
            )]),
            output.is_error,
        );
        // The tool may have yielded while another writer finalized the
        // operation or changed its saved effective arguments. Use the register
        // from this same stamped hydration: a separate read could observe an
        // external terminal's args deletion after the pending-state sample.
        let _line = self.lane.lane.line.acquire().await?;
        let h = self.lane.load_raw().await?;
        let expected = h.current.as_ref().is_some_and(|current| {
            pi_harness_core::tool_batch::expects_effect(
                current,
                &self.lane.owner.session.metadata().base_metadata().id,
                &plan,
            )
        });
        if !expected {
            return Ok(EffectOutput::NotStarted { key: plan.key });
        }
        let captured = self
            .captured_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&plan.key)
            .map(|capture| capture.arguments.clone())
            .ok_or_else(|| crate::restore::fault("tool execution capture absent"))?;
        if !captured_arguments_match(&h, &captured) {
            return Err(crate::restore::fault(
                "saved tool arguments changed during execution",
            ));
        }
        #[cfg(feature = "extensions")]
        self.require_routed_terminal(&plan.key)?;
        Ok(EffectOutput::ToolRaw(ToolRawOutput {
            key: plan.key,
            result: output.result,
            is_error: output.is_error,
        }))
    }
    pub(crate) async fn finalize(
        self: &Arc<Self>,
        plan: ToolEffectPlan<Telemetry>,
        output: ToolRawOutput,
    ) -> Result<ToolSettlementOutput, HarnessRuntimeError> {
        self.permission("finalize_tool").await?;
        if output.key != plan.key {
            return Err(self.lane.owner.fail("tool finalization key mismatch"));
        }
        // The manual gate may outlive a legitimate terminal transaction, which
        // removes saved arguments. Recheck authority before touching those facts.
        let _line = self.lane.lane.line.acquire().await?;
        let h = self.lane.load_raw().await?;
        let expected = h.current.as_ref().is_some_and(|current| {
            pi_harness_core::tool_batch::expects_effect(
                current,
                &self.lane.owner.session.metadata().base_metadata().id,
                &plan,
            )
        });
        if !expected {
            return Ok(ToolSettlementOutput {
                key: plan.key,
                result: output.result,
                is_error: output.is_error,
                terminate: false,
            });
        }
        let (arguments, captured_prepared) = {
            let captures = self
                .captured_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let captured = captures
                .get(&plan.key)
                .ok_or_else(|| crate::restore::fault("tool execution capture absent"))?;
            (
                captured.arguments.clone(),
                captured.prepared.as_ref().map(|(call, binding, context)| {
                    (call.clone(), binding.clone(), context.clone())
                }),
            )
        };
        if !captured_arguments_match(&h, &arguments) {
            return Err(crate::restore::fault(
                "saved tool arguments changed before finalization",
            ));
        }
        #[cfg(feature = "extensions")]
        self.require_routed_terminal(&plan.key)?;
        drop(_line);
        let Some((call, binding, context)) = captured_prepared else {
            return Ok(ToolSettlementOutput {
                key: plan.key,
                result: output.result,
                is_error: output.is_error,
                terminate: false,
            });
        };
        let prepared = PreparedToolCall {
            tool_call: call,
            args: arguments.value,
            binding,
        };
        let hooks = ToolHooks {
            fx: self.clone(),
            key: plan.key.clone(),
            binding: Some(prepared.binding.clone()),
        };
        let validator = ResultValidator {
            fx: self.as_ref(),
            call: &prepared.tool_call,
        };
        let run_after = !self.cancellation.is_cancelled()
            && self
                .executed_tools
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .contains(&plan.key);
        let result = pi_agent_loop::finalize_tool_call(
            &prepared,
            ExecutedToolOutcome {
                result: output.result,
                is_error: output.is_error,
            },
            context.as_ref(),
            &TokenCancellation(self.cancellation.clone()),
            &hooks,
            &validator,
            run_after,
        )
        .await?;
        if self
            .executed_tools
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .contains(&plan.key)
        {
            self.events().enqueue(
                HarnessEventType::ToolEnd,
                &plan.key.lane,
                HarnessEvent::ToolEnd(ToolEndEvent {
                    event_type: Default::default(),
                    lane: plan.key.lane.clone(),
                    recovery: self.recovery.then_some(pi_ai_core::LiteralTrue),
                    run_id: plan.key.operation_id.clone(),
                    turn_id: plan.key.step_or_task_id.as_str().into(),
                    tool_call_id: prepared.tool_call.id.clone(),
                    tool_name: prepared.tool_call.name.clone(),
                    result: result.result.clone(),
                    is_error: result.is_error,
                    terminate: result.terminate,
                }),
            );
            self.events().flush();
        }
        Ok(ToolSettlementOutput {
            key: plan.key,
            result: result.result,
            is_error: result.is_error,
            terminate: result.terminate,
        })
    }
}
struct ToolHooks<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: Arc<RuntimeEffects<C, S, M>>,
    key: EffectKey,
    binding: Option<AgentHarnessToolHandle<C>>,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    pi_agent_loop::ToolPhaseHooks<C> for ToolHooks<C, S, M>
{
    fn before<'a>(
        &'a self,
        call: &'a AgentToolCall,
        args: &'a JsonObject,
        _context: &'a C,
        _cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<BeforeToolResult>, pi_agent_loop::ToolPhaseError>> {
        Box::pin(async move {
            let tool = self.binding.as_ref().map(|binding| {
                let d = binding.descriptor();
                pi_ai_core::Tool {
                    name: d.name.clone(),
                    description: d.description.clone(),
                    parameters: d.parameters.clone(),
                    constrained_sampling: d.constrained_sampling.clone(),
                }
            });
            let result = self
                .fx
                .hook(
                    HookEvent::BeforeTool(BeforeToolEvent {
                        lane: self.key.lane.clone(),
                        run_id: self.key.operation_id.clone(),
                        tool_call_id: call.id.clone(),
                        tool_name: call.name.clone(),
                        args: args.clone(),
                    }),
                    true,
                    tool.as_ref(),
                )
                .await
                .map_err(pi_agent_loop::ToolPhaseError::Runtime)?;
            match result.result() {
                Some(HookResult::BeforeTool(result)) => Ok(result.clone()),
                _ => Err(pi_agent_loop::ToolPhaseError::Runtime(
                    self.fx.lane.owner.fail("invalid before-tool result"),
                )),
            }
        })
    }
    fn after<'a>(
        &'a self,
        call: &'a AgentToolCall,
        args: &'a JsonObject,
        outcome: &'a ExecutedToolOutcome,
        _context: &'a C,
        _cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Option<AfterToolResult>, pi_agent_loop::ToolPhaseError>> {
        Box::pin(async move {
            let result = self
                .fx
                .hook(
                    HookEvent::AfterTool(AfterToolEvent {
                        lane: self.key.lane.clone(),
                        run_id: self.key.operation_id.clone(),
                        tool_call_id: call.id.clone(),
                        tool_name: call.name.clone(),
                        args: args.clone(),
                        content: outcome.result.content.clone(),
                        details: Presence::Value(outcome.result.details.clone()),
                        is_error: outcome.is_error,
                        usage: outcome.result.usage.clone(),
                    }),
                    true,
                    None,
                )
                .await
                .map_err(pi_agent_loop::ToolPhaseError::Runtime)?;
            match result.result() {
                Some(HookResult::AfterTool(result)) => Ok(result.clone()),
                _ => Err(pi_agent_loop::ToolPhaseError::Runtime(
                    self.fx.lane.owner.fail("invalid after-tool result"),
                )),
            }
        })
    }
}
struct ResultValidator<'a, C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: &'a RuntimeEffects<C, S, M>,
    call: &'a AgentToolCall,
}
fn invalid_result() -> ToolExecutionError {
    ToolExecutionError::new(ErrorContext::new("tool result is invalid").expect("static context"))
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord>
    pi_agent_loop::ToolResultValidator for ResultValidator<'_, C, S, M>
{
    fn validate(&self, result: &AgentToolResult) -> Result<(), ToolExecutionError> {
        let message = AgentMessage::BuiltIn(pi_ai_core::Message::ToolResult(Box::new(
            pi_ai_core::ToolResultMessage {
                role: Default::default(),
                tool_call_id: self.call.id.clone(),
                tool_name: self.call.name.clone(),
                content: result.content.clone(),
                details: Presence::Value(result.details.clone()),
                usage: result.usage.clone(),
                added_tool_names: result.added_tool_names.clone(),
                is_error: false,
                timestamp: SafeUnsigned::new(self.fx.lane.owner.clock.now_unix_ms())
                    .map_err(|_| invalid_result())?,
            },
        )));
        self.fx
            .lane
            .owner
            .session
            .codec()
            .validate_message(&message)
            .map_err(|_| invalid_result())
    }
    fn validate_usage(&self, usage: &pi_ai_core::Usage) -> Result<(), ToolExecutionError> {
        let bytes = serde_json::to_vec(usage).map_err(|_| invalid_result())?;
        let _: pi_ai_core::Usage = serde_json::from_slice(&bytes).map_err(|_| invalid_result())?;
        Ok(())
    }
}
struct ToolEvents<C, S: RuntimeStorage, M: SessionMetadataRecord> {
    fx: Arc<RuntimeEffects<C, S, M>>,
    telemetry: Telemetry,
    plan: ToolEffectPlan<Telemetry>,
    arguments: CapturedArguments,
}
impl<C: Send + Sync + 'static, S: RuntimeStorage + 'static, M: SessionMetadataRecord> AgentEventSink
    for ToolEvents<C, S, M>
{
    fn emit(&self, event: AgentEvent) -> BoxFuture<'_, Result<(), HarnessRuntimeError>> {
        Box::pin(async move {
            self.fx.lane.owner.admission.check()?;
            if let AgentEvent::ToolExecutionUpdate {
                tool_call_id,
                tool_name,
                partial_result,
                ..
            } = event
            {
                let line = self.fx.lane.lane.line.acquire().await?;
                let h = self.fx.lane.load().await?;
                let expected = h.current.as_ref().is_some_and(|current| {
                    pi_harness_core::tool_batch::expects_effect(
                        current,
                        &self.fx.lane.owner.session.metadata().base_metadata().id,
                        &self.plan,
                    )
                });
                if !expected {
                    return Ok(());
                }
                if !captured_arguments_match(&h, &self.arguments) {
                    return Ok(());
                }
                let previous = {
                    let mut tools = self
                        .fx
                        .lane
                        .lane
                        .running_tools
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    tools.get_mut(&self.plan.key).and_then(|snapshot| {
                        snapshot.partial_result.replace(partial_result.clone())
                    })
                };
                drop(previous);
                self.fx
                    .events()
                    .with_context(self.telemetry.clone())
                    .enqueue(
                        HarnessEventType::ToolUpdate,
                        &self.plan.key.lane,
                        HarnessEvent::ToolUpdate(ToolUpdateEvent {
                            event_type: Default::default(),
                            lane: self.plan.key.lane.clone(),
                            recovery: None,
                            run_id: self.plan.key.operation_id.clone(),
                            turn_id: self.plan.key.step_or_task_id.as_str().into(),
                            tool_call_id,
                            tool_name,
                            partial_result,
                        }),
                    );
                drop(line);
                self.fx.events().flush();
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod exact_plan_tests {
    use super::*;

    #[test]
    fn saved_arguments_require_register_key_and_value_identity() {
        let args = JsonObject::default();
        let register = Register::OperationToolArguments {
            key: "expected".into(),
            value: args.clone(),
            seq: SafeUnsigned::new(1).unwrap(),
        };
        assert!(saved_arguments_match(Some(&register), "expected", &args));
        assert!(!saved_arguments_match(Some(&register), "other", &args));
        let different = JsonObject::new(BTreeMap::from([(
            "changed".into(),
            pi_ai_core::JsonValue::Bool(true),
        )]));
        assert!(!saved_arguments_match(
            Some(&register),
            "expected",
            &different
        ));
        assert!(!saved_arguments_match(None, "expected", &args));
    }
}
