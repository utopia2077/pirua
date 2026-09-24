//! One process-local, sequential echo invocation over a host-owned connection.
//!
//! This module owns no Session state. A caller must prove the exact durable
//! settlement or no-longer-expected decision before retiring a receipt.

use std::collections::VecDeque;
use std::fmt;
use std::future::poll_fn;
use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::task::{Poll, Waker};

use pi_agent_schema::ToolEffectPlan;
use pi_ai_core::{CancellationToken, JsonObject, TaskSpawner};
use pi_extension_protocol::activation::{ActiveGrant, HostAuthority};
use pi_extension_protocol::descriptor::canonical_profile_digest;
use pi_extension_protocol::endpoint::{ActiveFrame, SelectedEnvelope};
use pi_extension_protocol::tool_live::{
    InvocationBinding, LiveError, LivePhase, ReceiveOutcome, RetireReason, TerminalKind,
    ToolInvocation,
};
use pi_extension_protocol::wire::framing::{self, EncodedFrame};
use pi_extension_protocol::wire::{v1_0, v1_1};

use crate::extensions::domain::{DomainReceiveError, EchoDomainBinding};
use crate::extensions::key::encode_tool_effect_key;
use crate::extensions::transport::{
    ConnectionPump, MonotonicClock, OrderedByteChannel, PumpConfig,
};
use crate::extensions::value::encode_arguments;
use crate::scheduler::Supervisor;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DriverError {
    InvalidHost,
    Overloaded,
    PeerLost,
    MalformedPeer,
    HostFault,
    Deadline,
    Closed,
    WrongBinding,
    NoLongerExpected,
    UnadmittedLocalAbort,
}

impl fmt::Display for DriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidHost => "invalid host tool route",
            Self::Overloaded => "extension tool capacity exhausted",
            Self::PeerLost => "extension peer lost before terminal",
            Self::MalformedPeer => "extension peer frame rejected",
            Self::HostFault => "host tool domain fault",
            Self::Deadline => "extension tool deadline exceeded",
            Self::Closed => "extension tool route closed",
            Self::WrongBinding => "extension tool binding mismatch",
            Self::NoLongerExpected => "extension tool effect no longer expected",
            Self::UnadmittedLocalAbort => "extension tool locally aborted before admission",
        })
    }
}
impl std::error::Error for DriverError {}

#[derive(Clone, Copy, Debug)]
pub struct OwnerLimits {
    pub outbound_bytes: NonZeroUsize,
    pub receipt_bytes: NonZeroUsize,
    pub write_timeout_ms: NonZeroU64,
}

#[derive(Clone)]
pub enum TypedTerminal {
    Result(pi_agent_schema::AgentToolResult),
    PeerError(v1_1::ErrorCode),
}

/// Process-local call disposition. The no-write case has no peer terminal or
/// tool error and must be checked before publishing a ToolRaw value.
#[derive(Clone)]
pub enum CallOutcome {
    Terminal(TypedTerminal),
    NoLongerExpected,
    UnadmittedLocalAbort,
}

#[derive(Clone)]
enum CompletionValue {
    Terminal(TypedTerminal),
    Failed(DriverError),
    NoLongerExpected,
    UnadmittedLocalAbort,
}

struct Completion {
    value: Mutex<Option<CompletionValue>>,
    changed: CancellationToken,
}

impl Completion {
    fn new() -> Self {
        Self {
            value: Mutex::new(None),
            changed: CancellationToken::new(),
        }
    }
    fn finish(&self, value: CompletionValue) {
        let mut slot = self.value.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some(value);
            drop(slot);
            self.changed.cancel();
        }
    }
}

pub struct CallHandle {
    binding: InvocationBinding,
    completion: Arc<Completion>,
}

/// A committed, exact FIFO claim. The caller must release both Harness lines
/// before activation or drop: either operation may invoke host task or waker
/// code. After the lines are released, dropping the token rolls back its
/// inactive Request and wakes a reserved denial behind it.
#[must_use = "release Harness permits, then activate or drop the prepared call"]
pub struct PreparedCall {
    owner: SingleToolOwner,
    route: Arc<Route>,
    handle: Option<CallHandle>,
    invocation_deadline: u64,
    write_deadline: u64,
}

impl PreparedCall {
    pub fn binding(&self) -> &InvocationBinding {
        self.handle
            .as_ref()
            .expect("unconsumed prepared call")
            .binding()
    }

    pub fn activate(mut self) -> Result<CallHandle, DriverError> {
        let timer_owner = self.owner.clone();
        let timer_route = self.route.clone();
        let timer_binding = self.binding().clone();
        let invocation_deadline = self.invocation_deadline;
        let timer_cancel = CancellationToken::new();
        let timer_stop = timer_cancel.clone();
        if self
            .route
            .supervisor
            .submit(Box::pin(async move {
                let mut sleep = timer_route.clock.sleep_until(invocation_deadline);
                let mut cancelled = Box::pin(timer_stop.cancelled());
                let due = poll_fn(|cx| {
                    if cancelled.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(false);
                    }
                    sleep.as_mut().poll(cx).map(|()| true)
                })
                .await;
                if due {
                    timer_owner.expire(&timer_route, &timer_binding);
                }
            }))
            .is_err()
        {
            timer_cancel.cancel();
            if self.owner.reject_prepared(
                &self.route,
                self.binding(),
                CompletionValue::Failed(DriverError::HostFault),
            ) {
                self.handle = None;
                return Err(DriverError::HostFault);
            }
            // An inline poll or concurrent fence already resolved this exact
            // binding. Preserve its earlier disposition.
            return Ok(self.handle.take().expect("prepared call"));
        }
        let wake = {
            let state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
            if state.live.as_ref().is_some_and(|slot| {
                Arc::ptr_eq(&slot.route, &self.route)
                    && slot.invocation.binding().same_live_identity(self.binding())
                    && matches!(
                        slot.invocation.phase(),
                        LivePhase::Open | LivePhase::CancelRequested
                    )
            }) && self.route.status() != RouteStatus::Closed
            {
                let mut writes = self.route.writes.lock().unwrap_or_else(|p| p.into_inner());
                let job = writes.jobs.iter_mut().find(|job| {
                    matches!(job.kind, WriteKind::Request(id) if id == self.binding().request_id())
                });
                if let Some(job) =
                    job.filter(|job| job.deadline == self.write_deadline && !job.active)
                {
                    job.active = true;
                    writes.waker.take()
                } else {
                    None
                }
            } else {
                None
            }
        };
        let handle = self.handle.take().expect("prepared call");
        if let Some(waker) = wake {
            waker.wake();
        }
        Ok(handle)
    }
}

impl Drop for PreparedCall {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.owner.reject_prepared(
                &self.route,
                &handle.binding,
                CompletionValue::UnadmittedLocalAbort,
            );
        }
    }
}

impl CallHandle {
    pub fn binding(&self) -> &InvocationBinding {
        &self.binding
    }
    pub async fn outcome(&self) -> Result<CallOutcome, DriverError> {
        self.completion.changed.cancelled().await;
        match self
            .completion
            .value
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .expect("signalled completion")
        {
            CompletionValue::Terminal(terminal) => Ok(CallOutcome::Terminal(terminal.clone())),
            CompletionValue::Failed(error) => Err(*error),
            CompletionValue::NoLongerExpected => Ok(CallOutcome::NoLongerExpected),
            CompletionValue::UnadmittedLocalAbort => Ok(CallOutcome::UnadmittedLocalAbort),
        }
    }

    /// Compatibility access for callers that require a terminal. The exact
    /// no-write disposition remains separately available through `outcome`.
    pub async fn first_terminal(&self) -> Result<TypedTerminal, DriverError> {
        match self.outcome().await? {
            CallOutcome::Terminal(terminal) => Ok(terminal),
            CallOutcome::NoLongerExpected => Err(DriverError::NoLongerExpected),
            CallOutcome::UnadmittedLocalAbort => Err(DriverError::UnadmittedLocalAbort),
        }
    }
}

/// The checked caller's current local control cause. This operation does not
/// inspect Session state; the caller must first prove its full EffectPending key
/// is still expected and this cause is current for that exact binding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocalAbortCause {
    User,
    Parent,
    Deadline,
    Drain,
}

impl LocalAbortCause {
    fn wire_reason(self) -> v1_1::CancelReason {
        match self {
            Self::User => v1_1::CancelReason::CancelUser,
            Self::Parent => v1_1::CancelReason::CancelParent,
            Self::Deadline => v1_1::CancelReason::CancelDeadline,
            Self::Drain => v1_1::CancelReason::CancelDrain,
        }
    }
}

pub struct CallTiming {
    pub requested_after_ms: Option<u64>,
    pub method_limit_ms: u64,
    pub connection_remaining_ms: u64,
    pub parent_remaining_ms: Option<u64>,
}

/// Host policy fixed to one Ready echo generation. The cutoff uses that
/// route's monotonic clock, not Unix time or a duration sampled at preflight.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EchoRouteTiming {
    pub method_limit_ms: NonZeroU64,
    pub connection_deadline_ms: u64,
}

enum StartTiming {
    Raw(CallTiming),
    Absolute {
        requested_deadline_ms: Option<u64>,
        parent_deadline_ms: Option<u64>,
    },
}

struct LiveSlot {
    invocation: ToolInvocation,
    domain: EchoDomainBinding,
    route: Arc<Route>,
    completion: Arc<Completion>,
    send_started: bool,
    send_confirmed: bool,
    provisional_frame: Option<ActiveFrame>,
    local_fault: bool,
    cancel_deadline_ms: Option<u64>,
}

struct OwnerState {
    active: Option<Arc<Route>>,
    live: Option<LiveSlot>,
    highest_generation: u64,
    last_local_abort: Option<InvocationBinding>,
}

struct OwnerInner {
    state: Mutex<OwnerState>,
    host: Arc<dyn TaskSpawner>,
    limits: OwnerLimits,
}

/// One owner-global live/terminal slot, shared by successive generations.
#[derive(Clone)]
pub struct SingleToolOwner(Arc<OwnerInner>);

impl SingleToolOwner {
    /// Process-local identity check for a harness's one owner-wide live slot.
    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    pub fn new(host: Arc<dyn TaskSpawner>, limits: OwnerLimits) -> Self {
        Self(Arc::new(OwnerInner {
            state: Mutex::new(OwnerState {
                active: None,
                live: None,
                highest_generation: 0,
                last_local_abort: None,
            }),
            host,
            limits,
        }))
    }

    /// Complete the host-authenticated handshake before starting active reads.
    pub async fn connect(
        &self,
        host: HostAuthority,
        channel: Arc<dyn OrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
    ) -> Result<ReadyToolRoute, DriverError> {
        let mut pump = ConnectionPump::new(host, channel.clone(), clock.clone(), config)
            .map_err(|_| DriverError::InvalidHost)?;
        let grant = Arc::new(
            pump.drive_until_ready()
                .await
                .map_err(|_| DriverError::PeerLost)?,
        );
        let generation = grant.wire().connection_generation;
        let selected = grant
            .wire()
            .selected_protocol
            .ok_or(DriverError::InvalidHost)?;
        let selected_limits = grant
            .wire()
            .limits
            .as_ref()
            .ok_or(DriverError::InvalidHost)?;
        let frame_cap =
            usize::try_from(selected_limits.frame_bytes).map_err(|_| DriverError::InvalidHost)?;
        let blocking_cap = usize::try_from(selected_limits.blocking_queue_bytes)
            .map_err(|_| DriverError::InvalidHost)?;
        // Q, one full C and one fixed D have independent finite buckets. The
        // checked sum is the route's maximum outstanding outbound reservation.
        let bounded_outbound = frame_cap
            .checked_mul(2)
            .and_then(|control| {
                control.checked_add(blocking_cap.min(self.0.limits.outbound_bytes.get()))
            })
            .is_some();
        if generation == 0
            || selected.major != 1
            || selected.minor > 1
            || frame_cap == 0
            || selected_limits.cancellation_grace_ms == 0
            || !bounded_outbound
        {
            return Err(DriverError::InvalidHost);
        }
        // This route owns only one host-approved echo capability and one live
        // invocation. A wider grant needs a different selection/ownership path.
        let [tool] = grant.tools() else {
            return Err(DriverError::InvalidHost);
        };
        let reference = tool.reference();
        let profile = reference.profile.as_ref().ok_or(DriverError::InvalidHost)?;
        if grant.wire().selected_capabilities.len() != 1
            || grant.wire().selected_capabilities[0] != *reference
            || !grant.wire().permissions.is_empty()
            || tool.descriptor().name != "echo"
            || reference.authenticated_extension_id != grant.wire().authenticated_extension_id
            || reference.kind != "tool"
            || reference.name != "echo"
            || profile.name != "tool.v1"
            || profile.major != 1
            || canonical_profile_digest("tool.v1", 1, profile.minor)
                .is_none_or(|digest| reference.schema_digest_sha256 != digest)
        {
            return Err(DriverError::InvalidHost);
        }
        let route = Arc::new(Route {
            generation,
            grant,
            channel,
            clock,
            supervisor: Supervisor::new(self.0.host.clone()),
            status: std::sync::atomic::AtomicU8::new(0),
            next_id: AtomicU64::new(1),
            frame_cap,
            normal_cap: blocking_cap.min(self.0.limits.outbound_bytes.get()),
            control_cap: frame_cap,
            cancel_cap: frame_cap,
            timeout_ms: self.0.limits.write_timeout_ms.get(),
            timing: OnceLock::new(),
            writes: Mutex::new(WriteState::default()),
        });
        let prior = {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            if generation <= state.highest_generation {
                return Err(DriverError::InvalidHost);
            }
            state.highest_generation = generation;
            let prior = state.active.replace(route.clone());
            if let Some(old) = &prior {
                let old_live = state.live.as_ref().is_some_and(|slot| {
                    Arc::ptr_eq(&slot.route, old)
                        && matches!(
                            slot.invocation.phase(),
                            LivePhase::Open | LivePhase::CancelRequested
                        )
                });
                let retained_cancel = state.live.as_ref().is_some_and(|slot| {
                    Arc::ptr_eq(&slot.route, old)
                        && slot.invocation.phase() == LivePhase::TerminalReceived
                        && old.has_cancel_pending()
                });
                // A completed T may release the owner slot before its earlier
                // C reaches the writer. Replacement preserves that exact
                // bounded control token (and any previously reserved D).
                if old_live
                    || retained_cancel
                    || old.has_retired_cancel_pending()
                    || old.has_denial_pending()
                {
                    old.mark_draining();
                }
            }
            prior
        };
        let writer_route = route.clone();
        let writer_owner = self.clone();
        if route
            .supervisor
            .submit(Box::pin(async move {
                writer_loop(writer_owner, writer_route).await
            }))
            .is_err()
        {
            self.close_route(&route, DriverError::HostFault);
            if let Some(old) = &prior
                && old.status() != RouteStatus::Draining
            {
                self.close_route(old, DriverError::PeerLost);
            }
            return Err(DriverError::HostFault);
        }
        let reader_route = route.clone();
        let reader_owner = self.clone();
        if route
            .supervisor
            .submit(Box::pin(async move {
                reader_loop(reader_owner, reader_route, pump).await
            }))
            .is_err()
        {
            self.close_route(&route, DriverError::HostFault);
            if let Some(old) = &prior
                && old.status() != RouteStatus::Draining
            {
                self.close_route(old, DriverError::PeerLost);
            }
            return Err(DriverError::HostFault);
        }
        if let Some(old) = prior
            && old.status() != RouteStatus::Draining
        {
            self.close_route(&old, DriverError::PeerLost);
        }
        Ok(ReadyToolRoute {
            owner: self.clone(),
            route,
        })
    }

    pub fn retained_binding(&self) -> Option<InvocationBinding> {
        self.0
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .live
            .as_ref()
            .map(|slot| slot.invocation.binding().clone())
    }

    /// Inspect only the exact opaque live binding; a later generation is not
    /// evidence about this invocation.
    pub fn binding_phase(&self, binding: &InvocationBinding) -> Result<LivePhase, DriverError> {
        let state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        let slot = state.live.as_ref().ok_or(DriverError::WrongBinding)?;
        if !slot.invocation.binding().same_live_identity(binding) {
            return Err(DriverError::WrongBinding);
        }
        Ok(slot.invocation.phase())
    }

    /// Caller invokes only after exact no-write cleanup of a preterminal loss.
    pub fn acknowledge_peer_lost(&self, binding: &InvocationBinding) -> Result<(), DriverError> {
        let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        let slot = state.live.as_ref().ok_or(DriverError::WrongBinding)?;
        if !slot.invocation.binding().same_live_identity(binding)
            || slot.invocation.phase() != LivePhase::PeerLost
        {
            return Err(DriverError::WrongBinding);
        }
        state.live = None;
        Ok(())
    }

    /// Caller invokes after handling a local fault without blaming the peer.
    pub fn acknowledge_host_fault(&self, binding: &InvocationBinding) -> Result<(), DriverError> {
        let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        let slot = state.live.as_ref().ok_or(DriverError::WrongBinding)?;
        if !slot.invocation.binding().same_live_identity(binding) || !slot.local_fault {
            return Err(DriverError::WrongBinding);
        }
        state.live = None;
        Ok(())
    }

    /// Caller invokes only after checked commit or definitive NoLongerExpected.
    pub fn retire(
        &self,
        binding: &InvocationBinding,
        reason: RetireReason,
    ) -> Result<(), DriverError> {
        let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        let slot = state.live.as_mut().ok_or(DriverError::WrongBinding)?;
        slot.invocation
            .retire(binding, reason)
            .map_err(|_| DriverError::WrongBinding)?;
        if slot.cancel_deadline_ms.is_some() {
            slot.route
                .mark_retired_cancel_if_pending(binding.request_id());
        }
        state.live = None;
        Ok(())
    }

    /// Caller invokes only after checked current Session state proves the
    /// full durable effect key is definitively no longer expected. A retained
    /// first terminal instead uses `retire(..., NoLongerExpected)`.
    pub fn fence_no_longer_expected(&self, binding: &InvocationBinding) -> Result<(), DriverError> {
        let (route, completion, close) = {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            let slot = state.live.as_mut().ok_or(DriverError::WrongBinding)?;
            if slot.local_fault {
                return Err(DriverError::WrongBinding);
            }
            slot.invocation
                .fence_no_longer_expected(binding)
                .map_err(|_| DriverError::WrongBinding)?;
            let route = slot.route.clone();
            // The owner and queue lines jointly decide which denial existed
            // before the fence. Neither line performs channel I/O or wakes.
            let close = route.prepare_fenced_route(binding.request_id());
            let completion = slot.completion.clone();
            state.live = None;
            if !matches!(close, FenceRouteDecision::PreserveDenial(_))
                && state
                    .active
                    .as_ref()
                    .is_some_and(|active| Arc::ptr_eq(active, &route))
            {
                state.active = None;
            }
            (route, completion, close)
        };
        // Finish physical close before an arbitrary completion waker can poll
        // an admitted but incomplete write on a generation without D.
        route.finish_fence_decision(close);
        // Wake outside the owner line: arbitrary wakers may reenter the owner.
        completion.finish(CompletionValue::NoLongerExpected);
        Ok(())
    }

    /// Called only after a checked current Session boundary proves the same
    /// full durable EffectPending key and this local control cause are current.
    /// It does not mutate durable state or classify key absence.
    pub fn abort_local(
        &self,
        binding: &InvocationBinding,
        cause: LocalAbortCause,
    ) -> Result<(), DriverError> {
        let (route, completion, close, wake, grace_deadline) = {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            let Some(slot) = state.live.as_mut() else {
                return if state
                    .last_local_abort
                    .as_ref()
                    .is_some_and(|prior| prior.same_live_identity(binding))
                {
                    Ok(())
                } else {
                    Err(DriverError::WrongBinding)
                };
            };
            if !slot.invocation.binding().same_live_identity(binding) || slot.local_fault {
                return Err(DriverError::WrongBinding);
            }
            if slot.invocation.phase() == LivePhase::CancelRequested {
                return Ok(());
            }
            if slot.invocation.phase() != LivePhase::Open {
                return Err(DriverError::WrongBinding);
            }
            let route = slot.route.clone();
            let completion = slot.completion.clone();
            if !slot.send_started {
                let close = route.prepare_fenced_route(binding.request_id());
                state.last_local_abort = Some(binding.clone());
                state.live = None;
                if !matches!(close, FenceRouteDecision::PreserveDenial(_))
                    && state
                        .active
                        .as_ref()
                        .is_some_and(|active| Arc::ptr_eq(active, &route))
                {
                    state.active = None;
                }
                (route, completion, Some(close), None, None)
            } else {
                let now = route.clock.now_ms();
                let grace = route
                    .grant
                    .wire()
                    .limits
                    .as_ref()
                    .expect("selected limits")
                    .cancellation_grace_ms;
                let effective = slot.invocation.effective_deadline_ms();
                // Cap the relative grace before addition: the earlier finite
                // invocation/drain deadline can still be representable when
                // now + the full negotiated grace would overflow.
                let cutoff = now
                    .checked_add(grace.min(effective.saturating_sub(now)))
                    .ok_or(DriverError::Deadline)?;
                let frame = slot
                    .invocation
                    .prepare_cancel(cause.wire_reason(), now)
                    .map_err(|_| DriverError::Deadline)?;
                let (wake, _) = route.enqueue(frame, WriteKind::Cancel(binding.request_id()))?;
                slot.invocation.commit_prepared_cancel();
                slot.cancel_deadline_ms = Some(cutoff);
                state.last_local_abort = Some(binding.clone());
                (route, completion, None, wake, Some(cutoff))
            }
        };
        let unadmitted = close.is_some();
        if let Some(close) = close {
            route.finish_fence_decision(close);
        }
        if unadmitted {
            completion.finish(CompletionValue::UnadmittedLocalAbort);
        }
        if let Some(waker) = wake {
            waker.wake();
        }
        if let Some(deadline) = grace_deadline {
            let owner = self.clone();
            let timer_route = route.clone();
            let timer_binding = binding.clone();
            if route
                .supervisor
                .submit(Box::pin(async move {
                    timer_route.clock.sleep_until(deadline).await;
                    owner.expire(&timer_route, &timer_binding);
                }))
                .is_err()
            {
                self.close_route(&route, DriverError::HostFault);
                return Err(DriverError::HostFault);
            }
        }
        Ok(())
    }

    fn close_route(&self, route: &Arc<Route>, reason: DriverError) {
        let completion = {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            if state
                .active
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, route))
            {
                state.active = None;
            }
            state.live.as_mut().and_then(|slot| {
                if Arc::ptr_eq(&slot.route, route)
                    && matches!(
                        slot.invocation.phase(),
                        LivePhase::Open | LivePhase::CancelRequested
                    )
                {
                    slot.provisional_frame = None;
                    if reason == DriverError::HostFault {
                        slot.local_fault = true;
                    } else {
                        slot.invocation.peer_lost();
                    }
                    Some(slot.completion.clone())
                } else {
                    None
                }
            })
        };
        if let Some(completion) = completion {
            completion.finish(CompletionValue::Failed(reason));
        }
        route.close();
    }

    /// A rejected timer may roll back only its still unpublished exact claim.
    /// The queue decision also preserves a denial already reserved behind Q.
    fn reject_prepared(
        &self,
        route: &Arc<Route>,
        binding: &InvocationBinding,
        outcome: CompletionValue,
    ) -> bool {
        let (completion, decision) = {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            let Some(slot) = state.live.as_ref().filter(|slot| {
                Arc::ptr_eq(&slot.route, route)
                    && slot.invocation.binding().same_live_identity(binding)
                    && !slot.send_started
                    && matches!(
                        slot.invocation.phase(),
                        LivePhase::Open | LivePhase::CancelRequested
                    )
            }) else {
                return false;
            };
            if !route.has_inactive_request(binding.request_id()) {
                return false;
            }
            let completion = slot.completion.clone();
            let decision = route.prepare_fenced_route(binding.request_id());
            state.live = None;
            if !matches!(decision, FenceRouteDecision::PreserveDenial(_))
                && state
                    .active
                    .as_ref()
                    .is_some_and(|active| Arc::ptr_eq(active, route))
            {
                state.active = None;
            }
            (completion, decision)
        };
        route.finish_fence_decision(decision);
        completion.finish(outcome);
        true
    }

    fn expire(&self, route: &Arc<Route>, binding: &InvocationBinding) {
        let outcome = {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            let outcome = state.live.as_mut().and_then(|slot| {
                if Arc::ptr_eq(&slot.route, route)
                    && slot.invocation.binding().same_live_identity(binding)
                    && matches!(
                        slot.invocation.phase(),
                        LivePhase::Open | LivePhase::CancelRequested
                    )
                    && route.clock.now_ms()
                        >= slot
                            .cancel_deadline_ms
                            .unwrap_or(slot.invocation.effective_deadline_ms())
                {
                    slot.invocation.peer_lost();
                    slot.provisional_frame = None;
                    // Decide Q removal and whether an earlier D survives in
                    // one owner→queue admission order. An admitted Q remains
                    // irrevocable through its original write deadline.
                    let decision = route.prepare_fenced_route(binding.request_id());
                    Some((slot.completion.clone(), decision))
                } else {
                    None
                }
            });
            if outcome.as_ref().is_some_and(|(_, decision)| {
                !matches!(decision, FenceRouteDecision::PreserveDenial(_))
            }) && state
                .active
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, route))
            {
                state.active = None;
            }
            outcome
        };
        if let Some((completion, decision)) = outcome {
            completion.finish(CompletionValue::Failed(DriverError::PeerLost));
            route.finish_fence_decision(decision);
        }
    }

    fn admit_ordered_request(
        &self,
        route: &Arc<Route>,
        request_id: u64,
        write_deadline: u64,
    ) -> bool {
        let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        // The final monotonic sample and claim share the expiry owner line.
        // No await or queue operation may occur between them. Once claimed,
        // expiry cannot revoke this FIFO frame before its write deadline.
        if let Some(slot) = state.live.as_mut()
            && Arc::ptr_eq(&slot.route, route)
            && slot.invocation.binding().request_id() == request_id
            && matches!(
                slot.invocation.phase(),
                LivePhase::Open | LivePhase::CancelRequested
            )
            && route.status() != RouteStatus::Closed
        {
            let now = route.clock.now_ms();
            if now < slot.invocation.deadline_ms() && now < write_deadline {
                slot.send_started = true;
                route.mark_request_admitted(request_id);
                return true;
            }
        }
        false
    }

    fn admit_ordered_cancel(
        &self,
        route: &Arc<Route>,
        request_id: u64,
        write_deadline: u64,
    ) -> bool {
        let state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        if route.status() == RouteStatus::Closed {
            return false;
        }
        let cutoff = match state.live.as_ref().filter(|slot| {
            Arc::ptr_eq(&slot.route, route) && slot.invocation.binding().request_id() == request_id
        }) {
            Some(slot) if slot.invocation.phase() == LivePhase::CancelRequested => Some(
                slot.cancel_deadline_ms
                    .unwrap_or(slot.invocation.effective_deadline_ms()),
            ),
            Some(slot) if slot.invocation.phase() == LivePhase::TerminalReceived => None,
            _ if route.retired_cancel_pending(request_id) => None,
            _ => return false,
        };
        let now = route.clock.now_ms();
        if now < write_deadline && cutoff.is_none_or(|deadline| now < deadline) {
            route.mark_cancel_admitted(request_id);
            return true;
        }
        false
    }

    fn expire_request_id(&self, route: &Arc<Route>, request_id: u64) {
        let binding = self
            .0
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .live
            .as_ref()
            .filter(|slot| {
                Arc::ptr_eq(&slot.route, route)
                    && slot.invocation.binding().request_id() == request_id
            })
            .map(|slot| slot.invocation.binding().clone());
        if let Some(binding) = binding {
            self.expire(route, &binding);
        }
    }

    fn confirm_request_written(
        &self,
        route: &Arc<Route>,
        request_id: u64,
    ) -> Result<ReaderAction, DriverError> {
        let decision = {
            let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            let Some(slot) = state.live.as_mut() else {
                return Ok(self.after_lost_request_write(route));
            };
            if !Arc::ptr_eq(&slot.route, route)
                || slot.invocation.binding().request_id() != request_id
                || !slot.send_started
                || !matches!(
                    slot.invocation.phase(),
                    LivePhase::Open | LivePhase::CancelRequested
                )
            {
                return Ok(self.after_lost_request_write(route));
            }
            let now = route.clock.now_ms();
            if now
                >= slot
                    .cancel_deadline_ms
                    .unwrap_or(slot.invocation.effective_deadline_ms())
            {
                slot.invocation.peer_lost();
                slot.provisional_frame = None;
                Some(Ok((
                    Some((
                        slot.completion.clone(),
                        CompletionValue::Failed(DriverError::PeerLost),
                    )),
                    true,
                )))
            } else {
                slot.send_confirmed = true;
                slot.provisional_frame
                    .take()
                    .map(|frame| check_live_frame(slot, route, &frame, now))
            }
        };
        match decision {
            Some(decision) => self.finish_live_decision(route, decision),
            None => Ok(ReaderAction::Continue),
        }
    }

    fn after_lost_request_write(&self, route: &Route) -> ReaderAction {
        if route.status() != RouteStatus::Closed && route.has_control_pending() {
            ReaderAction::Continue
        } else {
            ReaderAction::Stop
        }
    }

    fn close_drained_after_cancel(&self, route: &Arc<Route>) -> bool {
        if route.status() != RouteStatus::Draining {
            return false;
        }
        let terminal_or_released = {
            let state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
            !state.live.as_ref().is_some_and(|slot| {
                Arc::ptr_eq(&slot.route, route)
                    && slot.invocation.phase() != LivePhase::TerminalReceived
            })
        };
        if !terminal_or_released || !route.close_unless_denial_pending() {
            return false;
        }
        let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        if state
            .active
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, route))
        {
            state.active = None;
        }
        true
    }
}

pub struct ReadyToolRoute {
    owner: SingleToolOwner,
    route: Arc<Route>,
}

impl ReadyToolRoute {
    pub fn grant(&self) -> &ActiveGrant {
        &self.route.grant
    }

    pub(crate) fn owner(&self) -> &SingleToolOwner {
        &self.owner
    }

    pub(crate) fn generation(&self) -> u64 {
        self.route.generation
    }

    pub(crate) fn is_current(&self) -> bool {
        let state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
        state
            .active
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, &self.route))
            && self.route.status() == RouteStatus::Active
    }

    pub(crate) fn has_echo_domain_projections(&self) -> bool {
        EchoDomainBinding::check_ready_echo_projections(self.grant()).is_ok()
    }

    /// Sample before the harness takes settings_line or its registry mutex.
    /// A clock callback may inspect harness state; admission checks expiry
    /// again at the synchronous claim using its own sample.
    pub(crate) fn check_timing(&self, timing: EchoRouteTiming) -> Result<(), DriverError> {
        (self.route.clock.now_ms() < timing.connection_deadline_ms)
            .then_some(())
            .ok_or(DriverError::InvalidHost)
    }

    /// Called once by the harness registration while holding settings_line.
    pub(crate) fn bind_timing(&self, timing: EchoRouteTiming) -> Result<(), DriverError> {
        let state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state
            .active
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, &self.route))
            || self.route.status() != RouteStatus::Active
        {
            return Err(DriverError::InvalidHost);
        }
        self.route
            .timing
            .set(timing)
            .map_err(|_| DriverError::InvalidHost)
    }

    /// Admit one exact planned echo call; this source unit grants zero progress credit.
    pub fn start_planned<C>(
        &self,
        plan: &ToolEffectPlan<C>,
        saved: &JsonObject,
        source_call_id: &str,
        timing: CallTiming,
    ) -> Result<CallHandle, DriverError> {
        self.prepare_planned_inner(plan, saved, source_call_id, StartTiming::Raw(timing))?
            .activate()
    }

    /// Direct-caller counterpart to `prepare_planned_with_bounds` using the
    /// legacy relative timing input. Activation still follows permit release.
    pub fn prepare_planned<C>(
        &self,
        plan: &ToolEffectPlan<C>,
        saved: &JsonObject,
        source_call_id: &str,
        timing: CallTiming,
    ) -> Result<PreparedCall, DriverError> {
        self.prepare_planned_inner(plan, saved, source_call_id, StartTiming::Raw(timing))
    }

    /// Claim using the policy bound at registration and invocation-scoped
    /// absolute cutoffs expressed in this route's monotonic clock domain.
    /// The first top-level echo passes None for both optional cutoffs.
    pub fn start_planned_with_bounds<C>(
        &self,
        plan: &ToolEffectPlan<C>,
        saved: &JsonObject,
        source_call_id: &str,
        requested_deadline_ms: Option<u64>,
        parent_deadline_ms: Option<u64>,
    ) -> Result<CallHandle, DriverError> {
        self.prepare_planned_with_bounds(
            plan,
            saved,
            source_call_id,
            requested_deadline_ms,
            parent_deadline_ms,
        )?
        .activate()
    }

    /// Commit the exact call and its inactive Request under the caller's
    /// checked Harness lines. No host task or waker is invoked in this phase.
    pub fn prepare_planned_with_bounds<C>(
        &self,
        plan: &ToolEffectPlan<C>,
        saved: &JsonObject,
        source_call_id: &str,
        requested_deadline_ms: Option<u64>,
        parent_deadline_ms: Option<u64>,
    ) -> Result<PreparedCall, DriverError> {
        self.prepare_planned_inner(
            plan,
            saved,
            source_call_id,
            StartTiming::Absolute {
                requested_deadline_ms,
                parent_deadline_ms,
            },
        )
    }

    fn prepare_planned_inner<C>(
        &self,
        plan: &ToolEffectPlan<C>,
        saved: &JsonObject,
        source_call_id: &str,
        timing: StartTiming,
    ) -> Result<PreparedCall, DriverError> {
        let key = encode_tool_effect_key(plan).map_err(|_| DriverError::InvalidHost)?;
        let arguments = encode_arguments(saved, &self.route.grant, "echo")
            .map_err(|_| DriverError::InvalidHost)?;
        let mut state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
        if !state
            .active
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, &self.route))
            || self.route.status() != RouteStatus::Active
        {
            return Err(DriverError::Closed);
        }
        if state.live.is_some() || self.route.normal_cap == 0 || self.route.has_cancel_pending() {
            return Err(DriverError::Overloaded);
        }
        if self.route.frame_cap > self.owner.0.limits.receipt_bytes.get() {
            return Err(DriverError::Overloaded);
        }
        // One sample under the owner admission lock sets every remaining bound
        // and the invocation's own deadline. No preflight duration can extend
        // an absolute caller, parent, or connection cutoff.
        let now = self.route.clock.now_ms();
        let timing = match timing {
            StartTiming::Raw(timing) => timing,
            StartTiming::Absolute {
                requested_deadline_ms,
                parent_deadline_ms,
            } => {
                let policy = self.route.timing.get().ok_or(DriverError::InvalidHost)?;
                let remaining = |deadline: u64| {
                    deadline
                        .checked_sub(now)
                        .filter(|remaining| *remaining > 0)
                        .ok_or(DriverError::Deadline)
                };
                CallTiming {
                    requested_after_ms: requested_deadline_ms.map(remaining).transpose()?,
                    method_limit_ms: policy.method_limit_ms.get(),
                    connection_remaining_ms: remaining(policy.connection_deadline_ms)?,
                    parent_remaining_ms: parent_deadline_ms.map(remaining).transpose()?,
                }
            }
        };
        // The owner lock serializes candidate IDs. Commit this ID only after
        // the complete Request has reached the queue with a live binding.
        let id = self.route.next_id.load(Ordering::SeqCst);
        if id == 0 || id == u64::MAX {
            return Err(DriverError::Overloaded);
        }
        let (invocation, frame) = ToolInvocation::start(
            &self.route.grant,
            "echo",
            pi_extension_protocol::tool_live::Start {
                effect_key: key,
                source_call_id: source_call_id.to_owned(),
                request_id: id,
                active_generation: self.route.generation,
                arguments,
                now_ms: now,
                requested_deadline_after_ms: timing.requested_after_ms,
                method_limit_ms: timing.method_limit_ms,
                connection_remaining_ms: timing.connection_remaining_ms,
                parent_remaining_ms: timing.parent_remaining_ms,
                initial_credit_bytes: Some(0),
            },
        )
        .map_err(|_| DriverError::InvalidHost)?;
        let domain = EchoDomainBinding::new(&self.route.grant, &invocation)
            .map_err(|_| DriverError::HostFault)?;
        let binding = invocation.binding().clone();
        let deadline = invocation.deadline_ms();
        let write_deadline = now
            .checked_add(self.route.timeout_ms)
            .ok_or(DriverError::Deadline)?;
        let completion = Arc::new(Completion::new());
        // This is the claim commit: queue reservation, live slot and ID move
        // together under owner then queue. A peer denial that reserved first
        // has already moved this route out of Active.
        {
            let mut writes = self.route.writes.lock().unwrap_or_else(|p| p.into_inner());
            if self.route.status() != RouteStatus::Active {
                return Err(DriverError::Closed);
            }
            writes
                .jobs
                .try_reserve(1)
                .map_err(|_| DriverError::Overloaded)?;
            writes.reserve(
                WriteKind::Request(id),
                frame.bytes.len(),
                self.route.normal_cap,
                self.route.control_cap,
                self.route.cancel_cap,
            )?;
            writes.jobs.push_back(WriteJob {
                frame,
                kind: WriteKind::Request(id),
                deadline: write_deadline,
                active: false,
            });
            state.live = Some(LiveSlot {
                invocation,
                domain,
                route: self.route.clone(),
                completion: completion.clone(),
                send_started: false,
                send_confirmed: false,
                provisional_frame: None,
                local_fault: false,
                cancel_deadline_ms: None,
            });
            self.route.next_id.store(id + 1, Ordering::SeqCst);
        }
        Ok(PreparedCall {
            owner: self.owner.clone(),
            route: self.route.clone(),
            handle: Some(CallHandle {
                binding,
                completion,
            }),
            invocation_deadline: deadline,
            write_deadline,
        })
    }

    /// Called under the host settings line before replacing this route.
    /// Claims already admitted under the owner line remain bound to this route.
    pub fn stop_new_calls(&self) {
        let _state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
        self.route.mark_draining();
    }

    pub fn close(&self) {
        self.owner.close_route(&self.route, DriverError::PeerLost);
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum RouteStatus {
    Active,
    Draining,
    Closed,
}

enum FenceRouteDecision {
    PreserveDenial(Option<Waker>),
    Close(Option<Waker>),
    AlreadyClosed,
}

struct Route {
    generation: u64,
    grant: Arc<ActiveGrant>,
    channel: Arc<dyn OrderedByteChannel>,
    clock: Arc<dyn MonotonicClock>,
    supervisor: Supervisor,
    status: std::sync::atomic::AtomicU8,
    next_id: AtomicU64,
    frame_cap: usize,
    normal_cap: usize,
    control_cap: usize,
    cancel_cap: usize,
    timeout_ms: u64,
    timing: OnceLock<EchoRouteTiming>,
    writes: Mutex<WriteState>,
}

impl Route {
    fn status(&self) -> RouteStatus {
        match self.status.load(Ordering::SeqCst) {
            0 => RouteStatus::Active,
            1 => RouteStatus::Draining,
            _ => RouteStatus::Closed,
        }
    }
    fn mark_draining(&self) {
        let _ = self
            .status
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst);
    }
    fn close(&self) {
        if self.status.swap(2, Ordering::SeqCst) == 2 {
            return;
        }
        self.channel.close();
        let wake = {
            let mut writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
            let wake = writes.waker.take();
            writes.jobs.clear();
            writes.normal_bytes = 0;
            writes.control_bytes = 0;
            writes.cancel_bytes = 0;
            writes.popped_unadmitted = None;
            writes.retired_cancel_id = None;
            wake
        };
        if let Some(waker) = wake {
            waker.wake();
        }
        self.supervisor.retire();
    }

    // Reservation and close must be decided under the same queue lock. A peer
    // Request wins by reserving control bytes first; otherwise enqueue sees
    // Closed and cannot create a denial that this abort would then discard.
    fn close_unless_denial_pending(&self) -> bool {
        let wake = {
            let mut writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
            if self.status() != RouteStatus::Closed && writes.control_bytes != 0 {
                return false;
            }
            if self.status.swap(2, Ordering::SeqCst) == 2 {
                return true;
            }
            let wake = writes.waker.take();
            writes.jobs.clear();
            writes.normal_bytes = 0;
            writes.control_bytes = 0;
            writes.cancel_bytes = 0;
            writes.popped_unadmitted = None;
            writes.retired_cancel_id = None;
            wake
        };
        self.channel.close();
        if let Some(waker) = wake {
            waker.wake();
        }
        self.supervisor.retire();
        true
    }

    // Called with the owner line held. This queue-lock decision is the exact
    // fence linearization: an earlier D survives, and a later D is rejected.
    // Channel close and arbitrary wakers must run after both locks are gone.
    fn prepare_fenced_route(&self, request_id: u64) -> FenceRouteDecision {
        let mut writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        self.mark_draining();
        let removed_head = Self::discard_unadmitted_request_locked(&mut writes, request_id);
        Self::discard_unadmitted_cancel_locked(&mut writes, request_id);
        writes.denial_fenced = true;
        if self.status() != RouteStatus::Closed && writes.control_bytes != 0 {
            return FenceRouteDecision::PreserveDenial(if removed_head {
                writes.waker.take()
            } else {
                None
            });
        }
        if self.status.swap(2, Ordering::SeqCst) == 2 {
            return FenceRouteDecision::AlreadyClosed;
        }
        let waker = writes.waker.take();
        writes.jobs.clear();
        writes.normal_bytes = 0;
        writes.control_bytes = 0;
        writes.cancel_bytes = 0;
        writes.popped_unadmitted = None;
        writes.retired_cancel_id = None;
        FenceRouteDecision::Close(waker)
    }

    fn finish_prepared_close(&self, waker: Option<Waker>) {
        self.channel.close();
        if let Some(waker) = waker {
            waker.wake();
        }
        self.supervisor.retire();
    }

    fn finish_fence_decision(&self, decision: FenceRouteDecision) {
        match decision {
            FenceRouteDecision::PreserveDenial(waker) => {
                if let Some(waker) = waker {
                    waker.wake();
                }
            }
            FenceRouteDecision::Close(waker) => self.finish_prepared_close(waker),
            FenceRouteDecision::AlreadyClosed => {}
        }
    }
    fn enqueue(
        &self,
        frame: EncodedFrame,
        kind: WriteKind,
    ) -> Result<(Option<Waker>, u64), DriverError> {
        if self.status() == RouteStatus::Closed {
            return Err(DriverError::Closed);
        }
        let bytes = frame.bytes.len();
        let denial = matches!(kind, WriteKind::Denial);
        let deadline = self
            .clock
            .now_ms()
            .checked_add(self.timeout_ms)
            .ok_or(DriverError::Deadline)?;
        let wake = {
            let mut writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
            if self.status() == RouteStatus::Closed
                || (denial && writes.denial_fenced)
                || (matches!(kind, WriteKind::Request(_)) && self.status() != RouteStatus::Active)
            {
                return Err(DriverError::Closed);
            }
            writes.reserve(
                kind,
                bytes,
                self.normal_cap,
                self.control_cap,
                self.cancel_cap,
            )?;
            if denial {
                self.mark_draining();
            }
            writes.jobs.push_back(WriteJob {
                frame,
                kind,
                deadline,
                active: true,
            });
            writes.waker.take()
        };
        Ok((wake, deadline))
    }

    fn discard_unadmitted_request(&self, request_id: u64) -> Option<Waker> {
        let mut writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        if Self::discard_unadmitted_request_locked(&mut writes, request_id)
            && !writes.jobs.is_empty()
        {
            writes.waker.take()
        } else {
            None
        }
    }

    fn has_inactive_request(&self, request_id: u64) -> bool {
        self.writes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .jobs
            .iter()
            .any(|job| {
                matches!(job.kind, WriteKind::Request(id) if id == request_id) && !job.active
            })
    }

    fn discard_unadmitted_request_locked(writes: &mut WriteState, request_id: u64) -> bool {
        if let Some(index) = writes
            .jobs
            .iter()
            .position(|job| matches!(job.kind, WriteKind::Request(id) if id == request_id))
        {
            let job = writes.jobs.remove(index).expect("queued request index");
            writes.normal_bytes -= job.frame.bytes.len();
            return index == 0;
        }
        if let Some((WriteKind::Request(id), size)) = writes.popped_unadmitted
            && id == request_id
        {
            writes.normal_bytes -= size;
            writes.popped_unadmitted = None;
        }
        false
    }

    fn discard_unadmitted_cancel_locked(writes: &mut WriteState, request_id: u64) {
        if let Some(index) = writes
            .jobs
            .iter()
            .position(|job| matches!(job.kind, WriteKind::Cancel(id) if id == request_id))
        {
            let job = writes.jobs.remove(index).expect("queued cancel index");
            writes.cancel_bytes -= job.frame.bytes.len();
        }
        if let Some((WriteKind::Cancel(id), size)) = writes.popped_unadmitted
            && id == request_id
        {
            writes.cancel_bytes -= size;
            writes.popped_unadmitted = None;
        }
        if writes.cancel_bytes == 0 && writes.retired_cancel_id == Some(request_id) {
            writes.retired_cancel_id = None;
        }
    }

    fn mark_request_admitted(&self, request_id: u64) {
        let mut writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        if writes
            .popped_unadmitted
            .is_some_and(|(kind, _)| matches!(kind, WriteKind::Request(id) if id == request_id))
        {
            writes.popped_unadmitted = None;
        }
    }

    fn mark_cancel_admitted(&self, request_id: u64) {
        let mut writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        if writes
            .popped_unadmitted
            .is_some_and(|(kind, _)| matches!(kind, WriteKind::Cancel(id) if id == request_id))
        {
            writes.popped_unadmitted = None;
        }
    }

    fn release_write(&self, kind: WriteKind, size: usize) {
        let mut writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        let reserved = match kind {
            WriteKind::Denial => &mut writes.control_bytes,
            WriteKind::Cancel(_) => &mut writes.cancel_bytes,
            WriteKind::Request(_) => &mut writes.normal_bytes,
        };
        *reserved = reserved.saturating_sub(size);
        if matches!(kind, WriteKind::Cancel(_)) && writes.cancel_bytes == 0 {
            writes.retired_cancel_id = None;
        }
    }

    fn has_denial_pending(&self) -> bool {
        self.writes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .control_bytes
            != 0
    }

    fn has_control_pending(&self) -> bool {
        let writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        writes.control_bytes != 0 || writes.cancel_bytes != 0
    }

    fn has_cancel_pending(&self) -> bool {
        self.writes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .cancel_bytes
            != 0
    }

    fn mark_retired_cancel_if_pending(&self, request_id: u64) {
        let mut writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        if writes.cancel_bytes != 0 {
            writes.retired_cancel_id = Some(request_id);
        }
    }

    fn retired_cancel_pending(&self, request_id: u64) -> bool {
        let writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        writes.cancel_bytes != 0 && writes.retired_cancel_id == Some(request_id)
    }

    fn has_retired_cancel_pending(&self) -> bool {
        let writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        writes.cancel_bytes != 0 && writes.retired_cancel_id.is_some()
    }
}

#[derive(Clone, Copy)]
enum WriteKind {
    Request(u64),
    Cancel(u64),
    Denial,
}
struct WriteJob {
    frame: EncodedFrame,
    kind: WriteKind,
    deadline: u64,
    active: bool,
}
#[derive(Default)]
struct WriteState {
    jobs: VecDeque<WriteJob>,
    normal_bytes: usize,
    control_bytes: usize,
    cancel_bytes: usize,
    retired_cancel_id: Option<u64>,
    popped_unadmitted: Option<(WriteKind, usize)>,
    denial_fenced: bool,
    waker: Option<Waker>,
}

impl WriteState {
    fn reserve(
        &mut self,
        kind: WriteKind,
        bytes: usize,
        normal_cap: usize,
        denial_cap: usize,
        cancel_cap: usize,
    ) -> Result<(), DriverError> {
        let (reserved, cap) = match kind {
            WriteKind::Request(_) => (&mut self.normal_bytes, normal_cap),
            WriteKind::Denial => (&mut self.control_bytes, denial_cap),
            WriteKind::Cancel(_) => (&mut self.cancel_bytes, cancel_cap),
        };
        *reserved = reserved
            .checked_add(bytes)
            .filter(|next| *next <= cap)
            .ok_or(DriverError::Overloaded)?;
        Ok(())
    }
}

async fn writer_loop(owner: SingleToolOwner, route: Arc<Route>) {
    loop {
        let job = poll_fn(|cx| {
            let mut writes = route.writes.lock().unwrap_or_else(|p| p.into_inner());
            if route.status() == RouteStatus::Closed {
                return Poll::Ready(None);
            }
            if writes.jobs.front().is_some_and(|job| !job.active) {
                let prior = writes.waker.replace(cx.waker().clone());
                drop(writes);
                drop(prior);
                return Poll::Pending;
            }
            if let Some(job) = writes.jobs.pop_front() {
                if matches!(job.kind, WriteKind::Request(_) | WriteKind::Cancel(_)) {
                    writes.popped_unadmitted = Some((job.kind, job.frame.bytes.len()));
                }
                return Poll::Ready(Some(job));
            }
            let prior = writes.waker.replace(cx.waker().clone());
            drop(writes);
            drop(prior);
            Poll::Pending
        })
        .await;
        let Some(job) = job else { break };
        if let WriteKind::Request(id) = job.kind
            && !owner.admit_ordered_request(&route, id, job.deadline)
        {
            owner.expire_request_id(&route, id);
            if let Some(waker) = route.discard_unadmitted_request(id) {
                waker.wake();
            }
            if route.status() == RouteStatus::Closed {
                break;
            }
            if route.has_denial_pending() {
                continue;
            }
            owner.close_route(&route, DriverError::PeerLost);
            break;
        }
        if let WriteKind::Cancel(id) = job.kind
            && !owner.admit_ordered_cancel(&route, id, job.deadline)
        {
            owner.expire_request_id(&route, id);
            route.release_write(job.kind, job.frame.bytes.len());
            if route.status() == RouteStatus::Closed {
                break;
            }
            if route.has_denial_pending() {
                continue;
            }
            owner.close_route(&route, DriverError::PeerLost);
            break;
        }
        let size = job.frame.bytes.len();
        if route.clock.now_ms() >= job.deadline {
            route.release_write(job.kind, size);
            owner.close_route(&route, DriverError::PeerLost);
            break;
        }
        let mut write = route.channel.write_frame(job.frame);
        let mut timer = route.clock.sleep_until(job.deadline);
        let completed = poll_fn(|cx| {
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(false);
            }
            write.as_mut().poll(cx).map(|result| result.is_ok())
        })
        .await
            && route.clock.now_ms() < job.deadline;
        let denial = matches!(job.kind, WriteKind::Denial);
        route.release_write(job.kind, size);
        if !completed {
            owner.close_route(&route, DriverError::PeerLost);
            break;
        }
        if let WriteKind::Request(id) = job.kind {
            match owner.confirm_request_written(&route, id) {
                Ok(ReaderAction::Continue) => {}
                Ok(ReaderAction::Stop) => break,
                Err(error) => {
                    owner.close_route(&route, error);
                    break;
                }
            }
        }
        if denial {
            owner.close_route(&route, DriverError::PeerLost);
            break;
        }
        if matches!(job.kind, WriteKind::Cancel(_)) && owner.close_drained_after_cancel(&route) {
            break;
        }
    }
}

enum ReaderAction {
    Continue,
    Stop,
}

async fn reader_loop(owner: SingleToolOwner, route: Arc<Route>, mut pump: ConnectionPump) {
    loop {
        match pump.next_active().await {
            Ok(Some(frame)) => match owner.on_frame(&route, frame) {
                Ok(ReaderAction::Continue) => {}
                Ok(ReaderAction::Stop) => break,
                Err(error) => {
                    owner.close_route(&route, error);
                    break;
                }
            },
            Ok(None) | Err(_) => {
                owner.close_route(&route, DriverError::PeerLost);
                break;
            }
        }
    }
}

enum Incoming {
    Item(u64),
    End(u64),
    GoAway(i32),
    Request(u64, String, Option<PeerRef>),
    Wrong,
}

struct PeerProfile {
    name: String,
    major: u32,
    minor: u32,
}

struct PeerRef {
    principal: String,
    kind: String,
    name: String,
    profile: Option<PeerProfile>,
    digest: Vec<u8>,
}

fn peer_ref_1_0(reference: &v1_0::CapabilityRef) -> PeerRef {
    PeerRef {
        principal: reference.authenticated_extension_id.clone(),
        kind: reference.kind.clone(),
        name: reference.name.clone(),
        profile: reference.profile.as_ref().map(|profile| PeerProfile {
            name: profile.name.clone(),
            major: profile.major,
            minor: profile.minor,
        }),
        digest: reference.schema_digest_sha256.clone(),
    }
}

fn peer_ref_1_1(reference: &v1_1::CapabilityRef) -> PeerRef {
    PeerRef {
        principal: reference.authenticated_extension_id.clone(),
        kind: reference.kind.clone(),
        name: reference.name.clone(),
        profile: reference.profile.as_ref().map(|profile| PeerProfile {
            name: profile.name.clone(),
            major: profile.major,
            minor: profile.minor,
        }),
        digest: reference.schema_digest_sha256.clone(),
    }
}

type LiveOutcome = (Option<(Arc<Completion>, CompletionValue)>, bool);
type LiveDecision = Result<LiveOutcome, DriverError>;

fn check_live_frame(
    slot: &mut LiveSlot,
    route: &Arc<Route>,
    frame: &ActiveFrame,
    now: u64,
) -> LiveDecision {
    match slot.domain.receive_checked_with_final_time(
        &mut slot.invocation,
        route.generation,
        frame,
        now,
        slot.cancel_deadline_ms,
        || route.clock.now_ms(),
    ) {
        Ok(checked) => match checked.outcome {
            ReceiveOutcome::Progress(_) | ReceiveOutcome::IgnoredCancelledProgress => {
                Ok((None, false))
            }
            ReceiveOutcome::TerminalReceived => {
                let terminal = match slot.invocation.receipt().expect("terminal retained").kind() {
                    TerminalKind::Result => match checked.value {
                        Some(value) => TypedTerminal::Result(value),
                        None => {
                            return Ok((
                                Some((
                                    slot.completion.clone(),
                                    CompletionValue::Failed(DriverError::HostFault),
                                )),
                                true,
                            ));
                        }
                    },
                    TerminalKind::Error => TypedTerminal::PeerError(
                        slot.invocation
                            .receipt()
                            .expect("error retained")
                            .error()
                            .expect("typed error")
                            .0,
                    ),
                };
                Ok((
                    Some((slot.completion.clone(), CompletionValue::Terminal(terminal))),
                    false,
                ))
            }
            ReceiveOutcome::Stale => Ok((None, true)),
        },
        Err(DomainReceiveError::Host(_)) => {
            slot.local_fault = true;
            Ok((
                Some((
                    slot.completion.clone(),
                    CompletionValue::Failed(DriverError::HostFault),
                )),
                true,
            ))
        }
        Err(DomainReceiveError::PeerLost)
        | Err(DomainReceiveError::Protocol(LiveError::PeerLost)) => {
            slot.provisional_frame = None;
            Ok((
                Some((
                    slot.completion.clone(),
                    CompletionValue::Failed(DriverError::PeerLost),
                )),
                true,
            ))
        }
        Err(DomainReceiveError::Protocol(_)) => Ok((
            Some((
                slot.completion.clone(),
                CompletionValue::Failed(DriverError::MalformedPeer),
            )),
            true,
        )),
    }
}

impl SingleToolOwner {
    fn on_frame(
        &self,
        route: &Arc<Route>,
        frame: ActiveFrame,
    ) -> Result<ReaderAction, DriverError> {
        let incoming = match &frame.envelope {
            SelectedEnvelope::V1_0(envelope) => match envelope.frame.as_ref() {
                Some(v1_0::envelope::Frame::StreamItem(item)) => Incoming::Item(item.request_id),
                Some(v1_0::envelope::Frame::StreamEnd(end)) => Incoming::End(end.request_id),
                Some(v1_0::envelope::Frame::GoAway(go)) => Incoming::GoAway(go.reason),
                Some(v1_0::envelope::Frame::Request(req)) => Incoming::Request(
                    req.request_id,
                    req.method.clone(),
                    req.capability_ref.as_ref().map(peer_ref_1_0),
                ),
                _ => Incoming::Wrong,
            },
            SelectedEnvelope::V1_1(envelope) => match envelope.frame.as_ref() {
                Some(v1_1::envelope::Frame::StreamItem(item)) => Incoming::Item(item.request_id),
                Some(v1_1::envelope::Frame::StreamEnd(end)) => Incoming::End(end.request_id),
                Some(v1_1::envelope::Frame::GoAway(go)) => Incoming::GoAway(go.reason),
                Some(v1_1::envelope::Frame::Request(req)) => Incoming::Request(
                    req.request_id,
                    req.method.clone(),
                    req.capability_ref.as_ref().map(peer_ref_1_1),
                ),
                _ => Incoming::Wrong,
            },
        };
        match incoming {
            Incoming::Item(id) | Incoming::End(id) => {
                let checked = (|| -> Result<Option<LiveOutcome>, DriverError> {
                    let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
                    let slot = state.live.as_mut().ok_or(DriverError::MalformedPeer)?;
                    if !Arc::ptr_eq(&slot.route, route)
                        || slot.invocation.binding().request_id() != id
                        || !slot.send_started
                    {
                        return Err(DriverError::MalformedPeer);
                    }
                    let now = route.clock.now_ms();
                    if slot.invocation.phase() == LivePhase::PeerLost {
                        return Ok(Some((None, false)));
                    }
                    if now
                        >= slot
                            .cancel_deadline_ms
                            .unwrap_or(slot.invocation.effective_deadline_ms())
                    {
                        slot.invocation.peer_lost();
                        slot.provisional_frame = None;
                        return Ok(Some((
                            Some((
                                slot.completion.clone(),
                                CompletionValue::Failed(DriverError::PeerLost),
                            )),
                            true,
                        )));
                    }
                    if !slot.send_confirmed {
                        if slot.provisional_frame.is_some()
                            || !matches!(
                                slot.invocation.phase(),
                                LivePhase::Open | LivePhase::CancelRequested
                            )
                        {
                            return Err(DriverError::MalformedPeer);
                        }
                        // At most one selected frame is held behind the full-write
                        // confirmation fence; zero credit means no valid item can
                        // precede the first terminal in this source unit.
                        slot.provisional_frame = Some(frame);
                        return Ok(None);
                    }
                    check_live_frame(slot, route, &frame, now).map(Some)
                })();
                match checked {
                    Ok(None) => Ok(ReaderAction::Continue),
                    Ok(Some(outcome)) => self.finish_live_decision(route, Ok(outcome)),
                    Err(error) => self.finish_live_decision(route, Err(error)),
                }
            }
            Incoming::GoAway(raw) => match v1_1::GoAwayReason::try_from(raw) {
                Ok(v1_1::GoAwayReason::GoAwayDrain) => {
                    let (pending, control_pending) = {
                        let state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
                        let writes = route.writes.lock().unwrap_or_else(|p| p.into_inner());
                        route.mark_draining();
                        (
                            state.live.as_ref().is_some_and(|slot| {
                                Arc::ptr_eq(&slot.route, route)
                                    && matches!(
                                        slot.invocation.phase(),
                                        LivePhase::Open | LivePhase::CancelRequested
                                    )
                            }),
                            writes.control_bytes != 0 || writes.cancel_bytes != 0,
                        )
                    };
                    if pending || control_pending {
                        Ok(ReaderAction::Continue)
                    } else {
                        self.close_route(route, DriverError::PeerLost);
                        Ok(ReaderAction::Stop)
                    }
                }
                Ok(
                    v1_1::GoAwayReason::GoAwayClose
                    | v1_1::GoAwayReason::GoAwayMalformed
                    | v1_1::GoAwayReason::GoAwayLimit,
                ) => {
                    self.close_route(route, DriverError::PeerLost);
                    Ok(ReaderAction::Stop)
                }
                _ => self.close_malformed(route),
            },
            Incoming::Request(..) if route.has_denial_pending() => Ok(ReaderAction::Continue),
            Incoming::Request(id, method, reference) => {
                if id == 0 || method.is_empty() {
                    return self.close_malformed(route);
                }
                let Some(reference) = reference else {
                    return self.close_malformed(route);
                };
                let Some(profile) = validated_peer_ref(route, &reference) else {
                    return self.close_malformed(route);
                };
                let code = if known_method(&profile.name, profile.major, profile.minor, &method) {
                    v1_1::ErrorCode::Forbidden
                } else {
                    v1_1::ErrorCode::UnsupportedMethod
                };
                let response = denial_frame(route, id, code)?;
                let (wake, deadline) = route.enqueue(response, WriteKind::Denial)?;
                let timer_owner = self.clone();
                let timer_route = route.clone();
                if route
                    .supervisor
                    .submit(Box::pin(async move {
                        timer_route.clock.sleep_until(deadline).await;
                        if timer_route.has_denial_pending() {
                            timer_owner.close_route(&timer_route, DriverError::PeerLost);
                        }
                    }))
                    .is_err()
                {
                    self.close_route(route, DriverError::HostFault);
                    return Err(DriverError::HostFault);
                }
                if let Some(waker) = wake {
                    waker.wake();
                }
                Ok(ReaderAction::Continue)
            }
            _ => self.close_malformed(route),
        }
    }

    fn close_malformed<T>(&self, route: &Arc<Route>) -> Result<T, DriverError> {
        self.close_route(route, DriverError::MalformedPeer);
        Err(DriverError::MalformedPeer)
    }

    fn finish_live_decision(
        &self,
        route: &Arc<Route>,
        checked: LiveDecision,
    ) -> Result<ReaderAction, DriverError> {
        let (finish, close) = match checked {
            Ok(value) => value,
            Err(DriverError::MalformedPeer) => return self.close_malformed(route),
            Err(error) => {
                self.close_route(route, error);
                return Err(error);
            }
        };
        let peer_lost_with_denial = finish.as_ref().is_some_and(|(_, value)| {
            matches!(value, CompletionValue::Failed(DriverError::PeerLost))
        }) && route.has_denial_pending();
        if let Some((completion, value)) = finish {
            completion.finish(value);
        }
        if (close && !peer_lost_with_denial)
            || (route.status() == RouteStatus::Draining && !route.has_control_pending())
        {
            self.close_route(route, DriverError::PeerLost);
            Ok(ReaderAction::Stop)
        } else {
            Ok(ReaderAction::Continue)
        }
    }
}

fn known_method(profile: &str, profile_major: u32, profile_minor: u32, method: &str) -> bool {
    if profile_major != 1 {
        return false;
    }
    let Ok(table) = serde_json::from_str::<serde_json::Value>(include_str!(
        "../../../pi-extension-protocol/schema/methods.json"
    )) else {
        return false;
    };
    let minor = match profile_minor {
        0 => "1.0",
        1 => "1.1",
        _ => return false,
    };
    table[minor][profile].get(method).is_some()
}

fn validated_peer_ref<'a>(route: &Route, reference: &'a PeerRef) -> Option<&'a PeerProfile> {
    let profile = reference.profile.as_ref()?;
    if reference.principal != route.grant.wire().authenticated_extension_id
        || reference.kind.is_empty()
        || reference.name.is_empty()
        || profile.name.is_empty()
        || profile.major == 0
        || reference.digest.len() != 32
    {
        return None;
    }
    let expected_kind = match profile.name.as_str() {
        "tool.v1" => Some("tool"),
        "hook.v1" => Some("hook"),
        "provider.v1" => Some("provider"),
        "event-sink.v1" => Some("event-sink"),
        "resource.v1" => Some("resource"),
        "host-action.v1" => Some("host-action"),
        "telemetry-sink.v1" => Some("telemetry-sink"),
        _ => None,
    };
    if expected_kind.is_some_and(|kind| reference.kind != kind) {
        return None;
    }
    if let Some(selected) = route
        .grant
        .wire()
        .selected_capabilities
        .iter()
        .find(|selected| {
            selected
                .profile
                .as_ref()
                .is_some_and(|version| version.name == profile.name)
        })
    {
        let selected_profile = selected.profile.as_ref()?;
        if reference.kind != selected.kind
            || profile.major != selected_profile.major
            || profile.minor != selected_profile.minor
            || reference.digest != selected.schema_digest_sha256
        {
            return None;
        }
    } else if let Some(expected) =
        canonical_profile_digest(&profile.name, profile.major, profile.minor)
        && reference.digest != expected
    {
        return None;
    }
    Some(profile)
}

fn denial_frame(
    route: &Route,
    id: u64,
    code: v1_1::ErrorCode,
) -> Result<EncodedFrame, DriverError> {
    let max = NonZeroUsize::new(route.frame_cap).ok_or(DriverError::InvalidHost)?;
    if route
        .grant
        .wire()
        .selected_protocol
        .ok_or(DriverError::InvalidHost)?
        .minor
        == 0
    {
        framing::encode(
            &v1_0::Envelope {
                frame: Some(v1_0::envelope::Frame::Response(v1_0::Response {
                    request_id: id,
                    terminal: Some(v1_0::response::Terminal::Error(v1_0::TypedError {
                        code: code as i32,
                        safe_message: String::new(),
                    })),
                })),
            },
            max,
        )
        .map_err(|_| DriverError::InvalidHost)
    } else {
        framing::encode(
            &v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::Response(v1_1::Response {
                    request_id: id,
                    terminal: Some(v1_1::response::Terminal::Error(v1_1::TypedError {
                        code: code as i32,
                        safe_message: String::new(),
                    })),
                })),
            },
            max,
        )
        .map_err(|_| DriverError::InvalidHost)
    }
}

#[cfg(test)]
mod local_cancel_queue_tests {
    use super::*;

    #[test]
    fn popped_unadmitted_request_releases_only_its_exact_reservation() {
        for denial_bytes in [0, 3] {
            let mut writes = WriteState {
                normal_bytes: 7,
                control_bytes: denial_bytes,
                popped_unadmitted: Some((WriteKind::Request(41), 7)),
                ..WriteState::default()
            };
            Route::discard_unadmitted_request_locked(&mut writes, 42);
            assert_eq!(writes.normal_bytes, 7);
            Route::discard_unadmitted_request_locked(&mut writes, 41);
            assert_eq!(writes.normal_bytes, 0);
            assert_eq!(writes.control_bytes, denial_bytes);
            assert!(writes.popped_unadmitted.is_none());
        }
    }

    #[test]
    fn popped_unadmitted_cancel_releases_only_cancel_bytes() {
        let mut writes = WriteState {
            cancel_bytes: 5,
            control_bytes: 3,
            popped_unadmitted: Some((WriteKind::Cancel(41), 5)),
            ..WriteState::default()
        };
        Route::discard_unadmitted_cancel_locked(&mut writes, 41);
        assert_eq!((writes.cancel_bytes, writes.control_bytes), (0, 3));
        assert!(writes.popped_unadmitted.is_none());
    }

    #[test]
    fn cancel_and_denial_each_keep_a_full_independent_control_reservation() {
        let mut writes = WriteState::default();
        writes.reserve(WriteKind::Cancel(41), 3, 1, 3, 3).unwrap();
        writes.reserve(WriteKind::Denial, 3, 1, 3, 3).unwrap();
        assert_eq!((writes.cancel_bytes, writes.control_bytes), (3, 3));
        assert_eq!(
            writes.reserve(WriteKind::Cancel(41), 1, 1, 3, 3),
            Err(DriverError::Overloaded)
        );
        assert_eq!(
            writes.reserve(WriteKind::Denial, 1, 1, 3, 3),
            Err(DriverError::Overloaded)
        );
    }
}
