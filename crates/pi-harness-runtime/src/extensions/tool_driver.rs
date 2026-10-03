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
use crate::extensions::receive_host8192::StagedReceive8192;
#[cfg(test)]
use crate::extensions::transport::source_observer;
use crate::extensions::transport::{
    ConnectionPump, MonotonicClock, OrderedByteChannel, PumpConfig, RetainedOrderedByteChannel,
};
use crate::extensions::value::encode_arguments;
use crate::scheduler::Supervisor;
use crate::tools::{CapturedArguments, OriginFence};

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

#[cfg(test)]
pub(crate) fn retained_resource_snapshot_for_test(generation: u64) {
    super::receive_host8192::scenario_snapshot_for_test(generation);
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

pub(crate) trait ToolProfileIngress: Send + Sync {
    fn incoming(&self, frame: ActiveFrame) -> Result<(), DriverError>;
    fn admit_write(&self, id: u64) -> pi_ai_core::BoxFuture<'_, bool>;
    fn written(&self, id: u64, completed: bool);
    fn close(&self);
}
struct ToolProfileIds {
    owner: SingleToolOwner,
    route: Arc<Route>,
}
impl pi_extension_protocol::profile_live::ConnectionIds for ToolProfileIds {
    fn connection(&self) -> pi_extension_protocol::profile_live::ConnectionIdentity {
        self.route.connection.clone()
    }
    fn reserve_request_id(
        &self,
        now_ms: u64,
        requested: Option<u64>,
        minimum: u64,
    ) -> Result<u64, pi_extension_protocol::profile_live::Error> {
        use pi_extension_protocol::profile_live::Error;
        let state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
        if state
            .active
            .as_ref()
            .is_none_or(|active| !Arc::ptr_eq(active, &self.route))
            || self.route.status() != RouteStatus::Active
            || self.route.timing.get().is_some_and(|timing| {
                now_ms.max(self.route.clock.now_ms()) >= timing.connection_deadline_ms
            })
        {
            return Err(Error::Generation);
        }
        let first = self.route.next_id.load(Ordering::SeqCst).max(minimum);
        let id = requested.unwrap_or(first);
        if id < first || id == 0 || id == u64::MAX {
            return Err(Error::Stale);
        }
        self.route.next_id.store(id + 1, Ordering::SeqCst);
        Ok(id)
    }
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

#[derive(Clone)]
struct CapturedCaller8192 {
    binding: Option<InvocationBinding>,
    key: pi_agent_schema::EffectKey,
    arguments: CapturedArguments,
    source_call_id: String,
    epoch: u64,
    origin: Arc<dyn OriginFence>,
}

impl CapturedCaller8192 {
    fn staged_identity(&self) -> crate::extensions::receive_host8192::CallerIdentity8192 {
        crate::extensions::receive_host8192::CallerIdentity8192 {
            arguments: self.arguments.clone(),
            source_call_id: self.source_call_id.clone(),
            epoch: self.epoch,
            origin: self.origin.clone(),
        }
    }

    fn matches<C>(
        &self,
        route: &Route,
        plan: &ToolEffectPlan<C>,
        captured: &CapturedArguments,
        source_call_id: &str,
        epoch: u64,
        origin: &Arc<dyn OriginFence>,
    ) -> bool {
        self.key == plan.key
            && self.arguments.key == plan.args_key
            && self.arguments.key == captured.key
            && self.arguments.value == captured.value
            && self.arguments.seq == captured.seq
            && self.source_call_id == source_call_id
            && self.epoch == epoch
            && self.epoch != 0
            && Arc::ptr_eq(&self.origin, origin)
            && route.generation != 0
    }
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

struct ToolProfileParent {
    owner: SingleToolOwner,
    binding: InvocationBinding,
}
impl pi_extension_protocol::profile_live::ParentCustody for ToolProfileParent {
    fn observe(
        &self,
        now_ms: u64,
    ) -> Result<
        pi_extension_protocol::profile_live::ParentObservation,
        pi_extension_protocol::profile_live::Error,
    > {
        use pi_extension_protocol::profile_live::{Error, ParentObservation};
        let state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
        let slot = state.live.as_ref().ok_or(Error::Parent)?;
        let binding = slot.invocation.binding();
        let now = now_ms.max(slot.route.clock.now_ms());
        if !binding.same_live_identity(&self.binding)
            || slot.invocation.phase() != LivePhase::Open
            || slot.local_fault
            || slot.cancel_deadline_ms.is_some()
            || slot.route.status() == RouteStatus::Closed
            || binding.generation() != slot.route.generation
            || binding.capability_ref().authenticated_extension_id
                != slot.route.grant.wire().authenticated_extension_id
            || now >= slot.invocation.effective_deadline_ms()
        {
            return Err(Error::Parent);
        }
        Ok(ParentObservation {
            connection: slot.route.connection.clone(),
            principal: binding.capability_ref().authenticated_extension_id.clone(),
            generation: binding.generation(),
            request_id: binding.request_id(),
            key: binding.effect_key().clone(),
            deadline_ms: slot.invocation.effective_deadline_ms(),
            depth: 1,
        })
    }
    fn reserve_request_id(
        &self,
        now_ms: u64,
        requested: Option<u64>,
        minimum: u64,
    ) -> Result<u64, pi_extension_protocol::profile_live::Error> {
        use pi_extension_protocol::profile_live::Error;
        let state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
        let slot = state.live.as_ref().ok_or(Error::Parent)?;
        let binding = slot.invocation.binding();
        let now = now_ms.max(slot.route.clock.now_ms());
        if !binding.same_live_identity(&self.binding)
            || slot.invocation.phase() != LivePhase::Open
            || slot.local_fault
            || slot.cancel_deadline_ms.is_some()
            || slot.route.status() != RouteStatus::Active
            || binding.generation() != slot.route.generation
            || binding.capability_ref().authenticated_extension_id
                != slot.route.grant.wire().authenticated_extension_id
            || now >= slot.invocation.effective_deadline_ms()
        {
            return Err(Error::Parent);
        }
        let first = slot.route.next_id.load(Ordering::SeqCst).max(minimum);
        let id = requested.unwrap_or(first);
        if id < first || id == 0 || id == u64::MAX {
            return Err(Error::Stale);
        }
        slot.route.next_id.store(id + 1, Ordering::SeqCst);
        Ok(id)
    }
}

struct ToolNativeProfileParent {
    owner: SingleToolOwner,
    binding: InvocationBinding,
    native_deadline_ms: u64,
}
impl pi_extension_protocol::profile_live::NativeCustody for ToolNativeProfileParent {
    fn observe(
        &self,
        now_ms: u64,
    ) -> Result<
        pi_extension_protocol::profile_live::NativeObservation,
        pi_extension_protocol::profile_live::Error,
    > {
        use pi_extension_protocol::profile_live::{Error, NativeObservation};
        let state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
        let slot = state.live.as_ref().ok_or(Error::Parent)?;
        let binding = slot.invocation.binding();
        let now = now_ms.max(slot.route.clock.now_ms());
        if !binding.same_live_identity(&self.binding)
            || !matches!(
                slot.invocation.phase(),
                LivePhase::Open | LivePhase::TerminalReceived
            )
            || slot.local_fault
            || slot.cancel_deadline_ms.is_some()
            || binding.generation() != slot.route.generation
            || binding.capability_ref().authenticated_extension_id
                != slot.route.grant.wire().authenticated_extension_id
            || now >= self.native_deadline_ms
            || slot.invocation.phase() == LivePhase::Open
                && (slot.route.status() != RouteStatus::Active
                    || now >= slot.invocation.effective_deadline_ms())
        {
            return Err(Error::Parent);
        }
        Ok(NativeObservation {
            connection: Some(slot.route.connection.clone()),
            key: Some(binding.effect_key().clone()),
            parent_request_id: Some(binding.request_id()),
            deadline_ms: if slot.invocation.phase() == LivePhase::TerminalReceived {
                self.native_deadline_ms
            } else {
                self.native_deadline_ms
                    .min(slot.invocation.effective_deadline_ms())
            },
        })
    }
    fn reserve_request_id(
        &self,
        now_ms: u64,
        requested: Option<u64>,
        minimum: u64,
    ) -> Result<Option<u64>, pi_extension_protocol::profile_live::Error> {
        use pi_extension_protocol::profile_live::Error;
        let state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
        let slot = state.live.as_ref().ok_or(Error::Parent)?;
        let now = now_ms.max(slot.route.clock.now_ms());
        if !slot.invocation.binding().same_live_identity(&self.binding)
            || !matches!(
                slot.invocation.phase(),
                LivePhase::Open | LivePhase::TerminalReceived
            )
            || slot.local_fault
            || slot.cancel_deadline_ms.is_some()
            || slot.route.status() != RouteStatus::Active
            || now >= self.native_deadline_ms
            || slot.invocation.phase() == LivePhase::Open
                && now >= slot.invocation.effective_deadline_ms()
        {
            return Err(Error::Parent);
        }
        let first = slot.route.next_id.load(Ordering::SeqCst).max(minimum);
        let id = requested.unwrap_or(first);
        if id < first || id == 0 || id == u64::MAX {
            return Err(Error::Stale);
        }
        slot.route.next_id.store(id + 1, Ordering::SeqCst);
        Ok(Some(id))
    }
}

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
        self.connect_inner(host, channel, None, clock, config, None)
            .await
    }
    pub(crate) async fn connect_profiles(
        &self,
        host: HostAuthority,
        channel: Arc<dyn OrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
        profiles: Arc<dyn ToolProfileIngress>,
    ) -> Result<ReadyToolRoute, DriverError> {
        self.connect_inner(host, channel, None, clock, config, Some(profiles))
            .await
    }
    pub(crate) async fn connect_profiles_retained(
        &self,
        host: HostAuthority,
        channel: Arc<dyn OrderedByteChannel>,
        retained_channel: Arc<dyn RetainedOrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
        profiles: Arc<dyn ToolProfileIngress>,
    ) -> Result<ReadyToolRoute, DriverError> {
        self.connect_inner(
            host,
            channel,
            Some(retained_channel),
            clock,
            config,
            Some(profiles),
        )
        .await
    }
    async fn connect_inner(
        &self,
        host: HostAuthority,
        channel: Arc<dyn OrderedByteChannel>,
        retained_channel: Option<Arc<dyn RetainedOrderedByteChannel>>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
        profiles: Option<Arc<dyn ToolProfileIngress>>,
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
        if (profiles.is_none()
            && (grant.wire().selected_capabilities.len() != 1
                || grant.wire().selected_capabilities[0] != *reference
                || !grant.wire().permissions.is_empty()))
            || (profiles.is_some()
                && (grant.profiles().is_empty()
                    || grant.wire().selected_capabilities.len() != grant.profiles().len() + 1
                    || !grant.wire().selected_capabilities.contains(reference)))
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
        let staged_receive = retained_channel
            .as_ref()
            .map(|channel| StagedReceive8192::stage(channel.clone(), generation))
            .transpose()
            .map_err(|_| DriverError::InvalidHost)?;
        let route = Arc::new(Route {
            generation,
            grant,
            connection: pi_extension_protocol::profile_live::ConnectionIdentity::new(),
            profiles,
            channel,
            retained_channel,
            staged_receive: Mutex::new(staged_receive.map(Arc::new)),
            caller: Mutex::new(None),
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
            #[cfg(test)]
            force_rollback_failure: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            force_retained_for_test: std::sync::atomic::AtomicBool::new(false),
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

    /// Capture actual opaque Tool owner/binding custody for subordinate profile
    /// calls. Caller-supplied keys and IDs cannot replace this binding.
    pub fn profile_parent(
        &self,
        binding: &InvocationBinding,
    ) -> Result<pi_extension_protocol::profile_live::ExternalParent, DriverError> {
        let parent = ToolProfileParent {
            owner: self.clone(),
            binding: binding.clone(),
        };
        use pi_extension_protocol::profile_live::ParentCustody;
        parent.observe(0).map_err(|_| DriverError::WrongBinding)?;
        Ok(pi_extension_protocol::profile_live::ExternalParent::from_owner(Arc::new(parent)))
    }

    pub(crate) fn native_profile_parent(
        &self,
        binding: &InvocationBinding,
        native_deadline_ms: u64,
    ) -> Result<Arc<dyn pi_extension_protocol::profile_live::NativeCustody>, DriverError> {
        let parent = ToolNativeProfileParent {
            owner: self.clone(),
            binding: binding.clone(),
            native_deadline_ms,
        };
        use pi_extension_protocol::profile_live::NativeCustody;
        parent.observe(0).map_err(|_| DriverError::WrongBinding)?;
        Ok(Arc::new(parent))
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
        let route = slot.route.clone();
        state.live = None;
        route.clear_caller(binding);
        Ok(())
    }

    /// Caller invokes after handling a local fault without blaming the peer.
    pub fn acknowledge_host_fault(&self, binding: &InvocationBinding) -> Result<(), DriverError> {
        let mut state = self.0.state.lock().unwrap_or_else(|p| p.into_inner());
        let slot = state.live.as_ref().ok_or(DriverError::WrongBinding)?;
        if !slot.invocation.binding().same_live_identity(binding) || !slot.local_fault {
            return Err(DriverError::WrongBinding);
        }
        let route = slot.route.clone();
        state.live = None;
        route.clear_caller(binding);
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
        let route = slot.route.clone();
        state.live = None;
        route.clear_caller(binding);
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
            route.clear_caller(binding);
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
                route.clear_caller(binding);
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
                    let binding = slot.invocation.binding().clone();
                    slot.route.clear_caller(&binding);
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
            route.clear_caller(binding);
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
                    let binding = slot.invocation.binding().clone();
                    route.clear_caller(&binding);
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

#[derive(Clone)]
pub struct ReadyToolRoute {
    owner: SingleToolOwner,
    route: Arc<Route>,
}

impl ReadyToolRoute {
    pub(crate) fn profile_ids(
        &self,
    ) -> Arc<dyn pi_extension_protocol::profile_live::ConnectionIds> {
        Arc::new(ToolProfileIds {
            owner: self.owner.clone(),
            route: self.route.clone(),
        })
    }
    pub(crate) fn peer_profile_parent(
        &self,
        id: u64,
    ) -> Result<pi_extension_protocol::profile_live::ExternalParent, DriverError> {
        let binding = {
            let state = self.owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
            let slot = state
                .live
                .as_ref()
                .filter(|slot| {
                    Arc::ptr_eq(&slot.route, &self.route)
                        && slot.invocation.binding().request_id() == id
                })
                .ok_or(DriverError::WrongBinding)?;
            slot.invocation.binding().clone()
        };
        self.owner.profile_parent(&binding)
    }
    pub(crate) fn enqueue_profile(
        &self,
        frame: EncodedFrame,
        id: u64,
        response: bool,
    ) -> Result<(), DriverError> {
        let kind = if response {
            WriteKind::ProfileResponse(id)
        } else {
            WriteKind::ProfileRequest(id)
        };
        let (wake, _) = self.route.enqueue(frame, kind)?;
        if let Some(wake) = wake {
            wake.wake();
        }
        Ok(())
    }
    pub(crate) fn close_profiles(&self) {
        self.owner.close_route(&self.route, DriverError::PeerLost);
    }
    pub(crate) fn connection_identity(
        &self,
    ) -> pi_extension_protocol::profile_live::ConnectionIdentity {
        self.route.connection.clone()
    }
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

    #[cfg(test)]
    fn install_staged_receive_for_test(&self, channel: Arc<dyn RetainedOrderedByteChannel>) {
        let staged = StagedReceive8192::stage(channel, self.route.generation)
            .expect("staged receive fixture");
        let mut slot = self
            .route
            .staged_receive
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        *slot = Some(Arc::new(staged));
    }

    #[cfg(test)]
    fn install_walking_receive_for_test(&self, channel: Arc<dyn RetainedOrderedByteChannel>) {
        self.route
            .force_retained_for_test
            .store(true, Ordering::Release);
        self.install_staged_receive_for_test(channel);
    }

    #[cfg(test)]
    fn force_staged_bind_failure_for_test(&self) {
        let staged = self
            .route
            .staged_receive
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        staged
            .as_ref()
            .expect("staged receive fixture")
            .force_next_bind_failure();
    }

    #[cfg(test)]
    fn force_rollback_failure_for_test(&self) {
        self.route
            .force_rollback_failure
            .store(true, Ordering::Release);
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
        self.prepare_planned_inner(plan, saved, source_call_id, None, StartTiming::Raw(timing))?
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
        self.prepare_planned_inner(plan, saved, source_call_id, None, StartTiming::Raw(timing))
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
            None,
            StartTiming::Absolute {
                requested_deadline_ms,
                parent_deadline_ms,
            },
        )
    }

    /// Commit a caller-captured echo claim. The captured key, arguments,
    /// sequence, source ID, route epoch, and origin remain one tuple under the
    /// same owner lock as the ordinary Request claim. Production credit stays
    /// at zero until native admission is separately accepted.
    pub(crate) fn prepare_planned_with_captured_bounds<C>(
        &self,
        plan: &ToolEffectPlan<C>,
        captured: &CapturedArguments,
        source_call_id: &str,
        epoch: u64,
        origin: Option<&Arc<dyn OriginFence>>,
    ) -> Result<PreparedCall, DriverError> {
        let caller = if self.route.retained_channel.is_some() || origin.is_some() {
            let origin = origin.ok_or(DriverError::WrongBinding)?;
            Some(CapturedCaller8192 {
                binding: None,
                key: plan.key.clone(),
                arguments: captured.clone(),
                source_call_id: source_call_id.to_owned(),
                epoch,
                origin: origin.clone(),
            })
        } else {
            None
        };
        self.prepare_planned_inner(
            plan,
            &captured.value,
            source_call_id,
            caller,
            StartTiming::Absolute {
                requested_deadline_ms: None,
                parent_deadline_ms: None,
            },
        )
    }

    fn prepare_planned_inner<C>(
        &self,
        plan: &ToolEffectPlan<C>,
        saved: &JsonObject,
        source_call_id: &str,
        mut caller: Option<CapturedCaller8192>,
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
        if let Some(caller) = caller.as_ref() {
            if caller.key != plan.key
                || caller.arguments.key != plan.args_key
                || caller.arguments.value != *saved
                || caller.source_call_id != source_call_id
                || caller.epoch == 0
            {
                return Err(DriverError::WrongBinding);
            }
            if self
                .route
                .caller
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .is_some_and(|existing| {
                    !existing.matches(
                        &self.route,
                        plan,
                        &caller.arguments,
                        source_call_id,
                        caller.epoch,
                        &caller.origin,
                    )
                })
            {
                return Err(DriverError::WrongBinding);
            }
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
        let staged_caller =
            caller.as_ref().map(
                |caller| crate::extensions::receive_host8192::CallerIdentity8192 {
                    arguments: caller.arguments.clone(),
                    source_call_id: caller.source_call_id.clone(),
                    epoch: caller.epoch,
                    origin: caller.origin.clone(),
                },
            );
        let staged_receive = self
            .route
            .staged_receive
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let (Some(staged_caller), Some(staged)) = (staged_caller.as_ref(), staged_receive) {
            staged
                .validate_caller(&binding, staged_caller)
                .map_err(|_| DriverError::WrongBinding)?;
        }
        if let Some(caller) = caller.as_mut() {
            caller.binding = Some(binding.clone());
        }
        let mut previous_route_caller = None;
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
            if let Some(caller) = caller {
                previous_route_caller = self
                    .route
                    .caller
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .replace(caller);
            }
            self.route.next_id.store(id + 1, Ordering::SeqCst);
        }
        let staged_bind_failed = if let Some(staged_caller) = staged_caller.as_ref() {
            let staged_receive = self
                .route
                .staged_receive
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            staged_receive.as_ref().is_some_and(|staged| {
                staged
                    .bind_caller(binding.clone(), staged_caller.clone())
                    .is_err()
            })
        } else {
            false
        };
        if staged_bind_failed {
            let rollback_succeeded = {
                let mut writes = self.route.writes.lock().unwrap_or_else(|p| p.into_inner());
                #[cfg(test)]
                if self
                    .route
                    .force_rollback_failure
                    .swap(false, Ordering::AcqRel)
                {
                    false
                } else {
                    writes.rollback_request(id)
                }
                #[cfg(not(test))]
                {
                    writes.rollback_request(id)
                }
            };
            state.live = None;
            self.route.next_id.store(id, Ordering::SeqCst);
            if previous_route_caller.is_some() {
                *self.route.caller.lock().unwrap_or_else(|p| p.into_inner()) =
                    previous_route_caller;
            } else {
                self.route
                    .caller
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take();
            }
            if rollback_succeeded {
                return Err(DriverError::WrongBinding);
            }
            if state
                .active
                .as_ref()
                .is_some_and(|active| Arc::ptr_eq(active, &self.route))
            {
                // The owner state is already held by this function; this
                // branch is unreachable after a successful tail rollback.
                // Route closure below must not leave a closed route published.
                state.active = None;
            }
            drop(state);
            self.route.close();
            return Err(DriverError::HostFault);
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
    connection: pi_extension_protocol::profile_live::ConnectionIdentity,
    profiles: Option<Arc<dyn ToolProfileIngress>>,
    channel: Arc<dyn OrderedByteChannel>,
    retained_channel: Option<Arc<dyn RetainedOrderedByteChannel>>,
    staged_receive: Mutex<Option<Arc<StagedReceive8192>>>,
    caller: Mutex<Option<CapturedCaller8192>>,
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
    #[cfg(test)]
    force_rollback_failure: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    force_retained_for_test: std::sync::atomic::AtomicBool,
}

impl Route {
    #[cfg(test)]
    fn connection_token(&self) -> source_observer::ConnectionToken {
        source_observer::ConnectionToken::from_identity(&self.connection)
    }

    fn prepare_retained_refusal(
        &self,
        binding: &InvocationBinding,
        caller: &crate::extensions::receive_host8192::CallerIdentity8192,
        frame: &ActiveFrame,
    ) -> Result<usize, DriverError> {
        let staged = self
            .staged_receive
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
            .ok_or(DriverError::WrongBinding)?;
        let decision =
            staged
                .prepare_refusal(binding, caller, frame)
                .map_err(|error| match error {
                    crate::extensions::receive_host8192::Receive8192Error::InvalidCaller => {
                        DriverError::WrongBinding
                    }
                    crate::extensions::receive_host8192::Receive8192Error::Capacity => {
                        DriverError::Overloaded
                    }
                    crate::extensions::receive_host8192::Receive8192Error::Closed => {
                        DriverError::Closed
                    }
                    crate::extensions::receive_host8192::Receive8192Error::UnsupportedFrame => {
                        DriverError::HostFault
                    }
                    crate::extensions::receive_host8192::Receive8192Error::NativeRefused
                    | crate::extensions::receive_host8192::Receive8192Error::Occupied
                    | crate::extensions::receive_host8192::Receive8192Error::InvalidGeneration => {
                        DriverError::HostFault
                    }
                })?;
        if !decision.binding.same_live_identity(binding) {
            return Err(DriverError::WrongBinding);
        }
        Ok(decision.body_len)
    }

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
    fn clear_caller(&self, binding: &InvocationBinding) {
        let route_matches = self
            .caller
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .is_some_and(|caller| {
                caller
                    .binding
                    .as_ref()
                    .is_some_and(|existing| existing.same_live_identity(binding))
            });
        if route_matches {
            self.caller.lock().unwrap_or_else(|p| p.into_inner()).take();
        }
        let staged_receive = self
            .staged_receive
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some(staged_receive) = staged_receive {
            staged_receive.clear_caller(binding);
        }
    }

    fn clear_any_caller(&self) {
        self.caller.lock().unwrap_or_else(|p| p.into_inner()).take();
        let staged_receive = self
            .staged_receive
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some(staged_receive) = staged_receive {
            staged_receive.clear_any_caller();
        }
    }
    fn finish_staged_receive_close(&self) {
        let staged_receive = self
            .staged_receive
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone();
        if let Some(staged_receive) = staged_receive {
            let _ = staged_receive.run_preopened();
            staged_receive.finish_close();
        }
    }
    fn close(&self) {
        if self.status.swap(2, Ordering::SeqCst) == 2 {
            self.clear_any_caller();
            self.finish_staged_receive_close();
            return;
        }
        self.clear_any_caller();
        self.channel.close();
        self.finish_staged_receive_close();
        if let Some(profiles) = &self.profiles {
            profiles.close();
        }
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
        self.clear_any_caller();
        self.channel.close();
        self.finish_staged_receive_close();
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
        self.clear_any_caller();
        self.channel.close();
        self.finish_staged_receive_close();
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
            FenceRouteDecision::AlreadyClosed => {
                self.clear_any_caller();
                self.finish_staged_receive_close();
            }
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
        #[cfg(test)]
        let route_token = self.connection_token();
        #[cfg(test)]
        let reserve_facts;
        #[cfg(test)]
        let enqueue_facts;
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
            #[cfg(test)]
            {
                let lane = match kind {
                    WriteKind::Request(_) | WriteKind::ProfileRequest(_) => "normal",
                    WriteKind::Denial | WriteKind::ProfileResponse(_) => "control",
                    WriteKind::Cancel(_) => "cancel",
                };
                let request_id = match kind {
                    WriteKind::Request(id)
                    | WriteKind::Cancel(id)
                    | WriteKind::ProfileRequest(id)
                    | WriteKind::ProfileResponse(id) => Some(id),
                    WriteKind::Denial => None,
                };
                reserve_facts = Some((
                    lane,
                    request_id,
                    writes.normal_bytes,
                    writes.control_bytes,
                    writes.cancel_bytes,
                    writes.jobs.len(),
                ));
            }
            if denial {
                self.mark_draining();
            }
            writes.jobs.push_back(WriteJob {
                frame,
                kind,
                deadline,
                active: true,
            });
            #[cfg(test)]
            {
                let lane = match kind {
                    WriteKind::Request(_) | WriteKind::ProfileRequest(_) => "normal",
                    WriteKind::Denial | WriteKind::ProfileResponse(_) => "control",
                    WriteKind::Cancel(_) => "cancel",
                };
                let request_id = match kind {
                    WriteKind::Request(id)
                    | WriteKind::Cancel(id)
                    | WriteKind::ProfileRequest(id)
                    | WriteKind::ProfileResponse(id) => Some(id),
                    WriteKind::Denial => None,
                };
                enqueue_facts = Some((
                    lane,
                    request_id,
                    writes.normal_bytes,
                    writes.control_bytes,
                    writes.cancel_bytes,
                    writes.jobs.len(),
                ));
            }
            writes.waker.take()
        };
        #[cfg(test)]
        {
            if let Some((lane, request_id, normal, control, cancel, jobs)) = reserve_facts {
                let mut event =
                    source_observer::SourceEvent::new("queue", file!(), line!(), "Route::enqueue");
                event.field = Some("P_req");
                event.owner = Some("Route");
                event.operation = Some("reserve");
                event.lane = Some(lane);
                event.bytes = Some(bytes);
                event.request_id = request_id;
                event.reserved_normal = Some(normal);
                event.reserved_control = Some(control);
                event.reserved_cancel = Some(cancel);
                event.jobs = Some(jobs);
                event.generation = Some(self.generation);
                event.token = Some(route_token.clone());
                source_observer::record(event);
            }
            if let Some((lane, request_id, normal, control, cancel, jobs)) = enqueue_facts {
                let mut event =
                    source_observer::SourceEvent::new("queue", file!(), line!(), "Route::enqueue");
                event.field = Some("Qreceive");
                event.owner = Some("Route");
                event.generation = Some(self.generation);
                event.operation = Some("enqueue");
                event.lane = Some(lane);
                event.bytes = Some(bytes);
                event.request_id = request_id;
                event.reserved_normal = Some(normal);
                event.reserved_control = Some(control);
                event.reserved_cancel = Some(cancel);
                event.jobs = Some(jobs);
                event.token = Some(route_token);
                source_observer::record(event);
            }
        }
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
        #[cfg(test)]
        let route_token = self.connection_token();
        #[cfg(test)]
        let release_facts;
        let mut writes = self.writes.lock().unwrap_or_else(|p| p.into_inner());
        let reserved = match kind {
            WriteKind::Denial | WriteKind::ProfileResponse(_) => &mut writes.control_bytes,
            WriteKind::Cancel(_) => &mut writes.cancel_bytes,
            WriteKind::Request(_) | WriteKind::ProfileRequest(_) => &mut writes.normal_bytes,
        };
        *reserved = reserved.saturating_sub(size);
        if matches!(kind, WriteKind::Cancel(_)) && writes.cancel_bytes == 0 {
            writes.retired_cancel_id = None;
        }
        #[cfg(test)]
        {
            let lane = match kind {
                WriteKind::Request(_) | WriteKind::ProfileRequest(_) => "normal",
                WriteKind::Denial | WriteKind::ProfileResponse(_) => "control",
                WriteKind::Cancel(_) => "cancel",
            };
            let request_id = match kind {
                WriteKind::Request(id)
                | WriteKind::Cancel(id)
                | WriteKind::ProfileRequest(id)
                | WriteKind::ProfileResponse(id) => Some(id),
                WriteKind::Denial => None,
            };
            release_facts = Some((
                lane,
                request_id,
                writes.normal_bytes,
                writes.control_bytes,
                writes.cancel_bytes,
                writes.jobs.len(),
            ));
        }
        drop(writes);
        #[cfg(test)]
        if let Some((lane, request_id, normal, control, cancel, jobs)) = release_facts {
            let mut event = source_observer::SourceEvent::new(
                "queue",
                file!(),
                line!(),
                "Route::release_write",
            );
            event.field = Some("P_req");
            event.owner = Some("Route");
            event.operation = Some("release");
            event.lane = Some(lane);
            event.bytes = Some(size);
            event.request_id = request_id;
            event.reserved_normal = Some(normal);
            event.reserved_control = Some(control);
            event.reserved_cancel = Some(cancel);
            event.jobs = Some(jobs);
            event.generation = Some(self.generation);
            event.token = Some(route_token.clone());
            source_observer::record(event);
            let mut queue = source_observer::SourceEvent::new(
                "queue",
                file!(),
                line!(),
                "Route::release_write",
            );
            queue.field = Some("Qreceive");
            queue.owner = Some("Route");
            queue.generation = Some(self.generation);
            queue.operation = Some("release");
            queue.lane = Some(lane);
            queue.bytes = Some(size);
            queue.request_id = request_id;
            queue.jobs = Some(jobs);
            queue.token = Some(route_token);
            source_observer::record(queue);
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
    ProfileRequest(u64),
    ProfileResponse(u64),
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
            WriteKind::Request(_) | WriteKind::ProfileRequest(_) => {
                (&mut self.normal_bytes, normal_cap)
            }
            WriteKind::Denial | WriteKind::ProfileResponse(_) => {
                (&mut self.control_bytes, denial_cap)
            }
            WriteKind::Cancel(_) => (&mut self.cancel_bytes, cancel_cap),
        };
        *reserved = reserved
            .checked_add(bytes)
            .filter(|next| *next <= cap)
            .ok_or(DriverError::Overloaded)?;
        Ok(())
    }

    fn rollback_request(&mut self, request_id: u64) -> bool {
        let Some(job) = self.jobs.back() else {
            return false;
        };
        if !matches!(job.kind, WriteKind::Request(id) if id == request_id) {
            return false;
        }
        let bytes = job.frame.bytes.len();
        if bytes > self.normal_bytes {
            return false;
        }
        self.jobs.pop_back();
        self.normal_bytes -= bytes;
        true
    }
}

async fn writer_loop(owner: SingleToolOwner, route: Arc<Route>) {
    loop {
        #[cfg(test)]
        let route_token = route.connection_token();
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
                #[cfg(test)]
                let pop_facts = (
                    job.frame.bytes.len(),
                    writes.jobs.len(),
                    match job.kind {
                        WriteKind::Request(id)
                        | WriteKind::Cancel(id)
                        | WriteKind::ProfileRequest(id)
                        | WriteKind::ProfileResponse(id) => Some(id),
                        WriteKind::Denial => None,
                    },
                );
                if matches!(job.kind, WriteKind::Request(_) | WriteKind::Cancel(_)) {
                    writes.popped_unadmitted = Some((job.kind, job.frame.bytes.len()));
                }
                #[cfg(test)]
                return Poll::Ready(Some((job, pop_facts)));
                #[cfg(not(test))]
                return Poll::Ready(Some(job));
            }
            let prior = writes.waker.replace(cx.waker().clone());
            drop(writes);
            drop(prior);
            Poll::Pending
        })
        .await;
        #[cfg(test)]
        let Some((job, (pop_bytes, pop_jobs, pop_request_id))) = job else {
            break;
        };
        #[cfg(not(test))]
        let Some(job) = job else { break };
        #[cfg(test)]
        {
            let mut event =
                source_observer::SourceEvent::new("queue", file!(), line!(), "writer_loop");
            event.field = Some("P_req");
            event.owner = Some("Route");
            event.operation = Some("pop");
            event.bytes = Some(pop_bytes);
            event.jobs = Some(pop_jobs);
            event.request_id = pop_request_id;
            event.generation = Some(route.generation);
            event.token = Some(route_token.clone());
            source_observer::record(event);
            let mut queue =
                source_observer::SourceEvent::new("queue", file!(), line!(), "writer_loop");
            queue.field = Some("Qreceive");
            queue.owner = Some("Route");
            queue.generation = Some(route.generation);
            queue.operation = Some("pop");
            queue.bytes = Some(pop_bytes);
            queue.jobs = Some(pop_jobs);
            queue.request_id = pop_request_id;
            queue.token = Some(route_token.clone());
            source_observer::record(queue);
        }
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
        if let WriteKind::ProfileRequest(id) | WriteKind::ProfileResponse(id) = job.kind {
            let admitted = if let Some(profiles) = &route.profiles {
                let mut admission = profiles.admit_write(id);
                let mut deadline = route.clock.sleep_until(job.deadline);
                poll_fn(|cx| {
                    if deadline.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(false);
                    }
                    admission.as_mut().poll(cx)
                })
                .await
            } else {
                false
            };
            if !admitted {
                route.release_write(job.kind, job.frame.bytes.len());
                if let Some(profiles) = &route.profiles {
                    profiles.written(id, false);
                }
                continue;
            }
        }
        let size = job.frame.bytes.len();
        #[cfg(test)]
        {
            let mut event =
                source_observer::SourceEvent::new("interval", file!(), line!(), "writer_loop");
            event.field = Some("overlap");
            event.owner = Some("Route");
            event.operation = Some("physical_write");
            event.outcome = Some("start");
            event.bytes = Some(size);
            event.request_id = match job.kind {
                WriteKind::Request(id)
                | WriteKind::Cancel(id)
                | WriteKind::ProfileRequest(id)
                | WriteKind::ProfileResponse(id) => Some(id),
                WriteKind::Denial => None,
            };
            event.generation = Some(route.generation);
            event.token = Some(route.connection_token());
            source_observer::record(event);
            let mut queue =
                source_observer::SourceEvent::new("queue", file!(), line!(), "writer_loop");
            queue.field = Some("Qreceive");
            queue.owner = Some("Route");
            queue.generation = Some(route.generation);
            queue.operation = Some("write");
            queue.outcome = Some("start");
            queue.bytes = Some(size);
            queue.request_id = match job.kind {
                WriteKind::Request(id)
                | WriteKind::Cancel(id)
                | WriteKind::ProfileRequest(id)
                | WriteKind::ProfileResponse(id) => Some(id),
                WriteKind::Denial => None,
            };
            queue.token = Some(route.connection_token());
            source_observer::record(queue);
        }
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
        if let WriteKind::ProfileRequest(id) | WriteKind::ProfileResponse(id) = job.kind
            && let Some(profiles) = &route.profiles
        {
            profiles.written(id, completed);
        }
        #[cfg(test)]
        {
            let mut event =
                source_observer::SourceEvent::new("interval", file!(), line!(), "writer_loop");
            event.field = Some("overlap");
            event.owner = Some("Route");
            event.operation = Some("physical_write");
            event.outcome = Some("end");
            event.bytes = Some(size);
            event.generation = Some(route.generation);
            event.token = Some(route.connection_token());
            source_observer::record(event);
            let mut queue =
                source_observer::SourceEvent::new("queue", file!(), line!(), "writer_loop");
            queue.field = Some("Qreceive");
            queue.owner = Some("Route");
            queue.generation = Some(route.generation);
            queue.operation = Some("write");
            queue.outcome = Some("end");
            queue.bytes = Some(size);
            queue.request_id = match job.kind {
                WriteKind::Request(id)
                | WriteKind::Cancel(id)
                | WriteKind::ProfileRequest(id)
                | WriteKind::ProfileResponse(id) => Some(id),
                WriteKind::Denial => None,
            };
            queue.token = Some(route.connection_token());
            source_observer::record(queue);
            if completed {
                let mut ack =
                    source_observer::SourceEvent::new("queue", file!(), line!(), "writer_loop");
                ack.field = Some("Qreceive");
                ack.owner = Some("Route");
                ack.generation = Some(route.generation);
                ack.operation = Some("ack");
                ack.outcome = Some("complete");
                ack.bytes = Some(size);
                ack.request_id = match job.kind {
                    WriteKind::Request(id)
                    | WriteKind::Cancel(id)
                    | WriteKind::ProfileRequest(id)
                    | WriteKind::ProfileResponse(id) => Some(id),
                    WriteKind::Denial => None,
                };
                ack.token = Some(route.connection_token());
                source_observer::record(ack);
            }
        }
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
    let staged_receive = route
        .staged_receive
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    if let Some(staged_receive) = staged_receive {
        let callback_receive = staged_receive.clone();
        if pump
            .set_active_body_callback(move |body, frame| {
                match callback_receive.capture_body(frame, body) {
                    Ok(())
                    | Err(
                        crate::extensions::receive_host8192::Receive8192Error::UnsupportedFrame,
                    )
                    | Err(crate::extensions::receive_host8192::Receive8192Error::Closed) => Ok(()),
                    Err(_) => Err(()),
                }
            })
            .is_err()
        {
            owner.close_route(&route, DriverError::HostFault);
            return;
        }
    }
    loop {
        match pump.next_active().await {
            Ok(Some(frame)) => {
                if let Some(profiles) = &route.profiles {
                    let profile_id = match &frame.envelope {
                        SelectedEnvelope::V1_1(e) => match &e.frame {
                            Some(v1_1::envelope::Frame::Request(request)) => {
                                let Some(reference) = request.capability_ref.as_ref() else {
                                    owner.close_route(&route, DriverError::MalformedPeer);
                                    break;
                                };
                                if validated_peer_ref(&route, &peer_ref_1_1(reference)).is_none() {
                                    owner.close_route(&route, DriverError::MalformedPeer);
                                    break;
                                }
                                Some(request.request_id)
                            }
                            Some(v1_1::envelope::Frame::Response(response)) => {
                                Some(response.request_id)
                            }
                            Some(v1_1::envelope::Frame::StreamEnd(end)) => Some(end.request_id),
                            Some(v1_1::envelope::Frame::StreamItem(item)) => Some(item.request_id),
                            Some(v1_1::envelope::Frame::Cancel(cancel)) => Some(cancel.request_id),
                            Some(v1_1::envelope::Frame::Credit(credit)) => Some(credit.request_id),
                            _ => None,
                        },
                        SelectedEnvelope::V1_0(e) => match &e.frame {
                            Some(v1_0::envelope::Frame::Request(request)) => {
                                let Some(reference) = request.capability_ref.as_ref() else {
                                    owner.close_route(&route, DriverError::MalformedPeer);
                                    break;
                                };
                                if validated_peer_ref(&route, &peer_ref_1_0(reference)).is_none() {
                                    owner.close_route(&route, DriverError::MalformedPeer);
                                    break;
                                }
                                Some(request.request_id)
                            }
                            Some(v1_0::envelope::Frame::Response(response)) => {
                                Some(response.request_id)
                            }
                            Some(v1_0::envelope::Frame::StreamEnd(end)) => Some(end.request_id),
                            Some(v1_0::envelope::Frame::StreamItem(item)) => Some(item.request_id),
                            Some(v1_0::envelope::Frame::Cancel(cancel)) => Some(cancel.request_id),
                            Some(v1_0::envelope::Frame::Credit(credit)) => Some(credit.request_id),
                            _ => None,
                        },
                    };
                    if let Some(id) = profile_id {
                        let tool_id = {
                            let state = owner.0.state.lock().unwrap_or_else(|p| p.into_inner());
                            state
                                .live
                                .as_ref()
                                .filter(|slot| Arc::ptr_eq(&slot.route, &route))
                                .map(|slot| slot.invocation.binding().request_id())
                        };
                        let peer_request = matches!(&frame.envelope,SelectedEnvelope::V1_1(e) if matches!(e.frame,Some(v1_1::envelope::Frame::Request(_))))
                            || matches!(&frame.envelope,SelectedEnvelope::V1_0(e) if matches!(e.frame,Some(v1_0::envelope::Frame::Request(_))));
                        if peer_request || tool_id != Some(id) {
                            if let Err(error) = profiles.incoming(frame) {
                                owner.close_route(&route, error);
                                break;
                            }
                            continue;
                        }
                    }
                }
                match owner.on_frame(&route, frame) {
                    Ok(ReaderAction::Continue) => {}
                    Ok(ReaderAction::Stop) => break,
                    Err(error) => {
                        owner.close_route(&route, error);
                        break;
                    }
                }
            }
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

#[cfg(test)]
fn check_retained_indexed_for_test(
    slot: &mut LiveSlot,
    route: &Arc<Route>,
    now: u64,
    body_len: usize,
) -> LiveDecision {
    let staged = route
        .staged_receive
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .clone()
        .ok_or(DriverError::WrongBinding)?;
    let body = staged
        .take_captured_body(slot.invocation.binding().request_id(), body_len)
        .ok_or(DriverError::HostFault)?;
    let reader = staged.claim_reader().map_err(|_| DriverError::HostFault)?;
    let sequence_before = slot.invocation.next_sequence();
    let credit = slot
        .invocation
        .grant_credit(body_len as u64, now)
        .map_err(|_| DriverError::HostFault)?;
    let mut credit_event = source_observer::SourceEvent::new(
        "credit",
        file!(),
        line!(),
        "check_retained_indexed_for_test",
    );
    credit_event.outcome = Some("prepared");
    credit_event.operation = Some("additional");
    credit_event.method = Some("Credit".into());
    credit_event.bytes = Some(credit.body_len);
    credit_event.request_id = Some(slot.invocation.binding().request_id());
    credit_event.generation = Some(route.generation);
    source_observer::record(credit_event);

    let checked = staged.with_indexed_body(&body, |body, arena| {
        slot.domain.receive_indexed_checked_with_final_time(
            &mut slot.invocation,
            route.generation,
            body,
            arena,
            now,
            slot.cancel_deadline_ms,
            || route.clock.now_ms(),
        )
    });
    drop(reader);
    let checked = checked.map_err(|_| DriverError::HostFault)?;
    let checked = match checked {
        Ok(checked) => checked,
        Err(DomainReceiveError::Host(_)) => {
            slot.local_fault = true;
            return Ok((
                Some((
                    slot.completion.clone(),
                    CompletionValue::Failed(DriverError::HostFault),
                )),
                true,
            ));
        }
        Err(DomainReceiveError::PeerLost)
        | Err(DomainReceiveError::Protocol(LiveError::PeerLost)) => {
            return Ok((
                Some((
                    slot.completion.clone(),
                    CompletionValue::Failed(DriverError::PeerLost),
                )),
                true,
            ));
        }
        Err(DomainReceiveError::Protocol(_)) => {
            return Ok((
                Some((
                    slot.completion.clone(),
                    CompletionValue::Failed(DriverError::MalformedPeer),
                )),
                true,
            ));
        }
    };
    match checked.outcome {
        ReceiveOutcome::Progress(_) | ReceiveOutcome::IgnoredCancelledProgress => Ok((None, false)),
        ReceiveOutcome::TerminalReceived => {
            let sequence_after = slot.invocation.next_sequence();
            let mut receive_event = source_observer::SourceEvent::new(
                "receive",
                file!(),
                line!(),
                "check_retained_indexed_for_test",
            );
            receive_event.outcome = Some("terminal_received");
            receive_event.operation = Some("indexed");
            receive_event.bytes = Some(body_len);
            receive_event.body_len = Some(body_len);
            receive_event.request_id = Some(slot.invocation.binding().request_id());
            receive_event.generation = Some(route.generation);
            receive_event.wire_sequence_before = Some(sequence_before);
            receive_event.wire_sequence_after = Some(sequence_after);
            source_observer::record(receive_event);
            let mut selection_event = source_observer::SourceEvent::new(
                "body_capture",
                file!(),
                line!(),
                "check_retained_indexed_for_test",
            );
            selection_event.field = Some("D_req");
            selection_event.owner = Some("StagedReceive8192");
            selection_event.operation = Some("indexed_selection");
            selection_event.outcome = Some("complete");
            selection_event.bytes = Some(body_len);
            selection_event.body_len = Some(body_len);
            selection_event.request_id = Some(slot.invocation.binding().request_id());
            selection_event.generation = Some(route.generation);
            selection_event.wire_sequence_before = Some(sequence_before);
            selection_event.wire_sequence_after = Some(sequence_after);
            source_observer::record(selection_event);
            let mut terminal_event = source_observer::SourceEvent::new(
                "body_capture",
                file!(),
                line!(),
                "check_retained_indexed_for_test",
            );
            terminal_event.field = Some("D_req");
            terminal_event.owner = Some("StagedReceive8192");
            terminal_event.operation = Some("indexed_terminal");
            terminal_event.outcome = Some("complete");
            terminal_event.bytes = Some(body_len);
            terminal_event.body_len = Some(body_len);
            terminal_event.request_id = Some(slot.invocation.binding().request_id());
            terminal_event.generation = Some(route.generation);
            terminal_event.wire_sequence_before = Some(sequence_before);
            terminal_event.wire_sequence_after = Some(sequence_after);
            source_observer::record(terminal_event);

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
    }
}

fn check_live_frame(
    slot: &mut LiveSlot,
    route: &Arc<Route>,
    frame: &ActiveFrame,
    now: u64,
) -> LiveDecision {
    #[cfg(test)]
    if route.force_retained_for_test.load(Ordering::Acquire)
        && source_observer::c3_walking_enabled()
    {
        let caller = route
            .caller
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .ok_or(DriverError::WrongBinding)?
            .staged_identity();
        let body_len = route.prepare_retained_refusal(slot.invocation.binding(), &caller, frame)?;
        return check_retained_indexed_for_test(slot, route, now, body_len);
    }
    if route.retained_channel.is_some() {
        let caller = route
            .caller
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .as_ref()
            .ok_or(DriverError::WrongBinding)?
            .staged_identity();
        route.prepare_retained_refusal(slot.invocation.binding(), &caller, frame)?;
        return Ok((
            Some((
                slot.completion.clone(),
                CompletionValue::Failed(DriverError::HostFault),
            )),
            true,
        ));
    }
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
pub(crate) mod local_cancel_queue_tests {
    use super::*;
    use std::collections::VecDeque;
    use std::future::{Future, poll_fn};
    use std::num::{NonZeroU64, NonZeroUsize};
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::task::{Context, Poll};

    use crate::extensions::transport::ChannelError;
    use pi_ai_core::{BoxFuture, SafeUnsigned, SpawnError, TaskSpawner};
    use pi_extension_protocol::activation::{
        AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs,
    };
    use pi_extension_protocol::jcs::Limits as JsonLimits;
    use pi_extension_protocol::wire::framing::EncodedFrame;
    use pi_extension_protocol::wire::v1_1;
    use serde_json::Value;

    const CATALOG: &str =
        include_str!("../../../pi-extension-protocol/schema/fixtures/host-catalog.json");
    const VECTORS: &str =
        include_str!("../../../pi-extension-protocol/schema/fixtures/activation-vectors.json");
    const RAW: &str =
        include_str!("../../../pi-extension-protocol/schema/fixtures/raw-vectors.json");

    #[derive(Default)]
    struct LifecycleSpawner {
        tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    }

    impl LifecycleSpawner {
        fn drive<F: Future>(&self, future: F) -> F::Output {
            let mut future = Box::pin(future);
            for _ in 0..128 {
                match future
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                {
                    Poll::Ready(value) => return value,
                    Poll::Pending => {}
                }
            }
            panic!("lifecycle fixture future did not complete");
        }

        fn tick(&self) {
            let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
            for mut task in tasks {
                if task
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending()
                {
                    self.tasks.lock().unwrap().push(task);
                }
            }
        }
    }

    impl TaskSpawner for LifecycleSpawner {
        fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            self.tasks.lock().unwrap().push(task);
            Ok(())
        }
    }

    struct LifecycleClock {
        now: AtomicU64,
    }

    impl LifecycleClock {
        fn new(now: u64) -> Self {
            Self {
                now: AtomicU64::new(now),
            }
        }
        fn set(&self, now: u64) {
            self.now.store(now, Ordering::SeqCst);
        }
    }

    impl MonotonicClock for LifecycleClock {
        fn now_ms(&self) -> u64 {
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

    struct LifecycleChannel {
        reads: Mutex<VecDeque<Option<Vec<u8>>>>,
        writes: Mutex<Vec<Vec<u8>>>,
        closed: AtomicBool,
    }

    impl LifecycleChannel {
        fn new() -> Self {
            Self {
                reads: Mutex::new(VecDeque::new()),
                writes: Mutex::new(Vec::new()),
                closed: AtomicBool::new(false),
            }
        }

        fn push(&self, bytes: Vec<u8>) {
            self.reads.lock().unwrap().push_back(Some(bytes));
        }
    }

    impl OrderedByteChannel for LifecycleChannel {
        fn max_read_scratch_bytes(&self) -> usize {
            0
        }

        fn read_chunk<'a>(
            &'a self,
            buffer: &'a mut [u8],
        ) -> BoxFuture<'a, Result<Option<usize>, ChannelError>> {
            Box::pin(poll_fn(move |_| {
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
            }))
        }

        fn write_frame(&self, frame: EncodedFrame) -> BoxFuture<'_, Result<(), ChannelError>> {
            Box::pin(async move {
                if self.closed.load(Ordering::SeqCst) {
                    return Err(ChannelError::Closed);
                }
                self.writes.lock().unwrap().push(frame.bytes);
                Ok(())
            })
        }

        fn close(&self) {
            self.closed.store(true, Ordering::SeqCst);
        }
    }

    impl RetainedOrderedByteChannel for LifecycleChannel {
        fn take_notifications(&self) -> [Option<Waker>; 2] {
            [None, None]
        }

        fn close_retained(&self) -> [Option<Waker>; 2] {
            self.close();
            [None, None]
        }
    }

    struct LifecycleOrigin;

    impl OriginFence for LifecycleOrigin {
        fn begin_failure(&self, _drive_only: bool) {}
        fn finish_failure(&self) {}
        fn token(&self) -> Arc<()> {
            Arc::new(())
        }
        fn transfer_to_work(&self) {}
        fn disarm(&self) {}
        fn defer_release(&self, release: Box<dyn FnOnce() + Send>) {
            release();
        }
        fn belongs_to_drive(&self, _stamp: &Arc<()>) -> bool {
            false
        }
        fn return_to_drive(&self) {}
        fn is_work(&self) -> bool {
            true
        }
        fn is_active(&self) -> bool {
            true
        }
    }

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    fn varint(mut value: usize) -> Vec<u8> {
        let mut bytes = Vec::new();
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            bytes.push(byte);
            if value == 0 {
                return bytes;
            }
        }
    }

    fn field(number: u8, value: &[u8]) -> Vec<u8> {
        [(number << 3) | 2]
            .into_iter()
            .chain(varint(value.len()))
            .chain(value.iter().copied())
            .collect()
    }

    fn frame(number: u8, value: &[u8]) -> Vec<u8> {
        let envelope = field(number, value);
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

    fn host(generation: u64) -> HostAuthority {
        let catalog: Value = serde_json::from_str(CATALOG).unwrap();
        let entry = &catalog["entries"][0];
        let agent = &entry["agentTool"];
        let descriptor = AgentTool {
            name: agent["name"].as_str().unwrap().into(),
            label: agent["label"].as_str().unwrap().into(),
            description: agent["description"].as_str().unwrap().into(),
            parameters: agent["parameters"].clone(),
            execution_mode: AgentToolMode::Sequential,
            replay: ToolReplay::Safe,
        };
        let inputs = [0, 1]
            .into_iter()
            .map(|minor| {
                let set = &entry["selected_minor_schema_sets"][format!("1.{minor}")];
                ToolSchemaInputs {
                    minor,
                    arguments: serde_json::to_vec(&set["arguments"]["schema"]).unwrap(),
                    progress: serde_json::to_vec(&set["progress"]["schema"]).unwrap(),
                    raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).unwrap(),
                }
            })
            .collect();
        let tool =
            ApprovedTool::new("peer-a".into(), descriptor, inputs, JsonLimits::default()).unwrap();
        HostAuthority::new(
            "peer-a".into(),
            generation,
            1,
            1,
            v1_1::ProtocolLimits {
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
            },
            vec![tool],
            vec![],
        )
        .unwrap()
    }

    fn owner_and_route() -> (
        Arc<LifecycleSpawner>,
        SingleToolOwner,
        ReadyToolRoute,
        Arc<LifecycleChannel>,
        Arc<LifecycleClock>,
    ) {
        owner_and_route_at(7)
    }

    fn owner_and_route_at(
        generation: u64,
    ) -> (
        Arc<LifecycleSpawner>,
        SingleToolOwner,
        ReadyToolRoute,
        Arc<LifecycleChannel>,
        Arc<LifecycleClock>,
    ) {
        let spawner = Arc::new(LifecycleSpawner::default());
        let owner = SingleToolOwner::new(
            spawner.clone(),
            OwnerLimits {
                outbound_bytes: nz(8192),
                receipt_bytes: nz(8192),
                write_timeout_ms: NonZeroU64::new(10).unwrap(),
            },
        );
        let channel = Arc::new(LifecycleChannel::new());
        channel.push(hello());
        channel.push(manifest());
        channel.push(frame(5, &[]));
        let clock = Arc::new(LifecycleClock::new(100));
        let route = spawner
            .drive(owner.connect(
                host(generation),
                channel.clone(),
                clock.clone(),
                PumpConfig {
                    max_read_chunk: nz(8192),
                    write_timeout_ms: NonZeroU64::new(10).unwrap(),
                },
            ))
            .unwrap();
        route
            .bind_timing(EchoRouteTiming {
                method_limit_ms: NonZeroU64::new(1000).unwrap(),
                connection_deadline_ms: 10_000,
            })
            .unwrap();
        (spawner, owner, route, channel, clock)
    }

    fn plan() -> ToolEffectPlan<()> {
        let index = SafeUnsigned::new(0).unwrap();
        ToolEffectPlan {
            key: pi_agent_schema::EffectKey {
                session_id: pi_agent_schema::SessionId::new("session").unwrap(),
                lane: pi_agent_schema::LaneId::new("main").unwrap(),
                operation_id: pi_agent_schema::OperationId::new("op").unwrap(),
                kind: pi_agent_schema::EffectKind::Tool,
                step_or_task_id: pi_agent_schema::EffectStepId::new("step").unwrap(),
                logical_attempt: index,
                item_index: Some(index),
            },
            assistant_entry_id: pi_agent_schema::EntryId::new("assistant-entry").unwrap(),
            source_index: index,
            args_key: "op.tool_args".into(),
            telemetry_context: (),
        }
    }

    fn captured(_source: &str) -> CapturedArguments {
        CapturedArguments {
            key: "op.tool_args".into(),
            value: serde_json::from_str(r#"{"text":"ping"}"#).unwrap(),
            seq: SafeUnsigned::new(0).unwrap(),
        }
    }

    fn install(route: &ReadyToolRoute) {
        route.install_staged_receive_for_test(Arc::new(LifecycleChannel::new()));
    }

    fn refusal_frame(body_len: usize, supported: bool) -> ActiveFrame {
        ActiveFrame {
            envelope: SelectedEnvelope::V1_1(v1_1::Envelope {
                frame: Some(if supported {
                    v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
                        request_id: 1,
                        ..Default::default()
                    })
                } else {
                    v1_1::envelope::Frame::Credit(v1_1::Credit {
                        request_id: 1,
                        additional_bytes: 37,
                    })
                }),
            }),
            body_len,
        }
    }

    #[test]
    fn retained_refusal_keeps_exact_binding_and_measured_length_without_mutation() {
        let (_spawner, owner, route, _channel, _clock) = owner_and_route();
        install(&route);
        let origin: Arc<dyn OriginFence> = Arc::new(LifecycleOrigin);
        let prepared = route
            .prepare_planned_with_captured_bounds(
                &plan(),
                &captured("refusal"),
                "refusal",
                1,
                Some(&origin),
            )
            .unwrap();
        let binding = prepared.binding().clone();
        let _handle = prepared.activate().unwrap();
        let caller = route
            .route
            .caller
            .lock()
            .unwrap()
            .clone()
            .unwrap()
            .staged_identity();
        let mut wrong_caller = caller.clone();
        wrong_caller.source_call_id.push_str("-wrong");
        assert_eq!(
            route
                .route
                .prepare_retained_refusal(&binding, &wrong_caller, &refusal_frame(37, true)),
            Err(DriverError::WrongBinding),
            "a mismatched caller tuple is refused before any debit"
        );
        assert_eq!(
            route
                .route
                .prepare_retained_refusal(&binding, &caller, &refusal_frame(37, true))
                .unwrap(),
            37,
            "the inert witness keeps the transmitted length"
        );
        assert_eq!(
            route
                .route
                .prepare_retained_refusal(&binding, &caller, &refusal_frame(0, true)),
            Err(DriverError::Overloaded),
            "an empty measured body is refused before any debit"
        );
        assert_eq!(
            owner.binding_phase(&binding),
            Ok(LivePhase::Open),
            "refusal does not advance or retire the live binding"
        );
    }

    #[test]
    fn retained_refusal_checks_caller_frame_and_closed_custody_before_mutation() {
        let (_spawner, owner, route, _channel, _clock) = owner_and_route();
        install(&route);
        let origin: Arc<dyn OriginFence> = Arc::new(LifecycleOrigin);
        let prepared = route
            .prepare_planned_with_captured_bounds(
                &plan(),
                &captured("staging"),
                "staging",
                1,
                Some(&origin),
            )
            .unwrap();
        let binding = prepared.binding().clone();
        let _handle = prepared.activate().unwrap();
        let caller = route
            .route
            .caller
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .staged_identity();
        let before = {
            let state = owner.0.state.lock().unwrap();
            let slot = state.live.as_ref().unwrap();
            let writes = route.route.writes.lock().unwrap();
            (
                slot.invocation.credit(),
                slot.invocation.next_sequence(),
                slot.invocation.phase(),
                slot.invocation.receipt().is_some(),
                route.route.next_id.load(Ordering::SeqCst),
                writes.jobs.len(),
                writes.normal_bytes,
            )
        };
        for axis in 0..6 {
            let mut wrong = caller.clone();
            match axis {
                0 => wrong.arguments.key.push_str("-wrong"),
                1 => {
                    wrong.arguments.value = serde_json::from_str(r#"{"text":"changed"}"#).unwrap();
                }
                2 => wrong.arguments.seq = SafeUnsigned::new(1).unwrap(),
                3 => wrong.source_call_id.push_str("-wrong"),
                4 => wrong.epoch = 0,
                5 => wrong.origin = Arc::new(LifecycleOrigin),
                _ => unreachable!(),
            }
            assert_eq!(
                route
                    .route
                    .prepare_retained_refusal(&binding, &wrong, &refusal_frame(37, true)),
                Err(DriverError::WrongBinding),
                "caller axis {axis} must be refused"
            );
        }
        for (frame, error) in [
            (refusal_frame(0, true), DriverError::Overloaded),
            (refusal_frame(8193, true), DriverError::Overloaded),
            (refusal_frame(37, false), DriverError::HostFault),
        ] {
            assert_eq!(
                route
                    .route
                    .prepare_retained_refusal(&binding, &caller, &frame),
                Err(error)
            );
        }
        let staged = route
            .route
            .staged_receive
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone();
        let decision = staged
            .prepare_refusal(&binding, &caller, &refusal_frame(37, true))
            .unwrap();
        assert!(decision.binding.same_live_identity(&binding));
        assert_eq!(decision.binding.effect_key(), binding.effect_key());
        assert_eq!(decision.body_len, 37);
        let reader = staged.claim_reader().unwrap();
        staged.finish_close();
        assert_eq!(
            route
                .route
                .prepare_retained_refusal(&binding, &caller, &refusal_frame(37, true)),
            Err(DriverError::Closed)
        );
        assert!(!staged.is_cleaned_for_test());
        drop(reader);
        assert!(staged.is_cleaned_for_test());
        assert_eq!(
            route
                .route
                .prepare_retained_refusal(&binding, &caller, &refusal_frame(37, true)),
            Err(DriverError::Closed)
        );
        let state = owner.0.state.lock().unwrap();
        let slot = state.live.as_ref().unwrap();
        let writes = route.route.writes.lock().unwrap();
        assert_eq!(
            (
                slot.invocation.credit(),
                slot.invocation.next_sequence(),
                slot.invocation.phase(),
                slot.invocation.receipt().is_some(),
                route.route.next_id.load(Ordering::SeqCst),
                writes.jobs.len(),
                writes.normal_bytes,
            ),
            before,
            "refusal preparation must not debit, retain a receipt or move the queue/ID"
        );
    }

    pub(crate) fn retained_indexed_walking_branch_for_test() {
        let (spawner, owner, route, channel, _clock) = owner_and_route();
        route.install_walking_receive_for_test(channel.clone());
        source_observer::set_c3_walking(true);
        let origin: Arc<dyn OriginFence> = Arc::new(LifecycleOrigin);
        let prepared = route
            .prepare_planned_with_captured_bounds(
                &plan(),
                &captured("walking"),
                "walking",
                1,
                Some(&origin),
            )
            .unwrap();
        let binding = prepared.binding().clone();
        let _handle = prepared.activate().unwrap();
        spawner.tick();
        channel.push(raw("terminal", 1));
        spawner.tick();
        source_observer::set_c3_walking(false);

        assert_eq!(
            owner.binding_phase(&binding),
            Ok(LivePhase::TerminalReceived),
            "the test-only branch must retain the indexed terminal"
        );
        let events = source_observer::snapshot();
        assert!(events.iter().any(|event| {
            event.kind == "body_capture"
                && event.request_id == Some(binding.request_id())
                && event.bytes == event.body_len
                && event
                    .bound_bytes
                    .is_some_and(|bound| event.bytes.is_some_and(|body| bound >= body))
                && event.raw_body.is_none()
        }));
        assert!(events.iter().any(|event| {
            event.kind == "body_capture"
                && event.owner == Some("ConnectionPump")
                && event.operation == Some("pump_borrow")
                && event.outcome == Some("retained")
                && event.request_id == Some(binding.request_id())
                && event.bytes == event.body_len
                && event.raw_body.is_none()
        }));
        assert!(events.iter().any(|event| {
            event.kind == "credit"
                && event.operation == Some("additional")
                && event.outcome == Some("prepared")
                && event.request_id == Some(binding.request_id())
        }));
        assert!(events.iter().any(|event| {
            event.kind == "receive"
                && event.operation == Some("indexed")
                && event.outcome == Some("terminal_received")
                && event.bytes == event.body_len
                && event.request_id == Some(binding.request_id())
        }));
        owner
            .retire(&binding, RetireReason::Committed)
            .expect("terminal receipt retires under the existing owner guard");
        route.close();
    }

    #[test]
    fn retained_indexed_walking_branch_uses_body_lease_credit_and_terminal_receipt() {
        source_observer::reset();
        retained_indexed_walking_branch_for_test();
    }

    fn raw(name: &str, request_id: u8) -> Vec<u8> {
        let vectors: Value = serde_json::from_str(RAW).unwrap();
        let mut bytes = hex(vectors[name]["frame_hex"].as_str().unwrap());
        assert_eq!(bytes[5], 0x08);
        assert_eq!(bytes[6], 17);
        bytes[6] = request_id;
        bytes
    }

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

    #[test]
    fn rollback_request_removes_only_the_unpublished_tail_and_restores_capacity() {
        let first = EncodedFrame {
            bytes: vec![0; 3],
            body_len: 3,
        };
        let second = EncodedFrame {
            bytes: vec![0; 5],
            body_len: 5,
        };
        let mut writes = WriteState::default();
        writes
            .reserve(WriteKind::Request(1), first.bytes.len(), 16, 16, 16)
            .unwrap();
        writes.jobs.push_back(WriteJob {
            frame: first,
            kind: WriteKind::Request(1),
            deadline: 10,
            active: false,
        });
        writes
            .reserve(WriteKind::Request(2), second.bytes.len(), 16, 16, 16)
            .unwrap();
        writes.jobs.push_back(WriteJob {
            frame: second,
            kind: WriteKind::Request(2),
            deadline: 10,
            active: false,
        });
        assert!(writes.rollback_request(2));
        assert_eq!(writes.normal_bytes, 3);
        assert_eq!(writes.jobs.len(), 1);
        assert!(!writes.rollback_request(2));
        assert!(writes.rollback_request(1));
        assert_eq!(writes.normal_bytes, 0);
        assert!(writes.jobs.is_empty());
    }

    #[test]
    fn route_forced_bind_and_tail_failure_closes_without_self_deadlock() {
        let (_spawner, owner, route, _channel, _clock) = owner_and_route();
        install(&route);
        route.force_staged_bind_failure_for_test();
        route.force_rollback_failure_for_test();
        let origin: Arc<dyn OriginFence> = Arc::new(LifecycleOrigin);
        let route_for_call = route.clone();
        let owner_for_call = owner.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let result = route_for_call.prepare_planned_with_captured_bounds(
                &plan(),
                &captured("rollback"),
                "rollback",
                1,
                Some(&origin),
            );
            done_tx.send(result).unwrap();
            drop(owner_for_call);
        });
        let result = done_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("forced rollback fallback must be bounded");
        assert!(matches!(result, Err(DriverError::HostFault)));
        assert!(matches!(route.route.status(), RouteStatus::Closed));
        assert!(owner.retained_binding().is_none());
        assert!(owner.0.state.lock().unwrap().active.is_none());
        assert!(route.route.caller.lock().unwrap().is_none());
        let staged = route.route.staged_receive.lock().unwrap().clone().unwrap();
        assert!(staged.is_cleaned_for_test());
        assert!(!staged.has_caller_for_test());
        let writes = route.route.writes.lock().unwrap();
        assert!(writes.jobs.is_empty());
        assert_eq!(writes.normal_bytes, 0);
        assert_eq!(route.route.next_id.load(Ordering::SeqCst), 1);
        let after_close_origin: Arc<dyn OriginFence> = Arc::new(LifecycleOrigin);
        assert!(matches!(
            route.prepare_planned_with_captured_bounds(
                &plan(),
                &captured("after-close"),
                "after-close",
                1,
                Some(&after_close_origin),
            ),
            Err(DriverError::Closed)
        ));
    }

    #[test]
    fn same_route_distinct_claim_after_terminal_clears_staged_caller() {
        let (spawner, owner, route, channel, _clock) = owner_and_route();
        install(&route);
        let origin: Arc<dyn OriginFence> = Arc::new(LifecycleOrigin);
        let first = route
            .prepare_planned_with_captured_bounds(
                &plan(),
                &captured("first"),
                "first",
                1,
                Some(&origin),
            )
            .unwrap();
        let first_binding = first.binding().clone();
        let first_handle = first.activate().unwrap();
        spawner.tick();
        channel.push(raw("terminal", 1));
        spawner.tick();
        assert_eq!(
            owner.binding_phase(&first_binding).unwrap(),
            LivePhase::TerminalReceived
        );
        owner
            .retire(&first_binding, RetireReason::Committed)
            .unwrap();
        assert!(owner.retained_binding().is_none());
        assert!(route.route.caller.lock().unwrap().is_none());
        let staged = route.route.staged_receive.lock().unwrap().clone().unwrap();
        assert!(!staged.has_caller_for_test());

        let second = route
            .prepare_planned_with_captured_bounds(
                &plan(),
                &captured("second"),
                "second",
                1,
                Some(&origin),
            )
            .unwrap();
        assert_eq!(second.binding().request_id(), 2);
        let second_binding = second.binding().clone();
        let _second_handle = second.activate().unwrap();
        owner.fence_no_longer_expected(&second_binding).unwrap();
        drop(first_handle);
        route.close();
    }

    #[test]
    fn same_route_captured_tuple_mismatch_rejects_before_queue_or_id_mutation() {
        let (spawner, owner, route, channel, _clock) = owner_and_route();
        install(&route);
        let origin: Arc<dyn OriginFence> = Arc::new(LifecycleOrigin);
        let first = route
            .prepare_planned_with_captured_bounds(
                &plan(),
                &captured("first"),
                "first",
                1,
                Some(&origin),
            )
            .unwrap();
        let first_binding = first.binding().clone();
        let _first_handle = first.activate().unwrap();
        spawner.tick();
        channel.push(raw("terminal", 1));
        spawner.tick();
        assert_eq!(
            owner.binding_phase(&first_binding).unwrap(),
            LivePhase::TerminalReceived
        );
        owner
            .retire(&first_binding, RetireReason::Committed)
            .unwrap();
        route
            .route
            .caller
            .lock()
            .unwrap()
            .replace(CapturedCaller8192 {
                binding: Some(first_binding),
                key: plan().key,
                arguments: captured("stale"),
                source_call_id: "stale".into(),
                epoch: 1,
                origin: origin.clone(),
            });
        let next_id = route.route.next_id.load(Ordering::SeqCst);
        let writes = route.route.writes.lock().unwrap();
        let before = (writes.jobs.len(), writes.normal_bytes);
        drop(writes);
        let result = route.prepare_planned_with_captured_bounds(
            &plan(),
            &captured("second"),
            "second",
            1,
            Some(&origin),
        );
        assert!(matches!(result, Err(DriverError::WrongBinding)));
        assert_eq!(route.route.next_id.load(Ordering::SeqCst), next_id);
        let writes = route.route.writes.lock().unwrap();
        assert_eq!((writes.jobs.len(), writes.normal_bytes), before);
        drop(writes);
        route.close();
    }

    #[test]
    fn route_close_matrix_clears_owner_and_staged_custody_once() {
        {
            let (_spawner, owner, route, _channel, _clock) = owner_and_route();
            install(&route);
            owner.close_route(&route.route, DriverError::PeerLost);
            assert!(matches!(route.route.status(), RouteStatus::Closed));
            assert!(owner.0.state.lock().unwrap().active.is_none());
            let staged = route.route.staged_receive.lock().unwrap().clone().unwrap();
            assert!(staged.is_cleaned_for_test());
            route.close();
        }

        {
            let (_spawner, owner, route, _channel, _clock) = owner_and_route();
            install(&route);
            let origin: Arc<dyn OriginFence> = Arc::new(LifecycleOrigin);
            let prepared = route
                .prepare_planned_with_captured_bounds(
                    &plan(),
                    &captured("fence"),
                    "fence",
                    1,
                    Some(&origin),
                )
                .unwrap();
            let binding = prepared.binding().clone();
            let _handle = prepared.activate().unwrap();
            owner.fence_no_longer_expected(&binding).unwrap();
            assert!(matches!(route.route.status(), RouteStatus::Closed));
            assert!(owner.retained_binding().is_none());
            assert!(
                route
                    .route
                    .staged_receive
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap()
                    .is_cleaned_for_test()
            );
        }

        {
            let (_spawner, owner, route, _channel, _clock) = owner_and_route();
            install(&route);
            let prepared = route
                .prepare_planned_with_bounds(&plan(), &captured("abort").value, "abort", None, None)
                .unwrap();
            drop(prepared);
            assert!(matches!(route.route.status(), RouteStatus::Closed));
            assert!(owner.retained_binding().is_none());
        }

        {
            let (_spawner, owner, route, _channel, clock) = owner_and_route();
            install(&route);
            let prepared = route
                .prepare_planned_with_bounds(
                    &plan(),
                    &captured("expiry").value,
                    "expiry",
                    None,
                    None,
                )
                .unwrap();
            let binding = prepared.binding().clone();
            let _handle = prepared.activate().unwrap();
            clock.set(5_000);
            owner.expire(&route.route, &binding);
            assert!(matches!(route.route.status(), RouteStatus::Closed));
            assert_eq!(owner.binding_phase(&binding).unwrap(), LivePhase::PeerLost);
            owner.acknowledge_peer_lost(&binding).unwrap();
            assert!(owner.retained_binding().is_none());
        }

        {
            let (_spawner, owner, route, _channel, _clock) = owner_and_route();
            install(&route);
            route.route.mark_draining();
            assert!(owner.close_drained_after_cancel(&route.route));
            assert!(matches!(route.route.status(), RouteStatus::Closed));
            assert!(owner.0.state.lock().unwrap().active.is_none());
        }

        {
            let (_spawner, owner, route, _channel, _clock) = owner_and_route();
            install(&route);
            route.close();
            let decision = route.route.prepare_fenced_route(99);
            route.route.finish_fence_decision(decision);
            owner.close_route(&route.route, DriverError::PeerLost);
            route.close();
            assert!(matches!(route.route.status(), RouteStatus::Closed));
            assert!(
                route
                    .route
                    .staged_receive
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap()
                    .is_cleaned_for_test()
            );
        }
    }

    #[test]
    fn route_reader_cleanup_defers_and_refuses_until_guard_drop() {
        let (_spawner, owner, route, _channel, _clock) = owner_and_route();
        install(&route);
        let staged = route.route.staged_receive.lock().unwrap().clone().unwrap();
        let reader = staged.claim_reader().unwrap();
        route.close();
        assert!(!staged.is_cleaned_for_test());
        assert!(matches!(
            staged.claim_reader(),
            Err(crate::extensions::receive_host8192::Receive8192Error::Occupied)
        ));
        drop(reader);
        assert!(staged.is_cleaned_for_test());
        assert!(matches!(
            staged.claim_reader(),
            Err(crate::extensions::receive_host8192::Receive8192Error::Closed)
        ));
        owner.close_route(&route.route, DriverError::PeerLost);
    }

    #[test]
    fn route_preserve_denial_clears_invocation_caller_before_deferred_cleanup() {
        let (_spawner, owner, route, _channel, _clock) = owner_and_route();
        install(&route);
        let origin: Arc<dyn OriginFence> = Arc::new(LifecycleOrigin);
        let prepared = route
            .prepare_planned_with_captured_bounds(
                &plan(),
                &captured("denial"),
                "denial",
                1,
                Some(&origin),
            )
            .unwrap();
        let binding = prepared.binding().clone();
        let _handle = prepared.activate().unwrap();
        let denial = denial_frame(&route.route, 17, v1_1::ErrorCode::Forbidden).unwrap();
        let denial_size = denial.bytes.len();
        route.route.enqueue(denial, WriteKind::Denial).unwrap();
        owner.fence_no_longer_expected(&binding).unwrap();
        assert!(matches!(route.route.status(), RouteStatus::Draining));
        assert!(owner.retained_binding().is_none());
        assert!(route.route.caller.lock().unwrap().is_none());
        let staged = route.route.staged_receive.lock().unwrap().clone().unwrap();
        assert!(!staged.has_caller_for_test());
        assert!(!staged.is_cleaned_for_test());
        route.route.release_write(WriteKind::Denial, denial_size);
        assert!(owner.close_drained_after_cancel(&route.route));
        assert!(matches!(route.route.status(), RouteStatus::Closed));
        assert!(staged.is_cleaned_for_test());
    }

    #[test]
    fn route_replacement_closes_old_owner_and_preserves_new_route() {
        let (spawner, owner, old, _channel, _clock) = owner_and_route();
        install(&old);
        let prepared = old
            .prepare_planned_with_bounds(
                &plan(),
                &captured("replacement").value,
                "replacement",
                None,
                None,
            )
            .unwrap();
        let _handle = prepared.activate().unwrap();
        let new_channel = Arc::new(LifecycleChannel::new());
        new_channel.push(hello());
        new_channel.push(manifest());
        new_channel.push(frame(5, &[]));
        let new_clock = Arc::new(LifecycleClock::new(200));
        let new_route = spawner
            .drive(owner.connect(
                host(8),
                new_channel,
                new_clock,
                PumpConfig {
                    max_read_chunk: nz(8192),
                    write_timeout_ms: NonZeroU64::new(10).unwrap(),
                },
            ))
            .unwrap();
        assert!(matches!(old.route.status(), RouteStatus::Draining));
        owner.close_route(&old.route, DriverError::PeerLost);
        assert!(matches!(old.route.status(), RouteStatus::Closed));
        assert!(new_route.is_current());
        new_route.close();
    }
}
