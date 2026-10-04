//! One native-owned profile route, using the existing framed byte pump.
#[cfg(test)]
use crate::extensions::transport::source_observer;
use crate::extensions::transport::{
    ConnectionPump, MonotonicClock, OrderedByteChannel, PumpConfig, PumpError,
    RetainedOrderedByteChannel,
};
use pi_agent_schema::{BoxFuture, EffectKey, HarnessRuntimeError};
use pi_extension_protocol::profiles::NativeValidator;
use pi_extension_protocol::{
    activation::{ActiveGrant, HostAuthority},
    endpoint::SelectedEnvelope,
    profile_live::{self, Accepted, Binding, Reservations, RetireReason, Role, Start},
    profiles::{Direction, ValidatedPayload},
    wire::{framing, v1_0, v1_1},
};
use std::{
    collections::BTreeMap,
    future::poll_fn,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
};

pub trait HostActions: Send + Sync {
    fn execute<'a>(
        &'a self,
        invocation: ActionInvocation,
        payload: ValidatedPayload,
    ) -> BoxFuture<'a, Result<Vec<u8>, v1_1::ErrorCode>>;
}
struct DeferredCloseChannel {
    inner: Arc<dyn OrderedByteChannel>,
    armed: AtomicBool,
}
impl DeferredCloseChannel {
    fn new(inner: Arc<dyn OrderedByteChannel>) -> Self {
        Self {
            inner,
            armed: AtomicBool::new(false),
        }
    }
    fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }
}
impl OrderedByteChannel for DeferredCloseChannel {
    fn max_read_scratch_bytes(&self) -> usize {
        self.inner.max_read_scratch_bytes()
    }
    fn read_chunk<'a>(
        &'a self,
        buffer: &'a mut [u8],
    ) -> BoxFuture<'a, Result<Option<usize>, crate::extensions::transport::ChannelError>> {
        self.inner.read_chunk(buffer)
    }
    fn write_frame(
        &self,
        frame: framing::EncodedFrame,
    ) -> BoxFuture<'_, Result<(), crate::extensions::transport::ChannelError>> {
        self.inner.write_frame(frame)
    }
    fn close(&self) {
        if !self.armed.load(Ordering::Acquire) {
            self.inner.close();
        }
    }
}
#[derive(Default)]
struct PeerCloseFence {
    clean_eof: bool,
    deferred_cancel_id: Option<u64>,
}
fn peer_close_fence_matches(fence: &PeerCloseFence, id: u64) -> bool {
    fence.clean_eof
        && fence
            .deferred_cancel_id
            .is_none_or(|deferred| deferred == id)
}
#[derive(Clone)]
pub struct ActionInvocation {
    connection: Arc<ProfileConnection>,
    binding: Binding,
}
impl ActionInvocation {
    /// Check immediately before invoking a native public action. No peer wait
    /// occurs while the native lane owns its mutation line.
    pub fn dispatch(&self) -> Result<(), v1_1::ErrorCode> {
        let result = self
            .connection
            .owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dispatched_at(&self.binding, self.connection.clock.now_ms())
            .map_err(|_| v1_1::ErrorCode::InvalidParent);
        #[cfg(test)]
        if result.is_ok() {
            self.connection.record_owner_lifecycle(
                "dispatch_release",
                "complete",
                Some(&self.binding),
                None,
            );
        }
        result
    }
}
pub struct ProfileConnection {
    owner: Mutex<profile_live::Owner>,
    pump: Mutex<Option<ConnectionPump>>,
    channel: Arc<dyn OrderedByteChannel>,
    clock: Arc<dyn MonotonicClock>,
    grant: ActiveGrant,
    tool_connection: Option<profile_live::ConnectionIdentity>,
    tool: Option<Arc<crate::extensions::tool_driver::ReadyToolRoute>>,
    inbox: Option<Arc<super::tool_route::ToolInbox>>,
    service: Option<Arc<super::service::Writer>>,
    resource_catalogs: Mutex<BTreeMap<(String, String), super::resources::ResourceCatalog>>,
    subscriptions: Mutex<BTreeMap<String, (Binding, Arc<super::events::ProjectedEvents>)>>,
    passive_changed: Mutex<pi_ai_core::CancellationToken>,
    active_actions: Mutex<BTreeMap<u64, Arc<dyn HostActions>>>,
    dispatch_scopes: Mutex<BTreeMap<u64, Arc<dyn DispatchScope>>>,
    timeout_ms: u64,
    slot_line: crate::scheduler::MutationLine,
    waiting: AtomicUsize,
    peer_close: Mutex<PeerCloseFence>,
}
struct ConnectToolArgs<'a> {
    owner: &'a crate::extensions::tool_driver::SingleToolOwner,
    host: HostAuthority,
    channel: Arc<dyn OrderedByteChannel>,
    retained_channel: Option<Arc<dyn RetainedOrderedByteChannel>>,
    clock: Arc<dyn MonotonicClock>,
    config: PumpConfig,
    reservations: Reservations,
    remaining_ms: u64,
}
pub(crate) trait DispatchScope: Send + Sync {
    #[cfg(test)]
    fn observe_started(
        &self,
        _connection: &profile_live::ConnectionIdentity,
        _binding: &Binding,
        _request: &v1_1::Request,
    ) {
    }
    fn prepare(
        &self,
    ) -> BoxFuture<'_, Result<crate::scheduler::MutationPermit, HarnessRuntimeError>>;
}
pub(crate) struct CallScope {
    pub key: Option<EffectKey>,
    pub role: Role,
    pub parent: Option<Binding>,
    pub native_parent: Option<profile_live::NativeParent>,
    pub permit: Option<crate::scheduler::MutationPermit>,
    pub slot: Option<RouteSlot>,
    pub dispatch: Option<Arc<dyn DispatchScope>>,
}
pub struct Reply {
    pub binding: Binding,
    pub payload: Option<ValidatedPayload>,
    pub error: Option<v1_1::ErrorCode>,
}

#[cfg(test)]
fn observed_role(role: Role) -> &'static str {
    match role {
        Role::Planned => "Planned",
        Role::Inline => "Inline",
        Role::Preaccept => "Preaccept",
        Role::HostAction => "HostAction",
        Role::Passive => "Passive",
    }
}

impl ProfileConnection {
    #[cfg(test)]
    fn record_owner_lifecycle(
        &self,
        operation: &'static str,
        outcome: &'static str,
        binding: Option<&Binding>,
        request_id: Option<u64>,
    ) {
        if operation == "admit" && source_observer::owner_admission_suppressed() {
            return;
        }
        if operation == "final_retire" && source_observer::final_retire_suppressed() {
            return;
        }
        let mut event = source_observer::SourceEvent::new(
            "owner_lifecycle",
            file!(),
            line!(),
            "ProfileConnection::owner_lifecycle",
        );
        event.field = Some("P_req");
        event.owner = Some("profile_live::Owner");
        event.operation = Some(operation);
        event.outcome = Some(outcome);
        event.request_id = request_id.or_else(|| binding.map(Binding::id));
        event.generation = Some(self.grant.wire().connection_generation);
        event.token = Some(source_observer::ConnectionToken::from_identity(
            &self.connection_identity(),
        ));
        source_observer::record(event);
    }

    pub async fn connect(
        host: HostAuthority,
        channel: Arc<dyn OrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
        reservations: Reservations,
        connection_remaining_ms: u64,
    ) -> Result<Arc<Self>, HarnessRuntimeError> {
        let mut pump = ConnectionPump::new(host, channel.clone(), clock.clone(), config)
            .map_err(|_| super::fault())?;
        let grant = pump.drive_until_ready().await.map_err(|_| super::fault())?;
        let owner = profile_live::Owner::new(
            grant.clone(),
            reservations,
            clock.now_ms(),
            connection_remaining_ms,
        )
        .map_err(|_| super::fault())?;
        Ok(Arc::new(Self {
            owner: Mutex::new(owner),
            pump: Mutex::new(Some(pump)),
            channel,
            clock,
            grant,
            tool_connection: None,
            tool: None,
            inbox: None,
            service: None,
            resource_catalogs: Mutex::new(BTreeMap::new()),
            subscriptions: Mutex::new(BTreeMap::new()),
            passive_changed: Mutex::new(pi_ai_core::CancellationToken::new()),
            active_actions: Mutex::new(BTreeMap::new()),
            dispatch_scopes: Mutex::new(BTreeMap::new()),
            timeout_ms: config.write_timeout_ms.get(),
            slot_line: crate::scheduler::MutationLine::new(crate::scheduler::Admission::new()),
            waiting: AtomicUsize::new(0),
            peer_close: Mutex::new(PeerCloseFence::default()),
        }))
    }
    pub(crate) async fn connect_tool(
        owner: &crate::extensions::tool_driver::SingleToolOwner,
        host: HostAuthority,
        channel: Arc<dyn OrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
        reservations: Reservations,
        remaining_ms: u64,
    ) -> Result<(Arc<Self>, crate::extensions::tool_driver::ReadyToolRoute), HarnessRuntimeError>
    {
        Self::connect_tool_inner(ConnectToolArgs {
            owner,
            host,
            channel,
            retained_channel: None,
            clock,
            config,
            reservations,
            remaining_ms,
        })
        .await
    }

    pub(crate) async fn connect_tool_retained(
        owner: &crate::extensions::tool_driver::SingleToolOwner,
        host: HostAuthority,
        channel: Arc<dyn RetainedOrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
        reservations: Reservations,
        remaining_ms: u64,
    ) -> Result<(Arc<Self>, crate::extensions::tool_driver::ReadyToolRoute), HarnessRuntimeError>
    {
        let ordered: Arc<dyn OrderedByteChannel> = channel.clone();
        Self::connect_tool_inner(ConnectToolArgs {
            owner,
            host,
            channel: ordered,
            retained_channel: Some(channel),
            clock,
            config,
            reservations,
            remaining_ms,
        })
        .await
    }

    async fn connect_tool_inner(
        ConnectToolArgs {
            owner,
            host,
            channel,
            retained_channel,
            clock,
            config,
            reservations,
            remaining_ms,
        }: ConnectToolArgs<'_>,
    ) -> Result<(Arc<Self>, crate::extensions::tool_driver::ReadyToolRoute), HarnessRuntimeError>
    {
        let max = usize::try_from(reservations.blocking_bytes).map_err(|_| super::fault())?;
        let inbox = super::tool_route::ToolInbox::new(max, 256).map_err(|_| super::fault())?;
        let route = match retained_channel {
            Some(retained) => {
                owner
                    .connect_profiles_retained(
                        host,
                        channel.clone(),
                        retained,
                        clock.clone(),
                        config,
                        inbox.clone(),
                    )
                    .await
            }
            None => {
                owner
                    .connect_profiles(host, channel.clone(), clock.clone(), config, inbox.clone())
                    .await
            }
        }
        .map_err(|error| {
            let _ = error;
            super::fault()
        })?;
        let grant = route.grant().clone();
        let identity = route.connection_identity();
        let live = profile_live::Owner::new_with_connection(
            grant.clone(),
            reservations,
            clock.now_ms(),
            remaining_ms,
            identity.clone(),
        )
        .and_then(|owner| owner.with_connection_ids(route.profile_ids()))
        .map_err(|_| super::fault())?;
        let connection = Arc::new(Self {
            owner: Mutex::new(live),
            pump: Mutex::new(None),
            channel,
            clock,
            grant,
            tool_connection: Some(identity),
            tool: Some(Arc::new(route.clone())),
            inbox: Some(inbox.clone()),
            service: None,
            resource_catalogs: Mutex::new(BTreeMap::new()),
            subscriptions: Mutex::new(BTreeMap::new()),
            passive_changed: Mutex::new(pi_ai_core::CancellationToken::new()),
            active_actions: Mutex::new(BTreeMap::new()),
            dispatch_scopes: Mutex::new(BTreeMap::new()),
            timeout_ms: config.write_timeout_ms.get(),
            slot_line: crate::scheduler::MutationLine::new(crate::scheduler::Admission::new()),
            waiting: AtomicUsize::new(0),
            peer_close: Mutex::new(PeerCloseFence::default()),
        });
        inbox.attach(&connection);
        Ok((connection, route))
    }
    pub(crate) fn scope_deadline(&self) -> Result<u64, HarnessRuntimeError> {
        self.clock
            .now_ms()
            .checked_add(self.timeout_ms)
            .ok_or_else(super::fault)
    }
    pub(crate) fn clock(&self) -> Arc<dyn MonotonicClock> {
        self.clock.clone()
    }
    pub(crate) fn enqueue_profile(
        &self,
        frame: framing::EncodedFrame,
        id: u64,
        response: bool,
        deadline: u64,
    ) -> Result<(), crate::extensions::tool_driver::DriverError> {
        match (&self.tool, &self.service) {
            (Some(tool), _) => tool.enqueue_profile(frame, id, response),
            (_, Some(service)) => service.enqueue(frame, id, deadline),
            _ => Err(crate::extensions::tool_driver::DriverError::InvalidHost),
        }
    }
    pub(crate) fn outgoing_identity(
        &self,
        frame: &framing::EncodedFrame,
    ) -> Result<(u64, bool, bool), HarnessRuntimeError> {
        let mut reader = framing::FrameReader::<v1_1::Envelope>::new(
            NonZeroUsize::new(self.max_payload_bytes()).ok_or_else(super::fault)?,
        );
        let frame = reader
            .read_one(&frame.bytes)
            .map_err(|_| super::fault())?
            .frame
            .ok_or_else(super::fault)?
            .envelope;
        match frame.frame {
            Some(v1_1::envelope::Frame::Request(request)) => Ok((request.request_id, false, false)),
            Some(v1_1::envelope::Frame::Response(response)) => {
                Ok((response.request_id, true, false))
            }
            Some(v1_1::envelope::Frame::Cancel(cancel)) => Ok((cancel.request_id, false, true)),
            Some(v1_1::envelope::Frame::StreamItem(item)) => Ok((item.request_id, true, false)),
            Some(v1_1::envelope::Frame::StreamEnd(end)) => Ok((end.request_id, true, false)),
            _ => Err(super::fault()),
        }
    }
    fn request_peer_close(&self) {
        let deferred_cancel_id = {
            let mut fence = self.peer_close.lock().unwrap_or_else(|p| p.into_inner());
            // Publish clean EOF before asking the Writer to retire queued
            // work. The same fence is held until the exact active Cancel ID
            // is recorded, so a writer close cannot race publication.
            fence.clean_eof = true;
            fence.deferred_cancel_id = None;
            match (&self.service, &self.inbox) {
                (Some(service), Some(inbox)) => {
                    let deferred = service.request_peer_close(inbox);
                    fence.deferred_cancel_id = deferred;
                    deferred
                }
                _ => None,
            }
        };
        if deferred_cancel_id.is_none() {
            self.close();
        }
    }
    fn clean_peer_close_after_cancel(&self, id: u64) -> bool {
        let mut fence = self.peer_close.lock().unwrap_or_else(|p| p.into_inner());
        if !peer_close_fence_matches(&fence, id) {
            return false;
        }
        let acknowledged = self
            .inbox
            .as_ref()
            .is_some_and(|inbox| inbox.take_cancel_ack(id));
        if acknowledged {
            *fence = PeerCloseFence::default();
        }
        acknowledged
    }
    pub(crate) async fn admit_tool_write(&self, id: u64, response: bool, cancel: bool) -> bool {
        let dispatch = self
            .dispatch_scopes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&id)
            .cloned();
        let line = if !response && !cancel {
            match dispatch {
                Some(dispatch) => match dispatch.prepare().await {
                    Ok(line) => Some(line),
                    Err(_) => return false,
                },
                None => None,
            }
        } else {
            None
        };
        let mut owner = self.owner.lock().unwrap_or_else(|p| p.into_inner());
        let Some(binding) = owner.binding(id) else {
            return response && owner.denial_live(id, self.clock.now_ms());
        };
        if cancel {
            return matches!(
                owner.phase(&binding),
                Some(profile_live::Phase::CancelRequested)
            );
        }
        if response {
            return owner.phase(&binding).is_some() && !owner.draining();
        }
        let admitted = owner.dispatched_at(&binding, self.clock.now_ms()).is_ok();
        drop(owner);
        drop(line);
        #[cfg(test)]
        if admitted {
            self.record_owner_lifecycle("dispatch_release", "complete", Some(&binding), None);
        }
        admitted
    }
    pub(crate) fn same_tool_connection(
        &self,
        route: &crate::extensions::tool_driver::ReadyToolRoute,
    ) -> bool {
        self.tool_connection
            .as_ref()
            .is_some_and(|connection| connection.same_connection(&route.connection_identity()))
    }
    #[cfg(test)]
    pub(crate) fn tool_inbox_for_test(&self) -> Arc<super::tool_route::ToolInbox> {
        self.inbox
            .as_ref()
            .expect("ordinary Tool route inbox")
            .clone()
    }
    pub(crate) fn passive_control(&self, envelope: &SelectedEnvelope) -> bool {
        let id = match envelope {
            SelectedEnvelope::V1_1(e) => match &e.frame {
                Some(v1_1::envelope::Frame::Credit(value)) => Some(value.request_id),
                Some(v1_1::envelope::Frame::Cancel(value)) => Some(value.request_id),
                _ => None,
            },
            SelectedEnvelope::V1_0(e) => match &e.frame {
                Some(v1_0::envelope::Frame::Credit(value)) => Some(value.request_id),
                Some(v1_0::envelope::Frame::Cancel(value)) => Some(value.request_id),
                _ => None,
            },
        };
        id.is_some_and(|id| {
            self.owner
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .binding(id)
                .is_some_and(|binding| {
                    binding.role() == Role::Passive
                        && binding
                            .capability()
                            .profile
                            .as_ref()
                            .is_some_and(|profile| profile.name == "event-sink.v1")
                })
        })
    }
    fn notify_passive(&self) {
        let old = {
            let mut token = self
                .passive_changed
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            std::mem::replace(&mut *token, pi_ai_core::CancellationToken::new())
        };
        old.cancel();
    }
    async fn serve_peer_frame<
        C: Send + Sync + 'static,
        S: pi_agent_session::storage::RuntimeStorage + 'static,
        M: pi_agent_schema::SessionMetadataRecord,
    >(
        self: &Arc<Self>,
        lane: &crate::owner::RuntimeLane<C, S, M>,
        frame: pi_extension_protocol::endpoint::ActiveFrame,
    ) -> Result<(), HarnessRuntimeError> {
        if let Some(request) = peer_request(&frame.envelope) {
            return self.serve_tool_action(lane, request).await;
        }
        if !self.passive_control(&frame.envelope) {
            return Err(super::fault());
        }
        let policy = pi_ai_core::ModelHeaderPolicy::default();
        let mut native = super::native::NativeDomain::new(lane.owner.session.codec(), &policy);
        self.owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .receive(
                self.grant.wire().connection_generation,
                frame,
                self.clock.now_ms(),
                &mut native,
            )
            .map_err(|_| super::fault())?;
        self.notify_passive();
        Ok(())
    }
    pub(crate) fn register_resource_catalog(
        &self,
        capability: &v1_1::CapabilityRef,
        catalog: super::resources::ResourceCatalog,
    ) -> Result<(), HarnessRuntimeError> {
        if !self.reference_selected(capability)? {
            return Err(super::fault());
        }
        self.resource_catalogs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert((capability.kind.clone(), capability.name.clone()), catalog);
        Ok(())
    }
    pub fn grant(&self) -> &ActiveGrant {
        &self.grant
    }
    pub(crate) async fn acquire_slot(
        self: &Arc<Self>,
        cancel: &pi_ai_core::CancellationToken,
    ) -> Result<RouteSlot, HarnessRuntimeError> {
        let limit = self
            .grant
            .wire()
            .limits
            .expect("finite active limits")
            .in_flight_requests as usize;
        self.waiting
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                (n < limit).then_some(n + 1)
            })
            .map_err(|_| super::fault())?;
        let mut ticket = RouteSlot {
            connection: self.clone(),
            permit: None,
        };
        let mut acquire = Box::pin(self.slot_line.acquire());
        let deadline = self
            .clock
            .now_ms()
            .checked_add(self.timeout_ms)
            .ok_or_else(super::fault)?;
        let mut timer = Box::pin(self.clock.sleep_until(deadline));
        let mut cancelled = Box::pin(cancel.cancelled());
        let permit = poll_fn(|cx| {
            if timer.as_mut().poll(cx).is_ready() || cancelled.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(super::fault()));
            }
            acquire.as_mut().poll(cx)
        })
        .await?;
        ticket.permit = Some(permit);
        Ok(ticket)
    }
    pub fn max_payload_bytes(&self) -> usize {
        self.grant
            .wire()
            .limits
            .expect("active finite limits")
            .frame_bytes as usize
    }
    pub fn connection_identity(&self) -> profile_live::ConnectionIdentity {
        self.owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .connection_identity()
    }
    pub fn occupancy(&self) -> usize {
        self.owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .occupancy()
    }
    pub fn current(&self) -> bool {
        !self
            .owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .draining()
    }
    pub fn retained(&self, binding: &Binding) -> bool {
        self.owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .receipt(binding)
            .is_some()
    }
    pub fn retire(
        &self,
        binding: &Binding,
        reason: RetireReason,
    ) -> Result<(), HarnessRuntimeError> {
        let result = self
            .owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retire(binding, reason)
            .map_err(|_| super::fault());
        #[cfg(test)]
        if result.is_ok() {
            self.record_owner_lifecycle(
                "final_retire",
                match reason {
                    RetireReason::Consumed => "consumed",
                    RetireReason::NoLongerExpected => "no_longer_expected",
                },
                Some(binding),
                None,
            );
        }
        result
    }
    pub fn close(&self) {
        #[cfg(test)]
        {
            let mut event = source_observer::SourceEvent::new(
                "terminal",
                file!(),
                line!(),
                "ProfileConnection::close",
            );
            event.outcome = Some("close");
            event.field = Some("EH_inline_target_provider_allocator");
            event.owner = Some("ProfileConnection");
            event.cleanup = Some(true);
            event.generation = Some(self.grant.wire().connection_generation);
            source_observer::record(event);
        }
        self.owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .lose_generation(self.clock.now_ms())
            .ok();
        self.channel.close();
        if let Some(service) = &self.service {
            service.close();
        }
        if let Some(inbox) = &self.inbox {
            crate::extensions::tool_driver::ToolProfileIngress::close(inbox.as_ref());
        }
        let subscriptions = {
            let mut subscriptions = self.subscriptions.lock().unwrap_or_else(|p| p.into_inner());
            std::mem::take(&mut *subscriptions)
        };
        for (_, (_, subscription)) in subscriptions {
            subscription.cancel();
        }
        self.notify_passive();
        if let Some(route) = &self.tool {
            route.close_profiles();
        }
    }
    // Keep the captured native authority and independent finite route inputs explicit.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn call(
        self: &Arc<Self>,
        capability: v1_1::CapabilityRef,
        method: &str,
        payload: Vec<u8>,
        mut scope: CallScope,
        native: &mut (impl NativeValidator + Send),
        actions: Option<Arc<dyn HostActions>>,
        cancel: &pi_ai_core::CancellationToken,
    ) -> Result<Reply, HarnessRuntimeError> {
        if scope.slot.is_none() {
            if scope.permit.is_some() {
                return Err(super::fault());
            }
            scope.slot = Some(self.acquire_slot(cancel).await?);
        }
        let pump = if self.inbox.is_none() {
            Some(
                self.pump
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .take()
                    .ok_or_else(super::fault)?,
            )
        } else {
            None
        };
        let mut guard = PumpGuard {
            connection: self.clone(),
            pump,
            complete: false,
            id: None,
        };
        let contract = self
            .grant
            .profiles()
            .iter()
            .find(|p| p.reference() == &capability)
            .ok_or_else(super::fault)?;
        let now = self.clock.now_ms();
        let deadline = now.checked_add(self.timeout_ms).ok_or_else(super::fault)?;
        #[cfg(test)]
        let observed_role_value = scope.role;
        #[cfg(test)]
        let observed_parent = scope.parent.is_some();
        #[cfg(test)]
        let observed_native_parent = scope.native_parent.is_some();
        #[cfg(test)]
        {
            let mut event = source_observer::SourceEvent::new(
                "role",
                file!(),
                line!(),
                "ProfileConnection::call",
            );
            event.outcome = Some("start");
            event.field = Some("overlap");
            event.owner = Some("ProfileConnection");
            event.role = Some(observed_role(observed_role_value));
            event.parent_present = Some(observed_parent);
            event.native_parent_present = Some(observed_native_parent);
            event.method = Some(method.to_owned());
            event.generation = Some(self.grant.wire().connection_generation);
            source_observer::record(event);
        }
        let start = Start {
            capability,
            method: method.into(),
            direction: Direction::CoreToExtension,
            role: scope.role,
            key: scope.key.as_ref().map(key_wire),
            parent: scope.parent,
            scope: contract.approved().scope().into(),
            payload,
            now_ms: now,
            requested_deadline_ms: Some(self.timeout_ms),
            method_default_ms: self.timeout_ms,
            method_limit_ms: self.timeout_ms,
            initial_credit: Some(0),
            generation: self.grant.wire().connection_generation,
        };
        let started = {
            let mut owner = self.owner.lock().unwrap_or_else(|p| p.into_inner());
            match scope.native_parent {
                Some(parent) => owner.start_with_native_parent(start, parent, native),
                None => owner.start(start, native),
            }
        };
        drop(scope.permit.take());
        let (binding, request) = started.map_err(|_| super::fault())?;
        #[cfg(test)]
        self.record_owner_lifecycle("admit", "complete", Some(&binding), None);
        #[cfg(test)]
        {
            let mut event = source_observer::SourceEvent::new(
                "role",
                file!(),
                line!(),
                "ProfileConnection::call",
            );
            event.outcome = Some("started");
            event.field = Some("EH_inline_target_provider_allocator");
            event.owner = Some("ProfileConnection");
            event.role = Some(observed_role(observed_role_value));
            event.parent_present = Some(observed_parent);
            event.native_parent_present = Some(observed_native_parent);
            event.method = Some(method.to_owned());
            event.request_id = Some(binding.id());
            event.generation = Some(self.grant.wire().connection_generation);
            source_observer::record(event);
        }
        #[cfg(test)]
        if let Some(dispatch) = &scope.dispatch {
            dispatch.observe_started(&self.connection_identity(), &binding, &request);
        }
        guard.id = Some(binding.id());
        if let Some(dispatch) = scope.dispatch.take() {
            self.dispatch_scopes
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(binding.id(), dispatch);
        }
        if let Some(actions) = &actions {
            self.active_actions
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(binding.id(), actions.clone());
        }
        if self.tool.is_none() {
            let dispatch = self
                .dispatch_scopes
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&binding.id())
                .cloned();
            let line = match dispatch {
                Some(dispatch) => Some(dispatch.prepare().await?),
                None => None,
            };
            self.owner
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .dispatched_at(&binding, self.clock.now_ms())
                .map_err(|_| super::fault())?;
            drop(line);
        }
        self.write(
            encode_request(&request, self.core_minor(), self.max_payload_bytes())?,
            deadline,
        )
        .await?;
        let mut cancel_sent = false;
        loop {
            let event = {
                let mut read: BoxFuture<
                    '_,
                    Result<
                        Option<pi_extension_protocol::endpoint::ActiveFrame>,
                        crate::extensions::transport::PumpError,
                    >,
                > = match guard.pump.as_mut() {
                    Some(pump) => Box::pin(pump.next_active()),
                    None => Box::pin(self.inbox.as_ref().expect("Tool inbox").next(false)),
                };
                let mut timer = Box::pin(self.clock.sleep_until(deadline));
                let mut cancelled = Box::pin(cancel.cancelled());
                poll_fn(|cx| {
                    if timer.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(ReadOutcome::Timeout);
                    }
                    if !cancel_sent && cancelled.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(ReadOutcome::Cancel);
                    }
                    read.as_mut().poll(cx).map(ReadOutcome::Frame)
                })
                .await
            };
            match event {
                ReadOutcome::Timeout => {
                    self.close();
                    return Err(super::fault());
                }
                ReadOutcome::Cancel => {
                    self.owner
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .cancel(&binding, 1, self.clock.now_ms())
                        .ok();
                    let frame =
                        encode_cancel(binding.id(), self.core_minor(), self.max_payload_bytes())?;
                    self.write(frame, deadline).await?;
                    cancel_sent = true;
                }
                ReadOutcome::Frame(event @ (Err(_) | Ok(None))) => {
                    let error = Self::peer_terminal_error(
                        event,
                        self.clean_peer_close_after_cancel(binding.id()),
                    );
                    self.close();
                    return Err(error);
                }
                ReadOutcome::Frame(Ok(Some(frame))) => {
                    if let Some(request) = peer_request(&frame.envelope) {
                        self.host_request(request, &binding, native, actions.as_deref(), deadline)
                            .await?;
                        continue;
                    }
                    if matches!(&frame.envelope,SelectedEnvelope::V1_0(e) if matches!(e.frame,Some(v1_0::envelope::Frame::Credit(_))))
                        || matches!(&frame.envelope,SelectedEnvelope::V1_1(e) if matches!(e.frame,Some(v1_1::envelope::Frame::Credit(_))))
                    {
                        return Err(super::fault());
                    }
                    let accepted = self
                        .owner
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .receive_checked(
                            self.grant.wire().connection_generation,
                            frame,
                            self.clock.now_ms(),
                            native,
                            || self.clock.now_ms(),
                        );
                    let accepted = match accepted {
                        Ok(value) => value,
                        Err(profile_live::Error::Validation(
                            pi_extension_protocol::profiles::Error::NativeRejected,
                        )) if method == "before_tool"
                            || contract.approved().profile() == "hook.v1" =>
                        {
                            self.retire(&binding, RetireReason::NoLongerExpected)?;
                            guard.complete = true;
                            return Err(super::fault());
                        }
                        Err(error)
                            if matches!(
                                contract.approved().profile(),
                                "resource.v1" | "telemetry-sink.v1"
                            ) && matches!(
                                error,
                                profile_live::Error::Validation(
                                    pi_extension_protocol::profiles::Error::Shape
                                        | pi_extension_protocol::profiles::Error::NativeRejected
                                ) | profile_live::Error::Variant
                            ) =>
                        {
                            self.retire(&binding, RetireReason::NoLongerExpected)?;
                            guard.complete = true;
                            return Err(super::fault());
                        }
                        Err(_) => return Err(super::fault()),
                    };
                    match accepted {
                        Accepted::Terminal => {
                            #[cfg(test)]
                            {
                                let mut event = source_observer::SourceEvent::new(
                                    "terminal",
                                    file!(),
                                    line!(),
                                    "ProfileConnection::call",
                                );
                                event.outcome = Some("accepted");
                                event.field = Some("overlap");
                                event.owner = Some("ProfileConnection");
                                event.method = Some(method.to_owned());
                                event.request_id = Some(binding.id());
                                event.role = Some(observed_role(observed_role_value));
                                source_observer::record(event);
                            }
                            let owner = self.owner.lock().unwrap_or_else(|p| p.into_inner());
                            let receipt = owner.receipt(&binding).ok_or_else(super::fault)?;
                            let reply = Reply {
                                binding: binding.clone(),
                                payload: receipt.payload().cloned(),
                                error: receipt.error(),
                            };
                            drop(owner);
                            #[cfg(test)]
                            self.record_owner_lifecycle(
                                "terminal_retain",
                                "complete",
                                Some(&binding),
                                None,
                            );
                            guard.complete = true;
                            return Ok(reply);
                        }
                        Accepted::Progress(_) => return Err(super::fault()), // No positive credit before the separate receive certificate.
                        Accepted::Suppressed | Accepted::Cancelled | Accepted::Credited => {}
                    }
                }
            }
        }
    }
    fn reference_selected(
        &self,
        capability: &v1_1::CapabilityRef,
    ) -> Result<bool, HarnessRuntimeError> {
        if capability.authenticated_extension_id != self.grant.wire().authenticated_extension_id
            || capability.kind.is_empty()
            || capability.name.is_empty()
        {
            return Err(super::fault());
        }
        let profile = capability.profile.as_ref().ok_or_else(super::fault)?;
        if profile.major != 1
            || profile.minor > 1
            || capability.kind != profile.name.trim_end_matches(".v1")
            || pi_extension_protocol::descriptor::canonical_profile_digest(
                &profile.name,
                profile.major,
                profile.minor,
            )
            .is_none_or(|digest| capability.schema_digest_sha256 != digest)
        {
            return Err(super::fault());
        }
        match self.grant.profiles().iter().find(|selected| {
            selected.reference().kind == capability.kind
                && selected.reference().name == capability.name
        }) {
            Some(selected) => {
                if selected.reference() == capability {
                    Ok(true)
                } else {
                    Err(super::fault())
                }
            }
            None => Ok(false),
        }
    }
    async fn fixed_denial(
        &self,
        id: u64,
        code: v1_1::ErrorCode,
        deadline: u64,
    ) -> Result<(), HarnessRuntimeError> {
        self.owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .reserve_denial(
                id,
                self.grant.wire().connection_generation,
                self.clock.now_ms(),
            )
            .map_err(|_| super::fault())?;
        #[cfg(test)]
        self.record_owner_lifecycle("denial_reserve", "complete", None, Some(id));
        let result = self
            .write(
                encode_response(id, Err(code), self.core_minor(), self.max_payload_bytes())?,
                deadline,
            )
            .await;
        self.owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retire_denial(id)
            .map_err(|_| super::fault())?;
        #[cfg(test)]
        self.record_owner_lifecycle("denial_release", "complete", None, Some(id));
        result
    }
    async fn host_request(
        self: &Arc<Self>,
        request: v1_1::Request,
        parent: &Binding,
        native: &mut (impl NativeValidator + Send),
        actions: Option<&dyn HostActions>,
        deadline: u64,
    ) -> Result<(), HarnessRuntimeError> {
        let capability = request.capability_ref.clone().ok_or_else(super::fault)?;
        if !self.reference_selected(&capability)? {
            return self
                .fixed_denial(request.request_id, v1_1::ErrorCode::Forbidden, deadline)
                .await;
        }
        if request.method == "secret.resolve" {
            return self
                .fixed_denial(
                    request.request_id,
                    v1_1::ErrorCode::BrokerUnavailable,
                    deadline,
                )
                .await;
        }
        if self
            .grant
            .profiles()
            .iter()
            .find(|profile| profile.reference() == &capability)
            .is_none_or(|profile| {
                profile
                    .approved()
                    .registry()
                    .method(profile.approved().profile(), &request.method)
                    .is_none()
            })
        {
            return self
                .fixed_denial(
                    request.request_id,
                    v1_1::ErrorCode::UnsupportedMethod,
                    deadline,
                )
                .await;
        }

        let contract = self
            .grant
            .profiles()
            .iter()
            .find(|p| p.reference() == &capability);
        let scope = contract
            .map(|c| c.approved().scope())
            .unwrap_or("")
            .to_owned();
        let method = request.method.clone();
        let id = request.request_id;
        let context = Start {
            capability,
            method: method.clone(),
            direction: Direction::ExtensionToCore,
            role: Role::HostAction,
            key: None,
            parent: Some(parent.clone()),
            scope,
            payload: vec![],
            now_ms: self.clock.now_ms(),
            requested_deadline_ms: None,
            method_default_ms: self.timeout_ms,
            method_limit_ms: self.timeout_ms,
            initial_credit: Some(0),
            generation: self.grant.wire().connection_generation,
        };
        let started = self
            .owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accept_request(request, context, native);
        let result = match started {
            Ok(binding) => {
                #[cfg(test)]
                self.record_owner_lifecycle("admit", "complete", Some(&binding), None);
                let payload = self
                    .owner
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .request_payload(&binding)
                    .cloned()
                    .ok_or_else(super::fault)?;
                let action = ActionInvocation {
                    connection: self.clone(),
                    binding: binding.clone(),
                };
                let result = match actions {
                    Some(actions) => actions.execute(action, payload).await,
                    None => Err(v1_1::ErrorCode::Forbidden),
                };
                self.owner
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .retire(&binding, RetireReason::NoLongerExpected)
                    .map_err(|_| super::fault())?;
                result
            }
            Err(profile_live::Error::SecretBrokerUnavailable) => {
                Err(v1_1::ErrorCode::BrokerUnavailable)
            }
            Err(profile_live::Error::UnsupportedMethod) => Err(v1_1::ErrorCode::UnsupportedMethod),
            Err(profile_live::Error::Parent) => Err(v1_1::ErrorCode::InvalidParent),
            Err(profile_live::Error::Forbidden | profile_live::Error::Reference)
                if !self
                    .owner
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .draining() =>
            {
                Err(v1_1::ErrorCode::Forbidden)
            }
            Err(_) => {
                self.close();
                return Err(super::fault());
            }
        };
        self.write(
            encode_response(id, result, self.core_minor(), self.max_payload_bytes())?,
            deadline,
        )
        .await
    }
    async fn serve_tool_action<
        C: Send + Sync + 'static,
        S: pi_agent_session::storage::RuntimeStorage + 'static,
        M: pi_agent_schema::SessionMetadataRecord,
    >(
        self: &Arc<Self>,
        lane: &crate::owner::RuntimeLane<C, S, M>,
        request: v1_1::Request,
    ) -> Result<(), HarnessRuntimeError> {
        let capability = request.capability_ref.clone().ok_or_else(super::fault)?;
        let deadline = self.scope_deadline()?;
        if !self.reference_selected(&capability)? {
            return self
                .fixed_denial(request.request_id, v1_1::ErrorCode::Forbidden, deadline)
                .await;
        }
        if request.method == "secret.resolve" {
            return self
                .fixed_denial(
                    request.request_id,
                    v1_1::ErrorCode::BrokerUnavailable,
                    deadline,
                )
                .await;
        }

        let contract = self
            .grant
            .profiles()
            .iter()
            .find(|profile| profile.reference() == &capability)
            .ok_or_else(super::fault)?;
        if contract
            .approved()
            .registry()
            .method(contract.approved().profile(), &request.method)
            .is_none()
        {
            return self
                .fixed_denial(
                    request.request_id,
                    v1_1::ErrorCode::UnsupportedMethod,
                    deadline,
                )
                .await;
        }
        if contract.approved().profile() != "host-action.v1" {
            return self.serve_passive_request(lane, request).await;
        }
        let id = request.request_id;
        let method = request.method.clone();
        let Some(parent_id) = request.parent_request_id else {
            return self
                .fixed_denial(request.request_id, v1_1::ErrorCode::InvalidParent, deadline)
                .await;
        };
        let (parent, actions) = {
            let owner = self.owner.lock().unwrap_or_else(|p| p.into_inner());
            let parent = owner.binding(parent_id);
            let actions = self
                .active_actions
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&parent_id)
                .cloned();
            (parent, actions)
        };
        let external = if parent.is_none() {
            match self
                .tool
                .as_ref()
                .ok_or(crate::extensions::tool_driver::DriverError::InvalidHost)
                .and_then(|tool| tool.peer_profile_parent(parent_id))
            {
                Ok(parent) => Some(parent),
                Err(_) => {
                    return self
                        .fixed_denial(id, v1_1::ErrorCode::InvalidParent, deadline)
                        .await;
                }
            }
        } else {
            None
        };
        let context = Start {
            capability,
            method: method.clone(),
            direction: Direction::ExtensionToCore,
            role: Role::HostAction,
            key: None,
            parent: parent.clone(),
            scope: contract.approved().scope().into(),
            payload: vec![],
            now_ms: self.clock.now_ms(),
            requested_deadline_ms: None,
            method_default_ms: self.timeout_ms,
            method_limit_ms: self.timeout_ms,
            initial_credit: Some(0),
            generation: self.grant.wire().connection_generation,
        };
        let policy = pi_ai_core::ModelHeaderPolicy::default();
        let mut native = super::native::NativeDomain::new(lane.owner.session.codec(), &policy);
        let observed = external
            .as_ref()
            .map(|parent| parent.observation(self.clock.now_ms()))
            .transpose()
            .map_err(|_| super::fault())?;
        let binding = {
            let mut owner = self.owner.lock().unwrap_or_else(|p| p.into_inner());
            match external {
                Some(parent) => {
                    owner.accept_request_with_external_parent(request, context, parent, &mut native)
                }
                None => owner.accept_request(request, context, &mut native),
            }
        };
        let binding = match binding {
            Ok(binding) => binding,
            Err(profile_live::Error::Parent | profile_live::Error::Deadline) => {
                return self
                    .fixed_denial(id, v1_1::ErrorCode::InvalidParent, deadline)
                    .await;
            }
            Err(profile_live::Error::Forbidden) => {
                return self
                    .fixed_denial(id, v1_1::ErrorCode::Forbidden, deadline)
                    .await;
            }
            Err(profile_live::Error::Overloaded) => {
                return self
                    .fixed_denial(id, v1_1::ErrorCode::Overloaded, deadline)
                    .await;
            }
            Err(_) => return Err(super::fault()),
        };
        #[cfg(test)]
        self.record_owner_lifecycle("admit", "complete", Some(&binding), None);
        let actions = actions.or_else(|| {
            let key = observed
                .as_ref()
                .map(|parent| parent.key.clone())
                .or_else(|| parent.as_ref().and_then(|parent| parent.key().cloned()))?;
            let drive = lane
                .lane
                .active
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_ref()
                .and_then(|(_, drive)| drive.upgrade())?;
            drive.profile_actions(
                &key,
                contract.approved().registry().clone(),
                self.max_payload_bytes(),
            )
        });
        let payload = self
            .owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .request_payload(&binding)
            .cloned()
            .ok_or_else(super::fault)?;
        let result = match actions {
            Some(actions) => {
                actions
                    .execute(
                        ActionInvocation {
                            connection: self.clone(),
                            binding: binding.clone(),
                        },
                        payload,
                    )
                    .await
            }
            None => Err(v1_1::ErrorCode::InvalidParent),
        };
        if let Ok(bytes) = &result {
            contract
                .approved()
                .validate_payload(
                    contract
                        .reference()
                        .profile
                        .as_ref()
                        .expect("selected profile")
                        .minor,
                    &method,
                    pi_extension_protocol::profiles::Stage::Terminal,
                    bytes,
                    pi_extension_protocol::wire::value::Limits::default(),
                    pi_extension_protocol::jcs::Limits::default(),
                    &mut native,
                )
                .map_err(|_| super::fault())?;
        }
        let deadline = self
            .clock
            .now_ms()
            .checked_add(self.timeout_ms)
            .ok_or_else(super::fault)?;
        self.write(
            encode_response(id, result, self.core_minor(), self.max_payload_bytes())?,
            deadline,
        )
        .await?;
        self.retire(&binding, RetireReason::NoLongerExpected)
    }
    async fn serve_passive_request<
        C: Send + Sync + 'static,
        S: pi_agent_session::storage::RuntimeStorage + 'static,
        M: pi_agent_schema::SessionMetadataRecord,
    >(
        self: &Arc<Self>,
        lane: &crate::owner::RuntimeLane<C, S, M>,
        request: v1_1::Request,
    ) -> Result<(), HarnessRuntimeError> {
        use pi_extension_protocol::profiles::wire_v1_1 as dto;
        let capability = request.capability_ref.clone().ok_or_else(super::fault)?;
        let contract = self
            .grant
            .profiles()
            .iter()
            .find(|profile| profile.reference() == &capability)
            .ok_or_else(super::fault)?;
        if !matches!(
            request.method.as_str(),
            "resource.changed" | "event.subscribe" | "event.resync"
        ) {
            return Err(super::fault());
        }
        let method = request.method.clone();
        let id = request.request_id;
        let context = Start {
            capability: capability.clone(),
            method: method.clone(),
            direction: Direction::ExtensionToCore,
            role: Role::Passive,
            key: None,
            parent: None,
            scope: contract.approved().scope().into(),
            payload: vec![],
            now_ms: self.clock.now_ms(),
            requested_deadline_ms: None,
            method_default_ms: self.timeout_ms,
            method_limit_ms: self.timeout_ms,
            initial_credit: Some(0),
            generation: self.grant.wire().connection_generation,
        };
        let policy = pi_ai_core::ModelHeaderPolicy::default();
        let mut native = super::native::NativeDomain::new(lane.owner.session.codec(), &policy);
        let binding = self
            .owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .accept_request(request, context, &mut native)
            .map_err(|_| super::fault())?;
        #[cfg(test)]
        self.record_owner_lifecycle("admit", "complete", Some(&binding), None);
        let payload = self
            .owner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .request_payload(&binding)
            .cloned()
            .ok_or_else(super::fault)?;
        match method.as_str() {
            "resource.changed" => {
                let catalog = {
                    let mut catalogs = self
                        .resource_catalogs
                        .lock()
                        .unwrap_or_else(|p| p.into_inner());
                    catalogs
                        .entry((capability.kind.clone(), capability.name.clone()))
                        .or_insert(
                            super::resources::ResourceCatalog::new(
                                contract.approved().scope().into(),
                            )
                            .map_err(|_| super::fault())?,
                        )
                        .clone()
                };
                catalog
                    .changed(payload.fields["scope"].as_str().ok_or_else(super::fault)?)
                    .map_err(|_| super::fault())?;
                let body = profile_live::encode_payload(
                    &dto::ResourceChangedAck {},
                    self.max_payload_bytes(),
                )
                .map_err(|_| super::fault())?;
                self.write(
                    encode_response(id, Ok(body), self.core_minor(), self.max_payload_bytes())?,
                    self.scope_deadline()?,
                )
                .await?;
                self.retire(&binding, RetireReason::NoLongerExpected)
            }
            "event.resync" => {
                let sub_id = payload.fields["subscription_id"]
                    .as_str()
                    .ok_or_else(super::fault)?;
                let subscription = self
                    .subscriptions
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .get(sub_id)
                    .filter(|(binding, _)| {
                        binding.capability() == &capability
                            && binding.generation() == self.grant.wire().connection_generation
                    })
                    .map(|(_, subscription)| subscription.clone());
                let result = match subscription {
                    Some(subscription) => lane
                        .resync_profile_events(&subscription)
                        .await
                        .map(|snapshot| dto::ResyncResult {
                            snapshot: Some(snapshot),
                        })
                        .map_err(|_| v1_1::ErrorCode::InvalidParent),
                    None => Err(v1_1::ErrorCode::InvalidParent),
                };
                let body = match result {
                    Ok(result) => Ok(profile_live::encode_payload(
                        &result,
                        self.max_payload_bytes(),
                    )
                    .map_err(|_| super::fault())?),
                    Err(error) => Err(error),
                };
                self.write(
                    encode_response(id, body, self.core_minor(), self.max_payload_bytes())?,
                    self.scope_deadline()?,
                )
                .await?;
                self.retire(&binding, RetireReason::NoLongerExpected)
            }
            _ => {
                let limits = self.grant.wire().limits.expect("active limits");
                let subscription = lane
                    .subscribe_profile_events(
                        self,
                        &capability,
                        self.clock.clone(),
                        super::events::EventOptions {
                            queue_bytes: limits.passive_queue_bytes,
                            gap_bytes: 128,
                            deadline_ms: self.scope_deadline()?,
                            max_subscriptions: NonZeroUsize::new(
                                limits.in_flight_requests as usize,
                            )
                            .ok_or_else(super::fault)?,
                        },
                    )
                    .await?;
                let sub_id = subscription.subscription_id().to_owned();
                self.subscriptions
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(sub_id, (binding.clone(), subscription.clone()));
                let connection = self.clone();
                let lane = lane.clone();
                lane.owner
                    .host
                    .clone()
                    .spawn(Box::pin(async move {
                        connection.stream_passive(lane, binding, subscription).await;
                    }))
                    .map_err(|_| super::fault())?;
                Ok(())
            }
        }
    }
    async fn stream_passive<
        C: Send + Sync + 'static,
        S: pi_agent_session::storage::RuntimeStorage + 'static,
        M: pi_agent_schema::SessionMetadataRecord,
    >(
        self: Arc<Self>,
        lane: crate::owner::RuntimeLane<C, S, M>,
        binding: Binding,
        subscription: Arc<super::events::ProjectedEvents>,
    ) {
        let policy = pi_ai_core::ModelHeaderPolicy::default();
        let mut native = super::native::NativeDomain::new(lane.owner.session.codec(), &policy);
        let mut pending: Option<(u64, Vec<u8>)> = None;
        loop {
            let publication = subscription.changed();
            let credit = self
                .passive_changed
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone();
            let open = self
                .owner
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .phase(&binding)
                == Some(profile_live::Phase::Open);
            if !open || !subscription.active() || !self.current() {
                break;
            }
            if pending
                .as_ref()
                .is_some_and(|(epoch, _)| *epoch != subscription.epoch())
            {
                pending = None;
            }
            if pending.is_none() {
                match subscription.take() {
                    Ok(Some(bytes)) => pending = Some((subscription.epoch(), bytes)),
                    Ok(None) => {}
                    Err(_) => break,
                }
            }
            if let Some((_, bytes)) = &pending {
                let item = self
                    .owner
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .host_stream_item(&binding, bytes.clone(), self.clock.now_ms(), &mut native);
                match item {
                    Ok(item) => {
                        let Ok(frame) =
                            encode_stream_item(item, self.core_minor(), self.max_payload_bytes())
                        else {
                            break;
                        };
                        if self.write(frame, subscription.deadline()).await.is_err() {
                            break;
                        }
                        pending = None;
                        continue;
                    }
                    Err(profile_live::Error::Credit) => {}
                    Err(_) => break,
                }
            }
            let mut changed = Box::pin(publication.cancelled());
            let mut credited = Box::pin(credit.cancelled());
            let mut timer = self.clock.sleep_until(subscription.deadline());
            let mut closed = Box::pin(lane.owner.closed.cancelled());
            let stop = poll_fn(|cx| {
                if timer.as_mut().poll(cx).is_ready() || closed.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(true);
                }
                if changed.as_mut().poll(cx).is_ready() || credited.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(false);
                }
                Poll::Pending
            })
            .await;
            if stop {
                break;
            }
        }
        if self.current() {
            let end = pi_extension_protocol::profiles::wire_v1_1::SubscribeEnd {};
            if let Ok(payload) = profile_live::encode_payload(&end, self.max_payload_bytes())
                && let Ok(frame) = encode_stream_end(
                    binding.id(),
                    payload,
                    self.core_minor(),
                    self.max_payload_bytes(),
                )
                && let Ok(deadline) = self.scope_deadline()
            {
                self.write(frame, deadline).await.ok();
            }
        }
        subscription.cancel();
        self.subscriptions
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(subscription.subscription_id());
        self.retire(&binding, RetireReason::NoLongerExpected).ok();
    }
    fn core_minor(&self) -> u32 {
        self.grant
            .wire()
            .selected_protocol
            .as_ref()
            .expect("selected core")
            .minor
    }
    async fn write(
        &self,
        frame: framing::EncodedFrame,
        deadline: u64,
    ) -> Result<(), HarnessRuntimeError> {
        if let Some(inbox) = &self.inbox {
            return inbox.write(self, frame, deadline).await;
        }
        let mut write = self.channel.write_frame(frame);
        let mut timer = Box::pin(self.clock.sleep_until(deadline));
        let result = poll_fn(|cx| {
            if timer.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(()));
            }
            write.as_mut().poll(cx).map(|r| r.map_err(|_| ()))
        })
        .await;
        if result.is_err() {
            self.close();
            return Err(super::fault());
        }
        Ok(())
    }

    fn peer_terminal_error(
        event: Result<Option<pi_extension_protocol::endpoint::ActiveFrame>, PumpError>,
        exact_cancel_ack: bool,
    ) -> HarnessRuntimeError {
        match event {
            Ok(None) if exact_cancel_ack => HarnessRuntimeError::PeerLost,
            _ => super::fault(),
        }
    }
}
struct PumpGuard {
    connection: Arc<ProfileConnection>,
    pump: Option<ConnectionPump>,
    complete: bool,
    id: Option<u64>,
}
impl Drop for PumpGuard {
    fn drop(&mut self) {
        #[cfg(test)]
        {
            let mut event =
                source_observer::SourceEvent::new("cleanup", file!(), line!(), "PumpGuard::drop");
            event.outcome = Some("start");
            event.field = Some("EH_inline_target_provider_allocator");
            event.owner = Some("PumpGuard");
            event.cleanup = Some(false);
            event.request_id = self.id;
            event.generation = Some(self.connection.grant.wire().connection_generation);
            source_observer::record(event);
        }
        if !self.complete
            && let Some(id) = self.id
        {
            let mut owner = self
                .connection
                .owner
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if let Some(binding) = owner.binding(id)
                && owner.receipt(&binding).is_none()
            {
                #[cfg(test)]
                let token =
                    source_observer::ConnectionToken::from_identity(&owner.connection_identity());
                #[cfg(test)]
                let generation = binding.generation();
                #[cfg(test)]
                let retired = owner
                    .retire(&binding, RetireReason::NoLongerExpected)
                    .is_ok();
                #[cfg(not(test))]
                owner.retire(&binding, RetireReason::NoLongerExpected).ok();
                drop(owner);
                #[cfg(test)]
                if retired {
                    let mut event = source_observer::SourceEvent::new(
                        "cleanup",
                        file!(),
                        line!(),
                        "PumpGuard::drop",
                    );
                    event.field = Some("P_req");
                    event.owner = Some("PumpGuard");
                    event.operation = Some("preterminal_cleanup");
                    event.outcome = Some("complete");
                    event.request_id = Some(id);
                    event.generation = Some(generation);
                    event.token = Some(token);
                    source_observer::record(event);
                }
            }
        }
        if let Some(id) = self.id {
            self.connection
                .active_actions
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
            self.connection
                .dispatch_scopes
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&id);
        }
        if self.complete {
            let pump = self.pump.take();
            *self
                .connection
                .pump
                .lock()
                .unwrap_or_else(|p| p.into_inner()) = pump;
        } else {
            self.connection.close();
            drop(self.pump.take());
        }
        #[cfg(test)]
        {
            let mut event =
                source_observer::SourceEvent::new("cleanup", file!(), line!(), "PumpGuard::drop");
            event.outcome = Some("complete");
            event.field = Some("EH_inline_target_provider_allocator");
            event.owner = Some("PumpGuard");
            event.cleanup = Some(true);
            event.request_id = self.id;
            event.generation = Some(self.connection.grant.wire().connection_generation);
            source_observer::record(event);
        }
    }
}
// A single bounded frame stays on the stack rather than allocating another queue object.
#[allow(clippy::large_enum_variant)]
enum ReadOutcome {
    Timeout,
    Cancel,
    Frame(
        Result<
            Option<pi_extension_protocol::endpoint::ActiveFrame>,
            crate::extensions::transport::PumpError,
        >,
    ),
}

pub(crate) fn peer_request(frame: &SelectedEnvelope) -> Option<v1_1::Request> {
    match frame {
        SelectedEnvelope::V1_1(e) => match &e.frame {
            Some(v1_1::envelope::Frame::Request(r)) => Some(r.clone()),
            _ => None,
        },
        SelectedEnvelope::V1_0(e) => match &e.frame {
            Some(v1_0::envelope::Frame::Request(r)) => Some(v1_1::Request {
                request_id: r.request_id,
                parent_request_id: r.parent_request_id,
                capability_ref: r.capability_ref.as_ref().map(|c| v1_1::CapabilityRef {
                    authenticated_extension_id: c.authenticated_extension_id.clone(),
                    kind: c.kind.clone(),
                    name: c.name.clone(),
                    profile: c.profile.as_ref().map(|p| v1_1::ProfileVersion {
                        name: p.name.clone(),
                        major: p.major,
                        minor: p.minor,
                    }),
                    schema_digest_sha256: c.schema_digest_sha256.clone(),
                    handler_id: c.handler_id.clone(),
                }),
                effect_key: r.effect_key.as_ref().map(|k| v1_1::EffectKey {
                    session_id: k.session_id.clone(),
                    lane: k.lane.clone(),
                    operation_id: k.operation_id.clone(),
                    effect_kind: k.effect_kind.clone(),
                    step_or_task_id: k.step_or_task_id.clone(),
                    logical_attempt: k.logical_attempt,
                    item_index: k.item_index,
                }),
                method: r.method.clone(),
                deadline_after_ms: r.deadline_after_ms,
                initial_stream_credit_bytes: r.initial_stream_credit_bytes,
                payload: r.payload.clone(),
            }),
            _ => None,
        },
    }
}
fn encode_request(
    r: &v1_1::Request,
    minor: u32,
    max: usize,
) -> Result<framing::EncodedFrame, HarnessRuntimeError> {
    let cap = NonZeroUsize::new(max).ok_or_else(super::fault)?;
    if minor == 1 {
        framing::encode(
            &v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::Request(r.clone())),
            },
            cap,
        )
        .map_err(|_| super::fault())
    } else {
        let c = r.capability_ref.as_ref().ok_or_else(super::fault)?;
        framing::encode(
            &v1_0::Envelope {
                frame: Some(v1_0::envelope::Frame::Request(v1_0::Request {
                    request_id: r.request_id,
                    parent_request_id: r.parent_request_id,
                    capability_ref: Some(v1_0::CapabilityRef {
                        authenticated_extension_id: c.authenticated_extension_id.clone(),
                        kind: c.kind.clone(),
                        name: c.name.clone(),
                        profile: c.profile.as_ref().map(|p| v1_0::ProfileVersion {
                            name: p.name.clone(),
                            major: p.major,
                            minor: p.minor,
                        }),
                        schema_digest_sha256: c.schema_digest_sha256.clone(),
                        handler_id: c.handler_id.clone(),
                    }),
                    effect_key: r.effect_key.as_ref().map(|k| v1_0::EffectKey {
                        session_id: k.session_id.clone(),
                        lane: k.lane.clone(),
                        operation_id: k.operation_id.clone(),
                        effect_kind: k.effect_kind.clone(),
                        step_or_task_id: k.step_or_task_id.clone(),
                        logical_attempt: k.logical_attempt,
                        item_index: k.item_index,
                    }),
                    method: r.method.clone(),
                    deadline_after_ms: r.deadline_after_ms,
                    initial_stream_credit_bytes: r.initial_stream_credit_bytes,
                    payload: r.payload.clone(),
                })),
            },
            cap,
        )
        .map_err(|_| super::fault())
    }
}
fn encode_cancel(
    id: u64,
    minor: u32,
    max: usize,
) -> Result<framing::EncodedFrame, HarnessRuntimeError> {
    let cap = NonZeroUsize::new(max).ok_or_else(super::fault)?;
    if minor == 1 {
        framing::encode(
            &v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::Cancel(v1_1::Cancel {
                    request_id: id,
                    reason: 1,
                })),
            },
            cap,
        )
        .map_err(|_| super::fault())
    } else {
        framing::encode(
            &v1_0::Envelope {
                frame: Some(v1_0::envelope::Frame::Cancel(v1_0::Cancel {
                    request_id: id,
                    reason: 1,
                })),
            },
            cap,
        )
        .map_err(|_| super::fault())
    }
}
fn encode_response(
    id: u64,
    result: Result<Vec<u8>, v1_1::ErrorCode>,
    minor: u32,
    max: usize,
) -> Result<framing::EncodedFrame, HarnessRuntimeError> {
    let cap = NonZeroUsize::new(max).ok_or_else(super::fault)?;
    if minor == 1 {
        framing::encode(
            &v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::Response(v1_1::Response {
                    request_id: id,
                    terminal: Some(match result {
                        Ok(v) => v1_1::response::Terminal::Result(v),
                        Err(code) => v1_1::response::Terminal::Error(v1_1::TypedError {
                            code: code as i32,
                            safe_message: profile_live::safe_error(code).into(),
                        }),
                    }),
                })),
            },
            cap,
        )
        .map_err(|_| super::fault())
    } else {
        framing::encode(
            &v1_0::Envelope {
                frame: Some(v1_0::envelope::Frame::Response(v1_0::Response {
                    request_id: id,
                    terminal: Some(match result {
                        Ok(v) => v1_0::response::Terminal::Result(v),
                        Err(code) => v1_0::response::Terminal::Error(v1_0::TypedError {
                            code: code as i32,
                            safe_message: profile_live::safe_error(code).into(),
                        }),
                    }),
                })),
            },
            cap,
        )
        .map_err(|_| super::fault())
    }
}
pub(crate) fn key_wire(key: &EffectKey) -> v1_1::EffectKey {
    v1_1::EffectKey {
        session_id: key.session_id.as_str().into(),
        lane: key.lane.as_str().into(),
        operation_id: key.operation_id.as_str().into(),
        effect_kind: match key.kind {
            pi_agent_schema::EffectKind::Assistant => "assistant",
            pi_agent_schema::EffectKind::Hook => "hook",
            pi_agent_schema::EffectKind::Tool => "tool",
            pi_agent_schema::EffectKind::Summary => "summary",
            pi_agent_schema::EffectKind::Deferred => "deferred",
            pi_agent_schema::EffectKind::CancelDeferred => "cancel_deferred",
        }
        .into(),
        step_or_task_id: key.step_or_task_id.as_str().into(),
        logical_attempt: key.logical_attempt.get(),
        item_index: key.item_index.map(|v| v.get()),
    }
}

pub(crate) struct RouteSlot {
    connection: Arc<ProfileConnection>,
    permit: Option<crate::scheduler::MutationPermit>,
}
impl Drop for RouteSlot {
    fn drop(&mut self) {
        self.connection.waiting.fetch_sub(1, Ordering::AcqRel);
        drop(self.permit.take());
    }
}

impl<
    C: Send + Sync + 'static,
    S: pi_agent_session::storage::RuntimeStorage + 'static,
    M: pi_agent_schema::SessionMetadataRecord,
> crate::owner::RuntimeHarness<C, S, M>
{
    /// Opt in to the same Tool connection's selected profiles and sole framed
    /// reader/writer. The consumer is the actual captured native runtime lane.
    // Keep the captured native authority and independent finite route inputs explicit.
    #[allow(clippy::too_many_arguments)]
    pub async fn connect_profile_tool(
        &self,
        owner: &crate::extensions::tool_driver::SingleToolOwner,
        host: HostAuthority,
        channel: Arc<dyn OrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
        reservations: Reservations,
        remaining_ms: u64,
    ) -> Result<
        (
            Arc<ProfileConnection>,
            crate::extensions::tool_driver::ReadyToolRoute,
        ),
        HarnessRuntimeError,
    > {
        let (connection, route) = ProfileConnection::connect_tool(
            owner,
            host,
            channel,
            clock,
            config,
            reservations,
            remaining_ms,
        )
        .await?;
        let inbox = connection.inbox.as_ref().expect("Tool inbox").clone();
        let lane = self.main.clone();
        let weak = Arc::downgrade(&connection);
        self.main
            .owner
            .host
            .spawn(Box::pin(async move {
                loop {
                    let frame = match inbox.next(true).await {
                        Ok(Some(frame)) => frame,
                        _ => break,
                    };
                    let Some(connection) = weak.upgrade() else {
                        break;
                    };
                    let result = connection.serve_peer_frame(&lane, frame).await;
                    if result.is_err() {
                        connection.close();
                        break;
                    }
                }
            }))
            .map_err(|_| {
                connection.close();
                super::fault()
            })?;
        Ok((connection, route))
    }

    /// Host-owned retained-capability ingress for the staged 8192 receive
    /// path. The ordinary Tool entry above remains source-compatible for
    /// channels that advertise only [`OrderedByteChannel`]. This additive
    /// entry preserves the existing pump as the sole reader and carries the
    /// retained capability into the route before ordinary trait erasure.
    #[expect(
        clippy::too_many_arguments,
        reason = "Preserve the frozen retained-ingress public API."
    )]
    pub async fn connect_profile_tool_retained(
        &self,
        owner: &crate::extensions::tool_driver::SingleToolOwner,
        host: HostAuthority,
        channel: Arc<dyn RetainedOrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
        reservations: Reservations,
        remaining_ms: u64,
    ) -> Result<
        (
            Arc<ProfileConnection>,
            crate::extensions::tool_driver::ReadyToolRoute,
        ),
        HarnessRuntimeError,
    > {
        let (connection, route) = ProfileConnection::connect_tool_retained(
            owner,
            host,
            channel,
            clock,
            config,
            reservations,
            remaining_ms,
        )
        .await?;
        let inbox = connection.inbox.as_ref().expect("Tool inbox").clone();
        let lane = self.main.clone();
        let weak = Arc::downgrade(&connection);
        self.main
            .owner
            .host
            .spawn(Box::pin(async move {
                loop {
                    let frame = match inbox.next(true).await {
                        Ok(Some(frame)) => frame,
                        _ => break,
                    };
                    let Some(connection) = weak.upgrade() else {
                        break;
                    };
                    let result = connection.serve_peer_frame(&lane, frame).await;
                    if result.is_err() {
                        connection.close();
                        break;
                    }
                }
            }))
            .map_err(|_| {
                connection.close();
                super::fault()
            })?;
        Ok((connection, route))
    }
}

impl<
    C: Send + Sync + 'static,
    S: pi_agent_session::storage::RuntimeStorage + 'static,
    M: pi_agent_schema::SessionMetadataRecord,
> crate::owner::RuntimeHarness<C, S, M>
{
    pub async fn connect_profile_service(
        &self,
        host: HostAuthority,
        channel: Arc<dyn OrderedByteChannel>,
        clock: Arc<dyn MonotonicClock>,
        config: PumpConfig,
        reservations: Reservations,
        remaining_ms: u64,
    ) -> Result<Arc<ProfileConnection>, HarnessRuntimeError> {
        let pump_channel = Arc::new(DeferredCloseChannel::new(channel.clone()));
        let mut pump = ConnectionPump::new(host, pump_channel.clone(), clock.clone(), config)
            .map_err(|_| super::fault())?;
        let grant = pump.drive_until_ready().await.map_err(|_| super::fault())?;
        pump_channel.arm();
        let max_jobs = grant
            .wire()
            .limits
            .expect("active limits")
            .in_flight_requests as usize;
        let max_bytes = usize::try_from(reservations.blocking_bytes).map_err(|_| super::fault())?;
        let inbox =
            super::tool_route::ToolInbox::new(max_bytes, max_jobs).map_err(|_| super::fault())?;
        let writer =
            super::service::Writer::new(max_bytes, max_jobs).map_err(|_| super::fault())?;
        let owner =
            profile_live::Owner::new(grant.clone(), reservations, clock.now_ms(), remaining_ms)
                .map_err(|_| super::fault())?;
        let connection = Arc::new(ProfileConnection {
            owner: Mutex::new(owner),
            pump: Mutex::new(None),
            channel: channel.clone(),
            clock: clock.clone(),
            grant,
            tool_connection: None,
            tool: None,
            inbox: Some(inbox.clone()),
            service: Some(writer.clone()),
            resource_catalogs: Mutex::new(BTreeMap::new()),
            subscriptions: Mutex::new(BTreeMap::new()),
            passive_changed: Mutex::new(pi_ai_core::CancellationToken::new()),
            active_actions: Mutex::new(BTreeMap::new()),
            dispatch_scopes: Mutex::new(BTreeMap::new()),
            timeout_ms: config.write_timeout_ms.get(),
            slot_line: crate::scheduler::MutationLine::new(crate::scheduler::Admission::new()),
            waiting: AtomicUsize::new(0),
            peer_close: Mutex::new(PeerCloseFence::default()),
        });
        inbox.attach(&connection);
        let weak = Arc::downgrade(&connection);
        let read_inbox = inbox.clone();
        self.main
            .owner
            .host
            .spawn(Box::pin(async move {
                loop {
                    match pump.next_active().await {
                        Ok(Some(frame)) => {
                            if crate::extensions::tool_driver::ToolProfileIngress::incoming(
                                read_inbox.as_ref(),
                                frame,
                            )
                            .is_err()
                            {
                                if let Some(connection) = weak.upgrade() {
                                    connection.close();
                                }
                                break;
                            }
                        }
                        Ok(None) => {
                            if let Some(connection) = weak.upgrade() {
                                connection.request_peer_close();
                            }
                            break;
                        }
                        Err(_) => {
                            if let Some(connection) = weak.upgrade() {
                                connection.close();
                            }
                            break;
                        }
                    }
                }
            }))
            .map_err(|_| {
                connection.close();
                super::fault()
            })?;
        let weak = Arc::downgrade(&connection);
        let writer_inbox = inbox.clone();
        self.main
            .owner
            .host
            .spawn(Box::pin(async move {
                writer.run(weak, writer_inbox, channel, clock).await
            }))
            .map_err(|_| {
                connection.close();
                super::fault()
            })?;
        let weak = Arc::downgrade(&connection);
        let lane = self.main.clone();
        self.main
            .owner
            .host
            .spawn(Box::pin(async move {
                loop {
                    let frame = match inbox.next(true).await {
                        Ok(Some(frame)) => frame,
                        _ => break,
                    };
                    let Some(connection) = weak.upgrade() else {
                        break;
                    };
                    let result = connection.serve_peer_frame(&lane, frame).await;
                    if result.is_err() {
                        connection.close();
                        break;
                    }
                }
            }))
            .map_err(|_| {
                connection.close();
                super::fault()
            })?;
        Ok(connection)
    }
}

fn encode_stream_item(
    item: v1_1::StreamItem,
    minor: u32,
    max: usize,
) -> Result<framing::EncodedFrame, HarnessRuntimeError> {
    let cap = NonZeroUsize::new(max).ok_or_else(super::fault)?;
    if minor == 1 {
        framing::encode(
            &v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::StreamItem(item)),
            },
            cap,
        )
        .map_err(|_| super::fault())
    } else {
        framing::encode(
            &v1_0::Envelope {
                frame: Some(v1_0::envelope::Frame::StreamItem(v1_0::StreamItem {
                    request_id: item.request_id,
                    sequence: item.sequence,
                    payload: item.payload,
                })),
            },
            cap,
        )
        .map_err(|_| super::fault())
    }
}

fn encode_stream_end(
    id: u64,
    payload: Vec<u8>,
    minor: u32,
    max: usize,
) -> Result<framing::EncodedFrame, HarnessRuntimeError> {
    let cap = NonZeroUsize::new(max).ok_or_else(super::fault)?;
    if minor == 1 {
        framing::encode(
            &v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                    request_id: id,
                    terminal: Some(v1_1::stream_end::Terminal::Result(payload)),
                })),
            },
            cap,
        )
        .map_err(|_| super::fault())
    } else {
        framing::encode(
            &v1_0::Envelope {
                frame: Some(v1_0::envelope::Frame::StreamEnd(v1_0::StreamEnd {
                    request_id: id,
                    terminal: Some(v1_0::stream_end::Terminal::Result(payload)),
                })),
            },
            cap,
        )
        .map_err(|_| super::fault())
    }
}

#[cfg(test)]
mod tests {
    use super::{PeerCloseFence, ProfileConnection, peer_close_fence_matches};
    use crate::extensions::transport::{
        ChannelError, ConnectionPump, MonotonicClock, OrderedByteChannel, PumpConfig, PumpError,
    };
    use pi_agent_schema::HarnessRuntimeError;
    use pi_extension_protocol::{
        activation::HostAuthority,
        wire::{
            framing::{self, EncodedFrame},
            v1_1,
        },
    };
    use std::{
        collections::VecDeque,
        future::poll_fn,
        num::{NonZeroU64, NonZeroUsize},
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
        task::{Context, Poll, Waker},
    };

    #[test]
    fn read_write_and_clock_faults_stay_generic_after_cancel() {
        for error in [
            PumpError::Closed,
            PumpError::ReadFailed,
            PumpError::MalformedInput,
            PumpError::TruncatedEof,
            PumpError::ClockFault,
            PumpError::WriteTimeout,
        ] {
            assert!(
                matches!(
                    ProfileConnection::peer_terminal_error(Err(error), true),
                    HarnessRuntimeError::Fault(_)
                ),
                "{error:?}"
            );
        }
        assert!(matches!(
            ProfileConnection::peer_terminal_error(Ok(None), false),
            HarnessRuntimeError::Fault(_)
        ));
        assert!(matches!(
            ProfileConnection::peer_terminal_error(Ok(None), true),
            HarnessRuntimeError::PeerLost
        ));
    }

    #[test]
    fn peer_close_fence_rejects_other_cancel_and_retires_exact_id() {
        let mut fence = PeerCloseFence {
            clean_eof: true,
            deferred_cancel_id: Some(17),
        };
        assert!(peer_close_fence_matches(&fence, 17));
        assert!(!peer_close_fence_matches(&fence, 18));
        fence.deferred_cancel_id = Some(18);
        assert!(!peer_close_fence_matches(&fence, 17));
        assert!(peer_close_fence_matches(&fence, 18));
    }

    struct ProbeChannel {
        reads: Mutex<VecDeque<ProbeRead>>,
        stall_writes: AtomicBool,
        closed: AtomicBool,
        write_wake: Mutex<Option<Waker>>,
    }

    enum ProbeRead {
        Bytes(Vec<u8>),
        Eof,
        Failed,
    }

    impl ProbeChannel {
        fn new(read: ProbeRead) -> Arc<Self> {
            Self::from_reads(VecDeque::from([read]))
        }

        fn from_reads(reads: VecDeque<ProbeRead>) -> Arc<Self> {
            Arc::new(Self {
                reads: Mutex::new(reads),
                stall_writes: AtomicBool::new(false),
                closed: AtomicBool::new(false),
                write_wake: Mutex::new(None),
            })
        }
    }

    impl OrderedByteChannel for ProbeChannel {
        fn max_read_scratch_bytes(&self) -> usize {
            0
        }

        fn read_chunk<'a>(
            &'a self,
            buffer: &'a mut [u8],
        ) -> pi_agent_schema::BoxFuture<'a, Result<Option<usize>, ChannelError>> {
            Box::pin(poll_fn(move |_| {
                match self.reads.lock().unwrap().pop_front() {
                    Some(ProbeRead::Bytes(bytes)) => {
                        buffer[..bytes.len()].copy_from_slice(&bytes);
                        Poll::Ready(Ok(Some(bytes.len())))
                    }
                    Some(ProbeRead::Eof) => Poll::Ready(Ok(None)),
                    Some(ProbeRead::Failed) => Poll::Ready(Err(ChannelError::Failed)),
                    None if self.closed.load(Ordering::SeqCst) => {
                        Poll::Ready(Err(ChannelError::Closed))
                    }
                    None => Poll::Pending,
                }
            }))
        }

        fn write_frame(
            &self,
            _: EncodedFrame,
        ) -> pi_agent_schema::BoxFuture<'_, Result<(), ChannelError>> {
            Box::pin(poll_fn(move |cx| {
                if self.stall_writes.load(Ordering::SeqCst) {
                    *self.write_wake.lock().unwrap() = Some(cx.waker().clone());
                    Poll::Pending
                } else {
                    Poll::Ready(Ok(()))
                }
            }))
        }

        fn close(&self) {
            self.closed.store(true, Ordering::SeqCst);
            if let Some(waker) = self.write_wake.lock().unwrap().take() {
                waker.wake();
            }
        }
    }

    struct ProbeClock(AtomicU64);

    impl MonotonicClock for ProbeClock {
        fn now_ms(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }

        fn sleep_until(&self, deadline: u64) -> pi_agent_schema::BoxFuture<'_, ()> {
            Box::pin(poll_fn(move |_| {
                (self.now_ms() >= deadline)
                    .then_some(())
                    .map_or(Poll::Pending, Poll::Ready)
            }))
        }
    }

    fn poll_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        future.poll(&mut context)
    }

    fn probe_host() -> HostAuthority {
        HostAuthority::new(
            "peer-a".into(),
            7,
            1,
            1,
            v1_1::ProtocolLimits {
                pre_handshake_frame_bytes: 4096,
                frame_bytes: 4096,
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
                handshake_ms: 100,
                cancellation_grace_ms: 10,
                max_stream_credit_bytes: 4096,
            },
            vec![],
            vec![],
        )
        .unwrap()
    }

    fn hello_frame() -> Vec<u8> {
        framing::encode(
            &v1_1::Envelope {
                frame: Some(v1_1::envelope::Frame::Hello(v1_1::Hello {
                    protocol: Some(v1_1::VersionRange {
                        major: 1,
                        min_minor: 1,
                        max_minor: 1,
                    }),
                    supported_profiles: vec![],
                })),
            },
            NonZeroUsize::new(4096).unwrap(),
        )
        .unwrap()
        .bytes
    }

    fn physical_probe_error(reads: VecDeque<ProbeRead>) -> PumpError {
        let channel = ProbeChannel::from_reads(reads);
        let clock = Arc::new(ProbeClock(AtomicU64::new(0)));
        let mut pump = ConnectionPump::new(
            probe_host(),
            channel,
            clock,
            PumpConfig {
                max_read_chunk: NonZeroUsize::new(4096).unwrap(),
                write_timeout_ms: NonZeroU64::new(10).unwrap(),
            },
        )
        .unwrap();
        let mut future = Box::pin(pump.next_active());
        match poll_once(future.as_mut()) {
            Poll::Ready(Err(error)) => error,
            Poll::Ready(Ok(_)) => panic!("probe unexpectedly produced an active frame"),
            Poll::Pending => panic!("probe unexpectedly remained pending"),
        }
    }

    #[test]
    fn physical_transport_negative_probes_remain_generic_faults() {
        for (name, reads) in [
            ("read", VecDeque::from([ProbeRead::Failed])),
            ("eof", VecDeque::from([ProbeRead::Eof])),
            (
                "malformed",
                VecDeque::from([ProbeRead::Bytes(vec![0x02, 0x7a, 0x00])]),
            ),
            (
                "truncated",
                VecDeque::from([ProbeRead::Bytes(vec![0x02, 0x0a]), ProbeRead::Eof]),
            ),
        ] {
            let error = physical_probe_error(reads);
            eprintln!("physical probe {name}: {error:?}");
            assert!(
                matches!(
                    ProfileConnection::peer_terminal_error(Err(error), true),
                    HarnessRuntimeError::Fault(_)
                ),
                "{name}: {error:?}"
            );
        }

        let channel = ProbeChannel::new(ProbeRead::Bytes(hello_frame()));
        channel.stall_writes.store(true, Ordering::SeqCst);
        let clock = Arc::new(ProbeClock(AtomicU64::new(0)));
        let mut pump = ConnectionPump::new(
            probe_host(),
            channel,
            clock.clone(),
            PumpConfig {
                max_read_chunk: NonZeroUsize::new(4096).unwrap(),
                write_timeout_ms: NonZeroU64::new(10).unwrap(),
            },
        )
        .unwrap();
        let mut future = Box::pin(pump.next_active());
        assert!(poll_once(future.as_mut()).is_pending());
        clock.0.store(20, Ordering::SeqCst);
        let error = match poll_once(future.as_mut()) {
            Poll::Ready(Err(error)) => error,
            other => panic!("physical write timeout probe: {other:?}"),
        };
        assert_eq!(error, PumpError::WriteTimeout);
        assert!(matches!(
            ProfileConnection::peer_terminal_error(Err(error), true),
            HarnessRuntimeError::Fault(_)
        ));

        let clock_fault = match ConnectionPump::new(
            probe_host(),
            ProbeChannel::new(ProbeRead::Eof),
            Arc::new(ProbeClock(AtomicU64::new(u64::MAX))),
            PumpConfig {
                max_read_chunk: NonZeroUsize::new(4096).unwrap(),
                write_timeout_ms: NonZeroU64::new(10).unwrap(),
            },
        ) {
            Err(PumpError::ClockFault) => PumpError::ClockFault,
            Ok(_) => panic!("clock overflow unexpectedly constructed a pump"),
            Err(error) => panic!("unexpected clock probe error: {error:?}"),
        };
        assert_eq!(clock_fault, PumpError::ClockFault);
        assert!(matches!(
            ProfileConnection::peer_terminal_error(Err(clock_fault), true),
            HarnessRuntimeError::Fault(_)
        ));
    }
}
