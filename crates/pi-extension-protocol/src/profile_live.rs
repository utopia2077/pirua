//! Finite process-local six-profile invocation and passive component owners.
//!
//! Logical queue/receipt bounds are component contracts, not allocation or
//! positive-credit certificates. This module has no Session or Effects authority.
use crate::activation::{ActiveGrant, GrantedProfile};
use crate::endpoint::{ActiveFrame, SelectedEnvelope};
use crate::jcs;
use crate::profiles::wire_v1_1 as v1_1;
use crate::profiles::{self, Direction, Mode, NativeValidator, Stage, ValidatedPayload};
use crate::wire::value;
use prost::Message;
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    HostContract,
    Stale,
    Generation,
    Reference,
    Parent,
    Forbidden,
    SecretBrokerUnavailable,
    UnsupportedMethod,
    Overloaded,
    Deadline,
    Sequence,
    Credit,
    Variant,
    Frame,
    Validation(profiles::Error),
}
impl From<profiles::Error> for Error {
    fn from(e: profiles::Error) -> Self {
        Self::Validation(e)
    }
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "profile owner: {self:?}")
    }
}
impl std::error::Error for Error {}
type Result<T> = std::result::Result<T, Error>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Planned,
    Inline,
    Preaccept,
    HostAction,
    Passive,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Open,
    CancelRequested,
    TerminalReceived,
    Abandoned,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetireReason {
    Consumed,
    NoLongerExpected,
}

/// Opaque process-local identity of one physical connection. IDs, principal
/// and generation tuples do not distinguish independent owners.
#[derive(Clone, Debug)]
pub struct ConnectionIdentity(Arc<()>);
impl ConnectionIdentity {
    pub fn new() -> Self {
        Self(Arc::new(()))
    }
    pub fn same_connection(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Default for ConnectionIdentity {
    fn default() -> Self {
        Self::new()
    }
}
/// Trusted actual connection owner serializes every tool/profile/peer ID.
pub trait ConnectionIds: Send + Sync {
    fn connection(&self) -> ConnectionIdentity;
    fn reserve_request_id(&self, now_ms: u64, requested: Option<u64>, minimum: u64) -> Result<u64>;
}
/// Facts read from an actual host-owned parent, never from a peer Request.
#[derive(Clone, Debug)]
pub struct ParentObservation {
    pub connection: ConnectionIdentity,
    pub principal: String,
    pub generation: u64,
    pub request_id: u64,
    pub key: v1_1::EffectKey,
    pub deadline_ms: u64,
    pub depth: u32,
}
/// A trusted host implementation retains the opaque parent owner and binding.
/// `observe` must synchronously recheck that owner's exact live identity and
/// phase under its state line. It cannot perform I/O, wait, or reenter this owner.
pub trait ParentCustody: Send + Sync {
    fn observe(&self, now_ms: u64) -> Result<ParentObservation>;
    /// Reserve from the actual connection's shared ID allocator, under the
    /// same owner state line as a fresh parent liveness check. Failed request
    /// admission may consume an ID; it must never make that ID reusable.
    fn reserve_request_id(
        &self,
        _now_ms: u64,
        _requested: Option<u64>,
        _minimum: u64,
    ) -> Result<u64> {
        Err(Error::HostContract)
    }
}
/// Process-local custody for a parent owned by the existing tool route.
/// A plain ID/key tuple cannot construct this value without a trusted owner.
#[derive(Clone)]
pub struct ExternalParent(Arc<dyn ParentCustody>);
impl ExternalParent {
    pub fn from_owner(owner: Arc<dyn ParentCustody>) -> Self {
        Self(owner)
    }
    pub fn observation(&self, now_ms: u64) -> Result<ParentObservation> {
        self.0.observe(now_ms)
    }
    fn observe(&self, now_ms: u64) -> Result<ParentObservation> {
        self.0.observe(now_ms)
    }
}

/// Runtime-only native effect/accepted-operation custody. The implementation
/// retains the actual runtime owner and captured effect or operation identity;
/// current Session state is checked by runtime before this synchronous seam.
pub trait NativeCustody: Send + Sync {
    fn observe(&self, now_ms: u64) -> Result<NativeObservation>;
    fn reserve_request_id(
        &self,
        _now_ms: u64,
        _requested: Option<u64>,
        _minimum: u64,
    ) -> Result<Option<u64>> {
        Ok(None)
    }
}
#[derive(Clone, Debug)]
pub struct NativeObservation {
    pub connection: Option<ConnectionIdentity>,
    pub key: Option<v1_1::EffectKey>,
    pub parent_request_id: Option<u64>,
    pub deadline_ms: u64,
}
#[derive(Clone)]
pub struct NativeParent(Arc<dyn NativeCustody>);
impl NativeParent {
    pub fn from_runtime(owner: Arc<dyn NativeCustody>) -> Self {
        Self(owner)
    }
}

/// Encode an existing canonical selected DTO only after its finite host bound
/// has been checked. This helper conveys no grant or receive resource approval.
pub fn encode_payload<M: Message>(message: &M, max_bytes: usize) -> Result<Vec<u8>> {
    let size = message.encoded_len();
    if max_bytes == 0 || max_bytes == usize::MAX || size > max_bytes {
        return Err(Error::Frame);
    }
    Ok(message.encode_to_vec())
}
struct BindingCore {
    capability: v1_1::CapabilityRef,
    generation: u64,
    id: u64,
    key: Option<v1_1::EffectKey>,
    role: Role,
    parent: Option<u64>,
}
#[derive(Clone)]
pub struct Binding(Arc<BindingCore>);
impl std::fmt::Debug for Binding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Binding")
            .field("generation", &self.generation())
            .field("id", &self.id())
            .finish_non_exhaustive()
    }
}
impl Binding {
    pub fn same_identity(&self, b: &Self) -> bool {
        Arc::ptr_eq(&self.0, &b.0)
    }
    pub fn capability(&self) -> &v1_1::CapabilityRef {
        &self.0.capability
    }
    pub fn generation(&self) -> u64 {
        self.0.generation
    }
    pub fn id(&self) -> u64 {
        self.0.id
    }
    pub fn key(&self) -> Option<&v1_1::EffectKey> {
        self.0.key.as_ref()
    }
    pub fn role(&self) -> Role {
        self.0.role
    }
    pub fn parent(&self) -> Option<u64> {
        self.0.parent
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalType {
    Response,
    StreamEnd,
}
/// Only the owner creates receipts after shape, exposure and native admission.
pub struct Receipt {
    binding: Binding,
    kind: TerminalType,
    payload: Option<ValidatedPayload>,
    error: Option<v1_1::ErrorCode>,
    received_ms: u64,
}
impl Receipt {
    pub fn binding(&self) -> &Binding {
        &self.binding
    }
    pub fn terminal_type(&self) -> TerminalType {
        self.kind
    }
    pub fn payload(&self) -> Option<&ValidatedPayload> {
        self.payload.as_ref()
    }
    pub fn error(&self) -> Option<v1_1::ErrorCode> {
        self.error
    }
    pub fn received_ms(&self) -> u64 {
        self.received_ms
    }
}
/// Host-admitted finite component reservations. No universal default resource policy.
#[derive(Clone, Copy, Debug)]
pub struct Reservations {
    pub blocking_bytes: u64,
    pub control_bytes: u64,
    pub terminal_bytes: u64,
    pub progress_bytes: u64,
    pub passive_bytes: u64,
    pub telemetry_bytes: u64,
}
/// Host call context; keys are the actual existing planned key, when one exists.
pub struct Start {
    pub capability: v1_1::CapabilityRef,
    pub method: String,
    pub direction: Direction,
    pub role: Role,
    pub key: Option<v1_1::EffectKey>,
    pub parent: Option<Binding>,
    pub scope: String,
    pub payload: Vec<u8>,
    pub now_ms: u64,
    pub requested_deadline_ms: Option<u64>,
    pub method_default_ms: u64,
    pub method_limit_ms: u64,
    pub initial_credit: Option<u64>,
    pub generation: u64,
}
struct Invocation {
    binding: Binding,
    contract: GrantedProfile,
    method: String,
    mode: Mode,
    request: ValidatedPayload,
    phase: Phase,
    deadline: u64,
    credit: u64,
    sequence: u64,
    receipt: Option<Receipt>,
    blocking: u64,
}
pub enum Accepted {
    Progress(ValidatedPayload),
    Suppressed,
    Terminal,
    Cancelled,
    Credited,
}
/// Owns finite live bindings and reserved receipt/control capacity across transport loss.
pub struct Owner {
    grant: ActiveGrant,
    connection: ConnectionIdentity,
    ids: Option<Arc<dyn ConnectionIds>>,
    limits: v1_1::ProtocolLimits,
    reservations: Reservations,
    calls: BTreeMap<u64, Invocation>,
    denials: BTreeSet<u64>,
    external_parents: BTreeMap<u64, ExternalParent>,
    native_parents: BTreeMap<u64, NativeParent>,
    next_id: u64,
    blocking: u64,
    progress: u64,
    last_ms: u64,
    connection_deadline: u64,
    draining: bool,
    disabled: BTreeSet<(String, String)>,
    rate_window: u64,
    rate_count: u32,
}
impl Owner {
    pub fn new(
        grant: ActiveGrant,
        reservations: Reservations,
        now_ms: u64,
        connection_remaining_ms: u64,
    ) -> Result<Self> {
        Self::new_with_connection(
            grant,
            reservations,
            now_ms,
            connection_remaining_ms,
            ConnectionIdentity::new(),
        )
    }
    pub fn new_with_connection(
        grant: ActiveGrant,
        reservations: Reservations,
        now_ms: u64,
        connection_remaining_ms: u64,
        connection: ConnectionIdentity,
    ) -> Result<Self> {
        let limits = grant.wire().limits.ok_or(Error::HostContract)?;
        let seats = u64::from(limits.in_flight_requests);
        let receipt = limits
            .frame_bytes
            .checked_mul(seats)
            .ok_or(Error::HostContract)?;
        if connection_remaining_ms == 0
            || connection_remaining_ms == u64::MAX
            || reservations.control_bytes < receipt
            || reservations.terminal_bytes < receipt
            || reservations.blocking_bytes == 0
            || reservations.blocking_bytes > limits.blocking_queue_bytes
            || reservations.progress_bytes > limits.progress_queue_bytes
            || reservations.passive_bytes > limits.passive_queue_bytes
            || reservations.telemetry_bytes > limits.telemetry_queue_bytes
        {
            return Err(Error::HostContract);
        }
        Ok(Self {
            grant,
            connection,
            ids: None,
            limits,
            reservations,
            calls: BTreeMap::new(),
            denials: BTreeSet::new(),
            external_parents: BTreeMap::new(),
            native_parents: BTreeMap::new(),
            next_id: 1,
            blocking: 0,
            progress: 0,
            last_ms: now_ms,
            connection_deadline: now_ms
                .checked_add(connection_remaining_ms)
                .ok_or(Error::HostContract)?,
            draining: false,
            disabled: BTreeSet::new(),
            rate_window: now_ms,
            rate_count: 0,
        })
    }
    pub fn with_connection_ids(mut self, ids: Arc<dyn ConnectionIds>) -> Result<Self> {
        if !ids.connection().same_connection(&self.connection) {
            return Err(Error::HostContract);
        }
        self.ids = Some(ids);
        Ok(self)
    }
    pub fn connection_identity(&self) -> ConnectionIdentity {
        self.connection.clone()
    }
    /// Reserve a fixed no-work denial before its response allocation. This
    /// host control ticket conveys no payload/native/durability authority.
    pub fn reserve_denial(&mut self, id: u64, generation: u64, now: u64) -> Result<()> {
        self.sample(now)?;
        if self.draining || generation != self.grant.wire().connection_generation {
            return Err(Error::Generation);
        }
        let elapsed = now.checked_sub(self.rate_window).ok_or(Error::Deadline)?;
        let count = if elapsed >= 1000 { 0 } else { self.rate_count };
        if id < self.next_id
            || id == 0
            || id == u64::MAX
            || self.calls.contains_key(&id)
            || self.denials.contains(&id)
        {
            return Err(Error::Stale);
        }
        if self.calls.len() + self.denials.len() >= self.limits.in_flight_requests as usize
            || count >= self.limits.requests_per_second
        {
            return Err(Error::Overloaded);
        }
        if now >= self.connection_deadline {
            return Err(Error::Deadline);
        }
        if let Some(ids) = &self.ids
            && ids.reserve_request_id(now, Some(id), self.next_id)? != id
        {
            return Err(Error::Stale);
        }
        self.denials.insert(id);
        self.next_id = id + 1;
        self.rate_count = count + 1;
        if elapsed >= 1000 {
            self.rate_window = now;
        }
        Ok(())
    }
    pub fn denial_live(&self, id: u64, now: u64) -> bool {
        self.denials.contains(&id) && !self.draining && now < self.connection_deadline
    }
    pub fn retire_denial(&mut self, id: u64) -> Result<()> {
        if self.denials.remove(&id) {
            Ok(())
        } else {
            Err(Error::Stale)
        }
    }
    pub fn binding(&self, id: u64) -> Option<Binding> {
        self.calls.get(&id).map(|call| call.binding.clone())
    }
    fn sample(&mut self, now: u64) -> Result<()> {
        if now < self.last_ms {
            return Err(Error::Deadline);
        }
        self.last_ms = now;
        Ok(())
    }
    pub fn phase(&self, b: &Binding) -> Option<Phase> {
        self.calls
            .get(&b.id())
            .filter(|c| c.binding.same_identity(b))
            .map(|c| c.phase)
    }
    pub fn receipt(&self, b: &Binding) -> Option<&Receipt> {
        self.calls
            .get(&b.id())
            .filter(|c| c.binding.same_identity(b))?
            .receipt
            .as_ref()
    }
    pub fn occupancy(&self) -> usize {
        self.calls.len()
    }
    pub fn draining(&self) -> bool {
        self.draining
    }
    pub fn capability_disabled(&self, capability: &v1_1::CapabilityRef) -> bool {
        self.disabled
            .contains(&(capability.kind.clone(), capability.name.clone()))
    }
    pub fn next_id(&self) -> u64 {
        self.next_id
    }
    fn bound_limits(&self) -> value::Limits {
        value::Limits {
            depth: self.limits.value_depth as usize,
            elements: self.limits.value_elements as usize,
            string_bytes: self.limits.string_bytes as usize,
            bytes_length: self.limits.bytes_length as usize,
            decoded_bytes: self.limits.aggregate_decoded_bytes as usize,
            json_bytes: self.limits.aggregate_decoded_bytes as usize,
        }
    }
    fn json_limits(&self) -> jcs::Limits {
        let n = self.limits.aggregate_decoded_bytes as usize;
        jcs::Limits {
            input_bytes: self.limits.frame_bytes as usize,
            output_bytes: n,
            decoded_bytes: n,
            depth: self.limits.value_depth as usize,
            nodes: self.limits.value_elements as usize,
            elements: self.limits.value_elements as usize,
            schema_work: n.saturating_mul(8),
            instance_work: n.saturating_mul(8),
        }
    }
    fn parent_deadline(
        &self,
        parent: Option<&Binding>,
        role: Role,
        now: u64,
    ) -> Result<Option<u64>> {
        if parent.is_none() {
            return if matches!(role, Role::HostAction | Role::Inline) {
                Err(Error::Parent)
            } else {
                Ok(None)
            };
        }
        let p = parent.expect("present");
        let mut cursor = Some(p.id());
        let mut depth = 1;
        let mut deadline = None;
        while let Some(id) = cursor {
            depth += 1;
            if depth > 8 || depth > self.limits.nesting_depth {
                return Err(Error::Parent);
            }
            let c = self.calls.get(&id).ok_or(Error::Parent)?;
            if !matches!(c.phase, Phase::Open)
                || c.binding.generation() != self.grant.wire().connection_generation
                || c.binding.capability().authenticated_extension_id
                    != self.grant.wire().authenticated_extension_id
                || now >= c.deadline
            {
                return Err(Error::Parent);
            }
            if id == p.id() && !c.binding.same_identity(p) {
                return Err(Error::Parent);
            }
            deadline = Some(deadline.map_or(c.deadline, |d: u64| d.min(c.deadline)));
            if let Some(parent) = self.external_parents.get(&id) {
                let p = self.checked_external(parent, now)?;
                if depth
                    .checked_add(p.depth)
                    .is_none_or(|d| d > 8 || d > self.limits.nesting_depth)
                {
                    return Err(Error::Parent);
                }
                deadline = Some(deadline.expect("current deadline").min(p.deadline_ms));
                break;
            }
            if let Some(parent) = self.native_parents.get(&id) {
                let observed = parent.0.observe(now)?;
                if observed.key != c.binding.key().cloned() {
                    return Err(Error::Parent);
                }
                deadline = Some(
                    deadline
                        .expect("current deadline")
                        .min(observed.deadline_ms),
                );
                break;
            }
            cursor = c.binding.parent();
        }
        Ok(deadline)
    }
    pub fn start(
        &mut self,
        start: Start,
        native: &mut impl NativeValidator,
    ) -> Result<(Binding, v1_1::Request)> {
        self.start_with_id(start, None, None, None, native)
    }
    fn checked_external(&self, parent: &ExternalParent, now: u64) -> Result<ParentObservation> {
        let p = parent.observe(now)?;
        if !p.connection.same_connection(&self.connection)
            || p.principal != self.grant.wire().authenticated_extension_id
            || p.generation != self.grant.wire().connection_generation
            || p.request_id == 0
            || p.request_id == u64::MAX
            || p.depth == 0
            || p.depth >= 8
            || p.depth >= self.limits.nesting_depth
            || now >= p.deadline_ms
            || self.calls.contains_key(&p.request_id)
        {
            return Err(Error::Parent);
        }
        Ok(p)
    }
    /// Additive tool-parent entry; existing Start callers and profile parents
    /// retain their original semantics. The parent is rechecked at dispatch.
    pub fn start_with_external_parent(
        &mut self,
        start: Start,
        parent: ExternalParent,
        native: &mut impl NativeValidator,
    ) -> Result<(Binding, v1_1::Request)> {
        self.start_with_id(start, None, Some(parent), None, native)
    }
    /// Runtime-only inline or unit before_resume anchor. It cannot authorize a
    /// peer-originated HostAction or create an independent durable Hook intent.
    pub fn start_with_native_parent(
        &mut self,
        start: Start,
        parent: NativeParent,
        native: &mut impl NativeValidator,
    ) -> Result<(Binding, v1_1::Request)> {
        self.start_with_id(start, None, None, Some(parent), native)
    }
    pub fn request_payload(&self, binding: &Binding) -> Option<&ValidatedPayload> {
        self.calls
            .get(&binding.id())
            .filter(|c| c.binding.same_identity(binding))
            .map(|c| &c.request)
    }
    /// Peer IDs are accepted only after all gates and cannot reuse any earlier ID.
    pub fn accept_request(
        &mut self,
        request: v1_1::Request,
        mut context: Start,
        native: &mut impl NativeValidator,
    ) -> Result<Binding> {
        if request.request_id < self.next_id || request.request_id == u64::MAX {
            self.close_malformed();
            return Err(Error::Stale);
        }
        if request.capability_ref.as_ref() != Some(&context.capability)
            || request.effect_key != context.key
        {
            self.close_malformed();
            return Err(Error::Reference);
        }
        if request.parent_request_id != context.parent.as_ref().map(Binding::id) {
            return Err(Error::Parent);
        }
        context.method = request.method;
        context.payload = request.payload;
        context.requested_deadline_ms = request.deadline_after_ms;
        context.initial_credit = request.initial_stream_credit_bytes;
        let result = self
            .start_with_id(context, Some(request.request_id), None, None, native)
            .map(|(b, _)| b);
        if result.as_ref().is_err_and(|error| {
            matches!(
                error,
                Error::Reference
                    | Error::Validation(
                        profiles::Error::Projection
                            | profiles::Error::Shape
                            | profiles::Error::Exposure
                            | profiles::Error::Variant
                            | profiles::Error::Limit
                    )
            )
        }) {
            self.close_malformed();
        }
        result
    }
    fn close_malformed(&mut self) {
        self.draining = true;
        for call in self.calls.values_mut() {
            if call.phase != Phase::TerminalReceived {
                call.phase = Phase::Abandoned;
            }
        }
    }
    /// Admit the actual peer Request against opaque external Tool custody.
    /// Its wire parent ID is checked against that owner; the peer cannot mint
    /// custody by supplying the ID or an EffectKey tuple.
    pub fn accept_request_with_external_parent(
        &mut self,
        request: v1_1::Request,
        mut context: Start,
        parent: ExternalParent,
        native: &mut impl NativeValidator,
    ) -> Result<Binding> {
        let observed = self.checked_external(&parent, context.now_ms)?;
        if request.request_id == 0
            || request.request_id == u64::MAX
            || context.direction != Direction::ExtensionToCore
            || context.role != Role::HostAction
            || request.parent_request_id != Some(observed.request_id)
            || request.effect_key != context.key
            || request.capability_ref.as_ref() != Some(&context.capability)
            || request.method != context.method
        {
            return Err(Error::Parent);
        }
        context.parent = None;
        context.payload = request.payload;
        context.requested_deadline_ms = request.deadline_after_ms;
        context.initial_credit = request.initial_stream_credit_bytes;
        self.start_with_id(
            context,
            Some(request.request_id),
            Some(parent),
            None,
            native,
        )
        .map(|(binding, _)| binding)
    }
    fn start_with_id(
        &mut self,
        start: Start,
        peer_id: Option<u64>,
        external: Option<ExternalParent>,
        native_parent: Option<NativeParent>,
        native: &mut impl NativeValidator,
    ) -> Result<(Binding, v1_1::Request)> {
        self.sample(start.now_ms)?;
        if start.method == "secret.resolve" {
            return Err(Error::SecretBrokerUnavailable);
        }
        if start.generation != self.grant.wire().connection_generation || self.draining {
            return Err(Error::Generation);
        }
        let contract = self
            .grant
            .profiles()
            .iter()
            .find(|p| p.reference() == &start.capability)
            .ok_or(Error::Reference)?
            .clone();
        if self.capability_disabled(&start.capability) {
            return Err(Error::Forbidden);
        }
        let (direction, mode) = contract
            .approved()
            .registry()
            .method(contract.approved().profile(), &start.method)
            .ok_or(Error::UnsupportedMethod)?;
        if direction != start.direction {
            return Err(Error::Reference);
        }
        let external_observation = external
            .as_ref()
            .map(|p| self.checked_external(p, start.now_ms))
            .transpose()?;
        let native_observation = native_parent
            .as_ref()
            .map(|p| p.0.observe(start.now_ms))
            .transpose()?;
        if let Some(parent) = &native_observation
            && (parent
                .connection
                .as_ref()
                .is_some_and(|connection| !connection.same_connection(&self.connection))
                || external.is_some()
                || start.parent.is_some()
                || start.direction != Direction::CoreToExtension
                || contract.approved().profile() != "hook.v1"
                || parent.deadline_ms <= start.now_ms
                || parent.key != start.key
                || !(start.role == Role::Inline && start.key.is_some()
                    || start.role == Role::Preaccept
                        && start.key.is_none()
                        && start.method == "before_resume"))
        {
            return Err(Error::Parent);
        }
        if external.is_some()
            && (start.parent.is_some() || !matches!(start.role, Role::HostAction | Role::Inline))
        {
            return Err(Error::Parent);
        }
        let parent_deadline = match (&external_observation, &native_observation) {
            (Some(parent), _) => Some(parent.deadline_ms),
            (_, Some(parent)) => Some(parent.deadline_ms),
            _ => self.parent_deadline(start.parent.as_ref(), start.role, start.now_ms)?,
        };
        let role_valid = if native_observation.is_some() {
            true
        } else if start.role == Role::Inline && external_observation.is_some() {
            contract.approved().profile() == "hook.v1"
                && start.key.as_ref() == external_observation.as_ref().map(|p| &p.key)
        } else {
            valid_role(&start, contract.approved().profile())
        };
        if !role_valid {
            return Err(Error::Reference);
        }
        if contract.approved().scope() != start.scope
            || !contract.approved().permissions().iter().all(|(n, s)| {
                self.grant
                    .wire()
                    .permissions
                    .iter()
                    .any(|p| &p.name == n && &p.scope == s)
            })
            || required_permission(&start.method).is_some_and(|permission| {
                !self
                    .grant
                    .wire()
                    .permissions
                    .iter()
                    .any(|p| p.name == permission && p.scope == start.scope)
            })
        {
            return Err(Error::Forbidden);
        }
        let minor = start
            .capability
            .profile
            .as_ref()
            .ok_or(Error::Reference)?
            .minor;
        if start.method_default_ms == 0
            || start.method_limit_ms == 0
            || start.method_limit_ms == u64::MAX
        {
            return Err(Error::HostContract);
        }
        let relative = start
            .requested_deadline_ms
            .unwrap_or(start.method_default_ms)
            .min(start.method_limit_ms);
        let deadline = start
            .now_ms
            .checked_add(relative)
            .ok_or(Error::Deadline)?
            .min(self.connection_deadline)
            .min(parent_deadline.unwrap_or(u64::MAX));
        if deadline <= start.now_ms {
            return Err(Error::Deadline);
        }
        let credit = start.initial_credit.unwrap_or(0);
        if credit > self.limits.max_stream_credit_bytes || mode == Mode::Unary && credit != 0 {
            return Err(Error::Credit);
        }
        let first_id = self.next_id;
        let id = if let Some(parent) = &external {
            parent
                .0
                .reserve_request_id(start.now_ms, peer_id, first_id)?
        } else if let Some(parent) = &native_parent {
            match parent
                .0
                .reserve_request_id(start.now_ms, peer_id, first_id)?
            {
                Some(id) => id,
                None => match &self.ids {
                    Some(ids) => ids.reserve_request_id(start.now_ms, peer_id, first_id)?,
                    None => peer_id.unwrap_or(first_id),
                },
            }
        } else if let Some(ids) = &self.ids {
            ids.reserve_request_id(start.now_ms, peer_id, first_id)?
        } else {
            peer_id.unwrap_or(first_id)
        };
        if id < first_id {
            return Err(Error::Stale);
        }
        let next = id.checked_add(1).ok_or(Error::Overloaded)?;
        let request = v1_1::Request {
            request_id: id,
            parent_request_id: external_observation
                .as_ref()
                .map(|p| p.request_id)
                .or_else(|| {
                    native_observation
                        .as_ref()
                        .and_then(|p| p.parent_request_id)
                })
                .or_else(|| start.parent.as_ref().map(Binding::id)),
            capability_ref: Some(start.capability.clone()),
            effect_key: start.key.clone(),
            method: start.method.clone(),
            deadline_after_ms: start.requested_deadline_ms,
            initial_stream_credit_bytes: start.initial_credit,
            payload: start.payload,
        };
        let bytes = v1_1::Envelope {
            frame: Some(v1_1::envelope::Frame::Request(request.clone())),
        }
        .encoded_len() as u64;
        let charge = self.blocking.checked_add(bytes).ok_or(Error::Overloaded)?;
        let window = start
            .now_ms
            .checked_sub(self.rate_window)
            .ok_or(Error::Deadline)?;
        let count = if window >= 1000 { 0 } else { self.rate_count };
        if bytes > self.limits.frame_bytes
            || charge > self.reservations.blocking_bytes
            || self.calls.len() + self.denials.len() >= self.limits.in_flight_requests as usize
            || count >= self.limits.requests_per_second
        {
            return Err(Error::Overloaded);
        }
        // Shape and exposure validation does no consumer work; admission reservations
        // have already been checked before the trusted native callback is invoked.
        struct AdmissionOnly;
        impl NativeValidator for AdmissionOnly {
            fn validate(
                &mut self,
                _: &ValidatedPayload,
            ) -> std::result::Result<(), profiles::NativeError> {
                Ok(())
            }
        }
        let admitted = contract.approved().validate_payload(
            minor,
            &start.method,
            Stage::Request,
            &request.payload,
            self.bound_limits(),
            self.json_limits(),
            &mut AdmissionOnly,
        )?;
        if admitted
            .fields
            .get("scope")
            .is_some_and(|s| s != &start.scope)
        {
            return Err(Error::Forbidden);
        }
        native.validate(&admitted).map_err(|e| {
            Error::Validation(match e {
                profiles::NativeError::Rejected => profiles::Error::NativeRejected,
                profiles::NativeError::LocalFault => profiles::Error::HostLocalFault,
            })
        })?;
        let binding = Binding(Arc::new(BindingCore {
            capability: start.capability,
            generation: start.generation,
            id,
            key: start.key,
            role: start.role,
            parent: request.parent_request_id,
        }));
        self.calls.insert(
            id,
            Invocation {
                binding: binding.clone(),
                contract,
                method: start.method,
                mode,
                request: admitted,
                phase: Phase::Open,
                deadline,
                credit,
                sequence: 0,
                receipt: None,
                blocking: bytes,
            },
        );
        if let Some(parent) = external {
            self.external_parents.insert(id, parent);
        }
        if let Some(parent) = native_parent {
            self.native_parents.insert(id, parent);
        }
        self.next_id = next;
        self.blocking = charge;
        if window >= 1000 {
            self.rate_window = start.now_ms;
        }
        self.rate_count = count + 1;
        Ok((binding, request))
    }
    /// Release the finite blocking reservation only after transport/worker intake.
    pub fn dispatched(&mut self, b: &Binding) -> Result<()> {
        if let Some(parent) = self.native_parents.get(&b.id()) {
            let observed = parent.0.observe(self.last_ms)?;
            if observed.key.as_ref() != b.key() || observed.deadline_ms <= self.last_ms {
                return Err(Error::Parent);
            }
        }
        if let Some(parent) = self.external_parents.get(&b.id()) {
            self.checked_external(parent, self.last_ms)?;
        }
        let call = self
            .calls
            .get(&b.id())
            .filter(|c| c.binding.same_identity(b))
            .ok_or(Error::Stale)?;
        if call.phase != Phase::Open || self.last_ms >= call.deadline {
            return Err(Error::Deadline);
        }
        if let Some(parent) = call.binding.parent().filter(|_| {
            !self.external_parents.contains_key(&b.id())
                && !self.native_parents.contains_key(&b.id())
        }) {
            let p = self
                .calls
                .get(&parent)
                .ok_or(Error::Parent)?
                .binding
                .clone();
            self.parent_deadline(Some(&p), call.binding.role(), self.last_ms)?;
        }
        let c = self
            .calls
            .get_mut(&b.id())
            .filter(|c| c.binding.same_identity(b))
            .ok_or(Error::Stale)?;
        self.blocking = self
            .blocking
            .checked_sub(c.blocking)
            .ok_or(Error::HostContract)?;
        c.blocking = 0;
        Ok(())
    }
    /// Actual dispatch samples the host's monotonic clock again.
    pub fn dispatched_at(&mut self, b: &Binding, now_ms: u64) -> Result<()> {
        self.expire(now_ms)?;
        self.dispatched(b)
    }
    /// Validate and charge host-egress stream bytes for the actual admitted
    /// peer-origin passive request. This does not grant ingress credit.
    pub fn host_stream_item(
        &mut self,
        binding: &Binding,
        payload: Vec<u8>,
        now: u64,
        native: &mut impl NativeValidator,
    ) -> Result<v1_1::StreamItem> {
        self.expire(now)?;
        let limits = self.bound_limits();
        let json_limits = self.json_limits();
        let call = self
            .calls
            .get_mut(&binding.id())
            .filter(|call| call.binding.same_identity(binding))
            .ok_or(Error::Stale)?;
        if call.binding.role() != Role::Passive
            || call.method != "event.subscribe"
            || call.mode != Mode::Stream
            || call.phase != Phase::Open
        {
            return Err(Error::Reference);
        }
        if payload.len() > self.limits.frame_bytes as usize {
            return Err(Error::Frame);
        }
        let item = v1_1::StreamItem {
            request_id: binding.id(),
            sequence: call.sequence,
            payload,
        };
        let envelope = v1_1::Envelope {
            frame: Some(v1_1::envelope::Frame::StreamItem(item)),
        };
        let bytes = envelope.encoded_len() as u64;
        if bytes > self.limits.frame_bytes || bytes > call.credit {
            return Err(Error::Credit);
        }
        let Some(v1_1::envelope::Frame::StreamItem(item)) = envelope.frame else {
            return Err(Error::Frame);
        };
        call.contract.approved().validate_payload(
            binding
                .capability()
                .profile
                .as_ref()
                .ok_or(Error::Reference)?
                .minor,
            &call.method,
            Stage::Progress,
            &item.payload,
            limits,
            json_limits,
            native,
        )?;
        call.credit -= bytes;
        call.sequence = call.sequence.checked_add(1).ok_or(Error::Sequence)?;
        Ok(item)
    }
    pub fn receive(
        &mut self,
        generation: u64,
        frame: ActiveFrame,
        now: u64,
        native: &mut impl NativeValidator,
    ) -> Result<Accepted> {
        self.receive_checked(generation, frame, now, native, || now)
    }
    /// Sample the trusted receiver clock again after native payload validation,
    /// immediately before charging progress or retaining the first terminal.
    /// The original receive entry retains its supplied-time behavior.
    pub fn receive_checked(
        &mut self,
        generation: u64,
        frame: ActiveFrame,
        now: u64,
        native: &mut impl NativeValidator,
        final_now: impl Fn() -> u64,
    ) -> Result<Accepted> {
        let isolated = match &frame.envelope {
            SelectedEnvelope::V1_0(e) => match &e.frame {
                Some(crate::wire::v1_0::envelope::Frame::Response(r)) => Some(r.request_id),
                _ => None,
            },
            SelectedEnvelope::V1_1(e) => match &e.frame {
                Some(v1_1::envelope::Frame::Response(r)) => Some(r.request_id),
                _ => None,
            },
        }
        .and_then(|id| self.calls.get(&id))
        .filter(|c| {
            matches!(
                c.contract.approved().profile(),
                "telemetry-sink.v1" | "resource.v1"
            )
        })
        .map(|c| {
            (
                c.binding.capability().kind.clone(),
                c.binding.capability().name.clone(),
            )
        });
        let result = self.receive_inner(generation, frame, now, native, &final_now);
        if let Some(capability) = isolated
            && result.as_ref().is_err_and(|error| {
                matches!(
                    error,
                    Error::Validation(profiles::Error::Shape | profiles::Error::NativeRejected)
                        | Error::Variant
                )
            })
        {
            self.disabled.insert(capability.clone());
            for call in self.calls.values_mut() {
                if call.binding.capability().kind == capability.0
                    && call.binding.capability().name == capability.1
                    && call.phase != Phase::TerminalReceived
                {
                    call.phase = Phase::Abandoned;
                }
            }
            return result;
        }
        if result.as_ref().is_err_and(|error| {
            matches!(
                error,
                Error::Stale
                    | Error::Reference
                    | Error::Sequence
                    | Error::Credit
                    | Error::Variant
                    | Error::Frame
                    | Error::Validation(
                        profiles::Error::Projection
                            | profiles::Error::Shape
                            | profiles::Error::Exposure
                            | profiles::Error::Variant
                            | profiles::Error::Limit
                    )
            )
        }) {
            self.draining = true;
            for call in self.calls.values_mut() {
                if call.phase != Phase::TerminalReceived {
                    call.phase = Phase::Abandoned;
                }
            }
        }
        result
    }
    fn receive_inner(
        &mut self,
        generation: u64,
        frame: ActiveFrame,
        now: u64,
        native: &mut impl NativeValidator,
        final_now: &dyn Fn() -> u64,
    ) -> Result<Accepted> {
        self.sample(now)?;
        if generation != self.grant.wire().connection_generation {
            return Err(Error::Generation);
        }
        let selected = self
            .grant
            .wire()
            .selected_protocol
            .ok_or(Error::HostContract)?
            .minor;
        let body_len = frame.body_len as u64;
        let envelope = match (selected, frame.envelope) {
            (0, SelectedEnvelope::V1_0(e)) => {
                v1_1::Envelope::decode(e.encode_to_vec().as_slice()).map_err(|_| Error::Frame)?
            }
            (1, SelectedEnvelope::V1_1(e)) => e,
            _ => return Err(Error::Reference),
        };
        if body_len > self.limits.frame_bytes || body_len < (envelope.encoded_len() as u64) {
            return Err(Error::Frame);
        }
        let arm = envelope.frame.ok_or(Error::Frame)?;
        let id = match &arm {
            v1_1::envelope::Frame::Response(r) => r.request_id,
            v1_1::envelope::Frame::StreamEnd(r) => r.request_id,
            v1_1::envelope::Frame::StreamItem(r) => r.request_id,
            v1_1::envelope::Frame::Cancel(r) => r.request_id,
            v1_1::envelope::Frame::Credit(r) => r.request_id,
            _ => return Err(Error::Frame),
        };
        let limits = self.bound_limits();
        let json_limits = self.json_limits();
        let c = self.calls.get_mut(&id).ok_or(Error::Stale)?;
        if matches!(c.phase, Phase::TerminalReceived | Phase::Abandoned) {
            return Err(Error::Stale);
        }
        if now >= c.deadline {
            c.phase = Phase::Abandoned;
            return Err(Error::Deadline);
        }
        match arm {
            v1_1::envelope::Frame::Cancel(cancel) => {
                if !matches!(cancel.reason, 1..=4) {
                    return Err(Error::Variant);
                }
                let b = c.binding.clone();
                self.cancel(&b, cancel.reason, now)?;
                Ok(Accepted::Cancelled)
            }
            v1_1::envelope::Frame::Credit(credit) => {
                if c.phase != Phase::Open || c.mode != Mode::Stream {
                    return Err(Error::Credit);
                }
                c.credit = c
                    .credit
                    .checked_add(credit.additional_bytes)
                    .filter(|v| *v <= self.limits.max_stream_credit_bytes)
                    .ok_or(Error::Credit)?;
                Ok(Accepted::Credited)
            }
            v1_1::envelope::Frame::StreamItem(item) => {
                if c.mode != Mode::Stream {
                    return Err(Error::Reference);
                }
                if item.sequence != c.sequence {
                    return Err(Error::Sequence);
                }
                if body_len > c.credit {
                    return Err(Error::Credit);
                }
                let next = c.sequence.checked_add(1).ok_or(Error::Sequence)?;
                let progress = self
                    .progress
                    .checked_add(body_len)
                    .ok_or(Error::Overloaded)?;
                if progress > self.reservations.progress_bytes {
                    return Err(Error::Overloaded);
                }
                let p = c.contract.approved().validate_payload(
                    c.binding
                        .capability()
                        .profile
                        .as_ref()
                        .ok_or(Error::Reference)?
                        .minor,
                    &c.method,
                    Stage::Progress,
                    &item.payload,
                    limits,
                    json_limits,
                    native,
                )?;
                provider_identity(&c.request, &p)?;
                let sampled = final_now();
                if sampled < now || sampled >= c.deadline {
                    c.phase = Phase::Abandoned;
                    return Err(Error::Deadline);
                }
                c.credit -= body_len;
                c.sequence = next;
                if c.phase == Phase::CancelRequested {
                    Ok(Accepted::Suppressed)
                } else {
                    self.progress = progress;
                    Ok(Accepted::Progress(p))
                }
            }
            v1_1::envelope::Frame::Response(r) => {
                if c.mode != Mode::Unary {
                    return Err(Error::Reference);
                }
                let t = r.terminal.ok_or(Error::Frame)?;
                let (result, error) = match t {
                    v1_1::response::Terminal::Result(b) => (Some(b), None),
                    v1_1::response::Terminal::Error(e) => (None, Some(e)),
                };
                Self::terminal(
                    c,
                    TerminalType::Response,
                    result,
                    error,
                    now,
                    limits,
                    json_limits,
                    native,
                    final_now,
                )
            }
            v1_1::envelope::Frame::StreamEnd(r) => {
                if c.mode != Mode::Stream {
                    return Err(Error::Reference);
                }
                let t = r.terminal.ok_or(Error::Frame)?;
                let (result, error) = match t {
                    v1_1::stream_end::Terminal::Result(b) => (Some(b), None),
                    v1_1::stream_end::Terminal::Error(e) => (None, Some(e)),
                };
                Self::terminal(
                    c,
                    TerminalType::StreamEnd,
                    result,
                    error,
                    now,
                    limits,
                    json_limits,
                    native,
                    final_now,
                )
            }
            _ => Err(Error::Frame),
        }
    }
    #[allow(clippy::too_many_arguments)]
    fn terminal(
        c: &mut Invocation,
        kind: TerminalType,
        result: Option<Vec<u8>>,
        error: Option<v1_1::TypedError>,
        now: u64,
        limits: value::Limits,
        json_limits: jcs::Limits,
        native: &mut impl NativeValidator,
        final_now: &dyn Fn() -> u64,
    ) -> Result<Accepted> {
        let mut code = None;
        let mut payload = None;
        if let Some(e) = error {
            let ecode = v1_1::ErrorCode::try_from(e.code).map_err(|_| Error::Variant)?;
            if ecode == v1_1::ErrorCode::Unspecified || e.safe_message != safe_error(ecode) {
                return Err(Error::Variant);
            }
            code = Some(ecode);
        }
        if let Some(bytes) = result {
            let p = c.contract.approved().validate_payload(
                c.binding
                    .capability()
                    .profile
                    .as_ref()
                    .ok_or(Error::Reference)?
                    .minor,
                &c.method,
                Stage::Terminal,
                &bytes,
                limits,
                json_limits,
                native,
            )?;
            provider_identity(&c.request, &p)?;
            if c.method == "resource.read" && c.request.fields["digest"] != p.fields["digest"] {
                return Err(Error::Validation(profiles::Error::Shape));
            }
            if c.method == "telemetry.emit_batch"
                && p.fields["count"].as_u64()
                    != c.request.fields["records"]
                        .as_array()
                        .map(|a| a.len() as u64)
            {
                return Err(Error::Validation(profiles::Error::Shape));
            }
            if c.method == "telemetry.flush"
                && p.fields["flushed_through"].as_u64()
                    > c.request.fields["through_sequence"].as_u64()
            {
                return Err(Error::Validation(profiles::Error::Shape));
            }
            payload = Some(p);
        }
        let received_ms = final_now();
        if received_ms < now || received_ms >= c.deadline {
            c.phase = Phase::Abandoned;
            return Err(Error::Deadline);
        }
        c.receipt = Some(Receipt {
            binding: c.binding.clone(),
            kind,
            payload,
            error: code,
            received_ms,
        });
        c.phase = Phase::TerminalReceived;
        Ok(Accepted::Terminal)
    }
    pub fn release_progress(&mut self, bytes: u64) -> Result<()> {
        self.progress = self
            .progress
            .checked_sub(bytes)
            .ok_or(Error::HostContract)?;
        Ok(())
    }
    pub fn cancel(&mut self, b: &Binding, reason: i32, now: u64) -> Result<()> {
        self.sample(now)?;
        if !matches!(reason, 1..=4) {
            return Err(Error::Variant);
        }
        let c = self
            .calls
            .get(&b.id())
            .filter(|c| c.binding.same_identity(b))
            .ok_or(Error::Stale)?;
        if matches!(c.phase, Phase::TerminalReceived | Phase::Abandoned) {
            return Err(Error::Stale);
        }
        let mut ids = vec![b.id()];
        for _ in 0..8 {
            let before = ids.len();
            for c in self.calls.values() {
                if c.binding.parent().is_some_and(|p| ids.contains(&p))
                    && !ids.contains(&c.binding.id())
                {
                    ids.push(c.binding.id());
                }
            }
            if ids.len() == before {
                break;
            }
        }
        let grace = now
            .checked_add(self.limits.cancellation_grace_ms)
            .ok_or(Error::Deadline)?;
        for id in ids {
            let c = self.calls.get_mut(&id).expect("existing");
            if matches!(c.phase, Phase::Open | Phase::CancelRequested) {
                c.phase = Phase::CancelRequested;
                c.deadline = c.deadline.min(grace);
            }
        }
        Ok(())
    }
    pub fn drain(&mut self, now: u64) -> Result<()> {
        self.sample(now)?;
        self.draining = true;
        Ok(())
    }
    pub fn lose_generation(&mut self, now: u64) -> Result<()> {
        self.sample(now)?;
        self.draining = true;
        for c in self.calls.values_mut() {
            if c.phase != Phase::TerminalReceived {
                c.phase = Phase::Abandoned;
            }
        }
        Ok(())
    }
    pub fn expire(&mut self, now: u64) -> Result<()> {
        self.sample(now)?;
        let mut invalid: BTreeSet<_> = self
            .external_parents
            .iter()
            .filter_map(|(id, parent)| self.checked_external(parent, now).is_err().then_some(*id))
            .collect();
        invalid.extend(
            self.native_parents
                .iter()
                .filter_map(|(id, parent)| parent.0.observe(now).is_err().then_some(*id)),
        );
        let mut cancelled = invalid;
        for _ in 0..8 {
            let descendants: Vec<_> = self
                .calls
                .values()
                .filter_map(|c| {
                    c.binding
                        .parent()
                        .filter(|p| cancelled.contains(p))
                        .map(|_| c.binding.id())
                })
                .collect();
            let before = cancelled.len();
            cancelled.extend(descendants);
            if cancelled.len() == before {
                break;
            }
        }
        for c in self.calls.values_mut() {
            if cancelled.contains(&c.binding.id()) && c.phase == Phase::Open {
                c.phase = Phase::CancelRequested;
                c.deadline = c
                    .deadline
                    .min(now.saturating_add(self.limits.cancellation_grace_ms));
            }
            if now >= c.deadline && c.phase != Phase::TerminalReceived {
                c.phase = Phase::Abandoned;
                if matches!(
                    c.contract.approved().profile(),
                    "telemetry-sink.v1" | "resource.v1"
                ) {
                    self.disabled.insert((
                        c.binding.capability().kind.clone(),
                        c.binding.capability().name.clone(),
                    ));
                }
            }
        }
        Ok(())
    }
    pub fn retire(&mut self, b: &Binding, reason: RetireReason) -> Result<()> {
        let c = self
            .calls
            .get(&b.id())
            .filter(|c| c.binding.same_identity(b))
            .ok_or(Error::Stale)?;
        if reason == RetireReason::Consumed && c.phase != Phase::TerminalReceived {
            return Err(Error::Stale);
        }
        self.blocking = self
            .blocking
            .checked_sub(c.blocking)
            .ok_or(Error::HostContract)?;
        self.calls.remove(&b.id());
        self.external_parents.remove(&b.id());
        self.native_parents.remove(&b.id());
        Ok(())
    }
}
fn valid_role(s: &Start, profile: &str) -> bool {
    let key_valid = s.key.as_ref().is_none_or(|k| {
        !k.session_id.is_empty()
            && !k.lane.is_empty()
            && !k.operation_id.is_empty()
            && !k.effect_kind.is_empty()
            && !k.step_or_task_id.is_empty()
            && k.logical_attempt <= 9_007_199_254_740_991
            && k.item_index.is_none_or(|i| i <= 9_007_199_254_740_991)
    });
    key_valid
        && match s.role {
            Role::Planned => s.key.is_some() && matches!(profile, "hook.v1" | "provider.v1"),
            Role::Inline => {
                s.key.is_some()
                    && s.parent.as_ref().is_some_and(|p| p.key() == s.key.as_ref())
                    && profile == "hook.v1"
            }
            Role::Preaccept => {
                s.key.is_none()
                    && s.parent.is_none()
                    && profile == "hook.v1"
                    && s.method == "before_run"
            }
            Role::HostAction => s.key.is_none() && profile == "host-action.v1",
            Role::Passive => {
                s.key.is_none()
                    && matches!(
                        profile,
                        "event-sink.v1" | "telemetry-sink.v1" | "resource.v1"
                    )
            }
        }
}
fn required_permission(method: &str) -> Option<&str> {
    match method {
        "snapshot.get" => Some("conversation.read"),
        "resource.get" => Some("resource.read"),
        "queue.steer" => Some("queue.steer"),
        "queue.follow_up" => Some("queue.follow_up"),
        "queue.next_run" => Some("queue.next_run"),
        "operation.abort" => Some("operation.abort"),
        "resource.list" | "resource.read" | "resource.changed" => Some("resource.provide"),
        "telemetry.emit_batch" | "telemetry.flush" => Some("telemetry.receive"),
        _ => None,
    }
}
fn provider_identity(request: &ValidatedPayload, p: &ValidatedPayload) -> Result<()> {
    if p.method != "provider.invoke" {
        return Ok(());
    }
    let model = &request.projections["provider.model.v1"];
    let m = match p.stage {
        Stage::Terminal => &p.projections["provider.assistant_message.v1"],
        Stage::Progress => &p.projections["provider.assistant_event.v1"]["partial"],
        Stage::Request => return Ok(()),
    };
    if m["api"] != model["api"] || m["provider"] != model["provider"] || m["model"] != model["id"] {
        return Err(Error::Validation(profiles::Error::Shape));
    }
    Ok(())
}
pub fn safe_error(code: v1_1::ErrorCode) -> &'static str {
    match code {
        v1_1::ErrorCode::HandlerError => "handler failed",
        _ => "request failed",
    }
}

/// Reserved Gap is independent of ordinary passive bytes. It remains immutable
/// while later dropped source records advance the cursor.
pub struct EventQueue {
    subscription: String,
    generation: u64,
    capacity: u64,
    gap_capacity: u64,
    bytes: u64,
    queue: VecDeque<(u64, Vec<u8>)>,
    gap: Option<v1_1::EventGap>,
    paused: bool,
    next: u64,
    revision: u64,
    dropped: u64,
    gap_deadline: u64,
    retired: bool,
}
impl EventQueue {
    pub fn new(
        subscription: String,
        generation: u64,
        capacity: u64,
        gap_capacity: u64,
        next: u64,
        revision: u64,
        gap_deadline: u64,
    ) -> Result<Self> {
        if subscription.is_empty()
            || generation == 0
            || capacity == 0
            || capacity == u64::MAX
            || gap_capacity == 0
            || gap_capacity == u64::MAX
            || gap_deadline == 0
        {
            return Err(Error::HostContract);
        }
        let gap = v1_1::EventGap {
            subscription_id: subscription.clone(),
            first_dropped: u64::MAX,
            last_dropped: u64::MAX,
        };
        if gap.encoded_len() as u64 > gap_capacity {
            return Err(Error::HostContract);
        }
        Ok(Self {
            subscription,
            generation,
            capacity,
            gap_capacity,
            bytes: 0,
            queue: VecDeque::new(),
            gap: None,
            paused: false,
            next,
            revision,
            dropped: 0,
            gap_deadline,
            retired: false,
        })
    }
    pub fn paused(&self) -> bool {
        self.paused
    }
    pub fn gap(&self) -> Option<&v1_1::EventGap> {
        self.gap.as_ref()
    }
    pub fn cursor(&self) -> u64 {
        self.next
    }
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
    pub fn publish(
        &mut self,
        generation: u64,
        sequence: u64,
        revision: u64,
        encoded_event: Vec<u8>,
        now: u64,
    ) -> Result<()> {
        if self.retired || generation != self.generation {
            return Err(Error::Generation);
        }
        if self.paused && now >= self.gap_deadline {
            self.retired = true;
            return Err(Error::Deadline);
        }
        if sequence != self.next || revision < self.revision {
            return Err(Error::Sequence);
        }
        let next = self.next.checked_add(1).ok_or(Error::Sequence)?;
        let size = encoded_event.len() as u64;
        let total = self.bytes.checked_add(size).ok_or(Error::Overloaded)?;
        if self.paused {
            self.dropped = self.dropped.checked_add(1).ok_or(Error::Overloaded)?;
        } else if total > self.capacity {
            let first = self.queue.front().map_or(sequence, |(seq, _)| *seq);
            let gap = v1_1::EventGap {
                subscription_id: self.subscription.clone(),
                first_dropped: first,
                last_dropped: sequence,
            };
            if gap.encoded_len() as u64 > self.gap_capacity {
                return Err(Error::HostContract);
            }
            self.dropped = self
                .dropped
                .checked_add(self.queue.len() as u64)
                .and_then(|d| d.checked_add(1))
                .ok_or(Error::Overloaded)?;
            self.queue.clear();
            self.bytes = 0;
            self.gap = Some(gap);
            self.paused = true;
        } else {
            self.queue.push_back((sequence, encoded_event));
            self.bytes = total;
        }
        self.next = next;
        self.revision = revision;
        Ok(())
    }
    pub fn take_event(&mut self) -> Option<Vec<u8>> {
        if self.paused || self.retired {
            return None;
        }
        let (_, bytes) = self.queue.pop_front()?;
        self.bytes -= bytes.len() as u64;
        Some(bytes)
    }
    /// Atomic captured baseline replaces ordinary state and unpauses. The
    /// returned/sent Gap object is never edited by later publication or resync.
    pub fn resync(
        &mut self,
        generation: u64,
        snapshot_revision: u64,
        next: u64,
        now: u64,
    ) -> Result<()> {
        if self.retired || generation != self.generation {
            return Err(Error::Generation);
        }
        if self.paused && now >= self.gap_deadline {
            self.retired = true;
            return Err(Error::Deadline);
        }
        if snapshot_revision < self.revision || next != self.next {
            return Err(Error::Sequence);
        }
        self.queue.clear();
        self.bytes = 0;
        self.gap = None;
        self.paused = false;
        self.revision = snapshot_revision;
        Ok(())
    }
    pub fn cancel(&mut self) {
        self.retired = true;
        self.queue.clear();
        self.bytes = 0;
    }
}

/// An opaque record projected through the fixed allowlist before enqueue.
#[derive(Clone)]
pub struct TelemetryRecord(v1_1::SchemaBoundValue);
impl TelemetryRecord {
    pub fn wire(&self) -> &v1_1::SchemaBoundValue {
        &self.0
    }
}
/// Telemetry failure affects only this sink. Queueing and acknowledgement never
/// own a settlement or durability barrier.
pub struct Telemetry {
    capacity: u64,
    bytes: u64,
    queue: VecDeque<(u64, Vec<v1_1::SchemaBoundValue>, u64)>,
    next: u64,
    pending: BTreeMap<u64, u64>,
    pending_limit: usize,
    flushed: Option<u64>,
    last_emitted: Option<u64>,
    dropped: u64,
    disabled: bool,
    spans: BTreeMap<u64, bool>,
    next_span: u64,
}
impl Telemetry {
    pub fn new(capacity: u64, pending_limit: usize) -> Result<Self> {
        if capacity == 0 || capacity == u64::MAX || pending_limit == 0 || pending_limit > 256 {
            return Err(Error::HostContract);
        }
        Ok(Self {
            capacity,
            bytes: 0,
            queue: VecDeque::new(),
            next: 0,
            pending: BTreeMap::new(),
            pending_limit,
            flushed: None,
            last_emitted: None,
            dropped: 0,
            disabled: false,
            spans: BTreeMap::new(),
            next_span: 1,
        })
    }
    pub fn disabled(&self) -> bool {
        self.disabled
    }
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
    pub fn disable(&mut self) {
        self.disabled = true;
        self.queue.clear();
        self.pending.clear();
        self.spans.clear();
        self.bytes = 0;
    }
    /// Projects the fixed allowlist before a record reaches the bounded queue.
    pub fn project(
        &mut self,
        registry: &profiles::Registry,
        mut record: Value,
    ) -> Result<Option<TelemetryRecord>> {
        if self.disabled {
            return Ok(None);
        }
        let Some(name) = record["name"].as_str() else {
            return Ok(None);
        };
        if ![
            "pi.harness.run",
            "pi.harness.checkpoint",
            "pi.harness.structural",
            "pi.harness.turn",
            "pi.harness.step",
            "pi.harness.hook",
            "pi.harness.sleep",
            "pi.harness.tool",
            "pi.harness.event_handler",
        ]
        .contains(&name)
        {
            return Ok(None);
        }
        let kind = record["kind"].as_str().ok_or(Error::Variant)?.to_owned();
        if kind == "span_start" {
            if self.spans.len() >= self.pending_limit {
                return Err(Error::Overloaded);
            }
            let id = self.next_span;
            if id > 9_007_199_254_740_991 {
                self.disable();
                return Err(Error::Overloaded);
            }
            record["spanId"] = Value::from(id);
            record
                .as_object_mut()
                .ok_or(Error::Variant)?
                .remove("status");
        } else {
            let id = record["spanId"].as_u64().ok_or(Error::Variant)?;
            if !self.spans.contains_key(&id) {
                return Err(Error::Variant);
            }
            if kind == "span_event" {
                record
                    .as_object_mut()
                    .ok_or(Error::Variant)?
                    .remove("status");
            } else if kind == "span_finish" {
                if !matches!(record["status"].as_str(), Some("ok" | "error")) {
                    return Err(Error::Variant);
                }
            } else {
                return Err(Error::Variant);
            }
        }
        if record
            .get("parentSpanId")
            .is_some_and(|p| p.as_u64().is_none_or(|p| !self.spans.contains_key(&p)))
        {
            return Err(Error::Parent);
        }
        let attrs = record["attributes"]
            .as_object()
            .cloned()
            .unwrap_or_default();
        let mut projected = serde_json::Map::new();
        for (key, value) in attrs {
            let good = match key.as_str() {
                "pi.operation.kind" => {
                    matches!(value.as_str(), Some("run" | "compaction" | "navigation"))
                }
                "pi.operation.recovery" => value.is_boolean(),
                "pi.operation.outcome" => matches!(
                    value.as_str(),
                    Some("completed" | "declined" | "aborted" | "failed" | "suspended")
                ),
                "pi.checkpoint.kind" => matches!(
                    value.as_str(),
                    Some("normal" | "abort_reconcile" | "failure_drain")
                ),
                "pi.step.kind" => matches!(
                    value.as_str(),
                    Some("assistant" | "deferred" | "compaction" | "branch_summary")
                ),
                "pi.step.attempt" => value.as_u64().is_some_and(|n| n <= 9_007_199_254_740_991),
                "pi.step.outcome" => matches!(
                    value.as_str(),
                    Some("succeeded" | "retry" | "failed" | "aborted" | "deferred")
                ),
                "pi.hook.outcome" => matches!(
                    value.as_str(),
                    Some("completed" | "skipped" | "blocked" | "failed")
                ),
                "pi.hook.name" => matches!(
                    value.as_str(),
                    Some(
                        "before_run"
                            | "before_resume"
                            | "before_run_end"
                            | "transform_context"
                            | "before_request"
                            | "before_payload"
                            | "after_response"
                            | "before_tool"
                            | "after_tool"
                            | "before_compaction"
                            | "before_navigation"
                    )
                ),
                _ => false,
            };
            if good {
                projected.insert(key, value);
            }
        }
        let object = record.as_object_mut().ok_or(Error::Variant)?;
        object.retain(|key, _| {
            matches!(
                key.as_str(),
                "kind" | "spanId" | "parentSpanId" | "name" | "attributes" | "status"
            )
        });
        object.insert("attributes".into(), Value::Object(projected));
        let bound = v1_1::SchemaBoundValue {
            projection_id: "telemetry.record.v1".into(),
            schema_digest_sha256: registry
                .digest("telemetry.record.v1")
                .ok_or(Error::HostContract)?
                .to_vec(),
            value: Some(profiles::json_wire(&record)?),
        };
        registry.validate_v1_1(
            "telemetry.record.v1",
            &bound,
            value::Limits::default(),
            262_144,
        )?;
        let id = record["spanId"].as_u64().ok_or(Error::Variant)?;
        if kind == "span_start" {
            self.next_span = id.checked_add(1).ok_or_else(|| {
                self.disable();
                Error::Overloaded
            })?;
            self.spans.insert(id, true);
        }
        if kind == "span_finish" {
            self.spans.remove(&id);
        }
        Ok(Some(TelemetryRecord(bound)))
    }
    pub fn enqueue(&mut self, records: Vec<TelemetryRecord>) -> Result<Option<u64>> {
        if self.disabled {
            return Ok(None);
        }
        if self.queue.len() + self.pending.len() >= self.pending_limit {
            self.dropped = self
                .dropped
                .checked_add(records.len() as u64)
                .ok_or_else(|| {
                    self.disable();
                    Error::Overloaded
                })?;
            return Ok(None);
        }
        let records: Vec<_> = records.into_iter().map(|r| r.0).collect();
        let sequence = self.next;
        let next = sequence.checked_add(1).ok_or_else(|| {
            self.disable();
            Error::Overloaded
        })?;
        let bytes = v1_1::TelemetryEmitBatchRequest {
            schema_version: 1,
            sequence,
            records: records.clone(),
        }
        .encoded_len() as u64;
        let total = self.bytes.checked_add(bytes).ok_or_else(|| {
            self.disable();
            Error::Overloaded
        })?;
        if total > self.capacity {
            self.dropped = self
                .dropped
                .checked_add(records.len() as u64)
                .ok_or_else(|| {
                    self.disable();
                    Error::Overloaded
                })?;
            return Ok(None);
        }
        self.queue.push_back((sequence, records, bytes));
        self.next = next;
        self.bytes = total;
        Ok(Some(sequence))
    }
    pub fn take_batch(&mut self) -> Option<v1_1::TelemetryEmitBatchRequest> {
        if self.disabled {
            return None;
        }
        let (sequence, records, bytes) = self.queue.pop_front()?;
        self.bytes -= bytes;
        self.pending.insert(sequence, records.len() as u64);
        self.last_emitted = Some(sequence);
        Some(v1_1::TelemetryEmitBatchRequest {
            schema_version: 1,
            sequence,
            records,
        })
    }
    pub fn ack(&mut self, sequence: u64, ack: v1_1::TelemetryEmitBatchResult) -> Result<()> {
        let expected = self.pending.get(&sequence).copied();
        if expected.is_none() || ack.accepted.checked_add(ack.dropped) != expected {
            self.disable();
            return Err(Error::Variant);
        }
        self.pending.remove(&sequence);
        self.dropped = self.dropped.checked_add(ack.dropped).ok_or_else(|| {
            self.disable();
            Error::Overloaded
        })?;
        Ok(())
    }
    pub fn flush(&mut self, requested: u64, ack: u64) -> Result<()> {
        if self.disabled {
            return Err(Error::Stale);
        }
        if self.last_emitted.is_none_or(|last| requested > last)
            || ack > requested
            || self.flushed.is_some_and(|old| ack < old)
        {
            self.disable();
            return Err(Error::Variant);
        }
        self.flushed = Some(ack);
        Ok(())
    }
}
