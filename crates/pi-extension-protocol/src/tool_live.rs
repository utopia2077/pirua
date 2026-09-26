//! One host-minted `tool.v1` invocation and its first validated terminal.
//!
//! Callers obtain `ActiveGrant` only after Ready. They supply the current host
//! generation and the planned durable key; peer frames cannot mint either. A
//! `LiveError` from peer input means the caller must close that generation.
//! `HostDomainFault` instead comes only from a trusted domain callback and must
//! be reported as a host fault, without blaming the peer or mutating this call.
//! This module does not authorize durable settlement or own an in-flight queue.

use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

use prost::Message;

use crate::activation::{
    ActiveGrant, Projection as CatalogProjection, ToolProjections, ToolReplay,
};
use crate::endpoint::{ActiveFrame, SelectedEnvelope};
use crate::jcs;
use crate::wire::echo_bounded::{BorrowedIndexArena, ScanError, SelectedMinor};
use crate::wire::echo_selected::{
    self, ActiveArm, AdmissionError, ResultTerminal, RoleError, SelectError, SelectedActive,
};
use crate::wire::framing::{self, EncodedFrame, FrameError};
use crate::wire::value;
use crate::wire::{v1_0, v1_1};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiveError {
    InvalidHostBinding,
    WrongGeneration,
    WrongRequestId,
    WrongVersion,
    UnexpectedFrame,
    InvalidEffectKey,
    InvalidToolCallId,
    InvalidDeadline,
    CreditViolation,
    InvalidSequence,
    InvalidProgress,
    InvalidTerminal,
    /// A trusted domain validator failed locally; this is not malformed peer input.
    HostDomainFault,
    UnsupportedVariant,
    Limit,
    PeerLost,
    Stale,
    WrongBinding,
    Frame(FrameError),
    Value(value::Error),
}

impl fmt::Display for LiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidHostBinding => "invalid host invocation binding",
            Self::WrongGeneration => "wrong connection generation",
            Self::WrongRequestId => "wrong invocation request id",
            Self::WrongVersion => "wrong selected protocol version",
            Self::UnexpectedFrame => "unexpected active frame",
            Self::InvalidEffectKey => "invalid planned effect key",
            Self::InvalidToolCallId => "invalid source tool call id",
            Self::InvalidDeadline => "invalid relative deadline",
            Self::CreditViolation => "stream credit violation",
            Self::InvalidSequence => "stream sequence violation",
            Self::InvalidProgress => "invalid tool progress",
            Self::InvalidTerminal => "invalid tool terminal",
            Self::HostDomainFault => "host tool domain validator failed",
            Self::UnsupportedVariant => "unsupported closed variant",
            Self::Limit => "tool value or frame limit exceeded",
            Self::PeerLost => "peer lost before terminal",
            Self::Stale => "invocation frame is stale",
            Self::WrongBinding => "live binding mismatch",
            Self::Frame(_) => "bounded frame rejected",
            Self::Value(_) => "schema-bound value rejected",
        })
    }
}

impl std::error::Error for LiveError {}

impl From<FrameError> for LiveError {
    fn from(value: FrameError) -> Self {
        Self::Frame(value)
    }
}

impl From<value::Error> for LiveError {
    fn from(value: value::Error) -> Self {
        Self::Value(value)
    }
}

/// Caller-local scanner/policy failures; these never blame the peer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IndexedLocalFault {
    ScanLimit,
    ScanAllocation,
    InternalPolicy,
}

impl fmt::Display for IndexedLocalFault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ScanLimit => "indexed scan capacity exhausted",
            Self::ScanAllocation => "indexed scan allocation failed",
            Self::InternalPolicy => "indexed receive policy mismatch",
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexedReceiveError {
    Protocol(LiveError),
    Local(IndexedLocalFault),
}

impl fmt::Display for IndexedReceiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Protocol(error) => error.fmt(f),
            Self::Local(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for IndexedReceiveError {}

impl From<LiveError> for IndexedReceiveError {
    fn from(error: LiveError) -> Self {
        Self::Protocol(error)
    }
}

type IndexedResult<T> = std::result::Result<T, IndexedReceiveError>;

type Result<T> = std::result::Result<T, LiveError>;
const MAX_SAFE_UNSIGNED: u64 = 9_007_199_254_740_991;
const REMOTE_TOOL_ERROR_MESSAGE: &str = "remote tool error";

struct BindingCore {
    effect_key: v1_1::EffectKey,
    capability_ref: v1_1::CapabilityRef,
    generation: u64,
    request_id: u64,
}

/// Opaque process-local identity; equality requires the same minted allocation.
#[derive(Clone)]
pub struct InvocationBinding(Arc<BindingCore>);

impl fmt::Debug for InvocationBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InvocationBinding")
            .field("generation", &self.0.generation)
            .field("request_id", &self.0.request_id)
            .finish_non_exhaustive()
    }
}

impl InvocationBinding {
    pub fn same_live_identity(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn effect_key(&self) -> &v1_1::EffectKey {
        &self.0.effect_key
    }
    pub fn capability_ref(&self) -> &v1_1::CapabilityRef {
        &self.0.capability_ref
    }
    pub fn generation(&self) -> u64 {
        self.0.generation
    }
    pub fn request_id(&self) -> u64 {
        self.0.request_id
    }
}

/// Schema-bound argument DTO selected by the tool profile minor.
pub enum SelectedArguments {
    V1_0(v1_0::SchemaBoundValue),
    V1_1(v1_1::SchemaBoundValue),
}

/// Host-only planned call input. `active_generation` is the current generation,
/// so an older draining grant cannot mint another invocation.
pub struct Start {
    pub effect_key: v1_1::EffectKey,
    pub source_call_id: String,
    pub request_id: u64,
    pub active_generation: u64,
    pub arguments: SelectedArguments,
    pub now_ms: u64,
    pub requested_deadline_after_ms: Option<u64>,
    pub method_limit_ms: u64,
    pub connection_remaining_ms: u64,
    pub parent_remaining_ms: Option<u64>,
    pub initial_credit_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LivePhase {
    Open,
    CancelRequested,
    TerminalReceived,
    PeerLost,
    Fenced,
    Retired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetireReason {
    Committed,
    NoLongerExpected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalKind {
    Result,
    Error,
}

enum TerminalOutput {
    Result(Vec<u8>),
    Error { code: v1_1::ErrorCode },
}

/// Immutable validated terminal held until the runtime explicitly retires it.
pub struct TerminalReceipt {
    binding: InvocationBinding,
    output: TerminalOutput,
    received_ms: u64,
}

impl TerminalReceipt {
    pub fn binding(&self) -> &InvocationBinding {
        &self.binding
    }
    pub fn kind(&self) -> TerminalKind {
        match self.output {
            TerminalOutput::Result(_) => TerminalKind::Result,
            TerminalOutput::Error { .. } => TerminalKind::Error,
        }
    }
    pub fn result_json(&self) -> Option<&[u8]> {
        match &self.output {
            TerminalOutput::Result(bytes) => Some(bytes),
            TerminalOutput::Error { .. } => None,
        }
    }
    pub fn error(&self) -> Option<(v1_1::ErrorCode, &str)> {
        match &self.output {
            TerminalOutput::Error { code } => Some((*code, REMOTE_TOOL_ERROR_MESSAGE)),
            TerminalOutput::Result(_) => None,
        }
    }
    pub fn received_ms(&self) -> u64 {
        self.received_ms
    }
}

pub struct ValidatedProgress {
    json: Vec<u8>,
    percent_complete: Option<u32>,
}

impl ValidatedProgress {
    pub fn json(&self) -> &[u8] {
        &self.json
    }
    pub fn percent_complete(&self) -> Option<u32> {
        self.percent_complete
    }
}

pub enum ReceiveOutcome {
    Progress(ValidatedProgress),
    /// The item was validated and charged after local cancellation; no payload is delivered.
    IgnoredCancelledProgress,
    TerminalReceived,
    Stale,
}

/// The selected host projection being checked for Rust domain construction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DomainKind {
    Progress,
    Result,
}

/// A synchronous host verdict. It contains no peer value or diagnostic text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DomainDecision {
    Accept,
    PeerValueMismatch,
    /// A trusted binding/validator failure that leaves invocation state unchanged.
    HostFault,
}

/// Schema-admitted bounded JSON with its exact selected host binding.
pub struct DomainCheck<'a> {
    kind: DomainKind,
    json: &'a [u8],
    binding: &'a InvocationBinding,
    profile_minor: u32,
    projection_id: &'a str,
    schema_digest_sha256: [u8; 32],
}

impl DomainCheck<'_> {
    pub fn kind(&self) -> DomainKind {
        self.kind
    }
    pub fn json(&self) -> &[u8] {
        self.json
    }
    pub fn binding(&self) -> &InvocationBinding {
        self.binding
    }
    pub fn profile_minor(&self) -> u32 {
        self.profile_minor
    }
    pub fn projection_id(&self) -> &str {
        self.projection_id
    }
    pub fn schema_digest_sha256(&self) -> [u8; 32] {
        self.schema_digest_sha256
    }
}

struct Limits {
    frame: NonZeroUsize,
    value: value::Limits,
    json: jcs::Limits,
    max_credit: u64,
    string_bytes: usize,
}

impl Limits {
    fn from_grant(l: &v1_1::ProtocolLimits) -> Result<Self> {
        let frame = usize::try_from(l.frame_bytes)
            .ok()
            .and_then(NonZeroUsize::new)
            .ok_or(LiveError::Limit)?;
        let decoded = usize::try_from(l.aggregate_decoded_bytes).map_err(|_| LiveError::Limit)?;
        let elements = usize::try_from(l.value_elements).unwrap_or(usize::MAX);
        let string_bytes = usize::try_from(l.string_bytes).map_err(|_| LiveError::Limit)?;
        let bytes_length = usize::try_from(l.bytes_length).map_err(|_| LiveError::Limit)?;
        let json_bytes = frame.get().min(decoded);
        let json = jcs::Limits {
            input_bytes: frame.get(),
            depth: l.value_depth as usize,
            nodes: elements,
            elements,
            decoded_bytes: decoded,
            output_bytes: json_bytes,
            schema_work: decoded.saturating_mul(8).min(usize::MAX - 1),
            instance_work: decoded.saturating_mul(8).min(usize::MAX - 1),
        };
        let value = value::Limits {
            depth: l.value_depth as usize,
            elements,
            string_bytes,
            bytes_length,
            decoded_bytes: decoded,
            json_bytes,
        };
        Ok(Self {
            frame,
            value,
            json,
            max_credit: l.max_stream_credit_bytes,
            string_bytes,
        })
    }
}

/// A single invocation. No method here can commit a durable effect.
pub struct ToolInvocation {
    binding: InvocationBinding,
    source_call_id: String,
    core_minor: u32,
    tool_minor: u32,
    replay: ToolReplay,
    projections: ToolProjections,
    limits: Limits,
    deadline_ms: u64,
    drain_deadline_ms: Option<u64>,
    credit: u64,
    next_sequence: u64,
    phase: LivePhase,
    receipt: Option<TerminalReceipt>,
    #[cfg(test)]
    indexed_test_fault: Option<IndexedTestFault>,
}

impl ToolInvocation {
    pub fn start(
        grant: &ActiveGrant,
        tool_name: &str,
        start: Start,
    ) -> Result<(Self, EncodedFrame)> {
        let wire = grant.wire();
        let selected = wire
            .selected_protocol
            .ok_or(LiveError::InvalidHostBinding)?;
        let l = wire.limits.as_ref().ok_or(LiveError::InvalidHostBinding)?;
        let limits = Limits::from_grant(l)?;
        if selected.major != 1
            || selected.minor > 1
            || wire.authenticated_extension_id.is_empty()
            || wire.connection_generation == 0
            || start.active_generation != wire.connection_generation
            || start.request_id == 0
        {
            return Err(LiveError::InvalidHostBinding);
        }
        let tool = grant
            .tools()
            .iter()
            .find(|tool| tool.descriptor().name == tool_name)
            .ok_or(LiveError::InvalidHostBinding)?;
        let reference = tool.reference();
        let profile = reference
            .profile
            .as_ref()
            .ok_or(LiveError::InvalidHostBinding)?;
        if reference.authenticated_extension_id != wire.authenticated_extension_id
            || reference.kind != "tool"
            || reference.name != tool_name
            || profile.name != "tool.v1"
            || profile.major != 1
            || profile.minor > 1
            || !wire.selected_capabilities.contains(reference)
        {
            return Err(LiveError::InvalidHostBinding);
        }
        if !valid_effect_key(&start.effect_key, limits.string_bytes) {
            return Err(LiveError::InvalidEffectKey);
        }
        if start.source_call_id.is_empty() || start.source_call_id.len() > limits.string_bytes {
            return Err(LiveError::InvalidToolCallId);
        }
        let relative = start
            .requested_deadline_after_ms
            .unwrap_or(start.method_limit_ms)
            .min(start.method_limit_ms)
            .min(start.connection_remaining_ms)
            .min(start.parent_remaining_ms.unwrap_or(u64::MAX));
        if relative == 0 || start.method_limit_ms == 0 || start.connection_remaining_ms == 0 {
            return Err(LiveError::InvalidDeadline);
        }
        let deadline_ms = start
            .now_ms
            .checked_add(relative)
            .ok_or(LiveError::InvalidDeadline)?;
        let credit = start.initial_credit_bytes.unwrap_or(0);
        if credit > limits.max_credit {
            return Err(LiveError::CreditViolation);
        }
        let projection = tool.projections().arguments();
        let expected = selected_projection(projection);
        let payload = match (profile.minor, start.arguments) {
            (0, SelectedArguments::V1_0(arguments)) => {
                value::admit_schema_bound_v1_0(&arguments, &expected, limits.value, limits.json)?;
                let inner = v1_0::InvokeToolRequest {
                    tool_id: tool_name.into(),
                    arguments: Some(arguments),
                    tool_call_id: start.source_call_id.clone(),
                };
                encode_inner(&inner, limits.frame)?
            }
            (1, SelectedArguments::V1_1(arguments)) => {
                value::admit_schema_bound_v1_1(&arguments, &expected, limits.value, limits.json)?;
                let inner = v1_1::InvokeToolRequest {
                    tool_id: tool_name.into(),
                    arguments: Some(arguments),
                    tool_call_id: start.source_call_id.clone(),
                };
                encode_inner(&inner, limits.frame)?
            }
            _ => return Err(LiveError::WrongVersion),
        };
        let binding = InvocationBinding(Arc::new(BindingCore {
            effect_key: start.effect_key.clone(),
            capability_ref: reference.clone(),
            generation: wire.connection_generation,
            request_id: start.request_id,
        }));
        let outbound = if selected.minor == 0 {
            let request = v1_0::Request {
                request_id: start.request_id,
                parent_request_id: None,
                capability_ref: Some(convert_message(reference, limits.frame)?),
                effect_key: Some(convert_message(&start.effect_key, limits.frame)?),
                method: "tool.invoke".into(),
                deadline_after_ms: Some(relative),
                initial_stream_credit_bytes: start.initial_credit_bytes,
                payload,
            };
            framing::encode(
                &v1_0::Envelope {
                    frame: Some(v1_0::envelope::Frame::Request(request)),
                },
                limits.frame,
            )?
        } else {
            let request = v1_1::Request {
                request_id: start.request_id,
                parent_request_id: None,
                capability_ref: Some(reference.clone()),
                effect_key: Some(start.effect_key),
                method: "tool.invoke".into(),
                deadline_after_ms: Some(relative),
                initial_stream_credit_bytes: start.initial_credit_bytes,
                payload,
            };
            framing::encode(
                &v1_1::Envelope {
                    frame: Some(v1_1::envelope::Frame::Request(request)),
                },
                limits.frame,
            )?
        };
        Ok((
            Self {
                binding,
                source_call_id: start.source_call_id,
                core_minor: selected.minor,
                tool_minor: profile.minor,
                replay: tool.descriptor().replay,
                projections: tool.projections().clone(),
                limits,
                deadline_ms,
                drain_deadline_ms: None,
                credit,
                next_sequence: 0,
                phase: LivePhase::Open,
                receipt: None,
                #[cfg(test)]
                indexed_test_fault: None,
            },
            outbound,
        ))
    }

    pub fn binding(&self) -> &InvocationBinding {
        &self.binding
    }
    pub fn source_call_id(&self) -> &str {
        &self.source_call_id
    }
    pub fn replay(&self) -> ToolReplay {
        self.replay
    }
    pub fn phase(&self) -> LivePhase {
        self.phase
    }
    pub fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }
    pub fn effective_deadline_ms(&self) -> u64 {
        self.drain_deadline_ms
            .map_or(self.deadline_ms, |drain| self.deadline_ms.min(drain))
    }
    pub fn credit(&self) -> u64 {
        self.credit
    }
    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }
    pub fn receipt(&self) -> Option<&TerminalReceipt> {
        self.receipt.as_ref()
    }

    /// Protocol-schema-only admission with an allow-all domain check. A
    /// `tool.v1` runtime route that settles `AgentToolResult` must instead use
    /// `receive_checked_with_final_time` with its host-bound typed decoder and
    /// receiver-local final clock: portable `integer` can admit a float-arm
    /// `1.0` that Rust `SafeUnsigned` cannot construct.
    /// The host generation is supplied separately from the peer frame.
    pub fn receive(
        &mut self,
        generation: u64,
        frame: &ActiveFrame,
        now_ms: u64,
    ) -> Result<ReceiveOutcome> {
        self.receive_checked(generation, frame, now_ms, |_| DomainDecision::Accept)
    }

    /// Calls the host's pure domain construction check after selected schema
    /// admission, before progress accounting or first-result retention.
    /// The callback must do no I/O or durable mutation. It returns mismatch
    /// only for a peer value it cannot construct. A local callback panic
    /// unwinds before state mutation; the host must catch it as its own Fault.
    /// A peer-value mismatch closes this invocation with a fixed category and
    /// no exposed output. This compatibility path holds `now_ms` fixed through
    /// validation; a runtime with a live monotonic deadline uses
    /// `receive_checked_with_final_time`.
    pub fn receive_checked<F>(
        &mut self,
        generation: u64,
        frame: &ActiveFrame,
        now_ms: u64,
        domain: F,
    ) -> Result<ReceiveOutcome>
    where
        F: FnMut(DomainCheck<'_>) -> DomainDecision,
    {
        self.receive_checked_with_final_time(generation, frame, now_ms, None, domain, || now_ms)
    }

    /// Rechecks receiver-local monotonic time after selected schema and host
    /// domain admission, at the progress or first-terminal state transition.
    ///
    /// `additional_deadline_ms` is a trusted absolute cutoff such as a local
    /// cancellation grace deadline. The host supplies the same nondecreasing
    /// clock used for ingress and its timer; both callbacks are synchronous and
    /// must not perform I/O, Session access, or reenter this invocation. The
    /// owner must serialize this call with its expiry and cancellation paths.
    /// Existing fixed-time callers may continue to use `receive_checked`.
    pub fn receive_checked_with_final_time<F, N>(
        &mut self,
        generation: u64,
        frame: &ActiveFrame,
        ingress_now_ms: u64,
        additional_deadline_ms: Option<u64>,
        domain: F,
        final_now_ms: N,
    ) -> Result<ReceiveOutcome>
    where
        F: FnMut(DomainCheck<'_>) -> DomainDecision,
        N: FnMut() -> u64,
    {
        self.receive_engine(
            generation,
            ReceiveSource::Owned(frame),
            ingress_now_ms,
            additional_deadline_ms,
            domain,
            final_now_ms,
        )
        .map_err(|error| match error {
            IndexedReceiveError::Protocol(error) => error,
            IndexedReceiveError::Local(_) => LiveError::HostDomainFault,
        })
    }

    /// Inert caller-backed receive after Ready. Selection and debit use this
    /// invocation's stored grant and the actual body, excluding its prefix.
    /// Outer peer errors leave the invocation untouched: the caller closes the
    /// supplied generation and calls `peer_lost` only on matching bindings.
    /// Local faults leave reached invocation state unchanged and belong to the host.
    #[allow(clippy::too_many_arguments)]
    pub fn receive_indexed_checked_with_final_time<F, N>(
        &mut self,
        generation: u64,
        transmitted_body: &[u8],
        arena: &mut BorrowedIndexArena<'_>,
        ingress_now_ms: u64,
        additional_deadline_ms: Option<u64>,
        domain: F,
        final_now_ms: N,
    ) -> IndexedResult<ReceiveOutcome>
    where
        F: FnMut(DomainCheck<'_>) -> DomainDecision,
        N: FnMut() -> u64,
    {
        self.receive_engine(
            generation,
            ReceiveSource::Indexed {
                body: transmitted_body,
                arena,
            },
            ingress_now_ms,
            additional_deadline_ms,
            domain,
            final_now_ms,
        )
    }

    fn receive_engine<F, N>(
        &mut self,
        generation: u64,
        source: ReceiveSource<'_, '_, '_, '_>,
        ingress_now_ms: u64,
        additional_deadline_ms: Option<u64>,
        mut domain: F,
        mut final_now_ms: N,
    ) -> IndexedResult<ReceiveOutcome>
    where
        F: FnMut(DomainCheck<'_>) -> DomainDecision,
        N: FnMut() -> u64,
    {
        let (owned, transmitted_body, mut arena) = match source {
            ReceiveSource::Owned(frame) => (Some(frame), None, None),
            ReceiveSource::Indexed { body, arena } => (None, Some(body), Some(arena)),
        };
        let body_len = owned.map_or_else(
            || transmitted_body.unwrap_or_default().len(),
            |f| f.body_len,
        );
        let debit = u64::try_from(body_len).map_err(|_| local_policy())?;
        let input = if let Some(frame) = owned {
            ReceiveInput::Owned(frame)
        } else {
            let body = transmitted_body.ok_or_else(local_policy)?;
            if body.is_empty() {
                return Err(LiveError::Frame(FrameError::ZeroLength).into());
            }
            if body_len > self.limits.frame.get() {
                return Err(LiveError::Frame(FrameError::FrameTooLarge {
                    length: debit,
                    limit: self.limits.frame.get(),
                })
                .into());
            }
            #[cfg(test)]
            if let Some(IndexedTestFault::Outer(error)) = self.indexed_test_fault {
                return Err(outer_scan_error(error));
            }
            let view = arena
                .as_deref_mut()
                .ok_or_else(local_policy)?
                .scan_envelope(body)
                .map_err(outer_scan_error)?;
            #[cfg(test)]
            if let Some(IndexedTestFault::Selection(error)) = self.indexed_test_fault {
                return Err(select_error(error));
            }
            let selected = echo_selected::select_active(
                view,
                selected_minor(self.core_minor),
                self.limits.frame,
            )
            .map_err(select_error)?;
            ReceiveInput::Indexed(selected)
        };
        if generation != self.binding.generation() {
            return Err(LiveError::WrongGeneration.into());
        }
        if matches!(
            self.phase,
            LivePhase::TerminalReceived | LivePhase::Retired | LivePhase::Fenced
        ) {
            return Ok(ReceiveOutcome::Stale);
        }
        if self.phase == LivePhase::PeerLost {
            return Err(LiveError::PeerLost.into());
        }
        if self.expire_with_additional(ingress_now_ms, additional_deadline_ms) {
            return Err(LiveError::PeerLost.into());
        }
        if body_len == 0 || body_len > self.limits.frame.get() {
            return self.receive_failure(IndexedReceiveError::Protocol(LiveError::Limit));
        }
        let header = match input.header(self.core_minor) {
            Ok(header) => header,
            Err(IndexedReceiveError::Protocol(LiveError::WrongVersion)) => {
                return Err(LiveError::WrongVersion.into());
            }
            Err(error) => return self.receive_failure(error),
        };
        match header {
            InboundHeader::Item {
                request_id,
                sequence,
            } => {
                if request_id != self.binding.request_id() {
                    return self
                        .receive_failure(IndexedReceiveError::Protocol(LiveError::WrongRequestId));
                }
                if sequence != self.next_sequence {
                    return self.receive_failure(IndexedReceiveError::Protocol(
                        LiveError::InvalidSequence,
                    ));
                }
                if debit > self.credit {
                    return self.receive_failure(IndexedReceiveError::Protocol(
                        LiveError::CreditViolation,
                    ));
                }
                let progress = match input {
                    ReceiveInput::Owned(frame) => {
                        let Inbound::Item { payload, .. } = owned_inbound(frame, self.core_minor)?
                        else {
                            return self.receive_failure(local_policy());
                        };
                        if payload.len() > self.limits.frame.get() {
                            return self.receive_failure(LiveError::Limit.into());
                        }
                        self.validate_progress(payload)
                            .map_err(IndexedReceiveError::from)
                    }
                    ReceiveInput::Indexed(selected) => {
                        let payload = match selected.into_progress_payload() {
                            Ok(payload) => payload,
                            Err(error) => return self.receive_failure(role_error(error)),
                        };
                        let admission = payload.admit(
                            arena.as_deref_mut().ok_or_else(local_policy)?,
                            selected_minor(self.tool_minor),
                            &selected_projection(self.projections.progress()),
                            self.limits.value,
                            self.limits.json,
                        );
                        #[cfg(test)]
                        let admission = self.inject_indexed_admission(admission);
                        admission
                            .map(|p| ValidatedProgress {
                                json: p.json,
                                percent_complete: p.percent_complete,
                            })
                            .map_err(admission_error)
                    }
                };
                let progress = match progress {
                    Ok(progress) => progress,
                    Err(error) => return self.receive_failure(error),
                };
                let projection = self.projections.progress();
                match domain(DomainCheck {
                    kind: DomainKind::Progress,
                    json: progress.json(),
                    binding: &self.binding,
                    profile_minor: self.tool_minor,
                    projection_id: projection.id(),
                    schema_digest_sha256: projection.digest(),
                }) {
                    DomainDecision::Accept => {}
                    DomainDecision::PeerValueMismatch => {
                        return self.receive_failure(IndexedReceiveError::Protocol(
                            LiveError::InvalidProgress,
                        ));
                    }
                    DomainDecision::HostFault => return Err(LiveError::HostDomainFault.into()),
                }
                let next_sequence = match self.next_sequence.checked_add(1) {
                    Some(next) => next,
                    None => {
                        return self.receive_failure(IndexedReceiveError::Protocol(
                            LiveError::InvalidSequence,
                        ));
                    }
                };
                let final_ms = final_now_ms();
                if self.expire_with_additional(final_ms, additional_deadline_ms) {
                    return Err(LiveError::PeerLost.into());
                }
                self.credit -= debit;
                self.next_sequence = next_sequence;
                if self.phase == LivePhase::CancelRequested {
                    Ok(ReceiveOutcome::IgnoredCancelledProgress)
                } else {
                    Ok(ReceiveOutcome::Progress(progress))
                }
            }
            InboundHeader::End { request_id } => {
                if request_id != self.binding.request_id() {
                    return self
                        .receive_failure(IndexedReceiveError::Protocol(LiveError::WrongRequestId));
                }
                let output = match input {
                    ReceiveInput::Owned(frame) => {
                        let Inbound::End { terminal, .. } = owned_inbound(frame, self.core_minor)?
                        else {
                            return self.receive_failure(local_policy());
                        };
                        self.validate_terminal(terminal)
                            .map_err(IndexedReceiveError::from)
                    }
                    ReceiveInput::Indexed(selected) => {
                        let terminal = match selected.into_result_terminal() {
                            Ok(terminal) => terminal,
                            Err(error) => return self.receive_failure(role_error(error)),
                        };
                        match terminal {
                            ResultTerminal::Result(payload) => {
                                let admission = payload.admit(
                                    arena.ok_or_else(local_policy)?,
                                    selected_minor(self.tool_minor),
                                    &selected_projection(self.projections.raw_result()),
                                    self.limits.value,
                                    self.limits.json,
                                );
                                #[cfg(test)]
                                let admission = self.inject_indexed_admission(admission);
                                admission
                                    .map(|result| TerminalOutput::Result(result.json))
                                    .map_err(admission_error)
                            }
                            ResultTerminal::ErrorTerminal(error) => error
                                .validate(|code, message| self.validate_error(code, message))
                                .map_err(role_error)?
                                .map_err(IndexedReceiveError::from),
                        }
                    }
                };
                let output = match output {
                    Ok(output) => output,
                    Err(error) => return self.receive_failure(error),
                };
                if let TerminalOutput::Result(ref json) = output {
                    let projection = self.projections.raw_result();
                    match domain(DomainCheck {
                        kind: DomainKind::Result,
                        json,
                        binding: &self.binding,
                        profile_minor: self.tool_minor,
                        projection_id: projection.id(),
                        schema_digest_sha256: projection.digest(),
                    }) {
                        DomainDecision::Accept => {}
                        DomainDecision::PeerValueMismatch => {
                            return self.receive_failure(IndexedReceiveError::Protocol(
                                LiveError::InvalidTerminal,
                            ));
                        }
                        DomainDecision::HostFault => return Err(LiveError::HostDomainFault.into()),
                    }
                }
                let binding = self.binding.clone();
                let final_ms = final_now_ms();
                if self.expire_with_additional(final_ms, additional_deadline_ms) {
                    return Err(LiveError::PeerLost.into());
                }
                self.receipt = Some(TerminalReceipt {
                    binding,
                    output,
                    received_ms: final_ms,
                });
                self.phase = LivePhase::TerminalReceived;
                Ok(ReceiveOutcome::TerminalReceived)
            }
        }
    }

    /// Additional outstanding credit uses checked addition and the host maximum.
    pub fn grant_credit(&mut self, additional: u64, now_ms: u64) -> Result<EncodedFrame> {
        if self.expire(now_ms) || self.phase == LivePhase::PeerLost {
            return Err(LiveError::PeerLost);
        }
        if self.phase != LivePhase::Open {
            return Err(LiveError::Stale);
        }
        let next = self
            .credit
            .checked_add(additional)
            .ok_or(LiveError::CreditViolation)?;
        if additional == 0 || next > self.limits.max_credit {
            return Err(LiveError::CreditViolation);
        }
        let encoded = if self.core_minor == 0 {
            framing::encode(
                &v1_0::Envelope {
                    frame: Some(v1_0::envelope::Frame::Credit(v1_0::Credit {
                        request_id: self.binding.request_id(),
                        additional_bytes: additional,
                    })),
                },
                self.limits.frame,
            )?
        } else {
            framing::encode(
                &v1_1::Envelope {
                    frame: Some(v1_1::envelope::Frame::Credit(v1_1::Credit {
                        request_id: self.binding.request_id(),
                        additional_bytes: additional,
                    })),
                },
                self.limits.frame,
            )?
        };
        self.credit = next;
        Ok(encoded)
    }

    /// The call to this method wins local arrival order against a later terminal.
    pub fn prepare_cancel(&self, reason: v1_1::CancelReason, now_ms: u64) -> Result<EncodedFrame> {
        if now_ms >= self.effective_deadline_ms() || self.phase == LivePhase::PeerLost {
            return Err(LiveError::PeerLost);
        }
        if self.phase != LivePhase::Open {
            return Err(LiveError::Stale);
        }
        if reason == v1_1::CancelReason::Unspecified {
            return Err(LiveError::UnsupportedVariant);
        }
        let encoded = if self.core_minor == 0 {
            framing::encode(
                &v1_0::Envelope {
                    frame: Some(v1_0::envelope::Frame::Cancel(v1_0::Cancel {
                        request_id: self.binding.request_id(),
                        reason: reason as i32,
                    })),
                },
                self.limits.frame,
            )?
        } else {
            framing::encode(
                &v1_1::Envelope {
                    frame: Some(v1_1::envelope::Frame::Cancel(v1_1::Cancel {
                        request_id: self.binding.request_id(),
                        reason: reason as i32,
                    })),
                },
                self.limits.frame,
            )?
        };
        Ok(encoded)
    }

    /// Commit only after the ordered writer has reserved the prepared frame.
    /// The caller serializes this with first-terminal receipt and expiry.
    pub fn commit_prepared_cancel(&mut self) {
        debug_assert_eq!(self.phase, LivePhase::Open);
        self.phase = LivePhase::CancelRequested;
    }

    pub fn cancel(&mut self, reason: v1_1::CancelReason, now_ms: u64) -> Result<EncodedFrame> {
        if self.expire(now_ms) {
            return Err(LiveError::PeerLost);
        }
        let encoded = self.prepare_cancel(reason, now_ms)?;
        self.commit_prepared_cancel();
        Ok(encoded)
    }

    /// Draining permits only this already-minted binding until its deadline.
    pub fn begin_drain(&mut self, drain_deadline_ms: u64) {
        if matches!(self.phase, LivePhase::Open | LivePhase::CancelRequested) {
            self.drain_deadline_ms = Some(drain_deadline_ms);
        }
    }

    /// Returns true only when a preterminal deadline abandons this binding.
    pub fn expire(&mut self, now_ms: u64) -> bool {
        if matches!(
            self.phase,
            LivePhase::TerminalReceived
                | LivePhase::Retired
                | LivePhase::PeerLost
                | LivePhase::Fenced
        ) {
            return false;
        }
        if now_ms >= self.deadline_ms
            || self
                .drain_deadline_ms
                .is_some_and(|deadline| now_ms >= deadline)
        {
            self.phase = LivePhase::PeerLost;
            return true;
        }
        false
    }

    fn expire_with_additional(&mut self, now_ms: u64, additional_deadline_ms: Option<u64>) -> bool {
        if self.expire(now_ms) {
            return true;
        }
        if additional_deadline_ms.is_some_and(|deadline| now_ms >= deadline)
            && matches!(self.phase, LivePhase::Open | LivePhase::CancelRequested)
        {
            self.peer_lost();
            return true;
        }
        false
    }

    /// Connection loss after a valid terminal does not remove its receipt.
    pub fn peer_lost(&mut self) {
        if matches!(self.phase, LivePhase::Open | LivePhase::CancelRequested) {
            self.phase = LivePhase::PeerLost;
        }
    }

    /// Fence a preterminal binding after the caller has checked that its full
    /// durable effect key is definitively no longer expected. No receipt is
    /// created, and a retained first terminal must use checked retirement.
    pub fn fence_no_longer_expected(&mut self, binding: &InvocationBinding) -> Result<()> {
        if !self.binding.same_live_identity(binding) {
            return Err(LiveError::WrongBinding);
        }
        if !matches!(self.phase, LivePhase::Open | LivePhase::CancelRequested) {
            return Err(LiveError::Stale);
        }
        self.receipt = None;
        self.phase = LivePhase::Fenced;
        Ok(())
    }

    /// Runtime calls this only after durable commit or no-longer-expected proof.
    pub fn retire(&mut self, binding: &InvocationBinding, _reason: RetireReason) -> Result<()> {
        if !self.binding.same_live_identity(binding) {
            return Err(LiveError::WrongBinding);
        }
        if self.phase != LivePhase::TerminalReceived {
            return Err(LiveError::Stale);
        }
        self.receipt = None;
        self.phase = LivePhase::Retired;
        Ok(())
    }

    fn malformed<T>(&mut self, error: LiveError) -> Result<T> {
        self.phase = LivePhase::PeerLost;
        self.receipt = None;
        Err(error)
    }

    fn validate_progress(&self, payload: &[u8]) -> Result<ValidatedProgress> {
        let projection = selected_projection(self.projections.progress());
        if self.tool_minor == 0 {
            // Prost 1.0 drops unknown field 2, so probe its frozen 1.1 presence
            // before selected 1.0 decode and reject instead of stripping it.
            let future =
                v1_1::ToolProgress::decode(payload).map_err(|_| LiveError::InvalidProgress)?;
            if future.percent_complete.is_some() {
                return Err(LiveError::UnsupportedVariant);
            }
            let progress =
                v1_0::ToolProgress::decode(payload).map_err(|_| LiveError::InvalidProgress)?;
            let event = progress.event.as_ref().ok_or(LiveError::InvalidProgress)?;
            let json = value::admit_schema_bound_v1_0(
                event,
                &projection,
                self.limits.value,
                self.limits.json,
            )?;
            Ok(ValidatedProgress {
                json,
                percent_complete: None,
            })
        } else {
            let progress =
                v1_1::ToolProgress::decode(payload).map_err(|_| LiveError::InvalidProgress)?;
            if progress
                .percent_complete
                .is_some_and(|percent| percent > 100)
            {
                return Err(LiveError::InvalidProgress);
            }
            let event = progress.event.as_ref().ok_or(LiveError::InvalidProgress)?;
            let json = value::admit_schema_bound_v1_1(
                event,
                &projection,
                self.limits.value,
                self.limits.json,
            )?;
            Ok(ValidatedProgress {
                json,
                percent_complete: progress.percent_complete,
            })
        }
    }

    fn validate_terminal(&self, terminal: TerminalView<'_>) -> Result<TerminalOutput> {
        match terminal {
            TerminalView::Result(payload) => {
                if payload.len() > self.limits.frame.get() {
                    return Err(LiveError::Limit);
                }
                let projection = selected_projection(self.projections.raw_result());
                let json = if self.tool_minor == 0 {
                    let result = v1_0::InvokeToolResult::decode(payload)
                        .map_err(|_| LiveError::InvalidTerminal)?;
                    value::admit_schema_bound_v1_0(
                        result
                            .raw_result
                            .as_ref()
                            .ok_or(LiveError::InvalidTerminal)?,
                        &projection,
                        self.limits.value,
                        self.limits.json,
                    )?
                } else {
                    let result = v1_1::InvokeToolResult::decode(payload)
                        .map_err(|_| LiveError::InvalidTerminal)?;
                    value::admit_schema_bound_v1_1(
                        result
                            .raw_result
                            .as_ref()
                            .ok_or(LiveError::InvalidTerminal)?,
                        &projection,
                        self.limits.value,
                        self.limits.json,
                    )?
                };
                Ok(TerminalOutput::Result(json))
            }
            TerminalView::Error { code, message } => self.validate_error(code, message),
        }
    }

    fn validate_error(&self, code: i32, message: &str) -> Result<TerminalOutput> {
        let code = v1_1::ErrorCode::try_from(code).map_err(|_| LiveError::UnsupportedVariant)?;
        if code == v1_1::ErrorCode::Unspecified {
            return Err(LiveError::UnsupportedVariant);
        }
        if message.len() > self.limits.string_bytes {
            return Err(LiveError::Limit);
        }
        Ok(TerminalOutput::Error { code })
    }

    #[cfg(test)]
    fn inject_indexed_admission<T>(
        &self,
        result: std::result::Result<T, AdmissionError>,
    ) -> std::result::Result<T, AdmissionError> {
        match self.indexed_test_fault {
            Some(IndexedTestFault::Inner(error)) => Err(error),
            _ => result,
        }
    }

    fn receive_failure<T>(&mut self, error: IndexedReceiveError) -> IndexedResult<T> {
        match error {
            IndexedReceiveError::Protocol(error) => self.malformed(error).map_err(Into::into),
            IndexedReceiveError::Local(_) => Err(error),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy)]
enum IndexedTestFault {
    Outer(ScanError),
    Selection(SelectError),
    Inner(AdmissionError),
}

enum ReceiveSource<'frame, 'body, 'arena, 'storage> {
    Owned(&'frame ActiveFrame),
    Indexed {
        body: &'body [u8],
        arena: &'arena mut BorrowedIndexArena<'storage>,
    },
}

enum ReceiveInput<'frame, 'view, 'body> {
    Owned(&'frame ActiveFrame),
    Indexed(SelectedActive<'view, 'body>),
}

enum InboundHeader {
    Item { request_id: u64, sequence: u64 },
    End { request_id: u64 },
}

impl ReceiveInput<'_, '_, '_> {
    fn header(&self, core_minor: u32) -> IndexedResult<InboundHeader> {
        match self {
            Self::Owned(frame) => Ok(match owned_inbound(frame, core_minor)? {
                Inbound::Item {
                    request_id,
                    sequence,
                    ..
                } => InboundHeader::Item {
                    request_id,
                    sequence,
                },
                Inbound::End { request_id, .. } => InboundHeader::End { request_id },
            }),
            Self::Indexed(selected) => match selected.arm() {
                ActiveArm::StreamItem => Ok(InboundHeader::Item {
                    request_id: selected.request_id().map_err(role_error)?,
                    sequence: selected.sequence().map_err(role_error)?,
                }),
                ActiveArm::StreamEnd => {
                    if !selected.terminal_present().map_err(role_error)? {
                        return Err(LiveError::InvalidTerminal.into());
                    }
                    Ok(InboundHeader::End {
                        request_id: selected.request_id().map_err(role_error)?,
                    })
                }
                _ => Err(LiveError::InvalidTerminal.into()),
            },
        }
    }
}

fn selected_minor(minor: u32) -> SelectedMinor {
    if minor == 0 {
        SelectedMinor::V1_0
    } else {
        SelectedMinor::V1_1
    }
}
fn local_policy() -> IndexedReceiveError {
    IndexedReceiveError::Local(IndexedLocalFault::InternalPolicy)
}
fn outer_scan_error(error: ScanError) -> IndexedReceiveError {
    match error {
        ScanError::Malformed => LiveError::Frame(FrameError::InvalidEnvelope).into(),
        ScanError::UnsupportedVariant => LiveError::UnsupportedVariant.into(),
        ScanError::Limit => IndexedReceiveError::Local(IndexedLocalFault::ScanLimit),
        ScanError::Allocation => IndexedReceiveError::Local(IndexedLocalFault::ScanAllocation),
    }
}
fn select_error(error: SelectError) -> IndexedReceiveError {
    match error {
        SelectError::UnexpectedFrame => LiveError::UnexpectedFrame.into(),
        SelectError::FrameTooLarge { length, limit } => {
            LiveError::Frame(FrameError::FrameTooLarge { length, limit }).into()
        }
        SelectError::InternalPolicy => local_policy(),
    }
}
fn role_error(error: RoleError) -> IndexedReceiveError {
    match error {
        RoleError::InvalidTerminal => LiveError::InvalidTerminal.into(),
        RoleError::WrongArm | RoleError::InternalPolicy => local_policy(),
    }
}
fn admission_error(error: AdmissionError) -> IndexedReceiveError {
    match error {
        AdmissionError::InvalidProgress => LiveError::InvalidProgress.into(),
        AdmissionError::InvalidTerminal => LiveError::InvalidTerminal.into(),
        AdmissionError::UnsupportedVariant => LiveError::UnsupportedVariant.into(),
        AdmissionError::Value(error) => LiveError::Value(error).into(),
        AdmissionError::InternalPolicy => local_policy(),
        AdmissionError::LocalResource(error) => outer_scan_error(error),
    }
}

fn owned_inbound(frame: &ActiveFrame, core_minor: u32) -> Result<Inbound<'_>> {
    Ok(match (&frame.envelope, core_minor) {
        (SelectedEnvelope::V1_0(e), 0) => match e.frame.as_ref() {
            Some(v1_0::envelope::Frame::StreamItem(item)) => Inbound::Item {
                request_id: item.request_id,
                sequence: item.sequence,
                payload: &item.payload,
            },
            Some(v1_0::envelope::Frame::StreamEnd(end)) => Inbound::End {
                request_id: end.request_id,
                terminal: match end.terminal.as_ref() {
                    Some(v1_0::stream_end::Terminal::Result(bytes)) => TerminalView::Result(bytes),
                    Some(v1_0::stream_end::Terminal::Error(error)) => TerminalView::Error {
                        code: error.code,
                        message: &error.safe_message,
                    },
                    None => return Err(LiveError::InvalidTerminal),
                },
            },
            _ => return Err(LiveError::InvalidTerminal),
        },
        (SelectedEnvelope::V1_1(e), 1) => match e.frame.as_ref() {
            Some(v1_1::envelope::Frame::StreamItem(item)) => Inbound::Item {
                request_id: item.request_id,
                sequence: item.sequence,
                payload: &item.payload,
            },
            Some(v1_1::envelope::Frame::StreamEnd(end)) => Inbound::End {
                request_id: end.request_id,
                terminal: match end.terminal.as_ref() {
                    Some(v1_1::stream_end::Terminal::Result(bytes)) => TerminalView::Result(bytes),
                    Some(v1_1::stream_end::Terminal::Error(error)) => TerminalView::Error {
                        code: error.code,
                        message: &error.safe_message,
                    },
                    None => return Err(LiveError::InvalidTerminal),
                },
            },
            _ => return Err(LiveError::InvalidTerminal),
        },
        _ => return Err(LiveError::WrongVersion),
    })
}

enum Inbound<'a> {
    Item {
        request_id: u64,
        sequence: u64,
        payload: &'a [u8],
    },
    End {
        request_id: u64,
        terminal: TerminalView<'a>,
    },
}

enum TerminalView<'a> {
    Result(&'a [u8]),
    Error { code: i32, message: &'a str },
}

fn selected_projection(projection: &CatalogProjection) -> value::Projection<'_> {
    value::Projection {
        id: projection.id(),
        digest_sha256: projection.digest(),
        schema: projection.schema(),
    }
}

fn valid_effect_key(key: &v1_1::EffectKey, max_string: usize) -> bool {
    [
        &key.session_id,
        &key.lane,
        &key.operation_id,
        &key.step_or_task_id,
    ]
    .into_iter()
    .all(|field| !field.is_empty() && field.len() <= max_string)
        && key.effect_kind == "tool"
        && key.logical_attempt <= MAX_SAFE_UNSIGNED
        && key
            .item_index
            .is_some_and(|index| index <= MAX_SAFE_UNSIGNED)
}

fn encode_inner<M: Message>(message: &M, limit: NonZeroUsize) -> Result<Vec<u8>> {
    let length = message.encoded_len();
    if length > limit.get() {
        return Err(LiveError::Limit);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| LiveError::Limit)?;
    message
        .encode(&mut bytes)
        .map_err(|_| LiveError::InvalidTerminal)?;
    Ok(bytes)
}

fn convert_message<T: Message + Default, M: Message>(
    message: &M,
    limit: NonZeroUsize,
) -> Result<T> {
    let bytes = encode_inner(message, limit)?;
    T::decode(bytes.as_slice()).map_err(|_| LiveError::WrongVersion)
}

#[cfg(test)]
mod indexed_tests {
    use super::*;
    use crate::activation::{
        Activation, AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolSchemaInputs,
    };
    use crate::wire::echo_bounded::IndexStorage;
    use serde_json::Value;
    use std::cell::Cell;

    fn unhex(s: &str) -> Vec<u8> {
        s.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
            .collect()
    }
    fn invocation(core: u32, tool: u32) -> ToolInvocation {
        let catalog: Value =
            serde_json::from_str(include_str!("../schema/fixtures/host-catalog.json")).unwrap();
        let entry = &catalog["entries"][0];
        let descriptor = &entry["agentTool"];
        let approved = ApprovedTool::new(
            "peer-a".into(),
            AgentTool {
                name: "echo".into(),
                label: descriptor["label"].as_str().unwrap().into(),
                description: descriptor["description"].as_str().unwrap().into(),
                parameters: descriptor["parameters"].clone(),
                execution_mode: AgentToolMode::Sequential,
                replay: ToolReplay::Safe,
            },
            [0, 1]
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
                .collect(),
            jcs::Limits::default(),
        )
        .unwrap();
        let l = v1_1::ProtocolLimits {
            pre_handshake_frame_bytes: 4096,
            frame_bytes: 4096,
            aggregate_decoded_bytes: 8192,
            value_depth: 32,
            value_elements: 1024,
            string_bytes: 4096,
            bytes_length: 4096,
            in_flight_requests: 1,
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
        let host =
            HostAuthority::new("peer-a".into(), 7, core, tool, l, vec![approved], vec![]).unwrap();
        let mut a = Activation::new(host, 100);
        a.receive_hello(
            &v1_1::Hello {
                protocol: Some(v1_1::VersionRange {
                    major: 1,
                    min_minor: 0,
                    max_minor: 1,
                }),
                supported_profiles: vec![v1_1::ProfileRange {
                    name: "tool.v1".into(),
                    major: 1,
                    min_minor: 0,
                    max_minor: 1,
                }],
            }
            .encode_to_vec(),
            101,
        )
        .unwrap();
        let vectors: Value =
            serde_json::from_str(include_str!("../schema/fixtures/activation-vectors.json"))
                .unwrap();
        a.receive_manifest(
            &unhex(
                vectors["cases"]["valid_1_1"]["manifest_pb_hex"]
                    .as_str()
                    .unwrap(),
            ),
            102,
        )
        .unwrap();
        a.receive_ready(&[], 103).unwrap();
        let raw: Value =
            serde_json::from_str(include_str!("../schema/fixtures/raw-vectors.json")).unwrap();
        let envelope =
            v1_1::Envelope::decode(unhex(raw["request"]["body_hex"].as_str().unwrap()).as_slice())
                .unwrap();
        let Some(v1_1::envelope::Frame::Request(request)) = envelope.frame else {
            panic!()
        };
        let inner = v1_1::InvokeToolRequest::decode(request.payload.as_slice()).unwrap();
        let arguments = inner.arguments.unwrap();
        let arguments = if tool == 0 {
            SelectedArguments::V1_0(
                v1_0::SchemaBoundValue::decode(arguments.encode_to_vec().as_slice()).unwrap(),
            )
        } else {
            SelectedArguments::V1_1(arguments)
        };
        ToolInvocation::start(
            a.active_grant().unwrap(),
            "echo",
            Start {
                effect_key: request.effect_key.unwrap(),
                source_call_id: inner.tool_call_id,
                request_id: 17,
                active_generation: 7,
                arguments,
                now_ms: 200,
                requested_deadline_after_ms: Some(300),
                method_limit_ms: 300,
                connection_remaining_ms: 300,
                parent_remaining_ms: None,
                initial_credit_bytes: Some(4096),
            },
        )
        .unwrap()
        .0
    }
    fn body(progress: bool, sequence: u64) -> Vec<u8> {
        let raw: Value =
            serde_json::from_str(include_str!("../schema/fixtures/raw-vectors.json")).unwrap();
        let mut envelope = v1_1::Envelope::decode(
            unhex(
                raw[if progress { "progress" } else { "terminal" }]["body_hex"]
                    .as_str()
                    .unwrap(),
            )
            .as_slice(),
        )
        .unwrap();
        if let Some(v1_1::envelope::Frame::StreamItem(item)) = &mut envelope.frame {
            item.sequence = sequence;
        }
        envelope.encode_to_vec()
    }
    #[test]
    fn indexed_fault_classifiers_run_common_path_without_mutation_or_clock() {
        let exhausted = echo_selected::exhausted_scan_fixture();
        for core in [0, 1] {
            for tool in [0, 1] {
                for progress in [true, false] {
                    for (fault, expected) in [
                        (
                            IndexedTestFault::Outer(exhausted),
                            IndexedLocalFault::ScanLimit,
                        ),
                        (
                            IndexedTestFault::Outer(ScanError::Allocation),
                            IndexedLocalFault::ScanAllocation,
                        ),
                        (
                            IndexedTestFault::Selection(SelectError::InternalPolicy),
                            IndexedLocalFault::InternalPolicy,
                        ),
                        (
                            IndexedTestFault::Inner(AdmissionError::LocalResource(exhausted)),
                            IndexedLocalFault::ScanLimit,
                        ),
                        (
                            IndexedTestFault::Inner(AdmissionError::LocalResource(
                                ScanError::Allocation,
                            )),
                            IndexedLocalFault::ScanAllocation,
                        ),
                        (
                            IndexedTestFault::Inner(AdmissionError::InternalPolicy),
                            IndexedLocalFault::InternalPolicy,
                        ),
                    ] {
                        let mut live = invocation(core, tool);
                        live.indexed_test_fault = Some(fault);
                        let mut slot = IndexStorage::<1024, 1025>::new();
                        let mut arena = slot
                            .borrow(NonZeroUsize::new(1024).unwrap(), usize::MAX)
                            .unwrap();
                        let domain = Cell::new(0);
                        let clock = Cell::new(0);
                        let result = live.receive_indexed_checked_with_final_time(
                            7,
                            &body(progress, 0),
                            &mut arena,
                            201,
                            None,
                            |_| {
                                domain.set(domain.get() + 1);
                                DomainDecision::Accept
                            },
                            || {
                                clock.set(clock.get() + 1);
                                201
                            },
                        );
                        assert_eq!(result.err(), Some(IndexedReceiveError::Local(expected)));
                        assert_eq!(
                            (
                                live.phase(),
                                live.credit(),
                                live.next_sequence(),
                                domain.get(),
                                clock.get()
                            ),
                            (LivePhase::Open, 4096, 0, 0, 0)
                        );
                        assert!(live.receipt().is_none());
                    }
                }
            }
        }
    }
    #[test]
    fn indexed_sequence_overflow_after_domain_before_final_clock_and_debit() {
        for core in [0, 1] {
            for tool in [0, 1] {
                let mut live = invocation(core, tool);
                live.next_sequence = u64::MAX;
                let mut slot = IndexStorage::<1024, 1025>::new();
                let mut arena = slot
                    .borrow(NonZeroUsize::new(1024).unwrap(), usize::MAX)
                    .unwrap();
                let calls = Cell::new(0);
                let result = live.receive_indexed_checked_with_final_time(
                    7,
                    &body(true, u64::MAX),
                    &mut arena,
                    201,
                    None,
                    |check| {
                        calls.set(calls.get() + 1);
                        assert_eq!(check.kind(), DomainKind::Progress);
                        DomainDecision::Accept
                    },
                    || panic!("overflow skips final clock"),
                );
                assert_eq!(result.err(), Some(LiveError::InvalidSequence.into()));
                assert_eq!(calls.get(), 1);
                assert_eq!(
                    (live.phase(), live.credit(), live.next_sequence()),
                    (LivePhase::PeerLost, 4096, u64::MAX)
                );
                assert!(live.receipt().is_none());
            }
        }
    }
    #[test]
    fn indexed_invocation_exact_json_jcs_and_value_first_error_boundaries() {
        for core in [0, 1] {
            for tool in [0, 1] {
                for progress in [true, false] {
                    let raw = body(progress, 0);
                    let envelope = v1_1::Envelope::decode(raw.as_slice()).unwrap();
                    let oracle = ActiveFrame {
                        envelope: SelectedEnvelope::V1_1(envelope),
                        body_len: raw.len(),
                    };
                    let source = invocation(core, tool);
                    let expected = if progress {
                        let Inbound::Item { payload, .. } = owned_inbound(&oracle, 1).unwrap()
                        else {
                            panic!()
                        };
                        source.validate_progress(payload).unwrap().json
                    } else {
                        let Inbound::End { terminal, .. } = owned_inbound(&oracle, 1).unwrap()
                        else {
                            panic!()
                        };
                        let TerminalOutput::Result(json) =
                            source.validate_terminal(terminal).unwrap()
                        else {
                            panic!()
                        };
                        json
                    };
                    for json_gate in [true, false] {
                        for fits in [true, false] {
                            let mut live = invocation(core, tool);
                            let ceiling = expected.len() - usize::from(!fits);
                            if json_gate {
                                live.limits.value.json_bytes = ceiling;
                            } else {
                                live.limits.json.input_bytes = ceiling;
                            }
                            let mut slot = IndexStorage::<1024, 1025>::new();
                            let mut arena = slot
                                .borrow(NonZeroUsize::new(1024).unwrap(), usize::MAX)
                                .unwrap();
                            let calls = Cell::new(0);
                            let clock = Cell::new(0);
                            let result = live.receive_indexed_checked_with_final_time(
                                7,
                                &raw,
                                &mut arena,
                                201,
                                None,
                                |check| {
                                    calls.set(calls.get() + 1);
                                    assert_eq!(check.json(), expected);
                                    DomainDecision::Accept
                                },
                                || {
                                    clock.set(clock.get() + 1);
                                    201
                                },
                            );
                            if fits {
                                assert!(result.is_ok());
                                assert_eq!((calls.get(), clock.get()), (1, 1));
                            } else {
                                let error = if json_gate {
                                    value::Error::Limit
                                } else {
                                    value::Error::Schema(jcs::Error::Limit)
                                };
                                assert_eq!(result.err(), Some(LiveError::Value(error).into()));
                                assert_eq!(
                                    (
                                        calls.get(),
                                        clock.get(),
                                        live.credit(),
                                        live.next_sequence()
                                    ),
                                    (0, 0, 4096, 0)
                                );
                                assert!(live.receipt().is_none());
                            }
                        }
                    }
                    // Projection wins before missing value; full known-wire scanning
                    // already happened before semantic value/instance admission.
                    let mut live = invocation(core, tool);
                    live.limits.value.depth = 1;
                    let mut slot = IndexStorage::<1024, 1025>::new();
                    let mut arena = slot
                        .borrow(NonZeroUsize::new(1024).unwrap(), usize::MAX)
                        .unwrap();
                    let result = live.receive_indexed_checked_with_final_time(
                        7,
                        &raw,
                        &mut arena,
                        201,
                        None,
                        |_| panic!("depth precedes domain"),
                        || panic!("depth precedes clock"),
                    );
                    assert_eq!(
                        result.err(),
                        Some(LiveError::Value(value::Error::Limit).into())
                    );
                    assert_eq!(live.credit(), 4096);
                    assert!(live.receipt().is_none());
                }
            }
        }
    }
}
