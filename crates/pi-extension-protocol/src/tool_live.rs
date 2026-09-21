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
use crate::wire::framing::{self, EncodedFrame, FrameError};
use crate::wire::value;
use crate::wire::{v1_0, v1_1};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LiveError {
    InvalidHostBinding,
    WrongGeneration,
    WrongRequestId,
    WrongVersion,
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
    /// `receive_checked` with its host-bound typed decoder: portable `integer`
    /// can admit a float-arm `1.0` that Rust `SafeUnsigned` cannot construct.
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
    /// no exposed output.
    pub fn receive_checked<F>(
        &mut self,
        generation: u64,
        frame: &ActiveFrame,
        now_ms: u64,
        mut domain: F,
    ) -> Result<ReceiveOutcome>
    where
        F: FnMut(DomainCheck<'_>) -> DomainDecision,
    {
        if generation != self.binding.generation() {
            return Err(LiveError::WrongGeneration);
        }
        if matches!(self.phase, LivePhase::TerminalReceived | LivePhase::Retired) {
            return Ok(ReceiveOutcome::Stale);
        }
        if self.phase == LivePhase::PeerLost {
            return Err(LiveError::PeerLost);
        }
        if self.expire(now_ms) {
            return Err(LiveError::PeerLost);
        }
        if frame.body_len == 0 || frame.body_len > self.limits.frame.get() {
            return self.malformed(LiveError::Limit);
        }
        let inbound = match (&frame.envelope, self.core_minor) {
            (SelectedEnvelope::V1_0(e), 0) => match e.frame.as_ref() {
                Some(v1_0::envelope::Frame::StreamItem(item)) => Inbound::Item {
                    request_id: item.request_id,
                    sequence: item.sequence,
                    payload: &item.payload,
                },
                Some(v1_0::envelope::Frame::StreamEnd(end)) => Inbound::End {
                    request_id: end.request_id,
                    terminal: match end.terminal.as_ref() {
                        Some(v1_0::stream_end::Terminal::Result(bytes)) => {
                            TerminalView::Result(bytes)
                        }
                        Some(v1_0::stream_end::Terminal::Error(error)) => TerminalView::Error {
                            code: error.code,
                            message: &error.safe_message,
                        },
                        None => return self.malformed(LiveError::InvalidTerminal),
                    },
                },
                _ => return self.malformed(LiveError::InvalidTerminal),
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
                        Some(v1_1::stream_end::Terminal::Result(bytes)) => {
                            TerminalView::Result(bytes)
                        }
                        Some(v1_1::stream_end::Terminal::Error(error)) => TerminalView::Error {
                            code: error.code,
                            message: &error.safe_message,
                        },
                        None => return self.malformed(LiveError::InvalidTerminal),
                    },
                },
                _ => return self.malformed(LiveError::InvalidTerminal),
            },
            _ => return Err(LiveError::WrongVersion),
        };
        match inbound {
            Inbound::Item {
                request_id,
                sequence,
                payload,
            } => {
                if request_id != self.binding.request_id() {
                    return self.malformed(LiveError::WrongRequestId);
                }
                if sequence != self.next_sequence {
                    return self.malformed(LiveError::InvalidSequence);
                }
                if frame.body_len as u64 > self.credit {
                    return self.malformed(LiveError::CreditViolation);
                }
                if payload.len() > self.limits.frame.get() {
                    return self.malformed(LiveError::Limit);
                }
                let progress = match self.validate_progress(payload) {
                    Ok(progress) => progress,
                    Err(error) => return self.malformed(error),
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
                        return self.malformed(LiveError::InvalidProgress);
                    }
                    DomainDecision::HostFault => return Err(LiveError::HostDomainFault),
                }
                self.credit -= frame.body_len as u64;
                self.next_sequence = match self.next_sequence.checked_add(1) {
                    Some(next) => next,
                    None => return self.malformed(LiveError::InvalidSequence),
                };
                if self.phase == LivePhase::CancelRequested {
                    Ok(ReceiveOutcome::IgnoredCancelledProgress)
                } else {
                    Ok(ReceiveOutcome::Progress(progress))
                }
            }
            Inbound::End {
                request_id,
                terminal,
            } => {
                if request_id != self.binding.request_id() {
                    return self.malformed(LiveError::WrongRequestId);
                }
                let output = match self.validate_terminal(terminal) {
                    Ok(output) => output,
                    Err(error) => return self.malformed(error),
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
                            return self.malformed(LiveError::InvalidTerminal);
                        }
                        DomainDecision::HostFault => return Err(LiveError::HostDomainFault),
                    }
                }
                self.receipt = Some(TerminalReceipt {
                    binding: self.binding.clone(),
                    output,
                    received_ms: now_ms,
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
    pub fn cancel(&mut self, reason: v1_1::CancelReason, now_ms: u64) -> Result<EncodedFrame> {
        if self.expire(now_ms) || self.phase == LivePhase::PeerLost {
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
        self.phase = LivePhase::CancelRequested;
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
            LivePhase::TerminalReceived | LivePhase::Retired | LivePhase::PeerLost
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

    /// Connection loss after a valid terminal does not remove its receipt.
    pub fn peer_lost(&mut self) {
        if matches!(self.phase, LivePhase::Open | LivePhase::CancelRequested) {
            self.phase = LivePhase::PeerLost;
        }
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
            TerminalView::Error { code, message } => {
                let code =
                    v1_1::ErrorCode::try_from(code).map_err(|_| LiveError::UnsupportedVariant)?;
                if code == v1_1::ErrorCode::Unspecified {
                    return Err(LiveError::UnsupportedVariant);
                }
                if message.len() > self.limits.string_bytes {
                    return Err(LiveError::Limit);
                }
                Ok(TerminalOutput::Error { code })
            }
        }
    }
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
