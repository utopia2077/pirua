//! Host-bound, typed echo result admission before protocol receipt retention.
//!
//! This adapter neither reads a channel nor writes Session state. The caller
//! binds one current grant to one host-minted invocation before receiving peer
//! frames. The protocol supplies only already-bounded, schema-admitted JSON to
//! the synchronous check.

use std::fmt;

use pi_agent_schema::{AgentToolResult, AgentToolUpdate};
use pi_extension_protocol::activation::ActiveGrant;
use pi_extension_protocol::endpoint::ActiveFrame;
use pi_extension_protocol::tool_live::{
    DomainCheck, DomainDecision, DomainKind, InvocationBinding, LiveError, ReceiveOutcome,
    ToolInvocation,
};

const ECHO: &str = "echo";
const PROGRESS_ID: &str = "tool.progress:echo";
const RESULT_ID: &str = "tool.result:echo";
// Canonical raw SHA-256 of the frozen echo progress/result schema in both tool
// minors. This profile mapping cannot be widened by a host descriptor alone.
const ECHO_DOMAIN_DIGEST: [u8; 32] = [
    0xa0, 0x8e, 0x48, 0xcf, 0x53, 0x21, 0x24, 0x18, 0x17, 0xee, 0xfd, 0x20, 0x59, 0x3e, 0xa2, 0x7e,
    0xf0, 0x0f, 0x7e, 0xff, 0xb1, 0x11, 0x87, 0x96, 0xab, 0xe2, 0xda, 0x13, 0x4e, 0xa0, 0x03, 0x35,
];

/// Local binding/configuration failures are separate from peer-value mismatch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DomainHostError {
    Binding,
    Projection,
}

impl fmt::Display for DomainHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Binding => "host tool domain binding mismatch",
            Self::Projection => "host tool domain projection mismatch",
        })
    }
}

impl std::error::Error for DomainHostError {}

#[derive(Debug)]
pub enum DomainReceiveError {
    Host(DomainHostError),
    /// A bound invocation expired before a progress or terminal transition.
    PeerLost,
    Protocol(LiveError),
}

impl fmt::Display for DomainReceiveError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Host(error) => error.fmt(formatter),
            Self::PeerLost => formatter.write_str("peer lost before terminal"),
            Self::Protocol(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DomainReceiveError {}

/// A bounded typed value is returned only for accepted progress/result frames.
/// A peer error terminal has no Rust result body; cancelled progress is dropped.
pub struct CheckedEchoFrame {
    pub outcome: ReceiveOutcome,
    pub value: Option<AgentToolResult>,
}

/// Immutable per-invocation mapping from the active host grant to the Rust
/// `AgentToolResult`/`AgentToolUpdate` shape.
pub struct EchoDomainBinding {
    binding: InvocationBinding,
    tool_minor: u32,
    json_max: usize,
}

impl EchoDomainBinding {
    /// Grant-only echo projection pin. Registration calls this before making
    /// the handle visible; invocation binding repeats it before dispatch.
    pub(crate) fn check_ready_echo_projections(grant: &ActiveGrant) -> Result<(), DomainHostError> {
        let tool = grant
            .tools()
            .iter()
            .find(|tool| tool.descriptor().name == ECHO)
            .ok_or(DomainHostError::Binding)?;
        let projections = tool.projections();
        let progress = projections.progress();
        let result = projections.raw_result();
        if progress.id() != PROGRESS_ID
            || result.id() != RESULT_ID
            || progress.digest() != ECHO_DOMAIN_DIGEST
            || result.digest() != ECHO_DOMAIN_DIGEST
            || progress.digest() != progress.schema().canonical().sha256()
            || result.digest() != result.schema().canonical().sha256()
        {
            return Err(DomainHostError::Projection);
        }
        Ok(())
    }

    /// Bind before dispatch. A host catalog with a different echo domain is not
    /// suitable for this adapter even if activation approved its wire schema.
    pub fn new(grant: &ActiveGrant, invocation: &ToolInvocation) -> Result<Self, DomainHostError> {
        let wire = grant.wire();
        let selected = wire.selected_protocol.ok_or(DomainHostError::Binding)?;
        let host_limits = wire.limits.as_ref().ok_or(DomainHostError::Binding)?;
        let tool = grant
            .tools()
            .iter()
            .find(|tool| tool.descriptor().name == ECHO)
            .ok_or(DomainHostError::Binding)?;
        let reference = tool.reference();
        let profile = reference.profile.as_ref().ok_or(DomainHostError::Binding)?;
        let binding = invocation.binding();
        if selected.major != 1
            || selected.minor > 1
            || profile.name != "tool.v1"
            || profile.major != 1
            || profile.minor > 1
            || reference.kind != "tool"
            || reference.name != ECHO
            || reference.authenticated_extension_id != wire.authenticated_extension_id
            || !wire.selected_capabilities.contains(reference)
            || binding.capability_ref() != reference
            || binding.generation() != wire.connection_generation
            || binding.request_id() == 0
        {
            return Err(DomainHostError::Binding);
        }
        Self::check_ready_echo_projections(grant)?;
        let frame =
            usize::try_from(host_limits.frame_bytes).map_err(|_| DomainHostError::Binding)?;
        let decoded = usize::try_from(host_limits.aggregate_decoded_bytes)
            .map_err(|_| DomainHostError::Binding)?;
        let json_max = frame.min(decoded);
        if json_max == 0 {
            return Err(DomainHostError::Binding);
        }
        Ok(Self {
            binding: binding.clone(),
            tool_minor: profile.minor,
            json_max,
        })
    }

    /// The host-only failure path is checked before `ToolInvocation` can mutate
    /// its credit, sequence, phase or receipt. A peer parse error is the sole
    /// source of `PeerValueMismatch` passed to the protocol.
    pub fn receive_checked(
        &self,
        invocation: &mut ToolInvocation,
        generation: u64,
        frame: &ActiveFrame,
        now_ms: u64,
    ) -> Result<CheckedEchoFrame, DomainReceiveError> {
        self.receive_checked_with_final_time(invocation, generation, frame, now_ms, None, || now_ms)
            .map_err(|error| match error {
                DomainReceiveError::PeerLost => DomainReceiveError::Protocol(LiveError::PeerLost),
                other => other,
            })
    }

    /// Admit a typed echo value, then sample trusted monotonic time at the
    /// protocol's progress or first-terminal state transition.
    ///
    /// The caller serializes this synchronous call with its timer and local
    /// cancellation path. A trusted additional cutoff may represent the
    /// cancellation-grace deadline; neither callback performs I/O or Session
    /// access. Unlike the fixed-time compatibility method, preterminal loss is
    /// exposed separately from malformed peer input.
    pub fn receive_checked_with_final_time<N>(
        &self,
        invocation: &mut ToolInvocation,
        generation: u64,
        frame: &ActiveFrame,
        ingress_now_ms: u64,
        additional_deadline_ms: Option<u64>,
        final_now_ms: N,
    ) -> Result<CheckedEchoFrame, DomainReceiveError>
    where
        N: FnMut() -> u64,
    {
        if !self.binding.same_live_identity(invocation.binding()) {
            return Err(DomainReceiveError::Host(DomainHostError::Binding));
        }
        let mut typed = None;
        let mut host_error = None;
        let result = invocation.receive_checked_with_final_time(
            generation,
            frame,
            ingress_now_ms,
            additional_deadline_ms,
            |check| match self.construct(check) {
                Ok(value) => {
                    typed = value;
                    DomainDecision::Accept
                }
                Err(CheckError::PeerValue) => DomainDecision::PeerValueMismatch,
                Err(CheckError::Host(error)) => {
                    host_error = Some(error);
                    DomainDecision::HostFault
                }
            },
            final_now_ms,
        );
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(LiveError::HostDomainFault) => {
                return Err(DomainReceiveError::Host(
                    host_error.unwrap_or(DomainHostError::Binding),
                ));
            }
            Err(LiveError::PeerLost) => return Err(DomainReceiveError::PeerLost),
            Err(error) => return Err(DomainReceiveError::Protocol(error)),
        };
        if matches!(outcome, ReceiveOutcome::IgnoredCancelledProgress) {
            typed = None;
        }
        Ok(CheckedEchoFrame {
            outcome,
            value: typed,
        })
    }

    fn construct(&self, check: DomainCheck<'_>) -> Result<Option<AgentToolResult>, CheckError> {
        let id = match check.kind() {
            DomainKind::Progress => PROGRESS_ID,
            DomainKind::Result => RESULT_ID,
        };
        if !self.binding.same_live_identity(check.binding())
            || self.tool_minor != check.profile_minor()
            || check.json().len() > self.json_max
        {
            return Err(CheckError::Host(DomainHostError::Binding));
        }
        if check.projection_id() != id || check.schema_digest_sha256() != ECHO_DOMAIN_DIGEST {
            return Err(CheckError::Host(DomainHostError::Projection));
        }
        let value = match check.kind() {
            DomainKind::Progress => serde_json::from_slice::<AgentToolUpdate>(check.json()),
            DomainKind::Result => serde_json::from_slice::<AgentToolResult>(check.json()),
        }
        .map_err(|_| CheckError::PeerValue)?;
        Ok(Some(value))
    }
}

enum CheckError {
    Host(DomainHostError),
    PeerValue,
}
