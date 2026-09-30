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
    DomainCheck, DomainDecision, DomainKind, IndexedLocalFault, IndexedReceiveError,
    InvocationBinding, LiveError, ReceiveOutcome, ToolInvocation,
};

use pi_extension_protocol::wire::echo_bounded::BorrowedIndexArena;

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
    Indexed(IndexedLocalFault),
}

impl fmt::Display for DomainHostError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Binding => "host tool domain binding mismatch",
            Self::Projection => "host tool domain projection mismatch",
            Self::Indexed(error) => return error.fmt(formatter),
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

/// A receive caller's measured body. The length is captured before indexed
/// scanning so credit and queue reservations cannot be based on metadata or a
/// later normalized representation.
pub(crate) struct StagedIndexedBody<'a> {
    bytes: &'a [u8],
    body_len: usize,
}

impl<'a> StagedIndexedBody<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Result<Self, DomainHostError> {
        if bytes.is_empty() {
            return Err(DomainHostError::Binding);
        }
        Ok(Self {
            bytes,
            body_len: bytes.len(),
        })
    }

    pub(crate) fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    pub(crate) fn body_len(&self) -> usize {
        self.body_len
    }
}

/// Immutable per-invocation mapping from the active host grant to the Rust
/// `AgentToolResult`/`AgentToolUpdate` shape.
pub struct EchoDomainBinding {
    binding: InvocationBinding,
    tool_minor: u32,
    json_max: usize,
    #[cfg(test)]
    test_construction: Option<fn(bool)>,
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
            #[cfg(test)]
            test_construction: None,
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
        self.receive_typed(invocation, |invocation, domain| {
            invocation
                .receive_checked_with_final_time(
                    generation,
                    frame,
                    ingress_now_ms,
                    additional_deadline_ms,
                    domain,
                    final_now_ms,
                )
                .map_err(IndexedReceiveError::Protocol)
        })
    }

    /// Caller-backed indexed receive; the invocation selects its own core/tool
    /// minors and frame ceiling. Local indexed failures are host faults.
    #[allow(clippy::too_many_arguments)]
    pub fn receive_indexed_checked_with_final_time<N>(
        &self,
        invocation: &mut ToolInvocation,
        generation: u64,
        transmitted_body: &[u8],
        arena: &mut BorrowedIndexArena<'_>,
        ingress_now_ms: u64,
        additional_deadline_ms: Option<u64>,
        final_now_ms: N,
    ) -> Result<CheckedEchoFrame, DomainReceiveError>
    where
        N: FnMut() -> u64,
    {
        self.receive_typed(invocation, |invocation, domain| {
            invocation.receive_indexed_checked_with_final_time(
                generation,
                transmitted_body,
                arena,
                ingress_now_ms,
                additional_deadline_ms,
                domain,
                final_now_ms,
            )
        })
    }

    fn receive_typed<F>(
        &self,
        invocation: &mut ToolInvocation,
        receive: F,
    ) -> Result<CheckedEchoFrame, DomainReceiveError>
    where
        F: FnOnce(
            &mut ToolInvocation,
            &mut dyn FnMut(DomainCheck<'_>) -> DomainDecision,
        ) -> Result<ReceiveOutcome, IndexedReceiveError>,
    {
        if !self.binding.same_live_identity(invocation.binding()) {
            return Err(DomainReceiveError::Host(DomainHostError::Binding));
        }
        let mut typed = None;
        let mut host_error = None;
        let result = receive(invocation, &mut |check| match self.construct(check) {
            Ok(value) => {
                typed = value;
                DomainDecision::Accept
            }
            Err(CheckError::PeerValue) => DomainDecision::PeerValueMismatch,
            Err(CheckError::Host(error)) => {
                host_error = Some(error);
                DomainDecision::HostFault
            }
        });
        let outcome = match result {
            Ok(outcome) => outcome,
            Err(IndexedReceiveError::Protocol(LiveError::HostDomainFault)) => {
                return Err(DomainReceiveError::Host(
                    host_error.unwrap_or(DomainHostError::Binding),
                ));
            }
            Err(IndexedReceiveError::Protocol(LiveError::PeerLost)) => {
                return Err(DomainReceiveError::PeerLost);
            }
            Err(IndexedReceiveError::Protocol(error)) => {
                return Err(DomainReceiveError::Protocol(error));
            }
            Err(IndexedReceiveError::Local(error)) => {
                return Err(DomainReceiveError::Host(DomainHostError::Indexed(error)));
            }
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
        #[cfg(test)]
        if let Some(observe) = self.test_construction {
            observe(false);
        }
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
        #[cfg(test)]
        if let Some(observe) = self.test_construction {
            observe(true);
        }
        Ok(Some(value))
    }
}

enum CheckError {
    Host(DomainHostError),
    PeerValue,
}

#[cfg(test)]
mod indexed_order_tests {
    use super::*;
    use crate::extensions::value::encode_arguments;
    use pi_ai_core::JsonObject;
    use pi_extension_protocol::activation::{
        Activation, AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay,
        ToolSchemaInputs,
    };
    use pi_extension_protocol::endpoint::{ActiveFrame, SelectedEnvelope};
    use pi_extension_protocol::jcs::{self, Limits as JsonLimits};
    use pi_extension_protocol::tool_live::{LivePhase, SelectedArguments, Start};
    use pi_extension_protocol::wire::echo_bounded::IndexStorage;
    use pi_extension_protocol::wire::{framing, v1_0, v1_1};
    use serde_json::{Value, json};
    use std::cell::RefCell;
    use std::num::NonZeroUsize;
    const CATALOG: &str =
        include_str!("../../../pi-extension-protocol/schema/fixtures/host-catalog.json");
    const VECTORS: &str =
        include_str!("../../../pi-extension-protocol/schema/fixtures/activation-vectors.json");
    fn hex(value: &str) -> Vec<u8> {
        let (pairs, tail) = value.as_bytes().as_chunks::<2>();
        assert!(tail.is_empty());
        pairs
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect()
    }

    fn limits() -> v1_1::ProtocolLimits {
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
        }
    }

    fn active_axes(
        core: u32,
        tool_minor: u32,
        alter_progress: bool,
        decoded_limit: u64,
        frame_limit: u64,
    ) -> Activation {
        let catalog: Value = serde_json::from_str(CATALOG).unwrap();
        let entry = &catalog["entries"][0];
        let agent = &entry["agentTool"];
        let descriptor = AgentTool {
            name: "echo".into(),
            label: agent["label"].as_str().unwrap().into(),
            description: agent["description"].as_str().unwrap().into(),
            parameters: agent["parameters"].clone(),
            execution_mode: AgentToolMode::Sequential,
            replay: ToolReplay::Safe,
        };
        let mut alternate =
            entry["selected_minor_schema_sets"]["1.0"]["progress"]["schema"].clone();
        if alter_progress {
            alternate["maxProperties"] = json!(16);
        }
        let inputs = [0, 1]
            .into_iter()
            .map(|tool_minor| {
                let set = &entry["selected_minor_schema_sets"][format!("1.{tool_minor}")];
                ToolSchemaInputs {
                    minor: tool_minor,
                    arguments: serde_json::to_vec(&set["arguments"]["schema"]).unwrap(),
                    progress: serde_json::to_vec(if alter_progress {
                        &alternate
                    } else {
                        &set["progress"]["schema"]
                    })
                    .unwrap(),
                    raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).unwrap(),
                }
            })
            .collect();
        let tool =
            ApprovedTool::new("peer-a".into(), descriptor, inputs, JsonLimits::default()).unwrap();
        let mut host_limits = limits();
        host_limits.aggregate_decoded_bytes = decoded_limit;
        host_limits.frame_bytes = frame_limit;
        let host = HostAuthority::new(
            "peer-a".into(),
            7,
            core,
            tool_minor,
            host_limits,
            vec![tool],
            vec![],
        )
        .unwrap();
        let mut activation = Activation::new(host, 100);
        // Frozen Hello offers core 1.1 and tool.v1 1.1. The host ceiling selects
        // 1.0 or 1.1 without changing peer-owned bytes.
        let hello = [
            0x0a, 0x04, 0x08, 0x01, 0x18, 0x01, 0x12, 0x0d, 0x0a, 0x07, b't', b'o', b'o', b'l',
            b'.', b'v', b'1', 0x10, 0x01, 0x20, 0x01,
        ];
        activation.receive_hello(&hello, 101).unwrap();
        let fixture: Value = serde_json::from_str(VECTORS).unwrap();
        let mut manifest = hex(fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap());
        if alter_progress {
            let original = hex(
                entry["selected_minor_schema_sets"]["1.0"]["progress"]["schema_digest_sha256"]
                    .as_str()
                    .unwrap(),
            );
            let replacement = jcs::admit_schema(
                &serde_json::to_vec(&alternate).unwrap(),
                JsonLimits::default(),
            )
            .unwrap()
            .canonical()
            .sha256();
            let mut replaced = 0;
            for offset in 0..=manifest.len() - 32 {
                if manifest[offset..offset + 32] == original
                    && manifest[..offset]
                        .windows(b"tool.progress:echo".len())
                        .rposition(|window| window == b"tool.progress:echo")
                        .is_some_and(|position| offset - position < 60)
                {
                    manifest[offset..offset + 32].copy_from_slice(&replacement);
                    replaced += 1;
                }
            }
            assert_eq!(replaced, 2);
        }
        activation.receive_manifest(&manifest, 102).unwrap();
        activation.receive_ready(&[], 103).unwrap();
        activation
    }

    fn start(
        grant: &pi_extension_protocol::activation::ActiveGrant,
        request_id: u64,
    ) -> ToolInvocation {
        start_credit(grant, request_id, Some(4096))
    }

    fn start_credit(
        grant: &pi_extension_protocol::activation::ActiveGrant,
        request_id: u64,
        credit: Option<u64>,
    ) -> ToolInvocation {
        let saved: JsonObject = serde_json::from_str(r#"{"text":"ping"}"#).unwrap();
        let arguments: SelectedArguments = encode_arguments(&saved, grant, "echo").unwrap();
        ToolInvocation::start(
            grant,
            "echo",
            Start {
                effect_key: v1_1::EffectKey {
                    session_id: "s".into(),
                    lane: "main".into(),
                    operation_id: "op".into(),
                    effect_kind: "tool".into(),
                    step_or_task_id: "turn".into(),
                    logical_attempt: 0,
                    item_index: Some(0),
                },
                source_call_id: "call".into(),
                request_id,
                active_generation: 7,
                arguments,
                now_ms: 200,
                requested_deadline_after_ms: Some(700),
                method_limit_ms: 1000,
                connection_remaining_ms: 500,
                parent_remaining_ms: Some(300),
                initial_credit_bytes: credit,
            },
        )
        .unwrap()
        .0
    }

    // Self-contained protobuf encoding for the small echo fixture tree. The wire
    // values deliberately differ only in the usage.input integer/float oneof arm.
    fn varint(mut value: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while value >= 128 {
            out.push(value as u8 | 0x80);
            value >>= 7;
        }
        out.push(value as u8);
        out
    }
    fn bytes(tag: u8, payload: &[u8]) -> Vec<u8> {
        let mut out = vec![tag << 3 | 2];
        out.extend(varint(payload.len() as u64));
        out.extend(payload);
        out
    }
    fn integer(value: i64) -> Vec<u8> {
        let zigzag = ((value as u64) << 1) ^ ((value >> 63) as u64);
        let mut out = vec![3 << 3];
        out.extend(varint(zigzag));
        out
    }
    fn float_one() -> Vec<u8> {
        let mut out = vec![4 << 3 | 1];
        out.extend(1.0f64.to_le_bytes());
        out
    }
    fn map(entries: Vec<(&str, Vec<u8>)>) -> Vec<u8> {
        let mut map = Vec::new();
        for (key, value) in entries {
            let mut entry = bytes(1, key.as_bytes());
            entry.extend(bytes(2, &value));
            map.extend(bytes(1, &entry));
        }
        bytes(8, &map)
    }
    fn echo_value(float_input: bool) -> Vec<u8> {
        let cost = map(vec![
            ("input", integer(0)),
            ("output", integer(0)),
            ("cacheRead", integer(0)),
            ("cacheWrite", integer(0)),
            ("total", integer(0)),
        ]);
        let usage = map(vec![
            ("input", if float_input { float_one() } else { integer(1) }),
            ("output", integer(0)),
            ("cacheRead", integer(0)),
            ("cacheWrite", integer(0)),
            ("totalTokens", integer(0)),
            ("cost", cost),
        ]);
        map(vec![
            ("content", bytes(7, &[])),
            ("details", map(vec![])),
            ("usage", usage),
        ])
    }
    fn bound(kind: &str, float_input: bool) -> Vec<u8> {
        let mut out = bytes(1, format!("tool.{kind}:echo").as_bytes());
        out.extend(bytes(
            2,
            &hex("a08e48cf5321241817eefd20593ea27ef00f7effb1118796abe2da134ea00335"),
        ));
        out.extend(bytes(3, &echo_value(float_input)));
        out
    }
    fn frame(minor: u32, progress: bool, float_input: bool, request_id: u64) -> ActiveFrame {
        let payload = if progress {
            bytes(1, &bound("progress", float_input))
        } else {
            bytes(1, &bound("result", float_input))
        };
        let cap = NonZeroUsize::new(8192).unwrap();
        if minor == 0 {
            let frame = if progress {
                v1_0::envelope::Frame::StreamItem(v1_0::StreamItem {
                    request_id,
                    sequence: 0,
                    payload,
                })
            } else {
                v1_0::envelope::Frame::StreamEnd(v1_0::StreamEnd {
                    request_id,
                    terminal: Some(v1_0::stream_end::Terminal::Result(payload)),
                })
            };
            let envelope = v1_0::Envelope { frame: Some(frame) };
            let body_len = framing::encode(&envelope, cap).unwrap().body_len;
            ActiveFrame {
                envelope: SelectedEnvelope::V1_0(envelope),
                body_len,
            }
        } else {
            let frame = if progress {
                v1_1::envelope::Frame::StreamItem(v1_1::StreamItem {
                    request_id,
                    sequence: 0,
                    payload,
                })
            } else {
                v1_1::envelope::Frame::StreamEnd(v1_1::StreamEnd {
                    request_id,
                    terminal: Some(v1_1::stream_end::Terminal::Result(payload)),
                })
            };
            let envelope = v1_1::Envelope { frame: Some(frame) };
            let body_len = framing::encode(&envelope, cap).unwrap().body_len;
            ActiveFrame {
                envelope: SelectedEnvelope::V1_1(envelope),
                body_len,
            }
        }
    }

    thread_local! { static EVENTS: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) }; }
    fn construction(success: bool) {
        EVENTS.with_borrow_mut(|events| events.push(if success { "typed" } else { "construct" }));
    }
    fn sample() -> u64 {
        EVENTS.with_borrow_mut(|events| {
            assert_eq!(events.as_slice(), ["construct", "typed"]);
            events.push("clock");
        });
        201
    }
    #[test]
    fn indexed_actual_typed_construction_precedes_single_clock_and_host_checks_repeat() {
        for core in [0, 1] {
            for tool in [0, 1] {
                for progress in [true, false] {
                    let a = active_axes(core, tool, false, 8192, 8192);
                    let grant = a.active_grant().unwrap();
                    let alternate = active_axes(core, tool, true, 8192, 8192);
                    for fault in 0..6 {
                        if fault == 5 && !progress {
                            continue;
                        }
                        let selected_grant = if fault == 5 {
                            alternate.active_grant().unwrap()
                        } else {
                            grant
                        };
                        let mut invocation = start(selected_grant, 17);
                        let mut domain = if fault == 5 {
                            // A deliberately stale trusted mapping bypasses the
                            // registration constructor only in this private fixture.
                            assert!(matches!(
                                EchoDomainBinding::new(selected_grant, &invocation),
                                Err(DomainHostError::Projection)
                            ));
                            EchoDomainBinding {
                                binding: invocation.binding().clone(),
                                tool_minor: tool,
                                json_max: 8192,
                                test_construction: None,
                            }
                        } else {
                            EchoDomainBinding::new(grant, &invocation).unwrap()
                        };
                        domain.test_construction = Some(construction);
                        match fault {
                            2 => domain.tool_minor = 1 - tool,
                            3 => domain.json_max = 1,
                            _ => {}
                        }
                        let mut input = frame(core, progress, fault == 1, 17);
                        use prost::Message;
                        if fault == 5 {
                            let payload = match &mut input.envelope {
                                SelectedEnvelope::V1_0(e) => {
                                    let Some(v1_0::envelope::Frame::StreamItem(item)) =
                                        &mut e.frame
                                    else {
                                        panic!()
                                    };
                                    &mut item.payload
                                }
                                SelectedEnvelope::V1_1(e) => {
                                    let Some(v1_1::envelope::Frame::StreamItem(item)) =
                                        &mut e.frame
                                    else {
                                        panic!()
                                    };
                                    &mut item.payload
                                }
                            };
                            let mut dto = v1_1::ToolProgress::decode(payload.as_slice()).unwrap();
                            dto.event.as_mut().unwrap().schema_digest_sha256 = selected_grant
                                .tools()[0]
                                .projections()
                                .progress()
                                .digest()
                                .to_vec();
                            *payload = dto.encode_to_vec();
                        }
                        let body = match input.envelope {
                            SelectedEnvelope::V1_0(e) => e.encode_to_vec(),
                            SelectedEnvelope::V1_1(e) => e.encode_to_vec(),
                        };
                        let mut slot = IndexStorage::<1024, 1025>::new();
                        let mut arena = slot
                            .borrow(NonZeroUsize::new(1024).unwrap(), usize::MAX)
                            .unwrap();
                        if fault == 4 {
                            invocation.begin_drain(201);
                        }
                        EVENTS.with_borrow_mut(Vec::clear);
                        let result = domain.receive_indexed_checked_with_final_time(
                            &mut invocation,
                            7,
                            &body,
                            &mut arena,
                            200,
                            None,
                            sample,
                        );
                        let events = EVENTS.with_borrow(Clone::clone);
                        match fault {
                            0 => {
                                let value = result.unwrap();
                                assert_eq!(events, ["construct", "typed", "clock"]);
                                assert_eq!(value.value.unwrap().usage.unwrap().input.get(), 1);
                            }
                            1 => {
                                assert!(
                                    matches!(result,Err(DomainReceiveError::Protocol(error)) if error==if progress{LiveError::InvalidProgress}else{LiveError::InvalidTerminal})
                                );
                                assert_eq!(events, ["construct"]);
                            }
                            2 | 3 => {
                                assert!(matches!(
                                    result,
                                    Err(DomainReceiveError::Host(DomainHostError::Binding))
                                ));
                                assert_eq!(events, ["construct"]);
                                assert_eq!(invocation.phase(), LivePhase::Open);
                            }
                            4 => {
                                assert!(matches!(result, Err(DomainReceiveError::PeerLost)));
                                assert_eq!(events, ["construct", "typed", "clock"]);
                            }
                            5 => {
                                assert!(matches!(
                                    result,
                                    Err(DomainReceiveError::Host(DomainHostError::Projection))
                                ));
                                assert_eq!(events, ["construct"]);
                                assert_eq!(invocation.phase(), LivePhase::Open);
                            }
                            _ => unreachable!(),
                        }
                        if fault != 0 {
                            assert_eq!(
                                (invocation.credit(), invocation.next_sequence()),
                                (4096, 0)
                            );
                            assert!(invocation.receipt().is_none());
                        }
                    }
                }
            }
        }
    }
}
