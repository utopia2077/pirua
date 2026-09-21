#![cfg(feature = "extensions")]

use pi_agent_schema::{
    EffectKey, EffectKind, EffectStepId, EntryId, LaneId, OperationId, SessionId, ToolEffectPlan,
};
use pi_ai_core::{JS_SAFE_INTEGER_MAX, SafeUnsigned};
use pi_extension_protocol::activation::{
    Activation, AgentTool, AgentToolMode, ApprovedTool, HostAuthority, ToolReplay, ToolSchemaInputs,
};
use pi_extension_protocol::jcs::Limits as JsonLimits;
use pi_extension_protocol::tool_live::{Start, ToolInvocation};
use pi_extension_protocol::wire::v1_1;
use pi_harness_runtime::extensions::key::{ToolKeyError, encode_tool_effect_key};
use pi_harness_runtime::extensions::value::encode_arguments;
use serde_json::Value;

const CATALOG: &str = include_str!("../../pi-extension-protocol/schema/fixtures/host-catalog.json");
const VECTORS: &str =
    include_str!("../../pi-extension-protocol/schema/fixtures/activation-vectors.json");
const ECHO_SCHEMA: &str =
    include_str!("../../pi-extension-protocol/schema/dynamic/tool.arguments_echo.schema.json");

fn hex(value: &str) -> Vec<u8> {
    let (pairs, tail) = value.as_bytes().as_chunks::<2>();
    assert!(tail.is_empty());
    pairs
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn active(minor: u32) -> Activation {
    let catalog: Value = serde_json::from_str(CATALOG).unwrap();
    let entry = &catalog["entries"][0];
    let descriptor = AgentTool {
        name: "echo".into(),
        label: entry["agentTool"]["label"].as_str().unwrap().into(),
        description: entry["agentTool"]["description"].as_str().unwrap().into(),
        parameters: entry["agentTool"]["parameters"].clone(),
        execution_mode: AgentToolMode::Sequential,
        replay: ToolReplay::Safe,
    };
    let schemas = [0, 1]
        .into_iter()
        .map(|version| {
            let set = &entry["selected_minor_schema_sets"][format!("1.{version}")];
            ToolSchemaInputs {
                minor: version,
                arguments: ECHO_SCHEMA.as_bytes().to_vec(),
                progress: serde_json::to_vec(&set["progress"]["schema"]).unwrap(),
                raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).unwrap(),
            }
        })
        .collect();
    let tool =
        ApprovedTool::new("peer-a".into(), descriptor, schemas, JsonLimits::default()).unwrap();
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
    let host =
        HostAuthority::new("peer-a".into(), 7, minor, minor, limits, vec![tool], vec![]).unwrap();
    let mut activation = Activation::new(host, 100);
    let mut hello = vec![0x0a, 0x04, 0x08, 0x01, 0x18, 0x01, 0x12, 0x0d, 0x0a, 0x07];
    hello.extend_from_slice(b"tool.v1");
    hello.extend_from_slice(&[0x10, 0x01, 0x20, 0x01]);
    activation.receive_hello(&hello, 101).unwrap();
    let vectors: Value = serde_json::from_str(VECTORS).unwrap();
    let manifest = hex(vectors["cases"]["valid_1_1"]["manifest_pb_hex"]
        .as_str()
        .unwrap());
    activation.receive_manifest(&manifest, 102).unwrap();
    activation.receive_ready(&[], 103).unwrap();
    activation
}

fn plan(index: u64) -> ToolEffectPlan<()> {
    let index = SafeUnsigned::new(index).unwrap();
    ToolEffectPlan {
        key: EffectKey {
            session_id: SessionId::new("session/\u{0000}β").unwrap(),
            lane: LaneId::new("lane\nopaque").unwrap(),
            operation_id: OperationId::new("operation/δ").unwrap(),
            kind: EffectKind::Tool,
            step_or_task_id: EffectStepId::new("turn/\u{0000}ζ").unwrap(),
            logical_attempt: SafeUnsigned::new(0).unwrap(),
            item_index: Some(index),
        },
        assistant_entry_id: EntryId::new("assistant-entry").unwrap(),
        source_index: index,
        args_key: "op.tool_args/opaque".into(),
        telemetry_context: (),
    }
}

#[test]
fn exact_typed_tool_plan_preserves_all_wire_key_fields_and_safe_maximum() {
    let plan = plan(JS_SAFE_INTEGER_MAX);
    let wire = encode_tool_effect_key(&plan).unwrap();
    assert_eq!(
        wire.session_id.as_bytes(),
        plan.key.session_id.as_str().as_bytes()
    );
    assert_eq!(wire.lane.as_bytes(), plan.key.lane.as_str().as_bytes());
    assert_eq!(
        wire.operation_id.as_bytes(),
        plan.key.operation_id.as_str().as_bytes()
    );
    assert_eq!(wire.effect_kind, "tool");
    assert_eq!(
        wire.step_or_task_id.as_bytes(),
        plan.key.step_or_task_id.as_str().as_bytes()
    );
    assert_eq!(wire.logical_attempt, 0);
    assert_eq!(wire.item_index, Some(JS_SAFE_INTEGER_MAX));
    assert!(SafeUnsigned::new(JS_SAFE_INTEGER_MAX + 1).is_err());
}

#[test]
fn inconsistent_local_tool_plan_is_rejected_before_it_can_be_sent() {
    let valid = plan(9);
    let mut wrong_kind = valid.clone();
    wrong_kind.key.kind = EffectKind::Hook;
    let mut numbered_attempt = valid.clone();
    numbered_attempt.key.logical_attempt = SafeUnsigned::new(1).unwrap();
    let mut absent_index = valid.clone();
    absent_index.key.item_index = None;
    let mut wrong_index = valid;
    wrong_index.key.item_index = Some(SafeUnsigned::new(8).unwrap());

    for invalid in [wrong_kind, numbered_attempt, absent_index, wrong_index] {
        let error = encode_tool_effect_key(&invalid).unwrap_err();
        assert_eq!(error, ToolKeyError::InvalidToolPlan);
        assert_eq!(error.to_string(), "invalid planned tool effect key");
        assert!(!format!("{error:?}").contains("session/"));
        assert!(!error.to_string().contains("operation/"));
    }
}

#[test]
fn selected_host_grant_accepts_the_converted_exact_tool_key() {
    let saved = serde_json::from_str(r#"{"text":"ping"}"#).unwrap();
    let plan = plan(9);
    let expected = encode_tool_effect_key(&plan).unwrap();
    for minor in [0, 1] {
        let activation = active(minor);
        let grant = activation.active_grant().unwrap();
        let arguments = encode_arguments(&saved, grant, "echo").unwrap();
        let (invocation, frame) = ToolInvocation::start(
            grant,
            "echo",
            Start {
                effect_key: expected.clone(),
                source_call_id: "source-call/ξ".into(),
                request_id: 17,
                active_generation: 7,
                arguments,
                now_ms: 200,
                requested_deadline_after_ms: None,
                method_limit_ms: 300,
                connection_remaining_ms: 300,
                parent_remaining_ms: None,
                initial_credit_bytes: None,
            },
        )
        .unwrap();
        assert_eq!(invocation.binding().effect_key(), &expected);
        assert_eq!(invocation.binding().generation(), 7);
        assert_eq!(invocation.binding().request_id(), 17);
        assert_eq!(invocation.source_call_id(), "source-call/ξ");
        assert!(!frame.bytes.is_empty());
        assert!(frame.body_len < frame.bytes.len());
    }
}
