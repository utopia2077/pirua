use pi_extension_protocol::activation::{
    Activation, ActivationPhase, AgentTool, AgentToolMode, ApprovedTool, HostAuthority,
    OwnedInventoryCaps, ParameterMapRecipe, ToolReplay, ToolSchemaInputs, WireGrant,
};
use pi_extension_protocol::jcs::Limits as JsonLimits;
use pi_extension_protocol::wire::v1_1;
use prost::Message;
use serde_json::Value;

const VECTORS: &str = include_str!("../schema/fixtures/activation-vectors.json");
const CATALOG: &str = include_str!("../schema/fixtures/host-catalog.json");

fn decode_hex(hex: &str) -> Vec<u8> {
    assert_eq!(hex.len() % 2, 0);
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            (pair[0] as char).to_digit(16).unwrap() as u8 * 16
                + (pair[1] as char).to_digit(16).unwrap() as u8
        })
        .collect()
}

fn wire_grant(grant: &WireGrant) -> v1_1::CapabilityGrant {
    match grant {
        WireGrant::V1_0(old) => {
            v1_1::CapabilityGrant::decode(old.encode_to_vec().as_slice()).unwrap()
        }
        WireGrant::V1_1(new) => new.clone(),
    }
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

fn host(principal: &str, max_minor: u32) -> HostAuthority {
    host_with(principal, max_minor, max_minor, AgentToolMode::Sequential)
}

fn host_with(
    principal: &str,
    core_minor: u32,
    tool_minor: u32,
    mode: AgentToolMode,
) -> HostAuthority {
    let catalog: Value = serde_json::from_str(CATALOG).unwrap();
    let entry = &catalog["entries"][0];
    let agent = &entry["agentTool"];
    let descriptor = AgentTool {
        name: agent["name"].as_str().unwrap().into(),
        label: agent["label"].as_str().unwrap().into(),
        description: agent["description"].as_str().unwrap().into(),
        parameters: agent["parameters"].clone(),
        execution_mode: mode,
        replay: ToolReplay::Safe,
    };
    let sets = &entry["selected_minor_schema_sets"];
    let inputs = [0, 1]
        .into_iter()
        .map(|minor| {
            let set = &sets[format!("1.{minor}")];
            ToolSchemaInputs {
                minor,
                arguments: serde_json::to_vec(&set["arguments"]["schema"]).unwrap(),
                progress: serde_json::to_vec(&set["progress"]["schema"]).unwrap(),
                raw_result: serde_json::to_vec(&set["raw_result"]["schema"]).unwrap(),
            }
        })
        .collect();
    let tool = ApprovedTool::new(
        entry["authenticated_extension_id"].as_str().unwrap().into(),
        descriptor,
        inputs,
        JsonLimits::default(),
    )
    .unwrap();
    HostAuthority::new(
        principal.into(),
        7,
        core_minor,
        tool_minor,
        limits(),
        vec![tool],
        vec![],
    )
    .unwrap()
}

fn hello() -> v1_1::Hello {
    v1_1::Hello {
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
}

fn begin(principal: &str, minor: u32) -> Activation {
    let mut activation = Activation::new(host(principal, minor), 100);
    let ack = activation
        .receive_hello(&hello().encode_to_vec(), 101)
        .unwrap();
    assert_eq!(ack.selected_protocol.unwrap().minor, minor);
    activation
}

#[test]
fn all_29_frozen_raw_manifests_match_reference_outcomes() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let cases = fixture["cases"].as_object().unwrap();
    assert_eq!(cases.len(), 29);
    for (name, case) in cases {
        let minor = if case["selected_minor"] == "1.0" {
            0
        } else {
            1
        };
        let mut activation = begin("peer-a", minor);
        let raw = decode_hex(case["manifest_pb_hex"].as_str().unwrap());
        let expected = &case["model_result"];
        match activation.receive_manifest(&raw, 102) {
            Ok(grant) => {
                assert_eq!(expected["status"], "active", "{name}");
                let wire = wire_grant(&grant);
                assert_eq!(wire.selected_protocol.unwrap().minor, minor, "{name}");
                assert_eq!(wire.connection_generation, 7, "{name}");
                if minor == 0 {
                    assert!(matches!(grant, WireGrant::V1_0(_)), "{name}");
                }
                assert!(activation.active_grant().is_none(), "{name}");
                activation
                    .receive_ready(&v1_1::Ready {}.encode_to_vec(), 103)
                    .unwrap();
                let active = activation.active_grant().unwrap();
                let selected: Vec<_> = active
                    .tools()
                    .iter()
                    .map(|tool| tool.descriptor().name.as_str())
                    .collect();
                let expected_selected: Vec<_> = expected["selected"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap())
                    .collect();
                assert_eq!(selected, expected_selected, "{name}");
                let expected_omitted: Vec<_> = expected["omitted"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap())
                    .collect();
                assert_eq!(active.omitted(), expected_omitted, "{name}");
                if let Some(tool) = active.tools().first() {
                    let entry: Value = serde_json::from_str(CATALOG).unwrap();
                    let host_tool = &entry["entries"][0];
                    assert_eq!(
                        tool.descriptor().parameters,
                        host_tool["agentTool"]["parameters"],
                        "{name}"
                    );
                    assert_eq!(tool.descriptor().label, "Echo", "{name}");
                    assert_eq!(
                        tool.reference().authenticated_extension_id,
                        "peer-a",
                        "{name}"
                    );
                    let profile_digest =
                        &host_tool["selected_minor_profile_digests"][format!("1.{minor}")];
                    assert_eq!(
                        tool.reference().schema_digest_sha256,
                        decode_hex(profile_digest.as_str().unwrap()),
                        "{name}"
                    );
                    let set = &host_tool["selected_minor_schema_sets"][format!("1.{minor}")];
                    for (slot, projection) in [
                        ("arguments", tool.projections().arguments()),
                        ("progress", tool.projections().progress()),
                        ("raw_result", tool.projections().raw_result()),
                    ] {
                        assert_eq!(
                            projection.id(),
                            set[slot]["projection_id"].as_str().unwrap(),
                            "{name}"
                        );
                        assert_eq!(
                            projection.digest().to_vec(),
                            decode_hex(set[slot]["schema_digest_sha256"].as_str().unwrap()),
                            "{name}"
                        );
                    }
                }
                assert_eq!(activation.phase(), ActivationPhase::Active, "{name}");
            }
            Err(error) => {
                assert_eq!(expected["status"], "reject", "{name}: {error}");
                assert_eq!(error.reason, expected["reason"].as_str().unwrap(), "{name}");
                assert_eq!(activation.phase(), ActivationPhase::Rejected, "{name}");
            }
        }
    }
}

#[test]
fn state_version_time_and_limits_are_host_controlled() {
    let mut activation = Activation::new(host("peer-a", 1), 100);
    assert_eq!(
        activation.receive_ready(&[], 101).unwrap_err().reason,
        "invalid_handshake_state"
    );
    assert_eq!(activation.phase(), ActivationPhase::Rejected);
    let mut activation = Activation::new(host("peer-a", 1), 100);
    assert_eq!(
        activation.receive_manifest(&[], 101).unwrap_err().reason,
        "invalid_handshake_state"
    );
    let mut activation = Activation::new(host("peer-a", 1), 100);
    let mut wrong_major = hello();
    wrong_major.protocol.as_mut().unwrap().major = 2;
    assert_eq!(
        activation
            .receive_hello(&wrong_major.encode_to_vec(), 101)
            .unwrap_err()
            .reason,
        "unsupported_version"
    );
    assert_eq!(activation.phase(), ActivationPhase::Rejected);
    let mut activation = Activation::new(host("peer-a", 1), 100);
    assert_eq!(
        activation
            .receive_hello(&hello().encode_to_vec(), 1101)
            .unwrap_err()
            .reason,
        "handshake_timeout"
    );
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&hello().encode_to_vec(), 101)
        .unwrap();
    assert!(activation.active_grant().is_none());
    let mut activation = begin("peer-a", 1);
    assert_eq!(
        activation
            .receive_hello(&hello().encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "invalid_handshake_state"
    );
    let mut activation = begin("peer-a", 1);
    assert_eq!(
        activation
            .receive_manifest(&vec![0; 4097], 102)
            .unwrap_err()
            .reason,
        "handshake_frame_too_large"
    );
    let mut invalid = limits();
    invalid.progress_queue_bytes = 0;
    assert_eq!(
        HostAuthority::new("peer-a".into(), 7, 1, 1, invalid, vec![], vec![])
            .unwrap_err()
            .reason,
        "invalid_limits"
    );
}

#[test]
fn every_protocol_limit_is_finite_and_json_budgets_follow_host_caps() {
    let mut caps = limits();
    caps.frame_bytes = 8191;
    caps.aggregate_decoded_bytes = 37;
    caps.value_elements = 13;
    let host = HostAuthority::new("peer-a".into(), 7, 1, 1, caps, vec![], vec![]).unwrap();
    let json = host.json_limits();
    assert_eq!(json.input_bytes, 8191);
    assert_eq!(json.decoded_bytes, 37);
    assert_eq!(json.elements, 13);
    assert_eq!(json.nodes, 13);
    assert_eq!(json.depth, 32);
    macro_rules! zero_rejects {
        ($($field:ident),+ $(,)?) => {$({
            let mut invalid = self::limits();
            invalid.$field = 0;
            assert_eq!(HostAuthority::new("peer-a".into(), 7, 1, 1, invalid, vec![], vec![]).unwrap_err().reason, "invalid_limits", stringify!($field));
        })+};
    }
    zero_rejects!(
        pre_handshake_frame_bytes,
        frame_bytes,
        aggregate_decoded_bytes,
        value_depth,
        value_elements,
        string_bytes,
        bytes_length,
        in_flight_requests,
        blocking_queue_bytes,
        progress_queue_bytes,
        passive_queue_bytes,
        telemetry_queue_bytes,
        nesting_depth,
        requests_per_second,
        handshake_ms,
        cancellation_grace_ms,
        max_stream_credit_bytes
    );
    let mut invalid = limits();
    invalid.pre_handshake_frame_bytes = invalid.frame_bytes + 1;
    assert_eq!(
        HostAuthority::new("peer-a".into(), 7, 1, 1, invalid, vec![], vec![])
            .unwrap_err()
            .reason,
        "invalid_limits"
    );
    let mut invalid = limits();
    invalid.max_stream_credit_bytes = u64::MAX;
    assert_eq!(
        HostAuthority::new("peer-a".into(), 7, 1, 1, invalid, vec![], vec![])
            .unwrap_err()
            .reason,
        "invalid_limits"
    );
}

#[test]
fn unsupported_profiles_and_permissions_cannot_create_shell_grants() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = decode_hex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    let mut manifest = v1_1::Manifest::decode(raw.as_slice()).unwrap();
    let mut shell = manifest.capabilities[0].clone();
    shell.kind = "hook".into();
    shell.name = "callback".into();
    shell.profile.as_mut().unwrap().name = "hook.v1".into();
    shell.tool_schema_sets.clear();
    shell.required = false;
    manifest.capabilities.push(shell.clone());
    let mut activation = begin("peer-a", 1);
    let grant = activation
        .receive_manifest(&manifest.encode_to_vec(), 102)
        .unwrap();
    assert_eq!(wire_grant(&grant).selected_capabilities.len(), 1);
    activation.receive_ready(&[], 103).unwrap();
    assert_eq!(activation.active_grant().unwrap().omitted(), ["callback"]);
    shell.required = true;
    manifest.capabilities[1] = shell;
    assert_eq!(
        begin("peer-a", 1)
            .receive_manifest(&manifest.encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "required_capability_denied"
    );
    manifest.capabilities.pop();
    manifest.permissions.push(v1_1::PermissionRequest {
        name: "secret".into(),
        scope: "*".into(),
        required: true,
    });
    assert_eq!(
        begin("peer-a", 1)
            .receive_manifest(&manifest.encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "required_capability_denied"
    );
    manifest.permissions[0].required = false;
    let mut activation = begin("peer-a", 1);
    let grant = activation
        .receive_manifest(&manifest.encode_to_vec(), 102)
        .unwrap();
    assert!(wire_grant(&grant).permissions.is_empty());
    assert_eq!(wire_grant(&grant).limits, Some(limits()));
}

#[test]
fn all_six_unimplemented_profile_shells_are_omitted_or_denied() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = decode_hex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    for (kind, profile) in [
        ("hook", "hook.v1"),
        ("provider", "provider.v1"),
        ("event-sink", "event-sink.v1"),
        ("resource", "resource.v1"),
        ("host-action", "host-action.v1"),
        ("telemetry-sink", "telemetry-sink.v1"),
    ] {
        let mut manifest = v1_1::Manifest::decode(raw.as_slice()).unwrap();
        let cap = &mut manifest.capabilities[0];
        cap.kind = kind.into();
        cap.profile.as_mut().unwrap().name = profile.into();
        cap.tool_schema_sets.clear();
        cap.required = false;
        if kind == "event-sink" {
            manifest.subscriptions.push(v1_1::EventSubscriptionRequest {
                scope: "session".into(),
            });
        }
        let mut activation = begin("peer-a", 1);
        let grant = activation
            .receive_manifest(&manifest.encode_to_vec(), 102)
            .unwrap();
        assert!(
            wire_grant(&grant).selected_capabilities.is_empty(),
            "{kind}"
        );
        activation.receive_ready(&[], 103).unwrap();
        assert_eq!(
            activation.active_grant().unwrap().omitted(),
            ["echo"],
            "{kind}"
        );
        manifest.capabilities[0].required = true;
        let mut activation = begin("peer-a", 1);
        assert_eq!(
            activation
                .receive_manifest(&manifest.encode_to_vec(), 102)
                .unwrap_err()
                .reason,
            "required_capability_denied",
            "{kind}"
        );
    }
}

#[test]
fn profile_ceiling_selects_highest_common_minor_and_tool_mode_is_independent() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = decode_hex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    let mut activation = Activation::new(host_with("peer-a", 1, 0, AgentToolMode::Parallel), 100);
    assert_eq!(
        activation
            .receive_hello(&hello().encode_to_vec(), 101)
            .unwrap()
            .selected_protocol
            .unwrap()
            .minor,
        1
    );
    let grant = activation.receive_manifest(&raw, 102).unwrap();
    assert_eq!(
        wire_grant(&grant).selected_capabilities[0]
            .profile
            .as_ref()
            .unwrap()
            .minor,
        0
    );
    assert!(activation.active_grant().is_none());
    activation.receive_ready(&[], 103).unwrap();
    let tool = &activation.active_grant().unwrap().tools()[0];
    assert_eq!(tool.descriptor().execution_mode, AgentToolMode::Parallel);
    assert_eq!(
        tool.reference().schema_digest_sha256,
        decode_hex("a995f8db0e619aeba78b054bc6bfbdcd9e3c8987e4e6535b6eb7d3585e60b8d3")
    );

    let mut hello = hello();
    hello.supported_profiles[0].min_minor = 1;
    let mut activation = Activation::new(host_with("peer-a", 1, 0, AgentToolMode::Sequential), 100);
    activation
        .receive_hello(&hello.encode_to_vec(), 101)
        .unwrap();
    assert_eq!(
        activation.receive_manifest(&raw, 102).unwrap_err().reason,
        "required_capability_denied"
    );
}

#[test]
fn permission_grant_is_exact_host_intersection() {
    let host = HostAuthority::new(
        "peer-a".into(),
        7,
        1,
        1,
        limits(),
        vec![],
        vec![v1_1::PermissionGrant {
            name: "read".into(),
            scope: "project-a".into(),
        }],
    )
    .unwrap();
    let mut activation = Activation::new(host, 100);
    activation
        .receive_hello(&hello().encode_to_vec(), 101)
        .unwrap();
    let manifest = v1_1::Manifest {
        declared_extension_id: "forged-metadata".into(),
        extension_version: "0.1".into(),
        protocol: Some(v1_1::VersionRange {
            major: 1,
            min_minor: 0,
            max_minor: 1,
        }),
        capabilities: vec![],
        permissions: vec![
            v1_1::PermissionRequest {
                name: "read".into(),
                scope: "project-a".into(),
                required: true,
            },
            v1_1::PermissionRequest {
                name: "read".into(),
                scope: "project-b".into(),
                required: false,
            },
        ],
        subscriptions: vec![],
    };
    let grant = activation
        .receive_manifest(&manifest.encode_to_vec(), 102)
        .unwrap();
    let wire = wire_grant(&grant);
    assert_eq!(wire.authenticated_extension_id, "peer-a");
    assert_eq!(
        wire.permissions,
        vec![v1_1::PermissionGrant {
            name: "read".into(),
            scope: "project-a".into()
        }]
    );
    assert!(wire.selected_capabilities.is_empty());
}

#[test]
fn malformed_optional_declaration_rejects_before_optional_omission() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = decode_hex(
        fixture["cases"]["unapproved_host_ceiling_optional"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    let mut manifest = v1_1::Manifest::decode(raw.as_slice()).unwrap();
    let malformed = decode_hex(
        fixture["cases"]["optional_malformed_missing_minor"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    manifest.capabilities.push(
        v1_1::Manifest::decode(malformed.as_slice())
            .unwrap()
            .capabilities
            .remove(0),
    );
    let error = begin("peer-a", 1)
        .receive_manifest(&manifest.encode_to_vec(), 102)
        .unwrap_err();
    assert_eq!(error.reason, "duplicate_or_missing_tool_schema_minor");
}

#[test]
fn catalog_principal_is_checked_against_authenticated_identity() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = decode_hex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    assert_eq!(
        begin("peer-b", 1)
            .receive_manifest(&raw, 102)
            .unwrap_err()
            .reason,
        "host_catalog_principal_mismatch"
    );
}

#[test]
fn complete_future_optional_hook_major_is_omitted() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = decode_hex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    let mut manifest = v1_1::Manifest::decode(raw.as_slice()).unwrap();
    let cap = &mut manifest.capabilities[0];
    cap.kind = "hook".into();
    cap.name = "future".into();
    cap.profile = Some(v1_1::ProfileRange {
        name: "hook.v2".into(),
        major: 2,
        min_minor: 0,
        max_minor: 0,
    });
    cap.supported_descriptor_digests.truncate(1);
    cap.tool_schema_sets.clear();
    cap.required = false;
    let mut offered = hello();
    offered
        .supported_profiles
        .push(cap.profile.clone().unwrap());
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    let grant = activation
        .receive_manifest(&manifest.encode_to_vec(), 102)
        .unwrap();
    assert!(wire_grant(&grant).selected_capabilities.is_empty());
    activation.receive_ready(&[], 103).unwrap();
    assert_eq!(activation.active_grant().unwrap().omitted(), ["future"]);

    manifest.capabilities[0].required = true;
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    assert_eq!(
        activation
            .receive_manifest(&manifest.encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "required_capability_denied"
    );

    manifest.capabilities[0].required = false;
    manifest.capabilities[0]
        .profile
        .as_mut()
        .unwrap()
        .name
        .clear();
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    assert_eq!(
        activation
            .receive_manifest(&manifest.encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "invalid_capability"
    );
}

#[test]
fn future_kind_is_omitted_but_malformed_declarations_reject_atomically() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = decode_hex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    let mut manifest = v1_1::Manifest::decode(raw.as_slice()).unwrap();
    let cap = &mut manifest.capabilities[0];
    cap.kind = "widget".into();
    cap.name = "future".into();
    cap.profile.as_mut().unwrap().name = "widget.v1".into();
    cap.supported_descriptor_digests.truncate(1);
    cap.profile.as_mut().unwrap().max_minor = 0;
    cap.tool_schema_sets.clear();
    cap.required = false;
    let mut offered = hello();
    offered
        .supported_profiles
        .push(cap.profile.clone().unwrap());
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    let wire = activation
        .receive_manifest(&manifest.encode_to_vec(), 102)
        .unwrap();
    assert!(wire_grant(&wire).selected_capabilities.is_empty());
    activation.receive_ready(&[], 103).unwrap();
    assert_eq!(activation.active_grant().unwrap().omitted(), ["future"]);

    let mut required = manifest.clone();
    required.capabilities[0].required = true;
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    assert_eq!(
        activation
            .receive_manifest(&required.encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "required_capability_denied"
    );

    let mut duplicate = manifest.clone();
    let duplicated_digest = duplicate.capabilities[0].supported_descriptor_digests[0].clone();
    duplicate.capabilities[0]
        .supported_descriptor_digests
        .push(duplicated_digest);
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    assert_eq!(
        activation
            .receive_manifest(&duplicate.encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "duplicate_or_missing_profile_minor"
    );

    let mut gapped = manifest.clone();
    gapped.capabilities[0].profile.as_mut().unwrap().max_minor = 2;
    let mut minor_two = gapped.capabilities[0].supported_descriptor_digests[0].clone();
    minor_two.minor = 2;
    gapped.capabilities[0]
        .supported_descriptor_digests
        .push(minor_two);
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    assert_eq!(
        activation
            .receive_manifest(&gapped.encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "duplicate_or_missing_profile_minor"
    );

    let mut unknown_enum = manifest.clone();
    unknown_enum.capabilities[0].execution = 127;
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    assert_eq!(
        activation
            .receive_manifest(&unknown_enum.encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "unsupported_variant"
    );

    for known_profile in ["provider.v1", "tool.v1"] {
        let mut mismatched = manifest.clone();
        mismatched.capabilities[0].kind = "hook".into();
        mismatched.capabilities[0].profile.as_mut().unwrap().name = known_profile.into();
        let mut activation = Activation::new(host("peer-a", 1), 100);
        activation
            .receive_hello(&offered.encode_to_vec(), 101)
            .unwrap();
        assert_eq!(
            activation
                .receive_manifest(&mismatched.encode_to_vec(), 102)
                .unwrap_err()
                .reason,
            "non_tool_or_profile_mismatch",
            "{known_profile}"
        );
    }
    let mut wrong_major = manifest;
    wrong_major.capabilities[0].kind = "hook".into();
    wrong_major.capabilities[0].profile.as_mut().unwrap().name = "hook.v1".into();
    wrong_major.capabilities[0].profile.as_mut().unwrap().major = 2;
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    assert_eq!(
        activation
            .receive_manifest(&wrong_major.encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "non_tool_or_profile_mismatch"
    );
}

#[test]
fn core_1_0_can_select_independent_tool_1_1() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = decode_hex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    let mut offered = hello();
    offered.protocol.as_mut().unwrap().max_minor = 0;
    let mut activation = Activation::new(host_with("peer-a", 0, 1, AgentToolMode::Sequential), 100);
    let ack = activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    assert_eq!(ack.selected_protocol.unwrap().minor, 0);
    let grant = activation.receive_manifest(&raw, 102).unwrap();
    assert_eq!(
        wire_grant(&grant).selected_capabilities[0]
            .profile
            .as_ref()
            .unwrap()
            .minor,
        1
    );
    assert!(matches!(grant, WireGrant::V1_0(_)));
    activation.receive_ready(&[], 103).unwrap();
    assert_eq!(
        activation.active_grant().unwrap().tools()[0]
            .reference()
            .profile
            .as_ref()
            .unwrap()
            .minor,
        1
    );
}

#[test]
fn pending_wire_grant_cannot_change_active_catalog() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = decode_hex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    let mut activation = begin("peer-a", 1);
    let mut pending: WireGrant = activation.receive_manifest(&raw, 102).unwrap();
    assert!(activation.active_grant().is_none());
    let WireGrant::V1_1(ref mut wire) = pending else {
        panic!("selected 1.1");
    };
    wire.selected_capabilities.clear();
    wire.permissions.push(v1_1::PermissionGrant {
        name: "forged".into(),
        scope: "*".into(),
    });
    activation.receive_ready(&[], 103).unwrap();
    let active = activation.active_grant().unwrap();
    assert_eq!(active.tools().len(), 1);
    assert_eq!(active.wire().selected_capabilities.len(), 1);
    assert!(active.wire().permissions.is_empty());
}

#[test]
fn nonformula_future_profile_omits_optional_and_denies_required() {
    let fixture: Value = serde_json::from_str(VECTORS).unwrap();
    let raw = decode_hex(
        fixture["cases"]["valid_1_1"]["manifest_pb_hex"]
            .as_str()
            .unwrap(),
    );
    let mut manifest = v1_1::Manifest::decode(raw.as_slice()).unwrap();
    let cap = &mut manifest.capabilities[0];
    cap.kind = "ui".into();
    cap.name = "component".into();
    cap.profile = Some(v1_1::ProfileRange {
        name: "ui.component.v1".into(),
        major: 1,
        min_minor: 0,
        max_minor: 0,
    });
    cap.supported_descriptor_digests.truncate(1);
    cap.tool_schema_sets.clear();
    cap.required = false;
    let mut offered = hello();
    offered
        .supported_profiles
        .push(cap.profile.clone().unwrap());
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    let wire = activation
        .receive_manifest(&manifest.encode_to_vec(), 102)
        .unwrap();
    assert!(wire_grant(&wire).selected_capabilities.is_empty());
    activation.receive_ready(&[], 103).unwrap();
    assert_eq!(activation.active_grant().unwrap().omitted(), ["component"]);

    manifest.capabilities[0].required = true;
    let mut activation = Activation::new(host("peer-a", 1), 100);
    activation
        .receive_hello(&offered.encode_to_vec(), 101)
        .unwrap();
    assert_eq!(
        activation
            .receive_manifest(&manifest.encode_to_vec(), 102)
            .unwrap_err()
            .reason,
        "required_capability_denied"
    );
}
#[test]
fn bundled_profile_digest_lookup_matches_selected_tool_vectors() {
    use pi_extension_protocol::descriptor::canonical_profile_digest;

    for (minor, expected) in [
        (
            0,
            "a995f8db0e619aeba78b054bc6bfbdcd9e3c8987e4e6535b6eb7d3585e60b8d3",
        ),
        (
            1,
            "1616abb00ef9a2089194e834fa5dc2de722cd4b56082adbbec992fca42f77d33",
        ),
    ] {
        assert_eq!(
            canonical_profile_digest("tool.v1", 1, minor)
                .unwrap()
                .to_vec(),
            decode_hex(expected)
        );
        for profile in [
            "hook.v1",
            "provider.v1",
            "event-sink.v1",
            "resource.v1",
            "host-action.v1",
            "telemetry-sink.v1",
        ] {
            assert!(
                canonical_profile_digest(profile, 1, minor).is_some(),
                "{profile}/{minor}"
            );
        }
    }
    assert!(canonical_profile_digest("tool.v1", 1, 999).is_none());
    assert!(canonical_profile_digest("host-action.v1", 2, 0).is_none());
    assert!(canonical_profile_digest("future.v1", 1, 0).is_none());
}

fn inventory_tool(
    parameters: Value,
    argument: &[u8],
    label: String,
    minors: &[u32],
) -> ApprovedTool {
    ApprovedTool::new(
        "peer-a".into(),
        AgentTool {
            name: "echo".into(),
            label,
            description: "Echo".into(),
            parameters,
            execution_mode: AgentToolMode::Sequential,
            replay: ToolReplay::Safe,
        },
        minors
            .iter()
            .map(|minor| ToolSchemaInputs {
                minor: *minor,
                arguments: argument.to_vec(),
                progress: br#"{"type":"null"}"#.to_vec(),
                raw_result: br#"{"type":"null"}"#.to_vec(),
            })
            .collect(),
        JsonLimits::default(),
    )
    .unwrap()
}

fn inventory_host(principal: String, label: String) -> HostAuthority {
    let argument = br#"{"type":"object","properties":{},"additionalProperties":false}"#;
    HostAuthority::new(
        principal,
        7,
        1,
        1,
        limits(),
        vec![inventory_tool(
            serde_json::from_slice(argument).unwrap(),
            argument,
            label,
            &[0, 1],
        )],
        vec![],
    )
    .unwrap()
}

#[test]
fn exact_capacities_and_all_six_private_projection_trees_are_borrowed() {
    let host = inventory_host("peer-a".into(), "Echo".into());
    let caps = OwnedInventoryCaps::default();
    let inventory = host.bounded_owned_inventory(&caps).unwrap();
    assert!(inventory.echo_family);
    assert!(inventory.byte_vec_capacity >= 6 * 15);
    assert!(inventory.other_vec_requested_bytes > 0);
    assert!(inventory.btree_entries >= 3);
    for field in 0..6 {
        let mut cap = caps;
        match field {
            0 => cap.string_capacity = inventory.string_capacity - 1,
            1 => cap.byte_vec_capacity = inventory.byte_vec_capacity - 1,
            2 => cap.other_vec_requested_bytes = inventory.other_vec_requested_bytes - 1,
            3 => cap.btree_entries = inventory.btree_entries - 1,
            4 => cap.nodes = inventory.nodes - 1,
            _ => cap.visits = inventory.visits - 1,
        }
        assert_eq!(
            host.bounded_owned_inventory(&cap).unwrap_err().reason,
            "owned_inventory_capacity"
        );
        assert_eq!(host.bounded_owned_inventory(&caps).unwrap(), inventory);
    }
    let mut exact = caps;
    exact.string_capacity = inventory.string_capacity;
    exact.byte_vec_capacity = inventory.byte_vec_capacity;
    exact.other_vec_requested_bytes = inventory.other_vec_requested_bytes;
    exact.btree_entries = inventory.btree_entries;
    exact.nodes = inventory.nodes;
    exact.visits = inventory.visits;
    assert_eq!(host.bounded_owned_inventory(&exact).unwrap(), inventory);
}

#[test]
fn short_strings_with_surplus_capacity_refuse_before_owned_activation() {
    let mut principal = String::with_capacity(257);
    principal.push_str("peer-a");
    let surplus_host = inventory_host(principal, "Echo".into());
    assert_eq!(
        surplus_host
            .bounded_owned_inventory(&OwnedInventoryCaps::default())
            .unwrap_err()
            .reason,
        "owned_inventory_capacity"
    );
    assert_eq!(
        Activation::new_bounded_receive(&surplus_host, 0)
            .err()
            .unwrap()
            .reason,
        "owned_inventory_capacity"
    );
    let mut label = String::with_capacity(65_537);
    label.push_str("Echo");
    let host = inventory_host("peer-a".into(), label);
    assert_eq!(
        host.bounded_owned_inventory(&OwnedInventoryCaps::default())
            .unwrap_err()
            .reason,
        "owned_inventory_capacity"
    );
}

#[test]
fn canonical_parameters_replace_surplus_map_and_array_reservations_for_both_minors() {
    let argument = br#"{"type":"object","properties":{},"additionalProperties":false}"#;
    let canonical: Value = serde_json::from_slice(argument).unwrap();
    let mut map = serde_json::Map::with_capacity(65_536);
    for (key, value) in canonical.as_object().unwrap() {
        map.insert(key.clone(), value.clone());
    }
    let approved = inventory_tool(Value::Object(map), argument, "Echo".into(), &[0, 1]);
    assert_eq!(approved.descriptor().parameters, canonical);
    let host =
        HostAuthority::new("peer-a".into(), 7, 1, 1, limits(), vec![approved], vec![]).unwrap();
    let inventory = host
        .bounded_owned_inventory(&OwnedInventoryCaps::default())
        .unwrap();
    assert!(inventory.requested_bytes < 100_000);

    let argument = br#"{"const":[]}"#;
    let mut surplus = Vec::with_capacity(65_536);
    surplus.clear();
    let parameters = serde_json::json!({"const": Value::Array(surplus)});
    let approved = inventory_tool(parameters, argument, "Echo".into(), &[0, 1]);
    let host =
        HostAuthority::new("peer-a".into(), 7, 1, 1, limits(), vec![approved], vec![]).unwrap();
    assert!(
        host.bounded_owned_inventory(&OwnedInventoryCaps::default())
            .unwrap()
            .other_vec_requested_bytes
            < 100_000
    );
}

#[test]
fn local_canonical_order_and_wrong_parameters_preserve_schema_semantics() {
    let argument = br#"{"type":"object","properties":{"z":{"type":"null"},"a":{"type":"null"}}}"#;
    let caller: Value = serde_json::from_slice(argument).unwrap();
    let approved = inventory_tool(caller.clone(), argument, "Echo".into(), &[0, 1]);
    assert_eq!(approved.descriptor().parameters, caller);
    let normalized = serde_json::to_string(&approved.descriptor().parameters).unwrap();
    assert!(normalized.find("\"a\"").unwrap() < normalized.find("\"z\"").unwrap());
    let mut descriptor = approved.descriptor().clone();
    descriptor.parameters = Value::Null;
    for minor in [0, 1] {
        assert_eq!(
            ApprovedTool::new(
                "peer-a".into(),
                descriptor.clone(),
                vec![ToolSchemaInputs {
                    minor,
                    arguments: argument.to_vec(),
                    progress: br#"{"type":"null"}"#.to_vec(),
                    raw_result: br#"{"type":"null"}"#.to_vec(),
                }],
                JsonLimits::default(),
            )
            .unwrap_err()
            .reason,
            "host_parameters_invalid"
        );
    }
    let host = inventory_host("peer-a".into(), "Echo".into());
    let inventory = host
        .bounded_owned_inventory(&OwnedInventoryCaps::default())
        .unwrap();
    match inventory.parameter_map {
        ParameterMapRecipe::BTreeFresh => assert_eq!(std::mem::size_of::<Value>(), 32),
        ParameterMapRecipe::IndexMapFreshSse2 => assert_eq!(std::mem::size_of::<Value>(), 72),
    }
}

#[test]
fn pre4096_is_optional_and_checked_before_receive_clone() {
    let mut protocol_limits = limits();
    protocol_limits.pre_handshake_frame_bytes = 4097;
    let host =
        HostAuthority::new("peer-a".into(), 7, 1, 1, protocol_limits, vec![], vec![]).unwrap();
    assert_eq!(
        Activation::new_bounded_receive(&host, 0)
            .err()
            .unwrap()
            .reason,
        "owned_receive_frame_capacity"
    );
    assert_eq!(
        Activation::new(host, 0).phase(),
        ActivationPhase::AwaitHello
    );
}

#[test]
fn grant4096_precedes_old_minor_reencode_and_new_minor_clone() {
    let permissions = (0..79)
        .map(|i| v1_1::PermissionGrant {
            name: format!("n{i}"),
            scope: "s".repeat(40),
        })
        .collect::<Vec<_>>();
    let manifest = v1_1::Manifest {
        protocol: Some(v1_1::VersionRange {
            major: 1,
            min_minor: 0,
            max_minor: 1,
        }),
        declared_extension_id: "x".into(),
        extension_version: "x".into(),
        permissions: permissions
            .iter()
            .map(|permission| v1_1::PermissionRequest {
                name: permission.name.clone(),
                scope: permission.scope.clone(),
                required: true,
            })
            .collect(),
        ..v1_1::Manifest::default()
    };
    for minor in [0, 1] {
        let host = HostAuthority::new(
            "p".repeat(256),
            7,
            minor,
            minor,
            limits(),
            vec![],
            permissions.clone(),
        )
        .unwrap();
        let offered = v1_1::Hello {
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: 1,
            }),
            supported_profiles: vec![],
        };
        let input = manifest.encode_to_vec();
        assert!(input.len() <= 4096);
        let mut ordinary = Activation::new(host.clone(), 0);
        ordinary.receive_hello(&offered.encode_to_vec(), 1).unwrap();
        let grant = ordinary.receive_manifest(&input, 2).unwrap();
        let size = match grant {
            WireGrant::V1_0(grant) => grant.encoded_len(),
            WireGrant::V1_1(grant) => grant.encoded_len(),
        };
        assert!(size > 4096);
        let mut bounded = Activation::new_bounded_receive(&host, 0).unwrap();
        bounded.receive_hello(&offered.encode_to_vec(), 1).unwrap();
        assert_eq!(
            bounded.receive_manifest(&input, 2).unwrap_err().reason,
            "owned_receive_grant_capacity"
        );
        assert!(bounded.active_grant().is_none());
    }
}

#[test]
fn e1421_hpre_inventory_capacity_minus_one_precedes_root_or_clone() {
    let host = inventory_host("peer-a".into(), "Echo".into());
    let inventory = host
        .bounded_owned_inventory(&OwnedInventoryCaps::default())
        .unwrap();
    let caps = OwnedInventoryCaps {
        visits: inventory.visits - 1,
        ..OwnedInventoryCaps::default()
    };
    assert_eq!(
        host.bounded_owned_inventory(&caps).unwrap_err().reason,
        "owned_inventory_capacity"
    );
    let bounded = Activation::new_bounded_receive(&host, 0).unwrap();
    assert_eq!(bounded.phase(), ActivationPhase::AwaitHello);
    assert!(bounded.active_grant().is_none());
    assert_eq!(
        Activation::new(host, 0).phase(),
        ActivationPhase::AwaitHello
    );
}
