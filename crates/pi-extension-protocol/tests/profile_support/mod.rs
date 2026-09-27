#![allow(dead_code)]
use pi_extension_protocol::{
    activation::*,
    descriptor::canonical_profile_digest,
    jcs,
    profiles::*,
    wire::{v1_1, value},
};
use prost::Message;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
pub fn vectors() -> Value {
    serde_json::from_slice(include_bytes!(
        "../../../../conformance/extension-protocol/six-profiles-v1.json"
    ))
    .unwrap()
}
pub fn registry() -> Registry {
    Registry::bundled().unwrap()
}
pub fn limits() -> v1_1::ProtocolLimits {
    v1_1::ProtocolLimits {
        pre_handshake_frame_bytes: 262_144,
        frame_bytes: 262_144,
        aggregate_decoded_bytes: 1_048_576,
        value_depth: 64,
        value_elements: 16_384,
        string_bytes: 262_144,
        bytes_length: 262_144,
        in_flight_requests: 32,
        blocking_queue_bytes: 1_048_576,
        progress_queue_bytes: 1_048_576,
        passive_queue_bytes: 1_048_576,
        telemetry_queue_bytes: 1_048_576,
        nesting_depth: 8,
        requests_per_second: 100,
        handshake_ms: 1000,
        cancellation_grace_ms: 20,
        max_stream_credit_bytes: 1_048_576,
    }
}
pub fn bound(root: &str, v: &Value) -> v1_1::SchemaBoundValue {
    v1_1::SchemaBoundValue {
        projection_id: root.into(),
        schema_digest_sha256: registry().digest(root).unwrap().to_vec(),
        value: Some(json_wire(v).unwrap()),
    }
}
pub fn schema(v: Value) -> jcs::PortableSchema {
    jcs::admit_schema(&serde_json::to_vec(&v).unwrap(), jcs::Limits::default()).unwrap()
}
pub fn exposure() -> Exposure {
    let empty = schema(json!({"type":"object","properties":{},"additionalProperties":false}));
    let null = schema(json!({"type":"null"}));
    let mut leaves = BTreeMap::new();
    for path in [
        "hook.input:before_tool.event.args",
        "hook.input:after_tool.event.args",
        "provider.tools.v1[].parameters",
        "provider.assistant_event.v1.toolCall.arguments",
    ] {
        leaves.insert(path.into(), empty.clone());
    }
    leaves.insert(
        "hook.input:before_payload.event.payload".into(),
        null.clone(),
    );
    leaves.insert("hook.output:before_payload.result.payload".into(), null);
    Exposure::new(leaves, BTreeSet::new()).unwrap()
}
pub fn profile(
    profile: &str,
    name: &str,
    scope: &str,
    permissions: BTreeSet<(String, String)>,
) -> ApprovedProfile {
    let execution = match profile {
        "provider.v1" => 2,
        "event-sink.v1" | "telemetry-sink.v1" => 3,
        _ => 1,
    };
    ApprovedProfile::new(ProfileApproval {
        principal: "peer-a".into(),
        kind: profile.trim_end_matches(".v1").into(),
        name: name.into(),
        profile: profile.into(),
        handler: "qualified_handler".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution,
        scope: scope.into(),
        permissions,
        exposure: exposure(),
    })
    .unwrap()
}
pub fn activate(
    core: u32,
    profiles: Vec<ApprovedProfile>,
    permissions: Vec<v1_1::PermissionGrant>,
) -> ActiveGrant {
    let mut a = Activation::new(
        HostAuthority::new(
            "peer-a".into(),
            7,
            core,
            1,
            limits(),
            vec![],
            permissions.clone(),
        )
        .unwrap()
        .with_profiles(profiles.clone())
        .unwrap(),
        0,
    );
    let range = |name: String| v1_1::ProfileRange {
        name,
        major: 1,
        min_minor: 0,
        max_minor: 1,
    };
    a.receive_hello(
        &v1_1::Hello {
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: core,
            }),
            supported_profiles: profiles
                .iter()
                .map(|p| (p.profile(), range(p.profile().into())))
                .collect::<BTreeMap<_, _>>()
                .into_values()
                .collect(),
        }
        .encode_to_vec(),
        1,
    )
    .unwrap();
    let caps = profiles
        .iter()
        .map(|p| v1_1::CapabilityDeclaration {
            kind: p.kind().into(),
            name: p.name().into(),
            profile: Some(range(p.profile().into())),
            handler_id: p.handler().into(),
            replay: p.replay(),
            execution: p.execution(),
            required: true,
            supported_descriptor_digests: (0..=1)
                .map(|minor| v1_1::SupportedProfileDigest {
                    minor,
                    descriptor_digest_sha256: canonical_profile_digest(p.profile(), 1, minor)
                        .unwrap()
                        .to_vec(),
                })
                .collect(),
            display_label: None,
            tool_schema_sets: vec![],
        })
        .collect();
    a.receive_manifest(
        &v1_1::Manifest {
            declared_extension_id: "peer-claimed".into(),
            extension_version: "1".into(),
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: core,
            }),
            capabilities: caps,
            permissions: permissions
                .iter()
                .map(|p| v1_1::PermissionRequest {
                    name: p.name.clone(),
                    scope: p.scope.clone(),
                    required: true,
                })
                .collect(),
            subscriptions: vec![],
        }
        .encode_to_vec(),
        2,
    )
    .unwrap();
    a.receive_ready(&[], 3).unwrap();
    a.active_grant().unwrap().clone()
}
#[derive(Default)]
pub struct Native {
    pub work: usize,
    pub fail: Option<NativeError>,
}
impl NativeValidator for Native {
    fn validate(&mut self, _candidate: &ValidatedPayload) -> Result<(), NativeError> {
        self.work += 1;
        self.fail.map_or(Ok(()), Err)
    }
}
pub fn value_limits() -> value::Limits {
    value::Limits::default()
}
