mod profile_support;
use pi_extension_protocol::{
    activation::*,
    descriptor::canonical_profile_digest,
    jcs,
    profiles::*,
    wire::{v1_0, v1_1},
};
use profile_support::*;
use prost::Message;
use serde_json::{Value, json};
use std::collections::BTreeSet;

#[test]
fn exact_derivatives_and_all_projection_digests_shapes_both_minors() {
    let r = registry();
    let v = vectors();
    assert_eq!(r.roots().count(), 37);
    let registry_hash = ring::digest::digest(&ring::digest::SHA256, STATIC_REGISTRY);
    assert_eq!(
        registry_hash.as_ref(),
        &unhex("a735c6862639f36a24e3190d650207193ef125b6a5b329a30701c2ace3aa60be")
    );
    let vector_bytes =
        include_bytes!("../../../conformance/extension-protocol/six-profiles-v1.json");
    assert_eq!(
        ring::digest::digest(&ring::digest::SHA256, vector_bytes).as_ref(),
        &unhex("dfd59501a43b34abb40bf7ef5985f6715a14c8cdd4c37ed98567ed2371fbad37")
    );
    let mut count = 0;
    for case in v["projection_vectors"].as_array().unwrap() {
        let Some(root) = case["projection"].as_str() else {
            continue;
        };
        let Some(value) = case.get("value") else {
            continue;
        };
        let id = case["id"].as_str().unwrap();
        if root == "hook.output:before_resume" {
            assert!(
                r.validate_v1_1(root, &bound(root, value), value_limits(), 262_144)
                    .is_err()
            );
            continue;
        }
        let positive = id.ends_with(".positive");
        let mut b = bound(root, value);
        if id.ends_with(".wrong_digest") {
            b.schema_digest_sha256[0] ^= 1;
        }
        let result = r.validate_v1_1(root, &b, value_limits(), 262_144);
        if positive {
            assert!(result.is_ok(), "{id}: {result:?}");
            count += 1;
        } else {
            assert!(result.is_err(), "{id}");
        }
        let old = v1_0::SchemaBoundValue::decode(b.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            r.validate_v1_0(root, &old, value_limits(), 262_144).is_ok(),
            positive,
            "minor0 {id}"
        );
    }
    assert_eq!(count, 36);
    for root in r.roots().filter(|r| *r != "hook.output:before_resume") {
        let missing = v1_1::SchemaBoundValue {
            projection_id: root.into(),
            schema_digest_sha256: r.digest(root).unwrap().to_vec(),
            value: None,
        };
        assert!(
            r.validate_v1_1(root, &missing, value_limits(), 262_144)
                .is_err()
        );
    }
}
fn unhex(s: &str) -> Vec<u8> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}

#[test]
fn json_wire_sorts_map_entries_recursively() {
    let input: Value =
        serde_json::from_str(r#"{"z":{"z":1,"a":[{"z":2,"a":3}]},"a":0,"m":{"z":4,"a":5}}"#)
            .unwrap();
    let wire = json_wire(&input).unwrap();

    fn assert_sorted(value: &v1_1::WireValue) {
        match value.value.as_ref() {
            Some(v1_1::wire_value::Value::MapValue(map)) => {
                let keys = map
                    .entries
                    .iter()
                    .map(|entry| entry.key.as_str())
                    .collect::<Vec<_>>();
                let mut sorted = keys.clone();
                sorted.sort_unstable();
                assert_eq!(keys, sorted);
                for entry in &map.entries {
                    assert_sorted(entry.value.as_ref().unwrap());
                }
            }
            Some(v1_1::wire_value::Value::ListValue(list)) => {
                for item in &list.items {
                    assert_sorted(item);
                }
            }
            _ => {}
        }
    }

    assert_sorted(&wire);
    let Some(v1_1::wire_value::Value::MapValue(root)) = wire.value.as_ref() else {
        panic!("expected root map");
    };
    assert_eq!(
        root.entries
            .iter()
            .map(|entry| entry.key.as_str())
            .collect::<Vec<_>>(),
        ["a", "m", "z"]
    );
}

#[test]
fn every_static_root_checks_missing_wrong_projection_and_digest_in_both_minors() {
    let r = registry();
    let v = vectors();
    let cases = v["projection_vectors"].as_array().unwrap();
    for root in r
        .roots()
        .filter(|root| *root != "hook.output:before_resume")
    {
        let sample = cases
            .iter()
            .find(|c| {
                c["projection"] == root
                    && c["id"].as_str().is_some_and(|id| id.ends_with(".positive"))
            })
            .unwrap();
        for kind in [0, 1] {
            let mut b = v1_1::SchemaBoundValue {
                projection_id: root.into(),
                schema_digest_sha256: r.digest(root).unwrap().to_vec(),
                value: Some(json_wire(&sample["value"]).unwrap()),
            };
            if kind == 0 {
                b.projection_id = "peer-alias".into();
            } else {
                b.schema_digest_sha256[0] ^= 1;
            }
            assert_eq!(
                r.validate_v1_1(root, &b, value_limits(), 262_144),
                Err(Error::Projection)
            );
            let old = v1_0::SchemaBoundValue::decode(b.encode_to_vec().as_slice()).unwrap();
            assert_eq!(
                r.validate_v1_0(root, &old, value_limits(), 262_144),
                Err(Error::Projection)
            );
        }
    }
}

#[test]
fn native_field_presence_integer_null_variants_and_correction_controls() {
    let r = registry();
    let v = vectors();
    for (name, table) in v["native_type_field_default_vectors"].as_object().unwrap() {
        if matches!(name.as_str(), "ModelCompat" | "hook.output:before_resume") {
            continue;
        }
        let specimen = json_wire(&table["specimen"]).unwrap();
        assert!(
            r.validate_type(name, &specimen, value_limits(), 1_000_000)
                .is_ok(),
            "{name}"
        );
        if let Some(variants) = table["variant_specimens"].as_array() {
            for variant in variants {
                assert!(
                    r.validate_type(
                        name,
                        &json_wire(&variant["value"]).unwrap(),
                        value_limits(),
                        1_000_000
                    )
                    .is_ok(),
                    "{name} {}",
                    variant["variant"]
                );
            }
        }
    }
    for name in ["SafeUnsigned", "OneBasedSafeUnsigned"] {
        for value in [
            json!(1.0),
            json!(true),
            json!(-1),
            json!(9_007_199_254_740_992_i64),
        ] {
            assert!(
                r.validate_type(name, &json_wire(&value).unwrap(), value_limits(), 262_144)
                    .is_err()
            );
        }
    }
    let usage = v["native_type_field_default_vectors"]["Usage"]["specimen"].clone();
    let mut u = usage.clone();
    u["cacheWrite1h"] = json!(3);
    assert!(
        r.validate_type("Usage", &json_wire(&u).unwrap(), value_limits(), 262_144)
            .is_ok()
    );
    u.as_object_mut().unwrap().remove("cacheWrite1h");
    u["cacheWrite1H"] = json!(3);
    assert!(
        r.validate_type("Usage", &json_wire(&u).unwrap(), value_limits(), 262_144)
            .is_err()
    );
    for data in [None, Some(Value::Null), Some(json!({"x":1}))] {
        let mut p = json!({"entryId":"e","type":"custom"});
        if let Some(data) = data {
            p["data"] = data;
        }
        assert!(
            r.validate_type(
                "PendingCustomWrite",
                &json_wire(&p).unwrap(),
                value_limits(),
                262_144
            )
            .is_ok()
        );
    }
    assert!(
        r.validate_type(
            "AgentToolResult",
            &json_wire(&json!({"content":[],"details":null})).unwrap(),
            value_limits(),
            262_144
        )
        .is_ok()
    );
    assert!(
        r.validate_type(
            "AgentToolResult",
            &json_wire(&json!({"content":[]})).unwrap(),
            value_limits(),
            262_144
        )
        .is_err()
    );
    for good in [json!({}), json!({"decline":false}), json!({"decline":true})] {
        assert!(
            r.validate_type(
                "BeforeCompactionResult",
                &json_wire(&good).unwrap(),
                value_limits(),
                262_144
            )
            .is_ok()
        );
    }
    assert!(
        r.validate_type(
            "BeforeCompactionResult",
            &json_wire(&json!({"decline":null})).unwrap(),
            value_limits(),
            262_144
        )
        .is_err()
    );
    let mut m = bound("provider.context.v1", &json!({"messages":[]}));
    let w = m.value.as_mut().unwrap();
    if let Some(v1_1::wire_value::Value::MapValue(map)) = &mut w.value {
        map.entries.push(map.entries[0].clone());
    }
    assert!(
        r.validate_v1_1("provider.context.v1", &m, value_limits(), 262_144)
            .is_err()
    );
    assert!(
        r.validate_v1_1(
            "provider.context.v1",
            &bound("provider.context.v1", &json!({"messages":[]})),
            value_limits(),
            1
        )
        .is_err()
    );
}

fn wire_fixtures() -> Value {
    serde_json::from_slice(include_bytes!("../schema/projections/method-wire-v1.json")).unwrap()
}
fn profile_for_method(method: &str, profile_name: &str) -> ApprovedProfile {
    profile(
        profile_name,
        if profile_name == "hook.v1" {
            method
        } else {
            "catalog"
        },
        "allowed",
        BTreeSet::new(),
    )
}
#[test]
fn all_25_methods_canonical_descriptor_protoc_payloads_both_minors() {
    let fixtures = wire_fixtures();
    let rows = fixtures["methods"].as_array().unwrap();
    assert_eq!(rows.len(), 50);
    let mut seen = BTreeSet::new();
    for row in rows {
        let method = row["method"].as_str().unwrap();
        let profile_name = row["profile"].as_str().unwrap();
        let minor = row["minor"].as_u64().unwrap() as u32;
        let p = profile_for_method(method, profile_name);
        let mut native = Native::default();
        seen.insert(method);
        for (stage, payload) in row["payloads"].as_object().unwrap() {
            let stage = match stage.as_str() {
                "request" => Stage::Request,
                "progress" => Stage::Progress,
                "terminal" => Stage::Terminal,
                _ => panic!("stage"),
            };
            let bytes = unhex(payload["hex"].as_str().unwrap());
            let admitted = p.validate_payload(
                minor,
                method,
                stage,
                &bytes,
                value_limits(),
                jcs::Limits::default(),
                &mut native,
            );
            assert!(
                admitted.is_ok(),
                "{method} minor{minor} {stage:?}: {admitted:?}"
            );
            assert_eq!(payload["protoc_decode_exit"], 0);
        }
        if profile_name == "hook.v1" {
            let default = p
                .validate_payload(
                    minor,
                    method,
                    Stage::Terminal,
                    &[],
                    value_limits(),
                    jcs::Limits::default(),
                    &mut native,
                )
                .unwrap();
            assert!(
                default.projections.is_empty(),
                "absent {method} output was manufactured"
            );
        }
        assert!(
            p.validate_payload(
                2,
                method,
                Stage::Request,
                &[],
                value_limits(),
                jcs::Limits::default(),
                &mut native
            )
            .is_err()
        );
    }
    assert_eq!(seen.len(), 25);
}

#[test]
fn host_catalog_minor_ceiling_atomic_optional_denial_and_native_faults() {
    let profiles = vec![
        profile("provider.v1", "remote", "allowed", BTreeSet::new()),
        profile("resource.v1", "resources", "allowed", BTreeSet::new()),
    ];
    for core in [0, 1] {
        for profile_minor in [0, 1] {
            let mut chosen = profiles.clone();
            let p = ProfileApproval {
                principal: "peer-a".into(),
                kind: "provider".into(),
                name: "remote".into(),
                profile: "provider.v1".into(),
                handler: "qualified_handler".into(),
                minors: BTreeSet::from([profile_minor]),
                replay: 1,
                execution: 2,
                scope: "allowed".into(),
                permissions: BTreeSet::new(),
                exposure: exposure(),
            };
            chosen[0] = ApprovedProfile::new(p).unwrap();
            let g = activate(core, chosen, vec![]);
            assert_eq!(g.wire().authenticated_extension_id, "peer-a");
            assert_eq!(
                g.profiles()[0].reference().profile.as_ref().unwrap().minor,
                profile_minor
            );
            assert_eq!(g.wire().selected_protocol.unwrap().minor, core);
        }
    }
    let p = profile("provider.v1", "remote", "allowed", BTreeSet::new());
    let cap = v1_1::CapabilityDeclaration {
        kind: "provider".into(),
        name: "remote".into(),
        profile: Some(v1_1::ProfileRange {
            name: "provider.v1".into(),
            major: 1,
            min_minor: 0,
            max_minor: 1,
        }),
        handler_id: "qualified_handler".into(),
        replay: 1,
        execution: 2,
        required: false,
        supported_descriptor_digests: (0..=1)
            .map(|minor| v1_1::SupportedProfileDigest {
                minor,
                descriptor_digest_sha256: canonical_profile_digest("provider.v1", 1, minor)
                    .unwrap()
                    .to_vec(),
            })
            .collect(),
        display_label: None,
        tool_schema_sets: vec![],
    };
    for defect in [0, 1, 2, 3] {
        let host = HostAuthority::new("peer-a".into(), 7, 1, 1, limits(), vec![], vec![])
            .unwrap()
            .with_profiles(vec![p.clone()])
            .unwrap();
        let mut a = Activation::new(host, 0);
        a.receive_hello(
            &v1_1::Hello {
                protocol: Some(v1_1::VersionRange {
                    major: 1,
                    min_minor: 0,
                    max_minor: 1,
                }),
                supported_profiles: vec![cap.profile.clone().unwrap()],
            }
            .encode_to_vec(),
            1,
        )
        .unwrap();
        let mut bad = cap.clone();
        if defect == 0 {
            bad.supported_descriptor_digests[0].descriptor_digest_sha256[0] ^= 1;
        }
        if defect == 1 {
            bad.replay = 91;
        }
        if defect == 2 {
            bad.supported_descriptor_digests.pop();
        }
        if defect == 3 {
            bad.required = true;
            bad.handler_id = "peer-replaced".into();
        }
        let m = v1_1::Manifest {
            declared_extension_id: "claimed".into(),
            extension_version: "1".into(),
            protocol: Some(v1_1::VersionRange {
                major: 1,
                min_minor: 0,
                max_minor: 1,
            }),
            capabilities: vec![bad],
            permissions: vec![],
            subscriptions: vec![],
        };
        let result = a.receive_manifest(&m.encode_to_vec(), 2);
        if defect == 0 {
            assert!(result.is_ok());
            a.receive_ready(&[], 3).unwrap();
            assert!(a.active_grant().unwrap().profiles().is_empty());
        } else {
            assert!(result.is_err());
            assert!(a.active_grant().is_none());
        }
    }
    let fixture = wire_fixtures();
    let row = fixture["methods"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["method"] == "provider.invoke")
        .unwrap();
    let bytes = unhex(row["payloads"]["request"]["hex"].as_str().unwrap());
    let mut native = Native {
        work: 0,
        fail: Some(NativeError::LocalFault),
    };
    assert_eq!(
        p.validate_payload(
            0,
            "provider.invoke",
            Stage::Request,
            &bytes,
            value_limits(),
            jcs::Limits::default(),
            &mut native
        ),
        Err(Error::HostLocalFault)
    );
    native.fail = Some(NativeError::Rejected);
    assert_eq!(
        p.validate_payload(
            0,
            "provider.invoke",
            Stage::Request,
            &bytes,
            value_limits(),
            jcs::Limits::default(),
            &mut native
        ),
        Err(Error::NativeRejected)
    );
}

#[test]
fn exposure_and_auth_none_reject_secret_sentinels_without_native_work() {
    let p = profile("provider.v1", "remote", "allowed", BTreeSet::new());
    let fixture = wire_fixtures();
    let row = fixture["methods"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["method"] == "provider.invoke")
        .unwrap();
    let bytes = unhex(row["payloads"]["request"]["hex"].as_str().unwrap());
    let original = wire_v1_1::InvokeProviderRequest::decode(bytes.as_slice()).unwrap();
    for kind in 0..4 {
        let mut r = original.clone();
        match kind {
            0 => r.auth_mode = "api-key".into(),
            1 => {
                let mut j = registry()
                    .validate_v1_1(
                        "provider.model.v1",
                        r.model.as_ref().unwrap(),
                        value_limits(),
                        262_144,
                    )
                    .unwrap();
                j["headers"] = json!({});
                r.model = Some(bound("provider.model.v1", &j));
            }
            2 => {
                r.options = Some(bound(
                    "provider.options.v1",
                    &json!({"headers":{"Authorization":"SECRET_SENTINEL"}}),
                ))
            }
            _ => {
                r.context = Some(bound(
                    "provider.context.v1",
                    &json!({"messages":[{"role":"registered_custom","value":"SECRET_SENTINEL"}]}),
                ))
            }
        };
        let mut native = Native::default();
        assert!(
            p.validate_payload(
                1,
                "provider.invoke",
                Stage::Request,
                &r.encode_to_vec(),
                value_limits(),
                jcs::Limits::default(),
                &mut native
            )
            .is_err()
        );
        assert_eq!(native.work, 0);
    }
}

#[test]
fn local_dtos_roundtrip_every_canonical_field_optional_presence_and_oneof_arm() {
    let fixtures = wire_fixtures();
    let rows = fixtures["field_witnesses"].as_array().unwrap();
    assert_eq!(rows.len(), 78);
    macro_rules! selected {
        ($module:ident,$name:expr,$bytes:expr) => {{
            match $name {
                "BeforeResumeUnitResult" => {
                    $module::BeforeResumeUnitResult::decode($bytes.as_slice())
                        .unwrap()
                        .encode_to_vec()
                }
                "EventGap" => $module::EventGap::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "EventNotification" => $module::EventNotification::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "EventSinkItem" => $module::EventSinkItem::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "InvokeHookRequest" => $module::InvokeHookRequest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "InvokeHookResult" => $module::InvokeHookResult::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "InvokeProviderRequest" => {
                    $module::InvokeProviderRequest::decode($bytes.as_slice())
                        .unwrap()
                        .encode_to_vec()
                }
                "InvokeProviderResult" => $module::InvokeProviderResult::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "OperationAbortRequest" => {
                    $module::OperationAbortRequest::decode($bytes.as_slice())
                        .unwrap()
                        .encode_to_vec()
                }
                "OperationAbortResult" => $module::OperationAbortResult::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ProviderStreamEvent" => $module::ProviderStreamEvent::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "QueueFollowUpRequest" => $module::QueueFollowUpRequest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "QueueNextRunRequest" => $module::QueueNextRunRequest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "QueueResult" => $module::QueueResult::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "QueueSteerRequest" => $module::QueueSteerRequest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResourceChanged" => $module::ResourceChanged::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResourceChangedAck" => $module::ResourceChangedAck::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResourceGetRequest" => $module::ResourceGetRequest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResourceGetResult" => $module::ResourceGetResult::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResourceListRequest" => $module::ResourceListRequest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResourceListResult" => $module::ResourceListResult::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResourceManifest" => $module::ResourceManifest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResourceReadRequest" => $module::ResourceReadRequest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResourceReadResult" => $module::ResourceReadResult::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResyncRequest" => $module::ResyncRequest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "ResyncResult" => $module::ResyncResult::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "SnapshotGetRequest" => $module::SnapshotGetRequest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "SnapshotGetResult" => $module::SnapshotGetResult::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "SubscribeEnd" => $module::SubscribeEnd::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "SubscribeRequest" => $module::SubscribeRequest::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "SubscriptionSnapshot" => $module::SubscriptionSnapshot::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                "TelemetryEmitBatchRequest" => {
                    $module::TelemetryEmitBatchRequest::decode($bytes.as_slice())
                        .unwrap()
                        .encode_to_vec()
                }
                "TelemetryEmitBatchResult" => {
                    $module::TelemetryEmitBatchResult::decode($bytes.as_slice())
                        .unwrap()
                        .encode_to_vec()
                }
                "TelemetryFlushRequest" => {
                    $module::TelemetryFlushRequest::decode($bytes.as_slice())
                        .unwrap()
                        .encode_to_vec()
                }
                "TelemetryFlushResult" => $module::TelemetryFlushResult::decode($bytes.as_slice())
                    .unwrap()
                    .encode_to_vec(),
                _ => panic!("uncovered DTO {}", $name),
            }
        }};
    }
    for row in rows {
        let bytes = unhex(row["hex"].as_str().unwrap());
        let name = row["message"].as_str().unwrap();
        let encoded = if row["minor"] == 0 {
            selected!(wire_v1_0, name, bytes)
        } else {
            selected!(wire_v1_1, name, bytes)
        };
        assert_eq!(encoded, bytes, "{name} {}", row["minor"]);
        assert_eq!(row["protoc_decode_exit"], 0);
    }
}

#[test]
fn aggregate_bound_budget_and_required_exposure_are_enforced_before_native_callback() {
    let fixture = wire_fixtures();
    let row = fixture["methods"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["method"] == "provider.invoke")
        .unwrap();
    let bytes = unhex(row["payloads"]["request"]["hex"].as_str().unwrap());
    let mut request = wire_v1_1::InvokeProviderRequest::decode(bytes.as_slice()).unwrap();
    request.options = Some(bound("provider.options.v1", &json!({"timeoutMs":1})));
    let bytes = request.encode_to_vec();
    let p = profile("provider.v1", "remote", "allowed", BTreeSet::new());
    let mut native = Native::default();
    let mut limits = value_limits();
    limits.elements = 15;
    assert!(
        p.validate_payload(
            1,
            "provider.invoke",
            Stage::Request,
            &bytes,
            limits,
            jcs::Limits::default(),
            &mut native
        )
        .is_err()
    );
    assert_eq!(native.work, 0);
    let missing = ProfileApproval {
        principal: "peer-a".into(),
        kind: "hook".into(),
        name: "before_payload".into(),
        profile: "hook.v1".into(),
        handler: "qualified_handler".into(),
        minors: BTreeSet::from([0, 1]),
        replay: 1,
        execution: 1,
        scope: "allowed".into(),
        permissions: BTreeSet::new(),
        exposure: Exposure::new(Default::default(), Default::default()).unwrap(),
    };
    assert!(matches!(
        ApprovedProfile::new(missing),
        Err(Error::HostContract)
    ));
}

#[test]
fn static_projection_identity_guard_rejects_normalized_wrong_id_and_digest() {
    let r = registry();
    let mut b = bound("provider.context.v1", &json!({"messages":[]}));
    assert!(
        r.validate_v1_1("provider.context.v1", &b, value_limits(), 262_144)
            .is_ok()
    );
    b.projection_id = "peer-alias".into();
    assert_eq!(
        r.validate_v1_1("provider.context.v1", &b, value_limits(), 262_144),
        Err(Error::Projection)
    );
    b.projection_id = "provider.context.v1".into();
    b.schema_digest_sha256[0] ^= 1;
    assert_eq!(
        r.validate_v1_1("provider.context.v1", &b, value_limits(), 262_144),
        Err(Error::Projection)
    );
}

#[test]
fn real_endpoint_ready_freezes_catalog_for_all_four_core_profile_pairs() {
    use pi_extension_protocol::profile_live::Reservations;
    use pi_extension_protocol::{
        endpoint::{Endpoint, EndpointEvent},
        wire::framing,
    };
    use std::num::NonZeroUsize;
    let reservations = Reservations {
        blocking_bytes: 1_048_576,
        control_bytes: 8_388_608,
        terminal_bytes: 8_388_608,
        progress_bytes: 1_048_576,
        passive_bytes: 1_048_576,
        telemetry_bytes: 1_048_576,
    };
    for core in [0, 1] {
        for minor in [0, 1] {
            let approved = ApprovedProfile::new(ProfileApproval {
                principal: "peer-a".into(),
                kind: "provider".into(),
                name: "remote".into(),
                profile: "provider.v1".into(),
                handler: "qualified_handler".into(),
                minors: BTreeSet::from([minor]),
                replay: 1,
                execution: 2,
                scope: "allowed".into(),
                permissions: BTreeSet::new(),
                exposure: exposure(),
            })
            .unwrap();
            let host = HostAuthority::new("peer-a".into(), 7, core, 1, limits(), vec![], vec![])
                .unwrap()
                .with_profiles(vec![approved])
                .unwrap();
            let mut endpoint = Endpoint::new(host, 0);
            assert!(endpoint.profile_owner(reservations, 0, 200).is_err());
            let send = |arm| {
                framing::encode(
                    &v1_1::Envelope { frame: Some(arm) },
                    NonZeroUsize::new(262_144).unwrap(),
                )
                .unwrap()
            };
            let hello = send(v1_1::envelope::Frame::Hello(v1_1::Hello {
                protocol: Some(v1_1::VersionRange {
                    major: 1,
                    min_minor: 0,
                    max_minor: core,
                }),
                supported_profiles: vec![v1_1::ProfileRange {
                    name: "provider.v1".into(),
                    major: 1,
                    min_minor: 0,
                    max_minor: 1,
                }],
            }));
            assert!(matches!(
                endpoint.read_one(&hello.bytes, 1).unwrap().event,
                Some(EndpointEvent::Send(_))
            ));
            let manifest = send(v1_1::envelope::Frame::Manifest(v1_1::Manifest {
                declared_extension_id: "peer-claim".into(),
                extension_version: "1".into(),
                protocol: Some(v1_1::VersionRange {
                    major: 1,
                    min_minor: 0,
                    max_minor: core,
                }),
                capabilities: vec![v1_1::CapabilityDeclaration {
                    kind: "provider".into(),
                    name: "remote".into(),
                    profile: Some(v1_1::ProfileRange {
                        name: "provider.v1".into(),
                        major: 1,
                        min_minor: 0,
                        max_minor: 1,
                    }),
                    handler_id: "qualified_handler".into(),
                    replay: 1,
                    execution: 2,
                    required: true,
                    supported_descriptor_digests: (0..=1)
                        .map(|m| v1_1::SupportedProfileDigest {
                            minor: m,
                            descriptor_digest_sha256: canonical_profile_digest("provider.v1", 1, m)
                                .unwrap()
                                .to_vec(),
                        })
                        .collect(),
                    display_label: None,
                    tool_schema_sets: vec![],
                }],
                permissions: vec![],
                subscriptions: vec![],
            }));
            endpoint.read_one(&manifest.bytes, 2).unwrap();
            assert!(endpoint.profile_owner(reservations, 2, 200).is_err());
            let ready = send(v1_1::envelope::Frame::Ready(v1_1::Ready {}));
            assert!(matches!(
                endpoint.read_one(&ready.bytes, 3).unwrap().event,
                Some(EndpointEvent::Ready)
            ));
            let grant = endpoint.active_grant().unwrap();
            assert_eq!(grant.wire().authenticated_extension_id, "peer-a");
            assert_eq!(
                grant.profiles()[0]
                    .reference()
                    .profile
                    .as_ref()
                    .unwrap()
                    .minor,
                minor
            );
            assert!(endpoint.profile_owner(reservations, 3, 200).is_ok());
        }
    }
}
