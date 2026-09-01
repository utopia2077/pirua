use std::collections::BTreeMap;

use pi_agent_schema::state::v1::ToolCallState;
use pi_agent_schema::{
    AgentHarnessStreamOptions, AgentMessage, CommitResult, DurableStructuralPreparation, Entry,
    LaneLastResult, Operation, OperationState, ProviderHeaderClassificationError,
    ProviderHeaderPolicy, RawAgentHarnessStreamOptions, RawProviderHeaders, Register, ToolReplay,
    Write,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct NegativeCase {
    name: String,
    target: String,
    json: String,
}

fn rejects(case: &NegativeCase) -> bool {
    match case.target.as_str() {
        "state" => serde_json::from_str::<OperationState>(&case.json).is_err(),
        "entry" => serde_json::from_str::<Entry>(&case.json).is_err(),
        "commit" => serde_json::from_str::<CommitResult>(&case.json).is_err(),
        "operation" => serde_json::from_str::<Operation>(&case.json).is_err(),
        "message" => serde_json::from_str::<AgentMessage>(&case.json).is_err(),
        "toolCall" => serde_json::from_str::<ToolCallState>(&case.json).is_err(),
        "lastResult" => serde_json::from_str::<LaneLastResult>(&case.json).is_err(),
        "register" => serde_json::from_str::<Register>(&case.json).is_err(),
        "write" => serde_json::from_str::<Write>(&case.json).is_err(),
        "rawOptions" => serde_json::from_str::<RawAgentHarnessStreamOptions>(&case.json).is_err(),
        "durableOptions" => serde_json::from_str::<AgentHarnessStreamOptions>(&case.json).is_err(),
        "preparation" => serde_json::from_str::<DurableStructuralPreparation>(&case.json).is_err(),
        target => panic!("unknown negative target {target}"),
    }
}

#[test]
fn malformed_and_overconstrained_v1_values_are_rejected() {
    let cases: Vec<NegativeCase> = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/negative-v1.json"
    ))
    .expect("negative fixture must be valid JSON");
    assert_eq!(cases.len(), 44);
    for case in cases {
        assert!(rejects(&case), "negative case accepted: {}", case.name);
    }
}

#[test]
fn built_in_message_construction_cannot_bypass_encode_validation() {
    let base = r#"{
        "role":"assistant","content":[],"api":"future-api",
        "provider":"future-provider","model":"future-model",
        "usage":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,
          "totalTokens":0,"cost":{"input":0.0,"output":0.0,"cacheRead":0.0,
          "cacheWrite":0.0,"total":0.0}},
        "stopReason":"STOP_REASON","timestamp":0
    }"#;
    for reason in ["pending", "error", "aborted"] {
        let message: pi_ai_core::Message =
            serde_json::from_str(&base.replace("STOP_REASON", reason))
                .expect("pi-ai Message permits the transient shape");
        let durable = AgentMessage::BuiltIn(message);
        assert!(
            serde_json::to_value(&durable).is_err(),
            "encode accepted {reason}"
        );
        assert!(
            serde_json::to_string(&durable).is_err(),
            "string encode accepted {reason}"
        );
    }

    let settled = base
        .replace("STOP_REASON", "error")
        .replace("\"timestamp\":0", "\"errorMessage\":\"\",\"timestamp\":0");
    let durable: AgentMessage = serde_json::from_str(&settled).expect("settled error decodes");
    assert!(serde_json::to_value(durable).is_ok());
}

#[test]
fn custom_role_dispatch_is_independent_of_role_field_order() {
    for json in [
        r#"{"role":"future-role","value":{"ok":true}}"#,
        r#"{"value":{"ok":true},"role":"future-role"}"#,
    ] {
        let message: AgentMessage = serde_json::from_str(json).expect("custom role decodes");
        assert!(matches!(message, AgentMessage::Custom(_)));
    }
}

#[test]
fn harness_header_classification_is_default_deny_and_hard_deny() {
    let mut public = BTreeMap::new();
    public.insert("X-Public".to_owned(), "secret-looking-sk-marker".to_owned());
    let raw = RawProviderHeaders::new(public).expect("unique raw headers");
    assert!(matches!(
        raw.clone().classify(&ProviderHeaderPolicy::default()),
        Err(ProviderHeaderClassificationError::PolicyDenied(_))
    ));

    let policy = ProviderHeaderPolicy::from_allowlist(["x-public".to_owned()]);
    let classified = raw.classify(&policy).expect("allowlisted public header");
    assert_eq!(
        classified.as_map().get("X-Public").map(String::as_str),
        Some("secret-looking-sk-marker")
    );
    assert!(!format!("{classified:?}").contains("secret-looking-sk-marker"));

    for name in [
        "authorization",
        "Proxy-Authorization",
        "COOKIE",
        "set-cookie",
        "Api-Key",
        "x-api-key",
        "X-Goog-Api-Key",
    ] {
        let mut values = BTreeMap::new();
        values.insert(name.to_owned(), "sentinel".to_owned());
        let raw = RawProviderHeaders::new(values).expect("one raw header");
        let allow = ProviderHeaderPolicy::from_allowlist([name.to_owned()]);
        assert!(matches!(
            raw.classify(&allow),
            Err(ProviderHeaderClassificationError::HardDenied(_))
        ));
    }
}

#[test]
fn policy_aware_options_conversion_is_the_only_nonempty_durable_path() {
    let json = r#"{
        "transport":"sse",
        "headers":{"X-Public":"value"},
        "deferred":{"window":"1h"}
    }"#;
    let raw: RawAgentHarnessStreamOptions =
        serde_json::from_str(json).expect("raw Harness options decode");
    let policy = ProviderHeaderPolicy::from_allowlist(["x-public".to_owned()]);
    let durable = raw.into_durable(&policy).expect("header is classified");
    assert_eq!(
        serde_json::to_value(&durable).expect("durable options serialize"),
        serde_json::json!({
            "transport": "sse",
            "headers": {"X-Public": "value"},
            "deferred": {"window": "1h"}
        })
    );
    assert!(serde_json::from_str::<AgentHarnessStreamOptions>(json).is_err());
    assert!(serde_json::from_str::<AgentHarnessStreamOptions>(r#"{"headers":{}}"#).is_ok());
    assert!(
        serde_json::from_str::<RawProviderHeaders>(r#"{"X-Public":"one","X-Public":"two"}"#)
            .is_err()
    );
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
struct ReplayProbe {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    replay: Option<ToolReplay>,
}

#[test]
fn omitted_and_explicit_never_replay_remain_distinct() {
    let omitted: ReplayProbe = serde_json::from_str("{}").expect("omitted replay");
    let explicit: ReplayProbe =
        serde_json::from_str(r#"{"replay":"never"}"#).expect("explicit replay");
    assert_eq!(omitted, ReplayProbe { replay: None });
    assert_eq!(
        explicit,
        ReplayProbe {
            replay: Some(ToolReplay::Never),
        }
    );
    assert_eq!(serde_json::to_string(&omitted).unwrap(), "{}");
    assert_eq!(
        serde_json::to_string(&explicit).unwrap(),
        r#"{"replay":"never"}"#
    );
}
