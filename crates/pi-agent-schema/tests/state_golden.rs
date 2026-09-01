use std::collections::BTreeSet;

use pi_agent_schema::{OPERATION_STATE_V1_INVENTORY, OperationState};
use serde::Deserialize;

#[derive(Deserialize)]
struct StateCase {
    name: String,
    value: serde_json::Value,
}

#[test]
fn every_physical_v1_state_golden_round_trips() {
    let cases: Vec<StateCase> = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/states-v1.json"
    ))
    .expect("state fixture must be valid JSON");
    assert_eq!(cases.len(), 25);

    let mut covered = BTreeSet::new();
    for case in cases {
        let state: OperationState = serde_json::from_value(case.value.clone())
            .unwrap_or_else(|error| panic!("{} must decode: {error}", case.name));
        let encoded = serde_json::to_value(state)
            .unwrap_or_else(|error| panic!("{} must encode: {error}", case.name));
        assert_eq!(encoded, case.value, "{} must round-trip exactly", case.name);
        if case.name == "run.tools.all_call_statuses" {
            covered.insert("run.tools.planned".to_owned());
            covered.insert("run.tools.effect_pending".to_owned());
            covered.insert("run.tools.completed".to_owned());
        } else {
            covered.insert(case.name);
        }
    }

    let expected = [
        "run.checkpoint",
        "run.assistant.ready",
        "run.assistant.effect_pending",
        "run.assistant.retry_wait",
        "run.tools.planned",
        "run.tools.effect_pending",
        "run.tools.completed",
        "run.compaction.deciding",
        "run.compaction.generating.ready",
        "run.compaction.generating.effect_pending.request_missing",
        "run.compaction.generating.effect_pending.request_present",
        "run.compaction.generating.retry_wait",
        "run.deferred.suspended",
        "run.deferred.effect_pending",
        "run.failure_drain.response",
        "run.failure_drain.structural",
        "compaction.deciding",
        "compaction.generating.ready",
        "compaction.generating.effect_pending.request_missing",
        "compaction.generating.effect_pending.request_present",
        "compaction.generating.retry_wait",
        "navigation.unsummarized.ready_to_commit",
        "navigation.summarized.deciding",
        "navigation.summarized.generating.ready",
        "navigation.summarized.generating.effect_pending.request_missing",
        "navigation.summarized.generating.effect_pending.request_present",
        "navigation.summarized.generating.retry_wait",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    let declared = OPERATION_STATE_V1_INVENTORY
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(declared, expected, "source state inventory is incomplete");
    assert_eq!(covered, expected, "v1 state inventory and goldens diverged");
}

#[test]
fn current_state_is_a_reexport_only() {
    let current = include_str!("../src/state/current.rs");
    let code = current
        .lines()
        .filter(|line| !line.trim().is_empty() && !line.trim_start().starts_with("//!"))
        .collect::<Vec<_>>();
    assert_eq!(code, ["pub use super::v1::*;"]);
    assert!(!current.contains("migrated_effect_pending"));
    let v1 = include_str!("../src/state/v1.rs");
    assert!(!v1.contains("migrated_effect_pending"));
    assert!(!v1.contains("Finished"));
}

#[test]
fn every_one_based_state_number_rejects_zero_and_accepts_safe_max() {
    let cases: Vec<StateCase> = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/states-v1.json"
    ))
    .expect("state fixture must be valid JSON");
    let checks: &[(&str, &[&str])] = &[
        (
            "run.assistant.ready",
            &[
                "phase",
                "generation",
                "context",
                "retryPolicy",
                "maxAttempts",
            ],
        ),
        (
            "run.assistant.ready",
            &["phase", "generation", "nextAttempt"],
        ),
        (
            "run.assistant.effect_pending",
            &["phase", "generation", "attempt"],
        ),
        (
            "run.assistant.retry_wait",
            &["phase", "generation", "nextAttempt"],
        ),
        (
            "run.compaction.generating.ready",
            &[
                "phase",
                "structural",
                "generation",
                "context",
                "retryPolicy",
                "maxAttempts",
            ],
        ),
        (
            "run.compaction.generating.ready",
            &["phase", "structural", "generation", "nextAttempt"],
        ),
        (
            "run.compaction.generating.effect_pending.request_missing",
            &["phase", "structural", "generation", "attempt"],
        ),
        (
            "run.compaction.generating.retry_wait",
            &["phase", "structural", "generation", "nextAttempt"],
        ),
        (
            "run.deferred.effect_pending",
            &["phase", "deferred", "poll"],
        ),
    ];

    for (name, path) in checks {
        let original = cases
            .iter()
            .find(|case| case.name == *name)
            .unwrap_or_else(|| panic!("missing state case {name}"))
            .value
            .clone();
        let mut zero = original.clone();
        set_number(&mut zero, path, 0);
        assert!(
            serde_json::from_value::<OperationState>(zero).is_err(),
            "{name} {path:?} accepted zero"
        );

        let mut maximum = original;
        set_number(&mut maximum, path, 9_007_199_254_740_991);
        assert!(
            serde_json::from_value::<OperationState>(maximum).is_ok(),
            "{name} {path:?} rejected safe max"
        );
    }
}

#[test]
fn suspended_poll_source_index_and_summary_request_index_accept_zero() {
    let cases: Vec<StateCase> = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/states-v1.json"
    ))
    .expect("state fixture must be valid JSON");
    for name in [
        "run.deferred.suspended",
        "run.tools.all_call_statuses",
        "run.compaction.generating.effect_pending.request_present",
    ] {
        let value = cases
            .iter()
            .find(|case| case.name == name)
            .unwrap_or_else(|| panic!("missing zero-based state case {name}"))
            .value
            .clone();
        assert!(
            serde_json::from_value::<OperationState>(value).is_ok(),
            "{name} rejected a legal zero-based value"
        );
    }
}

fn set_number(value: &mut serde_json::Value, path: &[&str], number: u64) {
    let mut current = value;
    for key in path {
        current = current
            .get_mut(*key)
            .unwrap_or_else(|| panic!("missing numeric path component {key}"));
    }
    *current = serde_json::Value::Number(number.into());
}
