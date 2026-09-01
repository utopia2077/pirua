use std::collections::BTreeSet;

use pi_agent_schema::{
    AgentHarnessStreamOptions, CommitResult, DurableStructuralPreparation, Entry, EntryBase,
    EntryStructure, LaneLastResult, NewEntry, NewUsageRow, Operation, REGISTER_NAMESPACES,
    Register, RetryPolicy, Transaction, UsageRow, Write,
};
use serde::Deserialize;
use serde::de::DeserializeOwned;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DurableFixture {
    entries: Vec<serde_json::Value>,
    entry_bases: Vec<serde_json::Value>,
    new_entries: Vec<serde_json::Value>,
    entry_structures: Vec<serde_json::Value>,
    usage_rows: Vec<serde_json::Value>,
    new_usage_rows: Vec<serde_json::Value>,
    operations: Vec<serde_json::Value>,
    last_results: Vec<serde_json::Value>,
    preparations: Vec<serde_json::Value>,
    retry_policies: Vec<serde_json::Value>,
    registers: Vec<serde_json::Value>,
    writes: Vec<serde_json::Value>,
    transactions: Vec<serde_json::Value>,
    commit_results: Vec<serde_json::Value>,
    stream_options: Vec<serde_json::Value>,
}

fn assert_exact_round_trip<T>(kind: &str, values: &[serde_json::Value])
where
    T: DeserializeOwned + serde::Serialize,
{
    for (index, value) in values.iter().enumerate() {
        let decoded: T = serde_json::from_value(value.clone())
            .unwrap_or_else(|error| panic!("{kind}[{index}] must decode: {error}"));
        let encoded = serde_json::to_value(decoded)
            .unwrap_or_else(|error| panic!("{kind}[{index}] must encode: {error}"));
        assert_eq!(&encoded, value, "{kind}[{index}] must round-trip exactly");
    }
}

#[test]
fn all_durable_v1_shapes_have_exact_goldens() {
    let fixture: DurableFixture = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .expect("durable fixture must be valid JSON");
    assert_eq!(fixture.last_results.len(), 12);
    assert_eq!(fixture.writes.len(), 26);
    let last_result_inventory = fixture
        .last_results
        .iter()
        .map(|result| {
            let kind = result["kind"].as_str().expect("last-result kind");
            let outcome = result["outcome"].as_str().expect("last-result outcome");
            let completion = result
                .get("runCompletion")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("-");
            format!("{kind}.{outcome}.{completion}")
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        last_result_inventory,
        BTreeSet::from([
            "run.completed.assistant".to_owned(),
            "run.completed.terminated_tools".to_owned(),
            "run.failed.-".to_owned(),
            "run.aborted.-".to_owned(),
            "compaction.completed.-".to_owned(),
            "compaction.failed.-".to_owned(),
            "compaction.declined.-".to_owned(),
            "compaction.aborted.-".to_owned(),
            "navigation.completed.-".to_owned(),
            "navigation.failed.-".to_owned(),
            "navigation.declined.-".to_owned(),
            "navigation.aborted.-".to_owned(),
        ])
    );

    assert_exact_round_trip::<Entry>("entries", &fixture.entries);
    assert_exact_round_trip::<EntryBase>("entryBases", &fixture.entry_bases);
    assert_exact_round_trip::<NewEntry>("newEntries", &fixture.new_entries);
    assert_exact_round_trip::<EntryStructure>("entryStructures", &fixture.entry_structures);
    assert_exact_round_trip::<UsageRow>("usageRows", &fixture.usage_rows);
    assert_exact_round_trip::<NewUsageRow>("newUsageRows", &fixture.new_usage_rows);
    assert_exact_round_trip::<Operation>("operations", &fixture.operations);
    assert_exact_round_trip::<LaneLastResult>("lastResults", &fixture.last_results);
    assert_exact_round_trip::<DurableStructuralPreparation>("preparations", &fixture.preparations);
    assert_exact_round_trip::<RetryPolicy>("retryPolicies", &fixture.retry_policies);
    assert_exact_round_trip::<Register>("registers", &fixture.registers);
    assert_exact_round_trip::<Write>("writes", &fixture.writes);
    assert_exact_round_trip::<Transaction>("transactions", &fixture.transactions);
    assert_exact_round_trip::<CommitResult>("commitResults", &fixture.commit_results);
    assert_exact_round_trip::<AgentHarnessStreamOptions>("streamOptions", &fixture.stream_options);
}

#[test]
fn navigation_intent_preserves_all_target_and_summarize_combinations() {
    let fixture: DurableFixture = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .expect("durable fixture must be valid JSON");
    let combinations = fixture
        .operations
        .iter()
        .filter(|operation| operation["intent"]["kind"] == "navigation")
        .map(|operation| {
            (
                operation["intent"]["targetId"].is_null(),
                operation["intent"]["summarize"]
                    .as_bool()
                    .expect("summarize bool"),
            )
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        combinations,
        BTreeSet::from([(true, false), (true, true), (false, false), (false, true)])
    );
}

#[test]
fn run_failed_and_aborted_final_assistant_is_independently_optional() {
    let fixture: DurableFixture = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .expect("durable fixture must be valid JSON");
    let mut failed = fixture
        .last_results
        .iter()
        .find(|result| result["kind"] == "run" && result["outcome"] == "failed")
        .expect("run failed golden")
        .clone();
    failed
        .as_object_mut()
        .expect("failed result object")
        .remove("finalAssistantEntryId");
    assert!(serde_json::from_value::<LaneLastResult>(failed).is_ok());

    let mut aborted = fixture
        .last_results
        .iter()
        .find(|result| result["kind"] == "run" && result["outcome"] == "aborted")
        .expect("run aborted golden")
        .clone();
    aborted
        .as_object_mut()
        .expect("aborted result object")
        .insert(
            "finalAssistantEntryId".to_owned(),
            serde_json::Value::String("entry-final".to_owned()),
        );
    assert!(serde_json::from_value::<LaneLastResult>(aborted).is_ok());
}

#[test]
fn register_and_write_inventories_are_exhaustive() {
    let fixture: DurableFixture = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .expect("durable fixture must be valid JSON");
    let expected = [
        "lane.leaf",
        "lane.config",
        "lane.state",
        "lane.lastResult",
        "op.meta",
        "op.state",
        "op.tool_args",
        "op.preparation",
        "pending.entry",
        "fact.name",
        "fact.label",
        "fact.custom",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<BTreeSet<_>>();
    let declared = REGISTER_NAMESPACES
        .iter()
        .map(|namespace| (*namespace).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        declared, expected,
        "source register inventory is incomplete"
    );

    let register_namespaces = fixture
        .registers
        .iter()
        .map(|register| {
            register["namespace"]
                .as_str()
                .expect("register namespace string")
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(register_namespaces, expected);

    for operation in ["set", "delete"] {
        let namespaces = fixture
            .writes
            .iter()
            .filter(|write| write["kind"] == "register" && write["op"] == operation)
            .map(|write| {
                write["namespace"]
                    .as_str()
                    .expect("write namespace string")
                    .to_owned()
            })
            .collect::<BTreeSet<_>>();
        assert_eq!(namespaces, expected, "{operation} write inventory diverged");
    }

    assert!(fixture.writes.iter().any(|write| write["kind"] == "entry"));
    assert!(fixture.writes.iter().any(|write| write["kind"] == "usage"));
}

#[test]
fn fact_custom_null_is_data_and_deletion_is_a_distinct_write() {
    let fixture: DurableFixture = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/durable-v1.json"
    ))
    .expect("durable fixture must be valid JSON");
    let fact_writes = fixture
        .transactions
        .get(1)
        .and_then(|transaction| transaction["writes"].as_array())
        .expect("fact transaction writes");
    assert_eq!(fact_writes[0]["op"], "set");
    assert!(fact_writes[0]["value"].is_null());
    assert_eq!(fact_writes[1]["op"], "delete");
    assert!(fact_writes[1].get("value").is_none());
}
