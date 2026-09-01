use std::collections::BTreeSet;

use pi_agent_schema::*;

fn inventory() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-surface/inventory.json"
    ))
    .expect("C2 inventory must be valid JSON")
}

fn strings(value: &serde_json::Value, key: &str) -> Vec<String> {
    value[key]
        .as_array()
        .unwrap_or_else(|| panic!("{key} must be an array"))
        .iter()
        .map(|value| {
            value
                .as_str()
                .unwrap_or_else(|| panic!("{key} item must be a string"))
                .to_owned()
        })
        .collect()
}

fn trait_methods(source: &str, marker: &str) -> BTreeSet<String> {
    let body = source
        .split_once(marker)
        .unwrap_or_else(|| panic!("missing trait marker {marker}"))
        .1
        .split_once("\n}")
        .unwrap_or_else(|| panic!("unterminated trait {marker}"))
        .0;
    body.lines()
        .filter_map(|line| line.trim_start().strip_prefix("fn "))
        .map(|line| {
            line.split(['(', '<'])
                .next()
                .expect("method name")
                .to_owned()
        })
        .collect()
}

fn expected_methods(values: &[&str]) -> BTreeSet<String> {
    values.iter().map(|value| (*value).to_owned()).collect()
}

fn struct_fields(source: &str, marker: &str) -> BTreeSet<String> {
    let body = source
        .split_once(marker)
        .unwrap_or_else(|| panic!("missing struct marker {marker}"))
        .1
        .split_once("\n}")
        .unwrap_or_else(|| panic!("unterminated struct {marker}"))
        .0;
    body.lines()
        .filter_map(|line| line.trim_start().strip_prefix("pub "))
        .filter_map(|line| {
            line.split_once(':')
                .map(|(field, _)| field.trim().to_owned())
        })
        .collect()
}

#[test]
fn rev20_inventory_counts_are_exact() {
    let inventory = inventory();
    let counts = &inventory["counts"];
    assert_eq!(counts["eventTags"], 29);
    assert_eq!(counts["eventStructuralPayloads"], 43);
    assert_eq!(counts["eventConcretePayloads"], 53);
    assert_eq!(counts["eventStructuralEnvelopes"], 45);
    assert_eq!(counts["eventConcreteEnvelopes"], 55);
    assert_eq!(counts["eventPayloadFixtures"], 54);
    assert_eq!(counts["eventEnvelopeFixtures"], 56);
    assert_eq!(counts["runOutcomes"], 8);
    assert_eq!(counts["compactionOutcomes"], 5);
    assert_eq!(counts["navigationOutcomes"], 5);
    assert_eq!(counts["navigationFixtures"], 6);
    assert_eq!(counts["resumeOutcomes"], 18);
    assert_eq!(counts["resumeFixtures"], 19);
    assert_eq!(counts["hooks"], 11);
    assert_eq!(counts["hookResultDtos"], 10);
    assert_eq!(counts["expectedErrorTags"], 14);
    assert_eq!(counts["storageMethods"], 10);

    assert_eq!(strings(&inventory, "eventTags").len(), 29);
    assert_eq!(strings(&inventory, "eventConcretePayloads").len(), 53);
    assert_eq!(strings(&inventory, "eventEnvelopeFixtures").len(), 56);
    assert_eq!(strings(&inventory, "runOutcomes").len(), 8);
    assert_eq!(strings(&inventory, "compactionOutcomes").len(), 5);
    assert_eq!(strings(&inventory, "navigationOutcomes").len(), 5);
    assert_eq!(strings(&inventory, "navigationFixtures").len(), 6);
    assert_eq!(strings(&inventory, "resumeOutcomes").len(), 18);
    assert_eq!(strings(&inventory, "hooks").len(), 11);
    assert_eq!(strings(&inventory, "hookResultDtos").len(), 10);
    assert_eq!(strings(&inventory, "expectedErrorTags").len(), 14);
    assert_eq!(strings(&inventory, "snapshots").len(), 11);
    assert_eq!(strings(&inventory, "storageMethods").len(), 10);

    let closed = inventory["closedDiscriminants"]
        .as_object()
        .expect("closedDiscriminants must be an object");
    assert_eq!(closed.len(), 32);
    for (name, values) in closed {
        let values = values
            .as_array()
            .unwrap_or_else(|| panic!("{name} must be an array"));
        assert!(!values.is_empty(), "{name} inventory is empty");
        let unique = values
            .iter()
            .map(|value| value.as_str().expect("discriminant string"))
            .collect::<BTreeSet<_>>();
        assert_eq!(unique.len(), values.len(), "{name} repeats a discriminant");
    }
    let correction = strings(&inventory, "correctionEvidence");
    assert_eq!(correction.len(), 22);
    assert_eq!(
        correction.iter().collect::<BTreeSet<_>>().len(),
        correction.len()
    );
}

#[test]
fn source_constants_match_the_golden_inventory() {
    let inventory = inventory();
    let expected_events = strings(&inventory, "eventTags")
        .into_iter()
        .collect::<BTreeSet<_>>();
    let actual_events = HARNESS_EVENT_TYPES
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_events, expected_events);

    let expected_hooks = strings(&inventory, "hooks")
        .into_iter()
        .collect::<BTreeSet<_>>();
    let actual_hooks = HOOK_NAMES
        .iter()
        .map(|value| (*value).to_owned())
        .collect::<BTreeSet<_>>();
    assert_eq!(actual_hooks, expected_hooks);

    assert_eq!(STORAGE_METHODS, strings(&inventory, "storageMethods"));

    let methods = inventory["surfaceMethods"]
        .as_object()
        .expect("surfaceMethods object");
    let nested = |name: &str| {
        methods[name]
            .as_array()
            .unwrap_or_else(|| panic!("{name} methods"))
            .iter()
            .map(|value| value.as_str().expect("method string"))
            .collect::<Vec<_>>()
    };
    assert_eq!(SESSION_TREE_METHODS, nested("SessionTree"));
    assert_eq!(SESSION_METHODS, nested("Session"));
    assert_eq!(SESSION_REPO_METHODS, nested("SessionRepo"));
    assert_eq!(SEARCH_METHODS, nested("Search"));
    assert_eq!(EFFECTS_METHODS, nested("Effects"));
    assert_eq!(AGENT_LANE_METHODS, nested("AgentLane"));
    assert_eq!(AGENT_HARNESS_METHODS, nested("Harness"));
    assert_eq!(AGENT_HARNESS_FACTORY_METHODS, nested("HarnessFactory"));
    assert_eq!(AGENT_HARNESS_PROPERTIES, nested("HarnessProperties"));

    let agent_loop = inventory["agentLoopSurface"]
        .as_object()
        .expect("agentLoopSurface object");
    let fields = |name: &str| {
        agent_loop[name]
            .as_array()
            .unwrap_or_else(|| panic!("{name} fields"))
            .iter()
            .map(|value| value.as_str().expect("field string"))
            .collect::<Vec<_>>()
    };
    assert_eq!(TOOL_BATCH_INPUT_FIELDS, fields("ToolBatchInput"));
    assert_eq!(PREPARED_TOOL_CALL_FIELDS, fields("PreparedToolCall"));
    assert_eq!(
        IMMEDIATE_TOOL_OUTCOME_FIELDS,
        fields("ImmediateToolOutcome")
    );
    assert_eq!(EXECUTED_TOOL_OUTCOME_FIELDS, fields("ExecutedToolOutcome"));
    assert_eq!(FINALIZED_TOOL_CALL_FIELDS, fields("FinalizedToolCall"));
    assert_eq!(EXECUTED_TOOL_BATCH_FIELDS, fields("ExecutedToolBatch"));
    assert_eq!(TOOL_BATCH_PHASE_METHODS, fields("ToolBatchPhases"));
    assert_eq!(AGENT_EVENT_SINK_METHODS, fields("AgentEventSink"));
    assert_eq!(
        AGENT_CONTEXT_TRANSFORM_METHODS,
        fields("AgentContextTransform")
    );
    assert_eq!(
        PROVIDER_PAYLOAD_TRANSFORM_METHODS,
        fields("ProviderPayloadTransform")
    );
    assert_eq!(
        ASSISTANT_RESPONSE_TRANSFORM_METHODS,
        fields("AssistantResponseTransform")
    );
    assert_eq!(
        STREAM_PROVIDER_MESSAGE_CONVERTER_METHODS,
        fields("StreamProviderMessageConverter")
    );
    assert_eq!(
        STREAM_ASSISTANT_CONFIG_FIELDS,
        fields("StreamAssistantConfig")
    );
    assert_eq!(STREAM_ASSISTANT_METHODS, fields("StreamAssistant"));
    assert_eq!(
        EXECUTE_TOOL_BATCH_SIGNATURE,
        inventory["executeToolBatchS14Signature"]
            .as_str()
            .expect("signature string")
    );
}

#[test]
fn exact_key_inventory_covers_every_event_tag() {
    let inventory = inventory();
    let key_sets = inventory["requiredEnvelopeKeys"]
        .as_object()
        .expect("requiredEnvelopeKeys must be an object");
    assert_eq!(key_sets.len(), 36);
    for (name, keys) in key_sets {
        let keys = keys
            .as_array()
            .unwrap_or_else(|| panic!("{name} keys must be an array"));
        assert!(keys.iter().any(|key| key == "type"), "{name} lacks type");
        let mut unique = BTreeSet::new();
        for key in keys {
            assert!(
                unique.insert(key.as_str().expect("key must be a string")),
                "{name} repeats a key"
            );
        }
    }

    let snapshot_keys = inventory["snapshotKeySets"]
        .as_object()
        .expect("snapshotKeySets must be an object");
    assert_eq!(snapshot_keys.len(), strings(&inventory, "snapshots").len());
    let outcome_keys = inventory["outcomeKeySets"]
        .as_object()
        .expect("outcomeKeySets must be an object");
    assert_eq!(outcome_keys.len(), 18);
}

#[test]
fn source_inventory_guards_abi_and_forbidden_edges() {
    let manifest = include_str!("../Cargo.toml");
    assert!(!manifest.contains("pi-telemetry"));
    assert!(!manifest.contains("async-trait"));
    assert!(!manifest.contains("futures"));

    let planner = include_str!("../src/surface/planner.rs");
    for required in [
        "pub struct EffectKey",
        "pub enum EffectPlan<C>",
        "pub enum Action<C>",
        "pub trait Effects<C>",
        "pub struct SummaryRequestPlan<C>",
        "pub struct SummaryRequestSettlement<C>",
        "pub struct PendingGeneration",
        "pub struct PendingSummaryGeneration",
        "pub struct PendingDeferred",
        "pub struct HookEffectOutput",
    ] {
        assert!(planner.contains(required), "planner missing {required}");
    }
    assert!(!planner.contains("use serde"));
    assert!(!planner.contains("#[serde"));
    assert!(!planner.contains("TelemetryContext"));
    assert!(!planner.contains("LiveEffect"));
    assert!(!planner.contains("DriveState"));
    assert!(!planner.contains("pub generation: Generation,"));
    assert!(!planner.contains("pub generation: SummaryGeneration,"));
    assert!(!planner.contains("pub deferred: Deferred,"));

    let errors = include_str!("../src/error.rs");
    assert!(errors.contains("#[non_exhaustive]"));
    assert!(errors.contains("pub enum StorageError"));
    assert!(errors.contains("pub enum SessionError"));
    assert!(!errors.contains("use serde"));
    assert!(!errors.contains("#[serde"));

    let session = include_str!("../src/surface/session.rs");
    assert!(session.contains("pub store_generation: SafeUnsigned"));
    assert!(
        session
            .matches("type Metadata: SessionMetadataRecord")
            .count()
            >= 2
    );
    assert!(session.contains("pub struct SessionCodecOptions"));
    let search = include_str!("../src/surface/search.rs");
    assert!(search.contains("type Error: std::error::Error"));
    assert!(search.contains("Option<BoxFuture<'_, Result<Vec<EntrySearchHit>, Self::Error>>>"));
    assert!(!search.contains("filter"));
    assert!(!search.contains("where:"));

    let outcomes = include_str!("../src/outcome.rs");
    assert!(!outcomes.contains("HarnessExpectedError"));
    assert!(outcomes.contains("pub type HarnessCall<T, E>"));
    assert!(outcomes.contains("expected_union!(RunExpectedError"));
    assert!(outcomes.contains("expected_union!(ResumeExpectedError"));
    assert!(!outcomes.contains("pub struct CompactionUnsuccessful<"));
    assert!(!outcomes.contains("pub struct NavigationUnsuccessful<"));
    assert!(!outcomes.contains("pub struct ResumeRun<"));
    assert!(!outcomes.contains("pub struct ResumeCompaction<"));
    assert!(!outcomes.contains("pub struct ResumeNavigation<"));

    let hooks = include_str!("../src/hook.rs");
    assert!(hooks.contains("pub enum RawHookEvent"));
    assert!(hooks.contains("pub enum RawHookResult"));
    assert!(hooks.contains("pub struct MetadataPatch"));

    let agent_loop = include_str!("../src/surface/agent_loop.rs");
    for required in [
        "pub struct ToolBatchInput<'a, C>",
        "pub struct PreparedToolCall<B>",
        "pub enum ToolPreparation<B>",
        "pub struct ExecutedToolBatch",
        "pub trait ToolBatchPhases<C>",
        "type Binding: Send + Sync",
        "type Error: std::error::Error",
        "pub struct StreamAssistantConfig<C, M>",
        "pub trait StreamAssistant<C, M>",
    ] {
        assert!(
            agent_loop.contains(required),
            "agent loop missing {required}"
        );
    }
    assert!(!agent_loop.contains("#[serde"));
    assert!(!agent_loop.contains("AbortSignal"));
    assert!(
        !agent_loop
            .lines()
            .any(|line| line.trim_start().starts_with("pub fn execute_tool_batch"))
    );
    assert!(agent_loop.contains("pub const EXECUTE_TOOL_BATCH_SIGNATURE"));

    let harness = include_str!("../src/surface/harness.rs");
    for hook in HOOK_NAMES {
        let rust_name = hook.to_owned();
        assert!(
            harness.contains(&format!("fn {rust_name}(")),
            "Hooks missing {rust_name}"
        );
    }
    assert!(harness.contains("pub struct AgentHarnessOptions<C, T, S, M>"));
    assert!(harness.contains("pub trait AgentHarnessFactory<C, T>"));
    assert!(harness.contains("pub trait AgentHarnessTool<C>"));
    assert!(harness.contains("pub telemetry_context: T"));
    assert!(harness.contains("Result<S::Result, HandlerError>"));
    assert!(harness.contains("fn cancelled(&self) -> BoxFuture<'_, ()>"));
    assert!(!harness.contains("pub telemetry_context: C"));

    let lane = include_str!("../src/surface/lane.rs");
    assert!(lane.contains("Result<(), HandlerError>"));

    for c2_source in [
        include_str!("../src/error.rs"),
        include_str!("../src/event.rs"),
        include_str!("../src/hook.rs"),
        include_str!("../src/outcome.rs"),
        include_str!("../src/resource.rs"),
        include_str!("../src/scan.rs"),
        include_str!("../src/snapshot.rs"),
    ] {
        assert!(!c2_source.contains("pub struct EntryBase"));
        assert!(!c2_source.contains("pub enum OperationState"));
        assert!(!c2_source.contains("pub struct LaneConfiguration"));
    }
}

#[test]
fn method_inventories_match_trait_sources_exactly() {
    let session = include_str!("../src/surface/session.rs");
    assert_eq!(
        trait_methods(session, "pub trait SessionTree:"),
        expected_methods(SESSION_TREE_METHODS)
    );
    assert_eq!(
        trait_methods(session, "pub trait Session: SessionTree"),
        expected_methods(SESSION_METHODS)
    );
    assert_eq!(
        trait_methods(session, "pub trait SessionRepo:"),
        expected_methods(SESSION_REPO_METHODS)
    );

    let search = include_str!("../src/surface/search.rs");
    assert_eq!(
        trait_methods(search, "pub trait SessionSearchService:"),
        expected_methods(SEARCH_METHODS)
    );
    let planner = include_str!("../src/surface/planner.rs");
    assert_eq!(
        trait_methods(planner, "pub trait Effects<C>"),
        expected_methods(EFFECTS_METHODS)
    );
    let lane = include_str!("../src/surface/lane.rs");
    assert_eq!(
        trait_methods(lane, "pub trait AgentLane:"),
        expected_methods(AGENT_LANE_METHODS)
    );
    let harness = include_str!("../src/surface/harness.rs");
    assert_eq!(
        trait_methods(harness, "pub trait AgentHarness<C>"),
        expected_methods(AGENT_HARNESS_METHODS)
    );
    assert_eq!(
        trait_methods(harness, "pub trait AgentHarnessFactory<C, T>"),
        expected_methods(AGENT_HARNESS_FACTORY_METHODS)
    );
    let agent_loop = include_str!("../src/surface/agent_loop.rs");
    assert_eq!(
        trait_methods(agent_loop, "pub trait ToolBatchPhases<C>"),
        expected_methods(TOOL_BATCH_PHASE_METHODS)
    );
    assert_eq!(
        trait_methods(agent_loop, "pub trait StreamAssistant<C, M>"),
        expected_methods(STREAM_ASSISTANT_METHODS)
    );
    for (marker, methods) in [
        ("pub trait AgentEventSink:", AGENT_EVENT_SINK_METHODS),
        (
            "pub trait AgentContextTransform:",
            AGENT_CONTEXT_TRANSFORM_METHODS,
        ),
        (
            "pub trait ProviderPayloadTransform:",
            PROVIDER_PAYLOAD_TRANSFORM_METHODS,
        ),
        (
            "pub trait AssistantResponseTransform:",
            ASSISTANT_RESPONSE_TRANSFORM_METHODS,
        ),
        (
            "pub trait StreamProviderMessageConverter:",
            STREAM_PROVIDER_MESSAGE_CONVERTER_METHODS,
        ),
    ] {
        assert_eq!(trait_methods(agent_loop, marker), expected_methods(methods));
    }

    for (marker, fields) in [
        ("pub struct ToolBatchInput<'a, C>", TOOL_BATCH_INPUT_FIELDS),
        ("pub struct PreparedToolCall<B>", PREPARED_TOOL_CALL_FIELDS),
        (
            "pub struct ImmediateToolOutcome",
            IMMEDIATE_TOOL_OUTCOME_FIELDS,
        ),
        (
            "pub struct ExecutedToolOutcome",
            EXECUTED_TOOL_OUTCOME_FIELDS,
        ),
        ("pub struct FinalizedToolCall", FINALIZED_TOOL_CALL_FIELDS),
        ("pub struct ExecutedToolBatch", EXECUTED_TOOL_BATCH_FIELDS),
        (
            "pub struct StreamAssistantConfig<C, M>",
            STREAM_ASSISTANT_CONFIG_FIELDS,
        ),
    ] {
        assert_eq!(struct_fields(agent_loop, marker), expected_methods(fields));
    }
}
