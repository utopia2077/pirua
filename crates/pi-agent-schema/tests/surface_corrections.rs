use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use pi_agent_schema::*;
use serde::Deserialize;

#[derive(Deserialize)]
struct StateCase {
    name: String,
    value: serde_json::Value,
}

fn state_case(name: &str) -> serde_json::Value {
    let cases: Vec<StateCase> = serde_json::from_str(include_str!(
        "../../../conformance/schema/agent-state/states-v1.json"
    ))
    .expect("state fixture");
    cases
        .into_iter()
        .find(|case| case.name == name)
        .unwrap_or_else(|| panic!("missing state {name}"))
        .value
}

fn nested(mut value: serde_json::Value, path: &[&str]) -> serde_json::Value {
    for key in path {
        value = value
            .get_mut(*key)
            .unwrap_or_else(|| panic!("missing path {key}"))
            .take();
    }
    value
}

fn user_message() -> serde_json::Value {
    serde_json::json!({"role":"user","content":"hello","timestamp":0})
}

fn raw_model(headers: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "id":"model","name":"Model","api":"api","provider":"provider",
        "baseUrl":"https://example.invalid/v1","reasoning":false,"input":["text"],
        "cost":{"input":0.0,"output":0.0,"cacheRead":0.0,"cacheWrite":0.0},
        "contextWindow":1000,"maxTokens":100,"headers":headers
    })
}

fn compaction_result() -> serde_json::Value {
    serde_json::json!({"summary":"summary","tokensBefore":0,"retainedTail":[]})
}

fn branch_result() -> serde_json::Value {
    serde_json::json!({"summary":"summary","readFiles":[],"modifiedFiles":[]})
}

fn compaction_preparation() -> serde_json::Value {
    serde_json::json!({
        "messagesToSummarize":[],"turnPrefixMessages":[],"retainedTail":[],
        "isSplitTurn":false,"tokensBefore":0,
        "fileOps":{"read":[],"written":[],"edited":[]},
        "settings":{"enabled":true,"reserveTokens":10,"keepRecentTokens":20}
    })
}

#[test]
fn pending_only_projections_reject_every_other_status() {
    for name in ["run.assistant.ready", "run.assistant.retry_wait"] {
        let value = nested(state_case(name), &["phase", "generation"]);
        let generation: Generation = serde_json::from_value(value).expect("generation");
        assert!(matches!(
            PendingGeneration::try_from(generation),
            Err(PendingProjectionError::GenerationNotPending)
        ));
    }
    let value = nested(
        state_case("run.assistant.effect_pending"),
        &["phase", "generation"],
    );
    assert!(
        PendingGeneration::try_from(
            serde_json::from_value::<Generation>(value).expect("pending generation")
        )
        .is_ok()
    );

    for name in [
        "compaction.generating.ready",
        "compaction.generating.retry_wait",
    ] {
        let value = nested(state_case(name), &["structural", "generation"]);
        let generation: SummaryGeneration = serde_json::from_value(value).expect("summary");
        assert!(matches!(
            PendingSummaryGeneration::try_from(generation),
            Err(PendingProjectionError::SummaryNotPending)
        ));
    }
    let value = nested(
        state_case("compaction.generating.effect_pending.request_present"),
        &["structural", "generation"],
    );
    assert!(
        PendingSummaryGeneration::try_from(
            serde_json::from_value::<SummaryGeneration>(value).expect("pending summary")
        )
        .is_ok()
    );

    let suspended = nested(state_case("run.deferred.suspended"), &["phase", "deferred"]);
    assert!(matches!(
        PendingDeferred::try_from(
            serde_json::from_value::<Deferred>(suspended).expect("suspended deferred")
        ),
        Err(PendingProjectionError::DeferredNotPending)
    ));
    let pending = nested(
        state_case("run.deferred.effect_pending"),
        &["phase", "deferred"],
    );
    assert!(
        PendingDeferred::try_from(
            serde_json::from_value::<Deferred>(pending).expect("pending deferred")
        )
        .is_ok()
    );
}

#[derive(Clone)]
struct ExtendedMetadata {
    base: SessionMetadata,
    tenant: String,
}

impl SessionMetadataRecord for ExtendedMetadata {
    fn base_metadata(&self) -> &SessionMetadata {
        &self.base
    }
}

fn require_metadata_record<T: SessionMetadataRecord>(metadata: &T) -> &SessionMetadata {
    metadata.base_metadata()
}

#[test]
fn extended_metadata_exposes_required_base_and_codec_rejects_builtin_roles() {
    let base: SessionMetadata = serde_json::from_value(serde_json::json!({
        "id":"session","createdAt":0,"storageVersion":1,"storeGeneration":3
    }))
    .expect("metadata");
    let extended = ExtendedMetadata {
        base: base.clone(),
        tenant: "tenant".to_owned(),
    };
    assert_eq!(require_metadata_record(&extended), &base);
    assert_eq!(extended.tenant, "tenant");

    for role in ["", "user", "assistant", "toolResult"] {
        let schemas = BTreeMap::from([(role.to_owned(), pi_ai_core::JsonSchema::default())]);
        assert!(SessionCodecOptions::from_role_schemas(schemas).is_err());
    }
    let schemas = BTreeMap::from([("artifact".to_owned(), pi_ai_core::JsonSchema::default())]);
    let options = SessionCodecOptions::from_role_schemas(schemas).expect("custom role");
    assert_eq!(options.custom_message_schemas().len(), 1);
}

#[test]
fn metadata_patch_preserves_set_null_delete_and_clear_all() {
    let patch: MetadataPatch = serde_json::from_value(serde_json::json!({
        "set":{"nullData":null,"value":"kept"},"delete":["old","stale"]
    }))
    .expect("valid patch");
    assert!(matches!(
        patch.set().as_map().get("nullData"),
        Some(pi_ai_core::JsonValue::Null)
    ));
    assert_eq!(patch.delete(), ["old", "stale"]);
    assert!(
        serde_json::from_value::<MetadataPatch>(serde_json::json!({
            "set":{},"delete":["z","a"]
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<MetadataPatch>(serde_json::json!({
            "set":{},"delete":["a","a"]
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<MetadataPatch>(serde_json::json!({
            "set":{"same":null},"delete":["same"]
        }))
        .is_err()
    );

    let from_js = MetadataPatch::from_javascript_properties(BTreeMap::from([
        ("delete".to_owned(), None),
        ("nullData".to_owned(), Some(pi_ai_core::JsonValue::Null)),
    ]));
    assert_eq!(from_js.delete(), ["delete"]);
    assert!(matches!(
        from_js.set().as_map().get("nullData"),
        Some(pi_ai_core::JsonValue::Null)
    ));

    let clear: AgentHarnessStreamOptionsPatch =
        serde_json::from_value(serde_json::json!({"metadata":null})).expect("clear all");
    assert!(matches!(clear.metadata, pi_ai_core::Presence::Null));
}

#[test]
fn structural_hook_results_preserve_false_true_supply_and_conflicts() {
    for name in ["before_compaction", "before_navigation"] {
        let omitted = serde_json::json!({"name":name});
        let result: HookResult = serde_json::from_value(omitted.clone()).expect("omitted");
        assert_eq!(serde_json::to_value(result).expect("encodes"), omitted);

        let empty = serde_json::json!({"name":name,"result":{}});
        let result: HookResult = serde_json::from_value(empty.clone()).expect("explicit empty");
        assert_eq!(serde_json::to_value(result).expect("encodes"), empty);

        let false_value = serde_json::json!({"name":name,"result":{"decline":false}});
        let result: HookResult =
            serde_json::from_value(false_value.clone()).expect("explicit false");
        assert_eq!(serde_json::to_value(result).expect("encodes"), false_value);

        let true_value = serde_json::json!({"name":name,"result":{"decline":true}});
        let result: HookResult = serde_json::from_value(true_value.clone()).expect("explicit true");
        assert_eq!(serde_json::to_value(result).expect("encodes"), true_value);

        let field = if name == "before_compaction" {
            "compaction"
        } else {
            "summary"
        };
        let supplied = if name == "before_compaction" {
            compaction_result()
        } else {
            branch_result()
        };
        for decline in [None, Some(false)] {
            let mut result = serde_json::Map::new();
            result.insert(field.to_owned(), supplied.clone());
            if let Some(decline) = decline {
                result.insert("decline".to_owned(), decline.into());
            }
            let value = serde_json::json!({"name":name,"result":result});
            let decoded: HookResult = serde_json::from_value(value.clone()).expect("supplied");
            assert_eq!(serde_json::to_value(decoded).expect("encodes"), value);
        }
        let conflict = serde_json::json!({
            "name":name,"result":{"decline":true,(field):supplied}
        });
        assert!(serde_json::from_value::<HookResult>(conflict).is_err());
        assert!(
            serde_json::from_value::<HookResult>(serde_json::json!({
                "name":name,"result":{"future":true}
            }))
            .is_err()
        );
    }
}

#[test]
fn optional_hook_results_omit_and_reject_explicit_null() {
    for name in HOOK_NAMES {
        let absent = serde_json::json!({"name":name});
        let decoded: HookResult =
            serde_json::from_value(absent.clone()).expect("absent result must decode");
        assert_eq!(serde_json::to_value(decoded).expect("encodes"), absent);
        assert!(
            serde_json::from_value::<HookResult>(serde_json::json!({
                "name":name,"result":null
            }))
            .is_err()
        );
    }
}

#[test]
fn raw_hook_roots_reopen_with_same_policies_and_fail_closed() {
    let event_json = serde_json::json!({
        "name":"before_request",
        "event":{
            "lane":"main","runId":"op","model":raw_model(serde_json::json!({"X-Model":"value"})),
            "step":"assistant","attempt":1,
            "streamOptions":{"headers":{"X-Provider":"value"}}
        }
    });
    let model_policy = pi_ai_core::ModelHeaderPolicy::from_allowlist(["x-model".to_owned()]);
    let provider_policy = ProviderHeaderPolicy::from_allowlist(["x-provider".to_owned()]);
    let raw: RawHookEvent = serde_json::from_value(event_json).expect("raw event");
    assert!(
        raw.clone()
            .into_durable(
                &pi_ai_core::ModelHeaderPolicy::default(),
                &ProviderHeaderPolicy::default(),
            )
            .is_err()
    );
    let typed = raw
        .into_durable(&model_policy, &provider_policy)
        .expect("classified event");
    let bytes = serde_json::to_vec(&typed).expect("typed event serializes");
    assert!(serde_json::from_slice::<HookEvent>(&bytes).is_err());
    let reopened: RawHookEvent = serde_json::from_slice(&bytes).expect("raw reopens");
    let reopened = reopened
        .into_durable(&model_policy, &provider_policy)
        .expect("same policy");
    assert_eq!(serde_json::to_vec(&reopened).expect("encodes"), bytes);

    let payload_json = serde_json::json!({
        "name":"before_payload",
        "event":{"lane":"main","runId":"op","model":raw_model(serde_json::json!({"X-Model":"value"})),"payload":null}
    });
    let raw: RawHookEvent = serde_json::from_value(payload_json).expect("raw payload");
    let typed_payload = raw
        .into_durable(&model_policy, &provider_policy)
        .expect("classified payload event");
    let payload_bytes = serde_json::to_vec(&typed_payload).expect("payload event serializes");
    assert!(serde_json::from_slice::<HookEvent>(&payload_bytes).is_err());
    let reopened_payload: RawHookEvent =
        serde_json::from_slice(&payload_bytes).expect("raw payload reopens");
    assert!(
        reopened_payload
            .into_durable(&model_policy, &provider_policy)
            .is_ok()
    );

    let result_json = serde_json::json!({
        "name":"before_request",
        "result":{"streamOptions":{"headers":{"X-Provider":"value"}}}
    });
    let raw: RawHookResult = serde_json::from_value(result_json).expect("raw result");
    assert!(
        raw.clone()
            .into_durable(&ProviderHeaderPolicy::default())
            .is_err()
    );
    let typed = raw
        .into_durable(&provider_policy)
        .expect("classified result");
    let bytes = serde_json::to_vec(&typed).expect("typed result serializes");
    assert!(serde_json::from_slice::<HookResult>(&bytes).is_err());
    let reopened: RawHookResult = serde_json::from_slice(&bytes).expect("raw result reopens");
    assert!(reopened.into_durable(&provider_policy).is_ok());

    let hard_model: RawHookEvent = serde_json::from_value(serde_json::json!({
        "name":"before_payload","event":{
            "lane":"main","runId":"op","model":raw_model(serde_json::json!({"Authorization":"sentinel"})),"payload":null
        }
    }))
    .expect("raw hard model");
    let allow_model = pi_ai_core::ModelHeaderPolicy::from_allowlist(["authorization".to_owned()]);
    assert!(
        hard_model
            .into_durable(&allow_model, &provider_policy)
            .is_err()
    );

    let hard_provider_event: RawHookEvent = serde_json::from_value(serde_json::json!({
        "name":"before_request","event":{
            "lane":"main","runId":"op","model":raw_model(serde_json::json!({})),
            "step":"assistant","attempt":1,
            "streamOptions":{"headers":{"Authorization":"sentinel"}}
        }
    }))
    .expect("raw hard provider event");
    let allow_provider = ProviderHeaderPolicy::from_allowlist(["authorization".to_owned()]);
    assert!(
        hard_provider_event
            .into_durable(&pi_ai_core::ModelHeaderPolicy::default(), &allow_provider)
            .is_err()
    );

    let hard_result: RawHookResult = serde_json::from_value(serde_json::json!({
        "name":"before_request","result":{"streamOptions":{"headers":{"Authorization":"sentinel"}}}
    }))
    .expect("raw hard result");
    assert!(hard_result.into_durable(&allow_provider).is_err());
    let hard_delete: RawHookResult = serde_json::from_value(serde_json::json!({
        "name":"before_request","result":{"streamOptions":{"headers":{"Authorization":null}}}
    }))
    .expect("raw hard delete");
    assert!(hard_delete.into_durable(&allow_provider).is_err());
}

fn effect_key() -> EffectKey {
    EffectKey {
        session_id: SessionId::new("session").expect("id"),
        lane: LaneId::new("main").expect("id"),
        operation_id: OperationId::new("op").expect("id"),
        kind: EffectKind::Hook,
        step_or_task_id: EffectStepId::new("hook").expect("id"),
        logical_attempt: pi_ai_core::SafeUnsigned::new(1).expect("attempt"),
        item_index: None,
    }
}

#[test]
fn hook_effect_output_requires_a_matched_pair() {
    let event: HookEvent = serde_json::from_value(serde_json::json!({
        "name":"before_tool","event":{
            "lane":"main","runId":"op","toolCallId":"call","toolName":"tool","args":{}
        }
    }))
    .expect("hook event");
    let plan = HookEffectPlan {
        key: effect_key(),
        event,
        telemetry_context: (),
    };
    let matched = HookEffectOutput::new(&plan, HookResult::BeforeTool(None)).expect("matched");
    assert_eq!(matched.key(), &plan.key);
    assert!(matches!(EffectOutput::Hook(matched), EffectOutput::Hook(_)));
    assert!(HookEffectOutput::new(&plan, HookResult::AfterTool(None)).is_err());
}

struct AwaitCancellation;

impl AgentContextTransform for AwaitCancellation {
    fn transform<'a>(
        &'a self,
        messages: Vec<AgentMessage>,
        cancellation: &'a dyn Cancellation,
    ) -> BoxFuture<'a, Result<Vec<AgentMessage>, HarnessRuntimeError>> {
        Box::pin(async move {
            cancellation.cancelled().await;
            Ok(messages)
        })
    }
}

struct BorrowModel;

impl ProviderPayloadTransform for BorrowModel {
    fn transform<'a>(
        &'a self,
        payload: pi_ai_core::JsonValue,
        model: &'a pi_ai_core::Model,
    ) -> BoxFuture<'a, Result<Option<pi_ai_core::JsonValue>, HarnessRuntimeError>> {
        Box::pin(async move {
            std::future::ready(()).await;
            let _model_name = &model.name;
            Ok(Some(payload))
        })
    }
}

#[test]
fn callback_ports_compile_with_natural_borrowed_futures() {
    fn accepts_context(_: &dyn AgentContextTransform) {}
    fn accepts_payload(_: &dyn ProviderPayloadTransform) {}
    accepts_context(&AwaitCancellation);
    accepts_payload(&BorrowModel);
}

#[derive(Debug)]
struct SourceMarker(&'static str);

impl fmt::Display for SourceMarker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for SourceMarker {}

#[test]
fn run_when_idle_and_error_sources_preserve_causality_without_debug_leaks() {
    let sentinel = "source-secret-sentinel";
    let source: SharedErrorSource = Arc::new(SourceMarker(sentinel));
    let context = ErrorContext::with_source("bounded", source).expect("context");
    let storage = StorageError::Io(context.clone());
    assert_eq!(
        std::error::Error::source(&storage)
            .expect("source")
            .to_string(),
        sentinel
    );
    assert!(!format!("{storage:?}").contains(sentinel));

    let session = SessionError::Storage(storage.clone());
    assert_eq!(
        std::error::Error::source(&session)
            .expect("storage source")
            .to_string(),
        "io: bounded"
    );
    assert!(!format!("{session:?}").contains(sentinel));

    let fault = HarnessFault::new(context.clone());
    assert_eq!(
        std::error::Error::source(&fault)
            .expect("fault source")
            .to_string(),
        sentinel
    );
    assert!(!format!("{fault:?}").contains(sentinel));
    let runtime_fault = HarnessRuntimeError::Fault(fault);
    assert_eq!(
        std::error::Error::source(&runtime_fault)
            .expect("runtime source")
            .to_string(),
        "bounded"
    );
    assert!(!format!("{runtime_fault:?}").contains(sentinel));

    let callback = CallbackError::new(context);
    assert_eq!(
        std::error::Error::source(&callback)
            .expect("callback source")
            .to_string(),
        sentinel
    );
    let callback_error = RunWhenIdleError::Callback(callback);
    assert!(matches!(callback_error, RunWhenIdleError::Callback(_)));
    assert!(!format!("{callback_error:?}").contains(sentinel));
    let runtime_error = RunWhenIdleError::Runtime(HarnessRuntimeError::Closed(HarnessClosed));
    assert!(matches!(runtime_error, RunWhenIdleError::Runtime(_)));

    fn accepts_call(_: RunWhenIdleCall) {}
    accepts_call(Ok(()));
}

#[test]
fn response_and_raw_patch_constructors_enforce_names_without_failing_sanitization() {
    assert!(
        ResponseHeaders::new(BTreeMap::from([(
            "Authorization".to_owned(),
            "sentinel".to_owned(),
        )]))
        .is_err()
    );
    assert!(
        ResponseHeaders::new(BTreeMap::from([
            ("X-Test".to_owned(), "one".to_owned()),
            ("x-test".to_owned(), "two".to_owned()),
        ]))
        .is_err()
    );
    assert!(serde_json::from_str::<ResponseHeaders>(r#"{"X":"one","X":"two"}"#).is_err());
    assert!(
        ResponseHeaders::from_entries([
            ("X".to_owned(), "one".to_owned()),
            ("X".to_owned(), "two".to_owned()),
        ])
        .is_err()
    );
    assert!(
        RawProviderHeaderPatch::new(BTreeMap::from([
            ("X-Test".to_owned(), Some("one".to_owned())),
            ("x-test".to_owned(), Some("two".to_owned())),
        ]))
        .is_err()
    );
    assert!(
        RawProviderHeaderPatch::from_entries([
            ("X".to_owned(), Some("one".to_owned())),
            ("X".to_owned(), Some("two".to_owned())),
        ])
        .is_err()
    );

    let sanitized = ResponseHeaders::sanitize(BTreeMap::from([
        ("Authorization".to_owned(), "sentinel".to_owned()),
        ("X-Public".to_owned(), "value".to_owned()),
    ]))
    .expect("safe header remains");
    assert_eq!(sanitized.as_map().len(), 1);
    assert!(sanitized.as_map().contains_key("X-Public"));
    let sanitized_duplicate = ResponseHeaders::sanitize(BTreeMap::from([
        ("X-Duplicate".to_owned(), "one".to_owned()),
        ("x-duplicate".to_owned(), "two".to_owned()),
        ("X-Safe".to_owned(), "safe".to_owned()),
    ]))
    .expect("unrelated safe header remains");
    assert_eq!(sanitized_duplicate.as_map().len(), 1);
    assert!(sanitized_duplicate.as_map().contains_key("X-Safe"));
    assert!(
        ResponseHeaders::sanitize(BTreeMap::from([(
            "Authorization".to_owned(),
            "sentinel".to_owned(),
        )]))
        .is_none()
    );
}

#[test]
fn search_and_remaining_hook_alternatives_round_trip() {
    let query = serde_json::json!({"text":"auth","limit":10});
    let query_value: SearchQuery = serde_json::from_value(query.clone()).expect("query");
    assert_eq!(serde_json::to_value(query_value).expect("encodes"), query);
    let session_hit = serde_json::json!({
        "sessionId":"session","score":1.0,
        "top":{"entryId":"entry","snippet":"auth","timestamp":1}
    });
    let hit: SessionSearchHit = serde_json::from_value(session_hit.clone()).expect("session hit");
    assert_eq!(serde_json::to_value(hit).expect("encodes"), session_hit);
    let entry_hit = serde_json::json!({
        "sessionId":"session","entryId":"entry","timestamp":1,"snippet":"auth","score":0.5
    });
    let hit: EntrySearchHit = serde_json::from_value(entry_hit.clone()).expect("entry hit");
    assert_eq!(serde_json::to_value(hit).expect("encodes"), entry_hit);

    for event in [
        serde_json::json!({"name":"before_resume","event":{"lane":"main","runId":"op","kind":"run","prompt":[user_message()]}}),
        serde_json::json!({"name":"before_resume","event":{"lane":"main","runId":"op","kind":"compaction","sourceLeafId":null}}),
        serde_json::json!({"name":"before_resume","event":{"lane":"main","runId":"op","kind":"navigation","sourceLeafId":null,"targetId":null,"summarize":false}}),
    ] {
        let decoded: HookEvent = serde_json::from_value(event.clone()).expect("resume alternative");
        assert_eq!(serde_json::to_value(decoded).expect("encodes"), event);
    }

    for step in ["assistant", "deferred", "compaction", "branch_summary"] {
        let event = serde_json::json!({
            "name":"before_request","event":{
                "lane":"main","runId":"op","model":raw_model(serde_json::json!({})),
                "step":step,"attempt":1,"streamOptions":{}
            }
        });
        let decoded: HookEvent = serde_json::from_value(event.clone()).expect("request step");
        assert_eq!(serde_json::to_value(decoded).expect("encodes"), event);
    }

    for reason in ["manual", "threshold", "overflow"] {
        let event = serde_json::json!({
            "name":"before_compaction","event":{
                "lane":"main","runId":"op","reason":reason,
                "preparation":compaction_preparation()
            }
        });
        let decoded: HookEvent = serde_json::from_value(event.clone()).expect("structural reason");
        assert_eq!(serde_json::to_value(decoded).expect("encodes"), event);
    }
}

#[test]
fn broad_optional_omission_presence_null_matrix_is_symmetric() {
    for value in [
        serde_json::json!({"type":"message_start","lane":"main","message":user_message()}),
        serde_json::json!({"type":"message_start","lane":"main","runId":"op","message":user_message()}),
    ] {
        let decoded: HarnessEvent = serde_json::from_value(value.clone()).expect("event optional");
        assert_eq!(serde_json::to_value(decoded).expect("encodes"), value);
    }
    assert!(
        serde_json::from_value::<HarnessEvent>(serde_json::json!({
            "type":"message_start","lane":"main","runId":null,"message":user_message()
        }))
        .is_err()
    );
    assert!(
        serde_json::from_value::<HarnessEvent>(serde_json::json!({
            "type":"retry_end","lane":"main","runId":"op","step":"step","attempt":1,
            "success":false,"finalError":null
        }))
        .is_err()
    );
    for value in [
        serde_json::json!({"type":"fact_update","fact":"custom","key":"key"}),
        serde_json::json!({"type":"fact_update","fact":"custom","key":"key","value":null}),
        serde_json::json!({"type":"fact_update","fact":"custom","key":"key","value":false}),
    ] {
        let decoded: HarnessEvent = serde_json::from_value(value.clone()).expect("presence event");
        assert_eq!(serde_json::to_value(decoded).expect("encodes"), value);
    }

    for value in [
        serde_json::json!({}),
        serde_json::json!({"metadata":null}),
        serde_json::json!({"metadata":{"set":{"nullData":null},"delete":[]}}),
    ] {
        let decoded: AgentHarnessStreamOptionsPatch =
            serde_json::from_value(value.clone()).expect("hook metadata presence");
        assert_eq!(serde_json::to_value(decoded).expect("encodes"), value);
    }
    assert!(
        serde_json::from_value::<BeforeRunResult>(serde_json::json!({
            "systemPrompt":null
        }))
        .is_err()
    );
    for value in [
        serde_json::json!({}),
        serde_json::json!({"details":null}),
        serde_json::json!({"details":{"ok":true}}),
    ] {
        let decoded: AfterToolResult =
            serde_json::from_value(value.clone()).expect("hook detail presence");
        assert_eq!(serde_json::to_value(decoded).expect("encodes"), value);
    }

    let operation = serde_json::json!({
        "id":"op","kind":"run","status":"running","startedAt":0,"runningTools":[]
    });
    assert!(serde_json::from_value::<OperationSnapshot>(operation.clone()).is_ok());
    for field in ["suspended", "streamingMessage", "retry"] {
        let mut null = operation.clone();
        null[field] = serde_json::Value::Null;
        assert!(
            serde_json::from_value::<OperationSnapshot>(null).is_err(),
            "snapshot accepted null {field}"
        );
    }
    for value in [
        serde_json::json!({"entryId":"e","type":"custom"}),
        serde_json::json!({"entryId":"e","type":"custom","data":null}),
        serde_json::json!({"entryId":"e","type":"custom","data":{"ok":true}}),
    ] {
        let decoded: PendingWrite = serde_json::from_value(value.clone()).expect("pending data");
        assert_eq!(serde_json::to_value(decoded).expect("encodes"), value);
    }

    assert!(serde_json::from_value::<Resources>(serde_json::json!({})).is_ok());
    assert!(
        serde_json::from_value::<Resources>(serde_json::json!({
            "promptTemplates":[],"skills":[]
        }))
        .is_ok()
    );
    assert!(serde_json::from_value::<Resources>(serde_json::json!({"skills":null})).is_err());
    assert!(
        serde_json::from_value::<PromptTemplate>(serde_json::json!({
            "name":"t","content":"c","description":null
        }))
        .is_err()
    );

    assert!(serde_json::from_value::<EntryQuery>(serde_json::json!({})).is_ok());
    assert!(serde_json::from_value::<EntryQuery>(serde_json::json!({"limit":1})).is_ok());
    assert!(serde_json::from_value::<EntryQuery>(serde_json::json!({"limit":null})).is_err());

    let navigation = serde_json::json!({"kind":"completed","oldLeafId":null,"newLeafId":null});
    assert!(serde_json::from_value::<NavigationOutcome>(navigation.clone()).is_ok());
    let mut null_summary = navigation;
    null_summary["summaryEntry"] = serde_json::Value::Null;
    assert!(serde_json::from_value::<NavigationOutcome>(null_summary).is_err());
    for value in [
        serde_json::json!({"code":"code","message":"message"}),
        serde_json::json!({"code":"code","message":"message","details":null}),
        serde_json::json!({"code":"code","message":"message","details":false}),
    ] {
        let decoded: OperationError = serde_json::from_value(value.clone()).expect("details");
        assert_eq!(serde_json::to_value(decoded).expect("encodes"), value);
    }
}
