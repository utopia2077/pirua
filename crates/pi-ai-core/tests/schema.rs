use std::collections::BTreeMap;

use pi_ai_core::*;

use serde::{Deserialize, Serialize};
use serde_json::Value;

const CONTENT_MESSAGES: &str =
    include_str!("../../../conformance/schema/ai/content-messages.golden.json");
const EVENTS: &str = include_str!("../../../conformance/schema/ai/events.golden.json");
const MODELS_OPTIONS: &str =
    include_str!("../../../conformance/schema/ai/models-options.golden.json");
const AUTH_DIAGNOSTICS: &str =
    include_str!("../../../conformance/schema/ai/auth-diagnostics.golden.json");
const COMPAT: &str = include_str!("../../../conformance/schema/ai/compat.golden.json");
const MODEL_HEADERS: &str =
    include_str!("../../../conformance/schema/ai/model-headers.golden.json");

fn assert_value_round_trip<T>(value: Value)
where
    T: for<'de> Deserialize<'de> + Serialize,
{
    let decoded: T = serde_json::from_value(value.clone()).expect("golden fixture must decode");
    let encoded = serde_json::to_value(decoded).expect("golden DTO must encode");
    assert_eq!(encoded, value);
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ContentMessagesFixture {
    text_signature: Value,
    commentary_signature: Value,
    content: Vec<Value>,
    messages: Vec<Value>,
    custom: Value,
    context: Context,
}

#[test]
fn content_and_message_golden_round_trips_every_variant() {
    let original: Value = serde_json::from_str(CONTENT_MESSAGES).expect("valid content fixture");
    let fixture: ContentMessagesFixture =
        serde_json::from_value(original.clone()).expect("valid content fixture");
    assert_eq!(
        serde_json::to_value(&fixture).expect("content fixture encodes"),
        original
    );
    assert_value_round_trip::<TextSignatureV1>(fixture.text_signature);
    assert_value_round_trip::<TextSignatureV1>(fixture.commentary_signature);
    assert_eq!(fixture.content.len(), 4);
    assert_value_round_trip::<TextContent>(fixture.content[0].clone());
    assert_value_round_trip::<ThinkingContent>(fixture.content[1].clone());
    assert_value_round_trip::<ImageContent>(fixture.content[2].clone());
    assert_value_round_trip::<ToolCall>(fixture.content[3].clone());
    for message in &fixture.messages {
        assert_value_round_trip::<Message>(message.clone());
    }
    assert_value_round_trip::<CustomMessage>(fixture.custom);
    assert!(fixture.context.messages.is_empty());

    let missing: Message =
        serde_json::from_value(fixture.messages[3].clone()).expect("missing details");
    let null: Message = serde_json::from_value(fixture.messages[4].clone()).expect("null details");
    let value: Message =
        serde_json::from_value(fixture.messages[5].clone()).expect("value details");
    let Message::ToolResult(missing) = missing else {
        panic!("expected tool result");
    };
    let Message::ToolResult(null) = null else {
        panic!("expected tool result");
    };
    let Message::ToolResult(value) = value else {
        panic!("expected tool result");
    };
    assert!(matches!(missing.details, Presence::Missing));
    assert!(matches!(null.details, Presence::Null));
    assert!(matches!(value.details, Presence::Value(_)));
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CompatFixture {
    openai_completions: OpenAiCompletionsCompat,
    openai_responses: OpenAiResponsesCompat,
    anthropic: AnthropicMessagesCompat,
}

#[test]
fn every_typed_model_compat_shape_matches_the_golden_contract() {
    let value: Value = serde_json::from_str(COMPAT).expect("valid compat fixture");
    let fixture: CompatFixture =
        serde_json::from_value(value.clone()).expect("compat fixture decodes");
    assert_eq!(
        serde_json::to_value(fixture).expect("compat fixture encodes"),
        value
    );

    let cases = [
        ("openai-completions", &value["openaiCompletions"]),
        ("openai-responses", &value["openaiResponses"]),
        ("anthropic-messages", &value["anthropic"]),
    ];
    for (api, compat) in cases {
        let raw: ModelCompat = serde_json::from_value(compat.clone()).expect("raw compat object");
        let typed = raw.decode_for_api(&ApiId::new(api).expect("known API"));
        assert!(typed.is_ok());
    }
    let raw: ModelCompat =
        serde_json::from_value(serde_json::json!({})).expect("raw compat object");
    assert!(
        raw.decode_for_api(&ApiId::new("future-api").expect("future API"))
            .is_err()
    );
}

#[test]
fn assistant_event_golden_round_trips_all_sixteen_variants() {
    let value: Value = serde_json::from_str(EVENTS).expect("valid event fixture");
    let events: Vec<AssistantMessageEvent> =
        serde_json::from_value(value.clone()).expect("events decode");
    assert_eq!(events.len(), 16);
    assert_eq!(serde_json::to_value(events).expect("events encode"), value);
}

#[test]
fn terminal_event_invariants_reject_causal_contradictions_both_directions() {
    for fixture in [
        include_str!("../../../conformance/schema/ai/negative/terminal-reason-mismatch.json"),
        include_str!("../../../conformance/schema/ai/negative/terminal-pending.json"),
        include_str!("../../../conformance/schema/ai/negative/terminal-missing-error-message.json"),
    ] {
        assert!(serde_json::from_str::<AssistantMessageEvent>(fixture).is_err());
    }

    let events: Vec<AssistantMessageEvent> =
        serde_json::from_str(EVENTS).expect("valid event fixture");
    let mut mismatched = events[10].clone();
    let AssistantMessageEvent::Done { message, .. } = &mut mismatched else {
        panic!("fixture index 10 must be a done event");
    };
    message.stop_reason = pi_ai_core::StopReason::Length;
    assert!(serde_json::to_value(mismatched).is_err());

    let mut pending = events[10].clone();
    let AssistantMessageEvent::Done { message, .. } = &mut pending else {
        panic!("fixture index 10 must be a done event");
    };
    message.stop_reason = pi_ai_core::StopReason::Pending;
    assert!(serde_json::to_value(pending).is_err());

    let mut missing_error = events[14].clone();
    let AssistantMessageEvent::Error { error, .. } = &mut missing_error else {
        panic!("fixture index 14 must be an error event");
    };
    error.error_message = None;
    assert!(serde_json::to_value(missing_error).is_err());

    let mut empty_error = events[14].clone();
    let AssistantMessageEvent::Error { error, .. } = &mut empty_error else {
        panic!("fixture index 14 must be an error event");
    };
    error.error_message = Some(String::new());
    assert!(empty_error.validate().is_err());
    assert!(serde_json::to_value(empty_error).is_err());

    let mut empty_value: Value = serde_json::from_str(EVENTS).expect("valid event fixture");
    empty_value[15]["error"]["errorMessage"] = Value::String(String::new());
    assert!(serde_json::from_value::<Vec<AssistantMessageEvent>>(empty_value).is_err());
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ModelsOptionsFixture {
    model: Model,
    store: ModelsStoreEntry,
    tools: Vec<Tool>,
    stream_options: StreamOptionsWire,
    simple_stream_options: SimpleStreamOptionsWire,
    deferred_fetch_options: DeferredFetchOptionsWire,
    deferred_cancel_options: DeferredCancelOptionsWire,
    retry: RetryPolicy,
    raw_headers: ProviderHeaders,
    provider_env: ProviderEnv,
}

#[test]
fn models_options_tools_and_routing_match_golden_shape() {
    let value: Value = serde_json::from_str(MODELS_OPTIONS).expect("valid models fixture");
    let fixture: ModelsOptionsFixture =
        serde_json::from_value(value.clone()).expect("models fixture decodes");
    assert_eq!(fixture.tools.len(), 3);
    assert!(
        fixture
            .model
            .typed_compat()
            .expect("typed compatibility")
            .is_some()
    );
    assert_eq!(
        serde_json::to_value(fixture).expect("models fixture encodes"),
        value
    );
}

#[test]
fn model_serde_validates_compat_family_on_input_and_output() {
    for fixture in [
        include_str!("../../../conformance/schema/ai/negative/unsupported-model-compat.json"),
        include_str!("../../../conformance/schema/ai/negative/wrong-model-compat-family.json"),
        include_str!("../../../conformance/schema/ai/negative/unknown-model-compat-field.json"),
    ] {
        assert!(serde_json::from_str::<Model>(fixture).is_err());
    }

    let source: Value = serde_json::from_str(MODELS_OPTIONS).expect("models fixture");
    for api in ["azure-openai-responses", "openai-codex-responses"] {
        let mut model = source["model"].clone();
        model["api"] = Value::String(api.to_owned());
        model["compat"] = serde_json::json!({ "supportsToolSearch": true });
        assert!(serde_json::from_value::<Model>(model).is_err());
    }

    let mut invalid: Model = serde_json::from_value(source["model"].clone()).expect("valid model");
    invalid.api = ApiId::new("future-api").expect("future API");
    invalid.compat = Some(ModelCompat::default());
    assert!(serde_json::to_value(invalid).is_err());
}

#[test]
fn model_header_policy_is_default_deny_and_store_restore_reclassifies() {
    let value: Value = serde_json::from_str(MODEL_HEADERS).expect("model-header fixture");
    assert!(serde_json::from_value::<Model>(value["model"].clone()).is_err());
    assert!(serde_json::from_value::<ModelsStoreEntry>(value["store"].clone()).is_err());

    let raw: RawModel = serde_json::from_value(value["model"].clone()).expect("raw model");
    assert!(
        raw.clone()
            .into_model(&ModelHeaderPolicy::default())
            .is_err()
    );
    let policy = ModelHeaderPolicy::from_allowlist(vec!["X-PUBLIC-MODEL".to_owned()]);
    let model = raw
        .into_model(&policy)
        .expect("explicit public classification");
    assert_eq!(
        serde_json::to_value(&model).expect("classified model serializes"),
        value["model"]
    );
    assert!(!format!("{model:?}").contains("public-value"));

    let raw_store: RawModelsStoreEntry =
        serde_json::from_value(value["store"].clone()).expect("raw store");
    assert!(
        raw_store
            .clone()
            .into_store(&ModelHeaderPolicy::default())
            .is_err()
    );
    let store = raw_store
        .into_store(&policy)
        .expect("current policy accepts stored headers");
    assert_eq!(
        serde_json::to_value(store).expect("classified store serializes"),
        value["store"]
    );

    let mut header_free = value["model"].clone();
    header_free
        .as_object_mut()
        .expect("model object")
        .remove("headers");
    assert!(serde_json::from_value::<Model>(header_free).is_ok());
    let mut present_empty = value["model"].clone();
    present_empty["headers"] = serde_json::json!({});
    assert!(serde_json::from_value::<Model>(present_empty).is_ok());
}

#[test]
fn model_header_classification_hard_denies_credentials_and_policy_secrets() {
    for name in [
        "Authorization",
        "PROXY-AUTHORIZATION",
        "cookie",
        "Set-Cookie",
        "api-key",
        "X-API-KEY",
        "x-goog-api-key",
    ] {
        let hard_denied = RawModelHeaders::new(BTreeMap::from([(
            name.to_owned(),
            "unique-credential-sentinel".to_owned(),
        )]))
        .expect("no duplicate names");
        let mistaken_policy = ModelHeaderPolicy::from_allowlist(vec![name.to_owned()]);
        assert!(matches!(
            hard_denied.classify(&mistaken_policy),
            Err(ModelHeaderClassificationError::HardDenied(_))
        ));
    }

    let policy_secret = RawModelHeaders::new(BTreeMap::from([(
        "x-policy-secret".to_owned(),
        "unique-credential-sentinel".to_owned(),
    )]))
    .expect("no duplicate names");
    assert!(matches!(
        policy_secret.classify(&ModelHeaderPolicy::default()),
        Err(ModelHeaderClassificationError::PolicyDenied(_))
    ));

    let benign = RawModelHeaders::new(BTreeMap::from([(
        "x-benign".to_owned(),
        "ordinary".to_owned(),
    )]))
    .expect("no duplicate names");
    assert!(benign.classify(&ModelHeaderPolicy::default()).is_err());

    let anti_overconstraint = RawModelHeaders::new(BTreeMap::from([(
        "x-benign".to_owned(),
        "Bearer token key".to_owned(),
    )]))
    .expect("no duplicate names");
    assert!(
        anti_overconstraint
            .classify(&ModelHeaderPolicy::from_allowlist(vec![
                "x-benign".to_owned()
            ]))
            .is_ok()
    );
}

#[test]
fn model_base_url_validation_rejects_only_credential_capable_components() {
    let source: Value = serde_json::from_str(MODELS_OPTIONS).expect("models fixture");
    let base_model = source["model"].clone();

    for rejected in [
        "",
        "https://userinfo-sentinel:password@localhost/path",
        "opaque//userinfo-sentinel@localhost/path",
        "https://localhost/path?query-sentinel=value",
        "https://localhost/path#fragment-sentinel",
    ] {
        let mut model_value = base_model.clone();
        model_value["baseUrl"] = Value::String(rejected.to_owned());
        let raw: RawModel =
            serde_json::from_value(model_value.clone()).expect("raw model accepts transient URL");
        assert!(raw.into_model(&ModelHeaderPolicy::default()).is_err());
        assert!(serde_json::from_value::<Model>(model_value.clone()).is_err());

        let store_value = serde_json::json!({"models":[model_value],"checkedAt":1});
        let raw_store: RawModelsStoreEntry =
            serde_json::from_value(store_value.clone()).expect("raw store accepts transient URL");
        assert!(raw_store.into_store(&ModelHeaderPolicy::default()).is_err());
        assert!(serde_json::from_value::<ModelsStoreEntry>(store_value).is_err());
    }

    for accepted in [
        "https://{location}-aiplatform.googleapis.com/v1",
        "https://gateway.ai.cloudflare.com/v1/{accountId}/{gatewayId}",
        "http://localhost:0",
        "relative/path",
        "custom+unix://localhost/socket/path",
        "opaque//localhost/path/user@value",
    ] {
        let mut model_value = base_model.clone();
        model_value["baseUrl"] = Value::String(accepted.to_owned());
        assert_value_round_trip::<Model>(model_value);

        let public = PublicModelBaseUrl::new(accepted).expect("structurally public endpoint");
        assert_eq!(public.as_str(), accepted);
    }
}

#[test]
fn every_raw_validated_and_store_model_debug_redacts_the_full_base_url() {
    let source: Value = serde_json::from_str(MODELS_OPTIONS).expect("models fixture");
    let raw_url =
        "https://debug-userinfo-sentinel:password@localhost/path?debug-query-sentinel=value";

    let mut raw_model_value = source["model"].clone();
    raw_model_value["baseUrl"] = Value::String(raw_url.to_owned());
    let raw_model: RawModel = serde_json::from_value(raw_model_value.clone()).expect("raw model");
    let raw_store: RawModelsStoreEntry = serde_json::from_value(serde_json::json!({
        "models": [raw_model_value],
        "checkedAt": 1
    }))
    .expect("raw store");

    for rendered in [format!("{raw_model:?}"), format!("{raw_store:?}")] {
        assert!(!rendered.contains("debug-userinfo-sentinel"));
        assert!(!rendered.contains("debug-query-sentinel"));
    }

    let accepted = "https://localhost/path/debug-userinfo-sentinel/debug-query-sentinel";
    let public_url = PublicModelBaseUrl::new(accepted).expect("public path endpoint");
    let public_rendered = format!("{public_url:?}");
    assert!(!public_rendered.contains("debug-userinfo-sentinel"));
    assert!(!public_rendered.contains("debug-query-sentinel"));
    let rejected = PublicModelBaseUrl::new("https://localhost/path?debug-query-sentinel=value")
        .expect_err("query must be rejected");
    assert!(!rejected.to_string().contains("debug-query-sentinel"));
    let mut model_value = source["model"].clone();
    model_value["baseUrl"] = Value::String(accepted.to_owned());
    let model: Model = serde_json::from_value(model_value).expect("validated model");
    let store = ModelsStoreEntry {
        models: vec![model.clone()],
        last_modified: None,
        checked_at: None,
        etag: None,
    };

    for rendered in [format!("{model:?}"), format!("{store:?}")] {
        assert!(!rendered.contains("debug-userinfo-sentinel"));
        assert!(!rendered.contains("debug-query-sentinel"));
    }
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AuthDiagnosticsFixture {
    credentials: Vec<Credential>,
    credential_info: CredentialInfo,
    auth_result: AuthResult,
    auth_check: AuthCheck,
    prompts: Vec<AuthPrompt>,
    events: Vec<AuthEvent>,
    diagnostic: AssistantMessageDiagnostic,
    models_error_codes: Vec<ModelsErrorCode>,
}

#[test]
fn auth_diagnostics_and_error_codes_match_golden_shape() {
    let value: Value = serde_json::from_str(AUTH_DIAGNOSTICS).expect("valid auth fixture");
    let fixture: AuthDiagnosticsFixture =
        serde_json::from_value(value.clone()).expect("auth fixture decodes");
    assert_eq!(fixture.credentials.len(), 3);
    assert_eq!(fixture.prompts.len(), 4);
    assert_eq!(fixture.events.len(), 4);
    assert_eq!(fixture.models_error_codes.len(), 6);
    assert_eq!(
        serde_json::to_value(fixture).expect("auth fixture encodes"),
        value
    );
}

#[test]
fn optional_non_null_fields_reject_null_but_preserve_omission_and_values() {
    let explicit_null =
        include_str!("../../../conformance/schema/ai/negative/optional-non-null.json");
    assert!(serde_json::from_str::<TextContent>(explicit_null).is_err());
    assert!(serde_json::from_str::<TextContent>(r#"{"type":"text","text":"complete"}"#).is_ok());

    let preserved: SimpleStreamOptionsWire = serde_json::from_str(
        r#"{"timeoutMs":0,"metadata":{},"deferred":false,"thinkingBudgets":{"minimal":0}}"#,
    )
    .expect("zero, false, and empty object remain valid");
    assert_eq!(
        serde_json::to_value(preserved).expect("options serialize"),
        serde_json::json!({"timeoutMs":0,"metadata":{},"deferred":false,"thinkingBudgets":{"minimal":0}})
    );

    for simple_only in [
        r#"{"toolChoice":"none"}"#,
        r#"{"reasoning":"high"}"#,
        r#"{"deferred":false}"#,
        r#"{"thinkingBudgets":{"low":1}}"#,
    ] {
        assert!(serde_json::from_str::<StreamOptionsWire>(simple_only).is_err());
        assert!(serde_json::from_str::<SimpleStreamOptionsWire>(simple_only).is_ok());
    }
}

#[test]
fn optional_and_presence_source_guard_prevents_future_plain_option_regression() {
    let sources = [
        include_str!("../src/auth.rs"),
        include_str!("../src/content.rs"),
        include_str!("../src/context.rs"),
        include_str!("../src/diagnostic.rs"),
        include_str!("../src/message.rs"),
        include_str!("../src/model.rs"),
        include_str!("../src/options.rs"),
        include_str!("../src/tool.rs"),
        include_str!("../src/usage.rs"),
    ];
    for source in sources {
        let lines: Vec<&str> = source.lines().collect();
        for (index, line) in lines.iter().enumerate() {
            if line.contains(": Option<") && line.trim_end().ends_with(',') {
                let immediate = lines[index - 1].trim();
                if immediate.contains("serde-non-null: delegated-to-raw-model") {
                    continue;
                }
                let start = (0..index)
                    .rev()
                    .find(|candidate| lines[*candidate].trim_start().starts_with("#[serde("))
                    .expect("multiline serde attribute must have a start");
                let attributes = lines[start..index].join("\n");
                assert!(attributes.contains("#[serde("));
                assert!(
                    attributes.contains("deserialize_optional_non_null")
                        || attributes.contains("deserialize_optional_btree_map_non_null"),
                    "plain optional wire field lacks non-null enforcement: {line}"
                );
            }
            if line.contains(": Presence<") && line.trim_end().ends_with(',') {
                let immediate = lines[index - 1].trim();
                assert!(
                    immediate.starts_with("#[serde("),
                    "Presence field has no direct serde attribute: {line}"
                );
                let attributes = immediate;
                assert!(
                    attributes.contains("default"),
                    "Presence field lacks default: {line}"
                );
                assert!(
                    attributes.contains("Presence::is_missing"),
                    "Presence field lacks missing-value skip: {line}"
                );
            }
        }
    }
}

#[test]
fn oauth_extras_reject_reserved_keys_and_preserve_nonreserved_values() {
    for key in ["type", "refresh", "access", "expires"] {
        let extras = BTreeMap::from([(key.to_owned(), JsonValue::Null)]);
        assert!(OAuthExtraFields::new(extras).is_err());
        assert!(
            serde_json::from_value::<OAuthExtraFields>(serde_json::json!({(key): null})).is_err()
        );
    }
    let extras = OAuthExtraFields::new(BTreeMap::from([(
        "accountId".to_owned(),
        JsonValue::String("account".to_owned()),
    )]))
    .expect("nonreserved extension");
    assert_eq!(
        serde_json::to_value(extras).expect("extensions serialize"),
        serde_json::json!({"accountId":"account"})
    );
    let duplicate =
        include_str!("../../../conformance/schema/ai/negative/oauth-duplicate-reserved.json");
    assert!(serde_json::from_str::<Credential>(duplicate).is_err());
}

#[test]
fn retry_policy_keeps_normalized_attempt_count_javascript_safe() {
    let invalid =
        include_str!("../../../conformance/schema/ai/negative/retry-max-unsafe-normalization.json");
    assert!(serde_json::from_str::<RetryPolicy>(invalid).is_err());
    let retries = MaxRetries::new(JS_SAFE_INTEGER_MAX - 1).expect("largest safe retry count");
    assert_eq!(retries.max_attempts(), JS_SAFE_INTEGER_MAX);
    assert!(MaxRetries::new(JS_SAFE_INTEGER_MAX).is_err());
    let policy: RetryPolicy = serde_json::from_value(serde_json::json!({
        "enabled": true,
        "maxRetries": JS_SAFE_INTEGER_MAX - 1,
        "baseDelayMs": JS_SAFE_INTEGER_MAX
    }))
    .expect("base delay retains full SafeUnsigned range");
    assert_eq!(policy.base_delay_ms.get(), JS_SAFE_INTEGER_MAX);
}

#[test]
fn open_maps_reject_exact_and_case_insensitive_duplicates() {
    assert!(
        serde_json::from_str::<JsonObject>(include_str!(
            "../../../conformance/schema/ai/negative/json-object-duplicate.json"
        ))
        .is_err()
    );
    assert!(
        serde_json::from_str::<ProviderHeaders>(include_str!(
            "../../../conformance/schema/ai/negative/provider-headers-duplicate.json"
        ))
        .is_err()
    );
    assert!(
        serde_json::from_str::<ProviderEnv>(include_str!(
            "../../../conformance/schema/ai/negative/provider-env-duplicate.json"
        ))
        .is_err()
    );
    assert!(
        serde_json::from_str::<RawModelHeaders>(include_str!(
            "../../../conformance/schema/ai/negative/raw-model-headers-case-duplicate.json"
        ))
        .is_err()
    );
    assert!(
        RawModelHeaders::new(BTreeMap::from([
            ("X-Public".to_owned(), "first".to_owned()),
            ("x-public".to_owned(), "second".to_owned()),
        ]))
        .is_err()
    );
}

#[test]
fn unknown_closed_tags_fields_and_literals_are_rejected() {
    let unknown_event =
        include_str!("../../../conformance/schema/ai/negative/unknown-event-tag.json");
    let unknown_field =
        include_str!("../../../conformance/schema/ai/negative/unknown-content-field.json");
    let unknown_credential =
        include_str!("../../../conformance/schema/ai/negative/unknown-credential-tag.json");
    let true_literal =
        include_str!("../../../conformance/schema/ai/negative/literal-false-is-true.json");
    assert!(serde_json::from_str::<AssistantMessageEvent>(unknown_event).is_err());
    assert!(serde_json::from_str::<TextContent>(unknown_field).is_err());
    assert!(serde_json::from_str::<Credential>(unknown_credential).is_err());
    assert!(serde_json::from_str::<LiteralFalse>(true_literal).is_err());
    assert!(serde_json::from_str::<LiteralTrue>("false").is_err());
    assert!(
        serde_json::from_str::<Message>(r#"{"role":"future","content":"hello","timestamp":1}"#)
            .is_err()
    );
}

#[test]
fn open_ids_and_safe_numbers_enforce_the_frozen_boundary() {
    let empty = include_str!("../../../conformance/schema/ai/negative/empty-open-id.json");
    let unsafe_unsigned =
        include_str!("../../../conformance/schema/ai/negative/unsafe-unsigned.json");
    assert!(serde_json::from_str::<ApiId>(empty).is_err());
    let future: ApiId = serde_json::from_str(r#""future-api""#).expect("future API accepted");
    assert_eq!(future.as_str(), "future-api");
    let whitespace: ApiId = serde_json::from_str(r#""   ""#).expect("nonempty whitespace accepted");
    assert_eq!(whitespace.as_str(), "   ");
    assert!(serde_json::from_str::<SafeUnsigned>(unsafe_unsigned).is_err());
    assert!(serde_json::from_str::<SafeUnsigned>("-1").is_err());
    assert!(serde_json::from_str::<SafeUnsigned>("1.5").is_err());
    assert_eq!(ApiId::KNOWN.len(), 4);
    assert_eq!(ProviderId::KNOWN.len(), 4);
}

#[test]
fn credential_and_transient_map_debug_output_is_redacted() {
    let value: Value = serde_json::from_str(AUTH_DIAGNOSTICS).expect("valid auth fixture");
    let credential: Credential =
        serde_json::from_value(value["credentials"][2].clone()).expect("OAuth credential");
    let rendered = format!("{credential:?}");
    assert!(rendered.contains("[REDACTED]"));
    assert!(!rendered.contains("secret-refresh"));
    assert!(!rendered.contains("secret-access"));

    let headers: ProviderHeaders =
        serde_json::from_value(serde_json::json!({"authorization":"secret"}))
            .expect("headers decode");
    assert!(!format!("{headers:?}").contains("secret"));

    let auth: ModelAuth = serde_json::from_value(serde_json::json!({
        "apiKey": "debug-api-key-sentinel",
        "headers": {"authorization": "debug-header-sentinel"},
        "baseUrl": "https://user:password@example.invalid/path?token=debug-query-sentinel"
    }))
    .expect("model auth decodes");
    let rendered = format!("{auth:?}");
    for secret in [
        "debug-api-key-sentinel",
        "debug-header-sentinel",
        "user:password",
        "debug-query-sentinel",
    ] {
        assert!(!rendered.contains(secret));
    }
    let auth_result = AuthResult {
        auth,
        env: None,
        source: None,
    };
    let rendered = format!("{auth_result:?}");
    assert!(!rendered.contains("user:password"));
    assert!(!rendered.contains("debug-query-sentinel"));
}

#[test]
fn public_source_contains_no_generated_catalog_constants() {
    let source = concat!(
        include_str!("../src/lib.rs"),
        include_str!("../src/model.rs")
    );
    for forbidden in [
        "pub const MODELS",
        "pub const IMAGE_MODELS",
        "BuiltinProvider",
    ] {
        assert!(
            !source.contains(forbidden),
            "found forbidden generated surface {forbidden}"
        );
    }
}

#[test]
fn assistant_content_union_rejects_image_blocks() {
    let image = r#"{"type":"image","data":"aA==","mimeType":"image/png"}"#;
    assert!(serde_json::from_str::<AssistantContent>(image).is_err());
}

#[test]
fn recursive_json_rejects_nested_unsafe_numbers() {
    assert!(serde_json::from_str::<JsonValue>(r#"{"nested":[9007199254740992]}"#).is_err());
}
