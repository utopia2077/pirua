use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use pi_ai_core::{
    BoxFuture, CancellationToken, Cancelled, Clock, Context, JsonValue, Model, ModelThinkingLevel,
    RequestRuntime, SafeUnsigned, SpawnError, StopReason, TaskSpawner, Tool,
};
use pi_ai_providers::api::google_shared::*;
use pi_ai_providers::api::wire_json::stringify_wire_json;
use pi_ai_transport::{
    HttpHeaders, HttpRequest, HttpResponse, HttpTransport, ProviderAttemptFailure,
    ProviderRetryError, RawHttpClient, RetryJitter, TransportError, TransportErrorKind,
};
use serde_json::{Value, json};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/provider-helpers/google-shared/vectors.json"
    ))
    .unwrap()
}
fn safe(value: u64) -> SafeUnsigned {
    SafeUnsigned::new(value).unwrap()
}
fn model(value: &Value) -> Model {
    serde_json::from_value(value.clone()).unwrap()
}
fn tools(value: &Value) -> Vec<Tool> {
    serde_json::from_value(value.clone()).unwrap()
}

#[test]
fn exact_export_inventory_and_installed_sdk_enums_are_complete() {
    let f = fixture();
    assert_eq!(
        f["exports"],
        json!([
            "GoogleApiThinkingLevel",
            "ResolvedGoogleThinkingLevel",
            "resolveGoogleThinkingLevel",
            "isThinkingPart",
            "retainThoughtSignature",
            "requiresToolCallId",
            "convertMessages",
            "convertTools",
            "supportsGoogleStrictToolSampling",
            "mapToolChoice",
            "resolveGoogleFunctionCallingMode",
            "mapStopReason",
            "mapStopReasonString",
            "retryGoogleRequest"
        ])
    );
    for (_, value) in f["enums"]["ThinkingLevel"].as_object().unwrap() {
        let level: GoogleApiThinkingLevel = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(level).unwrap(), *value);
    }
    for (_, value) in f["enums"]["FunctionCallingConfigMode"].as_object().unwrap() {
        let mode: GoogleFunctionCallingConfigMode = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(mode).unwrap(), *value);
    }
    assert_eq!(f["enums"]["FinishReason"].as_object().unwrap().len(), 17);
    for value in [json!(null), json!("FUTURE"), json!(true), json!(0)] {
        assert!(serde_json::from_value::<GoogleApiThinkingLevel>(value.clone()).is_err());
        assert!(serde_json::from_value::<ResolvedGoogleThinkingLevel>(value.clone()).is_err());
        assert!(serde_json::from_value::<GoogleFinishReason>(value.clone()).is_err());
        assert!(serde_json::from_value::<GoogleFunctionCallingConfigMode>(value).is_err());
    }
}

#[test]
fn all_thinking_mappings_markers_and_retention_match_pinned_execution() {
    let f = fixture();
    for case in f["thinking"].as_array().unwrap() {
        let level: ModelThinkingLevel = serde_json::from_value(case["level"].clone()).unwrap();
        let result = resolve_google_thinking_level(&model(&case["model"]), level);
        if case["error"] == true {
            assert_eq!(
                result.unwrap_err(),
                GoogleSharedError::UnsupportedThinkingLevel
            );
        } else {
            assert_eq!(json!(result.unwrap()), case["expected"], "{case}");
        }
    }
    for case in f["markers"].as_array().unwrap() {
        let part = GooglePart {
            thought: case["part"]["thought"].as_bool(),
            thought_signature: case["part"]["thoughtSignature"].as_str().map(str::to_owned),
            ..Default::default()
        };
        assert_eq!(is_thinking_part(&part), case["expected"].as_bool().unwrap());
    }
    for case in f["retention"].as_array().unwrap() {
        assert_eq!(
            json!(retain_thought_signature(
                case["existing"].as_str(),
                case["incoming"].as_str()
            )),
            case["expected"]
        );
    }
}

#[test]
fn prefix_case_sensitivity_versions_choices_and_both_stop_dispatches_match_source() {
    let f = fixture();
    for case in f["models"].as_array().unwrap() {
        let id = case["id"].as_str().unwrap();
        assert_eq!(
            requires_tool_call_id(id),
            case["requiresId"].as_bool().unwrap(),
            "{id}"
        );
        assert_eq!(
            supports_google_strict_tool_sampling(id),
            case["strict"].as_bool().unwrap(),
            "{id}"
        );
    }
    for case in f["choices"].as_array().unwrap() {
        assert_eq!(
            json!(map_tool_choice(case["choice"].as_str().unwrap())),
            case["expected"]
        );
    }
    for case in f["stops"].as_array().unwrap() {
        assert_eq!(
            json!(map_stop_reason_string(case["reason"].as_str().unwrap())),
            case["raw"]
        );
        let known = serde_json::from_value::<GoogleFinishReason>(case["reason"].clone());
        if case["known"].is_null() {
            assert!(known.is_err());
        } else {
            assert_eq!(json!(map_stop_reason(known.unwrap())), case["known"]);
        }
    }
}

#[test]
fn all_complete_message_projections_match_executed_source_without_mutation() {
    for case in fixture()["messages"].as_array().unwrap() {
        let m = model(&case["model"]);
        let context: Context = serde_json::from_value(case["context"].clone()).unwrap();
        let before = serde_json::to_value(&context).unwrap();
        let output = convert_messages(&m, &context, safe(7)).unwrap();
        assert_eq!(json!(output), case["expected"], "{}", case["id"]);
        assert_eq!(serde_json::to_value(context).unwrap(), before);
        assert!(!format!("{output:?}").contains("attached image"));
    }
}

#[test]
fn signed_empty_blocks_are_preserved_as_distinct_parts() {
    let f = fixture();
    let case = f["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["id"] == "signature-2")
        .unwrap();
    let context: Context = serde_json::from_value(case["context"].clone()).unwrap();
    let output = convert_messages(&model(&case["model"]), &context, safe(7)).unwrap();
    assert_eq!(output[0].parts.len(), 3);
    assert_eq!(output[0].parts[0].text.as_deref(), Some(""));
    assert_eq!(
        output[0].parts[0].thought_signature.as_deref(),
        Some("AAAA")
    );
    assert_eq!(output[0].parts[1].thought, Some(true));
    assert_eq!(
        output[0].parts[1].thought_signature.as_deref(),
        Some("AAAA")
    );
    assert_eq!(
        output[0].parts[2].thought_signature.as_deref(),
        Some("AAAA")
    );
}

#[test]
fn schema_arrays_are_not_sanitized_and_strict_mode_short_circuits_exactly() {
    let f = fixture();
    for case in f["tools"].as_array().unwrap() {
        let input = tools(&case["tools"]);
        let before = json!(input);
        let result = convert_tools(
            &input,
            case["useParameters"].as_bool().unwrap(),
            case["supportsStrictMode"].as_bool().unwrap(),
        );
        if case["error"] == true {
            assert!(result.is_err(), "{case}");
        } else {
            assert_eq!(json!(result.unwrap()), case["expected"], "{case}");
        }
        assert_eq!(json!(input), before);
    }
    for case in f["modes"].as_array().unwrap() {
        let result = resolve_google_function_calling_mode(
            &tools(&case["tools"]),
            case["choice"].as_str(),
            case["supports"].as_bool().unwrap(),
        );
        if case["error"] == true {
            assert!(result.is_err(), "{case}");
        } else {
            assert_eq!(json!(result.unwrap()), case["expected"], "{case}");
        }
    }
    let case = f["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| {
            case["useParameters"] == true && case["tools"][0]["parameters"].get("anyOf").is_some()
        })
        .unwrap();
    let out = json!(convert_tools(&tools(&case["tools"]), true, true).unwrap());
    assert_eq!(
        out[0]["functionDeclarations"][0]["parameters"]["anyOf"][0]["$id"],
        "keep"
    );
    assert!(
        out[0]["functionDeclarations"][0]["parameters"]
            .get("$id")
            .is_none()
    );
    let schema = &out[0]["functionDeclarations"][0]["parameters"];
    assert!(schema.get("__proto__").is_none());
    assert!(schema["properties"].get("__proto__").is_none());
    assert_eq!(
        schema["properties"]["constructor"],
        json!({"type":"boolean"})
    );
    assert_eq!(
        schema["default"]["x"][0]["__proto__"],
        json!({"array":"own"})
    );
    assert_eq!(
        schema["default"]["x"][0]["constructor"],
        "array-constructor"
    );
}

#[test]
fn core_identity_collision_and_schema_resource_limits_are_not_silently_weakened() {
    let f = fixture();
    let case = &f["messages"][0];
    let mut value = case["context"].clone();
    value["messages"][0]["model"] = json!("foreign");
    value["messages"][0]["content"] = json!([
        {"type":"toolCall","id":"a|b","name":"read","arguments":{}},
        {"type":"toolCall","id":"a/b","name":"read","arguments":{}}]);
    let context: Context = serde_json::from_value(value).unwrap();
    assert!(matches!(
        convert_messages(&model(&case["model"]), &context, safe(7)),
        Err(GoogleSharedError::MessageTransform(_))
    ));
    let mut tool =
        tools(&json!([{"name":"limit","description":"fixture","parameters":{"type":"object"}}]))
            .remove(0);
    let mut nested = JsonValue::Null;
    for _ in 0..130 {
        nested = JsonValue::Array(vec![nested]);
    }
    tool.parameters.0 = pi_ai_core::JsonObject::new(std::collections::BTreeMap::from([(
        "default".into(),
        nested,
    )]));
    assert!(matches!(
        convert_tools(&[tool], true, false),
        Err(GoogleSharedError::WireJson(_))
    ));
}

fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut TaskContext::from_waker(Waker::noop()))
}
fn ready<F: Future>(future: F) -> F::Output {
    match poll(Box::pin(future).as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected ready"),
    }
}
struct NeverClient;
impl RawHttpClient for NeverClient {
    fn send<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        panic!("shared retry must execute the supplied complete attempt, not send HTTP")
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        0.4
    }
}
struct Spawner;
impl TaskSpawner for Spawner {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        panic!("helper never spawns")
    }
}
#[derive(Default)]
struct TestClock {
    sleeps: Mutex<Vec<Duration>>,
    blocked: bool,
    drops: Arc<AtomicUsize>,
}
struct DropWitness(Arc<AtomicUsize>);
impl Drop for DropWitness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        self.sleeps.lock().unwrap().push(duration);
        let witness = DropWitness(self.drops.clone());
        Box::pin(async move {
            let _witness = witness;
            if self.blocked {
                pending::<()>().await;
            }
            Ok(())
        })
    }
}
fn runtime(clock: Arc<TestClock>) -> RequestRuntime {
    RequestRuntime::with_noop_telemetry(CancellationToken::new(), clock, Arc::new(Spawner))
}
fn transport() -> HttpTransport {
    HttpTransport::new(Arc::new(NeverClient), Arc::new(Jitter))
}
// No Clone/Debug/Display/Error/Serde: preserving this box proves opaque ownership.
struct OpaqueFailure(Box<u64>);

#[test]
fn google_missing_headers_normalization_and_retry_policy_match_executed_sdk_errors() {
    for case in fixture()["retries"].as_array().unwrap() {
        let clock = Arc::new(TestClock::default());
        let runtime = runtime(clock.clone());
        let transport = transport();
        let calls = AtomicUsize::new(0);
        let mut identity = Some(Box::new(77));
        let original = identity.as_ref().unwrap().as_ref() as *const u64;
        let outcome = ready(retry_google_request(
            &transport,
            &runtime,
            GoogleRetryOptions {
                max_retries: case["maxRetries"].as_u64().map(safe),
                max_retry_delay_ms: case["maxRetryDelayMs"].as_u64().map(safe),
            },
            || {
                let result = if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    let error = OpaqueFailure(identity.take().unwrap());
                    let status = case["status"]
                        .as_u64()
                        .map(|value| u16::try_from(value).unwrap());
                    Err(match case["kind"].as_str().unwrap() {
                        "api" => GoogleRequestFailure::ApiError { status, error },
                        "provider" => {
                            let mut headers = HttpHeaders::new();
                            for pair in case["headers"].as_array().unwrap() {
                                headers
                                    .append(pair[0].as_str().unwrap(), pair[1].as_str().unwrap())
                                    .unwrap();
                            }
                            GoogleRequestFailure::Provider {
                                status,
                                headers: Some(headers),
                                error,
                            }
                        }
                        _ => GoogleRequestFailure::Other(error),
                    })
                } else {
                    Ok("ok")
                };
                Box::pin(async move { result })
            },
        ));
        match outcome {
            Ok(value) => assert_eq!(value, case["outcome"].as_str().unwrap()),
            Err(ProviderRetryError::Attempt(failure)) => {
                assert_eq!(case["outcome"], "attempt");
                let error = match failure {
                    ProviderAttemptFailure::Provider { error, .. }
                    | ProviderAttemptFailure::Other(error) => error,
                };
                assert_eq!(error.0.as_ref() as *const u64, original);
            }
            Err(ProviderRetryError::Policy {
                error,
                previous:
                    Some(ProviderAttemptFailure::Provider {
                        error: previous, ..
                    }),
            }) => {
                assert_eq!(case["outcome"], "policy");
                assert_eq!(error.kind(), TransportErrorKind::RetryDelayExceeded);
                assert_eq!(previous.0.as_ref() as *const u64, original);
            }
            _ => panic!("unexpected retry result"),
        }
        assert_eq!(
            calls.load(Ordering::SeqCst) as u64,
            case["calls"].as_u64().unwrap()
        );
        let delays: Vec<_> = clock
            .sleeps
            .lock()
            .unwrap()
            .iter()
            .map(|delay| delay.as_millis() as u64)
            .collect();
        assert_eq!(json!(delays), case["delays"]);
    }
}

#[test]
fn caller_cancellation_releases_noncooperative_attempt_and_backoff() {
    for backoff in [false, true] {
        let clock = Arc::new(TestClock {
            blocked: true,
            ..Default::default()
        });
        let runtime = runtime(clock.clone());
        let transport = transport();
        let drops = Arc::new(AtomicUsize::new(0));
        let calls = AtomicUsize::new(0);
        let mut future = retry_google_request(
            &transport,
            &runtime,
            GoogleRetryOptions {
                max_retries: Some(safe(3)),
                ..Default::default()
            },
            || {
                calls.fetch_add(1, Ordering::SeqCst);
                let witness = DropWitness(drops.clone());
                Box::pin(async move {
                    let _witness = witness;
                    if !backoff {
                        pending::<()>().await;
                    }
                    Err::<(), _>(GoogleRequestFailure::ApiError {
                        status: Some(429),
                        error: OpaqueFailure(Box::new(77)),
                    })
                })
            },
        );
        assert!(poll(future.as_mut()).is_pending());
        runtime.cancellation.cancel();
        match ready(future) {
            Err(ProviderRetryError::Cancelled { previous }) => {
                assert_eq!(previous.is_some(), backoff)
            }
            _ => panic!("caller cancellation must win"),
        }
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(clock.drops.load(Ordering::SeqCst), usize::from(backoff));
    }
    let runtime = runtime(Arc::new(TestClock::default()));
    runtime.cancellation.cancel();
    let transport = transport();
    assert!(matches!(
        ready(retry_google_request::<(), (), _>(
            &transport,
            &runtime,
            GoogleRetryOptions::default(),
            || { panic!("pre-cancelled caller must not invoke attempt") }
        )),
        Err(ProviderRetryError::Cancelled { previous: None })
    ));
}

#[test]
fn transient_projections_and_opaque_failures_redact_debug() {
    let part = GooglePart {
        text: Some("secret-marker".into()),
        thought_signature: Some("secret-marker".into()),
        ..Default::default()
    };
    assert!(!format!("{part:?}").contains("secret-marker"));
    let failure = GoogleRequestFailure::ApiError {
        status: Some(429),
        error: "secret-marker",
    };
    assert!(!format!("{failure:?}").contains("secret-marker"));
    let tool = tools(
        &json!([{"name":"secret-marker","description":"secret-marker","parameters":{"type":"object"}}]),
    );
    assert!(!format!("{:?}", convert_tools(&tool, false, true).unwrap()).contains("secret-marker"));
    // Shared wire stringification, not durable number spelling, owns SDK JSON bytes.
    assert_eq!(
        stringify_wire_json(&json!({"2":1.0,"10":2.0,"x":-0.0})).unwrap(),
        "{\"2\":1,\"10\":2,\"x\":0}"
    );
    assert_eq!(map_stop_reason_string("unknown"), StopReason::Error);
}
