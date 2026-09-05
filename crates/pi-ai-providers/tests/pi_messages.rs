#![cfg(feature = "wire-pi-messages")]

use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

use pi_ai_core::{
    ApiAdapter, AssistantMessage, AssistantMessageEvent, BoxFuture, CancellationToken, Cancelled,
    Clock, JsonValue, ModelsError, ModelsErrorCode, ProviderEnv, ProviderHeaders, RequestEndpoint,
    RequestRuntime, ResolvedAuth, ResolvedRequest, SafeUnsigned, SecretString, SimpleStreamOptions,
    SimpleStreamOptionsWire, SimpleStreamRequest, SpawnError, StopReason, StreamOptions,
    StreamOptionsWire, TaskSpawner, TextStreamRequest, Usage, is_context_overflow,
    is_retryable_assistant_error,
};
use pi_ai_providers::api::pi_messages::{
    PiMessagesApi, PiMessagesEvent, PiMessagesOptions, PiMessagesResponseError,
};
use pi_ai_providers::api::runtime::{
    AdapterResponseHead, AdapterServices, PayloadMiddleware, ResponseMiddleware,
};
use pi_ai_transport::{
    HttpHeaders, HttpRequest, HttpResponse, HttpTransport, RawHttpClient, ResponseBody,
    RetryJitter, TransportError,
};
use serde_json::{Value, json};

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/pi-messages/trace.json"
    ))
    .unwrap()
}

struct TestClock;
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        panic!("pi-messages must ignore timeouts/retry sleeps")
    }
}
struct NoJitter;
impl RetryJitter for NoJitter {
    fn sample(&self) -> f64 {
        panic!("pi-messages must not retry")
    }
}

#[derive(Default)]
struct Spawner {
    tasks: Mutex<Vec<BoxFuture<'static, ()>>>,
    reject: bool,
}
impl TaskSpawner for Spawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        if self.reject {
            return Err(SpawnError::default());
        }
        self.tasks.lock().unwrap().push(task);
        Ok(())
    }
}
impl Spawner {
    fn take(&self) -> BoxFuture<'static, ()> {
        self.tasks.lock().unwrap().pop().expect("one admitted task")
    }
}

#[derive(Default)]
struct Observed {
    requests: Mutex<Vec<(String, HttpHeaders, Value)>>,
    order: Mutex<Vec<&'static str>>,
    reads: AtomicUsize,
    closes: AtomicUsize,
    dropped_waits: AtomicUsize,
}

struct PendingRead {
    observed: Arc<Observed>,
}
impl Future for PendingRead {
    type Output = Result<Option<Vec<u8>>, TransportError>;
    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for PendingRead {
    fn drop(&mut self) {
        self.observed.dropped_waits.fetch_add(1, Ordering::SeqCst);
    }
}

struct Body {
    chunks: VecDeque<Vec<u8>>,
    block: bool,
    observed: Arc<Observed>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        self.observed.reads.fetch_add(1, Ordering::SeqCst);
        self.observed.order.lock().unwrap().push("body");
        if self.chunks.is_empty() && self.block {
            return Box::pin(PendingRead {
                observed: self.observed.clone(),
            });
        }
        let result = self.chunks.pop_front();
        Box::pin(async move { Ok(result) })
    }
    fn close(&mut self) {
        self.observed.closes.fetch_add(1, Ordering::SeqCst);
    }
}

struct Client {
    response: Mutex<Option<HttpResponse>>,
    observed: Arc<Observed>,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("send");
            self.observed.requests.lock().unwrap().push((
                request.endpoint.expose_for_transport().to_owned(),
                request.headers.clone(),
                serde_json::from_slice(&request.body).unwrap(),
            ));
            Ok(self
                .response
                .lock()
                .unwrap()
                .take()
                .expect("no HTTP retries"))
        })
    }
}

fn setup(
    status: u16,
    chunks: Vec<Vec<u8>>,
    block: bool,
) -> (
    PiMessagesApi,
    Arc<Spawner>,
    Arc<Observed>,
    TextStreamRequest,
) {
    setup_with_status_text(status, "", chunks, block)
}

fn setup_with_status_text(
    status: u16,
    status_text: &str,
    chunks: Vec<Vec<u8>>,
    block: bool,
) -> (
    PiMessagesApi,
    Arc<Spawner>,
    Arc<Observed>,
    TextStreamRequest,
) {
    let observed = Arc::new(Observed::default());
    let mut headers = HttpHeaders::new();
    headers
        .set("x-pi-gateway-upstream-provider", "anthropic")
        .unwrap();
    let client = Arc::new(Client {
        observed: observed.clone(),
        response: Mutex::new(Some(HttpResponse {
            status,
            status_text: status_text.to_owned(),
            headers,
            body: Box::new(Body {
                chunks: chunks.into(),
                block,
                observed: observed.clone(),
            }),
        })),
    });
    let services = AdapterServices::new(Arc::new(HttpTransport::new(client, Arc::new(NoJitter))));
    let spawner = Arc::new(Spawner::default());
    let f = fixture();
    let request = TextStreamRequest {
        resolved: ResolvedRequest {
            model: serde_json::from_value(f["model"].clone()).unwrap(),
            endpoint: RequestEndpoint::new_resolved("https://gateway.invalid/v1/").unwrap(),
        },
        context: serde_json::from_value(f["context"].clone()).unwrap(),
        auth: ResolvedAuth {
            api_key: Some(SecretString::new("test-key")),
            ..Default::default()
        },
        options: StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime: RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                Arc::new(TestClock),
                spawner.clone(),
            ),
        },
    };
    (PiMessagesApi::new(services), spawner, observed, request)
}

fn poll<F: Future + ?Sized>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut TaskContext::from_waker(Waker::noop()))
}
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match poll(future.as_mut()) {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("expected ready"),
    }
}
fn event_bytes(events: &[Value]) -> Vec<u8> {
    events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>()
        .into_bytes()
}
fn done() -> Value {
    json!({ "type": "done", "reason": "stop", "usage": fixture()["usage"] })
}

fn complete(
    api: &PiMessagesApi,
    spawner: &Spawner,
    request: TextStreamRequest,
    options: PiMessagesOptions,
) -> (AssistantMessage, Vec<AssistantMessageEvent>) {
    let mut stream = api.stream_with_options(request, options);
    if !spawner.tasks.lock().unwrap().is_empty() {
        ready(spawner.take());
    }
    let result = ready(stream.result());
    let mut events = Vec::new();
    while let Some(event) = ready(stream.next()) {
        events.push(event);
    }
    (result, events)
}

#[test]
fn source_derived_request_and_complete_event_snapshots_match_at_every_byte_split() {
    let f = fixture();
    let mut wire: Vec<Value> = f["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["wire"].clone())
        .collect();
    wire.push(json!({ "type":"done", "reason":"toolUse", "usage":f["usage"], "responseId":"resp_1", "rewrite":f["rewrite"] }));
    let bytes = event_bytes(&wire);
    for chunks in [
        vec![bytes.clone()],
        bytes.iter().map(|byte| vec![*byte]).collect(),
    ] {
        let (api, spawner, observed, mut request) = setup(200, chunks, false);
        request.options.wire.max_tokens = Some(SafeUnsigned::new(100).unwrap());
        request.options.wire.session_id = Some("session-1".to_owned());
        request.auth.headers = ProviderHeaders::new(BTreeMap::from([(
            "x-custom".to_owned(),
            Some("1".to_owned()),
        )]));
        let options = serde_json::from_value(json!({ "toolChoice":"auto" })).unwrap();
        let (result, events) = complete(&api, &spawner, request, options);
        assert_eq!(result.stop_reason, StopReason::ToolUse);
        assert_eq!(
            serde_json::to_value(&result.content).unwrap(),
            f["contents"]["tool-end"]
        );
        assert_eq!(serde_json::to_value(&result.usage).unwrap(), f["usage"]);
        let base = json!({ "role":"assistant", "api":"pi-messages", "provider":"radius", "model":"auto", "timestamp":7, "stopReason":"pending", "usage":f["zeroUsage"] });
        for (actual, expected) in events.iter().zip(f["events"].as_array().unwrap()) {
            let mut partial = base.clone();
            partial["content"] = f["contents"][expected["snapshot"].as_str().unwrap()].clone();
            let mut full_event = expected["expected"].clone();
            full_event["partial"] = partial;
            assert_eq!(serde_json::to_value(actual).unwrap(), full_event);
        }
        assert_eq!(events.len(), wire.len());
        let mut terminal = base.clone();
        terminal["content"] = f["contents"]["tool-end"].clone();
        terminal["usage"] = f["usage"].clone();
        terminal["stopReason"] = json!("toolUse");
        terminal["responseId"] = json!("resp_1");
        terminal["diagnostics"] =
            json!([{ "type":"pi_messages_rewrite", "timestamp":7, "details":f["rewrite"] }]);
        assert_eq!(
            serde_json::to_value(events.last().unwrap()).unwrap(),
            json!({ "type":"done", "reason":"toolUse", "message":terminal })
        );
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
        let requests = observed.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].0, "https://gateway.invalid/v1/messages");
        assert_eq!(
            requests[0].1.get_for_wire("authorization"),
            Some("Bearer test-key")
        );
        assert_eq!(requests[0].1.get_for_wire("x-custom"), Some("1"));
        assert_eq!(requests[0].2, f["request"]);
    }
}

#[test]
fn numeric_wire_review_regressions_accept_index_usage_and_rewrite_spellings() {
    let terminal = done().to_string();
    let usage = fixture()["usage"].to_string();
    let cases = [
        format!("data: {{\"type\":\"text_start\",\"contentIndex\":0.0}}\n\ndata: {terminal}\n\n"),
        format!(
            "data: {}\n\n",
            terminal.replace("\"input\":10", "\"input\":1e1")
        ),
        format!(
            "data: {{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{usage},\"rewrite\":{{\"policyId\":\"numeric\",\"policyVersion\":1.0,\"changed\":true,\"tokenCountChange\":-2e0,\"messageCountChange\":0.0,\"systemPromptChanged\":false}}}}\n\n"
        ),
    ];
    let mut failed = Vec::new();
    for (index, bytes) in cases.into_iter().enumerate() {
        let (api, spawner, _, request) = setup(200, vec![bytes.into_bytes()], false);
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        if result.stop_reason != StopReason::Stop {
            failed.push(index);
        }
    }
    assert!(
        failed.is_empty(),
        "wire numeric regression cases failed: {failed:?}"
    );
}

fn numeric_cases() -> Vec<Value> {
    serde_json::from_str::<Value>(include_str!(
        "../../../conformance/provider-traces/pi-messages/wire-numbers.json"
    ))
    .unwrap()["cases"]
        .as_array()
        .unwrap()
        .clone()
}

fn raw_count(mut object: Value, field: &str, raw: &str) -> String {
    object[field] = json!("__raw_wire_number__");
    object
        .to_string()
        .replacen("\"__raw_wire_number__\"", raw, 1)
}

#[test]
fn all_usage_count_fields_project_raw_js_numbers_on_done_and_error() {
    for terminal in ["done", "error"] {
        for field in [
            "input",
            "output",
            "cacheRead",
            "cacheWrite",
            "cacheWrite1h",
            "reasoning",
            "totalTokens",
        ] {
            for case in numeric_cases() {
                let usage = raw_count(
                    fixture()["usage"].clone(),
                    field,
                    case["raw"].as_str().unwrap(),
                );
                let (reason, detail) = if terminal == "done" {
                    ("stop", String::new())
                } else {
                    ("error", ",\"errorMessage\":\"upstream failure\"".to_owned())
                };
                let bytes = format!(
                    "data: {{\"type\":\"{terminal}\",\"reason\":\"{reason}\",\"usage\":{usage},\"responseId\":\"numeric-terminal\"{detail}}}\n\n"
                );
                let (api, spawner, _, request) = setup(200, vec![bytes.into_bytes()], false);
                let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
                if case["unsignedSafe"] == true {
                    assert_eq!(
                        result.response_id.as_ref().map(|id| id.as_str()),
                        Some("numeric-terminal"),
                        "{terminal} {field} {case}"
                    );
                    assert_eq!(
                        serde_json::to_value(result.usage).unwrap()[field].as_u64(),
                        Some(case["numberText"].as_str().unwrap().parse::<u64>().unwrap()),
                        "{terminal} {field} {case}"
                    );
                } else {
                    assert_eq!(
                        result.stop_reason,
                        StopReason::Error,
                        "{terminal} {field} {case}"
                    );
                    assert!(result.response_id.is_none(), "{terminal} {field} {case}");
                    assert_eq!(
                        serde_json::to_value(result.diagnostics).unwrap()[0]["details"]["kind"],
                        "protocol",
                        "{terminal} {field} {case}"
                    );
                }
            }
        }
    }
}

#[test]
fn all_rewrite_count_fields_project_signed_or_unsigned_raw_js_numbers() {
    for terminal in ["done", "error"] {
        for field in ["policyVersion", "tokenCountChange", "messageCountChange"] {
            for case in numeric_cases() {
                let rewrite = raw_count(
                    fixture()["rewrite"].clone(),
                    field,
                    case["raw"].as_str().unwrap(),
                );
                let usage = fixture()["usage"].to_string();
                let (reason, detail) = if terminal == "done" {
                    ("stop", String::new())
                } else {
                    ("error", ",\"errorMessage\":\"upstream failure\"".to_owned())
                };
                let bytes = format!(
                    "data: {{\"type\":\"{terminal}\",\"reason\":\"{reason}\",\"usage\":{usage},\"rewrite\":{rewrite},\"responseId\":\"numeric-terminal\"{detail}}}\n\n"
                );
                let (api, spawner, _, request) = setup(200, vec![bytes.into_bytes()], false);
                let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
                let safe = case[if field == "policyVersion" {
                    "unsignedSafe"
                } else {
                    "signedSafe"
                }]
                .as_bool()
                .unwrap();
                if safe {
                    assert_eq!(
                        result.response_id.as_ref().map(|id| id.as_str()),
                        Some("numeric-terminal"),
                        "{terminal} {field} {case}"
                    );
                    assert_eq!(
                        serde_json::to_value(result.diagnostics).unwrap()[0]["details"][field]
                            .as_i64(),
                        Some(case["numberText"].as_str().unwrap().parse::<i64>().unwrap()),
                        "{terminal} {field} {case}"
                    );
                } else {
                    assert_eq!(
                        result.stop_reason,
                        StopReason::Error,
                        "{terminal} {field} {case}"
                    );
                    assert!(result.response_id.is_none(), "{terminal} {field} {case}");
                    assert_eq!(
                        serde_json::to_value(result.diagnostics).unwrap()[0]["details"]["kind"],
                        "protocol",
                        "{terminal} {field} {case}"
                    );
                }
            }
        }
    }
}

#[test]
fn every_index_event_projects_numeric_spellings_before_reducer_limits() {
    let templates = [
        json!({"type":"text_start"}),
        json!({"type":"text_delta","delta":"x"}),
        json!({"type":"text_end","content":"x"}),
        json!({"type":"thinking_start"}),
        json!({"type":"thinking_delta","delta":"x"}),
        json!({"type":"thinking_end","content":"x"}),
        json!({"type":"toolcall_start","id":"call","toolName":"read"}),
        json!({"type":"toolcall_delta","delta":"{}"}),
        json!({"type":"toolcall_end","toolCall":{"type":"toolCall","id":"call","name":"read","arguments":{}}}),
    ];
    for template in templates {
        for case in numeric_cases() {
            let event = raw_count(
                template.clone(),
                "contentIndex",
                case["raw"].as_str().unwrap(),
            );
            let safe = case["unsignedSafe"].as_bool().unwrap();
            let index = if safe {
                case["numberText"].as_str().unwrap().parse::<u64>().unwrap()
            } else {
                0
            };
            if safe {
                let decoded: PiMessagesEvent = serde_json::from_str(&event).unwrap();
                assert_eq!(
                    serde_json::to_value(decoded).unwrap()["contentIndex"].as_u64(),
                    Some(index),
                    "{template} {case}"
                );
            } else {
                assert!(
                    serde_json::from_str::<PiMessagesEvent>(&event).is_err(),
                    "{template} {case}"
                );
            }
            let mut prefix = Vec::new();
            if safe && index < 32 {
                for position in 0..index {
                    prefix.push(json!({"type":"text_start","contentIndex":position}));
                }
                let kind = template["type"].as_str().unwrap();
                if !kind.ends_with("_start") {
                    let start = if kind.starts_with("text_") {
                        json!({"type":"text_start","contentIndex":index})
                    } else if kind.starts_with("thinking_") {
                        json!({"type":"thinking_start","contentIndex":index})
                    } else {
                        json!({"type":"toolcall_start","contentIndex":index,"id":"call","toolName":"read"})
                    };
                    prefix.push(start);
                }
            }
            let mut bytes = event_bytes(&prefix);
            bytes.extend_from_slice(format!("data: {event}\n\n").as_bytes());
            bytes.extend(event_bytes(&[done()]));
            let (mut api, spawner, _, request) = setup(200, vec![bytes], false);
            api.limits.max_content_blocks = 32;
            let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
            if safe && index < 32 {
                assert_eq!(result.stop_reason, StopReason::Stop, "{template} {case}");
            } else {
                assert_eq!(
                    serde_json::to_value(result.diagnostics).unwrap()[0]["details"]["kind"],
                    if safe { "limit" } else { "protocol" },
                    "{template} {case}"
                );
            }
        }
    }
}

#[test]
fn count_projection_keeps_nonnumber_missing_duplicate_unknown_and_null_rejections() {
    let usage = fixture()["usage"].to_string();
    let rewrite = fixture()["rewrite"].to_string();
    let mut invalid = Vec::new();
    for raw in ["null", "\"1\"", "true", "false", "[]", "{}"] {
        invalid.push(format!(
            "{{\"type\":\"text_start\",\"contentIndex\":{raw}}}"
        ));
        for field in [
            "input",
            "output",
            "cacheRead",
            "cacheWrite",
            "cacheWrite1h",
            "reasoning",
            "totalTokens",
        ] {
            let value = raw_count(fixture()["usage"].clone(), field, raw);
            invalid.push(format!(
                "{{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{value}}}"
            ));
        }
        for field in ["policyVersion", "tokenCountChange", "messageCountChange"] {
            let value = raw_count(fixture()["rewrite"].clone(), field, raw);
            invalid.push(format!(
                "{{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{usage},\"rewrite\":{value}}}"
            ));
        }
    }
    invalid.extend([
        "{\"type\":\"text_start\",\"contentIndex\":0.0,\"contentIndex\":0e0}".to_owned(),
        "{\"type\":\"text_start\",\"contentIndex\":0.0,\"unknown\":1}".to_owned(),
        "{\"type\":\"text_start\"}".to_owned(),
        format!(
            "{{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{}}}",
            usage.replacen("\"input\":10", "\"input\":1e1,\"input\":10.0", 1)
        ),
        format!(
            "{{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{}}}",
            usage.replacen('{', "{\"unknown\":1.0,", 1)
        ),
        format!(
            "{{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{}}}",
            usage.replacen("\"input\":10,", "", 1)
        ),
        format!(
            "{{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{}}}",
            usage.replacen("\"cost\":{", "\"cost\":{\"unknown\":1.0,", 1)
        ),
        format!(
            "{{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{usage},\"rewrite\":{}}}",
            rewrite.replacen(
                "\"policyVersion\":1",
                "\"policyVersion\":1.0,\"policyVersion\":1e0",
                1
            )
        ),
        format!(
            "{{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{usage},\"rewrite\":{}}}",
            rewrite.replacen('{', "{\"unknown\":0.0,", 1)
        ),
        format!("{{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{usage},\"rewrite\":null}}"),
    ]);
    for raw in invalid {
        let (api, spawner, _, request) =
            setup(200, vec![format!("data: {raw}\n\n").into_bytes()], false);
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(result.stop_reason, StopReason::Error, "{raw}");
        assert_eq!(
            serde_json::to_value(result.diagnostics).unwrap()[0]["details"]["kind"],
            "protocol",
            "{raw}"
        );
    }
}

#[test]
fn wire_count_projection_does_not_change_tool_arguments_or_cost_values() {
    let mut bytes = event_bytes(&[
        json!({"type":"toolcall_start","contentIndex":0,"id":"call","toolName":"read"}),
    ]);
    bytes.extend_from_slice(br#"data: {"type":"toolcall_end","contentIndex":0.0,"toolCall":{"type":"toolCall","id":"call","name":"read","arguments":{"n":2.0,"fraction":0.5,"input":-1.5,"cacheRead":"1e1","policyVersion":null,"nested":{"messageCountChange":0.5}}}}

"#);
    let mut usage = fixture()["usage"].clone();
    usage["cost"]["input"] = json!(-1.5);
    let usage = raw_count(usage, "totalTokens", "1e1");
    bytes.extend_from_slice(
        format!("data: {{\"type\":\"done\",\"reason\":\"toolUse\",\"usage\":{usage}}}\n\n")
            .as_bytes(),
    );
    let (api, spawner, _, request) = setup(200, vec![bytes], false);
    let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
    assert_eq!(result.stop_reason, StopReason::ToolUse);
    let pi_ai_core::AssistantContent::ToolCall(call) = &result.content[0] else {
        panic!("tool call expected")
    };
    assert!(
        matches!(call.arguments.as_map().get("n"),Some(JsonValue::Number(pi_ai_core::JsonNumber::Float(number))) if number.get()==2.0)
    );
    assert_eq!(
        serde_json::to_value(&call.arguments).unwrap(),
        json!({"n":2.0,"fraction":0.5,"input":-1.5,"cacheRead":"1e1","policyVersion":null,"nested":{"messageCountChange":0.5}})
    );
    assert_eq!(result.usage.cost.input.get(), -1.5);
    assert_eq!(result.usage.total_tokens.get(), 10);
}

#[test]
fn core_durable_integer_decoding_remains_strict_and_optional_counts_stay_omitted() {
    for raw in ["0.0", "1e1", "-0"] {
        assert!(serde_json::from_str::<SafeUnsigned>(raw).is_err());
    }
    for raw in ["0.0", "-2e0", "-0"] {
        assert!(serde_json::from_str::<pi_ai_core::SafeSigned>(raw).is_err());
    }
    let usage = raw_count(fixture()["usage"].clone(), "input", "1e1");
    assert!(serde_json::from_str::<Usage>(&usage).is_err());
    let (api, spawner, _, request) = setup(
        200,
        vec![
            format!("data: {{\"type\":\"done\",\"reason\":\"stop\",\"usage\":{usage}}}\n\n")
                .into_bytes(),
        ],
        false,
    );
    let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
    assert_eq!(result.stop_reason, StopReason::Stop);
    assert_eq!(result.usage.input.get(), 10);
    assert!(result.usage.cache_write_1h.is_none());
    assert!(result.usage.reasoning.is_none());
}

#[test]
fn terminal_short_circuits_hostile_tail_and_framing_accepts_trailing_and_first_data() {
    let terminal = done();
    for (bytes, block) in [
        (
            format!("data: {terminal}\n\ndata: {{bad-json\n\n").into_bytes(),
            true,
        ),
        (
            format!("data: {terminal}\n\ndata: {}\n\n", "x".repeat(300_000)).into_bytes(),
            true,
        ),
        (
            format!("\u{feff}:comment\r\ndata: [DONE]\r\n\r\ndata: {terminal}").into_bytes(),
            false,
        ),
        (
            format!("data: {terminal}\ndata: ignored-malformed\n\n").into_bytes(),
            true,
        ),
    ] {
        let (api, spawner, observed, request) = setup(200, vec![bytes], block);
        let (result, events) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(result.stop_reason, StopReason::Stop);
        assert_eq!(events.len(), 1);
        assert_eq!(
            observed.reads.load(Ordering::SeqCst),
            if block { 1 } else { 2 }
        );
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn headers_filter_null_cache_env_obeys_truthiness_and_ignored_options_stay_ignored() {
    for (scoped, ambient, explicit, expected) in [
        ("", "long", None, Some("long")),
        ("short", "long", None, None),
        ("long", "", Some("none"), Some("none")),
        ("", "", None, None),
    ] {
        let (mut api, spawner, observed, mut request) =
            setup(200, vec![event_bytes(&[done()])], false);
        api.services.env = ProviderEnv::new(BTreeMap::from([(
            "PI_CACHE_RETENTION".to_owned(),
            ambient.to_owned(),
        )]));
        request.auth.env = ProviderEnv::new(BTreeMap::from([(
            "PI_CACHE_RETENTION".to_owned(),
            scoped.to_owned(),
        )]));
        request.options.wire.cache_retention =
            explicit.map(|v| serde_json::from_value(json!(v)).unwrap());
        request.options.wire.timeout_ms = Some(SafeUnsigned::new(1).unwrap());
        request.options.wire.max_retries = Some(SafeUnsigned::new(100).unwrap());
        request.options.wire.sampling_params =
            Some(serde_json::from_value(json!({"injected":true})).unwrap());
        request.options.wire.metadata =
            Some(serde_json::from_value(json!({"secret":"hidden"})).unwrap());
        request.auth.headers = ProviderHeaders::new(BTreeMap::from([
            ("Authorization".to_owned(), None),
            ("x-null".to_owned(), None),
            ("ACCEPT".to_owned(), Some("custom".to_owned())),
        ]));
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(result.stop_reason, StopReason::Stop);
        let requests = observed.requests.lock().unwrap();
        assert_eq!(
            requests[0].1.get_for_wire("authorization"),
            Some("Bearer test-key")
        );
        assert_eq!(
            requests[0].1.get_for_wire("accept"),
            Some("text/event-stream, custom")
        );
        assert!(requests[0].1.get_for_wire("x-null").is_none());
        assert_eq!(
            requests[0].2["options"]
                .get("cacheRetention")
                .and_then(Value::as_str),
            expected
        );
        assert!(requests[0].2["options"].get("timeoutMs").is_none());
        assert!(requests[0].2["options"].get("samplingParams").is_none());
        assert!(requests[0].2["options"].get("metadata").is_none());
    }
}

#[test]
fn url_parsing_uses_resolved_endpoint_and_debug_replaces_existing_query() {
    let (api, spawner, observed, mut request) = setup(200, vec![event_bytes(&[done()])], false);
    request.resolved.endpoint = RequestEndpoint::new_resolved(
        "https://gateway.invalid/a/../v1?key=synthetic&debug=old#fragment",
    )
    .unwrap();
    complete(
        &api,
        &spawner,
        request,
        serde_json::from_value(json!({"debug":true})).unwrap(),
    );
    assert_eq!(
        observed.requests.lock().unwrap()[0].0,
        "https://gateway.invalid/v1?key=synthetic&debug=1#fragment/messages"
    );
}

#[test]
fn typed_options_preserve_named_required_false_and_reject_null() {
    for value in [
        json!({}),
        json!({"toolChoice":"required","debug":false,"reasoning":"high"}),
        json!({"toolChoice":{"type":"function","function":{"name":"read"}}}),
    ] {
        let options: PiMessagesOptions = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(options).unwrap(), value);
    }
    for value in [
        json!({"debug":null}),
        json!({"reasoning":null}),
        json!({"toolChoice":null}),
        json!({"toolChoice":"any"}),
    ] {
        assert!(serde_json::from_value::<PiMessagesOptions>(value).is_err());
    }
    assert!(serde_json::from_str::<PiMessagesEvent>(r#"{"type":"start","unknown":1}"#).is_err());
    assert!(
        serde_json::from_str::<PiMessagesEvent>(
            r#"{"type":"text_start","contentIndex":0,"contentIndex":1}"#
        )
        .is_err()
    );
}

#[test]
fn http_and_server_error_classification_survives_redaction() {
    for (status, message, overflow, retryable) in [
        (400, "prompt is too long", true, false),
        (503, "prompt is too long", true, true),
        (429, "rate limit exceeded", false, true),
        (500, "billing unavailable", false, false),
        (401, "Token expired", false, false),
    ] {
        let body = json!({"error":{"message":format!("{message}; body-secret https://private.invalid?key=credential"),"code":"private-code"}}).to_string().into_bytes();
        let (api, spawner, observed, request) = setup(status, vec![body], false);
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(
            (
                is_context_overflow(&result, None),
                is_retryable_assistant_error(&result)
            ),
            (overflow, retryable)
        );
        let encoded = serde_json::to_string(&result).unwrap();
        for marker in [
            "body-secret",
            "private.invalid",
            "credential",
            "private-code",
        ] {
            assert!(!encoded.contains(marker));
        }
        assert_eq!(
            result.diagnostics.as_ref().unwrap()[0]
                .diagnostic_type
                .as_str(),
            "pi_messages_response_failure"
        );
        assert_eq!(
            serde_json::to_value(&result.diagnostics).unwrap()[0]["details"]["status"],
            status
        );
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
    }
    for reason in ["error", "aborted"] {
        let events = vec![
            json!({"type":"text_start","contentIndex":0}),
            json!({"type":"text_delta","contentIndex":0,"delta":"partial"}),
            json!({"type":"error","reason":reason,"usage":fixture()["usage"],"errorMessage":"prompt is too long; server error; body-secret"}),
        ];
        let (api, spawner, _, request) = setup(200, vec![event_bytes(&events)], false);
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert!(!result.content.is_empty());
        assert_eq!(
            serde_json::to_value(&result.usage).unwrap(),
            fixture()["usage"]
        );
        assert_eq!(
            (
                is_context_overflow(&result, None),
                is_retryable_assistant_error(&result)
            ),
            if reason == "error" {
                (true, true)
            } else {
                (false, false)
            }
        );
        assert!(!result.error_message.unwrap().contains("body-secret"));
    }
}

#[test]
fn protocol_failures_discard_partial_output_and_missing_terminal_is_retryable() {
    for tail in [
        b"data: {bad}\n\n".to_vec(),
        Vec::new(),
        event_bytes(&[json!({"type":"text_delta","contentIndex":99,"delta":"bad"})]),
    ] {
        let mut bytes = event_bytes(&[
            json!({"type":"text_start","contentIndex":0}),
            json!({"type":"text_delta","contentIndex":0,"delta":"partial"}),
        ]);
        bytes.extend_from_slice(&tail);
        let (api, spawner, observed, request) = setup(200, vec![bytes], false);
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(result.stop_reason, StopReason::Error);
        assert!(result.content.is_empty());
        assert_eq!(result.usage, Usage::zero());
        assert_eq!(is_retryable_assistant_error(&result), tail.is_empty());
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn caller_cancellation_discards_partial_output_and_releases_blocked_read() {
    let bytes = event_bytes(&[
        json!({"type":"text_start","contentIndex":0}),
        json!({"type":"text_delta","contentIndex":0,"delta":"partial"}),
    ]);
    let (api, spawner, observed, request) = setup(200, vec![bytes], true);
    let token = request.options.runtime.cancellation.clone();
    let mut stream = api.stream(request);
    let mut task = spawner.take();
    assert!(poll(task.as_mut()).is_pending());
    token.cancel();
    let result = ready(stream.result());
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert!(result.content.is_empty());
    assert_eq!(result.usage, Usage::zero());
    assert!(poll(task.as_mut()).is_ready());
    assert_eq!(observed.dropped_waits.load(Ordering::SeqCst), 1);
    assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    let mut terminals = 0;
    while let Some(event) = ready(stream.next()) {
        if matches!(
            event,
            AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. }
        ) {
            terminals += 1;
        }
    }
    assert_eq!(terminals, 1);
}

struct Hooks {
    observed: Arc<Observed>,
    replacement: Option<JsonValue>,
    fail_payload: bool,
    fail_response: bool,
    block_payload: bool,
    block_response: bool,
}
impl PayloadMiddleware for Hooks {
    fn on_payload<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        payload: &'a mut JsonValue,
    ) -> BoxFuture<'a, Result<Option<JsonValue>, ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("payload");
            assert!(!request.options.runtime.cancellation.is_cancelled());
            assert_eq!(request.context.messages.len(), 1);
            if self.block_payload {
                pending::<()>().await;
            }
            if self.fail_payload {
                return Err(ModelsError::new(ModelsErrorCode::Provider));
            }
            *payload = serde_json::from_value(json!({"mutated":true})).unwrap();
            Ok(self.replacement.clone())
        })
    }
}
impl ResponseMiddleware for Hooks {
    fn on_response<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        response: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("response");
            assert!(!request.options.runtime.cancellation.is_cancelled());
            assert_eq!(
                response
                    .headers
                    .get_for_wire("x-pi-gateway-upstream-provider"),
                Some("anthropic")
            );
            if self.block_response {
                pending::<()>().await;
            }
            if self.fail_response {
                return Err(ModelsError::new(ModelsErrorCode::Provider));
            }
            Ok(())
        })
    }
}

#[test]
fn middleware_mutation_replacement_failure_and_observation_order_are_exact() {
    for (replacement, fail_payload, fail_response, status) in [
        (None, false, false, 200),
        (Some(JsonValue::Null), false, false, 200),
        (None, true, false, 200),
        (None, false, true, 200),
        (None, false, false, 401),
    ] {
        let (mut api, spawner, observed, request) =
            setup(status, vec![event_bytes(&[done()])], false);
        let hooks = Arc::new(Hooks {
            observed: observed.clone(),
            replacement: replacement.clone(),
            fail_payload,
            fail_response,
            block_payload: false,
            block_response: false,
        });
        api.services.payload = Some(hooks.clone());
        api.services.response = Some(hooks);
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        let order = observed.order.lock().unwrap();
        if fail_payload {
            assert_eq!(*order, vec!["payload"]);
            assert_eq!(result.stop_reason, StopReason::Error);
        } else {
            assert_eq!(&order[..3], &["payload", "send", "response"]);
            assert_eq!(
                observed.requests.lock().unwrap()[0].2,
                if replacement.is_some() {
                    Value::Null
                } else {
                    json!({"mutated":true})
                }
            );
            if fail_response {
                assert_eq!(order.len(), 3);
                assert_eq!(result.stop_reason, StopReason::Error);
            } else {
                assert_eq!(order[3], "body");
            }
        }
    }
}

#[test]
fn noncooperative_payload_and_response_hooks_are_cancelled_without_body_reads() {
    for block_response in [false, true] {
        let (mut api, spawner, observed, request) = setup(200, vec![event_bytes(&[done()])], false);
        let token = request.options.runtime.cancellation.clone();
        let hooks = Arc::new(Hooks {
            observed: observed.clone(),
            replacement: None,
            fail_payload: false,
            fail_response: false,
            block_payload: !block_response,
            block_response,
        });
        api.services.payload = Some(hooks.clone());
        api.services.response = Some(hooks);
        let stream = api.stream(request);
        let mut task = spawner.take();
        assert!(poll(task.as_mut()).is_pending());
        token.cancel();
        assert!(poll(task.as_mut()).is_ready());
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
        assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
        if block_response {
            assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
        }
    }
}

#[test]
fn setup_api_mismatch_and_spawn_failures_never_dispatch_http() {
    for failure in ["api", "auth", "spawn"] {
        let (api, spawner, observed, mut request) = setup(200, vec![event_bytes(&[done()])], false);
        match failure {
            "api" => request.resolved.model.api = "other-api".parse().unwrap(),
            "auth" => request.auth.api_key = None,
            "spawn" => {
                request.options.runtime.spawner = Arc::new(Spawner {
                    reject: true,
                    ..Default::default()
                })
            }
            _ => unreachable!(),
        }
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(result.stop_reason, StopReason::Error);
        assert!(observed.requests.lock().unwrap().is_empty());
        if failure == "api" {
            assert_eq!(
                serde_json::to_value(result.diagnostics).unwrap()[0]["details"]["kind"],
                "api_mismatch"
            );
        }
    }
}

#[test]
fn raw_defaults_and_simple_options_have_separate_reasoning_ownership() {
    let (mut api, spawner, observed, request) = setup(200, vec![event_bytes(&[done()])], false);
    api.defaults =
        serde_json::from_value(json!({"reasoning":"high","toolChoice":"required","debug":true}))
            .unwrap();
    let simple = SimpleStreamRequest {
        resolved: request.resolved,
        context: request.context,
        auth: request.auth,
        options: SimpleStreamOptions {
            wire: serde_json::from_value::<SimpleStreamOptionsWire>(
                json!({"toolChoice":"none","deferred":true}),
            )
            .unwrap(),
            runtime: request.options.runtime,
        },
    };
    let stream = api.stream_simple(simple);
    ready(spawner.take());
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Stop);
    let requests = observed.requests.lock().unwrap();
    assert_eq!(requests[0].0, "https://gateway.invalid/v1/messages?debug=1");
    assert_eq!(requests[0].2["options"], json!({"toolChoice":"none"}));
}

#[test]
fn redacted_response_error_retains_only_explicit_code_inspection() {
    let (api, spawner, _, request) = setup(200, vec![event_bytes(&[done()])], false);
    let (template, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
    let error = PiMessagesResponseError::from_response(
        401,
        "Unauthorized",
        br#"{"error":{"message":"secret-url-body","code":"secret-code"}}"#,
        &template,
    )
    .unwrap();
    assert_eq!(error.code_for_inspection(), Some("secret-code"));
    assert!(!format!("{error} {error:?}").contains("secret"));
    assert!(
        !serde_json::to_string(&error.diagnostic_details())
            .unwrap()
            .contains("secret")
    );
}

#[test]
fn actual_status_text_body_and_code_preserve_all_14_pinned_classifier_pairs() {
    let matrix: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/pi-messages/error-classification.json"
    ))
    .unwrap();
    assert_eq!(matrix["cases"].as_array().unwrap().len(), 14);
    for case in matrix["cases"].as_array().unwrap() {
        let status = u16::try_from(case["status"].as_u64().unwrap()).unwrap();
        let (api, spawner, _, request) = setup_with_status_text(
            status,
            case["statusText"].as_str().unwrap(),
            vec![case["body"].as_str().unwrap().as_bytes().to_vec()],
            false,
        );
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(
            is_context_overflow(&result, None),
            case["overflow"].as_bool().unwrap(),
            "{case}"
        );
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "{case}"
        );
        assert!(
            !serde_json::to_string(&result)
                .unwrap()
                .contains("billing secret")
        );
    }
    let (api, spawner, _, request) = setup_with_status_text(
        400,
        "prompt is too long; private-status-sentinel",
        vec![b"{}".to_vec()],
        false,
    );
    let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
    assert!(is_context_overflow(&result, None));
    assert!(
        !serde_json::to_string(&result)
            .unwrap()
            .contains("private-status-sentinel")
    );
}

#[test]
fn raw_reason_phrase_bytes_preserve_node_oracle_classification_after_redaction() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/pi-messages/status-text-decoding.json"
    ))
    .unwrap();
    for case in fixture["cases"].as_array().unwrap() {
        let bytes: Vec<u8> = serde_json::from_value(case["bytes"].clone()).unwrap();
        let raw: String = bytes.into_iter().map(char::from).collect();
        let (api, spawner, _, request) =
            setup_with_status_text(400, &raw, vec![b"{}".to_vec()], false);
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(
            is_context_overflow(&result, None),
            case["overflow"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        let rendered = serde_json::to_string(&result).unwrap();
        for raw_character in ['é', '�', '\u{feff}', '😀'] {
            assert!(!rendered.contains(raw_character));
        }
    }
    let (api, spawner, _, request) =
        setup_with_status_text(400, "server\u{100}error", vec![b"{}".to_vec()], false);
    let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
    assert_eq!(result.stop_reason, StopReason::Error);
    assert!(!is_retryable_assistant_error(&result));
    assert_eq!(
        serde_json::to_value(result.diagnostics).unwrap()[0]["details"]["kind"],
        "transport"
    );
}

#[test]
fn reducer_and_error_body_limits_are_exact_and_cleanup_is_not_truncation() {
    let terminal = done();
    for allowance in [terminal.to_string().len(), terminal.to_string().len() - 1] {
        let (mut api, spawner, observed, request) = setup(
            200,
            vec![event_bytes(std::slice::from_ref(&terminal))],
            false,
        );
        api.limits.max_stream_bytes = allowance;
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(
            result.stop_reason,
            if allowance == terminal.to_string().len() {
                StopReason::Stop
            } else {
                StopReason::Error
            }
        );
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
    let tool = [
        json!({"type":"toolcall_start","contentIndex":0,"id":"call","toolName":"read"}),
        json!({"type":"toolcall_delta","contentIndex":0,"delta":"{}"}),
        terminal,
    ];
    for allowance in [2, 1] {
        let (mut api, spawner, observed, request) = setup(200, vec![event_bytes(&tool)], false);
        api.limits.max_tool_input_bytes = allowance;
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(
            result.stop_reason,
            if allowance == 2 {
                StopReason::Stop
            } else {
                StopReason::Error
            }
        );
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
    for index in [0, 1, 9_007_199_254_740_991u64] {
        let (mut api, spawner, observed, request) = setup(
            200,
            vec![event_bytes(&[
                json!({"type":"text_start","contentIndex":index}),
                done(),
            ])],
            false,
        );
        api.limits.max_content_blocks = 1;
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(
            result.stop_reason,
            if index == 0 {
                StopReason::Stop
            } else {
                StopReason::Error
            }
        );
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
    for limit in [2, 1] {
        let (mut api, spawner, observed, request) = setup(401, vec![b"{}".to_vec()], false);
        api.services.max_error_body_bytes = limit;
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(
            result.diagnostics.unwrap()[0].diagnostic_type.as_str(),
            if limit == 2 {
                "pi_messages_response_failure"
            } else {
                "pi_messages_failure"
            }
        );
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn deferred_capabilities_are_absent_and_cancellation_error_is_typed() {
    let (api, _, observed, request) = setup(200, Vec::new(), false);
    let handle = serde_json::from_value(
        json!({"provider":"radius","api":"pi-messages","modelId":"auto","id":"deferred"}),
    )
    .unwrap();
    let fetch = pi_ai_core::DeferredFetchRequest {
        resolved: request.resolved,
        auth: request.auth,
        handle,
        options: pi_ai_core::DeferredFetchOptions {
            wire: Default::default(),
            runtime: request.options.runtime,
        },
    };
    assert!(matches!(
        api.fetch_deferred(fetch),
        Err(pi_ai_core::CapabilityError::Unsupported)
    ));
    let (_, _, _, request) = setup(200, Vec::new(), false);
    let cancel = pi_ai_core::DeferredCancelRequest {
        resolved: request.resolved,
        auth: request.auth,
        handle: serde_json::from_value(
            json!({"provider":"radius","api":"pi-messages","modelId":"auto","id":"deferred"}),
        )
        .unwrap(),
        options: pi_ai_core::DeferredCancelOptions {
            wire: Default::default(),
            runtime: request.options.runtime,
        },
    };
    let error = ready(api.cancel_deferred(cancel)).unwrap_err();
    assert_eq!(error.code(), ModelsErrorCode::Stream);
    assert!(
        std::error::Error::source(&error)
            .unwrap()
            .downcast_ref::<pi_ai_core::CapabilityError>()
            .is_some()
    );
    assert!(observed.requests.lock().unwrap().is_empty());
}

struct MutableClock(AtomicUsize);
impl Clock for MutableClock {
    fn now_unix_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst) as u64
    }
    fn sleep<'a>(
        &'a self,
        _duration: Duration,
        _cancellation: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        panic!("unexpected sleep")
    }
}

#[test]
fn partial_timestamp_precedes_dispatch_and_bodyless_status_is_not_early_eof() {
    let (api, spawner, _, mut request) = setup(200, vec![event_bytes(&[done()])], false);
    let clock = Arc::new(MutableClock(AtomicUsize::new(7)));
    request.options.runtime.clock = clock.clone();
    let stream = api.stream(request);
    clock.0.store(99, Ordering::SeqCst);
    ready(spawner.take());
    assert_eq!(
        ready(stream.result()).timestamp,
        SafeUnsigned::new(7).unwrap()
    );
    for status in [204, 205] {
        let (api, spawner, observed, request) = setup(status, Vec::new(), false);
        let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(result.stop_reason, StopReason::Error);
        assert!(!is_retryable_assistant_error(&result));
        assert_eq!(observed.reads.load(Ordering::SeqCst), 0);
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
}

#[test]
fn pre_cancel_and_missing_error_messages_still_settle_without_hanging() {
    let (api, spawner, observed, request) = setup(200, Vec::new(), true);
    request.options.runtime.cancellation.cancel();
    let (result, events) = complete(&api, &spawner, request, PiMessagesOptions::default());
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert_eq!(events.len(), 1);
    assert!(observed.requests.lock().unwrap().is_empty());
    for message in [None, Some("")] {
        let mut event = json!({"type":"error","reason":"error","usage":fixture()["usage"]});
        if let Some(message) = message {
            event["errorMessage"] = json!(message);
        }
        let (api, spawner, _, request) = setup(200, vec![event_bytes(&[event])], false);
        let (result, events) = complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(result.stop_reason, StopReason::Error);
        assert_eq!(events.len(), 1);
        assert!(result.error_message.is_some_and(|value| !value.is_empty()));
    }
}

#[test]
fn exact_header_key_replaces_but_case_variant_joins_with_fetch_whitespace_rules() {
    for (name, expected) in [
        ("authorization", "custom"),
        ("Authorization", "Bearer test-key, custom"),
    ] {
        let (api, spawner, observed, mut request) = setup(200, vec![event_bytes(&[done()])], false);
        request.auth.headers = ProviderHeaders::new(BTreeMap::from([(
            name.to_owned(),
            Some("\r\n \tcustom\t \r\n".to_owned()),
        )]));
        complete(&api, &spawner, request, PiMessagesOptions::default());
        assert_eq!(
            observed.requests.lock().unwrap()[0]
                .1
                .get_for_wire("authorization"),
            Some(expected)
        );
    }
}

#[test]
fn fetch_byte_string_header_validation_and_error_body_bom_are_preserved() {
    let (api, spawner, observed, mut request) = setup(200, vec![event_bytes(&[done()])], false);
    request.auth.headers = ProviderHeaders::new(BTreeMap::from([(
        "x-unrepresentable".to_owned(),
        Some("☃".to_owned()),
    )]));
    let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
    assert_eq!(result.stop_reason, StopReason::Error);
    assert!(observed.requests.lock().unwrap().is_empty());
    let body = "\u{feff}{\"error\":{\"message\":\"server error\",\"details\":\"billing secret\"}}"
        .as_bytes()
        .to_vec();
    let (api, spawner, _, request) = setup(503, vec![body], false);
    let (result, _) = complete(&api, &spawner, request, PiMessagesOptions::default());
    assert!(is_retryable_assistant_error(&result));
    assert!(
        !serde_json::to_string(&result)
            .unwrap()
            .contains("billing secret")
    );
}

#[cfg(feature = "native")]
mod native {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    use pi_ai_transport::{NativeHttpClient, NativeRetryJitter, TokioClock};

    #[test]
    fn actual_adapter_http_stream_and_cancellation_release_loopback_connections() {
        for cancel in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let mut wire = vec![
                json!({"type":"start"}),
                json!({"type":"text_start","contentIndex":0}),
                json!({"type":"text_delta","contentIndex":0,"delta":"Hello☃"}),
            ];
            if !cancel {
                wire.push(json!({"type":"text_end","contentIndex":0,"content":"Hello☃","contentSignature":"native-signature"}));
                wire.push(done());
            }
            let bytes = event_bytes(&wire);
            let (sent, received) = mpsc::channel();
            let server = thread::spawn(move || {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                socket
                    .set_write_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    socket.read_exact(&mut byte).unwrap();
                    head.push(byte[0]);
                    assert!(head.len() < 16_384);
                }
                assert!(
                    head.windows(b"x-byte: \xe9\r\n".len())
                        .any(|line| line == b"x-byte: \xe9\r\n")
                );
                assert!(
                    !head
                        .windows(b"x-byte: \xc3\xa9\r\n".len())
                        .any(|line| line == b"x-byte: \xc3\xa9\r\n")
                );
                let head: String = head.into_iter().map(char::from).collect();
                let length = head
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap();
                let mut body = vec![0; length];
                socket.read_exact(&mut body).unwrap();
                socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap();
                // Actual HTTP chunks split JSON and UTF-8 bytes. No EOF or final
                // chunk is sent: the adapter must close on terminal/cancellation.
                for chunk in bytes.chunks(3) {
                    write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                    socket.write_all(chunk).unwrap();
                    socket.write_all(b"\r\n").unwrap();
                    socket.flush().unwrap();
                }
                let mut byte = [0];
                let closed = match socket.read(&mut byte) {
                    Ok(0) => true,
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                        ) =>
                    {
                        true
                    }
                    _ => false, // A timeout is not evidence of connection release.
                };
                sent.send((
                    head,
                    serde_json::from_slice::<Value>(&body).unwrap(),
                    closed,
                ))
                .unwrap();
            });
            let engine = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let (_, spawner, _, mut request) = setup(200, Vec::new(), false);
            request.resolved.endpoint =
                RequestEndpoint::new_resolved(format!("http://{address}/v1/")).unwrap();
            request.options.runtime.clock = Arc::new(TokioClock::new(engine.handle().clone()));
            request.options.wire.max_tokens = Some(SafeUnsigned::new(100).unwrap());
            request.options.wire.session_id = Some("session-1".to_owned());
            request.options.wire.timeout_ms = Some(SafeUnsigned::new(1).unwrap());
            request.options.wire.max_retries = Some(SafeUnsigned::new(9).unwrap());
            request.auth.headers = ProviderHeaders::new(BTreeMap::from([(
                "x-byte".to_owned(),
                Some("é".to_owned()),
            )]));
            let token = request.options.runtime.cancellation.clone();
            let api = PiMessagesApi::new(AdapterServices::new(Arc::new(HttpTransport::new(
                Arc::new(NativeHttpClient::new(engine.handle().clone())),
                Arc::new(NativeRetryJitter),
            ))));
            let mut stream = api.stream_with_options(
                request,
                serde_json::from_value(json!({"toolChoice":"auto","debug":true})).unwrap(),
            );
            let task = spawner.take();
            let (result, events) = engine.block_on(async {
                tokio::time::timeout(Duration::from_secs(5), async {
                    let worker = tokio::spawn(task);
                    let mut events = Vec::new();
                    while let Some(event) = stream.next().await {
                        if cancel && matches!(event, AssistantMessageEvent::TextDelta { .. }) {
                            token.cancel();
                        }
                        events.push(event);
                    }
                    worker.await.unwrap();
                    (stream.result().await, events)
                })
                .await
                .expect("loopback adapter must settle")
            });
            let (head, body, closed) = received.recv_timeout(Duration::from_secs(6)).unwrap();
            server.join().unwrap();
            assert!(head.starts_with("POST /v1/messages?debug=1 HTTP/1.1\r\n"));
            let head = head.to_ascii_lowercase();
            assert!(head.contains("authorization: bearer test-key\r\n"));
            assert!(head.contains("accept: text/event-stream\r\n"));
            assert!(head.contains("content-type: application/json\r\n"));
            assert_eq!(body, fixture()["request"]);
            assert!(closed, "native adapter must release the response socket");
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(
                        event,
                        AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. }
                    ))
                    .count(),
                1
            );
            assert!(result.timestamp.get() > 0);
            if cancel {
                assert_eq!(result.stop_reason, StopReason::Aborted);
                assert!(result.content.is_empty());
                assert_eq!(result.usage, Usage::zero());
            } else {
                assert_eq!(result.stop_reason, StopReason::Stop);
                assert_eq!(
                    serde_json::to_value(&result.content).unwrap(),
                    json!([{"type":"text","text":"Hello☃","textSignature":"native-signature"}])
                );
                assert_eq!(
                    serde_json::to_value(&result.usage).unwrap(),
                    fixture()["usage"]
                );
            }
        }
    }
}
