#![cfg(feature = "wire-azure-openai-responses")]

use pi_ai_core::{
    ApiAdapter, AssistantMessageEvent, BoxFuture, CancellationToken, Cancelled, Clock, ModelsError,
    ProviderHeaders, RequestEndpoint, RequestRuntime, ResolvedAuth, ResolvedRequest, SafeUnsigned,
    SecretString, SimpleStreamOptions, SimpleStreamOptionsWire, SimpleStreamRequest, SpawnError,
    StopReason, StreamOptions, TaskSpawner, TextStreamRequest, is_context_overflow,
    is_retryable_assistant_error,
};
use pi_ai_providers::api::azure_openai_responses::*;
use pi_ai_providers::api::runtime::{
    AdapterResponseHead, AdapterServices, PayloadMiddleware, ResponseMiddleware,
};
use pi_ai_providers::api::wire_json::WirePayload;
use pi_ai_transport::{
    HttpHeaders, HttpRequest, HttpResponse, HttpTransport, RawHttpClient, ResponseBody,
    RetryJitter, TransportError,
};
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, pending};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::Duration;

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/azure-openai-responses/trace.json"
    ))
    .unwrap()
}
fn first() -> Value {
    fixture()["cases"][0].clone()
}

#[test]
fn azure_raw_domains_passive_factory_and_transient_url_custody() {
    for value in [
        json!({}),
        json!({"reasoningSummary":null}),
        json!({"azureBaseUrl":""}),
        json!({"azureDeploymentName":" "}),
        json!({"toolChoice":{"type":"allowed_tools","mode":"required","tools":[]}}),
    ] {
        let options: AzureOpenAIResponsesOptions = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(json!(options), value);
    }
    for value in [
        json!({"azureBaseUrl":null}),
        json!({"azureApiVersion":null}),
        json!({"azureResourceName":null}),
        json!({"azureDeploymentName":null}),
        json!({"reasoningEffort":null}),
        json!({"toolChoice":null}),
        json!({"serviceTier":"priority"}),
    ] {
        assert!(serde_json::from_value::<AzureOpenAIResponsesOptions>(value).is_err());
    }
    let (api, spawner, observed, _, request, _) = setup(&first(), 200, vec![], false);
    let factory = azure_openai_responses_api(api.services.clone());
    assert_eq!(factory.api_id().as_str(), "azure-openai-responses");
    assert!(spawner.tasks.lock().unwrap().is_empty());
    assert!(observed.requests.lock().unwrap().is_empty());
    let original = json!(request.resolved.model);
    let options = AzureOpenAIResponsesOptions {
        azure_base_url: Some("https://proxy.invalid/v1?credential=synthetic-custody-secret".into()),
        ..Default::default()
    };
    assert!(!format!("{options:?}").contains("synthetic-custody-secret"));
    let stream = factory.stream_with_options(request, options);
    ready(spawner.take());
    let result = ready(stream.result());
    assert_eq!(result.stop_reason, StopReason::Error);
    assert_eq!(observed.models.lock().unwrap()[0], original);
    assert!(
        !json!(result)
            .to_string()
            .contains("synthetic-custody-secret")
    );
    // Source preserves non-Azure query text, so the endpoint suffix is part of
    // the existing value before SDK default api-version is appended.
    assert_eq!(
        observed.requests.lock().unwrap()[0]["url"],
        "https://proxy.invalid/v1?credential=synthetic-custody-secret%2Fresponses&api-version=v1"
    );
}

#[test]
fn azure_defaults_and_simple_auth_failure_do_not_borrow_standard_semantics() {
    let case = first();
    let (mut api, spawner, observed, _, request, _) = setup(
        &case,
        200,
        vec![case["sse"].as_str().unwrap().as_bytes().to_vec()],
        false,
    );
    api.defaults.azure_deployment_name = Some("adapter-default".into());
    let stream = api.stream(request);
    ready(spawner.take());
    assert_eq!(ready(stream.result()).usage.cost.total.get(), 20.0);
    assert_eq!(
        observed.requests.lock().unwrap()[0]["body"]["model"],
        "adapter-default"
    );
    let (api, spawner, observed, _, mut request, _) = setup(&case, 200, vec![], false);
    request.auth.api_key = None;
    request.auth.headers = ProviderHeaders::new(BTreeMap::from([(
        "Authorization".into(),
        Some("Bearer header-only".into()),
    )]));
    let stream = api.stream_simple(SimpleStreamRequest {
        resolved: request.resolved,
        context: request.context,
        auth: request.auth,
        options: SimpleStreamOptions {
            wire: Default::default(),
            runtime: request.options.runtime,
        },
    });
    ready(spawner.take());
    assert_eq!(ready(stream.result()).stop_reason, StopReason::Error);
    assert!(observed.order.lock().unwrap().is_empty());
}

#[test]
fn azure_alone_preserves_shared_sdk_decode_stage_and_operand_classification() {
    // AzureOpenAI.responses is the same installed resource and _thenUnwrap.
    // HTTP200 decode failures have no APIError prefix in either source wrapper.
    let corpus: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/openai-responses/decode-stage.json"
    ))
    .unwrap();
    assert_eq!(corpus["cases"].as_array().unwrap().len(), 68);
    for case in corpus["cases"].as_array().unwrap() {
        let mut input = first();
        input["replacement"] = json!({"stream":false});
        input["options"] = json!({"maxRetries":3});
        input["contentType"] = case
            .get("contentType")
            .cloned()
            .unwrap_or(json!("application/json"));
        let (api, spawner, observed, _, request, options) = setup(
            &input,
            200,
            vec![case["body"].as_str().unwrap().as_bytes().to_vec()],
            false,
        );
        let mut stream = api.stream_with_options(request, options);
        ready(spawner.take());
        let result = ready(stream.result());
        let mut events = vec![];
        while let Some(e) = ready(stream.next()) {
            events.push(json!(e)["type"].clone());
        }
        assert_eq!(json!(events), case["events"], "{}", case["id"]);
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(
            is_context_overflow(&result, None),
            case["contextOverflow"].as_bool().unwrap(),
            "{}",
            case["id"]
        );
        let order: Vec<_> = observed
            .order
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|v| *v != "body")
            .collect();
        assert_eq!(json!(order), case["order"], "{}", case["id"]);
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    }
}
fn ready<F: Future>(future: F) -> F::Output {
    let mut future = Box::pin(future);
    match future
        .as_mut()
        .poll(&mut TaskContext::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("expected ready"),
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
        self.tasks.lock().unwrap().pop().unwrap()
    }
}
#[derive(Default)]
struct ClockProbe {
    sleeps: Mutex<Vec<Duration>>,
    timeout: bool,
}
impl Clock for ClockProbe {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        duration: Duration,
        _token: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        self.sleeps.lock().unwrap().push(duration);
        Box::pin(async move {
            if self.timeout || duration < Duration::from_millis(1000) {
                Ok(())
            } else {
                pending().await
            }
        })
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        0.0
    }
}
#[derive(Default)]
struct Observed {
    requests: Mutex<Vec<Value>>,
    order: Mutex<Vec<&'static str>>,
    closed: AtomicUsize,
    dropped: AtomicUsize,
    models: Mutex<Vec<Value>>,
    response_heads: Mutex<Vec<AdapterResponseHead>>,
}
struct PendingRead(Arc<Observed>);
impl Future for PendingRead {
    type Output = Result<Option<Vec<u8>>, TransportError>;
    fn poll(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for PendingRead {
    fn drop(&mut self) {
        self.0.dropped.fetch_add(1, Ordering::SeqCst);
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
        self.observed.order.lock().unwrap().push("body");
        if self.chunks.is_empty() && self.block {
            return Box::pin(PendingRead(self.observed.clone()));
        }
        let next = self.chunks.pop_front();
        Box::pin(async move { Ok(next) })
    }
    fn close(&mut self) {
        self.observed.closed.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    responses: Mutex<VecDeque<HttpResponse>>,
    observed: Arc<Observed>,
    block: bool,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("send");
            self.observed.requests.lock().unwrap().push(json!({"url":request.endpoint.expose_for_transport(),"headers":request.headers.iter_for_wire().collect::<BTreeMap<_,_>>(),"body":serde_json::from_slice::<Value>(&request.body).unwrap_or(Value::Null)}));
            if self.block {
                pending::<()>().await;
            }
            Ok(self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("one configured attempt"))
        })
    }
}
struct Middleware {
    observed: Arc<Observed>,
    replacement: Option<WirePayload>,
    block_payload: bool,
    block_response: bool,
}
impl PayloadMiddleware for Middleware {
    fn on_payload<'a>(
        &'a self,
        request: &'a TextStreamRequest,
        _payload: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("payload");
            self.observed
                .models
                .lock()
                .unwrap()
                .push(serde_json::to_value(&request.resolved.model).unwrap());
            if self.block_payload {
                pending::<()>().await;
            }
            Ok(self.replacement.clone())
        })
    }
}
impl ResponseMiddleware for Middleware {
    fn on_response<'a>(
        &'a self,
        _request: &'a TextStreamRequest,
        head: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("response");
            self.observed
                .response_heads
                .lock()
                .unwrap()
                .push(head.clone());
            if self.block_response {
                pending::<()>().await;
            }
            Ok(())
        })
    }
}
fn setup(
    case: &Value,
    status: u16,
    chunks: Vec<Vec<u8>>,
    block: bool,
) -> (
    AzureOpenAIResponsesApi,
    Arc<Spawner>,
    Arc<Observed>,
    Arc<ClockProbe>,
    TextStreamRequest,
    AzureOpenAIResponsesOptions,
) {
    let observed = Arc::new(Observed::default());
    let mut headers = HttpHeaders::new();
    headers
        .set(
            "content-type",
            case["contentType"].as_str().unwrap_or("text/event-stream"),
        )
        .unwrap();
    for (key, value) in case["responseHeaders"].as_object().into_iter().flatten() {
        headers.set(key, value.as_str().unwrap()).unwrap();
    }
    let client = Arc::new(Client {
        responses: Mutex::new(VecDeque::from([HttpResponse {
            status,
            status_text: String::new(),
            headers,
            body: Box::new(Body {
                chunks: chunks.into(),
                block,
                observed: observed.clone(),
            }),
        }])),
        observed: observed.clone(),
        block: false,
    });
    let middleware = Arc::new(Middleware {
        observed: observed.clone(),
        replacement: case.get("replacement").map(|v| WirePayload::new(v.clone())),
        block_payload: false,
        block_response: false,
    });
    let mut services = AdapterServices::new(Arc::new(
        HttpTransport::new(client, Arc::new(Jitter))
            .with_fetch_response_source(pi_ai_transport::FetchResponseSource::Preconstructed),
    ));
    services.env =
        serde_json::from_value(case.get("ambient").cloned().unwrap_or(json!({}))).unwrap();
    services.payload = Some(middleware.clone());
    services.response = Some(middleware);
    let spawner = Arc::new(Spawner::default());
    let clock = Arc::new(ClockProbe::default());
    let mut common = case["options"].clone();
    let mut family = json!({});
    for key in [
        "reasoningEffort",
        "reasoningSummary",
        "azureApiVersion",
        "azureResourceName",
        "azureBaseUrl",
        "azureDeploymentName",
        "toolChoice",
    ] {
        if let Some(value) = common.as_object_mut().unwrap().remove(key) {
            family[key] = value;
        }
    }
    let headers = common
        .as_object_mut()
        .unwrap()
        .remove("headers")
        .unwrap_or(json!({}));
    let env = common
        .as_object_mut()
        .unwrap()
        .remove("env")
        .unwrap_or(json!({}));
    common.as_object_mut().unwrap().remove("reasoning");
    let request = TextStreamRequest {
        resolved: ResolvedRequest {
            model: serde_json::from_value(case["model"].clone()).unwrap(),
            endpoint: RequestEndpoint::new_resolved(
                case["endpoint"].as_str().unwrap_or(
                    case["model"]["baseUrl"]
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .unwrap_or("https://unused.invalid"),
                ),
            )
            .unwrap(),
        },
        context: serde_json::from_value(case["context"].clone()).unwrap(),
        auth: ResolvedAuth {
            api_key: if case["noKey"] == true {
                None
            } else {
                Some(SecretString::new("synthetic-key"))
            },
            headers: serde_json::from_value(headers).unwrap(),
            env: serde_json::from_value(env).unwrap(),
        },
        options: StreamOptions {
            wire: serde_json::from_value(common).unwrap(),
            runtime: RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                clock.clone(),
                spawner.clone(),
            ),
        },
    };
    (
        AzureOpenAIResponsesApi::new(services),
        spawner,
        observed,
        clock,
        request,
        serde_json::from_value(family).unwrap(),
    )
}

#[test]
fn pinned_azure_sdk_full_requests_and_snapshots_match_75_cases() {
    let f = fixture();
    assert_eq!(f["cases"].as_array().unwrap().len(), 75);
    for case in f["cases"].as_array().unwrap() {
        let (api, spawner, observed, _, request, options) = setup(
            case,
            case["status"].as_u64().unwrap() as u16,
            case["sse"]
                .as_str()
                .unwrap()
                .as_bytes()
                .chunks(3)
                .map(<[u8]>::to_vec)
                .collect(),
            false,
        );
        let mut stream = if case["simple"] == true {
            api.stream_simple(SimpleStreamRequest {
                resolved: request.resolved,
                context: request.context,
                auth: request.auth,
                options: SimpleStreamOptions {
                    wire: SimpleStreamOptionsWire {
                        stream: request.options.wire,
                        reasoning: Some(
                            serde_json::from_value(case["options"]["reasoning"].clone()).unwrap(),
                        ),
                        tool_choice: Some(pi_ai_core::ToolChoice::None),
                        ..Default::default()
                    },
                    runtime: request.options.runtime,
                },
            })
        } else {
            api.stream_with_options(request, options)
        };
        ready(spawner.take());
        let result = ready(stream.result());
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "{} retry",
            case["id"]
        );
        assert_eq!(
            is_context_overflow(&result, None),
            case["contextOverflow"].as_bool().unwrap(),
            "{} overflow",
            case["id"]
        );
        let mut events = vec![];
        while let Some(e) = ready(stream.next()) {
            events.push(json!(e));
        }
        let normalize = |mut events: Value| {
            for event in events.as_array_mut().unwrap() {
                if event["type"] == "error" {
                    event["error"]["errorMessage"] = json!("normalized error");
                    event["error"]
                        .as_object_mut()
                        .unwrap()
                        .remove("diagnostics");
                }
            }
            serde_json::from_value::<Vec<AssistantMessageEvent>>(events).unwrap()
        };
        assert_eq!(
            normalize(json!(events)),
            normalize(case["events"].clone()),
            "{} snapshots",
            case["id"]
        );
        assert_eq!(
            json!(*observed.requests.lock().unwrap()),
            case["requests"],
            "{} request",
            case["id"]
        );
        let order: Vec<_> = observed
            .order
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|v| *v != "body")
            .collect();
        assert_eq!(json!(order), case["order"], "{} order", case["id"]);
        assert_eq!(
            observed.closed.load(Ordering::SeqCst),
            usize::from(!case["requests"].as_array().unwrap().is_empty()),
            "{} closes",
            case["id"]
        );
    }
}

#[test]
fn sdk_string_and_ordered_header_correction_matches_66_source_cases() {
    let mut f: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/azure-openai-responses/correction.json"
    ))
    .unwrap();
    let records: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/azure-openai-responses/correction-custom-record.json"
    ))
    .unwrap();
    assert_eq!(f["cases"].as_array().unwrap().len(), 53);
    assert_eq!(records["cases"].as_array().unwrap().len(), 13);
    f["cases"]
        .as_array_mut()
        .unwrap()
        .extend(records["cases"].as_array().unwrap().iter().cloned());
    for case in f["cases"].as_array().unwrap() {
        let (api, spawner, observed, _, request, options) = setup(
            case,
            case["status"].as_u64().unwrap() as u16,
            case["sse"]
                .as_str()
                .unwrap()
                .as_bytes()
                .chunks(3)
                .map(<[u8]>::to_vec)
                .collect(),
            false,
        );
        let mut stream = if case["simple"] == true {
            api.stream_simple(SimpleStreamRequest {
                resolved: request.resolved,
                context: request.context,
                auth: request.auth,
                options: SimpleStreamOptions {
                    wire: SimpleStreamOptionsWire {
                        stream: request.options.wire,
                        reasoning: Some(
                            serde_json::from_value(case["options"]["reasoning"].clone()).unwrap(),
                        ),
                        tool_choice: Some(pi_ai_core::ToolChoice::None),
                        ..Default::default()
                    },
                    runtime: request.options.runtime,
                },
            })
        } else {
            api.stream_with_options(request, options)
        };
        ready(spawner.take());
        let result = ready(stream.result());
        assert_eq!(
            is_retryable_assistant_error(&result),
            case["retryable"].as_bool().unwrap(),
            "{} retry",
            case["id"]
        );
        assert_eq!(
            is_context_overflow(&result, None),
            case["contextOverflow"].as_bool().unwrap(),
            "{} overflow",
            case["id"]
        );
        let mut events = vec![];
        while let Some(e) = ready(stream.next()) {
            events.push(json!(e));
        }
        let normalize = |mut events: Value| {
            for event in events.as_array_mut().unwrap() {
                if event["type"] == "error" {
                    event["error"]["errorMessage"] = json!("normalized error");
                    event["error"]
                        .as_object_mut()
                        .unwrap()
                        .remove("diagnostics");
                }
            }
            serde_json::from_value::<Vec<AssistantMessageEvent>>(events).unwrap()
        };
        assert_eq!(
            normalize(json!(events)),
            normalize(case["events"].clone()),
            "{} snapshots",
            case["id"]
        );
        assert_eq!(
            json!(*observed.requests.lock().unwrap()),
            case["requests"],
            "{} request",
            case["id"]
        );
        let order: Vec<_> = observed
            .order
            .lock()
            .unwrap()
            .iter()
            .copied()
            .filter(|v| *v != "body")
            .collect();
        assert_eq!(json!(order), case["order"], "{} order", case["id"]);
        assert_eq!(
            observed.closed.load(Ordering::SeqCst),
            usize::from(!case["requests"].as_array().unwrap().is_empty()),
            "{} closes",
            case["id"]
        );
    }
}

struct GatedErrorBody {
    bytes: Option<Vec<u8>>,
    eof: Arc<AtomicBool>,
    observed: Arc<Observed>,
}
impl ResponseBody for GatedErrorBody {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        self.observed.order.lock().unwrap().push("body");
        if let Some(bytes) = self.bytes.take() {
            return Box::pin(async move { Ok(Some(bytes)) });
        }
        Box::pin(std::future::poll_fn(|_| {
            if self.eof.load(Ordering::SeqCst) {
                Poll::Ready(Ok(None))
            } else {
                Poll::Pending
            }
        }))
    }
    fn close(&mut self) {
        self.observed.closed.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn error_body_eof_precedes_retry_and_cancellation_never_starts_successor() {
    for cancel in [false, true] {
        let case = first();
        let (mut api, spawner, observed, _, mut request, options) =
            setup(&case, 200, vec![], false);
        let eof = Arc::new(AtomicBool::new(false));
        let mut headers = HttpHeaders::new();
        headers.set("retry-after-ms", "0").unwrap();
        let first = HttpResponse {
            status: 429,
            status_text: String::new(),
            headers,
            body: Box::new(GatedErrorBody {
                bytes: Some(b"context length exceeded".to_vec()),
                eof: eof.clone(),
                observed: observed.clone(),
            }),
        };
        let second = HttpResponse {
            status: 200,
            status_text: String::new(),
            headers: HttpHeaders::new(),
            body: Box::new(Body {
                chunks: vec![case["sse"].as_str().unwrap().as_bytes().to_vec()].into(),
                block: false,
                observed: observed.clone(),
            }),
        };
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(Client {
                responses: Mutex::new(VecDeque::from([first, second])),
                observed: observed.clone(),
                block: false,
            }),
            Arc::new(Jitter),
        ));
        request.options.wire.max_retries = Some(SafeUnsigned::new(1).unwrap());
        let token = request.options.runtime.cancellation.clone();
        let stream = api.stream_with_options(request, options);
        let mut worker = spawner.take();
        assert!(
            worker
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(observed.requests.lock().unwrap().len(), 1);
        assert!(!observed.order.lock().unwrap().contains(&"response"));
        assert_eq!(observed.closed.load(Ordering::SeqCst), 0);
        if cancel {
            token.cancel();
        } else {
            eof.store(true, Ordering::SeqCst);
        }
        ready(worker);
        assert_eq!(
            ready(stream.result()).stop_reason,
            if cancel {
                StopReason::Aborted
            } else {
                StopReason::Stop
            }
        );
        assert_eq!(
            observed.requests.lock().unwrap().len(),
            if cancel { 1 } else { 2 }
        );
        assert_eq!(
            observed
                .order
                .lock()
                .unwrap()
                .iter()
                .filter(|v| **v == "payload")
                .count(),
            1
        );
        assert_eq!(
            observed.closed.load(Ordering::SeqCst),
            if cancel { 1 } else { 2 }
        );
    }
}

#[cfg(feature = "native")]
#[test]
fn native_loopback_request_fragmented_sse_and_abort_release_the_socket() {
    use pi_ai_transport::NativeHttpClient;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;
    for cancel in [false, true] {
        let case = first();
        let bytes = case["sse"].as_str().unwrap().as_bytes().to_vec();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
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
                assert!(head.len() < 16384);
            }
            let head = String::from_utf8(head).unwrap();
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
            socket.write_all(b"HTTP/1.1 200 Synthetic\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap();
            for chunk in bytes.chunks(3) {
                write!(socket, "{:x}\r\n", chunk.len()).unwrap();
                socket.write_all(chunk).unwrap();
                socket.write_all(b"\r\n").unwrap();
                socket.flush().unwrap();
            }
            // Standard SDK keeps consuming after response.completed; success
            // needs EOF, while cancellation must release a still-blocked body.
            if !cancel {
                socket.write_all(b"0\r\n\r\n").unwrap();
                socket.flush().unwrap();
            }
            let closed = match socket.read(&mut [0]) {
                Ok(0) => true,
                Err(error) => matches!(
                    error.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
                ),
                _ => false,
            };
            (
                head,
                serde_json::from_slice::<Value>(&body).unwrap(),
                closed,
            )
        });
        let engine = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (mut api, spawner, _, _, mut request, options) = setup(&case, 200, vec![], false);
        request.resolved.endpoint =
            RequestEndpoint::new_resolved(format!("http://{address}/v1/")).unwrap();
        let token = request.options.runtime.cancellation.clone();
        api.services.transport = Arc::new(HttpTransport::new(
            Arc::new(NativeHttpClient::new(engine.handle().clone())),
            Arc::new(Jitter),
        ));
        let mut stream = api.stream_with_options(request, options);
        let task = spawner.take();
        let result = engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let worker = tokio::spawn(task);
                while let Some(event) = stream.next().await {
                    if cancel && matches!(event, AssistantMessageEvent::TextEnd { .. }) {
                        token.cancel();
                    }
                }
                worker.await.unwrap();
                stream.result().await
            })
            .await
            .unwrap()
        });
        let (head, body, closed) = server.join().unwrap();
        assert!(
            closed,
            "socket release requires EOF/reset, not read timeout"
        );
        assert!(head.starts_with("POST /v1/responses?api-version=v1 HTTP/1.1\r\n"));
        let head = head.to_ascii_lowercase();
        assert!(head.contains("api-key: synthetic-key\r\n"));
        assert!(head.contains("accept: application/json\r\n"));
        assert!(head.contains("content-type: application/json\r\n"));
        assert_eq!(body, case["requests"][0]["body"]);
        assert_eq!(
            serde_json::to_value(&result).unwrap()["content"][0]["text"],
            "hello"
        );
        if cancel {
            assert_eq!(result.stop_reason, StopReason::Aborted);
        } else {
            assert_eq!(result.stop_reason, StopReason::Stop);
            assert_eq!(result.usage.input.get(), 8);
        }
    }
}
#[test]
fn cancellation_after_usage_only_update_preserves_snapshot_and_drops_pending_body() {
    let case = first();
    let (api, spawner, observed, _, request, options) = setup(
        &case,
        200,
        vec![case["sse"].as_str().unwrap().as_bytes().to_vec()],
        true,
    );
    let token = request.options.runtime.cancellation.clone();
    let mut stream = api.stream_with_options(request, options);
    let mut worker = spawner.take();
    assert!(
        worker
            .as_mut()
            .poll(&mut TaskContext::from_waker(Waker::noop()))
            .is_pending()
    );
    token.cancel();
    let result = ready(stream.result());
    assert_eq!(result.stop_reason, StopReason::Aborted);
    assert_eq!(result.usage.input.get(), 8);
    assert_eq!(
        serde_json::to_value(&result).unwrap()["content"][0]["text"],
        "hello"
    );
    ready(worker);
    assert_eq!(observed.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(observed.closed.load(Ordering::SeqCst), 1);
    let mut terminals = 0;
    while let Some(event) = ready(stream.next()) {
        if matches!(
            event,
            AssistantMessageEvent::Error { .. } | AssistantMessageEvent::Done { .. }
        ) {
            terminals += 1;
        }
    }
    assert_eq!(terminals, 1);
}

#[test]
fn middleware_cancellation_and_auth_preflight_never_send_or_publish_start_early() {
    for response in [false, true] {
        let (mut api, spawner, observed, _, request, options) = setup(&first(), 200, vec![], true);
        let middleware = Arc::new(Middleware {
            observed: observed.clone(),
            replacement: None,
            block_payload: !response,
            block_response: response,
        });
        api.services.payload = Some(middleware.clone());
        api.services.response = Some(middleware);
        let token = request.options.runtime.cancellation.clone();
        let mut stream = api.stream_with_options(request, options);
        let mut worker = spawner.take();
        assert!(
            worker
                .as_mut()
                .poll(&mut TaskContext::from_waker(Waker::noop()))
                .is_pending()
        );
        token.cancel();
        ready(worker);
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Aborted);
        assert!(matches!(
            ready(stream.next()),
            Some(AssistantMessageEvent::Error { .. })
        ));
        assert_eq!(
            observed.requests.lock().unwrap().len(),
            usize::from(response)
        );
        assert_eq!(
            observed.closed.load(Ordering::SeqCst),
            usize::from(response)
        );
    }
    for value in [None, Some(""), Some(" \u{feff}\t")] {
        let (api, spawner, observed, _, mut request, options) = setup(&first(), 200, vec![], false);
        request.auth.api_key = None;
        request.auth.headers = ProviderHeaders::new(BTreeMap::from([(
            "Authorization".to_owned(),
            value.map(str::to_owned),
        )]));
        let stream = api.stream_with_options(request, options);
        ready(spawner.take());
        assert_eq!(ready(stream.result()).stop_reason, StopReason::Error);
        assert!(observed.order.lock().unwrap().is_empty());
    }
}
