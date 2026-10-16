#![cfg(feature = "wire-google-genai")]

mod retained_support;
use pi_ai_core::*;
use pi_ai_providers::api::{google_genai::GoogleGenAiApi, runtime::*, wire_json::*};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

#[derive(Default)]
struct Spawner(Mutex<Vec<BoxFuture<'static, ()>>>);
impl TaskSpawner for Spawner {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.lock().unwrap().push(task);
        Ok(())
    }
}
struct ClockPort;
impl Clock for ClockPort {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        panic!("no retry expected")
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        panic!("no retry expected")
    }
}
fn ready<F: Future>(future: F) -> F::Output {
    match Box::pin(future)
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("unexpected pending"),
    }
}
#[derive(Default)]
struct Observed {
    order: Mutex<Vec<&'static str>>,
    keys: Mutex<Vec<String>>,
    config_keys: Mutex<Vec<String>>,
    requests: Mutex<Vec<Value>>,
    bodies: Mutex<Vec<Vec<u8>>>,
    closes: AtomicUsize,
}
struct Body {
    data: Option<Vec<u8>>,
    observed: Arc<Observed>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move { Ok(self.data.take()) })
    }
    fn close(&mut self) {
        self.observed.closes.fetch_add(1, Ordering::SeqCst);
    }
}
struct Client {
    data: Vec<u8>,
    observed: Arc<Observed>,
    google: bool,
}
impl RawHttpClient for Client {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("send");
            self.observed
                .bodies
                .lock()
                .unwrap()
                .push(request.body.clone());
            if self.google {
                let headers: BTreeMap<_, _> = request
                    .headers
                    .iter_for_wire()
                    .map(|(key, _)| (key, request.headers.get_for_fetch(key).unwrap()))
                    .collect();
                self.observed.requests.lock().unwrap().push(json!({"url":request.endpoint.expose_for_transport(),"method":request.method,"headers":headers,"body":serde_json::from_slice::<Value>(&request.body).unwrap()}));
            }
            Ok(HttpResponse {
                status: 200,
                status_text: String::new(),
                headers: HttpHeaders::new(),
                body: Box::new(Body {
                    data: Some(self.data.clone()),
                    observed: self.observed.clone(),
                }),
            })
        })
    }
}
struct Hook {
    case: Value,
    replacement: Option<Value>,
    observed: Arc<Observed>,
}
impl PayloadMiddleware for Hook {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        payload: &'a mut WirePayload,
    ) -> BoxFuture<'a, Result<Option<WirePayload>, ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("payload");
            *self.observed.keys.lock().unwrap() = payload
                .as_value()
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect();
            if let Some(config) = payload.as_value().get("config") {
                *self.observed.config_keys.lock().unwrap() =
                    config.as_object().unwrap().keys().cloned().collect();
            }
            if self.case["shrink"] == true {
                return Ok(Some(WirePayload::new(json!({"small":true}))));
            }
            if self.case["mutate"] == true {
                *payload.as_value_mut() = self.replacement.clone().unwrap();
                return Ok(None);
            }
            if let Some(headers) = self.case.get("mutation") {
                payload.as_value_mut()["config"]["httpOptions"] = json!({"headers":headers});
            }
            Ok(self.replacement.clone().map(WirePayload::new))
        })
    }
}
impl ResponseMiddleware for Hook {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            self.observed.order.lock().unwrap().push("response");
            Ok(())
        })
    }
}
fn execute(
    case: &Value,
    model: Value,
    context: Value,
    replacement: Option<Value>,
    data: Vec<u8>,
    google: bool,
) -> (Arc<Observed>, AssistantMessage, Value) {
    let observed = Arc::new(Observed::default());
    let spawner = Arc::new(Spawner::default());
    // Source observes SDK/custom-fetch arguments and supplies constructed Response bodies.
    let transport = HttpTransport::new(
        Arc::new(Client {
            data,
            observed: observed.clone(),
            google,
        }),
        Arc::new(Jitter),
    )
    .with_fetch_response_source(FetchResponseSource::Preconstructed);
    let mut services = AdapterServices::new(Arc::new(transport));
    let hook = Arc::new(Hook {
        case: case.clone(),
        replacement,
        observed: observed.clone(),
    });
    services.payload = Some(hook.clone());
    services.response = Some(hook);
    let request = TextStreamRequest {
        resolved: ResolvedRequest {
            endpoint: RequestEndpoint::new_resolved(model["baseUrl"].as_str().unwrap()).unwrap(),
            model: serde_json::from_value(model).unwrap(),
        },
        context: serde_json::from_value(context).unwrap(),
        auth: ResolvedAuth {
            api_key: Some(SecretString::new("synthetic-key")),
            ..Default::default()
        },
        options: StreamOptions {
            wire: serde_json::from_value(case.get("options").cloned().unwrap_or(json!({})))
                .unwrap(),
            runtime: RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                Arc::new(ClockPort),
                spawner.clone(),
            ),
        },
    };
    assert!(google);
    let mut stream = GoogleGenAiApi::new(services).stream(request);
    ready(spawner.0.lock().unwrap().pop().unwrap());
    let result = retained_support::message(ready(stream.result()));
    let mut events = Vec::new();
    while let Some(event) = ready(stream.next()) {
        events.push(retained_support::without_duration(json!(event)));
    }
    (observed, result, json!(events))
}

#[test]
fn complete_google_sdk_ordered_replacement_mutation_and_noop_match_source() {
    let fixture: Value = retained_support::fixture(include_str!(
        "../../../conformance/provider-traces/wire-payload/google.json"
    ));
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 6);
    for case in fixture["cases"].as_array().unwrap() {
        let (observed, result, events) = execute(case, case["model"].clone(), case["context"].clone(), case.get("replacement").cloned(), b"data: {\"candidates\":[{\"finishReason\":\"STOP\",\"content\":{\"parts\":[{\"text\":\"done\"}]}}]}\n\n".to_vec(), true);
        assert_eq!(
            json!(*observed.requests.lock().unwrap()),
            case["requests"],
            "{} requests",
            case["id"]
        );
        assert_eq!(
            json!(*observed.keys.lock().unwrap()),
            case["logicalKeys"],
            "{} logical keys",
            case["id"]
        );
        assert_eq!(
            json!(*observed.config_keys.lock().unwrap()),
            case["configKeys"],
            "{} config keys",
            case["id"]
        );
        assert_eq!(json!(*observed.order.lock().unwrap()), case["order"]);
        assert_eq!(
            serde_json::from_value::<Vec<AssistantMessageEvent>>(events).unwrap(),
            serde_json::from_value::<Vec<AssistantMessageEvent>>(case["events"].clone()).unwrap()
        );
        assert_eq!(
            result,
            serde_json::from_value::<AssistantMessage>(case["result"].clone()).unwrap()
        );
        assert_eq!(observed.closes.load(Ordering::SeqCst), 1);
    }
}

fn nested(depth: usize) -> Value {
    (0..depth).fold(Value::Null, |value, _| Value::Array(vec![value]))
}
#[test]
fn carrier_is_ordered_redacted_unvalidated_and_existing_wire_limits_are_unchanged() {
    let value: Value =
        serde_json::from_str(r#"{"z":"secret payload","a":1,"10":10,"2":2}"#).unwrap();
    let mut payload = WirePayload::new(value.clone());
    assert_eq!(format!("{payload:?}"), "WirePayload([REDACTED])");
    assert_eq!(
        payload
            .clone()
            .into_value()
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        value.as_object().unwrap().keys().collect::<Vec<_>>()
    );
    payload.as_value_mut()["last"] = json!(true);
    assert_eq!(
        payload
            .as_value()
            .as_object()
            .unwrap()
            .keys()
            .next_back()
            .unwrap(),
        "last"
    );
    // Causal lossy control: durable core DTO ordering is intentionally unchanged.
    let core: JsonValue = serde_json::from_value(value.clone()).unwrap();
    let lossy = serde_json::to_value(core).unwrap();
    assert_ne!(
        stringify_wire_json(&value).unwrap(),
        stringify_wire_json(&lossy).unwrap()
    );
    assert!(stringify_wire_json(&nested(128)).is_ok());
    assert_eq!(
        stringify_wire_json(&nested(129)),
        Err(WireJsonError::DepthLimit)
    );
    assert_eq!(
        stringify_wire_json(&json!("x".repeat(1048574)))
            .unwrap()
            .len(),
        1048576
    );
    assert_eq!(
        stringify_wire_json(&json!("x".repeat(1048575))),
        Err(WireJsonError::ByteLimit)
    );
    for oversized in [nested(129), json!("x".repeat(1048577))] {
        let mut payload = WirePayload::new(oversized);
        assert!(stringify_wire_json(payload.as_value()).is_err());
        *payload.as_value_mut() = json!({"small":true});
        assert_eq!(
            stringify_wire_json(payload.as_value()).unwrap(),
            r#"{"small":true}"#
        );
    }
}
