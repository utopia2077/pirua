#![cfg(feature = "wire-bedrock-converse-stream")]
use pi_ai_core::*;
use pi_ai_providers::api::{
    bedrock_converse_stream::*,
    runtime::{AdapterResponseHead, AdapterServices, PayloadMiddleware, ResponseMiddleware},
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
struct ClockPort;
impl Clock for ClockPort {
    fn now_unix_ms(&self) -> u64 {
        1767323045000
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async { Ok(()) })
    }
}
struct Spawn(tokio::runtime::Handle);
impl TaskSpawner for Spawn {
    fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.spawn(task);
        Ok(())
    }
}
struct Entropy;
impl BedrockEntropy for Entropy {
    fn invocation_bytes(&self) -> Result<[u8; 16], ModelsError> {
        Ok([
            0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x41, 0x23, 0x81, 0x23, 0x12, 0x34, 0x56, 0x78,
            0x9a, 0xbc,
        ])
    }
    fn retry_jitter(&self) -> Result<f64, ModelsError> {
        Ok(0.5)
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        panic!("generic retry must stay unused")
    }
}
#[derive(Default)]
struct Observed {
    requests: Mutex<Vec<Value>>,
    payloads: Mutex<Vec<Value>>,
    heads: Mutex<Vec<Value>>,
    reads: AtomicUsize,
    closed: AtomicUsize,
    credentials: AtomicUsize,
}
struct Credentials(Arc<Observed>);
impl BedrockConfigLeaf<BedrockCredentials> for Credentials {
    fn resolve<'a>(
        &'a self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<BedrockCredentials, BedrockLeafError>> {
        Box::pin(async move {
            self.0.credentials.fetch_add(1, Ordering::SeqCst);
            Ok(BedrockCredentials {
                access_key_id: SecretString::new("AKID_SYNTHETIC"),
                secret_access_key: SecretString::new("synthetic-secret"),
                session_token: Some(SecretString::new("synthetic-session")),
                expiration_unix_ms: None,
            })
        })
    }
}
struct Config {
    row: Value,
    observed: Arc<Observed>,
}
impl BedrockConfigProvider for Config {
    fn create_session(&self, _: BedrockConfigKey<'_>) -> Result<BedrockSettings, ModelsError> {
        let cfg = &self.row["sdkConfig"];
        Ok(BedrockSettings {
            credentials: BedrockSetting::Leaf(Arc::new(Credentials(self.observed.clone()))),
            region: BedrockSetting::Value("us-east-1".into()),
            token: BedrockSetting::Value(BedrockToken {
                token: SecretString::new("synthetic-token"),
                expiration_unix_ms: None,
            }),
            max_attempts: BedrockSetting::Value(
                FiniteF64::new(cfg["maxAttempts"].as_f64().unwrap_or(1.0)).unwrap(),
            ),
            use_fips_endpoint: BedrockSetting::Value(
                cfg["useFipsEndpoint"].as_bool().unwrap_or(false),
            ),
            use_dualstack_endpoint: BedrockSetting::Value(
                cfg["useDualstackEndpoint"].as_bool().unwrap_or(false),
            ),
            user_agent_app_id: BedrockSetting::Value(Some("synthetic".into())),
            endpoint_override: BedrockSetting::Value(
                self.row["configuredEndpoint"]
                    .as_str()
                    .map(|s| Arc::new(RequestEndpoint::new_resolved(s).unwrap())),
            ),
            ..Default::default()
        })
    }
}
struct Body {
    chunks: VecDeque<Vec<u8>>,
    observed: Arc<Observed>,
    closed: bool,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            self.observed.reads.fetch_add(1, Ordering::SeqCst);
            Ok(self.chunks.pop_front())
        })
    }
    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.observed.closed.fetch_add(1, Ordering::SeqCst);
        }
    }
}
struct Raw {
    row: Value,
    next: Mutex<usize>,
    observed: Arc<Observed>,
}
impl RawHttpClient for Raw {
    fn send<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        panic!("Bedrock must select Smithy")
    }
    fn send_smithy<'a>(
        &'a self,
        request: &'a HttpRequest,
        _: &'a RequestRuntime,
        profile: &'a SmithyRequestProfile,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let index = {
                let mut n = self.next.lock().unwrap();
                let index = *n;
                *n += 1;
                index
            };
            let pseudo = match profile {
                SmithyRequestProfile::Http2 { pseudo_headers, .. } => {
                    json!({"authority":pseudo_headers.authority,"scheme":pseudo_headers.scheme})
                }
                _ => Value::Null,
            };
            self.observed.requests.lock().unwrap().push(json!({"endpoint":request.endpoint.expose_for_transport(),"body":String::from_utf8(request.body.clone()).unwrap(),"method":request.method,"headers":request.headers.iter_for_wire().collect::<Vec<_>>(),"pseudo":pseudo}));
            let head = &self.row["responses"][index];
            let mut headers = HttpHeaders::new();
            for (k, v) in head["headers"].as_object().unwrap() {
                if k != ":status" {
                    headers.append(k, v.as_str().unwrap()).unwrap();
                }
            }
            let chunks = self.row["responseBodies"][index]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| decode_hex(v.as_str().unwrap()))
                .collect();
            Ok(HttpResponse {
                status: head["status"].as_u64().unwrap() as u16,
                status_text: String::new(),
                headers,
                body: Box::new(Body {
                    chunks,
                    observed: self.observed.clone(),
                    closed: false,
                }),
            })
        })
    }
}
fn decode_hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn encode64(bytes: &[u8]) -> String {
    const B: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::new();
    for c in bytes.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        for shift in [18, 12] {
            out.push(B[((n >> shift) & 63) as usize] as char);
        }
        out.push(if c.len() > 1 {
            B[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            B[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}
fn observation(v: &BedrockValue) -> Value {
    match v {
        BedrockValue::Bytes(b) => json!({"$bytes":encode64(b)}),
        BedrockValue::Array(a) => Value::Array(a.iter().map(observation).collect()),
        BedrockValue::Object(o) => Value::Object(
            o.iter()
                .map(|(k, v)| (k.to_owned(), observation(v)))
                .collect(),
        ),
        _ => v.to_json(),
    }
}
struct Hook {
    row: Value,
    observed: Arc<Observed>,
}
impl PayloadMiddleware<BedrockPayload> for Hook {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        payload: &'a mut BedrockPayload,
    ) -> BoxFuture<'a, Result<Option<BedrockPayload>, ModelsError>> {
        Box::pin(async move {
            self.observed
                .payloads
                .lock()
                .unwrap()
                .push(observation(payload.as_value()));
            if let Some(mode) = self.row["blobMode"].as_str() {
                if mode == "json-clone" {
                    return Ok(Some(BedrockPayload::from_json(payload.to_json())));
                }
                let bytes = payload
                    .as_value_mut()
                    .get_mut("messages")
                    .unwrap()
                    .as_array_mut()
                    .unwrap()[0]
                    .get_mut("content")
                    .unwrap()
                    .as_array_mut()
                    .unwrap()[0]
                    .get_mut("image")
                    .unwrap()
                    .get_mut("source")
                    .unwrap()
                    .get_mut("bytes")
                    .unwrap();
                match mode {
                    "changed-bytes" => *bytes = BedrockValue::Bytes(vec![4, 5]),
                    "empty-bytes" => *bytes = BedrockValue::Bytes(vec![]),
                    "plain-array" => *bytes = BedrockValue::from_json(json!([1, 2, 3])),
                    "plain-object" => *bytes = BedrockValue::from_json(json!({"0":1,"1":2,"2":3})),
                    "string" => *bytes = BedrockValue::String("AQID".into()),
                    _ => {}
                }
            }
            Ok(self
                .row
                .get("replacement")
                .map(|v| BedrockPayload::from_json(v.clone())))
        })
    }
}
impl ResponseMiddleware for Hook {
    fn on_response<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        head: &'a AdapterResponseHead,
    ) -> BoxFuture<'a, Result<(), ModelsError>> {
        Box::pin(async move {
            let entries = head
                .headers
                .iter()
                .map(|(k, v)| {
                    (
                        k,
                        match v {
                            pi_ai_providers::api::runtime::ResponseHeaderValue::Text(s) => json!(s),
                            pi_ai_providers::api::runtime::ResponseHeaderValue::Number(n) => {
                                json!(n.get())
                            }
                        },
                    )
                })
                .collect::<Vec<_>>();
            self.observed.heads.lock().unwrap().push(json!({"status":head.status,"entries":entries,"reads":self.observed.reads.load(Ordering::SeqCst)}));
            Ok(())
        })
    }
}
fn normalize(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for v in map.values_mut() {
                normalize(v);
            }
        }
        Value::Array(array) => {
            for v in array {
                normalize(v)
            }
        }
        Value::Number(n) => *value = json!(n.as_f64().unwrap()),
        _ => {}
    }
}
fn normalize_errors(events: &mut Value) {
    for event in events.as_array_mut().unwrap() {
        for field in ["partial", "message", "error"] {
            if let Some(content) = event
                .get_mut(field)
                .and_then(|m| m.get_mut("content"))
                .and_then(Value::as_array_mut)
            {
                for block in content {
                    if let Some(map) = block.as_object_mut() {
                        map.remove("index");
                        map.remove("partialJson");
                        map.remove("redactedChunks");
                    }
                }
            }
        }
        if event["type"] == "error" {
            let message = &mut event["error"];
            let parsed: AssistantMessage = serde_json::from_value(message.clone()).unwrap();
            message["errorMessage"] = json!({"retryable":is_retryable_assistant_error(&parsed),"overflow":is_context_overflow(&parsed,None)});
        }
    }
    normalize(events);
}
#[test]
fn complete_pinned_wrapper_sdk_wire_blob_hooks_and_binary_events_match() {
    let source: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/bedrock-converse-stream/family.json"
    ))
    .unwrap();
    assert!(source["cases"].as_array().unwrap().len() >= 60);
    run_source_cases(&source);
}
fn run_source_cases(source: &Value) {
    for row in source["cases"].as_array().unwrap() {
        let engine = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let observed = Arc::new(Observed::default());
        let mut services = AdapterServices::<BedrockPayload>::new(Arc::new(HttpTransport::new(
            Arc::new(Raw {
                row: row.clone(),
                next: Mutex::new(0),
                observed: observed.clone(),
            }),
            Arc::new(Jitter),
        )));
        let hook = Arc::new(Hook {
            row: row.clone(),
            observed: observed.clone(),
        });
        services.payload = Some(hook.clone());
        services.response = Some(hook);
        let api = BedrockApi::new(
            services,
            Arc::new(Config {
                row: row.clone(),
                observed: observed.clone(),
            }),
            Arc::new(Entropy),
        );
        let options = &row["options"];
        let mut raw_options = options.clone();
        for key in [
            "env",
            "headers",
            "apiKey",
            "temperature",
            "maxTokens",
            "cacheRetention",
            "timeoutMs",
            "maxRetries",
            "maxRetryDelayMs",
        ] {
            raw_options.as_object_mut().unwrap().remove(key);
        }
        let family: BedrockOptions = serde_json::from_value(raw_options).unwrap();
        let mut neutral = serde_json::Map::new();
        for key in [
            "temperature",
            "maxTokens",
            "cacheRetention",
            "timeoutMs",
            "maxRetries",
            "maxRetryDelayMs",
        ] {
            if let Some(v) = options.get(key) {
                neutral.insert(key.into(), v.clone());
            }
        }
        let mut model = row["model"].clone();
        let endpoint = model["baseUrl"].as_str().unwrap().to_owned();
        model["baseUrl"] = json!("https://public.invalid");
        let request = TextStreamRequest {
            resolved: ResolvedRequest {
                model: serde_json::from_value(model).unwrap(),
                endpoint: RequestEndpoint::new_resolved(endpoint).unwrap(),
            },
            context: serde_json::from_value(row["context"].clone()).unwrap(),
            auth: ResolvedAuth {
                api_key: options["apiKey"].as_str().map(SecretString::new),
                headers: serde_json::from_value(
                    options.get("headers").cloned().unwrap_or(json!({})),
                )
                .unwrap(),
                env: serde_json::from_value(options.get("env").cloned().unwrap_or(json!({})))
                    .unwrap(),
            },
            options: StreamOptions {
                wire: serde_json::from_value(Value::Object(neutral)).unwrap(),
                runtime: RequestRuntime::with_noop_telemetry(
                    CancellationToken::new(),
                    Arc::new(ClockPort),
                    Arc::new(Spawn(engine.handle().clone())),
                ),
            },
        };
        let mut stream = if row["simple"] == true {
            let simple = SimpleStreamRequest {
                resolved: request.resolved,
                context: request.context,
                auth: request.auth,
                options: SimpleStreamOptions {
                    wire: SimpleStreamOptionsWire {
                        stream: request.options.wire,
                        reasoning: family.reasoning,
                        thinking_budgets: family.thinking_budgets,
                        ..Default::default()
                    },
                    runtime: request.options.runtime,
                },
            };
            api.stream_simple(simple)
        } else {
            api.stream_with_options(request, family)
        };
        let events = engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                let mut events = Vec::new();
                while let Some(event) = stream.next().await {
                    events.push(serde_json::to_value(event).unwrap());
                }
                events
            })
            .await
            .unwrap()
        });
        let mut payloads = json!(*observed.payloads.lock().unwrap());
        let mut expected = row["payloads"].clone();
        normalize(&mut payloads);
        normalize(&mut expected);
        assert_eq!(payloads, expected, "{} logical payload", row["id"]);
        let requests = observed.requests.lock().unwrap();
        assert_eq!(
            requests.len(),
            row["wire"].as_array().unwrap().len(),
            "{} attempts",
            row["id"]
        );
        for (actual, expected) in requests.iter().zip(row["wire"].as_array().unwrap()) {
            assert_eq!(actual["body"], expected["body"], "{} SDK body", row["id"]);
            assert_eq!(actual["method"], expected["method"]);
            let port = expected["port"]
                .as_u64()
                .map_or(String::new(), |v| format!(":{v}"));
            assert_eq!(
                actual["endpoint"],
                json!(format!(
                    "{}//{}{}{}",
                    expected["protocol"].as_str().unwrap(),
                    expected["hostname"].as_str().unwrap(),
                    port,
                    expected["path"].as_str().unwrap()
                )),
                "{} endpoint",
                row["id"]
            );
            let headers = expected["headers"]
                .as_object()
                .unwrap()
                .iter()
                .filter(|(k, _)| !k.starts_with(':'))
                .map(|(k, v)| json!([k.to_ascii_lowercase(), v]))
                .collect::<Vec<_>>();
            assert_eq!(
                actual["headers"],
                json!(headers),
                "{} signed record projection",
                row["id"]
            );
        }
        let heads = observed.heads.lock().unwrap();
        assert_eq!(heads.len(), row["responses"].as_array().unwrap().len());
        for (actual, expected) in heads.iter().zip(row["responses"].as_array().unwrap()) {
            let mut entries = json!(
                expected["headers"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            );
            let mut actual = actual["entries"].clone();
            normalize(&mut entries);
            normalize(&mut actual);
            assert_eq!(actual, entries, "{} response record", row["id"]);
        }
        let mut actual = json!(events);
        let mut expected = row["events"].clone();
        normalize_errors(&mut actual);
        normalize_errors(&mut expected);
        assert_eq!(actual, expected, "{} events", row["id"]);
        assert_eq!(
            observed.closed.load(Ordering::SeqCst),
            requests.len(),
            "{} body ownership",
            row["id"]
        );
    }
}

fn malformed_case(id: &str) {
    let mut source: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/bedrock-converse-stream/malformed-event.json"
    ))
    .unwrap();
    let rows = source["cases"].as_array_mut().unwrap();
    assert_eq!(rows.len(), 19);
    rows.retain(|row| row["id"] == id);
    assert_eq!(rows.len(), 1);
    run_source_cases(&source);
}
macro_rules! malformed_test {
    ($name:ident,$id:literal) => {
        #[test]
        fn $name() {
            malformed_case($id);
        }
    };
}
malformed_test!(
    priming_short_parser_preview,
    "review-malformed-event-0-false"
);
malformed_test!(stream_short_parser_preview, "review-malformed-event-0-true");
malformed_test!(priming_long_body_fallback, "review-malformed-event-1-false");
malformed_test!(
    stream_long_body_outside_preview,
    "review-malformed-event-1-true"
);
malformed_test!(
    priming_property_value_preview,
    "review-malformed-event-2-false"
);
malformed_test!(
    stream_property_value_preview,
    "review-malformed-event-2-true"
);
malformed_test!(
    priming_numeric_body_fallback,
    "review-malformed-event-3-false"
);
malformed_test!(
    stream_numeric_syntax_diagnostic,
    "review-malformed-event-3-true"
);

malformed_test!(stream_preview_twenty_units, "review-neighbor-0");
malformed_test!(stream_preview_twenty_one_units, "review-neighbor-1");
malformed_test!(stream_preview_internal_suffix, "review-neighbor-2");
malformed_test!(stream_preview_internal_prefix, "review-neighbor-3");
malformed_test!(stream_eof_position_retry_fact, "review-neighbor-4");
malformed_test!(stream_syntax_position_retry_fact, "review-neighbor-5");
malformed_test!(stream_preview_overflow_fact, "review-neighbor-6");
malformed_test!(stream_overflow_outside_preview, "review-neighbor-7");

malformed_test!(stream_eof_value_has_no_position_fact, "review-neighbor-8");
malformed_test!(stream_eof_array_has_position_fact, "review-neighbor-9");

malformed_test!(stream_cr_line_position_fact, "review-neighbor-10");
