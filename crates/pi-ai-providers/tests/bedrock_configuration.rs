#![cfg(feature = "wire-bedrock-converse-stream")]
use pi_ai_core::*;
use pi_ai_providers::api::{
    bedrock_converse_stream::*,
    runtime::{AdapterServices, PayloadMiddleware},
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
#[derive(Default)]
struct Observed {
    blocked: Mutex<Option<String>>,
    pending: AtomicUsize,
    released: AtomicUsize,
    effects: Mutex<Vec<String>>,
    sends: AtomicUsize,
    closes: AtomicUsize,
    waits: Mutex<Vec<u64>>,
    requests: Mutex<Vec<Value>>,
}
struct Guard<'a>(&'a Observed);
impl Drop for Guard<'_> {
    fn drop(&mut self) {
        self.0.released.fetch_add(1, Ordering::SeqCst);
    }
}
impl Observed {
    async fn pause(&self, stage: &str) {
        if self.blocked.lock().unwrap().as_deref() == Some(stage) {
            self.pending.fetch_add(1, Ordering::SeqCst);
            let _guard = Guard(self);
            std::future::pending::<()>().await;
        }
    }

    fn effect(&self, s: &str) {
        self.effects.lock().unwrap().push(s.into());
    }
}
struct ClockPort {
    now: AtomicU64,
    observed: Arc<Observed>,
}
impl Clock for ClockPort {
    fn now_unix_ms(&self) -> u64 {
        self.now.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(async move {
            self.observed
                .waits
                .lock()
                .unwrap()
                .push(d.as_millis() as u64);
            self.observed.pause("backoff").await;
            self.now.fetch_add(d.as_millis() as u64, Ordering::SeqCst);
            Ok(())
        })
    }
}
struct Spawn(tokio::runtime::Handle);
impl TaskSpawner for Spawn {
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.spawn(f);
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
        panic!("generic retry")
    }
}
struct Leaf<T> {
    name: &'static str,
    value: T,
    mode: String,
    observed: Arc<Observed>,
}
impl<T: Clone + Send + Sync> BedrockConfigLeaf<T> for Leaf<T> {
    fn resolve<'a>(&'a self, _: &'a RequestRuntime) -> BoxFuture<'a, Result<T, BedrockLeafError>> {
        Box::pin(async move {
            self.observed.effect(self.name);
            self.observed.pause(self.name).await;
            if self.mode == format!("fail:{}", self.name)
                || (self.mode == "token-failure" && self.name == "token")
            {
                Err(BedrockLeafError {
                    failure: ModelsError::new(ModelsErrorCode::Auth),
                    retry_class: SmithyRetryClass::Transient,
                })
            } else {
                Ok(self.value.clone())
            }
        })
    }
}
struct Credentials {
    mode: String,
    observed: Arc<Observed>,
}
impl BedrockConfigLeaf<BedrockCredentials> for Credentials {
    fn resolve<'a>(
        &'a self,
        r: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<BedrockCredentials, BedrockLeafError>> {
        Box::pin(async move {
            self.observed.effect("credentials");
            self.observed.pause("credentials").await;
            if self.mode == "fail:credentials" {
                return Err(BedrockLeafError {
                    failure: ModelsError::new(ModelsErrorCode::Auth),
                    retry_class: SmithyRetryClass::Transient,
                });
            }
            Ok(BedrockCredentials {
                access_key_id: SecretString::new("AKID_SYNTHETIC"),
                secret_access_key: SecretString::new("synthetic-secret"),
                session_token: Some(SecretString::new("synthetic-session")),
                expiration_unix_ms: match self.mode.as_str() {
                    "retry-expiring" => {
                        Some(FiniteF64::new(r.clock.now_unix_ms() as f64 + 299999.0).unwrap())
                    }
                    "retry-expiry-exact" => {
                        Some(FiniteF64::new(r.clock.now_unix_ms() as f64 + 300000.0).unwrap())
                    }
                    _ => None,
                },
            })
        })
    }
}
struct Config {
    mode: String,
    observed: Arc<Observed>,
}
impl Config {
    fn leaf<T: Clone + Send + Sync + 'static>(
        &self,
        name: &'static str,
        value: T,
    ) -> BedrockSetting<T> {
        BedrockSetting::Leaf(Arc::new(Leaf {
            name,
            value,
            mode: self.mode.clone(),
            observed: self.observed.clone(),
        }))
    }
}
impl BedrockConfigProvider for Config {
    fn create_session(&self, key: BedrockConfigKey<'_>) -> Result<BedrockSettings, ModelsError> {
        assert_eq!(key.profile, Some("synthetic"));
        self.observed.effect("constructed");
        Ok(BedrockSettings {
            region: self.leaf("region", "us-east-1".into()),
            auth_scheme_preference: self.leaf(
                "authPreference",
                vec![
                    if matches!(self.mode.as_str(), "bearer" | "token-failure") {
                        BedrockAuthScheme::Bearer
                    } else {
                        BedrockAuthScheme::SigV4
                    },
                ],
            ),
            credentials: BedrockSetting::Leaf(Arc::new(Credentials {
                mode: self.mode.clone(),
                observed: self.observed.clone(),
            })),
            token: self.leaf(
                "token",
                BedrockToken {
                    token: SecretString::new("synthetic-bearer"),
                    expiration_unix_ms: None,
                },
            ),
            endpoint_override: self.leaf(
                "endpoint",
                Some(Arc::new(
                    RequestEndpoint::new_resolved("https://synthetic.invalid/base").unwrap(),
                )),
            ),
            use_fips_endpoint: self.leaf("fips", false),
            use_dualstack_endpoint: self.leaf("dual", false),
            retry_mode: self.leaf("retryMode", SmithyRetryMode::Standard),
            user_agent_app_id: self.leaf("appId", Some("synthetic".into())),
            max_attempts: self.leaf("maxAttempts", FiniteF64::new(3.0).unwrap()),
            user_agent: vec![],
            ..Default::default()
        })
    }
}
struct Hook(Arc<Observed>);
impl PayloadMiddleware<BedrockPayload> for Hook {
    fn on_payload<'a>(
        &'a self,
        _: &'a TextStreamRequest,
        _: &'a mut BedrockPayload,
    ) -> BoxFuture<'a, Result<Option<BedrockPayload>, ModelsError>> {
        Box::pin(async move {
            self.0.effect("payload");
            self.0.pause("payload").await;
            Ok(Some(BedrockPayload::from_json(
                json!({"modelId":"synthetic","messages":[]}),
            )))
        })
    }
}
struct Body {
    bytes: Option<Vec<u8>>,
    observed: Arc<Observed>,
    closed: bool,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            if self.bytes.is_none() {
                self.observed.pause("body").await;
            }
            Ok(self.bytes.take())
        })
    }
    fn close(&mut self) {
        if !self.closed {
            self.closed = true;
            self.observed.closes.fetch_add(1, Ordering::SeqCst);
        }
    }
}
struct Raw {
    mode: String,
    observed: Arc<Observed>,
}
impl RawHttpClient for Raw {
    fn send<'a>(
        &'a self,
        _: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        panic!("generic HTTP")
    }
    fn send_smithy<'a>(
        &'a self,
        r: &'a HttpRequest,
        _: &'a RequestRuntime,
        _: &'a SmithyRequestProfile,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let n = self.observed.sends.fetch_add(1, Ordering::SeqCst) + 1;
            self.observed.effect(&format!("handle:{n}"));
            self.observed.requests.lock().unwrap().push(json!({"date":r.headers.get_for_wire("x-amz-date"),"authorization":r.headers.get_for_wire("authorization"),"attempt":r.headers.get_for_wire("amz-sdk-request"),"body":String::from_utf8(r.body.clone()).unwrap()}));
            self.observed.pause("acquisition").await;
            if self.mode.starts_with("annotation:")
                && (self.mode != "annotation:transient" || (n - 1) % 3 < 2)
            {
                let error = TransportError::new(TransportErrorKind::Connection);
                return Err(match self.mode.as_str() {
                    "annotation:transient" => {
                        error.with_smithy_retry(SmithyRetryDisposition::Transient)
                    }
                    "annotation:final-retry" => error
                        .with_smithy_retry(SmithyRetryDisposition::NotRetryable)
                        .with_smithy_assistant_retry(true),
                    "annotation:nonretryable" => {
                        error.with_smithy_retry(SmithyRetryDisposition::NotRetryable)
                    }
                    _ => error,
                });
            }

            let retry = (self.mode.starts_with("retry-") || self.mode == "block:backoff") && n < 3;
            let mut headers = HttpHeaders::new();
            if retry && self.mode == "retry-skew" {
                headers
                    .append("date", "Fri, 02 Jan 2026 04:04:05 GMT")
                    .unwrap();
            }
            Ok(HttpResponse {
                status: if retry { 503 } else { 200 },
                status_text: String::new(),
                headers,
                body: Box::new(Body {
                    bytes: if self.mode == "block:after-first" {
                        let corpus:Value=serde_json::from_str(include_str!("../../../conformance/provider-traces/bedrock-converse-stream/family.json")).unwrap();
                        Some(
                            corpus["cases"][0]["responseBodies"][0][0]
                                .as_str()
                                .unwrap()
                                .as_bytes()
                                .chunks(2)
                                .map(|p| {
                                    u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap()
                                })
                                .collect(),
                        )
                    } else {
                        retry.then(|| b"{\"message\":\"synthetic retry\"}".to_vec())
                    },
                    observed: self.observed.clone(),
                    closed: false,
                }),
            })
        })
    }
}
fn request(runtime: RequestRuntime) -> TextStreamRequest {
    let c: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/bedrock-converse-stream/family.json"
    ))
    .unwrap();
    TextStreamRequest {
        resolved: ResolvedRequest {
            model: serde_json::from_value(c["cases"][0]["model"].clone()).unwrap(),
            endpoint: RequestEndpoint::new_resolved(
                "https://bedrock-runtime.us-east-1.amazonaws.com",
            )
            .unwrap(),
        },
        context: serde_json::from_value(json!({"messages":[]})).unwrap(),
        auth: ResolvedAuth::default(),
        options: StreamOptions {
            wire: StreamOptionsWire::default(),
            runtime,
        },
    }
}
#[test]
fn configured_source_leaf_failure_stages_retry_identity_and_clock_skew() {
    let source: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/bedrock-converse-stream/service-config-stage.json"
    ))
    .unwrap();
    assert_eq!(source["cases"].as_array().unwrap().len(), 16);
    let mut now = source["clockStart"].as_u64().unwrap();
    for row in source["cases"].as_array().unwrap() {
        let mode = row["mode"].as_str().unwrap().to_owned();
        let engine = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let observed = Arc::new(Observed::default());
        let clock = Arc::new(ClockPort {
            now: AtomicU64::new(now),
            observed: observed.clone(),
        });
        let runtime = RequestRuntime::with_noop_telemetry(
            CancellationToken::new(),
            clock.clone(),
            Arc::new(Spawn(engine.handle().clone())),
        );
        let mut services = AdapterServices::<BedrockPayload>::new(Arc::new(HttpTransport::new(
            Arc::new(Raw {
                mode: mode.clone(),
                observed: observed.clone(),
            }),
            Arc::new(Jitter),
        )));
        services.env = serde_json::from_value(json!({"AWS_PROFILE":"synthetic"})).unwrap();
        services.payload = Some(Arc::new(Hook(observed.clone())));
        let api = BedrockApi::new(
            services,
            Arc::new(Config {
                mode: mode.clone(),
                observed: observed.clone(),
            }),
            Arc::new(Entropy),
        );
        let mut stream = api.stream_with_options(request(runtime), BedrockOptions::default());
        engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while stream.next().await.is_some() {}
            })
            .await
            .unwrap();
        });
        let expected = row["effects"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v.as_str())
            .filter(|s| !matches!(*s, "build" | "response"))
            .collect::<Vec<_>>();
        assert_eq!(*observed.effects.lock().unwrap(), expected, "{mode}");
        assert_eq!(
            observed.sends.load(Ordering::SeqCst),
            row["sends"].as_u64().unwrap() as usize,
            "{mode}"
        );
        assert_eq!(
            observed.closes.load(Ordering::SeqCst),
            observed.sends.load(Ordering::SeqCst)
        );
        assert_eq!(
            json!(*observed.waits.lock().unwrap()),
            row["waits"],
            "{mode}"
        );
        let actual = observed.requests.lock().unwrap();
        for (a, e) in actual.iter().zip(row["requests"].as_array().unwrap()) {
            for key in ["date", "authorization", "attempt", "body"] {
                assert_eq!(
                    a.get(key).filter(|v| !v.is_null()),
                    e.get(key),
                    "{mode} {key}"
                );
            }
        }
        now = clock.now_unix_ms();
    }
}

#[test]
fn original_caller_cancellation_releases_each_noncooperative_stage() {
    for stage in [
        "payload",
        "region",
        "authPreference",
        "credentials",
        "endpoint",
        "fips",
        "dual",
        "retryMode",
        "appId",
        "maxAttempts",
        "acquisition",
        "backoff",
        "body",
        "after-first",
    ] {
        let engine = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let observed = Arc::new(Observed::default());
        *observed.blocked.lock().unwrap() = Some(
            if stage == "after-first" {
                "body"
            } else {
                stage
            }
            .into(),
        );
        let clock = Arc::new(ClockPort {
            now: AtomicU64::new(1767323045000),
            observed: observed.clone(),
        });
        let cancel = CancellationToken::new();
        let runtime = RequestRuntime::with_noop_telemetry(
            cancel.clone(),
            clock,
            Arc::new(Spawn(engine.handle().clone())),
        );
        let mode = format!("block:{stage}");
        let mut services = AdapterServices::<BedrockPayload>::new(Arc::new(HttpTransport::new(
            Arc::new(Raw {
                mode: mode.clone(),
                observed: observed.clone(),
            }),
            Arc::new(Jitter),
        )));
        services.env = serde_json::from_value(json!({"AWS_PROFILE":"synthetic"})).unwrap();
        services.payload = Some(Arc::new(Hook(observed.clone())));
        let api = BedrockApi::new(
            services,
            Arc::new(Config {
                mode,
                observed: observed.clone(),
            }),
            Arc::new(Entropy),
        );
        let mut stream = api.stream_with_options(request(runtime), BedrockOptions::default());
        let events = engine.block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                while observed.pending.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
                cancel.cancel();
                let mut events = vec![];
                while let Some(event) = stream.next().await {
                    events.push(event);
                }
                while observed.released.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
                tokio::task::yield_now().await;
                events
            })
            .await
            .unwrap()
        });
        assert_eq!(observed.pending.load(Ordering::SeqCst), 1, "{stage}");
        assert_eq!(observed.released.load(Ordering::SeqCst), 1, "{stage}");
        let terminal = events.last().unwrap();
        assert!(
            matches!(
                terminal,
                AssistantMessageEvent::Error {
                    reason: ErrorReason::Aborted,
                    ..
                }
            ),
            "{stage}: {terminal:?}"
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, AssistantMessageEvent::Error { .. }))
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, AssistantMessageEvent::Start { .. }))
                .count(),
            usize::from(stage == "after-first")
        );
        let expected_sends = usize::from(matches!(
            stage,
            "acquisition" | "backoff" | "body" | "after-first"
        ));
        assert_eq!(
            observed.sends.load(Ordering::SeqCst),
            expected_sends,
            "{stage}"
        );
        assert_eq!(
            observed.closes.load(Ordering::SeqCst),
            usize::from(matches!(stage, "backoff" | "body" | "after-first")),
            "{stage}"
        );
    }
}

#[test]
fn explicit_retry_annotation_is_consumed_and_identity_is_per_invocation() {
    for mode in [
        "annotation:transient",
        "annotation:nonretryable",
        "annotation:opaque",
        "annotation:final-retry",
    ] {
        let engine = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let observed = Arc::new(Observed::default());
        let mut services = AdapterServices::<BedrockPayload>::new(Arc::new(HttpTransport::new(
            Arc::new(Raw {
                mode: mode.into(),
                observed: observed.clone(),
            }),
            Arc::new(Jitter),
        )));
        services.env = serde_json::from_value(json!({"AWS_PROFILE":"synthetic"})).unwrap();
        services.payload = Some(Arc::new(Hook(observed.clone())));
        let api = BedrockApi::new(
            services,
            Arc::new(Config {
                mode: mode.into(),
                observed: observed.clone(),
            }),
            Arc::new(Entropy),
        );
        for _ in 0..2 {
            let clock = Arc::new(ClockPort {
                now: AtomicU64::new(1767323045000),
                observed: observed.clone(),
            });
            let runtime = RequestRuntime::with_noop_telemetry(
                CancellationToken::new(),
                clock,
                Arc::new(Spawn(engine.handle().clone())),
            );
            let mut stream = api.stream_with_options(request(runtime), BedrockOptions::default());
            let last = engine.block_on(async {
                let mut last = None;
                while let Some(event) = stream.next().await {
                    last = Some(event);
                }
                last.unwrap()
            });
            if mode != "annotation:transient" {
                if let AssistantMessageEvent::Error { error, .. } = last {
                    assert_eq!(
                        is_retryable_assistant_error(&error),
                        mode == "annotation:final-retry"
                    );
                } else {
                    panic!("expected terminal failure");
                }
            }
        }
        let retry = mode == "annotation:transient";
        assert_eq!(
            observed.sends.load(Ordering::SeqCst),
            if retry { 6 } else { 2 },
            "{mode}"
        );
        assert_eq!(
            observed.closes.load(Ordering::SeqCst),
            if retry { 2 } else { 0 }
        );
        for effect in ["constructed", "payload", "credentials"] {
            assert_eq!(
                observed
                    .effects
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|s| *s == effect)
                    .count(),
                2,
                "{mode} {effect}"
            );
        }
        assert_eq!(
            observed.waits.lock().unwrap().len(),
            if retry { 4 } else { 0 }
        );
    }
}
