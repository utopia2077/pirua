#![cfg(all(
    feature = "oauth-anthropic",
    feature = "oauth-openai-codex",
    feature = "oauth-openrouter"
))]
use pi_ai_core::*;
use pi_ai_providers::auth::{OAuthServices, oauth::*};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
const JWT: &str = "e30.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoic3ludGhldGljLWFjY291bnQifX0=.synthetic";
type Manual = std::result::Result<String, AuthInteractionError>;
struct Harness {
    scenario: Value,
    trace: Arc<Mutex<Vec<Value>>>,
    queue: Mutex<VecDeque<CallbackRequest>>,
    request_waker: Mutex<Option<Waker>>,
    manual: Mutex<Option<tokio::sync::oneshot::Sender<Manual>>>,
    observers: Mutex<Vec<BoxFuture<'static, ()>>>,
    now: AtomicU64,
    serial: AtomicU64,
    timers: Mutex<BTreeMap<u64, (u64, Waker)>>,
    sleeps: Mutex<Vec<u64>>,
    listening: AtomicBool,
    path: Mutex<String>,
    polls: AtomicU64,
}
impl Harness {
    fn new(scenario: Value) -> Arc<Self> {
        let now = scenario
            .get("now")
            .and_then(Value::as_u64)
            .unwrap_or(1_700_000_000_000);
        Arc::new(Self {
            scenario,
            trace: Arc::new(Mutex::new(vec![])),
            queue: Mutex::new(VecDeque::new()),
            request_waker: Mutex::new(None),
            manual: Mutex::new(None),
            observers: Mutex::new(vec![]),
            now: AtomicU64::new(now),
            serial: AtomicU64::new(0),
            timers: Mutex::new(BTreeMap::new()),
            sleeps: Mutex::new(vec![]),
            listening: AtomicBool::new(false),
            path: Mutex::new(String::new()),
            polls: AtomicU64::new(0),
        })
    }
    fn flag(&self, key: &str) -> bool {
        self.scenario[key].as_bool().unwrap_or(false)
    }
    fn record(&self, value: Value) {
        self.trace.lock().unwrap().push(value);
    }
    fn request(self: &Arc<Self>, target: String) {
        self.request_method(target, "GET");
    }
    fn request_method(self: &Arc<Self>, target: String, method: &str) {
        self.queue.lock().unwrap().push_back(CallbackRequest {
            method: method.into(),
            target,
            reply: Box::new(Reply(self.clone())),
        });
        let w = self.request_waker.lock().unwrap().take();
        if let Some(w) = w {
            w.wake();
        }
    }
    fn close(&self) {
        if self.listening.swap(false, Ordering::SeqCst) {
            self.record(json!(["close"]));
        }
    }
    fn advance(&self) {
        let (at, wakers) = {
            let mut timers = self.timers.lock().unwrap();
            let at = timers
                .values()
                .map(|(at, _)| *at)
                .min()
                .expect("pending flow must have a clock deadline");
            let ids: Vec<_> = timers
                .iter()
                .filter(|(_, v)| v.0 == at)
                .map(|(id, _)| *id)
                .collect();
            let wakes = ids
                .into_iter()
                .map(|id| timers.remove(&id).unwrap().1)
                .collect::<Vec<_>>();
            (at, wakes)
        };
        self.now.store(at, Ordering::SeqCst);
        for w in wakers {
            w.wake();
        }
    }
    fn services(self: &Arc<Self>) -> OAuthServices {
        OAuthServices {
            transport: Arc::new(HttpTransport::new_custom(
                Arc::new(Fetch(self.clone())),
                Arc::new(Jitter),
            )),
            clock: Arc::new(TestClock(self.clone())),
            spawner: Arc::new(NoSpawn),
            crypto: Arc::new(Crypto),
            callbacks: Arc::new(Listener(self.clone())),
            callback_host: "127.0.0.1".into(),
            callback_limits: CallbackLimits::default(),
            max_token_response_bytes: 65536,
        }
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        panic!("OAuth has no retry")
    }
}
struct NoSpawn;
impl TaskSpawner for NoSpawn {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> std::result::Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
struct Crypto;
impl OAuthCrypto for Crypto {
    fn fill_random(&self, bytes: &mut [u8]) -> std::result::Result<(), AuthHostError> {
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }
        Ok(())
    }
    fn sha256(&self, input: &[u8]) -> std::result::Result<[u8; 32], AuthHostError> {
        assert_eq!(input, b"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8");
        Ok([
            234, 134, 106, 117, 126, 76, 56, 186, 191, 168, 18, 124, 190, 154, 64, 157, 62, 31,
            147, 160, 15, 241, 72, 143, 247, 53, 252, 249, 23, 175, 255, 208,
        ])
    }
}
struct TestClock(Arc<Harness>);
struct Sleep {
    h: Arc<Harness>,
    id: u64,
    at: u64,
    c: BoxFuture<'static, ()>,
}
impl Future for Sleep {
    type Output = std::result::Result<(), Cancelled>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.c.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(Cancelled));
        }
        if self.h.now.load(Ordering::SeqCst) >= self.at {
            return Poll::Ready(Ok(()));
        }
        let w = cx.waker().clone();
        let old = self.h.timers.lock().unwrap().insert(self.id, (self.at, w));
        drop(old);
        Poll::Pending
    }
}
impl Drop for Sleep {
    fn drop(&mut self) {
        let old = self
            .h
            .timers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.id);
        drop(old);
    }
}
impl Clock for TestClock {
    fn now_unix_ms(&self) -> u64 {
        self.0.now.load(Ordering::SeqCst)
    }
    fn sleep<'a>(
        &'a self,
        d: Duration,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<(), Cancelled>> {
        let ms = d.as_millis() as u64;
        if self.0.flag("device") {
            self.0.sleeps.lock().unwrap().push(ms);
        }
        let c = c.clone();
        Box::pin(Sleep {
            h: self.0.clone(),
            id: self.0.serial.fetch_add(1, Ordering::SeqCst),
            at: self.now_unix_ms() + ms,
            c: Box::pin(async move {
                c.cancelled().await;
            }),
        })
    }
}
struct Listener(Arc<Harness>);
struct Session(Arc<Harness>, u16);
impl CallbackListener for Listener {
    fn bind<'a>(
        &'a self,
        config: CallbackBind,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<Box<dyn CallbackSession>, AuthHostError>> {
        Box::pin(async move {
            self.0.record(json!(["bind", config.host, config.port]));
            if self.0.flag("bindFailure") {
                return Err(AuthHostError::new(AuthHostErrorKind::Io));
            }
            self.0.listening.store(true, Ordering::SeqCst);
            Ok(Box::new(Session(
                self.0.clone(),
                if config.port == 0 { 40000 } else { config.port },
            )) as Box<dyn CallbackSession>)
        })
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.0.close();
    }
}
impl CallbackSession for Session {
    fn local_addr(&self) -> std::net::SocketAddr {
        ([127, 0, 0, 1], self.1).into()
    }
    fn next<'a>(
        &'a mut self,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<CallbackRequest, AuthHostError>> {
        Box::pin(poll_fn(move |cx| {
            if c.is_cancelled() {
                return Poll::Ready(Err(AuthHostError::new(AuthHostErrorKind::Cancelled)));
            }
            if let Some(request) = self.0.queue.lock().unwrap().pop_front() {
                return Poll::Ready(Ok(request));
            }
            let old = self
                .0
                .request_waker
                .lock()
                .unwrap()
                .replace(cx.waker().clone());
            drop(old);
            Poll::Pending
        }))
    }
    fn close<'a>(&'a mut self) -> BoxFuture<'a, std::result::Result<(), AuthHostError>> {
        Box::pin(async move {
            self.0.close();
            Ok(())
        })
    }
}
struct Reply(Arc<Harness>);
impl CallbackReply for Reply {
    fn send<'a>(
        self: Box<Self>,
        response: CallbackResponse,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, std::result::Result<(), AuthHostError>> {
        Box::pin(async move {
            let mut headers = serde_json::Map::new();
            headers.insert("content-type".into(), json!(response.content_type));
            if response.no_store {
                headers.insert("cache-control".into(), json!("no-store"));
            }
            self.0
                .record(json!(["reply", response.status, response.body, headers]));
            if self.0.flag("holdReply") {
                c.cancelled().await;
                return Err(AuthHostError::new(AuthHostErrorKind::Cancelled));
            }
            Ok(())
        })
    }
}
struct CancelTrace(Arc<Mutex<Vec<Value>>>);
impl Wake for CancelTrace {
    fn wake(self: Arc<Self>) {
        self.0.lock().unwrap().push(json!(["manual-cancel"]));
    }
}
struct Interaction(Arc<Harness>);
impl AuthInteraction for Interaction {
    fn prompt<'a>(&'a self, prompt: AuthPrompt, c: &'a CancellationToken) -> BoxFuture<'a, Manual> {
        self.0
            .record(json!(["prompt", serde_json::to_value(&prompt).unwrap()]));
        if matches!(prompt, AuthPrompt::Select { .. }) {
            return Box::pin(async move {
                Ok(self
                    .0
                    .scenario
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or(if self.0.flag("device") {
                        "device_code"
                    } else {
                        "browser"
                    })
                    .into())
            });
        }
        let token = c.clone();
        let mut observer: BoxFuture<'static, ()> = Box::pin(async move {
            token.cancelled().await;
        });
        let waker = Waker::from(Arc::new(CancelTrace(self.0.trace.clone())));
        let _ = observer.as_mut().poll(&mut Context::from_waker(&waker));
        self.0.observers.lock().unwrap().push(observer);
        if self.0.flag("manualRejectBefore") {
            return Box::pin(async { Err(AuthInteractionError::new()) });
        }
        if let Some(value) = self.0.scenario.get("manual").and_then(Value::as_str) {
            let value = value.to_owned();
            return Box::pin(async move { Ok(value) });
        }
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        *self.0.manual.lock().unwrap() = Some(tx);
        let mut cancel = Box::pin(c.cancelled());
        Box::pin(poll_fn(move |cx| {
            if cancel.as_mut().poll(cx).is_ready() {
                return Poll::Ready(Err(AuthInteractionError::new()));
            }
            Pin::new(&mut rx)
                .poll(cx)
                .map(|v| v.unwrap_or_else(|_| Err(AuthInteractionError::new())))
        }))
    }
    fn notify(&self, event: AuthEvent) -> std::result::Result<(), AuthInteractionError> {
        self.0
            .record(json!(["notify", serde_json::to_value(&event).unwrap()]));
        if let AuthEvent::AuthUrl { url, .. } = event
            && (self.0.flag("callback") || self.0.flag("denied") || self.0.flag("invalidOnly"))
        {
            let url = url::Url::parse(&url).unwrap();
            let router = self.0.scenario["provider"] == "openrouter";
            let redirect = url
                .query_pairs()
                .find(|(k, _)| {
                    k == if router {
                        "callback_url"
                    } else {
                        "redirect_uri"
                    }
                })
                .unwrap()
                .1
                .into_owned();
            let mut target = url::Url::parse(&redirect).unwrap();
            *self.0.path.lock().unwrap() = target.path().into();
            if self.0.flag("invalidOnly") {
                self.0.request("/wrong-route".into());
                return Ok(());
            }
            if self.0.flag("invalid") {
                self.0.request("/not-callback".into());
                self.0.request(format!(
                    "{}{}",
                    target.path(),
                    if router {
                        "?state=wrong"
                    } else {
                        "?code=synthetic-code&state=wrong"
                    }
                ));
            }
            if self.0.flag("extraError") {
                self.0
                    .request(format!("{}?error=synthetic-denied", target.path()));
            }
            if self.0.flag("malformedTarget") {
                self.0.request("http://[".into());
            }
            if self.0.flag("invalidMethod") {
                self.0
                    .request_method(format!("{}?code=synthetic-wrong", target.path()), "POST");
            }
            if self.0.flag("denied") {
                self.0.request(format!(
                    "{}?error=denied&error_description=Denied%20%3Cb%3E%26",
                    target.path()
                ));
                return Ok(());
            }
            target
                .query_pairs_mut()
                .append_pair("code", "synthetic-callback-code");
            if !router {
                let state = url
                    .query_pairs()
                    .find(|(k, _)| k == "state")
                    .unwrap()
                    .1
                    .into_owned();
                target.query_pairs_mut().append_pair("state", &state);
            }
            self.0.request_method(
                format!("{}?{}", target.path(), target.query().unwrap()),
                self.0.scenario["callbackMethod"].as_str().unwrap_or("GET"),
            );
        }
        Ok(())
    }
}
struct Fetch(Arc<Harness>);
struct Body {
    h: Arc<Harness>,
    url: String,
    body: Option<Vec<u8>>,
}
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, std::result::Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move {
            if self.body.is_some() {
                self.h.record(json!(["read", self.url]));
                if self.url.ends_with("/deviceauth/token") {
                    self.h.now.fetch_add(
                        self.h.scenario["pollAdvance"].as_u64().unwrap_or(0),
                        Ordering::SeqCst,
                    );
                }
                if self.h.flag("readError") {
                    return Err(TransportError::new(TransportErrorKind::Connection));
                }
            }
            Ok(self.body.take())
        })
    }
    fn close(&mut self) {
        self.body = None;
    }
}
impl CustomFetch for Fetch {
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
        runtime: &'a RequestRuntime,
    ) -> BoxFuture<'a, std::result::Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let url = request.endpoint.expose_for_transport();
            let headers = match &request.headers {
                FetchHeaders::Record(v) => v.iter().map(|(a, b)| json!([a, b])).collect::<Vec<_>>(),
                _ => panic!("source raw header record"),
            };
            let body = match &request.body {
                FetchBody::Text(v) => v,
                _ => panic!("source text request"),
            };
            self.0.record(json!(["fetch", url, headers, body]));
            if self.0.flag("claimedManual") {
                if let Some(tx) = self.0.manual.lock().unwrap().take() {
                    let _ = tx.send(if self.0.flag("manualError") {
                        Err(AuthInteractionError::new())
                    } else {
                        Ok("synthetic-later-manual".into())
                    });
                }
                let path = self.0.path.lock().unwrap().clone();
                self.0.request(format!("{path}?code=synthetic-duplicate"));
            }
            if self.0.flag("hold") {
                runtime.cancellation.cancelled().await;
                return Err(TransportError::new(TransportErrorKind::Cancelled));
            }
            let mut status = self.0.scenario["status"].as_u64().unwrap_or(200) as u16;
            let mut body = if self.0.scenario["provider"] == "openrouter" {
                if let Some(detail) = self.0.scenario.get("errorDescription") {
                    {
                        let mut value = json!({"error_description":detail});
                        if let Some(message) = self.0.scenario.get("errorMessage") {
                            value["message"] = message.clone();
                        }
                        value
                    }
                } else {
                    json!({"key":self.0.scenario.get("key").cloned().unwrap_or(json!("synthetic-key"))})
                }
            } else {
                json!({"access_token":self.0.scenario.get("access").cloned().unwrap_or(json!(JWT)),"refresh_token":"synthetic-refresh","expires_in":self.0.scenario.get("expires").cloned().unwrap_or(json!(3600))})
            };
            if url.ends_with("/usercode") {
                body = json!({"device_auth_id":"synthetic-device","user_code":"synthetic-user","interval":self.0.scenario.get("interval").cloned().unwrap_or(json!(0))});
            }
            if url.ends_with("/deviceauth/token") {
                let polls = self.0.scenario["polls"].as_array().unwrap();
                let index = self.0.polls.fetch_add(1, Ordering::SeqCst) as usize;
                let next = &polls[index.min(polls.len() - 1)];
                status = next.as_u64().unwrap_or(400) as u16;
                body = if next == 200 {
                    json!({"authorization_code":"synthetic-device-code","code_verifier":"synthetic-device-verifier"})
                } else {
                    json!({"error":if next=="pending"{json!("deviceauth_authorization_pending")}else if next=="nested_pending"{json!({"code":"deviceauth_authorization_pending"})}else{next.clone()}})
                };
            }
            Ok(HttpResponse {
                status,
                status_text: "Synthetic".into(),
                headers: HttpHeaders::new(),
                body: Box::new(Body {
                    h: self.0.clone(),
                    url: url.into(),
                    body: Some(if self.0.flag("invalidJson") {
                        b"{".to_vec()
                    } else {
                        body.to_string().into_bytes()
                    }),
                }),
            })
        })
    }
}
struct Flag(AtomicBool);
impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}
fn drive<T>(h: &Harness, mut future: BoxFuture<'_, T>) -> T {
    let flag = Arc::new(Flag(AtomicBool::new(true)));
    let waker = Waker::from(flag.clone());
    let mut cx = Context::from_waker(&waker);
    for _ in 0..512 {
        flag.0.store(false, Ordering::SeqCst);
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
        if !flag.0.load(Ordering::SeqCst) {
            h.advance();
        }
    }
    panic!("flow failed to settle")
}
fn source() -> Value {
    serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/oauth-selected/source-observations.json"
    ))
    .unwrap()
}
#[test]
fn oauth_selected_complete_source_trace_parity() {
    assert_source_trace_parity(None);
}
#[test]
fn oauth_openrouter_successful_body_read_failure_is_json_admission() {
    assert_source_trace_parity(Some("openrouter-callback-body-read-failure"));
}
#[test]
fn oauth_openrouter_callback_timeout_preserves_fixed_page_detail() {
    assert_source_trace_parity(Some("openrouter-callback-exchange-timeout"));
}
fn assert_source_trace_parity(selected: Option<&str>) {
    let source = source();
    let observations = source["observations"].as_array().unwrap();
    assert_eq!(observations.len(), 49);
    let mut compared = 0;
    for expected in observations {
        if selected.is_some_and(|id| expected["id"] != id) {
            continue;
        }
        compared += 1;
        let scenario = expected["scenario"].clone();
        let id = expected["id"].as_str().unwrap();
        let h = Harness::new(scenario);
        let services = h.services();
        let flow: Box<dyn OAuthAuth> = match h.scenario["provider"].as_str().unwrap() {
            "anthropic" => Box::new(AnthropicOAuth::new(services)),
            "codex" => Box::new(OpenAICodexOAuth::new(services)),
            "openrouter" => Box::new(OpenRouterOAuth::new(services)),
            _ => unreachable!(),
        };
        let i = Interaction(h.clone());
        let c = CancellationToken::new();
        let credential:OAuthCredential=serde_json::from_value(json!({"type":"oauth","access":"synthetic-old","refresh":"synthetic-refresh","expires":0,"extra":"synthetic-extra"})).unwrap();
        let result = drive(
            &h,
            if h.flag("refresh") {
                flow.refresh(&credential, &c)
            } else {
                flow.login(&i, &c)
            },
        );
        if id == "codex-device-fractional" || id.starts_with("admission-") {
            let error = result.unwrap_err();
            assert_eq!(
                std::error::Error::source(&error)
                    .unwrap()
                    .downcast_ref::<OAuthFlowError>()
                    .unwrap()
                    .kind(),
                OAuthFlowErrorKind::InvalidResponse,
                "{id}"
            );
            let expected_trace = if id == "codex-device-fractional" {
                json!(expected["trace"].as_array().unwrap()[..3])
            } else {
                expected["trace"].clone()
            };
            assert_eq!(
                json!(*h.trace.lock().unwrap()),
                expected_trace,
                "{id} inherited admission"
            );
            continue;
        }
        match result {
            Ok(credential) => {
                let value = serde_json::to_value(&credential).unwrap();
                let auth = drive(&h, flow.to_auth(&credential, &c)).unwrap();
                let projected = json!({"authMatchesAccess":auth.api_key.as_ref()==Some(&credential.credentials.access),"type":"oauth","expires":value["expires"],"refreshEmpty":credential.credentials.refresh.expose_secret().is_empty(),"accountId":value.get("accountId").cloned().unwrap_or(Value::Null),"extra":value.get("extra").cloned().unwrap_or(Value::Null)});
                assert_eq!(projected, expected["result"], "{id} result");
                assert!(expected["error"].is_null());
            }
            Err(error) => {
                assert!(!expected["error"].is_null(), "{id}: {error:?}");
                assert!(!format!("{error:?} {error}").contains("synthetic"));
                let expected_kind = match id {
                    "anthropic-bind-failure"
                    | "openrouter-callback-manual-error"
                    | "openrouter-manual-rejected" => OAuthFlowErrorKind::Host,
                    "anthropic-state-mismatch" => OAuthFlowErrorKind::StateMismatch,
                    "anthropic-manual-empty-state" => OAuthFlowErrorKind::MissingState,
                    "anthropic-missing-code" => OAuthFlowErrorKind::MissingCode,
                    "codex-unknown-method" => OAuthFlowErrorKind::UnknownMethod,
                    "codex-device-disabled" => OAuthFlowErrorKind::DeviceDisabled,
                    "codex-device-denied" => OAuthFlowErrorKind::DeviceDenied,
                    "codex-device-timeout" => OAuthFlowErrorKind::DeviceTimeout,
                    "codex-device-slow-timeout" => OAuthFlowErrorKind::DeviceSlowDownTimeout,
                    "openrouter-denied" => OAuthFlowErrorKind::AuthorizationDenied,
                    "openrouter-timeout"
                    | "openrouter-exchange-timeout"
                    | "openrouter-callback-exchange-timeout"
                    | "openrouter-invalid-route-timeout" => OAuthFlowErrorKind::Timeout,
                    "codex-invalid-jwt"
                    | "openrouter-empty-key"
                    | "openrouter-invalid-json"
                    | "openrouter-callback-body-read-failure" => {
                        OAuthFlowErrorKind::InvalidResponse
                    }
                    "anthropic-http-error"
                    | "openrouter-callback-http-error"
                    | "codex-http-body-failure"
                    | "openrouter-http-body-failure"
                    | "openrouter-error-detail"
                    | "openrouter-empty-error-detail" => OAuthFlowErrorKind::Exchange,
                    _ => panic!("missing source error classification: {id}"),
                };
                assert_eq!(
                    std::error::Error::source(&error)
                        .unwrap()
                        .downcast_ref::<OAuthFlowError>()
                        .unwrap()
                        .kind(),
                    expected_kind,
                    "{id}"
                );
            }
        }
        assert_eq!(
            json!(*h.sleeps.lock().unwrap()),
            expected["sleeps"],
            "{id} sleeps"
        );
        assert_eq!(
            json!(*h.trace.lock().unwrap()),
            expected["trace"],
            "{id} trace"
        );
    }
    assert_eq!(compared, if selected.is_some() { 1 } else { 49 });
}
#[test]
fn oauth_selected_full_pinned_source_reexecutes_with_exact_hashes() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/provider-traces/oauth-selected");
    let output = std::process::Command::new("/usr/bin/node")
        .arg(root.join("source.mjs"))
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(actual, source());
}

#[cfg(feature = "auth-native")]
#[test]
fn oauth_selected_source_credential_values_match_without_logging_tokens() {
    fn matches(value: &str, expected: &Value) -> bool {
        let bytes = NativeOAuthCrypto.sha256(value.as_bytes()).unwrap();
        let digest = bytes.iter().map(|b| format!("{b:02x}")).collect::<String>();
        expected.as_str() == Some(digest.as_str())
    }
    let source = source();
    let expected = source["credentialDigests"].as_object().unwrap();
    let mut compared = 0;
    for observation in source["observations"].as_array().unwrap() {
        let id = observation["id"].as_str().unwrap();
        let Some(digests) = expected.get(id) else {
            continue;
        };
        if id == "codex-device-fractional" || id.starts_with("admission-") {
            continue;
        }
        let h = Harness::new(observation["scenario"].clone());
        let services = h.services();
        let flow: Box<dyn OAuthAuth> = match h.scenario["provider"].as_str().unwrap() {
            "anthropic" => Box::new(AnthropicOAuth::new(services)),
            "codex" => Box::new(OpenAICodexOAuth::new(services)),
            "openrouter" => Box::new(OpenRouterOAuth::new(services)),
            _ => unreachable!(),
        };
        let interaction = Interaction(h.clone());
        let cancellation = CancellationToken::new();
        let previous:OAuthCredential=serde_json::from_value(json!({"type":"oauth","access":"synthetic-old","refresh":"synthetic-refresh","expires":0,"extra":"synthetic-extra"})).unwrap();
        let credential = drive(
            &h,
            if h.flag("refresh") {
                flow.refresh(&previous, &cancellation)
            } else {
                flow.login(&interaction, &cancellation)
            },
        )
        .unwrap();
        for (field, value) in [
            ("access", credential.credentials.access.expose_secret()),
            ("refresh", credential.credentials.refresh.expose_secret()),
        ] {
            assert!(
                matches(value, &digests[field]),
                "synthetic {field} credential equality failed in {id}"
            );
            let mut changed = value.to_owned();
            changed.push('\0');
            assert!(
                !matches(&changed, &digests[field]),
                "credential equality control failed in {id}"
            );
        }
        compared += 1;
    }
    assert_eq!(compared, expected.len() - 3);
    assert!(compared > 0);
}

#[cfg(feature = "auth-native")]
mod native_composition {
    use super::*;
    static NATIVE_PORTS: Mutex<()> = Mutex::new(());
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    struct NativeClock(TokioClock);
    impl Clock for NativeClock {
        fn now_unix_ms(&self) -> u64 {
            1_700_000_000_000
        }
        fn sleep<'a>(
            &'a self,
            d: Duration,
            c: &'a CancellationToken,
        ) -> BoxFuture<'a, std::result::Result<(), Cancelled>> {
            self.0.sleep(d, c)
        }
    }
    struct HeldFetch {
        router: bool,
        started: Arc<tokio::sync::Notify>,
        release: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    }
    struct NativeBody(Option<Vec<u8>>);
    impl ResponseBody for NativeBody {
        fn next<'a>(
            &'a mut self,
            _: &'a RequestRuntime,
        ) -> BoxFuture<'a, std::result::Result<Option<Vec<u8>>, TransportError>> {
            Box::pin(async move { Ok(self.0.take()) })
        }
        fn close(&mut self) {
            self.0 = None;
        }
    }
    impl CustomFetch for HeldFetch {
        fn fetch<'a>(
            &'a self,
            request: &'a FetchRequest,
            _: &'a RequestRuntime,
        ) -> BoxFuture<'a, std::result::Result<HttpResponse, TransportError>> {
            Box::pin(async move {
                let body = match &request.body {
                    FetchBody::Text(v) => v,
                    _ => panic!("text"),
                };
                assert!(body.contains("synthetic-callback-code"));
                self.started.notify_one();
                let release = self.release.lock().unwrap().take().unwrap();
                release.await.unwrap();
                let body = if self.router {
                    json!({"key":"synthetic-key"})
                } else {
                    json!({"access_token":JWT,"refresh_token":"synthetic-refresh","expires_in":3600})
                };
                Ok(HttpResponse {
                    status: 200,
                    status_text: "OK".into(),
                    headers: HttpHeaders::new(),
                    body: Box::new(NativeBody(Some(body.to_string().into_bytes()))),
                })
            })
        }
    }
    struct ObservedListener {
        inner: NativeCallbackListener,
        state: Arc<NativeState>,
    }
    struct ObservedSession {
        inner: Box<dyn CallbackSession>,
        state: Arc<NativeState>,
    }
    impl CallbackListener for ObservedListener {
        fn bind<'a>(
            &'a self,
            b: CallbackBind,
            c: &'a CancellationToken,
        ) -> BoxFuture<'a, std::result::Result<Box<dyn CallbackSession>, AuthHostError>> {
            Box::pin(async move {
                let inner = self.inner.bind(b, c).await?;
                *self.state.addr.lock().unwrap() = Some(inner.local_addr());
                Ok(Box::new(ObservedSession {
                    inner,
                    state: self.state.clone(),
                }) as Box<dyn CallbackSession>)
            })
        }
    }
    impl CallbackSession for ObservedSession {
        fn local_addr(&self) -> std::net::SocketAddr {
            self.inner.local_addr()
        }
        fn next<'a>(
            &'a mut self,
            c: &'a CancellationToken,
        ) -> BoxFuture<'a, std::result::Result<CallbackRequest, AuthHostError>> {
            Box::pin(async move {
                let mut request = self.inner.next(c).await?;
                if self.state.hold_primary.load(Ordering::SeqCst) {
                    request.reply = Box::new(HeldNativeReply {
                        inner: Some(request.reply),
                        state: self.state.clone(),
                        primary: false,
                    });
                }
                Ok(request)
            })
        }
        fn close<'a>(&'a mut self) -> BoxFuture<'a, std::result::Result<(), AuthHostError>> {
            Box::pin(async move {
                self.inner.close().await?;
                if !self.state.closed.swap(true, Ordering::SeqCst) {
                    self.state.close_notice.notify_one();
                }
                Ok(())
            })
        }
    }
    struct NativeState {
        addr: Mutex<Option<std::net::SocketAddr>>,
        closed: AtomicBool,
        close_notice: tokio::sync::Notify,
        callback_done: AtomicBool,
        callback_notice: tokio::sync::Notify,
        manual_tokens: Mutex<Vec<CancellationToken>>,
        prompt_notice: tokio::sync::Notify,
        tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
        hold_primary: AtomicBool,
        expect_eof: AtomicBool,
        reply_ready: tokio::sync::Notify,
        reply_release: tokio::sync::Notify,
        reply_drops: AtomicU64,
    }
    impl NativeState {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                addr: Mutex::new(None),
                closed: AtomicBool::new(false),
                close_notice: tokio::sync::Notify::new(),
                callback_done: AtomicBool::new(false),
                callback_notice: tokio::sync::Notify::new(),
                manual_tokens: Mutex::new(vec![]),
                prompt_notice: tokio::sync::Notify::new(),
                tasks: Mutex::new(vec![]),
                hold_primary: AtomicBool::new(false),
                expect_eof: AtomicBool::new(false),
                reply_ready: tokio::sync::Notify::new(),
                reply_release: tokio::sync::Notify::new(),
                reply_drops: AtomicU64::new(0),
            })
        }
    }
    struct HeldNativeReply {
        inner: Option<Box<dyn CallbackReply>>,
        state: Arc<NativeState>,
        primary: bool,
    }
    impl Drop for HeldNativeReply {
        fn drop(&mut self) {
            if self.primary {
                self.state.reply_drops.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    impl CallbackReply for HeldNativeReply {
        fn send<'a>(
            mut self: Box<Self>,
            response: CallbackResponse,
            c: &'a CancellationToken,
        ) -> BoxFuture<'a, std::result::Result<(), AuthHostError>> {
            Box::pin(async move {
                if response.status == 200 {
                    self.primary = true;
                    self.state.reply_ready.notify_one();
                    self.state.reply_release.notified().await;
                }
                self.inner.take().unwrap().send(response, c).await
            })
        }
    }
    struct NativeInteraction {
        state: Arc<NativeState>,
        handle: tokio::runtime::Handle,
        callbacks: bool,
    }
    async fn get(addr: std::net::SocketAddr, target: &str) -> u16 {
        let mut peer = tokio::net::TcpStream::connect(addr).await.unwrap();
        peer.write_all(format!("GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut bytes = vec![];
        peer.read_to_end(&mut bytes).await.unwrap();
        let response = String::from_utf8(bytes).unwrap();
        if response.is_empty() {
            return 0;
        }
        response.split(' ').nth(1).unwrap().parse().unwrap()
    }
    impl AuthInteraction for NativeInteraction {
        fn prompt<'a>(&'a self, p: AuthPrompt, c: &'a CancellationToken) -> BoxFuture<'a, Manual> {
            if matches!(p, AuthPrompt::Select { .. }) {
                return Box::pin(async { Ok("browser".into()) });
            }
            self.state.manual_tokens.lock().unwrap().push(c.clone());
            self.state.prompt_notice.notify_one();
            Box::pin(async move {
                c.cancelled().await;
                Err(AuthInteractionError::new())
            })
        }
        fn notify(&self, e: AuthEvent) -> std::result::Result<(), AuthInteractionError> {
            if let AuthEvent::AuthUrl { url, .. } = e
                && self.callbacks
            {
                let url = url::Url::parse(&url).unwrap();
                let redirect = url
                    .query_pairs()
                    .find(|(k, _)| matches!(k.as_ref(), "redirect_uri" | "callback_url"))
                    .unwrap()
                    .1
                    .into_owned();
                let mut target = url::Url::parse(&redirect).unwrap();
                let addr = ([127, 0, 0, 1], target.port().unwrap()).into();
                target
                    .query_pairs_mut()
                    .append_pair("code", "synthetic-callback-code");
                if let Some((_, state)) = url.query_pairs().find(|(k, _)| k == "state") {
                    target.query_pairs_mut().append_pair("state", &state);
                }
                let target = format!("{}?{}", target.path(), target.query().unwrap());
                let state = self.state.clone();
                let task = self.handle.spawn(async move {
                    assert_eq!(get(addr, "/wrong-route").await, 404);
                    assert_eq!(
                        get(addr, &target).await,
                        if state.expect_eof.load(Ordering::SeqCst) {
                            0
                        } else {
                            200
                        }
                    );
                    state.callback_done.store(true, Ordering::SeqCst);
                    state.callback_notice.notify_one();
                });
                self.state.tasks.lock().unwrap().push(task);
            }
            Ok(())
        }
    }
    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }
    fn flow(id: &str, s: OAuthServices) -> Arc<dyn OAuthAuth> {
        match id {
            "anthropic" => Arc::new(AnthropicOAuth::new(s)),
            "codex" => Arc::new(OpenAICodexOAuth::new(s)),
            "openrouter" => Arc::new(OpenRouterOAuth::new(s)),
            _ => unreachable!(),
        }
    }
    #[test]
    fn oauth_selected_native_callback_reply_and_cleanup_stages() {
        let _ports = NATIVE_PORTS.lock().unwrap();
        let runtime = runtime();
        runtime.block_on(async {
            for id in ["anthropic", "codex", "openrouter"] {
                let state = NativeState::new();
                let started = Arc::new(tokio::sync::Notify::new());
                let (tx, rx) = tokio::sync::oneshot::channel();
                let s = OAuthServices {
                    transport: Arc::new(HttpTransport::new_custom(
                        Arc::new(HeldFetch {
                            router: id == "openrouter",
                            started: started.clone(),
                            release: Mutex::new(Some(rx)),
                        }),
                        Arc::new(Jitter),
                    )),
                    clock: Arc::new(NativeClock(TokioClock::new(runtime.handle().clone()))),
                    spawner: Arc::new(NoSpawn),
                    crypto: Arc::new(Crypto),
                    callbacks: Arc::new(ObservedListener {
                        inner: NativeCallbackListener::new(runtime.handle().clone()),
                        state: state.clone(),
                    }),
                    callback_host: "127.0.0.1".into(),
                    callback_limits: CallbackLimits::default(),
                    max_token_response_bytes: 65536,
                };
                let flow = flow(id, s);
                let i = Arc::new(NativeInteraction {
                    state: state.clone(),
                    handle: runtime.handle().clone(),
                    callbacks: true,
                });
                let task = runtime
                    .spawn(async move { flow.login(i.as_ref(), &CancellationToken::new()).await });
                tokio::time::timeout(Duration::from_secs(2), started.notified())
                    .await
                    .unwrap();
                assert!(!task.is_finished());
                if id == "openrouter" {
                    assert!(!state.closed.load(Ordering::SeqCst));
                    assert!(
                        tokio::time::timeout(
                            Duration::from_millis(30),
                            state.callback_notice.notified()
                        )
                        .await
                        .is_err()
                    );
                } else {
                    tokio::time::timeout(Duration::from_secs(2), state.close_notice.notified())
                        .await
                        .unwrap();
                    let rebound =
                        std::net::TcpListener::bind(state.addr.lock().unwrap().unwrap()).unwrap();
                    drop(rebound);
                    assert!(
                        state
                            .manual_tokens
                            .lock()
                            .unwrap()
                            .iter()
                            .all(CancellationToken::is_cancelled)
                    );
                }
                tx.send(()).unwrap();
                assert!(
                    tokio::time::timeout(Duration::from_secs(2), task)
                        .await
                        .unwrap()
                        .unwrap()
                        .is_ok()
                );
                assert!(state.closed.load(Ordering::SeqCst));
                assert!(
                    state
                        .manual_tokens
                        .lock()
                        .unwrap()
                        .iter()
                        .all(CancellationToken::is_cancelled)
                );
                let tasks = std::mem::take(&mut *state.tasks.lock().unwrap());
                for task in tasks {
                    task.await.unwrap();
                }
                assert!(state.callback_done.load(Ordering::SeqCst));
                let rebound =
                    std::net::TcpListener::bind(state.addr.lock().unwrap().unwrap()).unwrap();
                drop(rebound);
            }
        });
    }
    #[test]
    fn oauth_selected_native_cancellation_releases_listener_and_prompt() {
        let _ports = NATIVE_PORTS.lock().unwrap();
        let runtime = runtime();
        runtime.block_on(async {
            for id in ["anthropic", "codex", "openrouter"] {
                let state = NativeState::new();
                let h = Harness::new(json!({"provider":id}));
                let mut s = h.services();
                s.clock = Arc::new(NativeClock(TokioClock::new(runtime.handle().clone())));
                s.callbacks = Arc::new(ObservedListener {
                    inner: NativeCallbackListener::new(runtime.handle().clone()),
                    state: state.clone(),
                });
                let flow = flow(id, s);
                let i = Arc::new(NativeInteraction {
                    state: state.clone(),
                    handle: runtime.handle().clone(),
                    callbacks: false,
                });
                let c = CancellationToken::new();
                let child = c.clone();
                let task = runtime.spawn(async move { flow.login(i.as_ref(), &child).await });
                tokio::time::timeout(Duration::from_secs(2), state.prompt_notice.notified())
                    .await
                    .unwrap();
                c.cancel();
                let error = tokio::time::timeout(Duration::from_secs(2), task)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap_err();
                assert_eq!(
                    std::error::Error::source(&error)
                        .unwrap()
                        .downcast_ref::<OAuthFlowError>()
                        .unwrap()
                        .kind(),
                    OAuthFlowErrorKind::Cancelled
                );
                assert!(state.closed.load(Ordering::SeqCst));
                assert!(
                    state
                        .manual_tokens
                        .lock()
                        .unwrap()
                        .iter()
                        .all(CancellationToken::is_cancelled)
                );
                let rebound =
                    std::net::TcpListener::bind(state.addr.lock().unwrap().unwrap()).unwrap();
                drop(rebound);
            }
        });
    }
    #[test]
    fn oauth_selected_native_codex_bind_fallback_and_anthropic_failure() {
        struct ManualFallback(AtomicU64);
        impl AuthInteraction for ManualFallback {
            fn prompt<'a>(
                &'a self,
                p: AuthPrompt,
                _: &'a CancellationToken,
            ) -> BoxFuture<'a, Manual> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move {
                    Ok(if matches!(p, AuthPrompt::Select { .. }) {
                        "browser"
                    } else {
                        "synthetic-code"
                    }
                    .into())
                })
            }
            fn notify(&self, _: AuthEvent) -> std::result::Result<(), AuthInteractionError> {
                Ok(())
            }
        }
        let _ports = NATIVE_PORTS.lock().unwrap();
        let runtime = runtime();
        runtime.block_on(async {
            for (id, port) in [("codex", 1455), ("anthropic", 53692)] {
                let occupied =
                    std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).unwrap();
                let h = Harness::new(json!({"provider":id}));
                let mut services = h.services();
                services.callbacks =
                    Arc::new(NativeCallbackListener::new(runtime.handle().clone()));
                let flow = flow(id, services);
                let interaction = ManualFallback(AtomicU64::new(0));
                let result = tokio::time::timeout(
                    Duration::from_secs(2),
                    flow.login(&interaction, &CancellationToken::new()),
                )
                .await
                .unwrap();
                if id == "codex" {
                    assert!(result.is_ok());
                    assert_eq!(interaction.0.load(Ordering::SeqCst), 2);
                    let trace_len = h.trace.lock().unwrap().len();
                    let state = NativeState::new();
                    let pending_interaction = Arc::new(NativeInteraction {
                        state: state.clone(),
                        handle: runtime.handle().clone(),
                        callbacks: false,
                    });
                    let cancellation = CancellationToken::new();
                    let child = cancellation.clone();
                    let pending_flow = flow.clone();
                    let pending = runtime.spawn(async move {
                        pending_flow
                            .login(pending_interaction.as_ref(), &child)
                            .await
                    });
                    tokio::time::timeout(Duration::from_secs(2), state.prompt_notice.notified())
                        .await
                        .unwrap();
                    cancellation.cancel();
                    let error = tokio::time::timeout(Duration::from_secs(2), pending)
                        .await
                        .unwrap()
                        .unwrap()
                        .unwrap_err();
                    assert_eq!(
                        std::error::Error::source(&error)
                            .unwrap()
                            .downcast_ref::<OAuthFlowError>()
                            .unwrap()
                            .kind(),
                        OAuthFlowErrorKind::Cancelled
                    );
                    assert!(
                        state
                            .manual_tokens
                            .lock()
                            .unwrap()
                            .iter()
                            .all(CancellationToken::is_cancelled)
                    );
                    assert_eq!(h.trace.lock().unwrap().len(), trace_len);
                } else {
                    let error = result.unwrap_err();
                    assert_eq!(
                        std::error::Error::source(&error)
                            .unwrap()
                            .downcast_ref::<OAuthFlowError>()
                            .unwrap()
                            .kind(),
                        OAuthFlowErrorKind::Host
                    );
                    assert_eq!(interaction.0.load(Ordering::SeqCst), 0);
                    assert!(h.trace.lock().unwrap().is_empty());
                }
                drop(occupied);
            }
        });
    }
    #[test]
    fn oauth_selected_native_stalled_primary_ack_retains_cancellation_and_timeout_owners() {
        let _ports = NATIVE_PORTS.lock().unwrap();
        let runtime = runtime();
        runtime.block_on(async {
            for (id, mode) in [
                ("anthropic", "release"),
                ("anthropic", "cancel"),
                ("codex", "release"),
                ("codex", "cancel"),
                ("openrouter", "release"),
                ("openrouter", "cancel"),
                ("openrouter", "timeout"),
            ] {
                let state = NativeState::new();
                state.hold_primary.store(true, Ordering::SeqCst);
                state.expect_eof.store(mode != "release", Ordering::SeqCst);
                let h = Harness::new(json!({"provider":id}));
                let mut services = h.services();
                services.callbacks = Arc::new(ObservedListener {
                    inner: NativeCallbackListener::new(runtime.handle().clone()),
                    state: state.clone(),
                });
                let flow = flow(id, services);
                let interaction = Arc::new(NativeInteraction {
                    state: state.clone(),
                    handle: runtime.handle().clone(),
                    callbacks: true,
                });
                let cancellation = CancellationToken::new();
                let child = cancellation.clone();
                let task =
                    runtime.spawn(async move { flow.login(interaction.as_ref(), &child).await });
                tokio::time::timeout(Duration::from_secs(2), state.reply_ready.notified())
                    .await
                    .unwrap();
                assert!(!task.is_finished());
                assert!(!state.closed.load(Ordering::SeqCst));
                assert_eq!(state.reply_drops.load(Ordering::SeqCst), 0);
                match mode {
                    "release" => state.reply_release.notify_one(),
                    "cancel" => cancellation.cancel(),
                    "timeout" => h.advance(),
                    _ => unreachable!(),
                }
                let result = tokio::time::timeout(Duration::from_secs(2), task)
                    .await
                    .unwrap()
                    .unwrap();
                if mode == "release" {
                    assert!(result.is_ok());
                } else {
                    let error = result.unwrap_err();
                    assert_eq!(
                        std::error::Error::source(&error)
                            .unwrap()
                            .downcast_ref::<OAuthFlowError>()
                            .unwrap()
                            .kind(),
                        if mode == "cancel" {
                            OAuthFlowErrorKind::Cancelled
                        } else {
                            OAuthFlowErrorKind::Timeout
                        },
                        "{id} {mode}"
                    );
                }
                assert!(state.closed.load(Ordering::SeqCst));
                assert_eq!(state.reply_drops.load(Ordering::SeqCst), 1);
                assert!(
                    state
                        .manual_tokens
                        .lock()
                        .unwrap()
                        .iter()
                        .all(CancellationToken::is_cancelled)
                );
                let tasks = std::mem::take(&mut *state.tasks.lock().unwrap());
                for task in tasks {
                    tokio::time::timeout(Duration::from_secs(2), task)
                        .await
                        .unwrap()
                        .unwrap();
                }
                let rebound =
                    std::net::TcpListener::bind(state.addr.lock().unwrap().unwrap()).unwrap();
                drop(rebound);
            }
        });
    }
}
