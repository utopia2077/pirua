#![cfg(all(feature = "oauth-anthropic", feature = "oauth-openai-chatgpt"))]
use pi_ai_core::*;
use pi_ai_providers::auth::{
    OAuthServices,
    oauth::{AnthropicOAuth, OpenAIChatGPTOAuth},
};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    future::Future,
    net::SocketAddr,
    sync::{Arc, Mutex},
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use url::Url;
fn ready<T>(future: impl Future<Output = T>) -> T {
    let mut future = Box::pin(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut TaskContext::from_waker(std::task::Waker::noop()))
        {
            return value;
        }
    }
    panic!("offline flow pending")
}
struct Host {
    spec: Value,
    authorization: Mutex<Option<Url>>,
    binds: Mutex<Vec<u16>>,
    requests: Mutex<Vec<Value>>,
    prompts: Mutex<Vec<String>>,
    closed: Mutex<bool>,
}
impl Host {
    fn new(spec: Value) -> Self {
        Self {
            spec,
            authorization: Mutex::new(None),
            binds: Mutex::new(Vec::new()),
            requests: Mutex::new(Vec::new()),
            prompts: Mutex::new(Vec::new()),
            closed: Mutex::new(false),
        }
    }
    fn flag(&self, name: &str) -> bool {
        self.spec[name] == true
    }
    fn services(self: &Arc<Self>) -> OAuthServices {
        OAuthServices {
            transport: Arc::new(HttpTransport::new_custom(self.clone(), self.clone())),
            clock: self.clone(),
            spawner: self.clone(),
            crypto: self.clone(),
            callbacks: Arc::new(Listener(self.clone())),
            callback_host: "127.0.0.1".into(),
            device_id: (!self.flag("noDevice"))
                .then(|| "ABCDEF01-2345-6789-ABCD-EF0123456789".into()),
            agent_name: Some("Pirua".into()),
            callback_limits: CallbackLimits::default(),
            max_token_response_bytes: 65536,
        }
    }
}
impl Clock for Host {
    fn now_unix_ms(&self) -> u64 {
        7
    }
    fn sleep<'a>(
        &'a self,
        _: Duration,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), Cancelled>> {
        Box::pin(std::future::pending())
    }
}
impl TaskSpawner for Host {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        panic!("flow must not spawn")
    }
}
impl RetryJitter for Host {
    fn sample(&self) -> f64 {
        panic!("no retries")
    }
}
impl OAuthCrypto for Host {
    fn fill_random(&self, bytes: &mut [u8]) -> Result<(), AuthHostError> {
        bytes.fill(1);
        Ok(())
    }
    fn sha256(&self, _: &[u8]) -> Result<[u8; 32], AuthHostError> {
        Ok([2; 32])
    }
}
struct Listener(Arc<Host>);
struct Session(Arc<Host>, u16);
impl CallbackListener for Listener {
    fn bind<'a>(
        &'a self,
        config: CallbackBind,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CallbackSession>, AuthHostError>> {
        Box::pin(async move {
            self.0.binds.lock().unwrap().push(config.port);
            if c.is_cancelled() {
                return Err(AuthHostError::new(AuthHostErrorKind::Cancelled));
            }
            if self.0.flag("failAll") || (self.0.flag("failPreferred") && config.port != 0) {
                return Err(AuthHostError::new(AuthHostErrorKind::Io));
            }
            Ok(Box::new(Session(
                self.0.clone(),
                if config.port == 0 { 41234 } else { config.port },
            )) as Box<dyn CallbackSession>)
        })
    }
}
impl CallbackSession for Session {
    fn local_addr(&self) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.1))
    }
    fn next<'a>(
        &'a mut self,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<CallbackRequest, AuthHostError>> {
        Box::pin(std::future::pending())
    }
    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), AuthHostError>> {
        Box::pin(async move {
            *self.0.closed.lock().unwrap() = true;
            Ok(())
        })
    }
}
struct Body(Option<Vec<u8>>);
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        Box::pin(async move { Ok(self.0.take()) })
    }
    fn close(&mut self) {}
}
impl CustomFetch for Host {
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            let FetchBody::Text(text) = &request.body else {
                panic!("OAuth body must be text")
            };
            let body = if text.starts_with('{') {
                serde_json::from_str::<Value>(text).unwrap()
            } else {
                let values = url::form_urlencoded::parse(text.as_bytes())
                    .map(|(key, value)| (key.into_owned(), Value::String(value.into_owned())))
                    .collect::<serde_json::Map<_, _>>();
                Value::Object(values)
            };
            self.requests
                .lock()
                .unwrap()
                .push(json!({"url":request.endpoint.expose_for_transport(),"body":body}));
            let body = json!({"access_token":"synthetic-access","refresh_token":"synthetic-refresh","expires_in":3600,"id_token":"synthetic-id","scope":if self.flag("badScope"){ "openid" }else{ "openid chatgpt.tokens.use.direct" }});
            Ok(HttpResponse {
                status: 200,
                status_text: "OK".into(),
                headers: HttpHeaders::new(),
                body: Box::new(Body(Some(serde_json::to_vec(&body).unwrap()))),
            })
        })
    }
}
impl AuthInteraction for Host {
    fn notify(&self, event: AuthEvent) -> Result<(), AuthInteractionError> {
        if let AuthEvent::AuthUrl { url, .. } = event {
            *self.authorization.lock().unwrap() = Some(Url::parse(&url).unwrap());
        }
        Ok(())
    }
    fn prompt<'a>(
        &'a self,
        prompt: AuthPrompt,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>> {
        self.prompts.lock().unwrap().push(
            match prompt {
                AuthPrompt::Select { .. } => "select",
                AuthPrompt::ManualCode { .. } => "manual_code",
                _ => panic!("unexpected prompt"),
            }
            .into(),
        );
        Box::pin(async move {
            if matches!(prompt, AuthPrompt::Select { .. }) {
                return Ok(self.spec["method"].as_str().unwrap().into());
            }
            let auth = self.authorization.lock().unwrap();
            let auth = auth.as_ref().unwrap();
            let state = if self.flag("wrongState") {
                "wrong".into()
            } else {
                auth.query_pairs()
                    .find(|(key, _)| key == "state")
                    .unwrap()
                    .1
                    .into_owned()
            };
            if self.spec["provider"] == "anthropic" {
                return Ok(format!("synthetic-code#{state}"));
            }
            let mut url = Url::parse(if self.flag("wrongOrigin") {
                "http://localhost:1455/auth/callback"
            } else {
                "http://127.0.0.1:1455/auth/callback"
            })
            .unwrap();
            url.query_pairs_mut()
                .append_pair("code", "synthetic-code")
                .append_pair("state", &state);
            if !self.flag("missingClient") {
                url.query_pairs_mut()
                    .append_pair("client_id", "issued-client");
            }
            Ok(url.to_string())
        })
    }
}
#[test]
fn oauth_flows_match_complete_target_source_for_registration_refresh_and_rejection() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../conformance/provider-traces/v1.4.0/oauth-observations.json"
    ))
    .unwrap();
    assert_eq!(fixture["cases"].as_array().unwrap().len(), 14);
    for case in fixture["cases"].as_array().unwrap() {
        let spec = &case["spec"];
        let id = spec["id"].as_str().unwrap();
        let host = Arc::new(Host::new(spec.clone()));
        let flow: Box<dyn OAuthAuth> = if spec["provider"] == "anthropic" {
            Box::new(AnthropicOAuth::new(host.services()))
        } else {
            Box::new(OpenAIChatGPTOAuth::new(host.services()))
        };
        let cancellation = CancellationToken::new();
        let mut previous =
            json!({"type":"oauth","access":"old","refresh":"synthetic-old-refresh","expires":0});
        if !host.flag("missingStoredClient") {
            previous["clientId"] = json!("issued-client");
        }
        let previous: OAuthCredential = serde_json::from_value(previous).unwrap();
        let result = if host.flag("refresh") {
            ready(flow.refresh(&previous, &cancellation))
        } else {
            ready(flow.login(host.as_ref(), &cancellation))
        };
        assert_eq!(result.is_err(), case["error"], "{id}: outcome");
        if let Ok(credential) = result {
            assert_eq!(
                serde_json::to_value(credential).unwrap(),
                case["credential"],
                "{id}: credential"
            );
        }
        assert_eq!(
            json!(*host.binds.lock().unwrap()),
            case["binds"],
            "{id}: bind"
        );
        assert_eq!(
            json!(*host.requests.lock().unwrap()),
            case["requests"],
            "{id}: requests"
        );
        assert_eq!(
            json!(*host.prompts.lock().unwrap()),
            case["prompts"],
            "{id}: prompts"
        );
        assert_eq!(
            json!(*host.closed.lock().unwrap()),
            case["closed"],
            "{id}: close"
        );
        let auth = host.authorization.lock().unwrap();
        let auth = auth
            .as_ref()
            .map(|url| {
                json!(
                    url.query_pairs()
                        .map(|(key, value)| (key.into_owned(), value.into_owned()))
                        .collect::<std::collections::BTreeMap<_, _>>()
                )
            })
            .unwrap_or(Value::Null);
        assert_eq!(auth, case["authorization"], "{id}: authorize");
    }
}
#[test]
fn target_oauth_source_oracle_reexecutes_offline() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("node")
        .arg(root.join("conformance/provider-traces/v1.4.0/oauth-oracle.mjs"))
        .env("NODE_NO_WARNINGS", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
