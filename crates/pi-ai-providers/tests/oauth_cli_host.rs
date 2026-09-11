#![cfg(all(
    feature = "oauth-anthropic",
    feature = "oauth-openai-codex",
    feature = "oauth-openrouter",
    feature = "auth-native"
))]
// Compile the owning CLI body; no facade-to-transport edge and no copied algorithms.
#[allow(dead_code)]
#[path = "../../pi-ai/src/bin/pi_ai_cli/mod.rs"]
mod cli;
use cli::{Files, Io, Login, Selected};
use pi_ai_core::*;
use pi_ai_providers::auth::{OAuthServices, oauth::*};
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
const JWT: &str = "e30.eyJodHRwczovL2FwaS5vcGVuYWkuY29tL2F1dGgiOnsiY2hhdGdwdF9hY2NvdW50X2lkIjoic3ludGhldGljLWFjY291bnQifX0=.synthetic";
struct Host {
    target: Mutex<Option<String>>,
    manual: AtomicBool,
    closed: AtomicUsize,
    replies: AtomicUsize,
    fetches: AtomicUsize,
    device: bool,
    trigger: Option<PathBuf>,
    saved: Mutex<Option<Value>>,
    trace: Mutex<Vec<&'static str>>,
}
impl Host {
    fn new(device: bool, trigger: Option<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            target: Mutex::new(None),
            manual: AtomicBool::new(false),
            closed: AtomicUsize::new(0),
            replies: AtomicUsize::new(0),
            fetches: AtomicUsize::new(0),
            device,
            trigger,
            saved: Mutex::new(None),
            trace: Mutex::new(vec![]),
        })
    }
    fn observe(&self, text: &str) {
        if let Some(auth) = text.lines().find(|line| line.starts_with("https://")) {
            let url = url::Url::parse(auth).unwrap();
            let fields = url
                .query_pairs()
                .collect::<std::collections::BTreeMap<_, _>>();
            if let Some(callback) = fields
                .get("redirect_uri")
                .or_else(|| fields.get("callback_url"))
            {
                let callback = url::Url::parse(callback).unwrap();
                let mut target = format!("{}?code=synthetic-code", callback.path());
                if let Some(state) = fields.get("state") {
                    target.push_str("&state=");
                    target.push_str(state)
                }
                *self.target.lock().unwrap() = Some(target);
            }
        }
    }
    fn services(self: &Arc<Self>) -> OAuthServices {
        OAuthServices {
            transport: Arc::new(HttpTransport::new_custom(self.clone(), Arc::new(Jitter))),
            clock: Arc::new(TokioClock::new(tokio::runtime::Handle::current())),
            spawner: Arc::new(cli::native::Spawner(tokio::runtime::Handle::current())),
            crypto: Arc::new(Crypto),
            callbacks: Arc::new(Listener(self.clone())),
            callback_host: "127.0.0.1".into(),
            callback_limits: CallbackLimits::default(),
            max_token_response_bytes: 1024 * 1024,
        }
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        panic!("OAuth retry unexpectedly used")
    }
}
struct Crypto;
impl OAuthCrypto for Crypto {
    fn fill_random(&self, bytes: &mut [u8]) -> Result<(), AuthHostError> {
        for (i, b) in bytes.iter_mut().enumerate() {
            *b = i as u8;
        }
        Ok(())
    }
    fn sha256(&self, _: &[u8]) -> Result<[u8; 32], AuthHostError> {
        Ok([0; 32])
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
    fn close(&mut self) {
        self.0 = None;
    }
}
impl CustomFetch for Host {
    fn fetch<'a>(
        &'a self,
        request: &'a FetchRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.fetches.fetch_add(1, Ordering::SeqCst);
            self.trace.lock().unwrap().push("fetch");
            let endpoint = request.endpoint.expose_for_transport();
            let value = if endpoint.ends_with("/deviceauth/usercode") {
                json!({"device_auth_id":"synthetic-id","user_code":"SYNTHETIC","interval":1})
            } else if endpoint.ends_with("/deviceauth/token") {
                json!({"authorization_code":"synthetic-code","code_verifier":"synthetic-verifier"})
            } else if endpoint.starts_with("https://openrouter.ai/") {
                json!({"key":"synthetic-access"})
            } else if endpoint.starts_with("https://auth.openai.com/") {
                json!({"access_token":JWT,"refresh_token":"synthetic-refresh","expires_in":3600})
            } else {
                assert!(endpoint.starts_with("https://platform.claude.com/"));
                json!({"access_token":"synthetic-access","refresh_token":"synthetic-refresh","expires_in":3600})
            };
            Ok(HttpResponse {
                status: 200,
                status_text: "Synthetic".into(),
                headers: HttpHeaders::new(),
                body: Box::new(Body(Some(value.to_string().into_bytes()))),
            })
        })
    }
}
struct Listener(Arc<Host>);
struct Session {
    host: Arc<Host>,
    port: u16,
    closed: bool,
}
impl CallbackListener for Listener {
    fn bind<'a>(
        &'a self,
        config: CallbackBind,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Box<dyn CallbackSession>, AuthHostError>> {
        Box::pin(async move {
            self.0.trace.lock().unwrap().push("bind");
            Ok(Box::new(Session {
                host: self.0.clone(),
                port: if config.port == 0 { 40000 } else { config.port },
                closed: false,
            }) as Box<dyn CallbackSession>)
        })
    }
}
impl CallbackSession for Session {
    fn local_addr(&self) -> std::net::SocketAddr {
        ([127, 0, 0, 1], self.port).into()
    }
    fn next<'a>(
        &'a mut self,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<CallbackRequest, AuthHostError>> {
        Box::pin(async move {
            loop {
                if c.is_cancelled() {
                    return Err(AuthHostError::new(AuthHostErrorKind::Cancelled));
                }
                if self.host.manual.load(Ordering::SeqCst)
                    && self.host.trigger.as_ref().is_none_or(|p| p.exists())
                    && let Some(target) = self.host.target.lock().unwrap().take()
                {
                    return Ok(CallbackRequest {
                        method: "GET".into(),
                        target,
                        reply: Box::new(Reply(self.host.clone())),
                    });
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    }
    fn close<'a>(&'a mut self) -> BoxFuture<'a, Result<(), AuthHostError>> {
        Box::pin(async move {
            if self.closed {
                return Ok(());
            }
            self.closed = true;
            self.host.closed.fetch_add(1, Ordering::SeqCst);
            self.host.trace.lock().unwrap().push("close");
            Ok(())
        })
    }
}
struct Reply(Arc<Host>);
impl CallbackReply for Reply {
    fn send<'a>(
        self: Box<Self>,
        response: CallbackResponse,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<(), AuthHostError>> {
        Box::pin(async move {
            assert_eq!(response.status, 200);
            self.0.replies.fetch_add(1, Ordering::SeqCst);
            self.0.trace.lock().unwrap().push("reply");
            Ok(())
        })
    }
}
struct ScriptIo(Arc<Host>);
impl Io for ScriptIo {
    fn open(&self) -> Result<(), cli::Error> {
        self.0.trace.lock().unwrap().push("io-open");
        Ok(())
    }
    fn close(&self) {
        self.0.trace.lock().unwrap().push("io-close");
    }
    fn output(&self, text: &str) -> Result<(), cli::Error> {
        self.0.observe(text);
        Ok(())
    }
    fn line<'a>(
        &'a self,
        question: &'a str,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, cli::Error>> {
        Box::pin(async move {
            if question.starts_with("Enter number") {
                return Ok(if self.0.device { "2junk" } else { "1junk" }.into());
            }
            self.0.manual.store(true, Ordering::SeqCst);
            c.cancelled().await;
            self.0.trace.lock().unwrap().push("manual-cancel");
            Err(cli::Error::Interaction)
        })
    }
}
impl Files for Host {
    fn exists(&self) -> Result<bool, cli::Error> {
        self.trace.lock().unwrap().push("file-exists");
        Ok(true)
    }
    fn read(&self) -> Result<String, cli::Error> {
        self.trace.lock().unwrap().push("file-read");
        Ok(r#"{"unrelated":{"keep":true}}"#.into())
    }
    fn write(&self, text: &str) -> Result<(), cli::Error> {
        self.trace.lock().unwrap().push("file-write");
        *self.saved.lock().unwrap() = Some(serde_json::from_str(text).unwrap());
        Ok(())
    }
}
struct ActualLogin(Arc<Host>);
impl Login for ActualLogin {
    fn login<'a>(
        &'a self,
        p: Selected,
        i: &'a dyn AuthInteraction,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, cli::Error>> {
        Box::pin(async move {
            let services = self.0.services();
            let oauth: Box<dyn OAuthAuth> = match p {
                Selected::Anthropic => Box::new(AnthropicOAuth::new(services)),
                Selected::Codex => Box::new(OpenAICodexOAuth::new(services)),
                Selected::OpenRouter => Box::new(OpenRouterOAuth::new(services)),
            };
            let result = oauth.login(i, c).await.map_err(|_| cli::Error::Login);
            self.0.trace.lock().unwrap().push("login-return");
            result
        })
    }
}
fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
#[test]
fn oauth_cli_actual_selected_four_login_paths_and_export() {
    runtime().block_on(async {
        for (p, device) in [
            (Selected::Anthropic, false),
            (Selected::Codex, false),
            (Selected::Codex, true),
            (Selected::OpenRouter, false),
        ] {
            let h = Host::new(device, None);
            cli::run(
                cli::Plan::Login(Some(p)),
                &ScriptIo(h.clone()),
                h.as_ref(),
                &ActualLogin(h.clone()),
            )
            .await
            .unwrap();
            assert_eq!(h.fetches.load(Ordering::SeqCst), if device { 3 } else { 1 });
            assert_eq!(h.closed.load(Ordering::SeqCst), usize::from(!device));
            assert_eq!(h.replies.load(Ordering::SeqCst), usize::from(!device));
            let saved = h.saved.lock().unwrap();
            let value = saved.as_ref().unwrap();
            assert_eq!(value["unrelated"], json!({"keep":true}));
            let credential = &value[p.id()];
            assert!(credential["type"] == "oauth");
            assert!(
                credential["access"]
                    == if p == Selected::Codex {
                        JWT
                    } else {
                        "synthetic-access"
                    },
                "access value changed"
            );
            assert!(
                credential["refresh"]
                    == if p == Selected::OpenRouter {
                        ""
                    } else {
                        "synthetic-refresh"
                    },
                "refresh value changed"
            );
            if p == Selected::Codex {
                assert!(
                    credential["accountId"] == "synthetic-account",
                    "credential extra changed"
                );
            }
            let trace = h.trace.lock().unwrap();
            let at = |s| trace.iter().position(|x| *x == s).unwrap();
            assert!(at("login-return") < at("file-read"));
            assert!(at("file-read") < at("file-write"));
            assert!(at("file-write") < at("io-close"));
            if !device {
                assert!(at("close") < at("login-return"));
                if p == Selected::OpenRouter {
                    assert!(at("fetch") < at("reply"));
                } else {
                    assert!(at("fetch") < at("close"));
                }
            }
        }
    });
}
// An internal test process, never a command or option in pi_ai_cli.
struct ChildIo {
    console: cli::console::Console,
    host: Arc<Host>,
}
impl Io for ChildIo {
    fn open(&self) -> Result<(), cli::Error> {
        self.console.open()
    }
    fn close(&self) {
        self.console.close()
    }
    fn output(&self, text: &str) -> Result<(), cli::Error> {
        self.host.observe(text);
        self.console.output(text)
    }
    fn line<'a>(
        &'a self,
        q: &'a str,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, cli::Error>> {
        self.host.manual.store(true, Ordering::SeqCst);
        self.console.line(q, c)
    }
}
#[test]
fn oauth_cli_internal_browser_child() {
    let Some(trigger) = std::env::var_os("PI_TEST_OAUTH_CLI_TRIGGER") else {
        return;
    };
    let h = Host::new(false, Some(PathBuf::from(trigger)));
    let console = ChildIo {
        console: cli::console::Console::new(),
        host: h.clone(),
    };
    let rt = runtime();
    rt.block_on(cli::run(
        cli::Plan::Login(Some(Selected::Anthropic)),
        &console,
        h.as_ref(),
        &ActualLogin(h.clone()),
    ))
    .unwrap();
    assert_eq!(h.closed.load(Ordering::SeqCst), 1);
    assert_eq!(h.replies.load(Ordering::SeqCst), 1);
    assert!(h.saved.lock().unwrap().is_some());
    drop(rt);
}
#[test]
#[cfg(target_os = "linux")]
fn oauth_cli_browser_success_exits_while_physical_stdin_remains_blocked() {
    let directory = std::env::temp_dir().join(format!("oauth-cli-physical-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let trigger = directory.join("callback-ready");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "oauth_cli_internal_browser_child", "--nocapture"])
        .env_clear()
        .env("PI_TEST_OAUTH_CLI_TRIGGER", &trigger)
        .current_dir(&directory)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdin = child.stdin.take().unwrap();
    let start = Instant::now();
    loop {
        let tasks = std::fs::read_dir(format!("/proc/{}/task", child.id())).unwrap();
        let blocked = tasks.flatten().any(|entry| {
            let p = entry.path();
            std::fs::read_to_string(p.join("comm")).is_ok_and(|s| s.trim() == "oauth-cli-stdin")
                && std::fs::read_to_string(p.join("wchan"))
                    .is_ok_and(|s| matches!(s.trim(), "pipe_read" | "anon_pipe_read"))
        });
        if blocked {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("stdin did not reach physical pipe_read")
        };
        std::thread::sleep(Duration::from_millis(5));
    }
    // With the pipe still open and unwritten, the source-shaped callback can now win.
    std::fs::write(&trigger, b"ready").unwrap();
    let start = Instant::now();
    loop {
        if child.try_wait().unwrap().is_some() {
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            child.kill().unwrap();
            let _ = child.wait();
            panic!("browser success joined blocked stdin")
        };
        std::thread::sleep(Duration::from_millis(5));
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "included CLI child failed");
    assert!(String::from_utf8_lossy(&output.stdout).contains("Credentials saved to auth.json"));
    drop(stdin);
    std::fs::remove_dir_all(directory).unwrap();
}
#[test]
fn oauth_cli_included_source_hashes_bind_host_proof() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let fixture: Value = serde_json::from_str(
        &std::fs::read_to_string(
            root.join("conformance/provider-traces/oauth-cli/rust-source-hashes.json"),
        )
        .unwrap(),
    )
    .unwrap();
    for (path, digest) in fixture.as_object().unwrap() {
        let bytes = std::fs::read(root.join(path)).unwrap();
        let actual = NativeOAuthCrypto
            .sha256(&bytes)
            .unwrap()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        assert_eq!(
            actual,
            digest.as_str().unwrap(),
            "included source changed: {path}"
        );
    }
}
