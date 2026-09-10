use pi_ai_core::*;
use pi_ai_providers::auth::*;
use std::{
    collections::BTreeMap,
    future::poll_fn,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::Poll,
};

struct Env {
    values: BTreeMap<String, String>,
    seen: Mutex<Vec<String>>,
    cancel: bool,
}
impl Env {
    fn new(values: &[(&str, &str)]) -> Self {
        Self {
            values: values
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            seen: Mutex::new(vec![]),
            cancel: false,
        }
    }
}
impl AuthContext for Env {
    fn env<'a>(
        &'a self,
        n: &'a str,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async move {
            self.seen.lock().unwrap().push(n.into());
            if self.cancel {
                c.cancel();
            }
            Ok(self.values.get(n).cloned())
        })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}
struct Interaction {
    cancel: bool,
}
impl AuthInteraction for Interaction {
    fn prompt<'a>(
        &'a self,
        p: AuthPrompt,
        c: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>> {
        Box::pin(async move {
            assert!(matches!(p, AuthPrompt::Secret { .. }));
            if self.cancel {
                c.cancel();
            }
            Ok(String::new())
        })
    }
    fn notify(&self, _: AuthEvent) -> Result<(), AuthInteractionError> {
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
#[test]
fn auth_foundation_key_precedence_truthiness_and_cancellation() {
    runtime().block_on(async {
        let env = Env::new(&[
            (ANTHROPIC_AUTH_TOKEN_ENV, "synthetic-bearer"),
            (ANTHROPIC_OAUTH_TOKEN_ENV, "synthetic-oauth"),
            (ANTHROPIC_API_KEY_ENV, "synthetic-api"),
        ]);
        let c = CancellationToken::new();
        let helper = anthropic_api_key_auth();
        let credential = ApiKeyCredential {
            key: Some(SecretString::new("synthetic-stored")),
            env: Some(ProviderEnv::new(BTreeMap::from([(
                "CONFIG".into(),
                "synthetic".into(),
            )]))),
            ..Default::default()
        };
        let result = helper
            .resolve(ApiKeyAuthInput {
                context: &env,
                credential: Some(&credential),
                cancellation: &c,
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.source.as_deref(), Some("stored credential"));
        assert_eq!(result.env, credential.env);
        assert!(env.seen.lock().unwrap().is_empty());
        let empty = ApiKeyCredential {
            key: Some(SecretString::new("")),
            ..Default::default()
        };
        let result = helper
            .resolve(ApiKeyAuthInput {
                context: &env,
                credential: Some(&empty),
                cancellation: &c,
            })
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.source.as_deref(), Some(ANTHROPIC_AUTH_TOKEN_ENV));
        assert!(result.auth.api_key.is_none());
        assert_eq!(
            result.auth.headers.unwrap().as_map()["Authorization"].as_deref(),
            Some("Bearer synthetic-bearer")
        );
        for (values, expected) in [
            (
                vec![(ANTHROPIC_OAUTH_TOKEN_ENV, "synthetic-oauth")],
                ANTHROPIC_OAUTH_TOKEN_ENV,
            ),
            (
                vec![(ANTHROPIC_API_KEY_ENV, "synthetic-api")],
                ANTHROPIC_API_KEY_ENV,
            ),
        ] {
            let env = Env::new(&values);
            assert_eq!(
                helper
                    .resolve(ApiKeyAuthInput {
                        context: &env,
                        credential: None,
                        cancellation: &c
                    })
                    .await
                    .unwrap()
                    .unwrap()
                    .source
                    .as_deref(),
                Some(expected)
            );
        }
        let env = Env::new(&[("ONE", ""), ("TWO", " ")]);
        let helper = env_api_key_auth("Fixture", vec!["ONE".into(), "TWO".into()]);
        assert_eq!(
            helper
                .resolve(ApiKeyAuthInput {
                    context: &env,
                    credential: None,
                    cancellation: &c
                })
                .await
                .unwrap()
                .unwrap()
                .auth
                .api_key
                .unwrap()
                .expose_secret(),
            " "
        );
        assert_eq!(*env.seen.lock().unwrap(), ["ONE", "TWO"]);
        assert_eq!(
            helper
                .login(&Interaction { cancel: false }, &c)
                .unwrap()
                .await
                .unwrap()
                .key
                .unwrap()
                .expose_secret(),
            ""
        );
        let cancel = CancellationToken::new();
        assert!(
            helper
                .login(&Interaction { cancel: true }, &cancel)
                .unwrap()
                .await
                .is_err()
        );
        let env = Env {
            cancel: true,
            ..Env::new(&[("ONE", "synthetic")])
        };
        assert!(
            helper
                .resolve(ApiKeyAuthInput {
                    context: &env,
                    credential: None,
                    cancellation: &CancellationToken::new()
                })
                .await
                .is_err()
        );
        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let env = Env::new(&[]);
        assert!(
            helper
                .resolve(ApiKeyAuthInput {
                    context: &env,
                    credential: None,
                    cancellation: &cancelled
                })
                .await
                .is_err()
        );
        assert!(env.seen.lock().unwrap().is_empty());
    });
}
#[test]
fn auth_foundation_context_and_selected_legacy_discovery() {
    runtime().block_on(async {
        let raw = Arc::new(Env::new(&[
            ("BOM", "\u{feff} \t"),
            ("NEL", "\u{85}"),
            ("VALUE", "  synthetic  "),
            (ANTHROPIC_AUTH_TOKEN_ENV, "synthetic-bearer"),
            (ANTHROPIC_API_KEY_ENV, "synthetic-api"),
        ]));
        let default = DefaultProviderAuthContext::new(raw.clone());
        let c = CancellationToken::new();
        assert_eq!(default.env("BOM", &c).await.unwrap(), None);
        assert_eq!(
            default.env("NEL", &c).await.unwrap().as_deref(),
            Some("\u{85}")
        );
        assert_eq!(
            default.env("VALUE", &c).await.unwrap().as_deref(),
            Some("  synthetic  ")
        );
        assert_eq!(
            find_env_keys("anthropic", None, raw.as_ref(), &c)
                .await
                .unwrap()
                .unwrap(),
            [ANTHROPIC_AUTH_TOKEN_ENV, ANTHROPIC_API_KEY_ENV]
        );
        assert_eq!(
            get_env_api_key("anthropic", None, raw.as_ref(), &c)
                .await
                .unwrap()
                .unwrap()
                .expose_secret(),
            "synthetic-api"
        );
        let scoped = ProviderEnv::new(BTreeMap::from([("VALUE".into(), "".into())]));
        assert_eq!(
            get_provider_env_value("VALUE", Some(&scoped), raw.as_ref(), &c)
                .await
                .unwrap()
                .as_deref(),
            Some("  synthetic  ")
        );
        for id in ["openai-codex", "google-vertex", "amazon-bedrock", "custom"] {
            assert!(
                find_env_keys(id, None, raw.as_ref(), &c)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(selected_api_key_env_vars("google"), ["GEMINI_API_KEY"]);
    });
}
struct Spawner(tokio::runtime::Handle);
impl TaskSpawner for Spawner {
    fn spawn(&self, t: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.spawn(t);
        Ok(())
    }
}
struct Reject;
impl TaskSpawner for Reject {
    fn spawn(&self, _: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        Err(SpawnError::new())
    }
}
struct Flow;
impl OAuthAuth for Flow {
    fn name(&self) -> &str {
        "Loaded"
    }
    fn login<'a>(
        &'a self,
        _: &'a dyn AuthInteraction,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async { Ok(credential()) })
    }
    fn refresh<'a>(
        &'a self,
        c: &'a OAuthCredential,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<OAuthCredential, AuthProviderError>> {
        Box::pin(async move { Ok(c.clone()) })
    }
    fn to_auth<'a>(
        &'a self,
        c: &'a OAuthCredential,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<ModelAuth, AuthProviderError>> {
        Box::pin(async move {
            Ok(ModelAuth {
                api_key: Some(c.credentials.access.clone()),
                ..Default::default()
            })
        })
    }
}
fn credential() -> OAuthCredential {
    serde_json::from_value(serde_json::json!({"type":"oauth","access":"synthetic-access","refresh":"","expires":9007199254740991_u64})).unwrap()
}
struct Loader {
    calls: AtomicUsize,
    fail: bool,
    ready: Arc<tokio::sync::Notify>,
}
impl OAuthLoader for Loader {
    fn load(self: Arc<Self>) -> BoxFuture<'static, Result<Arc<dyn OAuthAuth>, AuthProviderError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.ready.notified().await;
            if self.fail {
                Err(AuthProviderError::new())
            } else {
                Ok(Arc::new(Flow) as Arc<dyn OAuthAuth>)
            }
        })
    }
}
fn metadata() -> LazyOAuthMetadata {
    LazyOAuthMetadata {
        name: "Fixture OAuth".into(),
        is_subscription: true,
        login_label: Some("Fixture login".into()),
    }
}
#[test]
fn auth_foundation_lazy_oauth_memoizes_success_failure_and_survives_waiter_drop() {
    let rt = runtime();
    rt.block_on(async {
        for fail in [false, true] {
            let ready = Arc::new(tokio::sync::Notify::new());
            let loader = Arc::new(Loader {
                calls: AtomicUsize::new(0),
                fail,
                ready: ready.clone(),
            });
            let lazy = LazyOAuth::new(
                metadata(),
                loader.clone(),
                Arc::new(Spawner(rt.handle().clone())),
            );
            assert_eq!(lazy.name(), "Fixture OAuth");
            assert!(lazy.is_subscription());
            assert_eq!(lazy.login_label(), Some("Fixture login"));
            assert_eq!(loader.calls.load(Ordering::SeqCst), 0);
            let c = CancellationToken::new();
            let credential = credential();
            let mut first = lazy.to_auth(&credential, &c);
            poll_fn(|cx| {
                assert!(first.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            drop(first);
            ready.notify_one();
            let result = lazy.refresh(&credential, &c).await;
            assert_eq!(result.is_err(), fail);
            assert_eq!(lazy.to_auth(&credential, &c).await.is_err(), fail);
            assert_eq!(loader.calls.load(Ordering::SeqCst), 1);
        }
        let loader = Arc::new(Loader {
            calls: AtomicUsize::new(0),
            fail: false,
            ready: Arc::new(tokio::sync::Notify::new()),
        });
        let lazy = LazyOAuth::new(metadata(), loader.clone(), Arc::new(Reject));
        assert!(
            lazy.to_auth(&credential(), &CancellationToken::new())
                .await
                .is_err()
        );
        assert!(
            lazy.to_auth(&credential(), &CancellationToken::new())
                .await
                .is_err()
        );
        assert_eq!(loader.calls.load(Ordering::SeqCst), 0);
    });
}
#[test]
fn auth_foundation_inherited_numeric_admission_is_not_rounding() {
    assert_eq!(
        checked_oauth_expiry(1_700_000_000_000, 3600.0, 300000)
            .unwrap()
            .get(),
        1_700_003_300_000
    );
    assert_eq!(checked_oauth_expiry(1000, 0.5, 0).unwrap().get(), 1500);
    for seconds in [
        f64::NAN,
        f64::INFINITY,
        -2.0,
        0.0005,
        9_007_199_254_740_991.0,
    ] {
        assert!(checked_oauth_expiry(1000, seconds, 0).is_err());
    }
    assert!(checked_oauth_expiry(1000, 0.0, 300000).is_err());
    assert!(checked_auth_integer(0.5).is_err());
    assert_eq!(
        checked_auth_integer(9_007_199_254_740_991.0).unwrap().get(),
        9_007_199_254_740_991
    );
}

#[test]
fn auth_foundation_complete_pinned_source_and_hashes_remain_exact() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/provider-traces/auth-foundation");
    let output = std::process::Command::new("/usr/bin/node")
        .arg(root.join("source.mjs"))
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "source failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    let expected: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("source-observations.json")).unwrap())
            .unwrap();
    assert_eq!(actual, expected);
}

#[test]
fn auth_foundation_lazy_cancelled_waiter_does_not_cancel_shared_load() {
    let rt = runtime();
    rt.block_on(async {
        let ready = Arc::new(tokio::sync::Notify::new());
        let loader = Arc::new(Loader {
            calls: AtomicUsize::new(0),
            fail: false,
            ready: ready.clone(),
        });
        let lazy = LazyOAuth::new(
            metadata(),
            loader.clone(),
            Arc::new(Spawner(rt.handle().clone())),
        );
        let c = CancellationToken::new();
        let credential = credential();
        let mut first = lazy.to_auth(&credential, &c);
        poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        c.cancel();
        assert!(first.await.is_err());
        ready.notify_one();
        assert!(
            lazy.to_auth(&credential, &CancellationToken::new())
                .await
                .unwrap()
                .api_key
                .is_some()
        );
        assert_eq!(loader.calls.load(Ordering::SeqCst), 1);
    });
}

#[cfg(feature = "native")]
#[test]
fn auth_foundation_native_context_reads_only_requested_fixture_path() {
    runtime().block_on(async {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let host = NativeProviderAuthContext::new(Some(root.to_string_lossy().into_owned()));
        let c = CancellationToken::new();
        assert!(
            host.file_exists("~/tests/auth_foundation.rs", &c)
                .await
                .unwrap()
        );
        assert!(
            !host
                .file_exists("~/tests/missing-synthetic-auth-foundation-file", &c)
                .await
                .unwrap()
        );
        c.cancel();
        assert!(
            host.file_exists("~/tests/auth_foundation.rs", &c)
                .await
                .is_err()
        );
    });
}

#[test]
fn auth_foundation_lazy_concurrent_waiters_and_accepted_task_drop() {
    struct Discard;
    impl TaskSpawner for Discard {
        fn spawn(&self, task: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
            drop(task);
            Ok(())
        }
    }
    let rt = runtime();
    rt.block_on(async {
        let loader = Arc::new(Loader {
            calls: AtomicUsize::new(0),
            fail: false,
            ready: Arc::new(tokio::sync::Notify::new()),
        });
        let lazy = LazyOAuth::new(metadata(), loader, Arc::new(Discard));
        assert!(
            lazy.to_auth(&credential(), &CancellationToken::new())
                .await
                .is_err()
        );
        let ready = Arc::new(tokio::sync::Notify::new());
        let loader = Arc::new(Loader {
            calls: AtomicUsize::new(0),
            fail: false,
            ready: ready.clone(),
        });
        let lazy = LazyOAuth::new(
            metadata(),
            loader.clone(),
            Arc::new(Spawner(rt.handle().clone())),
        );
        let c = CancellationToken::new();
        let credential = credential();
        let mut first = lazy.to_auth(&credential, &c);
        let mut second = lazy.refresh(&credential, &c);
        poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            assert!(second.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        ready.notify_one();
        assert!(first.await.is_ok());
        assert!(second.await.is_ok());
        assert_eq!(loader.calls.load(Ordering::SeqCst), 1);
    });
}
