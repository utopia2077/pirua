//! Host `models.json` providers reach the retained Completions wire through real ports.
use pi_acp_server::{
    composition::compose_registry,
    credentials::Credentials,
    models_config::{ModelsConfig, register_configured_providers},
};
use pi_ai::*;
use pi_ai_transport::*;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

const VLLM_RECORDING: &[u8] = include_bytes!(
    "../../../conformance/provider-traces/openai-completions/recorded/sse_vllm021_delta_reasoning.txt"
);

struct Spawner(tokio::runtime::Handle);
impl TaskSpawner for Spawner {
    fn spawn(&self, f: BoxFuture<'static, ()>) -> Result<(), SpawnError> {
        self.0.spawn(f);
        Ok(())
    }
}
struct Environment(BTreeMap<&'static str, &'static str>);
impl AuthContext for Environment {
    fn env<'a>(
        &'a self,
        name: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        let value = self.0.get(name).map(|v| (*v).to_owned());
        Box::pin(async move { Ok(value) })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        Box::pin(async { Ok(false) })
    }
}
struct Body(Option<Vec<u8>>);
impl ResponseBody for Body {
    fn next<'a>(
        &'a mut self,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<Option<Vec<u8>>, TransportError>> {
        let b = self.0.take();
        Box::pin(async move { Ok(b) })
    }
    fn close(&mut self) {
        self.0 = None;
    }
}
#[derive(Clone, Debug)]
struct Sent {
    url: String,
    authorization: Option<String>,
    body: Value,
}
struct Gateway {
    sent: Arc<Mutex<Vec<Sent>>>,
    response: &'static [u8],
}
impl RawHttpClient for Gateway {
    fn send<'a>(
        &'a self,
        request: &'a HttpRequest,
        _: &'a RequestRuntime,
    ) -> BoxFuture<'a, Result<HttpResponse, TransportError>> {
        Box::pin(async move {
            self.sent.lock().unwrap().push(Sent {
                url: request.endpoint.expose_for_transport().to_owned(),
                authorization: request
                    .headers
                    .iter_for_wire()
                    .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
                    .map(|(_, v)| v.to_owned()),
                body: serde_json::from_slice(&request.body).unwrap(),
            });
            let mut headers = HttpHeaders::new();
            headers.append("content-type", "text/event-stream")?;
            Ok(HttpResponse {
                status: 200,
                status_text: "OK".into(),
                headers,
                body: Box::new(Body(Some(self.response.to_vec()))),
            })
        })
    }
}
struct Jitter;
impl RetryJitter for Jitter {
    fn sample(&self) -> f64 {
        0.5
    }
}
fn directory() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "pirua-acp-models-{}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}
fn write_models(root: &Path, value: Value) -> PathBuf {
    std::fs::create_dir_all(root).unwrap();
    let path = root.join("models.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap()).unwrap();
    path
}
struct Harness {
    root: PathBuf,
    runtime: RequestRuntime,
    registry: Arc<ModelsRegistry>,
    sent: Arc<Mutex<Vec<Sent>>>,
}
async fn harness(
    rt: &tokio::runtime::Runtime,
    models: Value,
    env: BTreeMap<&'static str, &'static str>,
) -> Harness {
    let root = directory();
    let config = ModelsConfig::load(&write_models(&root, models)).unwrap();
    let runtime = RequestRuntime::with_noop_telemetry(
        CancellationToken::new(),
        Arc::new(TokioClock::new(rt.handle().clone())),
        Arc::new(Spawner(rt.handle().clone())),
    );
    let sent = Arc::new(Mutex::new(vec![]));
    let transport = Arc::new(HttpTransport::new(
        Arc::new(Gateway {
            sent: sent.clone(),
            response: VLLM_RECORDING,
        }),
        Arc::new(Jitter),
    ));
    let credentials = Arc::new(Credentials::open(&root.join("auth.sqlite")).unwrap());
    let registry = compose_registry(
        credentials,
        Arc::new(Environment(env)),
        transport.clone(),
        &runtime,
        None,
    )
    .await
    .unwrap();
    register_configured_providers(&registry, &config, transport)
        .await
        .unwrap();
    Harness {
        root,
        runtime,
        registry,
        sent,
    }
}
fn gateway_models() -> Value {
    json!({"providers":{"gateway":{
        "name":"Gateway",
        "baseUrl":"https://gateway.invalid/v1",
        "api":"openai-completions",
        "apiKey":"literal-gateway-key",
        "compat":{"supportsDeveloperRole":false},
        "models":[{"id":"deepseekv4-flash","reasoning":true}]
    }}})
}
async fn ask(h: &Harness, model: Model) -> AssistantMessage {
    h.registry
        .stream_simple(
            model,
            serde_json::from_value(json!({"systemPrompt":"system","messages":[
                {"role":"user","content":"1+2+...+100","timestamp":1}]}))
            .unwrap(),
            ModelsRequestOverrides::default(),
            SimpleStreamOptions {
                wire: Default::default(),
                runtime: h.runtime.clone(),
            },
        )
        .result()
        .await
}
fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn configured_provider_streams_through_its_base_url_with_its_key_and_compat() {
    let rt = rt();
    rt.block_on(async {
        let h = harness(&rt, gateway_models(), BTreeMap::new()).await;
        let provider = ProviderId::new("gateway").unwrap();
        assert_eq!(
            h.registry.get_provider(&provider).unwrap().name(),
            "Gateway"
        );
        let models = h.registry.get_models(Some(&provider));
        assert_eq!(models.len(), 1);
        let model = models[0].clone();
        // Unset model fields take the upstream models.json defaults.
        let value = serde_json::to_value(&model).unwrap();
        assert_eq!(value["name"], "deepseekv4-flash");
        assert_eq!(value["api"], "openai-completions");
        assert_eq!(value["baseUrl"], "https://gateway.invalid/v1");
        assert_eq!(value["contextWindow"], 128000);
        assert_eq!(value["maxTokens"], 16384);
        assert_eq!(value["input"], json!(["text"]));
        assert_eq!(value["reasoning"], true);
        assert_eq!(value["compat"], json!({"supportsDeveloperRole":false}));
        assert_eq!(
            value["cost"],
            json!({"input":0.0,"output":0.0,"cacheRead":0.0,"cacheWrite":0.0})
        );

        let message = ask(&h, model).await;
        let sent = h.sent.lock().unwrap().clone();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].url, "https://gateway.invalid/v1/chat/completions");
        assert_eq!(
            sent[0].authorization.as_deref(),
            Some("Bearer literal-gateway-key")
        );
        assert_eq!(sent[0].body["model"], "deepseekv4-flash");
        assert_eq!(sent[0].body["messages"][0]["role"], "system");
        assert_eq!(message.stop_reason, StopReason::Stop);
        let content = serde_json::to_value(&message.content).unwrap();
        assert_eq!(content[0]["thinkingSignature"], "reasoning");
        assert!(content[1]["text"].as_str().unwrap().contains("5050"));
        std::fs::remove_dir_all(&h.root).unwrap();
    });
}

#[test]
fn application_reads_models_json_from_its_config_dir_and_selects_a_configured_model() {
    let rt = rt();
    rt.block_on(async {
        let root = directory();
        write_models(&root.join("config"), gateway_models());
        let options = pi_acp_server::Options::parse([
            "--data-dir".into(),
            root.join("data").to_string_lossy().into_owned(),
            "--config-dir".into(),
            root.join("config").to_string_lossy().into_owned(),
            "--model".into(),
            "gateway/deepseekv4-flash".into(),
        ])
        .unwrap();
        let app = pi_acp_server::Application::new(options).await.unwrap();
        assert_eq!(app.default_model.provider.as_str(), "gateway");
        assert_eq!(app.default_model.id.as_str(), "deepseekv4-flash");
        assert!(app.models.iter().any(|m| m.provider.as_str() == "deepseek"));
        let registry = app.registry.as_ref().unwrap();
        assert!(
            registry
                .get_provider(&ProviderId::new("gateway").unwrap())
                .is_some()
        );
        std::fs::remove_dir_all(root).unwrap();
    });
}

fn keyed_models(api_key: &str) -> Value {
    let mut models = gateway_models();
    models["providers"]["gateway"]["apiKey"] = json!(api_key);
    models
}

#[test]
fn configured_api_key_interpolates_the_environment_at_request_time() {
    let rt = rt();
    rt.block_on(async {
        let provider = ProviderId::new("gateway").unwrap();
        let never = CancellationToken::new();
        for (template, expected) in [
            ("$GATEWAY_KEY", "env-gateway-key"),
            ("${GATEWAY_KEY}", "env-gateway-key"),
            ("pre-${GATEWAY_KEY}-$$-$!x", "pre-env-gateway-key-$-!x"),
            ("$1literal", "$1literal"),
            ("a${}b", "a${}b"),
            ("${1x}", "${1x}"),
            ("k${GATEWAY_KEY", "k${GATEWAY_KEY"),
            ("key$", "key$"),
        ] {
            let h = harness(
                &rt,
                keyed_models(template),
                BTreeMap::from([("GATEWAY_KEY", "env-gateway-key")]),
            )
            .await;
            let check = h.registry.check_auth(&provider, &never).await.unwrap();
            assert_eq!(check.unwrap().source.as_deref(), Some("configured API key"));
            let model = h.registry.get_models(Some(&provider))[0].clone();
            assert_eq!(ask(&h, model).await.stop_reason, StopReason::Stop);
            let expected = format!("Bearer {expected}");
            assert_eq!(
                h.sent.lock().unwrap()[0].authorization.as_deref(),
                Some(expected.as_str()),
                "{template}"
            );
            std::fs::remove_dir_all(&h.root).unwrap();
        }
        // An unset or empty variable leaves the provider without a credential.
        for env in [BTreeMap::new(), BTreeMap::from([("GATEWAY_KEY", "")])] {
            let h = harness(&rt, keyed_models("$GATEWAY_KEY"), env).await;
            assert!(
                h.registry
                    .check_auth(&provider, &never)
                    .await
                    .unwrap()
                    .is_none()
            );
            let model = h.registry.get_models(Some(&provider))[0].clone();
            assert_eq!(ask(&h, model).await.stop_reason, StopReason::Error);
            assert!(
                h.sent.lock().unwrap().is_empty(),
                "no request without a key"
            );
            std::fs::remove_dir_all(&h.root).unwrap();
        }
    });
}

struct Interaction;
impl AuthInteraction for Interaction {
    fn prompt<'a>(
        &'a self,
        prompt: AuthPrompt,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<String, AuthInteractionError>> {
        assert!(matches!(prompt, AuthPrompt::Secret { .. }));
        Box::pin(async { Ok("stored-gateway-key".into()) })
    }
    fn notify(&self, _: AuthEvent) -> Result<(), AuthInteractionError> {
        Ok(())
    }
}

#[test]
fn a_logged_in_key_takes_precedence_over_the_configured_key() {
    let rt = rt();
    rt.block_on(async {
        let provider = ProviderId::new("gateway").unwrap();
        let never = CancellationToken::new();
        let mut keyless = gateway_models();
        keyless["providers"]["gateway"]
            .as_object_mut()
            .unwrap()
            .remove("apiKey");
        for (models, before) in [
            (gateway_models(), Some("configured API key")),
            (keyless, None),
        ] {
            let h = harness(&rt, models, BTreeMap::new()).await;
            let check = h.registry.check_auth(&provider, &never).await.unwrap();
            assert_eq!(check.and_then(|c| c.source).as_deref(), before);
            h.registry
                .login(&provider, AuthType::ApiKey, &Interaction, &never)
                .await
                .unwrap();
            let check = h.registry.check_auth(&provider, &never).await.unwrap();
            assert_eq!(check.unwrap().source.as_deref(), Some("stored credential"));
            let model = h.registry.get_models(Some(&provider))[0].clone();
            assert_eq!(ask(&h, model).await.stop_reason, StopReason::Stop);
            assert_eq!(
                h.sent.lock().unwrap()[0].authorization.as_deref(),
                Some("Bearer stored-gateway-key")
            );
            std::fs::remove_dir_all(&h.root).unwrap();
        }
    });
}

fn provider(models: &mut Value) -> &mut serde_json::Map<String, Value> {
    models["providers"]["gateway"].as_object_mut().unwrap()
}

fn edit(change: impl FnOnce(&mut Value)) -> Value {
    let mut models = gateway_models();
    change(&mut models);
    models
}

#[test]
fn invalid_or_unsupported_configuration_is_rejected_with_its_location() {
    let cases: Vec<(&str, Value, &str)> = vec![
        (
            "unknown top-level field",
            edit(|m| m["extra"] = json!(1)),
            "unknown field `extra`",
        ),
        (
            "upstream modelOverrides",
            edit(|m| {
                provider(m).insert("modelOverrides".into(), json!({}));
            }),
            "provider \"gateway\": unknown field `modelOverrides`",
        ),
        (
            "upstream headers",
            edit(|m| {
                provider(m).insert("headers".into(), json!({"x":"y"}));
            }),
            "provider \"gateway\": unknown field `headers`",
        ),
        (
            "upstream oauth",
            edit(|m| {
                provider(m).insert("oauth".into(), json!("radius"));
            }),
            "provider \"gateway\": unknown field `oauth`",
        ),
        (
            "model-level baseUrl",
            edit(|m| {
                m["providers"]["gateway"]["models"][0]["baseUrl"] = json!("https://x.invalid")
            }),
            "provider \"gateway\" model \"deepseekv4-flash\": unknown field `baseUrl`",
        ),
        (
            "missing api",
            edit(|m| {
                provider(m).remove("api");
            }),
            "provider \"gateway\": missing field `api`",
        ),
        (
            "other api",
            edit(|m| m["providers"]["gateway"]["api"] = json!("anthropic-messages")),
            "provider \"gateway\": api must be \"openai-completions\"",
        ),
        (
            "missing baseUrl",
            edit(|m| {
                provider(m).remove("baseUrl");
            }),
            "provider \"gateway\": missing field `baseUrl`",
        ),
        (
            "relative baseUrl",
            edit(|m| m["providers"]["gateway"]["baseUrl"] = json!("gateway.invalid/v1")),
            "provider \"gateway\": baseUrl must be an absolute http or https URL",
        ),
        (
            "no models",
            edit(|m| m["providers"]["gateway"]["models"] = json!([])),
            "provider \"gateway\": at least one model is required",
        ),
        (
            "duplicate model",
            edit(|m| m["providers"]["gateway"]["models"] = json!([{"id":"a"},{"id":"a"}])),
            "provider \"gateway\" model \"a\": duplicate model id",
        ),
        (
            "command key",
            edit(|m| m["providers"]["gateway"]["apiKey"] = json!("!cat key.txt")),
            "provider \"gateway\": apiKey commands (\"!...\") are not supported",
        ),
        (
            "empty key",
            edit(|m| m["providers"]["gateway"]["apiKey"] = json!("")),
            "provider \"gateway\": apiKey must not be empty",
        ),
        (
            "compat typo",
            edit(|m| m["providers"]["gateway"]["compat"] = json!({"supportsDeveloperRol":false})),
            "provider \"gateway\" model \"deepseekv4-flash\": invalid model compatibility data",
        ),
        (
            "zero context",
            edit(|m| m["providers"]["gateway"]["models"][0]["contextWindow"] = json!(0)),
            "provider \"gateway\" model \"deepseekv4-flash\": contextWindow must be a positive integer",
        ),
        (
            "zero output",
            edit(|m| m["providers"]["gateway"]["models"][0]["maxTokens"] = json!(0)),
            "provider \"gateway\" model \"deepseekv4-flash\": maxTokens must be a positive integer",
        ),
        (
            "null apiKey",
            edit(|m| m["providers"]["gateway"]["apiKey"] = Value::Null),
            "provider \"gateway\": apiKey must be a string",
        ),
        (
            "null provider name",
            edit(|m| m["providers"]["gateway"]["name"] = Value::Null),
            "provider \"gateway\": invalid type: null, expected a string",
        ),
        (
            "empty provider name",
            edit(|m| m["providers"]["gateway"]["name"] = json!("")),
            "provider \"gateway\": name must not be empty",
        ),
        (
            "empty model name",
            edit(|m| m["providers"]["gateway"]["models"][0]["name"] = json!("")),
            "provider \"gateway\" model \"deepseekv4-flash\": name must not be empty",
        ),
        (
            "null compat",
            edit(|m| m["providers"]["gateway"]["compat"] = Value::Null),
            "provider \"gateway\": invalid type: null",
        ),
        (
            "null reasoning",
            edit(|m| m["providers"]["gateway"]["models"][0]["reasoning"] = Value::Null),
            "provider \"gateway\" model \"deepseekv4-flash\": invalid type: null",
        ),
        (
            "non-string $schema",
            edit(|m| m["$schema"] = json!(5)),
            "expected a string",
        ),
        (
            "baseUrl whitespace",
            edit(|m| m["providers"]["gateway"]["baseUrl"] = json!("https://gateway.invalid/v1 ")),
            "provider \"gateway\": baseUrl must not have surrounding whitespace",
        ),
        (
            "baseUrl query",
            edit(|m| {
                m["providers"]["gateway"]["baseUrl"] = json!("https://gateway.invalid/v1?x=1")
            }),
            "provider \"gateway\": baseUrl must not contain a query or fragment",
        ),
        (
            "baseUrl fragment",
            edit(|m| m["providers"]["gateway"]["baseUrl"] = json!("https://gateway.invalid/v1#x")),
            "provider \"gateway\": baseUrl must not contain a query or fragment",
        ),
        (
            "upstream built-in override",
            json!({"providers":{"anthropic":{"baseUrl":"https://proxy.example.com/v1"}}}),
            "provider \"anthropic\" is built in and cannot be redefined",
        ),
        (
            "built-in id in another case",
            json!({"providers":{"DeepSeek":gateway_models()["providers"]["gateway"].clone()}}),
            "provider \"DeepSeek\" is built in and cannot be redefined",
        ),
        (
            "ids differing only in case",
            json!({"providers":{
                "gateway2":gateway_models()["providers"]["gateway"].clone(),
                "Gateway2":gateway_models()["providers"]["gateway"].clone()}}),
            "provider \"Gateway2\" differs from \"gateway2\" only in letter case",
        ),
        (
            "provider id",
            json!({"providers":{"a:b":gateway_models()["providers"]["gateway"].clone()}}),
            "provider \"a:b\": provider ids may only contain ASCII letters, digits, '.', '_' and '-'",
        ),
    ];
    let root = directory();
    for (name, models, expected) in cases {
        let error = ModelsConfig::load(&write_models(&root, models))
            .expect_err(name)
            .to_string();
        assert!(error.contains("models.json"), "{name}: {error}");
        assert!(error.contains(expected), "{name}: {error}");
    }
    // Rejections never echo a secret back into the error text.
    for (name, models, expected, secret) in [
        (
            "numeric key",
            edit(|m| m["providers"]["gateway"]["apiKey"] = json!(9876543210u64)),
            "provider \"gateway\": apiKey must be a string",
            "9876543210",
        ),
        (
            "credentials in baseUrl",
            edit(|m| {
                m["providers"]["gateway"]["baseUrl"] =
                    json!("https://user:hunter2@gateway.invalid/v1")
            }),
            "provider \"gateway\": baseUrl must not embed credentials; use apiKey",
            "hunter2",
        ),
    ] {
        let error = ModelsConfig::load(&write_models(&root, models))
            .expect_err(name)
            .to_string();
        assert!(error.contains(expected), "{name}: {error}");
        assert!(!error.contains(secret), "{name} leaked its secret: {error}");
    }
    std::fs::write(root.join("models.json"), "{").unwrap();
    let error = ModelsConfig::load(&root.join("models.json"))
        .unwrap_err()
        .to_string();
    assert!(error.contains("models.json"), "{error}");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn configured_providers_never_replace_registered_providers() {
    let rt = rt();
    rt.block_on(async {
        let h = harness(&rt, gateway_models(), BTreeMap::new()).await;
        // Every live built-in is reserved before any field is read.
        for provider in h.registry.get_providers() {
            if provider.id().as_str() == "gateway" {
                continue;
            }
            let models =
                json!({"providers":{provider.id().as_str():{"baseUrl":"https://x.invalid/v1"}}});
            let error = ModelsConfig::load(&write_models(&h.root, models))
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("is built in and cannot be redefined"),
                "{error}"
            );
        }
        // Registration re-checks the live registry and registers nothing on a collision.
        let both = json!({"providers":{
            "aaa":gateway_models()["providers"]["gateway"].clone(),
            "gateway":gateway_models()["providers"]["gateway"].clone()}});
        let config = ModelsConfig::load(&write_models(&h.root, both)).unwrap();
        let transport = Arc::new(HttpTransport::new(
            Arc::new(Gateway {
                sent: Arc::default(),
                response: VLLM_RECORDING,
            }),
            Arc::new(Jitter),
        ));
        let error = register_configured_providers(&h.registry, &config, transport)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("provider \"gateway\" is already registered"),
            "{error}"
        );
        assert!(
            h.registry
                .get_provider(&ProviderId::new("aaa").unwrap())
                .is_none()
        );
        std::fs::remove_dir_all(&h.root).unwrap();
    });
}

#[test]
fn nested_compat_merges_provider_and_model_chat_template_arguments() {
    let rt = rt();
    rt.block_on(async {
        // The vLLM 0.21 profile from the recorded-stream README, split across levels.
        let models = edit(|m| {
            m["providers"]["gateway"]["compat"] = json!({
                "supportsDeveloperRole":false,"thinkingFormat":"chat-template","maxTokensField":"max_tokens",
                "chatTemplateKwargs":{"thinking":{"$var":"thinking.enabled"}}});
            m["providers"]["gateway"]["models"][0]["compat"] = json!({
                "maxTokensField":"max_completion_tokens",
                "chatTemplateKwargs":{"reasoning_effort":{"$var":"thinking.effort","omitWhenOff":true}}});
        });
        let h = harness(&rt, models, BTreeMap::new()).await;
        let model = h.registry.get_models(Some(&ProviderId::new("gateway").unwrap()))[0].clone();
        let message = h
            .registry
            .stream_simple(
                model,
                serde_json::from_value(json!({"systemPrompt":"system","messages":[
                    {"role":"user","content":"1+2+...+100","timestamp":1}]}))
                .unwrap(),
                ModelsRequestOverrides::default(),
                SimpleStreamOptions {
                    wire: SimpleStreamOptionsWire {
                        reasoning: Some(serde_json::from_value(json!("high")).unwrap()),
                        ..Default::default()
                    },
                    runtime: h.runtime.clone(),
                },
            )
            .result()
            .await;
        assert_eq!(message.stop_reason, StopReason::Stop);
        let body = h.sent.lock().unwrap()[0].body.clone();
        assert_eq!(
            body["chat_template_kwargs"],
            json!({"thinking":true,"reasoning_effort":"high"})
        );
        assert!(body.get("thinking").is_none() && body.get("reasoning_effort").is_none());
        assert_eq!(body["messages"][0]["role"], "system");
        // A model key overrides the same provider key.
        assert!(body.get("max_completion_tokens").is_some() && body.get("max_tokens").is_none());
        std::fs::remove_dir_all(&h.root).unwrap();
    });
}

#[test]
fn configured_providers_leave_default_model_selection_working() {
    let rt = rt();
    rt.block_on(async {
        let root = directory();
        write_models(&root.join("config"), gateway_models());
        let options = pi_acp_server::Options::parse([
            "--data-dir".into(),
            root.join("data").to_string_lossy().into_owned(),
            "--config-dir".into(),
            root.join("config").to_string_lossy().into_owned(),
        ])
        .unwrap();
        let app = pi_acp_server::Application::new(options).await.unwrap();
        assert!(app.models.contains(&app.default_model));
        assert!(app.models.iter().any(|m| m.provider.as_str() == "gateway"));
        std::fs::remove_dir_all(root).unwrap();
    });
}

#[test]
fn an_invalid_file_stops_production_startup_before_any_side_effect() {
    let rt = rt();
    rt.block_on(async {
        let root = directory();
        write_models(
            &root.join("config"),
            json!({"providers":{"gateway":{"baseUrl":"gateway.invalid"}}}),
        );
        let options = |fixture: bool| {
            let mut args = vec![
                "--data-dir".to_owned(),
                root.join("data").to_string_lossy().into_owned(),
                "--config-dir".to_owned(),
                root.join("config").to_string_lossy().into_owned(),
            ];
            if fixture {
                args.push("--fixture-provider".to_owned());
            }
            pi_acp_server::Options::parse(args).unwrap()
        };
        let error = pi_acp_server::Application::new(options(false))
            .await
            .err()
            .expect("an invalid models.json stops startup")
            .to_string();
        assert!(error.contains("models.json"), "{error}");
        assert!(
            !root.join("data").exists(),
            "nothing is created before validation"
        );
        // The deterministic fixture provider never reads models.json.
        pi_acp_server::Application::new(options(true))
            .await
            .unwrap();
        std::fs::remove_dir_all(root).unwrap();
    });
}

#[test]
fn configured_providers_register_in_file_order() {
    let rt = rt();
    rt.block_on(async {
        let entry = gateway_models()["providers"]["gateway"].clone();
        let models: Value = serde_json::from_str(&format!(
            r#"{{"providers":{{"zeta":{entry},"alpha":{entry}}}}}"#
        ))
        .unwrap();
        let h = harness(&rt, models, BTreeMap::new()).await;
        let ids = h
            .registry
            .get_providers()
            .iter()
            .map(|p| p.id().as_str().to_owned())
            .filter(|id| id == "zeta" || id == "alpha")
            .collect::<Vec<_>>();
        assert_eq!(ids, ["zeta", "alpha"]);
        std::fs::remove_dir_all(&h.root).unwrap();
    });
}

#[test]
fn integral_float_token_counts_are_accepted_and_fractions_rejected() {
    let root = directory();
    let config = ModelsConfig::load(&write_models(
        &root,
        edit(|m| {
            m["providers"]["gateway"]["models"][0]["contextWindow"] = json!(1e6);
            m["providers"]["gateway"]["models"][0]["maxTokens"] = json!(32768.0);
        }),
    ));
    assert!(config.is_ok(), "{config:?}");
    let error = ModelsConfig::load(&write_models(
        &root,
        edit(|m| m["providers"]["gateway"]["models"][0]["maxTokens"] = json!(1.5)),
    ))
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("model \"deepseekv4-flash\": maxTokens must be a positive integer"),
        "{error}"
    );
    std::fs::remove_dir_all(root).unwrap();
}
