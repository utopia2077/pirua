#![cfg(feature = "compat")]
use pi_ai::{AuthContext, AuthContextError, BoxFuture, CancellationToken, ProviderEnv};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::Path, process::Command, sync::Mutex};
struct Host {
    values: BTreeMap<String, String>,
    reads: Mutex<Vec<String>>,
}
impl AuthContext for Host {
    fn env<'a>(
        &'a self,
        name: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<Option<String>, AuthContextError>> {
        Box::pin(async move {
            self.reads.lock().unwrap().push(name.into());
            Ok(self.values.get(name).cloned())
        })
    }
    fn file_exists<'a>(
        &'a self,
        _: &'a str,
        _: &'a CancellationToken,
    ) -> BoxFuture<'a, Result<bool, AuthContextError>> {
        panic!("excluded discovery must not execute")
    }
}
#[test]
fn s05_selected_legacy_env_complete_source_and_facade_results() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let dir = root.join("conformance/ai-domain/s05-export-closure");
    let output = Command::new("node")
        .arg(dir.join("source-legacy-env.mjs"))
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .output()
        .unwrap();
    assert!(output.status.success(), "complete legacy source failed");
    let source: Value = serde_json::from_slice(&output.stdout).unwrap();
    let expected: Value =
        serde_json::from_slice(&std::fs::read(dir.join("legacy-env-observations.json")).unwrap())
            .unwrap();
    assert!(source == expected, "complete source observations changed");
    assert_eq!(expected["cases"].as_array().unwrap().len(), 37);
    assert_eq!(expected["excludedDiscoveryCalls"], json!([]));
    assert_eq!(
        expected["constants"],
        json!([
            pi_ai::compat::ANTHROPIC_AUTH_TOKEN_ENV,
            pi_ai::compat::ANTHROPIC_OAUTH_TOKEN_ENV,
            pi_ai::compat::ANTHROPIC_API_KEY_ENV
        ])
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    rt.block_on(async {
        for case in expected["cases"].as_array().unwrap() {
            let host = Host {
                values: serde_json::from_value(case["ambient"].clone()).unwrap(),
                reads: Mutex::new(vec![]),
            };
            let env = ProviderEnv::new(serde_json::from_value(case["scoped"].clone()).unwrap());
            let c = CancellationToken::new();
            let id = case["provider"].as_str().unwrap();
            let keys = pi_ai::compat::find_env_keys(id, Some(&env), &host, &c)
                .await
                .unwrap();
            assert_eq!(
                serde_json::to_value(keys).unwrap(),
                case["observed"]["keys"]
            );
            assert_eq!(
                json!(std::mem::take(&mut *host.reads.lock().unwrap())),
                case["observed"]["findReads"]
            );
            let key = pi_ai::compat::get_env_api_key(id, Some(&env), &host, &c)
                .await
                .unwrap();
            let expected_key = case["observed"]["keyIndex"]
                .as_u64()
                .map(|index| expected["values"][index as usize].as_str().unwrap());
            assert!(
                key.as_ref().map(|v| v.expose_secret()) == expected_key,
                "selected credential value differs from source"
            );
            assert_eq!(
                json!(*host.reads.lock().unwrap()),
                case["observed"]["getReads"]
            );
        }
    });
    // Discriminating source controls: fallback and Anthropic bearer/priority differ.
    let cases = expected["cases"].as_array().unwrap();
    let at = |id: &str| &cases.iter().find(|v| v["id"] == id).unwrap()["observed"];
    assert_ne!(
        at("openai-empty-scoped-fallback"),
        at("openai-whitespace-scoped")
    );
    assert_ne!(
        at("anthropic-bearer-only")["keyIndex"],
        at("anthropic-all")["keyIndex"]
    );
    assert_ne!(
        at("openai-ambient")["getReads"],
        at("openai-ambient")["findReads"]
    );
}
