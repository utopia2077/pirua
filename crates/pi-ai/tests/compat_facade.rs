#![cfg(feature = "compat")]
use std::{path::Path, process::Command};
fn root() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn source(args: &[&str]) -> std::process::Output {
    Command::new("/usr/bin/node")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .arg("--experimental-vm-modules")
        .arg(root().join("conformance/ai-compat/facade/source.mjs"))
        .args(args)
        .output()
        .unwrap()
}
#[test]
fn complete_source_facade_and_discriminating_registry_controls() {
    let output = source(&["--check"]);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("FACADE_COMPLETE_SOURCE_PASS"));
    for mutation in ["nonclobber", "remembered", "shortcut", "imageupsert"] {
        let output = source(&[&format!("--causal={mutation}")]);
        assert!(!output.status.success(), "{mutation}");
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("AssertionError"),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
#[test]
fn actual_linked_facade_executes_nine_non_native_contract_tests() {
    let output = Command::new("python3")
        .arg(root().join("conformance/ai-compat/facade/run.py"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("ACTUAL_LINKED_FACADE_9_EXACT_TESTS_PASSED")
    );
}
#[cfg(feature = "native")]
#[test]
fn actual_linked_facade_executes_physical_codex_snapshot_and_drop_proof() {
    let output = Command::new("python3")
        .arg(root().join("conformance/ai-compat/facade/run.py"))
        .arg("--native")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout)
            .contains("ACTUAL_LINKED_FACADE_11_EXACT_TESTS_PASSED")
    );
}
#[test]
fn actual_root_compat_and_oauth_projection_routes_are_callable() {
    let _: fn(pi_ai::images::ImagesModelsOptions) -> pi_ai::MutableImagesModels =
        pi_ai::create_images_models;
    let _: fn(pi_ai::CreateImagesProviderOptions) -> std::sync::Arc<dyn pi_ai::ImagesProvider> =
        pi_ai::compat::create_images_provider;
    let _: fn(std::sync::Arc<dyn pi_ai::AuthContext>) -> pi_ai::DefaultProviderAuthContext =
        pi_ai::compat::DefaultProviderAuthContext::new;
    let _: fn(
        pi_ai::compat::LazyOAuthMetadata,
        std::sync::Arc<dyn pi_ai::compat::OAuthLoader>,
        std::sync::Arc<dyn pi_ai::TaskSpawner>,
    ) -> pi_ai::LazyOAuth = pi_ai::compat::LazyOAuth::new;
    let _ = pi_ai::env_api_key_auth("synthetic", vec![]);
    let _: Option<pi_ai::compat::OAuthCredentials> = None;
    fn retry_port(_: Option<&dyn pi_ai::compat::RetryCallbacks>) {}
    retry_port(None);
    fn image_port(_: Option<&dyn pi_ai::compat::ImagesGenerator>) {}
    image_port(None);
}
