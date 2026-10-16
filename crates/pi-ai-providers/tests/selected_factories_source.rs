#[test]
fn selected_feature_manifests_and_sources_exclude_retired_builtins() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    for name in [
        "provider-openai-codex",
        "provider-openrouter",
        "images-openrouter",
        "wire-bedrock",
        "wire-google-vertex",
        "wire-azure",
    ] {
        assert!(!manifest.contains(name), "{name}");
    }
    for name in ["openai_codex", "openrouter", "openrouter_images"] {
        assert!(
            !root
                .join("src/providers")
                .join(format!("{name}.rs"))
                .exists()
        );
    }
}
