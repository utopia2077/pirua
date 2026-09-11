#![cfg(feature = "dev-catalog-tools")]

use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "pi-catalog-binaries-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}
fn binaries() -> [(&'static str, &'static str); 2] {
    [
        (env!("CARGO_BIN_EXE_catalog_gen"), "text"),
        (env!("CARGO_BIN_EXE_image_catalog_gen"), "images"),
    ]
}
fn invoke(binary: &str, cwd: &Path, args: &[&str]) -> Output {
    Command::new(binary)
        .args(args)
        .current_dir(cwd)
        .env("PYTHONHOME", "/missing-synthetic-python-home")
        .output()
        .unwrap()
}
fn success(output: Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let dest = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &dest);
        } else {
            fs::copy(entry.path(), dest).unwrap();
        }
    }
}

#[test]
fn actual_binaries_help_default_check_and_exact_deterministic_family_outputs() {
    let temporary = Temporary::new();
    for (binary, family) in binaries() {
        let help = invoke(binary, &temporary.0, &["--help"]);
        assert!(help.status.success());
        let help = String::from_utf8(help.stdout).unwrap();
        assert!(
            help.contains("offline")
                && help.contains("workspace root")
                && help.contains("NEW")
                && help.contains("cargo run -p pi-ai")
        );
        success(invoke(binary, &root(), &[]));
        success(invoke(binary, &root(), &["--check"]));
        let first = temporary.0.join(format!("{family} models.json"));
        let second = temporary.0.join(format!("{family}-again.json"));
        success(invoke(
            binary,
            &root(),
            &["--output", first.to_str().unwrap()],
        ));
        success(invoke(
            binary,
            &root(),
            &["--output", second.to_str().unwrap()],
        ));
        let bytes = fs::read(&first).unwrap();
        assert_eq!(bytes, fs::read(second).unwrap());
        let artifact: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(artifact["family"], family);
        assert_eq!(artifact["schemaVersion"], 1);
        let names = if family == "text" {
            vec![
                "openai",
                "google",
                "anthropic",
                "deepseek",
                "openai-codex",
                "openrouter",
            ]
        } else {
            vec!["openrouter"]
        };
        let catalogs = artifact["catalogs"].as_array().unwrap();
        assert_eq!(catalogs.len(), names.len());
        let mut count = 0;
        for (entry, provider) in catalogs.iter().zip(names) {
            assert_eq!(entry["provider"], provider);
            let path = root().join(format!(
                "conformance/catalog/generated/{family}/{provider}.json"
            ));
            let expected: Value = serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
            assert_eq!(entry["models"], expected);
            count += entry["models"].as_array().unwrap().len();
        }
        assert_eq!(count, if family == "text" { 450 } else { 52 });
        let expected: Value = serde_json::from_slice(
            &fs::read(root().join("conformance/catalog/generated/provenance.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(artifact["provenance"], expected);
        let refusal = invoke(binary, &root(), &["--output", first.to_str().unwrap()]);
        assert!(!refusal.status.success());
        assert_eq!(fs::read(first).unwrap(), bytes);
    }
}

#[test]
fn actual_binaries_reject_unknown_arguments_and_missing_workspace_without_output() {
    let temporary = Temporary::new();
    let output = temporary.0.join("never-created.json");
    for (binary, _) in binaries() {
        for args in [
            vec!["--unknown"],
            vec!["--help", "--unknown"],
            vec!["--check", "--check"],
            vec!["--output"],
            vec!["--check", "--output", output.to_str().unwrap()],
        ] {
            assert!(!invoke(binary, &root(), &args).status.success());
        }
        let missing = invoke(
            binary,
            &temporary.0,
            &["--output", output.to_str().unwrap()],
        );
        assert!(!missing.status.success());
        assert!(String::from_utf8_lossy(&missing.stderr).contains("workspace root"));
        assert!(!output.exists());
    }
}

#[test]
fn actual_binaries_reject_changed_capture_source_and_canonical_output_after_generation() {
    let temporary = Temporary::new();
    let fixture = temporary.0.join("fixture");
    copy_tree(
        &root().join("conformance/catalog"),
        &fixture.join("conformance/catalog"),
    );
    fs::create_dir_all(fixture.join("scripts")).unwrap();
    for name in ["catalog_cli.py", "generate_selected_catalogs.py"] {
        fs::copy(
            root().join("scripts").join(name),
            fixture.join("scripts").join(name),
        )
        .unwrap();
    }
    fs::copy(
        root().join("upstream-map.toml"),
        fixture.join("upstream-map.toml"),
    )
    .unwrap();
    let output = temporary.0.join("never-created.json");
    for file in [
        "capture.json",
        "source-manifest.json",
        "generated/provenance.json",
    ] {
        let path = fixture.join("conformance/catalog").join(file);
        let original = fs::read(&path).unwrap();
        let mut changed = original.clone();
        changed.push(b' ');
        fs::write(&path, changed).unwrap();
        for (binary, _) in binaries() {
            assert!(!invoke(binary, &fixture, &["--check"]).status.success());
            assert!(
                !invoke(binary, &fixture, &["--output", output.to_str().unwrap()])
                    .status
                    .success()
            );
            assert!(!output.exists());
        }
        fs::write(path, original).unwrap();
    }
}
