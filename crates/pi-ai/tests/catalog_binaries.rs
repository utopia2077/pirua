#![cfg(feature = "dev-catalog-tools")]
#[test]
fn release_catalog_binary_verifies_pinned_source_projection() {
    let result = std::process::Command::new(env!("CARGO_BIN_EXE_catalog_gen"))
        .arg("--check")
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("85 chat models"));
    assert!(
        !std::process::Command::new(env!("CARGO_BIN_EXE_catalog_gen"))
            .arg("--network")
            .status()
            .unwrap()
            .success()
    );
}

#[test]
fn release_catalog_checker_rejects_modified_evidence_and_outputs() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let result = std::process::Command::new("python3")
        .arg(root.join("conformance/catalog/v1.4.0/check_negative.py"))
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("rejects modified archive"));
}
