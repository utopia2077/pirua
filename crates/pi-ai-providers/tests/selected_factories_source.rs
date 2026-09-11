#[test]
fn complete_pinned_selected_factory_source_and_capture_are_fresh() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("/usr/bin/node")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("NODE_NO_WARNINGS", "1")
        .arg(root.join("conformance/providers/selected/source.mjs"))
        .arg("--check")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("seven complete bodies"));
}
#[cfg(all(feature = "all-providers", feature = "all-images"))]
#[test]
fn actual_linked_facade_assembly_and_partial_profiles_execute() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("python3")
        .arg(root.join("conformance/providers/selected/linked.py"))
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("ACTUAL_LINKED_FACADE_FIVE_TESTS_PASSED")
    );
}
