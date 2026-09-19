//! Run the read-only schema gate in the ordinary all-target Cargo test pass.

use std::process::Command;

#[test]
fn reviewed_wire_and_bounded_echo_model_match_checked_authority() {
    let output = Command::new("sh")
        .arg("schema/verify.sh")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("run schema verifier");
    assert!(
        output.status.success(),
        "schema verifier failed ({}):\nstdout:\n{}\nstderr:\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
