#![cfg(feature = "extensions")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

const SOURCE_FILES: [&str; 6] = [
    "crates/pi-harness-runtime/src/extensions/receive_host8192.rs",
    "crates/pi-harness-runtime/src/extensions/receive_backing8192.rs",
    "crates/pi-harness-runtime/src/extensions/tool_driver.rs",
    "crates/pi-harness-runtime/src/extensions/domain.rs",
    "crates/pi-harness-runtime/src/extensions/transport.rs",
    "crates/pi-harness-runtime/src/extensions/profiles/connection.rs",
];

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn evidence_dir(label: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("pi-harness-s12-d-adoption-{label}-{nonce}"));
    fs::create_dir_all(&path).expect("evidence directory");
    path
}

fn run_entry(root: &Path, evidence: &Path, extra: &[&str]) -> Output {
    let mut args = vec![
        "tools/verify_receive_backing.py".to_string(),
        "--check".to_string(),
        "--source".to_string(),
        root.display().to_string(),
        "--evidence-dir".to_string(),
        evidence.display().to_string(),
    ];
    args.extend(extra.iter().map(|value| (*value).to_string()));
    Command::new("python3")
        .args(args)
        .current_dir(root)
        .output()
        .expect("native entry process")
}

fn run_checker(root: &Path, evidence: &Path) -> Output {
    Command::new("python3")
        .args([
            "tools/receive_backing_order.py",
            "--source",
            root.to_str().unwrap(),
            "--evidence-dir",
            evidence.to_str().unwrap(),
        ])
        .current_dir(root)
        .output()
        .expect("checker process")
}

fn combined(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn copy_source(root: &Path, label: &str) -> PathBuf {
    let copy_root = evidence_dir(label).join("source-copy");
    fs::create_dir_all(&copy_root).expect("source copy root");
    let git_entry = root.join(".git");
    let gitdir = if git_entry.is_dir() {
        git_entry.canonicalize().expect("checkout git directory")
    } else {
        let contents = fs::read_to_string(&git_entry).expect("worktree git file");
        let raw = contents
            .lines()
            .find_map(|line| line.strip_prefix("gitdir:"))
            .map(str::trim)
            .expect("gitdir entry");
        let path = PathBuf::from(raw);
        let path = if path.is_absolute() {
            path
        } else {
            git_entry.parent().unwrap().join(path)
        };
        path.canonicalize().expect("worktree git directory")
    };
    fs::write(
        copy_root.join(".git"),
        format!("gitdir: {}\n", gitdir.display()),
    )
    .expect("copied git metadata");
    let mut files = SOURCE_FILES.to_vec();
    files.push("tools/receive_backing_order.py");
    for relative in files {
        let destination = copy_root.join(relative);
        fs::create_dir_all(destination.parent().unwrap()).expect("source copy parent");
        fs::copy(root.join(relative), destination).expect("source copy file");
    }
    copy_root
}

fn run_forge_script(root: &Path, evidence: &Path, complete: bool) -> Output {
    const SCRIPT: &str = r#"
from pathlib import Path
import hashlib
import json
import sys

root = Path(sys.argv[1])
evidence = Path(sys.argv[2])
complete = sys.argv[3] == "complete"
source_files = [
    "crates/pi-harness-runtime/src/extensions/receive_host8192.rs",
    "crates/pi-harness-runtime/src/extensions/receive_backing8192.rs",
    "crates/pi-harness-runtime/src/extensions/tool_driver.rs",
    "crates/pi-harness-runtime/src/extensions/domain.rs",
    "crates/pi-harness-runtime/src/extensions/transport.rs",
    "crates/pi-harness-runtime/src/extensions/profiles/connection.rs",
]
profiles = ["default-debug", "default-release", "unified-debug", "unified-release"]
measured_fields = [
    "Hboot", "Hpre", "Qread", "Qreceive", "overlap", "D_req", "P_req",
    "whole_stack", "allocator_peak", "work_cpu_headroom",
    "Memory_SQLite_peer_parity", "EH_inline_target_provider_allocator",
]

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def current_files():
    return {relative: digest(root / relative) for relative in source_files}

status_path = evidence / "native-proof-status.json"
resource_path = evidence / "resource-bound.json"
status = json.loads(status_path.read_text())
resource = json.loads(resource_path.read_text())
status["status"] = "supported"
status["reason"] = "forged complete evidence"
status["resource"] = {"status": "supported", "path": "resource-bound.json", "schema_version": "s12-d-resource-v3"}
(evidence / "images").mkdir()
for profile in profiles:
    image = evidence / "images" / (profile + ".bin")
    image.write_bytes(("forged-" + profile).encode())
    status["profiles"][profile] = {
        "status": "supported",
        "image_path": str(image.relative_to(evidence)),
        "image_sha256": digest(image),
        "invocation": ["rustc", "--profile", profile],
        "profile_identity": profile,
        "source_tree": status["source"]["tree"] if complete else None,
        "toolchain": {"rustc": "forged", "cargo": "forged", "llvm": "forged"} if complete else {},
        "target": "x86_64-unknown-linux-gnu" if complete else None,
        "profile": profile,
        "codegen": {"opt_level": "0", "debug": True, "lto": "off", "panic": "unwind"} if complete else {},
    }
oracle = evidence / "oracle.json"
oracle.write_text(json.dumps({
    "schema_version": "native-order-v1",
    "adoption_marker": "root-adopted-current",
    "kind": "forged",
}) + "\n")
status["oracle"] = {
    "status": "adopted",
    "path": "oracle.json",
    "sha256": digest(oracle),
    "schema_version": "native-order-v1",
    "adoption_marker": "root-adopted-current",
}
files = current_files()
status["source"]["files"] = files
resource["schema_version"] = "s12-d-resource-v3"
resource["status"] = "supported"
resource["declared"] = {
    "frame_bytes": 8192,
    "index_limit": 8193,
    "backing_pool_slots": 2,
    "body_buffer_bytes": 8192,
}
resource["measured"] = {
    field: {"status": "measured", "value": 1, "unit": "forged", "evidence": "forged"}
    for field in measured_fields
} if complete else {}
resource["unknown"] = [] if complete else measured_fields
resource["missing_source_files"] = []
resource["source"]["files"] = files
resource_path.write_text(json.dumps(resource, indent=2, sort_keys=True) + "\n")
status_path.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n")
marker = {
    "schema_version": "s12-d-native-evidence-marker-v1",
    "status": "supported",
    "producer": "receive_backing_build_gate",
    "source": status["source"],
    "profiles": profiles,
    "resource_sha256": digest(resource_path),
    "oracle_sha256": status["oracle"]["sha256"],
}
(evidence / "current-evidence-marker.json").write_text(json.dumps(marker, sort_keys=True) + "\n")
"#;
    Command::new("python3")
        .args([
            "-c",
            SCRIPT,
            root.to_str().unwrap(),
            evidence.to_str().unwrap(),
            if complete { "complete" } else { "incomplete" },
        ])
        .output()
        .expect("forge mutation script")
}

#[test]
fn complete_forged_evidence_stays_refused_without_external_adoption() {
    let root = workspace_root();
    let incomplete_evidence = evidence_dir("incomplete-forge");
    let baseline = run_entry(&root, &incomplete_evidence, &[]);
    assert_eq!(baseline.status.code(), Some(2), "{}", combined(&baseline));
    let incomplete_root = copy_source(&root, "incomplete-source");
    let incomplete_forge = run_forge_script(&incomplete_root, &incomplete_evidence, false);
    assert!(
        incomplete_forge.status.success(),
        "{}",
        combined(&incomplete_forge)
    );
    let incomplete_check = run_checker(&incomplete_root, &incomplete_evidence);
    assert_eq!(
        incomplete_check.status.code(),
        Some(2),
        "{}",
        combined(&incomplete_check)
    );
    assert!(combined(&incomplete_check).contains("resource unknowns remain unresolved"));

    let complete_evidence = evidence_dir("complete-forge");
    let complete_baseline = run_entry(&root, &complete_evidence, &[]);
    assert_eq!(
        complete_baseline.status.code(),
        Some(2),
        "{}",
        combined(&complete_baseline)
    );
    let complete_root = copy_source(&root, "complete-source");
    let complete_forge = run_forge_script(&complete_root, &complete_evidence, true);
    assert!(
        complete_forge.status.success(),
        "{}",
        combined(&complete_forge)
    );
    let complete_check = run_checker(&complete_root, &complete_evidence);
    assert_eq!(
        complete_check.status.code(),
        Some(2),
        "{}",
        combined(&complete_check)
    );
    assert!(combined(&complete_check).contains("source-committed adoption receipt is missing"));

    let restored_evidence = evidence_dir("restored");
    let restored = run_entry(&root, &restored_evidence, &[]);
    assert_eq!(restored.status.code(), Some(2), "{}", combined(&restored));
    assert!(combined(&restored).contains("current four-image/resource/oracle evidence is absent"));
    fs::remove_dir_all(incomplete_evidence).unwrap();
    fs::remove_dir_all(complete_evidence).unwrap();
    fs::remove_dir_all(restored_evidence).unwrap();
    fs::remove_dir_all(incomplete_root.parent().unwrap()).unwrap();
    fs::remove_dir_all(complete_root.parent().unwrap()).unwrap();
}

#[test]
fn schema_and_provenance_mutations_refuse() {
    let root = workspace_root();
    let native_evidence = evidence_dir("native-schema");
    let native = run_entry(&root, &native_evidence, &[]);
    assert_eq!(native.status.code(), Some(2), "{}", combined(&native));
    let native_path = native_evidence.join("native-proof-status.json");
    let native_status = fs::read_to_string(&native_path).unwrap();
    fs::write(
        &native_path,
        native_status.replace("s12-d-native-proof-v3", "forged-native-schema"),
    )
    .unwrap();
    let native_check = run_checker(&root, &native_evidence);
    assert_eq!(
        native_check.status.code(),
        Some(2),
        "{}",
        combined(&native_check)
    );
    assert!(combined(&native_check).contains("native schema version unsupported"));

    let resource_evidence = evidence_dir("resource-schema");
    let resource = run_entry(&root, &resource_evidence, &[]);
    assert_eq!(resource.status.code(), Some(2), "{}", combined(&resource));
    let resource_path = resource_evidence.join("resource-bound.json");
    let resource_record = fs::read_to_string(&resource_path).unwrap();
    fs::write(
        &resource_path,
        resource_record.replace("s12-d-resource-v3", "forged-resource-schema"),
    )
    .unwrap();
    let resource_check = run_checker(&root, &resource_evidence);
    assert_eq!(
        resource_check.status.code(),
        Some(2),
        "{}",
        combined(&resource_check)
    );
    assert!(combined(&resource_check).contains("resource schema version unsupported"));
    fs::remove_dir_all(native_evidence).unwrap();
    fs::remove_dir_all(resource_evidence).unwrap();
}

#[test]
fn timeout_and_profile_arguments_are_coherent() {
    let root = workspace_root();
    let timeout_evidence = evidence_dir("timeout");
    let timeout = run_entry(&root, &timeout_evidence, &["--timeout", "0"]);
    assert_eq!(timeout.status.code(), Some(2), "{}", combined(&timeout));
    assert!(combined(&timeout).contains("--timeout must be positive"));
    fs::remove_dir_all(timeout_evidence).unwrap();

    let evidence = evidence_dir("profiles");
    let entry = run_entry(&root, &evidence, &["--timeout", "5"]);
    assert_eq!(entry.status.code(), Some(2), "{}", combined(&entry));
    assert!(
        fs::read_to_string(evidence.join("checker.log"))
            .unwrap()
            .contains("--timeout 5")
    );
    let binary = evidence.join("receive_backing_build_gate");
    let build = Command::new("rustc")
        .args([
            "--edition=2024",
            root.join("tools/receive_backing_build_gate.rs")
                .to_str()
                .unwrap(),
            "-o",
            binary.to_str().unwrap(),
        ])
        .output()
        .expect("build gate compile");
    assert!(build.status.success(), "{}", combined(&build));

    let valid_profile = Command::new(&binary)
        .args([
            "--source",
            root.to_str().unwrap(),
            "--evidence-dir",
            evidence.to_str().unwrap(),
            "--profile",
            "default-debug",
            "--image",
            evidence.join("missing-image").to_str().unwrap(),
            "--timeout",
            "5",
        ])
        .output()
        .expect("valid profile invocation");
    assert_eq!(
        valid_profile.status.code(),
        Some(2),
        "{}",
        combined(&valid_profile)
    );
    assert!(combined(&valid_profile).contains("exact current image path is missing"));
    assert!(!combined(&valid_profile).contains("four current image profiles are not supplied"));

    let invalid_profile = Command::new(&binary)
        .args([
            "--source",
            root.to_str().unwrap(),
            "--evidence-dir",
            evidence.to_str().unwrap(),
            "--profile",
            "bogus",
            "--timeout",
            "5",
        ])
        .output()
        .expect("invalid profile invocation");
    assert_eq!(
        invalid_profile.status.code(),
        Some(2),
        "{}",
        combined(&invalid_profile)
    );
    assert!(combined(&invalid_profile).contains("unknown current profile"));

    let all_profile_status = Command::new(&binary)
        .args([
            "--source",
            root.to_str().unwrap(),
            "--evidence-dir",
            evidence.to_str().unwrap(),
            "--image",
            evidence.join("missing-image").to_str().unwrap(),
            "--timeout",
            "5",
        ])
        .output()
        .expect("explicit all-profile status invocation");
    assert_eq!(
        all_profile_status.status.code(),
        Some(2),
        "{}",
        combined(&all_profile_status)
    );
    assert!(combined(&all_profile_status).contains("exact current image path is missing"));
    fs::remove_dir_all(evidence).unwrap();
}
