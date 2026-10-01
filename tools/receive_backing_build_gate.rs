//! Current-source native entry probe.
//!
//! This standalone producer remains fail-closed. It accepts one explicit
//! profile invocation or an evidence directory whose status names all four
//! profiles; the checker owns the complete four-profile acceptance predicate.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const SOURCE_FILES: [&str; 3] = [
    "crates/pi-harness-runtime/src/extensions/receive_host8192.rs",
    "crates/pi-harness-runtime/src/extensions/receive_backing8192.rs",
    "crates/pi-harness-runtime/src/extensions/tool_driver.rs",
];
const PROFILES: [&str; 4] = [
    "default-debug",
    "default-release",
    "unified-debug",
    "unified-release",
];
const MARKER_SCHEMA: &str = "s12-d-native-evidence-marker-v1";

fn value(args: &[String], name: &str) -> Option<String> {
    args.windows(2)
        .find(|pair| pair[0] == name)
        .map(|pair| pair[1].clone())
}

fn write_marker(evidence: &Path, status: &str, reason: &str) {
    let marker = format!(
        "{{\"schema_version\":\"{MARKER_SCHEMA}\",\"status\":\"{status}\",\"producer\":\"receive_backing_build_gate\",\"reason\":\"{reason}\"}}\n"
    );
    let _ = fs::write(evidence.join("current-evidence-marker.json"), marker);
}

fn refused(evidence: Option<&Path>, reason: &str) -> ExitCode {
    if let Some(path) = evidence {
        write_marker(path, "refused", reason);
    }
    println!("REFUSED: {reason}");
    ExitCode::from(2)
}

fn all_profiles_in_status(evidence: &Path) -> bool {
    let Ok(status) = fs::read_to_string(evidence.join("native-proof-status.json")) else {
        return false;
    };
    PROFILES
        .iter()
        .all(|profile| status.contains(&format!("\"{profile}\"")))
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let Some(source) = value(&args, "--source").map(PathBuf::from) else {
        return refused(None, "--source is required");
    };
    let Some(evidence) = value(&args, "--evidence-dir").map(PathBuf::from) else {
        return refused(None, "--evidence-dir is required");
    };
    if !source.is_dir() || !evidence.is_dir() {
        return refused(None, "source and evidence directories must exist");
    }
    if let Some(raw) = value(&args, "--timeout") {
        let Ok(timeout) = raw.parse::<f64>() else {
            return refused(Some(&evidence), "--timeout must be positive");
        };
        if !timeout.is_finite() || timeout <= 0.0 {
            return refused(Some(&evidence), "--timeout must be positive");
        }
    }
    for relative in SOURCE_FILES {
        if !source.join(relative).is_file() {
            return refused(Some(&evidence), "current source closure is incomplete");
        }
    }
    let host = fs::read_to_string(source.join(SOURCE_FILES[0])).unwrap_or_default();
    let driver = fs::read_to_string(source.join(SOURCE_FILES[2])).unwrap_or_default();
    if !host.contains("NativeRefused") {
        return refused(Some(&evidence), "frozen NativeRefused guard is absent");
    }
    if !driver.contains("initial_credit_bytes: Some(0)") {
        return refused(Some(&evidence), "frozen zero-credit guard is absent");
    }
    if let Some(profile) = value(&args, "--profile") {
        if !PROFILES.contains(&profile.as_str()) {
            return refused(Some(&evidence), "unknown current profile");
        }
    } else if !all_profiles_in_status(&evidence) {
        return refused(Some(&evidence), "explicit evidence status must name all four profiles");
    }
    let Some(image) = value(&args, "--image") else {
        return refused(Some(&evidence), "exact current image is not supplied");
    };
    if !Path::new(&image).is_file() {
        return refused(Some(&evidence), "exact current image path is missing");
    }
    let Some(resource) = value(&args, "--resource") else {
        return refused(Some(&evidence), "resource evidence is required");
    };
    if !Path::new(&resource).is_file() {
        return refused(Some(&evidence), "resource evidence path is missing");
    }
    let Some(oracle) = value(&args, "--oracle") else {
        return refused(Some(&evidence), "adopted current oracle is required");
    };
    if !Path::new(&oracle).is_file() {
        return refused(Some(&evidence), "adopted current oracle path is missing");
    }
    refused(
        Some(&evidence),
        "current image/resource/oracle review is not adopted by this entry",
    )
}
