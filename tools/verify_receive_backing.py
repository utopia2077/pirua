#!/usr/bin/env python3
"""Orchestrate the current S12-D entry with bounded child timeouts."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

SOURCE_FILES = (
    "crates/pi-harness-runtime/src/extensions/receive_host8192.rs",
    "crates/pi-harness-runtime/src/extensions/receive_backing8192.rs",
    "crates/pi-harness-runtime/src/extensions/tool_driver.rs",
    "crates/pi-harness-runtime/src/extensions/domain.rs",
    "crates/pi-harness-runtime/src/extensions/transport.rs",
    "crates/pi-harness-runtime/src/extensions/profiles/connection.rs",
)
REQUIRED_PROFILES = ("default-debug", "default-release", "unified-debug", "unified-release")
CAPTURE_SCHEMA_VERSION = "s12-d-native-measurement-v2"
NATIVE_SCHEMA_VERSION = "s12-d-native-proof-v3"
RESOURCE_SCHEMA_VERSION = "s12-d-resource-v3"
MARKER_PATH = "current-evidence-marker.json"
ADOPTION_RECEIPT_PATH = "conformance/extension-protocol/native-order-adoption-v1.json"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git(source: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(source), *args], text=True, stderr=subprocess.STDOUT
    ).strip()


def write_log(path: Path, command: list[str], stdout: str, stderr: str, exit_code: str) -> None:
    path.write_text(
        "\n".join(
            [
                "$ " + " ".join(command),
                "",
                "stdout:",
                stdout,
                "stderr:",
                stderr,
                f"exit_code: {exit_code}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def run_child(command: list[str], timeout: float) -> tuple[subprocess.CompletedProcess[str] | None, str | None]:
    try:
        return subprocess.run(command, text=True, capture_output=True, check=False, timeout=timeout), None
    except subprocess.TimeoutExpired as error:
        return None, f"child timeout after {timeout:g}s: {' '.join(command)} ({error})"


def read_profile_capture(evidence: Path, profile: str) -> dict | None:
    """Project one independently verified capture into the aggregate status.

    The aggregate checker still refuses support without resource/oracle/
    adoption review.  These records make the four real image runs visible to
    that refusal path instead of replacing them with ``missing`` placeholders.
    """
    path = evidence / profile / "capture.json"
    if not path.is_file():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or value.get("schema_version") != CAPTURE_SCHEMA_VERSION:
        return None
    if value.get("profile") != profile:
        return None
    source = value.get("source")
    image = value.get("image")
    toolchain = value.get("toolchain")
    artifact = value.get("artifact")
    codegen = value.get("codegen")
    invocation = value.get("cargo_invocation")
    if not all(
        isinstance(item, dict)
        for item in (source, image, toolchain, artifact, codegen)
    ):
        return None
    if not isinstance(invocation, list) or not invocation:
        return None
    image_path = image.get("path")
    image_sha256 = image.get("sha256")
    if not isinstance(image_path, str) or not isinstance(image_sha256, str):
        return None
    return {
        "status": "measured",
        "image_path": image_path,
        "image_sha256": image_sha256,
        "invocation": invocation + list(value.get("invocation", [])),
        "profile_identity": profile,
        "source_tree": source.get("tree"),
        "source_commit": source.get("commit"),
        "toolchain": {
            "rustc": toolchain.get("rustc"),
            "cargo": toolchain.get("cargo"),
            "llvm": toolchain.get("llvm_version"),
            "rustc_sha256": toolchain.get("rustc_sha256"),
            "cargo_sha256": toolchain.get("cargo_sha256"),
            "sysroot_sha256": toolchain.get("sysroot_sha256"),
            "target_libdir_sha256": toolchain.get("target_libdir_sha256"),
        },
        "target": toolchain.get("host"),
        "profile": profile,
        "codegen": codegen,
        "artifact_path": artifact.get("executable"),
        "artifact_sha256": image.get("artifact_sha256"),
        "trace": value.get("trace"),
        "resource": value.get("resource"),
        "inspection": value.get("inspection"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=1800.0)
    args = parser.parse_args()
    if not args.check:
        print("REFUSED: --check is required for the current entry")
        return 2
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        print("REFUSED: --timeout must be positive")
        return 2
    source = args.source.resolve()
    evidence = args.evidence_dir.resolve()
    evidence.mkdir(parents=True, exist_ok=True)
    logs = evidence
    try:
        commit = git(source, "rev-parse", "HEAD")
        tree = git(source, "rev-parse", "HEAD^{tree}")
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"REFUSED: source identity unavailable: {error}")
        return 2
    files = {}
    missing = []
    for relative in SOURCE_FILES:
        path = source / relative
        if not path.is_file():
            missing.append(relative)
        else:
            files[relative] = sha256_file(path)
    host_path = source / "crates/pi-harness-runtime/src/extensions/receive_host8192.rs"
    driver_path = source / "crates/pi-harness-runtime/src/extensions/tool_driver.rs"
    guards = {
        "zero_credit": driver_path.is_file() and "initial_credit_bytes: Some(0)" in driver_path.read_text(encoding="utf-8"),
        "native_refused": host_path.is_file() and "NativeRefused" in host_path.read_text(encoding="utf-8"),
    }
    status = {
        "schema_version": NATIVE_SCHEMA_VERSION,
        "status": "refused",
        "source": {"commit": commit, "tree": tree, "files": files},
        "toolchain": {"rustc": "unrecorded", "cargo": "unrecorded", "llvm": "unrecorded"},
        "profiles": {
            profile: {
                "status": "missing",
                "image_path": None,
                "image_sha256": None,
                "invocation": None,
                "profile_identity": profile,
                "source_tree": None,
                "toolchain": {},
                "target": None,
                "profile": profile,
                "codegen": {},
            }
            for profile in REQUIRED_PROFILES
        },
        "oracle": {
            "status": "missing",
            "path": None,
            "sha256": None,
            "schema_version": None,
            "adoption_marker": None,
        },
        "resource": {"status": "refused", "path": "resource-bound.json", "schema_version": RESOURCE_SCHEMA_VERSION},
        "independent_marker": {"path": MARKER_PATH},
        "adoption_receipt": {"path": ADOPTION_RECEIPT_PATH, "status": "missing"},
        "guards": guards,
        "missing_source_files": missing,
        "unknown": [
            "four current profile images",
            "source/IR/tool/sysroot/provider/allocator closure",
            "resource/P/CPU/EH/inline/target meaning",
            "source-committed adoption receipt and independent current-image review",
        ],
        "reason": "current four-image/resource/oracle evidence is absent; no supported image is recognized",
    }
    captured_profiles = []
    for profile in REQUIRED_PROFILES:
        capture = read_profile_capture(evidence, profile)
        if capture is None:
            continue
        status["profiles"][profile] = capture
        captured_profiles.append(profile)
    if captured_profiles:
        status["captured_profiles"] = captured_profiles
        status["reason"] = (
            "four current profile images and refusal traces are measured; "
            "independent resource/oracle/adoption review is absent"
        )
    (evidence / "native-proof-status.json").write_text(
        json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    timeout_arg = str(args.timeout)
    resource_command = [
        sys.executable,
        str(source / "tools/receive_resource_bound.py"),
        "--source",
        str(source),
        "--evidence-dir",
        str(evidence),
        "--timeout",
        timeout_arg,
    ]
    resource, timeout_error = run_child(resource_command, args.timeout)
    if timeout_error:
        write_log(logs / "resource-bound.log", resource_command, "", timeout_error, "timeout")
        print(f"REFUSED: {timeout_error}")
        return 2
    assert resource is not None
    write_log(logs / "resource-bound.log", resource_command, resource.stdout, resource.stderr, str(resource.returncode))
    with tempfile.TemporaryDirectory(prefix="s12-d-native-entry-") as build_dir:
        binary = Path(build_dir) / "receive_backing_build_gate"
        build_command = [
            "rustc",
            "--edition=2024",
            str(source / "tools/receive_backing_build_gate.rs"),
            "-o",
            str(binary),
        ]
        build, timeout_error = run_child(build_command, args.timeout)
        if timeout_error:
            write_log(logs / "producer-build.log", build_command, "", timeout_error, "timeout")
            print(f"REFUSED: {timeout_error}")
            return 2
        assert build is not None
        write_log(logs / "producer-build.log", build_command, build.stdout, build.stderr, str(build.returncode))
        if build.returncode == 0:
            producer_command = [
                str(binary),
                "--source",
                str(source),
                "--evidence-dir",
                str(evidence),
                "--timeout",
                timeout_arg,
            ]
            producer, timeout_error = run_child(producer_command, args.timeout)
            if timeout_error:
                write_log(logs / "producer-run.log", producer_command, "", timeout_error, "timeout")
                print(f"REFUSED: {timeout_error}")
                return 2
            assert producer is not None
            write_log(logs / "producer-run.log", producer_command, producer.stdout, producer.stderr, str(producer.returncode))
    checker_command = [
        sys.executable,
        str(source / "tools/receive_backing_order.py"),
        "--source",
        str(source),
        "--evidence-dir",
        str(evidence),
        "--timeout",
        timeout_arg,
    ]
    checker, timeout_error = run_child(checker_command, args.timeout)
    if timeout_error:
        write_log(logs / "checker.log", checker_command, "", timeout_error, "timeout")
        print(f"REFUSED: {timeout_error}")
        return 2
    assert checker is not None
    write_log(logs / "checker.log", checker_command, checker.stdout, checker.stderr, str(checker.returncode))
    print(checker.stdout, end="")
    if checker.stderr:
        print(checker.stderr, file=sys.stderr, end="")
    shutil.copy2(evidence / "native-proof-status.json", evidence / "native-proof-status.final.json")
    return checker.returncode or 2


if __name__ == "__main__":
    sys.exit(main())
