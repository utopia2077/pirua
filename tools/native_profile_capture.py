#!/usr/bin/env python3
"""Capture one bounded, test-only native measurement walking skeleton.

The capture is deliberately limited to the current pi-harness-runtime library
test executable and the 8192 retained-route measurement test.  It records
actual test output and /usr/bin/time -v observations, then checks that the
trace, resource record, and executable remain bound by a small hash chain.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

PROFILE = "default-debug"
TRACE_PREFIX = "NATIVE_MEASUREMENT "
SOURCE_FILES = (
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    "crates/pi-harness-runtime/src/lib.rs",
    "crates/pi-harness-runtime/src/extensions/mod.rs",
    "crates/pi-harness-runtime/src/extensions/index_pool.rs",
    "crates/pi-harness-runtime/src/extensions/receive_backing8192.rs",
    "crates/pi-harness-runtime/src/extensions/receive_host8192.rs",
    "crates/pi-harness-runtime/src/extensions/tool_driver.rs",
    "crates/pi-harness-runtime/src/extensions/domain.rs",
    "crates/pi-harness-runtime/src/extensions/transport.rs",
    "crates/pi-harness-runtime/src/extensions/profiles/connection.rs",
    "tools/native_profile_capture.py",
)
TEST_NAME = "extensions::receive_host8192::tests::native_measurement_trace"
SCHEMA_VERSION = "s12-d-native-measurement-v1"


class Refused(Exception):
    """A forged or incomplete measurement is rejected."""


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_json(value: Any) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )


def git(source: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", "-C", str(source), *args], text=True, stderr=subprocess.STDOUT
    ).strip()


def run_child(
    command: list[str], *, cwd: Path, timeout: float
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            command,
            cwd=cwd,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as error:
        raise Refused(f"child timed out after {timeout:g}s: {' '.join(command)}") from error


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def source_record(source: Path) -> dict[str, Any]:
    try:
        commit = git(source, "rev-parse", "HEAD")
        tree = git(source, "rev-parse", "HEAD^{tree}")
        diff = subprocess.check_output(
            ["git", "-C", str(source), "diff", "--binary", "--", *SOURCE_FILES],
            stderr=subprocess.STDOUT,
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise Refused(f"source identity unavailable: {error}") from error
    files: dict[str, str] = {}
    for relative in SOURCE_FILES:
        path = source / relative
        if not path.is_file():
            raise Refused(f"source file is missing: {relative}")
        files[relative] = sha256_file(path)
    return {
        "commit": commit,
        "tree": tree,
        "files": files,
        "working_tree_diff_sha256": sha256_bytes(diff),
    }


def toolchain_record(source: Path) -> dict[str, Any]:
    rustc = run_child(["rustc", "-vV"], cwd=source, timeout=30)
    cargo = run_child(["cargo", "-V"], cwd=source, timeout=30)
    if rustc.returncode != 0 or cargo.returncode != 0:
        raise Refused("rustc/cargo version probe failed")
    rustc_lines = dict(
        line.split(": ", 1)
        for line in rustc.stdout.splitlines()
        if ": " in line
    )
    return {
        "rustc": rustc.stdout.strip(),
        "cargo": cargo.stdout.strip(),
        "rustc_release": rustc_lines.get("release"),
        "llvm_version": rustc_lines.get("LLVM version"),
        "host": rustc_lines.get("host"),
    }


def parse_artifact(cargo_stdout: str) -> tuple[Path, dict[str, Any]]:
    artifacts: list[dict[str, Any]] = []
    for line in cargo_stdout.splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if value.get("reason") != "compiler-artifact":
            continue
        target = value.get("target")
        executable = value.get("executable")
        if not isinstance(target, dict) or not isinstance(executable, str):
            continue
        profile = value.get("profile")
        if (
            target.get("name") == "pi_harness_runtime"
            and "lib" in target.get("kind", [])
            and isinstance(profile, dict)
            and profile.get("test") is True
        ):
            artifacts.append(value)
    if len(artifacts) != 1:
        raise Refused(f"expected one pi_harness_runtime test artifact, found {len(artifacts)}")
    artifact = artifacts[0]
    executable = Path(artifact["executable"]).resolve()
    if not executable.is_file():
        raise Refused(f"Cargo reported a missing test executable: {executable}")
    return executable, {
        "target": artifact["target"],
        "profile": artifact.get("profile"),
        "filenames": artifact.get("filenames"),
        "executable": str(executable),
    }


def parse_time_measurements(time_output: str) -> dict[str, Any]:
    patterns = {
        "user_seconds": r"User time \(seconds\):\s*([0-9.]+)",
        "system_seconds": r"System time \(seconds\):\s*([0-9.]+)",
        "max_rss_kbytes": r"Maximum resident set size \(kbytes\):\s*(\d+)",
        "minor_page_faults": r"Minor \(reclaiming a frame\) page faults:\s*(\d+)",
        "major_page_faults": r"Major \(requiring I/O\) page faults:\s*(\d+)",
        "voluntary_context_switches": r"Voluntary context switches:\s*(\d+)",
        "involuntary_context_switches": r"Involuntary context switches:\s*(\d+)",
        "elapsed_wall_clock": r"Elapsed \(wall clock\) time \(h:mm:ss or m:ss\):\s*([^\n]+)",
    }
    result: dict[str, Any] = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, time_output)
        if match is None:
            raise Refused(f"/usr/bin/time -v field is missing: {key}")
        value = match.group(1).strip()
        if key.endswith("_seconds"):
            result[key] = float(value)
        elif key.endswith("_kbytes") or key.endswith("_faults") or key.endswith(
            "_switches"
        ):
            result[key] = int(value)
        else:
            result[key] = value
    return result


def extract_trace(output: str) -> dict[str, Any]:
    records = []
    for line in output.splitlines():
        marker = line.find(TRACE_PREFIX)
        if marker < 0:
            continue
        payload = line[marker + len(TRACE_PREFIX) :].strip()
        try:
            records.append(json.loads(payload))
        except json.JSONDecodeError as error:
            raise Refused(f"measurement marker is not JSON: {error}") from error
    if len(records) != 1:
        raise Refused(f"expected one measurement marker, found {len(records)}")
    trace = records[0]
    if not isinstance(trace, dict):
        raise Refused("measurement trace must be a JSON object")
    return trace


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise Refused(reason)


def require_trace(trace: dict[str, Any]) -> None:
    require(trace.get("trace_kind") == "refusal", "trace kind is not refusal")
    outcomes = trace.get("outcomes")
    require(isinstance(outcomes, dict), "trace outcomes are missing")
    require(outcomes.get("refusal") == "NativeRefused", "NativeRefused outcome is missing")
    for outcome in ("normal", "partial", "error", "cancel"):
        require(outcomes.get(outcome) == "not_executed", f"{outcome} outcome is ambiguous")
    events = trace.get("events")
    require(isinstance(events, list) and events, "trace events are missing")
    ordering = trace.get("ordering")
    require(isinstance(ordering, list), "trace ordering is missing")
    observed_order = [event.get("event") for event in events if isinstance(event, dict)]
    require(observed_order == ordering, "trace ordering does not match observed events")
    require(
        ordering
        == [
            "staged",
            "reader_claimed",
            "second_reader_refused",
            "reader_released",
            "run_preopened",
            "close_requested",
            "reader_drop_cleanup",
        ],
        "trace ordering is not the bounded retained-route sequence",
    )
    backing = trace.get("backing")
    require(isinstance(backing, dict), "backing measurement is missing")
    layout = backing.get("layout")
    pool = backing.get("pool")
    lease = backing.get("lease")
    require(isinstance(layout, dict), "layout measurement is missing")
    require(isinstance(pool, dict), "pool measurement is missing")
    require(isinstance(lease, dict), "lease measurement is missing")
    for key in ("slot_bytes", "total_slot_bytes", "body_bytes"):
        require(isinstance(layout.get(key), int) and layout[key] > 0, f"layout field missing: {key}")
    require(pool.get("configured_slots") == 2, "pool capacity observation is missing")
    require(pool.get("leases_acquired") == 2, "two lease observation is missing")
    require(pool.get("third_lease") in {"refused", "capacity_refused"}, "third lease was not refused")
    require(lease.get("selected_frame_bytes") == layout["body_bytes"], "selected frame was not measured")
    require(lease.get("body_capacity") == layout["body_bytes"], "body capacity was not measured")
    require(isinstance(lease.get("body_len"), int) and lease["body_len"] > 0, "body length was not measured")
    require(lease.get("overflow") in {"refused", "capacity_refused"}, "overflow was not refused")
    require(lease.get("overflow_len") > lease.get("body_capacity"), "overflow length is not causal")
    run_event = next(
        (event for event in events if isinstance(event, dict) and event.get("event") == "run_preopened"),
        None,
    )
    require(isinstance(run_event, dict), "run_preopened event is missing")
    require(run_event.get("result") == "NativeRefused", "run_preopened did not refuse")
    close_event = next(
        (event for event in events if isinstance(event, dict) and event.get("event") == "close_requested"),
        None,
    )
    require(isinstance(close_event, dict) and close_event.get("cleaned") is False, "close ordering is not deferred")
    cleanup_event = next(
        (event for event in events if isinstance(event, dict) and event.get("event") == "reader_drop_cleanup"),
        None,
    )
    require(isinstance(cleanup_event, dict) and cleanup_event.get("cleaned") is True, "reader cleanup is missing")


def load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise Refused(f"{label} is unreadable: {error}") from error
    require(isinstance(value, dict), f"{label} must be an object")
    return value


def verify_capture(source: Path, evidence: Path) -> None:
    capture = load_json(evidence / "capture.json", "capture")
    require(capture.get("schema_version") == SCHEMA_VERSION, "capture schema is unsupported")
    require(capture.get("profile") == PROFILE, "capture profile is unsupported")
    expected_source = source_record(source)
    require(capture.get("source") == expected_source, "capture source provenance mismatch")
    trace_meta = capture.get("trace")
    resource_meta = capture.get("resource")
    image_meta = capture.get("image")
    require(isinstance(trace_meta, dict), "capture trace binding is missing")
    require(isinstance(resource_meta, dict), "capture resource binding is missing")
    require(isinstance(image_meta, dict), "capture image binding is missing")
    trace_path = evidence / str(trace_meta.get("path"))
    resource_path = evidence / str(resource_meta.get("path"))
    image_path = Path(str(image_meta.get("path"))).resolve()
    require(trace_path.is_file(), "trace file is missing")
    require(resource_path.is_file(), "resource file is missing")
    require(image_path.is_file(), "compiler artifact is missing")
    require(trace_meta.get("sha256") == sha256_file(trace_path), "trace hash mismatch")
    require(resource_meta.get("sha256") == sha256_file(resource_path), "resource hash mismatch")
    require(image_meta.get("sha256") == sha256_file(image_path), "image hash mismatch")
    trace = load_json(trace_path, "trace")
    resource = load_json(resource_path, "resource")
    require_trace(trace)
    require(resource.get("schema_version") == SCHEMA_VERSION, "resource schema is unsupported")
    require(resource.get("source") == expected_source, "resource source provenance mismatch")
    require(resource.get("trace_sha256") == trace_meta.get("sha256"), "resource trace binding mismatch")
    require(resource.get("measurements") == trace["backing"], "resource measurement is not test output")
    process = resource.get("process")
    require(isinstance(process, dict), "process measurement is missing")
    require(isinstance(process.get("max_rss_kbytes"), int) and process["max_rss_kbytes"] > 0, "RSS was not measured")
    require(isinstance(process.get("user_seconds"), float), "user CPU was not measured")
    require(isinstance(process.get("system_seconds"), float), "system CPU was not measured")
    chain = capture.get("hash_chain")
    require(isinstance(chain, dict), "hash chain is missing")
    require(chain.get("trace_sha256") == trace_meta.get("sha256"), "hash chain trace mismatch")
    require(chain.get("resource_sha256") == resource_meta.get("sha256"), "hash chain resource mismatch")
    require(chain.get("image_sha256") == image_meta.get("sha256"), "hash chain image mismatch")
    root_payload = {
        "trace_sha256": chain.get("trace_sha256"),
        "resource_sha256": chain.get("resource_sha256"),
        "image_sha256": chain.get("image_sha256"),
    }
    require(
        chain.get("root_sha256") == sha256_bytes(canonical_json(root_payload)),
        "hash chain root mismatch",
    )


def write_log(path: Path, command: list[str], result: subprocess.CompletedProcess[str]) -> None:
    path.write_text(
        "\n".join(
            [
                "$ " + " ".join(command),
                "",
                "stdout:",
                result.stdout,
                "stderr:",
                result.stderr,
                f"exit_code: {result.returncode}",
                "",
            ]
        ),
        encoding="utf-8",
    )


def run_verifier(source: Path, evidence: Path, timeout: float) -> subprocess.CompletedProcess[str]:
    return run_child(
        [
            sys.executable,
            str(Path(__file__).resolve()),
            "--verify",
            "--source",
            str(source),
            "--evidence-dir",
            str(evidence),
        ],
        cwd=source,
        timeout=timeout,
    )


def mutate_and_check(
    source: Path,
    evidence: Path,
    timeout: float,
    label: str,
    mutate: Any,
) -> dict[str, Any]:
    mutate()
    try:
        result = run_verifier(source, evidence, timeout)
        (evidence / f"forge-{label}.log").write_text(
            result.stdout + result.stderr, encoding="utf-8"
        )
        return {"name": label, "exit_code": result.returncode, "refused": result.returncode == 2}
    finally:
        pass


def capture(source: Path, evidence: Path, timeout: float) -> int:
    evidence.mkdir(parents=True, exist_ok=True)
    provenance = source_record(source)
    toolchain = toolchain_record(source)
    cargo_command = [
        "cargo",
        "test",
        "-p",
        "pi-harness-runtime",
        "--features",
        "extensions",
        "--lib",
        "--no-run",
        "--message-format=json",
        "--locked",
        "--offline",
    ]
    build = run_child(cargo_command, cwd=source, timeout=timeout)
    write_log(evidence / "cargo-build.log", cargo_command, build)
    if build.returncode != 0:
        print("REFUSED: Cargo test executable build failed", file=sys.stderr)
        return 2
    try:
        executable, artifact = parse_artifact(build.stdout)
    except Refused as error:
        print(f"REFUSED: {error}", file=sys.stderr)
        return 2
    exact_command = [
        "/usr/bin/time",
        "-v",
        str(executable),
        "--exact",
        TEST_NAME,
        "--nocapture",
    ]
    measured = run_child(exact_command, cwd=source, timeout=timeout)
    write_log(evidence / "measurement-run.log", exact_command, measured)
    combined = measured.stdout + "\n" + measured.stderr
    if measured.returncode != 0:
        print("REFUSED: exact measurement test failed", file=sys.stderr)
        return 2
    try:
        trace = extract_trace(combined)
        require_trace(trace)
        process = parse_time_measurements(measured.stderr)
    except Refused as error:
        print(f"REFUSED: {error}", file=sys.stderr)
        return 2
    trace_path = evidence / "trace.json"
    resource_path = evidence / "resource-bound.json"
    write_json(trace_path, trace)
    resource = {
        "schema_version": SCHEMA_VERSION,
        "status": "measured",
        "profile": PROFILE,
        "source": provenance,
        "trace_sha256": sha256_file(trace_path),
        "measurements": trace["backing"],
        "process": process,
        "trace_kind": trace["trace_kind"],
        "observed_outcomes": trace["outcomes"],
    }
    write_json(resource_path, resource)
    trace_hash = sha256_file(trace_path)
    resource_hash = sha256_file(resource_path)
    image_hash = sha256_file(executable)
    chain_payload = {
        "trace_sha256": trace_hash,
        "resource_sha256": resource_hash,
        "image_sha256": image_hash,
    }
    capture_record = {
        "schema_version": SCHEMA_VERSION,
        "profile": PROFILE,
        "invocation": exact_command,
        "source": provenance,
        "toolchain": toolchain,
        "artifact": artifact,
        "target": artifact["target"],
        "image": {"path": str(executable), "sha256": image_hash},
        "trace": {"path": "trace.json", "sha256": trace_hash},
        "resource": {"path": "resource-bound.json", "sha256": resource_hash},
        "hash_chain": {
            **chain_payload,
            "root_sha256": sha256_bytes(canonical_json(chain_payload)),
        },
        "process": process,
    }
    write_json(evidence / "capture.json", capture_record)
    initial = run_verifier(source, evidence, timeout)
    (evidence / "initial-green.log").write_text(
        initial.stdout + initial.stderr, encoding="utf-8"
    )
    if initial.returncode != 0:
        print("REFUSED: initial capture verification failed", file=sys.stderr)
        return 2

    forge_results: list[dict[str, Any]] = []
    original_trace = trace_path.read_bytes()
    forged_trace = dict(trace)
    forged_trace["trace_kind"] = "forged"
    write_json(trace_path, forged_trace)
    try:
        forge_results.append(
            mutate_and_check(source, evidence, timeout, "trace", lambda: None)
        )
    finally:
        trace_path.write_bytes(original_trace)

    original_resource = resource_path.read_bytes()
    forged_resource = dict(resource)
    forged_resource["trace_kind"] = "forged"
    write_json(resource_path, forged_resource)
    try:
        forge_results.append(
            mutate_and_check(source, evidence, timeout, "resource", lambda: None)
        )
    finally:
        resource_path.write_bytes(original_resource)

    with executable.open("r+b") as stream:
        original_byte = stream.read(1)
        if not original_byte:
            print("REFUSED: compiler artifact is empty", file=sys.stderr)
            return 2
        stream.seek(0)
        stream.write(bytes([original_byte[0] ^ 0xFF]))
        stream.flush()
        os.fsync(stream.fileno())
    try:
        forge_results.append(
            mutate_and_check(source, evidence, timeout, "image", lambda: None)
        )
    finally:
        with executable.open("r+b") as stream:
            stream.seek(0)
            stream.write(original_byte)
            stream.flush()
            os.fsync(stream.fileno())

    final = run_verifier(source, evidence, timeout)
    (evidence / "restore-green.log").write_text(
        final.stdout + final.stderr, encoding="utf-8"
    )
    write_json(evidence / "forge-summary.json", {"mutations": forge_results})
    if final.returncode != 0 or not all(item["refused"] for item in forge_results):
        print("REFUSED: forged evidence was not refused or restore was not green", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "status": "captured",
                "profile": PROFILE,
                "executable": str(executable),
                "trace_sha256": trace_hash,
                "resource_sha256": resource_hash,
                "image_sha256": image_hash,
                "forge_exit_codes": [item["exit_code"] for item in forge_results],
                "restore_exit_code": final.returncode,
            },
            sort_keys=True,
        )
    )
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--profile", default=PROFILE)
    parser.add_argument("--timeout", type=float, default=1800.0)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    if args.profile != PROFILE:
        print(f"REFUSED: only {PROFILE} is allowed in this walking skeleton")
        return 2
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        print("REFUSED: --timeout must be positive")
        return 2
    source = args.source.resolve()
    evidence = args.evidence_dir.resolve()
    try:
        if args.verify:
            verify_capture(source, evidence)
            print("ACCEPTED: current measurement hash chain is valid")
            return 0
        return capture(source, evidence, args.timeout)
    except Refused as error:
        print(f"REFUSED: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
