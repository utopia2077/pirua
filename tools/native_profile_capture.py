#!/usr/bin/env python3
"""Capture a bounded, test-only current-native measurement.

The capture is deliberately limited to the current pi-harness-runtime library
test executable and the 8192 retained-route measurement test.  It records one
real Cargo compiler-artifact invocation for each supported profile, actual test
output and /usr/bin/time -v observations, then checks that the trace, resource
record, inspected image and copied image remain bound by a hash chain.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

PROFILES = (
    "default-debug",
    "default-release",
    "unified-debug",
    "unified-release",
)
PROFILE_CONFIG: dict[str, dict[str, Any]] = {
    "default-debug": {
        "features": ["--features", "extensions"],
        "release": False,
        "cargo_profile": "debug",
    },
    "default-release": {
        "features": ["--features", "extensions"],
        "release": True,
        "cargo_profile": "release",
    },
    "unified-debug": {
        "features": ["--all-features"],
        "release": False,
        "cargo_profile": "debug",
    },
    "unified-release": {
        "features": ["--all-features"],
        "release": True,
        "cargo_profile": "release",
    },
}
TRACE_PREFIX = "NATIVE_MEASUREMENT "
RESOURCE_PREFIX = "SOURCE_RESOURCE "
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
    "crates/pi-harness-runtime/src/extensions/profiles/service.rs",
    "crates/pi-harness-runtime/src/extensions/profiles/tool_route.rs",
    "crates/pi-harness-runtime/src/extensions/profiles/native.rs",
    "crates/pi-harness-runtime/src/extensions/profiles/provider.rs",
    "crates/pi-harness-runtime/tests/runtime_extension_profiles.rs",
    "crates/pi-harness-runtime/tests/runtime_extension_profiles_support/mod.rs",
    "crates/pi-harness-runtime/tests/runtime_extension_profiles_support/process.rs",
    "tools/receive_resource_bound.py",
    "tools/verify_receive_backing.py",
    "tools/receive_backing_order.py",
    "tools/native_profile_capture.py",
)
TEST_NAME = "extensions::receive_host8192::tests::native_measurement_trace"
SCHEMA_VERSION = "s12-d-native-measurement-v2"
FIELD_EVENT_KINDS = {
    "Hboot": {"resource", "cleanup"},
    "Hpre": {"phase", "read", "body_capture", "native_validation"},
    "Qread": {"resource", "read", "queue"},
    "Qreceive": {"queue"},
    "overlap": {"interval"},
    "D_req": {"body_capture", "provider", "native_validation", "source_current", "phase"},
    "P_req": {"queue", "owner_lifecycle", "cleanup"},
    "whole_stack": {"stack"},
    "allocator_peak": {"allocator"},
    "work_cpu_headroom": {"cpu"},
    "Memory_SQLite_peer_parity": {"peer_parity"},
    "EH_inline_target_provider_allocator": {
        "role",
        "provider",
        "allocator",
        "cleanup",
        "refusal",
    },
}


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


def sha256_directory(path: Path) -> str:
    """Hash the current contents of a directory by relative file and digest."""
    require(path.is_dir(), f"directory is missing: {path}")
    entries: list[dict[str, str]] = []
    for item in sorted(path.rglob("*")):
        if item.is_file():
            entries.append(
                {
                    "path": item.relative_to(path).as_posix(),
                    "sha256": sha256_file(item),
                }
            )
    require(entries, f"directory has no files: {path}")
    return sha256_bytes(canonical_json(entries))


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
    sysroot = run_child(["rustc", "--print", "sysroot"], cwd=source, timeout=30)
    target_libdir = run_child(
        ["rustc", "--print", "target-libdir"], cwd=source, timeout=30
    )
    if (
        rustc.returncode != 0
        or cargo.returncode != 0
        or sysroot.returncode != 0
        or target_libdir.returncode != 0
    ):
        raise Refused("rustc/cargo version probe failed")
    rustc_lines = dict(
        line.split(": ", 1)
        for line in rustc.stdout.splitlines()
        if ": " in line
    )
    rustc_path = Path(shutil.which("rustc") or "").resolve()
    cargo_path = Path(shutil.which("cargo") or "").resolve()
    require(rustc_path.is_file(), "rustc executable path is unavailable")
    require(cargo_path.is_file(), "cargo executable path is unavailable")
    sysroot_path = Path(sysroot.stdout.strip()).resolve()
    target_libdir_path = Path(target_libdir.stdout.strip()).resolve()
    require(sysroot_path.is_dir(), "rustc sysroot is unavailable")
    require(target_libdir_path.is_dir(), "rustc target libdir is unavailable")
    return {
        "rustc": rustc.stdout.strip(),
        "cargo": cargo.stdout.strip(),
        "rustc_release": rustc_lines.get("release"),
        "llvm_version": rustc_lines.get("LLVM version"),
        "host": rustc_lines.get("host"),
        "rustc_path": str(rustc_path),
        "rustc_sha256": sha256_file(rustc_path),
        "cargo_path": str(cargo_path),
        "cargo_sha256": sha256_file(cargo_path),
        "sysroot": str(sysroot_path),
        "sysroot_sha256": sha256_directory(sysroot_path),
        "target_libdir": str(target_libdir_path),
        "target_libdir_sha256": sha256_directory(target_libdir_path),
    }


def parse_artifact(
    cargo_stdout: str, profile_name: str
) -> tuple[Path, dict[str, Any]]:
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
    expected = PROFILE_CONFIG[profile_name]["cargo_profile"]
    artifact_profile = artifact.get("profile")
    require(
        isinstance(artifact_profile, dict),
        "Cargo compiler artifact profile is missing",
    )
    if expected == "debug":
        require(
            artifact_profile.get("debug_assertions") is True,
            "debug profile artifact lacks debug assertions",
        )
    else:
        require(
            artifact_profile.get("debug_assertions") is False,
            "release profile artifact retains debug assertions",
        )
    return executable, {
        "target": artifact["target"],
        "profile": artifact_profile,
        "filenames": artifact.get("filenames"),
        "executable": str(executable),
    }


def run_probe(
    command: list[str], *, cwd: Path, timeout: float, required: bool = True
) -> dict[str, Any]:
    result = run_child(command, cwd=cwd, timeout=timeout)
    if required and result.returncode != 0:
        raise Refused(
            f"required image probe failed ({result.returncode}): {' '.join(command)}"
        )
    return {
        "command": command,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "exit_code": result.returncode,
    }


def write_probe(path: Path, probe: dict[str, Any]) -> str:
    write_json(path, probe)
    return sha256_file(path)


def inspect_image(
    source: Path,
    evidence: Path,
    profile: str,
    executable: Path,
    timeout: float,
) -> dict[str, Any]:
    """Persist actual ELF/linker/allocator inspection for one captured image."""
    image_dir = evidence / "image-inspection" / profile
    image_dir.mkdir(parents=True, exist_ok=True)
    commands = {
        "file": ["file", "-b", str(executable)],
        "elf_header": ["readelf", "-h", str(executable)],
        "program_headers": ["readelf", "-l", str(executable)],
        "dynamic": ["readelf", "-d", str(executable)],
        "symbols": ["readelf", "-Ws", str(executable)],
        "dependencies": ["ldd", str(executable)],
    }
    probes: dict[str, Any] = {}
    for name, command in commands.items():
        probe = run_probe(
            command,
            cwd=source,
            timeout=timeout,
            required=name != "dependencies",
        )
        probe_path = image_dir / f"{name}.json"
        probe["path"] = str(probe_path.relative_to(evidence))
        probe["sha256"] = write_probe(probe_path, probe)
        probes[name] = probe
    symbols = probes["symbols"]["stdout"].splitlines()
    allocator_symbol_names = (
        "__rust_alloc",
        "__rust_dealloc",
        "__rust_realloc",
        "__rust_alloc_zeroed",
        "__rust_no_alloc_shim_is_unstable",
    )
    allocator_matches = [
        line
        for line in symbols
        if any(name in line for name in allocator_symbol_names)
    ]
    dynamic_lines = probes["dynamic"]["stdout"].splitlines()
    provider_matches = [
        line
        for line in dynamic_lines
        if "NEEDED" in line or "RUNPATH" in line or "RPATH" in line
    ]
    provider_files: list[dict[str, str]] = []
    for line in probes["dependencies"]["stdout"].splitlines():
        match = re.search(r"=>\s+(/[^\s]+)", line)
        if match is None:
            match = re.match(r"\s*(/[^\s]+)", line)
        if match is None:
            continue
        path = Path(match.group(1))
        if path.is_file():
            provider_files.append(
                {"path": str(path), "sha256": sha256_file(path)}
            )
    allocator_probe = probes["symbols"]["path"]
    dynamic_probe = probes["dynamic"]["path"]
    dependency_probe = probes["dependencies"]["path"]
    return {
        "image_sha256": sha256_file(executable),
        "probes": {
            name: {
                "path": value["path"],
                "sha256": value["sha256"],
                "exit_code": value["exit_code"],
            }
            for name, value in probes.items()
        },
        "allocator": {
            "symbol_names": list(allocator_symbol_names),
            "matched_symbols": allocator_matches,
            "matched_symbols_sha256": sha256_bytes(canonical_json(allocator_matches)),
            "source_probe": allocator_probe,
            "source_probe_sha256": probes["symbols"]["sha256"],
        },
        "provider": {
            "dynamic_dependency_lines": provider_matches,
            "dynamic_dependency_sha256": sha256_bytes(
                canonical_json(provider_matches)
            ),
            "provider_files": provider_files,
            "source_probe": dynamic_probe,
            "source_probe_sha256": probes["dynamic"]["sha256"],
            "ldd_probe": dependency_probe,
            "ldd_probe_sha256": probes["dependencies"]["sha256"],
        },
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


def extract_resource_observation(output: str) -> dict[str, Any]:
    records = []
    for line in output.splitlines():
        marker = line.find(RESOURCE_PREFIX)
        if marker < 0:
            continue
        payload = line[marker + len(RESOURCE_PREFIX) :].strip()
        try:
            records.append(json.loads(payload))
        except json.JSONDecodeError as error:
            raise Refused(f"source resource marker is not JSON: {error}") from error
    if len(records) != 1:
        raise Refused(f"expected one source resource marker, found {len(records)}")
    resource = records[0]
    if not isinstance(resource, dict):
        raise Refused("source resource observation must be a JSON object")
    require(
        resource.get("schema_version") == "s12-d-resource-observations-v1",
        "source resource observation schema is unsupported",
    )
    fields = resource.get("fields")
    require(isinstance(fields, dict), "source resource fields are missing")
    required = {
        "Hboot",
        "Hpre",
        "Qread",
        "Qreceive",
        "overlap",
        "D_req",
        "P_req",
        "whole_stack",
        "allocator_peak",
        "work_cpu_headroom",
        "Memory_SQLite_peer_parity",
        "EH_inline_target_provider_allocator",
    }
    require(set(fields) == required, "source resource field set is incomplete")
    for field, value in fields.items():
        require(isinstance(value, dict), f"source resource field is invalid: {field}")
        require(
            value.get("status") in {"observed", "unresolved"},
            f"source resource field status is invalid: {field}",
        )
        evidence = value.get("evidence")
        require(isinstance(evidence, dict), f"source resource evidence is missing: {field}")
        require(
            isinstance(evidence.get("event_ids"), list),
            f"source resource event IDs are missing: {field}",
        )
        event_ids = evidence["event_ids"]
        events = evidence.get("events")
        require(isinstance(evidence.get("source_owned"), bool), f"source resource ownership is missing: {field}")
        require(isinstance(events, list), f"source resource event records are missing: {field}")
        require(
            len(event_ids) == len(events),
            f"source resource event IDs do not match event records: {field}",
        )
        require(
            all(
                isinstance(event_id, str) and event_id
                for event_id in event_ids
            ),
            f"source resource event IDs are invalid: {field}",
        )
        for event in events:
            require(isinstance(event, dict), f"source resource event record is invalid: {field}")
            require(
                event.get("kind") in FIELD_EVENT_KINDS[field],
                f"source resource event kind is incompatible: {field}",
            )
        if value["status"] == "observed":
            require(
                evidence["source_owned"] and event_ids and events,
                f"observed source resource field lacks owned evidence: {field}",
            )
    return resource


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
    profile = capture.get("profile")
    require(profile in PROFILES, "capture profile is unsupported")
    expected_source = source_record(source)
    require(capture.get("source") == expected_source, "capture source provenance mismatch")
    require(capture.get("profile_config") == PROFILE_CONFIG[profile], "profile configuration mismatch")
    trace_meta = capture.get("trace")
    resource_meta = capture.get("resource")
    image_meta = capture.get("image")
    require(isinstance(trace_meta, dict), "capture trace binding is missing")
    require(isinstance(resource_meta, dict), "capture resource binding is missing")
    require(isinstance(image_meta, dict), "capture image binding is missing")
    trace_path = evidence / str(trace_meta.get("path"))
    resource_path = evidence / str(resource_meta.get("path"))
    image_path = Path(str(image_meta.get("path"))).resolve()
    artifact_path = Path(str(image_meta.get("artifact_path"))).resolve()
    require(trace_path.is_file(), "trace file is missing")
    require(resource_path.is_file(), "resource file is missing")
    source_resource_meta = capture.get("source_resource_observation")
    require(
        isinstance(source_resource_meta, dict),
        "source resource observation binding is missing",
    )
    source_resource_path = evidence / str(source_resource_meta.get("path"))
    require(source_resource_path.is_file(), "source resource observation is missing")
    require(
        source_resource_meta.get("sha256") == sha256_file(source_resource_path),
        "source resource observation hash mismatch",
    )
    require(image_path.is_file(), "compiler artifact is missing")
    require(artifact_path.is_file(), "Cargo compiler artifact is missing")
    require(trace_meta.get("sha256") == sha256_file(trace_path), "trace hash mismatch")
    require(resource_meta.get("sha256") == sha256_file(resource_path), "resource hash mismatch")
    require(image_meta.get("sha256") == sha256_file(image_path), "image hash mismatch")
    require(
        image_meta.get("artifact_sha256") == sha256_file(artifact_path),
        "Cargo artifact hash mismatch",
    )
    require(
        image_meta.get("sha256") == image_meta.get("artifact_sha256"),
        "captured image differs from Cargo artifact",
    )
    require(
        capture.get("artifact", {}).get("executable") == str(artifact_path),
        "artifact executable binding mismatch",
    )
    require(
        capture.get("inspection", {}).get("image_sha256") == image_meta.get("sha256"),
        "image inspection binding mismatch",
    )
    trace = load_json(trace_path, "trace")
    resource = load_json(resource_path, "resource")
    source_resource = load_json(source_resource_path, "source resource observation")
    require_trace(trace)
    require(resource.get("schema_version") == SCHEMA_VERSION, "resource schema is unsupported")
    require(resource.get("source") == expected_source, "resource source provenance mismatch")
    require(resource.get("trace_sha256") == trace_meta.get("sha256"), "resource trace binding mismatch")
    require(resource.get("measurements") == trace["backing"], "resource measurement is not test output")
    require(
        resource.get("source_resource_observation", {}).get("sha256")
        == source_resource_meta.get("sha256"),
        "resource/source resource observation binding mismatch",
    )
    require(
        source_resource.get("schema_version") == "s12-d-resource-observations-v1",
        "source resource observation schema is unsupported",
    )
    process = resource.get("process")
    require(isinstance(process, dict), "process measurement is missing")
    require(isinstance(process.get("max_rss_kbytes"), int) and process["max_rss_kbytes"] > 0, "RSS was not measured")
    require(isinstance(process.get("user_seconds"), float), "user CPU was not measured")
    require(isinstance(process.get("system_seconds"), float), "system CPU was not measured")
    require(resource.get("profile") == profile, "resource profile mismatch")
    require(resource.get("invocation") == capture.get("invocation"), "resource invocation mismatch")
    require(isinstance(capture.get("toolchain"), dict), "toolchain provenance is missing")
    for key in (
        "rustc_sha256",
        "cargo_sha256",
        "sysroot_sha256",
        "target_libdir_sha256",
    ):
        require(
            isinstance(capture["toolchain"].get(key), str)
            and capture["toolchain"][key],
            f"toolchain hash is missing: {key}",
        )
    artifact_profile = capture.get("artifact", {}).get("profile")
    require(
        artifact_profile == capture.get("codegen"),
        "compiler artifact/codegen profile binding mismatch",
    )
    target_provenance = capture.get("target_provenance")
    require(isinstance(target_provenance, dict), "target provenance is missing")
    require(
        target_provenance.get("triple") == capture["toolchain"].get("host"),
        "target triple binding mismatch",
    )
    require(
        target_provenance.get("libdir_sha256")
        == capture["toolchain"].get("target_libdir_sha256"),
        "target libdir hash binding mismatch",
    )
    require(
        resource.get("image_sha256") == image_meta.get("sha256"),
        "resource image binding mismatch",
    )
    chain = capture.get("hash_chain")
    require(isinstance(chain, dict), "hash chain is missing")
    require(chain.get("trace_sha256") == trace_meta.get("sha256"), "hash chain trace mismatch")
    require(chain.get("resource_sha256") == resource_meta.get("sha256"), "hash chain resource mismatch")
    require(
        chain.get("source_resource_sha256") == source_resource_meta.get("sha256"),
        "hash chain source resource mismatch",
    )
    require(chain.get("image_sha256") == image_meta.get("sha256"), "hash chain image mismatch")
    root_payload = {
        "trace_sha256": chain.get("trace_sha256"),
        "resource_sha256": chain.get("resource_sha256"),
        "source_resource_sha256": chain.get("source_resource_sha256"),
        "image_sha256": chain.get("image_sha256"),
    }
    require(
        chain.get("root_sha256") == sha256_bytes(canonical_json(root_payload)),
        "hash chain root mismatch",
    )
    inspection = capture.get("inspection")
    require(isinstance(inspection, dict), "image inspection is missing")
    for probe in inspection.get("probes", {}).values():
        require(isinstance(probe, dict), "image inspection probe is invalid")
        probe_path = evidence / str(probe.get("path"))
        require(probe_path.is_file(), "image inspection evidence is missing")
        require(probe.get("sha256") == sha256_file(probe_path), "image probe hash mismatch")


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


def capture_profile(
    source: Path, evidence: Path, profile: str, timeout: float
) -> int:
    require(profile in PROFILES, f"unsupported current profile: {profile}")
    provenance = source_record(source)
    toolchain = toolchain_record(source)
    cargo_command = [
        "cargo",
        "test",
        "-p",
        "pi-harness-runtime",
        *PROFILE_CONFIG[profile]["features"],
    ]
    if PROFILE_CONFIG[profile]["release"]:
        cargo_command.append("--release")
    cargo_command.extend(
        [
        "--lib",
        "--no-run",
        "--message-format=json",
        "--locked",
        "--offline",
        ]
    )
    build = run_child(cargo_command, cwd=source, timeout=timeout)
    evidence.mkdir(parents=True, exist_ok=True)
    write_log(evidence / "cargo-build.log", cargo_command, build)
    if build.returncode != 0:
        print("REFUSED: Cargo test executable build failed", file=sys.stderr)
        return 2
    try:
        executable, artifact = parse_artifact(build.stdout, profile)
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
    resource_command = [
        "cargo",
        "test",
        "-p",
        "pi-harness-runtime",
        *PROFILE_CONFIG[profile]["features"],
    ]
    if PROFILE_CONFIG[profile]["release"]:
        resource_command.append("--release")
    resource_command.extend(
        [
            "--test",
            "runtime_extension_profiles",
            "runtime_extension_profiles::source_root_structured_events_bind_real_resources",
            "--",
            "--ignored",
            "--exact",
            "--nocapture",
            "--test-threads=1",
        ]
    )
    resource_run = run_child(resource_command, cwd=source, timeout=timeout)
    write_log(evidence / "resource-measurement-run.log", resource_command, resource_run)
    try:
        require(resource_run.returncode == 0, "source resource measurement test failed")
        source_resource = extract_resource_observation(
            resource_run.stdout + "\n" + resource_run.stderr
        )
    except Refused as error:
        print(f"REFUSED: {error}", file=sys.stderr)
        return 2
    trace_path = evidence / "trace.json"
    resource_path = evidence / "resource-bound.json"
    write_json(trace_path, trace)
    source_resource_path = evidence / "source-resource-observation.json"
    write_json(source_resource_path, source_resource)
    image_dir = evidence / "images"
    image_dir.mkdir(parents=True, exist_ok=True)
    image_copy = image_dir / f"pi_harness_runtime-{profile}.bin"
    shutil.copy2(executable, image_copy)
    image_hash = sha256_file(executable)
    require(image_hash == sha256_file(image_copy), "Cargo artifact copy hash mismatch")
    inspection = inspect_image(source, evidence, profile, image_copy, timeout)
    codegen = artifact["profile"]
    resource = {
        "schema_version": SCHEMA_VERSION,
        "status": "measured",
        "profile": profile,
        "source": provenance,
        "trace_sha256": sha256_file(trace_path),
        "measurements": trace["backing"],
        "source_resource_observation": {
            "path": "source-resource-observation.json",
            "sha256": sha256_file(source_resource_path),
            "schema_version": source_resource["schema_version"],
        },
        "process": process,
        "trace_kind": trace["trace_kind"],
        "observed_outcomes": trace["outcomes"],
        "invocation": exact_command,
        "image_sha256": image_hash,
        "codegen": codegen,
        "target": artifact["target"],
        "target_provenance": {
            "triple": toolchain["host"],
            "libdir": toolchain["target_libdir"],
            "libdir_sha256": toolchain["target_libdir_sha256"],
        },
    }
    write_json(resource_path, resource)
    trace_hash = sha256_file(trace_path)
    resource_hash = sha256_file(resource_path)
    chain_payload = {
        "trace_sha256": trace_hash,
        "resource_sha256": resource_hash,
        "source_resource_sha256": sha256_file(source_resource_path),
        "image_sha256": image_hash,
    }
    capture_record = {
        "schema_version": SCHEMA_VERSION,
        "profile": profile,
        "profile_config": PROFILE_CONFIG[profile],
        "invocation": exact_command,
        "cargo_invocation": cargo_command,
        "source": provenance,
        "toolchain": toolchain,
        "artifact": artifact,
        "target": artifact["target"],
        "target_provenance": {
            "triple": toolchain["host"],
            "libdir": toolchain["target_libdir"],
            "libdir_sha256": toolchain["target_libdir_sha256"],
        },
        "codegen": codegen,
        "image": {
            "path": str(image_copy),
            "artifact_path": str(executable),
            "sha256": image_hash,
            "artifact_sha256": image_hash,
        },
        "inspection": inspection,
        "trace": {"path": "trace.json", "sha256": trace_hash},
        "source_resource_observation": {
            "path": "source-resource-observation.json",
            "sha256": sha256_file(source_resource_path),
        },
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
                "profile": profile,
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
    parser.add_argument("--profile", choices=PROFILES, default=PROFILES[0])
    parser.add_argument("--timeout", type=float, default=1800.0)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
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
        return capture_profile(source, evidence, args.profile, args.timeout)
    except Refused as error:
        print(f"REFUSED: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
