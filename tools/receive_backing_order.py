#!/usr/bin/env python3
"""Fail-closed checker for current native evidence and external adoption."""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
from pathlib import Path

SOURCE_FILES = (
    "crates/pi-harness-runtime/src/extensions/receive_host8192.rs",
    "crates/pi-harness-runtime/src/extensions/receive_backing8192.rs",
    "crates/pi-harness-runtime/src/extensions/tool_driver.rs",
    "crates/pi-harness-runtime/src/extensions/domain.rs",
    "crates/pi-harness-runtime/src/extensions/transport.rs",
    "crates/pi-harness-runtime/src/extensions/profiles/connection.rs",
)
PROFILES = ("default-debug", "default-release", "unified-debug", "unified-release")
NATIVE_SCHEMA_VERSION = "s12-d-native-proof-v3"
RESOURCE_SCHEMA_VERSION = "s12-d-resource-v3"
MARKER_SCHEMA_VERSION = "s12-d-native-evidence-marker-v1"
RECEIPT_SCHEMA_VERSION = "s12-d-native-adoption-receipt-v1"
ORACLE_SCHEMA_VERSION = "native-order-v1"
ORACLE_ADOPTION_MARKER = "root-adopted-current"
MARKER_PATH = "current-evidence-marker.json"
RESOURCE_PATH = "resource-bound.json"
ADOPTION_RECEIPT_PATH = "conformance/extension-protocol/native-order-adoption-v1.json"
DECLARED = {
    "frame_bytes": 8192,
    "index_limit": 8193,
    "backing_pool_slots": 2,
    "body_buffer_bytes": 8192,
}
REQUIRED_MEASURED_FIELDS = (
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
)
RESOURCE_UNKNOWN = list(REQUIRED_MEASURED_FIELDS)
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
    """A checked evidence condition that must remain refusal-only."""


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


def require(condition: bool, reason: str) -> None:
    if not condition:
        raise Refused(reason)


def read_json(path: Path, label: str) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise Refused(f"{label} is unreadable: {error}") from error
    require(isinstance(value, dict), f"{label} must be an object")
    return value


def source_identity(source: Path) -> tuple[str, str, dict[str, str]]:
    try:
        commit = git(source, "rev-parse", "HEAD")
        tree = git(source, "rev-parse", "HEAD^{tree}")
    except (OSError, subprocess.CalledProcessError) as error:
        raise Refused(f"source identity is unavailable: {error}") from error
    files: dict[str, str] = {}
    for relative in SOURCE_FILES:
        path = source / relative
        require(path.is_file(), f"current source file is missing: {relative}")
        files[relative] = sha256_file(path)
    host = (source / SOURCE_FILES[0]).read_text(encoding="utf-8")
    driver = (source / SOURCE_FILES[2]).read_text(encoding="utf-8")
    require("NativeRefused" in host, "frozen NativeRefused guard is absent from current source")
    require(
        "initial_credit_bytes: Some(0)" in driver,
        "frozen zero-credit guard is absent from current source",
    )
    return commit, tree, files


def require_source_record(record: object, commit: str, tree: str, files: dict[str, str], label: str) -> None:
    require(isinstance(record, dict), f"{label} source record is missing")
    require(record.get("commit") == commit, f"{label} source commit mismatch")
    require(record.get("tree") == tree, f"{label} source tree mismatch")
    require(record.get("files") == files, f"{label} source file hashes mismatch")


def require_status_shape(status: dict) -> None:
    require(status.get("schema_version") == NATIVE_SCHEMA_VERSION, "native schema version unsupported")
    require(status.get("independent_marker") == {"path": MARKER_PATH}, "independent marker binding is invalid")
    adoption = status.get("adoption_receipt")
    require(isinstance(adoption, dict), "adoption receipt binding is invalid")
    require(adoption.get("path") == ADOPTION_RECEIPT_PATH, "adoption receipt path binding is invalid")
    profiles = status.get("profiles")
    require(isinstance(profiles, dict), "native profiles schema is invalid")
    require(set(profiles) == set(PROFILES), "native profile set is incomplete")
    required_profile_fields = {
        "status",
        "image_path",
        "image_sha256",
        "invocation",
        "profile_identity",
        "source_tree",
        "toolchain",
        "target",
        "profile",
        "codegen",
    }
    for profile in PROFILES:
        record = profiles[profile]
        require(isinstance(record, dict), f"profile record is invalid: {profile}")
        require(required_profile_fields <= set(record), f"profile provenance schema is incomplete: {profile}")
    oracle = status.get("oracle")
    require(isinstance(oracle, dict), "oracle schema is invalid")
    require(
        {"status", "path", "sha256", "schema_version", "adoption_marker"} <= set(oracle),
        "oracle schema is incomplete",
    )
    resource = status.get("resource")
    require(isinstance(resource, dict), "native resource reference is invalid")
    require(resource.get("path") == RESOURCE_PATH, "native resource path binding is invalid")
    require(resource.get("schema_version") == RESOURCE_SCHEMA_VERSION, "native resource schema binding is invalid")


def check_measured(value: object, field: str) -> None:
    require(isinstance(value, dict), f"resource measured field is invalid: {field}")
    require(
        {"status", "value", "unit", "evidence"} <= set(value),
        f"resource measured field schema is incomplete: {field}",
    )
    require(value.get("status") == "measured", f"resource measured field is unresolved: {field}")
    number = value.get("value")
    require(
        isinstance(number, (int, float)) and not isinstance(number, bool) and math.isfinite(number) and number >= 0,
        f"resource measured value is invalid: {field}",
    )
    require(isinstance(value.get("unit"), str) and value["unit"], f"resource measured unit is invalid: {field}")
    require(
        isinstance(value.get("evidence"), str) and value["evidence"],
        f"resource measured evidence is invalid: {field}",
    )


def check_resource(evidence: Path, commit: str, tree: str, files: dict[str, str]) -> tuple[dict, Path]:
    path = evidence / RESOURCE_PATH
    require(path.is_file(), "resource-bound.json is missing")
    resource = read_json(path, "resource schema")
    require(resource.get("schema_version") == RESOURCE_SCHEMA_VERSION, "resource schema version unsupported")
    require_source_record(resource.get("source"), commit, tree, files, "resource")
    require(resource.get("declared") == DECLARED, "resource declarations are not the current schema")
    require(isinstance(resource.get("measured"), dict), "resource measured schema is invalid")
    observations = resource.get("observations")
    require(isinstance(observations, dict), "resource observations schema is missing")
    require(
        observations.get("schema_version") == "s12-d-resource-observations-v1",
        "resource observations schema is unsupported",
    )
    require(
        observations.get("field_set") == list(REQUIRED_MEASURED_FIELDS),
        "resource observations field set is incomplete",
    )
    require(
        isinstance(observations.get("profiles"), list),
        "resource observation profiles are missing",
    )
    for profile in observations["profiles"]:
        require(isinstance(profile, dict), "resource observation profile is invalid")
        require(
            isinstance(profile.get("fields"), dict)
            and set(profile["fields"]) == set(REQUIRED_MEASURED_FIELDS),
            "resource observation profile field set is incomplete",
        )
        for field, record in profile["fields"].items():
            require(isinstance(record, dict), f"resource observation field is invalid: {field}")
            require(
                record.get("status") in {"observed", "unresolved"},
                f"resource observation field status is invalid: {field}",
            )
            evidence = record.get("evidence")
            require(isinstance(evidence, dict), f"resource observation evidence is missing: {field}")
            event_ids = evidence.get("event_ids")
            events = evidence.get("events")
            require(isinstance(event_ids, list), f"resource observation event IDs are missing: {field}")
            require(isinstance(events, list) and len(event_ids) == len(events), f"resource observation events are invalid: {field}")
            require(isinstance(evidence.get("source_owned"), bool), f"resource observation ownership is missing: {field}")
            require(
                all(isinstance(event_id, str) and event_id for event_id in event_ids),
                f"resource observation event IDs are invalid: {field}",
            )
            for event in events:
                require(
                    isinstance(event, dict) and event.get("kind") in FIELD_EVENT_KINDS[field],
                    f"resource observation event kind is incompatible: {field}",
                )
            if record["status"] == "observed":
                require(
                    evidence["source_owned"] and event_ids and events,
                    f"observed resource field lacks owned evidence: {field}",
                )
    require(isinstance(resource.get("unknown"), list), "resource unknown schema is invalid")
    require(isinstance(resource.get("missing_source_files"), list), "resource missing-source schema is invalid")
    require(resource.get("status") in {"refused", "supported"}, "resource status is invalid")
    if resource.get("status") == "supported":
        require(resource.get("unknown") == [], "resource unknowns remain unresolved")
        require(set(resource["measured"]) == set(REQUIRED_MEASURED_FIELDS), "resource measured fields are incomplete")
        for field in REQUIRED_MEASURED_FIELDS:
            check_measured(resource["measured"][field], field)
    else:
        require(resource.get("unknown") == RESOURCE_UNKNOWN, "resource refusal unknowns are not the current schema")
    return resource, path


def check_marker(evidence: Path, commit: str, tree: str, files: dict[str, str]) -> tuple[dict, Path]:
    path = evidence / MARKER_PATH
    marker = read_json(path, "independent current-evidence marker")
    require(marker.get("schema_version") == MARKER_SCHEMA_VERSION, "independent marker schema version unsupported")
    require(marker.get("status") == "supported", "independent current-evidence marker is not supported")
    require(marker.get("producer") == "receive_backing_build_gate", "independent marker producer is invalid")
    require_source_record(marker.get("source"), commit, tree, files, "independent marker")
    require(marker.get("profiles") == list(PROFILES), "independent marker profile set is invalid")
    require(isinstance(marker.get("resource_sha256"), str), "independent marker resource binding is invalid")
    require(isinstance(marker.get("oracle_sha256"), str), "independent marker oracle binding is invalid")
    return marker, path


def evidence_path(evidence: Path, value: object, label: str) -> Path:
    require(isinstance(value, str) and value != "", f"{label} path is missing")
    path = Path(value)
    return path if path.is_absolute() else evidence / path


def check_profiles(status: dict, evidence: Path, tree: str) -> dict[str, str]:
    image_hashes: dict[str, str] = {}
    for profile in PROFILES:
        record = status["profiles"][profile]
        require(record.get("status") == "supported", f"profile is not supported: {profile}")
        require(record.get("profile_identity") == profile, f"profile identity mismatch: {profile}")
        require(record.get("profile") == profile, f"captured profile name mismatch: {profile}")
        require(record.get("source_tree") == tree, f"profile source tree mismatch: {profile}")
        toolchain = record.get("toolchain")
        require(isinstance(toolchain, dict), f"profile toolchain provenance is invalid: {profile}")
        for tool in ("rustc", "cargo", "llvm"):
            require(isinstance(toolchain.get(tool), str) and toolchain[tool], f"profile toolchain provenance is incomplete: {profile}")
        require(isinstance(record.get("target"), str) and record["target"], f"profile target provenance is missing: {profile}")
        codegen = record.get("codegen")
        require(isinstance(codegen, dict), f"profile codegen provenance is invalid: {profile}")
        for key in ("opt_level", "debug", "lto", "panic"):
            require(key in codegen, f"profile codegen provenance is incomplete: {profile}")
        invocation = record.get("invocation")
        require(
            isinstance(invocation, list) and invocation and all(isinstance(item, str) and item for item in invocation),
            f"profile invocation is missing: {profile}",
        )
        image = evidence_path(evidence, record.get("image_path"), f"profile image: {profile}")
        require(image.is_file(), f"profile image is missing: {profile}")
        digest = sha256_file(image)
        require(record.get("image_sha256") == digest, f"profile image hash mismatch: {profile}")
        image_hashes[profile] = digest
    return image_hashes


def check_oracle(status: dict, evidence: Path) -> str:
    oracle = status["oracle"]
    require(oracle.get("status") == "adopted", "current oracle is not independently reviewed and adopted")
    require(oracle.get("schema_version") == ORACLE_SCHEMA_VERSION, "oracle schema version unsupported")
    require(oracle.get("adoption_marker") == ORACLE_ADOPTION_MARKER, "oracle adoption marker is invalid")
    path = evidence_path(evidence, oracle.get("path"), "oracle")
    require(path.is_file(), "current oracle path is missing")
    digest = sha256_file(path)
    require(oracle.get("sha256") == digest, "current oracle file hash mismatch")
    payload = read_json(path, "current oracle")
    require(payload.get("schema_version") == ORACLE_SCHEMA_VERSION, "current oracle schema is unsupported")
    require(payload.get("adoption_marker") == ORACLE_ADOPTION_MARKER, "current oracle adoption marker is invalid")
    return digest


def check_adoption_receipt(
    source: Path,
    evidence: Path,
    commit: str,
    tree: str,
    files: dict[str, str],
    status: dict,
    resource_path: Path,
    marker_path: Path,
    image_hashes: dict[str, str],
    oracle_digest: str,
) -> None:
    receipt_path = source / ADOPTION_RECEIPT_PATH
    require(receipt_path.is_file(), "source-committed adoption receipt is missing")
    receipt = read_json(receipt_path, "source-committed adoption receipt")
    require(receipt.get("schema_version") == RECEIPT_SCHEMA_VERSION, "adoption receipt schema version unsupported")
    require(receipt.get("status") == "adopted", "source-committed adoption receipt is not adopted")
    require(receipt.get("reviewer"), "adoption receipt reviewer is missing")
    require(receipt.get("adoption_marker") == ORACLE_ADOPTION_MARKER, "adoption receipt marker is invalid")
    require_source_record(receipt.get("source"), commit, tree, files, "adoption receipt")
    require(receipt.get("profiles") == list(PROFILES), "adoption receipt profile identities are incomplete")
    require(receipt.get("profile_image_sha256") == image_hashes, "adoption receipt profile hashes mismatch")
    expected_evidence = {
        "native_status_sha256": sha256_file(evidence / "native-proof-status.json"),
        "resource_sha256": sha256_file(resource_path),
        "marker_sha256": sha256_file(marker_path),
        "oracle_sha256": oracle_digest,
    }
    require(receipt.get("evidence") == expected_evidence, "adoption receipt evidence hashes mismatch")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--evidence-dir", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=None)
    args = parser.parse_args()
    if args.timeout is not None and (not math.isfinite(args.timeout) or args.timeout <= 0):
        print("REFUSED: --timeout must be positive")
        return 2
    source = args.source.resolve()
    evidence = args.evidence_dir.resolve()
    try:
        commit, tree, files = source_identity(source)
        status_path = evidence / "native-proof-status.json"
        require(status_path.is_file(), "native-proof-status.json is missing")
        status = read_json(status_path, "native-proof status")
        require_status_shape(status)
        require_source_record(status.get("source"), commit, tree, files, "native")
        resource, resource_path = check_resource(evidence, commit, tree, files)
        if status.get("status") != "supported":
            raise Refused(status.get("reason", "native proof is not supported"))
        marker, marker_path = check_marker(evidence, commit, tree, files)
        image_hashes = check_profiles(status, evidence, tree)
        oracle_digest = check_oracle(status, evidence)
        require(resource.get("status") == "supported", "resource/P/CPU evidence is not supported")
        require(marker.get("resource_sha256") == sha256_file(resource_path), "independent marker resource hash mismatch")
        require(marker.get("oracle_sha256") == oracle_digest, "independent marker oracle hash mismatch")
        check_adoption_receipt(
            source,
            evidence,
            commit,
            tree,
            files,
            status,
            resource_path,
            marker_path,
            image_hashes,
            oracle_digest,
        )
    except Refused as error:
        print(f"REFUSED: {error}")
        return 2
    print("SUPPORTED: current native evidence schema is complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
