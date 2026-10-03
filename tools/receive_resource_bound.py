#!/usr/bin/env python3
"""Record current 8192 receive declarations without certifying support."""
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
RESOURCE_SCHEMA_VERSION = "s12-d-resource-v3"
DECLARED = {
    "frame_bytes": 8192,
    "index_limit": 8193,
    "backing_pool_slots": 2,
    "body_buffer_bytes": 8192,
}
RESOURCE_UNKNOWN = [
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
]
OBSERVATION_SCHEMA_VERSION = "s12-d-resource-observations-v1"
RESOURCE_FIELDS = tuple(RESOURCE_UNKNOWN)
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


def read_observations(evidence: Path) -> dict:
    observations = []
    for path in sorted(evidence.rglob("source-resource-observation.json")):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"source resource observation is unreadable: {path}: {error}") from error
        if not isinstance(value, dict) or value.get("schema_version") != OBSERVATION_SCHEMA_VERSION:
            raise ValueError(f"source resource observation schema is unsupported: {path}")
        fields = value.get("fields")
        if not isinstance(fields, dict) or set(fields) != set(RESOURCE_FIELDS):
            raise ValueError(f"source resource observation field set is incomplete: {path}")
        for field in RESOURCE_FIELDS:
            record = fields[field]
            if not isinstance(record, dict) or record.get("status") not in {"observed", "unresolved"}:
                raise ValueError(f"source resource observation field is invalid: {path}: {field}")
            evidence_record = record.get("evidence")
            if not isinstance(evidence_record, dict) or not isinstance(
                evidence_record.get("event_ids"), list
            ):
                raise ValueError(f"source resource observation evidence is invalid: {path}: {field}")
            event_ids = evidence_record["event_ids"]
            events = evidence_record.get("events")
            if not isinstance(evidence_record.get("source_owned"), bool):
                raise ValueError(f"source resource observation ownership is invalid: {path}: {field}")
            if not isinstance(events, list) or len(event_ids) != len(events):
                raise ValueError(f"source resource observation event records are invalid: {path}: {field}")
            if not all(isinstance(event_id, str) and event_id for event_id in event_ids):
                raise ValueError(f"source resource observation event IDs are invalid: {path}: {field}")
            for event in events:
                if not isinstance(event, dict) or event.get("kind") not in FIELD_EVENT_KINDS[field]:
                    raise ValueError(f"source resource observation event kind is incompatible: {path}: {field}")
            if record["status"] == "observed" and not (
                evidence_record["source_owned"] and event_ids and events
            ):
                raise ValueError(f"observed source resource field lacks owned evidence: {path}: {field}")
        observations.append(
            {
                "path": str(path.relative_to(evidence)),
                "sha256": sha256_file(path),
                "trace_id": value.get("trace_id"),
                "event_count": value.get("event_count"),
                "fields": fields,
                "guards": value.get("guards"),
            }
        )
    return {
        "schema_version": OBSERVATION_SCHEMA_VERSION,
        "profiles": observations,
        "complete_profiles": sum(
            all(record["status"] == "observed" for record in item["fields"].values())
            for item in observations
        ),
        "field_set": list(RESOURCE_FIELDS),
    }


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
    evidence.mkdir(parents=True, exist_ok=True)
    files = {}
    missing = []
    for relative in SOURCE_FILES:
        path = source / relative
        if not path.is_file():
            missing.append(relative)
        else:
            files[relative] = sha256_file(path)
    try:
        commit = git(source, "rev-parse", "HEAD")
        tree = git(source, "rev-parse", "HEAD^{tree}")
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"REFUSED: unable to identify source revision: {error}")
        return 2
    try:
        observations = read_observations(evidence)
    except ValueError as error:
        print(f"REFUSED: {error}")
        return 2
    record = {
        "schema_version": RESOURCE_SCHEMA_VERSION,
        "status": "refused",
        "source": {"commit": commit, "tree": tree, "files": files},
        "declared": DECLARED,
        "measured": {},
        "observations": observations,
        "unknown": RESOURCE_UNKNOWN,
        "missing_source_files": missing,
        "reason": (
            "source-bound observations are recorded, but aggregate support remains refused "
            "until current four-profile images, oracle, and adoption are present"
        ),
    }
    (evidence / "resource-bound.json").write_text(
        json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(record, sort_keys=True))
    return 2


if __name__ == "__main__":
    sys.exit(main())
