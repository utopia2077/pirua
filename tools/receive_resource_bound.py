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
    record = {
        "schema_version": RESOURCE_SCHEMA_VERSION,
        "status": "refused",
        "source": {"commit": commit, "tree": tree, "files": files},
        "declared": DECLARED,
        "measured": {},
        "unknown": RESOURCE_UNKNOWN,
        "missing_source_files": missing,
        "reason": "declarations are recorded; complete current native/resource evidence is absent",
    }
    (evidence / "resource-bound.json").write_text(
        json.dumps(record, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(record, sort_keys=True))
    return 2


if __name__ == "__main__":
    sys.exit(main())
