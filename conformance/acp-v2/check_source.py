#!/usr/bin/env python3
"""Verify the checked-in official ACP draft artifacts without network access."""

import hashlib
import json
from pathlib import Path


def main() -> None:
    root = Path(__file__).resolve().parent
    source = json.loads((root / "source.json").read_text())
    for name, item in source["files"].items():
        actual = hashlib.sha256((root / name).read_bytes()).hexdigest()
        if actual != item["sha256"]:
            raise SystemExit(f"ACP artifact differs from pinned source: {name}")
    schema = json.loads((root / "schema.json").read_text())
    metadata = json.loads((root / "meta.json").read_text())
    if source["protocolVersion"] != metadata["version"] or metadata["version"] != 2:
        raise SystemExit("ACP protocol version mismatch")
    required = schema["$defs"]["PromptResponse"]["required"]
    if "messageId" not in required:
        raise SystemExit("ACP v2 insertion receipt definition is missing")
    print(f"ACP v2 source pin verified ({len(source['files'])} artifacts)")


if __name__ == "__main__":
    main()
