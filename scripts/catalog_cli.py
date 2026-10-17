#!/usr/bin/env python3
"""Workspace-local text projection over the archived Pi v1.4.0 release."""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile

import importlib.util

TEXT = ('anthropic', 'deepseek', 'google', 'openai')


def release_checker(root: Path):
    spec = importlib.util.spec_from_file_location('pirua_release_catalog', root / 'conformance/catalog/v1.4.0/check.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def family_artifact(generated: Path, family: str) -> bytes:
    if family != "text":
        raise ValueError("unknown catalog family")
    providers = TEXT
    catalogs = []
    for provider in providers:
        # Preserve the accepted model-array JSON bytes and source model order.
        models = (generated / family / (provider + ".json")).read_text().strip()
        catalogs.append('{"provider":' + json.dumps(provider) + ',"models":' + models + '}')
    provenance = (generated.parent / "v1.4.0/provenance.json").read_text().strip()
    return ('{"schemaVersion":1,"family":' + json.dumps(family) + ',"provenance":' + provenance
            + ',"catalogs":[' + ','.join(catalogs) + ']}\n').encode()


def write_new(path: Path, data: bytes) -> None:
    # Stage in an owned private directory on the destination filesystem. Failures
    # clean only this storage: the shared final pathname is never unlinked.
    with tempfile.TemporaryDirectory(prefix=".pi-catalog-export-", dir=path.parent) as temporary:
        staged = Path(temporary) / "artifact"
        with staged.open("xb") as output:
            output.write(data)
            output.flush()
        # Publish only the fully written, closed inode. link is atomic and refuses
        # any existing destination, including a creator racing this exact call.
        os.link(staged, path, follow_symlinks=False)


def execute(family: str, output: Path | None, root: Path) -> None:
    if output is not None and os.path.lexists(output):
        raise FileExistsError("output already exists")
    release_checker(root).verify(root)
    if output is not None:
        write_new(output, family_artifact(root / 'conformance/catalog/generated', family))
    print("validated " + family + " catalog against the fixed released inputs")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("family", choices=("text",))
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--output", type=Path)
    argv = list(sys.argv[1:] if argv is None else argv)
    tail = argv[1:]
    if not (not tail or tail == ["--check"] or len(tail) == 2 and tail[0] == "--output"):
        parser.error("expected no arguments, --check, or --output PATH")
    args = parser.parse_args(argv)
    try:
        execute(args.family, args.output, Path.cwd())
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print("catalog developer command failed: " + type(error).__name__, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
