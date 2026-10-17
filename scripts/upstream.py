"""Resolve the pinned upstream object store independently of the caller cwd."""
from __future__ import annotations

import argparse
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CHECKOUT = "upstream/pi-mono"
PIN = "4a080252f06caf63a525d7873c5d94d3c23887e0"


def resolve_upstream(checkout: str | None = None, *, root: Path = ROOT) -> Path:
    # Changed probe-map checkout, env, local Git setting, then repo default.
    configured = subprocess.run(
        ["git", "-C", str(root), "config", "--local", "--get", "pirua.upstreamRoot"],
        capture_output=True, text=True,
    )
    fallback = configured.stdout.strip() if configured.returncode == 0 else ""
    value = (
        checkout if checkout and checkout != DEFAULT_CHECKOUT
        else os.environ.get("PIRUA_UPSTREAM_ROOT", fallback or DEFAULT_CHECKOUT)
    )
    path = Path(value).expanduser()
    path = (path if path.is_absolute() else root / path).resolve()
    if not path.is_dir():
        raise ValueError(f"upstream object-store checkout does not exist: {path}; set PIRUA_UPSTREAM_ROOT")
    result = subprocess.run(
        ["git", "-C", str(path), "cat-file", "-e", PIN + "^{commit}"],
        capture_output=True, text=True,
    )
    if result.returncode:
        raise ValueError("upstream object store lacks the pinned commit: " + PIN)
    return path


def upstream_environment(environment: dict[str, str], *, root: Path = ROOT) -> dict[str, str]:
    return {**environment, "PIRUA_UPSTREAM_ROOT": str(resolve_upstream(root=root))}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--checkout")
    args = parser.parse_args()
    print(resolve_upstream(args.checkout))
