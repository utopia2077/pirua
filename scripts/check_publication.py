#!/usr/bin/env python3
"""Check publishable Git inputs without printing matched content.

By default, inspect the index plus current tracked and unignored files. Ignored
local inputs are outside that scope; force-added ignored files are still checked.
With --history, also inspect all reachable blobs, commits, annotated tags, ref
names, and every path in every reachable commit tree. Unreachable objects,
reflogs, nested repositories, and arbitrary credential formats are not covered.
Use --self-check to exercise the CLI against isolated temporary Git repositories.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from typing import Iterator


# Assemble guard vocabulary so this source does not masquerade as leaked data.
PRIVATE_PATHS = (
    "." + "agents", "." + "claude", "skills" + "-lock.json",
    "docs/exec" + "-plans", "docs/hand" + "off", "orches" + "tration",
    "private-development" + "-records",
)
PRIVATE_REFERENCE = re.compile(
    b"|".join(re.escape(p.encode()) +
              (br"(?:/|\b)" if p in {"skills" + "-lock.json", "private-development" + "-records"}
               else b"/") for p in PRIVATE_PATHS)
)
PEM_LABEL = br"((?:RSA |EC |DSA |OPENSSH |ENCRYPTED )?PRIVATE KEY)"
PATTERNS = {
    "stored_private_key": re.compile(
        br"-----BEGIN " + PEM_LABEL + br"-----\r?\n"
        br"(?:(?:Proc-Type|DEK-Info):[^\r\n]*\r?\n)*\s*"
        br"[A-Za-z0-9+/=\r\n]{32,}-----END \1-----"
    ),
    "owner_home": re.compile(
        br"/(?:home|Users)/[A-Za-z0-9_.-]+(?:/|\b)|"
        br"[A-Za-z]:\\Users\\[A-Za-z0-9_.-]+(?:\\|\b)"
    ),
    "internal_model": re.compile(
        br"\bgpt-" + br"(?:6(?:\.1)?-(?:sol|astra|luna)|5\.6-sol)\b", re.I
    ),
    "internal_agent": re.compile(
        br"/root/" + br"(?:s\d{2}[_/][A-Za-z0-9_-]+|publication[_/][A-Za-z0-9_-]+)"
    ),
    "session_metadata": re.compile(
        b"source_" + br"session_id|source_" + br"agent:|context " + br"UUID"
    ),
    "github_token": re.compile(br"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{40,})\b"),
    "aws_access_key": re.compile(br"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"),
    "google_api_key": re.compile(br"\bAIza[0-9A-Za-z_-]{35}\b"),
    "slack_token": re.compile(br"\bxox[baprs]-[0-9]{8,}-[A-Za-z0-9-]{10,}\b"),
}
# These exact captured/generated data paths contain factual public model IDs.
# Only model-name matching is exempt; every other guard still applies.
PUBLIC_MODEL_DATA_NAMES = frozenset(name.encode() for name in (
    "conformance/catalog/generated/source-grouped.json",
    "conformance/catalog/generated/source-text.json",
    "conformance/catalog/generated/text/openai-codex.json",
    "conformance/catalog/generated/text/openai.json",
    "conformance/catalog/generated/text/openrouter.json",
    "conformance/catalog/inputs/models-dev.json",
    "conformance/catalog/inputs/openrouter-text.json",
    "conformance/catalog/inputs/vercel.json",
    "conformance/providers/selected/trace.json",
    "conformance/schema/ai/azure-runtime-resolved.golden.json",
))
KNOWN_PRIVATE_KEY_HASHES = {
    "3d56544226d12b51b4fbed83b5eee8407457a07124ef6826adefb0cb25b93ac6",
}


class CheckError(Exception):
    """A repository operation failed; its output must remain private."""


def git(repo: Path, *args: str) -> bytes:
    result = subprocess.run(["git", "-C", str(repo), *args], capture_output=True)
    if result.returncode:
        raise CheckError("git-operation-failed")
    return result.stdout


def display_path(raw: bytes) -> str:
    return os.fsdecode(raw)


def safe_label(raw: bytes) -> str:
    # Filenames/ref names can themselves contain a sensitive matched value.
    # Preserve a useful relative location while redacting recognized values.
    for category, pattern in PATTERNS.items():
        raw = pattern.sub(("[" + category + "]").encode(), raw)
    return display_path(raw)


def path_categories(raw: bytes) -> list[str]:
    normalized = display_path(raw).replace("\\", "/")
    return ["private_workflow_path"] if any(
        normalized == p or normalized.startswith(p + "/") or
        ("/" + p + "/") in ("/" + normalized + "/")
        for p in PRIVATE_PATHS
    ) else []


def content_categories(data: bytes, name: bytes) -> list[str]:
    categories = [category for category, pattern in PATTERNS.items()
                  if not (category == "internal_model" and name in PUBLIC_MODEL_DATA_NAMES)
                  and pattern.search(data)]
    if hashlib.sha256(data).hexdigest() in KNOWN_PRIVATE_KEY_HASHES:
        categories.append("known_private_key")
    reference_data = data
    if name == b".gitignore":
        # Only complete, positive ignore rules for the guarded local paths are
        # exempt from the reference check. Keys and all other categories still
        # scan the entire file; a comment or negated rule cannot hide a leak.
        allowed = {p.encode() for p in PRIVATE_PATHS}
        reference_data = b"\n".join(
            line for line in data.splitlines()
            if line.strip().strip(b"/") not in allowed
        )
    if PRIVATE_REFERENCE.search(reference_data):
        categories.append("private_workflow_reference")
    return sorted(set(categories))


def objects(repo: Path, oids: list[bytes]) -> Iterator[tuple[bytes, str, bytes]]:
    proc = subprocess.Popen(
        ["git", "-C", str(repo), "cat-file", "--batch"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
    )
    assert proc.stdin is not None and proc.stdout is not None
    try:
        for oid in oids:
            proc.stdin.write(oid + b"\n")
            proc.stdin.flush()
            header = proc.stdout.readline().split()
            if len(header) != 3 or header[0] != oid:
                raise CheckError("git-object-read-failed")
            size = int(header[2])
            data = proc.stdout.read(size)
            if len(data) != size or proc.stdout.read(1) != b"\n":
                raise CheckError("git-object-read-failed")
            yield oid, header[1].decode("ascii"), data
        proc.stdin.close()
        if proc.wait() != 0:
            raise CheckError("git-object-read-failed")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        proc.stdout.close()
        if not proc.stdin.closed:
            proc.stdin.close()


def scan(repo: Path, history: bool = False) -> dict:
    findings: list[dict] = []
    seen: set[tuple] = set()
    counts = {"index": 0, "current": 0, "history_objects": 0, "history_paths": 0}

    def record(scope: str, name: bytes, data: bytes, categories: list[str], oid: bytes = b"") -> None:
        if not categories:
            return
        digest = hashlib.sha256(data).hexdigest()
        identity = scope, name, digest, tuple(categories)
        if identity in seen:
            return
        seen.add(identity)
        item = {"path": scope + ":" + safe_label(name), "sha256": digest,
                "categories": sorted(set(categories))}
        if oid:
            item["gitObject"] = oid.decode("ascii")
        findings.append(item)

    index: dict[bytes, list[bytes]] = {}
    for entry in git(repo, "ls-files", "--stage", "-z").split(b"\0"):
        if not entry:
            continue
        meta, name = entry.split(b"\t", 1)
        mode, oid, stage = meta.split()
        counts["index"] += 1
        categories = path_categories(name)
        if stage != b"0":
            categories.append("unmerged_index")
        record("index-path", name, name, categories, oid)
        if mode != b"160000":  # Gitlinks are not files in this repository.
            index.setdefault(oid, []).append(name)
    for oid, kind, data in objects(repo, list(index)):
        if kind != "blob":
            raise CheckError("unexpected-index-object")
        for name in index[oid]:
            record("index", name, data, content_categories(data, name), oid)

    names = set(git(repo, "ls-files", "--cached", "--others", "--exclude-standard", "-z").split(b"\0"))
    for name in sorted(names - {b""}):
        record("current-path", name, name, path_categories(name))
        item = repo / display_path(name)
        try:
            if not item.parent.resolve().is_relative_to(repo):
                record("current", name, name, ["outside_worktree"])
                continue
            mode = item.lstat().st_mode
            if stat.S_ISLNK(mode):
                data = os.fsencode(os.readlink(item))
            elif stat.S_ISREG(mode):
                data = item.read_bytes()
            elif stat.S_ISDIR(mode):
                continue  # A submodule has its own independent object store.
            else:
                record("current", name, name, ["unsupported_file"])
                continue
        except FileNotFoundError:
            continue  # A deleted working file can still exist in the index.
        except OSError:
            record("current", name, name, ["unreadable_file"])
            continue
        counts["current"] += 1
        record("current", name, data, content_categories(data, name))

    if history:
        has_head = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--verify", "--quiet", "HEAD"],
            capture_output=True,
        ).returncode == 0
        revisions = ["--all", *(["HEAD"] if has_head else [])]
        locations: dict[bytes, set[bytes]] = {}
        trees = set(git(repo, "log", "--format=%T", *revisions).splitlines())
        # A public ref may also point directly to a tree or a tag of a tree.
        for ref_oid in git(repo, "for-each-ref", "--format=%(objectname)").splitlines():
            result = subprocess.run(
                ["git", "-C", str(repo), "rev-parse", ref_oid.decode() + "^{tree}"],
                capture_output=True,
            )
            if result.returncode == 0:
                trees.add(result.stdout.strip())
        for tree in sorted(trees):
            # Enumerate every historical path, including aliases of the same
            # blob; rev-list's single representative name cannot prove this.
            for entry in git(repo, "ls-tree", "-r", "-z", tree.decode()).split(b"\0"):
                if not entry:
                    continue
                meta, name = entry.split(b"\t", 1)
                _, _, oid = meta.split()
                locations.setdefault(oid, set()).add(name)
                counts["history_paths"] += 1
                record("history-path", name, name, path_categories(name), oid)
        oids = git(repo, "rev-list", "--objects", "--no-object-names", *revisions).splitlines()
        for oid, kind, data in objects(repo, oids):
            counts["history_objects"] += 1
            if kind in ("blob", "commit", "tag"):
                for name in sorted(locations.get(oid, {("<" + kind + ">").encode()})):
                    record("history", name, data, content_categories(data, name), oid)
        for row in git(repo, "for-each-ref", "--format=%(refname)%00%(objectname)").splitlines():
            name, oid = row.split(b"\0", 1)
            record("ref", name, name, sorted(set(path_categories(name) + content_categories(name, name))), oid)

    return {"status": "rejected" if findings else "passed", "counts": counts,
            "findings": sorted(findings, key=lambda f: (f["path"], f["sha256"]))}


def self_check() -> dict:
    checks: list[str] = []
    script = str(Path(__file__).resolve())
    with tempfile.TemporaryDirectory(prefix="pirua-publication-check-") as temp:
        repo = Path(temp)
        git(repo, "init", "-q")
        git(repo, "config", "user.name", "Publication Check")
        git(repo, "config", "user.email", "check@example.invalid")
        harmless = (b"Public Claude/GPT models, S03 suite, session_id, /root/v9/resource.\n"
                    b'Agent orchestration; {"orchestration":"parallel","agent":"public"}.\n')
        (repo / "README.md").write_bytes(harmless)
        ignore = b"\n".join(b"/" + p.encode() + b"/" for p in PRIVATE_PATHS) + b"\n"
        (repo / ".gitignore").write_bytes(ignore)
        git(repo, "add", ".")
        git(repo, "commit", "-qm", "test: establish public inputs")

        def expect(label: str, category: str | None = None, *, history: bool = False,
                   scope: str | None = None, expected_name: str | None = None) -> None:
            result = subprocess.run(
                [sys.executable, script, "--repo", str(repo), *(["--history"] if history else [])],
                capture_output=True,
            )
            try:
                report = json.loads(result.stdout)
            except (ValueError, UnicodeDecodeError):
                raise CheckError("self-check-failed") from None
            findings = report.get("findings", [])
            if category is None:
                valid = result.returncode == 0 and report.get("status") == "passed" and not findings
            else:
                valid = result.returncode == 1 and any(
                    category in f["categories"] and (scope is None or f["path"].startswith(scope + ":"))
                    and (expected_name is None or f["path"].endswith(":" + expected_name))
                    for f in findings
                )
            if not valid:
                raise CheckError("self-check-failed")
            # A scanner must never echo its input even when the input is fake.
            if b"A" * 64 in result.stdout or b"owner-control" in result.stdout:
                raise CheckError("self-check-output-leak")
            checks.append(label)

        def restore() -> None:
            git(repo, "reset", "--hard", "-q", "HEAD")
            git(repo, "clean", "-fdx", "-q")

        begin, end = b"-----BEGIN ", b"-----END "
        label = b"PRIVATE KEY"
        key = begin + label + b"-----\n" + b"A" * 64 + b"\n" + end + label + b"-----\n"
        owner = b"/home/" + b"owner-control/workspace/source\n"
        expect("public-provider-suite-and-orchestration-prose")
        (repo / "reference.txt").write_bytes(b"orches" + b"tration/ledger.md")
        expect("actual-orchestration-directory-reference", "private_workflow_reference",
               scope="current", expected_name="reference.txt")
        restore()
        orchestration_file = repo / ("orches" + "tration/control.json")
        orchestration_file.parent.mkdir()
        orchestration_file.write_bytes(harmless)
        git(repo, "add", "-f", str(orchestration_file.relative_to(repo)))
        expect("stored-orchestration-directory", "private_workflow_path", scope="index-path",
               expected_name=str(orchestration_file.relative_to(repo)))
        git(repo, "commit", "-qm", "test: retain orchestration path control")
        git(repo, "rm", "-q", str(orchestration_file.relative_to(repo)))
        git(repo, "commit", "-qm", "test: remove orchestration path control")
        expect("historical-orchestration-alias", "private_workflow_path", history=True,
               scope="history-path", expected_name=str(orchestration_file.relative_to(repo)))
        (repo / "pattern.txt").write_bytes(begin + label + b"-----\n")
        expect("key-guard-header-without-payload")
        restore()
        (repo / ".gitignore").write_bytes(ignore + key)
        expect("ignore-file-still-rejects-key", "stored_private_key")
        restore()
        (repo / ".gitignore").write_bytes(ignore + b"!/docs/exec" + b"-plans/control\n")
        expect("negated-private-ignore-is-not-exempt", "private_workflow_reference")
        restore()
        os.symlink(owner.decode().strip(), repo / "owner-link")
        expect("symlink-target-owner-path", "owner_home", scope="current")
        restore()
        data_file = repo / "conformance/catalog/inputs/models-dev.json"
        data_file.parent.mkdir(parents=True)
        model_data = b'{"id":"gpt-' + b'6-astra"}\n'
        data_file.write_bytes(model_data)
        expect("factual-public-model-identity")
        data_file.write_bytes(model_data + key)
        expect("catalog-still-rejects-private-key", "stored_private_key")
        data_file.write_bytes(model_data + owner)
        expect("catalog-still-rejects-owner-path", "owner_home")
        data_file.write_bytes(model_data + b"source_" + b"session_id: control")
        expect("catalog-still-rejects-session-metadata", "session_metadata")
        data_file.write_bytes(model_data + b"docs/exec" + b"-plans/control")
        expect("catalog-still-rejects-private-reference", "private_workflow_reference")
        data_file.write_bytes(model_data)
        git(repo, "add", str(data_file.relative_to(repo)))
        git(repo, "commit", "-qm", "test: retain public model control")
        (repo / "metadata.txt").write_bytes(model_data)
        git(repo, "add", "metadata.txt")
        git(repo, "commit", "-qm", "test: retain metadata alias control")
        git(repo, "rm", "-q", "metadata.txt")
        git(repo, "commit", "-qm", "test: remove metadata alias control")
        expect("historical-model-alias-outside-data-path", "internal_model", history=True,
               scope="history", expected_name="metadata.txt")
        (repo / "input.pem").write_bytes(key)
        expect("unignored-key-file", "stored_private_key", scope="current")
        git(repo, "add", "input.pem")
        (repo / "input.pem").write_bytes(harmless)
        expect("key-retained-only-in-index", "stored_private_key", scope="index")
        git(repo, "reset", "--hard", "-q")
        (repo / "README.md").write_bytes(owner)
        expect("modified-tracked-owner-path", "owner_home", scope="current")
        restore()
        (repo / "metadata.txt").write_bytes(
            b"source_" + b"agent: /root/" + b"s12_worker\n" + b"gpt-" + b"6-sol\n"
        )
        expect("internal-agent-metadata", "internal_agent")
        expect("internal-model-metadata", "internal_model")
        expect("source-session-metadata", "session_metadata")
        restore()
        private = repo / ("docs/exec" + "-plans/review.md")
        private.parent.mkdir(parents=True)
        private.write_bytes(harmless)
        git(repo, "add", "-f", str(private.relative_to(repo)))
        expect("force-added-ignored-process-path", "private_workflow_path", scope="index-path")
        git(repo, "commit", "-qm", "test: retain historical path control")
        git(repo, "rm", "-q", str(private.relative_to(repo)))
        git(repo, "commit", "-qm", "test: remove historical path control")
        expect("clean-current-after-private-path-deletion")
        expect("historical-alias-of-public-blob", "private_workflow_path", history=True,
               scope="history-path", expected_name=str(private.relative_to(repo)))
        (repo / "input.pem").write_bytes(key)
        git(repo, "add", "input.pem")
        git(repo, "commit", "-qm", "test: retain historical key control")
        git(repo, "rm", "-q", "input.pem")
        git(repo, "commit", "-qm", "test: remove historical key control")
        expect("clean-current-after-key-deletion")
        expect("deleted-key-still-reachable", "stored_private_key", history=True, scope="history")
        git(repo, "commit", "--allow-empty", "-qm", owner.decode().strip())
        expect("historical-commit-metadata", "owner_home", history=True, scope="history")
    return {"status": "passed", "selfChecks": checks, "findings": []}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1],
                        help="Git working tree to inspect (default: this repository)")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--history", action="store_true", help="include all reachable Git history")
    mode.add_argument("--self-check", action="store_true", help="run isolated Git rejection controls")
    args = parser.parse_args()
    try:
        report = self_check() if args.self_check else scan(args.repo.resolve(), args.history)
    except (CheckError, OSError, ValueError):
        # Git errors can contain local paths or data. Do not echo them.
        report = {"status": "error", "findings": [
            {"path": "<check>", "sha256": hashlib.sha256(b"operation-failed").hexdigest(),
             "categories": ["check_operation_failed"]}
        ]}
        print(json.dumps(report, indent=2, ensure_ascii=True))
        return 2
    print(json.dumps(report, indent=2, ensure_ascii=True))
    return 1 if report["findings"] else 0


if __name__ == "__main__":
    sys.exit(main())
