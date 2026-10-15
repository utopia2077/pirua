"""Read immutable local Git objects without the historical source-oracle pin."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess


class Invalid(ValueError):
    """Invalid input or an incomplete projection (exit 3)."""


class Unavailable(RuntimeError):
    """An unavailable local object or tool (exit 4)."""


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()


def digest(value: object) -> str:
    return hash_bytes(canonical(value))


def hash_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def local_path(root: Path, value: str) -> Path:
    if not isinstance(value, str) or not value or Path(value).is_absolute() or ".." in Path(value).parts:
        raise Invalid("evidence path must be relative and contained")
    path = (root / value).resolve()
    if not path.is_relative_to(root.resolve()) or not path.is_file():
        raise Invalid(f"missing or uncontained evidence: {value}")
    return path


@dataclass(frozen=True)
class Entry:
    path: str
    mode: str
    oid: str
    type: str

    def payload(self) -> dict:
        return dict(path=self.path, mode=self.mode, oid=self.oid, type=self.type)


class Objects:
    def __init__(self, root: Path):
        self.root = root
        self._blobs: dict[str, bytes] = {}
        self._trees: dict[str, dict[str, Entry]] = {}

    def git(self, *argv: str, data: bytes | None = None) -> bytes:
        env = {**os.environ, "GIT_NO_REPLACE_OBJECTS": "1", "GIT_OPTIONAL_LOCKS": "0", "GIT_NO_LAZY_FETCH": "1"}
        try:
            result = subprocess.run(["git", "-C", str(self.root), *argv], capture_output=True, env=env, input=data)
        except OSError as error:
            raise Unavailable("local Git tool unavailable") from error
        if result.returncode:
            raise Unavailable(f"local Git object operation failed: {argv[0]}")
        return result.stdout

    def commit(self, value: str) -> str:
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", value):
            raise Invalid("revision must be a full lowercase commit object id")
        object_format = self.git('rev-parse', '--show-object-format').strip()
        lengths = {b'sha1':40, b'sha256':64}
        if object_format not in lengths:
            raise Unavailable('unsupported local Git object format')
        if len(value) != lengths[object_format]:
            raise Invalid('revision must be full length for the Git object format')
        if self.git("cat-file", "-t", value).strip() != b"commit":
            raise Invalid("revision object must itself be a commit")
        if self.git('rev-parse', '--verify', value).strip().decode('ascii') != value:
            raise Invalid('revision must equal its canonical resolved object id')
        return value

    def tree(self, commit: str) -> dict[str, Entry]:
        self.commit(commit)
        if commit not in self._trees:
            entries = {}
            for record in self.git("ls-tree", "-r", "-z", "--full-tree", commit).split(b"\0"):
                if not record:
                    continue
                metadata, raw_path = record.split(b"\t", 1)
                try:
                    path = raw_path.decode("utf-8")
                except UnicodeError as error:
                    raise Invalid("Git path has unsupported UTF-8 encoding") from error
                if any(c in path for c in "\0\n\r"):
                    raise Invalid("Git path contains unsupported control characters")
                mode, typ, oid = metadata.decode("ascii").split()
                entries[path] = Entry(path, mode, oid, typ)
            blobs = sorted({e.oid for e in entries.values() if e.type == 'blob'})
            checks = self.git('cat-file', '--batch-check=%(objectname) %(objecttype)', data=('\n'.join(blobs)+'\n').encode()) if blobs else b''
            if any(row.endswith(b' missing') for row in checks.splitlines()):
                raise Unavailable('local Git tree references a missing blob')
            self._trees[commit] = entries
        return self._trees[commit]

    def blob(self, entry: Entry) -> bytes:
        if entry.type != "blob" or entry.mode not in {"100644", "100755"}:
            raise Invalid(f"unsupported Git object or mode: {entry.path} {entry.type} {entry.mode}")
        if entry.oid not in self._blobs:
            self._blobs[entry.oid] = self.git("cat-file", "blob", entry.oid)
        return self._blobs[entry.oid]

    def text(self, entry: Entry) -> str:
        try:
            raw = self.blob(entry)
            if b"\0" in raw:
                raise Invalid(f"binary source cannot be selected: {entry.path}")
            return raw.decode("utf-8")
        except UnicodeError as error:
            raise Invalid(f"source has unsupported UTF-8 encoding: {entry.path}") from error
