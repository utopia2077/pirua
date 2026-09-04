#!/usr/bin/env python3
"""Generate and verify the closed S03 AI behavior and export inventories."""

from __future__ import annotations

import argparse
import collections
import copy
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path
from typing import Any

import validate_docs as validator


ROOT = Path(__file__).resolve().parents[1]
BEHAVIOR_OUTPUT = ROOT / "conformance/ai-domain/behavior-inventory.json"
EXPORT_MAPPING = ROOT / "conformance/ai-domain/export-mapping.toml"
EXPORT_MANIFEST = ROOT / "upstream/manifests/ai-exports.toml"
EXPORT_OUTPUT = ROOT / "conformance/ai-domain/inventory.json"
ROUTES_MANIFEST = ROOT / "upstream/manifests/routes.toml"
UPSTREAM_MAP = ROOT / "upstream-map.toml"

SOURCE_COMMIT = "853a80d26c90a14c1886f0ebb8ffaae133ca2185"
EXPECTED_BEHAVIOR_ROWS = 327
EXPECTED_EXPORT_ROWS = 699
EXPECTED_TARGET_PROBES = 120
EXPECTED_SUBPATH_COUNTS = {".": 200, "./compat": 260, "other": 239}
EXPECTED_DISPOSITIONS = {
    "s02-dto": 146,
    "implemented": 123,
    "rust-nonapplicable": 22,
    "mapped-s04": 163,
    "mapped-s05": 239,
    "external-schema-projection": 6,
}
EXPECTED_SURFACES = {
    "ai.domain": 195,
    "ai.models-auth": 96,
    "ai.api-adapters": 163,
    "ai.retry-compat": 4,
    "ai.catalog.projection": 39,
    "ai.compat": 52,
    "ai.faux": 42,
    "ai.oauth-images": 26,
    "ai.provider-factories": 76,
    "external": 6,
}
EXTERNAL_KEYS = {
    ".:Static",
    ".:TSchema",
    ".:Type",
    "./compat:Static",
    "./compat:TSchema",
    "./compat:Type",
}
BEHAVIOR_FIELDS = {"expected", "id", "owner", "polarity", "source", "test"}
BEHAVIOR_POLARITIES = {"positive", "negative", "boundary", "race"}
FRAGMENTS = (
    ("conformance/ai-domain/stream-inventory.json", 22),
    ("conformance/ai-models/auth-store-inventory.json", 58),
    ("conformance/ai-models/registry-inventory.json", 30),
    ("conformance/ai-domain/tool-validation-inventory.json", 30),
    ("conformance/ai-domain/message-transform-inventory.json", 31),
    ("conformance/ai-domain/overflow-inventory.json", 43),
    ("conformance/ai-domain/retry-inventory.json", 54),
    ("conformance/ai-domain/models-helpers-inventory.json", 20),
    ("conformance/ai-domain/portable-helpers-inventory.json", 27),
    ("conformance/ai-domain/session-resources-inventory.json", 12),
)
EXPECTED_FRAGMENT_DIGESTS = {
    "conformance/ai-domain/stream-inventory.json": "8706960d4effb2c965140a2d594b284409ac83f9eee2ec5dc197d6ffe217e847",
    "conformance/ai-models/auth-store-inventory.json": "84d9668cc341a418ee2aa881394909ec0a18f92fc4c9ea3a9f239461bb213c37",
    "conformance/ai-models/registry-inventory.json": "3425d14ba300fa1f2d565709b3ae8c5c1b7de554e4e98b672d6c29e4a053bcbb",
    "conformance/ai-domain/tool-validation-inventory.json": "3cfbccec91cb156c4f88b9071bec1cb483d399b5b8961c5621cc9a4ecac916e0",
    "conformance/ai-domain/message-transform-inventory.json": "6ea66ad888628f3e2908a7f05958aab4bb860904765c5c27b176bba2d118e33a",
    "conformance/ai-domain/overflow-inventory.json": "18aeab2844e0229cad6fb89f78dea5384156a32dc9496661d7e038cd3da145c9",
    "conformance/ai-domain/retry-inventory.json": "a190d00c154d2ab272498d2e6ba7bcad265bf0cd4d56cc286b3cd4af82a9b24a",
    "conformance/ai-domain/models-helpers-inventory.json": "15cc02f90b0b17681dea7e65655c774b94a37213f91e3c20dadc0c2f3a26071f",
    "conformance/ai-domain/portable-helpers-inventory.json": "8aea0cbef184e4cea14d7f71bf33e9f77a5959e3a8c365c5b81158cde46b8736",
    "conformance/ai-domain/session-resources-inventory.json": "8cb1c74d1b99af601327e2c1fe94ef05a6c94cb0311c1a8036755713f70b7ee1",
}
EXPECTED_TREE_PACKAGES = {
    "ahash": "0.8.12",
    "fancy-regex": "0.19.0",
    "getrandom": "0.3.4",
    "jsonschema": "0.52.1",
    "regex": "1.13.1",
    "ryu-js": "1.0.3",
    "uuid-simd": "0.8.0",
}
FORBIDDEN_TREE_PACKAGES = {
    "async-trait",
    "idna",
    "jsonschema-macros",
    "reqwest",
    "rustls",
    "tokio",
}


class ConformanceError(RuntimeError):
    """A deterministic closure invariant failed."""


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ConformanceError(f"cannot read JSON {path.relative_to(ROOT)}: {error}") from error
    if not isinstance(value, dict):
        raise ConformanceError(f"JSON root must be an object: {path.relative_to(ROOT)}")
    return value


def load_toml(path: Path) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ConformanceError(f"cannot read TOML {path.relative_to(ROOT)}: {error}") from error


def json_text(value: dict[str, Any]) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2) + "\n"


def canonical_json_digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def rust_test_bindings(root: Path = ROOT) -> set[str]:
    bindings: set[str] = set()
    for path in (root / "crates").glob("**/*.rs"):
        text = path.read_text(encoding="utf-8")
        bindings.update(re.findall(r"(?:pub\s+)?fn\s+([a-z][a-z0-9_]*)\s*\(", text))
    return bindings


def build_behavior_inventory(
    fragments: list[tuple[str, int, dict[str, Any]]],
    bindings: set[str],
) -> dict[str, Any]:
    if [(path, count) for path, count, _ in fragments] != list(FRAGMENTS):
        raise ConformanceError("behavior fragment order changed")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for path, expected_count, document in fragments:
        declared_count = document.get("expectedRows")
        fragment_rows = document.get("rows")
        if declared_count != expected_count or not isinstance(fragment_rows, list):
            raise ConformanceError(
                f"fragment count contract failed for {path}: expected {expected_count}"
            )
        if len(fragment_rows) != expected_count:
            raise ConformanceError(
                f"fragment row count failed for {path}: {len(fragment_rows)} != {expected_count}"
            )
        for index, raw_row in enumerate(fragment_rows):
            if not isinstance(raw_row, dict) or set(raw_row) != BEHAVIOR_FIELDS:
                raise ConformanceError(f"behavior row fields invalid at {path}:{index}")
            if any(not isinstance(raw_row[field], str) or not raw_row[field] for field in BEHAVIOR_FIELDS):
                raise ConformanceError(f"behavior row contains an empty field at {path}:{index}")
            stable_id = raw_row["id"]
            if stable_id in seen:
                raise ConformanceError(f"duplicate behavior id: {stable_id}")
            if raw_row["polarity"] not in BEHAVIOR_POLARITIES:
                raise ConformanceError(f"invalid behavior polarity: {stable_id}")
            if not raw_row["owner"].startswith("S03-"):
                raise ConformanceError(f"behavior owner is outside S03: {stable_id}")
            if raw_row["test"] not in bindings:
                raise ConformanceError(f"behavior test binding is not live: {stable_id}")
            seen.add(stable_id)
            rows.append(copy.deepcopy(raw_row))
        actual_digest = canonical_json_digest(document)
        expected_digest = EXPECTED_FRAGMENT_DIGESTS.get(path)
        if actual_digest != expected_digest:
            raise ConformanceError(
                f"behavior fragment content changed for {path}: {actual_digest}"
            )
    if len(rows) != EXPECTED_BEHAVIOR_ROWS:
        raise ConformanceError(
            f"behavior total is {len(rows)}, expected {EXPECTED_BEHAVIOR_ROWS}"
        )
    return {
        "suite": "s03-ai-domain",
        "expectedRows": EXPECTED_BEHAVIOR_ROWS,
        "fragments": [path for path, _, _ in fragments],
        "fragmentDigests": EXPECTED_FRAGMENT_DIGESTS,
        "rows": rows,
    }


def expected_behavior_inventory(root: Path = ROOT) -> dict[str, Any]:
    fragments = [
        (path, count, load_json(root / path))
        for path, count in FRAGMENTS
    ]
    behavior = build_behavior_inventory(fragments, rust_test_bindings(root))
    validate_behavior_sources(
        behavior["rows"],
        load_toml(root / UPSTREAM_MAP.relative_to(ROOT)),
        root,
    )
    return behavior


def markdown_anchor(text: str) -> str:
    lowered = text.strip().lower()
    return re.sub(r"[^a-z0-9\u0080-\uffff -]", "", lowered).replace(" ", "-")


def validate_behavior_sources(
    rows: list[dict[str, str]],
    map_data: dict[str, Any],
    root: Path = ROOT,
) -> None:
    upstream = map_data.get("upstream", {})
    checkout_value = upstream.get("checkout")
    if upstream.get("commit") != SOURCE_COMMIT or not isinstance(checkout_value, str):
        raise ConformanceError("behavior provenance lacks the pinned upstream checkout")
    checkout = Path(checkout_value)
    pinned_pattern = re.compile(
        r"^(?P<path>[^@]+)@(?P<commit>[0-9a-f]{40})"
        r"(?::(?P<start>[0-9]+)(?:-(?P<end>[0-9]+))?)?$"
    )
    local_pattern = re.compile(
        r"^(?P<path>.+?)(?:(?::(?P<start>[0-9]+)(?:-(?P<end>[0-9]+))?)|#(?P<anchor>[a-z0-9-]+))$"
    )
    pinned_lines: dict[str, list[str]] = {}
    local_lines: dict[str, list[str]] = {}
    for row in rows:
        source = row["source"]
        pinned = pinned_pattern.fullmatch(source)
        if pinned is not None:
            if pinned.group("commit") != SOURCE_COMMIT:
                raise ConformanceError(f"behavior source commit changed: {row['id']}")
            path = pinned.group("path")
            lines = pinned_lines.get(path)
            if lines is None:
                try:
                    lines = validator.git_blob(checkout, SOURCE_COMMIT, path).decode(
                        "utf-8"
                    ).splitlines()
                except (RuntimeError, UnicodeDecodeError) as error:
                    raise ConformanceError(
                        f"behavior pinned source is missing: {row['id']}"
                    ) from error
                pinned_lines[path] = lines
            validate_source_range(row["id"], lines, pinned.group("start"), pinned.group("end"))
            continue
        local = local_pattern.fullmatch(source)
        path_value = source if local is None else local.group("path")
        path = root / path_value
        try:
            resolved = path.resolve(strict=True)
            resolved.relative_to(root.resolve())
        except (OSError, ValueError) as error:
            raise ConformanceError(f"behavior local source is missing: {row['id']}") from error
        lines = local_lines.get(str(resolved))
        if lines is None:
            lines = resolved.read_text(encoding="utf-8").splitlines()
            local_lines[str(resolved)] = lines
        validate_source_range(
            row["id"],
            lines,
            None if local is None else local.group("start"),
            None if local is None else local.group("end"),
        )
        anchor = None if local is None else local.group("anchor")
        if anchor is not None:
            anchors = {
                markdown_anchor(text)
                for _, _, text in validator.markdown_headings(lines)
            }
            if anchor not in anchors:
                raise ConformanceError(f"behavior source anchor is missing: {row['id']}")


def validate_source_range(
    stable_id: str,
    lines: list[str],
    start_value: str | None,
    end_value: str | None,
) -> None:
    if start_value is None:
        return
    start = int(start_value)
    end = int(end_value) if end_value is not None else start
    if start == 0 or end < start or end > len(lines):
        raise ConformanceError(f"behavior source line is out of range: {stable_id}")


def manifest_body_digest(text: str) -> str:
    try:
        _, body = text.split("\n\n", 1)
    except ValueError as error:
        raise ConformanceError("AI export manifest has no digest/body boundary") from error
    return "sha256:" + hashlib.sha256(body.encode()).hexdigest()


def mapping_digest(text: str) -> str:
    return "sha256:" + hashlib.sha256(text.encode()).hexdigest()


def selector_surface_map(
    map_data: dict[str, Any],
    checkout: Path,
    commit: str,
) -> tuple[dict[tuple[str, int], str], set[str]]:
    selected: dict[tuple[str, int], str] = {}
    selector_paths: set[str] = set()
    for selector in map_data.get("selector", []):
        if selector.get("partition") != "ai-retry":
            continue
        path = selector.get("path")
        surface = selector.get("surface")
        if not isinstance(path, str) or not isinstance(surface, str):
            raise ConformanceError("AI selector lacks path or surface")
        lines = validator.git_blob(checkout, commit, path).decode("utf-8").splitlines()
        line_set = validator.selector_line_set(selector, lines)
        if not line_set:
            raise ConformanceError(f"AI selector resolves to no lines: {selector.get('id')}")
        selector_paths.add(path)
        for index in line_set:
            key = (path, index + 1)
            if key in selected and lines[index].strip():
                raise ConformanceError(f"AI selector overlap at {path}:{index + 1}")
            selected[key] = surface
    return selected, selector_paths


def resolve_surfaces(
    symbols: list[dict[str, Any]],
    routes: dict[str, Any],
    map_data: dict[str, Any],
) -> list[str]:
    if routes.get("manifest_version") != 1 or routes.get("kind") != "route-inventory":
        raise ConformanceError("route manifest schema or kind changed")
    if routes.get("source_commit") != SOURCE_COMMIT:
        raise ConformanceError("route manifest source commit changed")
    commit = map_data.get("upstream", {}).get("commit")
    checkout_value = map_data.get("upstream", {}).get("checkout")
    if commit != SOURCE_COMMIT or not isinstance(checkout_value, str):
        raise ConformanceError("upstream map does not identify the pinned AI checkout")
    checkout = Path(checkout_value)
    route_surface = {
        entry.get("path"): entry.get("surface")
        for entry in routes.get("entry", [])
        if entry.get("watch") == "ai"
    }
    selected, selector_paths = selector_surface_map(map_data, checkout, commit)
    source_lines: dict[str, list[str]] = {}
    surfaces: list[str] = []
    for symbol in symbols:
        key = f"{symbol.get('subpath')}:{symbol.get('name')}"
        origin = symbol.get("origin")
        line = symbol.get("origin_line")
        declaration = symbol.get("origin_declaration")
        if key in EXTERNAL_KEYS:
            if origin != "typebox" or line != 0 or declaration != "":
                raise ConformanceError(f"external TypeBox origin changed: {key}")
            surfaces.append("external")
            continue
        if not isinstance(origin, str) or not isinstance(line, int) or line <= 0:
            raise ConformanceError(f"missing local export origin: {key}")
        if not isinstance(declaration, str) or not declaration:
            raise ConformanceError(f"missing origin declaration: {key}")
        if origin not in route_surface:
            raise ConformanceError(f"unrouted local export origin: {key} -> {origin}")
        lines = source_lines.get(origin)
        if lines is None:
            lines = validator.git_blob(checkout, commit, origin).decode("utf-8").splitlines()
            source_lines[origin] = lines
        if line > len(lines) or lines[line - 1].strip() != declaration:
            raise ConformanceError(f"stale origin line/declaration: {key}")
        if origin in selector_paths:
            surface = selected.get((origin, line))
            if surface is None:
                raise ConformanceError(f"origin line is outside its selector partition: {key}")
        else:
            surface = route_surface[origin]
        if not isinstance(surface, str):
            raise ConformanceError(f"invalid resolved surface: {key}")
        surfaces.append(surface)
    return surfaces


def rule_matches(rule: dict[str, Any], symbol: dict[str, Any], surface: str) -> bool:
    selectors = {
        "subpaths": symbol.get("subpath"),
        "names": symbol.get("name"),
        "origins": symbol.get("origin"),
        "surfaces": surface,
    }
    constrained = False
    for field, value in selectors.items():
        accepted = rule.get(field)
        if accepted is None:
            continue
        constrained = True
        if not isinstance(accepted, list) or value not in accepted:
            return False
    if not constrained:
        raise ConformanceError(f"mapping rule has no selector: {rule.get('id')}")
    return True


def projection_for(rule: dict[str, Any], name: str) -> str:
    if rule.get("target_mode") == "same-name":
        prefix = rule.get("target_prefix")
        if not isinstance(prefix, str):
            raise ConformanceError(f"same-name rule lacks target_prefix: {rule.get('id')}")
        overrides = rule.get("target_overrides", {})
        target = overrides.get(name, prefix + name)
    else:
        target = rule.get("rust_target")
    if not isinstance(target, str) or not target:
        raise ConformanceError(f"mapping rule lacks a concrete Rust target: {rule.get('id')}")
    if rule.get("compile_probe") and not re.fullmatch(
        r"pi_ai::[A-Za-z_][A-Za-z0-9_:]*", target
    ):
        raise ConformanceError(f"invalid exact Rust target for {rule.get('id')}:{name}")
    return target


def evidence_for(rule: dict[str, Any], name: str) -> tuple[str, str | None]:
    evidence = rule.get("evidence_overrides", {}).get(name, rule.get("evidence"))
    binding = rule.get("evidence_bindings", {}).get(
        name,
        rule.get("evidence_binding"),
    )
    if not isinstance(evidence, str):
        raise ConformanceError(f"mapping rule lacks evidence: {rule.get('id')}:{name}")
    if binding is not None and not isinstance(binding, str):
        raise ConformanceError(f"mapping rule has invalid evidence binding: {rule.get('id')}:{name}")
    return evidence, binding


def validate_evidence(
    rule: dict[str, Any],
    evidence: str,
    expected_binding: str | None,
    behavior_by_id: dict[str, dict[str, str]],
    root: Path,
) -> None:
    disposition = rule.get("disposition")
    if not isinstance(evidence, str) or ":" not in evidence:
        raise ConformanceError(f"mapping rule lacks evidence: {rule.get('id')}")
    kind, value = evidence.split(":", 1)
    if kind == "behavior":
        behavior = behavior_by_id.get(value)
        if behavior is None:
            raise ConformanceError(f"unknown behavior evidence id: {value}")
        if disposition == "implemented" and expected_binding is None:
            raise ConformanceError(
                f"implemented behavior evidence lacks a causal binding: {rule.get('id')}"
            )
        if expected_binding is not None and behavior["test"] != expected_binding:
            raise ConformanceError(
                f"behavior evidence binding mismatch for {rule.get('id')}: "
                f"{value} binds {behavior['test']}, expected {expected_binding}"
            )
    elif kind == "rust-test":
        if "#" not in value:
            raise ConformanceError(f"Rust test evidence lacks binding: {evidence}")
        path_value, binding = value.split("#", 1)
        path = root / path_value
        try:
            path.resolve(strict=True).relative_to(root.resolve())
        except (OSError, ValueError) as error:
            raise ConformanceError(f"Rust test evidence is not live: {evidence}") from error
        if f"fn {binding}(" not in path.read_text(encoding="utf-8"):
            raise ConformanceError(f"Rust test evidence is not live: {evidence}")
    elif kind == "future":
        if disposition not in {"mapped-s04", "mapped-s05"}:
            raise ConformanceError(f"present implementation claims future evidence: {rule.get('id')}")
    elif kind == "decision":
        if disposition != "rust-nonapplicable":
            raise ConformanceError(f"non-decision row uses decision evidence: {rule.get('id')}")
        path_value = value.split("#", 1)[0]
        try:
            (root / path_value).resolve(strict=True).relative_to(root.resolve())
        except (OSError, ValueError) as error:
            raise ConformanceError(f"decision evidence path is missing: {evidence}") from error
    else:
        raise ConformanceError(f"unknown evidence kind: {kind}")
    if disposition in {"implemented", "s02-dto"} and not rule.get("compile_probe"):
        raise ConformanceError(f"present projection lacks an exact target probe: {rule.get('id')}")
    if disposition == "s02-dto" and not evidence.startswith(
        "rust-test:crates/pi-ai-core/tests/schema.rs#"
    ):
        raise ConformanceError(f"S02 projection lacks schema evidence: {rule.get('id')}")
    if disposition in {"mapped-s04", "mapped-s05"} and rule.get("compile_probe"):
        raise ConformanceError(f"deferred projection claims a present implementation: {rule.get('id')}")
    if disposition == "rust-nonapplicable":
        required = ("reason", "replacement", "decision_reference", "revisit_trigger")
        if any(not isinstance(rule.get(field), str) or not rule[field] for field in required):
            raise ConformanceError(f"nonapplicable rule lacks rationale fields: {rule.get('id')}")


def validate_mapping_header(mapping: dict[str, Any]) -> list[dict[str, Any]]:
    if mapping.get("schema_version") != 1:
        raise ConformanceError("AI export mapping schema version changed")
    if mapping.get("source_commit") != SOURCE_COMMIT:
        raise ConformanceError("AI export mapping source commit changed")
    if mapping.get("expected_occurrences") != EXPECTED_EXPORT_ROWS:
        raise ConformanceError("AI export mapping expected occurrence count changed")
    if mapping.get("expected_dispositions") != EXPECTED_DISPOSITIONS:
        raise ConformanceError("AI export mapping disposition totals changed")
    if mapping.get("expected_surfaces") != EXPECTED_SURFACES:
        raise ConformanceError("AI export mapping surface totals changed")
    rules = mapping.get("rule")
    if not isinstance(rules, list) or not rules:
        raise ConformanceError("AI export mapping contains no rules")
    ids = [rule.get("id") for rule in rules]
    if any(not isinstance(rule_id, str) or not rule_id for rule_id in ids):
        raise ConformanceError("AI export mapping has a missing rule id")
    if len(ids) != len(set(ids)):
        raise ConformanceError("AI export mapping has duplicate rule ids")
    priorities = [rule.get("priority") for rule in rules]
    if any(not isinstance(priority, int) for priority in priorities):
        raise ConformanceError("AI export mapping has a non-integer priority")
    if priorities != sorted(priorities) or set(priorities) != set(range(1, 9)):
        raise ConformanceError("AI export mapping rule priority changed")
    expected_counts = [rule.get("expected_occurrences") for rule in rules]
    if any(not isinstance(count, int) or count <= 0 for count in expected_counts):
        raise ConformanceError("AI export mapping has an invalid rule count")
    if sum(expected_counts) != EXPECTED_EXPORT_ROWS:
        raise ConformanceError("AI export mapping rule counts do not total 699")
    for rule in rules:
        if rule.get("disposition") not in EXPECTED_DISPOSITIONS:
            raise ConformanceError(f"unknown disposition in rule: {rule.get('id')}")
        if not isinstance(rule.get("owner"), str) or not rule["owner"]:
            raise ConformanceError(f"mapping rule lacks owner: {rule.get('id')}")
        if rule.get("target_mode") not in {None, "same-name"}:
            raise ConformanceError(f"unknown target mode: {rule.get('id')}")
        selector_count = 0
        for field in ("subpaths", "names", "origins", "surfaces"):
            values = rule.get(field)
            if values is None:
                continue
            selector_count += 1
            if (
                not isinstance(values, list)
                or not values
                or any(not isinstance(value, str) or not value for value in values)
                or len(values) != len(set(values))
            ):
                raise ConformanceError(f"invalid {field} selector: {rule.get('id')}")
        if selector_count == 0:
            raise ConformanceError(f"mapping rule has no selector: {rule.get('id')}")
        if rule.get("target_mode") == "same-name":
            names = set(rule.get("names", []))
            for field in (
                "target_overrides",
                "evidence_overrides",
                "evidence_bindings",
            ):
                overrides = rule.get(field, {})
                if not isinstance(overrides, dict) or any(
                    not isinstance(key, str) or not isinstance(value, str) or not value
                    for key, value in overrides.items()
                ):
                    raise ConformanceError(f"invalid {field}: {rule.get('id')}")
                if not set(overrides).issubset(names):
                    raise ConformanceError(f"{field} contains an unknown name: {rule.get('id')}")
        if not isinstance(rule.get("evidence"), str) or ":" not in rule["evidence"]:
            raise ConformanceError(f"mapping rule lacks evidence: {rule.get('id')}")
        if rule.get("evidence_binding") is not None and not isinstance(
            rule["evidence_binding"], str
        ):
            raise ConformanceError(f"mapping rule has invalid evidence binding: {rule.get('id')}")
        disposition = rule["disposition"]
        if disposition in {"implemented", "s02-dto"} and not rule.get("compile_probe"):
            raise ConformanceError(
                f"present projection lacks an exact target compile probe: {rule.get('id')}"
            )
        if disposition in {"mapped-s04", "mapped-s05"} and rule.get("compile_probe"):
            raise ConformanceError(
                f"deferred projection claims a present implementation: {rule.get('id')}"
            )
        if disposition == "rust-nonapplicable":
            required = ("reason", "replacement", "decision_reference", "revisit_trigger")
            if any(
                not isinstance(rule.get(field), str) or not rule[field]
                for field in required
            ):
                raise ConformanceError(
                    f"nonapplicable rule lacks rationale fields: {rule.get('id')}"
                )
    return rules


def build_export_inventory(
    manifest: dict[str, Any],
    manifest_digest: str,
    mapping: dict[str, Any],
    mapping_sha256: str,
    routes: dict[str, Any],
    map_data: dict[str, Any],
    behavior: dict[str, Any],
    root: Path = ROOT,
) -> tuple[dict[str, Any], list[tuple[str, str]]]:
    if manifest.get("manifest_version") != 1 or manifest.get("kind") != "ai-public-exports":
        raise ConformanceError("AI export manifest schema or kind changed")
    if manifest.get("source_commit") != SOURCE_COMMIT:
        raise ConformanceError("AI export manifest source commit changed")
    if manifest.get("content_digest") != manifest_digest:
        raise ConformanceError("AI export manifest content digest is stale")
    symbols = manifest.get("symbol")
    if not isinstance(symbols, list):
        raise ConformanceError("AI export manifest lacks symbols")
    if len(symbols) != EXPECTED_EXPORT_ROWS:
        raise ConformanceError(f"AI export occurrence count is {len(symbols)}, expected 699")
    required_symbol_fields = {
        "subpath",
        "name",
        "kind",
        "source_path",
        "source_blob_oid",
        "origin",
        "origin_line",
        "origin_declaration",
    }
    for index, symbol in enumerate(symbols):
        if not isinstance(symbol, dict) or set(symbol) != required_symbol_fields:
            raise ConformanceError(f"AI export symbol fields changed at index {index}")
        if symbol["kind"] not in {"type", "value", "type-and-value"}:
            raise ConformanceError(f"AI export symbol kind changed at index {index}")
        for field in ("subpath", "name", "source_path", "source_blob_oid", "origin"):
            if not isinstance(symbol[field], str) or not symbol[field]:
                raise ConformanceError(f"AI export symbol has an empty {field} at index {index}")
        if not isinstance(symbol["origin_line"], int) or not isinstance(
            symbol["origin_declaration"], str
        ):
            raise ConformanceError(f"AI export origin location changed at index {index}")
    keys = [f"{symbol.get('subpath')}:{symbol.get('name')}" for symbol in symbols]
    if len(keys) != len(set(keys)):
        raise ConformanceError("AI export occurrences were collapsed or duplicated")
    subpath_counts = collections.Counter(
        symbol.get("subpath") if symbol.get("subpath") in {".", "./compat"} else "other"
        for symbol in symbols
    )
    if dict(subpath_counts) != EXPECTED_SUBPATH_COUNTS:
        raise ConformanceError(f"AI root/compat/other occurrence counts changed: {dict(subpath_counts)}")

    rules = validate_mapping_header(mapping)
    behavior_by_id = {row["id"]: row for row in behavior.get("rows", [])}
    surfaces = resolve_surfaces(symbols, routes, map_data)
    if dict(collections.Counter(surfaces)) != EXPECTED_SURFACES:
        raise ConformanceError(
            f"AI selector-resolved surface totals changed: {dict(collections.Counter(surfaces))}"
        )

    matches_by_rule: dict[str, list[str]] = {rule["id"]: [] for rule in rules}
    rows: list[dict[str, Any]] = []
    target_probes: list[tuple[str, str]] = []
    for symbol, surface in sorted(
        zip(symbols, surfaces, strict=True),
        key=lambda item: (item[0]["subpath"], item[0]["name"]),
    ):
        key = f"{symbol['subpath']}:{symbol['name']}"
        matches = [rule for rule in rules if rule_matches(rule, symbol, surface)]
        if not matches:
            raise ConformanceError(f"AI export has zero matching rules: {key}")
        if len(matches) != 1:
            raise ConformanceError(
                f"AI export has multiple matching rules: {key} -> {[rule['id'] for rule in matches]}"
            )
        rule = matches[0]
        if symbol["origin"] == "packages/ai/src/utils/retry.ts" and "surfaces" not in rule:
            raise ConformanceError(f"retry export mapping must use its line selector surface: {key}")
        matches_by_rule[rule["id"]].append(key)
        target = projection_for(rule, symbol["name"])
        evidence, evidence_binding = evidence_for(rule, symbol["name"])
        validate_evidence(rule, evidence, evidence_binding, behavior_by_id, root)
        if rule.get("compile_probe"):
            target_probes.append((target, symbol["kind"]))
        row = {
            "key": key,
            "subpath": symbol["subpath"],
            "name": symbol["name"],
            "kind": symbol["kind"],
            "sourcePath": symbol["source_path"],
            "origin": symbol["origin"],
            "originLine": symbol["origin_line"],
            "originDeclaration": symbol["origin_declaration"],
            "surface": surface,
            "disposition": rule["disposition"],
            "rustTarget": target,
            "owner": rule["owner"],
            "evidence": evidence,
            "ruleId": rule["id"],
        }
        if rule["disposition"] == "rust-nonapplicable":
            for field in ("reason", "replacement", "decision_reference", "revisit_trigger"):
                row[field] = rule[field]
        rows.append(row)

    for rule in rules:
        matched = matches_by_rule[rule["id"]]
        expected = rule.get("expected_occurrences")
        if len(matched) != expected:
            raise ConformanceError(
                f"mapping rule {rule['id']} matched {len(matched)}, expected {expected}"
            )
        if set(rule.get("subpaths", [])) == {".", "./compat"} and rule.get("names"):
            expected_keys = {
                f"{subpath}:{name}"
                for subpath in rule["subpaths"]
                for name in rule["names"]
            }
            if set(matched) != expected_keys:
                raise ConformanceError(f"root/compat pair coverage changed for rule {rule['id']}")

    dispositions = collections.Counter(row["disposition"] for row in rows)
    if dict(dispositions) != EXPECTED_DISPOSITIONS:
        raise ConformanceError(f"AI disposition totals changed: {dict(dispositions)}")
    external = {row["key"] for row in rows if row["surface"] == "external"}
    if external != EXTERNAL_KEYS:
        raise ConformanceError(f"external schema projection keys changed: {sorted(external)}")
    target_probes = sorted(set(target_probes))
    if len(target_probes) != EXPECTED_TARGET_PROBES:
        raise ConformanceError(
            f"exact Rust target probe count is {len(target_probes)}, expected 120"
        )
    return (
        {
            "schemaVersion": 1,
            "sourceCommit": SOURCE_COMMIT,
            "sourceManifestDigest": manifest_digest,
            "mappingDigest": mapping_sha256,
            "expectedRows": EXPECTED_EXPORT_ROWS,
            "dispositionTotals": EXPECTED_DISPOSITIONS,
            "surfaceTotals": EXPECTED_SURFACES,
            "rows": rows,
        },
        target_probes,
    )


def expected_export_inventory(
    behavior: dict[str, Any],
    root: Path = ROOT,
) -> tuple[dict[str, Any], list[tuple[str, str]]]:
    manifest_text = (root / EXPORT_MANIFEST.relative_to(ROOT)).read_text(encoding="utf-8")
    mapping_text = (root / EXPORT_MAPPING.relative_to(ROOT)).read_text(encoding="utf-8")
    manifest = tomllib.loads(manifest_text)
    mapping = tomllib.loads(mapping_text)
    routes = load_toml(root / ROUTES_MANIFEST.relative_to(ROOT))
    map_data = load_toml(root / UPSTREAM_MAP.relative_to(ROOT))
    return build_export_inventory(
        manifest,
        manifest_body_digest(manifest_text),
        mapping,
        mapping_digest(mapping_text),
        routes,
        map_data,
        behavior,
        root,
    )


def compile_target_probe(targets: list[tuple[str, str]], root: Path = ROOT) -> None:
    if not targets:
        raise ConformanceError("exact Rust target compile probe contains no targets")
    type_targets = [target for target, kind in targets if kind != "value"]
    value_targets = [target for target, kind in targets if kind == "value"]
    imports = "\n".join(
        f"use {target} as ProjectionType{index:03d};"
        for index, target in enumerate(type_targets)
    )
    expressions = "\n    ".join(
        f"let _ = {target_probe_expression(target)};" for target in value_targets
    )
    source = (
        "#![allow(unused_imports)]\n"
        f"{imports}\n"
        "#[allow(dead_code)]\n"
        "fn probe_values() {\n"
        f"    {expressions}\n"
        "}\n"
        "fn main() {}\n"
    )
    target_dir = root / "target/ai-conformance-probe"
    target_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pi-ai-facade-") as directory:
        project = Path(directory)
        (project / "src").mkdir()
        cargo = (
            "[package]\n"
            'name = "pi-ai-facade-probe"\n'
            'version = "0.0.0"\n'
            'edition = "2024"\n\n'
            "[workspace]\n\n"
            "[dependencies]\n"
            f"pi-ai = {{ path = {json.dumps(str(root / 'crates/pi-ai'))} }}\n"
        )
        (project / "Cargo.toml").write_text(cargo, encoding="utf-8")
        (project / "src/main.rs").write_text(source, encoding="utf-8")
        environment = os.environ.copy()
        environment["CARGO_NET_OFFLINE"] = "true"
        environment["CARGO_TARGET_DIR"] = str(target_dir)
        result = subprocess.run(
            [
                "cargo",
                "check",
                "--quiet",
                "--offline",
                "--manifest-path",
                str(project / "Cargo.toml"),
            ],
            cwd=root,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            diagnostic = (result.stdout + result.stderr).strip()
            raise ConformanceError(f"exact Rust target compile probe failed:\n{diagnostic}")


def target_probe_expression(target: str) -> str:
    if target == "pi_ai::parse_json_with_repair":
        return target + "::<pi_ai::JsonValue>"
    return target


def verify_dependency_contract(
    root: Path = ROOT,
    workspace_data: dict[str, Any] | None = None,
    core_data: dict[str, Any] | None = None,
    tree_output: str | None = None,
) -> None:
    if workspace_data is None:
        workspace_data = load_toml(root / "Cargo.toml")
    if core_data is None:
        core_data = load_toml(root / "crates/pi-ai-core/Cargo.toml")
    dependencies = workspace_data.get("workspace", {}).get("dependencies", {})
    expected_direct = {
        "jsonschema": ("=0.52.1", False, []),
        "regex": ("=1.13.1", False, ["std"]),
        "ryu-js": ("=1.0.3", False, []),
    }
    for name, (version, defaults, features) in expected_direct.items():
        declaration = dependencies.get(name)
        if not isinstance(declaration, dict):
            raise ConformanceError(f"missing exact S03 dependency declaration: {name}")
        actual = (
            declaration.get("version"),
            declaration.get("default-features", True),
            declaration.get("features", []),
        )
        if actual != (version, defaults, features):
            raise ConformanceError(f"S03 dependency declaration changed: {name} -> {actual}")
    core_dependencies = core_data.get("dependencies", {})
    expected_core_dependencies = {
        "jsonschema",
        "pi-telemetry",
        "regex",
        "ryu-js",
        "serde",
        "serde_json",
    }
    if set(core_dependencies) != expected_core_dependencies:
        raise ConformanceError(
            f"pi-ai-core dependency set changed: {sorted(core_dependencies)}"
        )
    for name in expected_direct:
        if core_dependencies.get(name) != {"workspace": True}:
            raise ConformanceError(f"pi-ai-core does not inherit exact dependency: {name}")

    if tree_output is None:
        result = subprocess.run(
            [
                "cargo",
                "tree",
                "-p",
                "pi-ai-core",
                "-e",
                "normal",
                "--locked",
                "--offline",
                "--prefix",
                "none",
                "--format",
                "{p}|{f}",
            ],
            cwd=root,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise ConformanceError(
                "cannot inspect pi-ai-core dependency tree:\n"
                + (result.stdout + result.stderr).strip()
            )
        tree_output = result.stdout
    packages: dict[str, set[str]] = collections.defaultdict(set)
    features: dict[str, set[str]] = collections.defaultdict(set)
    for line in tree_output.splitlines():
        package_text, _, feature_text = line.partition("|")
        match = re.match(r"^([A-Za-z0-9_-]+) v([^ ]+)", package_text)
        if match is None:
            continue
        name, version = match.groups()
        packages[name].add(version)
        features[name].update(feature for feature in feature_text.split(",") if feature)
    for name, version in EXPECTED_TREE_PACKAGES.items():
        if packages.get(name) != {version}:
            raise ConformanceError(
                f"required dependency tree node changed: {name} -> {sorted(packages.get(name, set()))}"
            )
    forbidden = sorted(FORBIDDEN_TREE_PACKAGES.intersection(packages))
    if forbidden:
        raise ConformanceError(f"forbidden pi-ai-core dependencies present: {forbidden}")
    if features.get("jsonschema"):
        raise ConformanceError(
            f"jsonschema optional features are enabled: {sorted(features['jsonschema'])}"
        )
    if features.get("ryu-js"):
        raise ConformanceError(
            f"ryu-js optional features are enabled: {sorted(features['ryu-js'])}"
        )


def check_generated(path: Path, expected: str) -> None:
    if not path.is_file() or path.read_text(encoding="utf-8") != expected:
        try:
            display_path = path.relative_to(ROOT)
        except ValueError:
            display_path = path
        raise ConformanceError(f"stale generated AI conformance output: {display_path}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        behavior = expected_behavior_inventory()
        inventory, target_probes = expected_export_inventory(behavior)
        outputs = {
            BEHAVIOR_OUTPUT: json_text(behavior),
            EXPORT_OUTPUT: json_text(inventory),
        }
        if args.check:
            for path, expected in outputs.items():
                check_generated(path, expected)
        else:
            for path, expected in outputs.items():
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(expected, encoding="utf-8")
        verify_dependency_contract()
        compile_target_probe(target_probes)
    except (ConformanceError, OSError, tomllib.TOMLDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    if not args.check:
        print(
            f"generated {EXPECTED_BEHAVIOR_ROWS} behavior rows and "
            f"{EXPECTED_EXPORT_ROWS} export occurrences"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
