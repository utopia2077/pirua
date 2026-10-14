#!/usr/bin/env python3
"""Validate design structure, the 15-crate DAG, and pinned upstream routing."""

from __future__ import annotations

import argparse
import hashlib
import re
import subprocess
import sys
import tomllib
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote


try:
    from .upstream import resolve_upstream, upstream_environment
except ImportError:
    from upstream import resolve_upstream, upstream_environment

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TOPOLOGY_DOC = ROOT / "docs/architecture/02-crate-topology.md"
DEFAULT_TOPOLOGY_MAP = ROOT / "workspace-topology.toml"
APPROVED_DEV_EDGES = {("pi-harness-runtime", "pi-agent-sqlite")}
DEFAULT_UPSTREAM_MAP = ROOT / "upstream-map.toml"
PROPOSAL = ROOT / "docs/scope.md"
PLAN = ROOT / "docs/verification-plan.md"

PROPOSAL_SECTIONS = [
    "Status",
    "Intent type",
    "Why now",
    "Scope",
    "Affected modules / capabilities",
    "Stakeholders / Approver",
    "Success criteria",
    "Risks & open questions",
]
AUTHORITY_CLASSES = {
    "target-spec",
    "source-and-tests",
    "generator",
    "generated-projection",
    "compatibility",
    "host-only",
    "disposable-scaffold",
}
MARKDOWN_LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)]+)\)")
CRATE_ROW = re.compile(r"^\|\s*(\d+)\s*\|\s*`([^`]+)`\s*\|", re.MULTILINE)
PLAN_SLICE = re.compile(r"^###\s+(S\d{2}):", re.MULTILINE)
HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)


@dataclass(frozen=True)
class TreeEntry:
    path: str
    oid: str
    mode: str


@dataclass(frozen=True)
class RoutedEntry:
    watch: str
    path: str
    oid: str
    mode: str
    route: str
    surface: str
    role: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topology", type=Path, default=DEFAULT_TOPOLOGY_DOC)
    parser.add_argument("--topology-map", type=Path, default=DEFAULT_TOPOLOGY_MAP)
    parser.add_argument("--map", type=Path, default=DEFAULT_UPSTREAM_MAP)
    parser.add_argument("--print-route-digests", action="store_true")
    parser.add_argument("--skip-manifests", action="store_true")
    return parser.parse_args()


def run_git(checkout: Path, arguments: list[str], *, binary: bool = False) -> bytes | str:
    result = subprocess.run(
        ["git", "-C", str(checkout), *arguments],
        check=False,
        capture_output=True,
        text=not binary,
    )
    if result.returncode != 0:
        stderr = result.stderr.decode() if binary else result.stderr
        raise RuntimeError(stderr.strip() or f"git {' '.join(arguments)} failed")
    return result.stdout


def git_tree(checkout: Path, commit: str, root: str) -> list[TreeEntry]:
    raw = run_git(checkout, ["ls-tree", "-r", "-z", commit, "--", root], binary=True)
    assert isinstance(raw, bytes)
    entries: list[TreeEntry] = []
    for record in raw.split(b"\0"):
        if not record:
            continue
        metadata, path_bytes = record.split(b"\t", 1)
        mode, object_type, oid = metadata.decode().split(" ")
        if object_type != "blob":
            continue
        entries.append(TreeEntry(path_bytes.decode(), oid, mode))
    return entries


def git_blob(checkout: Path, commit: str, path: str) -> bytes:
    raw = run_git(checkout, ["show", f"{commit}:{path}"], binary=True)
    assert isinstance(raw, bytes)
    return raw


def compile_glob(pattern: str) -> re.Pattern[str]:
    output = ["^"]
    index = 0
    while index < len(pattern):
        character = pattern[index]
        if character == "*":
            if index + 1 < len(pattern) and pattern[index + 1] == "*":
                output.append(".*")
                index += 2
                continue
            output.append("[^/]*")
        elif character == "?":
            output.append("[^/]")
        else:
            output.append(re.escape(character))
        index += 1
    output.append("$")
    return re.compile("".join(output))


def markdown_headings(lines: list[str]) -> list[tuple[int, int, str]]:
    headings: list[tuple[int, int, str]] = []
    fence: str | None = None
    for index, line in enumerate(lines):
        stripped = line.lstrip()
        if stripped.startswith(("```", "~~~")):
            marker = stripped[:3]
            if fence is None:
                fence = marker
            elif fence == marker:
                fence = None
            continue
        if fence is not None:
            continue
        match = re.match(r"^(#{1,6})\s+(.+?)\s*$", line)
        if match is not None:
            headings.append((index, len(match.group(1)), match.group(2)))
    return headings


def selector_line_set(
    selector: dict[str, object],
    lines: list[str],
) -> set[int]:
    kind = selector.get("kind")
    if kind == "text-line":
        needle = selector.get("needle", "")
        matches = [index for index, line in enumerate(lines) if line.startswith(needle)]
        return set(matches) if len(matches) == 1 else set()
    if kind == "text-span":
        start = selector.get("start", "")
        end = selector.get("end", "")
        starts = [index for index, line in enumerate(lines) if line.startswith(start)]
        ends = (
            [len(lines)]
            if end == "$EOF"
            else [index for index, line in enumerate(lines) if line.startswith(end)]
        )
        if len(starts) != 1 or len(ends) != 1 or starts[0] >= ends[0]:
            return set()
        return set(range(starts[0], ends[0]))
    if kind != "markdown-section":
        return set()
    headings = markdown_headings(lines)
    heading = selector.get("heading")
    matches = [entry for entry in headings if entry[2] == heading]
    if len(matches) != 1:
        return set()
    start, level, _ = matches[0]
    later = [index for index, later_level, _ in headings if index > start and later_level <= level]
    end = min(later) if later else len(lines)
    selected = set(range(start, end))
    for excluded_heading in selector.get("exclude_headings", []):
        excluded_matches = [
            entry
            for entry in headings
            if entry[2] == excluded_heading and start < entry[0] < end
        ]
        if len(excluded_matches) != 1:
            continue
        excluded_start, excluded_level, _ = excluded_matches[0]
        excluded_later = [
            index
            for index, later_level, _ in headings
            if excluded_start < index < end and later_level <= excluded_level
        ]
        excluded_end = min(excluded_later) if excluded_later else end
        selected.difference_update(range(excluded_start, excluded_end))
    return selected


def route_matches(route: dict[str, object], path: str) -> bool:
    includes = [compile_glob(value) for value in route.get("include", [])]
    excludes = [compile_glob(value) for value in route.get("exclude", [])]
    return any(pattern.match(path) for pattern in includes) and not any(
        pattern.match(path) for pattern in excludes
    )


def load_toml(path: Path) -> dict[str, object]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def validate_text_format(source: Path, text: str, errors: list[str]) -> None:
    try:
        label = source.relative_to(ROOT)
    except ValueError:
        label = source
    if text and not text.endswith("\n"):
        errors.append(f"text file has no final newline: {label}")
    for line_number, line in enumerate(text.splitlines(), start=1):
        if line != line.rstrip():
            errors.append(f"trailing whitespace: {label}:{line_number}")


def validate_proposal(errors: list[str]) -> None:
    text = PROPOSAL.read_text(encoding="utf-8")
    headings = re.findall(r"^##\s+(.+?)\s*$", text, re.MULTILINE)
    positions: list[int] = []
    for section in PROPOSAL_SECTIONS:
        if section not in headings:
            errors.append(f"proposal missing required section: {section}")
            continue
        positions.append(headings.index(section))
    if len(positions) == len(PROPOSAL_SECTIONS) and positions != sorted(positions):
        errors.append("proposal required sections are out of order")
    status = re.search(r"^## Status\s+([^#]+?)\s*(?=^## )", text, re.MULTILINE | re.DOTALL)
    if status is None or status.group(1).strip() != "accepted":
        errors.append("proposal status must be exactly 'accepted'")


def link_target(source: Path, raw_target: str) -> Path | None:
    target = raw_target.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1]
    if target.startswith(("http://", "https://", "mailto:", "#")):
        return None
    target = unquote(target.split("#", 1)[0])
    if not target:
        return None
    candidate = Path(target)
    return candidate if candidate.is_absolute() else source.parent / candidate


def validate_links_and_format(errors: list[str]) -> int:
    text_files = sorted(
        path
        for path in (ROOT / name for name in subprocess.check_output(["git", "-C", str(ROOT), "ls-files", "-z", "--cached", "--others", "--exclude-standard"]).decode().split("\0") if name)
        if path.is_file()
        and ".git" not in path.parts
        and path.suffix in {".md", ".toml", ".py", ".sh"}
    )
    markdown_count = 0
    for source in text_files:
        text = source.read_text(encoding="utf-8")
        validate_text_format(source, text, errors)
        if source.suffix != ".md":
            continue
        markdown_count += 1
        for raw_target in MARKDOWN_LINK.findall(text):
            target = link_target(source, raw_target)
            if target is not None and not target.resolve().exists():
                errors.append(
                    f"broken Markdown link: {source.relative_to(ROOT)} -> {raw_target}"
                )
    return markdown_count


def validate_topology(
    topology_doc: Path,
    topology_map: Path,
    errors: list[str],
) -> set[str]:
    text = topology_doc.read_text(encoding="utf-8")
    rows = [(int(number), name) for number, name in CRATE_ROW.findall(text)]
    numbers = [number for number, _ in rows]
    documented = [name for _, name in rows]
    if numbers != list(range(1, 16)):
        errors.append(f"crate inventory numbers must be 1..15, got {numbers}")
    if len(documented) != 15 or len(set(documented)) != 15:
        errors.append(f"production crate inventory must contain 15 unique rows, got {documented}")

    data = load_toml(topology_map)
    if data.get("schema_version") != 1:
        errors.append("workspace topology schema_version must be 1")
    crates = data.get("production_crates", [])
    if crates != documented:
        errors.append("workspace-topology production_crates must equal the documented order")
    crate_set = set(crates)

    feature_groups = data.get("feature_group", [])
    feature_group_ids: set[str] = set()
    for group in feature_groups:
        group_id = group.get("id")
        if not group_id or group_id in feature_group_ids:
            errors.append(f"duplicate or missing feature group: {group_id}")
            continue
        feature_group_ids.add(group_id)
        if not group.get("members"):
            errors.append(f"feature group has no members: {group_id}")

    edges = data.get("edge", [])
    seen_edges: set[tuple[str, str]] = set()
    graph: dict[str, set[str]] = {crate: set() for crate in crates}
    indegree = {crate: 0 for crate in crates}
    for edge in edges:
        consumer = edge.get("consumer")
        dependency = edge.get("dependency")
        kind = edge.get("kind")
        if consumer not in crate_set or dependency not in crate_set:
            errors.append(f"topology edge has unknown crate: {consumer} -> {dependency}")
            continue
        if consumer == dependency:
            errors.append(f"topology self-edge: {consumer}")
            continue
        identity = (consumer, dependency)
        if identity in seen_edges:
            errors.append(f"duplicate topology edge: {consumer} -> {dependency}")
            continue
        seen_edges.add(identity)
        if kind not in {"required", "optional"}:
            errors.append(f"invalid edge kind for {consumer} -> {dependency}: {kind}")
        if kind == "optional":
            selectors = [bool(edge.get("feature")), bool(edge.get("feature_group"))]
            if sum(selectors) != 1:
                errors.append(
                    f"optional edge needs exactly one feature or feature_group: "
                    f"{consumer} -> {dependency}"
                )
            if edge.get("feature_group") and edge.get("feature_group") not in feature_group_ids:
                errors.append(
                    f"optional edge has unknown feature_group: "
                    f"{consumer} -> {dependency}"
                )
        graph[dependency].add(consumer)
        indegree[consumer] += 1

    dev_edges = data.get("dev_edge", [])
    seen_dev_edges: set[tuple[str, str]] = set()
    for edge in dev_edges:
        consumer = edge.get("consumer")
        dependency = edge.get("dependency")
        identity = (consumer, dependency)
        if consumer not in crate_set or dependency not in crate_set or consumer == dependency:
            errors.append(f"invalid dev-only topology edge: {consumer} -> {dependency}")
            continue
        if identity in seen_edges or identity in seen_dev_edges:
            errors.append(f"duplicate topology edge: {consumer} -> {dependency}")
            continue
        if set(edge) != {"consumer", "dependency"}:
            errors.append(f"dev-only topology edge has extra fields: {consumer} -> {dependency}")
        seen_dev_edges.add(identity)
        graph[dependency].add(consumer)
        indegree[consumer] += 1
    if seen_dev_edges != APPROVED_DEV_EDGES:
        errors.append(
            "dev-only edge inventory must be exactly "
            "pi-harness-runtime -> pi-agent-sqlite"
        )

    ready = deque(sorted(crate for crate, count in indegree.items() if count == 0))
    visited: list[str] = []
    while ready:
        current = ready.popleft()
        visited.append(current)
        for consumer in sorted(graph[current]):
            indegree[consumer] -= 1
            if indegree[consumer] == 0:
                ready.append(consumer)
    if len(visited) != len(crates):
        errors.append("workspace topology contains a required/all-feature cycle")

    dependency_section = text.split("## Dependency graph", 1)
    if len(dependency_section) != 2:
        errors.append("topology document lacks dependency graph section")
        return crate_set
    dependency_table = dependency_section[1].split("## Literal schema", 1)[0]
    documented_edges: set[tuple[str, str, str]] = set()
    for line in dependency_table.splitlines():
        match = re.match(r"^\|\s*`([^`]+)`\s*\|(.*)\|(.*)\|$", line)
        if match is None or match.group(1) not in crate_set:
            continue
        consumer = match.group(1)
        for dependency in re.findall(r"`([^`]+)`", match.group(2)):
            if dependency in crate_set:
                documented_edges.add((consumer, dependency, "required"))
        for dependency in re.findall(r"`([^`]+)`", match.group(3)):
            if dependency in crate_set:
                documented_edges.add((consumer, dependency, "optional"))
    machine_edges = {
        (edge["consumer"], edge["dependency"], edge["kind"])
        for edge in edges
        if edge.get("consumer") in crate_set and edge.get("dependency") in crate_set
    }
    if documented_edges != machine_edges:
        missing = sorted(machine_edges - documented_edges)
        extra = sorted(documented_edges - machine_edges)
        errors.append(
            f"documented/machine dependency edges differ: missing={missing}, extra={extra}"
        )
    documented_dev_edges = set(
        re.findall(
            r"^- `([^`]+)` → `([^`]+)` \(dev-only\)$",
            dependency_table,
            re.MULTILINE,
        )
    )
    if documented_dev_edges != seen_dev_edges:
        errors.append(
            "documented/machine dev-only edges differ: "
            f"missing={sorted(seen_dev_edges - documented_dev_edges)}, "
            f"extra={sorted(documented_dev_edges - seen_dev_edges)}"
        )
    return crate_set


def validate_plan(errors: list[str]) -> None:
    text = PLAN.read_text(encoding="utf-8")
    slices = PLAN_SLICE.findall(text)
    expected = [f"S{number:02d}" for number in range(1, 17)]
    if slices != expected:
        errors.append(f"plan slices must be exactly S01..S16, got {slices}")
    revision = re.search(r"^revision:\s*(\d+)\s*$", text, re.MULTILINE)
    if revision is None or int(revision.group(1)) < 5:
        errors.append("plan revision must include the corrected Harness build order")


def validate_upstream_map(
    map_path: Path,
    crates: set[str],
    errors: list[str],
    *,
    permit_pending_digest: bool,
) -> tuple[dict[str, str], list[RoutedEntry], dict[str, object]]:
    data = load_toml(map_path)
    if data.get("schema_version") != 2:
        errors.append("upstream map schema_version must be 2")

    upstream = data.get("upstream", {})
    try:
        checkout = resolve_upstream(upstream.get("checkout"))
    except ValueError as error:
        errors.append(str(error))
        return {}, [], data
    commit = upstream.get("commit", "")
    if not checkout.is_dir():
        errors.append(f"upstream object-store checkout does not exist: {checkout}")
        return {}, [], data
    try:
        resolved = run_git(checkout, ["rev-parse", f"{commit}^{{commit}}"])
    except RuntimeError as error:
        errors.append(f"cannot resolve pinned upstream commit: {error}")
        return {}, [], data
    assert isinstance(resolved, str)
    if resolved.strip() != commit:
        errors.append(f"upstream commit must be full and canonical: {commit}")

    policy = data.get("policy", {})
    if set(policy.get("authority_classes", [])) != AUTHORITY_CLASSES:
        errors.append("upstream authority_classes must equal the accepted closed taxonomy")
    allowed_dispositions = set(policy.get("dispositions", []))
    allowed_statuses = set(policy.get("statuses", []))

    owners = data.get("owner", [])
    owner_by_id: dict[str, dict[str, object]] = {}
    for owner in owners:
        owner_id = owner.get("id")
        if not owner_id or owner_id in owner_by_id:
            errors.append(f"duplicate or missing owner id: {owner_id}")
            continue
        owner_by_id[owner_id] = owner
        kind = owner.get("kind")
        if kind == "rust":
            if owner.get("crate") not in crates:
                errors.append(f"Rust owner {owner_id} has unknown crate: {owner.get('crate')}")
        elif kind == "outside-scope":
            if not owner.get("decision_ref"):
                errors.append(f"outside-scope owner lacks decision_ref: {owner_id}")
        else:
            errors.append(f"owner {owner_id} has invalid kind: {kind}")

    divergences = data.get("divergence", [])
    divergence_ids: set[str] = set()
    divergence_fields = {
        "id",
        "upstream_behavior",
        "rust_behavior",
        "owner",
        "rationale",
        "compatibility_consequence",
        "replacement_fixture",
        "revisit_trigger",
    }
    for divergence in divergences:
        divergence_id = divergence.get("id")
        if not divergence_id or divergence_id in divergence_ids:
            errors.append(f"duplicate or missing divergence id: {divergence_id}")
        divergence_ids.add(divergence_id)
        missing = divergence_fields - divergence.keys()
        if missing:
            errors.append(f"divergence {divergence_id} missing fields: {sorted(missing)}")
        if divergence.get("owner") not in owner_by_id:
            errors.append(f"divergence {divergence_id} has unknown owner")

    surfaces = data.get("surface", [])
    surface_by_id: dict[str, dict[str, object]] = {}
    required_surface_fields = {
        "id",
        "authority",
        "primary_owner",
        "collaborators",
        "disposition",
        "status",
        "sync",
        "conformance",
        "divergences",
        "lifecycle",
    }
    for surface in surfaces:
        surface_id = surface.get("id")
        if not surface_id or surface_id in surface_by_id:
            errors.append(f"duplicate or missing surface id: {surface_id}")
            continue
        surface_by_id[surface_id] = surface
        missing = required_surface_fields - surface.keys()
        if missing:
            errors.append(f"surface {surface_id} missing fields: {sorted(missing)}")
        if surface.get("authority") not in AUTHORITY_CLASSES:
            errors.append(f"surface {surface_id} has invalid authority: {surface.get('authority')}")
        if surface.get("disposition") not in allowed_dispositions:
            errors.append(f"surface {surface_id} has invalid disposition")
        if surface.get("status") not in allowed_statuses:
            errors.append(f"surface {surface_id} has invalid status")
        primary_owner = owner_by_id.get(surface.get("primary_owner"))
        if primary_owner is None:
            errors.append(f"surface {surface_id} has unknown primary owner")
        elif surface.get("disposition") == "outside-scope" and primary_owner.get("kind") != "outside-scope":
            errors.append(f"outside-scope surface {surface_id} must have outside-scope owner")
        elif surface.get("disposition") != "outside-scope" and primary_owner.get("kind") != "rust":
            errors.append(f"in-scope/disposable surface {surface_id} must have Rust owner")
        for collaborator in surface.get("collaborators", []):
            if collaborator not in owner_by_id:
                errors.append(f"surface {surface_id} has unknown collaborator: {collaborator}")
        for divergence_id in surface.get("divergences", []):
            if divergence_id not in divergence_ids:
                errors.append(f"surface {surface_id} has unknown divergence: {divergence_id}")
        lifecycle = surface.get("lifecycle", {})
        lifecycle_required = {"introduced_at", "removal_condition", "reclassification_condition"}
        if not lifecycle_required.issubset(lifecycle):
            errors.append(f"surface {surface_id} has incomplete lifecycle")

    for surface_id, surface in surface_by_id.items():
        generated_by = surface.get("generated_by")
        if generated_by and generated_by not in surface_by_id:
            errors.append(f"surface {surface_id} has unknown generator surface: {generated_by}")
        if surface.get("authority") == "generated-projection":
            if not generated_by:
                errors.append(f"generated projection {surface_id} lacks generated_by")
            if surface.get("provenance_status") not in {
                "complete",
                "missing-intake-snapshot",
            }:
                errors.append(
                    f"generated projection {surface_id} has invalid/missing provenance_status"
                )
            if surface.get("provenance_status") == "complete":
                snapshot = surface.get("provenance_snapshot", {})
                if not {"id", "sha256", "source_ids"}.issubset(snapshot):
                    errors.append(
                        f"complete generated projection {surface_id} lacks pinned provenance_snapshot"
                    )

    watches = data.get("watch", [])
    watch_by_id: dict[str, dict[str, object]] = {}
    for watch in watches:
        watch_id = watch.get("id")
        if not watch_id or watch_id in watch_by_id:
            errors.append(f"duplicate or missing watch id: {watch_id}")
            continue
        watch_by_id[watch_id] = watch
        if watch.get("coverage") != "complete" or watch.get("unmatched") != "error":
            errors.append(f"watch {watch_id} must require complete/error coverage")

    routes = data.get("route", [])
    route_ids: set[str] = set()
    routes_by_watch: dict[str, list[dict[str, object]]] = defaultdict(list)
    for route in routes:
        route_id = route.get("id")
        if not route_id or route_id in route_ids:
            errors.append(f"duplicate or missing route id: {route_id}")
            continue
        route_ids.add(route_id)
        watch_id = route.get("watch")
        if watch_id not in watch_by_id:
            errors.append(f"route {route_id} has unknown watch: {watch_id}")
        if route.get("surface") not in surface_by_id:
            errors.append(f"route {route_id} has unknown surface: {route.get('surface')}")
        if not route.get("include"):
            errors.append(f"route {route_id} has no include patterns")
        routes_by_watch[watch_id].append(route)

    route_digests: dict[str, str] = {}
    routed_entries: list[RoutedEntry] = []
    route_match_count: dict[str, int] = defaultdict(int)
    for watch_id, watch in watch_by_id.items():
        try:
            entries = git_tree(checkout, commit, watch.get("root", ""))
        except RuntimeError as error:
            errors.append(f"cannot inventory watch {watch_id}: {error}")
            continue
        if len(entries) != watch.get("baseline_path_count"):
            errors.append(
                f"watch {watch_id} path count mismatch: expected "
                f"{watch.get('baseline_path_count')}, got {len(entries)}"
            )
        digest_rows: list[bytes] = []
        for entry in entries:
            matches = [
                route
                for route in routes_by_watch.get(watch_id, [])
                if route_matches(route, entry.path)
            ]
            if len(matches) != 1:
                errors.append(
                    f"watch {watch_id} path must match exactly one route: "
                    f"{entry.path} -> {[route.get('id') for route in matches]}"
                )
                continue
            route = matches[0]
            route_id = route["id"]
            route_match_count[route_id] += 1
            routed = RoutedEntry(
                watch_id,
                entry.path,
                entry.oid,
                entry.mode,
                route_id,
                route["surface"],
                route["role"],
            )
            routed_entries.append(routed)
            digest_rows.append(
                (
                    f"{entry.path}\0{entry.oid}\0{route_id}\0"
                    f"{route['surface']}\0{route['role']}\n"
                ).encode()
            )
        digest = "sha256:" + hashlib.sha256(b"".join(sorted(digest_rows))).hexdigest()
        route_digests[watch_id] = digest
        expected_digest = watch.get("route_digest")
        if permit_pending_digest:
            pass
        elif expected_digest != digest:
            errors.append(
                f"watch {watch_id} route_digest mismatch: expected {expected_digest}, got {digest}"
            )

    for route_id in route_ids:
        if route_match_count[route_id] == 0:
            errors.append(f"route matches no pinned path: {route_id}")

    selector_ids: set[str] = set()
    lines_by_path: dict[str, list[str]] = {}
    selected_by_partition: dict[tuple[str, str], list[tuple[str, set[int]]]] = defaultdict(list)
    partitions = {route.get("selector_partition") for route in routes if route.get("selector_partition")}
    selector_partitions: set[str] = set()
    for selector in data.get("selector", []):
        selector_id = selector.get("id")
        if not selector_id or selector_id in selector_ids:
            errors.append(f"duplicate or missing selector id: {selector_id}")
            continue
        selector_ids.add(selector_id)
        selector_partitions.add(selector.get("partition"))
        if selector.get("surface") not in surface_by_id:
            errors.append(f"selector {selector_id} has unknown surface")
        path = selector.get("path", "")
        try:
            text = git_blob(checkout, commit, path).decode("utf-8")
        except (RuntimeError, UnicodeDecodeError) as error:
            errors.append(f"selector {selector_id} cannot read pinned source: {error}")
            continue
        lines = lines_by_path.setdefault(path, text.splitlines())
        if selector.get("kind") not in {"markdown-section", "text-line", "text-span"}:
            errors.append(f"selector {selector_id} has unsupported kind: {selector.get('kind')}")
            continue
        selected = selector_line_set(selector, lines)
        if not selected:
            errors.append(f"selector {selector_id} resolves to no lines")
            continue
        selected_by_partition[(selector.get("partition"), path)].append(
            (selector_id, selected)
        )
    if partitions != selector_partitions:
        errors.append(
            f"selector partitions mismatch: routes={sorted(partitions)} "
            f"selectors={sorted(selector_partitions)}"
        )
    for (partition, path), selections in selected_by_partition.items():
        lines = lines_by_path[path]
        owners_by_line: dict[int, list[str]] = defaultdict(list)
        for selector_id, selected in selections:
            for line_number in selected:
                owners_by_line[line_number].append(selector_id)
        uncovered = [
            index + 1
            for index, line in enumerate(lines)
            if line.strip() and not owners_by_line.get(index)
        ]
        overlaps = [
            index + 1
            for index, owners_for_line in owners_by_line.items()
            if len(owners_for_line) > 1 and lines[index].strip()
        ]
        if uncovered:
            errors.append(
                f"selector partition {partition} has uncovered nonblank lines in {path}: "
                f"{uncovered[:12]}{'...' if len(uncovered) > 12 else ''}"
            )
        if overlaps:
            errors.append(
                f"selector partition {partition} has overlapping nonblank lines in {path}: "
                f"{overlaps[:12]}{'...' if len(overlaps) > 12 else ''}"
            )

    return route_digests, routed_entries, data


def validate_catalog_provenance(
    map_data: dict[str, object],
    catalog_inputs: dict[str, object],
    errors: list[str],
) -> None:
    catalog_surface = next(
        (
            surface
            for surface in map_data.get("surface", [])
            if surface.get("id") == "ai.catalog.projection"
        ),
        {},
    )
    map_status = catalog_surface.get("provenance_status")
    manifest_status = catalog_inputs.get("provenance_status")
    if manifest_status != map_status:
        errors.append("catalog map and input-manifest provenance states must agree")
        return
    if map_status != "complete":
        return

    snapshot = catalog_surface.get("provenance_snapshot", {})
    map_id = snapshot.get("id")
    map_sha256 = snapshot.get("sha256")
    map_sources = snapshot.get("source_ids")
    manifest_id = catalog_inputs.get("snapshot_id")
    manifest_sha256 = catalog_inputs.get("snapshot_sha256")
    manifest_sources = catalog_inputs.get("source_ids")
    digest_pattern = re.compile(r"^sha256:[0-9a-f]{64}$")
    values_valid = (
        isinstance(map_id, str)
        and bool(map_id)
        and isinstance(manifest_id, str)
        and bool(manifest_id)
        and isinstance(map_sha256, str)
        and bool(digest_pattern.fullmatch(map_sha256))
        and isinstance(manifest_sha256, str)
        and bool(digest_pattern.fullmatch(manifest_sha256))
        and isinstance(map_sources, list)
        and bool(map_sources)
        and all(isinstance(value, str) and value for value in map_sources)
        and len(map_sources) == len(set(map_sources))
        and isinstance(manifest_sources, list)
        and bool(manifest_sources)
        and all(isinstance(value, str) and value for value in manifest_sources)
        and len(manifest_sources) == len(set(manifest_sources))
    )
    if not values_valid:
        errors.append("complete catalog provenance has empty or invalid snapshot identity")
        return
    if (
        map_id != manifest_id
        or map_sha256 != manifest_sha256
        or set(map_sources) != set(manifest_sources)
    ):
        errors.append("complete catalog map/manifest snapshot identities must match exactly")


def validate_manifests(map_data: dict[str, object], errors: list[str]) -> None:
    manifests = map_data.get("manifest", [])
    manifest_ids: set[str] = set()
    for manifest in manifests:
        manifest_id = manifest.get("id")
        if not manifest_id or manifest_id in manifest_ids:
            errors.append(f"duplicate or missing manifest id: {manifest_id}")
            continue
        manifest_ids.add(manifest_id)
        path = ROOT / manifest.get("path", "")
        if not path.is_file():
            errors.append(f"generated upstream manifest missing: {path.relative_to(ROOT)}")
    if errors:
        return
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts/generate_upstream_manifests.py"), "--check"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        errors.append(
            "upstream manifests are stale: "
            + (result.stdout + result.stderr).strip()
        )
        return

    routes = load_toml(ROOT / "upstream/manifests/routes.toml").get("entry", [])
    route_keys = [(entry.get("watch"), entry.get("path")) for entry in routes]
    expected_route_count = sum(
        watch.get("baseline_path_count", 0) for watch in map_data.get("watch", [])
    )
    if len(routes) != expected_route_count or len(set(route_keys)) != len(route_keys):
        errors.append("route manifest must contain one unique entry per watched pinned path")

    for name in ("ai", "agent"):
        exports = load_toml(ROOT / f"upstream/manifests/{name}-exports.toml")
        symbols = exports.get("symbol", [])
        identities = [(item.get("subpath"), item.get("name")) for item in symbols]
        if not symbols or len(identities) != len(set(identities)):
            errors.append(f"{name} export manifest lacks a unique symbol inventory")
        invalid_kinds = {
            item.get("kind")
            for item in symbols
            if item.get("kind") not in {"type", "value", "type-and-value"}
        }
        if invalid_kinds:
            errors.append(f"{name} export manifest has invalid symbol kinds: {invalid_kinds}")

    providers = load_toml(ROOT / "upstream/manifests/providers.toml").get("provider", [])
    required_provider_fields = {
        "module",
        "factory_symbol",
        "kind",
        "dynamic",
        "provider_ids",
        "api_ids",
        "has_oauth",
        "oauth_flows",
        "catalog_symbol",
    }
    if not any(provider.get("factory_symbol") == "fauxProvider" for provider in providers):
        errors.append("provider manifest must include public fauxProvider")
    for provider in providers:
        missing = required_provider_fields - provider.keys()
        if missing:
            errors.append(
                f"provider {provider.get('factory_symbol')} lacks fields: {sorted(missing)}"
            )

    tests = load_toml(ROOT / "upstream/manifests/tests.toml").get("test", [])
    tests_by_path = {test.get("path"): test for test in tests}
    if any(not test.get("evidence_surfaces") for test in tests):
        errors.append("every test manifest entry must name at least one evidence surface")
    expected_test_routing = {
        "packages/ai/test/provider-retry.test.ts": {"ai.transport-utils"},
        "packages/ai/test/retry.test.ts": {"ai.domain", "ai.retry-compat"},
        "packages/coding-agent/test/model-registry.test.ts": {"coding.models"},
        "packages/coding-agent/test/interactive-tui.test.ts": {"coding.package-host"},
    }
    for path, surfaces in expected_test_routing.items():
        actual = set(tests_by_path.get(path, {}).get("evidence_surfaces", []))
        if not surfaces.issubset(actual):
            errors.append(
                f"test manifest misroutes representative test: {path} -> {sorted(surfaces)}"
            )

    catalog_inputs = load_toml(ROOT / "upstream/manifests/catalog-inputs.toml")
    validate_catalog_provenance(map_data, catalog_inputs, errors)
    input_paths = {item.get("path") for item in catalog_inputs.get("input", [])}
    if "packages/ai/src/api/cloudflare.ts" not in input_paths:
        errors.append("catalog input manifest must include imported cloudflare constants")


def main() -> int:
    args = parse_args()
    errors: list[str] = []
    required = [PROPOSAL, PLAN, args.topology, args.topology_map, args.map]
    for path in required:
        if not path.resolve().is_file():
            errors.append(f"required file missing: {path}")
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1

    validate_proposal(errors)
    markdown_count = validate_links_and_format(errors)
    crates = validate_topology(args.topology.resolve(), args.topology_map.resolve(), errors)
    validate_plan(errors)
    route_digests, routed_entries, map_data = validate_upstream_map(
        args.map.resolve(),
        crates,
        errors,
        permit_pending_digest=args.print_route_digests,
    )
    if not args.skip_manifests and not args.print_route_digests:
        validate_manifests(map_data, errors)

    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1
    if args.print_route_digests:
        for watch_id, digest in sorted(route_digests.items()):
            print(f"{watch_id} {digest}")
        return 0
    print(
        "docs validation: ok "
        f"({markdown_count} Markdown files, {len(crates)} production crates, "
        f"{len(map_data.get('surface', []))} surfaces, "
        f"{len(routed_entries)} pinned paths)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
