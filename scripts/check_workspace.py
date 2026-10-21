#!/usr/bin/env python3
"""Check the Cargo workspace against the accepted machine topology."""

from __future__ import annotations

import fnmatch
import json
import os
import subprocess
import sys
import tomllib
from collections import defaultdict, deque
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TOPOLOGY_PATH = ROOT / "workspace-topology.toml"
APPROVED_DEV_EDGES: set[tuple[str, str]] = set()


def load_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as file:
        return tomllib.load(file)


def cargo_metadata(*arguments: str) -> dict[str, Any]:
    cargo = os.environ.get("CARGO", "cargo")
    result = subprocess.run(
        [
            cargo,
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--offline",
            *arguments,
        ],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"cargo metadata failed: {detail}")
    return json.loads(result.stdout)


def internal_edges(
    metadata: dict[str, Any], crate_names: set[str]
) -> set[tuple[str, str]]:
    packages_by_id = {package["id"]: package for package in metadata["packages"]}
    edges: set[tuple[str, str]] = set()
    for node in metadata["resolve"]["nodes"]:
        consumer = packages_by_id[node["id"]]["name"]
        if consumer not in crate_names:
            continue
        for dependency in node["deps"]:
            dependency_name = packages_by_id[dependency["pkg"]]["name"]
            if dependency_name in crate_names:
                edges.add((consumer, dependency_name))
    return edges


def cycle_in(edges: set[tuple[str, str]], crates: list[str]) -> list[str] | None:
    consumers: dict[str, set[str]] = defaultdict(set)
    indegree = dict.fromkeys(crates, 0)
    for consumer, dependency in edges:
        consumers[dependency].add(consumer)
        indegree[consumer] += 1
    ready = deque(sorted(crate for crate, degree in indegree.items() if degree == 0))
    visited: list[str] = []
    while ready:
        dependency = ready.popleft()
        visited.append(dependency)
        for consumer in sorted(consumers[dependency]):
            indegree[consumer] -= 1
            if indegree[consumer] == 0:
                ready.append(consumer)
    if len(visited) == len(crates):
        return None
    return sorted(crate for crate, degree in indegree.items() if degree > 0)


def formatted_edges(edges: set[tuple[str, str]]) -> str:
    return ", ".join(f"{consumer} -> {dependency}" for consumer, dependency in sorted(edges))


def validate_manifest_inheritance(root: Path, crates: list[str], errors: list[str]) -> None:
    inherited_fields = {"version", "edition", "rust-version", "license", "publish"}
    for crate in crates:
        manifest_path = root / "crates" / crate / "Cargo.toml"
        if not manifest_path.is_file():
            errors.append(f"missing member manifest: {manifest_path.relative_to(root)}")
            continue
        manifest = load_toml(manifest_path)
        package = manifest.get("package", {})
        if package.get("name") != crate:
            errors.append(f"member manifest name does not match directory: {crate}")
        for field in sorted(inherited_fields):
            if package.get(field) != {"workspace": True}:
                errors.append(f"{crate} must inherit package.{field}")
        if manifest.get("lints") != {"workspace": True}:
            errors.append(f"{crate} must inherit workspace lints")


def feature_activates_dependency(
    features: dict[str, list[str]], feature: str, dependency: str
) -> bool:
    pending = [feature]
    visited: set[str] = set()
    while pending:
        current = pending.pop()
        if current in visited:
            continue
        visited.add(current)
        for activation in features.get(current, []):
            if activation == f"dep:{dependency}":
                return True
            dependency_feature, separator, _ = activation.partition("/")
            if separator and dependency_feature == dependency:
                return True
            if activation in features:
                pending.append(activation)
    return False


def validate_local_packages(
    metadata: dict[str, Any], crate_names: set[str], root: Path,
    profile: str, errors: list[str],
) -> list[dict[str, Any]]:
    packages_by_id = {package["id"]: package for package in metadata["packages"]}
    workspace_packages = [packages_by_id[value] for value in metadata["workspace_members"]]
    names = {package["name"] for package in workspace_packages}
    if names != crate_names or len(workspace_packages) != len(crate_names):
        errors.append(f"{profile} Cargo workspace package inventory mismatch")
    for package in workspace_packages:
        name = package["name"]
        expected = (root / "crates" / name / "Cargo.toml").resolve()
        if package.get("source") is not None or Path(package["manifest_path"]).resolve() != expected:
            errors.append(f"{profile} {name} must resolve to canonical local manifest")
    workspace_ids = set(metadata["workspace_members"])
    hidden = [package["name"] for package in metadata["packages"]
              if package.get("source") is None and package["id"] not in workspace_ids]
    if hidden:
        errors.append(f"{profile} metadata has reachable local path package outside workspace: "
                      + ", ".join(sorted(hidden)))
    return workspace_packages


def validate_dependency_declarations(
    root: Path,
    workspace_packages: list[dict[str, Any]],
    crate_names: set[str],
    expected_edges: dict[tuple[str, str], dict[str, Any]],
    expected_dev_edges: set[tuple[str, str]],
    errors: list[str],
) -> None:
    expected_paths = {
        crate: (root / "crates" / crate).resolve() for crate in crate_names
    }
    paths_to_crates = {path: crate for crate, path in expected_paths.items()}
    declarations: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for package in workspace_packages:
        consumer = package["name"]
        for dependency_record in package["dependencies"]:
            dependency = dependency_record["name"]
            dependency_path = dependency_record.get("path")
            path_crate = (
                paths_to_crates.get(Path(dependency_path).resolve())
                if dependency_path is not None
                else None
            )
            if dependency not in crate_names and path_crate is None:
                continue
            canonical_dependency = dependency if dependency in crate_names else path_crate
            assert canonical_dependency is not None
            identity = (consumer, canonical_dependency)
            declarations[identity].append(dependency_record)
            if dependency != canonical_dependency:
                errors.append(
                    f"{consumer} -> {canonical_dependency} must use exact canonical package name"
                )

    declared_edges = set(declarations)
    expected_identities = set(expected_edges) | expected_dev_edges
    missing_edges = expected_identities - declared_edges
    extra_edges = declared_edges - expected_identities
    if missing_edges:
        errors.append(f"missing internal edges: {formatted_edges(missing_edges)}")
    if extra_edges:
        errors.append(f"unlisted internal edges: {formatted_edges(extra_edges)}")

    for identity in sorted(expected_identities & declared_edges):
        consumer, dependency = identity
        records = declarations[identity]
        if len(records) != 1:
            errors.append(
                f"internal edge must have exactly one declaration: {consumer} -> {dependency} "
                f"has {len(records)}"
            )
        dev_only = identity in expected_dev_edges
        expected_optional = (
            not dev_only and expected_edges[identity].get("kind") == "optional"
        )
        for record in records:
            if record.get("kind") != ("dev" if dev_only else None):
                expected_kind = "dev" if dev_only else "normal"
                errors.append(
                    f"{consumer} -> {dependency} kind must be {expected_kind}"
                )
            if record.get("target") is not None:
                errors.append(f"{consumer} -> {dependency} must be untargeted")
            if record.get("rename") is not None:
                errors.append(f"{consumer} -> {dependency} must be unrenamed")
            dependency_path = record.get("path")
            if dependency_path is None or Path(dependency_path).resolve() != expected_paths[dependency]:
                errors.append(
                    f"{consumer} -> {dependency} must resolve to canonical local path "
                    f"{expected_paths[dependency]}"
                )
            if record.get("optional") is not expected_optional:
                errors.append(
                    f"internal edge optionality mismatch: {consumer} -> {dependency} "
                    f"expected optional={expected_optional}"
                )


PROVIDERS = ("anthropic", "deepseek", "google", "openai")
OAUTH = ("anthropic", "openai-chatgpt")
FACTORY_FEATURES = {
    "provider-anthropic": ["wire-anthropic-messages", "catalog-anthropic"],
    "provider-deepseek": ["wire-openai-completions", "catalog-deepseek"],
    "provider-google": ["wire-google-genai", "catalog-google"],
    "provider-openai": ["wire-openai-responses", "catalog-openai"],
    "all-providers": ["provider-" + name for name in PROVIDERS],
    "all-oauth": ["oauth-" + name for name in OAUTH],
}
FACADE_FEATURES = {
    **{"catalog-" + name: ["dep:pi-ai-providers", "pi-ai-providers/catalog-" + name] for name in PROVIDERS},
    **{"provider-" + name: ["catalog-" + name, "pi-ai-providers/provider-" + name] for name in PROVIDERS},
    **{"oauth-" + name: ["dep:pi-ai-providers", "pi-ai-providers/oauth-" + name] for name in OAUTH},
    **{name: values for name, values in FACTORY_FEATURES.items() if name.startswith("all-")},
    "native": ["dep:pi-ai-providers", "pi-ai-providers/native"],
    "auth-native": ["native", "pi-ai-providers/auth-native"],
}


def validate_feature_inventory(package: dict[str, Any], expected: dict, errors: list[str]) -> None:
    features = package["features"]
    if set(features) != set(expected):
        errors.append(f"{package['name']} selected feature inventory changed")
    for name, values in expected.items():
        actual = features.get(name)
        if actual is None or set(actual) != set(values) or len(actual) != len(values):
            errors.append(f"{package['name']} feature ownership changed: {name}")


def validate_provider_features(package: dict[str, Any], errors: list[str]) -> None:
    expected = {
        "default": [], "native": ["pi-ai-transport/native"],
        "auth-native": ["native", "pi-ai-transport/auth-native"],
        **FACTORY_FEATURES,
        **{"catalog-" + name: [] for name in PROVIDERS},
        **{"oauth-" + name: ["dep:url"] for name in OAUTH},
        **{"wire-" + name: ["dep:url"] for name in
           ("anthropic-messages", "openai-responses", "openai-completions", "google-genai")},
    }
    validate_feature_inventory(package, expected, errors)
    for dependency in package["dependencies"]:
        if dependency["name"] == "url" and not dependency.get("optional"):
            errors.append("provider URL dependency must stay optional")


def validate_facade_features(package: dict[str, Any], errors: list[str]) -> None:
    # Tokio's implicit optional feature is unrelated to provider selection.
    validate_feature_inventory(package, {"default": [], "dev-catalog-tools": [],
        "tokio": ["dep:tokio"], **FACADE_FEATURES}, errors)
    binaries = [target for target in package["targets"] if "bin" in target["kind"]]
    if len(binaries) != 1 or binaries[0]["name"] != "catalog_gen" or binaries[0].get("required-features") != ["dev-catalog-tools"]:
        errors.append("AI facade binary must remain the gated catalog developer tool")


def validate_workspace(
    root: Path,
    topology: dict[str, Any],
    default_metadata: dict[str, Any],
    all_feature_metadata: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    crates = topology.get("production_crates", [])
    if not crates or len(set(crates)) != len(crates):
        errors.append("workspace-topology.toml must name unique production crates")
        return errors
    crate_names = set(crates)
    if topology.get("schema_version") != 1:
        errors.append("unsupported topology schema version")
    if "test_only_package" in topology:
        errors.append("no separate local test-only package is authorized")

    root_manifest = load_toml(root / "Cargo.toml")
    workspace = root_manifest.get("workspace", {})
    expected_members = [f"crates/{crate}" for crate in crates]
    if workspace.get("members") != expected_members:
        errors.append("Cargo.toml workspace members do not match topology order")
    if workspace.get("resolver") != "3":
        errors.append("Cargo.toml must use resolver 3")
    package_defaults = workspace.get("package", {})
    if package_defaults.get("edition") != "2024":
        errors.append("workspace package edition must be 2024")
    if package_defaults.get("rust-version") != "1.98.0":
        errors.append("workspace package rust-version must be 1.98.0")
    if package_defaults.get("publish") is not False:
        errors.append("workspace packages must be private")
    unsafe_lint = workspace.get("lints", {}).get("rust", {}).get("unsafe_code")
    if unsafe_lint != "forbid":
        errors.append("workspace rust lint unsafe_code must be forbid")

    toolchain = load_toml(root / "rust-toolchain.toml").get("toolchain", {})
    if toolchain.get("channel") != "1.98.0":
        errors.append("rust-toolchain.toml must pin Rust 1.98.0")
    if toolchain.get("profile") != "minimal":
        errors.append("rust-toolchain.toml must use the minimal profile")
    if set(toolchain.get("components", [])) != {"rustfmt", "clippy"}:
        errors.append("rust-toolchain.toml must install rustfmt and clippy")

    manifest_crates = {
        path.parent.name
        for path in (root / "crates").glob("*/Cargo.toml")
        if path.is_file()
    }
    if manifest_crates != crate_names:
        errors.append(
            "crate manifest inventory mismatch: "
            f"expected [{', '.join(sorted(crate_names))}], "
            f"actual [{', '.join(sorted(manifest_crates))}]"
        )
    validate_manifest_inheritance(root, crates, errors)

    expected_edges: dict[tuple[str, str], dict[str, Any]] = {}
    for edge in topology.get("edge", []):
        identity = (edge.get("consumer"), edge.get("dependency"))
        if identity in expected_edges:
            errors.append(f"duplicate topology edge: {identity[0]} -> {identity[1]}")
        if identity[0] not in crate_names or identity[1] not in crate_names or identity[0] == identity[1]:
            errors.append(f"invalid topology edge: {identity}")
            continue
        if edge.get("kind") not in {"required", "optional"}:
            errors.append(f"invalid edge kind: {identity}")
        if edge.get("kind") == "optional" and bool(edge.get("feature")) == bool(edge.get("feature_group")):
            errors.append(f"optional edge needs exactly one feature selector: {identity}")
        if edge.get("kind") == "required" and ("feature" in edge or "feature_group" in edge):
            errors.append(f"required edge must not have feature selector: {identity}")
        if "workspace_default" in edge and (
            edge.get("kind") != "optional" or not isinstance(edge["workspace_default"], bool)
        ):
            errors.append(f"workspace_default must be a boolean on an optional edge: {identity}")
        expected_edges[identity] = edge
    expected_dev_edges: set[tuple[str, str]] = set()
    for edge in topology.get("dev_edge", []):
        identity = (edge.get("consumer"), edge.get("dependency"))
        if (
            identity[0] not in crate_names
            or identity[1] not in crate_names
            or identity[0] == identity[1]
            or identity in expected_edges
            or identity in expected_dev_edges
        ):
            errors.append(f"invalid or duplicate dev edge: {identity[0]} -> {identity[1]}")
            continue
        expected_dev_edges.add(identity)
    if expected_dev_edges != APPROVED_DEV_EDGES:
        errors.append(
            "dev-only edge inventory must be exactly "
            + formatted_edges(APPROVED_DEV_EDGES)
        )
    required_edges = {
        identity for identity, edge in expected_edges.items() if edge.get("kind") == "required"
    }
    all_edges = set(expected_edges)
    cycle = cycle_in(all_edges | expected_dev_edges, crates)
    if cycle is not None:
        errors.append(f"topology graph contains a cycle involving: {', '.join(cycle)}")

    workspace_packages = validate_local_packages(
        default_metadata, crate_names, root, "default", errors
    )
    validate_local_packages(
        all_feature_metadata,
        crate_names,
        root,
        "all-feature",
        errors,
    )
    validate_dependency_declarations(
        root,
        workspace_packages,
        crate_names,
        expected_edges,
        expected_dev_edges,
        errors,
    )
    groups = {group["id"]: group["members"] for group in topology.get("feature_group", [])}
    packages_by_name = {package["name"]: package for package in workspace_packages}
    if "pi-ai-providers" in packages_by_name:
        validate_provider_features(packages_by_name["pi-ai-providers"], errors)
    if "pi-ai" in packages_by_name:
        validate_facade_features(packages_by_name["pi-ai"], errors)
    for (consumer, dependency), edge in sorted(expected_edges.items()):
        if edge.get("kind") != "optional" or consumer not in packages_by_name:
            continue
        features = packages_by_name[consumer]["features"]
        if feature := edge.get("feature"):
            if feature not in features:
                errors.append(f"missing feature {consumer}/{feature} for optional edge to {dependency}")
            elif not feature_activates_dependency(features, feature, dependency):
                errors.append(f"feature {consumer}/{feature} does not activate dep:{dependency}")
            authorized_features = {feature}
        else:
            group_id = edge.get("feature_group")
            if group_id in features:
                errors.append(f"feature group {group_id} must not be a Cargo feature on {consumer}")
            patterns = groups.get(group_id, [])
            matching = sorted(
                feature_name
                for feature_name in features
                if any(fnmatch.fnmatchcase(feature_name, pattern) for pattern in patterns)
            )
            if not matching:
                errors.append(f"feature group {group_id} has no Cargo feature on {consumer}")
            for feature_name in matching:
                if not feature_activates_dependency(features, feature_name, dependency):
                    errors.append(
                        f"feature {consumer}/{feature_name} does not activate dep:{dependency}"
                    )
            provider_features = [name for name in matching if name.startswith("provider-")]
            if group_id == "provider-selection" and not provider_features:
                errors.append(f"provider-selection needs a provider-* feature on {consumer}")
            authorized_features = set(matching)
        for feature_name in sorted(features):
            if feature_name in authorized_features:
                continue
            if feature_activates_dependency(features, feature_name, dependency):
                errors.append(
                    f"unauthorized feature {consumer}/{feature_name} activates optional edge "
                    f"{consumer} -> {dependency}"
                )

    default_edges = internal_edges(default_metadata, crate_names)
    # A concrete workspace application may select library features even when
    # that library's own default feature set remains empty. Record those exact
    # optional edges explicitly instead of accepting every activated edge.
    expected_default_edges = required_edges | expected_dev_edges | {
        identity for identity, edge in expected_edges.items()
        if edge.get("kind") == "optional" and edge.get("workspace_default") is True
    }
    if default_edges != expected_default_edges:
        missing = expected_default_edges - default_edges
        extra = default_edges - expected_default_edges
        errors.append(
            "default feature graph mismatch: "
            f"missing [{formatted_edges(missing)}], extra [{formatted_edges(extra)}]"
        )
    activated_edges = internal_edges(all_feature_metadata, crate_names)
    expected_all_edges = all_edges | expected_dev_edges
    if activated_edges != expected_all_edges:
        missing = expected_all_edges - activated_edges
        extra = activated_edges - expected_all_edges
        errors.append(
            "all-feature graph mismatch: "
            f"missing [{formatted_edges(missing)}], extra [{formatted_edges(extra)}]"
        )
    activated_cycle = cycle_in(activated_edges, crates)
    if activated_cycle is not None:
        errors.append(f"Cargo all-feature graph contains a cycle involving: {', '.join(activated_cycle)}")
    return errors


def validate() -> list[str]:
    topology = load_toml(TOPOLOGY_PATH)
    default_metadata = cargo_metadata()
    all_feature_metadata = cargo_metadata("--all-features")
    return validate_workspace(ROOT, topology, default_metadata, all_feature_metadata)


def main() -> int:
    try:
        errors = validate()
    except (OSError, RuntimeError, tomllib.TOMLDecodeError, json.JSONDecodeError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    if errors:
        for error in errors:
            print(f"error: {error}", file=sys.stderr)
        return 1
    topology = load_toml(TOPOLOGY_PATH)
    required = sum(edge["kind"] == "required" for edge in topology["edge"])
    optional = sum(edge["kind"] == "optional" for edge in topology["edge"])
    dev_only = len(topology.get("dev_edge", []))
    print(
        "workspace topology: ok "
        f"({len(topology['production_crates'])} crates, {required} required edges, "
        f"{optional} optional edges, {dev_only} dev-only edges)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
