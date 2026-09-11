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
    metadata: dict[str, Any], crate_names: set[str], profile: str, errors: list[str]
) -> list[dict[str, Any]]:
    packages_by_id = {package["id"]: package for package in metadata["packages"]}
    workspace_packages = [packages_by_id[package_id] for package_id in metadata["workspace_members"]]
    workspace_names = {package["name"] for package in workspace_packages}
    if workspace_names != crate_names or len(workspace_packages) != len(crate_names):
        errors.append(
            f"{profile} Cargo workspace package inventory mismatch: "
            f"expected [{', '.join(sorted(crate_names))}], "
            f"actual [{', '.join(sorted(workspace_names))}]"
        )
    workspace_ids = set(metadata["workspace_members"])
    hidden_local_packages = sorted(
        (
            f"{package['name']} ({package.get('manifest_path', package['id'])})"
            for package in metadata["packages"]
            if package.get("source") is None and package["id"] not in workspace_ids
        )
    )
    if hidden_local_packages:
        errors.append(
            f"{profile} metadata has reachable local path package outside workspace: "
            + ", ".join(hidden_local_packages)
        )
    return workspace_packages


def validate_dependency_declarations(
    root: Path,
    workspace_packages: list[dict[str, Any]],
    crate_names: set[str],
    expected_edges: dict[tuple[str, str], dict[str, Any]],
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
            dependency_path = dependency_record.get("path")
            path_crate = (
                paths_to_crates.get(Path(dependency_path).resolve())
                if dependency_path is not None
                else None
            )
            dependency = dependency_record["name"]
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
    expected_identities = set(expected_edges)
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
        expected_optional = expected_edges[identity].get("kind") == "optional"
        for record in records:
            if record.get("kind") is not None:
                errors.append(f"{consumer} -> {dependency} kind must be normal")
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



S05_PROVIDERS = ("anthropic", "deepseek", "google", "openai", "openai-codex", "openrouter")
S05_CATALOGS = (*S05_PROVIDERS, "openrouter-images")
S05_OAUTH = ("anthropic", "openai-codex", "openrouter")
S05_FACTORY_FEATURES = {
    "provider-anthropic": ["wire-anthropic-messages", "catalog-anthropic"],
    "provider-deepseek": ["wire-openai-completions", "catalog-deepseek"],
    "provider-google": ["wire-google-genai", "catalog-google"],
    "provider-openai": ["wire-openai-responses", "catalog-openai"],
    "provider-openai-codex": ["wire-openai-codex-responses", "catalog-openai-codex"],
    "provider-openrouter": ["wire-openai-completions", "catalog-openrouter"],
    "images-openrouter": ["wire-openrouter-images", "catalog-openrouter-images"],
    "all-providers": ["provider-" + name for name in S05_PROVIDERS],
    "all-images": ["images-openrouter"],
    "all-oauth": ["oauth-" + name for name in S05_OAUTH],
}
S05_FACADE_FEATURES = {
    **{"catalog-" + name: ["dep:pi-ai-providers", "pi-ai-providers/catalog-" + name] for name in S05_CATALOGS},
    **{"provider-" + name: ["catalog-" + name, "pi-ai-providers/provider-" + name] for name in S05_PROVIDERS},
    "images-runtime": ["dep:pi-ai-providers"],
    "images-openrouter": ["images-runtime", "catalog-openrouter-images", "pi-ai-providers/images-openrouter"],
    **{"oauth-" + name: ["dep:pi-ai-providers", "pi-ai-providers/oauth-" + name] for name in S05_OAUTH},
    **{name: values for name, values in S05_FACTORY_FEATURES.items() if name.startswith("all-")},
    "native": ["dep:pi-ai-providers", "pi-ai-providers/native"],
    "auth-native": ["native", "pi-ai-providers/auth-native"],
}
S05_FACADE_GROUP = [*S05_FACADE_FEATURES, "catalog-gen", "dev-oauth-cli"]


def validate_s05_facade_features(package: dict[str, Any], errors: list[str]) -> None:
    expected = {"default": [], "compat": [], "dev-catalog-tools": [], "dev-oauth-cli": ["all-oauth", "auth-native", "dep:tokio"], **S05_FACADE_FEATURES}
    if set(package["features"]) != set(expected):
        errors.append("S05 facade feature inventory changed")
    for name, values in expected.items():
        actual = package["features"].get(name)
        if actual is None or set(actual) != set(values) or len(actual) != len(values):
            errors.append("S05 facade feature ownership changed: " + name)


def validate_s05_oauth_cli(package: dict[str, Any], errors: list[str]) -> None:
    normal = [d for d in package["dependencies"] if d["name"] == "tokio" and d.get("kind") is None]
    if len(normal) != 1 or not normal[0].get("optional") or normal[0].get("uses_default_features") or normal[0].get("req") != "=1.53.1" or set(normal[0].get("features", [])) != {"rt-multi-thread", "net", "io-util", "sync", "time"}:
        errors.append("S05 CLI Tokio must be exact optional existing host dependency")
    for name in package["features"]:
        if name != "dev-oauth-cli" and feature_activates_dependency(package["features"], name, "tokio"):
            errors.append("S05 CLI Tokio activated outside developer target: " + name)
    targets = [t for t in package["targets"] if t["name"] == "pi_ai_cli"]
    if len(targets) != 1 or targets[0].get("kind") != ["bin"] or targets[0].get("required-features") != ["dev-oauth-cli"] or not targets[0]["src_path"].endswith("/src/bin/pi_ai_cli.rs"):
        errors.append("S05 CLI target admission changed")


def validate_s05_facade_topology(topology: dict[str, Any], errors: list[str]) -> None:
    groups = {group["id"]: group["members"] for group in topology.get("feature_group", [])}
    if groups.get("ai-facade-provider-selection") != S05_FACADE_GROUP:
        errors.append("S05 exact facade selector group changed")
    if groups.get("provider-selection") != ["provider-*", "oauth-*", "images-*", "catalog-gen"]:
        errors.append("S05 coding-runtime selector authorization changed")
    for edge in topology.get("edge", []):
        if edge.get("dependency") == "pi-ai-providers" and edge.get("consumer") in {"pi-ai", "pi-coding-runtime"}:
            expected = "ai-facade-provider-selection" if edge["consumer"] == "pi-ai" else "provider-selection"
            if edge.get("feature_group") != expected:
                errors.append("S05 provider edge selector changed: " + edge["consumer"])


def validate_s04_provider_features(package: dict[str, Any], errors: list[str]) -> None:
    """Wire selection never grants native IO or enables default provider breadth."""
    features = package["features"]
    if features.get("default") != []:
        errors.append("S04 provider default features must remain empty")
    if features.get("native") != ["pi-ai-transport/native"]:
        errors.append("S04 provider native feature must own transport native activation")
    for name in S05_FACTORY_FEATURES:
        if name not in features:
            errors.append("S05 missing selected provider feature: " + name)
    for name, activations in features.items():
        if name in {"default", "native"}:
            continue
        if name in S05_FACTORY_FEATURES:
            expected = set(S05_FACTORY_FEATURES[name])
        elif name in {"catalog-openai", "catalog-google", "catalog-anthropic", "catalog-deepseek",
                    "catalog-openai-codex", "catalog-openrouter", "catalog-openrouter-images"}:
            expected = set()
        elif name in {"oauth-anthropic", "oauth-openai-codex", "oauth-openrouter"}:
            expected = {"dep:url"}
        elif name == "auth-native":
            expected = {"native", "pi-ai-transport/auth-native"}
        elif name == "cloudflare-gateway-binding":
            expected = {"dep:url", "serde_json/unbounded_depth"}
        elif name == "wire-bedrock-converse-stream":
            expected = {"dep:url", "dep:ring"}
        elif name.startswith("wire-"):
            expected = {"dep:url"}
        else:
            errors.append("S04 unexpected provider feature: " + name)
            continue
        if set(activations) != expected or len(activations) != len(expected):
            errors.append("S04 provider feature ownership changed: " + name)
    for dependency in package["dependencies"]:
        if dependency["name"] in {"url", "ring"} and not dependency.get("optional"):
            errors.append("S04 provider dependency must stay optional: " + dependency["name"])

def validate_workspace(
    root: Path,
    topology: dict[str, Any],
    default_metadata: dict[str, Any],
    all_feature_metadata: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    validate_s05_facade_topology(topology, errors)
    crates = topology.get("production_crates", [])
    if len(crates) != 15 or len(set(crates)) != 15:
        errors.append("workspace-topology.toml must name exactly 15 unique production crates")
        return errors
    crate_names = set(crates)

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
        expected_edges[identity] = edge
    required_edges = {
        identity for identity, edge in expected_edges.items() if edge.get("kind") == "required"
    }
    all_edges = set(expected_edges)
    cycle = cycle_in(all_edges, crates)
    if cycle is not None:
        errors.append(f"topology graph contains a cycle involving: {', '.join(cycle)}")

    workspace_packages = validate_local_packages(
        default_metadata, crate_names, "default", errors
    )
    validate_local_packages(all_feature_metadata, crate_names, "all-feature", errors)
    validate_dependency_declarations(
        root, workspace_packages, crate_names, expected_edges, errors
    )

    groups = {group["id"]: group["members"] for group in topology.get("feature_group", [])}
    packages_by_name = {package["name"]: package for package in workspace_packages}
    if "pi-ai-providers" in packages_by_name:
        validate_s04_provider_features(packages_by_name["pi-ai-providers"], errors)
    if "pi-ai" in packages_by_name:
        validate_s05_facade_features(packages_by_name["pi-ai"], errors)
        validate_s05_oauth_cli(packages_by_name["pi-ai"], errors)
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
    if default_edges != required_edges:
        missing = required_edges - default_edges
        extra = default_edges - required_edges
        errors.append(
            "default feature graph mismatch: "
            f"missing [{formatted_edges(missing)}], extra [{formatted_edges(extra)}]"
        )
    activated_edges = internal_edges(all_feature_metadata, crate_names)
    if activated_edges != all_edges:
        missing = all_edges - activated_edges
        extra = activated_edges - all_edges
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
    print(
        "workspace topology: ok "
        f"({len(topology['production_crates'])} crates, {required} required edges, "
        f"{optional} optional edges)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
