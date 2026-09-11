#!/usr/bin/env python3
"""Causal regression tests for the Cargo workspace policy checker."""

from __future__ import annotations

import copy
import sys
import unittest
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import check_workspace as checker  # noqa: E402


class WorkspaceCheckerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.topology = checker.load_toml(ROOT / "workspace-topology.toml")
        cls.default_metadata = checker.cargo_metadata()
        cls.all_feature_metadata = checker.cargo_metadata("--all-features")

    def validate_metadata(
        self,
        default_metadata: dict[str, Any],
        all_feature_metadata: dict[str, Any] | None = None,
    ) -> list[str]:
        return checker.validate_workspace(
            ROOT,
            self.topology,
            default_metadata,
            self.all_feature_metadata if all_feature_metadata is None else all_feature_metadata,
        )

    @staticmethod
    def dependency(
        metadata: dict[str, Any], consumer: str, dependency: str
    ) -> dict[str, Any]:
        package = next(package for package in metadata["packages"] if package["name"] == consumer)
        return next(
            record for record in package["dependencies"] if record["name"] == dependency
        )

    def test_s05_oauth_cli_dependency_target_and_activation_controls(self):
        original = next(p for p in self.default_metadata["packages"] if p["name"] == "pi-ai")
        errors = []
        checker.validate_s05_oauth_cli(original, errors)
        self.assertEqual(errors, [])
        for mutation in ("required", "target", "optional", "version", "features", "default", "native", "all-oauth", "unknown"):
            changed = copy.deepcopy(original)
            dep = next(d for d in changed["dependencies"] if d["name"] == "tokio" and d.get("kind") is None)
            target = next(t for t in changed["targets"] if t["name"] == "pi_ai_cli")
            if mutation == "required": target["required-features"] = []
            elif mutation == "target": target["src_path"] = "/wrong.rs"
            elif mutation == "optional": dep["optional"] = False
            elif mutation == "version": dep["req"] = "*"
            elif mutation == "features": dep["features"].append("io-std")
            else: changed["features"][mutation] = ["dev-oauth-cli"]
            errors = []
            checker.validate_s05_oauth_cli(changed, errors)
            self.assertTrue(errors, mutation)
        for bad in (["all-providers", "auth-native", "dep:tokio"], ["all-oauth", "auth-native"], []):
            changed = copy.deepcopy(original)
            changed["features"]["dev-oauth-cli"] = bad
            errors = []
            checker.validate_s05_facade_features(changed, errors)
            self.assertTrue(errors)

    def test_selected_catalog_features_are_data_only_and_unknown_catalogs_fail(self):
        package = next(package for package in self.default_metadata["packages"] if package["name"] == "pi-ai-providers")
        for name in ("catalog-openai", "catalog-google", "catalog-anthropic", "catalog-deepseek",
                     "catalog-openai-codex", "catalog-openrouter", "catalog-openrouter-images"):
            self.assertEqual(package["features"][name], [])
            for activation in ("native", "dep:ring", "wire-openai-responses"):
                changed = copy.deepcopy(package)
                changed["features"][name] = [activation]
                errors = []
                checker.validate_s04_provider_features(changed, errors)
                self.assertTrue(any("feature ownership changed" in error for error in errors), errors)
        changed = copy.deepcopy(package)
        changed["features"]["catalog-excluded"] = []
        errors = []
        checker.validate_s04_provider_features(changed, errors)
        self.assertTrue(any("unexpected provider feature" in error for error in errors), errors)

    def test_s05_exact_factory_and_facade_feature_controls(self):
        for owner, validator, expected in (
            ("pi-ai-providers", checker.validate_s04_provider_features, checker.S05_FACTORY_FEATURES),
            ("pi-ai", checker.validate_s05_facade_features, checker.S05_FACADE_FEATURES),
        ):
            original = next(p for p in self.default_metadata["packages"] if p["name"] == owner)
            errors = []
            validator(original, errors)
            self.assertEqual(errors, [])
            for name in expected:
                for mutation in ("missing", "extra", "wrong", "duplicate"):
                    changed = copy.deepcopy(original)
                    if mutation == "missing":
                        del changed["features"][name]
                    elif mutation == "extra":
                        changed["features"][name].append("native")
                    elif mutation == "wrong":
                        changed["features"][name] = ["pi-ai-providers/all-providers"]
                    else:
                        changed["features"][name] += changed["features"][name][:1]
                    errors = []
                    validator(changed, errors)
                    self.assertTrue(errors, (owner, name, mutation))
            for name in ("provider-excluded", "images-excluded", "catalog-excluded", "oauth-excluded"):
                changed = copy.deepcopy(original)
                changed["features"][name] = []
                errors = []
                validator(changed, errors)
                self.assertTrue(errors, (owner, name))

    def test_s05_facade_group_does_not_broaden_coding_or_compat(self):
        for group, member in (("ai-facade-provider-selection", "compat"),
                              ("ai-facade-provider-selection", "provider-*"),
                              ("provider-selection", "all-providers")):
            changed = copy.deepcopy(self.topology)
            next(g for g in changed["feature_group"] if g["id"] == group)["members"].append(member)
            errors = []
            checker.validate_s05_facade_topology(changed, errors)
            self.assertTrue(errors)
        changed = copy.deepcopy(self.topology)
        next(e for e in changed["edge"] if e["consumer"] == "pi-ai" and e["dependency"] == "pi-ai-providers")["feature_group"] = "provider-selection"
        errors = []
        checker.validate_s05_facade_topology(changed, errors)
        self.assertTrue(errors)

    def test_required_edges_reject_noncanonical_declaration_forms(self) -> None:
        counterexamples = {
            "dev": ("kind", "dev", "kind must be normal"),
            "build": ("kind", "build", "kind must be normal"),
            "target": ("target", "cfg(unix)", "must be untargeted"),
            "rename": ("rename", "telemetry-alias", "must be unrenamed"),
        }
        for label, (field, value, expected) in counterexamples.items():
            with self.subTest(label=label):
                metadata = copy.deepcopy(self.default_metadata)
                self.dependency(metadata, "pi-ai-core", "pi-telemetry")[field] = value
                errors = self.validate_metadata(metadata)
                self.assertTrue(any(expected in error for error in errors), errors)

    def test_reachable_local_path_package_outside_workspace_is_rejected(self) -> None:
        for profile in ("default", "all-feature"):
            with self.subTest(profile=profile):
                default_metadata = copy.deepcopy(self.default_metadata)
                all_feature_metadata = copy.deepcopy(self.all_feature_metadata)
                metadata = (
                    default_metadata if profile == "default" else all_feature_metadata
                )
                rogue_id = "path+file:///tmp/pi-hidden#0.0.0"
                metadata["packages"].append(
                    {
                        "name": "pi-hidden",
                        "id": rogue_id,
                        "source": None,
                        "manifest_path": "/tmp/pi-hidden/Cargo.toml",
                        "dependencies": [],
                        "features": {},
                    }
                )
                metadata["resolve"]["nodes"].append(
                    {"id": rogue_id, "dependencies": [], "deps": [], "features": []}
                )
                consumer_id = next(
                    package["id"]
                    for package in metadata["packages"]
                    if package["name"] == "pi-ai-core"
                )
                consumer_node = next(
                    node for node in metadata["resolve"]["nodes"] if node["id"] == consumer_id
                )
                consumer_node["deps"].append(
                    {
                        "name": "pi_hidden",
                        "pkg": rogue_id,
                        "dep_kinds": [{"kind": None, "target": None}],
                    }
                )
                errors = self.validate_metadata(default_metadata, all_feature_metadata)
                expected = f"{profile} metadata has reachable local path package outside workspace"
                self.assertTrue(any(expected in error for error in errors), errors)

    def test_unauthorized_feature_cannot_indirectly_activate_optional_edge(self) -> None:
        default_metadata = copy.deepcopy(self.default_metadata)
        all_feature_metadata = copy.deepcopy(self.all_feature_metadata)
        for metadata in (default_metadata, all_feature_metadata):
            package = next(
                package for package in metadata["packages"] if package["name"] == "pi-agent-loop"
            )
            package["features"]["proxy-alias"] = ["proxy-inner"]
            package["features"]["proxy-inner"] = ["proxy"]
        errors = self.validate_metadata(default_metadata, all_feature_metadata)
        self.assertTrue(
            any(
                "unauthorized feature pi-agent-loop/proxy-alias activates optional edge "
                "pi-agent-loop -> pi-ai-transport" in error
                for error in errors
            ),
            errors,
        )

    def test_selected_oauth_features_preserve_independent_native_activation(self):
        original = copy.deepcopy(next(p for p in self.default_metadata["packages"] if p["name"] == "pi-ai-providers"))
        for name in ("oauth-anthropic", "oauth-openai-codex", "oauth-openrouter"):
            self.assertEqual(original["features"][name], ["dep:url"])
        self.assertEqual(original["features"]["auth-native"], ["native", "pi-ai-transport/auth-native"])
        for name, activations in (
            ("oauth-anthropic", ["dep:url", "auth-native"]),
            ("oauth-openai-codex", ["dep:url", "native"]),
            ("oauth-openrouter", ["dep:url", "pi-ai-transport/auth-native"]),
            ("auth-native", ["native"]),
            ("oauth-excluded", ["dep:url"]),
        ):
            package = copy.deepcopy(original)
            package["features"][name] = activations
            errors = []
            checker.validate_s04_provider_features(package, errors)
            self.assertTrue(errors, name)
        errors = []
        checker.validate_s04_provider_features(original, errors)
        self.assertEqual(errors, [])

    def test_s04_provider_features_cannot_enable_defaults_or_move_native_ownership(self):
        original = next(package for package in self.default_metadata["packages"]
            if package["name"] == "pi-ai-providers")
        for feature, activation, diagnostic in (
            ("default", ["wire-openai-responses"], "default features must remain empty"),
            ("wire-openai-responses", ["dep:url", "native"], "feature ownership changed"),
            ("native", [], "native feature must own"),
        ):
            package = copy.deepcopy(original)
            package["features"][feature] = activation
            errors = []
            checker.validate_s04_provider_features(package, errors)
            self.assertTrue(any(diagnostic in error for error in errors), errors)
        package = copy.deepcopy(original)
        next(d for d in package["dependencies"] if d["name"] == "url")["optional"] = False
        errors = []
        checker.validate_s04_provider_features(package, errors)
        self.assertTrue(any("must stay optional" in error for error in errors), errors)
        errors = []
        checker.validate_s04_provider_features(original, errors)
        self.assertEqual(errors, [])

    def test_verify_runs_default_and_all_feature_profiles(self) -> None:
        script = (ROOT / "scripts/verify.sh").read_text(encoding="utf-8")
        expected_commands = [
            "cargo check --workspace --all-targets --locked --offline",
            "cargo check --workspace --all-targets --all-features --locked --offline",
            "cargo clippy --workspace --all-targets --locked --offline -- -D warnings",
            "cargo clippy --workspace --all-targets --all-features --locked --offline -- -D warnings",
            "cargo test --workspace --all-targets --locked --offline",
            "cargo test --workspace --all-targets --all-features --locked --offline",
            "cargo doc --workspace --no-deps --document-private-items --locked --offline",
            "cargo doc --workspace --all-features --no-deps --document-private-items --locked --offline",
        ]
        for command in expected_commands:
            with self.subTest(command=command):
                self.assertIn(command, script)


if __name__ == "__main__":
    unittest.main()
