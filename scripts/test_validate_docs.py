#!/usr/bin/env python3
"""Causal tests for the S01 document/upstream validator."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from scripts import validate_docs as validator_module


ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT / "scripts/validate_docs.py"
TOPOLOGY = ROOT / "docs/architecture/02-crate-topology.md"
UPSTREAM_MAP = ROOT / "upstream-map.toml"


def run_validator(*arguments: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(VALIDATOR), *arguments],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


class ValidatorCausalTests(unittest.TestCase):
    def assert_rejected(self, result: subprocess.CompletedProcess[str], needle: str) -> None:
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(needle, result.stderr)

    def test_baseline_is_accepted(self) -> None:
        result = run_validator()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_missing_inventory_row_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-topology-") as directory:
            probe = Path(directory) / "topology.md"
            text = TOPOLOGY.read_text(encoding="utf-8")
            probe.write_text(
                "\n".join(line for line in text.splitlines() if not line.startswith("| 15 |")) + "\n",
                encoding="utf-8",
            )
            result = run_validator("--topology", str(probe), "--skip-manifests")
            self.assert_rejected(result, "crate inventory numbers must be 1..")

    def test_missing_primary_owner_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-owner-") as directory:
            probe = Path(directory) / "upstream-map.toml"
            text = UPSTREAM_MAP.read_text(encoding="utf-8").replace(
                'primary_owner = "pi-telemetry::runtime"',
                'primary_owner = "missing::owner"',
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--map", str(probe), "--skip-manifests")
            self.assert_rejected(result, "unknown primary owner")

    def test_unmatched_pinned_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-unmatched-") as directory:
            probe = Path(directory) / "upstream-map.toml"
            text = UPSTREAM_MAP.read_text(encoding="utf-8").replace(
                '  "packages/coding-agent/src/bun/**",\n',
                "",
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--map", str(probe), "--skip-manifests")
            self.assert_rejected(result, "path must match exactly one route")

    def test_ambiguous_pinned_path_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-ambiguous-") as directory:
            probe = Path(directory) / "upstream-map.toml"
            text = UPSTREAM_MAP.read_text(encoding="utf-8").replace(
                'exclude = [\n  "packages/ai/src/providers/*.models.ts",\n  "packages/ai/src/providers/faux.ts",\n]',
                'exclude = ["packages/ai/src/providers/faux.ts"]',
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--map", str(probe), "--skip-manifests")
            self.assert_rejected(result, "path must match exactly one route")

    def test_missing_harness_heading_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-heading-") as directory:
            probe = Path(directory) / "upstream-map.toml"
            text = UPSTREAM_MAP.read_text(encoding="utf-8").replace(
                'heading = "Part 5 — Public surface"',
                'heading = "Part 5 — Missing heading"',
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--map", str(probe), "--skip-manifests")
            self.assert_rejected(result, "resolves to no lines")

    def test_overlapping_selector_partition_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-selectors-") as directory:
            probe = Path(directory) / "upstream-map.toml"
            text = UPSTREAM_MAP.read_text(encoding="utf-8").replace(
                'exclude_headings = ["Search"]\n',
                "",
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--map", str(probe), "--skip-manifests")
            self.assert_rejected(result, "overlapping nonblank lines")

    def test_text_span_can_select_through_eof(self) -> None:
        lines = ["prefix", "owned", "tail"]
        selected = validator_module.selector_line_set(
            {
                "kind": "text-span",
                "start": "owned",
                "end": "$EOF",
            },
            lines,
        )
        self.assertEqual(selected, {1, 2})

    def test_missing_catalog_provenance_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-provenance-") as directory:
            probe = Path(directory) / "upstream-map.toml"
            text = UPSTREAM_MAP.read_text(encoding="utf-8").replace(
                'provenance_status = "missing-intake-snapshot"\n',
                "",
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--map", str(probe), "--skip-manifests")
            self.assert_rejected(result, "invalid/missing provenance_status")

    def test_catalog_provenance_cannot_be_falsely_promoted(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-false-provenance-") as directory:
            probe = Path(directory) / "upstream-map.toml"
            text = UPSTREAM_MAP.read_text(encoding="utf-8").replace(
                'provenance_status = "missing-intake-snapshot"',
                'provenance_status = "complete"',
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--map", str(probe), "--skip-manifests")
            self.assert_rejected(result, "lacks pinned provenance_snapshot")

    def test_complete_catalog_snapshot_identity_must_match(self) -> None:
        map_data = {
            "surface": [
                {
                    "id": "ai.catalog.projection",
                    "provenance_status": "complete",
                    "provenance_snapshot": {
                        "id": "snapshot-a",
                        "sha256": "sha256:" + "a" * 64,
                        "source_ids": ["source-a"],
                    },
                }
            ]
        }
        catalog_inputs = {
            "provenance_status": "complete",
            "snapshot_id": "snapshot-b",
            "snapshot_sha256": "sha256:" + "b" * 64,
            "source_ids": ["source-b"],
        }
        errors: list[str] = []
        validator_module.validate_catalog_provenance(map_data, catalog_inputs, errors)
        self.assertIn(
            "complete catalog map/manifest snapshot identities must match exactly",
            errors,
        )

    def test_documented_dependency_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-dag-doc-") as directory:
            probe = Path(directory) / "topology.md"
            text = TOPOLOGY.read_text(encoding="utf-8").replace(
                "| `pi-agent-search` | `pi-agent-schema`, `pi-agent-session` | none;",
                "| `pi-agent-search` | none | none;",
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--topology", str(probe), "--skip-manifests")
            self.assert_rejected(result, "documented/machine dependency edges differ")

    def test_documented_dev_only_edge_drift_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-dev-dag-doc-") as directory:
            probe = Path(directory) / "topology.md"
            text = TOPOLOGY.read_text(encoding="utf-8").replace(
                "- `pi-harness-runtime` → `pi-agent-sqlite` (dev-only)\n",
                "",
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--topology", str(probe), "--skip-manifests")
            self.assert_rejected(result, "documented/machine dev-only edges differ")

    def test_second_documented_dev_only_edge_is_not_authorized(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-extra-dev-edge-") as directory:
            directory_path = Path(directory)
            probe_doc = directory_path / "topology.md"
            probe_map = directory_path / "topology.toml"
            text = TOPOLOGY.read_text(encoding="utf-8").replace(
                "- `pi-harness-runtime` → `pi-agent-sqlite` (dev-only)\n",
                "- `pi-harness-runtime` → `pi-agent-sqlite` (dev-only)\n"
                "- `pi-harness-runtime` → `pi-agent-search` (dev-only)\n",
                1,
            )
            probe_doc.write_text(text, encoding="utf-8")
            probe_map.write_text(
                (ROOT / "workspace-topology.toml").read_text(encoding="utf-8")
                + '\n[[dev_edge]]\nconsumer = "pi-harness-runtime"\n'
                + 'dependency = "pi-agent-search"\n',
                encoding="utf-8",
            )
            result = run_validator(
                "--topology", str(probe_doc),
                "--topology-map", str(probe_map),
                "--skip-manifests",
            )
            self.assert_rejected(result, "dev-only edge inventory must be exactly")

    def test_upstream_config_environment_and_default_precedence(self) -> None:
        configured = validator_module.resolve_upstream()
        with tempfile.TemporaryDirectory(prefix="rust-harness-resolver-") as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "--quiet", str(root)], check=True)
            default = root / "upstream/pi-mono"
            default.parent.mkdir()
            default.symlink_to(configured, target_is_directory=True)
            absent = str(root / "absent")
            with mock.patch.dict(os.environ, {}, clear=True):
                self.assertEqual(validator_module.resolve_upstream(root=root), configured)
                subprocess.run(["git", "-C", str(root), "config", "--local", "pirua.upstreamRoot", absent], check=True)
                with self.assertRaisesRegex(ValueError, "checkout does not exist"):
                    validator_module.resolve_upstream(root=root)
            with mock.patch.dict(os.environ, {"PIRUA_UPSTREAM_ROOT": str(configured)}):
                self.assertEqual(validator_module.resolve_upstream(root=root), configured)
                with self.assertRaisesRegex(ValueError, "checkout does not exist"):
                    validator_module.resolve_upstream(absent, root=root)
            subprocess.run(["git", "-C", str(root), "config", "--local", "pirua.upstreamRoot", str(configured)], check=True)
            with mock.patch.dict(os.environ, {"PIRUA_UPSTREAM_ROOT": absent}):
                with self.assertRaisesRegex(ValueError, "checkout does not exist"):
                    validator_module.resolve_upstream(root=root)
                self.assertEqual(validator_module.resolve_upstream(str(configured), root=root), configured)
            command = [sys.executable, "-c", (
                "import sys; from pathlib import Path; "
                "sys.path.insert(0, sys.argv[1]); from upstream import resolve_upstream; "
                "print(resolve_upstream(root=Path(sys.argv[2])))"
            ), str(ROOT / "scripts"), str(root)]
            result = subprocess.run(command, cwd=root, env={}, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout.strip(), str(configured))
            subprocess.run(["git", "-C", str(root), "config", "--local", "--unset", "pirua.upstreamRoot"], check=True)
            default.unlink()
            result = subprocess.run(command, cwd=root, env={}, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("checkout does not exist", result.stderr)

    def test_explicit_probe_checkout_is_not_bypassed_by_environment(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-explicit-checkout-") as directory:
            probe = Path(directory) / "upstream-map.toml"
            text = UPSTREAM_MAP.read_text(encoding="utf-8").replace(
                'checkout = "upstream/pi-mono"',
                f'checkout = "{Path(directory) / "absent-object-store"}"',
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--map", str(probe), "--skip-manifests")
            self.assert_rejected(result, "upstream object-store checkout does not exist")

    def test_dirty_checkout_does_not_change_pinned_evidence(self) -> None:
        with tempfile.TemporaryDirectory(prefix="rust-harness-dirty-") as directory:
            directory_path = Path(directory)
            checkout = directory_path / "pi-mono"
            clone = subprocess.run(
                ["git", "clone", "--quiet", str(validator_module.resolve_upstream()), str(checkout)],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(clone.returncode, 0, clone.stderr)
            dirty_source = checkout / "packages/agent/docs/harness.md"
            dirty_source.write_text("working tree is deliberately unrelated\n", encoding="utf-8")

            probe = directory_path / "upstream-map.toml"
            text = UPSTREAM_MAP.read_text(encoding="utf-8").replace(
                'checkout = "upstream/pi-mono"',
                f'checkout = "{checkout}"',
                1,
            )
            probe.write_text(text, encoding="utf-8")
            result = run_validator("--map", str(probe), "--skip-manifests")
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
