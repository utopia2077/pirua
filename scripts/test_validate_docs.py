#!/usr/bin/env python3
"""Causal controls for public topology and immutable upstream classification."""
from __future__ import annotations
import copy
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest import mock
from scripts import validate_docs as validator_module
from scripts.upstream_drift import render_map

ROOT = Path(__file__).resolve().parents[1]
VALIDATOR = ROOT/'scripts/validate_docs.py'
TOPOLOGY = ROOT/'docs/architecture/02-crate-topology.md'
UPSTREAM_MAP = ROOT/'upstream-map.toml'


def run_validator(*arguments):
    return subprocess.run([sys.executable, str(VALIDATOR), *arguments], cwd=ROOT,
                          check=False, capture_output=True, text=True)


class ValidatorCausalTests(unittest.TestCase):
    def assert_rejected(self, result, needle):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(needle, result.stderr)

    def probe_map(self, mutation, expected):
        data = tomllib.loads(UPSTREAM_MAP.read_text())
        mutation(data)
        with tempfile.TemporaryDirectory(prefix='pirua-map-') as directory:
            path = Path(directory)/'map.toml'
            path.write_text(render_map(data))
            self.assert_rejected(run_validator('--map', str(path), '--skip-manifests'), expected)

    def test_baseline_is_accepted(self):
        result = run_validator()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_missing_inventory_row_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'topology.md'
            count = len(tomllib.loads((ROOT/'workspace-topology.toml').read_text())['production_crates'])
            path.write_text('\n'.join(line for line in TOPOLOGY.read_text().splitlines() if not line.startswith(f'| {count} |'))+'\n')
            self.assert_rejected(run_validator('--topology', str(path), '--skip-manifests'), 'crate inventory numbers must be 1..')

    def test_missing_primary_owner_is_rejected(self):
        self.probe_map(lambda data: data['surface'][0].update(primary_owner='missing::owner'), 'unknown primary owner')

    def test_unmatched_pinned_path_is_rejected(self):
        self.probe_map(lambda data: data['route'][0]['include'].pop(), 'path must match exactly one route')

    def test_ambiguous_pinned_path_is_rejected(self):
        def mutation(data):
            route = copy.deepcopy(data['route'][0]); route['id'] += '.duplicate'; data['route'].append(route)
        self.probe_map(mutation, 'path must match exactly one route')

    def selectors(self, data, overlap):
        path = 'packages/agent/src/proxy.ts'
        route = next(row for row in data['route'] if path in row['include'])
        route['selector_partition'] = 'probe'
        source = validator_module.git_blob(validator_module.resolve_upstream(), data['upstream']['commit'], path).decode()
        selector = {'id':'probe-first', 'partition':'probe', 'path':path, 'surface':route['surface'],
                    'kind':'text-span', 'start':next(line for line in source.splitlines() if line.strip() and sum(other.startswith(line) for other in source.splitlines())==1), 'end':'$EOF'}
        if overlap:
            data['selector'] = [selector, {**selector, 'id':'probe-second'}]
        else:
            data['selector'] = [{**selector, 'kind':'markdown-section', 'heading':'Absent heading'}]

    def test_missing_selector_heading_is_rejected(self):
        self.probe_map(lambda data: self.selectors(data, False), 'resolves to no lines')

    def test_overlapping_selector_partition_is_rejected(self):
        self.probe_map(lambda data: self.selectors(data, True), 'overlapping nonblank lines')

    def test_text_span_can_select_through_eof(self):
        self.assertEqual(validator_module.selector_line_set({'kind':'text-span','start':'owned','end':'$EOF'}, ['prefix','owned','tail']), {1,2})

    def test_missing_catalog_provenance_is_rejected(self):
        def mutation(data):
            next(row for row in data['surface'] if row['id']=='ai.catalog.projection').pop('provenance_status')
        self.probe_map(mutation, 'invalid/missing provenance_status')

    def test_complete_catalog_requires_snapshot_identity(self):
        def mutation(data):
            next(row for row in data['surface'] if row['id']=='ai.catalog.projection').pop('provenance_snapshot')
        self.probe_map(mutation, 'lacks pinned provenance_snapshot')

    def test_complete_catalog_snapshot_identity_must_match(self):
        data = tomllib.loads(UPSTREAM_MAP.read_text())
        manifest = tomllib.loads((ROOT/'upstream/manifests/catalog-inputs.toml').read_text())
        for key, value in [('snapshot_id','wrong'), ('snapshot_sha256','sha256:'+'0'*64), ('source_ids',['wrong'])]:
            with self.subTest(key=key):
                errors = []
                validator_module.validate_catalog_provenance(data, {**manifest,key:value}, errors)
                self.assertIn('complete catalog map/manifest snapshot identities must match exactly', errors)

    def test_documented_dependency_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/'topology.md'
            lines=TOPOLOGY.read_text().splitlines()
            line=next(line for line in lines if line.startswith('| `pi-agent` |'))
            path.write_text('\n'.join('| `pi-agent` | none | none |' if row==line else row for row in lines)+'\n')
            self.assert_rejected(run_validator('--topology',str(path),'--skip-manifests'),'documented/machine dependency edges differ')

    def test_documented_dev_only_edge_is_not_authorized(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'topology.md'
            path.write_text(TOPOLOGY.read_text().replace('## Literal schema', '- `pi-agent` → `pi-coding-session` (dev-only)\n\n## Literal schema'))
            self.assert_rejected(run_validator('--topology',str(path),'--skip-manifests'),'documented/machine dev-only edges differ')

    def test_machine_dev_only_edge_is_not_authorized(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'topology.toml'
            path.write_text((ROOT/'workspace-topology.toml').read_text()+'\n[[dev_edge]]\nconsumer="pi-agent"\ndependency="pi-coding-session"\n')
            self.assert_rejected(run_validator('--topology-map',str(path),'--skip-manifests'),'no dev-only internal edge is authorized')

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
            dirty_source = checkout / "packages/agent/src/agent.ts"
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
