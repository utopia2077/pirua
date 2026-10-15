#!/usr/bin/env python3
"""Discriminating controls for the read-only assembly and proof resolver."""
from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import check_assembly as assembly


class AssemblyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.map_path = ROOT / "upstream-map.toml"
        cls.registry_path = ROOT / "conformance/assembly.json"
        cls.registry = assembly.load_json(cls.registry_path)
        cls.map = tomllib.loads(cls.map_path.read_text())
        cls.labels = {suite for surface in cls.map["surface"] for suite in surface["conformance"]}
        topology = tomllib.loads((ROOT / "workspace-topology.toml").read_text())
        errors = []
        _, entries, _ = assembly.validator.validate_upstream_map(
            cls.map_path, set(topology["production_crates"]), errors, permit_pending_digest=False)
        if errors:
            raise AssertionError(errors)
        cls.resolver = assembly.Resolver(cls.map, entries,
            assembly.resolve_upstream(cls.map["upstream"]["checkout"]))
        cls.agent = tomllib.loads((ROOT / "upstream/manifests/agent-exports.toml").read_text())["symbol"]

    def suite(self, registry, name):
        return next(row for row in registry["suites"] if row["id"] == name)

    def test_complete_join_is_read_only_and_does_not_execute_proof_commands(self):
        before = {row["path"]: hashlib.sha256((ROOT / row["path"]).read_bytes()).hexdigest()
                  for row in self.registry["preserved_inputs"]}
        run = subprocess.run
        def git_only(argv, *args, **kwargs):
            self.assertEqual(argv[0], "git", "pure assembly must never execute proof commands")
            return run(argv, *args, **kwargs)
        with mock.patch("subprocess.run", side_effect=git_only):
            result = assembly.check_assembly(self.map_path, self.registry_path)
        self.assertEqual(result["unresolved"], [])
        totals = result["totals"]
        self.assertEqual(totals["watched_paths"], 1144)
        self.assertEqual(totals["ai_exports"]["occurrences"], 699)
        self.assertEqual(totals["agent_exports"]["occurrences"], 675)
        self.assertEqual(sum(totals["agent_exports"]["surfaces"].values()), 675)
        self.assertEqual(totals["agent_exports"]["surfaces"]["telemetry.package"], 70)
        self.assertEqual(totals["agent_exports"]["surfaces"]["ai.domain"], 2)
        self.assertEqual(totals["agent_exports"]["surfaces"]["agent.harness.scaffold"], 473)
        self.assertEqual(totals["ai_exports"]["surfaces"]["external"], 6)
        self.assertEqual(totals["provider_identities"], {
            "selected_text": 6, "selected_image": 1, "excluded_text": 34, "excluded_faux-testkit": 1})
        self.assertEqual((totals["ai_behavior_rows"], totals["selected_scope_occurrences"]), (327, 241))
        self.assertEqual((totals["harness_selectors"], totals["harness_build_rows"]), (37, 18))
        self.assertEqual(totals["legacy_denominator_statuses"], {
            "source-confirmed": 6, "test-unwitnessed": 7, "non-applicable": 1})
        self.assertEqual((totals["legacy_corpus_rows"], totals["legacy_actual_bridge_rows"]), (12, 5))
        self.assertEqual(totals["coding_operations"], {
            "positive": 18, "negative": 18, "pending": 0, "default_applicable": 17, "extensions_applicable": 18})
        self.assertEqual(totals["proof_commands_executed"], 0)
        self.assertEqual(sum(totals["proof_suites"].values()), 41)
        self.assertEqual(before, {path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest() for path in before})

    def test_missing_logical_proof_is_named(self):
        registry = copy.deepcopy(self.registry)
        registry["suites"] = [row for row in registry["suites"] if row["id"] != "conformance/agent-proxy"]
        with self.assertRaisesRegex(assembly.AssemblyError, r"unresolved proof suites:.*conformance/agent-proxy"):
            assembly.validate_registry(registry, self.labels)

    def test_stale_test_anchor_is_named(self):
        registry = copy.deepcopy(self.registry)
        row = self.suite(registry, "conformance/providers")
        row["evidence"][0]["anchor"] = "test:nonexistent_selected_factory_test"
        with self.assertRaisesRegex(assembly.AssemblyError, "conformance/providers.*missing evidence anchor"):
            assembly.validate_registry(registry, self.labels)

    def test_helper_function_cannot_substitute_for_test_anchor(self):
        reference = {"path": "crates/pi-agent/tests/agent_observation_bridge.rs", "anchor": "test:run_single_turn"}
        with self.assertRaisesRegex(assembly.AssemblyError, "missing evidence anchor"):
            assembly.check_reference(reference, ROOT)

    def test_ignored_native_witness_cannot_substitute_for_default_proof(self):
        reference = {"path": "crates/pi-harness-runtime/tests/runtime_extension_profiles.rs",
                     "anchor": "test:source_root_allocator_peak_is_measured_and_cleanup_is_observed"}
        with self.assertRaisesRegex(assembly.AssemblyError, "missing evidence anchor"):
            assembly.check_reference(reference, ROOT)

    def test_workspace_pass_cannot_substitute_for_proxy_proof(self):
        registry = copy.deepcopy(self.registry)
        row = self.suite(registry, "conformance/agent-proxy")
        row["commands"] = [["cargo", "test", "--workspace", "--offline", "--locked"]]
        with self.assertRaisesRegex(assembly.AssemblyError, "concrete Cargo package and test target"):
            assembly.validate_registry(registry, self.labels)

    def test_target_feature_gate_cannot_be_removed(self):
        registry = copy.deepcopy(self.registry)
        command = self.suite(registry, "conformance/providers")["commands"][0]
        del command[command.index("--features"):]
        with self.assertRaisesRegex(assembly.AssemblyError, "target disabled by feature cfg.*selected_factories"):
            assembly.validate_registry(registry, self.labels)

    def test_function_cfg_anchor_cannot_claim_disabled_native_proof(self):
        registry = copy.deepcopy(self.registry)
        row = self.suite(registry, "conformance/images/fixtures")
        row["evidence"][0]["anchor"] = "test:native_image_post_decodes_and_closes_peer_while_adapter_stays_alive"
        with self.assertRaisesRegex(assembly.AssemblyError, "disabled.*native_image_post"):
            assembly.validate_registry(registry, self.labels)
        with tempfile.TemporaryDirectory(prefix='assembly-disabled-native-') as directory:
            path = Path(directory) / 'registry.json'
            path.write_text(json.dumps(registry))
            run = subprocess.run([sys.executable, str(ROOT / 'scripts/check_assembly.py'), '--registry', str(path)],
                                 cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(run.returncode, 1)
            self.assertIn('native_image_post_decodes_and_closes_peer_while_adapter_stays_alive', run.stderr)

    def test_unrelated_test_definition_cannot_be_unbound_evidence(self):
        registry = copy.deepcopy(self.registry)
        row = self.suite(registry, 'conformance/images/fixtures')
        row['evidence'].append({'path': 'crates/pi-agent/tests/telemetry_facade.rs',
                                'anchor': 'test:facade_exposes_detached_telemetry_recording'})
        with self.assertRaisesRegex(assembly.AssemblyError, 'unbound to a declared command'):
            assembly.validate_registry(registry, self.labels)

    def test_inline_and_external_module_cfg_and_ignore_precede_test(self):
        with tempfile.TemporaryDirectory(prefix="assembly-cfg-") as directory:
            root = Path(directory)
            base = root / "crates/fixture"
            (base / "tests").mkdir(parents=True)
            (base / "Cargo.toml").write_text('[package]\nname = "fixture"\n[features]\nsuite=[]\nnative=[]\n')
            (base / "tests/proof.rs").write_text('''#![cfg(feature = "suite")]
#[cfg(feature = "native")]
mod inline { #[test] fn gated_inline() {} }
#[cfg(feature = "native")]
#[path = "external.rs"]
mod external;
#[ignore]
#[test]
fn ignored_before_marker() {}
#[cfg(target_os = "unsupported")]
#[test]
fn unknown_condition() {}
''')
            (base / "tests/external.rs").write_text('#[test]\nfn gated_external() {}\n')
            command = ['cargo', 'test', '--locked', '--offline', '-p', 'fixture', '--test', 'proof', '--features', 'suite']
            for path, name in [('proof.rs', 'gated_inline'), ('external.rs', 'gated_external'),
                               ('proof.rs', 'ignored_before_marker'), ('proof.rs', 'unknown_condition')]:
                with self.subTest(name=name), self.assertRaises(assembly.AssemblyError):
                    assembly.check_command(command, [{'path': 'crates/fixture/tests/' + path, 'anchor': 'test:' + name}], root)
            native_command = [*command[:-1], 'suite,native']
            for path, name in [('proof.rs', 'inline::gated_inline'), ('external.rs', 'external::gated_external')]:
                assembly.check_command(native_command, [{'path': 'crates/fixture/tests/' + path, 'anchor': 'test:' + name}], root)

    def test_retained_adapter_family_has_complete_exact_proof_routes(self):
        inventory = assembly.load_json(ROOT / 'conformance/ai-domain/inventory.json')
        requirements = set()
        for row in inventory['rows']:
            if row['surface'] != 'ai.api-adapters':
                continue
            for test in row['executedTests']:
                if isinstance(test, str):
                    target, name = test.split('#')
                    requirements.add(('pi-ai-providers', target, name))
                else:
                    requirements.add((test['package'], test['target'], test['name']))
        suite = self.suite(self.registry, 'conformance/provider-traces')
        targets = {(command[command.index('-p') + 1], command[command.index('--test') + 1]) for command in suite['commands']}
        expected = {(package, target) for package, target, _ in requirements}
        self.assertTrue(expected <= targets, sorted(expected - targets))
        references = {(reference['path'], reference.get('anchor')) for reference in suite['evidence']}
        self.assertTrue({(f'crates/{package}/tests/{target}.rs', 'test:' + name)
                         for package, target, name in requirements} <= references)

    def test_retained_adapter_target_anchor_and_profile_gaps_fail_by_name(self):
        inventory = assembly.load_json(ROOT / 'conformance/ai-domain/inventory.json')
        for mutation, expected in [('target', 'missing target'), ('anchor', 'missing exact anchor'),
                                   ('profile', 'missing profile')]:
            suite = copy.deepcopy(self.suite(self.registry, 'conformance/provider-traces'))
            if mutation == 'target':
                suite['commands'] = [command for command in suite['commands'] if 'azure_openai_responses' not in command]
            elif mutation == 'anchor':
                suite['evidence'] = [reference for reference in suite['evidence']
                                     if 'azure_openai_responses.rs' not in reference['path']]
            else:
                command = next(command for command in suite['commands'] if 'azure_openai_responses' in command)
                del command[command.index('--features'):]
            with self.subTest(mutation=mutation), self.assertRaisesRegex(assembly.AssemblyError, expected + '.*azure_openai_responses'):
                assembly.validate_adapter_proofs(suite, inventory)

    def test_cargo_feature_closure_and_cfg_grammar_are_explicit(self):
        manifest = {"features": {"default": ["a"], "a": ["b"], "b": []}}
        self.assertNotIn('default', assembly.cargo_features(['cargo', 'test'], {'features': {}}))
        features = assembly.cargo_features(["cargo", "test"], manifest)
        self.assertTrue(assembly.cfg_enabled('all(feature = "a", any(feature = "b", feature = "c"), not(feature = "c"))', features))
        self.assertFalse(assembly.cfg_enabled('feature = "a"', assembly.cargo_features(
            ["cargo", "test", "--no-default-features"], manifest)))
        with self.assertRaisesRegex(assembly.AssemblyError, "unsupported proof cfg"):
            assembly.cfg_enabled('target_os = "unknown"', features)

    def test_accepted_limit_requires_public_evidence(self):
        registry = copy.deepcopy(self.registry)
        self.suite(registry, "conformance/agent-proxy")["limitations"] = []
        with self.assertRaisesRegex(assembly.AssemblyError, "accepted-limit lacks evidence.*agent-proxy"):
            assembly.validate_registry(registry, self.labels)
        self.suite(registry, "conformance/agent-proxy")["limitations"] = [{"path": "missing-limit.md"}]
        with self.assertRaisesRegex(assembly.AssemblyError, "agent-proxy.*missing evidence"):
            assembly.validate_registry(registry, self.labels)

    def test_registry_cannot_duplicate_ownership(self):
        registry = copy.deepcopy(self.registry)
        registry["suites"][0]["primary_owner"] = "invented-owner"
        with self.assertRaisesRegex(assembly.AssemblyError, "cannot duplicate ownership"):
            assembly.validate_registry(registry, self.labels)

    def test_evidence_cannot_escape_public_root(self):
        with self.assertRaisesRegex(assembly.AssemblyError, "non-public evidence path"):
            assembly.check_reference({"path": "../outside.md"}, ROOT)

    def test_cross_package_exports_resolve_actual_source_owners(self):
        external = [symbol for symbol in self.agent if symbol["origin"].startswith("@")]
        self.assertEqual(len(external), 72)
        for symbol in external:
            surface = self.resolver.export(symbol)
            self.assertEqual(surface, "ai.domain" if symbol["name"] == "uuidv7" else "telemetry.package")
        alias = next(symbol for symbol in external if symbol["name"] == "TelemetrySpanAttributes")
        self.assertEqual(self.resolver.external_name(alias["source_path"], alias["origin"], alias["name"]), "SpanAttributes")
        self.assertIn("SpanAttributes as TelemetrySpanAttributes,", "\n".join(
            self.resolver.lines("packages/agent/src/index.ts")))

    def test_nonexistent_alias_and_unwatched_package_fail_closed(self):
        alias = copy.deepcopy(next(symbol for symbol in self.agent if symbol["name"] == "TelemetrySpanAttributes"))
        alias["name"] = "NonexistentTelemetryAlias"
        with self.assertRaisesRegex(assembly.AssemblyError, "pinned external alias"):
            self.resolver.export(alias)
        alias["origin"] = "@unwatched/missing"
        with self.assertRaisesRegex(assembly.AssemblyError, "unresolved watched package reexport"):
            self.resolver.export(alias)

    def test_value_namespace_subset_accepts_class_but_rejects_namespace_loss(self):
        symbol = next(item for item in self.agent if item["name"] == "InMemoryTelemetryContext")
        self.assertEqual(symbol["kind"], "value")
        self.assertEqual(self.resolver.export(symbol), "telemetry.package")
        symbols = copy.deepcopy(self.resolver.package_symbols)
        try:
            for item in self.resolver.package_symbols["@earendil-works/pi-telemetry"]:
                if item["subpath"] == "." and item["name"] == symbol["name"]:
                    self.assertEqual(item["kind"], "type-and-value")
                    item["kind"] = "type"
            with self.assertRaisesRegex(assembly.AssemblyError, "package export namespace loss"):
                self.resolver.export(symbol)
        finally:
            self.resolver.package_symbols = symbols

    def test_cli_missing_proxy_owner_names_six_exports_and_u12_then_restores(self):
        saved = self.map_path.read_bytes()
        owner = re.compile(rb'\[\[owner\]\]\nid = "pi-agent-loop::proxy"\n(?:(?!\[\[).)*', re.DOTALL)
        changed, count = owner.subn(b"", saved)
        self.assertEqual(count, 1)
        upstream = ROOT / "upstream/manifests/agent-exports.toml"
        manifest_hash = hashlib.sha256(upstream.read_bytes()).hexdigest()
        with tempfile.TemporaryDirectory(prefix="assembly-map-") as directory:
            path = Path(directory) / "upstream-map.toml"
            path.write_bytes(changed)
            command = [sys.executable, str(ROOT / "scripts/check_assembly.py"), "--map", str(path)]
            red = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(red.returncode, 1, red.stdout + red.stderr)
            result = json.loads(red.stdout.split("\nAssembly:", 1)[0])
            failures = result["unresolved"]
            self.assertTrue(any("legacy:U12" in error and "pi-agent-loop::proxy" in error for error in failures))
            export_failures = [error for error in failures if error.startswith("export:agent:") and "agent.legacy-proxy" in error]
            self.assertEqual(len(export_failures), 6, failures)
            self.assertTrue(all("pi-agent-loop::proxy" in error for error in export_failures))
            path.write_bytes(saved)
            self.assertEqual(path.read_bytes(), saved)
            green = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
            self.assertEqual(green.returncode, 0, green.stdout + green.stderr)
            self.assertIn("0 unresolved mappings", green.stdout)
        self.assertEqual(self.map_path.read_bytes(), saved)
        self.assertEqual(hashlib.sha256(upstream.read_bytes()).hexdigest(), manifest_hash)


if __name__ == "__main__":
    unittest.main()
