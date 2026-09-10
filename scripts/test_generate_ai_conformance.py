#!/usr/bin/env python3

from __future__ import annotations

import copy
import json
import sys
import tempfile
import tomllib
import unittest
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

import generate_ai_conformance as generator


class AiConformanceGenerationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.fragments = [
            (path, count, generator.load_json(ROOT / path))
            for path, count in generator.FRAGMENTS
        ]
        cls.bindings = generator.rust_test_bindings()
        cls.behavior = generator.build_behavior_inventory(cls.fragments, cls.bindings)
        cls.manifest_text = generator.EXPORT_MANIFEST.read_text(encoding="utf-8")
        cls.mapping_text = generator.EXPORT_MAPPING.read_text(encoding="utf-8")
        cls.manifest = tomllib.loads(cls.manifest_text)
        cls.mapping = tomllib.loads(cls.mapping_text)
        cls.routes = generator.load_toml(generator.ROUTES_MANIFEST)
        cls.map_data = generator.load_toml(generator.UPSTREAM_MAP)
        cls.manifest_digest = generator.manifest_body_digest(cls.manifest_text)
        cls.mapping_digest = generator.mapping_digest(cls.mapping_text)

    def build_export(
        self,
        *,
        manifest: dict | None = None,
        mapping: dict | None = None,
        manifest_digest: str | None = None,
    ) -> tuple[dict, list[tuple[str, str]]]:
        return generator.build_export_inventory(
            copy.deepcopy(self.manifest if manifest is None else manifest),
            self.manifest_digest if manifest_digest is None else manifest_digest,
            copy.deepcopy(self.mapping if mapping is None else mapping),
            self.mapping_digest,
            self.routes,
            self.map_data,
            self.behavior,
        )

    def assert_fails(self, message: str, callable_value) -> None:
        with self.assertRaisesRegex(generator.ConformanceError, message):
            callable_value()

    def test_generated_behavior_and_export_outputs_are_exact(self) -> None:
        inventory, target_probes = self.build_export()
        self.assertEqual(len(self.behavior["rows"]), 327)
        self.assertEqual(len(inventory["rows"]), 699)
        self.assertEqual(inventory["dispositionTotals"], generator.EXPECTED_DISPOSITIONS)
        self.assertEqual(inventory["surfaceTotals"], generator.EXPECTED_SURFACES)
        self.assertEqual(len(target_probes), generator.EXPECTED_TARGET_PROBES)
        generator.verify_dependency_contract()
        self.assertEqual(
            generator.BEHAVIOR_OUTPUT.read_text(encoding="utf-8"),
            generator.json_text(self.behavior),
        )
        self.assertEqual(
            generator.EXPORT_OUTPUT.read_text(encoding="utf-8"),
            generator.json_text(inventory),
        )

    def test_immutable_699_source_identities_and_original_120_probes_are_preserved(self):
        inventory, targets = self.build_export()
        self.assertEqual(generator.canonical_json_digest({"targets": targets}),
            "fe856d504efe3ceb17c2afc715ef2ae366e1778e69245f9f82ee8f6370a5b508")
        fields = ["key", "kind", "sourcePath", "origin", "originLine", "originDeclaration", "surface"]
        self.assertEqual(generator.canonical_json_digest({"identities": [
            {field: row[field] for field in fields} for row in inventory["rows"]
        ]}), "4f207f6fcb4729cd2f0483d29c6e77539a00642b1a0ad50504db2e1768e3d8e3")

    def test_shared_export_manifests_carry_origin_locations(self) -> None:
        for package in ("ai", "agent"):
            document = generator.load_toml(ROOT / f"upstream/manifests/{package}-exports.toml")
            for symbol in document["symbol"]:
                self.assertIn("origin_line", symbol)
                self.assertIn("origin_declaration", symbol)
        missing_ai = {
            f"{symbol['subpath']}:{symbol['name']}"
            for symbol in self.manifest["symbol"]
            if symbol["origin_line"] == 0
        }
        self.assertEqual(missing_ai, generator.EXTERNAL_KEYS)

    def test_behavior_fragment_count_and_order_are_causal(self) -> None:
        wrong_count = copy.deepcopy(self.fragments)
        wrong_count[0][2]["rows"].pop()
        self.assert_fails(
            "fragment row count failed",
            lambda: generator.build_behavior_inventory(wrong_count, self.bindings),
        )
        self.assert_fails(
            "fragment order changed",
            lambda: generator.build_behavior_inventory(
                list(reversed(copy.deepcopy(self.fragments))), self.bindings
            ),
        )

    def test_behavior_ids_fields_owners_and_bindings_are_causal(self) -> None:
        duplicate = copy.deepcopy(self.fragments)
        duplicate[0][2]["rows"][1]["id"] = duplicate[0][2]["rows"][0]["id"]
        self.assert_fails(
            "duplicate behavior id",
            lambda: generator.build_behavior_inventory(duplicate, self.bindings),
        )
        missing_field = copy.deepcopy(self.fragments)
        del missing_field[0][2]["rows"][0]["expected"]
        self.assert_fails(
            "behavior row fields invalid",
            lambda: generator.build_behavior_inventory(missing_field, self.bindings),
        )
        wrong_owner = copy.deepcopy(self.fragments)
        wrong_owner[0][2]["rows"][0]["owner"] = "S04-not-allowed"
        self.assert_fails(
            "outside S03",
            lambda: generator.build_behavior_inventory(wrong_owner, self.bindings),
        )
        dead_binding = copy.deepcopy(self.fragments)
        dead_binding[0][2]["rows"][0]["test"] = "deleted_test_binding"
        self.assert_fails(
            "test binding is not live",
            lambda: generator.build_behavior_inventory(dead_binding, self.bindings),
        )

    def test_behavior_sources_and_expected_observations_are_frozen(self) -> None:
        for field, value in (
            (
                "source",
                "packages/ai/src/utils/retry.ts@"
                + generator.SOURCE_COMMIT
                + ":999999",
            ),
            ("expected", "invented but nonempty observation"),
        ):
            mutated = copy.deepcopy(self.fragments)
            mutated[0][2]["rows"][0][field] = value
            self.assert_fails(
                "behavior fragment content changed",
                lambda mutated=mutated: generator.build_behavior_inventory(
                    mutated, self.bindings
                ),
            )
        rows = copy.deepcopy(self.behavior["rows"])
        rows[0]["source"] = (
            "packages/ai/src/utils/event-stream.ts@"
            + generator.SOURCE_COMMIT
            + ":999999"
        )
        self.assert_fails(
            "behavior source line is out of range",
            lambda: generator.validate_behavior_sources(rows, self.map_data),
        )

    def test_root_and_compat_occurrences_cannot_disappear(self) -> None:
        for subpath in (".", "./compat"):
            manifest = copy.deepcopy(self.manifest)
            removed = next(
                index
                for index, symbol in enumerate(manifest["symbol"])
                if symbol["subpath"] == subpath
            )
            replacement = copy.deepcopy(manifest["symbol"][-1])
            replacement["subpath"] = "./synthetic"
            replacement["name"] = f"Synthetic{len(subpath)}"
            manifest["symbol"].pop(removed)
            manifest["symbol"].append(replacement)
            self.assert_fails(
                "root/compat/other occurrence counts changed",
                lambda manifest=manifest: self.build_export(manifest=manifest),
            )

    def test_duplicate_name_collapse_and_synthetic_export_are_rejected(self) -> None:
        collapsed = copy.deepcopy(self.manifest)
        by_name = {}
        for symbol in collapsed["symbol"]:
            by_name.setdefault(symbol["name"], symbol)
        collapsed["symbol"] = list(by_name.values())
        self.assert_fails(
            "occurrence count",
            lambda: self.build_export(manifest=collapsed),
        )
        synthetic = copy.deepcopy(self.manifest)
        extra = copy.deepcopy(synthetic["symbol"][0])
        extra["subpath"] = "./synthetic"
        extra["name"] = "SyntheticUpstreamExport"
        synthetic["symbol"].append(extra)
        self.assert_fails(
            "occurrence count",
            lambda: self.build_export(manifest=synthetic),
        )

    def test_rule_zero_and_double_matches_are_rejected(self) -> None:
        zero = copy.deepcopy(self.mapping)
        next(rule for rule in zero["rule"] if rule["id"] == "s03-message-transform")[
            "names"
        ] = ["DeletedTransform"]
        self.assert_fails(
            "zero matching rules",
            lambda: self.build_export(mapping=zero),
        )
        double = copy.deepcopy(self.mapping)
        next(rule for rule in double["rule"] if rule["id"] == "s03-auth")[
            "names"
        ].append("AssistantMessageEventStream")
        self.assert_fails(
            "multiple matching rules",
            lambda: self.build_export(mapping=double),
        )

    def test_retry_classifier_cannot_be_assigned_by_name_to_s05(self) -> None:
        mapping = copy.deepcopy(self.mapping)
        classifier = next(
            rule for rule in mapping["rule"] if rule["id"] == "s03-retry-classifier"
        )
        classifier["surfaces"] = ["ai.retry-compat"]
        self.assert_fails(
            "zero matching rules",
            lambda: self.build_export(mapping=mapping),
        )

    def test_missing_evidence_and_present_future_claims_are_rejected(self) -> None:
        missing = copy.deepcopy(self.mapping)
        next(rule for rule in missing["rule"] if rule["id"] == "s03-stream")[
            "evidence"
        ] = "behavior:deleted.evidence"
        self.assert_fails(
            "unknown behavior evidence id",
            lambda: self.build_export(mapping=missing),
        )
        unrelated = copy.deepcopy(self.mapping)
        model_helpers = next(
            rule for rule in unrelated["rule"] if rule["id"] == "s03-model-helpers"
        )
        model_helpers["evidence_overrides"]["calculateCost"] = (
            "behavior:helper.provider-builder.network-publication"
        )
        self.assert_fails(
            "behavior evidence binding mismatch",
            lambda: self.build_export(mapping=unrelated),
        )
        premature = copy.deepcopy(self.mapping)
        next(rule for rule in premature["rule"] if rule["id"] == "s05-openrouter-images")[
            "compile_probe"
        ] = True
        self.assert_fails(
            "deferred projection claims a present implementation",
            lambda: self.build_export(mapping=premature),
        )

    def test_origin_lines_and_source_digest_are_causal(self) -> None:
        stale_origin = copy.deepcopy(self.manifest)
        local = next(
            symbol for symbol in stale_origin["symbol"] if symbol["origin_line"] > 0
        )
        local["origin_line"] += 1
        self.assert_fails(
            "stale origin line/declaration",
            lambda: self.build_export(manifest=stale_origin),
        )
        stale_digest = copy.deepcopy(self.manifest)
        stale_digest["content_digest"] = "sha256:" + "0" * 64
        self.assert_fails(
            "content digest is stale",
            lambda: self.build_export(manifest=stale_digest),
        )

    def test_stale_generated_output_and_mapping_digest_are_causal(self) -> None:
        inventory, _ = self.build_export()
        mutated = copy.deepcopy(inventory)
        mutated["mappingDigest"] = "sha256:" + "0" * 64
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.json"
            path.write_text(generator.json_text(mutated), encoding="utf-8")
            self.assert_fails(
                "stale generated AI conformance output",
                lambda: generator.check_generated(path, generator.json_text(inventory)),
            )

    def test_declared_rust_target_is_the_real_compile_probe(self) -> None:
        mapping = copy.deepcopy(self.mapping)
        next(rule for rule in mapping["rule"] if rule["id"] == "s03-message-transform")[
            "rust_target"
        ] = "pi_ai::DefinitelyMissingS03Projection"
        _, target_probes = self.build_export(mapping=mapping)
        self.assert_fails(
            "exact Rust target compile probe failed",
            lambda: generator.compile_target_probe(target_probes),
        )

    def test_dependency_declarations_features_and_forbidden_nodes_are_causal(self) -> None:
        workspace = generator.load_toml(ROOT / "Cargo.toml")
        core = generator.load_toml(ROOT / "crates/pi-ai-core/Cargo.toml")
        wrong_direct = copy.deepcopy(workspace)
        wrong_direct["workspace"]["dependencies"]["jsonschema"]["default-features"] = True
        self.assert_fails(
            "dependency declaration changed",
            lambda: generator.verify_dependency_contract(
                workspace_data=wrong_direct,
                core_data=core,
                tree_output="",
            ),
        )
        synthetic_tree = "\n".join(
            f"{name} v{version}|"
            for name, version in generator.EXPECTED_TREE_PACKAGES.items()
        )
        synthetic_tree += "\ntokio v1.0.0|"
        self.assert_fails(
            "forbidden pi-ai-core dependencies present",
            lambda: generator.verify_dependency_contract(
                workspace_data=workspace,
                core_data=core,
                tree_output=synthetic_tree,
            ),
        )
        enabled_feature_tree = synthetic_tree.replace(
            "\ntokio v1.0.0|",
            "",
        ).replace("jsonschema v0.52.1|", "jsonschema v0.52.1|resolve-file")
        self.assert_fails(
            "jsonschema optional features are enabled",
            lambda: generator.verify_dependency_contract(
                workspace_data=workspace,
                core_data=core,
                tree_output=enabled_feature_tree,
            ),
        )


class S04ExportExecutionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.catalog = generator.load_json(ROOT / generator.S04_CLOSURE)
        cls.entry = next(entry for entry in cls.catalog["exports"]
            if entry["id"].endswith("/openai-responses.ts:OpenAIResponsesOptions"))

    def test_s04_missing_target_and_disabled_owning_feature_fail_real_compile(self):
        missing = copy.deepcopy(self.entry)
        missing["target"] = "pi_ai_providers::api::openai_responses::MissingExportProjection"
        with self.assertRaisesRegex(generator.ConformanceError, "exact typed target probe failed"):
            generator.compile_closure_profile([missing], missing["features"])
        with self.assertRaisesRegex(generator.ConformanceError, "exact typed target probe failed"):
            generator.compile_closure_profile([self.entry], [])
        generator.compile_closure_profile([self.entry], self.entry["features"])

    def test_s04_nonexistent_and_feature_filtered_tests_cannot_pass_on_zero(self):
        with self.assertRaisesRegex(generator.ConformanceError, "required test did not execute"):
            generator.execute_closure_tests(["lazy#missing_export_closure_test"], [])
        with self.assertRaisesRegex(generator.ConformanceError, "required test did not execute"):
            generator.execute_closure_tests([
                "cloudflare_gateway_binding#literals_and_complete_binding_source_matrix_match"
            ], [])
        generator.execute_closure_tests([
            "cloudflare_gateway_binding#literals_and_complete_binding_source_matrix_match"
        ], ["cloudflare-gateway-binding"])

    def test_s04_source_target_and_required_feature_bindings_are_distinct(self):
        origin, name = self.entry["id"].split(":", 1)
        symbol = {"origin": origin, "name": name}
        rule = {"evidence": "s04-export:" + self.entry["id"],
            "rust_target": self.entry["target"], "features": self.entry["features"]}
        generator.validate_closure_binding(rule, symbol, self.catalog)
        for field, replacement, diagnostic in (
            ("evidence", "s04-export:packages/ai/src/api/lazy.ts:lazyApi", "source/evidence"),
            ("rust_target", "pi_ai_providers::api::lazy::LazyApi", "target/evidence"),
            ("features", [], "required feature"),
        ):
            mutated = copy.deepcopy(rule)
            mutated[field] = replacement
            with self.assertRaisesRegex(generator.ConformanceError, diagnostic):
                generator.validate_closure_binding(mutated, symbol, self.catalog)

    def test_s04_final_partition_and_source_evidence_dependencies_are_frozen(self):
        catalog = generator.closure_catalog()
        inventory = generator.load_json(generator.EXPORT_OUTPUT)
        self.assertEqual((inventory["s04Occurrences"], inventory["s04UniqueSourceSymbols"]), (161, 108))
        self.assertEqual(inventory["distinctTargetProbes"],
            len({(row["rustTarget"], row["kind"]) for row in inventory["rows"]
                if row["disposition"] in {"implemented", "s02-dto"}}))
        self.assertEqual({row["key"] for row in inventory["rows"]
            if row["surface"] == "ai.api-adapters" and row["disposition"] == "mapped-s05"},
            generator.EXPECTED_S04_IMAGE_KEYS)
        for field in ("tests", "sources"):
            changed = copy.deepcopy(catalog)
            changed["exports"][0][field] = ["wrong-evidence"]
            with mock.patch.object(generator, "load_json", return_value=changed):
                with self.assertRaisesRegex(generator.ConformanceError, "catalog changed"):
                    generator.closure_catalog()
        # Even a deliberately rehashed catalog cannot redirect an origin to an unrelated oracle.
        changed = copy.deepcopy(catalog)
        changed["exports"][0]["sources"] = ["cloudflare"]
        with mock.patch.object(generator, "load_json", return_value=changed), mock.patch.object(
            generator, "S04_CLOSURE_DIGEST", generator.canonical_json_digest(changed)
        ):
            with self.assertRaisesRegex(generator.ConformanceError, "source/evidence binding mismatch"):
                generator.closure_catalog()
        path = "conformance/provider-traces/openai-codex-responses/source-comparison.mjs"
        self.assertIn(path, catalog["evidenceFiles"])
        changed = copy.deepcopy(catalog)
        changed["evidenceFiles"][path] = "0" * 64
        with mock.patch.object(generator, "load_json", return_value=changed), mock.patch.object(
            generator, "S04_CLOSURE_DIGEST", generator.canonical_json_digest(changed)
        ):
            with self.assertRaisesRegex(generator.ConformanceError, "source evidence dependency changed"):
                generator.closure_catalog()

    def test_s04_source_comparison_preserves_nested_own_key_order(self):
        expected = {"query": {"first": 1, "second": 2}}
        generator.require_source_observation(copy.deepcopy(expected), expected, "source")
        with self.assertRaisesRegex(generator.ConformanceError, "source observation changed"):
            generator.require_source_observation(
                {"query": {"second": 2, "first": 1}}, expected, "source")

    def test_s04_ignored_filtered_and_wrong_named_results_fail(self):
        name = "selected_test"
        for output in (
            "test result: ok. 0 passed; 0 failed; 0 ignored; 1 filtered out;",
            "test selected_test ... ignored\ntest result: ok. 0 passed; 0 failed; 1 ignored;",
            "test other_test ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;",
        ):
            with self.assertRaises(generator.ConformanceError):
                generator.require_executed_test(output, name)
        generator.require_executed_test(
            "test selected_test ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored;", name)


if __name__ == "__main__":
    unittest.main()
