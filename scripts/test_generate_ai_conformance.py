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
        rule = next(rule for rule in premature["rule"] if "./api/openrouter-images:generateImages" in rule.get("occurrences", []))
        rule.update(disposition="implemented", evidence="future:conformance/openrouter-images",
                    rust_target="pi_ai::compat::ImagesApiRegistry::generate", compile_probe=True)
        self.assert_fails(
            "present implementation claims future evidence",
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

    def test_s05_scope_partition_preserves_all_accepted_evidence(self):
        inventory, _ = self.build_export()
        rows = inventory["rows"]
        self.assertEqual(inventory["dispositionTotals"], {
            "s02-dto": 146, "implemented": 398, "rust-nonapplicable": 24,
            "owner-excluded": 125, "external-schema-projection": 6,
        })
        self.assertEqual(inventory["selectedProviderScope"]["scopeTotals"], {
            "selected": 17, "shared": 97, "owner-excluded": 125, "rust-platform": 2,
        })
        original_nonapplicable = [row for row in rows if row["disposition"] == "rust-nonapplicable"
                                  and row["owner"] != "S05-platform-replacement"]
        self.assertEqual(len(original_nonapplicable), 22)
        self.assertEqual(generator.canonical_json_digest({"rows": original_nonapplicable}),
            "3b299421b86261111412c16efc58d0bc56ba5d2ac31f1e0d524c714c74087fc0")
        self.assertEqual(generator.canonical_json_digest({"rows": [row for row in rows
            if row["evidence"].startswith("s04-export:")]}),
            "c9ea7f49563886c7145fd9b62a740433c1624d18364adae5c53712566fcbe9a0")
        aliases = [row for row in rows if row["origin"] == "packages/ai/src/legacy-api-aliases.ts"]
        self.assertEqual(len(aliases), 16)
        self.assertTrue(all(row["disposition"] == "implemented" and row["evidence"].startswith("s05-export:") for row in aliases))
        by_name = {row["name"]: row for row in rows}
        self.assertEqual(by_name["bedrockProviderModule"]["disposition"], "implemented")
        for name in ("amazonBedrockProvider", "AMAZON_BEDROCK_MODELS", "registerFauxProvider"):
            self.assertEqual(by_name[name]["disposition"], "owner-excluded")
        for name in ("registerBunOAuthFlows", "default"):
            self.assertEqual(by_name[name]["owner"], "S05-platform-replacement")

    def test_s05_count_preserving_factory_and_shared_alias_exclusions_fail(self):
        for retained, excluded in (
            ("./providers/openai:openaiProvider", "./providers/amazon-bedrock:amazonBedrockProvider"),
            ("./bedrock-provider:bedrockProviderModule", "./providers/amazon-bedrock:amazonBedrockProvider"),
            ("./compat:streamMistral", "./compat:registerFauxProvider"),
        ):
            with self.subTest(retained=retained):
                mapping = copy.deepcopy(self.mapping)
                retained_rule = next(rule for rule in mapping["rule"] if retained in rule.get("occurrences", []))
                excluded_rule = next(rule for rule in mapping["rule"] if excluded in rule.get("occurrences", []))
                retained_rule["occurrences"].remove(retained)
                retained_rule["occurrences"].append(excluded)
                excluded_rule["occurrences"].remove(excluded)
                excluded_rule["occurrences"].append(retained)
                inventory, _ = self.build_export()
                by_key = {row["key"]: row for row in inventory["rows"]}
                # Rebind every selector, not only the occurrence key, so the exact
                # evidence binding (rather than an unrelated missing match) rejects it.
                for selector, field in (("origins", "origin"), ("names", "name"), ("surfaces", "surface")):
                    retained_rule[selector] = [by_key[excluded][field]]
                if by_key[retained]["surface"] not in excluded_rule["surfaces"]:
                    excluded_rule["surfaces"].append(by_key[retained]["surface"])
                self.assert_fails("S05 symbol binding missing", lambda: self.build_export(mapping=mapping))
                # Preserve the original, independent scope-policy failure as well.
                swapped = copy.deepcopy(inventory["rows"])
                changed = {row["key"]: row for row in swapped}
                changed[retained]["disposition"], changed[excluded]["disposition"] = changed[excluded]["disposition"], changed[retained]["disposition"]
                self.assert_fails("scope disposition contradicts source decision",
                                  lambda: generator.validate_selected_provider_scope(swapped))

    def test_s05_exclusions_require_owner_decision_and_never_claim_implementation(self):
        for field in ("reason", "replacement", "decision_reference", "revisit_trigger"):
            mapping = copy.deepcopy(self.mapping)
            rule = next(rule for rule in mapping["rule"] if rule["disposition"] == "owner-excluded")
            del rule[field]
            self.assert_fails("rationale fields", lambda: self.build_export(mapping=mapping))
        for field, value, diagnostic in (
            ("evidence", "future:conformance/providers", "present implementation claims future evidence"),
            ("compile_probe", True, "invalid exact Rust target"),
            ("rust_target", "pi_ai::InventedFactory", "cannot claim a Rust product target"),
            ("decision_reference", "unrelated-owner-decision", "selected-scope owner decision"),
        ):
            mapping = copy.deepcopy(self.mapping)
            next(rule for rule in mapping["rule"] if rule["disposition"] == "owner-excluded")[field] = value
            self.assert_fails(diagnostic, lambda: self.build_export(mapping=mapping))

    def test_s05_scope_ledger_has_independent_factory_and_wrapper_controls(self):
        inventory, _ = self.build_export()
        scope = generator.load_json(ROOT / generator.SELECTED_SCOPE_PATH)
        mutated = copy.deepcopy(scope)
        mutated["selectedSourceEntries"][-1]["providerId"] = "openrouter-images"
        self.assert_fails("exact six text and one image", lambda: generator.validate_selected_provider_scope(
            inventory["rows"], document=mutated))
        for retained_name, excluded_name, diagnostic in (
            ("openaiProvider", "amazonBedrockProvider", "factory source classification"),
            ("streamMistral", "registerFauxProvider", "Faux compatibility registration remains owner-excluded"),
            ("bedrockProviderModule", "RadiusProviderOptions", "must remain shared"),
        ):
            mutated = copy.deepcopy(scope)
            left = next(row for row in mutated["rows"] if row["name"] == retained_name)
            right = next(row for row in mutated["rows"] if row["name"] == excluded_name)
            left["scope"], right["scope"] = right["scope"], left["scope"]
            # Rehashing a count-preserving ledger cannot change actual selected factories or shared wrappers.
            with mock.patch.object(generator, "SELECTED_SCOPE_DIGEST", generator.canonical_json_digest(mutated)):
                self.assert_fails(diagnostic, lambda: generator.validate_selected_provider_scope(
                    inventory["rows"], document=mutated))
        mutated = copy.deepcopy(scope)
        mutated["rows"].pop()
        self.assert_fails("all 241 source occurrences", lambda: generator.validate_selected_provider_scope(
            inventory["rows"], document=mutated))
        rows = copy.deepcopy(inventory["rows"])
        target = next(row for row in rows if row["name"] == "openaiProvider")
        target["key"] = "./providers/synthetic:syntheticProvider"
        self.assert_fails("source identity missing or changed", lambda: generator.validate_selected_provider_scope(rows))

    def test_s05_new_source_factory_and_changed_image_namespace_are_rejected(self):
        inventory, _ = self.build_export()
        providers = generator.load_toml(ROOT / "upstream/manifests/providers.toml")
        for change, diagnostic in (("new", "factory source classification"),
                                   ("image-id", "source entry IDs disagree")):
            mutated = copy.deepcopy(providers)
            if change == "new":
                extra = copy.deepcopy(mutated["provider"][0])
                extra.update(module="synthetic-new", factory_symbol="syntheticNewProvider",
                             provider_ids=["synthetic-new"])
                mutated["provider"].append(extra)
            else:
                next(entry for entry in mutated["provider"] if entry["kind"] == "image")["provider_ids"] = ["openrouter-images"]
            with mock.patch.object(generator, "load_toml", return_value=mutated):
                self.assert_fails(diagnostic, lambda: generator.validate_selected_provider_scope(inventory["rows"]))

    def test_s05_scope_cannot_turn_a_retained_future_into_unproved_implementation(self):
        inventory, _ = self.build_export()
        rows = copy.deepcopy(inventory["rows"])
        next(row for row in rows if row["name"] == "openaiProvider")["disposition"] = "mapped-s05"
        # The scope ledger allows progress; mapping/evidence checks still require the actual proof.
        generator.validate_selected_provider_scope(rows)
        mapping = copy.deepcopy(self.mapping)
        next(rule for rule in mapping["rule"] if "./providers/openai:openaiProvider" in rule.get("occurrences", []))["compile_probe"] = False
        self.assert_fails("present projection lacks an exact target compile probe",
                          lambda: self.build_export(mapping=mapping))


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
        owning = {(entry["owningTarget"], entry["sourceKind"]) for entry in generator.s05.catalog()["symbols"]}
        self.assertEqual(inventory["distinctTargetProbes"],
            len({(row["rustTarget"], row["kind"]) for row in inventory["rows"]
                if row["disposition"] in {"implemented", "s02-dto"}} | owning))
        self.assertEqual(inventory["s04DistinctTargetProbes"], 108)
        self.assertEqual(inventory["s05DistinctTargetProbes"], 131)
        self.assertEqual({row["key"] for row in inventory["rows"]
            if row["surface"] == "ai.api-adapters" and row["evidence"].startswith("s05-export:")},
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




class S05ExportAdmissionTests(unittest.TestCase):
    """Rehashed candidate content cannot redefine independent S05 authority."""
    @classmethod
    def setUpClass(cls):
        import s05_export_closure
        cls.s05 = s05_export_closure
        cls.document = cls.s05.catalog()

    def rejected(self, mutate):
        changed = copy.deepcopy(self.document)
        mutate(changed)
        with self.assertRaises(self.s05.ClosureError):
            self.s05.catalog(document=changed, expected_digest=self.s05.digest(changed), check_files=False)
        self.s05.catalog(document=self.document, check_files=False)

    def test_s05_schema_version_rejects_bool_and_float_after_rehash(self):
        for value in (True, 1.0):
            changed = copy.deepcopy(self.document)
            changed["schemaVersion"] = value
            with self.assertRaisesRegex(self.s05.ClosureError, "schema identity|type-preserving"):
                self.s05.catalog(document=changed, expected_digest=self.s05.digest(changed), check_files=False)
        self.s05.catalog(document=self.document, check_files=False)

    def test_s05_env_constant_probes_have_compile_time_value_checks(self):
        for name in ("ANTHROPIC_API_KEY_ENV", "ANTHROPIC_AUTH_TOKEN_ENV", "ANTHROPIC_OAUTH_TOKEN_ENV"):
            policy = self.s05.SYMBOL_POLICY["packages/ai/src/env-api-keys.ts:" + name]
            for body in [policy["owningProbe"], *[r["routeProbe"] for r in policy["occurrences"]]]:
                self.assertIn("const _: ()", body)
                self.assertIn('b"' + name.removesuffix("_ENV") + '"', body)
                self.assertEqual(body.count("$target"), 1)

    def test_s05_exact_occurrence_source_kind_and_route_policy_survive_rehash(self):
        def route(d):
            return next(s for s in d["symbols"] if any(r["routeTarget"].startswith("pi_ai::compat::") for r in s["occurrences"]))["occurrences"][0]
        for mutate in (
            lambda d: d["symbols"].pop(),
            lambda d: d["symbols"].append(copy.deepcopy(d["symbols"][0])),
            lambda d: d["symbols"][0]["occurrences"].pop(),
            lambda d: d["symbols"][0]["occurrences"].append(copy.deepcopy(d["symbols"][0]["occurrences"][0])),
            lambda d: d["symbols"][0].update(id="packages/ai/src/providers/faux.ts:createFauxCore"),
            lambda d: d["symbols"][0].update(sourceKind="type-and-value" if d["symbols"][0]["sourceKind"] != "type-and-value" else "type"),
            lambda d: route(d).update(routeTarget="pi_ai_providers::auth::LazyOAuth::new"),
            lambda d: route(d).update(routePackage="pi-ai-providers"),
            lambda d: route(d).update(routeFeatures=[]),
            lambda d: route(d).update(key=".:synthetic-swapped"),
        ):
            self.rejected(mutate)
        self.assertEqual(self.s05.counts(self.document), {"symbols":90,"occurrences":114,"owning":87,"routes":108,"union":131})

    def test_s05_exact_typed_probe_policy_rejects_import_option_unconstrained_and_wrong_outputs(self):
        for body in ("fn probe(){let _ = $target;}", "use $target as V;", "fn probe(){let _:Option<$target>=None;}", "fn probe(){let _:fn()->()=$target;}", "fn probe(){let _:fn()->BoxFuture<'static,()>= $target;}"):
            self.rejected(lambda d, body=body: d["symbols"][0].update(owningProbe=body))
        first = self.document["symbols"][0]
        with self.assertRaises(self.s05.ClosureError):
            self.s05.probe_source({(first["owningTarget"], first["sourceKind"]): "fn probe(){let _=$target;}"})
        trait = next(s for s in self.document["symbols"] if s["id"].endswith(":ImagesApiFunction"))
        self.assertIn("Result<BoxFuture<'a,Result<AssistantImages,ModelsError>>,ModelsError>", trait["owningProbe"])
        self.rejected(lambda d: next(s for s in d["symbols"] if s["id"]==trait["id"]).update(owningProbe=trait["owningProbe"].replace("Result<BoxFuture<'a,Result<AssistantImages,ModelsError>>,ModelsError>","BoxFuture<'a,AssistantImages>")))

    def test_s05_independent_source_ownership_modes_and_fingerprints_survive_rehash(self):
        def relabel(d):
            symbol=d["symbols"][0]
            unrelated=next(s for s in d["sources"] if s["id"] not in symbol["sources"])
            symbol["sources"]=[unrelated["id"]]
            unrelated["origins"].append(symbol["id"].rsplit(":",1)[0])
        self.rejected(relabel)
        for field,value in [("args",["--capture"]),("mode","success"),("executable","sh"),("script","unrelated.py"),("scriptSha256","0"*64),("expected","other.json"),("relation","arbitrary-success")]:
            self.rejected(lambda d,f=field,v=value:d["sources"][0].update({f:v}))
        for field in ("testFiles","evidenceFiles"):
            self.rejected(lambda d,f=field:d[f].update({next(iter(d[f])):"0"*64}))
        self.rejected(lambda d:d["symbols"][0]["tests"][0].update(sha256="0"*64))

    def test_s05_actual_stale_source_or_test_bytes_fail_without_catalog_changes(self):
        original = Path.read_bytes
        for field in ("testFiles", "evidenceFiles"):
            target = ROOT / next(iter(self.document[field]))
            def changed(path, target=target):
                return b"synthetic changed bytes" if path == target else original(path)
            with mock.patch.object(Path, "read_bytes", autospec=True, side_effect=changed):
                with self.assertRaisesRegex(self.s05.ClosureError, "evidence file changed"):
                    self.s05.catalog(document=self.document)
        self.s05.catalog(document=self.document)

    def test_s05_mandatory_composition_cannot_be_dropped_emptied_or_relabelled(self):
        self.assertEqual({c["id"] for c in self.document["composition"]},{"selected-oauth-flows","oauth-developer-cli","catalog-developer-binaries","codex-cleanup-ownership"})
        for mutate in (lambda d:d["composition"].pop(),lambda d:d["composition"][0].update(tests=[]),lambda d:d["composition"][0].update(sources=[]),lambda d:d["composition"][0].update(id="synthetic"),lambda d:d["composition"].append(copy.deepcopy(d["composition"][0]))):self.rejected(mutate)
        profiles=self.s05.test_profiles(self.document)
        names={t["name"] for tests in profiles.values() for t in tests.values()}
        self.assertIn("oauth_cli_real_binary_no_network_help_errors_pipes_and_eof",names)
        self.assertIn("actual_linked_facade_executes_physical_codex_snapshot_and_drop_proof",names)
        self.assertNotIn("oauth_cli_internal_browser_child",names)

    def test_s05_package_kind_profile_and_exact_executable_admission(self):
        good={"reason":"compiler-artifact","package_id":"local-package","target":{"name":"same-suite","kind":["test"]},"profile":{"test":True},"executable":"/synthetic/executable"}
        self.assertEqual(self.s05.select_artifacts([good],"local-package",{"same-suite":"test"}),{"same-suite":"/synthetic/executable"})
        variants=[[],[good,good]]
        for field,value in [("package_id","foreign-package"),("profile",{"test":False}),("executable",None),("target",{"name":"same-suite","kind":["lib"]})]:
            changed=copy.deepcopy(good);changed[field]=value;variants.append([changed])
        for messages in variants:
            with self.assertRaises(self.s05.ClosureError):self.s05.select_artifacts(messages,"local-package",{"same-suite":"test"})
        good_output="test exact_name ... ok\ntest result: ok. 1 passed; 0 failed; 0 ignored; 3 filtered out;\n"
        self.s05.executed_test(good_output,"exact_name")
        for bad in [good_output.replace("exact_name","wrong_name"),good_output.replace("1 passed","0 passed"),good_output.replace("0 ignored","1 ignored"),good_output.replace("... ok","... ignored"),good_output+good_output]:
            with self.assertRaises(self.s05.ClosureError):self.s05.executed_test(bad,"exact_name")

    def test_s05_measured_feature_removal_and_transitive_repair_fail(self):
        expected=self.s05.FEATURE_POLICY[("pi-ai",("catalog-openai",))]
        def metadata(features):
            return {"packages":[{"id":p,"name":p} for p in expected],"resolve":{"nodes":[{"id":p,"features":features[p]} for p in expected]}}
        self.s05.validate_feature_closure(metadata(expected),expected)
        manifests={p:generator.load_toml(ROOT/"crates"/p/"Cargo.toml") for p in expected}
        manifests["pi-ai"]["features"]["catalog-openai"]=["dep:pi-ai-providers"]
        broken=self.s05.local_feature_closure(ROOT,"pi-ai",["catalog-openai"],manifests)
        for p in expected:broken.setdefault(p,[])
        with self.assertRaises(self.s05.ClosureError):self.s05.validate_feature_closure(metadata(broken),expected)
        # Supplying the backend feature independently cannot replace the facade feature.
        repaired=copy.deepcopy(expected);repaired["pi-ai"]=[]
        with self.assertRaises(self.s05.ClosureError):self.s05.validate_feature_closure(metadata(repaired),expected)
        self.s05.validate_feature_closure(metadata(expected),expected)

    def test_s05_source_execution_requires_exact_json_or_approved_check_output(self):
        source=next(s for s in self.document["sources"] if s["mode"]=="json")
        expected=(ROOT/source["expected"]).read_text()
        result=mock.Mock(returncode=0,stdout=expected,stderr="")
        upstream_root = self.s05.resolve_upstream()
        expected_environment = {"PATH": "/usr/bin:/bin", "NODE_NO_WARNINGS": "1",
                                "PIRUA_UPSTREAM_ROOT": str(upstream_root)}
        # Keep this source-command mock independent of the resolver's Git calls.
        resolver_module = sys.modules[self.s05.upstream_environment.__module__]
        with mock.patch.object(resolver_module, "resolve_upstream", return_value=upstream_root):
            with mock.patch.object(self.s05.subprocess,"run",return_value=result) as run:
                self.s05.execute_sources([source])
                self.assertEqual(run.call_args.kwargs["env"], expected_environment)
            for stdout in ("success", "{}", "[]", ""):
                with mock.patch.object(self.s05.subprocess,"run",return_value=mock.Mock(returncode=0,stdout=stdout,stderr="")):
                    with self.assertRaises(self.s05.ClosureError):self.s05.execute_sources([source])
            altered=copy.deepcopy(source);altered["executable"]="sh"
            with mock.patch.object(self.s05.subprocess,"run") as run:
                with self.assertRaises(self.s05.ClosureError):self.s05.execute_sources([altered])
                run.assert_not_called()
            checked=next(s for s in self.document["sources"] if s["mode"]=="check" and s["stdout"])
            with mock.patch.object(self.s05.subprocess,"run",return_value=mock.Mock(returncode=0,stdout="arbitrary success\n",stderr="")):
                with self.assertRaises(self.s05.ClosureError):self.s05.execute_sources([checked])



class S05ConstantValueCompilerTests(unittest.TestCase):
    """Actual compiler controls, separate from the pure policy-review test class."""
    def test_s05_real_constant_probes_reject_wrong_values_then_restore(self):
        import os
        import subprocess
        import s05_export_closure as s05
        names = ("ANTHROPIC_API_KEY_ENV", "ANTHROPIC_AUTH_TOKEN_ENV", "ANTHROPIC_OAUTH_TOKEN_ENV")
        probes = []
        for name in names:
            policy = s05.SYMBOL_POLICY["packages/ai/src/env-api-keys.ts:" + name]
            probes.append((policy["owningTarget"], policy["owningProbe"]))
            probes += [(r["routeTarget"], r["routeProbe"]) for r in policy["occurrences"]]
        self.assertEqual(len(probes), 6)
        # Exact approved bodies/real referenced constants, with all names resolved.
        positive = "\n".join("mod p" + str(i) + "{" + body.replace("$target", target) + "}" for i, (target, body) in enumerate(probes)) + "\nfn main(){}\n"
        with tempfile.TemporaryDirectory(prefix="pi-s05-constant-values-") as directory:
            project = Path(directory)
            (project / "src").mkdir()
            manifest = '[package]\nname="s05-constant-value-probe"\nversion="0.0.0"\nedition="2024"\n[workspace]\n[dependencies]\n'
            for package, features in (("pi-ai", ["compat"]), ("pi-ai-providers", [])):
                manifest += package + "={path=" + json.dumps(str(ROOT / "crates" / package)) + ",default-features=false,features=" + json.dumps(features) + "}\n"
            (project / "Cargo.toml").write_text(manifest)
            environment = {**os.environ, "CARGO_NET_OFFLINE": "true", "CARGO_TARGET_DIR": str(ROOT / "target/ai-conformance-probe"), "CARGO_PROFILE_DEV_DEBUG": "0", "CARGO_PROFILE_TEST_DEBUG": "0", "CARGO_BUILD_JOBS": "2"}
            def check(source):
                (project / "src/main.rs").write_text(source)
                return subprocess.run(["cargo", "check", "--offline", "--manifest-path", str(project / "Cargo.toml")], cwd=ROOT, env=environment, capture_output=True, text=True)
            initial = check(positive)
            self.assertEqual(initial.returncode, 0, initial.stderr)
            print("S05_CONST_VALUES_POSITIVE: six real constants resolved and evaluated")
            for name in names:
                expected = 'b"' + name.removesuffix("_ENV") + '"'
                wrong = 'b"' + name + '"'
                self.assertIn(expected, positive)
                # Change only the expected VALUE, after positive type/name admission.
                # The candidate-policy guard is intentionally not the negative target.
                negative = check(positive.replace(expected, wrong, 1))
                self.assertNotEqual(negative.returncode, 0)
                self.assertIn("E0080", negative.stderr)
                self.assertIn("S05 constant value mismatch", negative.stderr)
                self.assertNotIn("cannot find", negative.stderr)
                self.assertNotIn("unresolved import", negative.stderr)
                print("EXPECTED_S05_CONST_VALUE_FAILURE:", name)
                print(negative.stderr)
            restored = check(positive)
            self.assertEqual(restored.returncode, 0, restored.stderr)
            print("S05_CONST_VALUES_RESTORED: all six positive assertions evaluated")


if __name__ == "__main__":
    unittest.main()
