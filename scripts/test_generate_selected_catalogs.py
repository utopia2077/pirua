#!/usr/bin/env python3
"""Offline causal controls for exact captured input and selected catalog projection."""
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import generate_selected_catalogs as generator


class SelectedCatalogTests(unittest.TestCase):
    def test_complete_pinned_generators_reproduce_identical_checked_in_outputs_twice(self):
        with tempfile.TemporaryDirectory() as temporary:
            first, second = Path(temporary) / "first", Path(temporary) / "second"
            generator.generate(first)
            generator.generate(second)
            generator.compare(first, second)
            generator.compare(first, ROOT / generator.CATALOG / "generated")
            proof = json.loads((first / "provenance.json").read_bytes())
            self.assertEqual(sum(proof["textCounts"].values()), 450)
            self.assertEqual((proof["imageCount"], proof["historicalImageCount"]), (52, 50))
            self.assertEqual(len(proof["executions"]["text"]["calls"]), 4)
            self.assertEqual(len(proof["executions"]["images"]["calls"]), 1)

    def test_changed_input_bytes_and_rehashed_capture_or_source_metadata_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            shutil.copytree(ROOT / generator.CATALOG / "inputs", root / generator.CATALOG / "inputs")
            for name in ("capture.json", "source-manifest.json"):
                shutil.copyfile(ROOT / generator.CATALOG / name, root / generator.CATALOG / name)
            generator.verify_inputs(root)
            feed = root / generator.CATALOG / "inputs/nvidia.json"
            original = feed.read_bytes()
            feed.write_bytes(original + b" ")
            with self.assertRaisesRegex(generator.CatalogError, "bytes/hash mismatch"):
                generator.verify_inputs(root)
            capture_path = root / generator.CATALOG / "capture.json"
            capture = json.loads(capture_path.read_bytes())
            next(item for item in capture["feeds"] if item["name"] == "nvidia")["sha256"] = generator.digest(feed.read_bytes())
            capture_path.write_text(json.dumps(capture))
            with self.assertRaisesRegex(generator.CatalogError, "bytes/hash mismatch: capture.json"):
                generator.verify_inputs(root)
            shutil.copyfile(ROOT / generator.CATALOG / "capture.json", capture_path)
            feed.write_bytes(original)
            source_path = root / generator.CATALOG / "source-manifest.json"
            source_path.write_bytes(source_path.read_bytes() + b" ")
            with self.assertRaisesRegex(generator.CatalogError, "bytes/hash mismatch: source-manifest.json"):
                generator.verify_inputs(root)

    def test_selected_entries_values_order_and_provenance_cannot_silently_change(self):
        original = ROOT / generator.CATALOG / "generated"
        for mutation in ("excluded", "missing", "vendor-filter", "reorder", "provenance"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as temporary:
                output = Path(temporary) / "generated"
                shutil.copytree(original, output)
                if mutation == "excluded":
                    (output / "text/amazon-bedrock.json").write_text("[]")
                elif mutation == "missing":
                    (output / "text/google.json").unlink()
                elif mutation in {"vendor-filter", "reorder"}:
                    path = output / "text/openrouter.json"
                    models = json.loads(path.read_bytes())
                    if mutation == "vendor-filter":
                        models = [model for model in models if model["id"].startswith("openai/")]
                    else:
                        models.reverse()
                    path.write_text(json.dumps(models))
                else:
                    path = output / "provenance.json"
                    proof = json.loads(path.read_bytes())
                    proof["clock"] += 1
                    path.write_text(json.dumps(proof))
                if mutation != "provenance":
                    with self.assertRaisesRegex(generator.CatalogError, "selected"):
                        generator.validate_selection(output)
                with self.assertRaisesRegex(generator.CatalogError, "stale catalog"):
                    generator.compare(original, output)

    def test_offline_driver_rejects_undeclared_url_and_request_options(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            feed = root / "feed.json"
            feed.write_text("{}")
            config = root / "config.json"
            config.write_text(json.dumps({"clock": generator.CLOCK, "trace": str(root / "trace.json"),
                "feeds": [{"url": "https://declared.invalid", "path": str(feed)}]}))
            for expression in ('await fetch("https://undeclared.invalid")',
                               'await fetch("https://declared.invalid", {headers:{authorization:"synthetic"}})'):
                result = subprocess.run(["node", "--import", str(ROOT / generator.CATALOG / "offline-preload.mjs"),
                    "--input-type=module", "-e", expression], env={**generator.ENV, "PI_CATALOG_CONFIG": str(config)},
                    text=True, capture_output=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Undeclared catalog request", result.stderr)
                self.assertEqual(json.loads((root / "trace.json").read_bytes())["calls"], [])


if __name__ == "__main__":
    unittest.main()
