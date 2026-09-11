import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
import catalog_cli


class CatalogCliTests(unittest.TestCase):
    def test_family_projection_preserves_all_source_arrays_and_provenance(self):
        generated = ROOT / "conformance/catalog/generated"
        for family, names, count in (("text", catalog_cli.accepted.TEXT, 450), ("images", ("openrouter",), 52)):
            raw = catalog_cli.family_artifact(generated, family)
            result = json.loads(raw)
            self.assertEqual([row["provider"] for row in result["catalogs"]], list(names))
            self.assertEqual(sum(len(row["models"]) for row in result["catalogs"]), count)
            self.assertEqual(result["provenance"], json.loads((generated / "provenance.json").read_bytes()))
            for row in result["catalogs"]:
                source = (generated / family / (row["provider"] + ".json")).read_bytes().strip()
                self.assertIn(source, raw)
            self.assertEqual(raw, catalog_cli.family_artifact(generated, family))

    def test_exclusive_creation_refuses_existing_file_and_symlink(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "output"
            catalog_cli.write_new(path, b"original")
            with self.assertRaises(FileExistsError):
                catalog_cli.write_new(path, b"replacement")
            self.assertEqual(path.read_bytes(), b"original")
            link = Path(temporary) / "link"
            link.symlink_to(Path(temporary) / "missing")
            with self.assertRaises(FileExistsError):
                catalog_cli.write_new(link, b"replacement")
            self.assertFalse((Path(temporary) / "missing").exists())

    def test_failed_flush_removes_the_old_identity_check_unlink_interval(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            target, displaced = root / "output", root / "displaced"
            original_open, original_lstat = Path.open, Path.lstat
            observations = []

            class FailedFlush:
                def __init__(self, file): self.file = file
                def __enter__(self): return self
                def __exit__(self, *args): self.file.close()
                def fileno(self): return self.file.fileno()
                def write(self, data): return self.file.write(data)
                def flush(self):
                    self.file.flush()
                    observations.append("actual-flush-failed")
                    raise OSError("controlled flush failure")

            def opened(path, *args, **kwargs):
                file = original_open(path, *args, **kwargs)
                return FailedFlush(file) if args == ("xb",) else file

            def measured_then_replaced(path, *args, **kwargs):
                measured = original_lstat(path, *args, **kwargs)
                if path == target:
                    observations.append("old-final-identity-measured")
                    path.rename(displaced)
                    with original_open(path, "wb") as file:
                        file.write(b"CONCURRENT OWNER DATA")
                return measured

            with patch.object(Path, "open", opened), patch.object(Path, "lstat", measured_then_replaced):
                with self.assertRaisesRegex(OSError, "controlled flush failure"):
                    catalog_cli.write_new(target, b"partial export")
            self.assertIn("actual-flush-failed", observations)
            if "old-final-identity-measured" in observations:
                # Exact reported interleaving: a measured final inode is replaced
                # before the old cleanup unlink. The old implementation loses it.
                self.assertTrue(target.exists(), "cleanup deleted concurrent replacement")
                self.assertEqual(target.read_bytes(), b"CONCURRENT OWNER DATA")
            else:
                # Corrected design never publishes or examines the final path on flush failure.
                self.assertFalse(target.exists())
                self.assertEqual(list(root.iterdir()), [])

    def test_write_and_flush_failures_never_publish_partial_final_artifacts(self):
        for failing_operation in ("write", "flush"):
            with self.subTest(operation=failing_operation), tempfile.TemporaryDirectory() as temporary:
                target = Path(temporary) / "output"
                original_open = Path.open
                observations = []

                class FailedOutput:
                    def __init__(self, file): self.file = file
                    def __enter__(self): return self
                    def __exit__(self, *args): self.file.close()
                    def fileno(self): return self.file.fileno()
                    def write(self, data):
                        self.file.write(data[:3])
                        if failing_operation == "write":
                            observations.append("write-failed")
                            raise OSError("controlled write failure")
                        return self.file.write(data[3:])
                    def flush(self):
                        self.file.flush()
                        observations.append("flush-failed")
                        raise OSError("controlled flush failure")

                def opened(path, *args, **kwargs):
                    file = original_open(path, *args, **kwargs)
                    return FailedOutput(file) if args == ("xb",) else file

                with patch.object(Path, "open", opened), patch.object(catalog_cli.os, "link") as publish:
                    with self.assertRaisesRegex(OSError, "controlled " + failing_operation + " failure"):
                        catalog_cli.write_new(target, b"complete export")
                    publish.assert_not_called()
                self.assertEqual(observations, [failing_operation + "-failed"])
                self.assertFalse(target.exists())
                self.assertEqual(list(Path(temporary).iterdir()), [])

    def test_complete_private_write_precedes_nonreplacing_publication_and_racing_creator_wins(self):
        for competitor in (False, True):
            with self.subTest(competitor=competitor), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                target = root / "output"
                original_link = os.link
                calls = []

                def publish(source, destination, **kwargs):
                    calls.append("publish")
                    self.assertEqual(Path(destination), target)
                    self.assertEqual(Path(source).read_bytes(), b"complete export")
                    self.assertFalse(target.exists())
                    if competitor:
                        target.write_bytes(b"CONCURRENT OWNER DATA")
                    return original_link(source, destination, **kwargs)

                with patch.object(catalog_cli.os, "link", publish):
                    if competitor:
                        with self.assertRaises(FileExistsError):
                            catalog_cli.write_new(target, b"complete export")
                    else:
                        catalog_cli.write_new(target, b"complete export")
                self.assertEqual(calls, ["publish"])
                self.assertEqual(target.read_bytes(), b"CONCURRENT OWNER DATA" if competitor else b"complete export")
                self.assertEqual(list(root.iterdir()), [target])

    def test_publication_failure_preserves_absence_and_cleans_only_private_storage(self):
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "output"
            calls = []

            def failed_publish(source, destination, **kwargs):
                self.assertEqual(Path(source).read_bytes(), b"complete export")
                self.assertEqual(Path(destination), target)
                self.assertFalse(target.exists())
                calls.append("actual-publication-failed")
                raise OSError("controlled publication failure")

            with patch.object(catalog_cli.os, "link", failed_publish):
                with self.assertRaisesRegex(OSError, "controlled publication failure"):
                    catalog_cli.write_new(target, b"complete export")
            self.assertEqual(calls, ["actual-publication-failed"])
            self.assertFalse(target.exists())
            self.assertEqual(list(Path(temporary).iterdir()), [])


if __name__ == "__main__":
    unittest.main()
