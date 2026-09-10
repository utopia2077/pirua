#!/usr/bin/env python3
"""Reproduce selected catalogs from a fixed public capture, without network access."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
CATALOG = Path("conformance/catalog")
PIN = "853a80d26c90a14c1886f0ebb8ffaae133ca2185"
CLOCK = 1788764386000
TEXT = ("openai", "google", "anthropic", "deepseek", "openai-codex", "openrouter")
TEXT_COUNTS = dict(zip(TEXT, (39, 22, 14, 3, 7, 365), strict=True))
URLS = {
    "models-dev": "https://models.dev/api.json",
    "openrouter-text": "https://openrouter.ai/api/v1/models",
    "vercel": "https://ai-gateway.vercel.sh/v1/models",
    "nvidia": "https://integrate.api.nvidia.com/v1/models",
    "openrouter-images": "https://openrouter.ai/api/v1/models?output_modalities=image",
}
CAPTURE_SHA256 = "0898cbffc0d5befc26b4580894872954faa355661be1097484b430dd96da0475"
SOURCES_SHA256 = "657fdb1217ed342982a63e7e77039a28df793378583fcb35aa39f5bb528d23f8"
ENV = {"PATH": "/usr/bin:/bin", "NODE_NO_WARNINGS": "1", "TZ": "UTC", "LC_ALL": "C.UTF-8"}


class CatalogError(ValueError):
    """A source, capture, selection or generated-value invariant changed."""


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def checked_bytes(path: Path, expected_hash: str, expected_length: int | None = None) -> bytes:
    data = path.read_bytes()
    if digest(data) != expected_hash or expected_length is not None and len(data) != expected_length:
        raise CatalogError("catalog bytes/hash mismatch: " + path.name)
    return data


def verify_inputs(root: Path = ROOT) -> tuple[dict, dict]:
    directory = root / CATALOG
    capture = json.loads(checked_bytes(directory / "capture.json", CAPTURE_SHA256))
    sources = json.loads(checked_bytes(directory / "source-manifest.json", SOURCES_SHA256))
    if capture["upstream"] != PIN or sources["upstream"] != PIN:
        raise CatalogError("catalog upstream identity changed")
    if {feed["name"]: feed["url"] for feed in capture["feeds"]} != URLS or len(capture["feeds"]) != 5:
        raise CatalogError("catalog capture source selection changed")
    for feed in capture["feeds"]:
        if feed["path"] != "inputs/" + feed["name"] + ".json" or feed["status"] != 200:
            raise CatalogError("catalog capture metadata changed")
        json.loads(checked_bytes(directory / feed["path"], feed["sha256"], feed["bytes"]))
    return capture, sources


def validate_selection(generated: Path) -> None:
    files = {path.name for path in (generated / "text").glob("*.json")}
    if files != {provider + ".json" for provider in TEXT}:
        raise CatalogError("catalog selected text entries changed")
    all_models = json.loads((generated / "source-text.json").read_bytes())
    grouped = json.loads((generated / "source-grouped.json").read_bytes())
    for provider in TEXT:
        models = json.loads((generated / "text" / (provider + ".json")).read_bytes())
        if len(models) != TEXT_COUNTS[provider] or models != list(all_models[provider].values()):
            raise CatalogError("catalog selected model values/order changed: " + provider)
        if any(model["provider"] != provider for model in models):
            raise CatalogError("catalog selected model provider changed")
        flattened = {key: value for group in grouped[provider].values() for key, value in group.items()}
        if flattened != all_models[provider]:
            raise CatalogError("catalog grouped/flat values differ")
    if {path.name for path in (generated / "images").glob("*.json")} != {"openrouter.json"}:
        raise CatalogError("catalog selected image entries changed")
    images = json.loads((generated / "images/openrouter.json").read_bytes())
    source_images = json.loads((generated / "source-images.json").read_bytes())
    if set(source_images) != {"openrouter"} or images != list(source_images["openrouter"].values()):
        raise CatalogError("catalog selected image values/order changed")
    if any(model["provider"] != "openrouter" or model["api"] != "openrouter-images" for model in images):
        raise CatalogError("catalog image namespace changed")


def run(command: list[str], *, environment: dict[str, str] = ENV) -> subprocess.CompletedProcess:
    result = subprocess.run(command, env=environment, text=True, capture_output=True)
    if result.returncode:
        raise CatalogError("offline catalog source execution failed:\n" + result.stdout + result.stderr)
    return result


def generate(destination: Path, root: Path = ROOT) -> None:
    capture, sources = verify_inputs(root)
    upstream = tomllib.loads((root / "upstream-map.toml").read_text())["upstream"]
    if upstream["commit"] != PIN:
        raise CatalogError("catalog upstream map changed")
    destination.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="pi-catalog-source-") as temporary:
        scratch = Path(temporary)
        source_root = scratch / "source"
        for entry in sources["sources"]:
            path = entry["source"]
            if not path.startswith("packages/ai/") or ".." in Path(path).parts:
                raise CatalogError("catalog source path changed")
            data = subprocess.check_output(["git", "-C", upstream["checkout"], "show", PIN + ":" + path])
            oid = subprocess.check_output(["git", "-C", upstream["checkout"], "rev-parse", PIN + ":" + path]).decode().strip()
            if digest(data) != entry["sha256"] or len(data) != entry["bytes"] or oid != entry["blobOid"]:
                raise CatalogError("pinned catalog source bytes changed")
            target = source_root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
        pinned_image = scratch / "historical-image.ts"
        shutil.copyfile(source_root / "packages/ai/src/image-models.generated.ts", pinned_image)
        traces = {}
        for kind, source, arguments, feeds in (
            ("text", "generate-models.ts", ["--strict", "--json-output", str(scratch / "text-output")], capture["feeds"][:4]),
            ("images", "generate-image-models.ts", ["--strict"], capture["feeds"][4:]),
        ):
            config = scratch / (kind + "-config.json")
            trace = scratch / (kind + "-trace.json")
            config.write_text(json.dumps({"clock": CLOCK, "trace": str(trace), "feeds": [
                {"url": feed["url"], "path": str(root / CATALOG / feed["path"])} for feed in feeds]}))
            run(["node", "--import", str(root / CATALOG / "offline-preload.mjs"),
                 str(source_root / "packages/ai/scripts" / source), *arguments],
                environment={**ENV, "PI_CATALOG_CONFIG": str(config)})
            traces[kind] = json.loads(trace.read_bytes())
            if traces[kind]["code"] != 0 or set(traces[kind]["calls"]) != {feed["url"] for feed in feeds}:
                raise CatalogError("catalog generator did not consume the exact declared inputs")
        run(["node", str(root / CATALOG / "project.mjs"), str(scratch / "text-output"),
             str(source_root), str(pinned_image), str(destination)])
    validate_selection(destination)
    provenance = {
        "schemaVersion": 1, "upstream": PIN,
        "claim": "reproducible-parity-for-dated-captured-inputs-not-historical-text-catalog",
        "clock": CLOCK, "captureSha256": CAPTURE_SHA256, "sourcesSha256": SOURCES_SHA256,
        "generatorSha256": digest((root / "scripts/generate_selected_catalogs.py").read_bytes()),
        "nodeBinarySha256": digest(Path("/usr/bin/node").read_bytes()),
        "drivers": {name: digest((root / CATALOG / name).read_bytes()) for name in ("offline-preload.mjs", "project.mjs")},
        "executions": traces, "textCounts": TEXT_COUNTS,
        "imageCount": len(json.loads((destination / "images/openrouter.json").read_bytes())),
        "historicalImageCount": len(json.loads((destination / "historical-images.json").read_bytes())["openrouter"]),
        "outputs": {str(path.relative_to(destination)): {"sha256": digest(path.read_bytes()), "bytes": path.stat().st_size}
                    for path in sorted(destination.rglob("*")) if path.is_file()},
    }
    (destination / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")


def compare(expected: Path, actual: Path) -> None:
    def contents(root: Path) -> dict[str, bytes]:
        return {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}
    if contents(expected) != contents(actual):
        raise CatalogError("stale catalog output/provenance/selection; regenerate from the approved capture")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        with tempfile.TemporaryDirectory(prefix="pi-selected-catalog-") as temporary:
            output = Path(temporary) / "generated"
            generate(output)
            target = ROOT / CATALOG / "generated"
            if args.check:
                compare(output, target)
            else:
                if target.exists():
                    shutil.rmtree(target)
                shutil.copytree(output, target)
        print("verified selected catalog: 450 text models; captured and historical image provenance kept separate")
        return 0
    except (CatalogError, OSError, ValueError, subprocess.CalledProcessError) as error:
        print(str(error))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
