#!/usr/bin/env python3
"""Generate revision-bound projections used by upstream-map v2 validation."""

from __future__ import annotations

import argparse
import hashlib
import json
import posixpath
import re
import sys
from pathlib import Path

import validate_docs as validator


try:
    from .upstream import resolve_upstream, upstream_environment
except ImportError:
    from upstream import resolve_upstream, upstream_environment

ROOT = Path(__file__).resolve().parents[1]
MAP_PATH = ROOT / "upstream-map.toml"
TOPOLOGY_PATH = ROOT / "workspace-topology.toml"


def quoted(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def manifest(kind: str, commit: str, body: str) -> str:
    digest = hashlib.sha256(body.encode()).hexdigest()
    return (
        "manifest_version = 1\n"
        f"kind = {quoted(kind)}\n"
        f"source_commit = {quoted(commit)}\n"
        f"content_digest = {quoted('sha256:' + digest)}\n\n"
        + body
    )


def route_manifest(commit: str, entries: list[validator.RoutedEntry]) -> str:
    chunks: list[str] = []
    for entry in sorted(entries, key=lambda item: (item.watch, item.path)):
        chunks.extend(
            [
                "[[entry]]",
                f"watch = {quoted(entry.watch)}",
                f"path = {quoted(entry.path)}",
                f"blob_oid = {quoted(entry.oid)}",
                f"mode = {quoted(entry.mode)}",
                f"route = {quoted(entry.route)}",
                f"surface = {quoted(entry.surface)}",
                f"role = {quoted(entry.role)}",
                "",
            ]
        )
    return manifest("route-inventory", commit, "\n".join(chunks))


def test_manifest(
    commit: str,
    checkout: Path,
    entries: list[validator.RoutedEntry],
    selector_surfaces: dict[str, set[str]],
) -> str:
    tests = [
        entry
        for entry in entries
        if "/test/" in entry.path
        or "/tests/" in entry.path
        or entry.path.endswith((".test.ts", ".spec.ts"))
    ]
    entries_by_path = {entry.path: entry for entry in entries}
    chunks: list[str] = []
    for entry in sorted(tests, key=lambda item: item.path):
        evidence = infer_test_surfaces(
            checkout,
            commit,
            entry.path,
            entries_by_path,
            selector_surfaces,
        )
        chunks.extend(
            [
                "[[test]]",
                f"path = {quoted(entry.path)}",
                f"blob_oid = {quoted(entry.oid)}",
                f"route_surface = {quoted(entry.surface)}",
                "evidence_surfaces = ["
                + ", ".join(quoted(surface) for surface in sorted(evidence))
                + "]",
                f"role = {quoted(entry.role)}",
                "",
            ]
        )
    return manifest("test-inventory", commit, "\n".join(chunks))


def resolve_source_path(from_path: str, specifier: str, paths: set[str]) -> str | None:
    if not specifier.startswith("."):
        return None
    base = posixpath.normpath(posixpath.join(posixpath.dirname(from_path), specifier))
    candidates = [base]
    if base.endswith(".js"):
        candidates.append(base[:-3] + ".ts")
    if base.endswith(".d.ts"):
        candidates.append(base)
    if not base.endswith((".ts", ".tsx", ".json")):
        candidates.extend([base + ".ts", base + ".tsx", base + "/index.ts"])
    return next((candidate for candidate in candidates if candidate in paths), None)


def infer_test_surfaces(
    checkout: Path,
    commit: str,
    path: str,
    entries_by_path: dict[str, validator.RoutedEntry],
    selector_surfaces: dict[str, set[str]],
    active: set[str] | None = None,
) -> set[str]:
    active = set() if active is None else set(active)
    if path in active:
        return set()
    active.add(path)
    entry = entries_by_path[path]
    generic_surfaces = {
        "ai.tests",
        "coding.test-evidence",
        "agent.public-and-tests",
        "coding.agent-session-layout",
    }
    try:
        text = validator.git_blob(checkout, commit, path).decode("utf-8")
    except (RuntimeError, UnicodeDecodeError):
        text = ""
    specifiers = set(
        re.findall(r'(?:from\s+|import\s*\(\s*)["\']([^"\']+)["\']', text)
    )
    surfaces: set[str] = set()
    all_paths = set(entries_by_path)
    for specifier in specifiers:
        target = resolve_source_path(path, specifier, all_paths)
        if target is None:
            continue
        target_entry = entries_by_path[target]
        if target_entry.surface in generic_surfaces or "/test/" in target:
            surfaces.update(
                infer_test_surfaces(
                    checkout,
                    commit,
                    target,
                    entries_by_path,
                    selector_surfaces,
                    active,
                )
            )
        else:
            surfaces.update(selector_surfaces.get(target, {target_entry.surface}))
    if surfaces:
        return surfaces

    lowered = path.lower()
    if "/coding-agent/" in lowered:
        if any(
            token in lowered
            for token in (
                "tui",
                "theme",
                "clipboard",
                "keybinding",
                "footer",
                "selector",
                "package-manager",
                "package-distribution",
                "interactive-mode",
                "rpc",
                "export-html",
                "trust-",
                "version-check",
            )
        ):
            return {"coding.package-host"}
        if any(token in lowered for token in ("model", "credential", "auth-", "catalog", "radius")):
            return {"coding.models"}
        if any(
            token in lowered
            for token in ("tool", "bash", "edit", "find", "grep", "powershell", "file-mutation")
        ):
            return {"coding.tools"}
        if "session-manager" in lowered or "session-file" in lowered:
            return {"coding.legacy-import"}
        return {"coding.composition"}
    if "/ai/" in lowered:
        if "faux" in lowered:
            return {"ai.faux"}
        if any(token in lowered for token in ("oauth", "auth", "credential", "env-api")):
            return {"ai.oauth-images"}
        if "image" in lowered:
            return {"ai.oauth-images"}
        if any(token in lowered for token in ("catalog", "model-data", "generate-model")):
            return {"ai.catalog.runtime"}
        if any(token in lowered for token in ("retry", "proxy", "fetch-option")):
            return {"ai.transport-utils"}
        if any(
            token in lowered
            for token in (
                "anthropic",
                "openai",
                "google",
                "bedrock",
                "mistral",
                "cloudflare",
                "copilot",
                "openrouter",
                "pi-messages",
                "sampling-options",
            )
        ):
            return {"ai.api-adapters"}
        return {"ai.domain"}
    return selector_surfaces.get(path, {entry.surface})


def selector_surfaces_by_path(map_data: dict[str, object]) -> dict[str, set[str]]:
    surfaces: dict[str, set[str]] = {}
    for selector in map_data.get("selector", []):
        path = selector.get("path")
        surface = selector.get("surface")
        if isinstance(path, str) and isinstance(surface, str):
            surfaces.setdefault(path, set()).add(surface)
    return surfaces


def merge_export_symbol(
    symbols: dict[str, tuple[str, str, int, str]],
    name: str,
    kind: str,
    origin: str,
    origin_line: int,
    origin_declaration: str,
) -> None:
    existing = symbols.get(name)
    if existing is None:
        symbols[name] = (kind, origin, origin_line, origin_declaration)
        return
    existing_kind, existing_origin, existing_line, existing_declaration = existing
    if existing_kind == kind:
        merged_kind = kind
    elif "type-and-value" in {existing_kind, kind} or {existing_kind, kind} == {"type", "value"}:
        merged_kind = "type-and-value"
    else:
        merged_kind = kind
    if existing_origin != origin:
        raise RuntimeError(
            f"ambiguous export origin for {name}: {existing_origin}, {origin}"
        )
    locations = sorted(
        {
            (line, declaration)
            for line, declaration in (
                (existing_line, existing_declaration),
                (origin_line, origin_declaration),
            )
            if line > 0 and declaration
        }
    )
    if locations:
        merged_line, merged_declaration = locations[0]
    else:
        merged_line, merged_declaration = 0, ""
    symbols[name] = (
        merged_kind,
        origin,
        merged_line,
        merged_declaration,
    )


def declaration_location(text: str, offset: int) -> tuple[int, str]:
    line = text.count("\n", 0, offset) + 1
    declaration = text.splitlines()[line - 1].strip()
    return line, declaration


def local_declaration(
    text: str,
    name: str,
) -> tuple[str, int, str] | None:
    pattern = re.compile(
        r"(?:export\s+)?(?:declare\s+)?(?:async\s+)?"
        r"(interface|type|class|function|const|let|var)\s*\*?\s+"
        + re.escape(name)
        + r"\b"
    )
    match = pattern.search(text)
    if match is None:
        return None
    kind = {
        "interface": "type",
        "type": "type",
        "class": "type-and-value",
    }.get(match.group(1), "value")
    line, declaration = declaration_location(text, match.start())
    return kind, line, declaration


def exported_symbols(
    checkout: Path,
    commit: str,
    path: str,
    paths: set[str],
    cache: dict[str, dict[str, tuple[str, str, int, str]]],
    active: set[str] | None = None,
) -> dict[str, tuple[str, str, int, str]]:
    if path in cache:
        return cache[path]
    active = set() if active is None else set(active)
    if path in active:
        return {}
    active.add(path)
    text = validator.git_blob(checkout, commit, path).decode()
    symbols: dict[str, tuple[str, str, int, str]] = {}

    for match in re.finditer(r'export\s+\*\s+from\s+["\']([^"\']+)["\']\s*;', text):
        target = resolve_source_path(path, match.group(1), paths)
        if target is None:
            continue
        for name, (kind, origin, origin_line, origin_declaration) in exported_symbols(
            checkout, commit, target, paths, cache, active
        ).items():
            if name != "default":
                merge_export_symbol(
                    symbols,
                    name,
                    kind,
                    origin,
                    origin_line,
                    origin_declaration,
                )

    named_pattern = re.compile(
        r'export\s+(type\s+)?\{(.*?)\}\s*(?:from\s+["\']([^"\']+)["\'])?\s*;',
        re.DOTALL,
    )
    for match in named_pattern.finditer(text):
        block_is_type = bool(match.group(1))
        target = resolve_source_path(path, match.group(3), paths) if match.group(3) else None
        target_symbols = (
            exported_symbols(checkout, commit, target, paths, cache, active)
            if target is not None
            else {}
        )
        for item in match.group(2).split(","):
            item = re.sub(r"/\*.*?\*/", "", item, flags=re.DOTALL).strip()
            if not item:
                continue
            item_is_type = block_is_type or item.startswith("type ")
            if item.startswith("type "):
                item = item[5:].strip()
            parts = re.split(r"\s+as\s+", item)
            source_name = parts[0].strip()
            export_name = parts[-1].strip()
            source_specifier = match.group(3)
            target_symbol = target_symbols.get(source_name)
            if target_symbol is not None:
                target_kind, origin, origin_line, origin_declaration = target_symbol
            elif target is not None:
                target_text = validator.git_blob(checkout, commit, target).decode()
                location = local_declaration(target_text, source_name)
                if location is None:
                    target_kind, origin_line, origin_declaration = (
                        "type" if item_is_type else "value",
                        0,
                        "",
                    )
                else:
                    target_kind, origin_line, origin_declaration = location
                origin = target
            elif source_specifier is None:
                location = local_declaration(text, source_name)
                if location is None:
                    target_kind = "type" if item_is_type else "value"
                    origin_line, origin_declaration = declaration_location(
                        text,
                        match.start(),
                    )
                else:
                    target_kind, origin_line, origin_declaration = location
                origin = path
            else:
                target_kind, origin, origin_line, origin_declaration = (
                    "type" if item_is_type else "value",
                    source_specifier,
                    0,
                    "",
                )
            merge_export_symbol(
                symbols,
                export_name,
                "type" if item_is_type else target_kind,
                origin,
                origin_line,
                origin_declaration,
            )

    declaration_pattern = re.compile(
        r"export\s+(?:declare\s+)?(?:async\s+)?"
        r"(interface|type|class|function|const|let|var)\s*\*?\s+([A-Za-z_$][\w$]*)"
    )
    for match in declaration_pattern.finditer(text):
        kind, name = match.groups()
        symbol_kind = {
            "interface": "type",
            "type": "type",
            "class": "type-and-value",
        }.get(kind, "value")
        line, declaration = declaration_location(text, match.start())
        merge_export_symbol(
            symbols,
            name,
            symbol_kind,
            path,
            line,
            declaration,
        )
    default_match = re.search(r"\bexport\s+default\b", text)
    if default_match is not None:
        line, declaration = declaration_location(text, default_match.start())
        merge_export_symbol(
            symbols,
            "default",
            "value",
            path,
            line,
            declaration,
        )
    cache[path] = symbols
    return symbols


def package_export_sources(
    exports: dict[str, object],
    package_root: str,
    paths: set[str],
) -> list[tuple[str, str]]:
    resolved: list[tuple[str, str]] = []
    for subpath, target_value in exports.items():
        if subpath == "./package.json":
            continue
        target = target_value.get("import") if isinstance(target_value, dict) else target_value
        if not isinstance(target, str) or not target.startswith("./dist/"):
            continue
        source_pattern = package_root + "/src/" + target[len("./dist/"):]
        if source_pattern.endswith(".js"):
            source_pattern = source_pattern[:-3] + ".ts"
        regex = validator.compile_glob(source_pattern)
        matches = sorted(path for path in paths if regex.match(path))
        for source in matches:
            if "*" in subpath:
                stem = Path(source).stem
                concrete_subpath = subpath.replace("*", stem)
            else:
                concrete_subpath = subpath
            resolved.append((concrete_subpath, source))
    return resolved


def exports_manifest(
    commit: str,
    checkout: Path,
    package_root: str,
    package_kind: str,
) -> str:
    package_path = f"{package_root}/package.json"
    barrel_path = f"{package_root}/src/index.ts"
    package_blob = validator.git_blob(checkout, commit, package_path)
    barrel_blob = validator.git_blob(checkout, commit, barrel_path)
    package_oid = validator.run_git(checkout, ["rev-parse", f"{commit}:{package_path}"])
    barrel_oid = validator.run_git(checkout, ["rev-parse", f"{commit}:{barrel_path}"])
    assert isinstance(package_oid, str)
    assert isinstance(barrel_oid, str)
    package_data = json.loads(package_blob)
    tree_paths = {
        entry.path
        for entry in validator.git_tree(checkout, commit, package_root)
    }
    exports_json = json.dumps(
        package_data.get("exports", {}),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    chunks = [
        f"package_path = {quoted(package_path)}",
        f"package_blob_oid = {quoted(package_oid.strip())}",
        f"exports_json = {quoted(exports_json)}",
        f"barrel_path = {quoted(barrel_path)}",
        f"barrel_blob_oid = {quoted(barrel_oid.strip())}",
        "",
    ]
    for line_number, line in enumerate(barrel_blob.decode().splitlines(), start=1):
        stripped = line.strip()
        if not stripped.startswith("export "):
            continue
        chunks.extend(
            [
                "[[barrel_export]]",
                f"line = {line_number}",
                f"declaration = {quoted(stripped)}",
                "",
            ]
        )
    symbol_cache: dict[str, dict[str, tuple[str, str, int, str]]] = {}
    for subpath, source_path in package_export_sources(
        package_data.get("exports", {}), package_root, tree_paths
    ):
        source_oid = validator.run_git(
            checkout, ["rev-parse", f"{commit}:{source_path}"]
        )
        assert isinstance(source_oid, str)
        symbols = exported_symbols(
            checkout,
            commit,
            source_path,
            tree_paths,
            symbol_cache,
        )
        for name, (kind, origin, origin_line, origin_declaration) in sorted(symbols.items()):
            chunks.extend(
                [
                    "[[symbol]]",
                    f"subpath = {quoted(subpath)}",
                    f"name = {quoted(name)}",
                    f"kind = {quoted(kind)}",
                    f"source_path = {quoted(source_path)}",
                    f"source_blob_oid = {quoted(source_oid.strip())}",
                    f"origin = {quoted(origin)}",
                    f"origin_line = {origin_line}",
                    f"origin_declaration = {quoted(origin_declaration)}",
                    "",
                ]
            )
    return manifest(f"{package_kind}-public-exports", commit, "\n".join(chunks))


def providers_manifest(commit: str, checkout: Path) -> str:
    all_path = "packages/ai/src/providers/all.ts"
    text = validator.git_blob(checkout, commit, all_path).decode()
    imports = re.findall(
        r'import \{ ([A-Za-z0-9_]+Provider) \} from "\./([^".]+)\.ts";',
        text,
    )
    text_factories = set(
        re.findall(r"^\s*([A-Za-z0-9_]+Provider)\(\),\s*$", text, re.MULTILINE)
    )
    image_block = re.search(
        r"builtinImagesProviders\(\): ImagesProvider\[\] \{\s*return \[(.*?)\];",
        text,
        re.DOTALL,
    )
    image_factories = set(re.findall(r"([A-Za-z0-9_]+Provider)\(\)", image_block.group(1))) if image_block else set()
    chunks = [f"source_path = {quoted(all_path)}", ""]
    for symbol, module in sorted(imports, key=lambda item: item[1]):
        if symbol not in text_factories and symbol not in image_factories:
            continue
        module_path = f"packages/ai/src/providers/{module}.ts"
        module_text = validator.git_blob(checkout, commit, module_path).decode()
        factory_start = re.search(rf"export function {re.escape(symbol)}\b", module_text)
        if factory_start is None:
            raise RuntimeError(f"cannot locate provider factory {symbol} in {module_path}")
        next_factory = re.search(r"\nexport function \w+Provider\b", module_text[factory_start.end():])
        factory_end = (
            factory_start.end() + next_factory.start()
            if next_factory is not None
            else len(module_text)
        )
        factory_text = module_text[factory_start.start():factory_end]
        provider_id_match = re.search(
            r"create(?:Images)?Provider(?:<[^>]+>)?\s*\(\s*\{.*?\bid\s*:\s*\"([^\"]+)\"",
            factory_text,
            re.DOTALL,
        )
        if provider_id_match is None:
            provider_id_match = re.search(
                r"\bconst\s+id\s*=.*?\?\?\s*\"([^\"]+)\"",
                factory_text,
            )
        if provider_id_match is None:
            raise RuntimeError(f"cannot resolve provider id for {symbol} in {module_path}")
        provider_ids = [provider_id_match.group(1)]
        api_ids = set(re.findall(r'Provider<"([^"]+)">', factory_text))
        generic_name = re.search(r"Provider<([A-Za-z0-9_]+)>", factory_text)
        if generic_name is not None:
            alias = re.search(
                rf"type\s+{re.escape(generic_name.group(1))}\s*=\s*(.*?);",
                module_text,
                re.DOTALL,
            )
            if alias is not None:
                api_ids.update(re.findall(r'"([a-z0-9-]+)"', alias.group(1)))
        api_object = re.search(r"\bapi\s*:\s*\{(.*?)\}\s*,", factory_text, re.DOTALL)
        if api_object is not None:
            api_ids.update(re.findall(r'"([a-z0-9-]+)"\s*:', api_object.group(1)))
        api_ids = sorted(api_ids)
        catalog_match = re.search(r"models:\s*Object\.values\(([^)]+)\)", factory_text)
        oauth_flows = sorted(set(re.findall(r"\b(load[A-Za-z0-9_]*OAuth)\b", module_text)))
        chunks.extend(
            [
                "[[provider]]",
                f"module = {quoted(module)}",
                f"factory_symbol = {quoted(symbol)}",
                f"kind = {quoted('image' if symbol in image_factories else 'text')}",
                f"dynamic = {'true' if catalog_match is None else 'false'}",
                "provider_ids = [" + ", ".join(quoted(value) for value in provider_ids) + "]",
                "api_ids = [" + ", ".join(quoted(value) for value in api_ids) + "]",
                f"has_oauth = {'true' if 'oauth:' in factory_text else 'false'}",
                "oauth_flows = [" + ", ".join(quoted(value) for value in oauth_flows) + "]",
                f"catalog_symbol = {quoted(catalog_match.group(1) if catalog_match else '')}",
                "",
            ]
        )
    faux_path = "packages/ai/src/providers/faux.ts"
    faux_text = validator.git_blob(checkout, commit, faux_path).decode()
    faux_provider = re.search(r'const DEFAULT_PROVIDER = "([^"]+)"', faux_text)
    faux_api = re.search(r'const DEFAULT_API = "([^"]+)"', faux_text)
    if faux_provider is None or faux_api is None or "export function fauxProvider" not in faux_text:
        raise RuntimeError("cannot resolve public fauxProvider inventory")
    chunks.extend(
        [
            "[[provider]]",
            'module = "faux"',
            'factory_symbol = "fauxProvider"',
            'kind = "faux-testkit"',
            "dynamic = true",
            f"provider_ids = [{quoted(faux_provider.group(1))}]",
            f"api_ids = [{quoted(faux_api.group(1))}]",
            "has_oauth = false",
            "oauth_flows = []",
            'catalog_symbol = ""',
            "",
        ]
    )
    return manifest("provider-inventory", commit, "\n".join(chunks))


def catalog_inputs_manifest(
    commit: str,
    checkout: Path,
    entries: list[validator.RoutedEntry],
) -> str:
    entries_by_path = {entry.path: entry for entry in entries}
    all_paths = set(entries_by_path)
    pending = [
        entry.path
        for entry in entries
        if entry.surface == "ai.catalog.generator"
    ]
    visited: set[str] = set()
    while pending:
        path = pending.pop()
        if path in visited:
            continue
        visited.add(path)
        try:
            text = validator.git_blob(checkout, commit, path).decode("utf-8")
        except UnicodeDecodeError:
            continue
        for specifier in re.findall(r'from\s+["\']([^"\']+)["\']', text):
            target = resolve_source_path(path, specifier, all_paths)
            if target is not None and target not in visited:
                pending.append(target)
    chunks = ['provenance_status = "missing-intake-snapshot"', ""]
    for path in sorted(visited):
        entry = entries_by_path[path]
        chunks.extend(
            [
                "[[input]]",
                f"path = {quoted(path)}",
                f"blob_oid = {quoted(entry.oid)}",
                f"surface = {quoted(entry.surface)}",
                f"role = {quoted('generator-root' if entry.surface == 'ai.catalog.generator' else 'imported-semantic-input')}",
                "",
            ]
        )
    return manifest("catalog-inputs", commit, "\n".join(chunks))


def heading_manifest(commit: str, checkout: Path) -> str:
    path = "packages/agent/docs/harness.md"
    blob = validator.git_blob(checkout, commit, path)
    chunks = [f"source_path = {quoted(path)}", ""]
    lines = blob.decode().splitlines()
    for index, level, text in validator.markdown_headings(lines):
        chunks.extend(
            [
                "[[heading]]",
                f"line = {index + 1}",
                f"level = {level}",
                f"text = {quoted(text)}",
                "",
            ]
        )
    return manifest("markdown-headings", commit, "\n".join(chunks))


def expected_outputs() -> dict[Path, str]:
    topology = validator.load_toml(TOPOLOGY_PATH)
    crates = set(topology["production_crates"])
    errors: list[str] = []
    _, entries, map_data = validator.validate_upstream_map(
        MAP_PATH,
        crates,
        errors,
        permit_pending_digest=False,
    )
    if errors:
        raise RuntimeError("; ".join(errors))
    upstream = map_data["upstream"]
    checkout = resolve_upstream(upstream["checkout"])
    commit = upstream["commit"]
    return {
        ROOT / "upstream/manifests/routes.toml": route_manifest(commit, entries),
        ROOT / "upstream/manifests/tests.toml": test_manifest(
            commit,
            checkout,
            entries,
            selector_surfaces_by_path(map_data),
        ),
        ROOT / "upstream/manifests/ai-exports.toml": exports_manifest(
            commit, checkout, "packages/ai", "ai"
        ),
        ROOT / "upstream/manifests/agent-exports.toml": exports_manifest(
            commit, checkout, "packages/agent", "agent"
        ),
        ROOT / "upstream/manifests/providers.toml": providers_manifest(commit, checkout),
        ROOT / "upstream/manifests/catalog-inputs.toml": catalog_inputs_manifest(
            commit, checkout, entries
        ),
        ROOT / "upstream/manifests/harness-headings.toml": heading_manifest(commit, checkout),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    try:
        outputs = expected_outputs()
    except RuntimeError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    stale: list[str] = []
    for path, content in outputs.items():
        if args.check:
            if not path.is_file() or path.read_text(encoding="utf-8") != content:
                stale.append(str(path.relative_to(ROOT)))
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    if stale:
        print("stale manifests: " + ", ".join(stale), file=sys.stderr)
        return 1
    if not args.check:
        print(f"generated {len(outputs)} upstream manifests")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
