#!/usr/bin/env python3
"""Join pinned source obligations to map owners and current proof definitions.

This is a read-only consistency check. Commands in the registry are inspected,
never executed; a reachable test definition is not a passing execution receipt.
The pinned reader, route/selector rules and projections are the existing oracles.
"""
from __future__ import annotations

import argparse
import ast
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import sys
import tomllib

try:
    from . import generate_ai_conformance as ai
    from . import generate_upstream_manifests as projections
    from . import validate_docs as validator
    from .upstream import resolve_upstream
except ImportError:
    import generate_ai_conformance as ai
    import generate_upstream_manifests as projections
    import validate_docs as validator
    from upstream import resolve_upstream

ROOT = Path(__file__).resolve().parents[1]
HARNESS = "packages/agent/docs/harness.md"
BRIDGE_IDS = (
    "single_turn.stop", "single_turn.transform_context",
    "tool.parallel_completion_source", "tool.length_truncated", "continue.existing_context",
)
CODING_IDS = (
    "model.credentials_catalog", "prompt.system_resources", "prompt.settings_snapshot",
    "tools.validate_progress_settle", "tools.coding_operations", "queue.steer_during_turn",
    "queue.follow_up_after_turn", "retry.overflow_precedence", "retry.provider_recovery",
    "abort.active_stream_cleanup", "compaction.context_projection", "branch.fork_atomic_publish",
    "restart.recover_session", "legacy_import.format3", "model_switch.next_run",
    "extension.participation_ordering", "lifecycle.builder_replace",
    "capability.required_prestart_failure",
)
LEGACY_STATUSES = (
    "source-confirmed", "source-confirmed", "source-confirmed", "test-unwitnessed",
    "test-unwitnessed", "test-unwitnessed", "source-confirmed", "test-unwitnessed",
    "source-confirmed", "test-unwitnessed", "source-confirmed", "test-unwitnessed",
    "test-unwitnessed", "non-applicable",
)
TEST_DEFINITION = re.compile(
    r"#\[(?:test|(?:[A-Za-z_][\w]*::)+test)(?:\([^\]]*\))?\]"
    r"\s*(?:#\[[^\]]*\]\s*)*(?:pub\s+)?(?:async\s+)?fn\s+(\w+)\s*\(",
    re.MULTILINE,
)


def active_tests(text: str) -> set[str]:
    """Declaration bookkeeping only; command applicability uses lexical items."""
    return {match.group(1) for match in TEST_DEFINITION.finditer(text)
            if not re.search(r"#\[ignore\b", match.group())}


class AssemblyError(ValueError):
    """A named inventory or evidence obligation cannot be resolved."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssemblyError(message)


def rust_tokens(text: str) -> list[tuple[str, int, int]]:
    """Tokenize delimiters without treating comments or literals as Rust items."""
    tokens, index = [], 0
    while index < len(text):
        start = index
        if text[index].isspace():
            index += 1
            continue
        if text.startswith("//", index):
            end = text.find("\n", index)
            index = len(text) if end < 0 else end
            continue
        if text.startswith("/*", index):
            index, depth = index + 2, 1
            while depth and index < len(text):
                if text.startswith("/*", index):
                    depth, index = depth + 1, index + 2
                elif text.startswith("*/", index):
                    depth, index = depth - 1, index + 2
                else:
                    index += 1
            require(depth == 0, "unterminated Rust proof comment")
            continue
        raw = re.compile(r'(?:br|r)(#*)"').match(text, index)
        if raw:
            marker = '"' + raw.group(1)
            end = text.find(marker, raw.end())
            require(end >= 0, "unterminated Rust proof raw string")
            index = end + len(marker)
        elif text[index] == '"':
            index += 1
            while index < len(text) and text[index] != '"':
                index += 2 if text[index] == "\\" else 1
            require(index < len(text), "unterminated Rust proof string")
            index += 1
        else:
            character = re.compile(r"'(?:[^'\\\n]|\\(?:u\{[0-9a-fA-F]+\}|x[0-9a-fA-F]{2}|.))'").match(text, index)
            word = re.compile(r"[A-Za-z_][A-Za-z_0-9]*").match(text, index)
            index += len(character.group()) if character else len(word.group()) if word else 1
        tokens.append((text[start:index], start, index))
    return tokens


def rust_test_inventory(path: Path, root: Path) -> tuple[list[dict], set[Path]]:
    """Read exact test items with lexical file/module/function attributes.

    This intentionally does not compile Rust. Unsupported conditions on a
    referenced item are rejected later. The one simple local ident/literal
    test-macro form used by the current adapter corpus is expanded as inert
    token substitution; other macro-generated names remain unresolved.
    """
    tests, sources = [], set()

    def read_file(source, modules, inherited, active, module_base=None):
        source = source.resolve()
        require(source.is_relative_to(root.resolve()) and source.is_file(), "missing public Rust test module")
        require(source not in active, f"cyclic Rust test module: {source.name}")
        sources.add(source)
        scan(source.read_text(encoding="utf-8"), source, modules, inherited,
             active | {source}, module_base or source.parent, {})

    def scan(text, source, modules, inherited, active, module_base, macros):
        tokens = rust_tokens(text)
        pairs, stack = {}, []
        opening = {"(": ")", "[": "]", "{": "}"}
        for index, (token, _, _) in enumerate(tokens):
            if token in opening:
                stack.append((index, opening[token]))
            elif token in opening.values():
                require(bool(stack) and stack[-1][1] == token, f"unbalanced Rust proof delimiters: {source.name}")
                start, _ = stack.pop()
                pairs[start] = index
        require(not stack, f"unbalanced Rust proof delimiters: {source.name}")

        def section(start, end):
            return text[tokens[start][1]:tokens[end - 1][2]] if start < end else ""

        def scope(start, end, scope_modules, attributes, base):
            pending, index = [], start
            attributes = list(attributes)
            while index < end:
                token = tokens[index][0]
                if token == "#":
                    inner = tokens[index + 1][0] == "!"
                    bracket = index + (2 if inner else 1)
                    require(tokens[bracket][0] == "[", f"unsupported Rust proof attribute: {source.name}")
                    attribute = section(bracket + 1, pairs[bracket]).strip()
                    (attributes if inner else pending).append(attribute)
                    index = pairs[bracket] + 1
                    continue
                if token == "mod" and index + 2 < end:
                    name, delimiter = tokens[index + 1][0], tokens[index + 2][0]
                    if delimiter == "{":
                        scope(index + 3, pairs[index + 2], (*scope_modules, name),
                              [*attributes, *pending], base / name)
                        index = pairs[index + 2] + 1
                    elif delimiter == ";":
                        explicit = [re.fullmatch(r'path\s*=\s*"([^"\n]+)"', item) for item in pending]
                        explicit = [match.group(1) for match in explicit if match]
                        require(len(explicit) <= 1, "ambiguous Rust module path attribute")
                        candidates = [base / explicit[0]] if explicit else [base / (name + ".rs"), base / name / "mod.rs"]
                        matches = [candidate for candidate in candidates if candidate.is_file()]
                        require(len(matches) == 1, f"missing or ambiguous Rust test module: {source.name}:{name}")
                        child = matches[0]
                        read_file(child, (*scope_modules, name), [*attributes, *pending], active,
                                  child.parent if child.name == "mod.rs" else child.parent / child.stem)
                        index += 3
                    else:
                        raise AssemblyError(f"unsupported Rust test module: {source.name}:{name}")
                    pending = []
                    continue
                if token == "fn" and index + 1 < end:
                    name = tokens[index + 1][0]
                    own_attributes = [*attributes, *pending]
                    if any(re.fullmatch(r"(?:[A-Za-z_][\w]*::)*test(?:\(.*\))?", item, re.DOTALL)
                           for item in pending):
                        tests.append({"path": source, "name": name,
                                      "qualified": "::".join((*scope_modules, name)), "attributes": own_attributes})
                    index += 2
                    while index < end and tokens[index][0] not in {"{", ";"}:
                        index = pairs[index] + 1 if tokens[index][0] in {"(", "["} else index + 1
                    if index < end:
                        index = pairs[index] + 1 if tokens[index][0] == "{" else index + 1
                    pending = []
                    continue
                if token == "macro_rules" and index + 3 < end and tokens[index + 1][0] == "!":
                    name, brace = tokens[index + 2][0], index + 3
                    require(tokens[brace][0] == "{", "unsupported Rust test macro delimiter")
                    body = section(brace + 1, pairs[brace])
                    match = re.fullmatch(r"\(\s*\$(\w+):ident\s*,\s*\$(\w+):literal\s*\)\s*=>\s*\{(.*)\}\s*;\s*", body, re.DOTALL)
                    if match:
                        macros[name] = (match.groups(), [*attributes, *pending])
                    index, pending = pairs[brace] + 1, []
                    continue
                if token in macros and index + 2 < end and tokens[index + 1][0] == "!":
                    bracket = index + 2
                    require(tokens[bracket][0] in {"(", "[", "{"}, "unsupported test macro invocation")
                    arguments = tokens[bracket + 1:pairs[bracket]]
                    require(len(arguments) in {3, 4} and arguments[1][0] == ","
                            and re.fullmatch(r"[A-Za-z_][\w]*", arguments[0][0]) is not None
                            and arguments[2][0].startswith('"'), "unsupported test macro arguments")
                    (name_parameter, literal_parameter, body), macro_attributes = macros[token]
                    body = re.sub(r"\$(\w+)", lambda match: {name_parameter: arguments[0][0],
                                  literal_parameter: arguments[2][0]}.get(match.group(1), match.group()), body)
                    require("$" not in body, "unsupported test macro substitution")
                    scan(body, source, scope_modules, [*macro_attributes, *attributes, *pending],
                         active, base, dict(macros))
                    index, pending = pairs[bracket] + 1, []
                    continue
                if token in {"{", "(", "["}:
                    index = pairs[index] + 1
                    if token == "{":
                        pending = []
                else:
                    if token == ";":
                        pending = []
                    index += 1
        scope(0, len(tokens), modules, inherited, module_base)

    read_file(path, (), [], frozenset())
    return tests, sources


def test_applicable(test: dict, features: set[str]) -> bool:
    enabled = True
    for attribute in test["attributes"]:
        if re.match(r"ignore\b", attribute):
            enabled = False
        elif attribute.startswith("cfg(") and attribute.endswith(")"):
            enabled = cfg_enabled(attribute[4:-1], features) and enabled
        elif re.match(r"(?:cfg|cfg_attr|ignore)\b", attribute):
            raise AssemblyError(f"unsupported relevant proof attribute: {attribute}")
    return enabled


def public_file(root: Path, value: str) -> Path:
    require(isinstance(value, str) and bool(value), "evidence path must be nonempty")
    path = Path(value)
    require(not path.is_absolute() and ".." not in path.parts, f"non-public evidence path: {value}")
    resolved = (root / path).resolve()
    require(resolved.is_relative_to(root.resolve()) and resolved.is_file(), f"missing evidence: {value}")
    return resolved


def load_json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    require(isinstance(value, dict), f"JSON root must be an object: {path.name}")
    return value


def check_reference(reference: dict, root: Path) -> None:
    require(isinstance(reference, dict), "evidence reference must be an object")
    path = public_file(root, reference.get("path"))
    anchor = reference.get("anchor")
    if anchor is None:
        return
    require(isinstance(anchor, str) and ":" in anchor, f"invalid evidence anchor: {path.name}")
    kind, value = anchor.split(":", 1)
    text = path.read_text(encoding="utf-8")
    if kind == "test":
        definitions, _ = rust_test_inventory(path, root)
        found = any(test["path"] == path and test["name"] == value.split("::")[-1]
                    and not any(re.match(r"ignore\b", item) for item in test["attributes"])
                    for test in definitions)
    elif kind == "python":
        found = value in {node.name for node in ast.walk(ast.parse(text))
                          if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    elif kind == "heading":
        found = value in {heading for _, _, heading in validator.markdown_headings(text.splitlines())}
    elif kind == "text":
        found = bool(value) and value in text
    elif kind == "json":
        current = json.loads(text)
        require(value.startswith("/"), f"JSON anchor must be a pointer: {anchor}")
        try:
            for component in value[1:].split("/"):
                key = component.replace("~1", "/").replace("~0", "~")
                current = current[int(key)] if isinstance(current, list) else current[key]
            found = True
        except (KeyError, IndexError, TypeError, ValueError):
            found = False
    else:
        raise AssemblyError(f"unsupported evidence anchor: {anchor}")
    require(found, f"missing evidence anchor: {reference['path']}#{anchor}")


def cfg_enabled(expression: str, features: set[str]) -> bool:
    """Evaluate the bounded feature-only cfg grammar used by proof targets."""
    expression = expression.strip()
    feature = re.fullmatch(r'feature\s*=\s*"([^"]+)"', expression)
    if feature:
        return feature.group(1) in features
    group = re.fullmatch(r"(all|any|not)\((.*)\)", expression, re.DOTALL)
    require(group is not None, f"unsupported proof cfg: {expression}")
    arguments, start, depth = [], 0, 0
    for index, character in enumerate(group.group(2)):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
        elif character == "," and depth == 0:
            arguments.append(group.group(2)[start:index])
            start = index + 1
    arguments.append(group.group(2)[start:])
    values = [cfg_enabled(argument, features) for argument in arguments if argument.strip()]
    if group.group(1) == "not":
        require(len(values) == 1, "proof cfg not() must have one argument")
        return not values[0]
    return all(values) if group.group(1) == "all" else any(values)


def cargo_features(command: list[str], manifest: dict) -> set[str]:
    definitions = manifest.get("features", {})
    chosen = set(definitions) if "--all-features" in command else set()
    if "--no-default-features" not in command and "default" in definitions:
        chosen.add("default")
    if "--features" in command:
        explicit = set(command[command.index("--features") + 1].split(","))
        require(explicit <= definitions.keys(), f"unknown features: {sorted(explicit - definitions.keys())}")
        chosen.update(explicit)
    pending = list(chosen)
    while pending:
        for feature in definitions.get(pending.pop(), []):
            if feature in definitions and feature not in chosen:
                chosen.add(feature)
                pending.append(feature)
    return chosen


def check_command(command: list[str], evidence: list[dict], root: Path) -> None:
    """Validate bounded known argv without starting any proof command."""
    require(isinstance(command, list) and bool(command) and all(isinstance(x, str) and x for x in command),
            "proof command must be nonempty argv")
    if command[:2] == ["cargo", "test"]:
        require("--workspace" not in command and "-p" in command and "--test" in command,
                "proof requires a concrete Cargo package and test target")
        require("--locked" in command and "--offline" in command, "Cargo proof must be locked and offline")
        index = 2
        while index < len(command):
            token = command[index]
            require(token in {"-p", "--test", "--features", "--locked", "--offline",
                              "--all-features", "--no-default-features"}, f"unknown Cargo proof option: {token}")
            require(command.count(token) == 1, f"duplicate Cargo proof option: {token}")
            index += 2 if token in {"-p", "--test", "--features"} else 1
        package = command[command.index("-p") + 1]
        target = command[command.index("--test") + 1]
        require(re.fullmatch(r"[a-z][a-z0-9_-]*", package) is not None
                and re.fullmatch(r"[a-z][a-z0-9_]*", target) is not None, "invalid Cargo target")
        manifest = tomllib.loads(public_file(root, f"crates/{package}/Cargo.toml").read_text())
        require(manifest["package"]["name"] == package, f"Cargo package mismatch: {package}")
        features = cargo_features(command, manifest)
        path = f"crates/{package}/tests/{target}.rs"
        definitions, sources = rust_test_inventory(public_file(root, path), root)
        source_paths = {str(source.relative_to(root.resolve())) for source in sources}
        references = [ref for ref in evidence if ref.get("path") in source_paths
                      and ref.get("anchor", "").startswith("test:")]
        require(bool(references), f"Cargo target lacks an actual test anchor: {path}")
        for reference in references:
            name = reference["anchor"][5:]
            matches = [test for test in definitions if str(test["path"].relative_to(root.resolve())) == reference["path"]
                       and (test["qualified"] == name if "::" in name else test["name"] == name)]
            require(len(matches) == 1, f"missing or ambiguous Cargo test anchor: {reference['path']}#{name}")
            require(test_applicable(matches[0], features),
                    f"proof target disabled by feature cfg or ignore: {reference['path']}#{name}")
    elif command[0] == "python3":
        require(len(command) == 3 and command[2] == "--check", "unknown Python proof argv")
        require(command[1] in {"scripts/generate_ai_conformance.py",
                              "scripts/generate_upstream_manifests.py",
                              "scripts/generate_selected_catalogs.py"}, "unknown Python proof suite")
        require(any(ref.get("path") == command[1] and ref.get("anchor") == "python:main"
                    for ref in evidence), f"Python proof lacks main anchor: {command[1]}")
    else:
        raise AssemblyError(f"unknown proof executable: {command[0]}")


def validate_registry(registry: dict, labels: set[str], root: Path = ROOT) -> dict[str, dict]:
    def forbid_owners(value):
        if isinstance(value, dict):
            require(not ({"primary_owner", "collaborators"} & value.keys()),
                    "proof registry cannot duplicate ownership")
            for child in value.values():
                forbid_owners(child)
        elif isinstance(value, list):
            for child in value:
                forbid_owners(child)
    forbid_owners(registry)
    require(registry.get("schema_version") == 1, "assembly registry schema must be 1")
    suites = registry.get("suites")
    require(isinstance(suites, list) and bool(suites), "missing proof suites")
    by_id = {}
    for row in suites:
        suite = row.get("id")
        require(isinstance(suite, str) and suite not in by_id, f"duplicate or missing proof suite: {suite}")
        by_id[suite] = row
        require(row.get("status") in {"executable", "accepted-limit"}, f"invalid proof status: {suite}")
        evidence, limitations, commands = row.get("evidence"), row.get("limitations"), row.get("commands")
        require(isinstance(evidence, list) and bool(evidence), f"missing proof evidence: {suite}")
        require(isinstance(limitations, list) and isinstance(commands, list), f"malformed proof suite: {suite}")
        require(row["status"] != "accepted-limit" or bool(limitations), f"accepted-limit lacks evidence: {suite}")
        require(row["status"] != "executable" or bool(commands), f"executable suite has no commands: {suite}")
        require(row["status"] != "executable" or not limitations,
                f"limited suite must retain accepted-limit status: {suite}")
        for reference in evidence + limitations:
            try:
                check_reference(reference, root)
            except AssemblyError as error:
                raise AssemblyError(f"proof {suite}: {error}") from error
        for command in commands:
            check_command(command, evidence, root)
        bound_paths = set()
        for command in commands:
            if command[:2] == ["cargo", "test"]:
                package = command[command.index("-p") + 1]
                target = command[command.index("--test") + 1]
                _, sources = rust_test_inventory(public_file(root, f"crates/{package}/tests/{target}.rs"), root)
                bound_paths.update(str(source.relative_to(root.resolve())) for source in sources)
        for reference in evidence:
            if reference.get("anchor", "").startswith("test:"):
                require(reference["path"] in bound_paths,
                        f"proof {suite}: test anchor is unbound to a declared command: {reference['path']}")
    require(set(by_id) == labels,
            f"unresolved proof suites: missing={sorted(labels - by_id.keys())}, extra={sorted(by_id.keys() - labels)}")
    return by_id


def adapter_proof_requirements(inventory: dict) -> set[tuple]:
    """Derive retained shared-adapter proof obligations from the frozen inventory."""
    required = set()
    for row in inventory["rows"]:
        if row["surface"] != "ai.api-adapters":
            continue
        require(bool(row.get("executedTests")), f"adapter export lacks proof bindings: {row['key']}")
        for test in row["executedTests"]:
            if isinstance(test, str):
                target, name = test.split("#", 1)
                package, features = "pi-ai-providers", row["features"]
            else:
                require(test.get("kind") == "test", f"unsupported adapter proof binding: {row['key']}")
                package, target, name, features = (test[key] for key in ("package", "target", "name", "features"))
            required.add((package, target, name, tuple(features)))
    return required


def validate_adapter_proofs(suite: dict, inventory: dict, root: Path = ROOT) -> None:
    """An executable logical adapter suite must retain every exact proof route."""
    if suite["status"] == "accepted-limit":
        return
    references = {(item["path"], item.get("anchor")) for item in suite["evidence"]}
    for package, target, name, features in sorted(adapter_proof_requirements(inventory)):
        path = f"crates/{package}/tests/{target}.rs"
        require((path, "test:" + name) in references,
                f"retained adapter proof missing exact anchor: {path}#{name}")
        commands = [command for command in suite["commands"] if command[:2] == ["cargo", "test"]
                    and command[command.index("-p") + 1] == package
                    and command[command.index("--test") + 1] == target]
        require(bool(commands), f"retained adapter proof missing target: {package}/{target}")
        manifest = tomllib.loads(public_file(root, f"crates/{package}/Cargo.toml").read_text())
        require(any(set(features) <= cargo_features(command, manifest) for command in commands),
                f"retained adapter proof missing profile: {package}/{target}#{name}:{list(features)}")


class Resolver:
    """Resolve declarations and watched package reexports through pinned blobs."""

    def __init__(self, map_data: dict, entries: list, checkout: Path):
        self.map = map_data
        self.commit = map_data["upstream"]["commit"]
        self.checkout = checkout
        self.routes = {entry.path: entry for entry in entries}
        self.surfaces = {surface["id"]: surface for surface in map_data["surface"]}
        self.owners = {owner["id"]: owner for owner in map_data["owner"]}
        self.sources = {}
        self.selections = {}
        self.packages = {}
        self.package_symbols = {}
        for selector in map_data["selector"]:
            path = selector["path"]
            selected = validator.selector_line_set(selector, self.lines(path))
            for line in selected:
                key = (path, line + 1)
                require(key not in self.selections or not self.lines(path)[line].strip(),
                        f"overlapping selector: {path}:{line + 1}")
                self.selections[key] = selector["surface"]
        for entry in entries:
            if entry.path.endswith("/package.json"):
                package = json.loads("\n".join(self.lines(entry.path)))
                if isinstance(package.get("name"), str) and "exports" in package:
                    require(package["name"] not in self.packages, f"ambiguous watched package: {package['name']}")
                    self.packages[package["name"]] = entry.path.rsplit("/", 1)[0]

    def lines(self, path: str) -> list[str]:
        if path not in self.sources:
            self.sources[path] = validator.git_blob(self.checkout, self.commit, path).decode().splitlines()
        return self.sources[path]

    def surface(self, path: str, line: int | None = None) -> str:
        require(path in self.routes, f"unrouted source obligation: {path}")
        if line is not None and any(selector["path"] == path for selector in self.map["selector"]):
            require((path, line) in self.selections, f"source obligation outside selector: {path}:{line}")
            return self.selections[path, line]
        return self.routes[path].surface

    def owner(self, surface: str, obligation: str) -> str:
        require(surface in self.surfaces, f"{obligation}: unknown surface {surface}")
        record = self.surfaces[surface]
        owner = record["primary_owner"]
        require(owner in self.owners, f"{obligation}: {surface} missing owner {owner}")
        expected = "outside-scope" if record["disposition"] == "outside-scope" else "rust"
        require(self.owners[owner]["kind"] == expected, f"{obligation}: {surface} wrong owner kind {owner}")
        return owner

    def external_name(self, path: str, origin: str, name: str, active: tuple = ()) -> str:
        """Recover an external alias from the pinned reexport clauses.

        The historical projection deliberately records the exported spelling and
        package specifier, not the imported spelling. Follow only its existing
        relative-barrel grammar using the established source-path resolver.
        """
        key = (path, origin, name)
        require(key not in active, f"cyclic external alias: {key}")
        text = "\n".join(self.lines(path))
        names = set()
        pattern = r'export\s+(?:type\s+)?\{(.*?)\}\s*from\s*["\']([^"\']+)["\']\s*;'
        for body, specifier in re.findall(pattern, text, re.DOTALL):
            for item in body.split(","):
                item = re.sub(r"/\*.*?\*/", "", item, flags=re.DOTALL).strip()
                item = re.sub(r"^type\s+", "", item)
                parts = re.split(r"\s+as\s+", item)
                if parts[-1].strip() != name:
                    continue
                source_name = parts[0].strip()
                if specifier == origin:
                    names.add(source_name)
                elif specifier.startswith("."):
                    target = projections.resolve_source_path(path, specifier, set(self.routes))
                    require(target is not None, f"missing external alias barrel: {path}:{specifier}")
                    names.add(self.external_name(target, origin, source_name, (*active, key)))
        for specifier in re.findall(r'export\s+\*\s+from\s+["\']([^"\']+)["\']\s*;', text):
            target = projections.resolve_source_path(path, specifier, set(self.routes))
            if target is None:
                continue
            symbols = projections.exported_symbols(self.checkout, self.commit, target,
                                                     set(self.routes), {})
            if name in symbols and symbols[name][1] == origin:
                names.add(self.external_name(target, origin, name, (*active, key)))
        require(len(names) == 1, f"missing or ambiguous pinned external alias: {key}")
        return names.pop()

    def export(self, symbol: dict, *, active: tuple = ()) -> str:
        origin, name = symbol["origin"], symbol["name"]
        if origin == "typebox":
            require(name in {"Type", "Static", "TSchema"} and symbol["origin_line"] == 0
                    and not symbol["origin_declaration"], f"unreviewed external schema projection: {name}")
            return "external"
        if origin.startswith("packages/"):
            line = symbol["origin_line"]
            require(isinstance(line, int) and 0 < line <= len(self.lines(origin))
                    and self.lines(origin)[line - 1].strip() == symbol["origin_declaration"],
                    f"stale declaration: {origin}:{name}")
            return self.surface(origin, line)
        require(symbol["origin_line"] == 0 and not symbol["origin_declaration"],
                f"external reexport carries local declaration: {origin}:{name}")
        candidates = [(package, root) for package, root in self.packages.items()
                      if origin == package or origin.startswith(package + "/")]
        require(len(candidates) == 1, f"unresolved watched package reexport: {origin}:{name}")
        package, package_root = candidates[0]
        subpath = "." + origin[len(package):]
        source_name = self.external_name(symbol["source_path"], origin, name)
        key = (package, subpath, source_name)
        require(key not in active, f"cyclic package reexport: {key}")
        if package not in self.package_symbols:
            self.package_symbols[package] = tomllib.loads(projections.exports_manifest(
                self.commit, self.checkout, package_root, "assembly-package"))["symbol"]
        matches = [item for item in self.package_symbols[package]
                   if item["subpath"] == subpath and item["name"] == source_name]
        require(len(matches) == 1, f"missing or ambiguous package export: {key}")
        namespaces = {"type": {"type"}, "value": {"value"}, "type-and-value": {"type", "value"}}
        require(namespaces[symbol["kind"]] <= namespaces[matches[0]["kind"]],
                f"package export namespace loss: {key}")
        return self.export(matches[0], active=(*active, key))


def check_assembly(map_path: Path, registry_path: Path, root: Path = ROOT) -> dict:
    """Return deterministic structural totals and named unresolved obligations."""
    errors, totals, resolved = [], {}, []
    map_data = tomllib.loads(map_path.read_text(encoding="utf-8"))
    topology = tomllib.loads((root / "workspace-topology.toml").read_text())
    _, entries, _ = validator.validate_upstream_map(
        map_path, set(topology["production_crates"]), errors, permit_pending_digest=False)
    require(map_data["upstream"]["commit"] == ai.SOURCE_COMMIT, "assembly requires the accepted pinned baseline")
    checkout = resolve_upstream(map_data["upstream"]["checkout"])
    resolver = Resolver(map_data, entries, checkout)
    registry = load_json(registry_path)
    labels = {name for surface in map_data["surface"] for name in surface["conformance"]}
    try:
        suites = validate_registry(registry, labels, root)
    except (AssemblyError, IndexError, KeyError, SyntaxError) as error:
        raise AssemblyError(f"proof registry: {error}") from error

    def obligation(surface, identity):
        try:
            owner = resolver.owner(surface, identity)
            surface_data = resolver.surfaces[surface]
            if surface_data["disposition"] == "included":
                require(bool(surface_data["conformance"]), f"{identity}: {surface} has no proof suites")
                for suite in surface_data["conformance"] if suites else []:
                    require(suite in suites, f"{identity}: {surface} missing proof {suite}")
            resolved.append({"id": identity, "surface": surface, "owner": owner})
        except AssemblyError as error:
            errors.append(str(error))

    # Recompute all seven manifests through the established inert pinned readers.
    expected = {
        "routes": projections.route_manifest(resolver.commit, entries),
        "tests": projections.test_manifest(resolver.commit, checkout, entries,
                                            projections.selector_surfaces_by_path(map_data)),
        "ai-exports": projections.exports_manifest(resolver.commit, checkout, "packages/ai", "ai"),
        "agent-exports": projections.exports_manifest(resolver.commit, checkout, "packages/agent", "agent"),
        "providers": projections.providers_manifest(resolver.commit, checkout),
        "catalog-inputs": projections.catalog_inputs_manifest(resolver.commit, checkout, entries),
        "harness-headings": projections.heading_manifest(resolver.commit, checkout),
    }
    manifests = {}
    for name, text in expected.items():
        require((root / f"upstream/manifests/{name}.toml").read_text() == text,
                f"stale pinned manifest: {name}")
        manifests[name] = tomllib.loads(text)
    require(len(entries) == 1144, "watched path universe must retain 1144 entries")
    for entry in entries:
        obligation(entry.surface, "path:" + entry.path)
    totals["watched_paths"] = len(entries)
    totals["source_dispositions"] = dict(sorted(Counter(s["disposition"] for s in map_data["surface"]).items()))
    for package, count in (("ai", 699), ("agent", 675)):
        symbols = manifests[package + "-exports"]["symbol"]
        require(len(symbols) == count, f"{package} export occurrence universe changed")
        surfaces = Counter()
        for symbol in symbols:
            identity = f"export:{package}:{symbol['subpath']}:{symbol['name']}"
            try:
                surface = resolver.export(symbol)
                surfaces[surface] += 1
                if surface == "external":
                    require(package == "ai" and f"{symbol['subpath']}:{symbol['name']}" in ai.EXTERNAL_KEYS,
                            f"unreviewed external occurrence: {identity}")
                    require("conformance/ai-domain" in suites, f"{identity}: missing external projection evidence")
                else:
                    obligation(surface, identity)
            except AssemblyError as error:
                errors.append(f"{identity}: {error}")
        totals[package + "_exports"] = {"occurrences": len(symbols), "surfaces": dict(sorted(surfaces.items()))}
    for test in manifests["tests"]["test"]:
        for surface in test["evidence_surfaces"]:
            obligation(surface, "upstream-test:" + test["path"])
    totals["upstream_test_paths"] = len(manifests["tests"]["test"])
    validator.validate_catalog_provenance(map_data, manifests["catalog-inputs"], errors)
    totals["catalog_inputs"] = len(manifests["catalog-inputs"]["input"])
    totals["historical_catalog_provenance"] = manifests["catalog-inputs"]["provenance_status"]

    behavior = ai.expected_behavior_inventory(root)
    require((root / "conformance/ai-domain/behavior-inventory.json").read_text() == ai.json_text(behavior),
            "AI behavior inventory differs from its source-bound fragments")
    mapping_text = (root / "conformance/ai-domain/export-mapping.toml").read_text()
    inventory, _ = ai.build_export_inventory(manifests["ai-exports"],
        ai.manifest_body_digest(expected["ai-exports"]), tomllib.loads(mapping_text),
        ai.mapping_digest(mapping_text), manifests["routes"], map_data, behavior, root)
    require((root / "conformance/ai-domain/inventory.json").read_text() == ai.json_text(inventory),
            "AI export inventory differs from its source-bound proof mapping")
    if "conformance/provider-traces" in suites:
        validate_adapter_proofs(suites["conformance/provider-traces"], inventory, root)
    for row in inventory["rows"]:
        if row["surface"] != "external":
            obligation(row["surface"], "ai-evidence:" + row["key"])
    # Local normative contract references require explicit source seams, not
    # historical evidence-owner strings. Line-specific upstream sources use selectors.
    test_definitions = set()
    for path in (root / "crates").glob("**/*.rs"):
        test_definitions.update(active_tests(path.read_text(encoding="utf-8")))
    for row in behavior["rows"]:
        require(row["test"] in test_definitions, f"behavior {row['id']}: missing actual test definition {row['test']}")
        source = row["source"]
        path = source.split("@", 1)[0].split(":", 1)[0].split("#", 1)[0]
        if "@" in source:
            location = source.split("@", 1)[1].split(":", 1)
            line = int(location[1].split("-", 1)[0]) if len(location) == 2 else None
            source_paths = [(path, line)]
        else:
            source_paths = [(value, None) for value in registry["behavior_contract_sources"].get(path, [])]
            require(bool(source_paths), f"behavior {row['id']}: unbound local contract {path}")
        for source_path, line in source_paths:
            obligation(resolver.surface(source_path, line), "ai-behavior:" + row["id"])
    totals["ai_behavior_rows"] = len(behavior["rows"])
    totals["ai_export_dispositions"] = inventory["dispositionTotals"]
    scope = ai.validate_selected_provider_scope(inventory["rows"], root)
    totals["selected_scope_occurrences"] = len(scope["rows"])
    providers = manifests["providers"]["provider"]
    require(len(providers) == 42, "provider identity universe must retain 42 entries")
    selected = {(item["kind"], item["module"]) for item in scope["selectedSourceEntries"]}
    provider_counts = Counter()
    for provider in providers:
        path = f"packages/ai/src/providers/{provider['module']}.ts"
        obligation(resolver.surface(path), f"provider:{provider['kind']}:{provider['module']}")
        classification = "selected_" + provider["kind"] if (provider["kind"], provider["module"]) in selected else "excluded_" + provider["kind"]
        provider_counts[classification] += 1
    require(dict(provider_counts) == {"excluded_text": 34, "selected_text": 6, "selected_image": 1, "excluded_faux-testkit": 1},
            f"provider selected/excluded partition changed: {dict(provider_counts)}")
    totals["provider_identities"] = dict(sorted(provider_counts.items()))
    factory_source = (root / "crates/pi-ai-providers/src/providers/mod.rs").read_text()
    factory_modules = re.findall(r'#\[cfg\(feature = "provider-([^"\n]+)"\)\]\s*mod\s+(\w+);', factory_source)
    require(set(factory_modules) == {(module, module.replace("-", "_"))
                                    for kind, module in selected if kind == "text"},
            "Rust factory modules must retain exactly the six selected text entrances")
    image_modules = re.findall(r'#\[cfg\(feature = "images-([^"\n]+)"\)\]\s*mod\s+(\w+);', factory_source)
    require(image_modules == [("openrouter", "openrouter_images")],
            "Rust factory modules must retain the separate selected image entrance")
    for _, module in factory_modules + image_modules:
        public_file(root, f"crates/pi-ai-providers/src/providers/{module}.rs")
    selectors = [s for s in map_data["selector"] if s["path"] == HARNESS]
    build_rows = [s for s in selectors if s["id"].startswith("harness.slice-")]
    require(len(selectors) == 37 and len(build_rows) == 18, "Harness selector/build-row universe changed")
    for selector in selectors:
        obligation(selector["surface"], "selector:" + selector["id"])
    totals["harness_selectors"], totals["harness_build_rows"] = len(selectors), len(build_rows)

    legacy = load_json(root / "conformance/agent-loop/manifest.json")
    require([(row["id"], row["status"]) for row in legacy["denominator"]] ==
            [(f"U{i}", status) for i, status in enumerate(LEGACY_STATUSES, 1)], "legacy U1-U14 identities/statuses changed")
    require(len(legacy["positiveRows"]) == len(legacy["negatives"]) == 12, "legacy corpus must retain 12/12 rows")
    for row in legacy["denominator"]:
        path = registry["legacy_sources"].get(row["id"])
        require(path is not None, f"legacy {row['id']}: missing source seam")
        obligation(resolver.surface(path), "legacy:" + row["id"])
        if row["status"] != "source-confirmed":
            require(row["id"] in registry["legacy_limits"], f"legacy {row['id']}: missing accepted-limit evidence")
            check_reference(registry["legacy_limits"][row["id"]], root)
            for label in resolver.surfaces[resolver.surface(path)]["conformance"]:
                require(suites.get(label, {}).get("status") == "accepted-limit",
                        f"legacy {row['id']}: {label} must preserve accepted-limit status")
    bridge = (root / "crates/pi-agent/tests/agent_observation_bridge.rs").read_text()
    bridge_body = bridge.split("fn actual_rust_agent_observations_compare_through_node_for_representable_rows()", 1)[1]
    actual_ids = re.findall(r'"([a-z_]+\.[a-z_.]+)"', bridge_body)
    require(tuple(actual_ids) == BRIDGE_IDS, "actual legacy bridge must retain exactly five observed IDs")
    totals["legacy_denominator_statuses"] = dict(sorted(Counter(LEGACY_STATUSES).items()))
    totals["legacy_corpus_rows"], totals["legacy_actual_bridge_rows"] = 12, len(actual_ids)
    coding = load_json(root / "conformance/coding-runtime/manifest.json")
    require(tuple(row["id"] for row in coding["positiveRows"]) == CODING_IDS,
            "coding operation identities/order changed")
    require(len(coding["negatives"]) == 18 and coding["pendingRows"] == [], "coding corpus must retain 18/18/0")
    for row in coding["positiveRows"]:
        paths = registry["coding_sources"].get(row["id"], [])
        require(bool(paths), f"coding {row['id']}: missing source seams")
        for path in paths:
            obligation(resolver.surface(path), "coding:" + row["id"])
    coding_bridge = (root / "crates/pi-coding-runtime/tests/coding_runtime_conformance.rs").read_text()
    coding_body = coding_bridge.split("fn assembled_coding_runtime_rows_compare_through_node()", 1)[1].split("#[test]", 1)[0]
    require(tuple(re.findall(r'^        "([a-z0-9_.]+)",$', coding_body, re.MULTILINE)) == CODING_IDS,
            "coding bridge operation identities/order changed")
    require('cfg!(feature = "extensions") { 18 } else { 17 }' in coding_body
            and 'extension.participation_ordering requires the extensions feature' in coding_body,
            "coding feature applicability lost")
    totals["coding_operations"] = {"positive": 18, "negative": 18, "pending": 0, "default_applicable": 17, "extensions_applicable": 18}
    for reference in registry["preserved_inputs"]:
        path = public_file(root, reference["path"])
        require(hashlib.sha256(path.read_bytes()).hexdigest() == reference["sha256"],
                f"preserved source/fixture identity changed: {reference['path']}")
    totals["proof_suites"] = dict(sorted(Counter(row["status"] for row in suites.values()).items()))
    totals["proof_commands_defined"] = sum(len(row["commands"]) for row in suites.values())
    totals["proof_commands_executed"] = 0
    return {"schema_version": 1, "source_commit": resolver.commit, "totals": totals,
            "unresolved": sorted(set(errors)), "resolved": resolved}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--map", type=Path, default=ROOT / "upstream-map.toml")
    parser.add_argument("--registry", type=Path, default=ROOT / "conformance/assembly.json")
    parser.add_argument("--json", action="store_true", help="include every resolved obligation")
    args = parser.parse_args()
    try:
        result = check_assembly(args.map.resolve(), args.registry.resolve())
    except (AssemblyError, ai.ConformanceError, OSError, RuntimeError, ValueError, KeyError, IndexError, TypeError) as error:
        print(f"assembly error: {error}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    else:
        print(json.dumps({key: value for key, value in result.items() if key != "resolved"},
                         ensure_ascii=False, indent=2, sort_keys=True))
        print(f"Assembly: {len(result['unresolved'])} unresolved mappings; proof commands executed: 0")
    return 1 if result["unresolved"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
