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
import os
from functools import lru_cache
import json
from pathlib import Path
import re
import sys
import tomllib

try:
    from . import generate_upstream_manifests as projections
    from . import validate_docs as validator
    from .upstream import resolve_upstream
except ImportError:
    import generate_upstream_manifests as projections
    import validate_docs as validator
    from upstream import resolve_upstream

ROOT = Path(__file__).resolve().parents[1]
HISTORICAL_SOURCE_REVISION = "ea7396d067d42e3fca3924aff36958dcd68f7d1b"
# Integration target names cannot contain @; this identity denotes Cargo --lib.
LIB_TARGET = "@lib"
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


def rust_test_inventory(path: Path, root: Path, *, file_texts=None) -> tuple[list[dict], set[Path]]:
    """Read exact test items with lexical file/module/function attributes.

    This intentionally does not compile Rust. Unsupported conditions on a
    referenced item are rejected later. The one simple local ident/literal
    test-macro form used by the current adapter corpus is expanded as inert
    token substitution; other macro-generated names remain unresolved.
    """
    tests, sources = [], set()

    def exists(source):
        if file_texts is None:
            return source.is_file()
        source = source.resolve()
        return source.is_relative_to(root.resolve()) and source.relative_to(root.resolve()).as_posix() in file_texts

    def read_file(source, modules, inherited, active, module_base=None):
        source = source.resolve()
        require(source.is_relative_to(root.resolve()) and exists(source), "missing public Rust test module")
        require(source not in active, f"cyclic Rust test module: {source.name}")
        sources.add(source)
        text = source.read_text(encoding="utf-8") if file_texts is None else file_texts[source.relative_to(root.resolve()).as_posix()]
        scan(text, source, modules, inherited,
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
                        attribute_base = source.parent if base == module_base else base
                        candidates = [attribute_base / explicit[0]] if explicit else [base / (name + ".rs"), base / name / "mod.rs"]
                        matches = [candidate for candidate in candidates if exists(candidate)]
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


def test_applicable(test: dict, features: set[str], *, cfg_platform: tuple[str, str] | None = None) -> bool:
    enabled = True
    for attribute in test["attributes"]:
        if re.match(r"ignore\b", attribute):
            enabled = False
        elif attribute.startswith("cfg(") and attribute.endswith(")"):
            enabled = cfg_enabled(attribute[4:-1], features, cfg_platform=cfg_platform) and enabled
        elif re.match(r"(?:cfg|cfg_attr|ignore)\b", attribute):
            raise AssemblyError(f"unsupported relevant proof attribute: {attribute}")
        elif re.match(r"(?:test_runner|reexport_test_harness_main|no_main)\b", attribute) or (
                re.match(r"feature\s*\(", attribute) and "custom_test_frameworks" in attribute):
            raise AssemblyError(f"unsupported custom Cargo test harness attribute: {attribute}")
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


def check_reference(reference: dict, root: Path, inventories: dict | None = None) -> None:
    require(isinstance(reference, dict), "evidence reference must be an object")
    path = public_file(root, reference.get("path"))
    anchor = reference.get("anchor")
    if anchor is None:
        return
    require(isinstance(anchor, str) and ":" in anchor, f"invalid evidence anchor: {path.name}")
    kind, value = anchor.split(":", 1)
    text = path.read_text(encoding="utf-8")
    if kind == "test":
        if inventories is None:
            definitions, _ = rust_test_inventory(path, root)
        else:
            if path not in inventories:
                inventories[path] = rust_test_inventory(path, root)
            definitions, _ = inventories[path]
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


def cfg_enabled(expression: str, features: set[str], *, cfg_platform: tuple[str, str] | None = None) -> bool:
    """Evaluate bounded cfg for a Cargo test harness, where cfg(test) is active."""
    expression = expression.strip()
    if expression == "test":
        return True
    if expression in {'unix', 'windows'}:
        family = os.name if cfg_platform is None else cfg_platform[0]
        return (family == 'posix') if expression == 'unix' else (family == 'nt')
    platform = re.fullmatch(r'target_os\s*=\s*"(linux|macos|windows)"', expression)
    if platform:
        actual = ({'linux':'linux', 'darwin':'macos', 'win32':'windows'}.get(sys.platform)
                  if cfg_platform is None else cfg_platform[1])
        require(actual is not None, 'unsupported host platform for proof cfg')
        return platform.group(1) == actual
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
    values = [cfg_enabled(argument, features, cfg_platform=cfg_platform)
              for argument in arguments if argument.strip()]
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


def cargo_test_selector(command: list[str]) -> tuple[str, str]:
    """Resolve exactly one concrete integration or default library test selector."""
    require(isinstance(command, list) and bool(command) and all(isinstance(x, str) and x for x in command),
            "proof command must be nonempty argv")
    require(command[:2] == ["cargo", "test"], "expected a Cargo test proof")
    require("--workspace" not in command and "-p" in command
            and ("--test" in command) != ("--lib" in command),
            "proof requires a concrete Cargo package and test target selector (--test or --lib)")
    require("--locked" in command and "--offline" in command, "Cargo proof must be locked and offline")
    index = 2
    while index < len(command):
        token = command[index]
        require(token in {"-p", "--test", "--lib", "--features", "--locked", "--offline",
                          "--all-features", "--no-default-features"}, f"unknown Cargo proof option: {token}")
        require(command.count(token) == 1, f"duplicate Cargo proof option: {token}")
        if token in {"-p", "--test", "--features"}:
            require(index + 1 < len(command) and not command[index + 1].startswith("-"),
                    f"missing Cargo proof option value: {token}")
            index += 2
        else:
            index += 1
    package = command[command.index("-p") + 1]
    target = LIB_TARGET if "--lib" in command else command[command.index("--test") + 1]
    require(re.fullmatch(r"[a-z][a-z0-9_-]*", package) is not None
            and ("--lib" in command or re.fullmatch(r"[a-z][a-z0-9_]*", target) is not None),
            "invalid Cargo target")
    return package, target


def cargo_test_target(command: list[str], manifest: dict) -> tuple[str, str, str, set[str]]:
    """Bind a parsed selector to the one supported manifest source and feature profile."""
    package, target = cargo_test_selector(command)
    require(manifest["package"]["name"] == package, f"Cargo package mismatch: {package}")
    features = cargo_features(command, manifest)
    if target == LIB_TARGET:
        registration = manifest.get("lib", {})
        require(isinstance(registration, dict), f"invalid Cargo library registration: {package}")
        require("lib" in manifest or manifest["package"].get("autolib", True),
                f"Cargo library target is not registered: {package}")
        require(registration.get("path", "src/lib.rs") == "src/lib.rs"
                and registration.get("test", True) is True
                and registration.get("harness", True) is True
                and registration.get("proc-macro", False) is False
                and registration.get("crate-type", ["lib"]) == ["lib"]
                and "required-features" not in registration,
                f"unsupported or disabled Cargo library registration: {package}")
        path = f"crates/{package}/src/lib.rs"
    else:
        registrations = [row for row in manifest.get('test', []) if row.get('name') == target]
        require(len(registrations) <= 1, f"ambiguous Cargo target registration: {package}/{target}")
        require(bool(registrations) or manifest['package'].get('autotests', True),
                f"Cargo target is not registered: {package}/{target}")
        if registrations:
            registration = registrations[0]
            require(registration.get('path', f'tests/{target}.rs') == f'tests/{target}.rs'
                    and registration.get('test', True) and registration.get('harness', True),
                    f"unsupported or disabled Cargo target registration: {package}/{target}")
            require(set(registration.get('required-features', [])) <= features,
                    f"Cargo target disabled by required-features: {package}/{target}")
        path = f"crates/{package}/tests/{target}.rs"
    return package, target, path, features


def check_command(command: list[str], evidence: list[dict], root: Path) -> None:
    """Validate bounded known argv without starting any proof command."""
    require(isinstance(command, list) and bool(command) and all(isinstance(x, str) and x for x in command),
            "proof command must be nonempty argv")
    if command[:2] == ["cargo", "test"]:
        package, _ = cargo_test_selector(command)
        manifest = tomllib.loads(public_file(root, f"crates/{package}/Cargo.toml").read_text())
        package, target, path, features = cargo_test_target(command, manifest)
        definitions, sources = rust_test_inventory(public_file(root, path), root)
        source_paths = {str(source.relative_to(root.resolve())) for source in sources}
        references = [ref for ref in evidence if ref.get("path") in source_paths
                      and ref.get("cargo_target", None if target == LIB_TARGET else target) == target
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
        require(command[1] in {"scripts/generate_upstream_manifests.py",
                              "conformance/catalog/v1.4.0/check.py"}, "unknown Python proof suite")
        require(any(ref.get("path") == command[1] and ref.get("anchor") == "python:main"
                    for ref in evidence), f"Python proof lacks main anchor: {command[1]}")
    else:
        raise AssemblyError(f"unknown proof executable: {command[0]}")


def cargo_test_context(commands: list[list[str]], root: Path) -> tuple[dict, set[tuple[str, str]]]:
    """Retain each declared compilation target's module bases and attributes."""
    inventories, bound_targets = {}, set()
    for command in commands:
        if isinstance(command, list) and command[:2] == ["cargo", "test"]:
            package, _ = cargo_test_selector(command)
            manifest = tomllib.loads(public_file(root, f"crates/{package}/Cargo.toml").read_text())
            _, target, path, _ = cargo_test_target(command, manifest)
            definitions, sources = rust_test_inventory(public_file(root, path), root)
            for source in sources:
                cached_tests, cached_sources = inventories.setdefault(source, ([], set()))
                cached_tests.extend(test for test in definitions if test["path"] == source)
                cached_sources.update(sources)
            bound_targets.update((str(source.relative_to(root.resolve())), target) for source in sources)
    return inventories, bound_targets


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
        # References inside foo.rs must retain the declared target's foo/ child
        # module base and inherited attributes, rather than becoming crate roots.
        inventories, bound_targets = cargo_test_context(commands, root)
        for reference in evidence + limitations:
            try:
                check_reference(reference, root, inventories)
            except AssemblyError as error:
                raise AssemblyError(f"proof {suite}: {error}") from error
        for command in commands:
            check_command(command, evidence, root)
        for reference in evidence:
            if reference.get("anchor", "").startswith("test:"):
                target = reference.get("cargo_target")
                bound = ((reference["path"], target) in bound_targets if target is not None else
                         any(path == reference["path"] and selected != LIB_TARGET for path, selected in bound_targets))
                require(bound, f"proof {suite}: test anchor is unbound to a declared command: {reference['path']}")
    require(set(by_id) == labels,
            f"unresolved proof suites: missing={sorted(labels - by_id.keys())}, extra={sorted(by_id.keys() - labels)}")
    return by_id


# Reviewed required proofs are independent of mutable tests and registry discovery.
# Retirement policy: additions may be generated. Removing a reviewed identity,
# target or its existing feature/dependency binding requires independent review,
# a committed replacement baseline, and an explicit manual update of this revision
# and enrollment. The generator has no retirement or baseline-refresh option.
REVIEWED_PROOF_REVISION = 'f8fc56454a2247f6dca4e7d14e54c54bef4ae9a1'
# Acceptance was reviewed on Linux; a different host cannot shrink this set.
REVIEWED_CFG_PLATFORM = ('posix', 'linux')
REVIEWED_PROOF_TARGETS = {'conformance/ai-core': {'pi-ai-core': ['auth_none_provider',
                                        'auth_store',
                                        'auth_store_concurrency',
                                        'auth_store_object_safety',
                                        'captured_dispatch',
                                        'deferred_lifecycle',
                                        'message_transform',
                                        'message_transform_object_safety',
                                        'models_helpers',
                                        'models_helpers_object_safety',
                                        'models_registry',
                                        'models_registry_concurrency',
                                        'models_registry_object_safety',
                                        'overflow',
                                        'portable_helpers',
                                        'portable_helpers_object_safety',
                                        'provider_headers_order',
                                        'registry_child_cancellation',
                                        'request_lifecycle',
                                        'request_runtime',
                                        'retry',
                                        'runtime_noop_telemetry',
                                        'schema',
                                        'session_resources',
                                        'session_resources_object_safety',
                                        'stream_cancellation_snapshot',
                                        'stream_runtime',
                                        'tool_validation',
                                        'tool_validation_contract',
                                        'v140_contract']},
 'conformance/ai-transport': {'pi-ai-transport': ['anthropic_sse',
                                                  'auth_host',
                                                  'codex_retry',
                                                  'fetch',
                                                  'fetch_body_presence',
                                                  'fetch_content',
                                                  'fetch_request',
                                                  'log_custody',
                                                  'native',
                                                  'native_fetch_headers',
                                                  'node_websocket',
                                                  'openai_sse',
                                                  'portable',
                                                  'provider_retry',
                                                  'raw_head_probe',
                                                  'request_encoding',
                                                  'response_body_presence',
                                                  'smithy_error',
                                                  'smithy_eventstream',
                                                  'smithy_http',
                                                  'smithy_retry']},
 'conformance/provider-traces/v1.4.0': {'pi-ai-providers': ['anthropic_messages',
                                                            'google_genai',
                                                            'google_shared',
                                                            'openai_completions',
                                                            'openai_responses',
                                                            'openai_responses_shared',
                                                            'simple_options',
                                                            'strict_schema_wire',
                                                            'provider_helpers',
                                                            'responses_deferred_order',
                                                            'v140_release',
                                                            'v140_review']},
 'conformance/selected-providers': {'pi-ai-providers': ['selected_factories',
                                                        'selected_factories_source',
                                                        'provider_header_order',
                                                        'auth_foundation',
                                                        'catalog',
                                                        'lazy',
                                                        'wire_payload',
                                                        'request_lifecycle',
                                                        'runtime_response_head',
                                                        'response_body_presence']},
 'conformance/oauth/v1.4.0': {'pi-ai-providers': ['oauth_selected']},
 'conformance/catalog/v1.4.0': {'pi-ai': ['selected_catalog_profiles', 'catalog_binaries'],
                                'pi-ai-providers': ['catalog']},
 'conformance/ai-facade': {'pi-ai': ['catalog_binaries',
                                     'message_transform_facade',
                                     'models_helpers_facade',
                                     'models_registry_facade',
                                     'portable_helpers_facade',
                                     'retry',
                                     'selected_catalog_profiles',
                                     'session_resources_facade',
                                     'tool_validation_facade']},
 'conformance/agent-v1.4.0': {'pi-agent': ['upstream_v140', 'native_lifecycle'],
                              'pi-agent-schema': ['tool_result_json']},
 'conformance/agent-proxy-v1.4.0': {'pi-agent-loop': ['proxy']},
 'conformance/coding-tools': {'pi-coding-runtime': ['tools',
                                                    'target_tools',
                                                    'local_boundaries']},
 'conformance/coding-models': {'pi-coding-runtime': ['model_composition', 'model_defaults']},
 'conformance/coding-session-runtime-v1.4.0': {'pi-coding-runtime': ['session_runtime',
                                                                     'session_source',
                                                                     'bash_execution']},
 'conformance/coding-session': {'pi-coding-session': ['sessions', 'upstream_projection'],
                                'pi-sqlite': ['database']},
 'conformance/coding-config': {'pi-coding-runtime': ['settings_snapshot', 'session_runtime']},
 'conformance/current-extensions': {'pi-extension-protocol': ['activation',
                                                              'cross_language_echo',
                                                              'cross_language_profiles',
                                                              'current_host',
                                                              'endpoint_handshake',
                                                              'jcs',
                                                              'profile_live',
                                                              'profiles',
                                                              'schema_authority',
                                                              'tool_live',
                                                              'wire_echo_bounded',
                                                              'wire_echo_selected',
                                                              'wire_framing',
                                                              'wire_generated',
                                                              'wire_raw_framing',
                                                              'wire_value'],
                                    'pi-coding-runtime': ['extensions']},
 'conformance/coding-tool-adapters-v1.4.0': {'pi-coding-runtime': ['mcp_adapter',
                                                                   'codemode_adapter']},
 'conformance/mcp': {'pi-mcp': ['client', 'http', 'stdio', 'oauth']},
 'conformance/codemode': {'pi-codemode': ['sandbox']},
 'conformance/telemetry': {'pi-telemetry': ['contract', 'propagation']}}
# Every enrolled proof uses this reviewed locked/offline all-feature profile.
REVIEWED_PROOF_COMMANDS = tuple(
    (suite, ('cargo','test','--locked','--offline','-p',package,'--test',target,'--all-features'))
    for suite, packages in REVIEWED_PROOF_TARGETS.items()
    for package, targets in packages.items() for target in targets
)


@lru_cache(maxsize=None)
def _reviewed_proofs(root_string: str, revision: str, enrollment: tuple, cfg_platform: tuple):
    if __package__:
        from .upstream_objects import Objects
    else:
        from upstream_objects import Objects
    root = Path(root_string)
    objects = Objects(root)
    tree = objects.tree(revision)
    class GitTexts:
        def __contains__(self, path):
            return path in tree and path.startswith('crates/') and path.endswith('.rs')
        def __getitem__(self, path):
            require(path in self, 'reviewed Rust source missing: ' + path)
            return objects.blob(tree[path]).decode('utf-8')
    files, proofs, profiles = GitTexts(), [], {}
    for suite, argv in enrollment:
        command = list(argv)
        package, target = cargo_test_selector(command)
        manifest_path = f'crates/{package}/Cargo.toml'
        require(manifest_path in tree, 'reviewed Cargo profile missing: ' + manifest_path)
        manifest = tomllib.loads(objects.blob(tree[manifest_path]).decode('utf-8'))
        profiles[package] = manifest
        _, target, path, features = cargo_test_target(command, manifest)
        definitions, _ = rust_test_inventory(root/path, root, file_texts=files)
        enabled = [test for test in definitions if test_applicable(test, features, cfg_platform=cfg_platform)]
        require(bool(enabled), f'reviewed proof target has no enabled tests: {package}/{target}')
        for test in enabled:
            proofs.append((suite, package, target, str(test['path'].relative_to(root)),
                           test['qualified'], tuple(test['attributes'])))
    return tuple(proofs), profiles


def reviewed_proof_requirements(root: Path = ROOT):
    return _reviewed_proofs(str(root.resolve()), REVIEWED_PROOF_REVISION,
                            REVIEWED_PROOF_COMMANDS, REVIEWED_CFG_PLATFORM)


def validate_reviewed_proofs(registry: dict, root: Path = ROOT, *, suite_names: set[str] | None = None) -> None:
    """A regenerated current discovery cannot retire the immutable reviewed set."""
    required, profiles = reviewed_proof_requirements(root)
    suites = {row['id']:row for row in registry['suites']}
    features, checked = {}, set()
    for suite, argv in REVIEWED_PROOF_COMMANDS:
        if suite_names is not None and suite not in suite_names:
            continue
        require(suite in suites and list(argv) in suites[suite]['commands'],
                f'reviewed proof command/profile retired: {suite}: {list(argv)}')
        command = list(argv)
        package = command[command.index('-p') + 1]
        if package not in checked:
            current = tomllib.loads(public_file(root,f'crates/{package}/Cargo.toml').read_text())
            original = profiles[package]
            # Existing feature/dependency activations cannot vanish behind a rewrite.
            for feature, activations in original.get('features', {}).items():
                require(feature in current.get('features', {})
                        and set(activations) <= set(current['features'][feature]),
                        f'reviewed proof feature binding retired: {package}/{feature}')
            for kind in ('dependencies','dev-dependencies','build-dependencies'):
                for dependency, binding in original.get(kind, {}).items():
                    actual = current.get(kind, {}).get(dependency)
                    if isinstance(binding, dict) and isinstance(actual, dict):
                        expected_fields = {key:value for key,value in binding.items() if key != 'features'}
                        actual_fields = {key:value for key,value in actual.items() if key != 'features'}
                        same = expected_fields == actual_fields and set(binding.get('features', [])) <= set(actual.get('features', []))
                    else:
                        same = actual == binding
                    require(same, f'reviewed proof dependency binding changed: {package}/{kind}/{dependency}')
            features[package] = cargo_features(command,current)
            checked.add(package)
    for suite, package, target, path, name, attributes in required:
        if suite_names is not None and suite not in suite_names:
            continue
        evidence = suites[suite]['evidence']
        require(any(row.get('path') == path and row.get('anchor') == 'test:' + name
                    and row.get('cargo_target') == target for row in evidence),
                f'reviewed required proof identity retired: {suite}: {path}#{name}')
        require(test_applicable({'attributes':attributes}, features[package]),
                f'reviewed required proof disabled by current profile: {path}#{name}')


def adapter_proof_requirements(inventory: dict | None = None, root: Path = ROOT) -> set[tuple]:
    """Selected adapter requirements come from the immutable reviewed input."""
    required, _ = reviewed_proof_requirements(root)
    return {(package,target,path,name) for suite,package,target,path,name,_ in required
            if suite == 'conformance/provider-traces/v1.4.0'}


def validate_adapter_proofs(suite: dict, inventory: dict, root: Path = ROOT) -> None:
    require(suite['status'] == 'executable', 'selected adapter family must be executable')
    references = {(item['path'], item.get('anchor')) for item in suite['evidence']}
    checked = set()
    for package, target, path, name in sorted(adapter_proof_requirements(inventory, root)):
        commands = [command for command in suite['commands'] if command[:2] == ['cargo', 'test']
                    and cargo_test_selector(command) == (package, target)]
        require(bool(commands), f'retained adapter proof missing target: {package}/{target}')
        require((path, 'test:' + name) in references,
                f'retained adapter proof missing exact anchor: {path}#{name}')
        for command in commands:
            identity = tuple(command)
            if identity not in checked:
                check_command(command, suite['evidence'], root)
                checked.add(identity)


def validate_preserved_inputs(references: list[dict], root: Path = ROOT) -> None:
    """Retain historical fixture bytes and their immutable Pirua baseline origin."""
    try:
        if __package__:
            from .upstream_objects import Objects
        else:
            from upstream_objects import Objects
        seen = set()
        for reference in references:
            require(reference.get('source_revision') == HISTORICAL_SOURCE_REVISION,
                    'historical evidence origin changed: ' + reference.get('path', ''))
            require(reference['path'] not in seen, 'duplicate historical evidence identity')
            seen.add(reference['path'])
            path = public_file(root, reference['path'])
            require(hashlib.sha256(path.read_bytes()).hexdigest() == reference['sha256'],
                    'preserved source/fixture identity changed: ' + reference['path'])
        objects = Objects(root)
        tree = objects.tree(HISTORICAL_SOURCE_REVISION)
        for reference in references:
            require(reference['path'] in tree and hashlib.sha256(objects.blob(tree[reference['path']])).hexdigest() == reference['sha256'],
                    'historical evidence differs from immutable repository origin: ' + reference['path'])
    except (KeyError, OSError) as error:
        raise AssemblyError('historical evidence identity unavailable') from error


def check_assembly(map_path: Path, registry_path: Path, root: Path = ROOT) -> dict:
    """Join all target source paths/exports/tests/providers to static proof routes."""
    snapshot = projections.build_snapshot(map_path)
    expected = projections.outputs(snapshot)
    for path, content in expected.items():
        relative = path.relative_to(projections.ROOT)
        require((root/relative).read_text() == content, f'stale pinned manifest: {relative}')
    registry = load_json(registry_path)
    require(registry.get('source_commit') == snapshot.commit, 'proof registry source identity differs from pinned map')
    labels = {name for surface in snapshot.surfaces.values() for name in surface['conformance']}
    validate_reviewed_proofs(registry, root)
    suites = validate_registry(registry, labels, root)
    validate_adapter_proofs(suites['conformance/provider-traces/v1.4.0'], {}, root)
    if __package__:
        from .generate_assembly_registry import expected_registry
    else:
        from generate_assembly_registry import expected_registry
    require(registry == expected_registry(root), 'stale generated proof registry; review source changes before refresh')
    errors, resolved = [], []
    def obligation(surface, identity):
        record = snapshot.surfaces[surface]
        owner = record['primary_owner']
        require(owner in snapshot.registrations, f'{identity}: missing owner {owner}')
        if record['disposition'] == 'included':
            require(record['conformance'], f'{identity}: included source has no proof suite')
            for suite in record['conformance']:
                require(suite in suites, f'{identity}: missing proof suite {suite}')
                commands = suites[suite]['commands']
                crate = snapshot.registrations[owner]['crate']
                require(any(command[:2] == ['cargo', 'test'] and command[command.index('-p') + 1] == crate for command in commands),
                        f'{identity}: source owner {owner} has no concrete package test command')
        resolved.append({'id':identity, 'surface':surface, 'owner':owner})
    for path, route in snapshot.routes.items():
        obligation(route['surface'], 'path:' + path)
    for row in snapshot.inventory.values():
        if row['family'] in {'exports', 'tests', 'providers', 'generator-inputs'}:
            obligation(row['surface'], row['family'] + ':' + row['key'])
    validate_preserved_inputs(registry['preserved_inputs'], root)
    totals = {'watched_paths':len(snapshot.routes),
              'watched_packages':{row['id']:row['baseline_path_count'] for row in snapshot.data['watch']},
              'source_dispositions':dict(sorted(Counter(row['disposition'] for row in snapshot.surfaces.values()).items())),
              'upstream_test_paths':sum(row['family'] == 'tests' for row in snapshot.inventory.values()),
              'proof_suites':dict(sorted(Counter(row['status'] for row in suites.values()).items())),
              'proof_commands_defined':sum(len(row['commands']) for row in suites.values()),
              'proof_commands_executed':0}
    for package in ['ai', 'agent']:
        symbols = tomllib.loads(expected[projections.ROOT/f'upstream/manifests/{package}-exports.toml'])['symbol']
        totals[package + '_exports'] = {'occurrences':len(symbols), 'surfaces':dict(sorted(Counter(row['surface'] for row in symbols).items()))}
    providers = [row for row in snapshot.inventory.values() if row['family'] == 'providers']
    totals['provider_identities'] = dict(sorted(Counter(('selected_' if snapshot.surfaces[row['surface']]['disposition'] == 'included' else 'excluded_') + row['payload']['namespace'] for row in providers).items()))
    totals['catalog_provenance'] = snapshot.surfaces['ai.catalog.projection']['provenance_kind']
    return {'schema_version':1, 'source_commit':snapshot.commit, 'totals':totals,
            'unresolved':sorted(set(errors)), 'resolved':resolved}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--map", type=Path, default=ROOT / "upstream-map.toml")
    parser.add_argument("--registry", type=Path, default=ROOT / "conformance/assembly.json")
    parser.add_argument("--json", action="store_true", help="include every resolved obligation")
    args = parser.parse_args()
    try:
        result = check_assembly(args.map.resolve(), args.registry.resolve())
    except (AssemblyError, OSError, RuntimeError, ValueError, KeyError, IndexError, TypeError) as error:
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
