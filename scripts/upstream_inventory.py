"""Strict, inert Git-bound routing, selections and semantic inventories."""
from __future__ import annotations

from collections import defaultdict
import json
import base64
import hashlib
import io
import tarfile
import os
from pathlib import Path
import posixpath
import re
import subprocess
import tomllib

try:
    from .upstream_objects import Objects, Invalid, Unavailable, canonical, digest, hash_bytes
    from .validate_docs import compile_glob, markdown_headings, selector_line_set
except ImportError:
    from upstream_objects import Objects, Invalid, Unavailable, canonical, digest, hash_bytes
    from validate_docs import compile_glob, markdown_headings, selector_line_set

ROOT = Path(__file__).resolve().parents[1]
CRATES = set(tomllib.loads((ROOT / 'workspace-topology.toml').read_text())['production_crates'])
AUTHORITIES = {"target-spec", "source-and-tests", "generator", "generated-projection", "compatibility", "host-only", "disposable-scaffold"}
DISPOSITIONS = {"included", "outside-scope", "disposable"}
STATUSES = {"source-landed", "spec-only", "redesign", "excluded", "disposable"}


def indexed(rows: object, label: str) -> dict:
    if not isinstance(rows, list):
        raise Invalid(f"{label} must be an array")
    result = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get('id'), str) or not row['id'] or '\0' in row['id'] or row['id'] in result:
            raise Invalid(f"duplicate or invalid {label} identity")
        result[row['id']] = row
    return result


def strings(value: object, label: str, *, nonempty: bool = False) -> list:
    if not isinstance(value, list) or any(not isinstance(x, str) or not x for x in value) or len(set(value)) != len(value) or (nonempty and not value):
        raise Invalid(f"{label} must contain unique nonempty strings")
    return value


def validate_map(data: dict) -> None:
    if data.get('schema_version') != 2 or not isinstance(data.get('upstream'), dict):
        raise Invalid('upstream map schema_version must be 2')
    policy = data.get('policy', {})
    for key, expected in [('authority_classes', AUTHORITIES), ('dispositions', DISPOSITIONS), ('statuses', STATUSES)]:
        if set(strings(policy.get(key), f'policy.{key}')) != expected:
            raise Invalid(f'policy.{key} must equal the closed taxonomy')
    owners = indexed(data.get('owner'), 'owner')
    for row in owners.values():
        if row.get('kind') == 'rust' and row.get('crate') not in CRATES:
            raise Invalid('Rust owner names an unknown production crate')
        required = row.get('crate' if row.get('kind') == 'rust' else 'decision_ref')
        if row.get('kind') not in {'rust', 'outside-scope'} or not isinstance(required, str) or not required:
            raise Invalid('invalid owner registration')
    divergences = indexed(data.get('divergence', []), 'divergence')
    for row in divergences.values():
        if row.get('owner') not in owners or any(not isinstance(row.get(k), str) or not row[k] for k in ['upstream_behavior', 'rust_behavior', 'rationale', 'compatibility_consequence', 'replacement_fixture', 'revisit_trigger']):
            raise Invalid('incomplete divergence record')
    surfaces = indexed(data.get('surface'), 'surface')
    for row in surfaces.values():
        if row.get('authority') not in AUTHORITIES or row.get('disposition') not in DISPOSITIONS or row.get('status') not in STATUSES:
            raise Invalid('invalid surface taxonomy')
        if row['authority'] == 'host-only' and row['disposition'] != 'outside-scope':
            raise Invalid('host-only authority requires outside-scope disposition and owner')
        if row['authority'] == 'disposable-scaffold' and row['disposition'] != 'disposable':
            raise Invalid('disposable scaffold requires disposable disposition and Rust intake owner')
        owner = owners.get(row.get('primary_owner'))
        if owner is None or (owner['kind'] == 'outside-scope') != (row['disposition'] == 'outside-scope'):
            raise Invalid(f"surface {row['id']} has unknown or incompatible primary owner {row.get('primary_owner')}")
        if any(x not in owners for x in strings(row.get('collaborators'), 'collaborators')):
            raise Invalid('unknown collaborator')
        strings(row.get('sync'), 'sync', nonempty=True)
        strings(row.get('conformance'), 'conformance')
        if any(x not in divergences for x in strings(row.get('divergences'), 'divergences')):
            raise Invalid('unknown divergence')
        life = row.get('lifecycle', {})
        if not isinstance(life, dict) or any(not isinstance(life.get(k), str) or not life[k] for k in ['introduced_at', 'removal_condition', 'reclassification_condition']):
            raise Invalid('incomplete lifecycle')
        if row['authority'] == 'generated-projection':
            generator = surfaces.get(row.get('generated_by'), {})
            if generator.get('authority') != 'generator' or row.get('provenance_status') not in {'complete', 'missing-intake-snapshot'}:
                raise Invalid('invalid generated projection provenance or generator')
    watches = indexed(data.get('watch'), 'watch')
    roots = []
    for row in watches.values():
        root = row.get('root')
        if not isinstance(root, str) or not root or root.startswith('/') or '..' in root.split('/') or row.get('coverage') != 'complete' or row.get('unmatched') != 'error':
            raise Invalid('invalid complete watch')
        if any(root == p or root.startswith(p + '/') or p.startswith(root + '/') for p in roots):
            raise Invalid('overlapping watch roots')
        roots.append(root)
    routes = indexed(data.get('route'), 'route')
    for row in routes.values():
        if row.get('watch') not in watches or row.get('surface') not in surfaces or not isinstance(row.get('role'), str) or not row['role']:
            raise Invalid('invalid route references')
        strings(row.get('include'), 'route.include', nonempty=True)
        strings(row.get('exclude', []), 'route.exclude')
    for row in indexed(data.get('selector', []), 'selector').values():
        if row['id'] in {'file', 'ownership', 'surface-metadata', 'provenance'} or row['id'].startswith('inventory:'):
            raise Invalid('selector id uses a reserved comparison identity')
        if row.get('surface') not in surfaces or not row.get('path') or not row.get('partition'):
            raise Invalid('invalid selector references')
    seen_exclusions = set()
    for row in indexed(data.get('export_exclusion', []), 'export exclusion').values():
        if not isinstance(row, dict) or row.get('surface') not in surfaces or surfaces[row['surface']]['disposition'] != 'outside-scope' or not isinstance(row.get('source'), str):
            raise Invalid('invalid source-bound export exclusion')
        strings(row.get('names'), 'export exclusion names', nonempty=True)
        for name in row['names']:
            key = (row['source'], name)
            if key in seen_exclusions:
                raise Invalid('overlapping source-bound export exclusions')
            seen_exclusions.add(key)
    manifest_paths = set()
    for row in indexed(data.get('manifest', []), 'manifest').values():
        if row.get('kind') not in {'route-inventory', 'public-exports', 'provider-inventory', 'catalog-inputs', 'test-inventory', 'markdown-headings'} or any(not isinstance(row.get(k), str) or not row[k] for k in ['path', 'source']):
            raise Invalid('invalid or unsupported manifest definition')
        if row['path'].startswith('/') or '..' in row['path'].split('/') or row['path'] in manifest_paths:
            raise Invalid('invalid or duplicate manifest destination')
        manifest_paths.add(row['path'])


class Syntax:
    def __init__(self, objects: Objects, tree: dict, dependency_root: Path):
        self.objects, self.tree = objects, tree
        self.cache: dict = {}
        library = dependency_root / 'node_modules/typescript/lib/typescript.js'
        try:
            package = json.loads((library.parent.parent / 'package.json').read_bytes())
            lock_bytes = (dependency_root / 'node_modules/.package-lock.json').read_bytes()
            lock = json.loads(lock_bytes)['packages']['node_modules/typescript']
            if lock['version'] != package['version'] or not lock.get('integrity'):
                raise Invalid('TypeScript parser is not lock-bound')
            self.identity = {'version': package['version'], 'library': hash_bytes(library.read_bytes()), 'lock': hash_bytes(lock_bytes)}
        except (OSError, KeyError, ValueError) as error:
            raise Unavailable('installed lock-bound TypeScript parser unavailable') from error
        self.library = library

    def load(self, paths: list[str]) -> None:
        files = []
        for path in sorted(set(paths) - self.cache.keys()):
            files.append({'path': path, 'text': self.objects.text(self.tree[path])})
        if not files:
            return
        try:
            result = subprocess.run(['node', str(ROOT / 'scripts/upstream_ast.mjs'), str(self.library)], input=canonical({'files': files}), capture_output=True)
        except OSError as error:
            raise Unavailable('TypeScript syntax tool unavailable') from error
        if result.returncode:
            raise Unavailable('TypeScript syntax projection failed')
        try:
            for row in json.loads(result.stdout):
                self.cache[row['path']] = row
        except (ValueError, KeyError) as error:
            raise Unavailable('invalid syntax tool result') from error

    def get(self, path: str) -> dict:
        self.load([path])
        row = self.cache[path]
        if row['errors']:
            raise Invalid(f"unsupported syntax in {path}: {row['errors'][0]}")
        return row


def resolve(path: str, specifier: str, tree: dict) -> str | None:
    if not specifier.startswith('.'):
        return None
    base = posixpath.normpath(posixpath.join(posixpath.dirname(path), specifier))
    choices = [base]
    if base.endswith(('.js', '.mjs', '.cjs')):
        choices += [base.rsplit('.', 1)[0] + suffix for suffix in ('.ts', '.tsx', '.mts', '.cts')]
    if not base.endswith(('.ts', '.tsx', '.json', '.mts', '.cts')):
        choices += [base + suffix for suffix in ('.ts', '.tsx', '/index.ts', '.json')]
    return next((p for p in choices if p in tree), None)


class Snapshot:
    def __init__(self, objects: Objects, commit: str, data: dict, dependency_root: Path, *, baseline: bool):
        validate_map(data)
        self.objects, self.commit, self.data = objects, commit, data
        self.baseline = baseline
        self.tree = objects.tree(commit)
        self.surfaces = indexed(data['surface'], 'surface')
        self.registrations = indexed(data['owner'], 'owner')
        self.routes: dict = {}
        self.selections: dict = {}
        self.inventory: dict = {}
        self._symbol_cache: dict = {}
        self.errors: list[str] = []
        self.limitations: list[str] = []
        self.missing_inputs: dict = {}
        self.external_dependencies: dict = {}
        self._workspace_targets: dict = {}
        self._workspace_packages: list | None = None
        self.route_digests: dict = {}
        self.syntax = Syntax(objects, self.tree, dependency_root)
        self.route(baseline)
        self.select()
        self.project()
        self.inventory_digest = digest(self.inventory)

    def text(self, path: str) -> str:
        return self.objects.text(self.tree[path])

    def route(self, baseline: bool) -> None:
        counts = defaultdict(int)
        for watch in self.data['watch']:
            entries = [e for p, e in self.tree.items() if p == watch['root'] or p.startswith(watch['root'] + '/')]
            rows = []
            for e in entries:
                matches = [r for r in self.data['route'] if r['watch'] == watch['id'] and any(compile_glob(g).match(e.path) for g in r['include']) and not any(compile_glob(g).match(e.path) for g in r.get('exclude', []))]
                if len(matches) != 1:
                    self.errors.append(f'exactly-one-route required: {e.path} ({len(matches)} matches)')
                    continue
                r = matches[0]
                self.routes[e.path] = r
                counts[r['id']] += 1
                rows.append(f"{e.path}\0{e.oid}\0{r['id']}\0{r['surface']}\0{r['role']}\n".encode())
                if e.type != 'blob' or e.mode not in {'100644', '100755'}:
                    self.errors.append(f'unsupported object/mode: {e.path} {e.type} {e.mode}')
            dg = hash_bytes(b''.join(sorted(rows)))
            self.route_digests[watch['id']] = dg
            if baseline and (watch.get('baseline_path_count') != len(entries) or watch.get('route_digest') != dg):
                self.errors.append(f"baseline route count/digest mismatch: {watch['id']}")
        for route in self.data['route']:
            if not counts[route['id']]:
                self.errors.append(f"route matches no path; remove it explicitly: {route['id']}")
        self.syntax.load([p for p in self.routes if p.endswith(('.ts', '.tsx', '.mts', '.cts')) and self.tree[p].mode in {'100644', '100755'}])

    def select(self) -> None:
        selectors = self.data.get('selector', [])
        for row in selectors:
            path = row['path']
            route = self.routes.get(path)
            if not route or route.get('selector_partition') != row['partition']:
                self.errors.append(f"selector is not bound to its routed partition: {row['id']}")
        for path, route in self.routes.items():
            if not route.get('selector_partition'):
                self.selections[path] = {'file': {'surface': route['surface'], 'lines': [], 'digest': self.tree[path].oid}}
                continue
            try:
                text = self.text(path)
                lines = text.splitlines()
                selected_rows = [s for s in selectors if s['path'] == path and s['partition'] == route['selector_partition']]
                kinds = {s.get('kind') for s in selected_rows}
                if 'json-pointer' in kinds and kinds != {'json-pointer'}:
                    raise Invalid('JSON pointer partitions cannot mix text selections')
                selections = {}
                occupied = defaultdict(list)
                pointers = []
                for s in selected_rows:
                    kind = s.get('kind')
                    if kind in {'text-line', 'text-span', 'markdown-section'}:
                        if kind == 'markdown-section':
                            headings = markdown_headings(lines)
                            parent = [h for h in headings if h[2] == s.get('heading')]
                            if len(parent) != 1:
                                raise Invalid('heading must occur exactly once')
                            for exclusion in s.get('exclude_headings', []):
                                candidates = [h for h in headings if h[2] == exclusion and h[0] > parent[0][0]]
                                if len(candidates) != 1 or candidates[0][0] not in selector_line_set({k:v for k,v in s.items() if k != 'exclude_headings'}, lines):
                                    raise Invalid('excluded heading must occur exactly once inside its parent')
                        selected = selector_line_set(s, lines)
                        content = '\n'.join(lines[i] for i in sorted(selected))
                    elif kind == 'file':
                        selected, content = set(range(len(lines))), text
                    elif kind == 'typescript-symbol':
                        local = self.syntax.get(path)['locals'].get(s.get('symbol'))
                        if not local or (s.get('export_kind') and s['export_kind'] != local['kind']):
                            raise Invalid('TypeScript symbol or export kind missing')
                        selected = {i for span in local['spans'] for i in range(span['first'], span['last'] + 1)}
                        content = '\n'.join(span['text'] for span in local['spans'])
                        if s.get('declaration_hash') and s['declaration_hash'] != hash_bytes(content.encode()):
                            raise Invalid('TypeScript declaration hash mismatch')
                    elif kind == 'json-pointer':
                        pointer = s.get('pointer')
                        if not isinstance(pointer, str) or (pointer and not pointer.startswith('/')):
                            raise Invalid('invalid JSON pointer')
                        value = json.loads(text)
                        for token in pointer.split('/')[1:] if pointer else []:
                            token = token.replace('~1', '/').replace('~0', '~')
                            value = value[int(token)] if isinstance(value, list) else value[token]
                        if any(pointer == q or pointer.startswith(q + '/') or q.startswith(pointer + '/') or not q or not pointer for q in pointers):
                            raise Invalid('overlapping JSON pointers')
                        pointers.append(pointer)
                        selected, content = set(), canonical(value).decode()
                    else:
                        raise Invalid(f'unsupported selector kind: {kind}')
                    if s.get('content_hash') and s['content_hash'] != hash_bytes(content.encode()):
                        raise Invalid('selector content hash mismatch')
                    if not selected and kind != 'json-pointer':
                        raise Invalid('selector resolves to no lines')
                    for i in selected:
                        occupied[i].append(s['id'])
                    selections[s['id']] = {'surface': s['surface'], 'lines': sorted(selected), 'digest': hash_bytes(content.encode())}
                if not selections:
                    raise Invalid('missing selector partition')
                if pointers:
                    def leaves(value, pointer=''):
                        if isinstance(value, dict) and value:
                            return [p for k,v in value.items() for p in leaves(v, pointer+'/'+k.replace('~','~0').replace('/','~1'))]
                        if isinstance(value, list) and value:
                            return [p for i,v in enumerate(value) for p in leaves(v, pointer+'/'+str(i))]
                        return [pointer]
                    if any(sum(p == q or p.startswith(q + '/') or not q for q in pointers) != 1 for p in leaves(json.loads(text))):
                        raise Invalid('uncovered JSON content')
                elif any(line.strip() and len(occupied[i]) != 1 for i, line in enumerate(lines)):
                    raise Invalid('selector partition has uncovered or overlapping nonblank lines')
                self.selections[path] = selections
                if path.endswith('.md'):
                    self.add('headings', path, path, route['surface'], {'headings': [[level, title] for _, level, title in markdown_headings(lines)]})
            except (Invalid, ValueError, KeyError, IndexError) as error:
                self.errors.append(f'selector uncertainty: {path}: {error}')

    def owners(self, path: str, first: int | None = None, last: int | None = None) -> list[str]:
        if path not in self.routes:
            raise Invalid(f'semantic input outside watched/routed authority: {path}')
        selected = self.selections.get(path, {})
        if first is None:
            surfaces = {s['surface'] for s in selected.values()} or {self.routes[path]['surface']}
            surfaces.update(row['surface'] for row in self.data.get('export_exclusion', []) if row['source'] == path)
            return sorted(surfaces)
        return sorted({s['surface'] for s in selected.values() if not s['lines'] or any(first <= i <= last for i in s['lines'])})

    def add(self, family: str, key: str, path: str, surface: str, payload: dict) -> None:
        identity = family + ':' + key + ':' + surface
        if identity in self.inventory and self.inventory[identity]['payload'] != payload:
            raise Invalid('duplicate conflicting inventory identity: ' + identity)
        self.inventory[identity] = {'family': family, 'key': key, 'path': path, 'surface': surface, 'payload': payload}

    def workspace_target(self, specifier: str) -> str | None:
        if specifier in self._workspace_targets:
            return self._workspace_targets[specifier]
        packages = []
        if self._workspace_packages is None:
            self._workspace_packages = []
            for p in self.routes:
                if not p.endswith('/package.json'):
                    continue
                self._workspace_packages.append((p, json.loads(self.text(p))))
        for p, package in self._workspace_packages:
            if p.endswith('/package.json'):
                name = package.get('name')
                if isinstance(name, str) and (specifier == name or specifier.startswith(name + '/')):
                    packages.append((name, p, package))
        if not packages:
            self._workspace_targets[specifier] = None
            return None
        name, package_path, package = max(packages, key=lambda row:len(row[0]))
        subpath = '.' + specifier[len(name):]
        exports = package.get('exports', {})
        if isinstance(exports, str):
            exports = {'.':exports}
        if not isinstance(exports, dict):
            raise Invalid('unsupported workspace export map')
        matches = [(key, value) for key,value in exports.items() if compile_glob(key).match(subpath)]
        if len(matches) != 1:
            raise Invalid('unresolved or ambiguous workspace package subpath: ' + specifier)
        key, value = matches[0]
        if isinstance(value, dict):
            value = value.get('import', value.get('require'))
        if not isinstance(value, str) or not value.startswith('./'):
            raise Invalid('unsupported workspace package target: ' + specifier)
        if '*' in key:
            before, after = key.split('*', 1)
            concrete = subpath[len(before):len(subpath)-len(after) if after else None]
            value = value.replace('*', concrete)
        target = posixpath.dirname(package_path) + '/' + value[2:]
        target = target.replace('/dist/', '/src/', 1)
        if target.endswith('.js'):
            target = target[:-3] + '.ts'
        if target not in self.tree:
            raise Invalid('workspace source target missing: ' + specifier)
        self._workspace_targets[specifier] = target
        return target

    def dependency(self, path: str, specifier: str) -> str | None:
        target = resolve(path, specifier, self.tree) if specifier.startswith('.') else self.workspace_target(specifier)
        if specifier.startswith('.') and target is None:
            missing = posixpath.normpath(posixpath.join(posixpath.dirname(path), specifier))
            projection = self.surfaces.get('ai.catalog.projection', {})
            if missing.startswith('packages/ai/src/providers/data/') and missing.endswith('.json'):
                route = self.routes.get(path, {})
                surface = self.surfaces.get(route.get('surface'), {})
                if projection.get('provenance_kind') == 'release-artifact':
                    provider = posixpath.basename(missing)[:-5]
                    snapshot = projection.get('provenance_snapshot', {})
                    selected = provider in snapshot.get('source_ids', []) or provider == '.manifest'
                    if selected or surface.get('disposition') == 'outside-scope':
                        self.missing_inputs[path + '::' + specifier] = self.tree[path].oid
                        self.external_dependencies[path + '::' + specifier] = {
                            'kind':'released-catalog-data' if selected else 'owner-excluded-catalog-data',
                            'specifier':specifier, 'git_path':missing, 'git_blob_exists':False,
                            'association':snapshot if selected else {}, 'manifests':[], 'lock':None}
                        return None
                if projection.get('provenance_status') == 'missing-intake-snapshot':
                    self.missing_inputs[path + '::' + specifier] = self.tree[path].oid
                    self.limitations.append('historical missing catalog input: ' + missing)
                    return None
            raise Invalid(f'unresolved relative dependency: {path} -> {specifier}')
        if not specifier.startswith('.'):
            parent = posixpath.dirname(path)
            manifests = []
            while True:
                manifest = (parent + '/' if parent else '') + 'package.json'
                if manifest in self.tree:
                    manifests.append(self.tree[manifest].payload())
                if not parent:
                    break
                parent = posixpath.dirname(parent)
            lock = self.tree.get('package-lock.json')
            self.external_dependencies[path + '::' + specifier] = {'specifier':specifier, 'manifests':manifests, 'lock':lock.payload() if lock else None, 'kind':'node-builtin' if specifier.startswith('node:') else 'external-module'}
        return target

    def closure(self, path: str) -> list[str]:
        pending, visited = [path], set()
        generator = self.surfaces[self.routes[path]['surface']]['authority'] == 'generator'
        while pending:
            current = pending.pop()
            if current in visited:
                continue
            visited.add(current)
            if (current != path and current in self.routes
                    and self.surfaces[self.routes[current]['surface']]['disposition'] == 'outside-scope'):
                continue
            if current.endswith(('.ts', '.tsx', '.mts', '.cts')):
                syntax = self.syntax.get(current)
                for row in syntax['imports'] + [e for e in syntax['exports'] if e.get('specifier')]:
                    target = self.dependency(current, row['specifier'])
                    if target and target not in self.routes and generator:
                        raise Invalid(f'generator semantic dependency outside watches: {current} -> {target}')
                    if target and target not in visited:
                        pending.append(target)
        return sorted(visited)

    def symbols(self, path: str, active: set | None = None) -> dict:
        if path in self._symbol_cache:
            return self._symbol_cache[path]
        active = set() if active is None else set(active)
        if path in active:
            raise Invalid(f'cyclic public export graph requires review: {path}')
        active.add(path)
        syntax = self.syntax.get(path)
        result = {}
        def merge(name, value):
            if name in result and result[name]['origin'] != value['origin']:
                raise Invalid(f'ambiguous export origin: {path}::{name}')
            if name in result and result[name]['kind'] != value['kind']:
                value = {**value, 'kind': 'type-and-value'}
            result[name] = value
        def local(name):
            row = syntax['locals'].get(name)
            if row:
                return {'kind':row['kind'], 'origin':path, 'oid':self.tree[path].oid,
                        'declaration': hash_bytes(canonical([s['text'] for s in row['spans']])),
                        'first':min(s['first'] for s in row['spans']), 'last':max(s['last'] for s in row['spans'])}
            for imp in syntax['imports']:
                for binding in imp['bindings']:
                    if binding['local'] == name:
                        return imported(imp['specifier'], binding['name'], binding.get('type', False))
            raise Invalid(f'unresolved local public export: {path}::{name}')
        def imported(specifier, name, is_type=False):
            target = self.dependency(path, specifier)
            if target is None:
                released = self.external_dependencies.get(path + '::' + specifier, {})
                if released.get('kind') in {'released-catalog-data', 'owner-excluded-catalog-data'}:
                    return {'kind':'value', 'origin':'artifact:' + released['git_path'], 'oid':'',
                            'declaration':digest(released), 'first':0, 'last':0}
                if specifier != 'typebox' or name not in {'Static', 'TSchema', 'Type'}:
                    raise Invalid(f'external public symbol requires an explicit projection: {specifier}::{name}')
                if name == '*':
                    raise Invalid(f'external wildcard export requires explicit projection: {specifier}')
                return {'kind': 'type' if is_type else 'value', 'origin':'external:' + specifier + '::' + name, 'oid':'', 'declaration':'', 'first':0, 'last':0}
            if target.endswith('.json'):
                json.loads(self.text(target))
                return {'kind':'value', 'origin':target, 'oid':self.tree[target].oid, 'declaration':self.tree[target].oid, 'first':0, 'last':len(self.text(target).splitlines())}
            if name == '*':
                return {'kind':'type-and-value', 'origin':target, 'oid':self.tree[target].oid, 'declaration':digest(self.symbols(target, active)), 'first':0, 'last':len(self.text(target).splitlines())}
            value = self.symbols(target, active).get(name)
            if value is None:
                raise Invalid(f'missing named export: {target}::{name}')
            return {**value, 'kind':'type' if is_type else value['kind']}
        for e in syntax['exports']:
            form = e['form']
            if form == 'star':
                target = self.dependency(path, e['specifier'])
                if target is None:
                    raise Invalid('external star export requires explicit projection')
                for name, value in self.symbols(target, active).items():
                    if name != 'default':
                        merge(name, {**value, 'kind':'type' if e.get('type') else value['kind']})
            elif form == 'namespace':
                merge(e['name'], imported(e['specifier'], '*'))
            elif form == 'named':
                value = imported(e['specifier'], e['source'], e.get('type', False)) if e['specifier'] else local(e['source'])
                merge(e['name'], {**value, 'kind':'type' if e.get('type') else value['kind']})
            elif form == 'local':
                merge(e['name'], local(e['source']))
            elif form == 'expression':
                merge(e['name'], {'kind':e['kind'], 'origin':path, 'oid':self.tree[path].oid, 'declaration':hash_bytes(e['text'].encode()), 'first':e['first'], 'last':e['last']})
        for ambient in syntax['ambient']:
            nested = ambient['projection']
            if nested['errors']:
                raise Invalid('unsupported ambient module syntax: ' + path)
            for row in nested['exports']:
                if row['form'] not in {'local', 'expression', 'named'} or row.get('specifier'):
                    raise Invalid('unsupported ambient export dependency: ' + path)
                declaration = nested['locals'].get(row.get('source'))
                kind = 'type' if row.get('type') else declaration['kind'] if declaration else row.get('kind', 'value')
                name = row['name']
                if name in result:
                    raise Invalid('ambiguous ambient/file export occurrence: ' + path)
                result[name] = {'kind':kind, 'origin':path, 'oid':self.tree[path].oid,
                                'declaration':hash_bytes(ambient['text'].encode()),
                                'first':ambient['first'], 'last':ambient['last'],
                                'ambient_module':ambient['module']}
        self._symbol_cache[path] = result
        return result

    def exports(self, package_path: str) -> None:
        root = posixpath.dirname(package_path)
        exports = json.loads(self.text(package_path)).get('exports')
        if exports is None:
            return
        if isinstance(exports, str):
            exports = {'.': exports}
        if not isinstance(exports, dict) or not all(k.startswith('.') for k in exports):
            raise Invalid('unsupported package export map')
        for subpath, target in sorted(exports.items()):
            original = target
            if isinstance(target, dict):
                candidates = {v for k,v in target.items() if k != 'types' and isinstance(v, str)}
                if len(candidates) != 1 or any(not isinstance(v, str) for v in target.values()):
                    raise Invalid(f'unsupported conditional package export: {package_path} {subpath}')
                target = next(iter(candidates))
            if not isinstance(target, str) or not target.startswith('./'):
                raise Invalid('unsupported package export target')
            for surface in self.owners(package_path):
                self.add('subpaths', package_path + '::' + subpath, package_path, surface, {'target':original})
            if target.endswith('.json') and '*' not in target:
                if root + '/' + target[2:] not in self.tree:
                    raise Invalid('package JSON export target missing')
                continue
            pattern = root + '/' + target[2:]
            pattern = pattern.replace('/dist/', '/src/', 1)
            if pattern.endswith('.js'):
                pattern = pattern[:-3] + '.ts'
            sources = sorted(p for p in self.tree if compile_glob(pattern).match(p))
            if not sources:
                raise Invalid(f'package export source missing: {package_path} {subpath}')
            for source in sources:
                wildcard = ''
                if '*' in pattern:
                    before, after = pattern.split('*', 1)
                    wildcard = source[len(before):len(source)-len(after) if after else None]
                concrete = subpath.replace('*', wildcard)
                for name, value in sorted(self.symbols(source).items()):
                    origin = value['origin']
                    source_surface = self.routes[source]['surface']
                    if self.surfaces[source_surface]['disposition'] == 'outside-scope':
                        # An excluded public entrance stays excluded when it reexports a shared type.
                        owners = [source_surface]
                    else:
                        owners = self.owners(source) if origin.startswith(('external:', 'artifact:')) else self.owners(origin, value['first'], value['last'])
                        for exclusion in self.data.get('export_exclusion', []):
                            if origin == exclusion['source'] and name in exclusion['names']:
                                owners = [exclusion['surface']]
                    if not owners:
                        raise Invalid('public symbol has no selector owner')
                    for surface in owners:
                        self.add('exports', package_path + '::' + concrete + '::' + name, source, surface, {'subpath':concrete, 'name':name, **value})

    def providers(self) -> None:
        path = 'packages/ai/src/providers/all.ts'
        if path not in self.routes:
            self.faux_provider()
            return
        syntax = self.syntax.get(path)
        for namespace, name in [('text', 'builtinProviders'), ('image', 'builtinImagesProviders')]:
            function = syntax['functions'].get(name)
            if function is None and namespace == 'image':
                registration = 'packages/ai/src/providers/images/register-builtins.ts'
                if registration not in self.routes:
                    raise Invalid('provider registration inventory incomplete: ' + name)
                parsed = self.syntax.get(registration)
                fn = parsed['functions'].get('registerBuiltInImagesApiProviders', {})
                if len(fn.get('registrations', [])) != 1:
                    raise Invalid('image API registration projection incomplete')
                properties = fn['registrations'][0]
                api = properties.get('api', '')
                literal = re.fullmatch(r'''(['"])(.*?)\1''', api)
                if not literal:
                    raise Invalid('image API registration id is not a literal')
                self.add('providers', 'image::' + literal[2], registration, self.routes[registration]['surface'], {'namespace':'image-api', 'factory':'registerBuiltInImagesApiProviders', 'properties':properties, 'source':self.tree[registration].payload()})
                continue
            if function is None or len(function['returns']) != 1 or any(x is None for x in function['returns'][0]):
                raise Invalid(f'provider registration inventory incomplete: {name}')
            factories = function['returns'][0]
            if len(set(factories)) != len(factories):
                raise Invalid('duplicate provider registration')
            for factory in factories:
                bindings = [(i['specifier'], b['name']) for i in syntax['imports'] for b in i['bindings'] if b['local'] == factory]
                if len(bindings) != 1:
                    raise Invalid('unresolved provider registration')
                source = self.dependency(path, bindings[0][0])
                if source is None:
                    raise Invalid('external provider registration')
                parsed = self.syntax.get(source)
                fn = parsed['functions'].get(bindings[0][1])
                if not fn or fn.get('providerErrors') or len(fn['registrations']) != 1:
                    raise Invalid(f'provider factory projection incomplete: {source} {factory}')
                properties = fn['registrations'][0]
                provider = properties.get('id', '')
                literal = re.fullmatch(r'([\'"])(.*?)\1', provider)
                if not literal:
                    local = parsed['locals'].get(provider)
                    defaults = re.findall(r'\bconst\s+' + re.escape(provider) + r'\s*=.*?\?\?\s*[\'"]([^\'"]+)[\'"]', '\n'.join(s['text'] for s in local['spans']) if local else fn['text'])
                    if len(set(defaults)) != 1:
                        raise Invalid(f'provider id unresolved: {source} {factory}')
                    provider_id = defaults[0]
                else:
                    provider_id = literal[2]
                text = self.text(source)
                apis = sorted(set(re.findall(r'Provider<[\'"]([^\'"]+)[\'"]>', fn['text']) + re.findall(r'[\'"]([a-z0-9-]+)[\'"]\s*:', properties.get('api', ''))))
                for surface in self.owners(source):
                    self.add('providers', namespace + '::' + provider_id, source, surface, {'namespace':namespace, 'id':provider_id, 'factory':factory, 'registration_oid':self.tree[path].oid, 'source_oid':self.tree[source].oid, 'properties':properties, 'api_ids':apis, 'oauth_flows':sorted(set(re.findall(r'\bload\w*OAuth\b', text))), 'catalog':properties.get('models', '')})
        self.faux_provider()

    def faux_provider(self) -> None:
        faux = 'packages/ai/src/providers/faux.ts'
        if faux in self.routes:
            parsed = self.syntax.get(faux)
            if 'fauxProvider' not in parsed['functions']:
                raise Invalid('public Faux factory missing')
            identities = {}
            for constant in ['DEFAULT_PROVIDER', 'DEFAULT_API']:
                expression = parsed['locals'].get(constant, {}).get('initializer', '')
                literal = re.fullmatch(r'([\'"])(.*?)\1', expression)
                if not literal:
                    raise Invalid('public Faux identity is not a declared literal: ' + constant)
                identities[constant] = literal[2]
            for surface in self.owners(faux):
                self.add('providers', 'faux-testkit::' + identities['DEFAULT_PROVIDER'], faux, surface, {'namespace':'faux-testkit', 'id':identities['DEFAULT_PROVIDER'], 'api_ids':[identities['DEFAULT_API']], 'oid':self.tree[faux].oid, 'factory':'fauxProvider'})

    def validate_release_provenance(self, surface: dict) -> None:
        """Bind absent Git model data to an archived release, never fabricate blobs."""
        snapshot = surface.get('provenance_snapshot', {})
        if surface.get('provenance_status') != 'complete' or snapshot.get('commit') != self.commit:
            raise Invalid('release provenance must bind the exact source commit')
        def read(key, expected):
            value = snapshot.get(key)
            if not isinstance(value, str) or value.startswith('/') or '..' in value.split('/'):
                raise Invalid('invalid release provenance path')
            try:
                payload = (ROOT / value).read_bytes()
            except OSError as error:
                raise Invalid('missing released artifact input: ' + value) from error
            if hash_bytes(payload) != expected:
                raise Invalid('release artifact hash mismatch: ' + value)
            return payload
        archive = read('archive', snapshot.get('sha256'))
        metadata = json.loads(read('manifest', snapshot.get('manifest_sha256')))
        attestations = json.loads(read('attestations', snapshot.get('attestations_sha256')))
        if metadata.get('upstreamCommit') != self.commit or metadata.get('sha256') != snapshot.get('sha256', '')[7:]:
            raise Invalid('release source association mismatch')
        integrity = base64.b64decode(metadata['integrity'].removeprefix('sha512-'))
        if hashlib.sha512(archive).digest() != integrity:
            raise Invalid('release integrity mismatch')
        statements = [json.loads(base64.b64decode(row['bundle']['dsseEnvelope']['payload']))
                      for row in attestations['attestations'] if row['predicateType'] == 'https://slsa.dev/provenance/v1']
        if len(statements) != 1 or statements[0]['predicate']['buildDefinition']['resolvedDependencies'][0]['digest']['gitCommit'] != self.commit or statements[0]['subject'][0]['digest']['sha512'] != integrity.hex():
            raise Invalid('release SLSA source association mismatch')
        if sorted(metadata['files']) != sorted(snapshot.get('source_ids', [])):
            raise Invalid('release provider source identity mismatch')
        with tarfile.open(fileobj=io.BytesIO(archive), mode='r:gz') as package:
            for provider, evidence in metadata['files'].items():
                payload = package.extractfile('package/' + evidence['packagePath']).read()
                if hash_bytes(payload) != 'sha256:' + evidence['packageDataSha256']:
                    raise Invalid('released catalog input hash mismatch')
                git_path = 'packages/ai/src/providers/data/' + provider + '.json'
                if git_path in self.tree:
                    raise Invalid('release association expects absent Git data; review new Git authority')

    def validate_provenance(self, surface: dict) -> None:
        snapshot = surface.get('provenance_snapshot', {})
        if surface.get('provenance_kind') == 'release-artifact':
            self.validate_release_provenance(surface)
            return
        if surface.get('provenance_status') != 'complete' or not isinstance(snapshot, dict) or not isinstance(snapshot.get('id'), str) or not snapshot['id']:
            raise Invalid('new generated inputs/output lack complete intake provenance')
        strings(snapshot.get('source_ids'), 'provenance source_ids', nonempty=True)
        source_inputs, outputs = snapshot.get('inputs'), snapshot.get('outputs')
        if not isinstance(source_inputs, dict) or not isinstance(outputs, dict):
            raise Invalid('provenance input/output maps missing')
        required_inputs = set()
        for row in self.inventory.values():
            if row['family'] == 'generator-inputs' and row['surface'] == surface['generated_by']:
                required_inputs.update(e['path'] for e in row['payload']['closure'])
                for external in row['payload']['external_dependencies'].values():
                    required_inputs.update(e['path'] for e in external['manifests'])
                    if external['lock']:
                        required_inputs.add(external['lock']['path'])
        required_outputs = {p for p,r in self.routes.items() if r['surface'] == surface['id']}
        if not required_inputs or not required_outputs or set(source_inputs) != required_inputs or set(outputs) != required_outputs:
            raise Invalid('provenance does not bind complete extracted input/output closure')
        for path, expected in {**source_inputs, **outputs}.items():
            if path not in self.tree or hash_bytes(self.objects.blob(self.tree[path])) != expected:
                raise Invalid('provenance source/output hash mismatch')
        capture = snapshot.get('capture')
        if not isinstance(capture, str) or capture not in self.tree or hash_bytes(self.objects.blob(self.tree[capture])) != snapshot.get('sha256'):
            raise Invalid('provenance capture bytes missing or mismatched')

    def project(self) -> None:
        package_sources = {m['source'].split('+')[0] for m in self.data.get('manifest', []) if m.get('kind') == 'public-exports'}
        package_sources.update(p for p in ['packages/ai/package.json', 'packages/agent/package.json'] if p in self.routes)
        for path in package_sources:
            if path not in self.tree or path not in self.routes:
                self.errors.append('public manifest source missing or outside routed authority: ' + path)
        operations = [(lambda p=p: self.exports(p)) for p in sorted(package_sources) if p in self.routes]
        operations.append(self.providers)
        for exclusion in self.data.get('export_exclusion', []):
            path = exclusion['source']
            if path not in self.routes or not set(exclusion['names']) <= self.symbols(path).keys():
                self.errors.append('source-bound export exclusion is stale or unresolved: ' + path)
        for path, route in sorted(self.routes.items()):
            self.add('routes', path, path, route['surface'], {**self.tree[path].payload(), 'route':route['id'], 'role':route['role']})
            is_test = '/test/' in path or '/tests/' in path or path.endswith(('.test.ts', '.spec.ts'))
            generator = self.surfaces[route['surface']]['authority'] == 'generator'
            if is_test or generator:
                if self.surfaces[route['surface']]['disposition'] == 'outside-scope':
                    self.add('tests', path, path, route['surface'], {'closure':[self.tree[path].payload()], 'external_dependencies':{}, 'classification':'owner-excluded-source-only'})
                    continue
                def closure_item(path=path, is_test=is_test, generator=generator):
                    closure = self.closure(path)
                    for family in (['tests'] if is_test else []) + (['generator-inputs'] if generator else []):
                        for surface in self.owners(path):
                            self.add(family, path, path, surface, {'closure':[{**self.tree[p].payload(), 'surfaces':self.owners(p) if p in self.routes else [], 'boundary':'watched' if p in self.routes else 'external-evidence'} for p in closure], 'external_dependencies':{k:v for k,v in sorted(self.external_dependencies.items()) if k.split('::')[0] in closure}, 'classification':'routed-import-closure'})
                operations.append(closure_item)
        for operation in operations:
            try:
                operation()
            except (Invalid, ValueError, KeyError) as error:
                self.errors.append(f'inventory uncertainty: {error}')
        for surface in self.surfaces.values():
            if surface.get('provenance_status') == 'complete':
                try:
                    self.validate_provenance(surface)
                except Invalid as error:
                    self.errors.append('generated provenance: ' + surface['id'] + ': ' + str(error))
            if surface.get('provenance_status') == 'missing-intake-snapshot':
                self.limitations.append(f"historical missing intake snapshot: {surface['id']}")

    def manifest(self) -> dict:
        return {'schema_version':1, 'source_commit':self.commit, 'map_digest':digest(self.data),
                'route_digests':self.route_digests, 'inventory_digest':self.inventory_digest,
                'parser':self.syntax.identity, 'inventory':self.inventory,
                'selections':self.selections, 'limitations':sorted(self.limitations)}
