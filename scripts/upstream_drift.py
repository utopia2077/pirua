#!/usr/bin/env python3
"""Compare local upstream commits and validate explicit review before emitting a patch."""
from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from copy import deepcopy
import difflib
import hashlib
import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tomllib

try:
    from .upstream_objects import Objects, Invalid, Unavailable, canonical, digest, hash_bytes, local_path
    from .upstream_inventory import Snapshot, indexed, validate_map, ROOT
except ImportError:
    from upstream_objects import Objects, Invalid, Unavailable, canonical, digest, hash_bytes, local_path
    from upstream_inventory import Snapshot, indexed, validate_map, ROOT

DISPOSITIONS = {'implemented', 'fixture-updated', 'intentional-divergence', 'host-only-reviewed', 'disposable-reviewed', 'generated-refresh', 'responsibility-migration', 'no-semantic-change'}
TOOLS = ['scripts/upstream_objects.py', 'scripts/upstream_inventory.py', 'scripts/upstream_ast.mjs', 'scripts/upstream_drift.py', 'scripts/validate_docs.py', 'scripts/upstream.py', 'scripts/generate_upstream_manifests.py', 'scripts/generate_assembly_registry.py', 'workspace-topology.toml']


def load_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, ValueError) as error:
        raise Invalid(f'invalid JSON input: {path.name}') from error
    if not isinstance(value, dict) or type(value.get('schema_version')) is not int or value.get('schema_version') != 1:
        raise Invalid('JSON input schema_version must be 1')
    return value


def overlay_map(base: dict, old: str, new: str, overlay: dict | None) -> dict:
    data = deepcopy(base)
    data['upstream']['commit'] = new
    if overlay is None:
        return data
    if overlay.get('schema_version') != 1 or overlay.get('old_commit') != old or overlay.get('new_commit') != new or overlay.get('base_map_digest') != digest(base):
        raise Invalid('stale or unbound intake overlay')
    allowed = {'schema_version', 'old_commit', 'new_commit', 'base_map_digest', 'replace', 'intake', 'inventory_digest', 'ownership'}
    if set(overlay) - allowed:
        raise Invalid('unknown overlay field')
    replacements = overlay.get('replace', {})
    if not isinstance(replacements, dict) or set(replacements) - {'owner', 'surface', 'route', 'selector', 'divergence', 'manifest', 'export_exclusion'}:
        raise Invalid('overlay cannot alter watches or pin history')
    for key, rows in replacements.items():
        indexed(rows, key)
        data[key] = deepcopy(rows)
    validate_map(data)
    return data


def item_id(old: str, new: str, kind: str, old_path: str, new_path: str, old_oid: str, new_oid: str, selector: str, surface: str) -> str:
    fields = [old, new, kind, old_path, new_path, old_oid, new_oid, selector, surface]
    return 'sha256:' + hashlib.sha256('\0'.join(fields).encode()).hexdigest()


class Comparison:
    def __init__(self, objects: Objects, base: dict, old: str, new: str, dependency_root: Path, overlay: dict | None = None):
        objects.commit(old)
        objects.commit(new)
        if base.get('upstream', {}).get('commit') != old:
            raise Invalid('old commit does not bind the base map')
        validate_map(base)
        self.base, self.effective = base, overlay_map(base, old, new, overlay)
        old_public_sources = {m['source'].split('+')[0] for m in base.get('manifest', []) if m.get('kind') == 'public-exports'}
        new_public_sources = {m['source'].split('+')[0] for m in self.effective.get('manifest', []) if m.get('kind') == 'public-exports'}
        if any(path in objects.tree(new) and path not in new_public_sources for path in old_public_sources):
            raise Invalid('existing public package inventory cannot be hidden by manifest removal or redirection')
        self.old = Snapshot(objects, old, base, dependency_root, baseline=True)
        self.new = self.old if old == new and self.effective == base else Snapshot(objects, new, self.effective, dependency_root, baseline=False)
        self.overlay = overlay or {}
        self.items: dict = {}
        self.errors = [f'old: {e}' for e in self.old.errors] + [f'new: {e}' for e in self.new.errors]
        self.pending_intake: list[str] = []
        self.intake()
        self.compare()
        for key, oid in self.new.missing_inputs.items():
            if self.old.missing_inputs.get(key) != oid:
                self.errors.append('changed unresolved generated input: ' + key)
        self.provenance()
        for item in self.items.values():
            item['uncertainty'] = sorted(e for e in self.errors if any(p and p in e for p in [item['old_path'], item['new_path']]))
        self.report = {'schema_version':1, 'old_commit':old, 'new_commit':new,
                       'base_map_digest':digest(base), 'effective_map_digest':digest(self.effective),
                       'old_inventory_digest':self.old.inventory_digest, 'new_inventory_digest':self.new.inventory_digest,
                       'parser':self.new.syntax.identity, 'overlay_digest':digest(overlay) if overlay else '',
                       'items':sorted(self.items.values(), key=lambda i:i['id']),
                       'errors':sorted(set(self.errors)), 'pending_intake':sorted(self.pending_intake),
                       'historical_limitations':sorted(set(self.old.limitations)),
                       'candidate_limitations':sorted(set(self.new.limitations))}
        self.report['report_digest'] = digest(self.report)

    def watched(self, snapshot: Snapshot) -> set[str]:
        return {p for p in snapshot.tree if any(p == w['root'] or p.startswith(w['root'] + '/') for w in snapshot.data['watch'])}

    def intake(self) -> None:
        additions = self.watched(self.new) - self.watched(self.old)
        rows = self.overlay.get('intake', [])
        if not isinstance(rows, list):
            raise Invalid('intake must be an array')
        confirmed = set()
        for row in rows:
            if not isinstance(row, dict) or set(row) != {'path', 'route', 'surface', 'selectors'}:
                raise Invalid('intake requires path, route, surface and selectors')
            path = row['path']
            route = self.new.routes.get(path, {})
            selected = sorted(k for k in self.new.selections.get(path, {}) if k != 'file')
            if path not in additions or path in confirmed or row['route'] != route.get('id') or row['surface'] != route.get('surface') or row['selectors'] != selected:
                raise Invalid('intake does not bind a unique extracted candidate path')
            confirmed.add(path)
        self.pending_intake = sorted(additions - confirmed)
        bound_digest = self.overlay.get('inventory_digest')
        if bound_digest is not None and bound_digest != self.new.inventory_digest:
            raise Invalid('overlay inventory digest does not match extracted candidate')

    def owner_registrations(self, snapshot: Snapshot, surfaces: list[str]) -> dict:
        owners = {owner for surface in surfaces for owner in [snapshot.surfaces[surface]['primary_owner'], *snapshot.surfaces[surface]['collaborators']]}
        return {owner:snapshot.registrations[owner] for owner in sorted(owners)}

    def add(self, kind: str, op: str, np: str, selector: str, surface: str, *, reason: str, old_value=None, new_value=None) -> None:
        oe, ne = self.old.tree.get(op), self.new.tree.get(np)
        oid, nid = oe.oid if oe else '', ne.oid if ne else ''
        ident = item_id(self.old.commit, self.new.commit, kind, op, np, oid, nid, selector, surface)
        old_owners = self.old.owners(op) if op in self.old.routes else []
        new_owners = self.new.owners(np) if np in self.new.routes else []
        if op and isinstance(old_value, dict) and old_value.get('surface') == surface:
            old_owners = sorted(set(old_owners + [surface]))
        if np and isinstance(new_value, dict) and new_value.get('surface') == surface:
            new_owners = sorted(set(new_owners + [surface]))
        old_metadata = self.old.surfaces.get(surface, {}) if surface in old_owners else {}
        new_metadata = self.new.surfaces.get(surface, {}) if surface in new_owners else {}
        metadata = new_metadata or old_metadata
        value = {'id':ident, 'kind':kind, 'old_path':op, 'new_path':np, 'old_oid':oid, 'new_oid':nid,
                 'old_mode':oe.mode if oe else '', 'new_mode':ne.mode if ne else '',
                 'selector_id':selector, 'surface_id':surface, 'authority':metadata['authority'],
                 'primary_owner':metadata['primary_owner'], 'collaborators':sorted(set(old_metadata.get('collaborators', []) + new_metadata.get('collaborators', []))),
                 'sync':sorted(set(old_metadata.get('sync', []) + new_metadata.get('sync', []))), 'conformance':sorted(set(old_metadata.get('conformance', []) + new_metadata.get('conformance', []))),
                 'lifecycle':metadata['lifecycle'], 'divergences':metadata['divergences'],
                 'old_surfaces':old_owners, 'new_surfaces':new_owners,
                 'old_ownership':{s:self.old.surfaces[s]['primary_owner'] for s in old_owners},
                 'new_ownership':{s:self.new.surfaces[s]['primary_owner'] for s in new_owners},
                 'old_owner_registrations':self.owner_registrations(self.old, old_owners),
                 'new_owner_registrations':self.owner_registrations(self.new, new_owners),
                 'reasons':[reason], 'uncertainty':[], 'old_value':old_value, 'new_value':new_value}
        if np and np not in self.old.tree:
            value['reasons'].append('new-path')
        existing = self.items.get(ident)
        if existing:
            existing['reasons'] = sorted(set(existing['reasons'] + value['reasons']))
        else:
            self.items[ident] = value

    def compare(self) -> None:
        old_paths, new_paths = self.watched(self.old), self.watched(self.new)
        deleted, added = old_paths - new_paths, new_paths - old_paths
        old_by_blob, new_by_blob = defaultdict(list), defaultdict(list)
        for p in deleted:
            old_by_blob[self.old.tree[p].oid].append(p)
        for p in added:
            new_by_blob[self.new.tree[p].oid].append(p)
        changes = []
        for oid in sorted(old_by_blob.keys() & new_by_blob.keys()):
            if len(old_by_blob[oid]) == len(new_by_blob[oid]) == 1:
                op, np = old_by_blob[oid][0], new_by_blob[oid][0]
                changes.append(('R', op, np))
                deleted.remove(op)
                added.remove(np)
        changes += [('D', p, '') for p in sorted(deleted)] + [('A', '', p) for p in sorted(added)]
        changes += [('M', p, p) for p in sorted(old_paths & new_paths) if self.old.tree[p] != self.new.tree[p] or self.old.routes.get(p) != self.new.routes.get(p) or self.old.selections.get(p) != self.new.selections.get(p)]
        for kind, op, np in changes:
            for snapshot, path in [(self.old, op), (self.new, np)]:
                if path and path in snapshot.tree:
                    try:
                        snapshot.objects.text(snapshot.tree[path])
                        if path.endswith(('.ts', '.tsx', '.mts', '.cts')):
                            snapshot.syntax.get(path)
                    except Invalid as error:
                        self.errors.append('file uncertainty: ' + str(error))
            old_select, new_select = self.old.selections.get(op, {}), self.new.selections.get(np, {})
            emitted = False
            for selector in sorted(old_select.keys() | new_select.keys()):
                a, b = old_select.get(selector), new_select.get(selector)
                if kind != 'M' or a is None or b is None or a['digest'] != b['digest'] or a['surface'] != b['surface']:
                    for surface in sorted({x['surface'] for x in (a,b) if x}):
                        self.add(kind, op, np, selector, surface, reason='selected-content', old_value=a, new_value=b)
                        emitted = True
            if not emitted:
                for surface in sorted(set(self.old.owners(op) if op in self.old.routes else []) | set(self.new.owners(np) if np in self.new.routes else [])):
                    self.add(kind, op, np, 'file', surface, reason='file-metadata-or-unselected-content')
        ownership = self.overlay.get('ownership', [])
        if not isinstance(ownership, list):
            raise Invalid('ownership review must be an array')
        used = []
        ownership_paths = [(p, p) for p in old_paths & new_paths] + [(op, np) for kind, op, np in changes if kind == 'R']
        for op, np in sorted(ownership_paths):
            if op not in self.old.routes or np not in self.new.routes:
                continue
            a = {s:self.old.surfaces[s] for s in self.old.owners(op)}
            b = {s:self.new.surfaces[s] for s in self.new.owners(np)}
            kind = 'M' if op == np else 'R'
            ownership_fields = ['authority', 'primary_owner', 'collaborators', 'disposition']
            old_registrations = self.owner_registrations(self.old, list(a))
            new_registrations = self.owner_registrations(self.new, list(b))
            registrations_changed = old_registrations != new_registrations
            ownership_changed = registrations_changed or {s:{k:r.get(k) for k in ownership_fields} for s,r in a.items()} != {s:{k:r.get(k) for k in ownership_fields} for s,r in b.items()}
            if a != b and not ownership_changed:
                for surface in sorted(a.keys() | b.keys()):
                    self.add(kind, op, np, 'surface-metadata', surface, reason='surface-metadata', old_value=a, new_value=b)
            if ownership_changed:
                binding = {'path':op} if op == np else {'old_path':op, 'new_path':np}
                binding.update(old_ownership={s:r['primary_owner'] for s,r in a.items()}, new_ownership={s:r['primary_owner'] for s,r in b.items()})
                if registrations_changed:
                    binding.update(old_registrations=old_registrations, new_registrations=new_registrations)
                if binding not in ownership:
                    path = op if op == np else op + ' -> ' + np
                    self.errors.append(f'ownership/metadata change requires explicit ownership overlay: {path}')
                else:
                    used.append(binding)
                for surface in sorted(a.keys() | b.keys()):
                    self.add(kind, op, np, 'ownership', surface, reason='ownership-transition', old_value=a, new_value=b)
        if len(used) != len(ownership):
            raise Invalid('unused or duplicate ownership overlay')
        for key in sorted(self.old.inventory.keys() | self.new.inventory.keys()):
            a, b = self.old.inventory.get(key), self.new.inventory.get(key)
            if a == b or (a or b)['family'] == 'routes':
                continue
            row = b or a
            family = row['family']
            if family == 'headings' and self.overlay.get('inventory_digest') != self.new.inventory_digest:
                self.errors.append(f'heading inventory changed; explicit manifest review required: {row["path"]}')
            kind = 'M' if a and b else 'A' if b else 'D'
            self.add(kind, a['path'] if a else '', b['path'] if b else '', 'inventory:' + key, row['surface'], reason='inventory:' + family, old_value=a, new_value=b)

    def provenance(self) -> None:
        changed = {i['surface_id'] for i in self.items.values()}
        # Dependency changes notify both the generator and its projections.
        for key in self.old.inventory.keys() | self.new.inventory.keys():
            a, b = self.old.inventory.get(key), self.new.inventory.get(key)
            if a != b and (a or b)['family'] == 'generator-inputs':
                changed.add((b or a)['surface'])
        for surface in self.new.surfaces.values():
            if surface['authority'] != 'generated-projection' or not ({surface['id'], surface['generated_by']} & changed):
                continue
            for path, route in self.new.routes.items():
                if route['surface'] == surface['id']:
                    self.add('M', path if path in self.old.tree else '', path, 'provenance', surface['id'], reason='generated-provenance')
            try:
                self.new.validate_provenance(surface)
            except (Invalid, KeyError, TypeError) as error:
                self.errors.append(f'generated provenance: {surface["id"]}: {error}')


def validate_acknowledgements(comparison: Comparison, document: dict | None) -> dict:
    if document is None:
        return {}
    report = comparison.report
    if document.get('schema_version') != 1 or document.get('report_digest') != report['report_digest'] or not isinstance(document.get('acknowledgements'), list):
        raise Invalid('acknowledgements bind the exact report digest')
    decisions = {}
    for row in document['acknowledgements']:
        if not isinstance(row, dict):
            raise Invalid('invalid acknowledgement record')
        ident = row.get('id')
        item = comparison.items.get(ident)
        if item is None or ident in decisions:
            raise Invalid('unknown or duplicate acknowledgement id')
        for field in ['old_commit', 'new_commit']:
            if row.get(field) != report[field]:
                raise Invalid('acknowledgement commit mismatch')
        if row.get('surface_id') != item['surface_id'] or row.get('primary_owner') != item['primary_owner']:
            raise Invalid('acknowledgement surface/owner mismatch')
        disposition = row.get('disposition')
        if disposition not in DISPOSITIONS or not isinstance(row.get('evidence'), list) or not row['evidence'] or any(not isinstance(x,str) or not x for x in row['evidence']) or not isinstance(row.get('decision_ref'), str) or not row['decision_ref']:
            raise Invalid('acknowledgement lacks disposition, evidence or decision reference')
        expected = {'host-only':'host-only-reviewed', 'disposable-scaffold':'disposable-reviewed', 'generated-projection':'generated-refresh'}
        if disposition in expected.values() and expected.get(item['authority']) != disposition:
            raise Invalid('disposition incompatible with authority')
        if item['authority'] in expected and disposition not in {expected[item['authority']], 'no-semantic-change', 'intentional-divergence', 'responsibility-migration'}:
            raise Invalid('authority requires its explicit review disposition')
        if disposition == 'intentional-divergence':
            if not row.get('divergence_id') or row['divergence_id'] not in item['divergences']:
                raise Invalid('intentional divergence must bind a complete map-linked record')
        if disposition == 'responsibility-migration':
            related = row.get('related_items')
            if not isinstance(related, list) or not related or len(set(related)) != len(related) or any(x not in comparison.items for x in related) or ident not in related:
                raise Invalid('responsibility migration must bind related item ids')
            endpoints = {'old':{}, 'new':{}}
            for related_id in related:
                endpoints['old'].update(comparison.items[related_id]['old_ownership'])
                endpoints['new'].update(comparison.items[related_id]['new_ownership'])
            if not endpoints['old'] or not endpoints['new'] or row.get('responsibilities') != endpoints:
                raise Invalid('responsibility migration endpoint ownership mismatch')
        if 'ownership-transition' in item['reasons'] and disposition != 'responsibility-migration':
            raise Invalid('ownership transition requires responsibility-migration')
        decisions[ident] = row
    return decisions


def binding(report: dict) -> dict:
    return {key:report[key] for key in ['old_commit', 'new_commit', 'report_digest', 'base_map_digest', 'effective_map_digest', 'old_inventory_digest', 'new_inventory_digest']}


def proof_inputs(root: Path) -> dict:
    try:
        result = subprocess.run(['git', '-C', str(root), 'ls-files', '-z', '--cached'], capture_output=True)
    except OSError as error:
        raise Unavailable('Git unavailable for proof input binding') from error
    if result.returncode:
        raise Invalid('proof root must be a local Git repository')
    paths = sorted(set(result.stdout.decode().split('\0')) - {''})
    files = [{'path':p, 'sha256':hash_bytes(local_path(root, p).read_bytes())} for p in paths]
    tools = TOOLS + (['scripts/check_assembly.py'] if (ROOT/'scripts/check_assembly.py').is_file() else [])
    return {'source_inputs':files, 'tool_inputs':{p:hash_bytes((ROOT/p).read_bytes()) for p in tools}}


def check_anchor(source: Path, anchor: str | None, root: Path = ROOT, inventories: dict | None = None) -> None:
    if anchor is None:
        return
    if not isinstance(anchor, str) or ':' not in anchor:
        raise Invalid('evidence anchor must have an explicit kind')
    kind, value = anchor.split(':', 1)
    if kind == 'test':
        reference = {'path':source.resolve().relative_to(root.resolve()).as_posix(), 'anchor':anchor}
        try:
            cargo_validator().check_reference(reference, root, inventories)
        except Exception as error:
            raise Invalid('Rust suite evidence validation failed: ' + str(error)) from error
        return
    text = source.read_text()
    found = False
    if kind == 'text':
        found = bool(value) and value in text
    elif kind == 'python':
        found = value in {node.name for node in ast.walk(ast.parse(text)) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))}
    elif kind == 'heading':
        try:
            from .validate_docs import markdown_headings
        except ImportError:
            from validate_docs import markdown_headings
        found = value in {h[2] for h in markdown_headings(text.splitlines())}
    elif kind == 'json':
        if value and not value.startswith('/'):
            raise Invalid('JSON anchor must be a pointer')
        current = json.loads(text)
        try:
            for token in value.split('/')[1:] if value else []:
                token = token.replace('~1','/').replace('~0','~')
                current = current[int(token)] if isinstance(current, list) else current[token]
            found = True
        except (KeyError, IndexError, TypeError, ValueError):
            found = False
    if not found:
        raise Invalid('suite evidence anchor missing or unsupported: ' + anchor)


def cargo_validator():
    # The shared checker's legacy projection imports use the scripts directory.
    # Keep that trusted import bridge local to loading the pure checker.
    directory = str(ROOT/'scripts')
    sys.path.insert(0, directory)
    try:
        module = importlib.import_module('.check_assembly', __package__) if __package__ else importlib.import_module('check_assembly')
        if not all(callable(getattr(module, name, None)) for name in
                   ('check_command', 'check_reference', 'cargo_test_context')):
            raise ImportError('missing pure activation entrypoints')
        return module
    except (ImportError, AttributeError) as error:
        raise Invalid('Cargo proof activation validator unavailable') from error
    finally:
        sys.path.remove(directory)


def validate_suite_activation(row: dict, root: Path, *, inventories: dict | None = None) -> None:
    rust = any(Path(command[0]).name == 'cargo' for command in row['commands']) or any(str(e.get('anchor', '')).startswith('test:') for e in row['evidence'])
    validator = cargo_validator()
    enrolled = row['id'] in validator.REVIEWED_PROOF_TARGETS or row['id'] in validator.DURABLE_PROOF_TARGETS
    if not rust and not enrolled:
        return
    try:
        if inventories is None:
            inventories, _ = validator.cargo_test_context(row['commands'], root)
        for reference in row['evidence']:
            validator.check_reference(reference, root, inventories)
        for command in row['commands']:
            validator.check_command(command, row['evidence'], root)
        if row['id'] == 'conformance/provider-traces/v1.4.0':
            inventory = tomllib.loads(local_path(root, 'upstream/manifests/providers.toml').read_text())
            validator.validate_adapter_proofs(row, inventory, root)
        if enrolled:
            validator.validate_reviewed_proofs({'suites':[row]}, root, suite_names={row['id']})
    except Exception as error:
        raise Invalid('Cargo suite activation validation failed: ' + str(error)) from error


def registry_suites(path: Path, root: Path, required: set[str]) -> dict:
    registry = load_json(path)
    suites = indexed(registry.get('suites'), 'suite')
    for name in required:
        row = suites.get(name)
        if not row or row.get('status') != 'executable' or row.get('limitations') or not isinstance(row.get('commands'), list) or not row['commands'] or any(not isinstance(a, list) or not a or any(not isinstance(s,str) or not s for s in a) for a in row['commands']):
            raise Invalid(f'conformance proof unavailable or accepted-limit: {name}')
        if not isinstance(row.get('evidence'), list) or not row['evidence']:
            raise Invalid(f'suite lacks bound evidence: {name}')
        validator = cargo_validator()
        try:
            inventories, _ = validator.cargo_test_context(row['commands'], root)
        except Exception as error:
            raise Invalid('Cargo suite activation validation failed: ' + str(error)) from error
        for evidence in row['evidence']:
            source = local_path(root, evidence.get('path'))
            check_anchor(source, evidence.get('anchor'), root, inventories)
        validate_suite_activation(row, root, inventories=inventories)
    return {name:suites[name] for name in sorted(required)}


def verify_proofs(comparison: Comparison, registry: Path, receipts: dict | None, root: Path) -> dict:
    required = {s for i in comparison.items.values() for s in i['conformance']}
    suites = registry_suites(registry, root, required)
    if receipts is None or receipts.get('schema_version') != 1 or not isinstance(receipts.get('receipts'), list):
        raise Invalid('guarded patch requires bound proof receipts')
    inputs = proof_inputs(root)
    if 'conformance/provider-traces/v1.4.0' in required and 'upstream/manifests/providers.toml' not in {entry['path'] for entry in inputs['source_inputs']}:
        raise Invalid('canonical provider inventory must be tracked and bound as proof input')
    expected_binding = binding(comparison.report)
    valid = {}
    for row in receipts['receipts']:
        name = row.get('suite') if isinstance(row, dict) else None
        if name not in suites or name in valid:
            raise Invalid('unknown or duplicate proof suite')
        if row.get('binding') != expected_binding or row.get('commands') != suites[name]['commands'] or type(row.get('exit_code')) is not int or row.get('exit_code') != 0:
            raise Invalid('stale, failed or mismatched proof receipt')
        input_paths = {entry['path'] for entry in inputs['source_inputs']}
        if any(e['path'] not in input_paths for e in suites[name]['evidence']):
            raise Invalid('suite evidence must be tracked and bound as proof input')
        if row.get('inputs') != inputs or row.get('registry_digest') != digest(suites[name]) or row.get('parser') != comparison.report['parser']:
            raise Invalid('proof source/tool/fixture/registry input binding mismatch')
        for field in ['logs', 'artifacts']:
            entries = row.get(field)
            if not isinstance(entries, list) or not entries:
                raise Invalid('proof requires real log and artifact hashes')
            for entry in entries:
                if not isinstance(entry, dict) or hash_bytes(local_path(root, entry.get('path')).read_bytes()) != entry.get('sha256'):
                    raise Invalid('proof log/artifact hash mismatch')
        valid[name] = row
    if valid.keys() != suites.keys():
        raise Invalid('required conformance proof missing')
    return valid


def toml_value(value: object) -> str:
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return '[' + ', '.join(toml_value(v) for v in value) + ']'
    if isinstance(value, dict):
        return '{ ' + ', '.join(toml_value(k) + ' = ' + toml_value(v) for k,v in sorted(value.items())) + ' }'
    raise Invalid('unsupported TOML map value')


def render_map(data: dict) -> str:
    def rows(value):
        return isinstance(value, list) and bool(value) and all(isinstance(v, dict) for v in value)
    lines = [key + ' = ' + toml_value(value) for key,value in data.items() if not isinstance(value, dict) and not rows(value)]
    lines.append('')
    for key, value in data.items():
        if isinstance(value, dict):
            lines.append('[' + key + ']')
            lines += [k + ' = ' + toml_value(v) for k,v in value.items()]
            lines.append('')
        elif rows(value):
            for row in value:
                lines.append('[[' + key + ']]')
                lines += [k + ' = ' + toml_value(v) for k,v in row.items()]
                lines.append('')
    return '\n'.join(lines).rstrip() + '\n'


def patch_bundle(comparison: Comparison, decisions: dict, proofs: dict, original_map: bytes) -> dict:
    if comparison.report['errors'] or comparison.pending_intake or decisions.keys() != comparison.items.keys():
        raise Invalid('patch requires complete valid comparison and decisions')
    expected_suites = {s for item in comparison.items.values() for s in item['conformance']}
    if proofs.keys() != expected_suites:
        raise Invalid('patch requires every referenced conformance proof')
    data = deepcopy(comparison.effective)
    for watch in data['watch']:
        watch['route_digest'] = comparison.new.route_digests[watch['id']]
        watch['baseline_path_count'] = sum(p == watch['root'] or p.startswith(watch['root'] + '/') for p in comparison.new.tree)
    rendered = render_map(data)
    if tomllib.loads(rendered) != data:
        raise Invalid('candidate map serialization failed round trip')
    # The new snapshot is carried in the bundle, never written over historical manifests.
    snapshot = comparison.new.manifest()
    snapshot['map_digest'] = digest(data)
    advanced = {'schema_version':1, **binding(comparison.report), 'advanced_map_digest':digest(data),
                'decisions':sorted(decisions.values(), key=lambda r:r['id']),
                'proof_receipt_digests':{s:digest(r) for s,r in sorted(proofs.items())},
                'inventory_digest':comparison.new.inventory_digest}
    patch = ''.join(difflib.unified_diff(original_map.decode().splitlines(True), rendered.splitlines(True), fromfile='a/upstream-map.toml', tofile='b/upstream-map.toml'))
    return {'schema_version':1, 'patch':patch, 'candidate_map':rendered, 'candidate_inventory':snapshot, 'advancement':advanced}


def evaluate(comparison: Comparison, acknowledgements: dict | None = None) -> tuple[int, dict, dict]:
    decisions = validate_acknowledgements(comparison, acknowledgements)
    report = deepcopy(comparison.report)
    report['decisions'] = sorted(decisions.values(), key=lambda r:r['id'])
    report['pending_items'] = sorted(comparison.items.keys() - decisions.keys())
    code = 3 if report['errors'] else 2 if report['pending_items'] or report['pending_intake'] else 0
    report['exit_code'] = code
    return code, report, decisions


def guard_outputs(args, upstream: Path, receipts: dict | None) -> None:
    outputs = [p.resolve() for p in [args.output, args.emit_patch] if p]
    if not outputs:
        return
    if len(set(outputs)) != len(outputs):
        raise Invalid('report and patch outputs must be separate artifacts')
    protected = {(ROOT/p).resolve() for p in TOOLS + ['scripts/check_assembly.py']}
    protected.update(p.resolve() for p in [args.map, args.overlay, args.acknowledgements, args.receipts, args.registry] if p)
    roots = {ROOT.resolve()}
    if args.emit_patch:
        roots.add(args.proof_root.resolve())
    for root in roots:
        result = subprocess.run(['git','-C',str(root),'ls-files','-z','--cached'],capture_output=True)
        if result.returncode:
            raise Invalid('artifact source root must be a local Git repository')
        protected.update((root/path).resolve() for path in result.stdout.decode().split('\0') if path)
    if receipts:
        for receipt in receipts.get('receipts', []):
            for field in ['logs', 'artifacts']:
                for entry in receipt.get(field, []):
                    protected.add(local_path(args.proof_root, entry.get('path')))
    for target in outputs:
        if target in protected or target.is_relative_to(upstream.resolve()) or target.is_relative_to((ROOT/'upstream/manifests').resolve()):
            raise Invalid('output must be a separate artifact, outside source and review/proof inputs')


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--map', type=Path, default=ROOT/'upstream-map.toml')
    parser.add_argument('--upstream', type=Path)
    parser.add_argument('--old')
    parser.add_argument('--new')
    parser.add_argument('--overlay', type=Path)
    parser.add_argument('--acknowledgements', type=Path)
    parser.add_argument('--receipts', type=Path)
    parser.add_argument('--registry', type=Path, default=ROOT/'conformance/assembly.json')
    parser.add_argument('--proof-root', type=Path, default=ROOT)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--emit-patch', type=Path)
    args = parser.parse_args(argv)
    try:
        original = args.map.read_bytes()
        base = tomllib.loads(original.decode())
        old = args.old or base['upstream']['commit']
        new = args.new or old
        if args.upstream:
            upstream = args.upstream.resolve()
        else:
            if __package__:
                from .upstream import resolve_upstream
            else:
                from upstream import resolve_upstream
            upstream = resolve_upstream(base['upstream'].get('checkout'))
        dependency_root = Path(os.environ.get('PIRUA_UPSTREAM_ROOT', str(upstream)))
        comparison = Comparison(Objects(upstream), base, old, new, dependency_root, load_json(args.overlay) if args.overlay else None)
        code, report, decisions = evaluate(comparison, load_json(args.acknowledgements) if args.acknowledgements else None)
        receipt_document = load_json(args.receipts) if args.receipts else None
        guard_outputs(args, upstream, receipt_document)
        if args.emit_patch:
            if code:
                raise Invalid('patch blocked by incomplete review, intake or invalid inventory')
            proofs = verify_proofs(comparison, args.registry, receipt_document, args.proof_root)
            bundle = patch_bundle(comparison, decisions, proofs, original)
            args.emit_patch.write_bytes(canonical(bundle))
        output = canonical(report)
        if args.output:
            args.output.write_bytes(output)
        else:
            sys.stdout.buffer.write(output)
        return code
    except Unavailable as error:
        print(json.dumps({'schema_version':1, 'exit_code':4, 'errors':[str(error)]}, sort_keys=True))
        return 4
    except (Invalid, OSError, ValueError, KeyError, TypeError, AttributeError) as error:
        print(json.dumps({'schema_version':1, 'exit_code':3, 'errors':[str(error)]}, sort_keys=True))
        return 3


if __name__ == '__main__':
    raise SystemExit(main())
