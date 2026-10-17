#!/usr/bin/env python3
"""Project immutable target source objects with the installed lock-bound TS parser.

This generator never imports or executes upstream modules. Public symbols include
selected and excluded occurrences; their source-bound disposition is explicit.
Released catalog JSON absent from Git is represented by its artifact association.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sys
import tomllib

try:
    from .upstream import resolve_upstream
    from .upstream_objects import Objects, Invalid, digest
    from .upstream_inventory import Snapshot
except ImportError:
    from upstream import resolve_upstream
    from upstream_objects import Objects, Invalid, digest
    from upstream_inventory import Snapshot

ROOT = Path(__file__).resolve().parents[1]
MAP_PATH = ROOT / 'upstream-map.toml'


def quoted(value: str) -> str:
    return json.dumps(value, ensure_ascii=False)


def manifest(kind: str, commit: str, body: str) -> str:
    return ('manifest_version = 1\nkind = ' + quoted(kind) + '\nsource_commit = '
            + quoted(commit) + '\ncontent_digest = '
            + quoted('sha256:' + hashlib.sha256(body.encode()).hexdigest()) + '\n\n' + body)


def value(item):
    if isinstance(item, str): return quoted(item)
    if isinstance(item, bool): return str(item).lower()
    if isinstance(item, int): return str(item)
    if isinstance(item, list): return '[' + ', '.join(value(x) for x in item) + ']'
    if isinstance(item, dict): return '{ ' + ', '.join(quoted(k) + ' = ' + value(v) for k,v in sorted(item.items())) + ' }'
    raise Invalid('unsupported manifest value')


def rows_manifest(kind: str, snapshot: Snapshot, label: str, rows: list[dict], prefix: dict | None = None) -> str:
    chunks = [key + ' = ' + value(item) for key,item in (prefix or {}).items()]
    for row in rows:
        chunks += ['', '[[' + label + ']]'] + [key + ' = ' + value(item) for key,item in row.items()]
    return manifest(kind, snapshot.commit, '\n'.join(chunks) + '\n')


def build_snapshot(map_path: Path = MAP_PATH) -> Snapshot:
    data = tomllib.loads(map_path.read_text())
    checkout = resolve_upstream(data['upstream']['checkout'])
    snapshot = Snapshot(Objects(checkout), data['upstream']['commit'], data, checkout, baseline=True)
    if snapshot.errors:
        raise Invalid('; '.join(sorted(set(snapshot.errors))))
    return snapshot


def outputs(snapshot: Snapshot) -> dict[Path, str]:
    surfaces = snapshot.surfaces
    def classification(surface):
        row = surfaces[surface]
        return {'surface':surface, 'disposition':row['disposition'], 'owner':row['primary_owner']}
    routes = [{'watch':route['watch'], 'path':path, 'blob_oid':snapshot.tree[path].oid,
               'mode':snapshot.tree[path].mode, 'route':route['id'], **classification(route['surface']),
               'role':route['role']} for path,route in sorted(snapshot.routes.items())]
    projected = sorted(snapshot.inventory.values(), key=lambda row: (row['family'], row['key'], row['surface']))
    tests = []
    for row in projected:
        if row['family'] != 'tests': continue
        payload = row['payload']
        evidence = sorted({surface for item in payload['closure'] for surface in item.get('surfaces', [])} or {row['surface']})
        tests.append({'path':row['path'], 'blob_oid':snapshot.tree[row['path']].oid,
                      'route_surface':row['surface'], 'evidence_surfaces':evidence,
                      'classification':payload['classification'],
                      'closure_digest':digest(payload), 'closure_paths':[item['path'] for item in payload['closure']]})
    result = {
        ROOT/'upstream/manifests/routes.toml':rows_manifest('route-inventory', snapshot, 'entry', routes),
        ROOT/'upstream/manifests/tests.toml':rows_manifest('test-inventory', snapshot, 'test', tests),
    }
    for package in ['ai', 'agent']:
        exports = []
        prefix = 'packages/' + package + '/package.json::'
        for row in projected:
            if row['family'] == 'exports' and row['key'].startswith(prefix):
                exports.append({**row['payload'], 'source_path':row['path'], **classification(row['surface'])})
        result[ROOT/f'upstream/manifests/{package}-exports.toml'] = rows_manifest('public-exports', snapshot, 'symbol', exports)
    providers = [{ 'identity':row['key'], **row['payload'], **classification(row['surface'])}
                 for row in projected if row['family'] == 'providers']
    result[ROOT/'upstream/manifests/providers.toml'] = rows_manifest('provider-inventory', snapshot, 'provider', providers)
    projection = surfaces['ai.catalog.projection']
    inputs = [{'path':row['path'], 'blob_oid':snapshot.tree[row['path']].oid,
               'surface':row['surface'], 'closure_digest':digest(row['payload']),
               'closure_paths':[item['path'] for item in row['payload']['closure']]}
              for row in projected if row['family'] == 'generator-inputs']
    artifact = projection['provenance_snapshot']
    result[ROOT/'upstream/manifests/catalog-inputs.toml'] = rows_manifest('catalog-inputs', snapshot, 'input', inputs, {
        'provenance_status':projection['provenance_status'], 'provenance_kind':projection['provenance_kind'],
        'snapshot_id':artifact['id'], 'snapshot_sha256':artifact['sha256'],
        'source_ids':artifact['source_ids'], 'release_association':artifact,
        'git_catalog_data_exists':False})
    return result


def expected_outputs() -> dict[Path, str]:
    return outputs(build_snapshot())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--check', action='store_true')
    args = parser.parse_args()
    try:
        expected = expected_outputs()
        stale = []
        for path, content in expected.items():
            if args.check:
                if not path.is_file() or path.read_text() != content: stale.append(str(path.relative_to(ROOT)))
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
        declared = {row['path'] for row in tomllib.loads(MAP_PATH.read_text())['manifest']}
        actual = {str(path.relative_to(ROOT)) for path in (ROOT/'upstream/manifests').glob('*.toml')}
        if actual != declared:
            raise Invalid('manifest destination universe differs: ' + repr(sorted(actual ^ declared)))
        if stale: raise Invalid('stale manifests: ' + ', '.join(stale))
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        print('error: ' + str(error), file=sys.stderr)
        return 1
    print(('checked' if args.check else 'generated') + f' {len(expected)} immutable-source manifests')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
