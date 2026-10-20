#!/usr/bin/env python3
"""Generate selected offline proof definitions; never execute proofs or bless fixtures.

Command registrations are an explicit selected-scope plan. Rust anchors derive
from the concrete enabled targets. Historical fixture identities derive only
from immutable Pirua baseline objects, independently of working-tree bytes.
"""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
import sys
import tomllib

try:
    from .upstream import PIN
    from .upstream_objects import Objects, Invalid
except ImportError:
    from upstream import PIN
    from upstream_objects import Objects, Invalid

ROOT = Path(__file__).resolve().parents[1]
HISTORICAL_SOURCE_REVISION = 'ea7396d067d42e3fca3924aff36958dcd68f7d1b'
HISTORICAL_ROOTS = ('conformance/ai-domain/', 'conformance/ai-models/',
                    'conformance/ai-transport/', 'conformance/provider-traces/')

SUITES = {'conformance/ai-core': {'pi-ai-core': '*'},
 'conformance/ai-transport': {'pi-ai-transport': '*'},
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
 'conformance/ai-facade': {'pi-ai': '*'},
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
 'conformance/current-extensions': {'pi-extension-protocol': '*',
                                    'pi-coding-runtime': ['extensions']},
 'conformance/coding-tool-adapters-v1.4.0': {'pi-coding-runtime': ['mcp_adapter',
                                                                   'codemode_adapter']},
 'conformance/mcp': {'pi-mcp': ['client', 'http', 'stdio', 'oauth']},
 'conformance/codemode': {'pi-codemode': ['sandbox']},
 'conformance/telemetry': {'pi-telemetry': ['contract', 'propagation']}}

# Explicit targets discover current enabled anchors. The separate Durable floor
# at committed e7fed05 retains its accepted identities and shared regressions;
# the original Agent floor is unchanged. Neither floor is refreshed here.
SUITES['conformance/durable-v1.4.0'] = {
    'pi-durable-session': ['conversation_observation', 'invariants', 'number_roundtrip', 'release_fixtures'],
    'pi-durable': ['bindings', 'close', 'context', 'initializer', 'invariants', 'release_fixtures',
                   'review_regressions', 'runtime_core', 'runtime_fixtures', 'views', '@lib'],
}


def historical_inputs(root: Path = ROOT) -> list[dict]:
    objects = Objects(root)
    tree = objects.tree(HISTORICAL_SOURCE_REVISION)
    references = []
    for path, entry in sorted(tree.items()):
        if (not any(path.startswith(prefix) for prefix in HISTORICAL_ROOTS)
                or '/v1.4.0/' in path or path.endswith(('.md', '.lock'))):
            continue
        original = objects.blob(entry)
        expected = hashlib.sha256(original).hexdigest()
        if not (root/path).is_file() or hashlib.sha256((root/path).read_bytes()).hexdigest() != expected:
            raise Invalid('refusing to bless changed historical fixture: ' + path)
        references.append({'path':path, 'sha256':expected,
                           'source_revision':HISTORICAL_SOURCE_REVISION})
    return references


def expected_registry(root: Path = ROOT) -> dict:
    # Import only the trusted inert Rust lexer; no Cargo or source oracle runs.
    if __package__:
        from . import check_assembly as assembly
    else:
        import check_assembly as assembly
    preserved = historical_inputs(root)
    suites = []
    for name, packages in SUITES.items():
        row = {'id':name, 'status':'executable', 'commands':[], 'evidence':[], 'limitations':[]}
        for package, selection in packages.items():
            folder = root/'crates'/package
            manifest = tomllib.loads((folder/'Cargo.toml').read_text())
            targets = sorted(path.stem for path in (folder/'tests').glob('*.rs')) if selection == '*' else selection
            for target in targets:
                selector = ['--lib'] if target == assembly.LIB_TARGET else ['--test',target]
                command = ['cargo','test','--locked','--offline','-p',package,*selector,'--all-features']
                _, target, path, features = assembly.cargo_test_target(command,manifest)
                tests, _ = assembly.rust_test_inventory(root/path,root)
                enabled = [test for test in tests if assembly.test_applicable(test,features)]
                if not enabled:
                    raise Invalid(f'selected target has no enabled test anchors: {package}/{target}')
                row['commands'].append(command)
                row['evidence'].extend({'path':str(test['path'].relative_to(root)),
                                        'anchor':'test:'+test['qualified'], 'cargo_target':target}
                                       for test in enabled)
        if name == 'conformance/catalog/v1.4.0':
            row['commands'].append(['python3','conformance/catalog/v1.4.0/check.py','--check'])
            row['evidence'].append({'path':'conformance/catalog/v1.4.0/check.py','anchor':'python:main'})
        suites.append(row)
    registry = {'schema_version':1, 'source_commit':PIN, 'suites':suites, 'preserved_inputs':preserved}
    assembly.validate_reviewed_proofs(registry,root)
    assembly.validate_registry(registry,set(SUITES),root)
    assembly.validate_adapter_proofs(next(row for row in suites if row['id']=='conformance/provider-traces/v1.4.0'),{},root)
    return registry


def render(registry: dict) -> str:
    return json.dumps(registry,indent=2)+'\n'


def main(argv=None, *, root: Path = ROOT) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--check',action='store_true')
    action.add_argument('--write',action='store_true')
    args = parser.parse_args(argv)
    try:
        expected = render(expected_registry(root))
        path = root/'conformance/assembly.json'
        if args.write:
            path.write_text(expected)
            print('generated selected proof registry; commands executed: 0')
        elif path.read_text() != expected:
            raise Invalid('stale proof registry; review source changes before --write')
        else:
            print('checked selected proof registry; commands executed: 0')
    except (OSError, ValueError, RuntimeError, KeyError) as error:
        print('registry error: ' + str(error),file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
