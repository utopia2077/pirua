#!/usr/bin/env python3
"""Counterexamples for selected-provider and Cargo dependency boundaries."""
from __future__ import annotations
import copy
import sys
import unittest
from unittest import mock
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import check_workspace as checker


class WorkspaceCheckerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.topology = checker.load_toml(ROOT / 'workspace-topology.toml')
        cls.default = checker.cargo_metadata()
        cls.all_features = checker.cargo_metadata('--all-features')

    @staticmethod
    def package(metadata, name):
        return next(p for p in metadata['packages'] if p['name'] == name)

    @classmethod
    def dependency(cls, metadata, consumer, dependency):
        return next(d for d in cls.package(metadata, consumer)['dependencies'] if d['name'] == dependency)

    def validate(self, default=None, all_features=None, topology=None):
        return checker.validate_workspace(ROOT, self.topology if topology is None else topology,
            self.default if default is None else default,
            self.all_features if all_features is None else all_features)

    def assert_rejected(self, errors, reason):
        self.assertTrue(errors, reason)
        self.assertTrue(any(reason in error for error in errors), errors)

    def test_current_default_and_all_feature_graphs_are_accepted(self):
        self.assertEqual(self.validate(), [])

    def test_durable_session_stays_independent_of_execution_owners(self):
        internal=set(self.topology['production_crates'])
        package=self.package(self.default,'pi-durable-session')
        self.assertEqual({item['name'] for item in package['dependencies'] if item['name'] in internal},{'pi-sqlite'})
        for dependency in ('pi-ai-core','pi-agent','pi-coding-runtime','pi-mcp','pi-codemode'):
            metadata=copy.deepcopy(self.all_features)
            source=self.package(metadata,'pi-durable-session')
            node=next(item for item in metadata['resolve']['nodes'] if item['id']==source['id'])
            node['deps'].append({'pkg':self.package(metadata,dependency)['id']})
            with self.subTest(dependency=dependency):
                self.assert_rejected(self.validate(all_features=metadata),'all-feature graph mismatch')

    def test_durable_runtime_uses_selected_compositions_without_direct_execution_owners(self):
        internal=set(self.topology['production_crates'])
        package=self.package(self.default,'pi-durable')
        self.assertEqual({item['name'] for item in package['dependencies'] if item['name'] in internal},
                         {'pi-durable-session','pi-ai','pi-coding-runtime','pi-mcp','pi-extension-protocol','pi-codemode'})
        for dependency in ('pi-ai-core','pi-ai-transport','pi-ai-providers','pi-agent-schema',
                           'pi-agent-loop','pi-agent','pi-coding-session','pi-sqlite'):
            metadata=copy.deepcopy(self.all_features)
            source=self.package(metadata,'pi-durable')
            node=next(item for item in metadata['resolve']['nodes'] if item['id']==source['id'])
            node['deps'].append({'pkg':self.package(metadata,dependency)['id']})
            with self.subTest(dependency=dependency):
                self.assert_rejected(self.validate(all_features=metadata),'all-feature graph mismatch')

    def test_missing_extra_renamed_and_moved_packages_are_rejected(self):
        for scenario in ('missing', 'extra', 'renamed', 'moved', 'registry'):
            with self.subTest(scenario=scenario):
                metadata = copy.deepcopy(self.default)
                package = self.package(metadata, 'pi-agent')
                if scenario == 'missing':
                    metadata['workspace_members'].remove(package['id'])
                elif scenario == 'extra':
                    extra = copy.deepcopy(package)
                    extra.update(name='rogue', id='rogue', manifest_path='/tmp/rogue/Cargo.toml')
                    metadata['packages'].append(extra)
                    metadata['workspace_members'].append('rogue')
                elif scenario == 'renamed': package['name'] = 'renamed-agent'
                elif scenario == 'moved': package['manifest_path'] = '/tmp/moved/Cargo.toml'
                else: package['source'] = 'registry+https://example.invalid'
                self.assert_rejected(self.validate(default=metadata), 'canonical' if scenario in ('moved', 'registry') else 'inventory')

    def test_hidden_local_package_is_rejected_even_without_workspace_membership(self):
        metadata = copy.deepcopy(self.default)
        metadata['packages'].append({'id': 'outside', 'name': 'outside', 'source': None,
                                    'manifest_path': '/tmp/outside/Cargo.toml'})
        self.assert_rejected(self.validate(default=metadata), 'outside workspace')

    def test_internal_edge_must_be_exact_normal_canonical_untargeted(self):
        cases = [('kind', 'build', 'kind'), ('kind', 'dev', 'kind'),
                 ('target', 'cfg(unix)', 'untargeted'), ('rename', 'renamed', 'unrenamed'),
                 ('path', '/tmp/foreign', 'canonical local path'), ('optional', True, 'optionality')]
        for key, value, reason in cases:
            with self.subTest(key=key, value=value):
                metadata = copy.deepcopy(self.default)
                self.dependency(metadata, 'pi-agent', 'pi-agent-loop')[key] = value
                self.assert_rejected(self.validate(default=metadata), reason)

    def test_duplicate_internal_declaration_is_rejected(self):
        metadata = copy.deepcopy(self.default)
        record = self.dependency(metadata, 'pi-agent', 'pi-agent-loop')
        self.package(metadata, 'pi-agent')['dependencies'].append(copy.deepcopy(record))
        self.assert_rejected(self.validate(default=metadata), 'exactly one declaration')

    def test_missing_extra_and_optional_edge_activation_are_rejected(self):
        for mode in ('missing', 'extra'):
            metadata = copy.deepcopy(self.all_features)
            pkg = self.package(metadata, 'pi-agent')
            node = next(n for n in metadata['resolve']['nodes'] if n['id'] == pkg['id'])
            if mode == 'missing':
                target = self.package(metadata, 'pi-agent-loop')['id']
                node['deps'] = [d for d in node['deps'] if d['pkg'] != target]
            else:
                node['deps'].append({'pkg': self.package(metadata, 'pi-coding-session')['id']})
            self.assert_rejected(self.validate(all_features=metadata), 'all-feature graph mismatch')
        metadata = copy.deepcopy(self.default)
        # The application host deliberately selects the AI provider facade.
        # Proxy transport remains an unselected optional edge in this profile.
        pkg = self.package(metadata, 'pi-agent-loop')
        node = next(n for n in metadata['resolve']['nodes'] if n['id'] == pkg['id'])
        node['deps'].append({'pkg': self.package(self.all_features, 'pi-ai-transport')['id']})
        self.assert_rejected(self.validate(default=metadata), 'default feature graph mismatch')

    def test_application_selected_optional_edge_is_explicit_and_required(self):
        topology = copy.deepcopy(self.topology)
        edge = next(e for e in topology['edge']
                    if (e['consumer'], e['dependency']) == ('pi-ai', 'pi-ai-providers'))
        del edge['workspace_default']
        self.assert_rejected(self.validate(topology=topology), 'default feature graph mismatch')
        metadata = copy.deepcopy(self.default)
        pkg = self.package(metadata, 'pi-ai')
        node = next(n for n in metadata['resolve']['nodes'] if n['id'] == pkg['id'])
        target = self.package(metadata, 'pi-ai-providers')['id']
        node['deps'] = [d for d in node['deps'] if d['pkg'] != target]
        self.assert_rejected(self.validate(default=metadata), 'default feature graph mismatch')

    def test_workspace_default_activation_requires_optional_boolean(self):
        for required, value in ((True, True), (False, 'true')):
            topology = copy.deepcopy(self.topology)
            edge = next(e for e in topology['edge']
                        if e['kind'] == ('required' if required else 'optional'))
            edge['workspace_default'] = value
            self.assert_rejected(self.validate(topology=topology),
                                 'workspace_default must be a boolean on an optional edge')

    def test_unauthorized_direct_and_indirect_optional_feature_activation(self):
        for value in ('dep:pi-ai-transport', 'proxy'):
            metadata = copy.deepcopy(self.default)
            self.package(metadata, 'pi-agent-loop')['features']['unapproved'] = [value]
            self.assert_rejected(self.validate(default=metadata), 'unauthorized feature')

    def test_dependency_weak_forwarding_does_not_activate_optional_dependency(self):
        self.assertFalse(checker.feature_activates_dependency({'foo': ['bar?/native']}, 'foo', 'bar'))
        self.assertTrue(checker.feature_activates_dependency({'foo': ['bar/native']}, 'foo', 'bar'))

    def test_exact_four_providers_two_oauth_and_wire_selection(self):
        for name, validator in (('pi-ai-providers', checker.validate_provider_features),
                                ('pi-ai', checker.validate_facade_features)):
            original = self.package(self.default, name)
            for key in ('provider-openai', 'provider-anthropic', 'provider-google', 'provider-deepseek',
                        'oauth-anthropic', 'oauth-openai-chatgpt', 'all-providers', 'all-oauth', 'native', 'auth-native'):
                for mutation in ('delete', 'native', 'duplicate'):
                    package = copy.deepcopy(original)
                    if mutation == 'delete': del package['features'][key]
                    elif mutation == 'native': package['features'][key].append('native')
                    else: package['features'][key] *= 2
                    errors = []
                    validator(package, errors)
                    self.assertTrue(errors, (name, key, mutation))
            for excluded in ('provider-openrouter', 'provider-openai-codex', 'images-openrouter',
                             'wire-bedrock-converse-stream', 'compat'):
                package = copy.deepcopy(original)
                package['features'][excluded] = []
                errors = []
                validator(package, errors)
                self.assert_rejected(errors, 'feature inventory')
            package = copy.deepcopy(original)
            package['features']['default'] = ['all-providers']
            errors = []
            validator(package, errors)
            self.assert_rejected(errors, 'feature ownership')

    def test_topology_cycle_duplicate_invalid_and_dev_only_edges(self):
        for case in ('cycle', 'duplicate', 'invalid', 'dev', 'test-only'):
            topology = copy.deepcopy(self.topology)
            if case == 'cycle':
                topology['edge'].append({'consumer': 'pi-ai-core', 'dependency': 'pi-agent', 'kind': 'required'})
                reason = 'cycle'
            elif case == 'duplicate':
                topology['edge'].append(copy.deepcopy(topology['edge'][0]))
                reason = 'duplicate topology'
            elif case == 'invalid':
                topology['edge'].append({'consumer': 'unknown', 'dependency': 'pi-agent', 'kind': 'required'})
                reason = 'invalid topology'
            elif case == 'dev':
                topology['dev_edge'] = [{'consumer': 'pi-agent', 'dependency': 'pi-coding-session'}]
                reason = 'dev-only edge'
            else:
                topology['test_only_package'] = {'name': 'old-allocator'}
                reason = 'test-only package'
            self.assert_rejected(self.validate(topology=topology), reason)

    def test_member_package_and_lint_inheritance_are_required(self):
        member = ROOT / 'crates/pi-telemetry/Cargo.toml'
        original = checker.load_toml
        for field in ('version', 'edition', 'rust-version', 'license', 'publish', 'lints'):
            changed = copy.deepcopy(original(member))
            if field == 'lints': changed['lints'] = {}
            else: changed['package'][field] = 'explicit override'
            with self.subTest(field=field), mock.patch.object(checker, 'load_toml', side_effect=lambda path: changed if path == member else original(path)):
                errors = self.validate()
                self.assert_rejected(errors, 'must inherit workspace lints' if field == 'lints' else 'must inherit package.' + field)

    def test_workspace_inheritance_defaults_and_toolchain_are_fixed(self):
        original = checker.load_toml
        cases = [('resolver', '2', 'must use resolver 3'),
                 ('members', [], 'workspace members do not match topology order'),
                 ('edition', '2021', 'edition must be 2024'),
                 ('rust-version', '1.0.0', 'rust-version must be 1.98.0'),
                 ('publish', True, 'packages must be private'),
                 ('unsafe_code', 'allow', 'unsafe_code must be forbid')]
        for field, value, reason in cases:
            root_manifest = copy.deepcopy(original(ROOT / 'Cargo.toml'))
            workspace = root_manifest['workspace']
            if field in ('resolver', 'members'): workspace[field] = value
            elif field == 'unsafe_code': workspace['lints']['rust'][field] = value
            else: workspace['package'][field] = value
            with self.subTest(field=field), mock.patch.object(checker, 'load_toml', side_effect=lambda path: root_manifest if path == ROOT / 'Cargo.toml' else original(path)):
                self.assert_rejected(self.validate(), reason)
        toolchain = copy.deepcopy(original(ROOT / 'rust-toolchain.toml'))
        toolchain['toolchain']['channel'] = 'stable'
        with mock.patch.object(checker, 'load_toml', side_effect=lambda path: toolchain if path == ROOT / 'rust-toolchain.toml' else original(path)):
            self.assert_rejected(self.validate(), 'must pin Rust 1.98.0')

    def test_verification_script_keeps_default_and_all_feature_builds(self):
        text = (ROOT / 'scripts/verify.sh').read_text()
        for command in ('cargo check', 'cargo clippy', 'cargo test', 'cargo doc'):
            lines = [line for line in text.splitlines() if line.startswith(command + ' --workspace')]
            self.assertTrue(any('--all-features' not in line for line in lines), command)
            self.assertTrue(any('--all-features' in line for line in lines), command)
        for test in (ROOT/'scripts').glob('test_*.py'):
            self.assertIn(str(test.relative_to(ROOT)),text)
        for command in ('python3 scripts/generate_upstream_manifests.py --check',
                        'python3 scripts/generate_assembly_registry.py --check',
                        'python3 scripts/upstream_drift.py', 'python3 scripts/check_assembly.py',
                        'python3 scripts/validate_docs.py', 'python3 scripts/check_workspace.py',
                        'python3 conformance/catalog/v1.4.0/check.py --check',
                        'python3 conformance/catalog/v1.4.0/check_negative.py',
                        'python3 scripts/check_publication.py --self-check',
                        'python3 scripts/check_publication.py',
                        'node conformance/durable-v1.4.0/runtime.mjs --check',
                        'node conformance/durable-v1.4.0/runtime.mjs --causal',
                        'node conformance/durable-v1.4.0/codemode.mjs --check',
                        'node conformance/durable-v1.4.0/codemode.mjs --causal'):
            self.assertIn(command,text)



if __name__ == '__main__':
    unittest.main()
