#!/usr/bin/env python3
"""Causal controls for static selected-source ownership and enabled proof routes."""
from __future__ import annotations
import copy
import hashlib
import json
import io
from contextlib import redirect_stderr
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
import tomllib
import unittest
from unittest import mock
from scripts import check_assembly as assembly
from scripts.upstream_drift import render_map

ROOT = Path(__file__).resolve().parents[1]


class AssemblyTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.map_path = ROOT/'upstream-map.toml'
        cls.registry_path = ROOT/'conformance/assembly.json'
        cls.registry = assembly.load_json(cls.registry_path)
        cls.map = tomllib.loads(cls.map_path.read_text())
        cls.labels = {name for surface in cls.map['surface'] for name in surface['conformance']}

    def suite(self, name):
        return copy.deepcopy(next(row for row in self.registry['suites'] if row['id']==name))

    def validate(self, row):
        return assembly.validate_registry({'schema_version':1,'suites':[row]}, {row['id']})

    def test_complete_join_is_read_only_and_does_not_execute_proof_commands(self):
        before = {row['path']:hashlib.sha256((ROOT/row['path']).read_bytes()).hexdigest() for row in self.registry['preserved_inputs']}
        run = subprocess.run
        def inert_only(argv,*args,**kwargs):
            self.assertTrue(argv[0]=='git' or argv[:2]==['node',str(ROOT/'scripts/upstream_ast.mjs')], 'assembly may execute only immutable readers and the inert AST parser')
            return run(argv,*args,**kwargs)
        with mock.patch('subprocess.run',side_effect=inert_only):
            result=assembly.check_assembly(self.map_path,self.registry_path)
        self.assertEqual(result['unresolved'],[])
        totals=result['totals']
        self.assertEqual(totals['watched_paths'],sum(row['baseline_path_count'] for row in self.map['watch']))
        for package in ['ai','agent']:
            symbols=tomllib.loads((ROOT/f'upstream/manifests/{package}-exports.toml').read_text())['symbol']
            self.assertEqual(totals[package+'_exports']['occurrences'],len(symbols))
            self.assertEqual(sum(totals[package+'_exports']['surfaces'].values()),len(symbols))
        self.assertEqual(totals['provider_identities']['selected_text'],4)
        self.assertEqual(totals['provider_identities']['excluded_image-api'],1)
        self.assertEqual(totals['proof_commands_executed'],0)
        self.assertEqual(before,{path:hashlib.sha256((ROOT/path).read_bytes()).hexdigest() for path in before})

    def test_stale_generated_source_manifest_is_rejected(self):
        path=ROOT/'upstream/manifests/routes.toml'
        with mock.patch.object(assembly.projections,'outputs',return_value={path:'stale generated source'}):
            with self.assertRaisesRegex(assembly.AssemblyError,'stale pinned manifest: upstream/manifests/routes.toml'):
                assembly.check_assembly(self.map_path,self.registry_path)

    def test_missing_logical_proof_is_named(self):
        registry={'schema_version':1,'suites':[self.suite('conformance/agent-proxy-v1.4.0')]}
        with self.assertRaisesRegex(assembly.AssemblyError,'unresolved proof suites:.*missing-suite'):
            assembly.validate_registry(registry,{'conformance/agent-proxy-v1.4.0','missing-suite'})

    def test_stale_test_anchor_is_named(self):
        row=self.suite('conformance/selected-providers');row['evidence'][0]['anchor']='test:nonexistent_factory_test'
        with self.assertRaisesRegex(assembly.AssemblyError,'selected-providers.*missing evidence anchor'):
            self.validate(row)

    def test_helper_function_cannot_substitute_for_test_anchor(self):
        with self.assertRaisesRegex(assembly.AssemblyError,'missing evidence anchor'):
            assembly.check_reference({'path':'crates/pi-coding-runtime/tests/tools.rs','anchor':'test:create_read_tool'},ROOT)

    def test_workspace_pass_cannot_substitute_for_concrete_proxy_proof(self):
        row=self.suite('conformance/agent-proxy-v1.4.0');row['commands']=[['cargo','test','--workspace','--locked','--offline']]
        with self.assertRaisesRegex(assembly.AssemblyError,'concrete Cargo package and test target'):
            self.validate(row)

    def test_target_feature_gate_cannot_be_removed(self):
        row=self.suite('conformance/agent-proxy-v1.4.0');row['commands'][0].remove('--all-features')
        with self.assertRaisesRegex(assembly.AssemblyError,'disabled by feature cfg'):
            self.validate(row)

    def test_unrelated_test_definition_cannot_be_unbound_evidence(self):
        row=self.suite('conformance/agent-proxy-v1.4.0')
        row['evidence'].append({'path':'crates/pi-telemetry/tests/contract.rs','anchor':'test:'+next(iter(assembly.active_tests((ROOT/'crates/pi-telemetry/tests/contract.rs').read_text())))})
        with self.assertRaisesRegex(assembly.AssemblyError,'unbound to a declared command'):
            self.validate(row)

    def test_inline_and_external_module_cfg_and_ignore_precede_test(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);base=root/'crates/fixture';(base/'tests').mkdir(parents=True)
            (base/'Cargo.toml').write_text('[package]\nname="fixture"\n[features]\nsuite=[]\nnative=[]\n')
            (base/'tests/proof.rs').write_text('''#![cfg(feature = "suite")]
#[cfg(feature = "native")] mod inline { #[test] fn gated_inline() {} }
#[cfg(feature = "native")] #[path = "external.rs"] mod external;
#[ignore] #[test] fn ignored_before_marker() {}
#[cfg(target_os = "unsupported")] #[test] fn unknown_condition() {}
''')
            (base/'tests/external.rs').write_text('#[test] fn gated_external() {}\n')
            command=['cargo','test','--locked','--offline','-p','fixture','--test','proof','--features','suite']
            for path,name in [('proof.rs','gated_inline'),('external.rs','gated_external'),('proof.rs','ignored_before_marker'),('proof.rs','unknown_condition')]:
                with self.subTest(name=name),self.assertRaises(assembly.AssemblyError):
                    assembly.check_command(command,[{'path':'crates/fixture/tests/'+path,'anchor':'test:'+name}],root)
            for path,name in [('proof.rs','inline::gated_inline'),('external.rs','external::gated_external')]:
                assembly.check_command([*command[:-1],'suite,native'],[{'path':'crates/fixture/tests/'+path,'anchor':'test:'+name}],root)

    def test_cargo_target_registration_and_required_features_are_enforced(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);base=root/'crates/fixture';(base/'tests').mkdir(parents=True)
            path=base/'Cargo.toml';prefix='[package]\nname="fixture"\nautotests=false\n[features]\nsuite=[]\n'
            path.write_text(prefix);(base/'tests/proof.rs').write_text('#[test] fn proof() {}')
            command=['cargo','test','--locked','--offline','-p','fixture','--test','proof']
            evidence=[{'path':'crates/fixture/tests/proof.rs','anchor':'test:proof'}]
            with self.assertRaisesRegex(assembly.AssemblyError,'not registered'):assembly.check_command(command,evidence,root)
            path.write_text(prefix+'[[test]]\nname="proof"\npath="tests/proof.rs"\nrequired-features=["suite"]\n')
            with self.assertRaisesRegex(assembly.AssemblyError,'required-features'):assembly.check_command(command,evidence,root)
            assembly.check_command([*command,'--features','suite'],evidence,root)

    def test_external_path_attributes_use_containing_file_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);folder=root/'nested';folder.mkdir()
            (root/'proof.rs').write_text('#[path="nested/support.rs"] mod support;')
            (folder/'support.rs').write_text('#[path="leaf.rs"] mod leaf;')
            (folder/'leaf.rs').write_text('#[test] fn witness() {}')
            tests,_=assembly.rust_test_inventory(root/'proof.rs',root)
            self.assertEqual([test['qualified'] for test in tests],['support::leaf::witness'])

    def test_adapter_family_keeps_all_exact_enabled_proof_routes(self):
        suite=self.suite('conformance/provider-traces/v1.4.0')
        assembly.validate_adapter_proofs(suite,{})
        for mutation,expected in [('target','missing target'),('anchor','missing exact anchor'),('profile','disabled by feature cfg')]:
            row=copy.deepcopy(suite)
            if mutation=='target':row['commands']=[command for command in row['commands'] if 'v140_release' not in command]
            elif mutation=='anchor':row['evidence']=[ref for ref in row['evidence'] if ref['cargo_target']!='v140_release']
            else:next(command for command in row['commands'] if 'v140_release' in command).remove('--all-features')
            with self.subTest(mutation=mutation),self.assertRaisesRegex(assembly.AssemblyError,expected):assembly.validate_adapter_proofs(row,{})

    def test_cargo_feature_closure_and_cfg_grammar_are_explicit(self):
        manifest={'features':{'default':['a'],'a':['b'],'b':[]}}
        features=assembly.cargo_features(['cargo','test'],manifest)
        self.assertTrue(assembly.cfg_enabled('all(feature = "a", any(feature = "b", feature = "c"), not(feature = "c"))',features))
        self.assertFalse(assembly.cfg_enabled('feature = "a"',assembly.cargo_features(['cargo','test','--no-default-features'],manifest)))
        self.assertTrue(assembly.cfg_enabled('all(unix, target_os = "linux")',features,cfg_platform=('posix','linux')))
        self.assertFalse(assembly.cfg_enabled('unix',features,cfg_platform=('nt','windows')))
        with self.assertRaisesRegex(assembly.AssemblyError,'unsupported proof cfg'):assembly.cfg_enabled('target_os = "unknown"',features)

    def test_accepted_limit_requires_public_evidence(self):
        row=self.suite('conformance/agent-proxy-v1.4.0');row.update(status='accepted-limit',limitations=[])
        with self.assertRaisesRegex(assembly.AssemblyError,'accepted-limit lacks evidence'):self.validate(row)
        row['limitations']=[{'path':'missing-limit.md'}]
        with self.assertRaisesRegex(assembly.AssemblyError,'missing evidence'):self.validate(row)

    def test_registry_cannot_duplicate_ownership(self):
        row=self.suite('conformance/agent-proxy-v1.4.0');row['primary_owner']='invented'
        with self.assertRaisesRegex(assembly.AssemblyError,'cannot duplicate ownership'):self.validate(row)

    def test_evidence_cannot_escape_public_root(self):
        with self.assertRaisesRegex(assembly.AssemblyError,'non-public evidence path'):assembly.check_reference({'path':'../outside.md'},ROOT)

    def test_unknown_or_online_command_cannot_register_as_proof(self):
        for command in [['curl','https://example.invalid'],['python3','unregistered.py','--check'],['python3','conformance/catalog/v1.4.0/check.py','--write']]:
            with self.subTest(command=command),self.assertRaises(assembly.AssemblyError):assembly.check_command(command,[],ROOT)

    def test_durable_runtime_requires_native_targets_and_preserves_deferred_scope(self):
        row=self.suite('conformance/durable-v1.4.0')
        targets={('pi-durable-session','conversation_observation'),
                 ('pi-durable-session','invariants'),('pi-durable-session','number_roundtrip'),
                 ('pi-durable-session','release_fixtures'),
                 ('pi-durable','bindings'),('pi-durable','close'),('pi-durable','context'),
                 ('pi-durable','initializer'),
                 ('pi-durable','invariants'),('pi-durable','release_fixtures'),
                 ('pi-durable','review_regressions'),('pi-durable','runtime_core'),
                 ('pi-durable','runtime_fixtures'),('pi-durable','views')}
        def identity(command):return command[command.index('-p')+1],command[command.index('--test')+1]
        self.assertEqual({identity(command) for command in row['commands']},targets)
        self.assertTrue(all(command==['cargo','test','--locked','--offline','-p',identity(command)[0],
                                     '--test',command[command.index('--test')+1],'--all-features']
                            for command in row['commands']))
        self.validate(row)
        extensions=self.suite('conformance/current-extensions')
        extension_target=('pi-extension-protocol','durable_host')
        self.assertIn(extension_target,{identity(command) for command in extensions['commands']})
        self.validate(extensions)
        registrations=[(row['id'],package,target) for package,target in targets]
        registrations.append((extensions['id'],*extension_target))
        for suite_id,package,target in registrations:
            reduced=copy.deepcopy(self.registry)
            suite=next(item for item in reduced['suites'] if item['id']==suite_id)
            suite['commands']=[command for command in suite['commands'] if identity(command)!=(package,target)]
            suite['evidence']=[reference for reference in suite['evidence']
                               if not (reference['path'].startswith('crates/'+package+'/') and reference['cargo_target']==target)]
            with self.subTest(package=package,target=target),mock.patch.object(assembly,'load_json',return_value=reduced):
                with self.assertRaisesRegex(assembly.AssemblyError,'stale generated proof registry'):
                    assembly.check_assembly(self.map_path,self.registry_path)
        routes={entry['path']:entry for entry in tomllib.loads((ROOT/'upstream/manifests/routes.toml').read_text())['entry']}
        for path in ['packages/durable/src/types.ts','packages/durable/src/session/session.ts',
                     'packages/durable/src/session/observation.ts','packages/durable/src/storage/sqlite/storage.ts']:
            self.assertEqual(routes[path]['surface'],'durable.session')
        for path in ['packages/durable/src/harness/scheduler.ts','packages/durable/src/tasks.ts',
                     'packages/durable/src/timers.ts']:
            self.assertEqual(routes[path]['surface'],'durable.scheduler')
        for path in ['packages/durable/src/harness/harness.ts','packages/durable/src/harness/registry.ts',
                     'packages/durable/src/harness/types.ts','packages/durable/src/harness/events.ts',
                     'packages/durable/src/harness/generation.ts','packages/durable/src/harness/tool.ts',
                     'packages/durable/src/harness/compaction.ts','packages/durable/src/harness/usage.ts',
                     'packages/durable/test/harness-nested-tools-restart.test.ts',
                     'packages/durable/test/harness-structured-output.test.ts']:
            self.assertEqual(routes[path]['surface'],'durable.runtime')
        for path in ['packages/durable/src/harness/context.ts','packages/durable/src/harness/view.ts',
                     'packages/durable/test/harness-context.test.ts','packages/durable/test/harness-view.test.ts']:
            self.assertEqual(routes[path]['surface'],'durable.context-views')
        for path in ['packages/durable/src/harness/task-graph.ts',
                     'packages/durable/test/harness-task-graph.test.ts',
                     'packages/durable/src/harness/telemetry.ts',
                     'packages/durable/test/harness-telemetry.test.ts',
                     'packages/durable/test/harness-telemetry-options.test.ts',
                     'packages/durable/src/codemode/codemode.ts',
                     'packages/durable/test/codemode-checkpoints.test.ts',
                     'packages/durable/src/tools/index.ts','packages/durable/src/env/index.ts']:
            self.assertEqual(routes[path]['surface'],'deferred.durable')
        surfaces={surface['id']:surface for surface in self.map['surface']}
        self.assertEqual(surfaces['durable.bindings']['authority'],'source-and-tests')
        self.assertIn('native compositions',surfaces['durable.bindings']['coverage_statement'])
        for deferred in ('TaskGraph','DURABLE_TELEMETRY_SCHEMA','persistent CodeMode'):
            self.assertIn(deferred,surfaces['deferred.durable']['coverage_statement'])
        for path in ['packages/durable/src/storage/jsonl/storage.ts',
                     'packages/durable/src/storage/sqlite/migrations.ts','packages/durable/src/storage/indexeddb.ts']:
            self.assertEqual(routes[path]['surface'],'excluded.host')
        self.assertEqual(routes['packages/chord/src/delta/index.ts']['surface'],'durable.delta')
        for path in ['packages/chord/src/delta/tracker.ts','packages/chord/src/host.ts','packages/chord/src/wire.ts']:
            self.assertEqual(routes[path]['surface'],'deferred.chord')

    def test_lost_source_owner_fails_by_name(self):
        data=copy.deepcopy(self.map);data['owner']=[row for row in data['owner'] if row['id']!='pi-agent-loop::selected']
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'map.toml';path.write_text(render_map(data))
            with self.assertRaisesRegex(ValueError,'agent.proxy.*pi-agent-loop::selected'):assembly.check_assembly(path,self.registry_path)

    def test_public_regeneration_rejects_deleted_reviewed_test_and_changed_profiles(self):
        # Exercise the actual CLI in a disposable copy. Its immutable objects are
        # shared read-only; neither original tests nor the root registry is edited.
        witness='crates/pi-ai-providers/tests/anthropic_messages.rs'
        required_name='installed_sdk_request_and_complete_pinned_event_snapshots'
        original_bytes=(ROOT/witness).read_bytes()
        with tempfile.TemporaryDirectory(prefix='reviewed-proof-retirement-') as directory:
            root=Path(directory)
            (root/'.git').symlink_to(ROOT/'.git',target_is_directory=True)
            shutil.copytree(ROOT/'scripts',root/'scripts',ignore=shutil.ignore_patterns('__pycache__'))
            shutil.copytree(ROOT/'crates',root/'crates')
            shutil.copyfile(ROOT/'workspace-topology.toml',root/'workspace-topology.toml')
            for reference in self.registry['preserved_inputs']:
                destination=root/reference['path'];destination.parent.mkdir(parents=True,exist_ok=True)
                shutil.copyfile(ROOT/reference['path'],destination)
            checker=root/'conformance/catalog/v1.4.0/check.py';checker.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(ROOT/'conformance/catalog/v1.4.0/check.py',checker)
            registry=root/'conformance/assembly.json';registry.write_bytes(self.registry_path.read_bytes())
            before=registry.read_bytes()
            command=[sys.executable,str(root/'scripts/generate_assembly_registry.py')]
            positive=subprocess.run([*command,'--check'],cwd=root,capture_output=True,text=True)
            self.assertEqual(positive.returncode,0,positive.stdout+positive.stderr)
            source=(root/witness).read_text();tokens=assembly.rust_tokens(source)
            start=next(index for index,token in enumerate(tokens) if token[0]=='fn' and tokens[index+1][0]==required_name)
            body=next(index for index in range(start,len(tokens)) if tokens[index][0]=='{')
            depth=1;end=body+1
            while depth:
                depth += (tokens[end][0]=='{') - (tokens[end][0]=='}');end+=1
            declaration=source.rfind('#[test]',0,tokens[start][1])
            self.assertGreaterEqual(declaration,0)
            (root/witness).write_text(source[:declaration]+source[tokens[end-1][2]:])
            self.assertNotIn(required_name,assembly.active_tests((root/witness).read_text()))
            rejected=subprocess.run([*command,'--write'],cwd=root,capture_output=True,text=True)
            self.assertNotEqual(rejected.returncode,0,rejected.stdout+rejected.stderr)
            self.assertIn('reviewed required proof identity retired',rejected.stderr)
            self.assertIn(required_name,rejected.stderr)
            self.assertEqual(registry.read_bytes(),before,'failed refresh published a smaller registry')
            (root/witness).write_bytes(original_bytes)
            manifest=root/'crates/pi-ai-providers/Cargo.toml';original_manifest=manifest.read_text()
            for old,new,reason in [
                ('provider-anthropic = ["wire-anthropic-messages", "catalog-anthropic"]',
                 'provider-anthropic = ["catalog-anthropic"]','reviewed proof feature binding retired'),
                ('url = { workspace = true, optional = true }',
                 'url = { workspace = true }','reviewed proof dependency binding changed')]:
                self.assertEqual(original_manifest.count(old),1)
                manifest.write_text(original_manifest.replace(old,new))
                rejected=subprocess.run([*command,'--write'],cwd=root,capture_output=True,text=True)
                self.assertNotEqual(rejected.returncode,0,rejected.stdout+rejected.stderr)
                self.assertIn(reason,rejected.stderr)
                self.assertEqual(registry.read_bytes(),before)
            manifest.write_text(original_manifest)
            generator=root/'scripts/generate_assembly_registry.py';original_generator=generator.read_text()
            self.assertEqual(original_generator.count("'anthropic_messages',"),1)
            generator.write_text(original_generator.replace("'anthropic_messages',",'',1))
            rejected=subprocess.run([*command,'--write'],cwd=root,capture_output=True,text=True)
            self.assertNotEqual(rejected.returncode,0,rejected.stdout+rejected.stderr)
            self.assertIn('reviewed proof command/profile retired',rejected.stderr)
            self.assertEqual(registry.read_bytes(),before)
            generator.write_text(original_generator)
            # New identities are allowed while the immutable reviewed set remains.
            (root/witness).write_text(source+'\n#[test] fn reviewed_addition_allowed() {}\n')
            added=subprocess.run([*command,'--write'],cwd=root,capture_output=True,text=True)
            self.assertEqual(added.returncode,0,added.stdout+added.stderr)
            refreshed=json.loads(registry.read_text())
            family=next(row for row in refreshed['suites'] if row['id']=='conformance/provider-traces/v1.4.0')
            self.assertTrue(any(ref.get('anchor')=='test:reviewed_addition_allowed' for ref in family['evidence']))
        self.assertEqual((ROOT/witness).read_bytes(),original_bytes)

    def test_reviewed_command_profiles_cannot_be_replaced(self):
        # The reviewed Linux acceptance set must not shrink on another host.
        required,_=assembly.reviewed_proof_requirements()
        try:
            with mock.patch.object(assembly.sys,'platform','win32'):
                assembly._reviewed_proofs.cache_clear()
                other_host,_=assembly.reviewed_proof_requirements()
                self.assertEqual(other_host,required)
        finally:
            assembly._reviewed_proofs.cache_clear()
        registry=copy.deepcopy(self.registry)
        row=next(suite for suite in registry['suites'] if suite['id']=='conformance/provider-traces/v1.4.0')
        row['commands'][0].remove('--all-features')
        with self.assertRaisesRegex(assembly.AssemblyError,'reviewed proof command/profile retired'):
            assembly.validate_reviewed_proofs(registry)
        from scripts import upstream_drift
        row=self.suite('conformance/agent-v1.4.0')
        row['commands']=[['python3','scripts/generate_upstream_manifests.py','--check']]
        row['evidence']=[{'path':'scripts/generate_upstream_manifests.py','anchor':'python:main'}]
        with self.assertRaisesRegex(ValueError,'reviewed proof command/profile retired'):
            upstream_drift.validate_suite_activation(row,ROOT)

    def test_excluded_compat_schemas_have_explicit_source_dispositions(self):
        symbols=tomllib.loads((ROOT/'upstream/manifests/ai-exports.toml').read_text())['symbol']
        self.assertEqual(len(symbols),1170)
        excluded={'BedrockCompat','BedrockCompatSchema','MistralConversationsCompat',
                  'MistralConversationsCompatSchema','SystemOneCompat','SystemOneCompatSchema'}
        selected={'AnthropicMessagesCompat','AnthropicMessagesCompatSchema',
                  'OpenAICompletionsCompat','OpenAICompletionsCompatSchema',
                  'OpenAIResponsesCompat','OpenAIResponsesCompatSchema','OpenRouterRouting'}
        source='packages/ai/src/providers/compat-schema.ts'
        source_symbols=[row for row in symbols if row['origin']==source]
        for name in excluded:
            matches=[row for row in source_symbols if row['name']==name]
            self.assertTrue(matches,name)
            self.assertTrue(all(row['disposition']=='outside-scope' for row in matches),name)
        subpath=[row for row in source_symbols if row['subpath']=='./providers/compat-schema']
        self.assertTrue(selected <= {row['name'] for row in subpath})
        self.assertTrue(all(row['disposition']=='included' for row in subpath if row['name'] in selected))
        root=[row for row in source_symbols if row['subpath']=='.' and row['name'] in excluded]
        self.assertEqual({row['name'] for row in root},{'BedrockCompat','MistralConversationsCompat','SystemOneCompat'})
        self.assertTrue(all(row['disposition']=='outside-scope' for row in root))

    def test_registry_regeneration_cannot_bless_changed_historical_fixtures(self):
        from scripts import generate_assembly_registry as generator
        witness=ROOT/self.registry['preserved_inputs'][0]['path']
        original=Path.read_bytes
        def read(path):
            return b'changed historical bytes' if path==witness else original(path)
        with mock.patch.object(Path,'read_bytes',autospec=True,side_effect=read), mock.patch.object(Path,'write_text') as write, redirect_stderr(io.StringIO()):
            self.assertEqual(generator.main(['--write']),1)
            write.assert_not_called()

    def test_historical_fixture_mutation_and_origin_relabelling_are_rejected(self):
        reference=copy.deepcopy(self.registry['preserved_inputs'][0]);reference['source_revision']='4a080252f06caf63a525d7873c5d94d3c23887e0'
        with self.assertRaisesRegex(assembly.AssemblyError,'historical evidence origin changed'):assembly.validate_preserved_inputs([reference])
        reference=copy.deepcopy(self.registry['preserved_inputs'][0])
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);path=root/reference['path'];path.parent.mkdir(parents=True);path.write_bytes(b'changed fixture')
            with self.assertRaisesRegex(assembly.AssemblyError,'preserved source/fixture identity changed'):assembly.validate_preserved_inputs([reference],root)


if __name__=='__main__':unittest.main()
