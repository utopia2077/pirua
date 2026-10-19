"""True local Git witnesses for immutable comparison, review and guarded patches."""
from __future__ import annotations

from copy import deepcopy
from argparse import Namespace
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch

from scripts import upstream_drift as drift
from scripts.upstream_inventory import Snapshot, AUTHORITIES
from scripts.upstream_objects import Objects, Invalid, Unavailable, canonical, digest, hash_bytes

DEPS = Path(os.environ.get('PIRUA_UPSTREAM_ROOT', str(drift.ROOT/'upstream/pi-mono')))


class Fixture:
    def __init__(self, root):
        self.root = root
        self.run('init', '-q')
        self.run('config', 'user.name', 'Fixture')
        self.run('config', 'user.email', 'fixture@example.invalid')

    def run(self, *args, data=None):
        return subprocess.run(['git', '-C', str(self.root), *args], input=data, capture_output=True, check=True).stdout.decode().strip()

    def write(self, path, text):
        target = self.root/path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def commit(self):
        self.run('add', '-A')
        self.run('commit', '-qm', 'Fixture revision')
        return self.run('rev-parse', 'HEAD')


def mapping(commit):
    surface = {'id':'source', 'authority':'source-and-tests', 'primary_owner':'rust::source', 'collaborators':[], 'disposition':'included', 'status':'source-landed', 'sync':['interface'], 'conformance':['conformance/fixture'], 'divergences':[], 'lifecycle':{'introduced_at':commit, 'removal_condition':'review upstream removal', 'reclassification_condition':'review replacement'}}
    return {'schema_version':2, 'upstream':{'repo':'fixture', 'checkout':'ignored', 'commit':commit},
            'policy':{'authority_classes':sorted(AUTHORITIES), 'dispositions':['included','outside-scope','disposable'], 'statuses':['source-landed','spec-only','redesign','excluded','disposable']},
            'owner':[{'id':'rust::source','kind':'rust','crate':'pi-ai-core'}], 'surface':[surface],
            'watch':[{'id':'pkg', 'root':'pkg', 'coverage':'complete','unmatched':'error'}],
            'route':[{'id':'all', 'watch':'pkg','surface':'source','role':'source','include':['pkg/**'],'exclude':[]}],
            'selector':[], 'manifest':[], 'divergence':[]}


def bind_map(fixture, data):
    result = deepcopy(data)
    snap = Snapshot(Objects(fixture.root), data['upstream']['commit'], data, DEPS, baseline=False)
    for watch in result['watch']:
        watch['route_digest'] = snap.route_digests[watch['id']]
        watch['baseline_path_count'] = sum(p.startswith(watch['root'] + '/') for p in snap.tree)
    return result


def overlay(comparison, **fields):
    return {'schema_version':1,'old_commit':comparison.old.commit,'new_commit':comparison.new.commit,'base_map_digest':digest(comparison.base), **fields}


def acknowledgements(comparison, subset=None):
    rows = []
    for ident, item in comparison.items.items():
        if subset is not None and ident not in subset:
            continue
        disposition = {'host-only':'host-only-reviewed', 'disposable-scaffold':'disposable-reviewed', 'generated-projection':'generated-refresh'}.get(item['authority'], 'implemented')
        rows.append({'id':ident,'old_commit':comparison.old.commit,'new_commit':comparison.new.commit,'surface_id':item['surface_id'],'primary_owner':item['primary_owner'],'disposition':disposition,'evidence':['conformance/fixture'],'decision_ref':'fixture-review'})
    return {'schema_version':1,'report_digest':comparison.report['report_digest'],'acknowledgements':rows}


class DriftTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.repo = self.root/'upstream'
        self.repo.mkdir()
        self.fixture = Fixture(self.repo)
        self.fixture.write('pkg/a.ts', 'export const answer = 1;\n')
        self.old = self.fixture.commit()
        self.base = bind_map(self.fixture, mapping(self.old))

    def tearDown(self):
        self.tmp.cleanup()

    def compare(self, new=None, data=None, ov=None):
        return drift.Comparison(Objects(self.repo), data or self.base, self.old, new or self.old, DEPS, ov)

    def change(self, text, path='pkg/a.ts'):
        self.fixture.write(path, text)
        return self.fixture.commit()

    def test_baseline_and_dirty_worktree_are_immutable(self):
        comparison = self.compare()
        self.assertEqual(drift.evaluate(comparison)[0], 0)
        self.fixture.write('pkg/a.ts', 'uncommitted and invalid TypeScript')
        self.assertEqual(self.compare().report, comparison.report)
        self.assertEqual(self.fixture.run('status','--porcelain'), 'M pkg/a.ts')
        self.assertNotEqual(self.old, '853a80d26c90a14c1886f0ebb8ffaae133ca2185')
        self.assertEqual(Objects(self.repo).commit(self.old), self.old)

    def test_full_commit_validation_and_missing_object(self):
        objects = Objects(self.repo)
        for value in [self.old[:12], 'HEAD', self.fixture.run('rev-parse','HEAD^{tree}')]:
            with self.assertRaises(Invalid): objects.commit(value)
        self.fixture.run('tag','-am','tag','tagged')
        with self.assertRaises(Invalid): objects.commit(self.fixture.run('rev-parse','tagged'))
        with self.assertRaises(Unavailable): objects.commit('f'*40)
        with patch('scripts.upstream_objects.subprocess.run', side_effect=FileNotFoundError):
            with self.assertRaises(Unavailable): objects.commit(self.old)

    def test_sha256_store_requires_exact_full_commit_object_id(self):
        root=self.root/'sha256'
        root.mkdir()
        subprocess.run(['git','-C',str(root),'init','-q','--object-format=sha256'],check=True)
        fixture=Fixture(root)
        fixture.write('pkg/a.ts','export const answer=1;\n')
        full=fixture.commit()
        self.assertEqual(len(full),64)
        objects=Objects(root)
        self.assertEqual(objects.commit(full),full)
        with self.assertRaisesRegex(Invalid,'full length'):
            objects.commit(full[:40])
        with self.assertRaises(Invalid):
            objects.tree(full[:40])
        base=bind_map(fixture,mapping(full))
        map_path=self.root/'sha256-map.toml'
        map_path.write_text(drift.render_map(base))
        command=[sys.executable,str(drift.ROOT/'scripts/upstream_drift.py'),'--upstream',str(root),'--map',str(map_path)]
        env={**os.environ,'PIRUA_UPSTREAM_ROOT':str(DEPS)}
        valid=subprocess.run(command,capture_output=True,env=env)
        self.assertEqual(valid.returncode,0,valid.stdout+valid.stderr)
        invalid=subprocess.run(command+['--old',full[:40]],capture_output=True,env=env)
        self.assertEqual(invalid.returncode,3,invalid.stdout+invalid.stderr)
        self.assertIn('full length',invalid.stdout.decode())

    def test_modified_canonical_id_metadata_and_partial_ack(self):
        new = self.change('export const answer = 2;\n')
        comparison = self.compare(new)
        code, report, _ = drift.evaluate(comparison)
        self.assertEqual(code, 2)
        item = report['items'][0]
        self.assertEqual((item['kind'],item['authority'],item['primary_owner'],item['conformance']), ('M','source-and-tests','rust::source',['conformance/fixture']))
        raw = '\0'.join([self.old,new,'M','pkg/a.ts','pkg/a.ts',self.fixture.run('rev-parse',self.old+':pkg/a.ts'),self.fixture.run('rev-parse',new+':pkg/a.ts'),'file','source'])
        self.assertEqual(item['id'], 'sha256:'+hashlib.sha256(raw.encode()).hexdigest())
        self.assertEqual(drift.evaluate(comparison, acknowledgements(comparison, set()))[0], 2)
        self.assertEqual(drift.evaluate(comparison, acknowledgements(comparison))[0], 0)
        self.assertEqual(report['report_digest'], drift.evaluate(comparison, acknowledgements(comparison))[1]['report_digest'])

    def test_mode_change_is_visible_without_oid_change(self):
        (self.repo/'pkg/a.ts').chmod(0o755)
        comparison = self.compare(self.fixture.commit())
        item = next(iter(comparison.items.values()))
        self.assertEqual(item['kind'],'M')
        self.assertEqual(item['old_oid'], item['new_oid'])
        self.assertEqual((item['old_mode'],item['new_mode']),('100644','100755'))
        self.assertEqual(comparison.old.route_digests, comparison.new.route_digests)

    def test_every_addition_requires_explicit_intake(self):
        new = self.change('export const other = 3;\n','pkg/b.ts')
        initial = self.compare(new)
        self.assertEqual(drift.evaluate(initial, acknowledgements(initial))[0],2)
        self.assertEqual(initial.report['pending_intake'],['pkg/b.ts'])
        ov = overlay(initial,intake=[{'path':'pkg/b.ts','route':'all','surface':'source','selectors':[]}])
        complete = self.compare(new,ov=ov)
        self.assertEqual(drift.evaluate(complete, acknowledgements(complete))[0],0)
        self.assertIn('new-path',next(iter(complete.items.values()))['reasons'])
        ov['new_commit'] = self.old
        with self.assertRaisesRegex(Invalid,'unbound'): self.compare(new,ov=ov)

    def test_unmatched_and_ambiguous_routes_are_structural(self):
        base = deepcopy(self.base)
        base['route'][0]['include'] = ['pkg/a.ts']
        base = bind_map(self.fixture,base)
        new = self.change('export const b = 1;\n','pkg/b.ts')
        initial = self.compare(new,data=base)
        self.assertEqual(drift.evaluate(initial)[0],3)
        route = {**base['route'][0], 'include':['pkg/**']}
        ov = overlay(initial, replace={'route':[route]}, intake=[{'path':'pkg/b.ts','route':'all','surface':'source','selectors':[]}])
        fixed = self.compare(new,data=base,ov=ov)
        self.assertEqual(drift.evaluate(fixed,acknowledgements(fixed))[0],0)
        ov['replace']['route'].append({**route,'id':'overlap'})
        with self.assertRaisesRegex(Invalid,'intake'):
            self.compare(new,data=base,ov=ov)

    def test_unique_rename_and_duplicate_blob_ambiguity(self):
        (self.repo/'pkg/a.ts').rename(self.repo/'pkg/b.ts')
        new = self.fixture.commit()
        initial = self.compare(new)
        self.assertEqual({i['kind'] for i in initial.items.values()},{'R'})
        self.assertEqual(initial.pending_intake,['pkg/b.ts'])
        accepted = self.compare(new,ov=overlay(initial,intake=[{'path':'pkg/b.ts','route':'all','surface':'source','selectors':[]}]))
        self.assertEqual(drift.evaluate(accepted,acknowledgements(accepted))[0],0)
        self.assertFalse(any('ownership-transition' in item['reasons'] for item in accepted.items.values()))
        metadata=deepcopy(self.base['surface'])
        metadata[0]['conformance']=['conformance/renamed']
        changed=self.compare(new,ov=overlay(initial,replace={'surface':metadata},intake=[{'path':'pkg/b.ts','route':'all','surface':'source','selectors':[]}]))
        selected=next(item for item in changed.items.values() if item['selector_id']=='surface-metadata')
        self.assertEqual(selected['kind'],'R')
        self.assertEqual(selected['conformance'],['conformance/fixture','conformance/renamed'])
        self.assertNotEqual(changed.report['report_digest'],accepted.report['report_digest'])
        self.fixture.write('pkg/c.ts',(self.repo/'pkg/b.ts').read_text())
        comparison = self.compare(self.fixture.commit())
        self.assertEqual(sorted(i['kind'] for i in comparison.items.values()),['A','A','D'])

    def test_cross_owner_rename_requires_bound_endpoint_migration(self):
        (self.repo/'pkg/a.ts').unlink()
        self.fixture.write('pkg/left/a.ts','export const answer = 1;\n')
        self.fixture.write('pkg/left/keep.ts','export const leftKeep = 2;\n')
        self.fixture.write('pkg/right/keep.ts','export const rightKeep = 3;\n')
        self.old=self.fixture.commit()
        base=mapping(self.old)
        base['owner'].append({'id':'rust::destination','kind':'rust','crate':'pi-ai'})
        destination={**deepcopy(base['surface'][0]),'id':'destination','primary_owner':'rust::destination'}
        base['surface'].append(destination)
        base['route']=[{**base['route'][0],'id':name,'surface':surface,'include':['pkg/'+name+'/**']} for name,surface in [('left','source'),('right','destination')]]
        base=bind_map(self.fixture,base)
        (self.repo/'pkg/left/a.ts').rename(self.repo/'pkg/right/a.ts')
        new=self.fixture.commit()
        initial=self.compare(new,data=base)
        ov=overlay(initial,intake=[{'path':'pkg/right/a.ts','route':'right','surface':'destination','selectors':[]}])
        blocked=self.compare(new,data=base,ov=ov)
        self.assertEqual(drift.evaluate(blocked)[0],3)
        self.assertTrue(any('ownership' in error for error in blocked.errors))
        binding={'old_path':'pkg/left/a.ts','new_path':'pkg/right/a.ts','old_ownership':{'source':'rust::source'},'new_ownership':{'destination':'rust::destination'},'old_registrations':{'rust::source':base['owner'][0]},'new_registrations':{'rust::destination':base['owner'][1]}}
        ov['ownership']=[binding]
        comparison=self.compare(new,data=base,ov=ov)
        self.assertFalse(comparison.errors)
        self.assertEqual(set(blocked.items),set(comparison.items))
        self.assertNotEqual(blocked.report['report_digest'],comparison.report['report_digest'])
        self.assertEqual({item['kind'] for item in comparison.items.values()},{'R'})
        self.assertEqual({item['selector_id'] for item in comparison.items.values()},{'file','ownership'})
        with self.assertRaisesRegex(Invalid,'report digest'):
            drift.evaluate(comparison,acknowledgements(blocked))
        with self.assertRaisesRegex(Invalid,'responsibility-migration'):
            drift.evaluate(comparison,acknowledgements(comparison))
        document=acknowledgements(comparison)
        endpoints={'old':binding['old_ownership'],'new':binding['new_ownership']}
        for row in document['acknowledgements']:
            row.update(disposition='responsibility-migration',related_items=list(comparison.items),responsibilities=endpoints)
        self.assertEqual(drift.evaluate(comparison,document)[0],0)
        wrong=deepcopy(document)
        wrong['acknowledgements'][0]['responsibilities']['new']={'destination':'rust::source'}
        with self.assertRaisesRegex(Invalid,'endpoint ownership'):
            drift.evaluate(comparison,wrong)
        for field in ['old_path','new_path']:
            wrong=deepcopy(ov)
            wrong['ownership'][0][field]='pkg/wrong.ts'
            with self.assertRaisesRegex(Invalid,'unused'):
                self.compare(new,data=base,ov=wrong)
        wrong=deepcopy(ov)
        wrong['ownership'][0].pop('old_path')
        wrong['ownership'][0].pop('new_path')
        wrong['ownership'][0]['path']='pkg/right/a.ts'
        with self.assertRaisesRegex(Invalid,'unused'):
            self.compare(new,data=base,ov=wrong)

    def test_rename_registration_and_collaborator_changes_require_migration(self):
        (self.repo/'pkg/a.ts').rename(self.repo/'pkg/b.ts')
        new=self.fixture.commit()
        for collaborator in [False,True]:
            base=deepcopy(self.base)
            if collaborator:
                base['owner'].append({'id':'rust::collaborator','kind':'rust','crate':'pi-ai-core'})
                base['surface'][0]['collaborators']=['rust::collaborator']
            owners=deepcopy(base['owner'])
            changed='rust::collaborator' if collaborator else 'rust::source'
            next(row for row in owners if row['id']==changed)['crate']='pi-ai'
            initial=self.compare(new,data=base)
            ov=overlay(initial,replace={'owner':owners},intake=[{'path':'pkg/b.ts','route':'all','surface':'source','selectors':[]}])
            blocked=self.compare(new,data=base,ov=ov)
            self.assertEqual(drift.evaluate(blocked)[0],3)
            binding={'old_path':'pkg/a.ts','new_path':'pkg/b.ts','old_ownership':{'source':'rust::source'},'new_ownership':{'source':'rust::source'},'old_registrations':{row['id']:row for row in base['owner']},'new_registrations':{row['id']:row for row in owners}}
            ov['ownership']=[binding]
            comparison=self.compare(new,data=base,ov=ov)
            with self.assertRaisesRegex(Invalid,'responsibility-migration'):
                drift.evaluate(comparison,acknowledgements(comparison))
            document=acknowledgements(comparison)
            for row in document['acknowledgements']:
                row.update(disposition='responsibility-migration',related_items=list(comparison.items),responsibilities={'old':binding['old_ownership'],'new':binding['new_ownership']})
            self.assertEqual(drift.evaluate(comparison,document)[0],0)
            self.assertEqual({item['kind'] for item in comparison.items.values()},{'R'})
            wrong=deepcopy(ov)
            wrong['ownership'][0]['new_registrations']=deepcopy(wrong['ownership'][0]['new_registrations'])
            wrong['ownership'][0]['new_registrations'][changed]['crate']='pi-agent'
            with self.assertRaisesRegex(Invalid,'unused'):
                self.compare(new,data=base,ov=wrong)

    def test_deleted_partition_and_last_route_require_cleanup(self):
        base = deepcopy(self.base)
        base['route'][0]['selector_partition']='one'
        base['selector']=[{'id':'whole','partition':'one','path':'pkg/a.ts','kind':'file','surface':'source'}]
        (self.repo/'pkg/a.ts').unlink()
        new = self.fixture.commit()
        initial = self.compare(new,data=base)
        self.assertEqual(drift.evaluate(initial)[0],3)
        fixed = self.compare(new,data=base,ov=overlay(initial,replace={'route':[],'selector':[]}))
        self.assertEqual({i['kind'] for i in fixed.items.values()},{'D'})
        self.assertEqual(drift.evaluate(fixed,acknowledgements(fixed))[0],0)

    def markdown_base(self):
        self.fixture.write('pkg/doc.md','# One\nfirst\n## Nested\nnested\n# Two\nsecond\n')
        self.old=self.fixture.commit()
        base=mapping(self.old)
        base['route']=[{'id':'source','watch':'pkg','surface':'source','role':'source','include':['pkg/a.ts'],'exclude':[]}, {'id':'doc','watch':'pkg','surface':'source','role':'spec','include':['pkg/doc.md'],'exclude':[],'selector_partition':'doc'}]
        base['selector']=[{'id':'one','path':'pkg/doc.md','partition':'doc','kind':'markdown-section','heading':'One','exclude_headings':['Nested'],'surface':'source'}, {'id':'nested','path':'pkg/doc.md','partition':'doc','kind':'markdown-section','heading':'Nested','surface':'source'}, {'id':'two','path':'pkg/doc.md','partition':'doc','kind':'markdown-section','heading':'Two','surface':'source'}]
        self.base=bind_map(self.fixture,base)

    def test_selector_hunks_boundaries_and_heading_structure(self):
        self.markdown_base()
        new=self.change('# One\nfirst changed\n## Nested\nnested changed\n# Two\nsecond changed\n','pkg/doc.md')
        comparison=self.compare(new)
        self.assertEqual({i['selector_id'] for i in comparison.items.values()},{'one','nested','two'})
        self.assertEqual(drift.evaluate(comparison)[0],2)
        new=self.change('# One\nfirst\n## Nested\nnested\n### Added\nnew heading\n# Two\nsecond\n','pkg/doc.md')
        comparison=self.compare(new)
        self.assertEqual(drift.evaluate(comparison)[0],3)
        fixed=self.compare(new,ov=overlay(comparison,inventory_digest=comparison.new.inventory_digest))
        self.assertEqual(drift.evaluate(fixed,acknowledgements(fixed))[0],0)

    def test_inserted_prose_moves_lines_without_heading_drift(self):
        self.markdown_base()
        new=self.change('# One\ninserted\nfirst\n## Nested\nnested\n# Two\nsecond\n','pkg/doc.md')
        comparison=self.compare(new)
        self.assertEqual(drift.evaluate(comparison)[0],2)
        self.assertEqual({i['selector_id'] for i in comparison.items.values()},{'one'})

    def test_missing_exclusion_gap_overlap_and_sentinels_block(self):
        self.markdown_base()
        for mutation in ['missing-exclusion','gap','overlap','duplicate-heading']:
            base=deepcopy(self.base)
            if mutation=='missing-exclusion': base['selector'][0]['exclude_headings']=['Absent']
            if mutation=='gap': base['selector']=base['selector'][:-1]
            if mutation=='overlap': base['selector'][0]['exclude_headings']=[]
            if mutation=='duplicate-heading': base['selector'][2]['heading']='One'
            self.assertEqual(drift.evaluate(self.compare(data=base))[0],3,mutation)
        base=deepcopy(self.base)
        base['selector']=[{'id':'span','path':'pkg/doc.md','partition':'doc','kind':'text-span','start':'# One','end':'$EOF','surface':'source'}]
        self.assertEqual(drift.evaluate(self.compare(data=base))[0],0)
        base['selector'][0]['end']='# Missing'
        self.assertEqual(drift.evaluate(self.compare(data=base))[0],3)

    def export_base(self):
        self.fixture.write('pkg/package.json',json.dumps({'exports':{'.':'./dist/a.js','./all':'./dist/a.js'}}))
        self.old=self.fixture.commit()
        self.base=mapping(self.old)
        self.base['manifest']=[{'id':'public','kind':'public-exports','source':'pkg/package.json+src/index.ts','path':'inventory.json'}]
        # Package exports resolve src; use a source tree rather than a working-file oracle.
        (self.repo/'pkg/a.ts').rename(self.repo/'pkg/src.ts')
        self.fixture.write('pkg/src/a.ts','export const answer = 1;\n')
        self.old=self.fixture.commit()
        self.base['upstream']['commit']=self.old
        self.base=bind_map(self.fixture,self.base)

    def test_ast_exports_enum_namespace_multiline_and_comment(self):
        self.export_base()
        text='// export const ghost = 1;\nexport enum Color { Red, Blue }\nexport const one = 1, two = 2;\nexport function sum(\n a: number,\n b: number\n) { return a+b; }\n'
        new=self.change(text,'pkg/src/a.ts')
        comparison=self.compare(new)
        exports=[r for r in comparison.new.inventory.values() if r['family']=='exports']
        self.assertEqual({r['payload']['name'] for r in exports},{'Color','one','two','sum'})
        self.assertEqual(len(exports),8)
        self.assertEqual({r['payload']['kind'] for r in exports if r['payload']['name']=='Color'},{'type-and-value'})
        new=self.change(text.replace('a+b','a-b'),'pkg/src/a.ts')
        self.assertTrue(any(i['selector_id'].startswith('inventory:exports:') for i in self.compare(new).items.values()))
        self.fixture.write('pkg/src/other.ts','export const hello = 1;\n')
        new=self.change('export * as ns from "./other.ts";\n','pkg/src/a.ts')
        self.assertFalse(self.compare(new).report['errors'])

    def test_missing_reexport_and_invalid_grammar_never_empty_success(self):
        self.export_base()
        for source in ['export * from "./missing.ts";\n','export = answer;\n','export const = ;\n']:
            new=self.change(source,'pkg/src/a.ts')
            comparison=self.compare(new)
            self.assertEqual(drift.evaluate(comparison)[0],3,source)
            self.assertTrue(any('inventory uncertainty' in x for x in comparison.errors))

    def test_test_helper_closure_and_unresolved_import(self):
        self.fixture.write('pkg/helper.ts','export const helper=1;\n')
        self.fixture.write('pkg/a.test.ts','import { helper } from "./helper.ts";\n')
        self.old=self.fixture.commit()
        self.base=bind_map(self.fixture,mapping(self.old))
        new=self.change('export const helper=2;\n','pkg/helper.ts')
        comparison=self.compare(new)
        self.assertTrue(any('inventory:tests' in i['reasons'] for i in comparison.items.values()))
        new=self.change('import "./absent.ts";\n','pkg/a.test.ts')
        self.assertEqual(drift.evaluate(self.compare(new))[0],3)

    def test_symlink_binary_and_submodule_are_explicit_uncertainty(self):
        (self.repo/'pkg/link').symlink_to('a.ts')
        (self.repo/'pkg/raw').write_bytes(b'\xff\x00')
        new=self.fixture.commit()
        comparison=self.compare(new)
        self.assertEqual(drift.evaluate(comparison)[0],3)
        self.assertTrue(any('unsupported object/mode' in x for x in comparison.errors))
        self.fixture.run('update-index','--add','--cacheinfo','160000,'+self.old+',pkg/sub')
        self.fixture.run('commit','-qm','Submodule fixture')
        self.assertEqual(drift.evaluate(self.compare(self.fixture.run('rev-parse','HEAD')))[0],3)

    def test_acknowledgement_stale_duplicate_owner_and_disposition_controls(self):
        comparison=self.compare(self.change('export const answer=2;\n'))
        for field,value in [('report_digest','stale'),('primary_owner','wrong'),('new_commit',self.old),('disposition','passed')]:
            document=acknowledgements(comparison)
            target=document if field=='report_digest' else document['acknowledgements'][0]
            target[field]=value
            with self.assertRaises(Invalid): drift.evaluate(comparison,document)
        document=acknowledgements(comparison)
        document['acknowledgements']*=2
        with self.assertRaisesRegex(Invalid,'duplicate'): drift.evaluate(comparison,document)

    def test_owner_change_binds_report_and_requires_migration(self):
        initial=self.compare()
        replacement=deepcopy(self.base['surface'])
        replacement[0]['primary_owner']='rust::replacement'
        owners=self.base['owner']+[{'id':'rust::replacement','kind':'rust','crate':'pi-ai-core'}]
        ov=overlay(initial,replace={'owner':owners,'surface':replacement})
        bad=self.compare(ov=ov)
        self.assertEqual(drift.evaluate(bad)[0],3)
        ov['ownership']=[{'path':'pkg/a.ts','old_ownership':{'source':'rust::source'},'new_ownership':{'source':'rust::replacement'},'old_registrations':{'rust::source':self.base['owner'][0]},'new_registrations':{'rust::replacement':owners[-1]}}]
        comparison=self.compare(ov=ov)
        document=acknowledgements(comparison)
        with self.assertRaises(Invalid): drift.evaluate(comparison,document)
        for row in document['acknowledgements']:
            row.update(disposition='responsibility-migration',related_items=list(comparison.items),responsibilities={'old':{'source':'rust::source'},'new':{'source':'rust::replacement'}})
        self.assertEqual(drift.evaluate(comparison,document)[0],0)
        self.assertNotEqual(initial.report['report_digest'],comparison.report['report_digest'])

    def proof_case(self):
        self.fixture.write('pkg/new.ts','export const acceptedNewFile = 1;\n')
        new=self.change('export enum Color { Red, Blue }\n')
        initial=self.compare(new)
        comparison=self.compare(new,ov=overlay(initial,intake=[{'path':'pkg/new.ts','route':'all','surface':'source','selectors':[]}]))
        root=self.root/'proof'
        root.mkdir()
        proof=Fixture(root)
        proof.write('proof_fixture.py',(drift.ROOT/'conformance/upstream-drift/proof_fixture.py').read_text())
        proof.write('case.json',json.dumps({'store':str(self.repo),'commit':new,'path':'pkg/a.ts','intake_path':'pkg/new.ts'}))
        proof.write('overlay.json',canonical(comparison.overlay).decode())
        proof.commit()
        suite={'id':'conformance/fixture','status':'executable','commands':[['python3','proof_fixture.py']], 'evidence':[{'path':'proof_fixture.py','anchor':'text:candidate enum contract missing'}], 'limitations':[]}
        registry=root/'registry.json'
        registry.write_bytes(canonical({'schema_version':1,'suites':[suite]}))
        result=subprocess.run(suite['commands'][0],cwd=root,capture_output=True)
        self.assertEqual(result.returncode,0,result.stderr)
        (root/'suite.log').write_bytes(result.stdout+result.stderr)
        receipt={'suite':suite['id'],'commands':suite['commands'],'exit_code':result.returncode,'binding':drift.binding(comparison.report),'inputs':drift.proof_inputs(root),'registry_digest':digest(suite),'parser':comparison.report['parser'],'logs':[{'path':'suite.log','sha256':hash_bytes((root/'suite.log').read_bytes())}],'artifacts':[{'path':'artifact.json','sha256':hash_bytes((root/'artifact.json').read_bytes())}]}
        return comparison,root,registry,{'schema_version':1,'receipts':[receipt]}

    def test_actual_proof_positive_and_failed_stale_tampered_controls(self):
        comparison,root,registry,receipts=self.proof_case()
        self.assertEqual(drift.evaluate(comparison,acknowledgements(comparison))[0],0)
        proofs=drift.verify_proofs(comparison,registry,receipts,root)
        bundle=drift.patch_bundle(comparison,drift.validate_acknowledgements(comparison,acknowledgements(comparison)),proofs,drift.render_map(self.base).encode())
        self.assertIn('+++ b/upstream-map.toml',bundle['patch'])
        (root/'upstream-map.toml').write_text(drift.render_map(self.base))
        applied=subprocess.run(['git','-C',str(root),'apply','--check','-'],input=bundle['patch'].encode(),capture_output=True)
        self.assertEqual(applied.returncode,0,applied.stderr)
        subprocess.run(['git','-C',str(root),'apply','-'],input=bundle['patch'].encode(),check=True)
        self.assertEqual((root/'upstream-map.toml').read_text(),bundle['candidate_map'])
        candidate=tomllib.loads(bundle['candidate_map'])
        self.assertEqual(candidate['upstream']['commit'],comparison.new.commit)
        rebound=Snapshot(Objects(self.repo),comparison.new.commit,candidate,DEPS,baseline=True)
        self.assertFalse(rebound.errors)
        self.assertEqual(rebound.inventory_digest,comparison.new.inventory_digest)
        self.assertEqual(bundle,drift.patch_bundle(comparison,drift.validate_acknowledgements(comparison,acknowledgements(comparison)),proofs,drift.render_map(self.base).encode()))
        for field,value in [('exit_code',1),('exit_code',0.0),('exit_code',True),('commands',[['arbitrary','command']]),('binding',{}),('inputs',{}),('registry_digest','bad')]:
            invalid=deepcopy(receipts)
            invalid['receipts'][0][field]=value
            with self.assertRaises(Invalid): drift.verify_proofs(comparison,registry,invalid,root)
        (root/'suite.log').write_text('tampered')
        with self.assertRaisesRegex(Invalid,'hash mismatch'): drift.verify_proofs(comparison,registry,receipts,root)
        self.assertEqual(self.base['upstream']['commit'],self.old)

    def test_proof_missing_suite_accepted_limit_and_source_tampering(self):
        comparison,root,registry,receipts=self.proof_case()
        doc=json.loads(registry.read_bytes())
        doc['suites'][0]['status']='accepted-limit'
        doc['suites'][0]['limitations']=[{'path':'proof_fixture.py','reason':'missing contract'}]
        registry.write_bytes(canonical(doc))
        with self.assertRaisesRegex(Invalid,'accepted-limit'): drift.verify_proofs(comparison,registry,receipts,root)
        doc['suites']=[]
        registry.write_bytes(canonical(doc))
        with self.assertRaisesRegex(Invalid,'unavailable'): drift.verify_proofs(comparison,registry,receipts,root)

    def test_repair_disabled_red_then_restored_green(self):
        new=self.change('export const b=2;\n','pkg/b.ts')
        initial=self.compare(new)
        ov=overlay(initial,intake=[{'path':'pkg/b.ts','route':'all','surface':'source','selectors':[]}])
        repaired=self.compare(new,ov=ov)
        self.assertEqual(drift.evaluate(repaired,acknowledgements(repaired))[0],0)
        # Disable precisely the intake repair. The broad route still matches.
        disabled=deepcopy(ov)
        disabled['intake']=[]
        red=self.compare(new,ov=disabled)
        self.assertEqual(drift.evaluate(red,acknowledgements(red))[0],2)
        self.assertEqual(red.pending_intake,['pkg/b.ts'])
        restored=self.compare(new,ov=ov)
        self.assertEqual(drift.evaluate(restored,acknowledgements(restored))[0],0)
        self.assertEqual(restored.report,repaired.report)

    def test_typescript_symbol_and_json_pointer_partitions(self):
        base=deepcopy(self.base)
        base['route'][0]['selector_partition']='symbols'
        base['selector']=[{'id':'answer','partition':'symbols','path':'pkg/a.ts','kind':'typescript-symbol','symbol':'answer','export_kind':'value','surface':'source'}]
        self.assertEqual(drift.evaluate(self.compare(data=base))[0],0)
        changed=self.change('export const answer = 2;\n')
        comparison=self.compare(changed,data=base)
        self.assertEqual({i['selector_id'] for i in comparison.items.values()},{'answer'})
        bad=deepcopy(base)
        bad['selector'][0]['export_kind']='type'
        self.assertEqual(drift.evaluate(self.compare(changed,data=bad))[0],3)
        self.fixture.write('pkg/data.json','{"one":1,"two":{"value":true}}\n')
        self.old=self.fixture.commit()
        base=mapping(self.old)
        base['route']=[{**base['route'][0],'exclude':['pkg/data.json']},{'id':'json','watch':'pkg','surface':'source','role':'schema','include':['pkg/data.json'],'exclude':[],'selector_partition':'json'}]
        base['selector']=[{'id':key,'partition':'json','path':'pkg/data.json','kind':'json-pointer','pointer':'/'+key,'surface':'source'} for key in ['one','two']]
        self.base=bind_map(self.fixture,base)
        changed=self.change('{"one":"retyped","two":{"value":false}}\n','pkg/data.json')
        comparison=self.compare(changed)
        self.assertEqual({i['selector_id'] for i in comparison.items.values()},{'one','two'})
        changed=self.change('{"one":1,"two":{},"three":3}\n','pkg/data.json')
        comparison=self.compare(changed)
        self.assertEqual(drift.evaluate(comparison)[0],3)
        selectors=deepcopy(base['selector'])+[{'id':'three','partition':'json','path':'pkg/data.json','kind':'json-pointer','pointer':'/three','surface':'source'}]
        fixed=self.compare(changed,ov=overlay(comparison,replace={'selector':selectors}))
        self.assertEqual(drift.evaluate(fixed,acknowledgements(fixed))[0],0)

    def test_ambient_declaration_occurrence_keeps_namespace_and_changes(self):
        self.export_base()
        text='declare module "*.json" { const value: unknown; export default value; }\n'
        new=self.change(text,'pkg/src/a.ts')
        comparison=self.compare(new)
        rows=[r for r in comparison.new.inventory.values() if r['family']=='exports']
        self.assertEqual(len(rows),2)
        self.assertEqual({r['payload']['ambient_module'] for r in rows},{'*.json'})
        self.assertEqual({r['payload']['name'] for r in rows},{'default'})
        # A namespace rename preserves occurrence identity but changes evidence.
        newer=self.change(text.replace('*.json','*.data'),'pkg/src/a.ts')
        latest=self.compare(newer)
        self.assertNotEqual(comparison.new.inventory_digest,latest.new.inventory_digest)
        self.assertTrue(any('inventory:exports' in i['reasons'] for i in latest.items.values()))

    def test_package_conditional_exports_subpaths_and_external_unknown(self):
        self.export_base()
        package={'exports':{'.':{'types':'./dist/a.d.ts','require':'./dist/a.js'},'./extra':'./dist/a.js'}}
        new=self.change(json.dumps(package),'pkg/package.json')
        comparison=self.compare(new)
        self.assertFalse(comparison.errors)
        self.assertTrue(any('inventory:subpaths' in i['reasons'] for i in comparison.items.values()))
        self.assertEqual({r['payload']['subpath'] for r in comparison.new.inventory.values() if r['family']=='exports'},{'.','./extra'})
        package['exports']['.']={'import':{'node':'./dist/a.js'}}
        new=self.change(json.dumps(package),'pkg/package.json')
        self.assertEqual(drift.evaluate(self.compare(new))[0],3)
        new=self.change('export * from "unknown-package";\n','pkg/src/a.ts')
        self.assertEqual(drift.evaluate(self.compare(new))[0],3)

    def test_provider_inventory_namespaces_identity_api_oauth_and_catalog(self):
        self.fixture.write('packages/ai/src/providers/all.ts','import { demoProvider } from "./demo.ts";\nimport { imageProvider } from "./image.ts";\nexport function builtinProviders() { return [demoProvider()]; }\nexport function builtinImagesProviders() { return [imageProvider()]; }\n')
        source='export function demoProvider() { return createProvider({ id:"shared", api:{"v1":handler}, auth:{oauth:loadDemoOAuth}, models:Object.values(CATALOG) }); }\n'
        self.fixture.write('packages/ai/src/providers/demo.ts',source)
        self.fixture.write('packages/ai/src/providers/image.ts','export function imageProvider() { return createImagesProvider({id:"shared",api:{"images":handler}}); }\n')
        self.old=self.fixture.commit()
        base=mapping(self.old)
        base['watch'][0]['root']='packages/ai'
        base['route'][0]['include']=['packages/ai/**']
        self.base=bind_map(self.fixture,base)
        baseline=self.compare()
        self.assertFalse(baseline.errors)
        rows=[r for r in baseline.new.inventory.values() if r['family']=='providers']
        self.assertEqual({r['key'] for r in rows},{'text::shared','image::shared'})
        for revised in [source.replace('"shared"','"new-id"'),source.replace('"v1"','"v2"'),source.replace('loadDemoOAuth','loadOtherOAuth'),source.replace('CATALOG','NEXT_CATALOG')]:
            changed=self.change(revised,'packages/ai/src/providers/demo.ts')
            comparison=self.compare(changed)
            self.assertEqual(drift.evaluate(comparison)[0],2,comparison.errors)
            self.assertTrue(any('inventory:providers' in i['reasons'] for i in comparison.items.values()))
        changed=self.change('export function demoProvider(){ return unknownFactory(); }\n','packages/ai/src/providers/demo.ts')
        self.assertEqual(drift.evaluate(self.compare(changed))[0],3)

    def generator_base(self):
        self.fixture.write('pkg/generator.ts','import { constant } from "./constants.ts";\nexport const generate = constant;\n')
        self.fixture.write('pkg/constants.ts','export const constant = 1;\n')
        self.fixture.write('pkg/built.ts','export const built = 1;\n')
        self.fixture.write('pkg/capture.json','{"feed":"fixture"}\n')
        self.old=self.fixture.commit()
        base=mapping(self.old)
        generator={**deepcopy(base['surface'][0]),'id':'generator','authority':'generator'}
        projection={**deepcopy(base['surface'][0]),'id':'projection','authority':'generated-projection','generated_by':'generator','provenance_status':'missing-intake-snapshot'}
        base['surface'] += [generator,projection]
        base['route'][0]['exclude']=['pkg/generator.ts','pkg/built.ts']
        base['route'] += [{'id':'generator','watch':'pkg','surface':'generator','role':'generator','include':['pkg/generator.ts'],'exclude':[]},{'id':'projection','watch':'pkg','surface':'projection','role':'generated','include':['pkg/built.ts'],'exclude':[]}]
        self.base=bind_map(self.fixture,base)

    def test_generator_imported_constant_notifies_projection_and_provenance(self):
        self.generator_base()
        self.assertEqual(drift.evaluate(self.compare())[0],0)
        changed=self.change('export const constant = 2;\n','pkg/constants.ts')
        comparison=self.compare(changed)
        self.assertEqual(drift.evaluate(comparison)[0],3)
        self.assertTrue(any('generated provenance' in e for e in comparison.errors))
        self.assertTrue({'generator','projection'} <= {i['surface_id'] for i in comparison.items.values()})
        self.assertTrue(any(i['selector_id']=='provenance' for i in comparison.items.values()))
        surfaces=deepcopy(self.base['surface'])
        projection=next(x for x in surfaces if x['id']=='projection')
        projection['provenance_status']='complete'
        entries=Objects(self.repo).tree(changed)
        projection['provenance_snapshot']={'id':'fixture-feed','source_ids':['fixture'], 'capture':'pkg/capture.json','sha256':hash_bytes(Objects(self.repo).blob(entries['pkg/capture.json'])), 'inputs':{p:hash_bytes(Objects(self.repo).blob(entries[p])) for p in ['pkg/generator.ts','pkg/constants.ts']},'outputs':{'pkg/built.ts':hash_bytes(Objects(self.repo).blob(entries['pkg/built.ts']))}}
        repaired=self.compare(changed,ov=overlay(comparison,replace={'surface':surfaces}))
        self.assertEqual(drift.evaluate(repaired,acknowledgements(repaired))[0],0,repaired.errors)
        projection['provenance_snapshot']['inputs']['pkg/constants.ts']='sha256:'+'0'*64
        tampered=self.compare(changed,ov=overlay(comparison,replace={'surface':surfaces}))
        self.assertEqual(drift.evaluate(tampered)[0],3)
        changed=self.change('export const built = 2;\n','pkg/built.ts')
        self.assertEqual(drift.evaluate(self.compare(changed))[0],3)

    def test_generator_external_relative_dependency_is_blocking(self):
        self.generator_base()
        self.fixture.write('outside.ts','export const external=1;\n')
        changed=self.change('import { external } from "../outside.ts";\nexport const generate=external;\n','pkg/generator.ts')
        comparison=self.compare(changed)
        self.assertEqual(drift.evaluate(comparison)[0],3)
        self.assertTrue(any('provenance' in e for e in comparison.errors))

    def test_host_disposable_compatibility_remain_pending(self):
        changed=self.change('export const answer=2;\n')
        for authority,disposition,status in [('host-only','outside-scope','excluded'),('disposable-scaffold','disposable','disposable'),('compatibility','included','source-landed')]:
            base=deepcopy(self.base)
            base['surface'][0].update(authority=authority,disposition=disposition,status=status)
            if authority=='host-only': base['owner'][0]={'id':'rust::source','kind':'outside-scope','decision_ref':'fixture-scope'}
            comparison=self.compare(changed,data=base)
            self.assertEqual(drift.evaluate(comparison)[0],2)
            self.assertEqual({i['authority'] for i in comparison.items.values()},{authority})
            self.assertEqual(drift.evaluate(comparison,acknowledgements(comparison))[0],0)

    def test_modified_move_stays_delete_add_with_bound_migration(self):
        (self.repo/'pkg/a.ts').unlink()
        changed=self.change('export const answer=2;\n','pkg/moved.ts')
        initial=self.compare(changed)
        self.assertEqual({i['kind'] for i in initial.items.values()},{'A','D'})
        comparison=self.compare(changed,ov=overlay(initial,intake=[{'path':'pkg/moved.ts','route':'all','surface':'source','selectors':[]}]))
        document=acknowledgements(comparison)
        for row in document['acknowledgements']:
            row.update(disposition='responsibility-migration',related_items=list(comparison.items),responsibilities={'old':{'source':'rust::source'},'new':{'source':'rust::source'}})
        self.assertEqual(drift.evaluate(comparison,document)[0],0)
        document['acknowledgements'][0]['responsibilities']['old']={}
        with self.assertRaisesRegex(Invalid,'endpoint'): drift.evaluate(comparison,document)

    def test_workspace_reexport_resolves_actual_origin_and_kind(self):
        self.fixture.write('pkg/one/package.json',json.dumps({'name':'local-one','exports':{'.':'./src/index.ts'}}))
        self.fixture.write('pkg/one/src/index.ts','export { Answer } from "local-two";\n')
        self.fixture.write('pkg/two/package.json',json.dumps({'name':'local-two','exports':{'.':'./src/index.ts'}}))
        self.fixture.write('pkg/two/src/index.ts','export class Answer {}\n')
        self.old=self.fixture.commit()
        base=mapping(self.old)
        base['manifest']=[{'id':'one','kind':'public-exports','source':'pkg/one/package.json+src/index.ts','path':'one.json'}]
        self.base=bind_map(self.fixture,base)
        comparison=self.compare()
        rows=[r for r in comparison.new.inventory.values() if r['family']=='exports']
        self.assertEqual(len(rows),1)
        self.assertEqual((rows[0]['payload']['origin'],rows[0]['payload']['kind']),('pkg/two/src/index.ts','type-and-value'))
        changed=self.change('export interface Answer { field: number }\n','pkg/two/src/index.ts')
        comparison=self.compare(changed)
        self.assertTrue(any('inventory:exports' in i['reasons'] for i in comparison.items.values()))
        self.assertEqual(next(r for r in comparison.new.inventory.values() if r['family']=='exports')['payload']['kind'],'type')

    def test_guard_mutation_witness_detects_missing_intake_guard(self):
        changed=self.change('export const newValue=2;\n','pkg/new.ts')
        comparison=self.compare(changed)
        self.assertEqual(drift.evaluate(comparison,acknowledgements(comparison))[0],2)
        original=drift.Comparison.intake
        def disabled(instance):
            original(instance)
            instance.pending_intake=[]
        # The specification assertion below fails with exactly the intake guard disabled.
        with patch.object(drift.Comparison,'intake',disabled):
            defective=self.compare(changed)
            with self.assertRaises(AssertionError):
                self.assertEqual(drift.evaluate(defective,acknowledgements(defective))[0],2)
        restored=self.compare(changed)
        self.assertEqual(drift.evaluate(restored,acknowledgements(restored))[0],2)
        self.assertEqual(restored.report,comparison.report)

    def test_cli_positive_patch_uses_actual_fixture_suite(self):
        comparison,root,registry,receipts=self.proof_case()
        map_path=root/'base.toml'
        map_path.write_text(drift.render_map(self.base))
        ack_path=root/'ack.json'
        ack_path.write_bytes(canonical(acknowledgements(comparison)))
        receipt_path=root/'receipts.json'
        receipt_path.write_bytes(canonical(receipts))
        output=root/'bundle.json'
        command=[sys.executable,str(drift.ROOT/'scripts/upstream_drift.py'),'--upstream',str(self.repo),'--map',str(map_path),'--new',comparison.new.commit,'--overlay',str(root/'overlay.json'),'--acknowledgements',str(ack_path),'--registry',str(registry),'--receipts',str(receipt_path),'--proof-root',str(root),'--emit-patch',str(output)]
        result=subprocess.run(command,capture_output=True,env={**os.environ,'PIRUA_UPSTREAM_ROOT':str(DEPS)})
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)
        self.assertEqual(json.loads(output.read_bytes())['advancement']['new_commit'],comparison.new.commit)
        output.unlink()
        duplicate=subprocess.run(command+['--output',str(output)],capture_output=True,env={**os.environ,'PIRUA_UPSTREAM_ROOT':str(DEPS)})
        self.assertEqual(duplicate.returncode,3)
        self.assertFalse(output.exists())
        before=registry.read_bytes()
        collision=subprocess.run(command+['--output',str(registry)],capture_output=True,env={**os.environ,'PIRUA_UPSTREAM_ROOT':str(DEPS)})
        self.assertEqual(collision.returncode,3)
        self.assertEqual(registry.read_bytes(),before)
        (root/'proof_fixture.py').write_text('raise AssertionError("tampered suite")\n')
        result=subprocess.run(command,capture_output=True,env={**os.environ,'PIRUA_UPSTREAM_ROOT':str(DEPS)})
        self.assertEqual(result.returncode,3)
        self.assertFalse(output.exists())

    def test_cargo_proof_guard_requires_shared_validator(self):
        comparison,root,registry,receipts=self.proof_case()
        document=json.loads(registry.read_bytes())
        document['suites'][0]['commands']=[['cargo','test','--offline','--locked','-p','fixture','--test','proof']]
        registry.write_bytes(canonical(document))
        with patch('scripts.upstream_drift.importlib.import_module',side_effect=ImportError('missing')):
            with self.assertRaisesRegex(Invalid,'activation validator unavailable'):
                drift.registry_suites(registry,root,{'conformance/fixture'})
        shared=drift.cargo_validator()
        with patch.object(shared,'cargo_test_context',None):
            with self.assertRaisesRegex(Invalid,'activation validator unavailable'):
                drift.registry_suites(registry,root,{'conformance/fixture'})

    def test_cargo_proof_guard_propagates_shared_activation_rejection(self):
        comparison,root,registry,receipts=self.proof_case()
        shared=drift.cargo_validator()
        document=json.loads(registry.read_bytes())
        commands=[['cargo','test','--offline','--locked','-p','fixture','--test','proof']]
        evidence=[{'path':'crates/fixture/tests/proof.rs','anchor':'test:inactive'}]
        tests=root/'crates/fixture/tests';tests.mkdir(parents=True)
        (tests.parent/'Cargo.toml').write_text('[package]\nname="fixture"\nversion="0.0.0"\n')
        (tests/'proof.rs').write_text('#[test]\nfn inactive() {}\n')
        document['suites'][0].update(commands=commands,evidence=evidence)
        registry.write_bytes(canonical(document))
        with patch.object(shared,'check_command',side_effect=ValueError('inactive Rust test anchor')) as check:
            with self.assertRaisesRegex(Invalid,'inactive Rust test anchor'):
                drift.registry_suites(registry,root,{'conformance/fixture'})
            check.assert_called_once_with(commands[0],evidence,root)

    def test_canonical_production_registry_accepts_shared_rust_anchors(self):
        registry=drift.ROOT/'conformance/assembly.json'
        document=json.loads(registry.read_bytes())
        expected={row['id']:row for row in document['suites'] if row['status']=='executable'}
        self.assertEqual(drift.registry_suites(registry,drift.ROOT,set(expected)),expected)
        family=expected['conformance/provider-traces/v1.4.0']
        anchors={row.get('anchor') for row in family['evidence']}
        for anchor in ['test:four_protocols_match_published_v140_wires_and_terminal_messages', 'test:target_release_oracle_reexecutes_without_network', 'test:all_complete_message_projections_match_executed_source_without_mutation']:
            self.assertIn(anchor,anchors)
        missing_target=deepcopy(family)
        missing_target['commands']=[command for command in missing_target['commands'] if command[command.index('--test')+1]!='google_shared']
        missing_anchor=deepcopy(family)
        missing_anchor['evidence'].remove(next(row for row in missing_anchor['evidence'] if row['path'].endswith('/google_shared.rs')))
        unrelated={**deepcopy(family),'commands':[['python3','scripts/generate_upstream_manifests.py','--check']],'evidence':[{'path':'scripts/generate_upstream_manifests.py','anchor':'python:main'}]}
        negative=self.root/'incomplete-family.json'
        for row,error in [(missing_target,'retained adapter proof missing target'),(missing_anchor,'retained adapter proof missing exact anchor'),(unrelated,'retained adapter proof missing target')]:
            negative.write_bytes(canonical({'schema_version':1,'suites':[row]}))
            with self.assertRaisesRegex(Invalid,error):
                drift.registry_suites(negative,drift.ROOT,{family['id']})

    def test_shared_rust_anchor_resolution_retains_activation_and_command_guards(self):
        root=self.root/'rust-proof'
        tests=root/'crates/fixture/tests'
        tests.mkdir(parents=True)
        (tests.parent/'Cargo.toml').write_text('[package]\nname="fixture"\nversion="0.0.0"\n[features]\ndefault=[]\nwire=[]\nnative=[]\nnative-bundle=["native"]\n')
        (tests/'proof.rs').write_text('''macro_rules! retained_test {
    ($name:ident,$id:literal) => { #[test] fn $name() { let _ = $id; } };
}
retained_test!(macro_case, "fixture");
#[cfg(feature = "native")]
retained_test!(native_macro_case, "native-fixture");
mod nested { #[test] fn qualified_case() {} }
mod sibling { #[test] fn qualified_case() {} }
#[cfg(feature = "native")]
mod parent { #[test] fn inherited_native() {} }
#[test] #[ignore] fn ignored_case() {}
macro_rules! unsupported_test { ($name:ident) => { #[test] fn $name() {} }; }
unsupported_test!(unsupported_macro_case);
''')
        (tests/'other.rs').write_text('#[test] fn unrelated() {}\n')
        command=['cargo','test','--offline','--locked','-p','fixture','--test','proof','--no-default-features','--features','wire']
        registry=root/'registry.json'
        def check(anchor, *, features='wire', target='proof', error=None):
            argv=list(command)
            argv[-1]=features
            argv[argv.index('--test')+1]=target
            row={'id':'conformance/fixture','status':'executable','commands':[argv],'evidence':[{'path':'crates/fixture/tests/proof.rs','anchor':'test:'+anchor}],'limitations':[]}
            registry.write_bytes(canonical({'schema_version':1,'suites':[row]}))
            if error:
                with self.assertRaisesRegex(Invalid,error):
                    drift.registry_suites(registry,root,{row['id']})
            else:
                self.assertEqual(drift.registry_suites(registry,root,{row['id']}),{row['id']:row})
        check('macro_case')
        check('nested::qualified_case')
        check('qualified_case',error='missing or ambiguous Cargo test anchor')
        check('native_macro_case',error='disabled by feature cfg or ignore')
        check('native_macro_case',features='native-bundle')
        check('parent::inherited_native',error='disabled by feature cfg or ignore')
        check('parent::inherited_native',features='native-bundle')
        check('ignored_case',error='missing evidence anchor')
        check('missing_macro_case',error='missing evidence anchor')
        check('unsupported_macro_case',error='missing evidence anchor')
        check('wrong_module::qualified_case',error='missing or ambiguous Cargo test anchor')
        check('macro_case',target='other',error='lacks an actual test anchor')

    def test_nested_library_context_reaches_both_drift_validation_entries(self):
        root=self.root/'library-proof'
        source=root/'crates/fixture/src'
        (source/'foo').mkdir(parents=True)
        (source.parent/'Cargo.toml').write_text('[package]\nname="fixture"\nversion="0.0.0"\n[features]\nnative=[]\n')
        (source/'lib.rs').write_text('#[cfg(test)] mod foo;\n')
        (source/'foo.rs').write_text('#[test] fn parent() {}\nmod child;\n')
        (source/'foo/child.rs').write_text('#[cfg(feature="native")] #[test] fn required() {}\n')
        row={'id':'conformance/fixture','status':'executable',
             'commands':[['cargo','test','--locked','--offline','-p','fixture','--lib','--all-features']],
             'evidence':[{'path':'crates/fixture/src/foo.rs','anchor':'test:foo::parent','cargo_target':'@lib'},
                         {'path':'crates/fixture/src/foo/child.rs','anchor':'test:foo::child::required','cargo_target':'@lib'}],
             'limitations':[]}
        registry=root/'registry.json'
        for entry in ('registry','activation'):
            def validate(candidate):
                if entry=='activation':
                    return drift.validate_suite_activation(candidate,root)
                registry.write_bytes(canonical({'schema_version':1,'suites':[candidate]}))
                return drift.registry_suites(registry,root,{candidate['id']})
            with self.subTest(entry=entry):
                validate(row)
                wrong=deepcopy(row);wrong['evidence'][1]['anchor']='test:wrong::required'
                with self.assertRaisesRegex(Invalid,'missing or ambiguous Cargo test anchor'):
                    validate(wrong)
                disabled=deepcopy(row);disabled['commands'][0].remove('--all-features')
                with self.assertRaisesRegex(Invalid,'disabled by feature cfg or ignore'):
                    validate(disabled)



    def test_intrinsic_authority_disposition_and_owner_coherence(self):
        for authority in ['host-only','disposable-scaffold']:
            invalid=deepcopy(self.base)
            invalid['surface'][0]['authority']=authority
            with self.assertRaisesRegex(Invalid,'requires'):
                self.compare(data=invalid)
        invalid=deepcopy(self.base)
        invalid['surface'][0].update(authority='host-only',disposition='outside-scope',status='excluded')
        with self.assertRaisesRegex(Invalid,'incompatible'):
            self.compare(data=invalid)
        valid=deepcopy(invalid)
        valid['owner'][0]={'id':'rust::source','kind':'outside-scope','decision_ref':'fixture-scope'}
        self.assertEqual(drift.evaluate(self.compare(data=valid))[0],0)
        invalid=deepcopy(valid)
        invalid['surface'][0].update(authority='disposable-scaffold',disposition='disposable',status='disposable')
        with self.assertRaisesRegex(Invalid,'incompatible'):
            self.compare(data=invalid)

    def test_faux_provider_and_api_identity_are_extracted_from_declarations(self):
        self.fixture.write('packages/ai/src/providers/faux.ts','const DEFAULT_PROVIDER="faux";\nconst DEFAULT_API="faux-api";\nexport function fauxProvider() {}\n')
        self.old=self.fixture.commit()
        base=mapping(self.old)
        base['watch'][0]['root']='packages/ai'
        base['route'][0]['include']=['packages/ai/**']
        self.base=bind_map(self.fixture,base)
        baseline=self.compare()
        rows=[r for r in baseline.new.inventory.values() if r['family']=='providers']
        self.assertEqual(len(rows),1)
        self.assertEqual((rows[0]['payload']['id'],rows[0]['payload']['api_ids']),('faux',['faux-api']))
        changed=self.change('const DEFAULT_PROVIDER="renamed-faux";\nconst DEFAULT_API="new-api";\nexport function fauxProvider() {}\n','packages/ai/src/providers/faux.ts')
        comparison=self.compare(changed)
        rows=[r for r in comparison.new.inventory.values() if r['family']=='providers']
        self.assertEqual((rows[0]['payload']['id'],rows[0]['payload']['api_ids']),('renamed-faux',['new-api']))
        self.assertTrue(any('inventory:providers' in i['reasons'] for i in comparison.items.values()))

    def test_artifact_collisions_preserve_tools_review_and_proof_inputs(self):
        separate=self.root/'artifacts'
        separate.mkdir()
        fake_root=self.root/'tools'
        fake_root.mkdir()
        Fixture(fake_root)
        (fake_root/'scripts').mkdir()
        checker=fake_root/'scripts/check_assembly.py'
        checker.write_text('shared activation checker bytes\n')
        inputs=[]
        for name in ['map.toml','overlay.json','ack.json','receipts.json','registry.json','suite.log','artifact.json']:
            path=separate/name
            path.write_text('preserve '+name+'\n')
            inputs.append(path)
        args=Namespace(map=inputs[0],overlay=inputs[1],acknowledgements=inputs[2],receipts=inputs[3],registry=inputs[4],proof_root=separate,output=None,emit_patch=None)
        receipt={'receipts':[{'logs':[{'path':'suite.log'}],'artifacts':[{'path':'artifact.json'}]}]}
        with patch.object(drift,'ROOT',fake_root):
            for target in [checker]+inputs:
                before=target.read_bytes()
                args.output=target
                with self.assertRaisesRegex(Invalid,'separate artifact'):
                    drift.guard_outputs(args,self.repo,receipt)
                self.assertEqual(target.read_bytes(),before)
            args.output=separate/'combined.json'
            args.emit_patch=args.output
            with self.assertRaisesRegex(Invalid,'separate artifacts'):
                drift.guard_outputs(args,self.repo,None)
            self.assertFalse(args.output.exists())

    def test_registered_owner_and_collaborator_changes_require_bound_migration(self):
        initial=self.compare()
        for collaborator in [False,True]:
            base=deepcopy(self.base)
            if collaborator:
                base['owner'].append({'id':'rust::collaborator','kind':'rust','crate':'pi-ai-core'})
                base['surface'][0]['collaborators']=['rust::collaborator']
            owner_id='rust::collaborator' if collaborator else 'rust::source'
            owners=deepcopy(base['owner'])
            next(row for row in owners if row['id']==owner_id)['crate']='pi-ai'
            ov=overlay(initial,replace={'owner':owners})
            ov['base_map_digest']=digest(base)
            blocked=self.compare(data=base,ov=ov)
            self.assertEqual(drift.evaluate(blocked)[0],3)
            self.assertTrue(blocked.items)
            self.assertTrue(any('ownership' in e for e in blocked.errors))
            binding={'path':'pkg/a.ts','old_ownership':{'source':'rust::source'},'new_ownership':{'source':'rust::source'},'old_registrations':{row['id']:row for row in base['owner']},'new_registrations':{row['id']:row for row in owners}}
            ov['ownership']=[binding]
            comparison=self.compare(data=base,ov=ov)
            document=acknowledgements(comparison)
            for row in document['acknowledgements']:
                row.update(disposition='responsibility-migration',related_items=list(comparison.items),responsibilities={'old':{'source':'rust::source'},'new':{'source':'rust::source'}})
            self.assertEqual(drift.evaluate(comparison,document)[0],0)
            item=next(iter(comparison.items.values()))
            self.assertEqual(item['new_owner_registrations'][owner_id]['crate'],'pi-ai')
            changed=deepcopy(ov)
            next(row for row in changed['replace']['owner'] if row['id']==owner_id)['crate']='pi-agent'
            changed['ownership'][0]['new_registrations'][owner_id]['crate']='pi-agent'
            later=self.compare(data=base,ov=changed)
            self.assertEqual(set(later.items),set(comparison.items))
            self.assertNotEqual(later.report['report_digest'],comparison.report['report_digest'])
            with self.assertRaisesRegex(Invalid,'report digest'):
                drift.evaluate(later,document)

    def test_manifest_shape_and_missing_baseline_source_are_invalid(self):
        for row in [{'id':'bad','kind':'unknown','path':'inventory.json','source':'pkg/package.json'}, {'id':'bad','kind':'public-exports','path':'inventory.json'}, {'id':'bad','kind':'public-exports','path':'../inventory.json','source':'pkg/package.json'}]:
            base=deepcopy(self.base)
            base['manifest']=[row]
            with self.assertRaisesRegex(Invalid,'manifest'):
                self.compare(data=base)
        base=deepcopy(self.base)
        base['manifest']=[{'id':'missing','kind':'public-exports','path':'inventory.json','source':'pkg/package.json'}]
        comparison=self.compare(data=base)
        self.assertEqual(drift.evaluate(comparison)[0],3)
        self.assertTrue(any('manifest source missing' in error for error in comparison.errors))

    def test_deleted_public_package_requires_manifest_cleanup(self):
        self.export_base()
        (self.repo/'pkg/package.json').unlink()
        changed=self.fixture.commit()
        initial=self.compare(changed)
        self.assertEqual(drift.evaluate(initial)[0],3)
        self.assertTrue(any('manifest source missing' in error for error in initial.errors))
        fixed=self.compare(changed,ov=overlay(initial,replace={'manifest':[]}))
        self.assertEqual(drift.evaluate(fixed,acknowledgements(fixed))[0],0)
        candidate=bind_map(self.fixture,fixed.effective)
        self.assertFalse(Snapshot(Objects(self.repo),changed,candidate,DEPS,baseline=True).errors)

    def test_provider_family_receipts_require_canonical_shared_family_validation(self):
        comparison,root,registry,receipts=self.proof_case()
        shared=drift.cargo_validator()
        document=json.loads(registry.read_bytes())
        row=document['suites'][0]
        row.update(id='conformance/provider-traces/v1.4.0',commands=[['python3','proof_fixture.py']])
        registry.write_bytes(canonical(document))
        with patch.object(shared,'check_command',return_value=None):
            with self.assertRaisesRegex(Invalid,'providers.toml'):
                drift.registry_suites(registry,root,{'conformance/provider-traces/v1.4.0'})
            path=root/'upstream/manifests/providers.toml'
            path.parent.mkdir(parents=True)
            inventory={'source':'fixture'}
            path.write_text('source="fixture"\n')
            with patch.object(shared,'validate_adapter_proofs',side_effect=ValueError('missing typed adapter family')) as check:
                with self.assertRaisesRegex(Invalid,'missing typed adapter family'):
                    drift.registry_suites(registry,root,{'conformance/provider-traces/v1.4.0'})
                check.assert_called_once_with(row,inventory,root)

    def test_existing_public_source_cannot_be_hidden_by_manifest_overlay(self):
        self.export_base()
        initial=self.compare()
        with self.assertRaisesRegex(Invalid,'cannot be hidden'):
            self.compare(ov=overlay(initial,replace={'manifest':[]}))
        redirected=deepcopy(self.base['manifest'])
        redirected[0]['source']='pkg/other.json'
        with self.assertRaisesRegex(Invalid,'cannot be hidden'):
            self.compare(ov=overlay(initial,replace={'manifest':redirected}))

    def test_source_bound_export_exclusions_preserve_owner_migration(self):
        self.fixture.write('pkg/package.json', '{"name":"fixture","exports":{".":"./src/index.ts"}}')
        self.fixture.write('pkg/src/index.ts', 'export * from "./types.ts";')
        self.fixture.write('pkg/src/types.ts', 'export interface ImageModel {}\nexport interface Message {}\n')
        self.old = self.fixture.commit()
        base = mapping(self.old)
        excluded = {**deepcopy(base['surface'][0]), 'id':'excluded', 'authority':'host-only',
                    'primary_owner':'outside', 'disposition':'outside-scope', 'status':'excluded', 'conformance':[]}
        base['surface'].append(excluded)
        base['owner'].append({'id':'outside','kind':'outside-scope','decision_ref':'owner decision'})
        base['manifest'] = [{'id':'public','kind':'public-exports','source':'pkg/package.json','path':'exports.toml'}]
        base['export_exclusion'] = [{'id':'excluded-image','source':'pkg/src/types.ts','names':['ImageModel'],'surface':'excluded'}]
        self.base = bind_map(self.fixture, base)
        initial = self.compare()
        self.assertFalse(initial.errors)
        exports = {row['payload']['name']:row['surface'] for row in initial.new.inventory.values() if row['family']=='exports'}
        self.assertEqual(exports, {'ImageModel':'excluded','Message':'source'})
        removed = self.compare(ov=overlay(initial, replace={'export_exclusion':[]}))
        self.assertTrue(any('ownership/metadata change requires explicit ownership overlay' in error for error in removed.errors))
        stale = deepcopy(self.base)
        stale['export_exclusion'][0]['names'] = ['AbsentSymbol']
        snapshot = Snapshot(Objects(self.repo), self.old, stale, DEPS, baseline=True)
        self.assertTrue(any('export exclusion is stale or unresolved' in error for error in snapshot.errors))
        duplicate = deepcopy(self.base)
        duplicate['export_exclusion'].append({**duplicate['export_exclusion'][0], 'id':'duplicate'})
        with self.assertRaisesRegex(Invalid,'overlapping source-bound export exclusions'):
            Snapshot(Objects(self.repo),self.old,duplicate,DEPS,baseline=True)

    def test_excluded_public_entrance_does_not_claim_shared_reexports(self):
        self.fixture.write('pkg/package.json', '{"name":"fixture","exports":{".":"./src/index.ts","./compat":"./src/compat.ts"}}')
        self.fixture.write('pkg/src/index.ts', 'export interface SharedMessage {}')
        self.fixture.write('pkg/src/compat.ts', 'export * from "./index.ts";')
        self.old=self.fixture.commit()
        base=mapping(self.old)
        base['owner'].append({'id':'outside','kind':'outside-scope','decision_ref':'owner decision'})
        base['surface'].append({**deepcopy(base['surface'][0]),'id':'excluded','authority':'host-only',
                                'primary_owner':'outside','disposition':'outside-scope','status':'excluded','conformance':[]})
        base['route'][0]['exclude']=['pkg/src/compat.ts']
        base['route'].append({'id':'compat','watch':'pkg','surface':'excluded','role':'excluded-public-entrance',
                              'include':['pkg/src/compat.ts'],'exclude':[]})
        base['manifest']=[{'id':'public','kind':'public-exports','source':'pkg/package.json','path':'exports.toml'}]
        self.base=bind_map(self.fixture,base)
        comparison=self.compare()
        self.assertFalse(comparison.errors)
        exports={row['payload']['subpath']:row['surface'] for row in comparison.new.inventory.values() if row['family']=='exports'}
        self.assertEqual(exports,{'.':'source','./compat':'excluded'})

    def test_release_artifact_association_rejects_wrong_identity_or_missing_bytes(self):
        data = tomllib.loads((drift.ROOT/'upstream-map.toml').read_text())
        surface = next(row for row in data['surface'] if row['id']=='ai.catalog.projection')
        snapshot = object.__new__(Snapshot)
        snapshot.commit = data['upstream']['commit']
        snapshot.tree = {}
        snapshot.validate_release_provenance(surface)
        for field,value in [('commit','0'*40), ('sha256','sha256:'+'0'*64),
                            ('source_ids',['unselected']), ('archive','missing-release-archive.tgz')]:
            changed = deepcopy(surface)
            changed['provenance_snapshot'][field] = value
            with self.subTest(field=field),self.assertRaises(Invalid):
                snapshot.validate_release_provenance(changed)

    def test_cli_exit_codes_and_patch_blocking(self):
        path=self.root/'map.toml'
        path.write_text(drift.render_map(self.base))
        command=[sys.executable,str(drift.ROOT/'scripts/upstream_drift.py'),'--map',str(path),'--upstream',str(self.repo)]
        env={**os.environ,'PIRUA_UPSTREAM_ROOT':str(DEPS)}
        for extra,expected in [([],0),(['--new',self.change('export const answer=3;\n')],2),(['--new','HEAD'],3),(['--new','f'*40],4)]:
            result=subprocess.run(command+extra,capture_output=True,env=env)
            self.assertEqual(result.returncode,expected,result.stdout+result.stderr)
        output=self.root/'forbidden.json'
        result=subprocess.run(command+['--new',self.fixture.run('rev-parse','HEAD'),'--emit-patch',str(output)],capture_output=True,env=env)
        self.assertEqual(result.returncode,3)
        self.assertFalse(output.exists())


if __name__=='__main__':
    unittest.main()
