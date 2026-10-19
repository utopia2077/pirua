import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, mkdirSync, rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';
import { directory, target, verifyEnvironment, sha256 } from './setup.mjs';
import { assertScalarStrings } from './trace-codec.mjs';
const args = process.argv.slice(2);
assert.equal(args.filter(a => ['--check','--write','--causal'].includes(a)).length, 1, 'Choose --check, --write or --causal');
const pin = verifyEnvironment({ pinPath: resolve(directory, 'runtime-source-pin.json') });
for (const [path, digest] of Object.entries(pin.frozenPriorFiles)) assert.equal(sha256(readFileSync(resolve(directory, path))), digest, `Frozen prior file changed: ${path}`);
const { executeRuntimeCase } = await import('./runtime-runner.mjs');
const fixtureOption = args.indexOf('--fixture');
const fixturePath = resolve(directory, fixtureOption < 0 ? 'fixtures/runtime.json' : args[fixtureOption + 1]);
const fixture = JSON.parse(readFileSync(args.includes('--write') ? resolve(directory, 'fixtures/runtime-inputs.json') : fixturePath, 'utf8'));
assert.equal(fixture.schemaVersion, 1); assert.equal(fixture.upstreamCommit, target); assertScalarStrings(fixture);
if (args.includes('--causal')) {
  const changed = structuredClone(fixture.cases.find(test => test.id === 'runtime-admission-dedup-inbox'));
  changed.expected.memory.observations.find(item => item.label === 'duplicate').value += 1;
  const path = resolve(directory, '.runtime', `runtime-causal-${process.pid}.json`); mkdirSync(resolve(directory, '.runtime'), { recursive: true }); writeFileSync(path, JSON.stringify({ ...fixture, cases: [changed] }));
  try { const checked = spawnSync(process.execPath, [resolve(directory, 'runtime.mjs'),'--check','--fixture',path], { cwd: directory, encoding:'utf8',timeout:30000,maxBuffer:4*1024*1024 }); assert.equal(checked.status,1); assert.match(checked.stderr,/Pinned runtime observations differ: runtime-admission-dedup-inbox/); console.log('PASS runtime causal control: actual --check rejects changed deduplicated submission identity'); }
  finally { rmSync(path,{force:true}); }
} else {
  const output = { schemaVersion:1,upstreamCommit:target,cases:[] }; let executions=0;
  for (const test of fixture.cases) {
    for (const path of test.basis.sources) assert.ok(pin.sources[path],`Unpinned runtime source ${path}`);
    const expected={}; for (const backend of test.backends??['memory','sqlite']) { expected[backend]=await executeRuntimeCase(test,backend); executions++; if(args.includes('--check'))assert.deepEqual(expected[backend],test.expected[backend],`Pinned runtime observations differ: ${test.id} (${backend})`); }
    output.cases.push({...test,expected});
  }
  assertScalarStrings(output); if(args.includes('--write'))writeFileSync(fixturePath,`${JSON.stringify(output,null,2)}\n`);
  console.log(`PASS runtime ${args.includes('--write')?'generation':'check'}: ${output.cases.length} cases, ${executions} actual release/backend executions; all27 prior files unchanged`);
}
