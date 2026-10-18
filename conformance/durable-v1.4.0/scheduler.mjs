import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, mkdirSync, rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';
import { directory, target, verifyEnvironment, sha256 } from './setup.mjs';
import { assertScalarStrings } from './trace-codec.mjs';

const args = process.argv.slice(2);
assert.equal(args.filter(a => ['--check', '--write', '--causal'].includes(a)).length, 1, 'Choose --check, --write or --causal');
const pin = verifyEnvironment({ pinPath: resolve(directory, 'scheduler-source-pin.json') });
for (const [path, expected] of Object.entries(pin.frozenSessionFiles)) assert.equal(sha256(readFileSync(resolve(directory, path))), expected, `Frozen Session file changed: ${path}`);
const { executeSchedulerCase } = await import('./scheduler-runner.mjs');
const fixtureIndex = args.indexOf('--fixture');
const fixturePath = resolve(directory, fixtureIndex < 0 ? 'fixtures/scheduler.json' : args[fixtureIndex + 1]);
const inputPath = resolve(directory, 'fixtures/scheduler-inputs.json');
const fixture = JSON.parse(readFileSync(args.includes('--write') ? inputPath : fixturePath, 'utf8'));
assert.equal(fixture.schemaVersion, 1);
assert.equal(fixture.upstreamCommit, target);
assertScalarStrings(fixture);
if (args.includes('--causal')) {
  const test = fixture.cases.find(test => test.id === 'scheduler-no-inspection-dispatch');
  const changed = structuredClone(test);
  changed.expected.memory.observations.find(item => item.label === 'before-resume').value.tasks[0].record.state.status = 'running';
  const negativePath = resolve(directory, '.runtime', `scheduler-causal-${process.pid}.json`);
  mkdirSync(resolve(directory, '.runtime'), { recursive: true });
  writeFileSync(negativePath, JSON.stringify({ ...fixture, cases: [changed] }));
  try {
    const checked = spawnSync(process.execPath, [resolve(directory, 'scheduler.mjs'), '--check', '--fixture', negativePath], { cwd: directory, encoding: 'utf8', timeout: 30000, maxBuffer: 4 * 1024 * 1024 });
    assert.equal(checked.status, 1, 'Actual scheduler --check must reject a changed inspection expectation');
    assert.match(checked.stderr, /Pinned scheduler observations differ: scheduler-no-inspection-dispatch/);
    console.log('PASS scheduler causal control: actual --check rejected running-before-resume expectation');
  } finally { rmSync(negativePath, { force: true }); }
} else {
  const output = { schemaVersion: 1, upstreamCommit: target, cases: [] };
  let executions = 0;
  for (const test of fixture.cases) {
    for (const source of test.basis.sources) assert.ok(pin.sources[source], `Unpinned scheduler source ${source}`);
    const expected = {};
    for (const backend of test.backends ?? ['memory', 'sqlite']) {
      expected[backend] = await executeSchedulerCase(test, backend);
      executions++;
      if (args.includes('--check')) assert.deepEqual(expected[backend], test.expected[backend], `Pinned scheduler observations differ: ${test.id} (${backend})`);
    }
    output.cases.push({ ...test, expected });
  }
  assertScalarStrings(output);
  if (args.includes('--write')) writeFileSync(fixturePath, `${JSON.stringify(output, null, 2)}\n`);
  console.log(`PASS scheduler ${args.includes('--write') ? 'generation' : 'check'}: ${output.cases.length} cases, ${executions} actual release/backend executions; all 18 Session files unchanged`);
}
