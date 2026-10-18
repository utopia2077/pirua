import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { directory, target, verifyEnvironment } from './setup.mjs';
import { assertScalarStrings } from './trace-codec.mjs';

const args = process.argv.slice(2);
assert.equal(args.filter(a => ['--check', '--write'].includes(a)).length, 1, 'Choose exactly one of --check or --write');
const fixtureArg = args.indexOf('--fixture');
const fixturePath = resolve(directory, fixtureArg < 0 ? 'fixtures/session.json' : args[fixtureArg + 1]);
const pinArg = args.indexOf('--pin');
const pin = verifyEnvironment(pinArg < 0 ? {} : { pinPath: resolve(directory, args[pinArg + 1]) });
const { executeCase } = await import('./runner.mjs');
const source = JSON.parse(readFileSync(args.includes('--write') ? resolve(directory, 'fixtures/session-inputs.json') : fixturePath, 'utf8'));
assertScalarStrings(source);
assert.equal(source.schemaVersion, 1);
assert.equal(source.upstreamCommit, target);
const output = { schemaVersion: 1, upstreamCommit: target, cases: [] };
let comparisons = 0;
for (const test of source.cases) {
  for (const path of test.basis.sources) assert.ok(pin.sources[path], `Case source is unpinned: ${path}`);
  const expected = {};
  for (const backend of test.family === 'delta' ? ['chord'] : test.backends ?? ['memory', 'sqlite']) {
    expected[backend] = await executeCase(test, backend);
    comparisons++;
    if (args.includes('--check')) assert.deepEqual(expected[backend], test.expected[backend], `Pinned Durable observations differ: ${test.id} (${backend})`);
  }
  output.cases.push({ ...test, expected });
}
assertScalarStrings(output);
if (args.includes('--write')) writeFileSync(fixturePath, `${JSON.stringify(output, null, 2)}\n`);
console.log(`PASS Durable Session ${args.includes('--write') ? 'generation' : 'check'}: ${source.cases.length} cases, ${comparisons} independent release/backend executions`);
