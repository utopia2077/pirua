import './setup.mjs';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { directory } from './setup.mjs';

const generated = execFileSync(process.execPath, [resolve(directory, 'runner.mjs')], {
  cwd: directory, maxBuffer: 32 * 1024 * 1024, timeout: 30000,
});
const fixtureArgument = process.argv.indexOf('--fixture');
const path = fixtureArgument === -1 ? resolve(directory, 'observations.json') : resolve(process.argv[fixtureArgument + 1]);
if (process.argv.includes('--write')) {
  writeFileSync(path, generated);
  console.log(`Wrote ${JSON.parse(generated).observations.length} target-source Agent cases`);
} else if (process.argv.includes('--check')) {
  assert.deepEqual(JSON.parse(generated), JSON.parse(readFileSync(path)), 'Target-source Agent observations differ');
  console.log(`PASS ${JSON.parse(generated).observations.length} target-source Agent cases`);
} else throw new Error('Use --check to verify fixtures or --write to regenerate them');
