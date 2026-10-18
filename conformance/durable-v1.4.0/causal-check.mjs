import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync, rmSync } from 'node:fs';
import { resolve } from 'node:path';
import { directory } from './setup.mjs';
import { assertScalarStrings, encodeTraceValue } from './trace-codec.mjs';

mkdirSync(resolve(directory, '.runtime'), { recursive: true });
const badFixture = resolve(directory, '.runtime', `causal-fixture-${process.pid}.json`);
const badPin = resolve(directory, '.runtime', `causal-pin-${process.pid}.json`);
const check = extra => spawnSync(process.execPath, [resolve(directory, 'session.mjs'), '--check', ...extra], { cwd: directory, encoding: 'utf8', timeout: 30000, maxBuffer: 4 * 1024 * 1024 });
try {
  const fixture = JSON.parse(readFileSync(resolve(directory, 'fixtures/session.json'), 'utf8'));
  assertScalarStrings(fixture);
  assert.equal(encodeTraceValue('😀x'), '😀x');
  assert.deepEqual(encodeTraceValue(String.fromCharCode(0xde00, 0x78)), { $utf16: [0xde00, 0x78] });
  const test = fixture.cases.find(c => c.id === 'fork-entry-commit-final-state');
  // The entry was appended before the document's second edit in one transaction.
  // An implementation that snapshots at the entry's append point incorrectly gives 1.
  test.expected.memory.observations.at(-1).value.count = 1;
  writeFileSync(badFixture, JSON.stringify(fixture));
  const changed = check(['--fixture', badFixture]);
  assert.equal(changed.status, 1, 'Changed final state of entry commit must fail');
  assert.match(changed.stderr, /Pinned Durable observations differ: fork-entry-commit-final-state/);
  const utf16Fixture = JSON.parse(readFileSync(resolve(directory, 'fixtures/session.json'), 'utf8'));
  const utf16Case = utf16Fixture.cases.find(c => c.id === 'delta-utf16-surrogate-split-upstream-domain');
  assert.deepEqual(utf16Case.expected.chord.observations[0].value.text, { $utf16: [0xde00, 0x78] });
  utf16Case.expected.chord.observations[0].value.text.$utf16[0] = 0xde01;
  writeFileSync(badFixture, JSON.stringify(utf16Fixture));
  const changedUtf16 = check(['--fixture', badFixture]);
  assert.equal(changedUtf16.status, 1, 'Changed observed UTF-16 code unit must fail');
  assert.match(changedUtf16.stderr, /Pinned Durable observations differ: delta-utf16-surrogate-split-upstream-domain/);
  const pin = JSON.parse(readFileSync(resolve(directory, 'source-pin.json'), 'utf8'));
  pin.releaseFiles['@earendil-works/pi-durable/dist/session/session.js'] = '0'.repeat(64);
  writeFileSync(badPin, JSON.stringify(pin));
  const stale = check(['--pin', badPin]);
  assert.equal(stale.status, 1, 'Changed source fingerprint must fail before execution');
  assert.match(stale.stderr, /Installed release source differs/);
  console.log('PASS causal controls: scalar JSON verified; changed fork/UTF-16 expectations and release source fingerprint rejected');
} finally {
  rmSync(badFixture, { force: true });
  rmSync(badPin, { force: true });
}
