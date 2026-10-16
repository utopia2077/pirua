import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync, writeFileSync, rmSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve, dirname } from 'node:path';

const directory = dirname(fileURLToPath(import.meta.url));
const bad = JSON.parse(readFileSync(resolve(directory, 'observations.json'), 'utf8'));
const scenario = bad.observations.find((item) => item.id === 'tool-order-parallel');
const indexes = scenario.events.flatMap((event, index) =>
  event.type === 'message_end' && event.message.role === 'toolResult' ? [index] : []);
assert.equal(indexes.length, 2);
[scenario.events[indexes[0]], scenario.events[indexes[1]]] = [scenario.events[indexes[1]], scenario.events[indexes[0]]];
const path = resolve(directory, '.runtime', `causal-${process.pid}.json`);
writeFileSync(path, `${JSON.stringify(bad)}\n`);
try {
  const result = spawnSync(process.execPath, [resolve(directory, 'oracle.mjs'), '--check', '--fixture', path], {
    cwd: directory, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024, timeout: 30000,
  });
  assert.equal(result.status, 1, 'Source comparison must reject swapped transcript result order');
  assert.match(result.stderr, /Target-source Agent observations differ/);
  console.log('PASS causal check: swapped parallel transcript results rejected');
} finally { rmSync(path); }
