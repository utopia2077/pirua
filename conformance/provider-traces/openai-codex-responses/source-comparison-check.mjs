import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {assertCodexSourceEqual, executionRoot, fixtureExecutionRoot} from './source-comparison.mjs';

const fixture = JSON.parse(readFileSync(new URL('./trace.json', import.meta.url), 'utf8'));
const expected = structuredClone(fixture.cases.find(value => value.result.diagnostics?.length));
assert(expected);
expected.result.diagnostics[0].error.stack += '\n    at external (file:///unrelated/remote.mjs:7:8)';
expected.trace.requests[0].body.diagnostics = [{error: {stack: fixtureExecutionRoot + 'payload-data.mjs:7:8'}}];
expected.result.content.push({type: 'toolCall', id: 'scope-control', name: 'tool', arguments: {diagnostics: [{error: {stack: fixtureExecutionRoot + 'tool-data.mjs:7:8'}}]}});
expected.ordinaryStack = {stack: fixtureExecutionRoot + 'not-a-diagnostic.mjs:1:2'};
const actual = structuredClone(expected);
let replacements = 0;
function relocateSnapshot(value) {
  if (value?.role !== 'assistant' || !Array.isArray(value.diagnostics)) return;
  for (const diagnostic of value.diagnostics) {
    if (typeof diagnostic?.error?.stack !== 'string') continue;
    assert(diagnostic.error.stack.includes(fixtureExecutionRoot));
    diagnostic.error.stack = diagnostic.error.stack.split(fixtureExecutionRoot).join(executionRoot);
    replacements++;
  }
}
relocateSnapshot(actual.result);
for (const event of actual.events) {
  for (const key of ['partial', 'message', 'error']) relocateSnapshot(event[key]);
}
assert(replacements > 0);
const unchanged = structuredClone(actual);
if (executionRoot !== fixtureExecutionRoot) {
  assert.throws(() => assert.deepEqual(actual, expected), assert.AssertionError,
    'raw comparison must observe the relocated diagnostic root');
}
assertCodexSourceEqual(actual, expected);
assertCodexSourceEqual({cases: [{first: actual, second: actual}]}, {cases: [{first: expected, second: expected}]});
assert.deepEqual(actual, unchanged, 'comparison must not rewrite raw observations');
const stack = value => value.result.diagnostics[0].error;
const negatives = {
  markerAlias(value) { stack(value).stack = stack(value).stack.split(executionRoot).join('file:///<codex-execution-root>/'); },
  diagnosticShapedRequest(value) { value.trace.requests[0].body.diagnostics[0].error.stack = executionRoot + 'payload-data.mjs:7:8'; },
  diagnosticShapedToolContent(value) { value.result.content.at(-1).arguments.diagnostics[0].error.stack = executionRoot + 'tool-data.mjs:7:8'; },
  function(value) { assert(stack(value).stack.includes('connectWebSocket')); stack(value).stack = stack(value).stack.replace('connectWebSocket', 'changedFunction'); },
  line(value) { stack(value).stack = stack(value).stack.replace(/:(\d+):(\d+)/, (_, line, column) => `:${Number(line) + 1}:${column}`); },
  column(value) { stack(value).stack = stack(value).stack.replace(/:(\d+):(\d+)/, (_, line, column) => `:${line}:${Number(column) + 1}`); },
  unrelatedUrl(value) { stack(value).stack = stack(value).stack.replace('file:///unrelated/remote.mjs', 'file:///elsewhere/remote.mjs'); },
  wire(value) { value.trace.requests[0].body.store = !value.trace.requests[0].body.store; },
  event(value) { value.events[0].partial.endTurn = true; },
  classifier(value) { value.flags.retryable = !value.flags.retryable; },
  diagnosticMessage(value) { stack(value).message += ' changed'; },
  ordinaryStack(value) { value.ordinaryStack.stack = value.ordinaryStack.stack.replace(fixtureExecutionRoot, executionRoot === fixtureExecutionRoot ? executionRoot + 'changed/' : executionRoot); },
};
for (const [name, mutate] of Object.entries(negatives)) {
  const changed = structuredClone(actual);
  mutate(changed);
  assert.throws(() => assertCodexSourceEqual(changed, expected), assert.AssertionError, name);
}
console.log(`PASS known-root relocation only; ${Object.keys(negatives).length} discriminating changes rejected; raw input unchanged`);
