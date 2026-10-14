import assert from 'node:assert/strict';

// Only these two execution roots are equivalent; no third raw marker is admitted.
export const fixtureExecutionRoot = 'file:///pirua-conformance/';
export const executionRoot = new URL('../../../', import.meta.url).href;

function comparable(input) {
  const copy = structuredClone(input);
  function snapshot(message) {
    if (message?.role !== 'assistant' || !Array.isArray(message.diagnostics)) return;
    for (const diagnostic of message.diagnostics) {
      if (typeof diagnostic?.error?.stack === 'string') {
        diagnostic.error.stack = diagnostic.error.stack.split(executionRoot).join(fixtureExecutionRoot);
      }
    }
  }
  function run(value) {
    if (value === null || typeof value !== 'object') return;
    snapshot(value.result);
    if (Array.isArray(value.events)) {
      for (const event of value.events) {
        snapshot(event?.partial);
        snapshot(event?.message);
        snapshot(event?.error);
      }
    }
  }
  // Traverse the recorded corpus envelope, never message content or wire data.
  const cases = Array.isArray(copy?.cases) ? copy.cases : [copy];
  for (const value of cases) {
    run(value);
    run(value?.first);
    run(value?.second);
  }
  return copy;
}

export function assertCodexSourceEqual(actual, expected) {
  assert.deepEqual(comparable(actual), comparable(expected));
}
