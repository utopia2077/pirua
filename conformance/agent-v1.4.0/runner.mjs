import assert from 'node:assert/strict';
import { createAssistantMessageEventStream, toToolDeclaration } from '@earendil-works/pi-ai';
import { Agent } from './.runtime/agent.ts';
import { runAgentLoop, runToolCall } from './.runtime/agent-loop.ts';

// Wall-clock values are fixed; only duration magnitudes are normalized below.
Date.now = () => 1700000000000;
const usage = { input: 1, output: 2, cacheRead: 0, cacheWrite: 0, totalTokens: 3,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
const model = (id = 'oracle-model') => ({ id, name: id, api: 'oracle', provider: 'oracle', baseUrl: '',
  reasoning: true, input: ['text', 'image'], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
  contextWindow: 10000, maxTokens: 1000 });
const user = (content) => ({ role: 'user', content, timestamp: 11 });
const system = (content, extra = {}) => ({ role: 'system', content, timestamp: 12, ...extra });
const call = (id, name = 'echo', args = {}) => ({ type: 'toolCall', id, name, arguments: args });
const assistant = (content = [{ type: 'text', text: 'done' }], stopReason = 'stop', extra = {}) => ({
  role: 'assistant', content, api: 'oracle', provider: 'oracle', model: 'oracle-model', usage,
  stopReason, timestamp: 13, ...extra,
});
const textResult = (text, extra = {}) => ({ content: [{ type: 'text', text }], details: { text }, ...extra });
const schema = { type: 'object', properties: { value: { type: 'string' } }, required: ['value'], additionalProperties: false };
const emptySchema = { type: 'object', properties: {}, additionalProperties: false };
const deferred = () => {
  let resolve;
  const promise = new Promise((done) => { resolve = done; });
  return { promise, resolve };
};
function snapshot(value) {
  return JSON.parse(JSON.stringify(value, (key, item) => {
    if (key === 'durationMs') {
      assert(Number.isInteger(item) && item >= 0, 'duration must be nonnegative integer milliseconds');
      return '<durationMs>';
    }
    if (typeof item === 'function') return '<callback>';
    if (item instanceof AbortSignal) return { aborted: item.aborted };
    if (item instanceof Set) return [...item];
    return item;
  }));
}
function recorder(id, input) {
  const observation = { id, input, requests: [], events: [], timeline: [] };
  const mark = (kind, data = {}) => observation.timeline.push(snapshot({ kind, ...data }));
  const emit = async (event) => {
    const index = observation.events.length;
    observation.events.push(snapshot(event));
    mark('event', { index });
  };
  return { observation, mark, emit };
}
function provider(rec, responses, { stream = false, beforeResponse } = {}) {
  return (requestedModel, context, options) => {
    const index = rec.observation.requests.length;
    assert(index < responses.length, `Unexpected extra provider request in ${rec.observation.id}`);
    rec.observation.requests.push(snapshot({ model: requestedModel, context, options }));
    rec.mark('request', { index });
    const response = createAssistantMessageEventStream();
    queueMicrotask(async () => {
      await beforeResponse?.(index);
      const message = structuredClone(responses[index]);
      if (stream) {
        const partial = { ...message, content: [] };
        response.push({ type: 'start', partial: structuredClone(partial) });
        response.push({ type: 'text_delta', contentIndex: 0, delta: 'done', partial: message });
      }
      if (message.stopReason === 'error' || message.stopReason === 'aborted') {
        response.push({ type: 'error', reason: message.stopReason, error: message });
      } else response.push({ type: 'done', reason: message.stopReason, message });
    });
    return response;
  };
}
function tool(name, execute, extra = {}) {
  return { name, label: name, description: `Tool ${name}`, parameters: emptySchema, execute, ...extra };
}
async function loop(rec, { tools = [], messages = [], prompts = [user('run')], config = {}, responses = [assistant()], stream = false,
  emit, beforeResponse } = {}) {
  const result = await runAgentLoop(prompts, { messages, tools }, { model: model(), convertToLlm: (items) => items, ...config },
    emit ?? rec.emit, undefined, provider(rec, responses, { stream, beforeResponse }));
  rec.observation.messages = snapshot(result);
  return rec.observation;
}
const observations = [];

// One explicit continuation is satisfied by a natural next request when available.
for (const natural of ['none', 'tool', 'steering', 'follow-up']) {
  const rec = recorder(`finish-continue-${natural}`, { kind: 'loop', finishFirst: 'continue', natural });
  let turns = 0;
  let steeringPolls = 0;
  let followUpPolls = 0;
  observations.push(await loop(rec, {
    tools: natural === 'tool' ? [tool('echo', async () => textResult('echo'))] : [],
    responses: [natural === 'tool' ? assistant([call('a')], 'toolUse') : assistant(), assistant()],
    config: {
      finishTurn: (turn) => {
        rec.mark('finishTurn', { turn, decision: ++turns === 1 ? 'continue' : null });
        return turns === 1 ? { action: 'continue' } : undefined;
      },
      getSteeringMessages: async () => {
        const delivered = natural === 'steering' && ++steeringPolls === 2 ? [user('steering')] : [];
        rec.mark('steeringPoll', { delivered });
        return delivered;
      },
      getFollowUpMessages: async () => {
        const delivered = natural === 'follow-up' && ++followUpPolls === 1 ? [user('follow-up')] : [];
        rec.mark('followUpPoll', { delivered });
        return delivered;
      },
      prepareNextTurn: (turn) => { rec.mark('prepareNextTurn', { turn }); },
    },
  }));
  assert.equal(rec.observation.requests.length, 2);
}
for (const reason of ['normal-end', 'error', 'aborted']) {
  const rec = recorder(`hard-exit-${reason}`, { kind: 'loop', response: reason, finish: reason === 'normal-end' ? 'end' : 'continue' });
  observations.push(await loop(rec, {
    tools: [tool('echo', async () => textResult('executed'))],
    responses: [assistant([call('a')], reason === 'normal-end' ? 'toolUse' : reason,
      reason === 'normal-end' ? {} : { errorMessage: `provider ${reason}` })],
    config: {
      finishTurn: (turn) => { rec.mark('finishTurn', { turn }); return { action: reason === 'normal-end' ? 'end' : 'continue' }; },
      getSteeringMessages: async () => { rec.mark('steeringPoll'); return []; },
      getFollowUpMessages: async () => { throw new Error('hard exit must not poll follow-up'); },
      prepareNextTurn: () => { throw new Error('hard exit must not prepare next turn'); },
    },
  }));
}

// First request replacement; no steering repoll after prepareRequest. The second
// turn repolls after prepareNextTurn only if the prior poll was empty.
for (const pendingBeforePrepare of [false, true]) {
  const rec = recorder(`preparation-${pendingBeforePrepare ? 'pending' : 'repoll'}`, {
    kind: 'loop', pendingBeforePrepare, initialSteering: 'startup', prepareRequestEnqueues: pendingBeforePrepare ? 'request-late' : null,
    prepareNextTurnEnqueues: 'preparation-late', replacementModel: 'replacement', thinkingLevels: ['high', 'off'],
  });
  let requestPreparations = 0;
  let nextPreparations = 0;
  const queued = [user('startup')];
  observations.push(await loop(rec, {
    responses: [assistant([call('a')], 'toolUse'), assistant()],
    tools: [tool('echo', async () => textResult('executed'))],
    config: {
      prepareRequest: (request) => {
        rec.mark('prepareRequest', { request });
        requestPreparations++;
        if (requestPreparations === 1) {
          if (pendingBeforePrepare) queued.push(user('request-late'));
          return { context: { ...request.context, messages: [system('canonical'), user('canonical user')] },
            model: model('replacement'), thinkingLevel: 'high' };
        }
        return { thinkingLevel: 'off' };
      },
      getSteeringMessages: async () => {
        const delivered = queued.splice(0, 1);
        rec.mark('steeringPoll', { delivered });
        return delivered;
      },
      prepareNextTurn: (turn) => {
        rec.mark('prepareNextTurn', { turn });
        nextPreparations++;
        queued.push(user('preparation-late'));
        return { messages: [system('prepared')] };
      },
      finishTurn: () => requestPreparations === 2 ? { action: 'end' } : undefined,
    },
  }));
  assert.equal(nextPreparations, 1);
}

// Transcript system anchors: pending declarations are replaced to match the
// executable set, while existing prompt/section fields remain intact.
{
  const old = tool('old', async () => textResult('old'));
  const current = tool('current', async () => textResult('current'), { parameters: schema });
  const rec = recorder('transcript-tool-delta', { kind: 'loop', committedTools: ['old'], executableTools: ['current'],
    pendingSystems: ['first', 'anchor-with-stale-tool-intent'], nextTools: [] });
  let turns = 0;
  observations.push(await loop(rec, {
    messages: [system('baseline', { toolsAdded: [toToolDeclaration(old)] })],
    tools: [current],
    prompts: [system('first'), user('between'), system('anchor', { sections: { note: '<note>retained</note>' },
      toolsAdded: [toToolDeclaration(old)], toolsRemoved: [{ name: 'invented' }] }), user('last')],
    responses: [assistant(), assistant()],
    config: {
      finishTurn: () => ++turns === 1 ? { action: 'continue' } : undefined,
      prepareNextTurn: ({ context }) => ({ context: { messages: context.messages.slice(), tools: [] }, messages: [user('after removal')] }),
    },
  }));
}
{
  const rec = recorder('stream-request-boundary', { kind: 'loop', stream: ['start', 'text_delta', 'done'],
    transform: 'remove custom; append transformed user', convert: 'append converted user',
    identity: { attribution: null, userAgent: { custom: 'oracle app' }, version: '1.0' }, apiKey: 'static-fallback', getApiKey: 'rotated' });
  observations.push(await loop(rec, {
    prompts: [user('original'), { role: 'custom', value: 'host only', timestamp: 14 }], stream: true,
    config: {
      reasoning: 'max', apiKey: 'static-fallback', identity: { attribution: null, userAgent: { custom: 'oracle app' }, version: '1.0' },
      transformContext: async (messages) => {
        rec.mark('transformContext', { messages });
        return [...messages.filter((message) => message.role !== 'custom'), user('transformed')];
      },
      convertToLlm: (messages) => {
        rec.mark('convertToLlm', { messages });
        return [...messages, user('converted')];
      },
      getApiKey: (provider) => { rec.mark('getApiKey', { provider }); return 'rotated'; },
    },
  }));
}

// Parallel preflight happens in source order before any execute(). A gate makes
// tool b end before a; transcript message emission still waits for both.
for (const mode of ['parallel', 'sequential', 'override']) {
  const rec = recorder(`tool-order-${mode}`, { kind: 'loop', mode, calls: ['a', 'b'], parallelCompletion: ['b', 'a'],
    override: mode === 'override' ? { b: 'sequential' } : {} });
  const releaseA = deferred();
  let rawUpdate;
  const tools = ['a', 'b'].map((name) => tool(name, async (id, args, signal, update) => {
    rec.mark('execute', { id, args, aborted: signal?.aborted ?? false });
    if (id === 'a') {
      rawUpdate = update;
      update(textResult('working'));
      if (mode === 'parallel') await releaseA.promise;
    }
    return textResult(id, { terminate: true });
  }, mode === 'override' && name === 'b' ? { executionMode: 'sequential' } : {}));
  observations.push(await loop(rec, {
    tools, responses: [assistant([call('a', 'a'), call('b', 'b')], 'toolUse')],
    config: {
      toolExecution: mode === 'sequential' ? 'sequential' : 'parallel',
      beforeToolCall: async ({ toolCall, args }) => { rec.mark('beforeToolCall', { id: toolCall.id, args }); },
      afterToolCall: async ({ toolCall, result }) => { rec.mark('afterToolCall', { id: toolCall.id, result }); },
      finishTurn: (turn) => { rec.mark('finishTurn', { turn }); },
    },
    emit: async (event) => {
      await rec.emit(event);
      if (event.type === 'tool_execution_end' && event.toolCallId === 'b') releaseA.resolve();
    },
  }));
  const beforeLate = rec.observation.events.length;
  rawUpdate(textResult('late after run'));
  await Promise.resolve();
  rec.observation.lateUpdate = { before: beforeLate, after: rec.observation.events.length };
}

// Accepted updates are awaited before the tool end. Updates from a settled
// parallel invocation are ignored while another invocation remains active.
{
  const rec = recorder('progress-await-and-late', { kind: 'loop', calls: ['a', 'b'],
    hold: ['update-listener', 'b-execute'], lateUpdates: ['after-a-before-b', 'after-run'] });
  const updateEntered = deferred();
  const releaseUpdate = deferred();
  const bStarted = deferred();
  const releaseB = deferred();
  const aEnded = deferred();
  let updateA;
  const running = loop(rec, {
    tools: [tool('a', async (_id, _args, _signal, update) => {
      updateA = update;
      update(textResult('progress'));
      return textResult('a', { terminate: true });
    }), tool('b', async () => {
      bStarted.resolve();
      await releaseB.promise;
      return textResult('b', { terminate: true });
    })],
    responses: [assistant([call('a', 'a'), call('b', 'b')], 'toolUse')],
    emit: async (event) => {
      await rec.emit(event);
      if (event.type === 'tool_execution_update') {
        updateEntered.resolve();
        await releaseUpdate.promise;
        rec.mark('updateListenerReleased');
      }
      if (event.type === 'tool_execution_end' && event.toolCallId === 'a') aEnded.resolve();
    },
  });
  await Promise.all([updateEntered.promise, bStarted.promise]);
  rec.mark('whileUpdateListenerBlocked', { toolEnds: rec.observation.events.filter((e) => e.type === 'tool_execution_end').length });
  releaseUpdate.resolve();
  await aEnded.promise;
  const before = rec.observation.events.length;
  updateA(textResult('late while b active'));
  await Promise.resolve();
  rec.mark('lateWhileActive', { before, after: rec.observation.events.length });
  releaseB.resolve();
  await running;
  updateA(textResult('late after run'));
  await Promise.resolve();
  observations.push(rec.observation);
}

// These validation/hook probes use the public runToolCall path; no scheduler or
// validation implementation is recreated in the oracle.
{
  const rec = recorder('tool-validation-and-hooks', { kind: 'runToolCall', cases: [
    'prepare-coerce-mutate', 'schema-coerce', 'schema-invalid', 'arguments-error', 'missing', 'blocked', 'throw', 'reported-error', 'after-throw',
  ] });
  const echo = tool('echo', async (id, args, _signal, update) => {
    rec.mark('execute', { id, args });
    update(textResult('progress'));
    if (id === 'throw') throw new Error('execute threw');
    return textResult(typeof args.value === 'string' ? args.value : JSON.stringify(args.value),
      { structuredContent: { value: args.value }, isError: id === 'reported-error' });
  }, { parameters: schema, prepareArguments: (args) => {
    rec.mark('prepareArguments', { args });
    return typeof args.legacyValue === 'string' ? { value: args.legacyValue } : args;
  } });
  const calls = [call('prepare-coerce-mutate', 'echo', { legacyValue: 'prepared' }), call('schema-coerce', 'echo', { value: 42 }),
    call('schema-invalid', 'echo', { value: {} }),
    { ...call('arguments-error', 'echo', { value: 'valid' }), argumentsError: 'unterminated JSON' },
    call('missing', 'missing'), ...['blocked', 'throw', 'reported-error', 'after-throw'].map((id) => call(id, 'echo', { value: id }))];
  rec.observation.outcomes = [];
  for (const toolCall of calls) {
    rec.observation.outcomes.push(snapshot(await runToolCall(toolCall, {
      tools: [echo], assistantMessage: assistant([toolCall], 'toolUse'), context: { messages: [] },
      beforeToolCall: async ({ toolCall: tc, args }) => {
        rec.mark('beforeToolCall', { id: tc.id, args });
        if (tc.id === 'blocked') return { block: true, reason: 'policy blocked', terminate: true };
        if (tc.id === 'prepare-coerce-mutate') args.value = { mutatedAfterValidation: true };
      },
      afterToolCall: async ({ toolCall: tc, args, result, isError }) => {
        rec.mark('afterToolCall', { id: tc.id, args, result, isError });
        if (tc.id === 'after-throw') throw new Error('after hook threw');
      },
      onUpdate: (partialResult) => { rec.mark('update', { partialResult }); },
    })));
  }
  observations.push(rec.observation);
}
{
  const variants = [{ content: [{ type: 'text', text: 'redacted' }] }, { structuredContent: { replaced: true } },
    { content: [{ type: 'text', text: 'redacted' }], structuredContent: { both: true } },
    { details: { replacement: true } }, { isError: false, terminate: false }];
  const rec = recorder('structured-content-overrides', { kind: 'runToolCall', variants });
  rec.observation.outcomes = [];
  for (const override of variants) {
    const tc = call('a');
    rec.observation.outcomes.push(snapshot(await runToolCall(tc, {
      tools: [tool('echo', async () => textResult('original', { structuredContent: { original: true },
        details: { old: true }, isError: true, terminate: true, usage }))],
      assistantMessage: assistant([tc], 'toolUse'), context: { messages: [] }, afterToolCall: async () => override,
    })));
  }
  observations.push(rec.observation);
}
{
  const rec = recorder('length-truncation', { kind: 'loop', stopReason: 'length', calls: ['schema-valid', 'arguments-error'] });
  observations.push(await loop(rec, {
    tools: [tool('echo', async () => { throw new Error('truncated calls must not execute'); }, { parameters: schema })],
    responses: [assistant([call('schema-valid', 'echo', { value: 'salvaged' }),
      { ...call('arguments-error'), argumentsError: 'bad JSON' }], 'length'), assistant()],
    config: { beforeToolCall: async () => { throw new Error('truncation must skip hooks'); } },
  }));
}
for (const unanimous of [true, false]) {
  const rec = recorder(`termination-${unanimous ? 'unanimous' : 'mixed'}`, { kind: 'loop',
    calls: ['blocked', 'executed'], blockTerminate: true, afterTerminate: unanimous });
  observations.push(await loop(rec, {
    tools: [tool('echo', async () => textResult('ok'))],
    responses: [assistant([call('blocked'), call('executed')], 'toolUse'), ...(unanimous ? [] : [assistant()])],
    config: {
      beforeToolCall: async ({ toolCall }) => toolCall.id === 'blocked' ? { block: true, reason: 'blocked', terminate: true } : undefined,
      afterToolCall: async () => ({ terminate: unanimous }),
    },
  }));
}
{
  const rec = recorder('duration-excludes-hooks', { kind: 'runToolCall', fakeMonotonicClock: true,
    beforeElapsed: 100, executeElapsed: 7, updateListenerElapsed: 300, afterElapsed: 200 });
  let clock = 0;
  const originalNow = performance.now;
  const releaseUpdate = deferred();
  performance.now = () => clock;
  try {
    const outcome = await runToolCall(call('a'), {
      tools: [tool('echo', async (_id, _args, _signal, update) => {
        update(textResult('progress'));
        clock += 7;
        // Release via a microtask after execute's returned promise settles, so
        // the update listener's 300 ms remains outside the measured duration.
        queueMicrotask(() => queueMicrotask(() => releaseUpdate.resolve()));
        return textResult('done');
      })],
      assistantMessage: assistant(), context: { messages: [] },
      beforeToolCall: async () => { clock += 100; },
      afterToolCall: async () => { clock += 200; },
      onUpdate: async () => { await releaseUpdate.promise; clock += 300; },
    });
    rec.observation.outcome = snapshot(outcome);
    rec.observation.measuredExecuteMilliseconds = outcome.durationMs;
    rec.observation.totalSyntheticMilliseconds = clock;
    assert.equal(outcome.durationMs, 7);
  } finally { performance.now = originalNow; }
  observations.push(rec.observation);
}

for (const abortAt of ['before-call', 'before-hook']) {
  const rec = recorder(`tool-abort-${abortAt}`, { kind: 'runToolCall', abortAt });
  const controller = new AbortController();
  if (abortAt === 'before-call') controller.abort();
  rec.observation.outcome = snapshot(await runToolCall(call('a'), {
    tools: [tool('echo', async () => { throw new Error('aborted call must not execute'); })],
    assistantMessage: assistant(), context: { messages: [] }, signal: controller.signal,
    beforeToolCall: async (_context, signal) => {
      rec.mark('beforeToolCall', { aborted: signal.aborted });
      if (abortAt === 'before-hook') controller.abort();
    },
    afterToolCall: async () => { throw new Error('aborted call must not finalize through after hook'); },
  }));
  observations.push(rec.observation);
}

for (const queueMode of ['one-at-a-time', 'all']) {
  const rec = recorder(`agent-assistant-tail-${queueMode}`, { kind: 'Agent', queueMode,
    initialMessages: ['initial user', 'initial assistant'], steering: ['s1', 's2'], followUp: ['f1', 'f2'] });
  const agent = new Agent({ initialState: { model: model(), systemPrompt: 'baseline',
    messages: [user('initial user'), assistant([{ type: 'text', text: 'initial assistant' }])] },
    steeringMode: queueMode, followUpMode: queueMode,
    streamFn: provider(rec, Array.from({ length: queueMode === 'all' ? 2 : 4 }, () => assistant())) });
  agent.subscribe(rec.emit);
  agent.steer(user('s1')); agent.steer(user('s2'));
  agent.followUp(user('f1')); agent.followUp(user('f2'));
  rec.mark('peek', { first: agent.peekQueuedMessages(), second: agent.peekQueuedMessages(), has: agent.hasQueuedMessages() });
  await agent.continue();
  rec.observation.state = snapshot(agent.state);
  rec.observation.queued = agent.hasQueuedMessages();
  observations.push(rec.observation);
}
{
  const rec = recorder('agent-end-listener-idle-barrier', { kind: 'Agent', finish: 'end',
    queuedDuringAssistant: ['s1', 's2'], followUp: ['f1'], holdAgentEndListener: true });
  const entered = deferred();
  const release = deferred();
  let promptSettled = false;
  let idleSettled = false;
  const agent = new Agent({ initialState: { model: model(), systemPrompt: 'baseline' },
    finishTurn: () => ({ action: 'end' }), streamFn: provider(rec, [assistant()]) });
  agent.followUp(user('f1'));
  agent.subscribe(async (event, signal) => {
    await rec.emit(event);
    if (event.type === 'message_end' && event.message.role === 'assistant') {
      agent.steer(user('s1')); agent.steer(user('s2'));
    }
    if (event.type === 'agent_end') {
      rec.mark('firstListenerEntered', { streaming: agent.state.isStreaming, signalActive: agent.signal === signal,
        streamingMessage: agent.state.streamingMessage ?? null, pending: [...agent.state.pendingToolCalls] });
      entered.resolve();
      await release.promise;
      rec.mark('firstListenerReleased');
    }
  });
  agent.subscribe(async (event) => {
    if (event.type === 'agent_end') rec.mark('secondListener', { streaming: agent.state.isStreaming });
  });
  const run = agent.prompt(user('run')).then(() => { promptSettled = true; });
  const idle = agent.waitForIdle().then(() => { idleSettled = true; });
  await entered.promise;
  await Promise.resolve();
  const rejections = {};
  for (const operation of ['prompt', 'continue', 'reset']) {
    try { await (operation === 'prompt' ? agent.prompt(user('blocked')) : operation === 'continue' ? agent.continue() : agent.reset()); }
    catch (error) { rejections[operation] = error.message; }
  }
  rec.mark('blockedAtAgentEnd', { promptSettled, idleSettled, streaming: agent.state.isStreaming, rejections,
    queued: agent.hasQueuedMessages(), peek: agent.peekQueuedMessages() });
  release.resolve();
  await Promise.all([run, idle]);
  rec.mark('idle', { promptSettled, idleSettled, streaming: agent.state.isStreaming, signal: agent.signal ?? null,
    pending: [...agent.state.pendingToolCalls], peek: agent.peekQueuedMessages() });
  agent.clearSteeringQueue();
  rec.mark('afterClearSteering', { peek: agent.peekQueuedMessages() });
  rec.observation.state = snapshot(agent.state);
  observations.push(rec.observation);
}

process.stdout.write(`${JSON.stringify({ formatVersion: 1, observations }, null, 2)}\n`);
