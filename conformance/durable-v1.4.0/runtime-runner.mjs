import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { resolve } from 'node:path';
import { createModels, createProvider, createAssistantMessageEventStream, fauxProvider } from '@earendil-works/pi-ai';
import { AgentDoc, CompactionTask, GenerationTask, InboxDoc, LiveDoc, ProviderDoc, ToolTask, UsageDoc, createRegistry, defineExtension, defineTool, Harness, hook, section } from '@earendil-works/pi-durable';
import { MemoryStorage } from '@earendil-works/pi-durable/storage/memory';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { applyImmutable } from '@earendil-works/chord/delta';
import { directory } from './setup.mjs';
import { encodeTraceValue } from './trace-codec.mjs';

// Approved test-host boundary, independent from Harness wall time and sleep wakeups.
// It is nondecreasing across all cases in this process. No elapsed-performance claim.
let monotonic = 0;
Object.defineProperty(performance, 'now', { configurable: true, value: () => monotonic });
const tokens = { agent: AgentDoc, inbox: InboxDoc, live: LiveDoc, provider: ProviderDoc, usage: UsageDoc };
const tick = () => new Promise(done => setImmediate(done));
async function flush() { for (let i = 0; i < 6; i++) await tick(); }
const errorValue = error => ({ category: error?.name ?? 'Error', message: error?.message ?? String(error) });
const copy = value => value === undefined ? { $undefined: true } : encodeTraceValue(JSON.parse(JSON.stringify(value, (_key, v) => v instanceof Error ? errorValue(v) : typeof v === 'function' ? undefined : v)));
function ref(value, aliases) {
  if (typeof value === 'string' && value.startsWith('$')) {
    const [name, ...keys] = value.slice(1).split('.');
    assert.ok(Object.hasOwn(aliases, name), `Unknown runtime reference ${value}`);
    let found = aliases[name];
    for (const key of keys) found = found[key];
    return structuredClone(found);
  }
  if (Array.isArray(value)) return value.map(child => ref(child, aliases));
  if (value && typeof value === 'object') {
    if (value.$undefined === true && Object.keys(value).length === 1) return undefined;
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, ref(child, aliases)]));
  }
  return value;
}
function bounded(promise, label) {
  let timer;
  return Promise.race([promise, new Promise((_, fail) => { timer = setTimeout(() => fail(new Error(`Runtime fixture timed out: ${label}`)), 5000); })]).finally(() => clearTimeout(timer));
}
async function eventually(check, label) {
  for (let attempt = 0; attempt < 300; attempt++) { if (await check()) return; await flush(); }
  throw new Error(`Runtime fixture condition not reached: ${label}`);
}
const zeroUsage = () => ({ input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } });
export async function executeRuntimeCase(test, backend) {
  mkdirSync(resolve(directory, '.runtime'), { recursive: true });
  const temporary = mkdtempSync(resolve(directory, '.runtime', 'runtime-'));
  const path = resolve(temporary, 'store.sqlite');
  const makeStorage = () => backend === 'memory' ? new MemoryStorage() : openNodeSqliteStorage(path);
  let storage = await makeStorage();
  let harness;
  let wall = test.input.clock ?? 1000;
  const aliases = { root: 1 };
  const gates = new Map();
  const timers = [];
  const requests = [];
  const toolEvents = [];
  const hookEvents = [];
  const publications = [];
  const reports = [];
  const observations = [];
  const effects = {};
  const watches = new Map();
  const toolDefinitions = new Map();
  const registry = createRegistry();
  const settings = { compaction: { enabled: false, reserveTokens: 1000, keepRecentTokens: 10, backgroundTokens: 0 }, progress: { partialIntervalMs: 10, outputIntervalMs: 10 }, ...structuredClone(test.input.settings ?? {}) };
  const responses = structuredClone(test.input.provider?.responses ?? []);
  const getBindings = extra => ({ ...aliases, clock: wall, ...extra });
  const makeGate = name => {
    if (!gates.has(name)) { let release; const promise = new Promise(done => { release = done; }); gates.set(name, { promise, release }); }
    return gates.get(name);
  };
  function waitGate(name, signal) {
    const gate = makeGate(name);
    return new Promise((done, fail) => {
      const abort = () => fail(signal.reason);
      if (signal?.aborted) return abort();
      signal?.addEventListener('abort', abort, { once: true });
      gate.promise.then(() => { signal?.removeEventListener('abort', abort); done(); });
    });
  }
  const metadata = fauxProvider({ api: 'oracle-script', provider: 'oracle', models: test.input.provider?.models ?? [{ id: 'scripted-1', reasoning: true, contextWindow: 100000, maxTokens: 1000 }] });
  function message(step, model) {
    return { role: 'assistant', content: [...(step.text === undefined ? [] : [{ type: 'text', text: step.text }]), ...(step.calls ?? []).map(call => ({ type: 'toolCall', ...call }))], api: model.api, provider: model.provider, model: model.id, usage: step.usage ?? zeroUsage(), stopReason: step.stopReason ?? (step.calls?.length ? 'toolUse' : 'stop'), timestamp: step.timestamp ?? wall, durationMs: 0, ...(step.errorMessage ? { errorMessage: step.errorMessage } : {}), ...(step.deferred ? { deferred: step.deferred } : {}) };
  }
  function stream(kind, model, transcript, options, handle) {
    const step = responses.shift();
    assert.ok(step, `No scripted provider response for ${test.id}`);
    assert.equal(step.op ?? 'stream', kind, 'Scripted provider operation differs');
    const { signal, telemetry, ...jsonOptions } = options ?? {};
    requests.push({ kind, model: copy(model), context: copy(transcript), options: copy(jsonOptions), ...(handle ? { handle: copy(handle) } : {}) });
    const events = createAssistantMessageEventStream();
    const final = message(step, model);
    let partial = { ...final, content: [], stopReason: 'pending' };
    queueMicrotask(async () => {
      try {
        if (step.gateBefore) await waitGate(step.gateBefore, signal);
        events.push({ type: 'start', partial: structuredClone(partial) });
        if (step.partial !== undefined) {
          partial = { ...partial, content: [{ type: 'text', text: step.partial }] };
          events.push({ type: 'text_start', contentIndex: 0, partial: { ...partial, content: [{ type: 'text', text: '' }] } });
          events.push({ type: 'text_delta', contentIndex: 0, delta: step.partial, partial: structuredClone(partial) });
        }
        if (step.gate) await waitGate(step.gate, signal);
        if (signal?.aborted) throw signal.reason;
        if (final.stopReason === 'error' || final.stopReason === 'aborted') events.push({ type: 'error', reason: final.stopReason, error: final });
        else events.push({ type: 'done', reason: final.stopReason, message: final });
        events.end(final);
      } catch (_error) {
        const aborted = { ...partial, stopReason: 'aborted', errorMessage: 'Scripted transport cancelled', durationMs: 0 };
        events.push({ type: 'error', reason: 'aborted', error: aborted }); events.end(aborted);
      }
    });
    return events;
  }
  const provider = createProvider({ id: 'oracle', auth: { apiKey: { name: 'Scripted local provider', resolve: async () => ({ auth: {} }) } }, models: metadata.models,
    api: { stream: (model, transcript, options) => stream('stream', model, transcript, options), streamSimple: (model, transcript, options) => stream('stream', model, transcript, options), fetchDeferred: (model, handle, options) => stream('fetch', model, undefined, options, handle), cancelDeferred: async (_model, handle) => { requests.push({ kind: 'cancel', handle: copy(handle) }); } } });
  const models = createModels(); models.setProvider(provider);
  function installTool(id, override = {}) {
    const descriptor = test.input.tools?.find(tool => tool.id === id);
    assert.ok(descriptor, `Unknown local tool ${id}`);
    const tool = defineTool({ name: descriptor.name, description: descriptor.description ?? descriptor.name, parameters: descriptor.parameters ?? { type: 'object', properties: {}, additionalProperties: false }, replay: override.replay ?? descriptor.replay ?? 'unsafe', ...(descriptor.structuredOutputSchema ? { structuredOutputSchema: descriptor.structuredOutputSchema } : {}), ...(descriptor.callers ? { callers: descriptor.callers } : {}),
      ...(descriptor.prepare ? { prepareArguments(args) {
        toolEvents.push({ kind: 'prepareArguments', tool: descriptor.name, args: copy(args) });
        const prepared = structuredClone(args);
        if (descriptor.prepare.numberField) prepared[descriptor.prepare.numberField] = Number(prepared[descriptor.prepare.numberField]);
        return prepared;
      } } : {}),
      async execute(args, api, c) {
        const run = effects[descriptor.name] = (effects[descriptor.name] ?? 0) + 1;
        const stored = await storage.task(api.taskId, context);
        toolEvents.push({ kind: 'execute', tool: descriptor.name, run, taskId: api.taskId, callId: api.callId, args: copy(args), persisted: copy(stored) });
        const local = {};
        for (const raw of descriptor.actions ?? []) {
          const action = ref(raw, getBindings({ args, local, run, taskId: api.taskId, callId: api.callId }));
          switch (action.do) {
            case 'output': api.output(action.text); break;
            case 'details':
              await api.details(action.value, c);
              toolEvents.push({ kind: 'detailsAcknowledged', taskId: api.taskId, value: copy(action.value), persistedLive: copy(await harness.snapshot(LiveDoc, api.conversationId, context)) });
              break;
            case 'diagnostic': api.diagnostic(action.value); break;
            case 'gate': if (action.firstRuns === undefined || run <= action.firstRuns) await waitGate(action.name, c.abortSignal); break;
            case 'nested': {
              try {
                const value = await api.executeTool(action.name, action.args ?? {}, c, { key: action.key, ...(action.progress === false ? { progress: false } : {}) });
                if (action.as) local[action.as] = value;
                toolEvents.push({ kind: 'nestedResult', taskId: api.taskId, key: action.key, value: copy(value) });
              } catch (error) {
                if (!action.catch) throw error;
                toolEvents.push({ kind: 'nestedRejected', taskId: api.taskId, error: errorValue(error) });
              }
              break;
            }
            case 'return': return action.value;
            case 'throw': throw new Error(action.message);
            default: throw new Error(`Unknown local-tool action ${action.do}`);
          }
        }
        return {};
      },
    });
    toolDefinitions.set(id, tool);
    registry.install(defineExtension({ name: `tool:${descriptor.name}`, tools: [tool] }));
  }
  for (const tool of test.input.tools ?? []) installTool(tool.id);
  const selectedHooks = test.input.hooks ?? {};
  let yieldCount = 0;
  if (selectedHooks.generation) registry.install(defineExtension({ name: 'oracle:generation-hooks', hooks: [hook(GenerationTask, {
    beforeRequest(request, api) {
      hookEvents.push({ kind: 'beforeRequest', taskId: api.taskId, messages: copy(request.messages) });
      if (selectedHooks.generation.appendUser) return { messages: [...request.messages, { role: 'user', content: selectedHooks.generation.appendUser, timestamp: wall }] };
    },
    afterResponse(answer, api) { hookEvents.push({ kind: 'afterResponse', taskId: api.taskId, message: copy(answer) }); },
    onYield(answer, api) { hookEvents.push({ kind: 'onYield', taskId: api.taskId, message: copy(answer) }); if (selectedHooks.generation.continueOnce && yieldCount++ === 0) return { continue: selectedHooks.generation.continueOnce }; },
  })] }));
  if (selectedHooks.tool) registry.install(defineExtension({ name: 'oracle:tool-hooks', hooks: [hook(ToolTask, {
    beforeTool(call, api) { hookEvents.push({ kind: 'beforeTool', taskId: api.taskId, call: copy(call) }); const decision = selectedHooks.tool.before?.[call.name]; if (decision) return structuredClone(decision); },
    afterTool(call, result, api) { hookEvents.push({ kind: 'afterTool', taskId: api.taskId, call: copy(call), result: copy(result) }); },
  })] }));
  if (selectedHooks.compaction) registry.install(defineExtension({ name: 'oracle:compaction-hooks', hooks: [hook(CompactionTask, {
    beforeCompact(input, api) { hookEvents.push({ kind: 'beforeCompact', taskId: api.taskId, input: copy(input) }); return structuredClone(selectedHooks.compaction); },
  })] }));
  for (const descriptor of test.input.sections ?? []) registry.install(defineExtension({ name: `section:${descriptor.key}`, sections: [section(descriptor.key, () => descriptor.text, { tag: descriptor.tag ?? false })] }));
  async function open() {
    harness = await Harness.open(storage, { models, registry, settings: () => settings, now: () => wall, onWake: () => {},
      setTimer(callback, delay, options) { if (options.background) return undefined; const timer = { callback, at: monotonic + delay, disarmed: false }; timers.push(timer); return () => { timer.disarmed = true; }; },
      conversationCreated: async (tx, conversation) => { (await tx.doc(ProviderDoc, conversation.id)).sessionId = `fixture-session-${conversation.id}`; },
      onReport(error, info) { reports.push({ error: errorValue(error), type: info.type, ...(info.task ? { task: copy(info.task) } : {}) }); },
    }, context);
    harness.subscribeCommits(publication => publications.push(publication));
    await harness.root(context, { agent: { model: { provider: 'oracle', modelId: 'scripted-1' }, ...test.input.agent } });
    await flush();
  }
  await open();
  const conversation = async id => { const handle = await harness.conversation(id ?? 1, context); assert.ok(handle, `Missing conversation ${id}`); return handle; };
  const submission = async id => { const handle = await harness.submission(id, context); assert.ok(handle, `Missing submission ${id}`); return handle; };
  const currentLive = async id => harness.snapshot(LiveDoc, id ?? 1, context);
  async function fireTimers() {
    const active = timers.filter(timer => !timer.disarmed);
    for (const timer of active) { timer.disarmed = true; monotonic = Math.max(monotonic, timer.at); timer.callback(); }
    await flush(); return active.length;
  }
  try {
    for (let index = 0; index < test.input.operations.length; index++) {
      const raw = test.input.operations[index];
      const op = ref(raw, getBindings({}));
      let result;
      try {
        const handle = ['submit','append','configure','context','entries','reset','fork','watch','compact','abortConversation'].includes(op.op) ? await conversation(op.conversationId) : undefined;
        switch (op.op) {
          case 'submit': result = (await handle.submit(op.value, context)).id; break;
          case 'status': result = await (await submission(op.id)).status(context); break;
          case 'wait': result = await bounded((await submission(op.id)).wait(context), `submission ${op.id}`); await flush(); break;
          case 'abortSubmission': result = await (await submission(op.id)).abort(context); await flush(); break;
          case 'append': result = await handle.commit(tx => tx.appendEntry(handle.id, op.value), context); break;
          case 'configure': result = await handle.configure(op.change, context); break;
          case 'context': result = await handle.context(context, op.at === undefined ? undefined : { at: op.at }); break;
          case 'entries': result = await handle.entries({ order: 'ascending' }, 1000, undefined, context); break;
          case 'docs': result = {}; for (const [name, token] of Object.entries(tokens)) result[name] = await harness.snapshot(token, op.conversationId ?? 1, context); break;
          case 'usage': result = await harness.usage(context); break;
          case 'task': result = await harness.getTask(op.id, context); break;
          case 'tasks': result = await harness.commit(tx => tx.scanTasks({ conversationId: op.conversationId ?? 1 }, 100), context); break;
          case 'live': result = await currentLive(op.conversationId); break;
          case 'inspect': result = await harness.inspect(context); break;
          case 'resume': harness.resume(); await flush(); result = 'resumed'; break;
          case 'idle': result = await bounded(harness.waitForIdle(context), 'idle'); await flush(); break;
          case 'requests': result = requests; break;
          case 'toolEvents': result = toolEvents; break;
          case 'effects': result = effects; break;
          case 'untilRequests': await eventually(() => requests.length >= op.count, 'provider request'); await flush(); result = requests.length; break;
          case 'untilEffects': await eventually(() => (effects[op.name] ?? 0) >= op.count, 'local tool execution'); await flush(); result = effects; break;
          case 'untilLive': await eventually(async () => {
            let value = await currentLive(op.conversationId); for (const key of op.path) value = value?.[key]; return op.equals === undefined ? value !== undefined : JSON.stringify(value) === JSON.stringify(op.equals);
          }, 'committed live path'); result = await currentLive(op.conversationId); break;
          case 'fireTimers': result = await fireTimers(); break;
          case 'settings': Object.assign(settings, op.change); result = settings; break;
          case 'advance': wall = op.to; harness.resume(); await flush(); result = wall; break;
          case 'wake': await flush(); result = harness.wake(); break;
          case 'release': makeGate(op.name).release(); await flush(); result = 'released'; break;
          case 'installTool': installTool(op.id, { replay: op.replay }); result = 'installed'; break;
          case 'reset': result = await handle.reset(op.handoff, context); break;
          case 'fork': result = (await handle.fork(op.at, { ownership: { kind: 'ownerless' } }, context)).id; break;
          case 'compact': result = await handle.compact(op.instructions, context); break;
          case 'waitTask': result = await bounded(harness.waitForTask(op.id, context), `task ${op.id}`); await flush(); break;
          case 'abortConversation': result = await bounded(handle.abort(context), 'conversation abort'); break;
          case 'watch': {
            const watch = await handle.watch(context); const frames = []; const initial = watch.value;
            const sourceSeqs = [];
            const stop = harness.subscribeCommits(publication => {
              if (publication.changes.some(change => change.type === 'entry' && change.value.conversationId === handle.id || change.type === 'document' && change.conversationId === handle.id && Object.values(tokens).some(token => token.definition.kind === change.record.kind) && change.ops.length > 0)) sourceSeqs.push(publication.seq);
            });
            watch.start(async (value, ops) => { frames.push({ value: copy(value), ops: copy(ops) }); });
            watches.set(op.name, { watch, initial, frames, sourceSeqs, stop }); result = initial; break;
          }
          case 'watchFrames': {
            await flush(); const watched = watches.get(op.name); let replay = watched.initial;
            for (const frame of watched.frames) { replay = applyImmutable(replay, frame.ops); assert.deepEqual(copy(replay), frame.value, 'Actual mounted source operations must replay'); }
            assert.equal(watched.frames.length, watched.sourceSeqs.length, 'One mounted frame per relevant source commit');
            result = { frames: watched.frames, touchingCommits: watched.sourceSeqs, final: watched.watch.value }; break;
          }
          case 'close': await bounded(harness.close(context), 'close'); result = 'closed'; break;
          case 'reopen': assert.equal(backend, 'sqlite'); await bounded(harness.close(context), 'reopen close'); storage = await makeStorage(); await open(); result = 'reopened paused'; break;
          default: throw new Error(`Unknown runtime host operation ${op.op}`);
        }
      } catch (error) {
        if (!op.errorExpected) { error.message = `${test.id} (${backend}) operation ${index} ${op.op}: ${error.message}`; throw error; }
        result = { error: errorValue(error) };
      }
      if (op.as) aliases[op.as] = result;
      observations.push({ label: op.label ?? op.as ?? `${index}:${op.op}`, value: copy(result) });
    }
    await flush();
    assert.equal(responses.length, 0, `Unused scripted responses in ${test.id}`);
    return { observations, requests: copy(requests), toolEvents: copy(toolEvents), hookEvents: copy(hookEvents), publications: copy(publications), reports: copy(reports) };
  } finally {
    for (const watched of watches.values()) { watched.stop(); await watched.watch.stop(); }
    await bounded(harness.close(context), 'final close');
    rmSync(temporary, { recursive: true, force: true });
  }
}
