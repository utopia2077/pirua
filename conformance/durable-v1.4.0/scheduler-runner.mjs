import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { resolve } from 'node:path';
import { createModels } from '@earendil-works/pi-ai';
import { createRegistry, defineExtension, defineTask, Harness, MemoryStorage } from '@earendil-works/pi-durable';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { directory } from './setup.mjs';
import { encodeTraceValue } from './trace-codec.mjs';

const tick = () => new Promise(done => setImmediate(done));
async function flush() { for (let i = 0; i < 5; i++) await tick(); }
const errorValue = error => ({ category: error?.name ?? 'Error', message: error?.message ?? String(error) });
const copy = value => value === undefined ? { $undefined: true } : encodeTraceValue(JSON.parse(JSON.stringify(value, (_key, v) => v instanceof Error ? errorValue(v) : v)));
function reference(value, bindings) {
  if (typeof value === 'string' && value.startsWith('$')) {
    const [name, ...keys] = value.slice(1).split('.');
    assert.ok(Object.hasOwn(bindings, name), `Unknown scheduler reference ${value}`);
    let found = bindings[name];
    for (const key of keys) found = found[key];
    return structuredClone(found);
  }
  if (Array.isArray(value)) return value.map(item => reference(item, bindings));
  if (value && typeof value === 'object') {
    if (value.$undefined === true && Object.keys(value).length === 1) return undefined;
    return Object.fromEntries(Object.entries(value).map(([key, child]) => [key, reference(child, bindings)]));
  }
  return value;
}
async function eventually(check, description) {
  for (let attempt = 0; attempt < 250; attempt++) {
    if (await check()) return;
    await flush();
  }
  throw new Error(`Scheduler fixture condition not reached: ${description}`);
}
function bounded(promise, label) {
  let timer;
  return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Scheduler fixture timed out: ${label}`)), 5000); })]).finally(() => clearTimeout(timer));
}
export async function executeSchedulerCase(test, backend) {
  mkdirSync(resolve(directory, '.runtime'), { recursive: true });
  const temporary = mkdtempSync(resolve(directory, '.runtime', 'scheduler-'));
  const path = resolve(temporary, 'store.sqlite');
  const makeStorage = () => backend === 'memory' ? new MemoryStorage() : openNodeSqliteStorage(path);
  let storage = await makeStorage();
  let harness;
  let clock = test.input.clock ?? 1000;
  const aliases = {};
  const gates = new Map();
  const pending = new Map();
  const taskPublications = [];
  const reports = [];
  const events = [];
  const observations = [];
  const definitions = new Map();
  const registry = createRegistry();
  const extensions = new Map();
  const timers = [];
  const binding = extra => ({ ...aliases, ...extra, clock });
  function gate(name) {
    if (!gates.has(name)) {
      let release;
      const promise = new Promise(done => { release = done; });
      gates.set(name, { promise, release });
    }
    return gates.get(name);
  }
  function install(id) {
    const token = definitions.get(id);
    assert.ok(token, `Unknown task definition ${id}`);
    const extension = defineExtension({ name: `oracle:${token.definition.name}`, tasks: [token] });
    extensions.set(id, extension);
    registry.install(extension);
  }
  async function runActions(descriptor, actions, task, runtime, callContext, mode) {
    const local = {};
    events.push({ kind: mode, definition: descriptor.id, taskId: task.id, checkpoint: copy(task.state.checkpoint), now: clock });
    const resolveValue = value => reference(value, binding({ task, local }));
    for (const raw of actions) {
      const action = resolveValue(raw);
      let result;
      switch (action.do) {
        case 'complete':
          result = await runtime.commit(() => ({ status: 'terminal', outcome: action.outcome ?? { status: 'completed', result: action.result ?? null } }), callContext);
          break;
        case 'next': result = await runtime.commit(() => action.state, callContext); break;
        case 'noProgressCommit': result = await runtime.commit(() => undefined, callContext); break;
        case 'counter':
          result = await runtime.commit((_tx, current) => {
            const count = current.state.checkpoint[action.field] + 1;
            return count >= action.until ? { status: 'terminal', outcome: { status: 'completed', result: count } } : { status: 'running', checkpoint: { ...current.state.checkpoint, [action.field]: count } };
          }, callContext);
          break;
        case 'sleep': result = await runtime.sleepUntil(action.until, action.checkpoint, callContext); break;
        case 'memo': result = action.read ? await runtime.memo(action.name, callContext) : await runtime.memo(action.name, action.candidate, callContext); break;
        case 'record': result = await runtime.getTask(action.id ?? task.id, callContext); break;
        case 'ownedTasks': result = await runtime.ownedTasks(callContext); break;
        case 'outcomes': result = await runtime.outcomes(action.ids, callContext); break;
        case 'spawn':
          await runtime.commit(async tx => {
            result = await tx.createTask(definitions.get(action.definition), action.input ?? null, { ownership: { kind: 'task', taskId: task.id } });
            return action.state;
          }, callContext);
          break;
        case 'wait':
          result = await runtime.commit(() => ({ status: 'waiting', checkpoint: action.checkpoint, on: action.on, policy: action.policy ?? 'allSettled' }), callContext);
          break;
        case 'waitForTask': result = await runtime.waitForTask(action.id, callContext); break;
        case 'gate': {
          const waiting = gate(action.name);
          if (action.abortable === false) await waiting.promise;
          else await new Promise((done, fail) => {
            let finished = false;
            const onAbort = async () => {
              if (finished) return;
              finished = true;
              const stored = await storage.task(task.id, context);
              events.push({ kind: 'signal', taskId: task.id, abortRequested: stored?.abortRequested, status: stored?.state.status });
              if (action.tryCommitOnSignal) {
                try { await runtime.commit(() => ({ status: 'terminal', outcome: { status: 'completed', result: 'stale' } }), context); }
                catch (error) { events.push({ kind: 'rejectedSignalCommit', taskId: task.id, error: errorValue(error) }); }
              }
              fail(runtime.signal.reason);
            };
            runtime.signal.addEventListener('abort', onAbort, { once: true });
            if (runtime.signal.aborted) void onAbort();
            waiting.promise.then(() => { if (!finished) { finished = true; runtime.signal.removeEventListener('abort', onAbort); done(); } });
          });
          break;
        }
        case 'lateCommit':
          try { await runtime.commit(() => action.state, context); result = 'accepted'; }
          catch (error) { result = { error: errorValue(error) }; }
          break;
        case 'timer': {
          runtime.setTimer(() => { events.push({ kind: 'timerCallback', taskId: task.id, name: action.name }); }, action.delay ?? 1);
          result = 'armed'; break;
        }
        case 'observe': result = action.value; break;
        case 'throw': throw new Error(action.message);
        default: throw new Error(`Unknown scheduler handler action ${action.do}`);
      }
      if (action.as) local[action.as] = result;
      if (action.label || ['memo', 'record', 'ownedTasks', 'outcomes', 'lateCommit', 'observe'].includes(action.do)) {
        events.push({ kind: 'handlerObservation', taskId: task.id, label: action.label ?? action.do, value: copy(result) });
      }
    }
  }
  for (const descriptor of test.input.tasks ?? []) {
    const phases = Object.fromEntries(Object.entries(descriptor.phases).map(([phase, actions]) => [phase, (task, runtime, c) => runActions(descriptor, actions, task, runtime, c, 'phase')]));
    definitions.set(descriptor.id, defineTask({
      name: descriptor.name,
      version: descriptor.version ?? 1,
      initial: input => reference(descriptor.initial ?? { phase: 'run' }, { input }),
      phases,
      abort: (task, runtime, c) => runActions(descriptor, descriptor.abort ?? [{ do: 'complete', outcome: { status: 'aborted', reason: 'oracle' } }], task, runtime, c, 'abort'),
    }));
  }
  for (const id of test.input.installed ?? [...definitions.keys()]) install(id);
  const initialWrites = [{ type: 'conversation', value: { id: 1 } }, ...(test.input.seed ?? [])];
  await storage.commit(initialWrites, context);
  if (test.input.seededReopen) {
    assert.equal(backend, 'sqlite');
    await storage.close(context);
    storage = await makeStorage();
  }
  async function open() {
    harness = await Harness.open(storage, {
      models: createModels(), registry, now: () => clock,
      settings: { documentRetentionMs: 0, ...test.input.settings },
      onWake: () => {},
      setTimer(callback, delayMs, options) {
        if (options.background) return undefined;
        const timer = { callback, delayMs, disarmed: false };
        timers.push(timer);
        return () => { timer.disarmed = true; };
      },
      onReport(error, info) {
        reports.push({ error: errorValue(error), type: info.type, ...(info.task ? { task: copy(info.task) } : {}), ...(info.reason ? { reason: info.reason } : {}) });
      },
    }, context);
    harness.subscribeCommits(publication => {
      const tasks = publication.changes.filter(change => change.type === 'task').map(change => change.value);
      if (tasks.length) taskPublications.push({ seq: publication.seq, tasks: copy(tasks) });
    });
    await flush();
  }
  await open();
  try {
    for (let index = 0; index < test.input.operations.length; index++) {
      const raw = test.input.operations[index];
      const op = reference(raw, binding({}));
      let result;
      switch (op.op) {
        case 'create':
          result = await harness.commit(tx => tx.createTask(definitions.get(op.definition), op.input ?? null, { ownership: op.owner ? { kind: 'task', taskId: op.owner } : { kind: 'conversation' }, conversationId: op.conversationId ?? 1, ...(op.background ? { background: true } : {}), ...(op.abandonOnRestart ? { abandonOnRestart: true } : {}) }), context);
          break;
        case 'createMany':
          result = await harness.commit(async tx => {
            const ids = [];
            for (const item of op.tasks) ids.push(await tx.createTask(definitions.get(item.definition), item.input ?? null, { ownership: item.owner ? { kind: 'task', taskId: item.owner } : { kind: 'conversation' }, conversationId: item.conversationId ?? 1 }));
            return ids;
          }, context);
          break;
        case 'resume': harness.resume(); await flush(); result = 'resumed'; break;
        case 'inspect': await flush(); result = await harness.inspect(context); break;
        case 'wake': await flush(); result = harness.wake(); break;
        case 'get': result = await harness.getTask(op.id, context); break;
        case 'wait': result = await bounded(harness.waitForTask(op.id, context), `wait ${op.id}`); await flush(); break;
        case 'until':
          await eventually(async () => {
            const record = await harness.getTask(op.id, context);
            return op.status ? record?.state.status === op.status : events.some(event => event.kind === op.event && (op.definition ? event.definition === op.definition : true));
          }, `${test.id} ${op.status ?? op.event}`);
          await flush(); result = op.id ? await harness.getTask(op.id, context) : events.filter(event => event.kind === op.event); break;
        case 'abortConversation': {
          const conversation = await harness.conversation(op.id ?? 1, context);
          result = await bounded(conversation.abort(context, { background: op.background ?? false }), 'conversation abort'); await flush(); break;
        }
        case 'abort': result = await bounded(harness.abortTask(op.id, context), `abort ${op.id}`); await flush(); break;
        case 'retry': result = await harness.retryTask(op.id, context); await flush(); break;
        case 'release': gate(op.name).release(); await flush(); result = 'released'; break;
        case 'advance': clock = op.to; if (op.resume !== false) harness.resume(); await flush(); result = clock; break;
        case 'install': install(op.definition); await flush(); result = 'installed'; break;
        case 'uninstall': registry.uninstall(extensions.get(op.definition)); await flush(); result = 'uninstalled'; break;
        case 'startWait': {
          const status = { done: false, value: undefined };
          const promise = op.scope === 'idle' ? harness.waitForIdle(context) : harness.waitForTask(op.id, context);
          promise.then(value => { status.done = true; status.value = copy(value); }, error => { status.done = true; status.value = { error: errorValue(error) }; });
          pending.set(op.name, { status, promise }); await flush(); result = status.done; break;
        }
        case 'waitStatus': await flush(); result = pending.get(op.name).status; break;
        case 'joinWait': result = await bounded(pending.get(op.name).promise, op.name); await flush(); break;
        case 'events': await flush(); result = events; break;
        case 'timers': result = timers.map(timer => ({ delayMs: timer.delayMs, disarmed: timer.disarmed })); break;
        case 'fireTimers': for (const timer of timers) timer.callback(); await flush(); result = 'fired retained host callbacks'; break;
        case 'close': await bounded(harness.close(context), 'close'); result = 'closed'; break;
        case 'rawTask': result = await storage.task(op.id, context); break;
        case 'reopen':
          assert.equal(backend, 'sqlite');
          await bounded(harness.close(context), 'reopen close');
          storage = await makeStorage(); await open(); result = 'reopened paused'; break;
        default: throw new Error(`Unknown scheduler host operation ${op.op}`);
      }
      if (op.as) aliases[op.as] = result;
      observations.push({ label: op.label ?? op.as ?? `${index}:${op.op}`, value: copy(result) });
    }
    await flush();
    return { observations, events: copy(events), taskPublications: copy(taskPublications), reports: copy(reports) };
  } finally {
    for (const held of gates.values()) held.release();
    await bounded(harness.close(context), 'final close');
    rmSync(temporary, { recursive: true, force: true });
  }
}
