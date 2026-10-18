import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { resolve } from 'node:path';
import { createSession, MemoryStorage, defineDoc, defineDocFamily } from '@earendil-works/pi-durable';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { apply, applyImmutable } from '@earendil-works/chord/delta';
import { directory } from './setup.mjs';
import { encodeTraceValue } from './trace-codec.mjs';

export const detached = value => value === undefined ? { $undefined: true } : encodeTraceValue(JSON.parse(JSON.stringify(value)));
const clone = value => structuredClone(value);
const tick = () => new Promise(done => setImmediate(done));
export async function flush() { for (let i = 0; i < 4; i++) await tick(); }
const errorValue = error => ({ error: { category: error.name, message: error.message } });
function lookup(ref, aliases) {
  const [name, ...path] = ref.slice(1).split('.');
  let value = aliases[name];
  assert.ok(Object.hasOwn(aliases, name), `Unknown trace reference ${ref}`);
  for (const key of path) value = value[key];
  return value;
}
function materialize(value, aliases) {
  if (typeof value === 'string' && value.startsWith('$')) return clone(lookup(value, aliases));
  if (Array.isArray(value)) return value.map(v => materialize(v, aliases));
  if (value && typeof value === 'object' && value.$undefined === true && Object.keys(value).length === 1) return undefined;
  if (value && typeof value === 'object') return Object.fromEntries(Object.entries(value).map(([k,v]) => [k, materialize(v, aliases)]));
  return value;
}
function editDraft(draft, edits) {
  for (const [verb, path, ...args] of edits) {
    let parent = draft;
    for (const key of path.slice(0, -1)) parent = parent[key];
    const key = path.at(-1);
    const value = path.length ? parent[key] : draft;
    switch (verb) {
      case 'set': parent[key] = clone(args[0]); break;
      case 'delete': delete parent[key]; break;
      case 'append': parent[key] += args[0]; break;
      case 'trim': parent[key] = value.slice(args[0]); break;
      case 'splice': value.splice(args[0], args[1], ...clone(args[2])); break;
      case 'reverse': value.reverse(); break;
      default: throw new Error(`Unsupported trace edit ${verb}`);
    }
  }
}
async function makeStorage(backend, temporary) {
  return backend === 'memory' ? new MemoryStorage() : openNodeSqliteStorage(resolve(temporary, 'store.sqlite'));
}
export async function executeCase(test, backend) {
  if (test.family === 'delta') {
    const previous = clone(test.input.value);
    try {
      const immutable = applyImmutable(previous, clone(test.input.ops));
      const mutable = apply(clone(test.input.value), clone(test.input.ops));
      return { observations: [{ label: 'apply', value: detached(immutable) }, { label: 'mutable', value: detached(mutable) }, { label: 'previous', value: detached(previous) }] };
    } catch (error) { return { observations: [{ label: 'apply', value: errorValue(error) }, { label: 'previous', value: detached(previous) }] }; }
  }
  mkdirSync(resolve(directory, '.runtime'), { recursive: true });
  const temporary = mkdtempSync(resolve(directory, '.runtime', 'session-'));
  let storage = await makeStorage(backend, temporary);
  const aliases = {};
  const observations = [];
  const publications = [];
  const writes = [];
  const checkpointCalls = [];
  const definitions = new Map();
  const watches = new Map();
  const states = new Map();
  let session;
  const observe = (label, value) => observations.push({ label, value: detached(value) });
  const argsFor = (op, includeSeed = false) => {
    const definition = definitions.get(op.doc);
    assert.ok(definition, `Unknown document ${op.doc}`);
    const args = [definition];
    if (definition.definition.scope !== 'session') args.push(op.owner);
    if (definition.definition.family) { args.push(op.key); if (includeSeed) args.push(op.seed); }
    return args;
  };
  const wrap = target => new Proxy(target, { get(t, property) {
    if (property === 'commit') return async (batch, c) => { writes.push(detached(batch)); return t.commit(batch, c); };
    const value = Reflect.get(t, property); return typeof value === 'function' ? value.bind(t) : value;
  }});
  function openSession() {
    session = createSession(wrap(storage), { now: () => 1234567890000 });
    session.subscribeCommits(publication => publications.push(publication));
  }
  for (const descriptor of test.input.definitions ?? []) {
    const { name, initial, checkpoint = 'never', family, ...metadata } = descriptor;
    const definition = { ...metadata, ...(family ? { family: true } : {}), initial: seed => clone(family && descriptor.seedField ? { ...initial, [descriptor.seedField]: seed } : initial), checkpointWhen(value, ops, info) {
      checkpointCalls.push({ doc: name, value: detached(value), ops: detached(ops), info: detached(info) });
      if (checkpoint === 'throw') throw new Error('oracle checkpoint policy failure');
      return checkpoint === 'always' || typeof checkpoint === 'number' && info.deltasSinceBase >= checkpoint;
    }};
    definitions.set(name, (family ? defineDocFamily : defineDoc)(definition));
  }
  if (test.family !== 'storage') openSession();
  async function txStep(tx, raw) {
    const step = materialize(raw, aliases);
    let result;
    switch (step.op) {
      case 'createConversation': result = await tx.createConversation({ ownership: step.ownership ?? { kind: 'ownerless' } }); break;
      case 'appendEntry': result = await tx.appendEntry(step.conversationId, step.value); break;
      case 'forkConversation': result = await tx.forkConversation(step.conversationId, step.at, { ownership: step.ownership ?? { kind: 'ownerless' } }); break;
      case 'createSubmission': result = await tx.createSubmission(step.value); break;
      case 'placeSubmission': result = tx.placeSubmission(step.id, step.entry); break;
      case 'settleSubmission': result = tx.settleSubmission(step.id, step.settlement); break;
      case 'doc': {
        const draft = await tx.doc(...argsFor(step, true));
        if (step.edits) editDraft(draft, step.edits);
        result = detached(draft);
        break;
      }
      case 'docSame': result = await tx.doc(...argsFor(step, true)) === await tx.doc(...argsFor(step, true)); break;
      case 'retireDoc': result = await tx.retireDoc(...argsFor(step)); break;
      case 'read': result = await tx[step.method](...(step.args ?? [])); break;
      case 'throw': throw new Error(step.message);
      default: throw new Error(`Unknown transaction operation ${step.op}`);
    }
    if (step.as) aliases[step.as] = result;
    return result;
  }
  try {
    for (let index = 0; index < test.input.operations.length; index++) {
      const raw = test.input.operations[index];
      const op = raw.op === 'transaction' ? { ...raw, steps: undefined } : materialize(raw, aliases);
      let result;
      try {
        switch (op.op) {
          case 'commit': result = await storage.commit(op.writes, context); break;
          case 'mint': result = []; for (let i=0; i<(op.count??1); i++) result.push(await storage.mintId()); break;
          case 'read': result = await storage[op.method](...(op.args ?? []), context); break;
          case 'transaction': {
            const values = [];
            try {
              await session.commit(async tx => { for (const step of raw.steps) values.push(detached(await txStep(tx, step))); }, context);
              result = { steps: values };
            } catch (error) { result = { steps: values, ...errorValue(error) }; }
            break;
          }
          case 'snapshot': result = await session.snapshot(...argsFor(op), context); break;
          case 'snapshotAsOf': result = await session.snapshotAsOf(...argsFor(op), op.at, context); break;
          case 'sessionRead': result = await session[op.method](...(op.args ?? []), context); break;
          case 'watch': {
            const handle = await session.watchDoc(...argsFor(op), context);
            result = handle?.value;
            if (handle) watches.set(op.name, { handle, frames: [], gate: undefined });
            break;
          }
          case 'watchStart': {
            const watch = watches.get(op.name);
            if (op.holdFirst) {
              let release;
              watch.gate = { promise: new Promise(done => { release = done; }), release: () => release() };
            }
            watch.handle.start(async (value, ops) => {
              watch.frames.push({ value: detached(value), ops: detached(ops) });
              if (watch.gate && watch.frames.length === 1) await watch.gate.promise;
            });
            await flush();
            result = watch.frames;
            break;
          }
          case 'watchRelease': watches.get(op.name).gate?.release(); await flush(); result = watches.get(op.name).frames; break;
          case 'watchFrames': await flush(); result = watches.get(op.name).frames; break;
          case 'watchStop': result = await watches.get(op.name).handle.stop(); break;
          case 'watchClosed': result = await watches.get(op.name).handle.closed; break;
          case 'watchValue': result = watches.get(op.name).handle.value; break;
          case 'state': {
            const state = await session.documentState(...argsFor(op), context);
            result = state?.value;
            if (state) {
              const frames = [];
              state.subscribe((value, _context, delivery) => frames.push({ value: detached(value), sequence: delivery.sequence }));
              states.set(op.name, { state, frames });
              await flush();
            }
            break;
          }
          case 'stateFrames': await flush(); result = states.get(op.name).frames; break;
          case 'stateValue': await flush(); result = states.get(op.name).state.value; break;
          case 'repeatTransactions': {
            for (let i = 0; i < op.count; i++) {
              await session.commit(async tx => {
                const draft = await tx.doc(...argsFor(op, true));
                draft[op.field] = op.from + i;
              }, context);
            }
            result = op.count; break;
          }
          case 'reopen': {
            if (backend !== 'sqlite') { result = { skipped: 'published MemoryStorage has no reopen API' }; break; }
            if (session) await session.close(context); else await storage.close(context);
            storage = await makeStorage(backend, temporary);
            if (session) openSession();
            result = 'reopened'; break;
          }
          case 'close': if (session) await session.close(context); else await storage.close(context); result = 'closed'; break;
          case 'retained': result = aliases[op.name]; break;
          default: throw new Error(`Unknown trace operation ${op.op}`);
        }
      } catch (error) { result = errorValue(error); }
      if (op.as) aliases[op.as] = result;
      observe(op.label ?? op.as ?? `${index}:${op.op}`, result);
    }
    await flush();
    return { observations, ...(test.family === 'storage' ? {} : { publications: detached(publications), writes, checkpointCalls }) };
  } finally {
    for (const { handle, gate } of watches.values()) { gate?.release(); await handle.stop(); }
    for (const { state } of states.values()) state.dispose();
    if (session) await session.close(context); else await storage.close(context);
    rmSync(temporary, { recursive: true, force: true });
  }
}
