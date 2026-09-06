import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { stripTypeScriptTypes } from 'node:module';
import vm from 'node:vm';
import { ApiError, FinishReason, FunctionCallingConfigMode, ThinkingLevel } from 'upstream/pi-mono/node_modules/@google/genai/dist/node/index.mjs';

const revision = '853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const sources = {};
let exportedNames = [];
const delays = [];
class FixedDate extends Date { static now() { return 7; } }
const math = Object.create(Math); math.random = () => 0.4;
const sandbox = vm.createContext({ FinishReason, FunctionCallingConfigMode, structuredClone, Error, Headers,
  Date: FixedDate, Math: math, setTimeout: (fn, ms) => { delays.push(ms); queueMicrotask(fn); return 1; }, clearTimeout: () => {} });
for (const path of ['utils/sanitize-unicode.ts', 'utils/provider-retry.ts', 'api/transform-messages.ts', 'api/constrained-sampling.ts', 'api/google-shared.ts']) {
  const text = execFileSync('git', ['show', `${revision}:packages/ai/src/${path}`], { cwd: 'upstream/pi-mono', encoding: 'utf8' });
  sources[path] = createHash('sha256').update(text).digest('hex');
  if (path === 'api/google-shared.ts') exportedNames = [...text.matchAll(/^export (?:type|function) (\w+)/gm)].map(match => match[1]);
  vm.runInContext(stripTypeScriptTypes(text.replace(/^import[\s\S]*?;\n/gm, '').replace(/^export /gm, '')), sandbox, { filename: path });
}
const model = (id = 'gemini-3-pro-preview', input = ['text', 'image']) => ({ id, name: id, api: 'google-generative-ai', provider: 'google',
  baseUrl: 'https://fixture.invalid', reasoning: true, input, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 128000, maxTokens: 8192 });
const usage = { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
const assistant = (m, content, overrides = {}) => ({ role: 'assistant', api: m.api, provider: m.provider, model: m.id, content, usage, stopReason: 'toolUse', timestamp: 7, ...overrides });
const call = (id = 'call_1', extra = {}) => ({ type: 'toolCall', id, name: 'read', arguments: { path: 'file', n: 1.0 }, ...extra });
const result = (id = 'call_1', content = [{ type: 'text', text: 'ok' }], isError = false) => ({ role: 'toolResult', toolCallId: id, toolName: 'read', content, isError, timestamp: 7 });
const messages = [];
function messageCase(id, m, list) {
  const context = { systemPrompt: 'not part of shared content', messages: list };
  messages.push({ id, model: m, context, expected: sandbox.convertMessages(m, context) });
}
const signatures = [undefined, '', 'AAAA', 'AAA=', 'AA==', 'A===', '====', 'A', ' AAAA', 'AAAA\n', 'AA-_', 'éééé', '/+==', 'AB==',
  'AAA\n', 'AAA\r', 'AAA\u2028', 'AAA\u2029', 'AA\r\n'];
for (const [index, signature] of signatures.entries()) {
  const m = model(); const sig = signature === undefined ? {} : { textSignature: signature };
  messageCase(`signature-${index}`, m, [assistant(m, [{ type: 'text', text: '', ...sig },
    { type: 'thinking', thinking: '\ufeff ', ...(signature === undefined ? {} : { thinkingSignature: signature }) },
    call('call_1', signature === undefined ? {} : { thoughtSignature: signature })]), result()]);
}
for (const change of [{}, { model: 'different' }, { provider: 'different' }, { api: 'google-vertex' }]) {
  const m = model(); messageCase(`identity-${JSON.stringify(change)}`, m, [assistant(m, [
    { type: 'text', text: '', textSignature: 'AAAA' }, { type: 'thinking', thinking: '', thinkingSignature: 'AAAA' },
    { type: 'thinking', thinking: 'visible', thinkingSignature: 'AAAA' },
    { type: 'thinking', thinking: '', thinkingSignature: 'AAAA', redacted: true }, call('a|😀', { thoughtSignature: 'AAAA' })], change), result('a|😀')]);
}
for (const id of ['gemini-2.5-flash', 'gemini-3-pro', 'GEMINI-LIVE-03-preview', 'claude-x', 'CLAUDE-x', 'gpt-oss-120b', 'other']) {
  for (const vision of [true, false]) {
    const m = model(id, vision ? ['text', 'image'] : ['text']);
    messageCase(`images-${id}-${vision}`, m, [{ role: 'user', content: [{ type: 'image', mimeType: 'image/png', data: 'eA==' }], timestamp: 7 },
      assistant(m, [call('a'), call('img'), call('b')]), result('a'), result('img', [{ type: 'image', mimeType: 'image/png', data: 'eA==' }], true),
      result('b', [{ type: 'text', text: '' }, { type: 'text', text: 'last' }])]);
  }
}
for (const id of ['x|😀/' + 'a'.repeat(70), '😀'.repeat(40), 'ok']) {
  for (const same of [true, false]) {
    const m = model(); messageCase(`id-${id}-${same}`, m, [assistant(m, [call(id)], same ? {} : { model: 'foreign' }), result(id)]);
  }
}
const m = model();
messageCase('blank-user-unicode-and-orphans', m, [{ role: 'user', content: '', timestamp: 7 }, { role: 'user', content: [], timestamp: 7 },
  assistant(m, [{ type: 'text', text: '\u0085' }, { type: 'text', text: '\ufeff' }, { type: 'thinking', thinking: '😀' }, call()]),
  { role: 'user', content: 'interrupt', timestamp: 7 }, assistant(m, [{ type: 'text', text: 'failed' }], { stopReason: 'error', errorMessage: 'fixture' }),
  result('orphan', []), result('other', [], true)]);

const modelIds = ['gemini-2.5', 'gemini-3', 'GEMINI-LIVE-3.1', 'gemini-00000003x', 'gemini-000', 'gemini-0002', 'gemini-20',
  'gemini-' + '9'.repeat(400), 'gemini-live-2', 'gemini--3', 'xgemini-3', 'claude-', 'CLAUDE-x', 'gpt-oss-', 'GPT-OSS-x', 'Gemini-٣', 'gemini-3\n'];
const models = modelIds.map(id => ({ id, requiresId: sandbox.requiresToolCallId(id), strict: sandbox.supportsGoogleStrictToolSampling(id) }));
const thinking = [];
for (const level of ['off', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max']) {
  for (const mapped of [undefined, null, 'minimal', 'LOW', 'Medium', 'HIGH', '', ' high ', 'extreme']) {
    const m = model(); if (mapped !== undefined) m.thinkingLevelMap = { [level]: mapped };
    try { thinking.push({ model: m, level, expected: sandbox.resolveGoogleThinkingLevel(m, level) }); }
    catch { thinking.push({ model: m, level, error: true }); }
  }
}
const markers = [];
for (const thought of [undefined, false, true]) for (const thoughtSignature of [undefined, '', 'opaque']) {
  const part = { ...(thought === undefined ? {} : { thought }), ...(thoughtSignature === undefined ? {} : { thoughtSignature }) };
  markers.push({ part, expected: sandbox.isThinkingPart(part) });
}
const retention = [];
for (const existing of [undefined, '', 'old']) for (const incoming of [undefined, '', 'new', ' ']) {
  retention.push({ existing, incoming, expected: sandbox.retainThoughtSignature(existing, incoming) ?? null });
}
const tool = (parameters, constrainedSampling) => ({ name: 'fixture', description: 'description', parameters,
  ...(constrainedSampling === undefined ? {} : { constrainedSampling }) });
// Inputs use core canonical schema ordering before the source strict conversion.
function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key])]));
  return value;
}
const loose = tool(canonical(JSON.parse('{"type":"object","$schema":"draft","$id":"id","$anchor":"a","$dynamicAnchor":"d","$vocabulary":{},"$comment":"c","$defs":{},"definitions":{},"properties":{"x":{"$ref":"#/$defs/x","$id":"drop","type":"string"},"$id":{"type":"number"},"constructor":{"type":"boolean"},"__proto__":{"type":"integer"}},"anyOf":[{"$id":"keep","type":"string"}],"default":{"$comment":"drop","x":[{"$id":"keep","__proto__":{"array":"own"},"constructor":"array-constructor"}]},"__proto__":{"inherited":true}}')));
const strict = tool(canonical({ type: 'object', properties: { z: { type: 'string' }, '10': { type: 'number' }, '2': { type: 'boolean' }, a: { type: 'integer' } }, required: ['z'] }), { type: 'json_schema', strict: 'require' });
const invalid = tool({ type: 'object', $ref: 'unsupported' }, { type: 'json_schema', strict: 'require' });
const prefer = tool({ type: 'object', $ref: 'unsupported' }, { type: 'json_schema', strict: 'prefer' });
const tools = [];
for (const list of [[], [loose], [strict], [invalid], [prefer], [strict, invalid], [invalid, strict]]) {
  for (const useParameters of [false, true]) for (const supportsStrictMode of [false, true]) {
    const item = { tools: list, useParameters, supportsStrictMode };
    try { tools.push({ ...item, expected: sandbox.convertTools(list, useParameters, supportsStrictMode) ?? null }); }
    catch { tools.push({ ...item, error: true }); }
  }
}
const modes = [];
for (const list of [[], [loose], [strict], [invalid], [prefer], [strict, invalid], [invalid, strict]]) {
  for (const choice of [undefined, '', 'auto', 'none', 'any', 'unknown']) for (const supports of [false, true]) {
    const item = { tools: list, choice, supports };
    try { modes.push({ ...item, expected: sandbox.resolveGoogleFunctionCallingMode(list, choice, supports) ?? null }); }
    catch { modes.push({ ...item, error: true }); }
  }
}
const stops = [...Object.values(FinishReason), 'unknown', '', 'stop'].map(reason => {
  let known; try { known = sandbox.mapStopReason(reason); } catch { known = null; }
  return { reason, known, raw: sandbox.mapStopReasonString(reason) };
});
const choices = ['auto', 'none', 'any', '', 'AUTO', 'custom'].map(choice => ({ choice, expected: sandbox.mapToolChoice(choice) }));
const retries = [];
for (const spec of [
  { kind: 'api', status: 429, maxRetries: 1 }, { kind: 'api', status: 429 }, { kind: 'api', status: 400, maxRetries: 2 },
  ...[408, 409, 500, 503, 599, 499].map(status => ({ kind: 'api', status, maxRetries: 1 })),
  { kind: 'api', maxRetries: 1 }, { kind: 'other', status: 429, maxRetries: 2 },
  { kind: 'provider', status: 429, headers: [['x-should-retry', 'false']], maxRetries: 1 },
  { kind: 'provider', status: 400, headers: [['x-should-retry', 'true'], ['retry-after-ms', '27']], maxRetries: 1 },
  { kind: 'provider', status: 503, headers: [['retry-after', '90']], maxRetries: 1 },
  { kind: 'provider', status: 503, headers: [['retry-after-ms', '1234']], maxRetries: 1, maxRetryDelayMs: 0 },
  { kind: 'api', status: 429, maxRetries: 9007199254740991 },
]) {
  delays.length = 0; let calls = 0;
  const error = spec.kind === 'other' ? { status: spec.status, message: 'opaque' } : new ApiError({ status: spec.status, message: 'fixture failure' });
  if (spec.kind === 'provider') error.headers = new Headers(spec.headers);
  try {
    const value = await sandbox.retryGoogleRequest(async () => { if (++calls === 1) throw error; return 'ok'; }, spec);
    retries.push({ ...spec, calls, delays: [...delays], outcome: value });
  } catch (caught) {
    retries.push({ ...spec, calls, delays: [...delays], outcome: caught === error ? 'attempt' : 'policy', sameError: caught === error });
  }
}
console.log(JSON.stringify({ revision, node: process.version, sdk: '1.52.0', sources,
  exports: exportedNames,
  enums: { FinishReason, FunctionCallingConfigMode, ThinkingLevel }, models, thinking, markers, retention, messages, tools, modes, stops, choices, retries }));
