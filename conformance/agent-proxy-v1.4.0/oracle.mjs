import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync, mkdirSync, existsSync, symlinkSync, realpathSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const directory = dirname(fileURLToPath(import.meta.url));
const pin = JSON.parse(readFileSync(resolve(directory, 'source-pin.json'), 'utf8'));
const upstream = process.env.PIRUA_UPSTREAM_ROOT;
if (!upstream) throw new Error('Set PIRUA_UPSTREAM_ROOT to a pi-mono Git checkout containing v1.4.0');
const hash = (bytes) => createHash('sha256').update(bytes).digest('hex');
mkdirSync(resolve(directory, '.runtime'), { recursive: true });
writeFileSync(resolve(directory, '.runtime/package.json'), '{"type":"module"}\n');
for (const [path, expected] of Object.entries(pin.sources)) {
  const bytes = execFileSync('git', ['-C', upstream, 'show', `${pin.upstreamCommit}:${path}`]);
  assert.equal(hash(bytes), expected, `Target source digest differs: ${path}`);
  if (path.endsWith('/src/proxy.ts')) writeFileSync(resolve(directory, '.runtime/proxy.ts'), bytes);
}
const dependencies = resolve(process.env.PIRUA_AGENT_ORACLE_DEPENDENCIES ?? resolve(directory, '../agent-v1.4.0/node_modules'));
assert.equal(JSON.parse(readFileSync(resolve(dependencies, '@earendil-works/pi-ai/package.json'), 'utf8')).version, '1.4.0');
for (const [path, expected] of Object.entries(pin.releaseHelpers)) {
  assert.equal(hash(readFileSync(resolve(dependencies, '@earendil-works/pi-ai/dist', path))), expected);
}
const link = resolve(directory, '.runtime/node_modules');
if (!existsSync(link)) symlinkSync(dependencies, link, 'dir');
assert.equal(realpathSync(link), realpathSync(dependencies), 'Ignored dependency link points to a different installation');
const { streamProxy } = await import(pathToFileURL(resolve(directory, '.runtime/proxy.ts')).href);
Date.now = () => 7;
const model = { id: 'test-model', name: 'Test model', api: 'openai-responses', provider: 'openai', baseUrl: 'https://provider.invalid/v1',
  reasoning: true, input: ['text'], cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 }, contextWindow: 10000, maxTokens: 1000 };
const context = { messages: [] };
const usage = { input: 5, output: 7, cacheRead: 1, cacheWrite: 2, totalTokens: 15, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
const done = { type: 'done', reason: 'stop', usage, providerThinkingLevel: 'high' };
const content = [
  { type: 'start' }, { type: 'text_start', contentIndex: 0 }, { type: 'text_delta', contentIndex: 0, delta: '你好🙂' }, { type: 'text_end', contentIndex: 0, contentSignature: 'text-sig' },
  { type: 'thinking_start', contentIndex: 1 }, { type: 'thinking_delta', contentIndex: 1, delta: 'consider' }, { type: 'thinking_end', contentIndex: 1, contentSignature: 'thinking-sig' },
  { type: 'toolcall_start', contentIndex: 2, id: 'old-id', toolName: 'lookup' }, { type: 'toolcall_delta', contentIndex: 2, delta: '{"value":"你' }, { type: 'toolcall_delta', contentIndex: 2, delta: '好"}' },
  { type: 'toolcall_end', contentIndex: 2, toolCall: { type: 'toolCall', id: 'final-id', name: 'lookup-final', arguments: { value: '你好' }, namespace: 'dynamic_tools', thoughtSignature: 'tool-sig' } },
  { type: 'done', reason: 'toolUse', usage, providerThinkingLevel: 'xhigh' },
];
const wire = (events, trailing = true) => events.map((event) => `data: ${JSON.stringify(event)}\n\n`).join('').replace(trailing ? /$^/ : /\n+$/, '');
const cases = [
  { id: 'fragmented-content', wire: wire(content, false), fragmented: true },
  { id: 'body-allowlist', wire: wire([done], false), options: { temperature: 0.5, samplingParams: { seed: 9 }, maxTokens: 22, reasoning: 'high', cacheRetention: 'long', sessionId: 'session', headers: { 'X-Explicit': 'yes', remove: null }, metadata: { task: 'test' }, transport: 'sse', thinkingBudgets: { high: 19 }, maxRetryDelayMs: 71, identity: { attribution: { name: 'secret-app', url: 'https://private.invalid' } }, apiKey: 'provider-secret', timeoutMs: 33, maxRetries: 4, websocketConnectTimeoutMs: 44, toolChoice: 'none', deferred: true } },
  { id: 'line-framing', wire: `\ufeff: comment\ndata:{ignored}\ndata: \n\nevent: ignored\ndata: {"type":"future_event"}\ndata: {"type":"start"}\r\ndata: ${JSON.stringify(done)}` },
  { id: 'eof-partial', wire: wire([{ type: 'start' }, { type: 'text_start', contentIndex: 0 }, { type: 'text_delta', contentIndex: 0, delta: 'partial' }]) },
  { id: 'reported-error', wire: wire([{ type: 'text_start', contentIndex: 0 }, { type: 'text_delta', contentIndex: 0, delta: 'partial' }, { type: 'error', reason: 'aborted', errorMessage: 'server cancelled', usage, providerThinkingLevel: 'native-effort' }], false) },
  { id: 'wrong-content', wire: wire([{ type: 'text_delta', contentIndex: 0, delta: 'x' }]) },
  { id: 'http-error', wire: '{"error":"rate limited"}', status: 502, statusText: 'Bad Gateway' },
  { id: 'http-status-fallback', wire: 'not JSON', status: 502, statusText: 'Bad Gateway' },
];
const normalize = (value) => JSON.parse(JSON.stringify(value, (key, item) => key === 'durationMs' ? '<durationMs>' : item));
const observations = [];
for (const input of cases) {
  let request;
  globalThis.fetch = async (url, options) => {
    request = { url, method: options.method, headers: Object.fromEntries(Object.entries(options.headers).map(([name, value]) => [name.toLowerCase(), value])), body: JSON.parse(options.body) };
    const bytes = Buffer.from(input.wire);
    const chunks = input.fragmented ? [...bytes].map((byte) => Uint8Array.of(byte)) : [bytes];
    const body = new ReadableStream({ pull(controller) { const chunk = chunks.shift(); if (chunk) controller.enqueue(chunk); else controller.close(); } });
    return new Response(body, { status: input.status ?? 200, statusText: input.statusText ?? 'OK' });
  };
  const stream = streamProxy(model, context, { ...input.options, authToken: 'proxy-secret', proxyUrl: 'https://proxy.invalid/base' });
  const events = [];
  for await (const event of stream) {
    // Native events own snapshots; the source events alias mutable partials.
    // This oracle records invariant event payloads plus the full final message.
    const { partial, message, error, ...payload } = event;
    events.push(normalize(payload));
  }
  observations.push({ id: input.id, input, request, events, result: normalize(await stream.result()) });
}
const output = `${JSON.stringify({ formatVersion: 1, observations }, null, 2)}\n`;
const path = resolve(directory, 'observations.json');
if (process.argv.includes('--write')) { writeFileSync(path, output); console.log(`Wrote ${observations.length} target-source proxy cases`); }
else if (process.argv.includes('--check')) { assert.deepEqual(JSON.parse(output), JSON.parse(readFileSync(path, 'utf8'))); console.log(`PASS ${observations.length} target-source proxy cases`); }
else throw new Error('Use --write or --check');
