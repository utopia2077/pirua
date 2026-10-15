// Execute bounded, unchanged functions extracted from the pinned upstream source.
// The caller supplies a checkout with TypeScript installed; no providers are loaded.
import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createRequire } from 'node:module';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { readFileSync, writeFileSync } from 'node:fs';

const revision = '4a080252f06caf63a525d7873c5d94d3c23887e0';
const upstream = process.argv[2] ?? process.env.PIRUA_UPSTREAM_ROOT;
if (!upstream) throw new Error('Pass an upstream checkout or set PIRUA_UPSTREAM_ROOT');
const require = createRequire(resolve(upstream, 'package.json'));
const ts = require('typescript');
const source = (path) => execFileSync('git', ['-C', upstream, 'show', `${revision}:${path}`], { encoding: 'utf8' });
const inputs = {
  'packages/coding-agent/src/core/session-manager.ts': [
    'buildEntryIndex', 'buildSessionPath', 'getSessionContextSettings',
    'sessionEntryToContextMessages', 'buildContextEntries', 'projectContextEntry', 'buildSessionProjection',
  ],
  'packages/coding-agent/src/core/messages.ts': [
    'createBranchSummaryMessage', 'createCompactionSummaryMessage', 'createCustomMessage',
  ],
};
const functions = [];
const hashes = {};
for (const [path, names] of Object.entries(inputs)) {
  const text = source(path);
  hashes[path] = createHash('sha256').update(text).digest('hex');
  const parsed = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  for (const name of names) {
    const node = parsed.statements.find((node) => ts.isFunctionDeclaration(node) && node.name?.text === name);
    if (!node) throw new Error(`Missing upstream function ${name}`);
    functions.push(node.getText(parsed));
  }
}
const emitted = ts.transpileModule(functions.join('\n'), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ESNext },
}).outputText;
const oracle = await import(`data:text/javascript;base64,${Buffer.from(emitted).toString('base64')}`);
const base = fileURLToPath(new URL('.', import.meta.url));
const cases = JSON.parse(readFileSync(resolve(base, 'cases.json'), 'utf8'));
const results = cases.map(({ name, snapshot }) => {
  const entries = snapshot.entries.map((entry) => ({ ...entry, timestamp: new Date(entry.timestamp).toISOString() }));
  const projection = oracle.buildSessionProjection(entries, snapshot.leafId);
  for (const entry of projection.entries) {
    entry.sourceEntry = { ...entry.sourceEntry, timestamp: new Date(entry.sourceEntry.timestamp).getTime() };
  }
  return { name, snapshot, expected: projection };
});
const output = JSON.stringify({ upstream: revision, sources: hashes, cases: results }, null, 2) + '\n';
const destination = resolve(base, 'trace.json');
if (process.argv.includes('--check')) {
  if (readFileSync(destination, 'utf8') !== output) throw new Error('Coding session fixture differs from target source');
} else {
  writeFileSync(destination, output);
}
console.log(`coding session oracle: ${results.length} cases, ${Object.keys(hashes).length} pinned source files`);
