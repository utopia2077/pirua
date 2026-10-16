import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFileSync, writeFileSync, mkdirSync, copyFileSync, existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve, dirname } from 'node:path';

export const directory = dirname(fileURLToPath(import.meta.url));
export const target = '4a080252f06caf63a525d7873c5d94d3c23887e0';
const hash = (bytes) => createHash('sha256').update(bytes).digest('hex');
const root = process.env.PIRUA_UPSTREAM_ROOT;
if (!root) throw new Error('Set PIRUA_UPSTREAM_ROOT to a pi-mono Git checkout containing v1.4.0');
const pin = JSON.parse(readFileSync(resolve(directory, 'source-pin.json'), 'utf8'));
if (pin.upstreamCommit !== target) throw new Error('Unexpected source pin');
mkdirSync(resolve(directory, '.runtime'), { recursive: true });
for (const [path, expected] of Object.entries(pin.sources)) {
  const bytes = execFileSync('git', ['-C', root, 'show', `${target}:${path}`]);
  if (hash(bytes) !== expected) throw new Error(`Target source digest differs: ${path}`);
  if (path.startsWith('packages/agent/src/')) writeFileSync(resolve(directory, '.runtime', path.split('/').at(-1)), bytes);
}
const artifactRoot = process.env.PIRUA_AI_ARTIFACT_ROOT ?? resolve(directory, '..');
for (const [path, expected] of Object.entries(pin.artifacts)) {
  const source = resolve(artifactRoot, path);
  if (hash(readFileSync(source)) !== expected) throw new Error(`Release artifact digest differs: ${path}`);
  copyFileSync(source, resolve(directory, '.runtime', path.split('/').at(-1)));
}
if (process.argv.includes('--install')) {
  execFileSync('npm', ['ci', '--ignore-scripts', '--no-audit', '--no-fund'], { cwd: directory, stdio: 'inherit' });
}
for (const [path, expected] of Object.entries(pin.releaseHelpers)) {
  const installed = resolve(directory, 'node_modules/@earendil-works/pi-ai/dist', path);
  if (!existsSync(installed)) throw new Error('Install the oracle dependencies first: node setup.mjs --install');
  if (hash(readFileSync(installed)) !== expected) throw new Error(`Installed release helper differs: ${path}`);
}
for (const [name, expected] of Object.entries(pin.dependencyVersions)) {
  const manifest = JSON.parse(readFileSync(resolve(directory, 'node_modules', name, 'package.json'), 'utf8'));
  if (manifest.version !== expected) throw new Error(`Installed dependency version differs: ${name}`);
}
