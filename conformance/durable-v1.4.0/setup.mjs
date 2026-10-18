import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { readFileSync, existsSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const directory = dirname(fileURLToPath(import.meta.url));
export const target = '4a080252f06caf63a525d7873c5d94d3c23887e0';
export const sha256 = bytes => createHash('sha256').update(bytes).digest('hex');
export function verifyEnvironment({ pinPath = resolve(directory, 'source-pin.json') } = {}) {
  const pin = JSON.parse(readFileSync(pinPath, 'utf8'));
  assert.equal(pin.upstreamCommit, target, 'Unexpected Durable source target');
  assert.equal(sha256(readFileSync(resolve(directory, 'package-lock.json'))), pin.packageLockSha256, 'Oracle dependency lock differs');
  for (const [path, expected] of Object.entries(pin.releaseFiles)) {
    assert.equal(sha256(readFileSync(resolve(directory, 'node_modules', path))), expected, `Installed release source differs: ${path}`);
  }
  for (const [name, artifact] of Object.entries(pin.packages)) {
    const manifest = JSON.parse(readFileSync(resolve(directory, 'node_modules', name, 'package.json'), 'utf8'));
    assert.equal(manifest.version, artifact.version, `Installed package version differs: ${name}`);
    const statement = JSON.parse(Buffer.from(JSON.parse(readFileSync(resolve(directory, artifact.attestation), 'utf8')).attestations.find(a => a.predicateType === 'https://slsa.dev/provenance/v1').bundle.dsseEnvelope.payload, 'base64'));
    assert.equal(statement.predicate.buildDefinition.resolvedDependencies[0].digest.gitCommit, target, 'Attested package commit differs');
    assert.equal(Buffer.from(artifact.integrity.slice(7), 'base64').toString('hex'), statement.subject[0].digest.sha512, 'Attested package integrity differs');
    assert.equal(sha256(readFileSync(resolve(directory, artifact.attestation))), artifact.attestationSha256, 'Attestation bytes differ');
  }
  // Installed release files always checked; tagged Git objects are checked when provided.
  // This never imports the checkout's mutable working tree.
  const root = process.env.PIRUA_UPSTREAM_ROOT;
  if (root) {
    for (const [path, expected] of Object.entries(pin.sources)) {
      assert.equal(sha256(execFileSync('git', ['-C', root, 'show', `${target}:${path}`])), expected, `Tagged source differs: ${path}`);
    }
  }
  return pin;
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  assert.ok(existsSync(resolve(directory, 'package.json')), 'Suite package manifest missing');
  if (process.argv.includes('--install')) {
    execFileSync('npm', ['--prefix', directory, 'ci', '--ignore-scripts', '--no-audit', '--no-fund'], { cwd: directory, stdio: 'inherit' });
  }
  verifyEnvironment();
  console.log('PASS Durable 1.4.0 release files, lock, attested target, and optional tagged Git sources');
}
