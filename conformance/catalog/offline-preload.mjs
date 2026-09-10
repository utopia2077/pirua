import { readFileSync, writeFileSync } from 'node:fs';

const config = JSON.parse(readFileSync(process.env.PI_CATALOG_CONFIG, 'utf8'));
const calls = [];
const bodies = new Map(config.feeds.map(feed => [feed.url, readFileSync(feed.path, 'utf8')]));
globalThis.fetch = async (input, options) => {
  const url = String(input);
  if (!bodies.has(url) || options !== undefined) throw new Error('Undeclared catalog request');
  calls.push(url);
  return { ok: true, status: 200, json: async () => JSON.parse(bodies.get(url)) };
};
const NativeDate = Date;
globalThis.Date = class extends NativeDate {
  constructor(...args) { super(...(args.length ? args : [config.clock])); }
  static now() { return config.clock; }
};
process.on('exit', code => writeFileSync(config.trace, JSON.stringify({
  code, calls, clock: config.clock, node: process.version,
  icu: process.versions.icu, locale: Intl.DateTimeFormat().resolvedOptions().locale,
}, null, 2) + '\n'));
