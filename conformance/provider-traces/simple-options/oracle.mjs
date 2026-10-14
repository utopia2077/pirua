import { execFileSync } from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import { stripTypeScriptTypes } from 'node:module';
import { readFileSync } from 'node:fs';
import { deepStrictEqual } from 'node:assert';
import { createHash } from 'node:crypto';
const commit = '853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const path = 'packages/ai/src/api/simple-options.ts';
const source = execFileSync('git',['show',`${commit}:${path}`],{cwd:upstreamRoot,encoding:'utf8'});
const body = source.slice(source.indexOf('export const MIN_ANSWER_TOKENS'));
const adjust = new Function(stripTypeScriptTypes(body).replaceAll('export ','')+'\nreturn adjustMaxTokensForThinking;')();
const cases = [
  {base:null,ceiling:1000,level:'high'}, {base:0,ceiling:20000,level:'high'},
  {base:1,ceiling:20000,level:'high'}, {base:5000,ceiling:20000,level:'high'},
  {base:9007199254740991,ceiling:9007199254740991,level:'max',custom:{high:9007199254740991}},
  {base:null,ceiling:0,level:'minimal'}, {base:null,ceiling:1024,level:'low'},
  {base:null,ceiling:1025,level:'medium'}, {base:null,ceiling:8193,level:'medium'},
  {base:0,ceiling:100,level:'max',custom:{high:0}},
].map(value=>({...value,expected:adjust(value.base??undefined,value.ceiling,value.level,value.custom)}));
const result={sourceCommit:commit,sourcePath:path,sourceSha256:createHash('sha256').update(source).digest('hex'),evidenceClass:'executed-pinned-functions',cases};
if(process.argv.includes('--check')) deepStrictEqual(result,JSON.parse(readFileSync(new URL('./vectors.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
