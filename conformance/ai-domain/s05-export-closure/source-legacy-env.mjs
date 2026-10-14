// Entry intake only: complete pinned modules, synthetic environment/import services.
import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {registerHooks,stripTypeScriptTypes} from 'node:module';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const paths=['packages/ai/src/env-api-keys.ts','packages/ai/src/utils/provider-env.ts'];
const source=Object.fromEntries(paths.map(p=>[p,execFileSync('/usr/bin/git',['-C',upstreamRoot,'show',`${pin}:${p}`],{encoding:'utf8',env:{PATH:'/usr/bin:/bin'}})]));
const hashes=Object.fromEntries(Object.entries(source).map(([p,s])=>[p,createHash('sha256').update(s).digest('hex')]));
const imports=[],hostCalls=[];
globalThis.envIntakeHost={imports,hostCalls};
registerHooks({resolve(s,c,next){
 if(s.startsWith('pinned:'))return{url:s,shortCircuit:true};
 if(c.parentURL?.startsWith('pinned:')){
  if(s.startsWith('.'))return{url:'pinned:'+new URL(s,'file:///'+c.parentURL.slice(7)).pathname.slice(1),shortCircuit:true};
  if(['node:fs','node:os','node:path'].includes(s))return{url:'synthetic:'+s,shortCircuit:true};
  throw Error('undeclared import');
 }
 return next(s,c);
},load(u,c,next){
 if(u.startsWith('pinned:')){const p=u.slice(7);assert(Object.hasOwn(source,p));return{format:'module',source:stripTypeScriptTypes(source[p]),shortCircuit:true};}
 if(u.startsWith('synthetic:')){const name=u.slice(10);imports.push(name);const symbol=({'node:fs':'existsSync','node:os':'homedir','node:path':'join'})[name];return{format:'module',source:`export function ${symbol}(){globalThis.envIntakeHost.hostCalls.push(${JSON.stringify(name)});throw Error('excluded discovery invoked');}`,shortCircuit:true};}
 return next(u,c);
}});
const actualProcess=process;globalThis.process={versions:{node:'synthetic'},env:{}};
const api=await import('pinned:packages/ai/src/env-api-keys.ts');
for(let i=0;i<10;i++)await Promise.resolve();
const selected={anthropic:'ANTHROPIC_API_KEY',deepseek:'DEEPSEEK_API_KEY',google:'GEMINI_API_KEY',openai:'OPENAI_API_KEY','openai-codex':null,openrouter:'OPENROUTER_API_KEY'};
const cases=[];
for(const[provider,key]of Object.entries(selected)){
 for(const mode of ['missing','ambient','scoped','empty-scoped-fallback','whitespace-scoped']){
  cases.push({id:provider+'-'+mode,provider,ambient:key&&mode!=='missing'?{[key]:'SYNTHETIC_AMBIENT'}:{},scoped:mode==='scoped'?{[key]:'SYNTHETIC_SCOPED'}:mode==='empty-scoped-fallback'?{[key]:''}:mode==='whitespace-scoped'?{[key]:' '}:{}});
 }
}
for(const[mode,ambient,scoped]of[
 ['bearer-only',{ANTHROPIC_AUTH_TOKEN:'SYNTHETIC_BEARER'},{}],
 ['all',{ANTHROPIC_AUTH_TOKEN:'SYNTHETIC_BEARER',ANTHROPIC_OAUTH_TOKEN:'SYNTHETIC_OAUTH',ANTHROPIC_API_KEY:'SYNTHETIC_KEY'},{}],
 ['scoped-oauth',{ANTHROPIC_AUTH_TOKEN:'SYNTHETIC_BEARER',ANTHROPIC_OAUTH_TOKEN:'SYNTHETIC_OAUTH',ANTHROPIC_API_KEY:'SYNTHETIC_KEY'},{ANTHROPIC_OAUTH_TOKEN:'SYNTHETIC_SCOPED'}],
 ['empty-oauth-fallback',{ANTHROPIC_OAUTH_TOKEN:'SYNTHETIC_OAUTH',ANTHROPIC_API_KEY:'SYNTHETIC_KEY'},{ANTHROPIC_OAUTH_TOKEN:''}],
 ['empty-all',{ANTHROPIC_AUTH_TOKEN:'',ANTHROPIC_OAUTH_TOKEN:'',ANTHROPIC_API_KEY:''},{}],
 ['whitespace-bearer',{ANTHROPIC_AUTH_TOKEN:' ',ANTHROPIC_API_KEY:'SYNTHETIC_KEY'},{}]
])cases.push({id:'anthropic-'+mode,provider:'anthropic',ambient,scoped});
cases.push({id:'unknown',provider:'synthetic-unknown',ambient:{OPENAI_API_KEY:'SYNTHETIC_AMBIENT'},scoped:{}});
const values=['SYNTHETIC_AMBIENT','SYNTHETIC_SCOPED',' ','SYNTHETIC_OAUTH','SYNTHETIC_KEY'];
for(const c of cases){
 const ambientReads=[];globalThis.process.env=new Proxy(c.ambient,{get(t,k){ambientReads.push(k);return t[k];}});
 const keys=api.findEnvKeys(c.provider,c.scoped);const findReads=ambientReads.splice(0);
 const key=api.getEnvApiKey(c.provider,c.scoped);const keyIndex=key===undefined?null:values.indexOf(key);assert.notEqual(keyIndex,-1);
 c.observed={keys:keys??null,keyIndex,findReads,getReads:ambientReads};
}
assert.deepEqual(hostCalls,[]);assert.deepEqual([...imports].sort(),['node:fs','node:os','node:path']);
assert.equal(cases.find(c=>c.id==='anthropic-bearer-only').observed.keyIndex,null);
assert.equal(cases.find(c=>c.id==='anthropic-all').observed.keyIndex,3);
globalThis.process=actualProcess;
console.log(JSON.stringify({pin,hashes,imports,constants:[api.ANTHROPIC_AUTH_TOKEN_ENV,api.ANTHROPIC_OAUTH_TOKEN_ENV,api.ANTHROPIC_API_KEY_ENV],excludedDiscoveryCalls:hostCalls,values,cases},null,2));
