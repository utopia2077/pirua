import { execFileSync } from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import { stripTypeScriptTypes, createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { deepStrictEqual } from 'node:assert';
import { createHash } from 'node:crypto';
const upstream=upstreamRoot;
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(`${upstream}/package.json`);
const OpenAI=require('openai').default;
deepStrictEqual(JSON.parse(readFileSync(`${upstream}/node_modules/openai/package.json`,'utf8')).version,'6.40.0');
// Invoke in a clean child environment; never discover host provider configuration.
if (Object.keys(process.env).some(key => /^(OPENAI_|PI_CACHE_RETENTION)/.test(key))) throw new Error('Run with env -i');
globalThis.fetch=()=>{throw new Error('live networking forbidden');};
const hashes={};
const deps={ OpenAI, partialParse:require('partial-json').parse };
function source(path) {
  const content=execFileSync('git',['show',`${commit}:packages/ai/src/${path}`],{cwd:upstream,encoding:'utf8'});
  hashes[path]=createHash('sha256').update(content).digest('hex'); return content;
}
function load(path, slice) {
  const full=source(path), text=slice?slice(full):full;
  const names=[...text.matchAll(/export (?:async )?(?:function|const|class) (\w+)/g)].map(v=>v[1]);
  const js=stripTypeScriptTypes(text).replace(/^import[\s\S]*?;\s*$/gm,'').replace(/\bexport /g,'');
  Object.assign(deps,new Function(...Object.keys(deps),js+`\nreturn {${names.join(',')}};`)(...Object.values(deps)));
}
load('models.ts',s=>s.slice(s.indexOf('export function calculateCost'),s.indexOf('export function modelsAreEqual')));
for(const path of ['utils/hash.ts','utils/json-parse.ts','utils/sanitize-unicode.ts','api/constrained-sampling.ts','api/transform-messages.ts','utils/deferred-tools.ts','utils/estimate.ts','api/simple-options.ts','api/github-copilot-headers.ts','api/openai-prompt-cache.ts','utils/provider-env.ts','utils/error-body.ts','utils/headers.ts','utils/provider-retry.ts','utils/overflow.ts','utils/retry.ts']) load(path);
deps.getPiUserAgent=()=> 'pi-ai-rust';
deps.Date=class extends Date { static now(){return 7;} };
load('utils/event-stream.ts');
load('api/cloudflare-gateway-binding.ts');
load('api/openai-completions.ts');
const base='https://gateway.ai.cloudflare.com/v1/account/gateway';
const model={id:'gpt-4.1',name:'Synthetic',api:'openai-completions',provider:'openai',baseUrl:base+'/openai',reasoning:false,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:4096,maxTokens:512};
const context={messages:[{role:'user',content:'hello',timestamp:7}]};
const sse='data: '+JSON.stringify({id:'synthetic',model:'gpt-4.1',choices:[{delta:{content:'done'},finish_reason:'stop'}],usage:{prompt_tokens:2,completion_tokens:1,total_tokens:3}})+'\n\ndata: [DONE]\n\n';
const cases=[];
const normalize=entries=>entries.filter(([key])=>!key.startsWith('x-stainless-')||['x-stainless-timeout','x-stainless-retry-count'].includes(key));
for(const spec of [{id:'forward',headers:{'x-alias':'lower','X-Alias':'upper'}},{id:'reverse',headers:{'X-Alias':'upper','x-alias':'lower'}},{id:'literal',headers:{'x-alias':'lower, upper'}},{id:'outside',headers:{},rejected:true}]) {
 const calls=[],forms=[],events=[];const controller=new AbortController();
 const push=deps.AssistantMessageEventStream.prototype.push;
 deps.AssistantMessageEventStream.prototype.push=function(event){events.push(JSON.parse(JSON.stringify(event)));return push.call(this,event);};
 const binding={gateway(id){calls.push({stage:'gateway',id});return {async run(data,options){if(!options.signal)throw new Error('missing signal');calls.push({stage:'run',data:{...data,headers:normalize(Object.entries(data.headers))}});return new Response(sse,{headers:{'content-type':'text/event-stream','content-encoding':'gzip'}});}};}};
 const custom=deps.createGatewayBindingFetch({binding,baseUrl:base,gateway:'gateway'});
 const headers={'cf-aig-authorization':'Bearer cloudflare-gateway-binding',Authorization:null,'x-api-key':null,...spec.headers};
 const target=spec.rejected?{...model,baseUrl:'https://outside.invalid/openai'}:model;
 const stream=deps.stream(target,context,{headers,signal:controller.signal,maxRetries:0,fetch:(url,init)=>{forms.push({headerKind:init.headers instanceof Headers?'Headers':'record',bodyKind:typeof init.body});return custom(url,init);}});
 const result=await stream.result();
 deps.AssistantMessageEventStream.prototype.push=push;
 deepStrictEqual(result.stopReason,spec.rejected?'error':'stop');deepStrictEqual(calls.length,spec.rejected?0:2);deepStrictEqual(forms,[{headerKind:'Headers',bodyKind:'string'}]);
 if(!spec.rejected)for(const name of ['authorization','x-api-key','cf-aig-authorization'])deepStrictEqual(calls[1].data.headers.some(([key])=>key===name),false);
 cases.push({...spec,model:target,context,headers,forms,calls,events,result,sse,retryable:deps.isRetryableAssistantError(result)});
}
const output={sourceCommit:commit,node:process.versions.node,sdkVersion:'6.40.0',sourceHashes:hashes,normalizations:['clock=7','pi user agent=pi-ai-rust','SDK platform header fields omitted after actual binding observation'],cases};
if(process.argv.includes('--check'))deepStrictEqual(JSON.parse(JSON.stringify(output)),JSON.parse(readFileSync(new URL('./openai.json',import.meta.url),'utf8')));else console.log(JSON.stringify(output,null,2));
