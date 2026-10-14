import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {createRequire,stripTypeScriptTypes} from 'node:module';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {deepStrictEqual} from 'node:assert';
import vm from 'node:vm';
const upstream=upstreamRoot,commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(`${upstream}/package.json`);
const OpenAI=require('openai').default,Anthropic=require('@anthropic-ai/sdk').default;
deepStrictEqual(JSON.parse(readFileSync(`${upstream}/node_modules/openai/package.json`,'utf8')).version,'6.40.0');
deepStrictEqual(JSON.parse(readFileSync(`${upstream}/node_modules/@anthropic-ai/sdk/package.json`,'utf8')).version,'0.91.1');
if(Object.keys(process.env).some(key=>/^(OPENAI_|ANTHROPIC_|PI_CACHE_RETENTION)/.test(key)))throw new Error('use sanitized environment');
const hashes={};
const source=path=>{const s=execFileSync('git',['show',`${commit}:packages/ai/src/${path}`],{cwd:upstream,encoding:'utf8'});hashes[path]=createHash('sha256').update(s).digest('hex');return s;};
const clean=s=>stripTypeScriptTypes(s.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const raw=entries=>'{'+entries.map(([k,v])=>JSON.stringify(k)+':'+JSON.stringify(v)).join(',')+'}';
const permutations=values=>values.length?values.flatMap((value,index)=>permutations(values.filter((_,i)=>i!==index)).map(tail=>[value,...tail])):[[]];
const inputs=[];
for(const nullable of [false,true])for(const entries of permutations([['x-order','lower'],['X-Order','upper'],['X-ORDER',nullable?null:'caps']]))inputs.push({raw:raw(entries)});
for(const entries of [
 [['x-order','lower'],['X-Order',null]], [['X-Order',null],['x-order','lower']],
 [['Accept',null],['accept','lower']], [['accept','lower'],['Accept',null]],
 [['accept',null],['Accept','upper']], [['Accept','upper'],['accept',null]],
 [['10','ten'],['2','two'],['01','one'],['0','zero'],['x-order','lower'],['X-Order','upper'],['4294967295','not-index'],['4294967294','index']],
])inputs.push({raw:raw(entries)});
const cases=[];
for(const family of ['pi-messages','mistral-conversations','anthropic-messages','openai-responses','openai-completions']){
 const context=vm.createContext({OpenAI,Anthropic,Headers,Response,ReadableStream,TextDecoder,TextEncoder,URL,AbortSignal,AbortController,process,setTimeout,clearTimeout,structuredClone,
  Date:class extends Date{static now(){return 7;}},getPiUserAgent:()=> 'pi-ai-rust',partialParse:require('partial-json').parse});
 for(const path of ['utils/hash.ts','utils/json-parse.ts','utils/sanitize-unicode.ts','utils/event-stream.ts','utils/headers.ts','utils/provider-env.ts','utils/provider-retry.ts','utils/diagnostics.ts','utils/deferred-tools.ts','utils/estimate.ts','api/constrained-sampling.ts','api/github-copilot-headers.ts','api/transform-messages.ts','api/simple-options.ts','api/openai-prompt-cache.ts','utils/error-body.ts','api/openai-responses-shared.ts'])vm.runInContext(clean(source(path)),context);
 vm.runInContext(clean(source('models.ts').match(/^export function calculateCost[\s\S]*?^}/m)[0]),context);
 vm.runInContext(clean(source(`api/${family}.ts`)),context);
 const model={id:'header-model',name:'Header model',api:family,provider:family==='anthropic-messages'?'anthropic':family==='mistral-conversations'?'mistral':'custom',baseUrl:'https://example.invalid/base',reasoning:false,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:4096,maxTokens:128};
 for(const input of inputs){
  const headers=JSON.parse(input.raw);let observed,calls=0;const order=[];
  const stream=vm.runInContext('stream',context)(model,{messages:[]},{apiKey:'synthetic-key',headers,onPayload:()=>{order.push('payload');},onResponse:()=>{order.push('response');},fetch:async(url,init)=>{
   calls++;order.push('send');const wire=new Headers(init.headers);observed=Object.fromEntries([...wire].filter(([name])=>name==='x-order'||name==='accept'||/^\d+$/.test(name)));
   return new Response('',{status:400,headers:{'content-type':'application/json'}});
  }});
  const result=await stream.result();
  if(!observed)throw new Error(`${family} did not send: ${result.errorMessage}`);
  cases.push({family,raw:input.raw,ownEntries:Object.entries(headers),serialized:JSON.stringify(headers),observed,calls,order});
 }
}
const models=source('models.ts');
const merge=vm.runInNewContext(clean(models.slice(models.indexOf('function mergeHeaders('),models.indexOf('class ModelsImpl')))+'\nmergeHeaders');
const merges=[];
for(const [base,later]of [
 ['{"Z":"base","x-order":"old","X-Order":"other"}','{"x-order":"lower","X-Order":"upper","A":"late"}'],
 ['{"Z":"base","x-order":"old"}','{"X-Order":"upper","x-order":null}'],
 ['{"10":"ten","2":"two","x-order":"old","Keep":null}','{"X-Order":"new","0":"zero"}'],
]){const result=merge(JSON.parse(base),JSON.parse(later));merges.push({base,later,entries:Object.entries(result),serialized:JSON.stringify(result)});}
const result={sourceCommit:commit,node:process.version,sdk:{openai:'6.40.0',anthropic:'0.91.1'},hashes,cases,merges};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));
else console.log(JSON.stringify(result,null,2));
