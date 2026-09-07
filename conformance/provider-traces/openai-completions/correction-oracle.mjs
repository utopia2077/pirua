import { execFileSync } from 'node:child_process';
import { stripTypeScriptTypes, createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { deepStrictEqual } from 'node:assert';
import { createHash } from 'node:crypto';
const upstream='upstream/pi-mono';
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
deps.AssistantMessageEventStream=class {
  events=[]; complete=new Promise(resolve=>{this.resolve=resolve;});
  push(event){this.events.push(JSON.parse(JSON.stringify(event,(key,value)=>['partialArgs','customInput','streamIndex'].includes(key)?undefined:value)));}
  end(){this.resolve();}
};
load('api/openai-completions.ts');
const model={id:'review-model',name:'Review',api:'openai-completions',provider:'custom',baseUrl:'https://example.invalid/v1',reasoning:true,input:['text','image'],cost:{input:1,output:2,cacheRead:3,cacheWrite:4},contextWindow:10000,maxTokens:4096};
const context={messages:[]};
const encode=values=>values.map(v=>'data: '+JSON.stringify(v)+'\n\n').join('');
const observations=[];
for(const [resolved,metadataOnly] of [[false,false],[true,false],[false,true],[true,true]]) {
 const chunks=[{...(metadataOnly?{}:{id:'first-id'}),choices:[{delta:{content:'prefix'}}]},
   {choices:[{delta:{tool_calls:[{index:0,...(resolved?{id:'call-id'}:{}),function:{name:'read',arguments:'{}'}}]}}]},
   metadataOnly?{id:'late-id',model:'late-reported'}:{model:'late-reported',choices:[],usage:{prompt_tokens:100,completion_tokens:7,cached_tokens:11}}];
 const signal=new AbortController();let reachBlocked;const blocked=new Promise(resolve=>reachBlocked=resolve);let controller;
 const order=[];let calls=0;
 const stream=deps.stream(model,context,{apiKey:'synthetic-key',signal:signal.signal,maxRetries:2,onPayload:()=>{order.push('payload');},onResponse:()=>{order.push('response');},fetch:async(_url,init)=>{
  calls++;order.push('send');let index=0;
  const body=new ReadableStream({start(c){controller=c;init.signal.addEventListener('abort',()=>c.error(new Error('aborted')),{once:true});},pull(c){if(index<chunks.length)c.enqueue(new TextEncoder().encode(encode([chunks[index++]])));else reachBlocked();}},{highWaterMark:0});
  return new Response(body,{headers:{'content-type':'text/event-stream'}});
 }});
 await blocked;signal.abort();await stream.complete;
 const result=stream.events.at(-1).error;
 observations.push({id:(resolved?'resolved-cancel':'unresolved-cancel')+(metadataOnly?'-metadata':''),model,context,options:{maxRetries:2},chunks,sse:encode(chunks),order,calls,result});
}
{
 let reads=0;const order=[];let calls=0;
 const stream=deps.stream(model,context,{apiKey:'synthetic-key',maxRetries:2,onPayload:()=>{order.push('payload');return {stream:false};},onResponse:()=>{order.push('response');},fetch:async()=>{calls++;order.push('send');return new Response(new ReadableStream({pull(){reads++;}},{highWaterMark:0}),{headers:{'content-type':'application/json','content-length':'0'}});}});
 await stream.complete;
 observations.push({id:'zero-length-nonstream',model,context,options:{maxRetries:2},replacement:{stream:false},contentType:'application/json',order,calls,reads,events:stream.events.map(v=>v.type),result:stream.events.at(-1).error});
}
{
 const target={...model,compat:{deferredToolsMode:'kimi',requiresAssistantAfterToolResult:true}};
 const zero={input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}};
 const tool=name=>({name,description:name,parameters:{type:'object',properties:{}}});
 const ctx={systemPrompt:'system',tools:[tool('load'),tool('late')],messages:[
  {role:'assistant',api:target.api,provider:target.provider,model:target.id,content:[{type:'thinking',thinking:'reason',thinkingSignature:'reasoning_content'},{type:'toolCall',id:'call|item/part',name:'load',arguments:{}}],usage:zero,stopReason:'toolUse',timestamp:7},
  {role:'toolResult',toolCallId:'call|item/part',toolName:'load',content:[{type:'image',mimeType:'image/png',data:'AA=='}],addedToolNames:['late'],isError:false,timestamp:7},
  {role:'user',content:'next',timestamp:7}]};
 const sse=encode([{choices:[{delta:{content:'ok'},finish_reason:'stop'}]}]);const requests=[];
 const stream=deps.stream({...target,baseUrl:'https://api.deepseek.com/v1'},ctx,{apiKey:'synthetic-key',fetch:async(url,init)=>{requests.push({url,body:JSON.parse(init.body)});return new Response(sse,{headers:{'content-type':'text/event-stream'}});}});
 await stream.complete;observations.push({id:'endpoint-kimi-replay',model:target,context:ctx,endpoint:'https://api.deepseek.com/v1',options:{},sse,requests,result:stream.events.at(-1).message});
}

for(const spec of [
 {id:'json-zero-skip',status:200,headers:[['content-type','application/json'],['content-length','0']],block:true},
 {id:'json-zero-spaces',status:200,headers:[['content-type','application/problem+json ; charset=utf-8'],['content-length',' 0 ']],block:true},
 {id:'json-double-zero-read',status:200,headers:[['content-type','application/json'],['content-length','00']]},
 {id:'json-joined-zero-read',status:200,headers:[['content-type','application/json'],['content-length','0'],['content-length','0']]},
 {id:'json-missing-length-read',status:200,headers:[['content-type','application/json']]},
 {id:'text-zero-read',status:200,headers:[['content-type','text/plain'],['content-length','0']]},
 {id:'nel-media-remains-text',status:200,headers:[['content-type','application/problem+json\u0085'],['content-length','0']]},
 {id:'status204-skip',status:204,headers:[['content-type','application/json']]},
 {id:'streaming-zero-read',status:200,headers:[['content-type','application/json'],['content-length','0']],streaming:true}
]){
 let reads=0,calls=0;const order=[];const done={choices:[{delta:{content:'ok'},finish_reason:'stop'}]};
 const body=spec.streaming?encode([done]):JSON.stringify([done]);
 const options={apiKey:'synthetic-key',onPayload:()=>{order.push('payload');return {stream:spec.streaming??false};},onResponse:()=>{order.push('response');},fetch:async()=>{
  calls++;order.push('send');let sent=false;
  const wire=spec.status===204?null:new ReadableStream({pull(c){reads++;if(spec.block)return;if(!sent){sent=true;c.enqueue(new TextEncoder().encode(body));}else c.close();}},{highWaterMark:0});
  return new Response(wire,{status:spec.status,headers:spec.headers});
 }};
 const stream=deps.stream(model,context,options);await stream.complete;
 observations.push({...spec,model,context,options:{},replacement:{stream:spec.streaming??false},body,order,calls,reads,events:stream.events,result:stream.events.at(-1).message??stream.events.at(-1).error});
}
const result={sourceCommit:commit,sdk:'6.40.0',observations};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./corrections.json',import.meta.url),'utf8')));
else console.log(JSON.stringify(result,null,2));
