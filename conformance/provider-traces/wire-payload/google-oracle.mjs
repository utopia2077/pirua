import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {createRequire,stripTypeScriptTypes} from 'node:module';
import {deepStrictEqual} from 'node:assert';
import vm from 'node:vm';
import {GoogleGenAI,FinishReason,FunctionCallingConfigMode} from 'upstream/pi-mono/node_modules/@google/genai/dist/node/index.mjs';
const upstream='upstream/pi-mono',commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(upstream+'/package.json');
const sdkPath=upstream+'/node_modules/@google/genai/dist/node/index.mjs';
const authority={commit,sdk:JSON.parse(readFileSync(upstream+'/node_modules/@google/genai/package.json','utf8')).version,sha256:createHash('sha256').update(readFileSync(sdkPath)).digest('hex'),node:process.version};
deepStrictEqual(authority.sdk,'1.52.0');deepStrictEqual(authority.sha256,'748f86443c1c2c80e4ecf568d0c033ad9f1e9f34926c810e35c77e965a951c33');
const source=p=>execFileSync('git',['show',commit+':packages/ai/src/'+p],{cwd:upstream,encoding:'utf8'});
const modules=['utils/json-parse.ts','utils/sanitize-unicode.ts','utils/event-stream.ts','utils/headers.ts','utils/provider-env.ts','utils/pi-user-agent.ts','utils/provider-retry.ts','utils/retry.ts','utils/overflow.ts','utils/deferred-tools.ts','utils/estimate.ts','utils/error-body.ts','api/constrained-sampling.ts','api/transform-messages.ts','api/simple-options.ts','api/google-shared.ts','api/google-generative-ai.ts'];
const texts=modules.map(p=>[p,source(p)]);const models=source('models.ts');
const clean=s=>stripTypeScriptTypes(s.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const model={id:'gemini-3-flash-preview',name:'Synthetic Gemini',api:'google-generative-ai',provider:'google',baseUrl:'https://example.invalid/v1beta',reasoning:true,input:['text','image'],cost:{input:1,output:2,cacheRead:3,cacheWrite:4},contextWindow:128000,maxTokens:4096};
const context={systemPrompt:'Be precise',messages:[{role:'user',content:'hello',timestamp:7}]};
const tool={name:'lookup',description:'Find a value',parameters:{type:'object',properties:{q:{type:'string'}},required:['q']}};
const done={candidates:[{finishReason:'STOP',content:{parts:[{text:'done'}]}}]};
const sse=chunks=>chunks.map(c=>'data: '+JSON.stringify(c)+'\n\n').join('');
const full=[
 {responseId:'response-first',candidates:[{content:{parts:[{text:'think',thought:true,thoughtSignature:'YWJj'}]}}],usageMetadata:{promptTokenCount:20,cachedContentTokenCount:3,candidatesTokenCount:1,thoughtsTokenCount:2,totalTokenCount:23}},
 {responseId:'response-second',candidates:[{content:{parts:[{text:' more',thought:true},{text:'hello',thoughtSignature:'ZGVm'},{text:'',thoughtSignature:''}]}}]},
 {candidates:[{content:{parts:[{functionCall:{id:'call-1',name:'lookup',args:{'10':'ten','2':'two',z:1,a:2}},thoughtSignature:'c2ln'},{text:'after'}]},finishReason:'STOP'}],usageMetadata:{promptTokenCount:30,cachedContentTokenCount:4,candidatesTokenCount:5,thoughtsTokenCount:3,totalTokenCount:38}},
 {usageMetadata:{promptTokenCount:32,cachedContentTokenCount:5,candidatesTokenCount:6,thoughtsTokenCount:4,totalTokenCount:42}},
];
const cases=[
{id:'replace-forward',replacement:{model:'synthetic',contents:'hello',config:{httpOptions:{headers:{'x-order':'lower','X-Order':'upper'}}}}},
{id:'replace-reverse',replacement:{model:'synthetic',contents:'hello',config:{httpOptions:{headers:{'X-Order':'upper','x-order':'lower'}}}}},
{id:'mutate-forward',mutation:{'x-order':'lower','X-Order':'upper'}},
{id:'mutate-reverse',mutation:{'X-Order':'upper','x-order':'lower'}},
{id:'noop',options:{temperature:0.5,maxTokens:32}},
{id:'numeric-controls',replacement:{model:'synthetic',contents:'hello',config:{httpOptions:{headers:{'z-order':'z','10':'ten','2':'two','01':'leading','x-number':1e20}},seed:-0}}},
];
const envKeys=['GOOGLE_GENAI_USE_ENTERPRISE','GOOGLE_GENAI_USE_VERTEXAI','GOOGLE_CLOUD_PROJECT','GOOGLE_CLOUD_LOCATION','GOOGLE_API_KEY','GEMINI_API_KEY','GOOGLE_VERTEX_BASE_URL','GOOGLE_GEMINI_BASE_URL'];
const results=[];const originalFetch=globalThis.fetch;const originalWarn=console.warn,originalDebug=console.debug;
try {for(const spec of cases){for(const key of envKeys)delete process.env[key];Object.assign(process.env,spec.env??{});let warnings=[];console.warn=(...v)=>warnings.push(String(v[0]));console.debug=(...v)=>warnings.push(String(v[0]));let events=[],requests=[],logical=null,logicalKeys=null,configKeys=null,order=[],sleeps=[];
 const fake=async(url,init)=>{order.push('send');const headers=Object.fromEntries(new Headers(init.headers));for(const key of ['user-agent','x-goog-api-client'])if(headers[key]?.startsWith('google-genai-sdk/1.52.0 gl-node/'+process.version))headers[key]=headers[key].replace('gl-node/'+process.version,'pi-ai-rust');requests.push({url:String(url),method:init.method,headers,body:JSON.parse(init.body)});let index=0;const chunks=spec.chunks??[sse([done])];return new Response(spec.status===204?null:new ReadableStream({pull(c){if(index<chunks.length)c.enqueue(new TextEncoder().encode(chunks[index++]));else c.close();}},{highWaterMark:0}),{status:spec.status??200,headers:{'content-type':spec.contentType??'text/event-stream',...spec.responseHeaders}});};globalThis.fetch=fake;
 const runtime=vm.createContext({Error,JSON,GoogleGenAI,FinishReason,FunctionCallingConfigMode,TextDecoder,TextEncoder,Response,ReadableStream,Headers,process,structuredClone,AbortController,fetch:fake,Date:class extends Date{static now(){return 7;}},Math:Object.assign(Object.create(Math),{random:()=>0}),setTimeout:(fn,ms)=>{sleeps.push(ms);queueMicrotask(fn);return 1;},clearTimeout:()=>{},partialParse:require('partial-json').parse});
 vm.runInContext(clean(models.slice(models.indexOf('export function calculateCost'),models.indexOf('export function modelsAreEqual'))),runtime);
 for(const [path,text]of texts)vm.runInContext(clean(text),runtime,{filename:path});vm.runInContext('getPiUserAgent=()=>"pi-ai-rust"',runtime);
 const Stream=vm.runInContext('AssistantMessageEventStream',runtime),push=Stream.prototype.push;Stream.prototype.push=function(event){const copy=JSON.parse(JSON.stringify(event));events.push(copy);return push.call(this,event);};
 const target={...model,...spec.model};const transcript=spec.context??context;const options={apiKey:spec.key??'synthetic-key',...spec.options,...(spec.headers?{headers:spec.headers}:{}),onPayload:p=>{order.push('payload');logical=JSON.parse(JSON.stringify(p));logicalKeys=Object.keys(p);configKeys=Object.keys(p.config);if(spec.mutation)p.config.httpOptions={headers:spec.mutation};return spec.replacement;},onResponse:()=>order.push('response')};
 let stream;try{stream=vm.runInContext(spec.simple?'streamSimple':'stream',runtime)(target,transcript,options);}catch(error){stream=null;results.push({...spec,model:target,context:transcript,options:spec.options??{},synchronous:true,sourceMessage:error.message,requests,logical,order,sleeps});continue;}
 const result=await stream.result();const flags={retryable:runtime.isRetryableAssistantError(result),overflow:runtime.isContextOverflow(result)};
 results.push({...spec,model:target,context:transcript,options:spec.options??{},requests,logical,logicalKeys,configKeys,order,sleeps,warnings,events,result,sourceFlags:flags});
}}finally{globalThis.fetch=originalFetch;console.warn=originalWarn;console.debug=originalDebug;for(const key of envKeys)delete process.env[key];}
deepStrictEqual(results[0].result?.stopReason,'stop');deepStrictEqual(results[0].requests.length,1);
const result={authority,evidence:'Pinned complete adapter plus installed SDK; injected global fetch observes SDK arguments before network defaults. Counter/clock start0/7 per case; SDK runtime identity normalized.',cases:results};
if(process.argv.includes('--check'))deepStrictEqual(JSON.parse(JSON.stringify(result)),JSON.parse(readFileSync(new URL('./google.json',import.meta.url),'utf8')));else {const range=process.argv.find(v=>v.startsWith('--range='))?.slice(8).split(':').map(Number);process.stdout.write(JSON.stringify(range?{...result,cases:result.cases.slice(...range)}:result,null,2)+'\n');}
