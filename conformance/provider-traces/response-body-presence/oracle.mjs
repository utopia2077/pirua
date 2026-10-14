const {GoogleGenAI,FinishReason,FunctionCallingConfigMode,ResourceScope,ThinkingLevel}=await import(upstreamUrl('node_modules/@google/genai/dist/node/index.mjs'));import {upstreamRoot, upstreamUrl} from '../../upstream.mjs';
import {execFileSync} from 'node:child_process';
import {createRequire,stripTypeScriptTypes} from 'node:module';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const root=upstreamRoot,pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185',require=createRequire(root+'/package.json'),hashes={};
const source=p=>{const s=execFileSync('git',['show',pin+':packages/ai/src/'+p],{cwd:root,encoding:'utf8'});hashes[p]=createHash('sha256').update(s).digest('hex');return s;};
const clean=s=>stripTypeScriptTypes(s.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
globalThis.fetch=()=>{throw Error('unconfigured network forbidden');};
const cases=[];
for(const family of ['pi-messages','anthropic-messages','mistral-conversations','openai-completions','openai-responses','azure-openai-responses','google-generative-ai','google-vertex'])for(const [status,absent] of (family.startsWith('google-')?[[200,false],[200,true]]:[[200,false],[200,true],[204,true],[205,true],[400,true]])){
 const context=vm.createContext({GoogleGenAI,FinishReason,FunctionCallingConfigMode,ResourceScope,ThinkingLevel,Error,OpenAI:require('openai').default,AzureOpenAI:require('openai').AzureOpenAI,Anthropic:require('@anthropic-ai/sdk').default,Request,Response,ReadableStream,Headers,URL,TextDecoder,TextEncoder,AbortSignal,AbortController,process,structuredClone,setTimeout,clearTimeout,Date:class extends Date{static now(){return 7;}},partialParse:require('partial-json').parse});
 const modelSource=source('models.ts');vm.runInContext(clean(modelSource.slice(modelSource.indexOf('export function calculateCost'),modelSource.indexOf('export function modelsAreEqual'))),context);
 for(const p of ['utils/hash.ts','utils/json-parse.ts','utils/sanitize-unicode.ts','utils/event-stream.ts','utils/headers.ts','utils/provider-env.ts','utils/pi-user-agent.ts','utils/provider-retry.ts','utils/retry.ts','utils/overflow.ts','utils/deferred-tools.ts','utils/estimate.ts','utils/error-body.ts','utils/diagnostics.ts','api/constrained-sampling.ts','api/transform-messages.ts','api/simple-options.ts','api/github-copilot-headers.ts','api/openai-prompt-cache.ts','api/openai-responses-shared.ts',...(family.startsWith('google-')?['api/google-shared.ts']:[]),'api/'+family+'.ts'])vm.runInContext(clean(source(p)),context,{filename:p});
 vm.runInContext('getPiUserAgent=()=>"pi-ai-rust"',context);
 const model={id:'synthetic',name:'Synthetic',api:family,provider:family==='google-generative-ai'?'google':family==='google-vertex'?'google-vertex':family==='pi-messages'?'pi':family==='anthropic-messages'?'anthropic':family==='mistral-conversations'?'mistral':'openai',baseUrl:'https://example.invalid/v1',reasoning:false,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:4096,maxTokens:512};
 const events=[],order=[];let sends=0,reads=0;
 const Stream=vm.runInContext('AssistantMessageEventStream',context),push=Stream.prototype.push;
 Stream.prototype.push=function(e){events.push(JSON.parse(JSON.stringify(e)));order.push(e.type);return push.call(this,e);};
 const fetch=async()=>{sends++;order.push('fetch');return new Response(absent?null:new ReadableStream({pull(c){reads++;c.close();}},{highWaterMark:0}),{status,headers:{'content-type':'text/event-stream'}});};
 context.fetch=fetch;const priorFetch=globalThis.fetch;if(family.startsWith('google-'))globalThis.fetch=fetch;
 const input={messages:family.startsWith('google-')?[{role:'user',content:'hello',timestamp:7}]:[]};
 const result=await vm.runInContext('stream',context)(model,input,{apiKey:'synthetic-key',fetch,maxRetries:2,timeoutMs:60000,onResponse(){order.push('response');}}).result();
 globalThis.fetch=priorFetch;assert.equal(sends,1,family+": "+result.errorMessage);cases.push({family,status,absent,model,context:input,sends,reads,order,events,result:JSON.parse(JSON.stringify(result)),retryable:context.isRetryableAssistantError(result)});
}
const output={pin,node:process.versions.node,sdks:{google:JSON.parse(readFileSync(root+'/node_modules/@google/genai/package.json','utf8')).version,openai:JSON.parse(readFileSync(root+'/node_modules/openai/package.json','utf8')).version,anthropic:JSON.parse(readFileSync(root+'/node_modules/@anthropic-ai/sdk/package.json','utf8')).version},hashes,cases};
if(process.argv.includes('--check'))assert.deepEqual(output,JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));else console.log(JSON.stringify(output,null,2));
