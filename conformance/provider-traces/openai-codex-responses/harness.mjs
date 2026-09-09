import {execFileSync} from 'node:child_process';
import {stripTypeScriptTypes,createRequire} from 'node:module';
import {createHash} from 'node:crypto';
import * as zlib from 'node:zlib';
import assert from 'node:assert/strict';
export const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const upstream='upstream/pi-mono',require=createRequire(upstream+'/package.json');
const cache=new Map(),hashes={};
function source(path){if(!cache.has(path)){const text=execFileSync('git',['show',pin+':packages/ai/src/'+path],{cwd:upstream,encoding:'utf8'});cache.set(path,text);hashes[path]=createHash('sha256').update(text).digest('hex');}return cache.get(path);}
export const authority=()=>({pin,node:process.version,hashes});
export const clone=v=>JSON.parse(JSON.stringify(v));
export const model={id:'gpt-5.5',name:'Synthetic Codex',api:'openai-codex-responses',provider:'openai-codex',baseUrl:'https://example.invalid/backend-api',reasoning:true,input:['text','image'],cost:{input:1,output:2,cacheRead:3,cacheWrite:4},contextWindow:10000,maxTokens:1000};
export const context={systemPrompt:'system',messages:[{role:'user',content:'hello',timestamp:7}]};
export const token=(account,escaped=false)=>{let json=JSON.stringify({'https://api.openai.com/auth':{chatgpt_account_id:account}});if(escaped)json=json.replace(/[\u007f-\uffff]/g,c=>'\\u'+c.charCodeAt(0).toString(16).padStart(4,'0'));return 'aaa.'+Buffer.from(json).toString('base64')+'.bbb';};
export const fullEvents=(id='r1',text='Hello')=>[
 {type:'response.created',response:{id}},
 {type:'response.output_item.added',output_index:0,item:{type:'message',id:'m-'+id,role:'assistant',status:'in_progress',content:[]}},
 {type:'response.content_part.added',output_index:0,content_index:0,part:{type:'output_text',text:''}},
 {type:'response.output_text.delta',output_index:0,content_index:0,delta:text},
 {type:'response.output_item.done',output_index:0,item:{type:'message',id:'m-'+id,role:'assistant',status:'completed',content:[{type:'output_text',text}]}},
 {type:'response.completed',response:{id,status:'completed',end_turn:false,service_tier:'default',usage:{input_tokens:20,output_tokens:3,total_tokens:23,input_tokens_details:{cached_tokens:2}}}},
];
export const sse=events=>events.map(event=>'data: '+JSON.stringify(event)+'\n\n').join('');
export function runtime(config={}){
 const trace={requests:[],connections:[],sent:[],closes:[],order:[],sleeps:[],snapshots:[],compressions:0};
 let now=7,nextTimer=0,sendIndex=0,fetchIndex=0;const timeouts=[],bodyControllers=[];const caller=new AbortController();const timers=new Map(),sockets=[];
 const fakeSetTimeout=(fn,ms)=>{const id=++nextTimer;timers.set(id,{fn,ms,at:now+ms});return id;};
 const fakeClearTimeout=id=>timers.delete(id);
 function fire(){const first=[...timers.entries()].sort((a,b)=>a[1].at-b[1].at)[0];assert(first,'pending source without controlled timer');timers.delete(first[0]);now=first[1].at;trace.sleeps.push(first[1].ms);first[1].fn();}
 class Socket {
  listeners=new Map();readyState=0;
  constructor(url,options){this.id=sockets.length+1;sockets.push(this);trace.connections.push({socket:this.id,url,headers:safeHeaders(options.headers)});const spec=config.connect?.[this.id-1]??{};queueMicrotask(()=>{if(spec.hold)return;if(spec.error){this.readyState=3;this.emit('error',{});}else{this.readyState=1;this.emit('open',{});}});}
  addEventListener(type,fn){if(!this.listeners.has(type))this.listeners.set(type,new Set());this.listeners.get(type).add(fn);}
  removeEventListener(type,fn){this.listeners.get(type)?.delete(fn);}
  emit(type,event){for(const fn of [...this.listeners.get(type)??[]])fn(event);}
  send(data){trace.order.push('ws-send');trace.sent.push({socket:this.id,json:data,body:JSON.parse(data)});const index=sendIndex++;const spec=config.ws?.[index]??{};if(spec.throw)throw Error(spec.throw);queueMicrotask(()=>{for(const event of spec.events??(spec.hold||spec.error||spec.close?[]:fullEvents('r'+sendIndex)))this.emit('message',{data:typeof event==='string'?event:JSON.stringify(event)});if(spec.error)this.emit('error',{});if(spec.close){this.readyState=3;this.emit('close',spec.close);}});}
  close(code,reason){trace.closes.push({socket:this.id,code,reason});this.readyState=3;}
 }
 function safeHeaders(headers){const record=Object.fromEntries(new Headers(headers));if(record.authorization)record.authorization='Bearer <synthetic>';return record;}
 async function fetch(url,init){const index=fetchIndex++,spec=config.http?.[index]??{};trace.order.push('fetch');const compressed=typeof init.body!=='string';const json=compressed?zlib.zstdDecompressSync(init.body).toString():init.body;trace.requests.push({url:String(url),method:init.method,headers:safeHeaders(init.headers),json,body:JSON.parse(json),compressed,wire:Buffer.from(init.body).toString('base64')});if(spec.error)throw Error(spec.error);const payload=spec.body??sse(spec.events??fullEvents('http'+(index+1)));let pos=0;const chunks=spec.chunks??[payload];const body=spec.nullBody?null:new ReadableStream({start(c){bodyControllers.push(c);if(config.manualTimeout)init.signal?.addEventListener("abort",()=>c.error(init.signal.reason),{once:true});},pull(c){if(pos<chunks.length){trace.order.push('body-chunk');c.enqueue(new TextEncoder().encode(chunks[pos++]));}else if(spec.readError){trace.order.push('body-error');c.error(Error(spec.readError));}else if(!spec.hold){trace.order.push('body-eof');c.close();}},cancel(){trace.order.push('body-cancel');}},{highWaterMark:0});return new Response(body,{status:spec.status??200,statusText:spec.statusText??'',headers:spec.headers??{'content-type':'text/event-stream'}});}
 const deps={AbortSignal:config.manualTimeout?{timeout(ms){const c=new AbortController();timeouts.push(c);return c.signal;}}:AbortSignal,partialParse:require('partial-json').parse,Date:class extends Date{static now(){return now;}},process:{versions:{node:process.versions.node},env:{},getBuiltinModule:()=>config.compression==='missing'?null:{...zlib,zstdCompressSync:(...args)=>{trace.compressions++;if(config.compression==='fail')throw Error('synthetic compression failure');return zlib.zstdCompressSync(...args);}}},globalThis:{fetch,...(config.noWebSocket?{}:{WebSocket:Socket})},fetch,setTimeout:fakeSetTimeout,clearTimeout:fakeClearTimeout,registerSessionResourceCleanup:()=>{},getPiUserAgent:()=> 'pi-ai-rust',uuidv7:()=> '00000000-0007-7000-8000-000000000001',resolveHttpProxyUrlForTarget:()=>undefined};
 function load(path,slice){const full=source(path),text=slice?slice(full):full;const names=[...text.matchAll(/export (?:async )?(?:function|const|class) (\w+)/g)].map(v=>v[1]);if(path==='api/openai-codex-responses.ts')names.push('mapCodexEvents','getRetryAfterDelayMs','validateRetryDelayMs');const js=stripTypeScriptTypes(text).replace(/^import[\s\S]*?;\s*$/gm,'').replace(/\bexport /g,'');Object.assign(deps,new Function(...Object.keys(deps),js+'\nreturn {'+names.join(',')+'};')(...Object.values(deps)));}
 load('models.ts',s=>s.slice(s.indexOf('export function calculateCost'),s.indexOf('export function modelsAreEqual')));
 for(const path of ['utils/hash.ts','utils/json-parse.ts','utils/sanitize-unicode.ts','api/constrained-sampling.ts','api/transform-messages.ts','utils/deferred-tools.ts','utils/estimate.ts','api/simple-options.ts','api/openai-prompt-cache.ts','utils/provider-env.ts','utils/error-body.ts','utils/headers.ts','utils/diagnostics.ts','utils/abort-signals.ts','utils/overflow.ts','utils/retry.ts','utils/event-stream.ts','api/openai-responses-shared.ts'])load(path);
 const Stream=deps.AssistantMessageEventStream,oldPush=Stream.prototype.push;
 Stream.prototype.push=function(event){this.snapshots??=[];const snapshot=clone(event);for(const block of (snapshot.partial??snapshot.message??snapshot.error)?.content??[]){delete block.partialJson;delete block.customInput;delete block.index;}this.snapshots.push(snapshot);return oldPush.call(this,event);};
 load('api/openai-codex-responses.ts');
 async function drive(promise){let settled=false,value,failure;promise.then(v=>{settled=true;value=v;},e=>{settled=true;failure=e;});for(let guard=0;guard<20&&!settled;guard++){for(let micro=0;micro<250&&!settled;micro++)await Promise.resolve();if(!settled)fire();}assert(settled,'bounded controlled source completion');if(failure)throw failure;return value;}
 function start(spec={}){const target={...model,...spec.model},input=spec.context??context,options={apiKey:token(Object.hasOwn(spec,'account')?spec.account:'account-a',spec.escapeAccount),...spec.options,fetch,onPayload:payload=>{trace.order.push('payload');trace.logical=clone(payload);if(spec.payloadError)throw Error(spec.payloadError);return spec.replacement;},onResponse:head=>{trace.order.push('response');if(spec.responseError&&fetchIndex===1)throw Error(spec.responseError);trace.lastHead=clone(head);}};if(spec.caller)options.signal=caller.signal;if(spec.noKey)delete options.apiKey;let stream;try{stream=(spec.simple?deps.streamSimple:deps.stream)(target,input,options);}catch(error){return{finish:async()=>({synchronous:true,error:error.message,model:target,context:input})};}return{stream,finish:async()=>{const result=await drive(stream.result());return{model:target,context:input,events:stream.snapshots,result:clone(result),flags:{retryable:deps.isRetryableAssistantError(result),overflow:deps.isContextOverflow(result)}};}};}
 return{trace,deps,sockets,timers,timeouts,bodyControllers,caller,fire,drive,start,async run(spec){return await start(spec).finish();},advance(ms){now+=ms;},reset:id=>deps.resetOpenAICodexWebSocketDebugStats(id),close:id=>deps.closeOpenAICodexWebSocketSessions(id),stats:id=>clone(deps.getOpenAICodexWebSocketDebugStats(id)??null)};
}
