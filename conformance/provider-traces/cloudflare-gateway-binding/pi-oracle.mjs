import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {readFileSync} from 'node:fs';
import {stripTypeScriptTypes,createRequire} from 'node:module';
import {createHash} from 'node:crypto';
import vm from 'node:vm';
import assert from 'node:assert/strict';
const upstream=upstreamRoot,pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(upstream+'/package.json'),hashes={};
const source=p=>{const s=execFileSync('git',['show',pin+':packages/ai/src/'+p],{cwd:upstream,encoding:'utf8'});hashes[p]=createHash('sha256').update(s).digest('hex');return s;};
const clean=s=>stripTypeScriptTypes(s.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const base='https://gateway.ai.cloudflare.com/v1/account/gateway';
const model={id:'synthetic',name:'Synthetic',api:'pi-messages',provider:'pi',baseUrl:base+'/pi',reasoning:false,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:4096,maxTokens:512};
const context={messages:[]};
const usage={input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}};
const cases=[];
for(const spec of [{id:'forward',headers:{'x-alias':'lower','X-Alias':'upper'}},{id:'reverse',headers:{'X-Alias':'upper','x-alias':'lower'}},{id:'literal-joined',headers:{'x-alias':'lower, upper'}},{id:'depth129',headers:{},depth:129},{id:'depth256',headers:{},depth:256},{id:'outside',headers:{},rejected:true}]) {
 const events=[],calls=[],fetchInputs=[];
 const runtime=vm.createContext({Error,JSON,URL,Request,Response,Headers,TextDecoder,TextEncoder,AbortSignal,Date:{now:()=>7},partialParse:require('partial-json').parse});
 for(const path of ['utils/json-parse.ts','utils/diagnostics.ts','utils/headers.ts','utils/provider-env.ts','utils/event-stream.ts','api/pi-messages.ts','api/cloudflare-gateway-binding.ts','utils/retry.ts'])vm.runInContext(clean(source(path)),runtime,{filename:path});
 const Stream=vm.runInContext('AssistantMessageEventStream',runtime),push=Stream.prototype.push;
 Stream.prototype.push=function(event){events.push(JSON.parse(JSON.stringify(event)));return push.call(this,event);};
 const controller=new AbortController();
 const binding={gateway(id){calls.push({stage:'gateway',id});return {run(data,options){assert.equal(options.signal,controller.signal);if(spec.depth){let value=data.query;for(let i=0;i<spec.depth;i++){assert.equal(value.length,1);value=value[0];}assert.equal(value,null);}
 calls.push({stage:'run',data:spec.depth?{...data,query:null,queryDepth:spec.depth}:data});return Promise.resolve(new Response('data: '+JSON.stringify({type:'done',reason:'stop',usage})+'\n\n',{headers:{'content-type':'text/event-stream'}}));}};}};
 const configured=vm.runInContext('createGatewayBindingFetch',runtime)({binding,baseUrl:base,gateway:'gateway'});
 const target=spec.rejected?{...model,baseUrl:'https://outside.invalid/pi'}:model;
 const result=await vm.runInContext('stream',runtime)(target,context,{apiKey:'synthetic-key',headers:spec.headers,signal:controller.signal,onPayload:spec.depth?()=>{let value=null;for(let i=0;i<spec.depth;i++)value=[value];return value;}:undefined,fetch:(url,init)=>{fetchInputs.push({url:String(url),headerKind:init.headers instanceof Headers?'Headers':'record',headers:Object.entries(init.headers),bodyKind:typeof init.body});return configured(url,init);}}).result();
 assert.equal(result.stopReason,spec.rejected?'error':'stop');assert.equal(calls.filter(c=>c.stage==='run').length,spec.rejected?0:1);
 cases.push({...spec,model:target,context,options:{},fetchInputs,calls,events,result,retryable:runtime.isRetryableAssistantError(result)});
}
assert.deepEqual(cases.slice(0,3).map(c=>c.calls[1].data.headers['x-alias']),['upper','lower','lower, upper']);
const output={pin,node:process.versions.node,hashes,cases};
if(process.argv.includes('--check'))assert.deepEqual(JSON.parse(JSON.stringify(output)),JSON.parse(readFileSync(new URL('./pi.json',import.meta.url),'utf8')));else console.log(JSON.stringify(output,null,2));
