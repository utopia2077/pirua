import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {stripTypeScriptTypes, createRequire} from 'node:module';
import {createHash} from 'node:crypto';
import {deepStrictEqual} from 'node:assert';
import vm from 'node:vm';
const upstream='upstream/pi-mono', commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(upstream+'/package.json'), hashes={};
const source=path=>{const text=execFileSync('git',['show',`${commit}:packages/ai/src/${path}`],{cwd:upstream,encoding:'utf8'});hashes[path]=createHash('sha256').update(text).digest('hex');return text;};
const clean=text=>stripTypeScriptTypes(text.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const model={id:'payload-model',name:'Payload',api:'pi-messages',provider:'pi',baseUrl:'https://example.invalid/base',reasoning:false,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:4096,maxTokens:512};
const usage={input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}};
const cases=[{id:'noop'},{id:'replace',kind:'ordered'},{id:'mutate',kind:'ordered',mutate:true},{id:'large',size:1048577},{id:'large-shrink',size:1048577,shrink:true},{id:'depth128',depth:128},{id:'depth129',depth:129},{id:'depth256',depth:256},{id:'depth-shrink',depth:256,shrink:true}];
const results=[];
for(const spec of cases){
  const events=[],order=[];let wire,keys;
  const runtime=vm.createContext({Error,JSON,URL,Headers,TextDecoder,AbortSignal,Date:{now:()=>7},partialParse:require('partial-json').parse});
  for(const path of ['utils/json-parse.ts','utils/diagnostics.ts','utils/headers.ts','utils/provider-env.ts','utils/event-stream.ts','api/pi-messages.ts'])vm.runInContext(clean(source(path)),runtime,{filename:path});
  const Stream=vm.runInContext('AssistantMessageEventStream',runtime),push=Stream.prototype.push;
  Stream.prototype.push=function(event){events.push(JSON.parse(JSON.stringify(event)));return push.call(this,event);};
  let replacement;
  if(spec.kind==='ordered')replacement=JSON.parse('{"z":1,"10":"ten","2":"two","01":"leading","4294967295":"nonindex","4294967294":"index","a":1e20,"zero":-0,"rounded":9007199254740993}');
  if(spec.size)replacement='x'.repeat(spec.size);
  if(spec.depth){replacement=null;for(let i=0;i<spec.depth;i++)replacement=[replacement];}
  const context=spec.size?{systemPrompt:'x'.repeat(spec.size),messages:[]}:{messages:[]};
  const options={apiKey:'synthetic-key',onPayload(p){order.push('payload');keys=Object.keys(p);if(spec.mutate){for(const key of Object.keys(p))delete p[key];Object.assign(p,replacement);return;}if(spec.shrink)return {small:true};return replacement;},onResponse(){order.push('response');},fetch:async(url,init)=>{order.push('send');wire=init.body;return new Response(`data: ${JSON.stringify({type:'done',reason:'stop',usage})}\n\n`,{headers:{'content-type':'text/event-stream'}});}};
  const result=await vm.runInContext('stream',runtime)(model,context,options).result();
  results.push({...spec,keys,order,bytes:Buffer.byteLength(wire),wire:wire.length<2048?wire:null,sha256:createHash('sha256').update(wire).digest('hex'),events,result});
}
const result={commit,node:process.version,hashes,model,usage,cases:results};
if(process.argv.includes('--check'))deepStrictEqual(JSON.parse(JSON.stringify(result)),JSON.parse(readFileSync(new URL('./pi.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
