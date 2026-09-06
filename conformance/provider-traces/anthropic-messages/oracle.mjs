import {execFileSync} from 'node:child_process';
import {createRequire,stripTypeScriptTypes} from 'node:module';
import vm from 'node:vm';
const root='upstream/pi-mono';
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(`${root}/package.json`);
const source=path=>execFileSync('git',['show',`${commit}:${path}`],{cwd:root,encoding:'utf8'});
const clean=text=>stripTypeScriptTypes(text.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const Anthropic=require('@anthropic-ai/sdk').default;
const context=vm.createContext({Anthropic,TextDecoder,TextEncoder,Response,ReadableStream,Headers,AbortController,
  process,Date:class extends Date {static now(){return 7;}},structuredClone,setTimeout,clearTimeout,
  partialParse:require('partial-json').parse});
for(const path of ['utils/json-parse.ts','utils/sanitize-unicode.ts','utils/event-stream.ts','utils/headers.ts',
  'utils/provider-env.ts','utils/pi-user-agent.ts','utils/provider-retry.ts','utils/deferred-tools.ts',
  'api/constrained-sampling.ts','api/github-copilot-headers.ts','api/transform-messages.ts']) {
  vm.runInContext(clean(source(`packages/ai/src/${path}`)),context,{filename:path});
}
vm.runInContext(clean(source('packages/ai/src/models.ts').match(/^export function calculateCost[\s\S]*?^}/m)[0]),context);
vm.runInContext(clean(source('packages/ai/src/api/anthropic-messages.ts')),context);
const Stream=vm.runInContext('AssistantMessageEventStream',context);
let events=[];const original=Stream.prototype.push;
const clone=value=>{
 const copy=structuredClone(value);
 const messages=copy.role==='assistant'?[copy]:[copy.partial,copy.message,copy.error];
 for(const message of messages)if(message?.role==='assistant')for(const block of message.content){delete block.index;delete block.partialJson;}
 if(copy.toolCall){delete copy.toolCall.index;delete copy.toolCall.partialJson;}
 return JSON.parse(JSON.stringify(copy));
};
Stream.prototype.push=function(event){events.push(clone(event));return original.call(this,event);};
const model={id:'claude-opus-4-6',name:'Claude',api:'anthropic-messages',provider:'anthropic',baseUrl:'https://gateway.invalid/prefix',reasoning:true,
  input:['text','image'],cost:{input:1,output:2,cacheRead:0.1,cacheWrite:0.2},contextWindow:200000,maxTokens:32000,
  compat:{forceAdaptiveThinking:true,supportsStrictTools:true,allowedFallbackModels:[{provider:'anthropic',model:'claude-fable-5',cost:{input:10,output:20,cacheRead:1,cacheWrite:2}}]}};
const transcript={systemPrompt:'Be precise',messages:[{role:'user',content:[{type:'text',text:'Hello'},{type:'image',data:'aGVsbG8=',mimeType:'image/png'}],timestamp:7}],
  tools:[{name:'read',description:'Read a file',parameters:{type:'object',properties:{mode:{type:'string'},path:{type:'string'}},required:['path']},constrainedSampling:{type:'json_schema',strict:'require'}}]};
const options={thinkingEnabled:true,effort:'xhigh',thinkingDisplay:'summarized',maxTokens:99,temperature:0,cacheRetention:'long',metadata:{user_id:'fixture-user',ignored:'value'}};
const chunks=[
 {type:'message_start',message:{id:'msg_fixture',model:'claude-fable-5',usage:{input_tokens:12,output_tokens:1,cache_read_input_tokens:2,cache_creation_input_tokens:6,cache_creation:{ephemeral_1h_input_tokens:4}}}},
 {type:'content_block_start',index:0,content_block:{type:'text',text:'Hello'}},
 {type:'content_block_delta',index:0,delta:{type:'text_delta',text:' world☃'}},
 {type:'content_block_stop',index:0},
 {type:'content_block_start',index:1,content_block:{type:'thinking',thinking:'reason',signature:'sig'}},
 {type:'content_block_delta',index:1,delta:{type:'signature_delta',signature:'-tail'}},
 {type:'content_block_delta',index:1,delta:{type:'thinking_delta',thinking:' more'}},
 {type:'content_block_stop',index:1},
 {type:'content_block_start',index:2,content_block:{type:'redacted_thinking',data:'opaque-redacted'}},
 {type:'content_block_stop',index:2},
 {type:'content_block_start',index:3,content_block:{type:'tool_use',id:'call_1',name:'read',input:{seed:true}}},
 {type:'content_block_delta',index:3,delta:{type:'input_json_delta',partial_json:'{"path":'}},
 {type:'content_block_delta',index:3,delta:{type:'input_json_delta',partial_json:'"a.txt"}'}},
 {type:'content_block_stop',index:3},
 {type:'message_delta',delta:{stop_reason:'tool_use',end_turn:true},usage:{input_tokens:null,output_tokens:5,cache_creation_input_tokens:null,cache_creation:{ephemeral_1h_input_tokens:999},output_tokens_details:{thinking_tokens:3}}},
 {type:'message_stop'}
];
const body=chunks.map(value=>`event: ${value.type}\ndata: ${JSON.stringify(value)}\n\n`).join('');
let captured;let logical;let responseObserved;
const stream=vm.runInContext('stream',context)(model,transcript,{...options,apiKey:'fixture-key',fetch:async(input,init)=>{
  const headers=Object.fromEntries(new Headers(init.headers));for(const name of Object.keys(headers))if(name.startsWith('x-stainless-')&&!['x-stainless-retry-count','x-stainless-timeout'].includes(name))delete headers[name];
  headers['user-agent']='pi-ai-rust';headers['x-api-key']='fixture-key';captured={url:String(input),headers,body:JSON.parse(init.body)};
  return new Response(body,{headers:{'content-type':'text/event-stream','request-id':'request-fixture'}});
},onPayload:payload=>{logical=structuredClone(payload);},onResponse:response=>{responseObserved=response;}});
const result=await stream.result();
if(result.stopReason!=='toolUse')throw new Error(JSON.stringify(result));
console.log(JSON.stringify({sourceCommit:commit,sdk:'@anthropic-ai/sdk@0.91.1',node:process.version,evidenceClass:'executed-pinned-adapter-and-installed-sdk',
  normalization:['clock=7','omit private index/partialJson','portable User-Agent=pi-ai-rust; omit SDK platform identity headers'],
  model,context:transcript,options,chunks,logical,request:captured,response:responseObserved,events,result:clone(result)},null,2));
