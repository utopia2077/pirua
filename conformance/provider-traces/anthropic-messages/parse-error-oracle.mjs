import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {readFileSync} from 'node:fs';
import {createRequire,stripTypeScriptTypes} from 'node:module';
import vm from 'node:vm';
const root=upstreamRoot,commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(root+'/package.json');
const source=p=>execFileSync('git',['show',commit+':packages/ai/src/'+p],{cwd:root,encoding:'utf8'});
const clean=t=>stripTypeScriptTypes(t.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const runtime=vm.createContext({Error,JSON,Anthropic:require('@anthropic-ai/sdk').default,TextDecoder,TextEncoder,Response,ReadableStream,Headers,process,structuredClone,setTimeout,clearTimeout,
  Date:class extends Date{static now(){return 7;}},partialParse:require('partial-json').parse});
for(const p of ['utils/json-parse.ts','utils/sanitize-unicode.ts','utils/event-stream.ts','utils/headers.ts','utils/provider-env.ts','utils/pi-user-agent.ts','utils/provider-retry.ts','utils/retry.ts','utils/overflow.ts','utils/deferred-tools.ts','api/constrained-sampling.ts','api/github-copilot-headers.ts','api/transform-messages.ts'])vm.runInContext(clean(source(p)),runtime);
vm.runInContext(clean(source('models.ts').match(/^export function calculateCost[\s\S]*?^}/m)[0]),runtime);
vm.runInContext(clean(source('api/anthropic-messages.ts')),runtime);
const stream=vm.runInContext('stream',runtime);
const trace=JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8'));
const frame=(name,data,lines='')=>'event: '+name+'\n'+lines+'data: '+data+'\n\n';
const ending=frame('message_delta','{"type":"message_delta","delta":{"stop_reason":"end_turn"}}')+frame('message_stop','{"type":"message_stop"}');
const start=trace.chunks.slice(0,3).map(value=>frame(value.type,JSON.stringify(value))).join('');
const cases=[
 ['billing-ignored',[frame('message_delta','invalid server error','ignored: billing\n')]],
 ['overflow-ignored',[frame('message_delta','invalid','ignored: prompt is too long\n')]],
 ['both-raw',[frame('message_delta','invalid','ignored: prompt is too long\n: server error\n')]],
 ['empty-flush-carry',[': billing\n\n'+frame('message_delta','invalid server error')]],
 ['null-overflow',[frame('message_delta','null',': prompt is too long\n')]],
 ['null-control',[frame('message_delta','null')]],
 ['repaired-control',[frame('message_delta','{"type":"message_delta","delta":{"stop_reason":"end_turn"},"noise":"a\\q"}',': billing prompt is too long\n')+frame('message_stop','{"type":"message_stop"}')]],
 ['ignored-event-control',[frame('proxy.stats','invalid server error',': prompt is too long\n')+ending]],
 ['named-error-control',[frame('error','server error',': billing\n')]],
 ['repeated-event-raw',['event: server error\nevent: message_delta\ndata: invalid\n\n']],
 ['repeated-data-veto',[frame('message_delta','invalid server error','data: billing\n')]],
 ['partial-usage-overflow',[start,frame('message_delta','invalid',': prompt is too long\n')]],
 ['eof-overflow',[frame('message_delta','invalid',': prompt is too long\n').replace(/\n\n$/,'')]],
 ['cr-read-boundary',['event: message_delta\r','\n: prompt is too long\r\ndata: invalid\r\n\r\n']],
 ['unknown-does-not-carry',[frame('proxy.stats','ignored',': billing\n')+frame('message_delta','invalid server error')]],
];
const results=[];
const Stream=vm.runInContext('AssistantMessageEventStream',runtime);
const push=Stream.prototype.push;
let eventTypes=[];
Stream.prototype.push=function(event){eventTypes.push(event.type);return push.call(this,event);};
for(const [id,chunks] of cases) {
 let calls=0;eventTypes=[];
 const value=await stream(trace.model,trace.context,{apiKey:'fixture-key',maxRetries:1,fetch:async()=>{calls++;return new Response(new ReadableStream({start(c){for(const chunk of chunks)c.enqueue(new TextEncoder().encode(chunk));c.close();}}),{headers:{'content-type':'text/event-stream'}});}}).result();
 const content=structuredClone(value.content);for(const block of content){delete block.index;delete block.partialJson;}
 results.push({id,chunks,sourceMessage:value.errorMessage??null,stop:value.stopReason,retryable:runtime.isRetryableAssistantError(value),overflow:runtime.isContextOverflow(value),
   calls,eventTypes,model:value.model,responseId:value.responseId??null,usage:value.usage,content});
}
console.log(JSON.stringify({commit,sdk:'@anthropic-ai/sdk@0.91.1',node:process.version,evidenceClass:'executed-pinned-full-adapter-parse-error-context',cases:results},null,2));
