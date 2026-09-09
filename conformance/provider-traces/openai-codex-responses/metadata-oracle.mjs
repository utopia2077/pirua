import {runtime,authority,model,clone} from './harness.mjs';
import {readFileSync} from 'node:fs';import assert from 'node:assert/strict';
const cases=[];
for(const initial of [undefined,true])for(const value of [true,false,undefined,null,'invalid'])for(const codex of [true,false]){
 const r=runtime();const output={role:'assistant',content:[],api:model.api,provider:model.provider,model:model.id,usage:{input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}},stopReason:'pending',timestamp:7,...(initial===undefined?{}:{endTurn:initial})};
 const raw={type:'response.completed',response:{id:'meta',status:'completed',usage:{input_tokens:17,output_tokens:0,total_tokens:17},...(value===undefined?{}:{end_turn:value})}};
 const events={async *[Symbol.asyncIterator](){yield raw;}};const stream=new r.deps.AssistantMessageEventStream();
 await r.deps.processResponsesStream(codex?r.deps.mapCodexEvents(events,output):events,output,stream,model,{});
 cases.push({id:`${codex?'codex':'shared'}-${initial??'missing'}-${value??(value===null?'null':'missing')}`,codex,initial:initial??null,raw,expected:clone(output)});
}
const result={authority:authority(),cases};if(process.argv.includes('--check'))assert.deepEqual(result,JSON.parse(readFileSync(new URL('./metadata.json',import.meta.url),'utf8')));else process.stdout.write(JSON.stringify(result,null,2)+'\n');
