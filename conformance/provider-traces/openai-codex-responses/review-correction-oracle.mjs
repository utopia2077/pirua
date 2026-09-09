import {runtime,authority,context} from './harness.mjs';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const cases=[];
for(const alias of ['response.done','response.completed','response.incomplete'])for(const before of [false,true])for(const value of [true,false,undefined,null,'invalid']){
 const event={type:alias,response:{id:'r1',status:'completed',usage:{input_tokens:17,output_tokens:0,total_tokens:17},...(value===undefined?{}:{end_turn:value})}};
 const events=[...(before?[{type:'response.created',response:{id:'r1'}}]:[]),event];
 const spec={id:`ws-${alias}-${before}-${value===undefined?'missing':String(value)}`,config:{ws:[{events}]},options:{transport:'auto',sessionId:'session'}};
 const r=runtime(spec.config),output=await r.run(spec);r.close();
 assert.equal(output.events[0].type,'start');assert.equal(output.events[0].partial.endTurn,!before&&typeof value==='boolean'?value:undefined);assert.equal(output.events[0].partial.usage.input,0);
 cases.push({...spec,...output});
}
for(const alias of ['response.done','response.completed','response.incomplete']){
 const spec={id:'http-'+alias,config:{http:[{events:[{type:alias,response:{id:'r1',status:'completed',end_turn:true,usage:{input_tokens:17,output_tokens:0,total_tokens:17}}}]}]},options:{transport:'sse'}};
 const r=runtime(spec.config),output=await r.run(spec);r.close();assert.equal(output.events[0].partial.endTurn,undefined);assert.equal(output.result.endTurn,true);cases.push({...spec,...output});
}
const r=runtime();for(let i=0;i<8;i++){await r.run({options:{transport:'auto',sessionId:'session'}});r.close();}
const closeCycles={cycles:8,closes:r.trace.closes.length,live:r.sockets.filter(s=>s.readyState!==3).length};assert.deepEqual(closeCycles,{cycles:8,closes:8,live:0});
const reuse=runtime(),options={transport:'auto',sessionId:'session'};const first=await reuse.run({caller:true,options});reuse.caller.abort();
const second=await reuse.run({options,context:{...context,messages:[...context.messages,first.result,{role:'user',content:'next',timestamp:8}]}});
const tokenReuse={connections:reuse.trace.connections.length,reused:reuse.stats('session').connectionsReused,delta:reuse.stats('session').deltaRequests,first,second};assert.equal(tokenReuse.connections,1);assert.equal(tokenReuse.reused,1);assert.equal(tokenReuse.delta,1);reuse.close();
const result={authority:authority(),cases,closeCycles,tokenReuse};
if(process.argv.includes('--check'))assert.deepEqual(result,JSON.parse(readFileSync(new URL('./review-correction.json',import.meta.url),'utf8')));else process.stdout.write(JSON.stringify(result,null,2)+'\n');
