import {assertCodexSourceEqual} from './source-comparison.mjs';import {runtime,authority} from './harness.mjs';import {readFileSync} from 'node:fs';import assert from 'node:assert/strict';
const cases=[];
for(const replacement of [null,true,1,[],['x',2], '', 'ab', '😀', {type:'custom',input:[]}])for(const cached of [false,true]){
 const config={ws:[{events:[{type:'response.completed',response:{id:'r1',status:'completed',usage:{input_tokens:1,output_tokens:0,total_tokens:1}}}]},{events:[{type:'response.completed',response:{id:'r2',status:'completed',usage:{input_tokens:1,output_tokens:0,total_tokens:1}}}]}]};
 const r=runtime(config),options={transport:'auto',...(cached?{sessionId:'session'}:{})};const first=await r.run({options,replacement}),second=await r.run({options,replacement});const stats=r.stats('session');r.close();const trace={...r.trace,sent:r.trace.sent.map(({socket,json})=>({socket,json}))};cases.push({id:`${typeof replacement}-${cases.length}`,replacement,options,config,first,second,stats,trace});
}
const result={authority:authority(),normalization:'String-spread surrogate halves retained only as raw wire JSON, not scalar-only parsed fixture values.',cases};if(process.argv.includes('--check'))assertCodexSourceEqual(result,JSON.parse(readFileSync(new URL('./payload.json',import.meta.url),'utf8')));else process.stdout.write(JSON.stringify(result,null,2)+'\n');
