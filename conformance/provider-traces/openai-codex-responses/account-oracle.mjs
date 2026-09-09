import {runtime,authority} from './harness.mjs';import {readFileSync} from 'node:fs';import assert from 'node:assert/strict';
const cases=[];
for(const [id,account,escapeAccount] of [['string','account'],['number',1],['true',true],['false',false],['zero',0],['null',null],['empty',''],['object',{}],['array',[]],['nested-array',[true,null,{},[1,2]]],['shadow-toString',{toString:'shadow'}],['shadow-both',{toString:null,valueOf:1}],['shadow-nested',[{toString:'shadow'}]],['shadow-valueOf',{valueOf:'shadow'}],['literal-latin','é'],['escaped-latin','é',true],['literal-utf8','雪'],['escaped-unicode','雪',true]]){
 const r=runtime(),spec={account,escapeAccount:escapeAccount??false,options:{sessionId:'session',transport:'auto'}};
 const first=await r.run(spec),second=await r.run(spec);const stats=r.stats('session');r.close();cases.push({id,...spec,first,second,stats,trace:r.trace});
}
{
 const r=runtime(),options={sessionId:'session',transport:'auto'};const output=[];for(const account of [1,'1',1])output.push(await r.run({account,options}));const stats=r.stats('session');r.close();cases.push({id:'number-string-distinct',options,output,stats,trace:r.trace});assert.equal(stats.connectionsCreated,2);assert.equal(stats.connectionsReused,1);
}
const result={authority:authority(),cases};if(process.argv.includes('--check'))assert.deepEqual(result,JSON.parse(readFileSync(new URL('./account.json',import.meta.url),'utf8')));else process.stdout.write(JSON.stringify(result,null,2)+'\n');
