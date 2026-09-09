import {execFileSync} from 'node:child_process';
import {stripTypeScriptTypes} from 'node:module';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185',hashes={};
const deps={};
for(const path of ['api/constrained-sampling.ts','api/openai-responses-shared.ts']){
 const source=execFileSync('git',['show',pin+':packages/ai/src/'+path],{cwd:'upstream/pi-mono',encoding:'utf8'});
 hashes[path]=createHash('sha256').update(source).digest('hex');
 const names=[...source.matchAll(/export (?:async )?(?:function|const|class) (\w+)/g)].map(m=>m[1]);
 const js=stripTypeScriptTypes(source).replace(/^import[\s\S]*?;\s*$/gm,'').replace(/\bexport /g,'');
 Object.assign(deps,new Function(...Object.keys(deps),js+`\nreturn {${names.join(',')}};`)(...Object.values(deps)));
}
const tool={name:'lookup',description:'Synthetic',parameters:{properties:{payload:{type:'string'}},required:['payload'],type:'object'}};
const cases=[];
for(const strict of [undefined,null,false,true])for(const deferLoading of [undefined,false,true])for(const supportsStrictMode of [false,true]){
 const options={...(strict===undefined?{}:{strict}),...(deferLoading===undefined?{}:{deferLoading}),supportsStrictMode};
 cases.push({tool,options,expectedJson:JSON.stringify(deps.convertResponsesTools([tool],options))});
}
const result={pin,hashes,cases};
if(process.argv.includes('--check'))assert.deepEqual(result,JSON.parse(readFileSync(new URL('./deferred-order.json',import.meta.url),'utf8')));else console.log(JSON.stringify(result,null,2));
