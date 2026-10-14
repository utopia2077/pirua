import { execFileSync } from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import { stripTypeScriptTypes } from 'node:module';
import vm from 'node:vm';
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const source=path=>execFileSync('git',['show',`${commit}:${path}`],{cwd:upstreamRoot,encoding:'utf8'});
const clean=text=>stripTypeScriptTypes(text.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const context=vm.createContext({Date:{now:()=>7}});
vm.runInContext(clean(source('packages/ai/src/models.ts').match(/^export function calculateCost[\s\S]*?^}/m)[0]),context);
vm.runInContext(clean(source('packages/ai/src/api/mistral-conversations.ts')),context);
const model={id:'test',api:'mistral-conversations',provider:'mistral',cost:{input:1,output:2,cacheRead:0.1,cacheWrite:0.2}};
context.model=model;
const inputs=[
 {id:'negative-cache',usage:{prompt_tokens:10,num_cached_tokens:-2}},
 {id:'huge-cache-clamps',usage:{prompt_tokens:10,num_cached_tokens:1e20}},
 {id:'fraction-clamps-to-zero',usage:{prompt_tokens:0,num_cached_tokens:0.5}},
 {id:'fraction-final-outside-core',usage:{prompt_tokens:10,num_cached_tokens:0.5}},
 {id:'false-precedes-number',usage:{prompt_tokens:10,promptTokensDetails:{cachedTokens:false},num_cached_tokens:7}},
 {id:'string-precedes-number',usage:{prompt_tokens:10,promptTokensDetails:{cachedTokens:'3'},num_cached_tokens:7}},
 {id:'null-uses-later',usage:{prompt_tokens:10,promptTokensDetails:{cachedTokens:null},num_cached_tokens:7}},
 {id:'missing-all',usage:{}},
 {id:'null-fallbacks',usage:{prompt_tokens:null,completion_tokens:null,total_tokens:null}}
];
const cases=[];
for(const input of inputs){
 const output=vm.runInContext('createOutput(model)',context);
 await context.consumeChatStream(model,output,{push(){}},(async function*(){yield {data:{choices:[{finish_reason:'stop',delta:{}}],usage:input.usage}};})());
 const admitted=['input','output','cacheRead','cacheWrite','totalTokens'].every(key=>Number.isSafeInteger(output.usage[key])&&output.usage[key]>=0);
 cases.push({...input,expected:output.usage,coreAdmitted:admitted});
}
console.log(JSON.stringify({sourceCommit:commit,source:'packages/ai/src/api/mistral-conversations.ts:533-612',evidenceClass:'executed-pinned-functions',cases},null,2));
