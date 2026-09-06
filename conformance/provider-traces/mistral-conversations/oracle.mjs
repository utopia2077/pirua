import { execFileSync } from 'node:child_process';
import { stripTypeScriptTypes, createRequire } from 'node:module';
import vm from 'node:vm';
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const root='upstream/pi-mono';
const source=path=>execFileSync('git',['show',`${commit}:${path}`],{cwd:root,encoding:'utf8'});
const clean=text=>stripTypeScriptTypes(text.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const require=createRequire(`${root}/package.json`);
const context=vm.createContext({Date:{now:()=>7},partialParse:require('partial-json').parse});
for(const path of ['packages/ai/src/utils/hash.ts','packages/ai/src/utils/json-parse.ts','packages/ai/src/utils/sanitize-unicode.ts']) vm.runInContext(clean(source(path)),context);
vm.runInContext(clean(source('packages/ai/src/models.ts').match(/^export function calculateCost[\s\S]*?^}/m)[0]),context);
vm.runInContext(clean(source('packages/ai/src/api/mistral-conversations.ts')),context);
const model={id:'mistral-large-latest',name:'Mistral Large',api:'mistral-conversations',provider:'mistral',baseUrl:'https://api.mistral.ai',reasoning:true,input:['text','image'],cost:{input:1,output:2,cacheRead:0.1,cacheWrite:0.2},contextWindow:128000,maxTokens:16384};
const chunks=[
 {id:'response-1',choices:[{finish_reason:null,delta:{content:[{type:'thinking',thinking:[{text:'reason'}]}]}}]},
 {id:'ignored-later',choices:[{delta:{content:[{type:'text',text:'answer'}]}}]},
 {choices:[{delta:{tool_calls:[{id:'abc123456',index:0,function:{name:'lookup',arguments:'{"query":'}}]}}]},
 {choices:[{finish_reason:'tool_calls',delta:{tool_calls:[{index:0,function:{name:'',arguments:'"pi"}'}}]}}],usage:{prompt_tokens:10,completion_tokens:4,total_tokens:14,prompt_tokens_details:{cached_tokens:3}}}
];
const clone=value=>JSON.parse(JSON.stringify(value,(key,value)=>key==='partialArgs'?undefined:value));
context.model=model;
const output=vm.runInContext('createOutput(model)',context);
const events=[{type:'start',partial:clone(output)}];
await context.consumeChatStream(model,output,{push:event=>events.push(clone(event))},(async function*(){for(const data of chunks)yield {data};})());
events.push({type:'done',reason:output.stopReason,message:clone(output)});
const timeouts=[];
for(const value of [0,1,0.5,-1,2147483647,2147483648,4294967295,4294967296,9007199254740991]) {
 try { const signal=AbortSignal.timeout(value); timeouts.push({value,accepted:true,initiallyAborted:signal.aborted}); }
 catch(error){timeouts.push({value,accepted:false,code:error.code});}
}
console.log(JSON.stringify({sourceCommit:commit,source:'packages/ai/src/api/mistral-conversations.ts:558-735',evidenceClass:'executed-pinned-functions',normalization:['timestamp=7','omit partialArgs scratch only','UA=pi-ai-rust; no OS discovery'],node:process.version,model,chunks,events,timeouts},null,2));
