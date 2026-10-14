import { execFileSync } from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import { stripTypeScriptTypes, createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { deepStrictEqual } from 'node:assert';
import { createHash } from 'node:crypto';
const upstream=upstreamRoot;
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(`${upstream}/package.json`);
const OpenAI=require('openai').default;
const family=process.argv.includes('--completions')?'openai-completions':'openai-responses';
deepStrictEqual(JSON.parse(readFileSync(`${upstream}/node_modules/openai/package.json`,'utf8')).version,'6.40.0');
// Invoke in a clean child environment; never discover host provider configuration.
if (Object.keys(process.env).some(key => /^(OPENAI_|AZURE_|PI_CACHE_RETENTION)/.test(key))) throw new Error('Run with env -i');
globalThis.fetch=()=>{throw new Error('live networking forbidden');};
const hashes={};
const deps={ OpenAI, partialParse:require('partial-json').parse };
function source(path) {
  const content=execFileSync('git',['show',`${commit}:packages/ai/src/${path}`],{cwd:upstream,encoding:'utf8'});
  hashes[path]=createHash('sha256').update(content).digest('hex'); return content;
}
function load(path, slice) {
  const full=source(path), text=slice?slice(full):full;
  const names=[...text.matchAll(/export (?:async )?(?:function|const|class) (\w+)/g)].map(v=>v[1]);
  const js=stripTypeScriptTypes(text).replace(/^import[\s\S]*?;\s*$/gm,'').replace(/\bexport /g,'');
  Object.assign(deps,new Function(...Object.keys(deps),js+`\nreturn {${names.join(',')}};`)(...Object.values(deps)));
}
load('models.ts',s=>s.slice(s.indexOf('export function calculateCost'),s.indexOf('export function modelsAreEqual')));
for(const path of ['utils/hash.ts','utils/json-parse.ts','utils/sanitize-unicode.ts','api/constrained-sampling.ts','api/transform-messages.ts','utils/deferred-tools.ts','utils/estimate.ts','api/simple-options.ts','api/github-copilot-headers.ts','api/openai-prompt-cache.ts','utils/provider-env.ts','utils/error-body.ts','utils/headers.ts','utils/provider-retry.ts','utils/overflow.ts','utils/retry.ts','api/openai-responses-shared.ts']) load(path);
deps.getPiUserAgent=()=> 'pi-ai-rust';
deps.Date=class extends Date { static now(){return 7;} };
deps.AssistantMessageEventStream=class {
  events=[]; complete=new Promise(resolve=>{this.resolve=resolve;});
  push(event){this.events.push(JSON.parse(JSON.stringify(event,(key,value)=>['partialJson','customInput','index'].includes(key)?undefined:value)));}
  end(){this.resolve();}
};
load(`api/${family}.ts`);

const baseModel={id:'gpt-5.5',name:'Azure fixture',api:family,provider:'openai',baseUrl:'https://example.invalid/v1',reasoning:true,input:['text','image'],cost:{input:1000000,output:2000000,cacheRead:3000000,cacheWrite:4000000},contextWindow:10000,maxTokens:4096};
const baseContext={systemPrompt:'system',messages:[{role:'user',content:'hi',timestamp:7}]};
const terminal='data: {"type":"response.completed","response":{"id":"r","status":"completed","service_tier":"priority","usage":{"input_tokens":10e0,"output_tokens":3.0,"total_tokens":13e0,"input_tokens_details":{"cached_tokens":2.0}}}}\n\n';
const text='data: {"type":"response.output_item.added","output_index":0.0,"item":{"type":"message","id":"m"}}\n\ndata: {"type":"response.output_text.delta","output_index":0e0,"delta":"hello"}\n\ndata: {"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"m","phase":"final_answer","content":[{"type":"output_text","text":"hello"}]}}\n\n';
const tool={name:'read',description:'Read',parameters:{type:'object',properties:{input:{type:'string'}},required:['input']}};
const encode=events=>events.map(event=>'data: '+JSON.stringify(event)+'\n\n').join('');
const replay={role:'assistant',api:baseModel.api,provider:baseModel.provider,model:baseModel.id,usage:{input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}},stopReason:'toolUse',timestamp:7,content:[{type:'thinking',thinking:'',thinkingSignature:'{"type":"reasoning","id":"rs","summary":[],"encrypted_content":"opaque"}'},{type:'toolCall',id:'call|item',name:'read',arguments:{input:'x'}}]};
const cases=[
 ...['\u0085','\u00a0',' ',''].map((suffix,i)=>({id:'media-'+i,options:{maxRetries:1},replacement:{stream:false},contentType:'application/problem+json'+suffix,sse:'[]'})),
 {id:'media-plain',replacement:{stream:false},contentType:'text/plain',sse:'[]'},
 {id:'media-substring-control',replacement:{stream:false},contentType:'application/json\u0085',sse:'[]'},
 ...['',' \t ','\u00a0','org','\u00a0org\u00a0','\u0085org\u0085','\ufefforg\u3000'].map((value,i)=>({id:'env-'+i,ambient:{OPENAI_ORG_ID:value,OPENAI_PROJECT_ID:value}})),
 ...['',' \t ','\u00a0','X-Custom: value','\u0085','\ufeffX-Custom: value\u3000'].map((value,i)=>({id:'custom-stage-'+i,ambient:{OPENAI_CUSTOM_HEADERS:value},options:{headers:{'bad name':'value'}}})),
 {id:'ordinary-header-nbsp',options:{headers:{'x-custom':'\u00a0value\u00a0'}}},
 {id:'scoped-pi-cache',options:{env:{PI_CACHE_RETENTION:' long '}}},
 {id:'custom-valid',ambient:{OPENAI_CUSTOM_HEADERS:'\u00a0X-Custom: value\u00a0'}},
 {id:'custom-lazy-org',ambient:{OPENAI_CUSTOM_HEADERS:'X-Custom: value',OPENAI_ORG_ID:'bad\nvalue'}},
 ...[' \t ','X-Custom: value'].map((value,i)=>({id:'custom-null-stage-'+i,ambient:{OPENAI_CUSTOM_HEADERS:value},options:{headers:{'bad name':null}}})),
 ...[9,10,11,12,13,32,160,5760,8192,8193,8194,8195,8196,8197,8198,8199,8200,8201,8202,8232,8233,8239,8287,12288,65279].map(code=>({id:'ecma-env-'+code,ambient:{OPENAI_ORG_ID:String.fromCodePoint(code)+'org'+String.fromCodePoint(code)}})),
];
const outputs=[];
const customRecord=process.argv.includes('--custom-record');
let parsedEntries;
if(customRecord){
 const sdkClient=readFileSync(`${upstream}/node_modules/openai/src/client.ts`,'utf8');
 const begin=sdkClient.indexOf('const parsed: Record<string, string> = {};');
 const end=sdkClient.indexOf('options.defaultHeaders = buildHeaders([parsed, options.defaultHeaders]);',begin);
 if(begin<0||end<0)throw new Error('SDK parsed-object source anchor changed');
 parsedEntries=new Function('customHeadersEnv',stripTypeScriptTypes(sdkClient.slice(begin,end))+'\nreturn Object.entries(parsed);');
 cases.splice(0,cases.length,...[
  ['repeat-exact-slot','x-order:one\nX-Order:two\nx-order:three'],
  ['distinct-alias-control','x-order:one\nX-Order:two'],
  ['exact-last-control','x-order:one\nx-order:three'],
  ['proto-string','__proto__: value\nconstructor: literal\nx-order:one'],
  ['overwritten-invalid-value','x-order: Ā\nx-order: valid'],
  ['trimmed-exact-key',' x-order : first\nX-Order: second\n\u00a0x-order\u00a0: third'],
  ['numeric-own-slots','x-tail: t\n10: ten\n2: two\n01: literal\n0: zero\n4294967295: not-index\n2: final'],
  ['blank-lines','\nignored\n\t\nX-Name: value\nno-colon\n'],
  ['blank-name',': value'],
  ['ordinary-prototype-names','constructor: literal\ntoString: string\nhasOwnProperty: own'],
  ['proto-case-control','__Proto__: ordinary\n__proto__: ignored'],
  ['colon-in-value','x-name: one:two:three'],
  ['discarded-proto-invalid','__proto__: Ā\nx-safe: valid'],
 ].map(([id,value])=>({id,ambient:{OPENAI_CUSTOM_HEADERS:value}})));
}
for(const item of cases){
 const model={...baseModel,...item.model},context={...baseContext,...item.context},options={...(item.noKey?{}:{apiKey:'synthetic-key'}),...item.options},order=[],requests=[];
 for(const[key,value]of Object.entries(item.ambient??{}))process.env[key]=value;
 options.onPayload=()=>{order.push('payload');return item.replacement;};options.onResponse=()=>{order.push('response');};
 const sse=item.sse??(family==='openai-completions'?'data: '+JSON.stringify({id:'chat_1',model:'reported',choices:[{delta:{content:'hello'},finish_reason:'stop'}],usage:{prompt_tokens:10,completion_tokens:3,total_tokens:13}})+'\n\n':text+terminal);
 options.fetch=async(url,init)=>{order.push('send');const headers=Object.fromEntries(init.headers);for(const key of Object.keys(headers))if(key.startsWith('x-stainless-')&&!['x-stainless-timeout','x-stainless-retry-count'].includes(key))delete headers[key];requests.push({url,headers,body:JSON.parse(init.body)});return new Response(item.status===204?null:sse,{status:item.status??200,headers:{'content-type':item.contentType??'text/event-stream',...item.responseHeaders}});};
 const stream=(item.simple?deps.streamSimple:deps.stream)(model,context,options);await stream.complete;
 for(const key of Object.keys(item.ambient??{}))delete process.env[key];
 const last=stream.events.at(-1),result=last.message??last.error;
 outputs.push({id:item.id,model,context,options:item.options??{},ambient:item.ambient??{},replacement:item.replacement,noKey:item.noKey??false,simple:item.simple??false,status:item.status??200,contentType:item.contentType??'text/event-stream',responseHeaders:item.responseHeaders??{},sse,requests,order,events:stream.events,retryable:deps.isRetryableAssistantError(result),contextOverflow:deps.isContextOverflow(result)});
}
const output={sourceCommit:commit,sdkVersion:'6.40.0',evidenceClass:'executed-pinned-wrapper-and-installed-SDK',sourceHashes:hashes,cases:outputs};
if(customRecord)for(const item of output.cases)item.parsedEntries=parsedEntries(item.ambient.OPENAI_CUSTOM_HEADERS.trim());
const file=(family==='openai-completions'?'completions':'responses')+(customRecord?'-custom-record':'')+'.json';
if(process.argv.includes('--check'))deepStrictEqual(JSON.parse(JSON.stringify(output)),JSON.parse(readFileSync(new URL('./'+file,import.meta.url),'utf8')));else process.stdout.write(JSON.stringify(output,null,process.argv.includes('--compact')?0:2)+'\n');
