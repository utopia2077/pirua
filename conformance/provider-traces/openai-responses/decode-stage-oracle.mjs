import { execFileSync } from 'node:child_process';
import { stripTypeScriptTypes, createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { deepStrictEqual } from 'node:assert';
import { createHash } from 'node:crypto';
const upstream='upstream/pi-mono';
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(`${upstream}/package.json`);
const OpenAI=require('openai').default;
deepStrictEqual(JSON.parse(readFileSync(`${upstream}/node_modules/openai/package.json`,'utf8')).version,'6.40.0');
// Invoke in a clean child environment; never discover host provider configuration.
if (Object.keys(process.env).some(key => /^(OPENAI_|PI_CACHE_RETENTION)/.test(key))) throw new Error('Run with env -i');
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
load('api/openai-responses.ts');
const model={id:'gpt-5.5',name:'Decode witness',api:'openai-responses',provider:'openai',baseUrl:'https://example.invalid/v1',reasoning:true,input:['text'],cost:{input:1,output:2,cacheRead:3,cacheWrite:4},contextWindow:10000,maxTokens:1000};
const context={messages:[]};
const message=content=>({object:'response',output:[{type:'message',content}]});
const text=value=>message([{type:'output_text',text:value}]);
const inputs=[
 ['root-null',null],['root-false',false],['root-number',2],['root-string','plain'],
 ['plain-object',{}],['other-object',{object:'other',output:null}],
 ['output-missing',{object:'response'}],
 ...[null,false,3,{},[], '', 'abc',[null],[false,2,'x',{}]].map((output,i)=>[`output-${i}`,{object:'response',output}]),
 ['content-missing',{object:'response',output:[{type:'message'}]}],
 ...[null,false,2,{},[], '', 'abc',[null],[false,2,'x',{}]].map((content,i)=>[`content-${i}`,message(content)]),
 ['text-missing',message([{type:'output_text'}])],
 ...[null,false,2,{},['x',null,[2,{},true]],{toString:null},{toString:'not callable'}, {valueOf:null}].map((value,i)=>[`text-${i}`,text(value)]),
 ['overwrite-output-text',{...text('new'),output_text:'old'}],
 ['own-request-id',{...text('new'),_request_id:{toString:null}}],
 ['own-proto',JSON.parse('{"__proto__":{"object":"response"},"output":null}')],
 ['own-proto-text',text(JSON.parse('{"__proto__":{"toString":null}}'))],
 ['array-terminal',[{type:'response.completed',response:{status:'completed'}}]],
 ['array-null',[null]],['array-empty',[]],
 ...[500,2,'prompt is too long','plain','500 billing','503 quota exceeded',
   '400 status code (no body)','413 status code (no body)',
   'Throttling error: prompt is too long','Service unavailable: prompt is too long',
   'prompt is too long rate limit','server error out of budget'].flatMap((value,index)=>[
   [`operand-json-${index}`,value],
   [`operand-text-${index}`,String(value),'text/plain'],
 ]),
 ['operand-object-control',{object:'response',output:null,marker:'500 prompt is too long'}],
 ['operand-nested-control',{object:'response',output:[{type:'message',content:null}],marker:'500 billing'}],
];
const cases=[];
for(const [id,value,contentType] of inputs){
 const body=contentType?String(value):JSON.stringify(value),order=[];let calls=0;
 const stream=deps.stream(model,context,{apiKey:'synthetic-key',maxRetries:3,onPayload:()=>{order.push('payload');return {stream:false};},onResponse:()=>{order.push('response');},fetch:async()=>{calls++;order.push('send');return new Response(body,{headers:{'content-type':contentType??'application/json','x-request-id':'synthetic-id'}});}});
 await stream.complete;
 const result=stream.events.at(-1);const final=result.message??result.error;
 const sdk=new OpenAI({apiKey:'synthetic-key',maxRetries:0,fetch:async()=>new Response(body,{headers:{'content-type':contentType??'application/json','x-request-id':'synthetic-id'}})});
 let decoded;
 try {const {data}=await sdk.responses.create({stream:false},{maxRetries:0}).withResponse();decoded={ok:true,data,ownOutputText:Object.hasOwn(data,'output_text'),requestIdDescriptor:Object.getOwnPropertyDescriptor(data,'_request_id')};}
 catch(error){decoded={ok:false,message:error.message};}
 cases.push({id,body,decoded,order,calls,events:stream.events.map(event=>event.type),stopReason:final.stopReason,retryable:deps.isRetryableAssistantError(final),contextOverflow:deps.isContextOverflow(final),...(contentType?{contentType}:{})});
}
const result={sourceCommit:commit,sdkVersion:'6.40.0',evidenceClass:'executed-pinned-wrapper-and-installed-sdk',model,context,sourceHashes:hashes,cases};
if(process.argv.includes('--check'))deepStrictEqual(JSON.parse(JSON.stringify(result)),JSON.parse(readFileSync(new URL('./decode-stage.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
