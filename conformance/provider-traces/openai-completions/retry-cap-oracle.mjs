import { execFileSync } from 'node:child_process';
import { createRequire,stripTypeScriptTypes } from 'node:module';
import { readFileSync } from 'node:fs';
import { deepStrictEqual } from 'node:assert';
const upstream='upstream/pi-mono';
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
if(Object.keys(process.env).some(key=>/^(OPENAI_|PI_CACHE_RETENTION)/.test(key))) throw new Error('Run with env -i');
const require=createRequire(`${upstream}/package.json`);
const OpenAI=require('openai').default;
const deps={};
for(const path of ['provider-retry','error-body','overflow','retry']) {
    const source=execFileSync('git',['show',`${commit}:packages/ai/src/utils/${path}.ts`],{cwd:upstream,encoding:'utf8'});
    const names=[...source.matchAll(/export (?:async )?(?:function|const) (\w+)/g)].map(v=>v[1]);
    const js=stripTypeScriptTypes(source).replace(/^import[\s\S]*?;\s*$/gm,'').replace(/\bexport /g,'');
    Object.assign(deps,new Function(js+`\nreturn {${names.join(',')}};`)());
}
globalThis.fetch=()=>{throw new Error('live networking forbidden');};
let attempts=0;
const client=new OpenAI({apiKey:'synthetic-key',baseURL:'https://example.invalid',fetch:async()=>{
    attempts++;return new Response('{"error":{"message":"context length exceeded"}}',{status:429,headers:{'retry-after-ms':'2'}});
}});
let message;
try {await deps.retryProviderRequest(()=>client.chat.completions.create({model:'synthetic',messages:[],stream:true},{maxRetries:0}).withResponse(),{maxRetries:1,maxRetryDelayMs:1});}
catch(error) {message=deps.formatProviderError(deps.normalizeProviderError(error));}
const classify=message=>({retryable:deps.isRetryableAssistantError({stopReason:'error',errorMessage:message}),contextOverflow:deps.isContextOverflow({stopReason:'error',errorMessage:message})});
const cases=[];
const bodies=[
 ...[400,413].flatMap(status=>['','false','null','{}'].map(body=>({id:`${status}-${body||'empty'}`,status,body}))),
 {id:'ordinary403',status:403,body:'ordinary'},
 {id:'billing403',status:403,body:'billing'},
 {id:'billing429',status:429,body:'billing'},
 {id:'quota503',status:503,body:'quota exceeded'},
 {id:'overflow429',status:429,body:'context length exceeded'},
 {id:'overflow-billing503',status:503,body:'prompt is too long billing'},
 {id:'both-vetoes429',status:429,body:'prompt is too long rate limit billing'},
 {id:'quota-code429',status:429,body:'{"error":{"message":"ordinary","code":"insufficient_quota"}}'},
 {id:'late-billing429',status:429,body:JSON.stringify({error:{message:'x'.repeat(4000)+'billing'}})},
 {id:'body-read-failure403',status:403,body:'',readFailure:true},
];
for(const input of bodies)for(const cap of [false,true]){
 let calls=0,sdkMessage,finalMessage;
 const headers=cap?{'x-should-retry':'true','retry-after-ms':'2'}:{};
 const client=new OpenAI({apiKey:'synthetic-key',baseURL:'https://example.invalid',fetch:async()=>{
  calls++;
  const body=input.readFailure?new ReadableStream({start(controller){controller.error(new Error('terminated'));}}):input.body;
  return new Response(body,{status:input.status,headers});
 }});
 try{await deps.retryProviderRequest(async()=>{
  try{return await client.chat.completions.create({model:'synthetic',messages:[],stream:true},{maxRetries:0}).withResponse();}
  catch(error){sdkMessage=error.message;throw error;}
 },{maxRetries:cap?1:0,maxRetryDelayMs:1});}
 catch(error){finalMessage=deps.formatProviderError(deps.normalizeProviderError(error));}
 cases.push({...input,id:`${input.id}-${cap?'cap':'plain'}`,cap,headers,calls,message:finalMessage,...classify(finalMessage),sdkMessage,sdkContextOverflow:classify(sdkMessage).contextOverflow});
}
const prefixEquivalenceCases=[];
for(const sdkMessage of ['400 status code (no body)','413 status code (no body)','Throttling error: prompt is too long','Service unavailable: prompt is too long','429 billing','503 quota exceeded']){
 let capMessage;
 try{await deps.retryProviderRequest(async()=>{throw new OpenAI.APIError(undefined,undefined,sdkMessage,new Headers({'retry-after-ms':'2'}));},{maxRetries:1,maxRetryDelayMs:1});}catch(error){capMessage=error.message;}
 const expected=classify(capMessage);
 deepStrictEqual(classify(`Server requested retry delay. ${sdkMessage}`),expected);
 prefixEquivalenceCases.push({sdkMessage,original:classify(sdkMessage),capMessage,...expected});
}
const result={sourceCommit:commit,evidenceClass:'executed-pinned-retry-and-installed-sdk',status:'complete-attempt-classification',attempts,message,...classify(message),cases,prefixEquivalenceCases};
if(process.argv.includes('--check')) deepStrictEqual(result,JSON.parse(readFileSync(new URL('./retry-cap.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
