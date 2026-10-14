import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {createRequire,stripTypeScriptTypes} from 'node:module';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {deepStrictEqual} from 'node:assert';
const upstream=upstreamRoot;
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
if(Object.keys(process.env).some(key=>/^(OPENAI_|PI_CACHE_RETENTION)/.test(key))) throw new Error('Run with env -i');
const source=execFileSync('git',['show',`${commit}:packages/ai/src/utils/provider-retry.ts`],{cwd:upstream,encoding:'utf8'});
const body=stripTypeScriptTypes(source).replace(/\bexport /g,'');
const sdkRequire=createRequire(`${upstream}/package.json`);
const OpenAI=sdkRequire('openai').default;
deepStrictEqual(JSON.parse(readFileSync(`${upstream}/node_modules/openai/package.json`,'utf8')).version,'6.40.0');
const math=Object.create(Math);math.random=()=>0.4;
const date=class extends Date {static now(){return 0;}};
const pure=new Function('Math','Date',body+';return {isRetryableProviderError,getRetryDelayMs};')(math,date);
const retries=new Function(body+';return retryProviderRequest;')();
const cases=[
    ...[undefined,200,400,401,403,408,409,429,500,599].map(status=>({id:`status-${status??'missing'}`,status:status??null,headers:[]})),
    {id:'forced',status:403,headers:[['x-should-retry','true']]},
    {id:'forced-edge-whitespace',status:403,headers:[['x-should-retry','\ttrue ']]},
    {id:'disabled',status:500,headers:[['x-should-retry','false']]},
    {id:'duplicate-forced',status:403,headers:[['x-should-retry','true'],['X-Should-Retry','true']]},
    {id:'duplicate-disabled',status:500,headers:[['x-should-retry','false'],['X-Should-Retry','false']]},
    {id:'server-ms',status:429,headers:[['retry-after-ms','2.5ms']]},
    {id:'duplicate-server',status:429,headers:[['retry-after-ms','2'],['retry-after-ms','3']]},
    {id:'server-seconds',status:429,headers:[['retry-after','1.25']]},
    {id:'server-date',status:503,headers:[['retry-after','Thu, 01 Jan 1970 00:00:02 GMT']]},
    {id:'server-iso-date',status:503,headers:[['retry-after','Thu Jan 01 1970 00:00:02 GMT+0000 (Coordinated Universal Time)']]},
    {id:'cap',status:429,headers:[['retry-after-ms','2']],cap:1},
    {id:'cap-inclusive',status:429,headers:[['retry-after-ms','2']],cap:2},
    {id:'cap-disabled',status:429,headers:[['retry-after-ms','60001']],cap:0},
    {id:'default-cap',status:429,headers:[['retry-after-ms','60001']]},
    ...[0,1,2,3,4,5,60].map(index=>({id:`backoff-${index}`,status:500,headers:[],index})),
];
for(const item of cases) {
    const error=Object.assign(new Error('context length exceeded'),{status:item.status??undefined,headers:new Headers(item.headers)});
    item.retryable=pure.isRetryableProviderError(error);
    if(item.retryable) {
        try {item.delay=pure.getRetryDelayMs(error,item.index??0,item.cap);}
        catch(error) {item.policyMessage=error.message;}
    }
}
globalThis.fetch=()=>{throw new Error('live networking forbidden');};
let resolveRead, controller;
const reading=new Promise(resolve=>{resolveRead=resolve;});
let attempts=0;
const client=new OpenAI({apiKey:'synthetic-key',baseURL:'https://example.invalid',fetch:async()=>{
    attempts++;
    if(attempts>1)return new Response('data: [DONE]\n\n',{status:200,headers:{'content-type':'text/event-stream'}});
    const body=new ReadableStream({start(value){controller=value;},pull(){resolveRead();}},{highWaterMark:0});
    return new Response(body,{status:429,headers:{'retry-after-ms':'0'}});
}});
const complete=retries(()=>client.responses.create({model:'synthetic',input:[],stream:true},{maxRetries:0}).withResponse(),{maxRetries:1});
await reading;
const attemptsBeforeErrorBody=attempts;
controller.enqueue(new TextEncoder().encode('{"error":{"message":"context length exceeded"}}'));controller.close();
const success=await complete;
await success.response.body.cancel();
let capAttempts=0,capMessage;
const capClient=new OpenAI({apiKey:'synthetic-key',baseURL:'https://example.invalid',fetch:async()=>{capAttempts++;return new Response('{"error":{"message":"context length exceeded"}}',{status:429,headers:{'retry-after-ms':'2'}});}});
try{await retries(()=>capClient.responses.create({model:'synthetic',input:[],stream:true},{maxRetries:0}).withResponse(),{maxRetries:1,maxRetryDelayMs:1});}catch(error){capMessage=error.message;}
const classifiers={};
for(const name of ['retry','overflow']){
    const source=execFileSync('git',['show',`${commit}:packages/ai/src/utils/${name}.ts`],{cwd:upstream,encoding:'utf8'});
    const exported=[...source.matchAll(/export (?:async )?(?:function|const) (\w+)/g)].map(v=>v[1]);
    Object.assign(classifiers,new Function(stripTypeScriptTypes(source).replace(/^import[\s\S]*?;\s*$/gm,'').replace(/\bexport /g,'')+`;return {${exported.join(',')}};`)());
}
const capError={stopReason:'error',errorMessage:capMessage};
const maxBudgetCases=[];
for(const failures of [0,1]){
    let calls=0;
    const value=await retries(async()=>{
        if(calls++<failures)throw Object.assign(new Error('synthetic transient'),{status:500,headers:new Headers({'retry-after-ms':'0'})});
        return 17;
    },{maxRetries:Number.MAX_SAFE_INTEGER});
    maxBudgetCases.push({maxRetries:Number.MAX_SAFE_INTEGER,failures,calls,result:value});
}
const result={sourceCommit:commit,sourceSha256:createHash('sha256').update(source).digest('hex'),evidenceClass:'executed-pinned-retry-and-installed-sdk',sdkVersion:'6.40.0',jitter:0.4,now:0,cases,attemptStage:{attemptsBeforeErrorBody,attemptsAfterErrorBody:attempts},capStage:{attempts:capAttempts,message:capMessage,retryable:classifiers.isRetryableAssistantError(capError),contextOverflow:classifiers.isContextOverflow(capError)},maxBudgetCases};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./vectors.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
