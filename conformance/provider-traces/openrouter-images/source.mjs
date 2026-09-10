// Complete immutable image adapter + installed versioned SDK, fake Fetch only.
import { execFileSync } from 'node:child_process';
import { mkdtempSync, mkdirSync, writeFileSync, readFileSync, readdirSync, symlinkSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, dirname, relative } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { createHash } from 'node:crypto';
import assert from 'node:assert/strict';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const upstream='upstream/pi-mono';
const here=dirname(fileURLToPath(import.meta.url));
const hash=x=>createHash('sha256').update(x).digest('hex');
const scratch=mkdtempSync(join(tmpdir(),'pi-openrouter-images-source-'));
try {
 const paths=['api/openrouter-images.ts','utils/error-body.ts','utils/headers.ts','utils/provider-retry.ts','utils/sanitize-unicode.ts'];
 const files={};
 for(const path of paths){ const name='packages/ai/src/'+path; const bytes=execFileSync('git',['-C',upstream,'show',pin+':'+name],{env:{PATH:'/usr/bin:/bin'}}); const dest=join(scratch,path);mkdirSync(dirname(dest),{recursive:true});writeFileSync(dest,bytes);files[name]=hash(bytes); }
 writeFileSync(join(scratch,'package.json'),'{"type":"module"}');
 symlinkSync(join(upstream,'node_modules'),join(scratch,'node_modules'),'dir');
 const sdkRoot=join(upstream,'node_modules/openai');
 const sdkFiles=[];
 function scan(path){for(const entry of readdirSync(path,{withFileTypes:true})){const child=join(path,entry.name);if(entry.isDirectory())scan(child);else if(entry.name.endsWith('.mjs')||entry.name==='package.json')sdkFiles.push([relative(sdkRoot,child),hash(readFileSync(child))]);}}
 scan(sdkRoot);sdkFiles.sort((a,b)=>a[0].localeCompare(b[0],'en'));
 const sdk={version:JSON.parse(readFileSync(join(sdkRoot,'package.json'))).version,files:sdkFiles.length,sha256:hash(JSON.stringify(sdkFiles))};
 assert.equal(sdk.version,'6.40.0');
 const {generateImages}=await import(pathToFileURL(join(scratch,'api/openrouter-images.ts')));
 Date.now=()=>1700000000000; Math.random=()=>0.5;
 const baseModel={id:'synthetic-image',name:'Synthetic',api:'openrouter-images',provider:'openrouter',baseUrl:'https://synthetic.invalid/api',input:['text','image'],output:['image','text'],cost:{input:1,output:2,cacheRead:3,cacheWrite:4},samplingParams:{ignored:7}};
 const input={input:[{type:'text',text:'hello ☀'},{type:'image',mimeType:'image/png',data:'A==='}]};
 const response={id:'synthetic-response',choices:[{message:{content:'caption',images:[{image_url:'data:image/png;base64,A==='},{image_url:{url:'data:image/webp;base64,+/='}},{image_url:'https://skip.invalid/a'},{image_url:'data:image/png;base64,a\nb'},{image_url:'data:;base64,A'},{image_url:'data:image/png;base64,'}]}}],usage:{prompt_tokens:20,completion_tokens:5,prompt_tokens_details:{cached_tokens:8,cache_write_tokens:3},total_tokens:999}};
 const cases=[];
 function add(id,config={}){cases.push({id,config});}
 add('normal');add('image-only',{model:{output:['image']}});
 add('no-key',{key:''});add('header-only-no-key',{key:'',headers:{authorization:'SYNTHETIC_HEADER'}});
 add('whitespace-key',{key:' '});add('null-auth',{headers:{Authorization:null}});
 add('header-order',{model:{headers:{'x-first':'model','x-null':'model'}},headers:{'X-A':'first','x-a':'later','X-A':'replacement','x-null':null,'content-type':'wrong','__proto__':'ignored'}});
 add('ambient-headers',{env:{OPENAI_ORG_ID:' synthetic-org ',OPENAI_PROJECT_ID:' synthetic-project ',OPENAI_CUSTOM_HEADERS:'x-env: one\nx-env: two\nAuthorization: SYNTHETIC_ENV'},headers:{'x-env':'explicit'}});
 add('bad-header-after-payload',{headers:{'bad header':'x'}});
 add('bad-header-before-payload',{env:{OPENAI_CUSTOM_HEADERS:'bad header: x'}});
 for(const [id,payload] of [['null',null],['false',false],['zero',0],['empty-string',''],['true',true],['number',3.5],['string','a'],['array',[]],['object',{}],['stream',{stream:true}]])add('payload-'+id,{payload});
 add('payload-mutate',{mutate:true});add('payload-reject',{payloadReject:true});add('response-reject',{responseReject:true});add('response-abort',{responseAbort:true});add('pre-abort',{preAbort:true});add('payload-abort',{payloadAbort:true});
 add('no-choice',{response:{id:'empty',choices:[]}});add('no-id-usage',{response:{choices:[{message:{content:''}}]}});
 add('bad-choices',{response:{...response,choices:null}});add('partial-images',{response:{...response,choices:[{message:{content:'retained',images:[{image_url:'data:image/png;base64,A'},null]}}]}});
 add('usage-zero',{response:{choices:[],usage:{}}});add('usage-negative-prompt',{response:{choices:[],usage:{prompt_tokens:-2,completion_tokens:1}}});
 add('no-usage-null',{response:{choices:[],usage:null}});
 add('json-invalid',{raw:'{'});add('plain-text',{raw:'synthetic text',contentType:'text/plain'});add('json-primitive',{raw:'3'});add('json-bom',{raw:'\ufeff'+JSON.stringify(response)});
 add('no-content',{status:204});add('json-length-zero',{status:200,raw:'',responseHeaders:{'content-length':'0'}});
 add('http-error',{status:400,raw:'{"error":{"message":"SYNTHETIC_PROVIDER_FAILURE"}}'});
 add('retry-success',{statuses:[429,200],maxRetries:1,responseHeaders:{'retry-after-ms':'0'}});
 add('retry-veto',{statuses:[500,200],maxRetries:1,responseHeaders:{'x-should-retry':'false'}});
 add('retry-cap',{statuses:[429,200],maxRetries:1,maxRetryDelayMs:1,responseHeaders:{'retry-after-ms':'2'}});
 add('fetch-retry',{fetchReject:1,maxRetries:1,responseHeaders:{'retry-after-ms':'0'}});
 add('success-read-failure',{readFailure:1,maxRetries:1});add('http-read-failure-retry',{readFailure:1,statuses:[500,200],maxRetries:1,responseHeaders:{'retry-after-ms':'0'}});
 add('endpoint-query',{endpoint:'https://synthetic.invalid/api?b=1&b=2&1=x&a=a%20b'});
 add('timeout-header',{timeoutMs:1501});add('timeout-zero',{timeoutMs:0});
 const admissionCases=[0.5,-1,9007199254740992].map((value,index)=>({id:`numeric-admission-${index}`,config:{response:{id:"kept",choices:[],usage:{completion_tokens:value}}}}));
 for(const row of [...cases,...admissionCases]){
  const c=row.config, model={...baseModel,...c.model};const calls=[];const control=new AbortController();let count=0;
  if(c.preAbort)control.abort();
  const oldEnv={...process.env};for(const [k,v]of Object.entries(c.env??{}))process.env[k]=v;
  const options={apiKey:c.key??'SYNTHETIC_IMAGE_KEY',headers:c.headers,signal:control.signal,timeoutMs:c.timeoutMs,maxRetries:c.maxRetries,maxRetryDelayMs:c.maxRetryDelayMs,
   onPayload:(p)=>{calls.push({kind:'payload',value:structuredClone(p)});if(c.payloadReject)throw Error('SYNTHETIC_PAYLOAD_ERROR');if(c.payloadAbort)control.abort();if(c.mutate)p.extra={z:1,'2':'two','1':'one'};return Object.hasOwn(c,'payload')?c.payload:undefined;},
   onResponse:(head)=>{calls.push({kind:'response',status:head.status,headers:head.headers});if(c.responseAbort)control.abort();if(c.responseReject)throw Error('SYNTHETIC_RESPONSE_ERROR');},
   fetch:async(url,init)=>{ count++;const headers=Object.fromEntries(new Headers(init.headers));for(const key of Object.keys(headers)){if(key==='user-agent')headers[key]='pi-ai-rust';else if(key.startsWith('x-stainless-')&&!['x-stainless-retry-count','x-stainless-timeout'].includes(key))delete headers[key];}
    calls.push({kind:'fetch',url:String(url),method:init.method,headers,headerForm:init.headers instanceof Headers?'constructed':'record',bodyForm:Object.hasOwn(init,'body')?typeof init.body:'absent',...(Object.hasOwn(init,'body')?{body:init.body}:{})});
    if(c.fetchReject&&count<=c.fetchReject)throw Error('synthetic fetch failure');
    const status=c.statuses?.[Math.min(count-1,c.statuses.length-1)]??c.status??200;
    const raw=status===204?null:c.raw??JSON.stringify(c.response??response);
    return new Response(c.readFailure&&count<=c.readFailure?new ReadableStream({start(controller){controller.error(Error('synthetic read failure'));}}):raw,{status,headers:{'content-type':c.contentType??'application/json',...c.responseHeaders}});
   }};
  row.model=model;row.context=input;row.calls=calls;row.result=await generateImages({...model,baseUrl:c.endpoint??model.baseUrl},input,options);
  for(const key of Object.keys(process.env))if(!Object.hasOwn(oldEnv,key))delete process.env[key];for(const[k,v]of Object.entries(oldEnv))process.env[k]=v;
 }
 const fetchPresence=[];
 for(const form of ["absent","text","bytes"]){const init={method:"POST",...(form==="absent"?{}:{body:form==="text"?"":new Uint8Array()})};const r=new Request("https://synthetic.invalid/",init);fetchPresence.push({form,bodyPresent:r.body!==null,contentType:r.headers.get("content-type"),text:await r.text()});}
 const output={node:process.version,driverSha256:hash(readFileSync(fileURLToPath(import.meta.url))),fetchPresence,upstream:pin,files,sdk,baseResponse:response,admissionCases,normalization:['SDK user-agent -> pi-ai-rust; platform x-stainless headers omitted','fixed injected timestamp and jitter','Rust errors compare presence/reason, not synthetic source prose'],cases};
 const serialized=JSON.stringify(output,null,2)+'\n';
 if(process.argv.includes('--check'))assert.equal(readFileSync(join(here,'trace.json'),'utf8'),serialized,'complete image source fixture drift');
 else writeFileSync(join(here,'trace.json'),serialized);
 console.log(`OpenRouter image source: ${cases.length} cases verified`);
} finally {rmSync(scratch,{recursive:true,force:true});}
