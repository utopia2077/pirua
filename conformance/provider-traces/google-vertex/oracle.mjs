import {execFileSync} from 'node:child_process';import {upstreamRoot, upstreamUrl} from '../../upstream.mjs';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {createRequire,stripTypeScriptTypes} from 'node:module';
import {deepStrictEqual} from 'node:assert';
import vm from 'node:vm';
const {GoogleGenAI: ActualGoogleGenAI,FinishReason,FunctionCallingConfigMode,ResourceScope,ThinkingLevel}=await import(upstreamUrl('node_modules/@google/genai/dist/node/index.mjs'));
const {GoogleAuth}=await import(upstreamUrl('node_modules/google-auth-library/build/src/index.js'));
const upstream=upstreamRoot,commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(upstream+'/package.json');
const sdkPath=upstream+'/node_modules/@google/genai/dist/node/index.mjs';
const authority={commit,sdk:JSON.parse(readFileSync(upstream+'/node_modules/@google/genai/package.json','utf8')).version,sha256:createHash('sha256').update(readFileSync(sdkPath)).digest('hex'),node:process.version};
deepStrictEqual(authority.sdk,'1.52.0');deepStrictEqual(authority.sha256,'748f86443c1c2c80e4ecf568d0c033ad9f1e9f34926c810e35c77e965a951c33');
const source=p=>execFileSync('git',['show',commit+':packages/ai/src/'+p],{cwd:upstream,encoding:'utf8'});
const modules=['utils/json-parse.ts','utils/sanitize-unicode.ts','utils/event-stream.ts','utils/headers.ts','utils/provider-env.ts','utils/pi-user-agent.ts','utils/provider-retry.ts','utils/retry.ts','utils/overflow.ts','utils/deferred-tools.ts','utils/estimate.ts','utils/error-body.ts','api/constrained-sampling.ts','api/transform-messages.ts','api/simple-options.ts','api/google-shared.ts','api/google-vertex.ts'];
const texts=modules.map(p=>[p,source(p)]);const models=source('models.ts');
const clean=s=>stripTypeScriptTypes(s.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const model={id:'gemini-3-flash-preview',name:'Synthetic Gemini',api:'google-vertex',provider:'google-vertex',baseUrl:'https://example.invalid/v1beta',reasoning:true,input:['text','image'],cost:{input:1,output:2,cacheRead:3,cacheWrite:4},contextWindow:128000,maxTokens:4096};
const context={systemPrompt:'Be precise',messages:[{role:'user',content:'hello',timestamp:7}]};
const tool={name:'lookup',description:'Find a value',parameters:{type:'object',properties:{q:{type:'string'}},required:['q']}};
const done={candidates:[{finishReason:'STOP',content:{parts:[{text:'done'}]}}]};
const sse=chunks=>chunks.map(c=>'data: '+JSON.stringify(c)+'\n\n').join('');
const full=[
 {responseId:'response-first',candidates:[{content:{parts:[{text:'think',thought:true,thoughtSignature:'YWJj'}]}}],usageMetadata:{promptTokenCount:20,cachedContentTokenCount:3,candidatesTokenCount:1,thoughtsTokenCount:2,totalTokenCount:23}},
 {responseId:'response-second',candidates:[{content:{parts:[{text:' more',thought:true},{text:'hello',thoughtSignature:'ZGVm'},{text:'',thoughtSignature:''}]}}]},
 {candidates:[{content:{parts:[{functionCall:{id:'call-1',name:'lookup',args:{'10':'ten','2':'two',z:1,a:2}},thoughtSignature:'c2ln'},{text:'after'}]},finishReason:'STOP'}],usageMetadata:{promptTokenCount:30,cachedContentTokenCount:4,candidatesTokenCount:5,thoughtsTokenCount:3,totalTokenCount:38}},
 {usageMetadata:{promptTokenCount:32,cachedContentTokenCount:5,candidatesTokenCount:6,thoughtsTokenCount:4,totalTokenCount:42}},
];
const cases=[{id:'full',chunks:[sse(full)],context:{...context,tools:[tool]}},{id:'raw-default'},{id:'raw-enabled-dynamic',options:{thinking:{enabled:true,budgetTokens:-1},temperature:0,maxTokens:123}},{id:'raw-budget-fraction',options:{thinking:{enabled:true,budgetTokens:-1.5}}},{id:'level-precedes-budget',options:{thinking:{enabled:true,level:'LOW',budgetTokens:100}}},{id:'nonreasoning-ignores-thinking',model:{reasoning:false},options:{thinking:{enabled:true,budgetTokens:20}}},
 {id:'nullable-header-suppression',headers:{'User-Agent':null,'x-goog-api-key':'override-key'}},{id:'case-headers',headers:{'user-agent':'custom','Content-Type':'application/json; charset=utf-8','X-Custom':' value '}},
 {id:'empty-key',key:''},{id:'ephemeral-key',key:'auth_tokens/synthetic'},
 {id:'tool-any',context:{...context,tools:[tool]},options:{toolChoice:'any'}},{id:'tool-none-strict',context:{...context,tools:[{...tool,constrainedSampling:{type:'json_schema',strict:'require'}}]},options:{toolChoice:'none'}},
 {id:'strict-rejected-two',model:{id:'gemini-2.5-flash'},context:{...context,tools:[{...tool,constrainedSampling:{type:'json_schema',strict:'require'}}]}},
 {id:'empty-contents',context:{messages:[]}},{id:'models-prefix',model:{id:'models/gemini-2.5-flash'}},{id:'tuned-prefix',model:{id:'tunedModels/synthetic'}},{id:'custom-model-path',model:{id:'other/gemini'}},{id:'invalid-model',model:{id:'a..b'}},
 {id:'query-base',model:{baseUrl:'https://example.invalid/v1beta?existing=a#fragment'}},{id:'double-trailing-slash',model:{baseUrl:'https://example.invalid/v1beta//'}},
 {id:'env-vertex',env:{GOOGLE_GENAI_USE_VERTEXAI:' true ',GOOGLE_CLOUD_PROJECT:'ignored',GOOGLE_CLOUD_LOCATION:'ignored'}},
 {id:'env-enterprise',env:{GOOGLE_GENAI_USE_ENTERPRISE:'TRUE'}},{id:'enterprise-false-wins',env:{GOOGLE_GENAI_USE_ENTERPRISE:'false',GOOGLE_GENAI_USE_VERTEXAI:'true'}},{id:'enterprise-empty-wins',env:{GOOGLE_GENAI_USE_ENTERPRISE:'',GOOGLE_GENAI_USE_VERTEXAI:'true'}},
 {id:'vertex-model-path',env:{GOOGLE_GENAI_USE_VERTEXAI:'true'},model:{id:'other/gemini/ignored'}},
];
for(const id of ['gemini-2.5-pro','gemini-2.5-flash','gemini-2.5-flash-lite','gemini-3.1-pro','gemini-3.7-flash','gemini-flash-lite-latest','gemma4','prefix-Gemma-4-it','other'])for(const reasoning of [null,'minimal','low','medium','high'])cases.push({id:'simple-'+id+'-'+reasoning,simple:true,model:{id},options:reasoning?{reasoning}:{}});
for(const mapped of ['HIGH','low','invalid'])cases.push({id:'mapped-'+mapped,simple:true,model:{id:'gemini-3.7-flash',thinkingLevelMap:{xhigh:mapped}},options:{reasoning:'xhigh'}});
cases.push({id:'custom-budget',simple:true,model:{id:'gemini-2.5-flash-lite'},options:{reasoning:'minimal',thinkingBudgets:{minimal:0}}});
for(const reason of Object.values(FinishReason).concat(['server error prompt is too long','unknown']))cases.push({id:'finish-'+reason,chunks:[sse([full[0],{responseId:'late',candidates:[{finishReason:reason}],usageMetadata:{promptTokenCount:999,candidatesTokenCount:99,totalTokenCount:1098}}]) ]});
cases.push({id:'tool-length',chunks:[sse([{candidates:[{content:{parts:[{functionCall:{id:'given',name:'lookup',args:{}}}]},finishReason:'MAX_TOKENS'}]}])]},
 {id:'generated-ids',chunks:[sse([{candidates:[{content:{parts:[{functionCall:{name:'lookup',args:{}}},{functionCall:{id:'given',name:'lookup',args:{}}},{functionCall:{id:'given',name:'lookup',args:{}}}]},finishReason:'STOP'}]}])]},
 {id:'signature-only-ignored',chunks:[sse([{candidates:[{content:{parts:[{text:'x'},{thoughtSignature:'bm90'}]},finishReason:'STOP'}]}])]},
 {id:'both-text-and-tool',chunks:[sse([{candidates:[{content:{parts:[{text:'x',functionCall:{id:'same-part',name:'lookup',args:{}}}]},finishReason:'STOP'}]}])]},
 {id:'no-candidate-usage',chunks:[sse([{usageMetadata:{promptTokenCount:4,totalTokenCount:4}},done])]},
 {id:'empty-response-id',chunks:[sse([{responseId:'',candidates:[]},{responseId:'later',...done}])]},
 {id:'null-root',chunks:[sse([done,null])]},
 {id:'ignored-leading-comment',chunks:[': ignored\n'+sse([done])]},
 {id:'ignored-leading-event',chunks:['event: message\n'+sse([done])]},
 {id:'incomplete-eof',chunks:[sse([done]).trimEnd()]},
 {id:'malformed-json',chunks:['data: invalid server error\n\n'],parserNormalization:true},
 {id:'done-token',chunks:['data: [DONE]\n\n'],parserNormalization:true},
 {id:'multi-data',chunks:['data: {}\ndata: {}\n\n'],parserNormalization:true},
 {id:'null-body204',status:204,chunks:[]},
 {id:'empty-stream',chunks:[]});
for(const separator of ['\n\n','\r\r','\r\n\r\n'])cases.push({id:'delimiter-'+Buffer.from(separator).toString('hex'),chunks:['data: '+JSON.stringify(done)+separator]});
const errorChunk=JSON.stringify({error:{code:503,status:'UNAVAILABLE',message:'server error prompt is too long'}});
cases.push({id:'whole-read-api-error',chunks:[errorChunk]},{id:'same-error-split',chunks:[errorChunk.slice(0,10),errorChunk.slice(10)]});
for(const [status,body,contentType] of [[503,'{"error":{"message":"billing server error"}}','application/json'],[429,'{"error":{"message":"quota exhausted"}}','application/json'],[400,'{"error":{"message":"prompt is too long"}}','application/json'],[403,'server error','text/plain'],[503,'not json','application/json'],[302,'redirect failure','text/plain']])cases.push({id:'http-'+status+'-'+contentType,status,chunks:[body],contentType,parserNormalization:contentType==='application/json'&&body==='not json'});
cases.push({id:'retry-ignores-response-headers',status:503,chunks:['{"error":{"message":"server error"}}'],contentType:'application/json',options:{maxRetries:1,maxRetryDelayMs:1},responseHeaders:{'x-should-retry':'false','retry-after-ms':'99999'}});
for(const cloud of [false,true]){
 const env=cloud?{GOOGLE_GENAI_USE_VERTEXAI:'true'}:{};
 const config={temperature:0,topP:0.9,topK:10,maxOutputTokens:44,systemInstruction:['one','two'],responseSchema:{type:['string','null']},cachedContent:'cache',safetySettings:[{category:'HARM_CATEGORY_HATE_SPEECH',threshold:'BLOCK_NONE'}],toolConfig:{functionCallingConfig:{mode:'ANY',allowedFunctionNames:['lookup']}},tools:[{functionDeclarations:[{name:'lookup',parameters:{type:'object',properties:{n:{type:'number'}},additionalProperties:false}}]}],speechConfig:'voice',thinkingConfig:{includeThoughts:false,thinkingBudget:0},httpOptions:{extraBody:{generationConfig:{seed:5},extra:{key:'value'}},headers:{'x-extra':'yes'}}};
 cases.push({id:'sdk-config-'+cloud,env,replacement:{model:'synthetic',contents:['hello',{text:'world'}],config}});
 for(const field of ['routingConfig','modelSelectionConfig','labels','audioTimestamp','modelArmorConfig','enableEnhancedCivicAnswers'])cases.push({id:'sdk-field-'+cloud+'-'+field,env,replacement:{model:'synthetic',contents:'hello',config:{[field]:null}}});
}
cases.push({id:'header-edge-crlf',headers:{'X-Custom':'\r\n \tvalue\t \r\n'}},{id:'header-inner-newline',headers:{'X-Custom':'one\ntwo'}},{id:'header-null-plus-case',headers:{'User-Agent':null,'user-agent':'lower'}},
 {id:'sdk-header-null-key',replacement:{model:'synthetic',contents:'hello',config:{httpOptions:{headers:{'x-goog-api-key':null,'User-Agent':'override'}}}}},
 {id:'sdk-schema-declaration',replacement:{model:'synthetic',contents:'hello',config:{responseSchema:{$schema:'https://json-schema.org/draft/2020-12/schema',type:'object',properties:{n:{type:'integer'}}}}}},
 {id:'sdk-mcp-marker',replacement:{model:'synthetic',contents:'hello',config:{tools:[{inputSchema:{type:'object'}}]}}});
for(const cloud of [false,true])for(const part of [{inlineData:{mimeType:'image/png',data:'eA==',displayName:null}},{functionCall:{name:'lookup',args:{},partialArgs:null}},{toolCall:{id:'server'}},{fileData:{fileUri:'gs://synthetic/a',mimeType:'text/plain'}}])cases.push({id:'sdk-part-'+cloud+'-'+Object.keys(part)[0],env:cloud?{GOOGLE_GENAI_USE_VERTEXAI:'true'}:{},replacement:{model:'synthetic',contents:[{role:'user',parts:[part]}],config:{}}});
cases.push({id:'sdk-number-header-spelling',replacement:{model:'synthetic',contents:'hello',config:{httpOptions:{headers:{'x-number':1e20,'x-small':1e-7}}}}});

for(const [id,key,options,scopedEnv,env] of [
['adc-explicit','gcp-vertex-credentials',{project:'p',location:'europe-west1'},{GOOGLE_APPLICATION_CREDENTIALS:'/synthetic/key.json'},{GOOGLE_API_KEY:'ignored'}],
['adc-global','<adc>',{project:'p',location:'global'},{},{}],
['adc-us','',{project:'p',location:'us'},{},{}],
['adc-eu','',{project:'p',location:'eu'},{},{}],
['adc-env-fallback','gcp-vertex-credentials',{}, {GOOGLE_CLOUD_PROJECT:'',GCLOUD_PROJECT:'secondary',GOOGLE_CLOUD_LOCATION:'global',GOOGLE_APPLICATION_CREDENTIALS:''},{GOOGLE_CLOUD_PROJECT:'primary',GOOGLE_APPLICATION_CREDENTIALS:'/synthetic/ambient.json'}],
['adc-space','',{project:' p ',location:' l '},{},{}],
['key-trim',' \\ufeffsynthetic-key\\u00a0 ',{project:'ignored',location:'ignored'},{},{}],
['not-placeholder','<>',{}, {},{}],
['placeholder','<configured>',{project:'p',location:'global'},{},{}],
['missing-location','',{project:'p'},{},{}],
['ambient-key-not-pi-key','',{}, {},{GOOGLE_API_KEY:'ignored',GOOGLE_CLOUD_LOCATION:'global'}],
]) cases.push({id,key,options,scopedEnv,env,model:{baseUrl:'https://{location}-aiplatform.googleapis.com'}});
for(const error of ['other','503','undefined','401'])cases.push({id:'auth-'+error,key:'gcp-vertex-credentials',options:{project:'p',location:'global',maxRetries:1},authError:error});
cases.push({id:'auth-present-header',key:'gcp-vertex-credentials',options:{project:'p',location:'global'},headers:{Authorization:'synthetic-override','x-goog-user-project':'override'}},
{id:'auth-http-retry',key:'gcp-vertex-credentials',options:{project:'p',location:'global',maxRetries:1},status:503,chunks:['{"error":{"message":"server error"}}'],contentType:'application/json'},
{id:'adc-env-base',key:'gcp-vertex-credentials',options:{project:'p',location:'global'},model:{baseUrl:'https://{location}-aiplatform.googleapis.com'},env:{GOOGLE_VERTEX_BASE_URL:' https://env.invalid/custom '}},
{id:'adc-request-base',key:'gcp-vertex-credentials',options:{project:'p',location:'global'},replacement:{model:'gemini-3-pro',contents:'modified',config:{httpOptions:{baseUrl:'https://override.invalid/root',baseUrlResourceScope:'PROJECT',apiVersion:'v9',headers:{Authorization:''}}}}});
cases.push({id:'auth-duplicate-returned-headers',key:'gcp-vertex-credentials',options:{project:'p',location:'global'},authHeaders:[['authorization','Bearer synthetic-adc'],['x-auth-extra','one'],['X-Auth-Extra','two'],['cookie','a=1'],['cookie','b=2'],['set-cookie','a=1'],['set-cookie','b=2']]});
const payloadOrder = process.argv.includes('--payload-order');
if (payloadOrder) {
 cases.length = 0;
 for (const [id, headers] of [
  ['payload-header-forward', {'x-order':'lower','X-Order':'upper'}],
  ['payload-header-reverse', {'X-Order':'upper','x-order':'lower'}],
 ]) cases.push({id,replacement:{model:'synthetic',contents:'hello',config:{httpOptions:{headers}}}});
}
const envKeys=['GOOGLE_GENAI_USE_ENTERPRISE','GOOGLE_GENAI_USE_VERTEXAI','GOOGLE_CLOUD_PROJECT','GOOGLE_CLOUD_LOCATION','GOOGLE_API_KEY','GEMINI_API_KEY','GOOGLE_VERTEX_BASE_URL','GOOGLE_GEMINI_BASE_URL','GOOGLE_APPLICATION_CREDENTIALS','GCLOUD_PROJECT'];
const results=[];const originalFetch=globalThis.fetch;const originalWarn=console.warn,originalDebug=console.debug;
try {for(const spec of cases){for(const key of envKeys)delete process.env[key];Object.assign(process.env,spec.env??{});let warnings=[];console.warn=(...v)=>warnings.push(String(v[0]));console.debug=(...v)=>warnings.push(String(v[0]));let events=[],requests=[],logical=null,order=[],sleeps=[],auth=[];
 const originalGet=GoogleAuth.prototype.getRequestHeaders;
 GoogleAuth.prototype.getClient=async()=>{throw new Error('host discovery forbidden');};
 GoogleAuth.prototype.getRequestHeaders=async function(url){order.push('auth');auth.push({url,keyFilename:this.keyFilename??null,scopes:this.scopes});if(spec.authError){const error=new Error('synthetic auth failure');if(spec.authError!=='other')error.status=spec.authError==='undefined'?undefined:Number(spec.authError);throw error;}return new Headers(spec.authHeaders??{Authorization:'Bearer synthetic-adc','x-goog-user-project':'synthetic-quota'});};
 class GoogleGenAI extends ActualGoogleGenAI {constructor(opts){if(!opts.apiKey)order.push('auth_create');super(opts);}}

 const fake=async(url,init)=>{order.push('send');const headers=Object.fromEntries(new Headers(init.headers));for(const key of ['user-agent','x-goog-api-client'])if(headers[key]?.startsWith('google-genai-sdk/1.52.0 gl-node/'+process.version))headers[key]=headers[key].replace('gl-node/'+process.version,'pi-ai-rust');requests.push({url:String(url),method:init.method,headers,body:JSON.parse(init.body)});let index=0;const chunks=spec.chunks??[sse([done])];return new Response(spec.status===204?null:new ReadableStream({pull(c){if(index<chunks.length)c.enqueue(new TextEncoder().encode(chunks[index++]));else c.close();}},{highWaterMark:0}),{status:spec.status??200,headers:{'content-type':spec.contentType??'text/event-stream',...spec.responseHeaders}});};globalThis.fetch=fake;
 const runtime=vm.createContext({Error,JSON,URL,GoogleGenAI,FinishReason,FunctionCallingConfigMode,ResourceScope,ThinkingLevel,TextDecoder,TextEncoder,Response,ReadableStream,Headers,process,structuredClone,AbortController,fetch:fake,Date:class extends Date{static now(){return 7;}},Math:Object.assign(Object.create(Math),{random:()=>0}),setTimeout:(fn,ms)=>{sleeps.push(ms);queueMicrotask(fn);return 1;},clearTimeout:()=>{},partialParse:require('partial-json').parse});
 vm.runInContext(clean(models.slice(models.indexOf('export function calculateCost'),models.indexOf('export function modelsAreEqual'))),runtime);
 for(const [path,text]of texts)vm.runInContext(clean(text),runtime,{filename:path});vm.runInContext('getPiUserAgent=()=>"pi-ai-rust"',runtime);
 const Stream=vm.runInContext('AssistantMessageEventStream',runtime),push=Stream.prototype.push;Stream.prototype.push=function(event){const copy=JSON.parse(JSON.stringify(event));events.push(copy);return push.call(this,event);};
 const target={...model,...spec.model};const transcript=spec.context??context;const options={apiKey:spec.key??'synthetic-key',...spec.options,...(spec.scopedEnv?{env:spec.scopedEnv}:{}),...(spec.headers?{headers:spec.headers}:{}),onPayload:p=>{order.push('payload');logical=JSON.parse(JSON.stringify(p));return spec.replacement;},onResponse:()=>order.push('response')};
 let stream;try{stream=vm.runInContext(spec.simple?'streamSimple':'stream',runtime)(target,transcript,options);}catch(error){stream=null;results.push({...spec,model:target,context:transcript,options:spec.options??{},synchronous:true,sourceMessage:error.message,requests,logical,order,sleeps});continue;}
 const result=await stream.result();const flags={retryable:runtime.isRetryableAssistantError(result),overflow:runtime.isContextOverflow(result)};
 results.push({...spec,model:target,context:transcript,options:spec.options??{},requests,logical,order,sleeps,warnings,auth,events,result,sourceFlags:flags});
GoogleAuth.prototype.getRequestHeaders=originalGet;}}finally{globalThis.fetch=originalFetch;console.warn=originalWarn;console.debug=originalDebug;for(const key of envKeys)delete process.env[key];}
deepStrictEqual(results[0].result?.stopReason,payloadOrder?'stop':'toolUse');deepStrictEqual(results[0].requests.length,1);
const result={authority,evidence:'Pinned complete adapter plus installed SDK; injected global fetch observes SDK arguments before network defaults. Counter/clock start0/7 per case; SDK runtime identity normalized.',cases:results};
if(process.argv.includes('--check'))deepStrictEqual(JSON.parse(JSON.stringify(result)),JSON.parse(readFileSync(new URL(payloadOrder?'./payload-order.json':'./trace.json',import.meta.url),'utf8')));else {const range=process.argv.find(v=>v.startsWith('--range='))?.slice(8).split(':').map(Number);process.stdout.write(JSON.stringify(range?{...result,cases:result.cases.slice(...range)}:result,null,2)+'\n');}
