import { execFileSync } from 'node:child_process';
import { stripTypeScriptTypes, createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { deepStrictEqual } from 'node:assert';
import { createHash } from 'node:crypto';
const upstream='upstream/pi-mono';
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(`${upstream}/package.json`);
const OpenAI=require('openai').default;
const AzureOpenAI=require('openai').AzureOpenAI;
deepStrictEqual(JSON.parse(readFileSync(`${upstream}/node_modules/openai/package.json`,'utf8')).version,'6.40.0');
// Invoke in a clean child environment; never discover host provider configuration.
if (Object.keys(process.env).some(key => /^(OPENAI_|AZURE_|PI_CACHE_RETENTION)/.test(key))) throw new Error('Run with env -i');
globalThis.fetch=()=>{throw new Error('live networking forbidden');};
const hashes={};
const deps={ OpenAI, AzureOpenAI, partialParse:require('partial-json').parse };
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
load('api/azure-openai-responses.ts');

const baseModel={id:'gpt-5.5',name:'Azure fixture',api:'azure-openai-responses',provider:'azure-openai-responses',baseUrl:'https://resource.openai.azure.com',reasoning:true,input:['text','image'],cost:{input:1000000,output:2000000,cacheRead:3000000,cacheWrite:4000000},contextWindow:10000,maxTokens:4096};
const baseContext={systemPrompt:'system',messages:[{role:'user',content:'hi',timestamp:7}]};
const terminal='data: {"type":"response.completed","response":{"id":"r","status":"completed","service_tier":"priority","usage":{"input_tokens":10e0,"output_tokens":3.0,"total_tokens":13e0,"input_tokens_details":{"cached_tokens":2.0}}}}\n\n';
const text='data: {"type":"response.output_item.added","output_index":0.0,"item":{"type":"message","id":"m"}}\n\ndata: {"type":"response.output_text.delta","output_index":0e0,"delta":"hello"}\n\ndata: {"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"m","phase":"final_answer","content":[{"type":"output_text","text":"hello"}]}}\n\n';
const tool={name:'read',description:'Read',parameters:{type:'object',properties:{input:{type:'string'}},required:['input']}};
const encode=events=>events.map(event=>'data: '+JSON.stringify(event)+'\n\n').join('');
const replay={role:'assistant',api:baseModel.api,provider:baseModel.provider,model:baseModel.id,usage:{input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}},stopReason:'toolUse',timestamp:7,content:[{type:'thinking',thinking:'',thinkingSignature:'{"type":"reasoning","id":"rs","summary":[],"encrypted_content":"opaque"}'},{type:'toolCall',id:'call|item',name:'read',arguments:{input:'x'}}]};
const cases=[
 {id:'default'},
 ...['openai.azure.com','cognitiveservices.azure.com','ai.azure.com'].flatMap(host=>['','/openai','/openai/v1/responses','/custom','/openai/v1/'].map(path=>({id:'url-'+host+path,options:{azureBaseUrl:' https://resource.'+host+path+'?key=synthetic-query#fragment '}}))),
 {id:'proxy-query',options:{azureBaseUrl:'https://proxy.invalid/v1?x=one&x=last&2=two&api-version=old',azureApiVersion:"v1 !*'()"}},
 {id:'existing-deployments',options:{azureBaseUrl:'https://resource.openai.azure.com/openai/deployments/existing',azureDeploymentName:'chosen'}},
 {id:'resource-option',options:{azureResourceName:'option-resource'}},
 {id:'resource-ambient',ambient:{AZURE_OPENAI_RESOURCE_NAME:'ambient-resource'}},
 {id:'resource-scoped',options:{env:{AZURE_OPENAI_RESOURCE_NAME:'scoped-resource'}},ambient:{AZURE_OPENAI_RESOURCE_NAME:'ambient-resource'}},
 {id:'base-option-over-all',options:{azureBaseUrl:'https://option.invalid/v1',azureResourceName:'option-resource',env:{AZURE_OPENAI_BASE_URL:'https://scoped.invalid/v1'}},ambient:{AZURE_OPENAI_BASE_URL:'https://ambient.invalid/v1'}},
 {id:'base-env-before-resource-option',options:{azureResourceName:'ignored-resource'},ambient:{AZURE_OPENAI_BASE_URL:'https://ambient.invalid/v1'}},
 {id:'empty-scoped-falls-back',options:{azureBaseUrl:' ',azureApiVersion:'',env:{AZURE_OPENAI_BASE_URL:'',AZURE_OPENAI_API_VERSION:''}},ambient:{AZURE_OPENAI_BASE_URL:'https://ambient.invalid/v1',AZURE_OPENAI_API_VERSION:'ambient-version'}},
 {id:'space-scoped-does-not-fallback',options:{env:{AZURE_OPENAI_BASE_URL:' '}},ambient:{AZURE_OPENAI_BASE_URL:'https://ignored.invalid/v1'}},
 {id:'api-version-option',options:{azureApiVersion:'custom',env:{AZURE_OPENAI_API_VERSION:'ignored'}},ambient:{AZURE_OPENAI_API_VERSION:'also-ignored'}},
 {id:'mapped-deployment',options:{env:{AZURE_OPENAI_DEPLOYMENT_NAME_MAP:' bad,=bad, gpt-5.5 = first , gpt-5.5=chosen=ignored , x = y '}}},
 {id:'empty-map-value-fallback',options:{env:{AZURE_OPENAI_DEPLOYMENT_NAME_MAP:'gpt-5.5= , gpt-5.5=   '}}},
 {id:'deployment-option',options:{azureDeploymentName:' option ',env:{AZURE_OPENAI_DEPLOYMENT_NAME_MAP:'gpt-5.5=ignored'}}},
 {id:'empty-scoped-deployment-map',options:{env:{AZURE_OPENAI_DEPLOYMENT_NAME_MAP:''}},ambient:{AZURE_OPENAI_DEPLOYMENT_NAME_MAP:'gpt-5.5=ambient-deployment'}},
 {id:'runtime-sentinel',model:{baseUrl:''},options:{azureResourceName:'configured'}},
 {id:'auth-required',noKey:true,options:{headers:{Authorization:'Bearer header-only'}}},
 {id:'api-key-null',options:{headers:{'api-key':null}}},
 {id:'headers',options:{headers:{'api-key':'synthetic-override',Authorization:'Bearer explicit',Accept:null,'Content-Type':null,'User-Agent':'custom'},timeoutMs:1001}},
 {id:'header-case-null-then-value',options:{headers:{Accept:null,accept:'last'}}},
 {id:'header-case-value-then-null',options:{headers:{Accept:'first',accept:null}}},
 {id:'invalid-header-lazy',options:{headers:{'bad name':null}}},
 {id:'invalid-header-eager',options:{headers:{'bad name':null}},ambient:{OPENAI_CUSTOM_HEADERS:'X-Custom: env'}},
 {id:'sdk-env-defaults',ambient:{OPENAI_ORG_ID:'synthetic-org',OPENAI_PROJECT_ID:'synthetic-project',OPENAI_CUSTOM_HEADERS:'X-Custom: env\nAccept: custom'},options:{headers:{'X-Custom':null}}},
 {id:'cache-none-still-key',options:{cacheRetention:'none',sessionId:'s'.repeat(67)},model:{compat:{supportsExplicitPromptCacheMode:true,sessionAffinityFormat:'openrouter'}}},
 {id:'cache-long-no-retention',options:{cacheRetention:'long',sessionId:'session'}},
 ...[0,1,15,16,17].map(maxTokens=>({id:'tokens-'+maxTokens,options:{maxTokens}})),
 {id:'reasoning-summary-null',options:{reasoningSummary:null}},
 {id:'reasoning-high-null',options:{reasoningEffort:'high',reasoningSummary:null}},
 {id:'summary-only',options:{reasoningSummary:'concise'}},
 {id:'mapped-effort',options:{reasoningEffort:'max'},model:{thinkingLevelMap:{max:'custom'}}},
 {id:'off-null',model:{thinkingLevelMap:{off:null}}},
 {id:'copilot-not-standard-special-case',model:{provider:'github-copilot'},options:{sessionId:'session'}},
 {id:'xai-not-standard-include',model:{provider:'xai'}},
 {id:'tools-default-strict',context:{tools:[tool]},options:{toolChoice:'required'}},
 {id:'tools-strict-false',context:{tools:[tool]},model:{compat:{supportsStrictMode:false}}},
 {id:'all-tools-ignore-deferred-mode',context:{tools:[tool,{...tool,name:'late'}],messages:[{role:'toolResult',toolCallId:'loader',toolName:'load',content:[],isError:false,timestamp:7,addedToolNames:['late']}]},model:{compat:{supportsAdditionalTools:true,supportsToolSearch:true}}},
 {id:'grammar',model:{compat:{supportsOpenAIGrammarTools:true}},context:{tools:[{...tool,constrainedSampling:{type:'grammar',variants:{openai_lark:'start: /x/'}}}]}},
 {id:'sampling-last-no-tier-pricing',options:{samplingParams:{model:'sampled',max_output_tokens:2,store:true,service_tier:'priority'},maxTokens:20}},
 {id:'simple',simple:true,options:{reasoning:'max',toolChoice:'none'},model:{thinkingLevelMap:{max:'high'},samplingParams:{temperature:0.25}}},
 {id:'replacement',replacement:{model:'replacement',stream:true,input:[]}},
 {id:'decode-object',replacement:{stream:false},contentType:'application/json',sse:'{"object":"response","output":null}'},
 {id:'decode-primitive',replacement:{stream:false},contentType:'application/json',sse:'500'},
 {id:'decode-array',replacement:{stream:false},contentType:'application/json',sse:'[{"type":"response.completed","response":{"status":"completed"}}]'},
 {id:'http-overflow',status:400,sse:'{"error":{"message":"prompt is too long"}}'},
 {id:'http-billing',status:429,sse:'{"error":{"message":"billing"}}'},
 {id:'http-empty',status:400,sse:''},
 {id:'http-cap-overflow',status:429,sse:'{"error":{"message":"prompt is too long"}}',options:{maxRetries:1,maxRetryDelayMs:1},responseHeaders:{'retry-after-ms':'2'}},
 {id:'http-cap-veto',status:503,sse:'{"error":{"message":"billing"}}',options:{maxRetries:1,maxRetryDelayMs:1},responseHeaders:{'retry-after-ms':'2'}},
 {id:'sse-error',sse:text+'data: {"error":{"message":"server error","metadata":{"raw":"billing"}}}\n\n'},
 {id:'missing-terminal',sse:text},
 {id:'no-body',status:204,sse:''},
 {id:'done-trailing',sse:text+terminal+'data: [DONE]\n\ndata: malformed ignored\n\n'},
 {id:'azure-reasoning-and-function-events',context:{tools:[tool]},sse:encode([
  {type:'response.output_item.done',output_index:0,item:{type:'reasoning',id:'rs',summary:[{text:'reason'}],encrypted_content:'done-opaque'}},
  {type:'response.output_item.added',output_index:1,item:{type:'function_call',id:'item',call_id:'call',name:'read',arguments:''}},
  {type:'response.function_call_arguments.delta',output_index:1,delta:'{"input":"x"}'},
  {type:'response.output_item.done',output_index:1,item:{type:'function_call',id:'item',call_id:'call',name:'read',arguments:'{"input":"x"}',namespace:'dynamic'}},
  {type:'response.completed',response:{id:'r',status:'completed',output:[{type:'reasoning',id:'rs',encrypted_content:'completion-opaque'}],usage:{input_tokens:10,output_tokens:3,total_tokens:13}}}
 ])},
 {id:'azure-signed-replay',context:{tools:[tool],messages:[replay,{role:'toolResult',toolCallId:'call|item',toolName:'read',content:[{type:'text',text:'result'}],isError:false,timestamp:7},{role:'user',content:'continue',timestamp:7}]}},
 {id:'azure-custom-tool-events',model:{compat:{supportsOpenAIGrammarTools:true}},context:{tools:[{...tool,constrainedSampling:{type:'grammar',variants:{openai_lark:'start: /x/'}}}]},sse:encode([
  {type:'response.output_item.added',output_index:0,item:{type:'custom_tool_call',id:'item',call_id:'call',name:'read',input:''}},
  {type:'response.custom_tool_call_input.delta',output_index:0,delta:'x\n'},
  {type:'response.output_item.done',output_index:0,item:{type:'custom_tool_call',id:'item',call_id:'call',name:'read',input:'x\n'}}
 ])+terminal},
];
const outputs=[];
for(const item of cases){
 const model={...baseModel,...item.model},context={...baseContext,...item.context},options={...(item.noKey?{}:{apiKey:'synthetic-key'}),...item.options},order=[],requests=[];
 for(const[key,value]of Object.entries(item.ambient??{}))process.env[key]=value;
 options.onPayload=()=>{order.push('payload');return item.replacement;};options.onResponse=()=>{order.push('response');};
 const sse=item.sse??(text+terminal);
 options.fetch=async(url,init)=>{order.push('send');const headers=Object.fromEntries(init.headers);for(const key of Object.keys(headers))if(key.startsWith('x-stainless-')&&!['x-stainless-timeout','x-stainless-retry-count'].includes(key))delete headers[key];requests.push({url,headers,body:JSON.parse(init.body)});return new Response(item.status===204?null:sse,{status:item.status??200,headers:{'content-type':item.contentType??'text/event-stream',...item.responseHeaders}});};
 const stream=(item.simple?deps.streamSimple:deps.stream)(model,context,options);await stream.complete;
 for(const key of Object.keys(item.ambient??{}))delete process.env[key];
 const last=stream.events.at(-1),result=last.message??last.error;
 outputs.push({id:item.id,model,context,options:item.options??{},ambient:item.ambient??{},replacement:item.replacement,noKey:item.noKey??false,simple:item.simple??false,status:item.status??200,contentType:item.contentType??'text/event-stream',responseHeaders:item.responseHeaders??{},sse,requests,order,events:stream.events,retryable:deps.isRetryableAssistantError(result),contextOverflow:deps.isContextOverflow(result)});
}
const output={sourceCommit:commit,sdkVersion:'6.40.0',evidenceClass:'executed-pinned-Azure-wrapper-and-installed-SDK',sourceHashes:hashes,cases:outputs};
if(process.argv.includes('--check'))deepStrictEqual(JSON.parse(JSON.stringify(output)),JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));else process.stdout.write(JSON.stringify(output,null,process.argv.includes('--compact')?0:2)+'\n');
