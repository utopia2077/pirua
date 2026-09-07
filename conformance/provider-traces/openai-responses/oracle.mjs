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
const baseModel={id:'gpt-5.5',name:'Synthetic',api:'openai-responses',provider:'openai',baseUrl:'https://example.invalid/v1',reasoning:true,input:['text','image'],cost:{input:1000000,output:2000000,cacheRead:3000000,cacheWrite:4000000},contextWindow:10000,maxTokens:1000};
const baseContext={systemPrompt:'system',messages:[{role:'user',content:'hi',timestamp:0}]};
const terminal='data: {"type":"response.completed","response":{"id":"r","status":"completed","usage":{"input_tokens":1e2,"output_tokens":3.0,"total_tokens":1.03e2,"input_tokens_details":{"cached_tokens":2.0,"cache_write_tokens":-0.0}}}}\n\n';
const text='data: {"type":"response.output_item.added","output_index":1.0,"item":{"type":"message","id":"m"}}\n\ndata: {"type":"response.output_text.delta","output_index":1e0,"delta":"hello"}\n\ndata: {"type":"response.output_item.done","output_index":1,"item":{"type":"message","id":"m","phase":"final_answer","content":[{"type":"output_text","text":"hello"}]}}\n\n';
const tool={name:'query',description:'synthetic',parameters:{type:'object',properties:{input:{type:'string'}},required:['input']}};
const cases=process.argv.includes('--header-stage') ? [
  {id:'lazy-null-header-validation',options:{headers:{'bad name':null}}},
  {id:'eager-null-header-validation',options:{headers:{'bad name':null}},clientCustomHeaders:'X-Custom: synthetic'},
] : [
  {id:'default',sse:text+terminal},
  ...[0,1,15,16,17].map(maxTokens=>({id:`tokens-${maxTokens}`,options:{maxTokens}})),
  {id:'nullable-summary',options:{reasoningSummary:null,serviceTier:null}},
  {id:'effort-summary-null',options:{reasoningEffort:'high',reasoningSummary:null}},
  {id:'summary-alone',options:{reasoningSummary:'concise'}},
  {id:'off-null',model:{thinkingLevelMap:{off:null}}},
  {id:'effort-map',options:{reasoningEffort:'max'},model:{thinkingLevelMap:{max:'deep'}}},
  {id:'xai',model:{provider:'xai',thinkingLevelMap:{off:null}}},
  {id:'copilot',model:{provider:'github-copilot'}},
  {id:'cache-long',options:{cacheRetention:'long',sessionId:'s'.repeat(67)}},
  {id:'cache-none',options:{cacheRetention:'none',sessionId:'s'},model:{compat:{supportsExplicitPromptCacheMode:true}}},
  {id:'cache-env',options:{env:{PI_CACHE_RETENTION:'long'}}},
  {id:'cache-unsupported',options:{cacheRetention:'long'},model:{compat:{supportsLongCacheRetention:false}}},
  ...['openrouter','openai-nosession','openai'].map(sessionAffinityFormat=>({id:sessionAffinityFormat,options:{sessionId:'session'},model:{compat:{sessionAffinityFormat}}})),
  {id:'headers',options:{sessionId:'session',headers:{Authorization:'Bearer header',session_id:null,'Content-Type':null,Accept:'custom','X-Stainless-Retry-Count':null},timeoutMs:1001}},
  {id:'header-auth',noKey:true,options:{headers:{'cf-aig-authorization':'synthetic'}}},
  {id:'required',options:{toolChoice:'required',temperature:0},context:{tools:[tool]}},
  {id:'grammar',model:{compat:{supportsOpenAIGrammarTools:true}},context:{tools:[{...tool,constrainedSampling:{type:'grammar',variants:{openai_lark:'start: /x/'}}}]}},
  {id:'strict',model:{compat:{supportsStrictMode:true,supportsDeveloperRole:false}},context:{tools:[tool]}},
  {id:'sampling-last',options:{maxTokens:20,samplingParams:{max_output_tokens:3,model:'override',store:true,temperature:0}}},
  {id:'replacement',replacement:{model:'replaced',input:[],stream:true}},
  {id:'simple',simple:true,model:{contextWindow:4500,samplingParams:{temperature:0.1,custom:1}},options:{reasoning:'max',samplingParams:{custom:2}}},
  {id:'priority',options:{serviceTier:'priority'}},
  {id:'flex',options:{serviceTier:'flex'}},
  {id:'response-tier',options:{serviceTier:'priority'},sse:terminal.replace('"status":"completed"','"status":"completed","service_tier":"flex"')},
  {id:'incomplete',sse:terminal.replace('"status":"completed"','"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}')},
  {id:'priority-other',model:{id:'gpt-5.4'},options:{serviceTier:'priority'}},
  {id:'nonreasoning',model:{reasoning:false},options:{reasoningEffort:'high'}},
  ...['supportsAdditionalTools','supportsToolSearch'].map(compat=>({id:compat,model:{compat:{[compat]:true}},context:{tools:[tool,{...tool,name:'late'}],messages:[...baseContext.messages,{role:'toolResult',toolCallId:'load',toolName:'loader',content:[],isError:false,timestamp:1,addedToolNames:['late']}]}})),
  {id:'reasoning-and-tool',sse:'data: {"type":"response.output_item.done","output_index":0.0,"item":{"type":"reasoning","id":"rs","summary":[{"text":"thought"}],"metadata":{"z":1.0,"2":-0.0}}}\n\ndata: {"type":"response.output_item.added","output_index":1e0,"item":{"type":"function_call","id":"fc","call_id":"call","name":"query","arguments":""}}\n\ndata: {"type":"response.function_call_arguments.delta","output_index":1.0,"delta":"{\\"input\\":\\"x\\"}"}\n\ndata: {"type":"response.output_item.done","output_index":1,"item":{"type":"function_call","id":"fc","call_id":"call","name":"query","arguments":"{\\"input\\":\\"x\\"}","namespace":"dynamic"}}\n\n'+terminal.replace('"usage":','"output":[{"type":"reasoning","id":"rs","encrypted_content":"opaque"}],"usage":')},
  {id:'endpoint-query',endpoint:'https://example.invalid/v1/?x=a%20b&x=c%20d'},
  {id:'missing-terminal',sse:text},
  {id:'http-overflow',status:400,sse:'{"error":{"message":"context length exceeded synthetic-private"}}'},
  {id:'http-quota',status:429,sse:'{"error":{"message":"synthetic-private","code":"insufficient_quota"}}'},
  {id:'provider-error',sse:text+'data: {"type":"error","code":"unknown","message":"server error synthetic-private"}\n\n'},
  {id:'sdk-error',sse:text+'data: {"error":{"message":"context length exceeded synthetic-private"}}\n\n'},
  {id:'unterminated-terminal',sse:terminal.trimEnd()},
  {id:'single-lf-terminal',sse:terminal.trimEnd()+'\n'},
  {id:'cr-terminal',sse:terminal.replaceAll('\n','\r')},
  {id:'content-filter',sse:terminal.replace('"status":"completed"','"status":"incomplete","incomplete_details":{"reason":"content_filter"}')},
  {id:'done-ignores-trailing',sse:terminal+'data: [DONE]suffix\n\ndata: {"type":"error","message":"ignored"}\n\n'},
  {id:'nonstream-array',replacement:{stream:false},contentType:'application/json',sse:'[{"type":"response.completed","response":{"status":"completed"}}]'},
  {id:'nonstream-text',replacement:{stream:false},sse:'plain text'},
  {id:'null-body',status:204,sse:''},
  {id:'custom-tool-events',model:{compat:{supportsOpenAIGrammarTools:true}},context:{tools:[{...tool,constrainedSampling:{type:'grammar',variants:{openai_lark:'start: /x/'}}}]},sse:'data: {"type":"response.output_item.added","output_index":0.0,"item":{"type":"custom_tool_call","id":"ctc","call_id":"call","name":"query","input":""}}\n\ndata: {"type":"response.custom_tool_call_input.delta","output_index":0e0,"delta":"x"}\n\ndata: {"type":"response.output_item.done","output_index":0,"item":{"type":"custom_tool_call","id":"ctc","call_id":"call","name":"query","input":"x"}}\n\n'+terminal},
];
const outputs=[];
for(const item of cases) {
  const model={...baseModel,...item.model},context={...baseContext,...item.context}, options={...(item.noKey?{}:{apiKey:'synthetic-key'}),...item.options};
  const order=[], requests=[];
  options.onPayload=payload=>{order.push('payload'); if(item.replacement) return item.replacement;};
  options.onResponse=()=>{order.push('response');};
  const sse=item.sse??terminal;
  options.fetch=async(url,init)=> {
    order.push('send');
    const headers=Object.fromEntries(init.headers);
    for(const key of Object.keys(headers)) if(key.startsWith('x-stainless-')&&!['x-stainless-timeout','x-stainless-retry-count'].includes(key)) delete headers[key];
    requests.push({url,headers,body:JSON.parse(init.body)});
    return new Response(item.status===204?null:sse,{status:item.status??200,headers:{'content-type':item.contentType??'text/event-stream'}});
  };
  if(item.clientCustomHeaders)process.env.OPENAI_CUSTOM_HEADERS=item.clientCustomHeaders;
  const stream=(item.simple?deps.streamSimple:deps.stream)(item.endpoint?{...model,baseUrl:item.endpoint}:model,context,options);
  await stream.complete;
  delete process.env.OPENAI_CUSTOM_HEADERS;
  const result=stream.events.at(-1);const message=result.message??result.error;
  outputs.push({id:item.id,model,context,options:item.options??{},noKey:item.noKey??false,simple:item.simple??false,replacement:item.replacement,endpoint:item.endpoint,status:item.status??200,contentType:item.contentType??'text/event-stream',sse,requests,order,events:stream.events,retryable:deps.isRetryableAssistantError(message),contextOverflow:deps.isContextOverflow(message),clientCustomHeaders:item.clientCustomHeaders});
}
const result={sourceCommit:commit,evidenceClass:'executed-pinned-wrapper-and-installed-sdk',sdkVersion:'6.40.0',sourceHashes:hashes,normalizations:['Date.now=7','Pi user agent=pi-ai-rust','SDK platform headers omitted','event snapshots copied at push; scratch-only fields omitted'],cases:outputs};
if(process.argv.includes('--check')) deepStrictEqual(JSON.parse(JSON.stringify(result)),JSON.parse(readFileSync(new URL(process.argv.includes('--header-stage')?'./header-stage.json':'./trace.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
