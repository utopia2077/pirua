import { execFileSync } from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import { stripTypeScriptTypes, createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { deepStrictEqual } from 'node:assert';
import { createHash } from 'node:crypto';
const upstream=upstreamRoot;
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
for(const path of ['utils/hash.ts','utils/json-parse.ts','utils/sanitize-unicode.ts','api/constrained-sampling.ts','api/transform-messages.ts','utils/deferred-tools.ts','utils/estimate.ts','api/simple-options.ts','api/github-copilot-headers.ts','api/openai-prompt-cache.ts','utils/provider-env.ts','utils/error-body.ts','utils/headers.ts','utils/provider-retry.ts','utils/overflow.ts','utils/retry.ts']) load(path);
deps.getPiUserAgent=()=> 'pi-ai-rust';
deps.Date=class extends Date { static now(){return 7;} };
deps.AssistantMessageEventStream=class {
  events=[]; complete=new Promise(resolve=>{this.resolve=resolve;});
  push(event){this.events.push(JSON.parse(JSON.stringify(event,(key,value)=>['partialArgs','customInput','streamIndex'].includes(key)?undefined:value)));}
  end(){this.resolve();}
};
load('api/openai-completions.ts');

const baseModel={id:'gpt-5.5',name:'Synthetic',api:'openai-completions',provider:'openai',baseUrl:'https://example.invalid/v1',reasoning:true,input:['text','image'],cost:{input:1000000,output:2000000,cacheRead:3000000,cacheWrite:4000000},contextWindow:10000,maxTokens:4096};
const baseContext={systemPrompt:'system',messages:[{role:'user',content:'hi',timestamp:0}]};
const chunk=(delta={},finish_reason=null,extra={})=>({id:'chat_1',model:'reported',choices:[{delta,finish_reason}],...extra});
const encode=chunks=>chunks.map(v=>'data: '+JSON.stringify(v)+'\n\n').join('');
const terminal=chunk({},'stop',{usage:{prompt_tokens:100,completion_tokens:3,cached_tokens:2,prompt_tokens_details:{cache_write_tokens:1},completion_tokens_details:{reasoning_tokens:1}}});
const tool={name:'read',description:'Read',parameters:{type:'object',properties:{input:{type:'string'}},required:['input']}};
const assistant={role:'assistant',api:baseModel.api,provider:baseModel.provider,model:baseModel.id,content:[{type:'thinking',thinking:'Thought',thinkingSignature:'reasoning'},{type:'text',text:'answer'},{type:'toolCall',id:'call|item',name:'read',arguments:{input:'x'}}],usage:{input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}},stopReason:'toolUse',timestamp:0};
const toolResult={role:'toolResult',toolCallId:'call|item',toolName:'read',content:[{type:'image',mimeType:'image/png',data:'AA=='}],isError:false,timestamp:1};
const cases=[
{id:'cache-without-tools',model:{provider:'openrouter',id:'anthropic/model'}},
{id:'baseten-null-effort',model:{compat:{thinkingFormat:'baseten'},thinkingLevelMap:{high:null}},options:{reasoningEffort:'high'}},
{id:'details-empty-fields',chunks:[chunk({reasoning_details:[{type:'reasoning.text',text:'a',id:null,format:'',signature:null}]}),chunk({reasoning_details:[{type:'reasoning.text',text:'b'}]}),terminal]},
{id:'tool-id-aliases',chunks:[chunk({tool_calls:[{index:0,id:'initial',function:{name:'read',arguments:'{"input":'}}]}),chunk({tool_calls:[{index:0,id:'changed',function:{name:null,arguments:'"x"'}}]}),chunk({tool_calls:[{id:'changed',function:{arguments:'}'}}]}),chunk({},'tool_calls')]},
{id:'function-empty-custom',chunks:[chunk({tool_calls:[{index:0,id:'c',function:{name:'read',arguments:'{}'},custom:{}}]}),chunk({},'tool_calls')]},
{id:'null-chunks',chunks:[null,17,[],terminal]},
{id:'nonstream-text',replacement:{stream:false},sse:'text'},
{id:'zai-tool-stream',model:{compat:{zaiToolStream:true}},context:{tools:[tool]}},
{id:'opencode-go-reasoning',model:{provider:'opencode-go'},chunks:[chunk({reasoning:'thought'}),terminal]},
{id:'strict-schema',context:{tools:[{...tool,constrainedSampling:{type:'json_schema',strict:'require'}}]}},
{id:'default',chunks:[chunk({content:'hi',reasoning:'thinking'}),terminal]},
...[0,1,4096].map(maxTokens=>({id:'tokens-'+maxTokens,options:{maxTokens}})),
...['zai','qwen','qwen-chat-template','chat-template','baseten','deepseek','openrouter','ant-ling','together','string-thinking','openai'].flatMap(thinkingFormat=>[false,true].map(on=>({id:thinkingFormat+'-'+on,model:{compat:{thinkingFormat,supportsReasoningEffort:true,thinkingTokenBudgetField:'thinking_budget',chatTemplateKwargs:{enable:{$var:'thinking.enabled'},effort:{$var:'thinking.effort'},budget:{$var:'thinking.budget'},optional:{$var:'thinking.effort',omitWhenOff:true},literal:null},chatTemplateArgs:{effort:{$var:'thinking.effort'}}},thinkingLevelMap:{off:'off-value',max:'max-value'}},options:on?{reasoningEffort:'max'}:{}}))),
...['zai','together','moonshotai','openrouter','cloudflare-workers-ai','cloudflare-ai-gateway','nvidia','ant-ling','deepseek','xai','cerebras','opencode','github-copilot'].map(provider=>({id:'provider-'+provider,model:{provider},options:{cacheRetention:'long',sessionId:'session',reasoningEffort:'high'},context:{tools:[tool]}})),
...['https://api.z.ai/v1','https://api.together.xyz/v1','https://api.moonshot.cn/v1','https://openrouter.ai/v1','https://api.cloudflare.com/v1','https://gateway.ai.cloudflare.com/v1','https://integrate.api.nvidia.com/v1','https://api.ant-ling.com/v1','https://DEEPSEEK.COM/v1','https://api.x.ai/v1','https://cerebras.ai/v1','https://opencode.ai/v1','https://chutes.ai/v1','https://api.openai.com/v1'].map((endpoint,i)=>({id:'endpoint-'+i,endpoint,options:{sessionId:'s',cacheRetention:'short'}})),
{id:'thinking-map-null',model:{thinkingLevelMap:{off:null,high:null},compat:{thinkingFormat:'zai',supportsReasoningEffort:true}},options:{reasoningEffort:'high'}},
{id:'cache-anthropic',model:{provider:'openrouter',id:'anthropic/model'},context:{tools:[tool]},options:{cacheRetention:'long'}},
{id:'routing',model:{compat:{openRouterRouting:{order:['one'],allow_fallbacks:false},vercelGatewayRouting:{only:[],order:['two']}}}},
{id:'replay',context:{messages:[assistant,toolResult,{role:'user',content:'next',timestamp:2}],tools:[tool]},model:{compat:{requiresAssistantAfterToolResult:true,requiresToolResultName:true,requiresThinkingAsText:true}}},
{id:'replay-reasoning',context:{messages:[assistant]}},
{id:'replay-details',context:{messages:[{...assistant,content:[{type:'thinking',thinking:'secret',thinkingSignature:JSON.stringify([{type:'reasoning.encrypted',data:'opaque',index:1}])},{type:'text',text:'answer'}]}]}},
{id:'kimi-deferred',model:{compat:{deferredToolsMode:'kimi'}},context:{messages:[assistant,{...toolResult,addedToolNames:['read']}],tools:[tool]}},
{id:'grammar',model:{compat:{supportsOpenAIGrammarTools:true}},context:{tools:[{...tool,constrainedSampling:{type:'grammar',variants:{openai_lark:'start: /x/'}}}]},chunks:[chunk({tool_calls:[{index:0,id:'c',custom:{name:'read',input:'x'}}]}),chunk({tool_calls:[{index:0,custom:{input:'\n"'}}]}),chunk({},'tool_calls')]},
{id:'reasoning-details',chunks:[chunk({reasoning_content:'thought',reasoning_details:[{type:'reasoning.text',text:'first',index:0}]}),chunk({reasoning_details:[{type:'reasoning.text',text:' last',signature:'sig',format:'fmt',id:null,index:2}]}),chunk({reasoning_details:[{type:'reasoning.summary',summary:'sum'},{type:'reasoning.encrypted',data:'opaque',id:'r'}]}),terminal]},
{id:'tool-fragments',chunks:[chunk({tool_calls:[{index:0,id:'c',function:{name:'read',arguments:'{"input":'}}]}),chunk({tool_calls:[{id:'c',function:{arguments:'"x"}'}}]}),chunk({},'tool_calls')]},
{id:'split-identities',chunks:[chunk({tool_calls:[{index:0,function:{arguments:'{"input":'}}]}),chunk({content:'interleaved',tool_calls:[{index:1,id:'other',function:{name:'read',arguments:'{}'}},{index:0,id:'c'}]}),chunk({tool_calls:[{index:0,function:{name:'read',arguments:'"x"}'}}]}),chunk({},'tool_calls')],publicationNormalization:true},
{id:'missing-id',chunks:[chunk({tool_calls:[{index:0,function:{name:'read',arguments:'{}'}}]}),chunk({},'tool_calls')],invalidIdentity:true},
{id:'usage-clamp',chunks:[chunk({},'stop',{usage:{prompt_tokens:-5,cached_tokens:2,completion_tokens:3}})]},
{id:'choice-usage',chunks:[{choices:[{finish_reason:'end',delta:{},usage:{prompt_tokens:4,completion_tokens:2,cached_tokens:1}}]}]},
{id:'no-finish',chunks:[chunk({content:'partial'})]},
{id:'optional-finish',model:{compat:{supportsFinishReason:false}},chunks:[chunk({content:'partial'})]},
...['content_filter','network_error','new_reason'].map(reason=>({id:'finish-'+reason,chunks:[chunk({},reason)]})),
{id:'http-overflow',status:400,sse:'{"error":{"message":"context length exceeded private"}}'},
{id:'http-quota',status:429,sse:'{"error":{"message":"private","code":"insufficient_quota"}}'},
{id:'http-no-body',status:400,sse:'{}'},
{id:'sdk-error',chunks:[chunk({content:'partial'}),{error:{message:'server error private',metadata:{raw:'context length exceeded'}}}]},
{id:'simple',simple:true,options:{reasoning:'max'},model:{contextWindow:4500,samplingParams:{temperature:0.1}}},
{id:'raw-omits-model-sampling',model:{samplingParams:{temperature:0.1}}},
{id:'sampling-last',options:{samplingParams:{stream_options:null,store:true,max_completion_tokens:8}}},
{id:'headers',options:{headers:{authorization:'Bearer synthetic',Accept:null,'content-type':null},timeoutMs:1001,sessionId:'s'},model:{compat:{sendSessionAffinityHeaders:true}}},
...['openai','openai-nosession','openrouter'].map(sessionAffinityFormat=>({id:'affinity-'+sessionAffinityFormat,model:{compat:{sessionAffinityFormat,sendSessionAffinityHeaders:true}},options:{sessionId:'s'}})),
{id:'cache-none',options:{cacheRetention:'none',sessionId:'s'},model:{compat:{sendSessionAffinityHeaders:true}}},
{id:'cache-env',options:{env:{PI_CACHE_RETENTION:'long'},sessionId:'s'}},
{id:'nonstream-array',replacement:{stream:false},contentType:'application/json',sse:JSON.stringify([terminal])},
{id:'nonstream-object',replacement:{stream:false},contentType:'application/json',sse:'{}'},
{id:'done-trailing',sse:encode([terminal])+'data: [DONE]suffix\n\ndata: invalid\n\n'},
{id:'unterminated',sse:encode([terminal]).trimEnd()},
{id:'grammar-tool-choice',options:{toolChoice:{type:'custom',custom:{name:'read'}}}},
{id:'allowed-tool-choice',options:{toolChoice:{type:'allowed_tools',allowed_tools:{mode:'required',tools:[{type:'function',function:{name:'read'}}]}}}},
];
const outputs=[];
for(const item of cases) {
 const model={...baseModel,...item.model},context={...baseContext,...item.context},options={apiKey:'synthetic-key',...item.options},requests=[],order=[];
 options.onPayload=()=>{order.push('payload');return item.replacement;};
 options.onResponse=()=>{order.push('response');};
 const sse=item.sse??encode(item.chunks??[terminal]);
 options.fetch=async(url,init)=>{
  order.push('send');const headers=Object.fromEntries(init.headers);
  for(const k of Object.keys(headers)) if(k.startsWith('x-stainless-')&&!['x-stainless-timeout','x-stainless-retry-count'].includes(k)) delete headers[k];
  requests.push({url,headers,body:JSON.parse(init.body)});
  return new Response(sse,{status:item.status??200,headers:{'content-type':item.contentType??'text/event-stream'}});
 };
 const stream=(item.simple?deps.streamSimple:deps.stream)(item.endpoint?{...model,baseUrl:item.endpoint}:model,context,options);
 await stream.complete;const terminalEvent=stream.events.at(-1),message=terminalEvent.message??terminalEvent.error;
 outputs.push({id:item.id,model,context,options:item.options??{},simple:item.simple??false,replacement:item.replacement,endpoint:item.endpoint,status:item.status??200,contentType:item.contentType??'text/event-stream',sse,requests,order,events:stream.events,retryable:deps.isRetryableAssistantError(message),contextOverflow:deps.isContextOverflow(message),publicationNormalization:item.publicationNormalization??false,invalidIdentity:item.invalidIdentity??false});
}
const result={sourceCommit:commit,evidenceClass:'executed-pinned-wrapper-and-installed-sdk',sdkVersion:'6.40.0',sourceHashes:hashes,normalizations:['Date.now=7','Pi user agent=pi-ai-rust','SDK platform headers omitted','snapshots copied at push; scratch-only fields omitted'],cases:outputs};
if(process.argv.includes('--check')) deepStrictEqual(JSON.parse(JSON.stringify(result)),JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
