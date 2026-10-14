import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {createRequire,stripTypeScriptTypes} from 'node:module';
import vm from 'node:vm';
const root=upstreamRoot,commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(`${root}/package.json`);
const source=path=>execFileSync('git',['show',`${commit}:packages/ai/src/${path}`],{cwd:root,encoding:'utf8'});
const clean=text=>stripTypeScriptTypes(text.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const runtime=vm.createContext({Anthropic:require('@anthropic-ai/sdk').default,TextDecoder,Response,Headers,process,structuredClone,setTimeout,clearTimeout,
 Date:class extends Date{static now(){return 7;}},partialParse:require('partial-json').parse});
for(const path of ['utils/json-parse.ts','utils/sanitize-unicode.ts','utils/event-stream.ts','utils/headers.ts','utils/provider-env.ts','utils/pi-user-agent.ts',
 'utils/provider-retry.ts','utils/deferred-tools.ts','api/constrained-sampling.ts','api/github-copilot-headers.ts','api/transform-messages.ts'])vm.runInContext(clean(source(path)),runtime);
vm.runInContext(clean(source('api/anthropic-messages.ts')),runtime);
const stream=vm.runInContext('stream',runtime);
const model={id:'claude-opus-4-6',name:'Claude',api:'anthropic-messages',provider:'anthropic',baseUrl:'https://gateway.invalid/base',reasoning:true,input:['text','image'],
 cost:{input:1,output:2,cacheRead:0.1,cacheWrite:0.2},contextWindow:200000,maxTokens:32000};
const context={systemPrompt:'System',messages:[{role:'user',content:'Hello',timestamp:7}]};
const tool={name:'read',description:'Read',parameters:{type:'object',properties:{path:{type:'string'}},required:['path'],title:'Not in legacy'}};
const cases=[
 {id:'default'},
 {id:'no-cache',options:{cacheRetention:'none'}},
 {id:'scoped-long-cache',options:{env:{PI_CACHE_RETENTION:'long'}}},
 {id:'unsupported-long-cache',model:{compat:{supportsLongCacheRetention:false}},options:{cacheRetention:'long'}},
 {id:'affinity',model:{compat:{sendSessionAffinityHeaders:true}},options:{sessionId:'session'}},
 {id:'affinity-disabled',model:{compat:{sendSessionAffinityHeaders:true}},options:{sessionId:'session',cacheRetention:'none'}},
 {id:'legacy-budget-zero',options:{thinkingEnabled:true,thinkingBudgetTokens:0,temperature:0}},
 {id:'legacy-budget-display',options:{thinkingEnabled:true,thinkingBudgetTokens:2345,thinkingDisplay:'omitted',effort:'max'}},
 {id:'adaptive-effort',model:{compat:{forceAdaptiveThinking:true}},options:{thinkingEnabled:true,effort:'xhigh',thinkingDisplay:'omitted'}},
 {id:'thinking-disabled',options:{thinkingEnabled:false,temperature:0}},
 {id:'disabled-unsupported',model:{thinkingLevelMap:{off:null}},options:{thinkingEnabled:false}},
 {id:'temperature-unsupported',model:{compat:{supportsTemperature:false}},options:{temperature:0}},
 {id:'sampling-ignored',options:{samplingParams:{top_p:0.7,max_tokens:1},metadata:{user_id:'u',other:'ignore'}}},
 {id:'oauth',options:{apiKey:'sk-ant-oat-fixture'},context:{tools:[tool]}},
 {id:'oauth-case-override',options:{apiKey:'sk-ant-oat-fixture',headers:{'User-Agent':'capitalized-override'}},context:{tools:[tool]}},
 {id:'oauth-lowercase-override',options:{apiKey:'sk-ant-oat-fixture',headers:{'user-agent':'explicit-lowercase'}},context:{tools:[tool]}},
 {id:'header-owned',noKey:true,options:{headers:{Authorization:'Bearer header-fixture'}}},
 {id:'cf-header-placeholder',noKey:true,options:{headers:{'cf-aig-authorization':'Bearer fixture','x-api-key':null}}},
 {id:'cf-without-placeholder',noKey:true,options:{headers:{'cf-aig-authorization':'Bearer fixture'}}},
 {id:'copilot',model:{provider:'github-copilot',compat:{forceAdaptiveThinking:true}},context:{tools:[tool],messages:[{role:'user',content:[{type:'image',mimeType:'image/png',data:'eA=='}],timestamp:7}]}},
 {id:'legacy-tool-beta',model:{compat:{supportsEagerToolInputStreaming:false}},context:{tools:[tool]}},
 {id:'tool-cache-unsupported',model:{compat:{supportsCacheControlOnTools:false}},context:{tools:[tool]}},
 {id:'header-null-and-body-override',options:{headers:{'anthropic-beta':null,'content-type':null,'x-custom':' \tvalue\r\n'}}},
 {id:'url-query-append',model:{baseUrl:'https://gateway.invalid/base?x=a+b&x=last&2=numeric#fragment'}},
 {id:'named-tool-choice-oauth',options:{apiKey:'sk-ant-oat-fixture',toolChoice:{type:'tool',name:'read'}},context:{tools:[tool]}},
 {id:'grammar-keeps-json-schema',context:{tools:[{...tool,constrainedSampling:{type:'grammar',variants:{openai_regex:'.*'}}}]}}
];
const results=[];
const warnings=[];const originalWarn=console.warn;
console.warn=(...args)=>warnings.push(String(args[0]));
for(const item of cases){
 let request,logical;const target={...model,...item.model};const transcript={...context,...item.context};
 const options={...(!item.noKey?{apiKey:'fixture-key'}:{}),...item.options};
 const result=await stream(target,transcript,{...options,onPayload:payload=>{logical=structuredClone(payload);},fetch:async(input,init)=>{
  const headers=Object.fromEntries(new Headers(init.headers));for(const name of Object.keys(headers))if(name.startsWith('x-stainless-')&&!['x-stainless-retry-count','x-stainless-timeout'].includes(name))delete headers[name];
  if(headers['user-agent']?.startsWith('pi ('))headers['user-agent']='pi-ai-rust';
  request={url:String(input),headers,body:JSON.parse(init.body)};return new Response('',{headers:{'content-type':'text/event-stream'}});
 }}).result();
 results.push({id:item.id,model:target,context:transcript,options,logical,request,sourceStop:result.stopReason});
}
console.warn=originalWarn;
console.log(JSON.stringify({sourceCommit:commit,sdk:'@anthropic-ai/sdk@0.91.1',evidenceClass:'executed-pinned-adapter-and-installed-sdk',warnings,cases:results},null,2));
