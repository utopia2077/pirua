import {createRequire,stripTypeScriptTypes} from 'node:module';import {upstreamRoot} from '../../upstream.mjs';
import {execFileSync} from 'node:child_process';
import {Readable} from 'node:stream';
import assert from 'node:assert/strict';
const root=upstreamRoot,pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require=createRequire(root+'/package.json');
const sdk=require('@aws-sdk/client-bedrock-runtime');
const {HttpResponse}=require('@smithy/core/protocols');
const {EventStreamCodec}=require('@smithy/core/event-streams');
const read=p=>execFileSync('git',['show',pin+':packages/ai/src/'+p],{cwd:root,encoding:'utf8'});
const clean=s=>stripTypeScriptTypes(s.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const load=(path,names,deps={})=>Function(...Object.keys(deps),clean(read(path))+';return {'+names.join(',')+'};')(...Object.values(deps));
const helpers={...load('utils/provider-env.ts',['getProviderEnvValue']),...load('utils/sanitize-unicode.ts',['sanitizeSurrogates']),...load('utils/headers.ts',['providerHeadersToRecord']),...load('utils/diagnostics.ts',['appendAssistantMessageDiagnostic']),...load('utils/error-body.ts',['normalizeProviderError']),...load('api/transform-messages.ts',['transformMessages']),...load('api/constrained-sampling.ts',['getJsonSchemaToolParameters','resolveJsonSchemaStrictSampling']),...load('utils/json-parse.ts',['parseStreamingJson'],{partialParse:require('partial-json').parse})};
const estimate=load('utils/estimate.ts',['estimateContextTokens']);Object.assign(helpers,load('api/simple-options.ts',['adjustMaxTokensForThinking','buildBaseOptions','clampMaxTokensToContext','clampReasoning'],estimate));
helpers.calculateCost=Function(clean(read('models.ts').match(/^export function calculateCost[\s\S]*?^}/m)[0])+';return calculateCost;')();
const date=Date;globalThis.Date=class extends date{constructor(...a){super(...(a.length?a:['2026-01-02T03:04:05Z']));}static now(){return 1767323045000;}};
globalThis.crypto.randomUUID=()=> '12345678-1234-4123-8123-123456789abc';
const codec=new EventStreamCodec(b=>Buffer.from(b).toString('utf8'),s=>Buffer.from(s));
const enc=(type,value,messageType='event')=>messageType==='error'?codec.encode({headers:{':message-type':{type:'string',value:'error'},':error-code':{type:'string',value:type},':error-message':{type:'string',value:value.message}},body:new Uint8Array()}):codec.encode({headers:{':message-type':{type:'string',value:messageType},[messageType==='exception'?':exception-type':':event-type']:{type:'string',value:type},':content-type':{type:'string',value:'application/json'}},body:Buffer.from(JSON.stringify(value))});
const defaultEvents=[['messageStart',{role:'assistant'}],['contentBlockDelta',{contentBlockIndex:0,delta:{text:'answer'}}],['contentBlockStop',{contentBlockIndex:0}],['messageStop',{stopReason:'end_turn'}],['metadata',{usage:{inputTokens:10,outputTokens:3,totalTokens:13,cacheReadInputTokens:2,cacheWriteInputTokens:1},metrics:{latencyMs:1}}]];
const snapshot=value=>JSON.parse(JSON.stringify(value,(k,v)=>v instanceof Uint8Array?{$bytes:Buffer.from(v).toString('base64')}:v));
const baseModel={id:'anthropic.claude-sonnet-4-6',name:'Claude Sonnet 4.6',api:'bedrock-converse-stream',provider:'amazon-bedrock',baseUrl:'https://synthetic.invalid/base',reasoning:true,input:['text','image'],cost:{input:1,output:2,cacheRead:.1,cacheWrite:.2},contextWindow:200000,maxTokens:16384};
const zeroUsage={input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}};
const history=[{role:'user',content:[{type:'text',text:' '},{type:'image',mimeType:'image/png',data:'AQID'}],timestamp:0},{role:'assistant',api:baseModel.api,provider:baseModel.provider,model:baseModel.id,content:[{type:'thinking',thinking:'reason',thinkingSignature:'signed'},{type:'toolCall',id:'call|x',name:'lookup',arguments:{'':'drop',z:1,a:{'':'drop',v:2}}}],usage:zeroUsage,stopReason:'toolUse',timestamp:0},{role:'toolResult',toolCallId:'call|x',toolName:'lookup',content:[{type:'text',text:' '}],isError:false,timestamp:0}];
const canonical=v=>Array.isArray(v)?v.map(canonical):v&&typeof v==='object'?Object.fromEntries(Object.keys(v).sort().map(k=>[k,canonical(v[k])])):v;
const rows=[
{id:'basic'},
...[[{x:1},'object'],[[1],'array'],['same','string'],[null,'null']].map(([contentBlockIndex,name])=>({id:'index-'+name,events:[['messageStart',{role:'assistant'}],['contentBlockDelta',{contentBlockIndex,delta:{text:'a'}}],['contentBlockDelta',{contentBlockIndex,delta:{text:'b'}}],['messageStop',{stopReason:'end_turn'}]]})),
...['added','existing'].map(slot=>({id:'strict-tool-'+slot,compat:{supportsStrictMode:true},context:{messages:[],tools:[{name:'lookup',description:'strict ordered',constrainedSampling:{type:'json_schema',strict:'require'},parameters:{type:'object',properties:{z:{type:'number'},a:{type:'string'}},required:['a'],...(slot==='existing'?{additionalProperties:false}:{})}}]}})),
...['{','[','"','{broken','[broken','"broken','t','true x','nul','null x','123 x','-','+1','undefined','gateway denied','gateway denied retry','gateway denied retry!','server error','  server error  ','[server error]','{"server error":}','\nserver error','{"a": 503x}','500xxxxxxxxxxxxxxxxxxxxxxxx','xxxxxxxxxxxxxxxxxxxxxxxx'].flatMap((httpBody,i)=>[400,503].map(httpStatus=>({id:'http-malformed-'+i+'-'+httpStatus,httpStatus,httpBody,expectError:true}))),
{id:'first-modeled-validation',expectError:true,events:[['validationException',{message:'synthetic validation'},'exception']]},
{id:'first-modeled-throttling',expectError:true,sdkConfig:{maxAttempts:3},events:[['throttlingException',{message:'synthetic throttle'},'exception']]},
...[[403,'gateway denied',null],[503,'{broken',null],[500,'{"message":"busy","__type":"InternalServerException"}',null],[400,'{"message":"bad"}','ValidationException'],[429,'{"message":"limit"}','ThrottlingException']].map(([httpStatus,httpBody,errorType])=>({id:'http-'+httpStatus+(httpBody==='{broken'?'-bad-json':''),expectError:true,httpStatus,httpBody,errorType})),

{id:'initial-response-then-error',sdkConfig:{maxAttempts:3},expectError:true,events:[['initial-response',{}],['ThrottlingException',{message:'synthetic throttle'},'error']]},
{id:'initial-response-success',events:[['initial-response',{}],['messageStart',{role:'assistant'}],['messageStop',{stopReason:'end_turn'}]]},

{id:'first-event-retry',sdkConfig:{maxAttempts:3},firstFailure:true},
{id:'after-first-event-no-retry',sdkConfig:{maxAttempts:3},expectError:true,events:[['messageStart',{role:'assistant'}],['ThrottlingException',{message:'synthetic throttle'},'error']]},

{id:'standard-endpoint-default',model:{baseUrl:'https://bedrock-runtime.us-east-1.amazonaws.com'}},
{id:'standard-endpoint-region',model:{baseUrl:'https://bedrock-runtime.us-east-1.amazonaws.com'},options:{region:'eu-west-1'}},
{id:'fips-regional',model:{baseUrl:'https://bedrock-runtime.us-east-1.amazonaws.com'},options:{region:'us-west-2'},sdkConfig:{useFipsEndpoint:true}},
{id:'dualstack-regional',model:{baseUrl:'https://bedrock-runtime.us-east-1.amazonaws.com'},options:{region:'us-west-2'},sdkConfig:{useDualstackEndpoint:true}},
{id:'china-regional',model:{baseUrl:'https://bedrock-runtime.us-east-1.amazonaws.com'},options:{region:'cn-north-1'}},
{id:'configured-endpoint-selected',model:{baseUrl:'https://bedrock-runtime.us-east-1.amazonaws.com'},options:{region:'eu-west-1'},configuredEndpoint:'https://sdk.synthetic.invalid/custom'},
{id:'configured-endpoint-skipped',configuredEndpoint:'https://sdk.synthetic.invalid/custom'},

...['unchanged','changed-bytes','empty-bytes','plain-array','plain-object','string','json-clone'].map(blobMode=>({id:'hook-blob-'+blobMode,blobMode,context:{messages:[{role:'user',content:[{type:'image',mimeType:'image/png',data:'AQID'}],timestamp:0}]}})),
{id:'serializer-null-fields',replacement:{modelId:'synthetic',messages:null,system:null,inferenceConfig:null,toolConfig:null,additionalModelRequestFields:null,requestMetadata:null}},
{id:'serializer-number-spellings',replacement:{modelId:'synthetic',messages:[],inferenceConfig:{maxTokens:3.5,temperature:-0.0,topP:0.5,stopSequences:['a',null,'b']},requestMetadata:{z:null,a:'kept'}}},
{id:'serializer-missing-model',expectError:true,replacement:{messages:[]}},
{id:'serializer-empty-model',replacement:{modelId:'',messages:[]}},
{id:'serializer-multiple-union-members',replacement:{modelId:'synthetic',messages:[{role:'user',content:[{text:'hello',image:{format:'png',source:{bytes:'AQI='}}}]}]}},

{id:'short-prefix-only',events:[],rawTail:[0,0,0],expectError:true},
{id:'stop-then-short-prefix',events:[['messageStart',{role:'assistant'}],['messageStop',{stopReason:'end_turn'}]],rawTail:[0,0,0]},
{id:'stop-then-bad-crc',events:[['messageStart',{role:'assistant'}],['messageStop',{stopReason:'end_turn'}]],corruptTail:true,expectError:true},
{id:'forced-h1',options:{env:{AWS_BEDROCK_FORCE_HTTP1:'1'}}},
{id:'budget-thinking',model:{id:'anthropic.claude-3-7-sonnet',name:'Claude 3.7'},options:{reasoning:'max',thinkingBudgets:{high:4096},thinkingDisplay:'omitted',interleavedThinking:false,temperature:0.25,maxTokens:100}},
{id:'gov-adaptive',model:{id:'arn:aws-us-gov:bedrock:us-gov-west-1:123:inference-profile/test',name:'Claude Opus 4.8'},options:{reasoning:'xhigh'}},
{id:'simple-budget',simple:true,model:{id:'anthropic.claude-3-7-sonnet',name:'Claude 3.7'},options:{reasoning:'high',maxTokens:100,thinkingBudgets:{high:2048}}},
{id:'empty-user',context:{messages:[{role:'user',content:' \t',timestamp:0}]}},
{id:'forced-cache',model:{id:'application-profile',name:'Application'},context:{systemPrompt:'system',messages:[{role:'user',content:'question',timestamp:0}]},options:{env:{AWS_BEDROCK_FORCE_CACHE:'1',PI_CACHE_RETENTION:'long'}}},
{id:'cache-none',context:{systemPrompt:'system',messages:[{role:'user',content:'question',timestamp:0}]},options:{cacheRetention:'none'}},
{id:'tool-none',context:{messages:[],tools:[{name:'lookup',description:'tool',parameters:{type:'object',properties:{}}}]},options:{toolChoice:'none'}},
{id:'tool-events',events:[['messageStart',{role:'assistant'}],['contentBlockStart',{contentBlockIndex:2,start:{toolUse:{toolUseId:'call-1',name:'lookup'}}}],['contentBlockDelta',{contentBlockIndex:2,delta:{toolUse:{input:'{"query":'}}}],['contentBlockDelta',{contentBlockIndex:2,delta:{toolUse:{input:'"pi"}'}}}],['contentBlockStop',{contentBlockIndex:2}],['messageStop',{stopReason:'tool_use'}]]},
{id:'tool-missing-stop',events:[['messageStart',{role:'assistant'}],['contentBlockStart',{contentBlockIndex:2,start:{toolUse:{toolUseId:'call-1',name:'lookup'}}}],['contentBlockDelta',{contentBlockIndex:2,delta:{toolUse:{input:'{"query":"pi"}'}}}],['messageStop',{stopReason:'tool_use'}]]},
{id:'signature-deltas',events:[['messageStart',{role:'assistant'}],['contentBlockDelta',{contentBlockIndex:2,delta:{reasoningContent:{text:'why',signature:'one'}}}],['contentBlockDelta',{contentBlockIndex:2,delta:{reasoningContent:{signature:'two'}}}],['contentBlockStop',{contentBlockIndex:2}],['messageStop',{stopReason:'end_turn'}]]},
...['stop_sequence','max_tokens','model_context_window_exceeded','guardrail_intervened','content_filtered','unknown_reason'].map(stop=>({id:'stop-'+stop,expectError:['guardrail_intervened','content_filtered','unknown_reason'].includes(stop),events:[['messageStart',{role:'assistant'}],['messageStop',{stopReason:stop}]]})),
{id:'wrong-role',expectError:true,events:[['messageStart',{role:'user'}]]},
{id:'usage-only',events:[['metadata',{usage:{inputTokens:10,outputTokens:2,totalTokens:0,cacheReadInputTokens:4,cacheWriteInputTokens:5}}],['messageStop',{stopReason:'end_turn'}]]},

{id:'ordered-rich',context:{systemPrompt:'system',messages:history,tools:[{name:'lookup',description:'look up',parameters:{type:'object',properties:{z:{type:'number'},a:{type:'string'}},required:['a']}}]},options:{reasoning:'xhigh',toolChoice:'any',cacheRetention:'long',requestMetadata:{z:'last',a:'first'},headers:{'x-z':'one','X-Z':'two','content-type':'custom','Authorization':'ignored','x-amz-date':'ignored'}},compat:{supportsStrictMode:true}},
{id:'nonclaude-redacted',model:{id:'openai.gpt-5-6',name:'GPT',reasoning:true},context:{messages:[{role:'assistant',api:baseModel.api,provider:baseModel.provider,model:'openai.gpt-5-6',content:[{type:'thinking',thinking:'[Reasoning redacted]',redacted:true,thinkingSignature:'AQID'}],usage:zeroUsage,stopReason:'stop',timestamp:0},{role:'user',content:'next',timestamp:0}]}},
{id:'bearer',options:{bearerToken:'synthetic-bearer'}},
{id:'skip-auth',options:{env:{AWS_BEDROCK_SKIP_AUTH:'1'}}},
{id:'missing-stop',events:[['messageStart',{role:'assistant'}],['contentBlockDelta',{contentBlockIndex:0,delta:{text:'partial'}}]]},
{id:'redacted-deltas',events:[['messageStart',{role:'assistant'}],['contentBlockDelta',{contentBlockIndex:0,delta:{reasoningContent:{signature:'old',text:'thinking'}}}],['contentBlockDelta',{contentBlockIndex:0,delta:{reasoningContent:{redactedContent:'AQI='}}}],['contentBlockDelta',{contentBlockIndex:0,delta:{reasoningContent:{redactedContent:'AwQ='}}}],['messageStop',{stopReason:'end_turn'}]]},
{id:'unknown-event',events:[['messageStart',{role:'assistant'}],['futureEvent',{value:1}],['messageStop',{stopReason:'end_turn'}]]},
{id:'modeled-exception',events:[['messageStart',{role:'assistant'}],['validationException',{message:'synthetic validation'},'exception']]},
{id:'unknown-exception',events:[['messageStart',{role:'assistant'}],['FutureException',{message:'synthetic future'},'exception']]},
{id:'payload-sdk-projection',replacement:{modelId:'model/path :+%',messages:[{role:'user',content:[{text:'kept',unknown:'drop'}],unknown:'drop'}],system:null,inferenceConfig:{maxTokens:3.0,temperature:null,unknown:1},requestMetadata:{z:'last',a:'first'},additionalModelRequestFields:{z:1,a:null},unknown:'drop'}},
];
const output=[];
for(const row of rows){
 const effects=[],wire=[],responses=[],payloads=[],events=[],responseBodies=[];let complete,sends=0;
 class Events{done=new Promise(r=>complete=r);push(e){effects.push('event:'+e.type);events.push(snapshot(e));}end(){complete();}}
 class Http1{metadata={handlerProtocol:'http/1.1'};}
 class Client extends sdk.BedrockRuntimeClient {
  constructor(config){effects.push('construct');const handlerProtocol=config.requestHandler?'http/1.1':'h2';
   const handler={metadata:{handlerProtocol},async handle(request,options){
    effects.push('handler');wire.push(snapshot(request));assert(options.abortSignal);sends++;const attempt=sends;const frames=[];responseBodies.push(frames);
    if(row.httpStatus){frames.push(Buffer.from(row.httpBody).toString('hex'));return {response:new HttpResponse({statusCode:row.httpStatus,headers:{...(handlerProtocol==='h2'?{':status':row.httpStatus}:{}),'content-type':'application/json','x-amzn-requestid':'request-id',...(row.errorType?{'x-amzn-errortype':row.errorType}:{})},body:Readable.from([row.httpBody])})};}
    const body=Readable.from((async function*(){
     for(const item of row.firstFailure&&attempt===1?[['ThrottlingException',{message:'synthetic throttle'},'error']]:(row.events??defaultEvents)){effects.push('body:'+item[0]);const bytes=enc(...item);frames.push(Buffer.from(bytes).toString('hex'));yield bytes;}
     if(row.rawTail){effects.push('body:raw-tail');frames.push(Buffer.from(row.rawTail).toString('hex'));yield Uint8Array.from(row.rawTail);}
     if(row.corruptTail){const tail=enc('futureEvent',{});tail[tail.length-1]^=1;effects.push('body:bad-crc');frames.push(Buffer.from(tail).toString('hex'));yield tail;}
    })());
    return {response:new HttpResponse({statusCode:200,headers:{...(handlerProtocol==='h2'?{':status':200}:{}),'content-type':'application/vnd.amazon.eventstream','x-amzn-requestid':'request-id','x-custom':'value'},body})};
   }};
   super({region:'us-east-1',credentials:async()=>{effects.push('credentials');return {accessKeyId:'AKID_SYNTHETIC',secretAccessKey:'synthetic-secret',sessionToken:'synthetic-session'};},token:{token:'synthetic-token'},maxAttempts:1,retryMode:'standard',authSchemePreference:['sigv4'],defaultsMode:'standard',useFipsEndpoint:false,useDualstackEndpoint:false,userAgentAppId:'synthetic',defaultUserAgentProvider:async()=>[['aws-sdk-js','3.1048.0'],['api','bedrock-runtime#3.1048.0']],credentialDefaultProvider:()=>{throw Error('discovery forbidden');},...config,...row.sdkConfig,requestHandler:handler});
   this.config.serviceConfiguredEndpoint=async()=>{effects.push('configuredEndpoint');return row.configuredEndpoint;};
  }
  async send(...args){const value=await super.send(...args);effects.push('sdk-send-resolved');return value;}
 }
 const deps={...sdk,...helpers,BedrockStopReason:sdk.StopReason,BedrockRuntimeClient:Client,NodeHttpHandler:Http1,AssistantMessageEventStream:Events,resolveHttpProxyUrlForTarget:()=>undefined};
 const functions=Function(...Object.keys(deps),clean(read('api/bedrock-converse-stream.ts'))+';return {stream,streamSimple};')(...Object.values(deps));
 if(row.context){for(const message of row.context.messages??[]){if(message.role==='assistant')for(const block of message.content??[])if(block.type==='toolCall')block.arguments=canonical(block.arguments);}for(const tool of row.context.tools??[])tool.parameters=canonical(tool.parameters);}
 const model={...baseModel,...row.model,...(row.compat?{compat:row.compat}:{})};
 const controller=new AbortController();const options={...row.options,signal:controller.signal,onPayload:value=>{effects.push('payload');payloads.push(snapshot(value));
 if(row.blobMode){const bytes=value.messages[0].content[0].image.source;
  if(row.blobMode==='changed-bytes')bytes.bytes=Uint8Array.from([4,5]);
  if(row.blobMode==='empty-bytes')bytes.bytes=new Uint8Array();
  if(row.blobMode==='plain-array')bytes.bytes=[1,2,3];
  if(row.blobMode==='plain-object')bytes.bytes={0:1,1:2,2:3};
  if(row.blobMode==='string')bytes.bytes='AQID';
  if(row.blobMode==='json-clone')return JSON.parse(JSON.stringify(value));
 }
 return row.replacement;},onResponse:value=>{effects.push('response');responses.push(snapshot(value));}};
 const s=functions[row.simple?'streamSimple':'stream'](model,row.context??{messages:[]},options);await s.done;
 assert.equal(events.at(-1)?.type,row.expectError||row.id.includes('exception')||row.id==='missing-stop'?'error':'done',row.id);
 output.push({id:row.id,model,context:row.context??{messages:[]},options:row.options??{},simple:row.simple??false,blobMode:row.blobMode,sdkConfig:row.sdkConfig,configuredEndpoint:row.configuredEndpoint,firstFailure:row.firstFailure,httpStatus:row.httpStatus,httpBody:row.httpBody,errorType:row.errorType,replacement:row.replacement,sourceEvents:row.events??defaultEvents,rawTail:row.rawTail,corruptTail:row.corruptTail,payloads,wire,responses,responseBodies,effects,events});
}
console.log(JSON.stringify({pin,node:process.versions.node,sdk:require('@aws-sdk/client-bedrock-runtime/package.json').version,normalization:['synthetic configured handler only','fixed clock and invocation UUID','input tool-argument/schema objects use unchanged core canonical construction order','explicit platform user-agent provider','event snapshots at push','configured leaf returns source-proven Node H1/H2 admitted header records','Uint8Array base64 observation'],cases:output},null,2));
