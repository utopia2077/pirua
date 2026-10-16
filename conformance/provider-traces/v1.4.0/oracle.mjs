// Offline differential oracle: immutable published release with exact source provenance.
import {mkdtempSync,mkdirSync,symlinkSync,readFileSync,writeFileSync,rmSync,existsSync} from 'node:fs';
import {tmpdir} from 'node:os';import {join,resolve,dirname} from 'node:path';import {execFileSync} from 'node:child_process';import {pathToFileURL,fileURLToPath} from 'node:url';import {createHash} from 'node:crypto';import {deepStrictEqual} from 'node:assert';
const here=dirname(fileURLToPath(import.meta.url));const repository=resolve(here,'../../..');
const provenance=JSON.parse(readFileSync(join(repository,'conformance/catalog/v1.4.0/provenance.json')));
const tarball=join(repository,'conformance/catalog/v1.4.0/pi-ai-1.4.0.tgz');
deepStrictEqual('sha512-'+createHash('sha512').update(readFileSync(tarball)).digest('base64'),provenance.integrity);
const upstream=process.env.PIRUA_UPSTREAM_ROOT || resolve(repository,'upstream/pi-mono');
const dependencies=join(here,'node_modules');
const manifest=JSON.parse(readFileSync(join(here,'package.json')));
for(const [dependency,version] of Object.entries(manifest.dependencies))deepStrictEqual(JSON.parse(readFileSync(join(dependencies,dependency,'package.json'))).version,version);
if(!existsSync(upstream))throw Error('Set PIRUA_UPSTREAM_ROOT to a Git checkout containing Pi v1.4.0 (default: upstream/pi-mono)');
deepStrictEqual(execFileSync('git',['rev-parse','v1.4.0'],{cwd:upstream,encoding:'utf8'}).trim(),provenance.upstreamCommit);
const temporary=mkdtempSync(join(tmpdir(),'pirua-v140-oracle-'));
const clean=value=>JSON.parse(JSON.stringify(value,(key,value)=>key==='durationMs'?undefined:value));
try{
 mkdirSync(join(temporary,'ai'));mkdirSync(join(temporary,'node_modules/@earendil-works/pi-telemetry'),{recursive:true});
 execFileSync('tar',['-xzf',tarball,'--strip-components=1','-C',join(temporary,'ai')]);
 const telemetry=readFileSync(join(here,'pi-telemetry-1.4.0.tgz'));
 deepStrictEqual('sha512-'+createHash('sha512').update(telemetry).digest('base64'),'sha512-MLIfowwUYIY65jSjMX82paZOrX/NTvLk6D08h+3Jq42a99RzUOewzuCR0zOlPul3D6UBfddat76VT6LvsHEs1w==');
 execFileSync('tar',['-xzf',join(here,'pi-telemetry-1.4.0.tgz'),'--strip-components=1','-C',join(temporary,'node_modules/@earendil-works/pi-telemetry')]);
 for(const dependency of ['openai','@anthropic-ai','@google','partial-json','typebox'])symlinkSync(join(dependencies,dependency),join(temporary,'node_modules',dependency));
 const load=path=>import(pathToFileURL(join(temporary,'ai/dist',path+'.js')));
 const transcript=await load('utils/transcript');const streamModule=await load('utils/event-stream');
 function assistantSnapshot(value) {
  const snapshot=clean(value);
  function block(block) {delete block.index;delete block.partialJson;delete block.partialArgs;delete block.streamIndex;if(block.type==='toolCall'&&(!block.arguments||Array.isArray(block.arguments)||typeof block.arguments!=='object'))block.arguments={};}
  for(const message of [snapshot.partial,snapshot.message,snapshot.error])if(message?.role==='assistant')for(const item of message.content)block(item);
  if(snapshot.toolCall)block(snapshot.toolCall);
  return snapshot;
 }
 const basePush=streamModule.AssistantMessageEventStream.prototype.push;
 const snapshots=new WeakMap();streamModule.AssistantMessageEventStream.prototype.push=function(event){snapshots.get(this)?.push(assistantSnapshot(event));return basePush.call(this,event);};
 Date.now=()=>7;
 const tool={name:'lookup',description:'Look up one value',parameters:{type:'object',properties:{key:{type:'string'}},required:['key']}};
 const late={...tool,name:'late'};
 const zero={input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}};
 const model=(provider,api)=>({id:'synthetic',name:'Synthetic',api,provider,baseUrl:'https://example.invalid/v1',reasoning:true,input:['text','image'],cost:{input:1,output:2,cacheRead:0.1,cacheWrite:0.2},contextWindow:100000,maxTokens:1000});
 const history=[{role:'system',content:'base',sections:{second:'<second>two</second>',first:'<first>one</first>'},toolsAdded:[tool],timestamp:0},{role:'user',content:'hi',timestamp:1},{role:'assistant',content:[{type:'text',text:'before'}],api:'openai-responses',provider:'openai',model:'synthetic',providerThinkingLevel:'low',usage:zero,stopReason:'stop',timestamp:2},{role:'system',content:'added',sections:{second:null,first:'<first>updated</first>'},toolsAdded:[late],toolsRemoved:[{name:'lookup'}],timestamp:3},{role:'user',content:[{type:'text',text:'see'},{type:'image',data:'AQ==',mimeType:'image/png'}],timestamp:4}];
 const protocols=[['openai','openai-responses'],['deepseek','openai-completions'],['anthropic','anthropic-messages'],['google','google-generative-ai']];
 const cases=[];
 for(const [provider,api] of protocols){
  const adapter=await load('api/'+api);let terminal=api==='openai-responses'?'data: {"type":"response.completed","response":{"id":"r","status":"completed","output":[],"usage":{"input_tokens":5,"output_tokens":2,"total_tokens":7}}}\n\n':api==='openai-completions'?'data: {"id":"c","choices":[{"index":0,"delta":{"content":"done"},"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":2,"total_tokens":7}}\n\ndata: [DONE]\n\n':api==='anthropic-messages'?'event: message_start\ndata: {"type":"message_start","message":{"id":"m","type":"message","role":"assistant","content":[],"model":"synthetic","usage":{"input_tokens":5,"output_tokens":0}}}\n\nevent: content_block_start\ndata: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}\n\nevent: content_block_delta\ndata: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"done"}}\n\nevent: content_block_stop\ndata: {"type":"content_block_stop","index":0}\n\nevent: message_delta\ndata: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}\n\nevent: message_stop\ndata: {"type":"message_stop"}\n\n':'data: {"responseId":"g","candidates":[{"content":{"role":"model","parts":[{"text":"done"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":2,"totalTokenCount":7}}\n\n';
  for(const mode of ['collapse','anchored','thinking','sampling','oauth','fallback','search','malformed']){
   let current=model(provider,api);let context={messages:structuredClone(history)};let options={apiKey:'sk-synthetic',maxRetries:0,identity:{userAgent:{custom:'pi-v140-oracle'}},maxTokens:20};
   if(mode==='anchored'){current.compat={supportsMidConvoSystemMessages:true,...api==='openai-responses'?{supportsAdditionalTools:true}:api==='anthropic-messages'?{supportsMidConvoToolChanges:true}: {}};}
   if(mode==='thinking'){current.compat=api==='anthropic-messages'?{supportsMidConvoSystemMessages:true,supportsMidConvoEffort:true,forceAdaptiveThinking:true}:api==='openai-responses'?{supportsMidConvoSystemMessages:true,supportsMidConvoEffort:true}:{supportsMidConvoSystemMessages:true};options={...options,...api==='anthropic-messages'?{thinkingEnabled:true,effort:'high'}:api==='google-generative-ai'?{thinking:{enabled:true,level:'HIGH'}}:{reasoningEffort:'high'}};}
   if(mode==='sampling'){current.samplingParams={temperature:0.1,top_p:0.8};current.samplingParamsByThinkingLevel={high:{temperature:0.2,top_k:4}};options={...options,...api==='openai-responses'||api==='openai-completions'?{reasoningEffort:'high'}:{},samplingParams:{top_p:0.5}};}
   if(mode==='oauth') {
    if(provider==='openai'){current.baseUrl='https://api.openai.com/v1';options.apiKey='oauth-synthetic';current.compat={supportsMidConvoEffort:true,supportsMidConvoSystemMessages:true};options.reasoningEffort='high';}
    if(provider==='anthropic')options.apiKey='sk-ant-oat-synthetic';
   }
   if(mode==='fallback'&&provider==='openai')options.serviceTier='fast';
   if(mode==='fallback'&&provider==='anthropic')current.compat={allowedFallbackModels:[{provider:'anthropic',model:'fallback',cost:current.cost}]};
   if(mode==='search'&&api==='openai-responses'){current.compat={supportsMidConvoSystemMessages:true,supportsToolSearch:true};delete context.messages[3].toolsRemoved;}
   if(mode==='malformed') {
    if(api==='openai-responses')terminal='data: '+JSON.stringify({type:'response.output_item.added',output_index:0,item:{type:'function_call',id:'fc',call_id:'c',name:'lookup',arguments:''}})+'\n\ndata: '+JSON.stringify({type:'response.function_call_arguments.delta',output_index:0,delta:'[1]'})+'\n\ndata: '+JSON.stringify({type:'response.output_item.done',output_index:0,item:{type:'function_call',id:'fc',call_id:'c',name:'lookup',arguments:'[1]'}})+'\n\ndata: '+JSON.stringify({type:'response.completed',response:{id:'r',status:'completed',output:[],usage:{input_tokens:5,output_tokens:2,total_tokens:7}}})+'\n\n';
    if(api==='openai-completions')terminal='data: '+JSON.stringify({id:'c',choices:[{index:0,delta:{tool_calls:[{index:0,id:'c',type:'function',function:{name:'lookup',arguments:'[1]'}}]},finish_reason:'tool_calls'}],usage:{prompt_tokens:5,completion_tokens:2,total_tokens:7}})+'\n\ndata: [DONE]\n\n';
    if(api==='anthropic-messages')terminal=[{type:'message_start',message:{id:'m',type:'message',role:'assistant',model:'synthetic',content:[],usage:{input_tokens:5,output_tokens:0}}},{type:'content_block_start',index:0,content_block:{type:'tool_use',id:'c',name:'lookup',input:{}}},{type:'content_block_delta',index:0,delta:{type:'input_json_delta',partial_json:'[1]'}},{type:'content_block_stop',index:0},{type:'message_delta',delta:{stop_reason:'tool_use'},usage:{output_tokens:2}},{type:'message_stop'}].map(event=>'event: '+event.type+'\ndata: '+JSON.stringify(event)+'\n\n').join('');
   }
   if(provider==='google'){delete options.reasoningEffort;delete current.compat;}
   const requests=[],payloads=[],providerEvents=[];
   const fetch=async(url,init)=>{const request=url instanceof Request?url:new Request(url,init);const body=init?.body ?? await request.text();requests.push({url:request.url,headers:Object.fromEntries(request.headers.entries()),body:typeof body==='string'?JSON.parse(body):body});return new Response(terminal,{status:200,headers:{'content-type':'text/event-stream'}});};
   globalThis.fetch=fetch;
   const stream=adapter.stream(current,transcript.normalizeContext(context),{...options,onPayload:payload=>{payloads.push(clean(payload));},onProviderStreamEvent:event=>{providerEvents.push(clean(event));}});
   const events=[];snapshots.set(stream,events);
   const result=clean(await stream.result());
   cases.push({id:provider+'-'+mode,model:current,context,options,terminal,requests,payloads,events,result,providerEvents,providerEventCount:providerEvents.length});
  }
 }
 const retainedRequests={};
 for(const [family,file] of [['openai-responses','openai-responses/trace.json'],['openai-completions','openai-completions/trace.json'],['anthropic-messages','anthropic-messages/requests.json'],['google-generative-ai','google-genai/trace.json']]) {
  const adapter=await load('api/'+family); const corpus=JSON.parse(readFileSync(join(repository,'conformance/provider-traces',file)));
  if(family==='anthropic-messages'){const trace=JSON.parse(readFileSync(join(repository,'conformance/provider-traces/anthropic-messages/trace.json')));corpus.cases.push({...trace,id:'release-main',options:{...trace.options,thinkingEnabled:true,effort:'xhigh',thinkingDisplay:'summarized',maxTokens:99,temperature:0,cacheRetention:'long'}});}
  const observations={};
  for(const entry of corpus.cases){
   if(JSON.stringify(entry.context).includes('addedToolNames'))continue;
   const requests=[],payloads=[];
   globalThis.fetch=async(url,init)=>{const request=url instanceof Request?url:new Request(url,init);requests.push({body:JSON.parse(init?.body??await request.text()),beta:request.headers.get('anthropic-beta')});return new Response('{"error":{"message":"fixture stop"}}',{status:400,headers:{'content-type':'application/json'}});};
   const options={...entry.options,...entry.noKey?{}:{apiKey:entry.options.apiKey??entry.key??'synthetic-key'},identity:{userAgent:{custom:'pi-v140-oracle'}},maxRetries:0,onPayload:payload=>{payloads.push(clean(payload));return entry.replacement;}};
   try {const stream=(entry.simple?adapter.streamSimple:adapter.stream)({...entry.model,baseUrl:entry.endpoint??entry.model.baseUrl},transcript.normalizeContext(entry.context),options);await stream.result();}catch{}
   observations[entry.id]={requests,payloads,rejected:requests.length===0&&payloads.length===0};
  }
  retainedRequests[family]=observations;
 }
 const retry=await load('utils/retry'); const identity=await load('utils/identity');
 const retryDelays=[0,1,1000,9007199254740991].flatMap(baseDelayMs=>[1,2,64,65,1024,1025].flatMap(attempt=>[undefined,250,9007199254740991].map(maxAgentDelayMs=>({policy:{baseDelayMs,...maxAgentDelayMs===undefined?{}:{maxAgentDelayMs}},attempt,delay:retry.retryDelayMs({baseDelayMs,maxAgentDelayMs},attempt)}))));
 const identityBase={userAgent:'short',attribution:{name:'pi',url:'https://pi.dev',categories:['cli-agent']},version:'1'};
 const identities=[{}, {attribution:null}, {attribution:{name:'app',url:'https://app.invalid'}}, {version:'2'}].map(request=>({base:identityBase,request,merged:identity.mergeClientIdentity(identityBase,request),headers:identity.getIdentityHeaders({provider:'custom',baseUrl:'https://openrouter.ai/api/v1'},identity.mergeClientIdentity(identityBase,request))}));
 const transcriptCase=transcript.normalizeContext({messages:history});
 const googleShared=await load('api/google-shared');
 const googleThinking=JSON.parse(readFileSync(join(repository,'conformance/provider-helpers/google-shared/vectors.json'))).thinking.map(entry=>{try{return {...entry,expected:googleShared.resolveGoogleThinkingLevel(entry.model,entry.level),error:false};}catch{return {...entry,error:true};}});
 const sectionEntries=[['10','ten'],['2','two'],['alpha','a'],['01','leading'],['alpha','updated']];
 const sectionMessage={role:'system',content:[{type:'text',text:'base'},{type:'text',text:'next'}],sections:Object.fromEntries(sectionEntries),timestamp:0};
 const text=await load('utils/text');
 const sectionOrder={entries:sectionEntries,message:sectionMessage,prompt:text.getSystemMessageText(sectionMessage)};
 const helpers={sectionOrder,googleThinking,retryDelays,identities,transcript:{input:{messages:history},current:transcript.getCurrentSystemMessage(history),tools:transcript.getCurrentTools(history),collapsed:transcript.collapseSystemMessages(transcriptCase)}};
 const observation={helpers,retainedRequests,target:provenance.upstreamCommit,package:provenance.package+'@'+provenance.version,cases};
 const output=JSON.stringify(observation,null,2)+'\n';const target=join(here,'wire-observations.json');
 if(process.argv.includes('--write'))writeFileSync(target,output);else{deepStrictEqual(JSON.parse(readFileSync(target)),observation);console.log('Pi v1.4.0 differential wire oracle verified: '+cases.length+' cases');}
}finally{rmSync(temporary,{recursive:true,force:true});}
