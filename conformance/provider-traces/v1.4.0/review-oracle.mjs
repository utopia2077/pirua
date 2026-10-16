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

 Date.now=()=>7;
 const transcript=await load('utils/transcript');
 const observations=JSON.parse(readFileSync(join(here,'wire-observations.json')));
 const zero={input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}};
 const assistant=(provider,api,text,extra={})=>({role:'assistant',api,provider,model:'synthetic',content:[{type:'text',text}],usage:zero,stopReason:'stop',timestamp:2,...extra});
 const baseAnthro=structuredClone(observations.cases.find(c=>c.id==='anthropic-thinking'));
 const baseResponses=structuredClone(observations.cases.find(c=>c.id==='openai-thinking'));
 const probes=[];
 function add(id,base,messages,patch={}) {const c=structuredClone(base);c.id=id;c.context={messages};Object.assign(c,patch);delete c.requests;delete c.payloads;delete c.events;delete c.result;delete c.providerEvents;delete c.providerEventCount;probes.push(c);}
 add('anthropic-effort-after-unmarked-assistant',baseAnthro,[{role:'user',content:'one',timestamp:1},assistant('anthropic','anthropic-messages','first'),{role:'user',content:'two',timestamp:3},assistant('anthropic','anthropic-messages','second',{providerThinkingLevel:'low'}),{role:'user',content:'three',timestamp:4}]);
 add('anthropic-effort-invalid-level',baseAnthro,[{role:'user',content:'one',timestamp:1},assistant('anthropic','anthropic-messages','first',{providerThinkingLevel:'arbitrary-native-value'}),{role:'user',content:'two',timestamp:3}]);
 const image={type:'image',data:'AQ==',mimeType:'image/png'};
 const calls=assistant('anthropic','anthropic-messages','', {content:[{type:'toolCall',id:'c1',name:'lookup',arguments:{}},{type:'toolCall',id:'c2',name:'lookup',arguments:{}}],stopReason:'toolUse'});
 add('anthropic-failed-tool-result-image',baseAnthro,[{role:'user',content:'one',timestamp:1},calls,{role:'toolResult',toolCallId:'c1',toolName:'lookup',isError:true,content:[{type:'text',text:'failed'},image],timestamp:3},{role:'toolResult',toolCallId:'c2',toolName:'lookup',isError:false,content:[{type:'text',text:'ok'}],timestamp:3}]);
 add('responses-other-model-effort',baseResponses,[{role:'user',content:'one',timestamp:1},assistant('openai','openai-responses','foreign-model-answer',{model:'different-model',providerThinkingLevel:'xhigh'}),{role:'user',content:'two',timestamp:3}]);
 add('responses-other-api-effort',baseResponses,[{role:'user',content:'one',timestamp:1},assistant('openai','openai-completions','foreign-api-answer',{providerThinkingLevel:'xhigh'}),{role:'user',content:'two',timestamp:3}]);
 const strict=structuredClone(baseAnthro);strict.model.compat={supportsStrictTools:true};strict.context={messages:[{role:'user',content:'one',timestamp:1}],tools:[{name:'lookup',description:'lookup',parameters:{type:'object',properties:{key:{type:'string'}},required:['key']},constrainedSampling:{type:'json_schema',strict:'prefer'}}]};
 add('anthropic-prefer-strict-limit-fallback',strict,strict.context.messages,{context:strict.context,retryProbe:true});
 // Focused eligibility/grouping and bounded-fallback controls from the same source.
 const prior=(provider='anthropic',api='anthropic-messages',extra={})=>assistant(provider,api,'prior',{providerThinkingLevel:'low',...extra});
 const users=[{role:'user',content:'one',timestamp:1},{role:'user',content:'two',timestamp:3}];
 add('anthropic-effort-other-api',baseAnthro,[users[0],prior('anthropic','openai-completions'),users[1]]);
 add('anthropic-effort-other-provider',baseAnthro,[users[0],prior('custom'),users[1]]);
 add('anthropic-effort-empty-and-pending-system',baseAnthro,[users[0],prior('anthropic','anthropic-messages',{content:[]}),{role:'system',content:'delta',timestamp:3},users[1],prior(),{role:'user',content:'three',timestamp:4}]);
 add('responses-empty-effort',baseResponses,[users[0],assistant('openai','openai-responses','',{content:[],providerThinkingLevel:'xhigh'}),users[1]]);
 add('responses-same-model-effort',baseResponses,[users[0],assistant('openai','openai-responses','prior',{providerThinkingLevel:'xhigh'}),users[1]]);
 add('responses-other-provider-effort',baseResponses,[users[0],assistant('custom','openai-responses','prior',{providerThinkingLevel:'xhigh'}),users[1]]);
 const imageCase=structuredClone(probes.find(c=>c.id==='anthropic-failed-tool-result-image'));
 imageCase.context.messages[2].content=[image,image,{type:'text',text:'between'},image];
 imageCase.context.messages[3].content=[{type:'text',text:'ok'},image];
 add('anthropic-error-image-runs-and-normal-image',imageCase,imageCase.context.messages);
 const fallback=structuredClone(probes.find(c=>c.id==='anthropic-prefer-strict-limit-fallback'));
 const errorBody=(message,type='invalid_request_error')=>JSON.stringify({type:'error',error:{type,message}});
 const rejected=message=>({status:400,body:errorBody(message)});
 const accepted={status:200,body:fallback.terminal};
 fallback.responses=[rejected('The compiled grammar is too large'),accepted];
 Object.assign(probes.find(c=>c.id===fallback.id),{responses:fallback.responses});
 function strictCase(id,responses,patch={}){add(id,fallback,fallback.context.messages,{context:structuredClone(fallback.context),responses,...patch});}
 strictCase('anthropic-prefer-strict-redacts-error-detail',[rejected('The compiled grammar is too large: fixture-sensitive-detail'),accepted]);
 strictCase('anthropic-prefer-strict-too-many-tools',[rejected('Too many strict tools (21)'),accepted]);
 strictCase('anthropic-prefer-strict-union-limit',[rejected('Schemas contains too many parameters with union types (17)'),accepted]);
 strictCase('anthropic-require-strict-no-fallback',[rejected('The compiled grammar is too large'),accepted]);
 probes.at(-1).context.tools[0].constrainedSampling.strict='require';
 strictCase('anthropic-unrelated400-no-fallback',[rejected('Unrelated invalid request'),accepted]);
 strictCase('anthropic-wrong-error-type-no-fallback',[{status:400,body:errorBody('The compiled grammar is too large','api_error')},accepted]);
 strictCase('anthropic-no-preferred-strict-no-fallback',[rejected('The compiled grammar is too large'),accepted]);
 probes.at(-1).model.compat.supportsStrictTools=false;
 strictCase('anthropic-strict-fallback-at-most-once',[rejected('The compiled grammar is too large'),rejected('The compiled grammar is too large'),accepted]);
 strictCase('anthropic-started-stream-no-fallback',[{status:200,body:fallback.terminal.split('event: message_delta')[0]+'event: error\ndata: '+errorBody('The compiled grammar is too large')+'\n\n'},accepted]);
 for(const c of probes) {
  const adapter=await load('api/'+c.model.api);c.expected=[];
  c.expectedRequests=[];
  const stream=adapter.stream(c.model,transcript.normalizeContext(c.context),{...c.options,onPayload:params=>{c.expected.push(JSON.parse(JSON.stringify(params)));if(!c.retryProbe)throw Error('stop after payload capture');},fetch:async(url,init)=>{if(!c.retryProbe)throw Error('network forbidden');c.expectedRequests.push(JSON.parse(init.body));const reply=c.responses[c.expectedRequests.length-1]??c.responses.at(-1);return new Response(reply.body,{status:reply.status,headers:{'content-type':reply.status===200?'text/event-stream':'application/json'}});}});
  const result=await stream.result();c.expectedStopReason=result.stopReason;c.expectedDiagnostics=result.diagnostics??[];
 }
 const filename=join(here,'review-observations.json');if(process.argv.includes('--write'))writeFileSync(filename,JSON.stringify(probes,null,2)+'\n');else deepStrictEqual(probes,JSON.parse(readFileSync(filename)));
 console.log('Pi v1.4.0 review regression oracle verified: '+probes.length+' cases');
}finally{rmSync(temporary,{recursive:true,force:true});}
