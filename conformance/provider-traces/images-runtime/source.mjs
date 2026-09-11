// Execute complete pinned runtime and wrappers with synthetic typed dependencies.
import {execFileSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync,symlinkSync,readdirSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname,relative} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {createHash} from 'node:crypto';
import {registerHooks} from 'node:module';
import assert from 'node:assert/strict';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185',upstream='upstream/pi-mono',here=dirname(fileURLToPath(import.meta.url));
const hash=x=>createHash('sha256').update(x).digest('hex');
const scratch=mkdtempSync(join(tmpdir(),'pi-images-runtime-source-'));
const files={};
try{
 for(const path of ['images-models.ts','images-api-registry.ts','images.ts','auth/context.ts','auth/credential-store.ts','auth/resolve.ts','utils/abort.ts','utils/diagnostics.ts','api/openrouter-images.lazy.ts','providers/images/register-builtins.ts','api/openrouter-images.ts','utils/error-body.ts','utils/headers.ts','utils/provider-retry.ts','utils/sanitize-unicode.ts']){
  const name='packages/ai/src/'+path,bytes=execFileSync('git',['-C',upstream,'show',pin+':'+name],{env:{PATH:'/usr/bin:/bin'}});const dest=join(scratch,path);mkdirSync(dirname(dest),{recursive:true});writeFileSync(dest,bytes);files[name]=hash(bytes);
 }
 writeFileSync(join(scratch,'package.json'),'{"type":"module"}');
 symlinkSync(join(upstream,'node_modules'),join(scratch,'node_modules'),'dir');
 const {createImagesModels,createImagesProvider}=await import(pathToFileURL(join(scratch,'images-models.ts')));
 const {InMemoryCredentialStore}=await import(pathToFileURL(join(scratch,'auth/credential-store.ts')));
 const {ModelsError}=await import(pathToFileURL(join(scratch,'auth/resolve.ts')));
 let now=1700000000000;Date.now=()=>now;
 const model={id:'synthetic-model',name:'Synthetic',api:'synthetic-images',provider:'openrouter',baseUrl:'https://public.invalid/api',input:['text'],output:['image'],cost:{input:1,output:2,cacheRead:3,cacheWrite:4}};
 const context={input:[{type:'text',text:'synthetic prompt'}]};
 const result=(m,label='ok')=>({api:m.api,provider:m.provider,model:m.id,output:[{type:'text',text:label}],stopReason:'stop',timestamp:Date.now()});
 const deferred=()=>{let resolve,reject;const promise=new Promise((a,b)=>{resolve=a;reject=b;});return {promise,resolve,reject};};
 const ctx={env:async()=>undefined,fileExists:async()=>false};
 async function reached(events,value){for(let i=0;i<100;i++){if(events.includes(value))return;await Promise.resolve();}throw Error('required branch not entered: '+value);}
 const configurations=[
  {id:'no-auth-success',auth:false},{id:'no-auth-reject',auth:false,reject:true},{id:'no-auth-launch-throw',auth:false,syncThrow:true},
  {id:'empty-auth-object-catches',auth:true,emptyAuth:true,reject:true},{id:'auth-success',auth:true},{id:'auth-reject',auth:true,reject:true},{id:'auth-launch-throw',auth:true,syncThrow:true},
  {id:'auth-failure',auth:true,authFail:true},{id:'unknown-provider',unknown:true},
  {id:'auth-cancel-after-entry',auth:true,authPending:true},
  {id:'no-auth-cancel-then-reject',auth:false,generationPending:true,reject:true},
  {id:'auth-cancel-then-reject',auth:true,generationPending:true,reject:true},
  {id:'auth-cancel-then-success',auth:true,generationPending:true},
  {id:'explicit-empty-key-env',auth:true,options:{apiKey:'',env:{}}},
  {id:'exact-header-env-endpoint-merge',auth:true,options:{apiKey:'SYNTHETIC_EXPLICIT',env:{A:'explicit',C:'extra'},headers:{'X-Shared':'explicit','x-shared':'case-later','X-Null':null}}},
  {id:'empty-auth-endpoint',auth:true,emptyEndpoint:true},
 ];
 const generations=[];
 for(const config of configurations){
  now=1700000000000;const events=[],calls=[],gate=deferred(),control=new AbortController();const store=new InMemoryCredentialStore();
  const credentials={read:async(...args)=>{events.push('read');return store.read(...args);},list:(...args)=>store.list(...args),modify:(...args)=>store.modify(...args)};
  const authResult={auth:{apiKey:'SYNTHETIC_RESOLVED',baseUrl:config.emptyEndpoint?'':'https://credential.invalid/api?synthetic=1',headers:{'X-Shared':'auth','X-Keep':'kept','X-Null':'auth'}},source:'API key',env:{A:'auth',B:'kept'}};
  const auth={apiKey:{label:'Synthetic',resolve:async()=>{events.push('auth');if(config.authPending)await gate.promise;if(config.authFail)throw new Error('SYNTHETIC_AUTH_ERROR');return config.auth?(config.emptyAuth?{auth:{}}:authResult):undefined;}}};
  const api={generateImages:(m,c,o)=>{events.push('generate');calls.push({endpoint:m.baseUrl,headers:Object.entries(o?.headers??{}),env:o?.env??{},key:o?.apiKey??null,context:c});if(config.syncThrow)throw Error('SYNTHETIC_LAUNCH_ERROR');return (async()=>{if(config.generationPending)await gate.promise;if(config.reject)throw Error('SYNTHETIC_ASYNC_ERROR');return result(m);})();}};
  const runtime=createImagesModels({credentials,authContext:ctx});if(!config.unknown)runtime.setProvider(createImagesProvider({id:'openrouter',auth,models:[model],api}));
  const call=runtime.generateImages(model,context,{...config.options,signal:control.signal});
  if(config.authPending){await reached(events,'auth');assert.deepEqual(events,['read','auth']);now+=17;control.abort();}
  if(config.generationPending){await reached(events,'generate');assert.equal(calls.length,1);now+=17;control.abort();gate.resolve();}
  let outcome;try{outcome={kind:'resolved',value:await call};}catch(error){assert(!(error instanceof TypeError));outcome={kind:'rejected',message:error.message};}
  if(config.authPending){assert.equal(outcome.kind,'resolved');assert.equal(outcome.value.stopReason,'error');assert(!outcome.value.errorMessage.includes('TypeError'));gate.resolve();}
  generations.push({config,events,calls,outcome});
 }
 now=1700000000000;
 const auth={apiKey:{label:'None',resolve:async()=>undefined}};
 const p=(id,models,name)=>createImagesProvider({id,name,auth,models,api:{generateImages:async(m)=>result(m,id)}});
 const runtime=createImagesModels({authContext:ctx});runtime.setProvider(p('a',[{...model,id:'same',name:'first'},{...model,id:'same',name:'second'}]));runtime.setProvider(p('b',[{...model,id:'b'}]));
 runtime.setProvider({...p('throw',[]),getModels(){throw Error('synthetic list error');}});
 const listing={initial:runtime.getProviders().map(v=>v.id),models:runtime.getModels().map(v=>v.id),first:runtime.getModel('a','same').name,unknown:runtime.getModels('missing')};
 runtime.setProvider(p('a',[model],'replacement'));listing.replaced=runtime.getProviders().map(v=>v.id);runtime.deleteProvider('a');runtime.setProvider(p('a',[model]));listing.reinserted=runtime.getProviders().map(v=>v.id);runtime.clearProviders();listing.cleared=runtime.getProviders().length;
 const live=[];
 for(const refreshMode of [false,true])for(const mode of ['add','replace','delete-reinsert','clear']){
  const r=createImagesModels({authContext:ctx}),events=[];
  const make=(id,label,action)=>{let once=action;return {id,name:label,auth,getModels(){if(!refreshMode){events.push(label);const fn=once;once=undefined;fn?.();}return [{...model,id:label}];},...(refreshMode?{refreshModels(){events.push('start:'+label);const fn=once;once=undefined;fn?.();return Promise.resolve().then(()=>{events.push('finish:'+label);});}}:{})};};
  const action=()=>{if(mode==='add')r.setProvider(make('b','b-new'));if(mode==='replace')r.setProvider(make('b','b-new'));if(mode==='delete-reinsert'){r.deleteProvider('b');r.setProvider(make('b','b-new'));}if(mode==='clear'){r.clearProviders();r.setProvider(make('d','d'));}};
  r.setProvider(make('a','a',action));if(mode!=='add'){r.setProvider(make('b','b-old'));r.setProvider(make('c','c'));}
  const snapshot=r.getProviders().map(v=>v.id);assert.equal(events.length,0);let models;
  if(refreshMode)await r.refresh();else models=r.getModels().map(v=>v.id);
  live.push({mode,refreshMode,snapshot,events,...(models?{models}:{}),providers:r.getProviders().map(v=>v.id)});
 }
 const nestedGates=[],nestedChildren=[];let nestedCalls=0,nestedProvider;
 nestedProvider=createImagesProvider({id:'nested',auth,models:[model],api:{generateImages:async(m)=>result(m)},refreshModels(){const call=++nestedCalls;const gate=deferred();nestedGates[call]=gate;if(call===1)nestedChildren.push(nestedProvider.refreshModels());return gate.promise.then(()=>[{...model,id:'model-'+call}]);}});
 const outer=nestedProvider.refreshModels(),joined=nestedProvider.refreshModels();const nestedRefresh={afterLaunchCalls:nestedCalls,joinedOuter:outer===joined};nestedGates[2].resolve();await nestedChildren[0];const third=nestedProvider.refreshModels();nestedRefresh.afterChildCalls=nestedCalls;nestedGates[1].resolve();await outer;await joined;const fourth=nestedProvider.refreshModels();nestedRefresh.afterOuterCalls=nestedCalls;nestedGates[4].resolve();await fourth;nestedRefresh.afterFourth=nestedProvider.getModels().map(v=>v.id);nestedGates[3].resolve();await third;nestedRefresh.final=nestedProvider.getModels().map(v=>v.id);
 const refreshEvents=[],gate=deferred();let fetches=0;let fail=true;
 const dynamic=createImagesProvider({id:'dynamic',auth,models:[model],api:{generateImages:async(m)=>result(m)},refreshModels:async()=>{fetches++;refreshEvents.push('fetch:'+fetches);if(fetches===1)await gate.promise;if(fail)throw Error('synthetic refresh');return [{...model,id:'fresh'}];}});
 const refreshRuntime=createImagesModels({authContext:ctx});refreshRuntime.setProvider(dynamic);
 const first=dynamic.refreshModels(),second=dynamic.refreshModels();assert.equal(first,second);const refresh={sharedPromise:first===second,fetchesBefore:fetches,oldWhilePending:dynamic.getModels().map(v=>v.id)};gate.resolve();const failures=await Promise.allSettled([first,second]);refresh.failures=failures.map(v=>v.status);refresh.oldAfterFailure=dynamic.getModels().map(v=>v.id);fail=false;await refreshRuntime.refresh('dynamic');refresh.afterRetry=dynamic.getModels().map(v=>v.id);refresh.fetches=fetches;
 refreshRuntime.setProvider({...p('coded',[]),refreshModels:async()=>{throw new ModelsError('oauth','synthetic coded');}});refreshRuntime.setProvider({...p('plain',[]),refreshModels:async()=>{throw Error('synthetic plain');}});
 for(const id of ['coded','plain']){try{await refreshRuntime.refresh(id);}catch(error){refresh[id+'Code']=error.code;}}
 await refreshRuntime.refresh();refresh.allSettled=true;await refreshRuntime.refresh('missing');refresh.unknownNoop=true;
 const refreshLaunch=[];
 for(const synchronous of [true,false]){let calls=0;const refreshModels=synchronous?()=>{calls++;throw Error('SYNTHETIC_REFRESH_LAUNCH');}:async()=>{calls++;throw Error('SYNTHETIC_REFRESH_ASYNC');};const sourceProvider=createImagesProvider({id:'launch',auth,models:[model],api:{generateImages:async(m)=>result(m)},refreshModels});const first=sourceProvider.refreshModels();await first.catch(()=>{});const second=sourceProvider.refreshModels();await second.catch(()=>{});refreshLaunch.push({synchronous,calls,samePromise:first===second,models:sourceProvider.getModels().map(v=>v.id)});}
 const sharedAuth=createImagesModels({authContext:ctx});sharedAuth.setProvider(createImagesProvider({id:'openrouter',auth:{apiKey:{label:'Synthetic',resolve:async()=>({auth:{headers:{'X-Auth':'auth'}},source:'API key'})}},models:[model],api:{generateImages:async(m)=>result(m)}}));
 const getAuth=await sharedAuth.getAuth({...model,headers:{'X-Model':'model'}});assert.deepEqual(getAuth.auth.headers,{'X-Auth':'auth'});
 const registry=await import(pathToFileURL(join(scratch,'images-api-registry.ts')));
 const bind=(label)=>({api:'synthetic-images',generateImages:async(m)=>result(m,label)});
 registry.registerImagesApiProvider(bind('old'),'source-old');const retained=registry.getImagesApiProvider('synthetic-images');registry.registerImagesApiProvider(bind('new'),'source-new');
 const binding={old:(await retained.generateImages(model,context)).output[0].text,new:(await registry.getImagesApiProvider('synthetic-images').generateImages(model,context)).output[0].text,unknown:registry.getImagesApiProvider('missing')===undefined};
 try{retained.generateImages({...model,api:'other'},context);}catch(error){binding.mismatch=error.message;}
 const replacementGate=deferred();const detached=createImagesProvider({id:'replace',auth,models:[model],api:{generateImages:async(m)=>result(m)},refreshModels:async()=>{await replacementGate.promise;return [{...model,id:'old-fresh'}];}});const replacementRuntime=createImagesModels({authContext:ctx});replacementRuntime.setProvider(detached);const pendingRefresh=replacementRuntime.refresh('replace');replacementRuntime.setProvider(p('replace',[{...model,id:'new-current'}]));replacementGate.resolve();await pendingRefresh;const refreshReplacement={old:detached.getModels().map(v=>v.id),current:replacementRuntime.getModels('replace').map(v=>v.id)};
 const wire=await import(pathToFileURL(join(scratch,'api/openrouter-images.ts')));const integrationEvents=[],wireCalls=[],wireControl=new AbortController();const wireModel={...model,api:'openrouter-images',output:['image','text']};const wireStore=new InMemoryCredentialStore();
 const wireRuntime=createImagesModels({credentials:{read:async(...args)=>{integrationEvents.push('read');return wireStore.read(...args);},list:(...args)=>wireStore.list(...args),modify:(...args)=>wireStore.modify(...args)},authContext:ctx});wireRuntime.setProvider(createImagesProvider({id:'openrouter',auth:{apiKey:{label:'Synthetic',resolve:async()=>{integrationEvents.push('auth');return {auth:{apiKey:'SYNTHETIC_WIRE_KEY',baseUrl:'https://credential.invalid/api',headers:{'X-Auth':'auth'}},source:'API key'};}}},models:[wireModel],api:wire}));
 const wireResponse={id:'integrated',choices:[{message:{content:'caption',images:[{image_url:'data:image/png;base64,A==='}]}}],usage:{prompt_tokens:20,completion_tokens:5}};
 const integrationOutput=await wireRuntime.generateImages(wireModel,context,{headers:{Authorization:null,'X-Explicit':'explicit'},signal:wireControl.signal,onPayload:()=>{integrationEvents.push('payload');},onResponse:()=>{integrationEvents.push('response');wireControl.abort();},fetch:async(url,init)=>{integrationEvents.push('fetch');const headers=Object.fromEntries(init.headers);for(const key of Object.keys(headers)){if(key==='user-agent')headers[key]='pi-ai-rust';else if(key.startsWith('x-stainless-')&&key!=='x-stainless-retry-count')delete headers[key];}wireCalls.push({url:String(url),headers,body:init.body});return new Response(JSON.stringify(wireResponse),{headers:{'content-type':'application/json'}});}});
 const integration={model:wireModel,response:wireResponse,events:integrationEvents,calls:wireCalls,output:integrationOutput,originalBaseUrl:wireModel.baseUrl,cancelled:wireControl.signal.aborted};assert.equal(integrationOutput.stopReason,'stop');assert.equal(integrationOutput.usage.totalTokens,25);
 const sdkRoot=join(upstream,'node_modules/openai'),sdkFiles=[];function scan(path){for(const entry of readdirSync(path,{withFileTypes:true})){const child=join(path,entry.name);if(entry.isDirectory())scan(child);else if(entry.name.endsWith('.mjs')||entry.name==='package.json')sdkFiles.push([relative(sdkRoot,child),hash(readFileSync(child))]);}}scan(sdkRoot);sdkFiles.sort((a,b)=>a[0].localeCompare(b[0],'en'));const sdk={version:JSON.parse(readFileSync(join(sdkRoot,'package.json'))).version,files:sdkFiles.length,sha256:hash(JSON.stringify(sdkFiles))};assert.equal(sdk.version,'6.40.0');
 // Dependency injection only at the wire module boundary, preserving complete wrappers.
 let active='';const lazyEvents=[];globalThis.__imageRuntimeResult=result;globalThis.__imageRuntimeEvents=lazyEvents;
 const hooks=registerHooks({resolve(specifier,context,next){if(specifier.endsWith('openrouter-images.ts')){lazyEvents.push('load:'+active);return {url:'synthetic:image-'+active,shortCircuit:true};}return next(specifier,context);},load(url,context,next){if(url.startsWith('synthetic:image-')){const fail=url.includes('failure');return {format:'module',source:fail?'throw Error("SYNTHETIC_LOAD_ERROR");':'export function generateImages(m){globalThis.__imageRuntimeEvents.push("generate");return Promise.resolve(globalThis.__imageRuntimeResult(m));}',shortCircuit:true};}return next(url,context);}});
 const lazy=[];
 for(const mode of ['per-call-success','per-call-failure','cached-success','cached-failure']){active=mode;lazyEvents.length=0;const values=[];
  if(mode.startsWith('per-call')){const mod=await import(pathToFileURL(join(scratch,'api/openrouter-images.lazy.ts'))+'?'+mode);const api=mod.openrouterImagesApi();for(let i=0;i<2;i++){try{values.push((await api.generateImages(model,context)).stopReason);}catch(error){values.push('rejected');}}}
  else {const mod=await import(pathToFileURL(join(scratch,'providers/images/register-builtins.ts'))+'?'+mode);for(let i=0;i<2;i++)values.push((await mod.generateImagesOpenRouter({...model,api:'openrouter-images'},context)).stopReason);}
  lazy.push({mode,events:[...lazyEvents],values});
 }
 const globalImages=await import(pathToFileURL(join(scratch,'images.ts')));let unknownGlobal;try{await globalImages.generateImages({...model,api:'unknown'},context);}catch(error){unknownGlobal=error.message;}
 hooks.deregister();delete globalThis.__imageRuntimeResult;delete globalThis.__imageRuntimeEvents;
 const output={upstream:pin,node:process.version,files,driverSha256:hash(readFileSync(fileURLToPath(import.meta.url))),model,context,generations,listing,live,nestedRefresh,refresh,refreshLaunch,refreshReplacement,getAuth,binding,lazy,unknownGlobal,integration,sdk};
 const text=JSON.stringify(output,null,2)+'\n';if(process.argv.includes('--check'))assert.equal(readFileSync(join(here,'trace.json'),'utf8'),text);else writeFileSync(join(here,'trace.json'),text);console.log('Images runtime complete source verified: '+generations.length+' generation cases plus listing/refresh/auth/registry/lazy branches');
}finally{rmSync(scratch,{recursive:true,force:true});}
