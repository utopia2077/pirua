import vm from 'node:vm';import {upstreamRoot} from '../../upstream.mjs';
import {readFileSync,writeFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import {execFileSync} from 'node:child_process';
import {stripTypeScriptTypes} from 'node:module';
import {createHash} from 'node:crypto';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const sources={};
const causal=process.argv.find(v=>v.startsWith('--causal='))?.slice(9);
const mutations={
 nonclobber:['compat.ts','if (!getApiProvider(api)) {','if (true) {'],
 remembered:['compat.ts','builtinApiProviderInstances.set(api, getApiProvider(api));','builtinApiProviderInstances.delete(api);'],
 shortcut:['compat.ts','candidate.api === model.api','candidate.id === model.id'],
 imageupsert:['providers/images/register-builtins.ts','registerImagesApiProvider({','false && registerImagesApiProvider({'],
};
function read(path){return sources[path]??=execFileSync('/usr/bin/git',['-C',upstreamRoot,'show',`${pin}:packages/ai/src/${path}`],{encoding:'utf8',env:{PATH:'/usr/bin:/bin'}});}
const context=vm.createContext({});
let envCalls=0,key='env-key',direct=0,resolved=0,loads=0,onEnv;
const factories=new Map();
const lazyNames={
'anthropic-messages':'anthropicMessagesApi','azure-openai-responses':'azureOpenAIResponsesApi','bedrock-converse-stream':'bedrockConverseStreamApi','google-generative-ai':'googleGenerativeAIApi','google-vertex':'googleVertexApi','mistral-conversations':'mistralConversationsApi','openai-codex-responses':'openAICodexResponsesApi','openai-completions':'openAICompletionsApi','openai-responses':'openAIResponsesApi','pi-messages':'piMessagesApi'};
function stream(tag){return (model,_c,options)=>({tag,model,options,result:async()=>tag});}
function synthetic(id,exports){const m=new vm.SyntheticModule(Object.keys(exports),function(){for(const [k,v]of Object.entries(exports))this.setExport(k,v)},{identifier:id,context});return m;}
const stubs=new Map();
const capturedImageBytes=readFileSync(new URL('../../catalog/generated/images/openrouter.json',import.meta.url));
const capturedImages=JSON.parse(capturedImageBytes);
stubs.set('image-models.generated.ts',synthetic('captured-images',{IMAGE_MODELS:{openrouter:Object.fromEntries(capturedImages.map(m=>[m.id,m]))}}));
for(const [id,name]of Object.entries(lazyNames)){factories.set(id,0);stubs.set(`api/${id}.lazy.ts`,synthetic(id,{[name]:()=>{factories.set(id,factories.get(id)+1);return{stream:stream(id),streamSimple:stream('simple-'+id)}}}));}
const builtin={getModels:()=>[{api:'openai-completions',id:'known'}],stream:(...args)=>{direct++;return stream('selected-direct')(...args)},streamSimple:(...args)=>{direct++;return stream('selected-simple')(...args)}};
stubs.set('providers/all.ts',synthetic('builtins',{builtinModels:()=>({getProvider:id=>id==='openrouter'?builtin:undefined,stream:()=>{resolved++;throw Error('wrong auth resolved dispatch')},streamSimple:()=>{resolved++;throw Error('wrong auth resolved dispatch')}}),getBuiltinModel:()=>undefined,getBuiltinModels:()=>[],getBuiltinProviders:()=>['openrouter']}));
stubs.set('env-api-keys.ts',synthetic('env',{getEnvApiKey:()=>{envCalls++;onEnv?.();return key}}));
stubs.set('providers/faux.ts',synthetic('excluded',{createFauxCore:()=>{throw Error('excluded function must remain uncalled')}}));
stubs.set('api/bedrock-converse-stream.ts',synthetic('bedrock',{stream:stream('bedrock-raw'),streamSimple:stream('bedrock-simple')}));
stubs.set('api/openrouter-images.ts',synthetic('image-wire',{generateImages:async()=>({stopReason:'stop',tag:'image-wire'})}));
const mods=new Map();
function resolve(spec,parent){const base=parent.split('/');base.pop();for(const part of spec.split('/')){if(part==='..')base.pop();else if(part!=='.')base.push(part)}return base.join('/');}
async function module(path){if(stubs.has(path))return stubs.get(path);if(mods.has(path))return mods.get(path);if(!['compat.ts','legacy-api-aliases.ts','bedrock-provider.ts','images.ts','images-api-registry.ts','providers/images/register-builtins.ts','session-resources.ts','image-models.ts'].includes(path)){assert(['index.ts'].includes(path),'undeclared synthetic boundary '+path);const s=synthetic(path,{});stubs.set(path,s);return s;}
let body=read(path);if(causal&&mutations[causal]?.[0]===path){const [,from,to]=mutations[causal];assert.equal(body.split(from).length,2);body=body.replace(from,to);}
const m=new vm.SourceTextModule(stripTypeScriptTypes(body),{identifier:path,context,importModuleDynamically:async(spec,parent)=>{loads++;const m=await module(resolve(spec,parent.identifier));if(m.status==='unlinked')await m.link(link);if(m.status==='linked')await m.evaluate();return m;}});mods.set(path,m);await m.link(link);return m;}
async function link(spec,parent){return module(resolve(spec,parent.identifier));}
const m=await module('compat.ts');await m.evaluate();const c=m.namespace;
const model={api:'openai-completions',provider:'openrouter',id:'NOT-IN-CATALOG'};
let overrides=0;
c.registerApiProvider({api:model.api,stream:()=>{overrides++;return{result:async()=> 'override'}},streamSimple:stream('override-simple')},'before');
const before=c.getApiProvider(model.api);c.registerBuiltInApiProviders();assert.equal(c.getApiProvider(model.api),before);assert.equal(c.stream(model,{},{}).tag,'selected-direct');assert.equal(direct,1);assert.equal(overrides,0);assert.equal(resolved,0);
c.registerApiProvider({api:model.api,stream:stream('later'),streamSimple:stream('later-simple')},'later');assert.equal(c.stream(model,{},{}).tag,'later');c.registerBuiltInApiProviders();assert.equal(c.stream(model,{},{}).tag,'selected-direct');
assert.throws(()=>before.stream({...model,api:'wrong'},{}),/Mismatched api/);
const options={apiKey:'  explicit-key  '};envCalls=0;assert.equal(c.stream(model,{},options).options,options);assert.equal(envCalls,0);assert.equal(options.apiKey,'  explicit-key  ');
key='env-key';assert.equal(c.stream(model,{},{apiKey:'  '}).options.apiKey,'env-key');key='<authenticated>';const blank={apiKey:'  '};assert.equal(c.stream(model,{},blank).options,blank);
assert.throws(()=>c.stream({...model,api:'missing'},{}),/No API provider/);
assert.equal(await c.complete(model,{},options),'selected-direct');assert.equal(await c.completeSimple(model,{},options),'selected-simple');
c.registerApiProvider({api:model.api,stream:stream('captured'),streamSimple:stream('captured-simple')},'captured');
onEnv=()=>{onEnv=undefined;c.registerApiProvider({api:model.api,stream:stream('after-env'),streamSimple:stream('after-env-simple')},'after-env');};
assert.equal(c.stream(model,{},{}).tag,'captured');assert.equal(c.stream(model,{},{}).tag,'after-env');
const order=Array.from(c.getApiProviders(),x=>x.api);c.registerApiProvider({api:model.api,stream:stream('replacement'),streamSimple:stream('replacement')},'replace');assert.deepEqual(Array.from(c.getApiProviders(),x=>x.api),order);c.unregisterApiProviders('later');assert(c.getApiProvider(model.api));c.unregisterApiProviders('replace');assert.equal(c.getApiProvider(model.api),undefined);c.resetApiProviders();assert.deepEqual(Array.from(c.getApiProviders(),x=>x.api),order);
const aliases=(await module('legacy-api-aliases.ts')).namespace;const aliasNames=Object.keys(aliases).filter(x=>x.startsWith('stream'));assert.equal(aliasNames.length,16);const counts=Array.from(factories.values());for(const name of aliasNames){aliases[name](model,{});aliases[name](model,{})}assert.deepEqual(Array.from(factories.values()),counts);
const b=await module('bedrock-provider.ts');await b.evaluate();assert.equal(b.namespace.bedrockProviderModule.stream(model,{}).tag,'bedrock-raw');
const img=(await module('images-api-registry.ts')).namespace;const im={api:'openrouter-images',provider:'openrouter',id:'image'};const kept=img.getImagesApiProvider(im.api);img.registerImagesApiProvider({api:im.api,generateImages:async()=>({tag:'override'})},'image-override');const ib=(await module('providers/images/register-builtins.ts')).namespace;ib.registerBuiltInImagesApiProviders();assert.notEqual(img.getImagesApiProvider(im.api),kept);assert.equal((await c.generateImages(im,{})).tag,'image-wire');assert.equal((await c.generateImages(im,{})).tag,'image-wire');assert.equal(loads,1);const previous=envCalls;await c.generateImages(im,{});assert.equal(envCalls,previous);await assert.rejects(c.generateImages({...im,api:'unknown'},{}),/No API provider/);assert.throws(()=>kept.generateImages({...im,api:'wrong'},{}),/Mismatched api/);
const sr=await module('session-resources.ts');await sr.evaluate();let cleaned=0;const remove=sr.namespace.registerSessionResourceCleanup(()=>{cleaned++});sr.namespace.cleanupSessionResources('one');sr.namespace.cleanupSessionResources('two');assert.equal(cleaned,2);remove();sr.namespace.cleanupSessionResources('three');assert.equal(cleaned,2);
assert.deepEqual(Array.from(c.getImageProviders()),['openrouter']);assert.equal(c.getImageModels('openrouter').length,52);assert.equal(c.getImageModel('openrouter','synthetic-unknown'),undefined);assert.equal(c.getImageModels('synthetic-unknown').length,0);
const observation={pin,capturedImagesSha256:createHash('sha256').update(capturedImageBytes).digest('hex'),imageCount:c.getImageModels('openrouter').length,hashes:Object.fromEntries(Object.entries(sources).map(([p,s])=>[p,createHash('sha256').update(s).digest('hex')])),boundaries:[...stubs.entries()].sort(([a],[b])=>a.localeCompare(b)).map(([path,module])=>({path,exports:Object.keys(module.namespace),sourceSha256:createHash('sha256').update(read(path)).digest('hex'),role:path==='image-models.generated.ts'?'dated-captured-image-input':path==='providers/faux.ts'?'excluded-uninvoked':path.startsWith('api/')?'configured-wire-boundary':path==='providers/all.ts'?'configured-builtin-direct-vs-auth-boundary':path==='env-api-keys.ts'?'synchronous-environment-boundary':'unexercised-reexport-boundary'})),order,aliases:aliasNames,retainedFactoryCounts:counts,direct,resolved,loads,cleaned};
const output=JSON.stringify(observation,null,2)+'\n';
if(process.argv.includes('--check'))assert.equal(readFileSync(new URL('source.json',import.meta.url),'utf8'),output);else if(!causal)writeFileSync(new URL('source.json',import.meta.url),output);
console.log('FACADE_COMPLETE_SOURCE_PASS: binding/order/direct/key/16aliases/images/resource-scope');
