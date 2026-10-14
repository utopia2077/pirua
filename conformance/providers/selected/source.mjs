// Complete pinned factory/all/model/lazy/auth source, with captured catalog inputs.
// Excluded factories are classification-only sentinels; no excluded body is claimed.
import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {readFileSync,writeFileSync,mkdtempSync,rmSync} from 'node:fs';
import {registerHooks} from 'node:module';
import {dirname,resolve,join} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {tmpdir} from 'node:os';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const here=dirname(fileURLToPath(import.meta.url)),root=resolve(here,'../../..');
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185',upstream=upstreamRoot;
const ids=['anthropic','deepseek','google','openai','openai-codex','openrouter'];
const temp=mkdtempSync(join(tmpdir(),'selected-factory-source-')),base=pathToFileURL(temp+'/').href,files={},inputs={};
const hash=b=>createHash('sha256').update(b).digest('hex');
const input=p=>{const b=readFileSync(join(root,p));inputs[p]=hash(b);return JSON.parse(b);};
const catalogs=Object.fromEntries(ids.map(id=>[id,Object.fromEntries(input(`conformance/catalog/generated/text/${id}.json`).map(m=>[m.id,m]))]));
const images={openrouter:Object.fromEntries(input('conformance/catalog/generated/images/openrouter.json').map(m=>[m.id,m]))};
const source=p=>{const b=execFileSync('git',['-C',upstream,'show',`${pin}:packages/ai/src/${p}`],{env:{PATH:'/usr/bin:/bin'}});files['packages/ai/src/'+p]=hash(b);return b.toString();};
const all=source('providers/all.ts'),excluded={};
for(const m of all.matchAll(/import \{ (\w+Provider) \} from "\.\/([^"/]+)\.ts"/g))if(![...ids,'openrouter-images'].includes(m[2]))excluded[m[2]]=m[1];
let loads=0;globalThis.factoryLoad=async()=>{loads++;throw Error('synthetic unavailable flow');};
const hooks=registerHooks({resolve(spec,context,next){if(spec.startsWith(base))return {url:spec,shortCircuit:true};if(context.parentURL?.startsWith(base)&&spec.startsWith('.'))return {url:new URL(spec,context.parentURL).href,shortCircuit:true};return next(spec,context);},load(url,ctx,next){if(!url.startsWith(base))return next(url,ctx);const p=url.slice(base.length);let code;
 if(p==='models.generated.ts')code=`export const MODELS=${JSON.stringify(catalogs)};`;
 else if(p==='image-models.generated.ts')code=`export const IMAGE_MODELS=${JSON.stringify(images)};`;
 else if(p==='providers/data/.manifest.json')return {format:'json',source:JSON.stringify({generatedAt:'2026-09-07T06:59:46.000Z'}),shortCircuit:true};
 else if(/^providers\/.+\.models\.ts$/.test(p)){const id=p.slice(10,-10);assert(ids.includes(id));code=`export const ${id.replaceAll('-','_').toUpperCase()}_MODELS=${JSON.stringify(catalogs[id])};`;}
 else if(p.startsWith('providers/')&&excluded[p.slice(10,-3)]){const id=p.slice(10,-3);code=`export function ${excluded[id]}(){return {id:${JSON.stringify(id)},excluded:true,auth:{},getModels:()=>[]};}`;}
 else if(p==='auth/oauth/load.ts')code='export const loadAnthropicOAuth=()=>globalThis.factoryLoad(),loadOpenAICodexOAuth=loadAnthropicOAuth,loadOpenRouterOAuth=loadAnthropicOAuth;';
 else code=source(p);
 return {format:'module-typescript',source:code,shortCircuit:true};}});
try {
 const api=await import(base+'providers/all.ts');
 const full=api.builtinProviders(),selected=full.filter(p=>ids.includes(p.id)),im=api.builtinImagesProviders();
 assert.equal(loads,0);assert.deepEqual(selected.map(p=>p.id),ids);
 const rows=[];
 for(const p of [...selected,...im]){
  const envCalls=[],signal=new AbortController().signal;
  const key=p.auth.apiKey;const resolved=key?await key.resolve({ctx:{env:async n=>{envCalls.push(n);return n==='ANTHROPIC_AUTH_TOKEN'?undefined:'SYNTHETIC_KEY';}},signal}):undefined;
  rows.push({id:p.id,name:p.name,namespace:im.includes(p)?'image':'text',key:key?.name??null,oauth:p.auth.oauth?{name:p.auth.oauth.name,isSubscription:p.auth.oauth.isSubscription??false,loginLabel:p.auth.oauth.loginLabel??null}:null,models:p.getModels().map(m=>m.id),refresh:p.refreshModels!==undefined,fetch:p.fetchDeferred!==undefined,cancel:p.cancelDeferred!==undefined,envCalls,authSource:resolved?.source??null});
 }
 const registry=api.builtinModels({authContext:{env:async()=>undefined,fileExists:async()=>false}}),imageRegistry=api.builtinImagesModels({authContext:{env:async()=>undefined,fileExists:async()=>false}});
 assert.equal(loads,0);
 const fresh=api.builtinProviders().filter(p=>ids.includes(p.id));assert(selected.every((p,i)=>p!==fresh[i]));
 const unknown={models:api.getBuiltinModels('synthetic-unknown'),model:api.getBuiltinModel('synthetic-unknown','x')??null};
 await assert.rejects(selected[0].auth.oauth.toAuth({},new AbortController().signal),/synthetic unavailable/);
 await assert.rejects(selected[0].auth.oauth.toAuth({},new AbortController().signal),/synthetic unavailable/);assert.equal(loads,1);
 const observation={rows,order:registry.getProviders().filter(p=>ids.includes(p.id)).map(p=>p.id),imageOrder:imageRegistry.getProviders().map(p=>p.id),catalogOrder:api.getBuiltinProviders(),generatedAt:api.getBuiltinModelDataGeneratedAt(),unknown,loads,excluded:full.filter(p=>p.excluded).map(p=>p.id)};
 const output=JSON.stringify({pin,files,inputs,observation},null,2)+'\n';
 if(process.argv.includes('--check'))assert.equal(readFileSync(join(here,'trace.json'),'utf8'),output);else writeFileSync(join(here,'trace.json'),output);
 console.log('selected factories: seven complete bodies; all.ts; actual models/lazy/auth helpers; source order/passive/cache controls passed');
}finally{hooks.deregister();delete globalThis.factoryLoad;rmSync(temp,{recursive:true,force:true});}
