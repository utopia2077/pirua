// Complete pinned helper/PKCE modules and complete Anthropic factory body.
// Only module dependencies unrelated to auth are configured synthetic bindings.
import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {registerHooks,stripTypeScriptTypes} from 'node:module';
import {createHash,webcrypto} from 'node:crypto';
import assert from 'node:assert/strict';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const root=upstreamRoot;
const hashes={};
registerHooks({resolve(specifier,context,next){
 if(specifier.startsWith('pinned:'))return {url:specifier,shortCircuit:true};
 if(context.parentURL?.startsWith('pinned:') && specifier.startsWith('.')){const p=new URL(specifier,'file:///'+context.parentURL.slice(7)).pathname.slice(1);return {url:'pinned:'+p,shortCircuit:true};}
 return next(specifier,context);
},load(url,context,next){
 if(!url.startsWith('pinned:'))return next(url,context);
 const p=url.slice(7);let source;
 if(p==='packages/ai/src/models.ts')source='export const createProvider=x=>x';
 else if(p==='packages/ai/src/providers/anthropic.models.ts')source='export const ANTHROPIC_MODELS={}';
 else if(p==='packages/ai/src/api/anthropic-messages.lazy.ts')source='export const anthropicMessagesApi=()=>({})';
 else {source=execFileSync('/usr/bin/git',['-C',root,'show',pin+':'+p],{encoding:'utf8',env:{PATH:'/usr/bin:/bin'}});hashes[p]=createHash('sha256').update(source).digest('hex');source=stripTypeScriptTypes(source);}
 return {format:'module',source,shortCircuit:true};
}});
Object.defineProperty(globalThis,'crypto',{value:{getRandomValues:b=>{for(let i=0;i<b.length;i++)b[i]=i;return b;},subtle:webcrypto.subtle}});
const {generatePKCE}=await import('pinned:packages/ai/src/auth/oauth/pkce.ts');
const pkce=await generatePKCE();
const {envApiKeyAuth,lazyOAuth}=await import('pinned:packages/ai/src/auth/helpers.ts');
const {anthropicProvider}=await import('pinned:packages/ai/src/providers/anthropic.ts');
const {defaultProviderAuthContext}=await import('pinned:packages/ai/src/auth/context.ts');
const signal=new AbortController().signal;const seen=[];
const key=envApiKeyAuth('Fixture',['ONE','TWO']);const emptyStored=await key.resolve({ctx:{env:async n=>{seen.push(n);return n==='ONE'?'':' '}},credential:{type:'api_key',key:''},signal});assert.equal(emptyStored.auth.apiKey,' ');
const bearer=await anthropicProvider().auth.apiKey.resolve({ctx:{env:async n=>n==='ANTHROPIC_AUTH_TOKEN'?'synthetic-bearer':'synthetic-other'},signal});assert.equal(bearer.auth.headers.Authorization,'Bearer synthetic-bearer');
process.env.SYNTHETIC_BOM='\ufeff \t';process.env.SYNTHETIC_NEL='\u0085';const ctx=defaultProviderAuthContext();assert.equal(await ctx.env('SYNTHETIC_BOM'),undefined);assert.equal(await ctx.env('SYNTHETIC_NEL'),'\u0085');
const memo=[];
for(const fail of [false,true]){let loads=0;const flow=lazyOAuth({name:'Fixture',load:async()=>{loads++;if(fail)throw Error('synthetic failure');return {toAuth:async()=>({}),refresh:async c=>c};}});for(let i=0;i<2;i++)await flow.toAuth({}).catch(()=>{});memo.push(loads);assert.equal(loads,1);}
console.log(JSON.stringify({pin,hashes,observations:{pkce,emptyStoredFallsThrough:emptyStored.source,envReads:seen,anthropicSource:bearer.source,bomAbsent:true,nelPresent:true,lazyLoadCounts:memo}},null,2));
