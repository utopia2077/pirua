import {createRequire} from 'node:module';import {upstreamPath} from '../../upstream.mjs';
import assert from 'node:assert/strict';
const require=createRequire(upstreamPath('package.json'));
const {memoizeIdentityProvider,isIdentityExpired,doesIdentityRequireRefresh}=require('@smithy/core');
let now=1000000;Date.now=()=>now;
const rows=[];
for(const expiry of [null,300000,299999]){
 let calls=0;const reads=[];
 const provider=async options=>{calls++;reads.push(!!options?.forceRefresh);return {accessKeyId:'SYNTH_'+calls,secretAccessKey:'synthetic-secret',...(expiry===null?{}:{expiration:new Date(now+expiry)})};};
 const owner=memoizeIdentityProvider(provider,isIdentityExpired,doesIdentityRequireRefresh);
 const initial=await owner();const cached=await owner();const forced=await owner({forceRefresh:true});
 const secondOwner=memoizeIdentityProvider(provider,isIdentityExpired,doesIdentityRequireRefresh);await secondOwner();
 rows.push({expiry,initial:initial.accessKeyId,cached:cached.accessKeyId,forced:forced.accessKeyId,calls,forcedReads:reads});
}
let calls=0,release;
const owner=memoizeIdentityProvider(()=>{calls++;return new Promise(r=>release=r);},isIdentityExpired,doesIdentityRequireRefresh);
const a=owner(),b=owner();assert.equal(calls,1);release({accessKeyId:'SYNTH_SHARED',secretAccessKey:'synthetic-secret'});assert.equal(await a,await b);
const forcedA=owner({forceRefresh:true}),forcedB=owner({forceRefresh:true});assert.equal(calls,2);release({accessKeyId:'SYNTH_REFRESH',secretAccessKey:'synthetic-secret'});assert.equal(await forcedA,await forcedB);
let failedCalls=0;const failing=memoizeIdentityProvider(async()=>{failedCalls++;if(failedCalls===1)throw Error('configured failure');return {accessKeyId:'SYNTH_RECOVERED',secretAccessKey:'synthetic-secret'};},isIdentityExpired,doesIdentityRequireRefresh);
await assert.rejects(failing());await failing();assert.equal(failedCalls,2);
console.log(JSON.stringify({rows,coalescedCalls:calls,failedThenRetriedCalls:failedCalls},null,2));
