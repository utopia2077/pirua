// Execute the complete pinned module. Replace only deterministic host timers/producer/observers.
import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {stripTypeScriptTypes} from 'node:module';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const path='packages/ai/src/utils/retry.ts';
const source=execFileSync('/usr/bin/git',['-C',upstreamRoot,'show',`${pin}:${path}`],{encoding:'utf8',env:{PATH:'/usr/bin:/bin'}});
const mutations={
 count:['policy.maxRetries : 0','Math.max(0, policy.maxRetries - 1) : 0'],
 order:['await callbacks?.onRetryAttemptStart?.();',''],
 classifier:['"billing",','"unused synthetic billing marker",'],
 sleep:['await sleep(delayMs, signal);','await sleep(1, signal);']
};
const mutation=process.env.RETRY_CAUSAL;
let executed=source;
if(mutation){assert(Object.hasOwn(mutations,mutation));const [from,to]=mutations[mutation];assert.equal(source.split(from).length,2);executed=source.replace(from,to);}
const {retryAssistantCall}=await import('data:text/javascript;base64,'+Buffer.from(stripTypeScriptTypes(executed)).toString('base64'));
const hash=s=>createHash('sha256').update(s).digest('hex');
const timers=process.binding('natives')['internal/timers'];
assert(timers.includes('msecs = MathTrunc(msecs)'));
assert(timers.includes('if (!(after >= 1 && after <= TIMEOUT_MAX))'));
const nativeSet=globalThis.setTimeout,nativeClear=globalThis.clearTimeout;
const timerObservations=[0,1,2.5,2147483647,2147483648,Infinity,NaN].map(raw=>{const timer=nativeSet(()=>{},raw);const admitted=timer._idleTimeout;nativeClear(timer);return {raw:Number.isNaN(raw)?'NaN':raw===Infinity?'Infinity':raw,admitted,effective:Math.trunc(admitted)};});
const cases=JSON.parse(readFileSync(new URL('cases.json',import.meta.url),'utf8'));
const observations=[];
for(const c of cases){
 assert([undefined,'cancel','fail'].includes(c.sleep),'unknown sleeper capability');
 assert([undefined,'scheduled','start','finish'].includes(c.fail),'unknown observer capability');
 assert(c.sequence.every(value=>['error','stop','aborted','billing','reject'].includes(value)),'unknown producer capability');
 const trace=[];let calls=0;const controller=new AbortController();if(c.preCancelled)controller.abort();
 const policy=Object.hasOwn(c,'policy')?c.policy:{enabled:true,maxRetries:2,baseDelayMs:2};
 let timerEntered=false,abortDelivered=false;
 globalThis.setTimeout=(resolve,ms)=>{timerEntered=true;trace.push(['sleep',ms]);if(c.sleep==='fail')throw Error('sleep');const handle={closed:false};queueMicrotask(()=>{if(c.sleep==='cancel'){abortDelivered=true;controller.abort();}else if(!handle.closed)resolve();});return handle;};
 globalThis.clearTimeout=handle=>{handle.closed=true;};
 const callbacks={
  onRetryScheduled:async(...args)=>{trace.push(['scheduled',...args]);if(c.cancelScheduled)controller.abort();if(c.fail==='scheduled')throw Error('scheduled');},
  onRetryAttemptStart:async()=>{trace.push(['start']);if(c.fail==='start')throw Error('start');},
  onRetryFinished:async(success,attempt,error)=>{trace.push(['finish',success,attempt,error??null]);if(c.fail==='finish')throw Error('finish');}
 };
 const produce=async()=>{const value=c.sequence[calls++];assert(value,'unexpected extra produce');trace.push(['produce',calls]);if(c.cancelProducer)controller.abort();if(value==='reject')throw Error('produce');return {role:'assistant',api:'fixture',provider:'fixture',model:'fixture',timestamp:0,content:[{type:'text',text:'retained'}],usage:{input:17,output:0,cacheRead:0,cacheWrite:0,totalTokens:17,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}},stopReason:value==='billing'?'error':value,errorMessage:value==='error'?'503 overloaded':value==='billing'?'billing 429':value==='aborted'?'provider aborted':undefined};};
 let result;
 try {const r=await retryAssistantCall(produce,policy,controller.signal,callbacks);result={stopReason:r.stopReason,errorMessage:r.errorMessage??null,content:r.content,input:r.usage.input};}
 catch(e){result={rejected:e.message};}
 if(c.sleep==='cancel'){assert(timerEntered&&abortDelivered,'cancellation branch must actually enter timer/abort listener');if(c.fail!=='finish'){assert.equal(result.stopReason,'aborted');assert.equal(result.errorMessage,null);}}
 observations.push({id:c.id,trace,result});
}
const extremes=[];
const numberBits=value=>{if(Number.isNaN(value))return 'NaN';if(value===Infinity)return 'Infinity';const b=Buffer.alloc(8);b.writeDoubleBE(value);return b.toString('hex');};
for(const base of [0,1,9007199254740991]){
 let calls=0;const scheduled=[],slept=[];
 globalThis.setTimeout=(resolve,ms)=>{slept.push(numberBits(ms));queueMicrotask(resolve);return {};};
 const result=await retryAssistantCall(async()=>({stopReason:++calls>1025?'stop':'error',errorMessage:'503 overloaded'}),{enabled:true,maxRetries:1025,baseDelayMs:base},undefined,{onRetryScheduled:(_a,_m,d)=>scheduled.push(numberBits(d))});
 if(!mutation){assert.equal(result.stopReason,'stop');assert.equal(calls,1026);assert.deepEqual(scheduled,slept);}
 extremes.push({base,calls,delays:scheduled});
}
globalThis.setTimeout=nativeSet;globalThis.clearTimeout=nativeClear;
console.log(JSON.stringify({pin,path,sourceSha256:hash(source),node:process.version,timersSha256:hash(timers),timerObservations,extremes,observations},null,2));
