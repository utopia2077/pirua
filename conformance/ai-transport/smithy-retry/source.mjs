import {createRequire} from 'node:module';
import {execFileSync} from 'node:child_process';
import assert from 'node:assert/strict';
if(process.argv[2]!=='child'){
 const rows=[];for(const flag of [null,'false','TRUE','1','true']){
  const env={PATH:'/usr/bin:/bin',NODE_NO_WARNINGS:'1',...(flag===null?{}:{SMITHY_NEW_RETRIES_2026:flag})};
  rows.push(JSON.parse(execFileSync(process.execPath,[import.meta.filename,'child'],{env,encoding:'utf8'})));
 }
 assert(rows.every(row=>row.active===(row.flag==='true')));console.log(JSON.stringify(rows,null,2));
}else{
 const require=createRequire('upstream/pi-mono/package.json');
 const r=require('@smithy/core/retry'),{HttpResponse}=require('@smithy/core/protocols');
 let now=1767323045000;Date.now=()=>now;Math.random=()=>0.5;
 const flag=process.env.SMITHY_NEW_RETRIES_2026??null,active=r.Retry.v2026;
 process.env.SMITHY_NEW_RETRIES_2026=active?'false':'true';assert.equal(r.Retry.v2026,active);
 const constants={delay:r.Retry.delay(),throttleDelay:r.Retry.throttlingDelay(),cost:r.Retry.cost(),modifiedCost:r.Retry.throttlingCost(),modifiedClass:r.Retry.modifiedCostType()};
 const cases=[];
 for(const type of ['TRANSIENT','THROTTLING','SERVER_ERROR','CLIENT_ERROR']){
  const s=new r.StandardRetryStrategy(3);let token=await s.acquireInitialRetryToken('bedrock');const events=[{initialDelay:token.getRetryDelay(),capacity:s.getCapacity()}];
  for(let i=0;i<3;i++){try{token=await s.refreshRetryTokenForRetry(token,{errorType:type});events.push({retry:token.getRetryCount(),delay:token.getRetryDelay(),cost:token.getRetryCost(),capacity:s.getCapacity()});}catch(e){events.push({rejected:true,capacity:s.getCapacity()});break;}}
  s.recordSuccess(token);events.push({successCapacity:s.getCapacity()});cases.push({type,events});
 }
 const quotas=[];for(const type of ['TRANSIENT','THROTTLING']){const s=new r.StandardRetryStrategy(1000);let t=await s.acquireInitialRetryToken('bedrock'),count=0;while(true){try{t=await s.refreshRetryTokenForRetry(t,{errorType:type});count++;}catch{break;}}quotas.push({type,count,capacity:s.getCapacity(),lastDelay:t.getRetryDelay()});}
 const hints=[];for(const hint of [-1000,0,200,10000,100000]){const s=new r.StandardRetryStrategy(3);let t=await s.acquireInitialRetryToken('bedrock');t=await s.refreshRetryTokenForRetry(t,{errorType:'TRANSIENT',retryAfterHint:new Date(now+hint)});hints.push({hint,delay:t.getRetryDelay()});}
 const parsed=[];for(const headers of [{'retry-after':'2.5'},{'retry-after':' 2 '},{'retry-after':'Wed, 01 Jan 2025 00:00:00 GMT'},{'retry-after':'Wed, 01 Jan 2025 00:00:00 GMT, 3.5'},{'x-amz-retry-after':'-10'},{'x-amz-retry-after':'','retry-after':'5'},{'retry-after':'no','x-amz-retry-after':'5'},...['inf','Infinity','+Infinity','-Infinity','infinity','1e999','0x10','0b11','0o10','-.5','  ','NaN'].map(v=>({'x-amz-retry-after':v}))]){const date=r.getRetryAfterHint(new HttpResponse({statusCode:503,headers}));parsed.push({headers,headerEntries:Object.entries(headers),present:!!date,invalid:!!date&&Number.isNaN(date.getTime()),delay:date?date.getTime()-now:null});}
 const adaptive=[];const waits=[];r.DefaultRateLimiter.setTimeoutFn=(cb,ms)=>{waits.push(ms);now+=Math.ceil(ms);cb();};
 const limiter=new r.DefaultRateLimiter();const s=new r.AdaptiveRetryStrategy(4,{rateLimiter:limiter});
 const snap=event=>({event,time:now,enabled:limiter.enabled,fillRate:limiter.fillRate,capacity:limiter.maxCapacity,tokens:limiter.availableTokens,measured:limiter.measuredTxRate,waits:waits.splice(0)});
 let token=await s.acquireInitialRetryToken('bedrock');adaptive.push(snap('initial'));
 for(const errorType of ['THROTTLING','TRANSIENT','THROTTLING']){token=await s.refreshRetryTokenForRetry(token,{errorType});adaptive.push({...snap(errorType),retryDelay:token.getRetryDelay()});now+=token.getRetryDelay();}
 s.recordSuccess(token);adaptive.push(snap('success'));
 console.log(JSON.stringify({flag,active,constants,cases,quotas,hints,parsed,adaptive}));
}
