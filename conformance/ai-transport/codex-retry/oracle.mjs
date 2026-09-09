import {runtime,authority} from '../../provider-traces/openai-codex-responses/harness.mjs';
import {readFileSync} from 'node:fs';import assert from 'node:assert/strict';
const r=runtime(),cases=[];
for(const ms of [undefined,'',' ','0x10','0b11','0o10','1.5','-5','10junk','Infinity','1e309','+0x1','0x20000000000001','0x20000000000003','0xfffffffffffffffffffff','0b'+'1'.repeat(60)])for(const seconds of [undefined,'2']){
 const headers={...(ms===undefined?{}:{'retry-after-ms':ms}),...(seconds===undefined?{}:{'retry-after':seconds})};const delay=r.deps.getRetryAfterDelayMs(new Headers(headers));cases.push({headers,expected:delay===undefined?{kind:'missing'}:Number.isFinite(delay)?{kind:'finite',value:delay}:{kind:'infinite'}});
}
for(const value of ['','0','  ','-1','1.5','0x10','1e308','Thu, 01 Jan 1970 00:00:02 GMT','1970-01-01T00:00:02Z','bad-date']){const headers={'retry-after':value},delay=r.deps.getRetryAfterDelayMs(new Headers(headers));cases.push({headers,expected:delay===undefined?{kind:'missing'}:Number.isFinite(delay)?{kind:'finite',value:delay}:{kind:'infinite'}});}
const caps=[];for(const delay of [0,1000,1000.5,60000,60001,Infinity])for(const cap of [undefined,0,1000]){let error=null;try{r.deps.validateRetryDelayMs(delay,{maxRetryDelayMs:cap});}catch(e){error=e.message;}caps.push({delay:Number.isFinite(delay)?delay:'infinity',cap:cap??null,error});}
const result={authority:authority(),cases,caps,nodeTimerProjection:'internal/timers Timeout clamps outside1..2147483647 to1; insert MathTrunc'};
if(process.argv.includes('--check'))assert.deepEqual(result,JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));else process.stdout.write(JSON.stringify(result,null,2)+'\n');
