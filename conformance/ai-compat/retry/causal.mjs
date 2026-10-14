import {upstreamEnvironment} from '../../upstream.mjs';
import {spawnSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
const expected=JSON.parse(readFileSync(new URL('source-observations.json',import.meta.url)));
for(const [mutation,id] of [['count','retry-loop.exhaustion-call-count'],['order','retry-loop.observer-order'],['classifier','retry-loop.finish-outcomes'],['sleep','retry-loop.exponential-delays']]){
 const child=spawnSync('/usr/bin/node',[new URL('source.mjs',import.meta.url).pathname],{encoding:'utf8',env:upstreamEnvironment({PATH:'/usr/bin:/bin',NODE_NO_WARNINGS:'1',RETRY_CAUSAL:mutation})});
 assert.equal(child.status,0,child.stderr);
 const actual=JSON.parse(child.stdout);
 const before=expected.observations.find(v=>v.id===id),after=actual.observations.find(v=>v.id===id);
 assert(before&&after);
 assert.throws(()=>assert.deepEqual(after,before),{code:'ERR_ASSERTION'});
 console.log(`${mutation}: ${id} fails its intended source comparison`);
}
