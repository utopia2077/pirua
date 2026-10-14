import {createRequire} from 'node:module';import {upstreamPath} from '../../upstream.mjs';
import {createServer} from 'node:http2';
import {once} from 'node:events';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
const require=createRequire(upstreamPath('package.json'));const {NodeHttp2Handler}=require('@smithy/node-http-handler');const {HttpRequest}=require('@smithy/core/protocols');
assert.equal(require('@smithy/node-http-handler/package.json').version,'4.7.3');
assert.equal(process.versions.node,'26.8.1');
const server=createServer();const events=[];let finish;
server.on('stream',stream=>{stream.respond({':status':200},{sendDate:false});events.push('head');stream.session.once('goaway',(code,last)=>{events.push(['goaway',code,last]);stream.end('after-goaway');finish();});});
server.listen(0,'127.0.0.1');await once(server,'listening');const observed=new Promise(r=>finish=r);const handler=new NodeHttp2Handler();
try{const {response}=await handler.handle(new HttpRequest({protocol:'http:',hostname:'127.0.0.1',port:server.address().port,path:'/',method:'POST',body:'{}'}),{isEventStream:true});events.push('resolved');await observed;let text='';for await(const chunk of response.body)text+=chunk.toString();assert.equal(text,'after-goaway');events.push('body');if(process.argv.includes('--check'))assert.deepEqual(events,JSON.parse(readFileSync(new URL('./goaway.json',import.meta.url),'utf8')));else console.log(JSON.stringify(events));}finally{handler.destroy();await new Promise(r=>server.close(r));}
