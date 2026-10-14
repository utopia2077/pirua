import {execFileSync} from 'node:child_process';import {stripTypeScriptTypes} from 'node:module';import net from 'node:net';import tls from 'node:tls';import http2 from 'node:http2';import {existsSync} from 'node:fs';import {homedir} from 'node:os';import {join} from 'node:path';import {fileURLToPath} from 'node:url';import {createRequire} from 'node:module';import assert from 'node:assert/strict';import {upstreamRoot, upstreamPath} from '../../upstream.mjs';
const require=createRequire(upstreamPath('package.json'));const {NodeHttpHandler,NodeHttp2Handler}=require('@smithy/node-http-handler');const{HttpRequest}=require('@smithy/core/protocols');const{isTransientError,isThrottlingError}=require('@smithy/core/retry');
const source=execFileSync('git',['show','853a80d26c90a14c1886f0ebb8ffaae133ca2185:packages/ai/src/utils/retry.ts'],{cwd:upstreamRoot,encoding:'utf8'});const isRetryableAssistantError=Function(stripTypeScriptTypes(source.replace(/^import[^\n]*\n/gm,'').replace(/^export /gm,''))+';return isRetryableAssistantError;')();
const rows=[];
async function run(id,server,handler=new NodeHttpHandler(),protocol='http:',portHint=0){
 await new Promise(r=>server.listen(portHint,'127.0.0.1',r));const port=server.address().port;if(id.startsWith('refused'))await new Promise(r=>server.close(r));let stage='head';try{const{response}=await handler.handle(new HttpRequest({protocol,hostname:'127.0.0.1',port,method:'GET',path:'/',headers:{}}));stage='body';for await(const chunk of response.body){}rows.push({id,success:true});}catch(e){rows.push({id,stage,name:e.name,code:e.code??null,message:e.message.replace(/127\.0\.0\.1:\d+/g,"127.0.0.1:<port>"),assistantRetry:isRetryableAssistantError({stopReason:"error",errorMessage:e.message}),transient:isTransientError(e),throttling:isThrottlingError(e)});}finally{handler.destroy();if(server.listening)await new Promise(r=>server.close(r));}}
await run('refused',net.createServer());
await run('reset-before-head',net.createServer(s=>s.once('data',()=>s.destroy())));
await run('malformed-head',net.createServer(s=>s.once('data',()=>s.end('NOT HTTP\r\n\r\n'))));
await run('truncated-body',net.createServer(s=>s.once('data',()=>s.end('HTTP/1.1 200 OK\r\nContent-Length: 9\r\n\r\na'))));
await run('malformed-chunk',net.createServer(s=>s.once('data',()=>s.end('HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nNO\r\n'))));
const rustupCargo=join(homedir(),'.cargo/bin/cargo');
const cargo=process.env.PIRUA_CARGO||(existsSync(rustupCargo)?rustupCargo:'cargo');
let material;
try{material=JSON.parse(execFileSync(cargo,['run','--quiet','--locked','--offline','-p','pi-ai-transport','--example','loopback_tls'],{cwd:fileURLToPath(new URL('../../../',import.meta.url)),encoding:'utf8',stdio:['ignore','pipe','pipe']}));}catch{throw Error('ephemeral loopback TLS helper failed');}
const pem=(label,bytes)=>'-----BEGIN '+label+'-----\n'+Buffer.from(bytes).toString('base64').match(/.{1,64}/g).join('\n')+'\n-----END '+label+'-----\n';
const cert=tls.createServer({key:pem('PRIVATE KEY',material.privateKeyDer),cert:pem('CERTIFICATE',material.certificateDer)});cert.on('tlsClientError',()=>{});await run('untrusted-cert',cert,new NodeHttpHandler(),'https:');
for(const[ id,reason]of [['h2-refused',http2.constants.NGHTTP2_REFUSED_STREAM],['h2-protocol',http2.constants.NGHTTP2_PROTOCOL_ERROR]]){const server=http2.createServer();server.on('stream',s=>{s.on('error',()=>{});s.close(reason)});await run(id,server,new NodeHttp2Handler());}
for(const port of [40001,42901,50001,50201,50301,50401,52401]){await run('refused-port-'+port,net.createServer(),new NodeHttpHandler(),'http:',port);}
assert.equal(rows.length,15);assert.deepEqual(rows.slice(8).map(r=>r.assistantRetry),[false,true,true,true,true,true,true]);assert.deepEqual(rows.slice(0,8).map(r=>r.transient),[true,true,false,true,true,false,true,false]);console.log(JSON.stringify(rows,null,2));
