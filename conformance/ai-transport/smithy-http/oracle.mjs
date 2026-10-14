import {createRequire,stripTypeScriptTypes} from 'node:module';import {upstreamRoot, upstreamPath} from '../../upstream.mjs';
import {execFileSync} from 'node:child_process';
import {createServer as netServer} from 'node:net';
import {createServer as h2Server} from 'node:http2';
import {createServer as h1Server,Agent} from 'node:http';
import {once} from 'node:events';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
import {Readable} from 'node:stream';
import {EventEmitter} from 'node:events';
const require=createRequire(upstreamPath('package.json'));
const {NodeHttpHandler,NodeHttp2Handler}=require('@smithy/node-http-handler');
const {HttpRequest}=require('@smithy/core/protocols');
assert.equal(require('@smithy/node-http-handler/package.json').version,'4.7.3');
assert.equal(process.versions.node,'26.8.1');
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const source=execFileSync('git',['show',pin+':packages/ai/src/api/bedrock-converse-stream.ts'],{cwd:upstreamRoot,encoding:'utf8'});
const begin=source.indexOf('function isSmithyHttpResponse('),end=source.indexOf('/**',begin);
const project=new Function(stripTypeScriptTypes(source.slice(begin,end))+'\nreturn toProviderResponse;')();
const raw=[['x-z','one'],['set-cookie','a=1'],['x-a','two'],['set-cookie','b=2'],['x-z','three'],['cookie','a=1'],['cookie','b=2'],['x-byte','\u00a0kept\u00a0'],['content-length','0']];
raw.push(...Array.from({length:80},(_,i)=>['x-grow-'+(79-i),'v']));
const cases=[];
for(const h2 of [false,true]) {
 const server=h2?h2Server():netServer(socket=>{let input='';socket.on('data',chunk=>{input+=chunk.toString();if(input.includes('\r\n\r\n')){socket.removeAllListeners('data');socket.end(Buffer.from('HTTP/1.1 200 Fine\r\n'+raw.map(([k,v])=>k+': '+v+(k==='x-z'?' \t':'')).join('\r\n')+'\r\n\r\n','latin1'));}});});
 if(h2)server.on('stream',stream=>{const headers={':status':200};for(const [k,v]of raw){if(k in headers)headers[k]=Array.isArray(headers[k])?[...headers[k],v]:[headers[k],v];else headers[k]=v;}stream.respond(headers,{sendDate:false});stream.end();});
 server.listen(0,'127.0.0.1');await once(server,'listening');const handler=h2?new NodeHttp2Handler():new NodeHttpHandler();
 try{const {response}=await handler.handle(new HttpRequest({protocol:'http:',hostname:'127.0.0.1',port:server.address().port,path:'/',method:'POST',headers:{'content-length':'2'},body:'{}'}),{isEventStream:true});for await(const _ of response.body){};const projected=project(response);cases.push({protocol:h2?'h2':'h1',raw,status:response.statusCode,entries:Object.entries(projected.headers)});}
 finally{handler.destroy();await new Promise(resolve=>server.close(resolve));}
}
const expect=[];
for(const h2 of [false,true])for(const name of ['expect','eXpEcT','dual',...(h2?[]:['external'])]) {
 let admittedBeforeContinue;
 let connection;
 const serve=(stream)=>{let data=false;stream.on('data',()=>{data=true;});setTimeout(()=>{admittedBeforeContinue=data;if(h2){stream.additionalHeaders({':status':100});stream.respond({':status':200},{sendDate:false});stream.end();}else{stream.res.writeContinue();stream.res.sendDate=false;stream.res.end();}},30);};
 const server=h2?h2Server():h1Server();if(h2)server.on('stream',serve);else server.on('checkContinue',(req,res)=>{connection=req.headers.connection;req.res=res;serve(req);});
 server.listen(0,'127.0.0.1');await once(server,'listening');const handler=h2?new NodeHttp2Handler():new NodeHttpHandler(name==='external'?{httpAgent:new Agent({keepAlive:false})}:{});
 const headers=name==='dual'?{Expect:'',expect:'100-continue'}:{[name==='external'?'expect':name]:'100-continue'};
 try{const {response}=await handler.handle(new HttpRequest({protocol:'http:',hostname:'127.0.0.1',port:server.address().port,path:'/',method:'POST',headers:{...headers,'content-length':'2'},body:'{}'}),{isEventStream:true});for await(const _ of response.body){};assert.equal(admittedBeforeContinue,name==='eXpEcT'||name==='external');if(!h2)assert.equal(connection,name==='expect'||name==='external'?'close':'keep-alive');expect.push({protocol:h2?'h2':'h1',name,admittedBeforeContinue,...(connection?{connection}:{})});}
 finally{handler.destroy();await new Promise(resolve=>server.close(resolve));}
}
const admission=[];
for(const fields of ['x-test: one\r\n two\r\n','x-test: one \t\r\n\t two \t\r\n','x-test: one\nx-b: two\n']) {
 const head='HTTP/1.1 200 OK\r\n'+fields+'content-length: 0\r\n\r\n';
 const server=netServer(socket=>socket.once('data',()=>socket.end(head)));server.listen(0,'127.0.0.1');await once(server,'listening');const handler=new NodeHttpHandler();let code;
 try{await handler.handle(new HttpRequest({protocol:'http:',hostname:'127.0.0.1',port:server.address().port,path:'/'}));assert.fail('invalid physical head must reject');}catch(error){code=error.code;assert.ok(code?.startsWith('HPE_'));}finally{handler.destroy();await new Promise(resolve=>server.close(resolve));}
 admission.push({head,errorCode:code});
}
const writerSource=readFileSync(upstreamPath('node_modules/@smithy/node-http-handler/dist-es/write-request-body.js'),'utf8');
let graceCallback,graceMs;const ends=[];
const writer=new Function('Readable','timing',writerSource.replace(/^import .*;$/gm,'').replace(/^export /gm,'')+'\nreturn writeRequestBody;')(Readable,{setTimeout:(callback,ms)=>{graceCallback=callback;graceMs=ms;return 1;},clearTimeout:()=>{}});
const sink=new EventEmitter();sink.end=value=>ends.push(value);const write=writer(sink,{headers:{expect:'100-continue'},body:'{}'});assert.equal(graceMs,6000);assert.deepEqual(ends,[]);graceCallback();await write;assert.deepEqual(ends,['{}']);
const output={pin,node:process.versions.node,smithy:'4.7.3',cases,expect,admission,grace:{milliseconds:graceMs,ends}};
if(process.argv.includes('--check'))assert.deepEqual(output,JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));
else console.log(JSON.stringify(output,null,2));
