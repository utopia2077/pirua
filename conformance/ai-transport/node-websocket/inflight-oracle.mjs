import zlib from 'node:zlib';
import {createServer} from 'node:http';
import {createHash} from 'node:crypto';
import {once} from 'node:events';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
import {createRequire} from 'node:module';
// This isolated oracle retains the actual Node inflater. Only delivery of its
// completed flush callback is held at an explicit causal barrier, before Undici
// publishes the message. No fake compressed output or arbitrary sleep is used.
const original=zlib.createInflateRaw;let completed,release;
let stage='initial';const watchdog=setTimeout(()=>{throw Error('oracle stalled at '+stage);},3000);
const wrappedInflate=(...args)=>{
 stage='inflate-created';const stream=original(...args),flush=stream.flush;
 stream.flush=function(callback){return flush.call(this,()=>{release=callback;completed();});};return stream;
};
// Execute the complete fingerprinted bundled source with a single dependency
// seam; Node's preloaded global constructor keeps its private builtin binding.
const bundled=process.binding('natives')['internal/deps/undici/undici'];
assert.equal(createHash('sha256').update(bundled).digest('hex'),'9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d');
const require=createRequire(import.meta.url),module={exports:{}};
new Function('require','module','exports',bundled)(name=>name==='node:zlib'?{...zlib,createInflateRaw:wrappedInflate}:require(name),module,module.exports);
const ControlledWebSocket=module.exports.WebSocket;
const frame=(op,data)=>{data=Buffer.from(data);assert(data.length<126);return Buffer.concat([Buffer.from([128|op,data.length]),data]);};
const compressed=zlib.deflateRawSync('compressed',{finishFlush:zlib.constants.Z_SYNC_FLUSH}).subarray(0,-4);
const results=[];
for(const closedFirst of [true,false]){
 const server=createServer();let peer;const sockets=new Set();server.on('connection',s=>{sockets.add(s);s.on('close',()=>sockets.delete(s));});
 server.on('upgrade',(req,s)=>{peer=s;const key=createHash('sha1').update(req.headers['sec-websocket-key']+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');s.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${key}\r\nSec-WebSocket-Extensions: permessage-deflate\r\n\r\n`);s.once('data',()=>{const bytes=frame(1,compressed);bytes[0]|=64;s.write(Buffer.concat([frame(1,'already-admitted'),bytes]));});});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try {
  const barrier=new Promise(r=>completed=r),events=[];
  const socket=new ControlledWebSocket(`ws://127.0.0.1:${server.address().port}/barrier`);
  socket.addEventListener('message',e=>events.push({type:'message',text:e.data}));socket.addEventListener('error',()=>events.push({type:'error'}));socket.addEventListener('close',e=>events.push({type:'close',code:e.code,reason:e.reason}));
  await once(socket,'open');stage='opened';socket.send('start');await barrier;stage='barrier';
  const closing=once(socket,'close');
  if(closedFirst){peer.end();await closing;release();}
  else {const message=once(socket,'message');release();await message;peer.end();await closing;}
  await new Promise(r=>setImmediate(r));
  assert.equal(events.filter(e=>e.type==='message').length,closedFirst?1:2);
  results.push({closedFirst,events});
 }finally{for(const s of sockets)s.destroy();await new Promise(r=>server.close(r));}
}
clearTimeout(watchdog);const result={node:process.version,undici:process.versions.undici,results};
if(process.argv.includes('--check'))assert.deepEqual(result,JSON.parse(readFileSync(new URL('./inflight.json',import.meta.url),'utf8')));else console.log(JSON.stringify(result,null,2));
