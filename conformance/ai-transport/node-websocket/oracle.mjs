import {createServer} from 'node:http';
import {createHash} from 'node:crypto';
import {createDeflateRaw,deflateRawSync,constants} from 'node:zlib';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
const actualHash=createHash('sha256').update(process.binding('natives')['internal/deps/undici/undici']).digest('hex');
assert.equal(process.version,'v26.8.1');assert.equal(process.versions.undici,'8.10.0');
assert.equal(actualHash,'9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d');
if(Object.keys(process.env).some(k=>/^(OPENAI_|AZURE_|ANTHROPIC_)/.test(k)))throw Error('sanitize environment');
function frame(op,data,fin=true,rsv=false){data=Buffer.from(data);let head;
 if(data.length<126)head=Buffer.from([(fin?128:0)|(rsv?64:0)|op,data.length]);
 else head=Buffer.from([(fin?128:0)|(rsv?64:0)|op,126,data.length>>8,data.length&255]);
 return Buffer.concat([head,data]);}
const compressed=text=>deflateRawSync(Buffer.from(text),{finishFlush:constants.Z_SYNC_FLUSH}).subarray(0,-4);
const close=(code=1000,reason='')=>frame(8,Buffer.concat([Buffer.from([code>>8,code&255]),Buffer.from(reason)]));
const cases=[
 {id:'text',frames:[frame(1,'hello'),close()]},
 {id:'text-bom',frames:[frame(1,'\ufeffhello'),close()]},
 {id:'binary',frames:[frame(2,[0,255,128,239,187,191]),close()]},
 {id:'empty',frames:[frame(1,''),frame(2,[]),close()]},
 {id:'fragment-ping',frames:[frame(1,'hel',false),frame(9,'ping'),frame(0,'lo'),close()]},
 {id:'fragment-utf8',frames:[frame(1,[240,159],false),frame(0,[146,161]),close()]},
 {id:'empty-fragment',frames:[frame(1,'',false),frame(0,'done'),close()]},
 {id:'compressed',extension:'permessage-deflate',frames:[frame(1,compressed('compressed'),true,true),close()]},
 {id:'compressed-binary',extension:'permessage-deflate',frames:[frame(2,compressed('binary'),true,true),close()]},
 {id:'close-reason',frames:[frame(1,'before'),close(1000,'peer-reason')]},
 {id:'close-empty',frames:[frame(8,[])]},
 {id:'close-1009',frames:[close(1009)]},
 {id:'close-double-bom',frames:[close(1000,'\ufeff\ufeffreason')]},
 {id:'invalid-text',frames:[frame(1,[255])]},
 {id:'invalid-close-code',frames:[close(1005)]},
 {id:'invalid-close-body',frames:[frame(8,[1])]},
 {id:'invalid-close-text',frames:[frame(8,[3,232,255])]},
 {id:'unexpected-continuation',frames:[frame(0,'bad')]},
 {id:'rsv1-without-negotiation',frames:[frame(1,compressed('bad'),true,true)]},
 {id:'prefix-before-codec-error',extension:'permessage-deflate',frames:[frame(1,'prefix'),frame(1,[255,255],true,true)]},
 {id:'partial-eof',frames:[Buffer.from([129,5,97,98])]},
 {id:'plain-eof',frames:[frame(1,'prefix')]},
 {id:'unknown-extension',extension:'other',frames:[]},
];
for(const window of ['8','9','10','11','12','13','14','15','08','','7','16','nine'])cases.push({id:'window-'+(window||'empty'),extension:'permessage-deflate; server_max_window_bits='+window,frames:[frame(1,compressed('window'),true,true),close()]});
for(const flag of ['', '; server_no_context_takeover']){
 const z=createDeflateRaw();let out=[];z.on('data',b=>out.push(b));
 const encode=async text=>{out=[];z.write(text);await new Promise(r=>z.flush(constants.Z_SYNC_FLUSH,r));return Buffer.concat(out).subarray(0,-4);};
 const text='dictionary-'+('abcdef0123456789'.repeat(40)),first=await encode(text),second=await encode(text);z.destroy();
 const half=Math.floor(second.length/2);
 cases.push({id:flag?'context-no-takeover':'context-takeover',extension:'permessage-deflate'+flag,frames:[frame(1,first,true,true),frame(1,second.subarray(0,half),false,true),frame(9,'p'),frame(0,second.subarray(half)),close()]});
}
const results=[];
cases.push({id:'informational-head',informational:true,frames:[frame(1,'hello'),close()]});
for(const [id,offered,responseProtocol] of [['protocol-accepted','alpha, beta','beta'],['protocol-quoted','"alpha,beta", other','"alpha,beta"']])cases.push({id,inputHeaders:{'sec-websocket-protocol':offered},responseProtocol,frames:[frame(1,'hello'),close()]});
for(const [id,inputHeaders] of [
 ['range-identity',{range:'bytes=0-1'}],['range-append',{range:'bytes=0-1','accept-encoding':'gzip'}],
 ['host-ignored',{host:'other.invalid'}],['cache-overrides',{pragma:'','cache-control':'custom'}],
 ['custom-extensions',{'sec-websocket-extensions':'private'}],
])cases.push({id,inputHeaders,frames:[frame(1,'hello'),close()]});
// Isolated source-crash witness: Node calls includes on null. Rust safely
// rejects this invalid handshake; it cannot claim a matching Node error event.
if(process.argv.includes('--unrequested-protocol'))cases.splice(0,cases.length,{id:'protocol-unrequested',responseProtocol:'other',frames:[]});
for(const item of cases){
 const sockets=new Set(),server=createServer();let requestHeaders;
 server.on('connection',s=>{sockets.add(s);s.on('close',()=>sockets.delete(s));});
 server.on('upgrade',(req,s)=>{requestHeaders={...req.headers};delete requestHeaders.host;delete requestHeaders['sec-websocket-key'];
  const accept=createHash('sha1').update(req.headers['sec-websocket-key']+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  s.write((item.informational?'HTTP/1.1 103 Early Hints\r\nLink: </safe>\r\n\r\n':'')+`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n${item.extension===undefined?'':`Sec-WebSocket-Extensions: ${item.extension}\r\n`}${item.responseProtocol===undefined?'':`Sec-WebSocket-Protocol: ${item.responseProtocol}\r\n`}\r\n`);
  let initial=true,pending=Buffer.alloc(0);
  s.on('data',bytes=>{
   pending=Buffer.concat([pending,bytes]);
   if(initial){initial=false;if(['plain-eof','partial-eof'].includes(item.id))s.end(Buffer.concat(item.frames));else s.write(Buffer.concat(item.frames));}
   while(pending.length>=6){let length=pending[1]&127,offset=2;if(length===126){if(pending.length<8)return;length=pending.readUInt16BE(2);offset=4;}else if(length===127){if(pending.length<14)return;length=Number(pending.readBigUInt64BE(2));offset=10;}const end=offset+4+length;if(pending.length<end)return;const opcode=pending[0]&15;pending=pending.subarray(end);if(opcode===8)s.end();}
  });});
 await new Promise(r=>server.listen(0,'127.0.0.1',r));
 try {
  const events=[],work=[];let opened=false;
  await new Promise((resolve,reject)=>{
   const timer=setTimeout(()=>reject(Error('oracle timeout '+item.id)),3000);
   const ws=new WebSocket(`ws://127.0.0.1:${server.address().port}/profile`,{headers:{'x-synthetic':'value',...item.inputHeaders}});
   ws.addEventListener('open',()=>{opened=true;ws.send('request');});
   ws.addEventListener('message',event=>{const record={type:'message'};events.push(record);if(typeof event.data==='string')record.text=event.data;else work.push(event.data.arrayBuffer().then(b=>record.binary=[...new Uint8Array(b)]));});
   ws.addEventListener('error',()=>events.push({type:'error'}));
   ws.addEventListener('close',event=>{events.push({type:'close',code:event.code,reason:event.reason});clearTimeout(timer);resolve();});
  });await Promise.all(work);
  // A Node Error followed by Close1006 is one typed transport failure. Successful
  // close observations are preserved separately, including1005.
  const expected=events.filter((event,index)=>event.type!=='close'||!events.slice(0,index).some(v=>v.type==='error'));
  results.push({id:item.id,extension:item.extension??null,frames:item.frames.map(b=>b.toString('base64')),opened,requestHeaders,expected,...(item.inputHeaders?{inputHeaders:item.inputHeaders}:{}),...(item.informational?{informational:true}:{}),...(item.responseProtocol?{responseProtocol:item.responseProtocol}:{})});
 }finally{for(const s of sockets)s.destroy();await new Promise(r=>server.close(r));}
}
const result={node:process.version,undici:process.versions.undici,bundleSha256:actualHash,cases:results};
if(process.argv.includes('--check'))assert.deepEqual(result,JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));else console.log(JSON.stringify(result,null,2));
