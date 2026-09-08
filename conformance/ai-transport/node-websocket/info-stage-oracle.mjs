import {createServer} from 'node:http';
import {createHash} from 'node:crypto';
import {deflateRawSync,constants} from 'node:zlib';
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
const finished=deflateRawSync(Buffer.from('finished'));
const cases=[
{id:'finished-one',extension:'permessage-deflate',frames:[frame(1,finished,true,true),close()]},
{id:'finished-two',extension:'permessage-deflate',frames:[frame(1,finished,true,true),frame(1,compressed('second'),true,true),close()]},
{id:'finished-garbage',extension:'permessage-deflate',frames:[frame(1,Buffer.concat([finished,Buffer.from([255,255])]),true,true),close()]},
{id:'finished-fragment',extension:'permessage-deflate',frames:[frame(1,finished,false,true),frame(0,compressed('second')),close()]},
{id:'window-http-whitespace',extension:'permessage-deflate;\tserver_max_window_bits=08\t',frames:[frame(1,compressed('window'),true,true),close()]},
{id:'window-nbsp',extension:'permessage-deflate;\u00a0server_max_window_bits=7',frames:[frame(1,compressed('window'),true,true),close()]},
{id:'nonminimal-control-length',frames:[Buffer.from([137,126,0,1,112]),frame(1,'after'),close()]},
{id:'nonminimal-data-length',frames:[Buffer.from([129,126,0,1,112]),close()]},
... [8,9,10].flatMap(op => [126,127].map(marker => ({id:`control-header-${op}-${marker}`,frames:[frame(1,'admitted'),Buffer.from([128|op,marker])]}))),
... [[131,126],[129,128],[193,126],[161,126],[145,126],[9,126],[128,126]].map((header,i)=>({id:`info-header-${i}`,frames:[frame(1,'admitted'),Buffer.from(header)]})),
{id:'fragment-needs-continuation',frames:[frame(1,'admitted'),frame(1,'part',false),Buffer.from([129,126])]},
{id:'fragment-needs-final',frames:[frame(1,'admitted'),frame(1,'part',false),Buffer.from([1,126])]},
{id:'nonminimal-data-length-64',frames:[Buffer.from([129,127,0,0,0,0,0,0,0,1,112]),close()]},
]; const results=[];
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
const output=JSON.stringify(result,null,2)+'\n';
if(process.argv.includes('--check')) assert.equal(readFileSync(new URL('./info-stage.json',import.meta.url),'utf8'),output);
else process.stdout.write(output);
