import http from 'node:http';
import {once} from 'node:events';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {deepStrictEqual} from 'node:assert';
import * as zlib from 'node:zlib';
const authority={node:process.version,undici:process.versions.undici,sha256:createHash('sha256').update(process.binding('natives')['internal/deps/undici/undici']).digest('hex')};
deepStrictEqual(authority,{node:'v26.8.1',undici:'8.10.0',sha256:'9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d'});
const pattern='data: {"text":"incremental synthetic"}\n\n';
const plain=Buffer.from(pattern.repeat(65536));
const codecs={gzip:zlib.gzipSync,deflate:zlib.deflateSync,raw:zlib.deflateRawSync,br:zlib.brotliCompressSync,zstd:zlib.zstdCompressSync};
let active;const server=http.createServer(async(_request,response)=>{
 response.writeHead(200,{'content-encoding':active.encoding});
 for(let i=0;i<active.bytes.length;i+=17){if(!response.write(active.bytes.subarray(i,i+17)))await once(response,'drain');}
 response.end();
});
server.listen(0,'127.0.0.1');await once(server,'listening');const cases=[];
try {
 for(const [coding,encode] of Object.entries(codecs))for(const empty of [false,true]) {
  const input=empty?Buffer.alloc(0):plain;active={encoding:coding==='raw'?'deflate':coding,bytes:encode(input)};
  const response=await fetch(`http://127.0.0.1:${server.address().port}/`);let total=0;const hash=createHash('sha256');
  for await(const chunk of response.body){total+=chunk.length;hash.update(chunk);}
  deepStrictEqual(total,input.length);deepStrictEqual(hash.digest('hex'),createHash('sha256').update(input).digest('hex'));
  cases.push({id:coding+(empty?'-encoded-empty':'-long'),encoding:active.encoding,encoded:active.bytes.toString('hex'),pattern:empty?'':pattern,repeats:empty?0:65536,bytes:total,sha256:createHash('sha256').update(input).digest('hex')});
 }
}finally{server.closeAllConnections();server.close();await once(server,'close');}
const result={authority,cases};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./incremental.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
