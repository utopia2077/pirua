import http from 'node:http';
import {once} from 'node:events';
import * as zlib from 'node:zlib';
import {createHash} from 'node:crypto';
import {deepStrictEqual} from 'node:assert';
import {readFileSync} from 'node:fs';
const authority={node:process.version,undici:process.versions.undici,sha256:createHash('sha256').update(process.binding('natives')['internal/deps/undici/undici']).digest('hex')};
deepStrictEqual(authority,{node:'v26.8.1',undici:'8.10.0',sha256:'9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d'});
const cases=[];
for(const [coding,encode] of Object.entries({gzip:zlib.gzipSync,deflate:zlib.deflateSync,br:zlib.brotliCompressSync,zstd:zlib.zstdCompressSync}))for(const tail of [false,true]) {
 const bytes=Buffer.concat([encode(Buffer.from('test')),tail?Buffer.from('garbage'):Buffer.alloc(0)]);
 const server=http.createServer((_q,r)=>{r.writeHead(200,{'content-encoding':coding});r.write(bytes);});server.listen(0,'127.0.0.1');await once(server,'listening');
 const signal=new AbortController();const timer=setTimeout(()=>signal.abort(),300);let stage='headers',size=0;
 try{const r=await fetch(`http://127.0.0.1:${server.address().port}`,{signal:signal.signal});stage='body';for await(const c of r.body)size+=c.length;stage='done';}catch(e){stage=e.name;}finally{clearTimeout(timer);server.closeAllConnections();server.close();await once(server,'close');}
 cases.push({coding,tail,stage,size,encoded:bytes.toString('hex')});
}
const result={authority,cases};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./held-body.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
