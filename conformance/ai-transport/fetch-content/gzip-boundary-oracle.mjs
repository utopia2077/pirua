import {createGunzip,gzipSync,constants} from 'node:zlib';
import http from 'node:http';
import {once} from 'node:events';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {deepStrictEqual} from 'node:assert';
const hash=s=>createHash('sha256').update(s).digest('hex');
const authority={node:process.version,undici:process.versions.undici,undiciHash:hash(process.binding('natives')['internal/deps/undici/undici']),zlibHash:hash(process.binding('natives').zlib),chunkSize:constants.Z_DEFAULT_CHUNK};
deepStrictEqual([authority.node,authority.undici,authority.undiciHash,authority.chunkSize],['v26.8.1','8.10.0','9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d',16384]);
const runs=chunks=>{const result=[];for(const chunk of chunks)for(const byte of chunk){const last=result.at(-1);if(last?.byte===byte)last.count++;else result.push({byte,count:1});}return result;};
const cases=[];
for(const size of [0,1,16383,16384,16385,32767,32768,32769,49151,49152,49153,65536,65537]) {
 const member=gzipSync(Buffer.alloc(size,65)),garbage=Buffer.from('garbage'),second=gzipSync(Buffer.alloc(17,66));
 for(const [kind,feeds] of [
  ['valid',[member]],['truncated',[member.subarray(0,-4)]],['tail',[Buffer.concat([member,garbage])]],
  ['member-split',[member,garbage]],['trailer-split',[member.subarray(0,-8),Buffer.concat([member.subarray(-8),garbage])]],
  ['two-members',[Buffer.concat([member,second,garbage])]],['empty-member',[Buffer.concat([member,gzipSync(Buffer.alloc(0)),garbage])]],
  ['offset-member-tail',[member,Buffer.concat([gzipSync(Buffer.alloc(32768,66)),garbage])]]
 ]) {
  // Exact transform/options created by the hashed Undici network callback.
  // Awaiting each write callback gives an explicit input-operation boundary.
  const decoder=createGunzip({flush:constants.Z_SYNC_FLUSH,finishFlush:constants.Z_SYNC_FLUSH});const chunks=[];let error=null;
  decoder.on('data',chunk=>chunks.push(Buffer.from(chunk)));decoder.on('error',e=>error=e.code);const steps=[];
  for(const feed of feeds){await new Promise(resolve=>{decoder.once('error',resolve);decoder.write(feed,()=>{decoder.removeListener('error',resolve);resolve();});});await new Promise(resolve=>setImmediate(resolve));steps.push({runs:runs(chunks),error});if(error)break;}
  if(!error){decoder.end();await once(decoder,'end');}else decoder.destroy();
  cases.push({id:kind+'-'+size,size,kind,feeds:feeds.map(v=>v.toString('hex')),steps,runs:runs(chunks),error});
 }
}
let active;const server=http.createServer((_q,r)=>{const bytes=Buffer.from(active.feeds[0],'hex');r.writeHead(200,{'content-encoding':'gzip','content-length':bytes.length});r.end(bytes);});
server.listen(0,'127.0.0.1');await once(server,'listening');const network=[];
try{for(const c of cases.filter(c=>c.kind==='tail')){active=c;const chunks=[];let error=null;try{const response=await fetch(`http://127.0.0.1:${server.address().port}/`);for await(const chunk of response.body)chunks.push(chunk);}catch(e){error=e.name;}const observed=runs(chunks);deepStrictEqual(observed,c.runs);network.push({id:c.id,runs:observed,error});}}
finally{server.closeAllConnections();server.close();await once(server,'close');}
const result={authority,evidence:'Actual Node Gunzip native-write callback boundaries plus real Fetch HTTP confirmation; runs encode every delivered byte, not chunk identity.',cases,network};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./gzip-boundaries.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
