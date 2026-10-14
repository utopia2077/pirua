import net from 'node:net';import {upstreamRoot} from '../../upstream.mjs';
import {once} from 'node:events';
import {execFileSync} from 'node:child_process';
import {stripTypeScriptTypes} from 'node:module';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {deepStrictEqual} from 'node:assert';
const source=execFileSync('git',['show','853a80d26c90a14c1886f0ebb8ffaae133ca2185:packages/ai/src/utils/headers.ts'],{cwd:upstreamRoot,encoding:'utf8'});
const headersToRecord=new Function(stripTypeScriptTypes(source.replace(/^import.*\n/gm,'').replace(/^export /gm,''))+'\nreturn headersToRecord;')();
const rows=[];
for(const values of [[' a '],[' a ',' b '],['a\t','\tb'],['',' b '],[' a ',''],['a','b'],['  ','\t'],['a  ',' b\t','c ']]) {
 const raw=['X-Test','Cookie','Set-Cookie'].flatMap(name=>values.map(value=>[name,value]));
 const server=net.createServer(socket=>socket.once('data',()=>socket.end(Buffer.from('HTTP/1.1 200 OK\r\n'+raw.map(([k,v])=>k+': '+v+'\r\n').join('')+'Content-Length: 0\r\nConnection: close\r\n\r\n','latin1'))));
 server.listen(0,'127.0.0.1');await once(server,'listening');
 try {const response=await fetch('http://127.0.0.1:'+server.address().port);await response.arrayBuffer();
  const observe=h=>({get:['x-test','cookie','set-cookie'].map(k=>[k,h.get(k)]),entries:[...h].filter(([k])=>['x-test','cookie','set-cookie'].includes(k)),own:Object.entries(headersToRecord(h)).filter(([k])=>['x-test','cookie','set-cookie'].includes(k))});
  rows.push({values,network:observe(response.headers),preconstructed:observe(new Headers(raw))});
 }finally{server.close();await once(server,'close');}
}
const bundled=process.binding('natives')['internal/deps/undici/undici'];
const result={node:process.version,undici:process.versions.undici,hash:createHash('sha256').update(bundled).digest('hex'),rows};
deepStrictEqual([result.node,result.undici,result.hash],['v26.8.1','8.10.0','9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d']);
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./headers.json',import.meta.url),'utf8')));
else console.log(JSON.stringify(result,null,2));
