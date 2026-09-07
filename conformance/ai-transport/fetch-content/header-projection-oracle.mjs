import net from 'node:net';
import {once} from 'node:events';
import {createHash} from 'node:crypto';
import {deepStrictEqual} from 'node:assert';
import {readFileSync} from 'node:fs';
const authority={node:process.version,undici:process.versions.undici,sha256:createHash('sha256').update(process.binding('natives')['internal/deps/undici/undici']).digest('hex')};
deepStrictEqual(authority,{node:'v26.8.1',undici:'8.10.0',sha256:'9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d'});
const headers=[['Cookie',' a=1\t'],['COOKIE','b=2'],['Location','/one'],['location','/two'],['set-cookie','a=1'],['set-cookie','b=2']];
const cases=[];
for(const method of ['post','pOsT','query','patch','pAtCh','GET','head']) {
 let captured;const server=net.createServer(socket=>{let bytes='';socket.on('data',chunk=>{bytes+=chunk.toString('latin1');if(bytes.includes('\r\n\r\n')){captured=bytes.slice(0,bytes.indexOf('\r\n\r\n'));socket.end('HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n');}});});server.listen(0,'127.0.0.1');await once(server,'listening');
 try{const response=await fetch(`http://127.0.0.1:${server.address().port}/`,{method,headers});await response.arrayBuffer();}finally{server.close();await once(server,'close');}
 const [line,...fields]=captured.split('\r\n');const physical={};for(const field of fields){const p=field.indexOf(':');const name=field.slice(0,p).toLowerCase(),value=field.slice(p+1).trim();physical[name]=physical[name]?physical[name]+', '+value:value;}physical.host='<loopback>';
 const fetchHeaders={...physical};delete fetchHeaders.host;delete fetchHeaders.connection;
 cases.push({method,normalized:line.split(' ')[0],physical,fetchHeaders});
}
const result={authority,observation:'Supplied-only header projection expanded to actual network Fetch defaults; Host/Connection remain protocol-owned and are also retained separately.',cases};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./header-projection.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
