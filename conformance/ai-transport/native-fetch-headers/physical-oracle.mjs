import net from 'node:net';
import {once} from 'node:events';
import {readFileSync} from 'node:fs';
import {deepStrictEqual} from 'node:assert';
const cases=[];
for(const [id,head] of [
 ['crlf','HTTP/1.1 200 OK\r\nX-Test: a \t\r\nContent-Length: 0\r\n\r\n'],
 ['lf','HTTP/1.1 200 OK\nX-Test: a \t\nContent-Length: 0\n\n'],
 ['obs-fold-space','HTTP/1.1 200 OK\r\nX-Test: a \r\n b \t\r\nContent-Length: 0\r\n\r\n'],
 ['obs-fold-tab','HTTP/1.1 200 OK\r\nX-Test: a\r\n\tb\r\nContent-Length: 0\r\n\r\n'],
 ['space-before-colon','HTTP/1.1 200 OK\r\nX-Test : a\r\nContent-Length: 0\r\n\r\n'],
 ['prefix-empty','\r\nHTTP/1.1 200 OK\r\nX-Test: a\r\nContent-Length: 0\r\n\r\n'],
 ['prefix-lf','\nHTTP/1.1 200 OK\r\nX-Test: a\r\nContent-Length: 0\r\n\r\n'],
 ['status-lf','HTTP/1.1 200 OK\nX-Test: a\r\nContent-Length: 0\r\n\r\n'],
 ['final-lf','HTTP/1.1 200 OK\r\nX-Test: a\r\nContent-Length: 0\r\n\n'],
 ['informational','HTTP/1.1 103 Early\r\nX-Test: ignored\r\n\r\nHTTP/1.1 200 OK\r\nX-Test: a \r\nContent-Length: 0\r\n\r\n'],
 ['latin1','HTTP/1.1 200 OK\r\nX-Test: \xe9\xff \r\nContent-Length: 0\r\n\r\n'],
]){
 const server=net.createServer(socket=>socket.once('data',()=>socket.end(Buffer.from(head,'latin1'))));server.listen(0,'127.0.0.1');await once(server,'listening');
 let result;
 try{const response=await fetch('http://127.0.0.1:'+server.address().port);await response.arrayBuffer();result={accepted:true,entries:[...response.headers]};}
 catch(error){result={accepted:false,error:error.cause?.code??error.name};}
 finally{server.close();await once(server,'close');}
 cases.push({id,head:Buffer.from(head,'latin1').toString('hex'),...result});
}
const result={node:process.version,undici:process.versions.undici,cases};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./physical.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
