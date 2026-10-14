import net from 'node:net';import {upstreamRoot} from '../../upstream.mjs';
import {once} from 'node:events';
import {execFileSync} from 'node:child_process';
import {stripTypeScriptTypes} from 'node:module';
import {readFileSync} from 'node:fs';
import {deepStrictEqual} from 'node:assert';
import {createHash} from 'node:crypto';
const source=execFileSync('git',['show','853a80d26c90a14c1886f0ebb8ffaae133ca2185:packages/ai/src/utils/provider-retry.ts'],{cwd:upstreamRoot,encoding:'utf8'});
const retry=new Function(stripTypeScriptTypes(source.replace(/^export /gm,''))+'\nreturn retryProviderRequest;')();
const cases=[];
for(const network of [false,true])for(const [status,value]of [[500,'false '],[400,'true ']]){
 let calls=0,header;const server=net.createServer(socket=>socket.once('data',()=>{calls++;socket.end(`HTTP/1.1 ${calls===1?status:200} Synthetic\r\nx-should-retry: ${value}\r\nretry-after-ms: 0\r\nContent-Length: 0\r\nConnection: close\r\n\r\n`);}));
 server.listen(0,'127.0.0.1');await once(server,'listening');let success;
 try{await retry(async()=>{
  const response=network?await fetch('http://127.0.0.1:'+server.address().port):new Response(null,{status:++calls===1?status:200,headers:{'x-should-retry':value,'retry-after-ms':'0'}});
  await response.text();if(!response.ok){header=response.headers.get('x-should-retry');throw Object.assign(new Error('synthetic'),{status:response.status,headers:response.headers});}return response;
 },{maxRetries:1});success=true;}catch{success=false;}finally{server.close();await once(server,'close');}
 cases.push({network,status,value,header,calls,success});
}
const result={node:process.version,undici:process.versions.undici,sourceHash:createHash('sha256').update(source).digest('hex'),cases};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./retry.json',import.meta.url),'utf8')));
else console.log(JSON.stringify(result,null,2));
