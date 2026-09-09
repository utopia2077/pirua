import http from 'node:http';
import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
assert.equal(process.versions.node,'26.8.1');
const server=http.createServer((request,response)=>{response.writeHead(Number(request.url.slice(1)),{'content-length':'0'});response.end();});
await new Promise(r=>server.listen(0,'127.0.0.1',r));
const cases=[];
try {for(const [method,status] of [['POST',200],['POST',204],['POST',205],['POST',304],['HEAD',200]]){
 const response=await fetch(`http://127.0.0.1:${server.address().port}/${status}`,{method});const absent=response.body===null;const text=await response.text();assert.equal(text,'');assert.equal(response.body===null,absent);cases.push({method,status,absent});
}}finally{server.closeAllConnections();await new Promise(r=>server.close(r));}
const constructed=[false,true].map(absent=>{const response=new Response(absent?null:'',{status:200});return {absent,observed:response.body===null};});
const result={node:process.versions.node,cases,constructed};
if(process.argv.includes('--check'))assert.deepEqual(result,JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));else console.log(JSON.stringify(result,null,2));
