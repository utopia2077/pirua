import http from 'node:http';
import {once} from 'node:events';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {deepStrictEqual} from 'node:assert';
import * as zlib from 'node:zlib';

const bundle=process.binding('natives')['internal/deps/undici/undici'];
const authority={node:process.version,undici:process.versions.undici,sha256:createHash('sha256').update(bundle).digest('hex')};
deepStrictEqual(authority,{node:'v26.8.1',undici:'8.10.0',sha256:'9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d'});
const plain=Buffer.from('data: {"type":"synthetic","text":"hello world"}\n\n'.repeat(64));
const encode={gzip:zlib.gzipSync,'x-gzip':zlib.gzipSync,deflate:zlib.deflateSync,raw:zlib.deflateRawSync,br:zlib.brotliCompressSync,zstd:zlib.zstdCompressSync};
const cases=[];
for(const coding of Object.keys(encode)) {
 const encoded=encode[coding](plain),header=coding==='raw'?'deflate':coding;
 cases.push({id:coding,encoding:header,encoded});
 cases.push({id:coding+'-empty',encoding:header,encoded:Buffer.alloc(0)});
 cases.push({id:coding+'-truncated',encoding:header,encoded:encoded.subarray(0,-4)});
 const corrupt=Buffer.from(encoded);corrupt[Math.min(10,corrupt.length-1)]^=255;
 cases.push({id:coding+'-corrupt',encoding:header,encoded:corrupt});
 cases.push({id:coding+'-trailing',encoding:header,encoded:Buffer.concat([encoded,Buffer.from('garbage')])});
}
for(const codings of [['gzip','br'],['zstd','deflate'],['br','gzip','zstd'],['gzip','gzip','gzip','gzip','gzip']]) {
 let encoded=plain;for(const coding of codings)encoded=encode[coding](encoded);
 cases.push({id:'stack-'+codings.join('-'),encoding:codings.join(', '),encoded});
}
cases.push({id:'concatenated-gzip',encoding:'gzip',encoded:Buffer.concat([encode.gzip(plain),encode.gzip(plain)])});
cases.push({id:'concatenated-zstd',encoding:'zstd',encoded:Buffer.concat([encode.zstd(plain),encode.zstd(plain)])});
cases.push({id:'unknown-chain',encoding:'unknown, gzip',encoded:encode.gzip(plain)});
cases.push({id:'identity-chain',encoding:'gzip, identity',encoded:encode.gzip(plain)});
cases.push({id:'trailing-comma',encoding:'gzip,',encoded:encode.gzip(plain)});
cases.push({id:'case-and-space',encoding:' GZip ',encoded:encode.gzip(plain)});
cases.push({id:'six-codings',encoding:'gzip,gzip,gzip,gzip,gzip,gzip',encoded:Buffer.alloc(0)});
cases.push({id:'six-unknown',encoding:'unknown,unknown,unknown,unknown,unknown,unknown',encoded:Buffer.alloc(0)});
for(const status of [204,205,304])cases.push({id:'null-'+status,status,encoding:'gzip,gzip,gzip,gzip,gzip,gzip',encoded:Buffer.alloc(0)});
cases.push({id:'head-skips',method:'HEAD',encoding:'gzip,gzip,gzip,gzip,gzip,gzip',encoded:Buffer.alloc(0)});
cases.push({id:'redirect-skips',status:302,location:'/plain',encoding:'gzip,gzip,gzip,gzip,gzip,gzip',encoded:Buffer.alloc(0)});
cases.push({id:'manual-decodes',status:302,redirect:'manual',location:'/plain',encoding:'gzip',encoded:encode.gzip(plain)});
for(const [id,headers,method,body] of [
 ['defaults',{},'POST','abc'],['override',{'accept':'custom','accept-language':'en','user-agent':'synthetic','accept-encoding':'identity','sec-fetch-mode':'navigate'},'POST','abc'],
 ['range',{'range':'bytes=0-3'},'GET',null],['range-override',{'range':'bytes=0-3','accept-encoding':'gzip'},'GET',null],
 ['conditional',{'if-none-match':'synthetic'},'GET',null],['conditional-override',{'if-match':'synthetic','cache-control':'custom','pragma':'custom'},'GET',null],
 ['empty-post',{},'POST',null],['empty-put',{},'PUT',null],['empty-patch',{},'PATCH',null]
]) cases.push({id,headers,method,body,encoded:plain});
let active;const captured=[];
const server=http.createServer(async(req,res)=>{
 const body=[];for await(const chunk of req)body.push(chunk);
 const headers={...req.headers};headers.host='<loopback>';
 captured.push({method:req.method,headers,body:Buffer.concat(body).toString('hex')});
 if(req.url==='/plain'){res.writeHead(200);res.end(plain);return;}
 const c=active;
 res.writeHead(c.status??200,{...(c.encoding?{'content-encoding':c.encoding}:{}),...(c.location?{location:c.location}:{})});
 res.end(c.encoded);
});
server.listen(0,'127.0.0.1');await once(server,'listening');
const port=server.address().port,results=[];
try {
 for(const c of cases){
  active=c;captured.length=0;let stage='acquisition',data=Buffer.alloc(0),status=null,error=null,headers=null;
  try {
   const response=await fetch(`http://127.0.0.1:${port}/case`,{method:c.method??'GET',headers:c.headers,body:c.body==null?undefined:Buffer.from(c.body),redirect:c.redirect??'follow'});
   stage='body';status=response.status;headers=Object.fromEntries(response.headers);delete headers.date;delete headers.connection;delete headers['keep-alive'];delete headers['transfer-encoding'];delete headers['content-length'];
   const chunks=[];try{for await(const chunk of response.body??[])chunks.push(Buffer.from(chunk));}finally{data=Buffer.concat(chunks);}
   stage='done';
  }catch(e){error={name:e.name,message:e.message};}
  results.push({id:c.id,encoding:c.encoding??null,encoded:c.encoded.toString('hex'),method:c.method??'GET',requestHeaders:c.headers??{},requestBody:c.body==null?'':Buffer.from(c.body).toString('hex'),initialStatus:c.status??200,redirect:c.redirect??'follow',location:c.location??null,status,headers,stage,error,decoded:data.toString('hex'),requests:[...captured]});
 }
}finally{server.closeAllConnections();server.close();await once(server,'close');}
const result={authority,plain:plain.toString('hex'),cases:results};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./vectors.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
