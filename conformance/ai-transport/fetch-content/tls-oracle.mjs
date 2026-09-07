import https from 'node:https';
import {once} from 'node:events';
import {spawn,execFileSync} from 'node:child_process';
import {readFileSync,mkdtempSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {deepStrictEqual} from 'node:assert';
import {gzipSync} from 'node:zlib';
const authority={node:process.version,undici:process.versions.undici,sha256:createHash('sha256').update(process.binding('natives')['internal/deps/undici/undici']).digest('hex')};
deepStrictEqual(authority,{node:'v26.8.1',undici:'8.10.0',sha256:'9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d'});
if(process.argv[2]==='child') {
 const response=await fetch(process.argv[3]);process.stdout.write(JSON.stringify({status:response.status,decoded:Buffer.from(await response.arrayBuffer()).toString('hex')}));
}else {
 // A fresh synthetic local root; no platform trust overrides or real credentials.
 const scratch=mkdtempSync(join(tmpdir(),'fetch-content-tls-')),key=join(scratch,'key.pem'),cert=join(scratch,'cert.pem');
 execFileSync('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',key,'-out',cert,'-days','1','-subj','/CN=localhost','-addext','subjectAltName=IP:127.0.0.1'],{stdio:'ignore',env:{PATH:'/usr/bin:/bin'}});
 const plain=Buffer.from('synthetic verified TLS');const encoded=gzipSync(plain);let headers;
 const server=https.createServer({key:readFileSync(key),cert:readFileSync(cert)},(req,res)=>{headers={...req.headers,host:'<loopback>'};res.writeHead(200,{'content-encoding':'gzip'});res.end(encoded);});
 server.listen(0,'127.0.0.1');await once(server,'listening');let childResult;
 try {
  const child=spawn(process.execPath,[fileURLToPath(import.meta.url),'child',`https://127.0.0.1:${server.address().port}/`],{env:{PATH:'/usr/bin:/bin',NODE_EXTRA_CA_CERTS:cert},stdio:['ignore','pipe','pipe']});let stdout='',stderr='';child.stdout.on('data',v=>stdout+=v);child.stderr.on('data',v=>stderr+=v);const [status]=await once(child,'close');deepStrictEqual(status,0,stderr);childResult=JSON.parse(stdout);
 }finally {server.closeAllConnections();server.close();await once(server,'close');}
 deepStrictEqual(childResult.decoded,plain.toString('hex'));
 const result={authority,headers,encoded:encoded.toString('hex'),...childResult};
 if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./tls.json',import.meta.url),'utf8')));
 else process.stdout.write(JSON.stringify(result,null,2)+'\n');
}
