import http2 from 'node:http2';
import {once} from 'node:events';
import {spawn,execFileSync} from 'node:child_process';
import {readFileSync,mkdtempSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {deepStrictEqual} from 'node:assert';
if(process.argv[2]==='child'){
 const rows=[];
 for(let i=0;i<5;i++){
  try{const response=await fetch(process.argv[3]+'/'+i);await response.arrayBuffer();rows.push({accepted:true,status:response.status,statusText:response.statusText,entries:[...response.headers].filter(([k])=>k!=='date')});}
  catch(error){rows.push({accepted:false,error:error.name});}
 }
 console.log(JSON.stringify(rows));
}else{
 const scratch=mkdtempSync(join(tmpdir(),'native-fetch-header-h2-')),key=join(scratch,'key.pem'),cert=join(scratch,'cert.pem');
 execFileSync('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',key,'-out',cert,'-days','1','-subj','/CN=localhost','-addext','subjectAltName=IP:127.0.0.1'],{stdio:'ignore',env:{PATH:'/usr/bin:/bin'}});
 const fields=[{'x-test':['a','b'],'cookie':['a=1','b=2'],'set-cookie':['a=1','b=2']},{'x-test':[' a ',' b '],'cookie':[' a ',' b '],'set-cookie':[' a ',' b ']},{'x-test':'éÿ'},{'x-test':'\ta','cookie':'a\t','set-cookie':'\tb\t','x-valid':'kept'},{'x-test':'\u00a0valid\u00a0'}];
 const alpn=[];const sessions=new Set();
 const server=http2.createSecureServer({key:readFileSync(key),cert:readFileSync(cert)});
 server.on('session',session=>{sessions.add(session);session.on('error',()=>{});session.on('close',()=>sessions.delete(session));});
 server.on('stream',(stream,headers)=>{stream.on('error',()=>{});alpn.push(stream.session.alpnProtocol);const i=Number(headers[':path'].slice(1));stream.respond({':status':200,...fields[i]});stream.end();});
 server.listen(0,'127.0.0.1');await once(server,'listening');let rows;
 try{
  const child=spawn(process.execPath,[fileURLToPath(import.meta.url),'child','https://127.0.0.1:'+server.address().port],{env:{PATH:'/usr/bin:/bin',NODE_EXTRA_CA_CERTS:cert},stdio:['ignore','pipe','pipe']});let out='',err='';child.stdout.on('data',v=>out+=v);child.stderr.on('data',v=>err+=v);const [status]=await once(child,'close');deepStrictEqual(status,0,err);rows=JSON.parse(out);
 }finally{for(const session of sessions)session.destroy();server.close();await once(server,'close');}
 deepStrictEqual(alpn,['h2','h2','h2','h2','h2']);const result={node:process.version,undici:process.versions.undici,alpn,fields,rows};
 if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./h2.json',import.meta.url),'utf8')));
 else console.log(JSON.stringify(result,null,2));
}
