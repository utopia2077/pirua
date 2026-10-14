import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {stripTypeScriptTypes} from 'node:module';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {deepStrictEqual} from 'node:assert';
import vm from 'node:vm';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const path='packages/ai/src/api/anthropic-messages.ts';
const source=execFileSync('git',['show',`${pin}:${path}`],{cwd:upstreamRoot,encoding:'utf8'});
const hash=createHash('sha256').update(source).digest('hex');
deepStrictEqual(hash,'c63b3cbe8afe2504bb5f60f2e7f7b8f773de68486a03cc0cc266c3c76f73f7f6');
const context=vm.createContext({TextDecoder});
vm.runInContext(stripTypeScriptTypes(source.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,'')),context);
async function execute(chunks) {
 let resolveRead,signalRead;let ready=new Promise(r=>{signalRead=r;});
 const body={getReader(){return {read(){signalRead();return new Promise(r=>{resolveRead=r;});},releaseLock(){}};}};
 const frames=[];const worker=(async()=>{for await(const {event,data,raw} of context.iterateSseMessages(body))frames.push({event,data,raw});})();
 const stages=[];
 for(const chunk of chunks){await ready;ready=new Promise(r=>{signalRead=r;});resolveRead({done:false,value:new Uint8Array(chunk)});await ready;stages.push(structuredClone(frames));}
 resolveRead({done:true});await worker;stages.push(structuredClone(frames));return stages;
}
const encode=text=>[...Buffer.from(text)];
const inputs=[
 ['comments-ignored',': billing\nunknown: prompt is too long\nid: ignored\nretry: 99\nevent: message_delta\ndata: invalid server error\n\n'],
 ['repeated-fields','event: first\nevent: last\ndata:  a \ndata:\tb\ndata\n: tail\n\n'],
 ['empty-flush-carry',': billing\n\nunknown: prompt is too long\n\nevent:\n\ndata: invalid\n\n'],
 ['eof','event: message_delta\n: prompt is too long\nignored\ndata: invalid'],
 ['only-ignored-eof',': billing\n\nunknown:x'],
 ['event-empty-overwrites','event: first\nevent:\n\n: carried\ndata\n\n'],
 ['crlf','event: message_delta\r\n: billing\r\ndata: invalid server error\r\n\r\n'],
 ['bom-unicode','\ufeff: ☃😀\r\ndata: invalid\r\n\r\n'],
].map(([id,text])=>({id,bytes:encode(text)}));
inputs.push({id:'invalid-utf8-eof',bytes:[...encode('event: message_delta\n: '),255,226,152,...encode('\ndata:'),226,152]});
const cases=[];
for(const item of inputs){const splits=[];for(let split=0;split<=item.bytes.length;split++)splits.push({split,stages:await execute([item.bytes.slice(0,split),item.bytes.slice(split)])});
 cases.push({...item,splits,bytewise:await execute(item.bytes.map(byte=>[byte]))});}
const result={evidenceClass:'executed-pinned-private-parser-raw-context',pin,path,sha256:hash,node:process.versions.node,cases};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./raw-context.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result)+'\n');
