import {execFileSync} from 'node:child_process';
import {stripTypeScriptTypes} from 'node:module';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {deepStrictEqual} from 'node:assert';
import vm from 'node:vm';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const root='upstream/pi-mono';
const get=path=>execFileSync('git',['show',`${pin}:${path}`],{cwd:root,encoding:'utf8'});
const path='packages/ai/src/api/anthropic-messages.ts';
const source=get(path);
const hash=createHash('sha256').update(source).digest('hex');
deepStrictEqual(hash,'c63b3cbe8afe2504bb5f60f2e7f7b8f773de68486a03cc0cc266c3c76f73f7f6');
const context=vm.createContext({TextDecoder});
const load=text=>vm.runInContext(stripTypeScriptTypes(text.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,'')),context);
load(get('packages/ai/src/utils/json-parse.ts'));load(source);
async function execute(chunks) {
 let resolveRead,signalRead;let ready=new Promise(r=>{signalRead=r;});
 const body={getReader(){return {read(){signalRead();return new Promise(r=>{resolveRead=r;});},releaseLock(){}};}};
 const frames=[];const worker=(async()=>{for await(const {event,data} of context.iterateSseMessages(body))frames.push({event,data});})();
 const stages=[];
 for(const chunk of chunks){await ready;ready=new Promise(r=>{signalRead=r;});resolveRead({done:false,value:new Uint8Array(chunk)});await ready;stages.push(structuredClone(frames));}
 resolveRead({done:true});await worker;stages.push(structuredClone(frames));return stages;
}
const inputs=[['lf','event: message_stop\ndata: {"type":"message_stop"}\n\n'],['crlf','event: message_stop\r\ndata: {"type":"message_stop"}\r\n\r\n'],['cr','event: e\rdata: x\r\r'],['mixed','event: e\r\ndata:a\n\r\n'],['eof','event: message_stop\ndata: {"type":"message_stop"}'],['event-only','event: error'],['colonless','data\n\n'],['unknown','event: proxy.stats\ndata: not json\n\n'],['comments',': ignored\n\nunknown:value\nevent:\ndata:\n\n'],['multiline','event: first\nevent: last\ndata:  a \ndata:\tb\n\n'],['bom','\ufeffdata: first\n\n\ufeffdata: second\n\n'],['utf8','data: ☃😀\r\n\r\n'],['missing-type','event: message_stop\ndata: {}\n\n'],['unnamed-json','data: {"type":"message_stop"}\n\n'],['terminal-tail','event: message_stop\ndata: {"type":"message_stop"}\n\nevent: proxy.stats\ndata: not json\n\n']].map(([id,text])=>({id,bytes:[...Buffer.from(text)]}));
inputs.push({id:'invalid-and-truncated',bytes:[...Buffer.from('data:'),255,192,175,10,10,...Buffer.from('data:'),226,152]});
inputs.push({id:'leading-empty-before-bom',bytes:[...Buffer.from('\n\ufeffdata: ignored\n\n')]});
const cases=[];
for(const item of inputs){const splits=[];for(let split=0;split<=item.bytes.length;split++){const chunks=[item.bytes.slice(0,split),item.bytes.slice(split)];splits.push({split,stages:await execute(chunks)});}cases.push({...item,splits,bytewise:await execute(item.bytes.map(x=>[x]))});}
const consumers=[];
for(const text of ['event: message_stop\ndata: {}\n\n','data: {"type":"message_stop"}\n\n','event: proxy.stats\ndata: not json\n\n','event: message_stop\ndata: {"type":"message_stop"}']){
 const values=[];let error=false;
 const body=new ReadableStream({start(c){c.enqueue(new TextEncoder().encode(text));c.close();}});
 try{for await(const value of context.iterateAnthropicEvents({body}))values.push(value);}catch{error=true;}
 consumers.push({text,values:JSON.parse(JSON.stringify(values)),error});
}
const result={evidenceClass:'executed-pinned-private-parser',pin,path,sha256:hash,node:process.versions.node,cases,consumers};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./frames.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result)+'\n');
