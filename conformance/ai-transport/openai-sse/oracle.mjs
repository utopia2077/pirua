import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { deepStrictEqual } from 'node:assert';
const root='upstream/pi-mono/node_modules';
const require=createRequire(`${root}/openai/package.json`);
const families=[['openai','6.40.0'],['@anthropic-ai/sdk','0.91.1']];
const sources={};
for (const [pkg,version] of families) {
  deepStrictEqual(JSON.parse(readFileSync(`${root}/${pkg}/package.json`,'utf8')).version,version);
  for(const path of ['src/core/streaming.ts','src/internal/decoders/line.ts','src/internal/utils/bytes.ts','core/streaming.js','internal/decoders/line.js','internal/utils/bytes.js']) sources[`${pkg}/${path}`]=createHash('sha256').update(readFileSync(`${root}/${pkg}/${path}`)).digest('hex');
}
async function execute(pkg,chunks) {
  const {_iterSSEMessages}=require(`${root}/${pkg}/core/streaming.js`);
  let pendingRead, waitRead;
  let readReady=new Promise(resolve=>{waitRead=resolve;});
  const body={
    [Symbol.asyncIterator](){return this;},
    next(){return new Promise(resolve=>{pendingRead=resolve;waitRead();});},
  };
  const events=[];
  const worker=(async()=>{for await (const {event,data} of _iterSSEMessages({body},new AbortController())) events.push({event,data});})();
  const stages=[];
  for (const chunk of chunks) {
    await readReady;
    readReady=new Promise(resolve=>{waitRead=resolve;});
    pendingRead({done:false,value:new Uint8Array(chunk)});
    await readReady;
    stages.push(structuredClone(events));
  }
  pendingRead({done:true,value:undefined});await worker;
  stages.push(structuredClone(events));return stages;
}
const strings=[
  ['lf','data: x\n\n'],['cr','data: x\r\r'],['crlf','data: x\r\n\r\n'],
  ['mixed-lf-crlf','data: x\n\r\n'],['mixed-crlf-cr','data: x\r\n\r'],
  ['bare','data: x'],['one-lf','data: x\n'],['one-cr','data: x\r'],
  ['bom-every-line','\ufeffdata: a\n\ufeffdata: b\n\n'],['event-only','event: ping\n\n'],
  ['colonless-data','data\n\n'],['empty-event','event:\n\n'],['comments',': ignored\n\ndata: y\n\n'],
  ['spaces','data:  x \n data: ignored\ndata:\ty\n\n'],['utf8','data: ☃😀\n\n'],
  ['cr-carry','data: a\r\rdata: b\r\r'],['mixed-then-gate','data: a\n\r\ndata: b\n\n'],
  ['trailing-partial','data: a\n\ndata: ignored'],['done','data: [DONE]suffix\n\n'],
];
const cases=strings.map(([id,raw])=>({id,bytes:[...new TextEncoder().encode(raw)]}));
cases.push({id:'utf8-replacement',bytes:[...Buffer.from('data: '),255,192,175,10,10]});
const outputs=[];
for(const item of cases) {
  const splits=[];
  for(let split=0;split<=item.bytes.length;split++) {
    const chunks=[item.bytes.slice(0,split),item.bytes.slice(split)];
    const stages=await execute('openai',chunks);
    // These installed SDK decoders share source and actual Node observations.
    deepStrictEqual(stages,await execute('@anthropic-ai/sdk',chunks));
    splits.push({split,stages});
  }
  outputs.push({...item,splits});
}
const result={evidenceClass:'executed-installed-sdk-iterator',families,sources,cases:outputs};
if(process.argv.includes('--check')) deepStrictEqual(result,JSON.parse(readFileSync(new URL('./frames.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
