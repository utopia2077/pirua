import {execFileSync} from 'node:child_process';import {readFileSync} from 'node:fs';import {createHash} from 'node:crypto';import {createRequire,stripTypeScriptTypes} from 'node:module';import {deepStrictEqual} from 'node:assert';import vm from 'node:vm';import net from 'node:net';import {once} from 'node:events';
import {GoogleGenAI,FinishReason,FunctionCallingConfigMode} from 'upstream/pi-mono/node_modules/@google/genai/dist/node/index.mjs';
const root='upstream/pi-mono',commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185',require=createRequire(root+'/package.json');
const source=p=>execFileSync('git',['show',commit+':packages/ai/src/'+p],{cwd:root,encoding:'utf8'}),clean=s=>stripTypeScriptTypes(s.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const paths=['utils/json-parse.ts','utils/sanitize-unicode.ts','utils/event-stream.ts','utils/headers.ts','utils/provider-env.ts','utils/pi-user-agent.ts','utils/provider-retry.ts','utils/retry.ts','utils/overflow.ts','utils/deferred-tools.ts','utils/estimate.ts','utils/error-body.ts','api/constrained-sampling.ts','api/transform-messages.ts','api/simple-options.ts','api/google-shared.ts','api/google-generative-ai.ts'];const code=paths.map(p=>clean(source(p)));const models=source('models.ts');code.unshift(clean(models.slice(models.indexOf('export function calculateCost'),models.indexOf('export function modelsAreEqual'))));
const fixture=JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8'));const model=fixture.cases[1].model,context=fixture.cases[1].context;const authority=fixture.authority;deepStrictEqual(createHash('sha256').update(readFileSync(root+'/node_modules/@google/genai/dist/node/index.mjs')).digest('hex'),authority.sha256);
async function run(fetch,base=model.baseUrl){const runtime=vm.createContext({Error,JSON,GoogleGenAI,FinishReason,FunctionCallingConfigMode,TextDecoder,TextEncoder,Response,ReadableStream,Headers,process,structuredClone,AbortController,fetch,Date:class extends Date{static now(){return 7;}},setTimeout,clearTimeout,partialParse:require('partial-json').parse});for(const c of code)vm.runInContext(c,runtime);vm.runInContext('getPiUserAgent=()=>"pi-ai-rust"',runtime);const events=[];const Stream=vm.runInContext('AssistantMessageEventStream',runtime),push=Stream.prototype.push;Stream.prototype.push=function(e){events.push(JSON.parse(JSON.stringify(e)));return push.call(this,e);};const stream=vm.runInContext('stream',runtime)({...model,baseUrl:base},context,{apiKey:'synthetic-key'});const result=await stream.result();return JSON.parse(JSON.stringify({events,result,retryable:runtime.isRetryableAssistantError(result),overflow:runtime.isContextOverflow(result)}));}

const undici=process.binding('natives')['internal/deps/undici/undici'];
const undiciHash=createHash('sha256').update(undici).digest('hex');
deepStrictEqual(undiciHash,'9090d7cdbb8b47c2b040f337b05704b385d2ba06d3b79709691c5e8b13090f4d');
const specs=[];
for(const index of [0,428,429,430,499,500,501,502,503,504])specs.push({id:'index-'+index,statusHex:'61'.repeat(index)+'ff'});
for(const unit of [255,256,428,429,430,499,500,501,502,503,504])specs.push({id:'unit-'+unit,statusHex:Buffer.from(String.fromCharCode(unit)).toString('hex')});
specs.push({id:'valid-e-acute',statusHex:'c3a9'},{id:'high-surrogate',statusHex:Buffer.from('🦫').toString('hex')},{id:'high-surrogate-at-429',statusHex:'61'.repeat(429)+Buffer.from('🦫').toString('hex')},{id:'latin1-prefix-not-byte-index',statusHex:'c3a9'.repeat(500)+'ff'});
for(const data of ['billing server error prompt is too long','quota exceeded','Throttling error','Service unavailable','400 (no body)'])specs.push({id:'template-context-'+data,statusHex:'61'.repeat(500)+'ff',data});
specs.push({id:'code-unit-billing-veto',statusHex:Buffer.from(String.fromCharCode(503)).toString('hex'),data:'billing'});
const observations=[],cases=[];const original=globalThis.fetch;
function retain(result){const key=JSON.stringify(result),i=observations.findIndex(v=>JSON.stringify(v)===key);if(i>=0)return i;observations.push(result);return observations.length-1;}
try{
 for(const spec of specs){
  const data=spec.data??'{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"ordinary text"}]}}]}';
  const frame=Buffer.from('data: '+data+'\n\n');let statusText,calls=0,constructorError;
  const server=net.createServer(socket=>{let received=Buffer.alloc(0),responded=false;socket.on('data',bytes=>{received=Buffer.concat([received,bytes]);const end=received.indexOf('\r\n\r\n');if(end<0||responded)return;const length=Number(received.subarray(0,end).toString().match(/content-length:\s*(\d+)/i)?.[1]??0);if(received.length<end+4+length)return;responded=true;socket.end(Buffer.concat([Buffer.from('HTTP/1.1 200 '),Buffer.from(spec.statusHex,'hex'),Buffer.from(`\r\nContent-Type: text/event-stream\r\nContent-Length: ${frame.length}\r\n\r\n`),frame]));});});
  server.listen(0,'127.0.0.1');await once(server,'listening');const base=`http://127.0.0.1:${server.address().port}/v1beta`;
  const fetch=async(url,init)=>{if(!String(url).startsWith(base))throw new Error('nonlocal request forbidden');calls++;const response=await original(url,init);statusText=response.statusText;try{new Response(data,{status:response.status,statusText});}catch(e){constructorError=String(e);}return response;};
  globalThis.fetch=fetch;let result;try{result=await run(fetch,base);}finally{server.close();await once(server,'close');}
  cases.push({...spec,data,statusText,constructorError:constructorError??null,calls,byteChunks:[frame.toString('hex')],observation:retain(result)});
 }
}finally{globalThis.fetch=original;}
const result={authority:{...authority,undici:process.versions.undici,undiciHash},evidence:'Actual loopback Node Fetch plus complete pinned Google adapter and SDK; fixed ByteString index and UTF16 code-unit template, full data context and controls.',model,context,observations,cases};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./metadata.json',import.meta.url),'utf8')));else process.stdout.write(JSON.stringify(result,null,2)+'\n');
