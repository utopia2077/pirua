import {execFileSync} from 'node:child_process';
import {stripTypeScriptTypes} from 'node:module';
import {deepStrictEqual,strictEqual} from 'node:assert';
import vm from 'node:vm';
import {readFileSync} from 'node:fs';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const source=p=>execFileSync('git',['show',pin+':packages/ai/src/'+p],{cwd:'upstream/pi-mono',encoding:'utf8'});
const clean=s=>stripTypeScriptTypes(s.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,''));
const context=vm.createContext({Error,Date:class extends Date{static now(){return 7;}}});
vm.runInContext(clean(source('utils/event-stream.ts'))+'\n'+clean(source('api/lazy.ts')),context);
const Stream=vm.runInContext('AssistantMessageEventStream',context),lazyStream=vm.runInContext('lazyStream',context),lazyApi=vm.runInContext('lazyApi',context);
const model={id:'m',api:'a',provider:'p'};
const message=(stopReason='pending',input=0)=>({role:'assistant',content:[],api:'a',provider:'p',model:'m',usage:{input,output:0,cacheRead:0,cacheWrite:0,totalTokens:input,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}},stopReason,timestamp:7});
const done=()=>{const s=new Stream();s.push({type:'done',reason:'stop',message:message('stop')});return s;};
const flush=async()=>{for(let i=0;i<8;i++)await Promise.resolve();};
const results={pin};
// P1: child state may update usage without emitting another progress event.
{
 const child=new Stream();let calls=0;const outer=lazyStream(model,()=>{calls++;return Promise.resolve(child);});strictEqual(calls,1);
 const iterator=outer[Symbol.asyncIterator]();child.push({type:'start',partial:message()});const first=await iterator.next();
 const state=message('pending',17);const controller=new AbortController();controller.signal.addEventListener('abort',()=>{state.stopReason='aborted';state.errorMessage='Request aborted';child.push({type:'error',reason:'aborted',error:state});},{once:true});controller.abort();
 const result=await outer.result();strictEqual(result.usage.input,17);results.cancellation={calls,prefix:first.value.type,prefixUsage:first.value.partial.usage.input,terminalUsage:result.usage.input};
}
// P2: per-call loader invocation, advertised capability presence and cache owner.
{
 let calls=0;const module={stream:done,streamSimple:done};const load=()=>{calls++;return Promise.resolve(module);};const plain=lazyApi(load);
 const before=calls;const a=plain.stream(model,{});const synchronous=calls;const b=plain.streamSimple(model,{});await Promise.all([a.result(),b.result()]);
 const enabled=lazyApi(load,{fetchDeferred:true,cancelDeferred:true});const failed=await enabled.fetchDeferred(model,{}).result();let cancelFailed=false;try{await enabled.cancelDeferred(model,{});}catch{cancelFailed=true;}
 let directThrows=false;try{lazyStream(model,()=>{throw new Error('synthetic direct setup');});}catch{directThrows=true;}
 results.loader={before,synchronous,after:calls,plainKeys:Object.keys(plain),enabledKeys:Object.keys(enabled),fetchFailure:failed.errorMessage,cancelFailed,directThrows};
 strictEqual(calls,4);strictEqual(synchronous,1);strictEqual(before,0);strictEqual(directThrows,true);
}
// P3: arbitrary iterable domain differs from canonical validated AssistantStream.
{
 const withResult={async *[Symbol.asyncIterator](){},result:async()=>message('stop',23)};
 const resultOnly=lazyStream(model,async()=>withResult);let count=0;for await(const _ of resultOnly)count++;const result=await resultOnly.result();strictEqual(count,0);strictEqual(result.usage.input,23);
 const noResult=lazyStream(model,async()=>({async *[Symbol.asyncIterator](){yield {type:'start',partial:message()};}}));let ended=0;for await(const _ of noResult)ended++;let settled=false;void noResult.result().then(()=>settled=true);await flush();strictEqual(settled,false);
 const throwing=lazyStream(model,async()=>({async *[Symbol.asyncIterator](){yield {type:'start',partial:message('pending',99)};throw new Error('synthetic iterator failure');}}));const tags=[];for await(const event of throwing)tags.push(event.type);const failure=await throwing.result();deepStrictEqual(tags,['start','error']);strictEqual(failure.usage.input,0);
 results.domain={resultOnlyEvents:count,resultOnlyUsage:result.usage.input,noResultEvents:ended,noResultSettled:settled,iteratorThrowTags:tags,iteratorThrowUsage:failure.usage.input};
}
// Execute each pinned factory body. Only the dynamic-import service is replaced
// with a controlled module loader; no provider SDK/discovery is invoked.
const names={
 'anthropic-messages':'anthropicMessagesApi','azure-openai-responses':'azureOpenAIResponsesApi',
 'google-generative-ai':'googleGenerativeAIApi','google-vertex':'googleVertexApi',
 'mistral-conversations':'mistralConversationsApi','openai-codex-responses':'openAICodexResponsesApi',
 'openai-completions':'openAICompletionsApi','openai-responses':'openAIResponsesApi',
 'pi-messages':'piMessagesApi','bedrock-converse-stream':'bedrockConverseStreamApi'};
let hostLoad; context.__load=specifier=>hostLoad(specifier);
results.families=[];
for(const [id,name] of Object.entries(names)) {
 const body=clean(source(`api/${id}.lazy.ts`)).replaceAll('import.meta.url',JSON.stringify(`file:///pinned/api/${id}.lazy.ts`)).replace(/\bimport\(/g,'__load(');
 vm.runInContext(body,context);
 let calls=0;const operations=[];
 hostLoad=specifier=>{calls++;strictEqual(specifier,`./${id}.ts`);return Promise.resolve({stream:()=>{operations.push('raw');return done();},streamSimple:()=>{operations.push('simple');return done();}});};
 const factory=vm.runInContext(name,context), api=factory();strictEqual(calls,0);
 const raw=api.stream(model,{});strictEqual(calls,1);const simple=api.streamSimple(model,{});strictEqual(calls,2);
 await Promise.all([raw.result(),simple.result()]);
 results.families.push({id,calls,operations,keys:Object.keys(api)});
}
{
 const factory=vm.runInContext('bedrockConverseStreamApi',context),set=vm.runInContext('setBedrockProviderModule',context);
 const marked=n=>({stream:()=>{const s=new Stream();s.push({type:'done',reason:'stop',message:message('stop',n)});return s;},streamSimple:done});
 let release,calls=0;hostLoad=()=>{calls++;return new Promise(r=>release=r);};
 const a=factory(),b=factory();const pending=a.stream(model,{});strictEqual(calls,1);
 set(marked(2));const replacement=b.stream(model,{});set(marked(3));release(marked(1));
 const values=[(await pending.result()).usage.input,(await replacement.result()).usage.input,(await a.stream(model,{}).result()).usage.input];
 set(undefined);hostLoad=()=>{calls++;return Promise.resolve(marked(4));};values.push((await b.stream(model,{}).result()).usage.input);
 deepStrictEqual(values,[1,2,3,4]);strictEqual(calls,2);results.bedrock={values,calls};
}
{
 let calls=0;const api=lazyApi(()=>{calls++;return Promise.reject(new Error('synthetic typed setup failure'));});
 const values=[];for(let i=0;i<2;i++){const output=await api.stream(model,{}).result();values.push({reason:output.stopReason,usage:output.usage.input});}
 results.repeatedFailure={calls,values};strictEqual(calls,2);
}
const output=JSON.stringify(results,null,2)+'\n';
if(process.argv.includes('--check'))strictEqual(readFileSync(new URL('./trace.json',import.meta.url),'utf8'),output);
else process.stdout.write(output);
