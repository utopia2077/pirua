import {execFileSync} from 'node:child_process';
import {stripTypeScriptTypes} from 'node:module';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {strictEqual, deepStrictEqual} from 'node:assert';
import vm from 'node:vm';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const paths=['utils/event-stream.ts','api/lazy.ts','utils/abort.ts','utils/diagnostics.ts','auth/resolve.ts','models.ts'];
const fingerprints={};
const clean=s=>stripTypeScriptTypes(s.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export \{[^}]*\} from [^;]*;\n/gm,'').replace(/^export /gm,''));
const context=vm.createContext({Error,AbortController,Date:class extends Date{static now(){return 7;}}});
for(const path of paths){
 const bytes=execFileSync('git',['show',pin+':packages/ai/src/'+path],{cwd:'upstream/pi-mono'});
 fingerprints[path]=createHash('sha256').update(bytes).digest('hex');
 vm.runInContext(clean(bytes.toString()),context,{filename:path});
}
const Stream=vm.runInContext('AssistantMessageEventStream',context),createModels=vm.runInContext('createModels',context),createProvider=vm.runInContext('createProvider',context);
const model={id:'m',api:'a',provider:'p',baseUrl:'https://synthetic.invalid'};
const message=(reason='pending',input=0)=>({role:'assistant',content:[{type:'text',text:'retained'}],api:'a',provider:'p',model:'m',usage:{input,output:0,cacheRead:0,cacheWrite:0,totalTokens:input,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}},stopReason:reason,timestamp:7});
const flush=async()=>{for(let i=0;i<40;i++)await Promise.resolve();};
const results=[];
for(const operation of ['raw','simple','deferred']) for(const duringHandoff of [false,true]) {
 const controller=new AbortController();let child, calls=0,seenSignal;
 const run=(_model,_input,options)=>{
  calls++;seenSignal=options.signal; child=new Stream();
  child.push({type:'start',partial:message()});
  const state=message('pending',17);
  const abort=()=>{state.stopReason='aborted';state.errorMessage='Request aborted';child.push({type:'error',reason:'aborted',error:state});};
  options.signal.addEventListener('abort',abort,{once:true});
  if(duringHandoff)controller.abort();
  return child;
 };
 const models=createModels({credentials:{read:async()=>undefined},modelsStore:{},authContext:{}});
 models.setProvider(createProvider({id:'p',auth:{apiKey:{name:'synthetic',resolve:async()=>({auth:{apiKey:'synthetic'}})}},models:[model],api:{stream:run,streamSimple:run,fetchDeferred:run}}));
 const options={apiKey:'synthetic',signal:controller.signal};
 const outer=operation==='raw'?models.stream(model,{},options):operation==='simple'?models.streamSimple(model,{},options):models.fetchDeferred(model,{id:'d'},options);
 await flush();strictEqual(calls,1);strictEqual(seenSignal,controller.signal);
 if(!duringHandoff)controller.abort();
 const result=await (operation==='deferred'?outer:outer.result());
 strictEqual(result.usage.input,17);strictEqual(result.content[0].text,'retained');
 const tags=[];
 if(operation!=='deferred')for await(const event of outer)tags.push(event.type);
 if(operation!=='deferred')deepStrictEqual(tags,['start','error']);
 results.push({operation,duringHandoff,calls,usage:result.usage.input,reason:result.stopReason,content:result.content[0].text,tags});
}
// Cancellation during noncooperative authentication: enter read and remain pending
// until the supplied original signal aborts. Negative modes exercise these same
// assertions rather than accepting another error with the same reason/usage.
{
 const controller=new AbortController();let calls=0,reads=0,settled=false,result;
 const credentials={read:(providerId,{signal})=>{
  reads++;strictEqual(providerId,'p');strictEqual(signal,controller.signal);
  return new Promise(()=>{});
 }};
 if(process.argv.includes('--control=wrong-method')){
  credentials.get=credentials.read;delete credentials.read;
 }
 const models=createModels({credentials,modelsStore:{},authContext:{}});
 models.setProvider({id:'p',auth:{apiKey:{name:'synthetic',resolve:async()=>undefined}},stream:()=>{calls++;throw new Error('unreachable');}});
 const outer=models.stream(model,{}, {signal:controller.signal});
 void outer.result().then(value=>{settled=true;result=value;});
 await flush();
 strictEqual(reads,1,'setup must enter credential read');
 strictEqual(settled,false,'setup must remain unsettled before abort');
 strictEqual(calls,0,'setup must not dispatch child before abort');
 const settledBeforeAbort=settled;
 const abortReason=new Error('synthetic registry setup cancellation');
 if(!process.argv.includes('--control=no-setup-abort'))controller.abort(abortReason);
 await flush();
 strictEqual(settled,true,'setup abort must settle outer');
 strictEqual(calls,0,'setup must not dispatch child after abort');
 strictEqual(reads,1,'setup must not retry credential read');
 strictEqual(result.errorMessage,abortReason.message,'setup result must come from original abort');
 strictEqual(result.stopReason,'error');strictEqual(result.usage.input,0);
 results.push({operation:'setup-cancel',reads,settledBeforeAbort,settledAfterAbort:settled,calls,usage:result.usage.input,reason:result.stopReason,errorMessage:result.errorMessage});
}
const output=JSON.stringify({pin,fingerprints,results},null,2)+'\n';
if(process.argv.includes('--check'))strictEqual(readFileSync(new URL('./trace.json',import.meta.url),'utf8'),output);
else process.stdout.write(output);
