import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync } from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { createModels, createProvider, createAssistantMessageEventStream, fauxProvider } from '@earendil-works/pi-ai';
import { Harness, ProviderDoc, LiveDoc, ToolTask, createRegistry, defineExtension, defineTool, hook } from '@earendil-works/pi-durable';
import { MemoryStorage } from '@earendil-works/pi-durable/storage/memory';
import { openNodeSqliteStorage } from '@earendil-works/pi-durable/storage/sqlite/node';
import { CodemodeSandbox } from '@earendil-works/pi-codemode';
import { BACKGROUND_CONTEXT as context } from '@earendil-works/chord/context';
import { directory } from './setup.mjs';
import { encodeTraceValue } from './trace-codec.mjs';
const { createCodemodeTool, CodemodeRunDoc, CodemodeJournalDoc, CodemodeStoreDoc } =
  await import(pathToFileURL(resolve(directory, 'node_modules/@earendil-works/pi-durable/dist/codemode/codemode.js')));
const errorValue = error => ({ category: error?.name ?? 'Error', message: error?.message ?? String(error) });
const copy = value => value === undefined ? { $undefined: true } : encodeTraceValue(JSON.parse(JSON.stringify(value, (_key,v) => v instanceof Error ? errorValue(v) : typeof v === 'function' ? undefined : v)));
function ref(value, aliases) {
  if (typeof value === 'string' && value.startsWith('$')) {
    const [name,...keys]=value.slice(1).split('.'); assert.ok(Object.hasOwn(aliases,name),'Unknown CodeMode reference '+value);
    let found=aliases[name]; for(const key of keys)found=found[key]; return structuredClone(found);
  }
  if(Array.isArray(value))return value.map(child=>ref(child,aliases));
  if(value&&typeof value==='object') {
    if(value.$undefined===true&&Object.keys(value).length===1)return undefined;
    return Object.fromEntries(Object.entries(value).map(([key,child])=>[key,ref(child,aliases)]));
  }
  return value;
}
const tick=()=>new Promise(done=>setImmediate(done));
async function flush(){for(let i=0;i<6;i++)await tick();}
function bounded(promise,label) {
  let timer; return Promise.race([promise,new Promise((_,fail)=>{timer=setTimeout(()=>fail(new Error('CodeMode host timeout: '+label)),10000);})]).finally(()=>clearTimeout(timer));
}
async function eventually(check,label) {
  const deadline=Date.now()+8000;
  while(Date.now()<deadline){if(await check())return;await new Promise(done=>setTimeout(done,2));}
  throw new Error('CodeMode condition not reached: '+label);
}
const zeroUsage=()=>({input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}});
export async function executeCodemodeCase(test,backend) {
  // Approved test-host entropy/duration boundary; restored when this case ends.
  const originalEntropy=crypto.getRandomValues;
  const originalEntropyDescriptor=Object.getOwnPropertyDescriptor(crypto,'getRandomValues');
  const originalNow=Object.getOwnPropertyDescriptor(performance,'now');
  Object.defineProperty(crypto,'getRandomValues',{configurable:true,value:function(array){if(array instanceof Uint32Array&&array.length===1){array[0]=424242;return array;}return originalEntropy.call(this,array);}});
  Object.defineProperty(performance,'now',{configurable:true,value:()=>0});
  mkdirSync(resolve(directory,'.runtime'),{recursive:true});
  const temporary=mkdtempSync(resolve(directory,'.runtime','codemode-'));const path=resolve(temporary,'store.sqlite');
  let denyNextCommit=false;
  const makeStorage=async()=>{
    const backendStorage=await(backend==='memory'?new MemoryStorage():openNodeSqliteStorage(path));
    return new Proxy(backendStorage,{get(target,key){
      if(key==='commit')return async(...args)=>{if(denyNextCommit){denyNextCommit=false;throw new Error('Oracle journal commit denied');}return target.commit(...args);};
      const found=Reflect.get(target,key,target);return typeof found==='function'?found.bind(target):found;
    }});
  };
  let storage=await makeStorage();let harness;let wall=1000;let codeTask;
  const aliases={root:1};const gates=new Map();const registry=createRegistry();const vmEvents=[];const effects={};const effectEvents=[];const publications=[];const reports=[];const observations=[];const files=[];const providerQueue=[];const decisions=[];
  const latestRuns=new Map();const latestJournals=new Map();const runs=[];let localSandbox;let vmStarts=0;let afterCount=0;
  const bindings=extra=>({...aliases,clock:wall,...extra});
  function gate(name){if(!gates.has(name)){let release;const promise=new Promise(done=>{release=done;});gates.set(name,{promise,release});}return gates.get(name);}
  function waitGate(name,signal,abortable=true){const found=gate(name);if(!abortable)return found.promise;return new Promise((done,fail)=>{const abort=()=>fail(signal?.reason??new Error('Aborted'));if(signal?.aborted)return abort();signal?.addEventListener('abort',abort,{once:true});found.promise.then(()=>{signal?.removeEventListener('abort',abort);done();});});}
  function published(publication) {
    publications.push(publication);
    for(const change of publication.changes)if(change.type==='document') {
      const owner=change.record.scope.taskId;
      if(change.record.kind==='pi.codemode.run')latestRuns.set(owner,change.value);
      if(change.record.kind==='pi.codemode.journal')latestJournals.set(owner,change.value);
    }
  }
  function vmRecord(kind,data={}){vmEvents.push({kind,attempt:vmStarts,...copy(data)});}
  function wrapRun(run,options) {
    const id=vmStarts;
    const wrapped={get finished(){return run.finished;},result:run.result,deliver(seq,at,callId,outcome){
      vmEvents.push({kind:'deliver',attempt:id,seq,at,id:callId,outcome:copy(outcome),committedJournal:copy(latestJournals.get(codeTask))});
      run.deliver(seq,at,callId,outcome);
    }};
    run.result.then(result=>vmEvents.push({kind:'terminal',attempt:id,result:copy(result)}));
    runs.push({run:wrapped,options,raw:run});return wrapped;
  }
  function scriptedStart(descriptor,options) {
    let finished=false;let complete;
    const result=new Promise(done=>{complete=done;});
    const emit=raw=>{
      const action=ref(raw,bindings({}));
      vmRecord('scriptedEmit',{action});
      if(action.event==='started')options.onStarted?.(action.fingerprint??descriptor.fingerprint??'scripted-vm-1');
      else if(action.event==='call')options.onCall(action.call);
      else if(action.event==='idle')options.onIdle?.(action.seq,action.issued);
      else if(action.event==='terminal'){if(!finished){finished=true;complete(action.result);}}
      else throw new Error('Unknown scripted sandbox event '+action.event);
    };
    options.signal?.addEventListener('abort',()=>{if(!finished){finished=true;complete({ok:false,error:{kind:'aborted',message:'Scripted sandbox aborted'},output:[],calls:[]});}},{once:true});
    const run={get finished(){return finished;},result,emit,deliver(seq,at,id,outcome){const actions=descriptor.onDeliver?.[String(seq)]??[];queueMicrotask(()=>{for(const action of actions)emit(ref(action,bindings({delivery:{seq,at,id,outcome}})));});},close(){if(!finished){finished=true;complete({ok:false,error:{kind:'aborted',message:'Scripted sandbox closed'},output:[],calls:[]});}}};
    queueMicrotask(()=>{for(const action of descriptor.initial??[])emit(action);});
    return run;
  }
  const sandbox={start(code,options) {
    vmStarts++;codeTask=[...latestRuns.keys()].at(-1);
    const {signal,onStarted,onCall,onIdle,...jsonOptions}=options;
    vmRecord('start',{code,options:jsonOptions,persistedRun:latestRuns.get(codeTask),persistedJournal:latestJournals.get(codeTask)});
    assert.ok(latestRuns.get(codeTask)?.start,'Actual start must commit before Sandbox.start');
    const observed={...options,onStarted:fingerprint=>{vmRecord('started',{fingerprint});onStarted?.(fingerprint);},onCall:call=>{vmRecord('call',{call});onCall(call);},onIdle:(seq,issued)=>{vmRecord('idle',{seq,issued});onIdle?.(seq,issued);}};
    let run;
    if(test.input.sandbox?.kind==='scripted')run=scriptedStart(test.input.sandbox.attempts[vmStarts-1],observed);
    else {localSandbox??=new CodemodeSandbox({memoryLimitBytes:64*1024*1024});run=localSandbox.start(code,observed);}
    return wrapRun(run,observed);
  }};
  for(const descriptor of test.input.tools??[]) {
    const tool=defineTool({name:descriptor.name,description:descriptor.name,parameters:descriptor.parameters??{type:'object',properties:{},additionalProperties:true},replay:descriptor.replay??'safe',...(descriptor.structuredOutputSchema?{structuredOutputSchema:descriptor.structuredOutputSchema}:{}),async execute(args,api,c){
      const run=effects[descriptor.name]=(effects[descriptor.name]??0)+1;
      const task=await storage.task(api.taskId,context);
      effectEvents.push({kind:'tool',name:descriptor.name,run,args:copy(args),task:copy(task),committedRun:copy(latestRuns.get(task.input.parent)),committedJournal:copy(latestJournals.get(task.input.parent))});
      if(descriptor.gate&&(descriptor.firstRuns===undefined||run<=descriptor.firstRuns))await waitGate(ref(descriptor.gate,bindings({args,run})),c.abortSignal);
      if(descriptor.output)api.output(ref(descriptor.output,bindings({args,run})));
      return ref(descriptor.result??{output:[{type:'text',text:descriptor.name}]},bindings({args,run}));
    }});
    registry.install(defineExtension({name:'local:'+descriptor.name,tools:[tool]}));
  }
  const globals=(test.input.globals??[]).map(descriptor=>({name:descriptor.name,spread:descriptor.spread===true,async execute(args,api,c){
    const run=effects[descriptor.name]=(effects[descriptor.name]??0)+1;
    effectEvents.push({kind:'global',name:descriptor.name,run,args:copy(args),committedRun:copy(latestRuns.get(api.taskId)),committedJournal:copy(latestJournals.get(api.taskId))});
    if(descriptor.gate&&(descriptor.firstRuns===undefined||run<=descriptor.firstRuns))await waitGate(descriptor.gate,c.abortSignal,descriptor.abortable!==false);
    if(descriptor.denyNextCommit)denyNextCommit=true;
    if(descriptor.throw)throw new Error(descriptor.throw);
    return ref(Object.hasOwn(descriptor,'value')?descriptor.value:{$undefined:true},bindings({args,run}));
  }}));
  registry.install(defineExtension({name:'oracle:codemode',tools:[createCodemodeTool({sandbox,models:false,globals})]}));
  if(test.input.holdAfterDecision)registry.install(defineExtension({name:'oracle:after-decision',hooks:[hook(ToolTask,{async afterTool(call,result,api,c){
    if(call.name==='codemode'&&++afterCount===1){decisions.push({taskId:api.taskId,run:copy(await harness.snapshot(CodemodeRunDoc,api.taskId,context))});await waitGate('after-decision',c.abortSignal);}
  }})]}));
  const metadata=fauxProvider({api:'oracle-code',provider:'oracle',models:[{id:'scripted-1',contextWindow:100000,maxTokens:1000}]});
  const transport=(model,_transcript,_options)=>{
    const descriptor=providerQueue.shift();assert.ok(descriptor,'Unexpected provider request in '+test.id);
    const message={role:'assistant',content:descriptor.code===undefined?[{type:'text',text:'done'}]:[{type:'toolCall',id:descriptor.callId,name:'codemode',arguments:{code:descriptor.code}}],api:model.api,provider:model.provider,model:model.id,usage:zeroUsage(),stopReason:descriptor.code===undefined?'stop':'toolUse',timestamp:wall,durationMs:0};
    const stream=createAssistantMessageEventStream();queueMicrotask(()=>{stream.push({type:'done',reason:message.stopReason,message});stream.end(message);});return stream;
  };
  const models=createModels();models.setProvider(createProvider({id:'oracle',auth:{apiKey:{name:'Offline script',resolve:async()=>({auth:{}})}},models:metadata.models,api:{stream:transport,streamSimple:transport}}));
  const environment={async createTempDir(prefix){files.push({kind:'createTempDir',prefix});return {ok:true,value:'/oracle/code-output'};},async writeFile(path,data){files.push({kind:'writeFile',path,data:typeof data==='string'?data:{base64:Buffer.from(data).toString('base64')},committedRun:copy(latestRuns.get(codeTask))});return {ok:true,value:undefined};}};
  async function open(){
    harness=await Harness.open(storage,{models,registry,now:()=>wall,onWake:()=>{},setTimer:()=>undefined,settings:()=>({compaction:{enabled:false},progress:{partialIntervalMs:10,outputIntervalMs:10}}),env:async()=>environment,conversationCreated:async(tx,conv)=>{(await tx.doc(ProviderDoc,conv.id)).sessionId='fixture-session-'+conv.id;},onReport(error,info){reports.push({error:errorValue(error),type:info.type,...(info.task?{task:copy(info.task)}:{})});}},context);
    harness.subscribeCommits(published);await harness.root(context,{agent:{model:{provider:'oracle',modelId:'scripted-1'}}});await flush();
  }
  async function conv(id=1){const handle=await harness.conversation(id,context);assert.ok(handle);return handle;}
  async function snapshotTaskDocs(id){return {run:await harness.snapshot(CodemodeRunDoc,id,context),journal:await harness.snapshot(CodemodeJournalDoc,id,context)};}
  async function changeDoc(token,id,change){
    return harness.commit(async tx=>{const draft=await tx.doc(token,id);for(const [path,value]of Object.entries(change.set??{})){const parts=path.split('.');const key=parts.pop();let object=draft;for(const part of parts)object=object[part];object[key]=value;}for(const path of change.delete??[]){const parts=path.split('.');const key=parts.pop();let object=draft;for(const part of parts)object=object[part];delete object[key];}},context);
  }
  try {
    await open();
    for(let index=0;index<test.input.operations.length;index++){
      const op=ref(test.input.operations[index],bindings({}));let value;
      try {
        switch(op.op) {
          case 'run':{providerQueue.push({code:op.code,callId:op.callId??'script-'+index});if(op.answer!==false)providerQueue.push({});value=(await(await conv(op.conversationId)).submit({type:'input',content:op.content??'run script'},context)).id;break;}
          case 'wait':value=await bounded((await harness.submission(op.id,context)).wait(context),'submission');await flush();break;
          case 'status':value=await(await harness.submission(op.id,context)).status(context);break;
          case 'untilEffects':await eventually(()=>(effects[op.name]??0)>=op.count,'effect '+op.name);await flush();value=effects;break;
          case 'untilVm':await eventually(()=>vmEvents.filter(event=>event.kind===op.kind&&(op.attempt===undefined||event.attempt===op.attempt)).length>=op.count,'VM '+op.kind);await flush();value=vmEvents;break;
          case 'untilJournal':await eventually(()=>{const journal=latestJournals.get(codeTask);if(!journal)return false;let v=journal;for(const key of op.path??[])v=v?.[key];return op.equals===undefined?v!==undefined:JSON.stringify(v)===JSON.stringify(op.equals);},'journal');value=latestJournals.get(codeTask);break;
          case 'untilDecision':await eventually(()=>decisions.length>0,'decided result');value=decisions;break;
          case 'taskDocs':value=await snapshotTaskDocs(op.id??codeTask);break;
          case 'committedTaskDocs':value={run:latestRuns.get(codeTask),journal:latestJournals.get(codeTask)};break;
          case 'closed':value=await bounded(harness.closed,'closed reason');break;
          case 'tasks':value=await harness.commit(tx=>tx.scanTasks({conversationId:op.conversationId??1},100),context);break;
          case 'entries':value=await(await conv(op.conversationId)).entries({order:'ascending'},1000,undefined,context);break;
          case 'live':value=await harness.snapshot(LiveDoc,op.conversationId??1,context);break;
          case 'store':value=await harness.snapshot(CodemodeStoreDoc,op.conversationId??1,context);break;
          case 'storeAsOf':value=await harness.snapshotAsOf(CodemodeStoreDoc,op.conversationId??1,op.at,context);break;
          case 'storeSet':value=await changeDoc(CodemodeStoreDoc,op.conversationId??1,{set:{values:op.values}});break;
          case 'checkpointPolicy':value={run:CodemodeRunDoc.definition.checkpointWhen({},[],{deltasSinceBase:1000}),journal:CodemodeJournalDoc.definition.checkpointWhen({entries:[]},[],{deltasSinceBase:1000}),store63:CodemodeStoreDoc.definition.checkpointWhen({values:{}},[],{deltasSinceBase:63}),store64:CodemodeStoreDoc.definition.checkpointWhen({values:{}},[],{deltasSinceBase:64})};break;
          case 'storeHistory':{let cutoff;for(let counter=0;counter<=64;counter++){const entry=await harness.commit(async tx=>{(await tx.doc(CodemodeStoreDoc,1)).values.counter=counter;return(await tx.appendEntry(1,{kind:'fixture.counter'})).id;},context);if(counter===32)cutoff=entry;}value={cutoff};break;}
          case 'storeCounter':value=await harness.commit(async tx=>{(await tx.doc(CodemodeStoreDoc,1)).values.counter=op.counter;},context);break;
          case 'storageStore':{const record=await storage.findDocument({kind:CodemodeStoreDoc.definition.kind,scope:{kind:'conversation',conversationId:1}},'current',context);value={record,materialized:record&&await storage.document(record.id,'current',context)};break;}
          case 'patchRun':value=await changeDoc(CodemodeRunDoc,op.id??codeTask,op);break;
          case 'patchJournal':value=await changeDoc(CodemodeJournalDoc,op.id??codeTask,op);break;
          case 'effects':value={effects,events:effectEvents};break;
          case 'vmEvents':value=vmEvents;break;
          case 'files':value=files;break;
          case 'release':gate(op.name).release();await flush();value='released';break;
          case 'advance':assert.ok(op.to>=wall);wall=op.to;value=wall;break;
          case 'resume':harness.resume();await flush();value='resumed';break;
          case 'fork':value=(await(await conv(op.conversationId)).fork(op.at,{ownership:{kind:'ownerless'}},context)).id;break;
          case 'abort':value=await bounded((await conv(op.conversationId)).abort(context),'durable abort');await flush();break;
          case 'sandboxClose':if(test.input.sandbox?.kind==='scripted')runs.at(-1).raw.close();else{await localSandbox.close();localSandbox=undefined;}await flush();value='sandbox closed';break;
          case 'emit':assert.equal(test.input.sandbox?.kind,'scripted');runs[op.attempt===undefined?runs.length-1:op.attempt-1].raw.emit(op.event);await flush();value='emitted';break;
          case 'reopen':assert.equal(backend,'sqlite');await bounded(harness.close(context),'close for reopen');storage=await makeStorage();await open();value='reopened paused';break;
          case 'close':await bounded(harness.close(context),'close');value='closed';break;
          default:throw new Error('Unknown CodeMode host operation '+op.op);
        }
      }catch(error){if(!op.errorExpected){error.message=test.id+' ('+backend+') operation '+index+' '+op.op+': '+error.message;throw error;}value={error:errorValue(error)};}
      if(op.as)aliases[op.as]=value;
      observations.push({label:op.label??op.as??index+':'+op.op,value:copy(value)});
    }
    await flush();assert.equal(providerQueue.length,0,'Unused provider responses');
    return {observations,vmEvents:copy(vmEvents),effects:copy(effects),effectEvents:copy(effectEvents),publications:copy(publications),reports:copy(reports),files:copy(files)};
  }finally{
    for(const found of gates.values())found.release();
    await bounded(harness?.close(context)??Promise.resolve(),'cleanup');
    await localSandbox?.close();rmSync(temporary,{recursive:true,force:true});
    if(originalEntropyDescriptor)Object.defineProperty(crypto,'getRandomValues',originalEntropyDescriptor);else delete crypto.getRandomValues;
    if(originalNow)Object.defineProperty(performance,'now',originalNow);else delete performance.now;
  }
}
