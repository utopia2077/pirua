// Complete immutable helper modules; no algorithm extraction or body rewrite.
import {execFileSync} from 'node:child_process';
import {mkdtempSync,mkdirSync,writeFileSync,readFileSync,rmSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname} from 'node:path';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {registerHooks} from 'node:module';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const root=dirname(fileURLToPath(import.meta.url));
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const scratch=mkdtempSync(join(tmpdir(),'summary-content710-'));
const hashes={};
try {
 for(const path of ['packages/agent/src/harness/compaction/compaction.ts','packages/agent/src/harness/compaction/branch-summarization.ts','packages/agent/src/harness/compaction/utils.ts','packages/agent/src/harness/messages.ts','packages/agent/src/harness/session/context.ts','packages/ai/src/utils/text.ts','packages/ai/src/utils/sanitize-unicode.ts','packages/ai/src/utils/estimate.ts','packages/ai/src/api/simple-options.ts']){
 const bytes=execFileSync('git',['-C','upstream/pi-mono','show',pin+':'+path],{env:{PATH:'/usr/bin:/bin'}});hashes[path]=createHash('sha256').update(bytes).digest('hex');const target=join(scratch,path);mkdirSync(dirname(target),{recursive:true});writeFileSync(target,bytes);
 }
 writeFileSync(join(scratch,'package.json'),'{"type":"module"}');
 // Bounded adapters only: transport retries invoke once; errors/result constructors;
 // deterministic clock/UUID; provider captures requests and returns supplied receipts.
 writeFileSync(join(scratch,'ai.mjs'),`export {contentText} from './packages/ai/src/utils/text.ts'; export const uuidv7=()=> 'oracle-routing'; export const retryAssistantCall=async f=>f();`);
 writeFileSync(join(scratch,'types.mjs'),`export class CompactionError extends Error {constructor(code,msg){super(msg);this.code=code}} export class BranchSummaryError extends CompactionError {} export class SessionError extends CompactionError {} export const ok=value=>({ok:true,value});export const err=error=>({ok:false,error});`);
 const hooks=registerHooks({resolve(specifier,context,next){if(specifier==='@earendil-works/pi-ai')return {url:pathToFileURL(join(scratch,'ai.mjs')).href,shortCircuit:true};if(context.parentURL?.includes('/harness/')&&(specifier==='../types.ts'||specifier==='../session/index.ts'))return {url:pathToFileURL(join(scratch,'types.mjs')).href,shortCircuit:true};return next(specifier,context)}});
 const c=await import(pathToFileURL(join(scratch,'packages/agent/src/harness/compaction/compaction.ts')));
 const b=await import(pathToFileURL(join(scratch,'packages/agent/src/harness/compaction/branch-summarization.ts')));
 const utils=await import(pathToFileURL(join(scratch,'packages/agent/src/harness/compaction/utils.ts')));
 const messages=await import(pathToFileURL(join(scratch,'packages/agent/src/harness/messages.ts')));
 const {sanitizeSurrogates}=await import(pathToFileURL(join(scratch,'packages/ai/src/utils/sanitize-unicode.ts')));
 const zero={input:0,output:0,cacheRead:0,cacheWrite:0,totalTokens:0,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}};
 const user=(text,timestamp=1)=>({role:'user',content:text,timestamp});
 const assistant=(text,timestamp=2)=>({role:'assistant',content:[{type:'text',text}],api:'synthetic',provider:'synthetic',model:'fixture',usage:structuredClone(zero),stopReason:'stop',timestamp});
 const entry=(message,i)=>({type:'message',id:'e'+i,parentId:i?'e'+(i-1):null,seq:i+1,timestamp:i+1,message});
 const settings={enabled:true,reserveTokens:100,keepRecentTokens:7};
 const scenarios=[
 {name:'whole-turn',entries:[user('old user text'),assistant('old answer'),user('new user text'),assistant('new answer')].map(entry),settings},
 {name:'split-prefix',entries:[user('a'.repeat(40)),assistant('b'.repeat(40)),assistant('c'.repeat(40))].map(entry),settings:{...settings,keepRecentTokens:10}},
 {name:'previous-tail',entries:[{type:'compaction',id:'e0',parentId:null,seq:1,timestamp:1,summary:'previous summary',tokensBefore:50,retainedTail:[user('tail user'),assistant('tail answer')],details:{readFiles:['old'],modifiedFiles:['modified']}},entry(user('fresh user'),1),entry(assistant('fresh answer'),2)],settings},
 {name:'tool-boundary',entries:[user('question'),{...assistant(''),content:[{type:'toolCall',id:'c',name:'read',arguments:{large:1e21,path:'source.rs',tiny:1e-7}}],stopReason:'toolUse'},{role:'toolResult',toolCallId:'c',toolName:'read',content:[{type:'text',text:'x'.repeat(2010)}],isError:false,timestamp:3},assistant('final text')].map(entry),settings},
 ];
 const observations=scenarios.map(s=>{const result=c.prepareCompaction(s.entries,s.settings);assert(result.ok);return {...s,preparation:result.value,cut:c.findCutPoint(s.entries,0,s.entries.length,s.settings.keepRecentTokens),estimate:c.estimateContextTokens(s.entries.filter(e=>e.type==='message').map(e=>e.message))}});
 const jsonNormalize=x=>JSON.parse(JSON.stringify(x,(_,v)=>v instanceof Set?[...v].sort():v));
 const generator=[];Date.now=()=>17;
 const model={maxTokens:999,contextWindow:1000,reasoning:true};
 for(const scenario of observations){const calls=[];const prep=scenario.preparation;const response={...assistant('summary'),usage:{...zero,input:2,output:3,totalTokens:5}};const models={completeSimple:async(model,context,options)=>{calls.push({context,options:{...options,signal:undefined},model});return response}};const result=await c.compact(prep,models,model,'focus',undefined,'high');assert(result.ok);generator.push({name:scenario.name,calls,result:result.value,convertedHistory:messages.convertToLlm(prep.messagesToSummarize),convertedPrefix:messages.convertToLlm(prep.turnPrefixMessages)});}
 const branch=[];
 for(const budget of [0,7,10]){const entries=scenarios[3].entries;branch.push({budget,entries,preparation:b.prepareBranchEntries(entries,budget)})}
 const summaryEntries=[{type:'branch_summary',id:'s0',parentId:null,fromId:'earlier',seq:1,timestamp:1,summary:'x'.repeat(200),details:{readFiles:['carry'],modifiedFiles:[]}},entry(user('x'.repeat(12)),1)];
 for(const budget of [3,10])branch.push({budget,entries:summaryEntries,preparation:b.prepareBranchEntries(summaryEntries,budget)});
 const branchCalls=[];const branchResult=await b.generateBranchSummary(scenarios[0].entries,{models:{completeSimple:async(model,context,options)=>{branchCalls.push({context,options:{...options,signal:undefined}});return assistant('branch text')}},model,signal:new AbortController().signal,reserveTokens:0});
 const empty=await b.generateBranchSummary([],{models:{completeSimple(){throw Error('empty requested')}},model,signal:new AbortController().signal});
 const transcriptMessages=[user('unicode 😀'),{...assistant('visible'),content:[{type:'thinking',thinking:'thinking'},{type:'text',text:'visible'},{type:'toolCall',id:'x',name:'edit',arguments:{'2':'two','10':'ten',alpha:1e-7,z:1e21}}]},scenarios[3].entries[2].message];
 const {estimateContextTokens}=await import(pathToFileURL(join(scratch,'packages/ai/src/utils/estimate.ts')));
 const {buildBaseOptions,adjustMaxTokensForThinking,clampMaxTokensToContext,clampReasoning}=await import(pathToFileURL(join(scratch,'packages/ai/src/api/simple-options.ts')));
 const unicodeEntries=[user('old request'),{role:'toolResult',toolCallId:'unicode',toolName:'read',content:[{type:'text',text:'a'.repeat(1999)+'😀tail'}],isError:false,timestamp:2},user('recent')].map(entry);
 const unicodePreparation=c.prepareCompaction(unicodeEntries,{enabled:true,reserveTokens:2000,keepRecentTokens:1}).value;
 let unicodeRequest;
 for(const focus of ['','a','aa','aaa']){await c.compact(unicodePreparation,{completeSimple:async(model,context,options)=>{const raw=context.messages[0].content[0].text;const cleaned=sanitizeSurrogates(raw);if(raw.length%4===1){const constrained={...model,contextWindow:estimateContextTokens(context).tokens+4096+90};const cleanContext={...context,messages:[{...context.messages[0],content:[{type:'text',text:cleaned}]}]};unicodeRequest={entries:unicodeEntries,preparation:unicodePreparation,focus,logicalUnits:raw.length,context:cleanContext,model:constrained,desired:options.maxTokens,rawEstimate:estimateContextTokens(context).tokens,cleanEstimate:estimateContextTokens(cleanContext).tokens,rawCap:buildBaseOptions(constrained,context,options).maxTokens,cleanCap:buildBaseOptions(constrained,cleanContext,options).maxTokens,convertedHistory:messages.convertToLlm(unicodePreparation.messagesToSummarize)};}return assistant('unicode summary');}},model,focus);}
 assert(unicodeRequest);assert.equal(unicodeRequest.rawCap,90);assert.equal(unicodeRequest.cleanCap,91);
 // Labelled composition driver of complete helpers, not a copied helper body or SDK invocation.
 const wireCases=[];
 for(const maxTokens of [0,8192])for(const desired of [0,1000])for(const room of [90,2000])for(const level of ['minimal','low','xhigh','max']){
  const model={maxTokens,contextWindow:514+4096+room};const context={messages:[user('x'.repeat(2053))]};
  const base=buildBaseOptions(model,context,{maxTokens:desired,reasoning:level});
  const adjusted=adjustMaxTokensForThinking(base.maxTokens,maxTokens,level);
  const finalCap=clampMaxTokensToContext(model,context,adjusted.maxTokens);
  const effectiveBudget=Math.min(adjusted.thinkingBudget,Math.max(0,finalCap-1024));
  wireCases.push({model,desired,level,finalCap,effectiveBudget,wireBase:finalCap-effectiveBudget,bucket:clampReasoning(level),payloadBudget:effectiveBudget||1024});
 }
 const scalarCases=[1998,1999].flatMap(count=>['','tail'].map(tail=>{const message={role:'toolResult',toolCallId:'scalar',toolName:'read',content:[{type:'text',text:'a'.repeat(count)+'😀'+tail}],isError:false,timestamp:1};const raw=utils.serializeConversation([message]);return {message,clean:sanitizeSurrogates(raw),logicalUnits:raw.length};}));
 const output=jsonNormalize({pin,hashes,wireCases,unicodeRequest,scalarCases,observations,generator,branch,branchCalls,branchResult,empty,transcriptMessages,transcript:utils.serializeConversation(transcriptMessages),tokens:transcriptMessages.map(c.estimateTokens)});
 const bytes=JSON.stringify(output,null,2)+'\n';const target=join(root,'golden.json');if(process.argv.includes('--check'))assert.equal(readFileSync(target,'utf8'),bytes);else writeFileSync(target,bytes);hooks.deregister();console.log('complete pinned helper oracle: '+observations.length+' preparation histories, '+generator.length+' compaction generations, 5 branch budgets, branch/empty/transcript');
}finally{rmSync(scratch,{recursive:true,force:true})}
