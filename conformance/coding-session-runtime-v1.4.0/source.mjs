// Exact release objects only. Node's bundled type stripper; no node_modules.
import fs from 'node:fs';
import vm from 'node:vm';
import {execFileSync} from 'node:child_process';
import {stripTypeScriptTypes} from 'node:module';
import {createHash} from 'node:crypto';
const commit='4a080252f06caf63a525d7873c5d94d3c23887e0';
const repo=process.argv[2];if(!repo)throw new Error('Provide a checkout containing Pi v1.4.0');
const sources={};
function load(file,names,context={}){
 const source=execFileSync('git',['-C',repo,'show',`${commit}:${file}`],{encoding:'utf8'});sources[file]=createHash('sha256').update(source).digest('hex');
 const noImports=source.replace(/^import\s[\s\S]*?from\s+["'][^"']+["'];?\s*$/gm,'').replace(/^export\s+(?:type\s+)?\{[^}]*\}(?:\s+from\s+["'][^"']+["'])?;?\s*$/gm,'');
 const code=stripTypeScriptTypes(noImports).replace(/\bexport\s+/g,'');
 return vm.runInNewContext(`${code}\n;({${names.join(',')}})`,{...context,console,Buffer,TextEncoder,Date});
}
const base='packages/coding-agent/src/core/';
const messages=load(base+'messages.ts',['convertToLlm','createBranchSummaryMessage','createCompactionSummaryMessage','createCustomMessage','bashExecutionToText']);
const contentText=(content,separator='\n')=>typeof content==='string'?content:content.filter(b=>b.type==='text').map(b=>b.text).join(separator);
const session=load(base+'session-manager.ts',['buildSessionProjection','sessionEntryToContextMessages'],{...messages,getCurrentSystemMessage:()=>undefined});
const utils=load(base+'compaction/utils.ts',['createFileOps','extractFileOpsFromMessage','computeFileLists','serializeConversation'],{contentText});
const compaction=load(base+'compaction/compaction.ts',['estimateTokens','estimateContextTokens','estimateProjectedContextTokens','shouldCompact','prepareCompaction','getSummarizationFailure'],{...messages,...utils,...session,getCurrentSystemMessage:messages=>messages.findLast(m=>m.role==='system')});
const branch=load(base+'compaction/branch-summarization.ts',['prepareBranchEntries'],{...messages,...utils,estimateTokens:compaction.estimateTokens});
const cache=load(base+'cache-warmer.ts',['getCacheWarmingDelayMs','getPromptCacheTtlMs','isReplayable'],{getProviderEnvValue:()=>undefined});
const virtual=load(base+'virtual-models.ts',['getBranchSelection','getVirtualModelState'],{THINKING_LEVELS:['off','minimal','low','medium','high','xhigh','max']});
const ansi=load('packages/coding-agent/src/utils/ansi.ts',['splitIncompleteAnsiSuffix','stripAnsi']);
const shell=load('packages/coding-agent/src/utils/shell.ts',['sanitizeBinaryOutput']);
const truncate=load(base+'tools/truncate.ts',['truncateTail','DEFAULT_MAX_BYTES']);
const bash=load(base+'bash-executor.ts',['executeBashWithOperations'],{...ansi,...shell,...truncate,TextDecoder,createOutputFileStream:()=>{throw new Error('Small source fixture unexpectedly spilled');}});
const cases=JSON.parse(fs.readFileSync(new URL('cases.json',import.meta.url),'utf8'));
const observations={};
for(const row of cases){
 if(row.kind==='bash-sanitize'){const chunks=[];const result=await bash.executeBashWithOperations('fixture','/tmp',{exec:async(_command,_cwd,{onData})=>{for(const bytes of row.chunks)onData(Buffer.from(bytes));return {exitCode:row.exitCode};}},{onChunk:chunk=>chunks.push(chunk)});observations[row.id]={...result,chunks};}
 if(row.kind==='tokens')observations[row.id]=compaction.estimateTokens(row.message);
 if(row.kind==='convert')observations[row.id]=messages.convertToLlm(row.messages);
 if(row.kind==='estimate')observations[row.id]=compaction.estimateContextTokens(row.messages);
 if(row.kind==='prepare'){
  const entries=row.entries.map(e=>({...e,timestamp:new Date(e.timestamp).toISOString()}));
  const projection=session.buildSessionProjection(entries);
  const preparation=compaction.prepareCompaction(entries,row.settings);
  const result=preparation?{...preparation,fileOps:{read:[...preparation.fileOps.read],written:[...preparation.fileOps.written],edited:[...preparation.fileOps.edited]}}:null;
  observations[row.id]={estimate:compaction.estimateProjectedContextTokens(projection,entries),preparation:result};
 }
 if(row.kind==='branch-prepare'){const entries=row.entries.map(e=>({...e,timestamp:new Date(e.timestamp).toISOString()}));const p=branch.prepareBranchEntries(entries,row.tokenBudget);observations[row.id]={messages:p.messages,totalTokens:p.totalTokens,fileOps:{read:[...p.fileOps.read],written:[...p.fileOps.written],edited:[...p.fileOps.edited]}};}
 if(row.kind==='cache-delay')observations[row.id]=cache.getCacheWarmingDelayMs(row.ttl)??null;
 if(row.kind==='cache-ttl')observations[row.id]=cache.getPromptCacheTtlMs(row.model,row.options)??null;
 if(row.kind==='replayable')observations[row.id]=cache.isReplayable(row.model,row.options);
 if(row.kind==='serialize')observations[row.id]=utils.serializeConversation(row.messages);
 if(row.kind==='branch-selection')observations[row.id]=virtual.getBranchSelection(row.entries,(_p,id)=>row.virtualRegistered&&id==='router'?{api:'pi-virtual'}:undefined)??null;
 if(row.kind==='virtual-state')observations[row.id]=virtual.getVirtualModelState(row.entries,row.provider,row.modelId)??null;
 if(row.kind==='failure')observations[row.id]=compaction.getSummarizationFailure(row.message,row.label)??null;
}
const trace=JSON.stringify({upstreamCommit:commit,sources,observations},null,2)+'\n';const target=new URL('trace.json',import.meta.url);
if(process.argv.includes('--check')){if(fs.readFileSync(target,'utf8')!==trace)throw new Error('Pinned CodingSession observations changed');}else fs.writeFileSync(target,trace);
console.log(`Pinned CodingSession source: ${cases.length} cases; ${Object.keys(sources).length} exact files.`);
