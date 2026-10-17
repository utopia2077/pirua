// Observe only the exact release objects. No upstream working-tree imports.
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
 return vm.runInNewContext(`${code}\n;({${names.join(',')}})`,{...context,console,Buffer,TextEncoder,Date,URL,setTimeout,clearTimeout});
}
const truncate=load('packages/coding-agent/src/core/tools/truncate.ts',['formatSize','truncateMiddle']);
const content=load('packages/mcp/src/protocol/content.ts',['toLlmContent']);
const mcp=load('packages/coding-agent/src/extensions/mcp/tools.ts',['createMcpToolName','createMcpResultSchema','convertMcpResult'],{...truncate,...content,createHash,READ_MCP_RESOURCE_TOOL:'read_mcp_resource'});
const parse=load('packages/codemode/src/source.ts',['parseCodemodeSource']);
const execute=load('packages/coding-agent/src/extensions/codemode/execute.ts',['formatOutput','toScriptValue'],{CODEMODE_DOCS_PATH:'codemode.md',CODEMODE_STORE_ENTRY_TYPE:'codemode-store'});
const search=load('packages/coding-agent/src/extensions/tool-search/tool.ts',['Bm25Ranker','createToolSearchDocument'],{Type:new Proxy({},{get:()=>()=>({})})});
const cases=JSON.parse(fs.readFileSync(new URL('cases.json',import.meta.url),'utf8'));
const observations={};
for(const row of cases){
 if(row.kind==='name')observations[row.id]=mcp.createMcpToolName(row.server,row.tool,()=>row.taken??false);
 if(row.kind==='schema')observations[row.id]=mcp.createMcpResultSchema(row.schema);
 if(row.kind==='mcp-result')observations[row.id]=await mcp.convertMcpResult('server','tool',row.result,{readableResources:true,saveOutput:async()=>'/fixture/output'});
 if(row.kind==='parse'){try{observations[row.id]=parse.parseCodemodeSource(row.source);}catch{observations[row.id]={error:true};}}
 if(row.kind==='format')observations[row.id]=execute.formatOutput(row.output);
 if(row.kind==='script-value'){try{const result=execute.toScriptValue(row.tool,row.outcome);observations[row.id]={value:result};}catch(error){observations[row.id]={error:error.message};}}
 if(row.kind==='search')observations[row.id]=new search.Bm25Ranker().rank(row.query,row.tools.map(tool=>search.createToolSearchDocument(tool)),8).map(match=>match.name);
}
const trace=JSON.stringify({upstreamCommit:commit,sources,observations},null,2)+'\n';
const target=new URL('trace.json',import.meta.url);
if(process.argv.includes('--check')){if(fs.readFileSync(target,'utf8')!==trace)throw new Error('Pinned adapter observations changed');}else fs.writeFileSync(target,trace);
console.log(`Pinned tool adapter source: ${cases.length} cases; ${Object.keys(sources).length} exact files.`);
