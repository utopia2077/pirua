import assert from 'node:assert/strict';
import { readFileSync, writeFileSync, mkdirSync, rmSync } from 'node:fs';
import { spawnSync } from 'node:child_process';
import { resolve } from 'node:path';
import { directory,target,sha256,verifyEnvironment } from './setup.mjs';
import { assertScalarStrings } from './trace-codec.mjs';
const args=process.argv.slice(2);
assert.equal(args.filter(a=>['--check','--write','--causal'].includes(a)).length,1,'Choose --check, --write or --causal');
const pin=verifyEnvironment({pinPath:resolve(directory,'codemode-source-pin.json')});
for(const [path,digest]of Object.entries(pin.frozenPriorFiles))assert.equal(sha256(readFileSync(resolve(directory,path))),digest,'Frozen prior file changed: '+path);
const {executeCodemodeCase}=await import('./codemode-runner.mjs');
const option=args.indexOf('--fixture');const path=resolve(directory,option<0?'fixtures/codemode.json':args[option+1]);
const fixture=JSON.parse(readFileSync(args.includes('--write')?resolve(directory,'fixtures/codemode-inputs.json'):path,'utf8'));
assert.equal(fixture.schemaVersion,1);assert.equal(fixture.upstreamCommit,target);assertScalarStrings(fixture);
if(args.includes('--causal')){
  const changed=structuredClone(fixture.cases.find(test=>test.id==='codemode-scripted-count-before-effect'));
  changed.expected.memory.effectEvents[0].committedJournal.firstIssued+=1;
  const temp=resolve(directory,'.runtime','codemode-causal-'+process.pid+'.json');mkdirSync(resolve(directory,'.runtime'),{recursive:true});writeFileSync(temp,JSON.stringify({...fixture,cases:[changed]}));
  try{const result=spawnSync(process.execPath,[resolve(directory,'codemode.mjs'),'--check','--fixture',temp],{cwd:directory,encoding:'utf8',timeout:30000,maxBuffer:4*1024*1024});assert.equal(result.status,1);assert.match(result.stderr,/Pinned CodeMode observations differ: codemode-scripted-count-before-effect/);console.log('PASS CodeMode causal control: actual --check rejects changed persisted count before effect');}
  finally{rmSync(temp,{force:true});}
}else{
  const output={schemaVersion:1,upstreamCommit:target,cases:[]};let executions=0;
  for(const test of fixture.cases){
    for(const path of test.basis.sources)assert.ok(pin.sources[path],'Unpinned CodeMode source '+path);
    const expected={};for(const backend of test.backends??['memory','sqlite']){const entropyBefore=Object.getOwnPropertyDescriptor(crypto,'getRandomValues');const nowBefore=Object.getOwnPropertyDescriptor(performance,'now');expected[backend]=await executeCodemodeCase(test,backend);assert.deepEqual(Object.getOwnPropertyDescriptor(crypto,'getRandomValues'),entropyBefore,'Entropy host must restore');assert.deepEqual(Object.getOwnPropertyDescriptor(performance,'now'),nowBefore,'Duration host must restore');executions++;if(args.includes('--check'))assert.deepEqual(expected[backend],test.expected[backend],'Pinned CodeMode observations differ: '+test.id+' ('+backend+')');}
    output.cases.push({...test,expected});
  }
  assertScalarStrings(output);if(args.includes('--write'))writeFileSync(path,JSON.stringify(output,null,2)+'\n');
  console.log('PASS CodeMode '+(args.includes('--write')?'generation':'check')+': '+output.cases.length+' cases, '+executions+' actual release/backend executions; all37 prior files unchanged');
}
