import {upstreamEnvironment} from '../../upstream.mjs';
import {execFileSync} from 'node:child_process';import {upstreamPath} from '../../upstream.mjs';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const root=new URL('./',import.meta.url);
const fingerprint=JSON.parse(readFileSync(new URL('source-fingerprints.json',root),'utf8'));
for(const[path,expected]of Object.entries(fingerprint.sha256)){assert.equal(createHash('sha256').update(readFileSync(upstreamPath('node_modules/')+path)).digest('hex'),expected,path);}
const runs=[['family-oracle.mjs','family.json'],['config-stage-oracle.mjs','config-stage.json'],['service-config-stage-oracle.mjs','service-config-stage.json'],['signing-vectors.mjs','signing-vectors.json'],['identity-oracle.mjs','identity.json'],['schema-oracle.mjs','schema.json'],['../../ai-transport/smithy-eventstream/source.mjs','../../ai-transport/smithy-eventstream/source.json'],['../../ai-transport/smithy-retry/source.mjs','../../ai-transport/smithy-retry/source.json'],['../../ai-transport/smithy-retry/native-errors.mjs','../../ai-transport/smithy-retry/native-errors.json']];
for(const[program,fixture]of runs){const actual=JSON.parse(execFileSync(process.execPath,[new URL(program,root).pathname],{env:upstreamEnvironment({PATH:'/usr/bin:/bin',NODE_NO_WARNINGS:'1',...(process.env.PIRUA_CARGO?{PIRUA_CARGO:process.env.PIRUA_CARGO}:{})}),encoding:'utf8',maxBuffer:32*1024*1024,timeout:30000}));const expected=JSON.parse(readFileSync(new URL(fixture,root),'utf8'));
 if(program.endsWith('native-errors.mjs')){
 // The original ephemeral refused-port row may happen to contain retry digits.
 // Seven fixed-port rows prove both sides without normalizing that observation.
 const a=structuredClone(actual),e=structuredClone(expected);delete a[0].assistantRetry;delete e[0].assistantRetry;assert.deepEqual(a,e,program);assert.equal(actual.length,15);
 }else{assert.equal(JSON.stringify(actual),JSON.stringify(expected),program);}
 console.log('PASS '+program);
}
console.log('PASS source fingerprints '+Object.keys(fingerprint.sha256).length+'; executed source programs '+runs.length);
