import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {stripTypeScriptTypes} from 'node:module';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import {deepStrictEqual} from 'node:assert';
import vm from 'node:vm';
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const hashes={};
const source=path=>{const value=execFileSync('git',['show',`${commit}:${path}`],{cwd:upstreamRoot,encoding:'utf8'});hashes[path]=createHash('sha256').update(value).digest('hex');return value;};
class Events {events=[];finished=Promise.withResolvers();push(event){this.events.push(structuredClone(event));}end(){this.finished.resolve(this.events.at(-1).error);}result(){return this.finished.promise;}}
const context=vm.createContext({Date:{now:()=>7},Headers,URL,AbortSignal,Response,TextDecoder,AssistantMessageEventStream:Events,transformMessages:messages=>messages,getPiUserAgent:()=> 'pi-ai-rust',headersToRecord:headers=>Object.fromEntries(headers)});
for(const path of ['packages/ai/src/utils/retry.ts','packages/ai/src/utils/overflow.ts','packages/ai/src/api/mistral-conversations.ts'])vm.runInContext(stripTypeScriptTypes(source(path).replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,'')),context);
context.model={api:'mistral-conversations',provider:'mistral',id:'test',baseUrl:'https://synthetic.invalid',input:['text']};
const rows=[];
for(const [id,status,body] of [
 ['billing429',429,'billing'],['quota503',503,'quota exceeded'],['bareQuota503',503,'quota'],
 ['ordinary429',429,'ordinary'],['ordinary503',503,'ordinary'],['billing403',403,'billing'],
 ['overflowBilling429',429,'prompt is too long billing'],['overflowQuota503',503,'prompt is too long quota exceeded'],
 ['truncatedBilling429',429,'x'.repeat(4000)+'billing'],['overflow503',503,'prompt is too long'],
 ['asciiOverflowBeyond4000',403,'x'.repeat(4000)+'prompt is too long'],
 ['astralOverflowBeyond4000',403,'😀'.repeat(2000)+'prompt is too long'],
 ['overflowInside4000',403,'x'.repeat(3980)+' prompt is too long'],
 ['billingInside4000',429,'x'.repeat(3980)+' billing'],
 ['astralBillingBeyond4000',429,'😀'.repeat(2000)+'billing'],
 ['bothVetoes429',429,'prompt is too long rate limit billing'],
]){
 context.options={apiKey:'synthetic',fetch:async()=>new Response(body,{status,statusText:'Synthetic'})};
 context.message=await vm.runInContext('stream(model,{messages:[]},options).result()',context);
 rows.push({id,status,body,retryable:vm.runInContext('isRetryableAssistantError(message)',context),overflow:vm.runInContext('isContextOverflow(message)',context)});
}
const result={sourceCommit:commit,sourceHashes:hashes,evidenceClass:'executed-pinned-full-error-stream',scope:'Empty transcript; transformMessages identity stub unused by error path; real formatter and final classifiers; injected Response only',cases:rows};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./errors.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
