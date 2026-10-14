import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {stripTypeScriptTypes} from 'node:module';
import vm from 'node:vm';
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const source=execFileSync('git',['show',`${commit}:packages/ai/src/utils/headers.ts`],{cwd:upstreamRoot,encoding:'utf8'});
const runtime=vm.createContext({});
vm.runInContext(stripTypeScriptTypes(source.replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,'')),runtime);
const raw=[['X-Repeat',' first '],['x-repeat','\tsecond\t'],['Cookie','a=1'],['cookie','b=2'],
 ['Set-Cookie','a=1; Expires=Wed, 09 Jun 2027 10:18:14 GMT'],['set-cookie',' b=2 '],['10','ten'],['2','two'],
 ['__proto__','ignored-string'],['constructor','literal-string'],['X-Empty',''],['X-Latin1',' é ']];
const headers=new Headers(raw);
const record=runtime.headersToRecord(headers);
console.log(JSON.stringify({sourceCommit:commit,node:process.version,evidenceClass:'executed-pinned-helper-and-native-Headers',raw,
 fetchEntries:[...headers.entries()],ownRecordEntries:Object.entries(record)},null,2));
