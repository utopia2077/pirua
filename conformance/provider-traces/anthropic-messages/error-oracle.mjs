import {execFileSync} from 'node:child_process';
import {createRequire,stripTypeScriptTypes} from 'node:module';
import vm from 'node:vm';
const commit='853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const root='upstream/pi-mono';const require=createRequire(`${root}/package.json`);
const SDK=require('@anthropic-ai/sdk').default;
const source=path=>execFileSync('git',['show',`${commit}:packages/ai/src/utils/${path}`],{cwd:root,encoding:'utf8'});
const context=vm.createContext({setTimeout,clearTimeout});
for(const path of ['retry.ts','overflow.ts'])vm.runInContext(stripTypeScriptTypes(source(path).replace(/^import[\s\S]*?;\n/gm,'').replace(/^export /gm,'')),context);
const inputs=[
 [429,'{"error":{"message":"billing exhausted"}}'],[503,'{"error":{"message":"quota exceeded"}}'],
 [503,'{"error":{"message":"server error"}}'],[429,'{"error":{"message":"rate limit"}}'],
 [400,'{"error":{"message":"prompt is too long"}}'],[403,'ordinary failure'],
 [403,'{"message":"visible outer message","error":{"message":"billing hidden"}}'],
 [403,'{"message":0,"error":{"message":"server error"}}'],[403,'false'],[403,'null'],[403,'0'],[403,'"text"'],[403,''],
 [503,'{"message":"billing prompt is too long"}'],[413,'\uFEFF{"error":{"type":"request_too_large"}}'],[400,''],[413,'']
];
const cases=[];
for(const [status,body] of inputs){
 const client=new SDK({apiKey:'fixture-key',authToken:null,baseURL:'https://fixture.invalid',maxRetries:0,fetch:async()=>new Response(body,{status,headers:{'content-type':'application/json'}})});
 try{await client.messages.create({model:'fixture',max_tokens:1,messages:[],stream:true},{maxRetries:0}).asResponse();throw new Error('expected SDK error');}
 catch(error){if(error.status!==status)throw error;const probe={stopReason:'error',errorMessage:error.message};
  cases.push({status,body,sdkMessage:error.message,overflow:context.isContextOverflow(probe),retryable:context.isRetryableAssistantError(probe),
   capOverflow:context.isContextOverflow({...probe,errorMessage:`Server requested 2s retry delay (max: 1s). ${error.message}`}),
   capRetryable:context.isRetryableAssistantError({...probe,errorMessage:`Server requested 2s retry delay (max: 1s). ${error.message}`})});}
}
console.log(JSON.stringify({sourceCommit:commit,sdk:'@anthropic-ai/sdk@0.91.1',evidenceClass:'executed-installed-sdk-and-pinned-classifiers',cases},null,2));
