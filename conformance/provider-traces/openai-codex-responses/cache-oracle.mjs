import {assertCodexSourceEqual} from './source-comparison.mjs';import {runtime,authority,fullEvents,context,clone} from './harness.mjs';
import {readFileSync} from 'node:fs';
import assert from 'node:assert/strict';
const results=[];
for(const transport of [undefined,'auto','websocket','websocket-cached']){
 const r=runtime(),options={sessionId:'session',...(transport?{transport}:{})};
 const first=await r.run({options});const secondContext={...context,messages:[...context.messages,first.result,{role:'user',content:'next',timestamp:8}]};
 const second=await r.run({options,context:secondContext});const stats=r.stats('session');r.close('session');
 results.push({id:'mode-'+(transport??'omitted'),options,first,second,stats,trace:r.trace});
 assert.equal(stats.connectionsReused,1);assert.equal(stats.deltaRequests,['auto','websocket-cached'].includes(transport)?1:0);
}
for(const mode of ['reset','close','closed','age','idle','none','account']){
 const r=runtime(),options={sessionId:'session',transport:'websocket-cached',...(mode==='none'?{cacheRetention:'none'}:{})};
 const first=await r.run({options});const before=r.stats('session');
 if(mode==='reset')r.reset('session');if(mode==='close')r.close('session');if(mode==='closed')r.sockets[0].readyState=3;if(mode==='age')r.advance(56*60*1000);if(mode==='idle')r.fire();
 const afterControl=r.stats('session');const second=await r.run({options,...(mode==='account'?{account:'account-b'}:{})});const stats=r.stats('session');r.close();results.push({id:mode,options,first,second,before,afterControl,stats,trace:r.trace});
}
{
 const r=runtime({ws:[{error:true},{},{}]}),options={sessionId:'session',transport:'auto'};
 const first=await r.run({options});const second=await r.run({options});r.close('session');const beforeReset=r.stats('session');r.reset('session');const third=await r.run({options});const stats=r.stats('session');r.close();results.push({id:'sticky',options,first,second,third,beforeReset,stats,trace:r.trace});assert.equal(r.trace.sent.length,2);assert.equal(beforeReset.sseFallbacks,2);
}
{
 const r=runtime({ws:[{hold:true},{}]}),options={sessionId:'session',transport:'auto'};
 const firstPending=r.start({options});for(let i=0;i<40;i++)await Promise.resolve();assert.equal(r.trace.sent.length,1);
 const second=await r.run({options});for(const e of fullEvents('r1'))r.sockets[0].emit('message',{data:JSON.stringify(e)});
 const first=await firstPending.finish();const stats=r.stats('session');r.close();results.push({id:'busy',options,first,second,stats,trace:r.trace});assert.equal(r.trace.connections.length,2);
}

for(const id of ['empty-delta','prefix-mismatch','ordered-body-mismatch','previous-after-start','previous-retry-sse','previous-twice','limit-after-start']){
 const error=code=>({type:'error',error:{code}}),started={type:'codex.rate_limits'};
 const config=id.startsWith('previous')?{ws:[{}, {events:[started,error('previous_response_not_found')]}, id==='previous-retry-sse'?{error:true}:id==='previous-twice'?{events:[error('previous_response_not_found')]}:{}]}:id==='limit-after-start'?{ws:[{}, {events:[started,error('websocket_connection_limit_reached')]}]}:{};
 const r=runtime(config),options={sessionId:'session',transport:'websocket-cached'};const first=await r.run({options});
 const messages=[...context.messages,first.result,...(id==='empty-delta'?[]:[{role:'user',content:'next',timestamp:8}])];if(id==='prefix-mismatch')messages[0]={role:'user',content:'changed',timestamp:7};
 const secondContext={...context,messages};let replacement;
 if(id==='ordered-body-mismatch'){
  const {type,model,...rest}=r.trace.sent[0].body;
  replacement={...rest,model,input:r.deps.convertResponsesMessages(first.model,secondContext,new Set(['openai','openai-codex','opencode']),{includeSystemPrompt:false})};
 }
 const second=await r.run({options,context:secondContext,replacement});const stats=r.stats('session');r.close();results.push({id,config,options,replacement,first,second,stats,trace:r.trace});
}
// Canonical DTO inputs differ only in whether strict conversion appends a field.
for(const changed of [false,true]) {
 const options={sessionId:'session',transport:'websocket-cached'};
 const schema={properties:{payload:{type:'string'}},required:['payload'],type:'object'};
 const tool={name:'lookup',description:'Synthetic',parameters:schema,constrainedSampling:{type:'json_schema',strict:'require'}};
 const firstContext={...context,tools:[tool]};const r=runtime();
 const first=await r.run({options,context:firstContext});
 const secondContext={...firstContext,tools:[{...tool,parameters:changed?{additionalProperties:false,...schema}:schema}],messages:[...context.messages,first.result,{role:'user',content:'next',timestamp:8}]};
 const second=await r.run({options,context:secondContext});const stats=r.stats('session');r.close();
 assert.equal(stats.deltaRequests,changed?0:1);assert.equal(stats.fullContextRequests,changed?2:1);
 results.push({id:'strict-schema-'+(changed?'existing-slot':'same-slot'),options,first,second,stats,trace:r.trace});
}
const result={authority:authority(),cases:results};
if(process.argv.includes('--check'))assertCodexSourceEqual(JSON.parse(JSON.stringify(result)),JSON.parse(readFileSync(new URL('./cache.json',import.meta.url),'utf8')));else process.stdout.write(JSON.stringify(result,null,2)+'\n');
