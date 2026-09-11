// Complete pinned selected OAuth modules, with explicit synthetic host services.
import {execFileSync} from 'node:child_process';
import {registerHooks,stripTypeScriptTypes} from 'node:module';
import {createHash} from 'node:crypto';
import {setImmediate as immediate} from 'node:timers/promises';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185', hashes={};
registerHooks({resolve(s,c,next){if(c.parentURL?.startsWith('pinned:')&&s==='node:crypto')return{url:'fixture:crypto',shortCircuit:true};if(s.startsWith('pinned:'))return{url:s,shortCircuit:true};if(c.parentURL?.startsWith('pinned:')&&s.startsWith('.'))return{url:'pinned:'+new URL(s,'file:///'+c.parentURL.slice(7)).pathname.slice(1),shortCircuit:true};return next(s,c);},load(u,c,next){
 if(u==='node:http')return{format:'module',source:'export const createServer=(fn)=>globalThis.__server(fn);',shortCircuit:true};
 if(u==='fixture:crypto')return{format:'module',source:'export const randomBytes=n=>Buffer.from(Array.from({length:n},(_,i)=>i));',shortCircuit:true};
 if(!u.startsWith('pinned:'))return next(u,c);const p=u.slice(7),s=execFileSync('/usr/bin/git',['-C','upstream/pi-mono','show',pin+':'+p],{encoding:'utf8',env:{PATH:'/usr/bin:/bin'}});hashes[p]=createHash('sha256').update(s).digest('hex');return{format:'module',source:stripTypeScriptTypes(s),shortCircuit:true};
}});
Object.defineProperty(globalThis,'crypto',{value:{getRandomValues:b=>{for(let i=0;i<b.length;i++)b[i]=i;return b;},subtle:{async digest(algorithm,bytes){if(algorithm!=='SHA-256')throw Error('unexpected digest');return Uint8Array.from(createHash('sha256').update(bytes).digest()).buffer;}},randomUUID:()=> '00010203-0405-4607-8809-0a0b0c0d0e0f'}});
const {anthropicOAuth}=await import('pinned:packages/ai/src/auth/oauth/anthropic.ts');
const {openaiCodexOAuth}=await import('pinned:packages/ai/src/auth/oauth/openai-codex.ts');
const {openRouterOAuth}=await import('pinned:packages/ai/src/auth/oauth/openrouter.ts');
await immediate();
const jwt='e30.'+Buffer.from(JSON.stringify({'https://api.openai.com/auth':{chatgpt_account_id:'synthetic-account'}})).toString('base64')+'.synthetic';
const token={access_token:jwt,refresh_token:'synthetic-refresh',expires_in:3600};
const scenarios=[
 {id:'anthropic-manual',provider:'anthropic',manual:'synthetic-code'},
 {id:'anthropic-callback',provider:'anthropic',callback:true,invalid:true},
 {id:'anthropic-bind-failure',provider:'anthropic',bindFailure:true},
 {id:'anthropic-state-mismatch',provider:'anthropic',manual:'synthetic-code#wrong-state'},
 {id:'anthropic-missing-code',provider:'anthropic',manual:''},
 {id:'anthropic-refresh',provider:'anthropic',refresh:true},
 {id:'anthropic-http-error',provider:'anthropic',refresh:true,status:400},
 {id:'codex-manual',provider:'codex',manual:'synthetic-code'},
 {id:'codex-callback',provider:'codex',callback:true,invalid:true},
 {id:'codex-bind-fallback',provider:'codex',bindFailure:true,manual:'synthetic-code'},
 {id:'codex-device',provider:'codex',device:true,polls:[403,404,'slow_down','pending',200]},
 {id:'codex-device-fractional',provider:'codex',device:true,interval:'0.5',polls:[200]},
 {id:'codex-device-timeout',provider:'codex',device:true,interval:900,polls:[403]},
 {id:'codex-device-slow-timeout',provider:'codex',device:true,interval:900,polls:['slow_down']},
 {id:'codex-refresh',provider:'codex',refresh:true},
 {id:'openrouter-manual',provider:'openrouter',manual:'synthetic-code'},
 {id:'openrouter-callback-claimed',provider:'openrouter',callback:true,claimedManual:true,invalid:true},
 {id:'openrouter-callback-manual-error',provider:'openrouter',callback:true,claimedManual:true,manualError:true},
 {id:'openrouter-denied',provider:'openrouter',denied:true},
 {id:'openrouter-timeout',provider:'openrouter'},
 {id:'openrouter-exchange-timeout',provider:'openrouter',manual:'synthetic-code',hold:true},
 {id:'codex-unknown-method',provider:'codex',method:'synthetic-unknown'},
 {id:'codex-device-disabled',provider:'codex',device:true,status:404},
 {id:'codex-device-denied',provider:'codex',device:true,polls:[500]},
 {id:'codex-invalid-jwt',provider:'codex',refresh:true,access:'synthetic-invalid'},
 {id:'anthropic-fractional-seconds',provider:'anthropic',refresh:true,expires:0.5},
 {id:'admission-anthropic-fractional-ms',provider:'anthropic',refresh:true,expires:0.0005},
 {id:'admission-anthropic-negative-expiry',provider:'anthropic',refresh:true,expires:0,now:0},
 {id:'openrouter-empty-key',provider:'openrouter',manual:'synthetic-code',key:''},
 {id:'openrouter-callback-http-error',provider:'openrouter',callback:true,status:403},
 {id:'anthropic-manual-empty-state',provider:'anthropic',manual:'code=synthetic-code&state='},
 {id:'anthropic-manual-url',provider:'anthropic',manual:'http://localhost:53692/callback?code=synthetic-code'},
 {id:'codex-manual-empty-state',provider:'codex',manual:'code=synthetic-code&state='},
 {id:'codex-device-hex-interval',provider:'codex',device:true,interval:'0x2',polls:['nested_pending',200]},
 {id:'codex-device-completes-after-deadline',provider:'codex',device:true,polls:[200],pollAdvance:1000000},
 {id:'codex-http-body-failure',provider:'codex',refresh:true,status:400,readError:true},
 {id:'openrouter-http-body-failure',provider:'openrouter',manual:'synthetic-code',status:400,readError:true},
 {id:'openrouter-invalid-json',provider:'openrouter',callback:true,invalidJson:true},
 {id:'openrouter-error-detail',provider:'openrouter',callback:true,status:400,errorDescription:'synthetic-private-detail <&>'},
 {id:'openrouter-manual-hash',provider:'openrouter',manual:'synthetic-code#synthetic-state'},
 {id:'openrouter-manual-rejected',provider:'openrouter',manualRejectBefore:true},
 {id:'anthropic-callback-error-routes',provider:'anthropic',callback:true,extraError:true,malformedTarget:true,callbackMethod:'POST'},
 {id:'codex-callback-error-routes',provider:'codex',callback:true,extraError:true,malformedTarget:true},
 {id:'openrouter-invalid-method',provider:'openrouter',callback:true,invalidMethod:true},
 {id:'openrouter-invalid-route-timeout',provider:'openrouter',invalidOnly:true,holdReply:true},
 {id:'openrouter-empty-error-detail',provider:'openrouter',callback:true,status:400,errorDescription:'',errorMessage:'synthetic-secondary-detail'},
 {id:'openrouter-refresh',provider:'openrouter',refresh:true},
 {id:'openrouter-callback-body-read-failure',provider:'openrouter',callback:true,readError:true},
 {id:'openrouter-callback-exchange-timeout',provider:'openrouter',callback:true,hold:true},
];
const observations=[];
const credentialDigests={};
for(const scenario of scenarios){
 let now=scenario.now??1700000000000,serial=0,timers=new Map(),trace=[],sleeps=[],server,manualResolve,manualReject,codePoll=0,callbackPath;
 Date.now=()=>now;
 globalThis.setTimeout=(fn,ms)=>{const id=++serial;timers.set(id,{fn,ms,at:now+ms});if(scenario.device)sleeps.push(ms);return id;};globalThis.clearTimeout=id=>timers.delete(id);
 Object.defineProperty(AbortSignal,'timeout',{configurable:true,value:ms=>{const c=new AbortController();setTimeout(()=>c.abort(Error('synthetic timeout')),ms);return c.signal;}});
 globalThis.__server=handler=>{const listeners=new Map();server={handler,listening:false,on(k,fn){listeners.set(k,fn);return this;},once(k,fn){listeners.set(k,fn);return this;},removeListener(k){listeners.delete(k);return this;},listen(port,host,fn){trace.push(['bind',host,port]);this.port=port||40000;queueMicrotask(()=>{if(scenario.bindFailure)listeners.get('error')?.(Error('synthetic bind failure'));else{this.listening=true;fn();}});return this;},address(){return{port:this.port};},close(){if(this.listening){trace.push(['close']);this.listening=false;}}};return server;};
 const request=(target,method='GET')=>{const response={statusCode:200,headers:{},writeHead(n,h){this.statusCode=n;for(const[k,v]of Object.entries(h??{}))this.headers[k.toLowerCase()]=v;},setHeader(k,v){this.headers[k.toLowerCase()]=v;},end(body){trace.push(['reply',this.statusCode,String(body),this.headers]);}};server.handler({url:target,method},response);};
 globalThis.fetch=async(url,init)=>{
  trace.push(['fetch',url,Object.entries(init.headers),String(init.body)]);
  if(scenario.claimedManual){if(scenario.manualError)manualReject?.(Error('synthetic manual rejection'));else manualResolve?.('synthetic-later-manual');request(callbackPath+'?code=synthetic-duplicate');}
  if(scenario.hold)return new Promise((_,reject)=>init.signal.addEventListener('abort',()=>reject(Error('synthetic timeout')),{once:true}));
  let status=scenario.status||200,body=scenario.provider==='openrouter'?(Object.hasOwn(scenario,'errorDescription')?{error_description:scenario.errorDescription,...(scenario.errorMessage?{message:scenario.errorMessage}:{})}:{key:scenario.key??'synthetic-key'}):{...token,access_token:scenario.access??token.access_token,expires_in:scenario.expires??token.expires_in};
  if(url.endsWith('/usercode'))body={device_auth_id:'synthetic-device',user_code:'synthetic-user',interval:scenario.interval??0};
  if(url.endsWith('/deviceauth/token')){const next=scenario.polls[Math.min(codePoll++,scenario.polls.length-1)];status=typeof next==='number'?next:400;body=next===200?{authorization_code:'synthetic-device-code',code_verifier:'synthetic-device-verifier'}:{error:next==='pending'?'deviceauth_authorization_pending':next==='nested_pending'?{code:'deviceauth_authorization_pending'}:next};}
  return{status,ok:status>=200&&status<300,statusText:'Synthetic',async json(){trace.push(['read',url]);if(url.endsWith('/deviceauth/token'))now+=scenario.pollAdvance??0;if(scenario.invalidJson||scenario.readError)throw Error('synthetic body failure');return body;},async text(){trace.push(['read',url]);if(url.endsWith('/deviceauth/token'))now+=scenario.pollAdvance??0;if(scenario.readError)throw Error('synthetic body failure');return scenario.invalidJson?'{':JSON.stringify(body);}};
 };
 const interaction={signal:new AbortController().signal,prompt(p){const {signal,...wire}=p;trace.push(['prompt',wire]);if(p.type==='select')return Promise.resolve(scenario.method??(scenario.device?'device_code':'browser'));signal?.addEventListener('abort',()=>trace.push(['manual-cancel']),{once:true});if(scenario.manualRejectBefore)return Promise.reject(Error('synthetic manual rejection'));if(Object.hasOwn(scenario,'manual'))return Promise.resolve(scenario.manual);return new Promise((resolve,reject)=>{manualResolve=resolve;manualReject=reject;});},notify(e){trace.push(['notify',e]);if(e.type==='auth_url'&&(scenario.callback||scenario.denied||scenario.invalidOnly)){queueMicrotask(()=>{const u=new URL(e.url),redirect=u.searchParams.get(scenario.provider==='openrouter'?'callback_url':'redirect_uri'),target=new URL(redirect);callbackPath=target.pathname;if(scenario.invalidOnly){request('/wrong-route');return;}if(scenario.invalid){request('/not-callback');request(target.pathname+(scenario.provider==='openrouter'?'?state=wrong':'?code=synthetic-code&state=wrong'));}if(scenario.extraError)request(target.pathname+'?error=synthetic-denied');if(scenario.malformedTarget)request('http://[');if(scenario.invalidMethod)request(target.pathname+'?code=synthetic-wrong','POST');if(scenario.denied){request(target.pathname+'?error=denied&error_description=Denied%20%3Cb%3E%26');return;}target.searchParams.set('code','synthetic-callback-code');if(scenario.provider!=='openrouter')target.searchParams.set('state',u.searchParams.get('state'));request(target.pathname+target.search,scenario.callbackMethod??'GET');});}}};
 const flow={anthropic:anthropicOAuth,codex:openaiCodexOAuth,openrouter:openRouterOAuth}[scenario.provider];
 let done=false,result,error;
 const credential={type:'oauth',access:'synthetic-old',refresh:'synthetic-refresh',expires:0,extra:'synthetic-extra'};
 const pending=scenario.refresh?flow.refresh(credential,interaction.signal):flow.login(interaction);
 pending.then(async v=>{credentialDigests[scenario.id]={access:createHash('sha256').update(v.access).digest('hex'),refresh:createHash('sha256').update(v.refresh).digest('hex')};const auth=await flow.toAuth(v);result={authMatchesAccess:auth.apiKey===v.access,type:v.type,expires:v.expires,refreshEmpty:v.refresh==='',accountId:v.accountId??null,extra:v.extra??null};done=true;},e=>{error=String(e.message);done=true;});
 for(let turn=0;!done&&turn<80;turn++){await immediate();if(!done&&timers.size){const [id,timer]=[...timers].sort((a,b)=>a[1].at-b[1].at)[0];timers.delete(id);now=timer.at;timer.fn();}}
 if(!done)throw Error('Unsettled source scenario '+scenario.id);
 observations.push({scenario,id:scenario.id,result:result??null,error:error??null,sleeps,trace});
}
console.log(JSON.stringify({pin,node:process.versions.node,hashes,observations,credentialDigests},null,2));
