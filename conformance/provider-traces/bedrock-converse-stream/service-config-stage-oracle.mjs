import {createRequire} from 'node:module';import {upstreamPath} from '../../upstream.mjs';
import {Readable} from 'node:stream';
import assert from 'node:assert/strict';
const require=createRequire(upstreamPath('package.json'));
const {BedrockRuntimeClient,ConverseStreamCommand}=require('@aws-sdk/client-bedrock-runtime');
const {HttpResponse}=require('@smithy/core/protocols');
const realTimeout=globalThis.setTimeout;let now=1767323045000;Date.now=()=>now;Math.random=()=>0.5;
globalThis.crypto.randomUUID=()=> '12345678-1234-4123-8123-123456789abc';
const rows=[];
for(const mode of ['default',...['region','authPreference','credentials','endpoint','fips','dual','appId','retryMode','maxAttempts'].map(x=>'fail:'+x),'bearer','token-failure','retry-three','retry-expiring','retry-expiry-exact','retry-skew']){
 const effects=[],requests=[],waits=[];let reads=0,sends=0;
 globalThis.setTimeout=(cb,ms,...args)=>{waits.push(ms);now+=ms;return realTimeout(cb,0,...args);};
 const leaf=(name,value)=>async()=>{effects.push(name);if(mode==='fail:'+name||(mode==='token-failure'&&name==='token'))throw Object.assign(new Error('configured '+name+' failed'),{name:'ConfiguredFailure',$metadata:{httpStatusCode:503}});return value;};
 const credential=async()=>{effects.push('credentials');reads++;if(mode==='fail:credentials')throw Object.assign(new Error('configured credentials failed'),{name:'ConfiguredFailure',$metadata:{httpStatusCode:503}});return {accessKeyId:'AKID_SYNTHETIC',secretAccessKey:'synthetic-secret',sessionToken:'synthetic-session',...(mode==='retry-expiring'||mode==='retry-expiry-exact'?{expiration:new Date(now+(mode==='retry-expiring'?299999:300000))}:{})};};
 const client=new BedrockRuntimeClient({region:leaf('region','us-east-1'),credentials:credential,token:leaf('token',{token:'synthetic-bearer'}),authSchemePreference:leaf('authPreference',[mode==='bearer'||mode==='token-failure'?'httpBearerAuth':'sigv4']),maxAttempts:leaf('maxAttempts',3),retryMode:leaf('retryMode','standard'),defaultsMode:'standard',useFipsEndpoint:leaf('fips',false),useDualstackEndpoint:leaf('dual',false),userAgentAppId:leaf('appId','synthetic'),defaultUserAgentProvider:async()=>[],credentialDefaultProvider:()=>{throw Error('discovery forbidden');},requestHandler:{metadata:{handlerProtocol:'h2'},async handle(request){sends++;effects.push('handle:'+sends);requests.push({date:request.headers['x-amz-date'],authorization:request.headers.authorization??request.headers.Authorization,attempt:request.headers['amz-sdk-request'],body:request.body});if(mode.startsWith('retry-')&&sends<3){return {response:new HttpResponse({statusCode:503,headers:{'content-type':'application/json',...(mode==='retry-skew'?{date:'Fri, 02 Jan 2026 04:04:05 GMT'}:{})},body:Readable.from(['{"message":"synthetic retry"}'])})};}return {response:new HttpResponse({statusCode:200,headers:{'content-type':'application/vnd.amazon.eventstream'},body:Readable.from([])})};}}});
 client.config.serviceConfiguredEndpoint=leaf('endpoint','https://synthetic.invalid/base');
 effects.push('constructed');assert.equal(reads,0);
 client.middlewareStack.add(next=>async args=>{effects.push('build');return next(args);},{step:'build',name:'source-build-probe',priority:'low'});
 effects.push('payload');let outcome,metadata;
 try{const response=await client.send(new ConverseStreamCommand({modelId:'synthetic',messages:[]}));effects.push('response');for await(const _ of response.stream){};outcome='ok';metadata=response.$metadata;}
 catch(e){outcome=e.name;metadata=e.$metadata;}
 client.destroy();rows.push({mode,effects,reads,sends,requests,waits,outcome,metadata});
 if(mode==='default')assert.equal(sends,1);if(mode.startsWith('fail:')||mode==='token-failure')assert.equal(sends,0,mode);
}
globalThis.setTimeout=realTimeout;
console.log(JSON.stringify({sdk:require('@aws-sdk/client-bedrock-runtime/package.json').version,clockStart:1767323045000,cases:rows},null,2));
