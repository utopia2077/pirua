import {execFileSync} from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import {stripTypeScriptTypes} from 'node:module';
import {readFileSync} from 'node:fs';
import {createHash} from 'node:crypto';
import assert from 'node:assert/strict';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185', hashes={};
const source=path=>{const text=execFileSync('git',['show',pin+':packages/ai/src/'+path],{cwd:upstreamRoot,encoding:'utf8'});hashes[path]=createHash('sha256').update(text).digest('hex');return text;};
const load=path=>{const text=source(path),names=[...text.matchAll(/export (?:function|const) (\w+)/g)].map(m=>m[1]);const js=stripTypeScriptTypes(text).replace(/^import[\s\S]*?;\s*$/gm,'').replace(/\bexport /g,'');return new Function(js+`\nreturn {${names.join(',')}};`)();};
const constants=load('api/cloudflare.ts');const {createGatewayBindingFetch,CLOUDFLARE_GATEWAY_BINDING_AUTH_SENTINEL}=load('api/cloudflare-gateway-binding.ts');
const base='https://gateway.ai.cloudflare.com/v1/account/gateway';
const cases=[
{id:'default'}, {id:'anthropic',path:'/anthropic/v1/messages'}, {id:'workers',path:'/workers-ai/v1/chat/completions'},
{id:'normalized',path:'/openai/../openai/v1/./responses?beta=true#ignored'},
{id:'encoded-dots',path:'/openai/%2e%2e/openai/responses'},
{id:'encoded-slash',path:'/openai/responses%2fpart?x=%2f&y=two#ignored'},
{id:'base-trailing',base:base+'/'}, {id:'base-query-fragment',base:base+'?x=ignored#ignored'},
{id:'origin-normalization',url:'https://GATEWAY.AI.CLOUDFLARE.COM:443/v1/account/gateway/openai/responses'},
{id:'empty-endpoint',path:'/openai/'}, {id:'empty-query',path:'/openai/responses?'},
{id:'outside',url:'https://elsewhere.invalid/v1/account/gateway/openai/responses'},
{id:'prefix-lookalike',url:base+'-other/openai/responses'}, {id:'dot-outside',path:'/../other/openai/responses'},
{id:'provider-only',path:'/openai'}, {id:'empty-provider',path:'//responses'},
{id:'invalid-url',url:'not a url'}, {id:'invalid-base',base:'not a url'},
{id:'get',method:'GET'}, {id:'lower-post',method:'post'}, {id:'put',method:'PUT'},
{id:'null',body:'null'}, {id:'boolean',body:'true'}, {id:'number',body:'9007199254740993'},
{id:'number-notation',body:'{"z":1e20,"negativeZero":-0,"round":9007199254740993}'},
{id:'array',body:'[1,null,"text"]'},
{id:'query-own-order',body:'{"z":1,"10":"ten","2":"two","01":"leading","4294967295":"nonindex","4294967294":"index","a":0}'},
{id:'nested-query-order',body:'{"z":{"10":1,"2":2,"a":3},"a":0}'}, {id:'duplicate-json',body:'{"z":1,"a":2,"z":3}'},
{id:'empty-body',body:''}, {id:'bad-json',body:'not JSON'}, {id:'trailing-json',body:'{} {}'},
{id:'text-bom',body:'\ufeffnull'}, {id:'bytes-bom',bytes:[239,187,191,110,117,108,108]},
{id:'bytes-replacement',bytes:[34,97,255,98,34]}, {id:'bytes-json',bytes:[123,125]},
{id:'record-alias',headers:[['x-alias','lower'],['X-Alias','upper']]},
{id:'record-reverse',headers:[['X-Alias','upper'],['x-alias','lower']]},
{id:'record-literal',headers:[['x-alias','lower, upper']]},
{id:'headers-alias',kind:'headers',headers:[['x-alias','lower'],['X-Alias','upper']]},
{id:'record-utf8',headers:[['X-Text','😀'],['x-space',' kept \t']]},
{id:'headers-utf8',kind:'headers',headers:[['x-text','😀']]},
{id:'headers-trim-cookie',kind:'headers',headers:[['x-space',' kept \t'],['set-cookie','a=1'],['set-cookie','b=2'],['cookie','a=1'],['cookie','b=2']]},
{id:'strip-only',headers:[['Host','ignored'],['Content-Length','100'],['CF-AIG-Authorization','Bearer cloudflare-gateway-binding'],['Authorization','Bearer synthetic-byok'],['x-api-key','synthetic-key'],['cf-aig-metadata','{"user":"42"}']]},
{id:'own-order',headers:[['z','first'],['2','two'],['01','leading'],['4294967295','nonindex'],['4294967294','index'],['z','last'],['__proto__','ignored'],['constructor','ordinary']]},
{id:'depth129',depth:129}, {id:'depth256',depth:256}, {id:'large',size:1048577},
{id:'response-redirect',responseStatus:302}, {id:'response-error',responseStatus:400},
];
const outputs=[];
for(const spec of cases){
 const order=[],controller=new AbortController();let captured;const response=new Response('already-decoded',{status:spec.responseStatus??200,headers:{'content-encoding':'gzip','location':'https://unused.invalid','x-metadata':'kept'}});
 const binding={gateway(id){order.push('gateway');assert.equal(id,'configured-name');return {async run(data,options){order.push('run');assert.equal(options.signal,controller.signal);captured=data;return response;}};}};
 let accepted=false,error,queryWire,query;
 try {
  const fetch=createGatewayBindingFetch({binding,baseUrl:spec.base??base,gateway:'configured-name'});
  const headers=spec.kind==='headers'?new Headers(spec.headers??[]):Object.fromEntries(spec.headers??[]);
  let body=spec.body??'{}';if(spec.bytes)body=new Uint8Array(spec.bytes);if(spec.depth)body='['.repeat(spec.depth)+'null'+']'.repeat(spec.depth);if(spec.size)body=JSON.stringify('x'.repeat(spec.size));
  const result=await fetch(spec.url??base+(spec.path??'/openai/responses'),{method:spec.method??'POST',headers,body,signal:controller.signal});assert.equal(result,response);assert.equal(await result.text(),'already-decoded');accepted=true;
  queryWire=JSON.stringify(captured.query);query=spec.depth||spec.size?null:captured.query;
 } catch(e){error=e instanceof Error?'Error':'other';}
 outputs.push({...spec,accepted,...(error?{error}:{}),order,...(captured?{provider:captured.provider,endpoint:captured.endpoint,headerEntries:Object.entries(captured.headers),queryNegativeZero:Object.is(captured.query?.negativeZero,-0),queryKeys:captured.query&&typeof captured.query==='object'&&!Array.isArray(captured.query)?Object.keys(captured.query):[],query,queryWire:queryWire.length<2048?queryWire:null,queryBytes:Buffer.byteLength(queryWire),querySha256:createHash('sha256').update(queryWire).digest('hex')}:{} )});
}
const output={pin,node:process.versions.node,hashes,constants,sentinel:CLOUDFLARE_GATEWAY_BINDING_AUTH_SENTINEL,base,cases:outputs};
if(process.argv.includes('--check'))assert.deepEqual(JSON.parse(JSON.stringify(output)),JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));else console.log(JSON.stringify(output,null,2));
