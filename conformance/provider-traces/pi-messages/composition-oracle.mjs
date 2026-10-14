import { execFileSync } from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import { stripTypeScriptTypes, createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { deepStrictEqual } from 'node:assert';
import { once } from 'node:events';
import http from 'node:http';
import { gzipSync } from 'node:zlib';
import vm from 'node:vm';

const upstream = upstreamRoot;
const commit = '853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const require = createRequire(`${upstream}/package.json`);
const hashes = {};
const source = path => {
  const text = execFileSync('git', ['show', `${commit}:packages/ai/src/${path}`], { cwd: upstream, encoding: 'utf8' });
  hashes[path] = createHash('sha256').update(text).digest('hex');
  return text;
};
const clean = text => stripTypeScriptTypes(text.replace(/^import[\s\S]*?;\n/gm, '').replace(/^export /gm, ''));
const clone = value => JSON.parse(JSON.stringify(value));
const rawHeaders = [['X-Trace', ' first '], ['x-trace', 'second'], ['Cookie', 'a=1'], ['cookie', 'b=2'], ['Set-Cookie', 'a=1'], ['set-cookie', 'b=2'], ['10', 'ten'], ['2', 'two'], ['__proto__', 'hidden'], ['constructor', 'ordinary']];
const usage = {input:2,output:1,cacheRead:0,cacheWrite:0,totalTokens:3,cost:{input:0,output:0,cacheRead:0,cacheWrite:0,total:0}};
const lines = values => values.map(value => `data: ${JSON.stringify(value)}\n\n`).join('');
const cases = [];
for (const family of ['pi-messages', 'mistral-conversations']) {
  for (const network of [false, true]) for (const mode of network ? ['success', 'error', 'cancel'] : ['success', 'error', 'hook-fail', 'hook-cancel']) {
    const token = new AbortController();
    const events = [], order = [], heads = [], payloads = [], requests = [];
    let reads = 0, resolveDone;
    const done = new Promise(resolve => { resolveDone = resolve; });
    const sandbox = vm.createContext({
      Date: {now:()=>7}, URL, Headers, TextDecoder, AbortSignal, Error,
      partialParse: require('partial-json').parse, getPiUserAgent: ()=>'pi-ai-rust',
      AssistantMessageEventStream: class {
        push(event) { events.push(clone(event)); if(mode==='cancel' && event.type==='text_delta') token.abort(); if(event.type==='done'||event.type==='error') resolveDone(); }
        end() { resolveDone(); }
      },
    });
    for (const path of ['utils/hash.ts','utils/json-parse.ts','utils/sanitize-unicode.ts','utils/diagnostics.ts','utils/headers.ts','utils/provider-env.ts','api/transform-messages.ts','utils/overflow.ts','utils/retry.ts']) vm.runInContext(clean(source(path)), sandbox);
    vm.runInContext(clean(source('models.ts').match(/^export function calculateCost[\s\S]*?^}/m)[0]), sandbox);
    vm.runInContext(clean(source(`api/${family}.ts`)), sandbox);
    const prefix = family==='pi-messages'
      ? lines([{type:'start'},{type:'text_start',contentIndex:0},{type:'text_delta',contentIndex:0,delta:'prefix'}])
      : lines([{id:'response-1',choices:[{delta:{content:'prefix'}}],usage:{prompt_tokens:2,completion_tokens:1,total_tokens:3}}]);
    const tail = family==='pi-messages'
      ? lines([{type:'text_end',contentIndex:0,content:'prefix'},{type:'done',reason:'stop',usage,responseId:'response-1'}])
      : lines([{choices:[{delta:{},finish_reason:'stop'}]}])+'data: [DONE]\n\n';
    const body = mode==='error' ? '{"error":{"message":"billing quota exhausted"}}' : prefix+(mode==='cancel'?'':tail);
    const encoded = gzipSync(body);
    const status = mode==='error'?429:200;
    const headers = [...rawHeaders,['Content-Type',mode==='error'?'application/json':'text/event-stream'],['Content-Encoding','gzip']];
    let server, baseUrl = 'https://example.invalid/base';
    const fetcher = network ? async (url, init) => {
      order.push('send'); return fetch(url,init);
    } : async (url,init) => {
      order.push('send'); requests.push({path:new URL(url).pathname,body:JSON.parse(init.body)});
      const response = new Response(new ReadableStream({pull(controller){reads++;controller.enqueue(new TextEncoder().encode(body));controller.close();}}, {highWaterMark:0}), {status,headers,statusText:'Synthetic'});
      return response;
    };
    if(network) {
      server = http.createServer(async (request,response) => {
        response.sendDate = false;
        const buffers=[];for await(const chunk of request)buffers.push(chunk);
        requests.push({path:request.url,body:JSON.parse(Buffer.concat(buffers).toString())});
        if(request.url!=='/final') {
          response.writeHead(307,['Location','/final','Content-Encoding','not-a-codec','Content-Length','0','Connection','close']);response.end();return;
        }
        const wireHeaders=[...headers,['Connection','close'],mode==='cancel'?['Transfer-Encoding','chunked']:['Content-Length',String(encoded.length)]];
        response.writeHead(status, 'Synthetic', wireHeaders.flat());
        // Split the physical body. No completion marker is sent for caller cancellation.
        for(let i=0;i<encoded.length;i+=3) response.write(encoded.subarray(i,i+3));
        if(mode!=='cancel')response.end();
      });
      server.listen(0,'127.0.0.1');await once(server,'listening');baseUrl=`http://127.0.0.1:${server.address().port}/base`;
    }
    const model={id:'composition-model',name:'Composition',api:family,provider:family==='pi-messages'?'pi':'mistral',baseUrl,reasoning:false,input:['text'],cost:{input:0,output:0,cacheRead:0,cacheWrite:0},contextWindow:4096,maxTokens:512};
    const context={messages:[]};
    const options={apiKey:'synthetic-key',maxRetries:3,timeoutMs:family==='pi-messages'?1:60000,maxTokens:8,signal:token.signal,fetch:fetcher,
      onPayload(payload){order.push('payload');payloads.push(clone(payload));},
      onResponse(head){order.push('response');heads.push({status:head.status,entries:Object.entries(head.headers)});if(mode==='hook-fail')throw new Error('synthetic hook rejection');if(mode==='hook-cancel'){token.abort();throw new Error('synthetic cancellation');}},
    };
    sandbox.model=model;sandbox.context=context;sandbox.options=options;
    try {
      vm.runInContext('stream(model,context,options)',sandbox);
      await done;
      const result=events.at(-1).message??events.at(-1).error;
      const classifiers=[sandbox.isContextOverflow(result),sandbox.isRetryableAssistantError(result)];
      for(const event of events) if(event.type==='error'){event.error.errorMessage='<safe>';delete event.error.diagnostics;}
      cases.push({family,network,mode,model:{...model,baseUrl:'https://example.invalid/base'},context,headers,status,body,encoded:encoded.toString('hex'),prefix,order,heads,payloads,requests,reads,events,classifiers});
    } finally {
      if(server){server.closeAllConnections();server.close();await once(server,'close');}
    }
  }
}
const result={sourceCommit:commit,node:process.version,undici:process.versions.undici,hashes,normalization:['timestamp=7','Mistral UA=pi-ai-rust','ephemeral base URL replaced','public error prose/diagnostics compared separately'],cases};
if(process.argv.includes('--check'))deepStrictEqual(result,JSON.parse(readFileSync(new URL('./composition.json',import.meta.url),'utf8')));
else process.stdout.write(JSON.stringify(result,null,2)+'\n');
