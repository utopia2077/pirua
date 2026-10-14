// Offline synthetic oracle: no installation, credentials or public endpoints.
import http from 'node:http';import {upstreamRoot, upstreamPath} from '../upstream.mjs';
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';
import { stripTypeScriptTypes } from 'node:module';
import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { strict as assert } from 'node:assert';
const require = createRequire(upstreamPath('package.json'));
const installed = require('undici');
const root = upstreamRoot;
const pin = '853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const source = execFileSync('git', ['show', `${pin}:packages/ai/src/utils/provider-retry.ts`], { cwd: root, encoding: 'utf8' });
const retry = await import(`data:text/javascript;base64,${Buffer.from(stripTypeScriptTypes(source)).toString('base64')}`);
const hash = value => createHash('sha256').update(value).digest('hex');
const bundles = process.binding('natives')['internal/deps/undici/undici'];
const results = { provenance: { node: process.versions.node, bundledUndici: process.versions.undici, installedUndici: require('undici/package.json').version, pi: pin,
  bundledSourceSha256: hash(bundles), installedFetchSha256: hash(readFileSync(`${root}/node_modules/undici/lib/web/fetch/index.js`)), installedUtilSha256: hash(readFileSync(`${root}/node_modules/undici/lib/web/fetch/util.js`)) }, policy: {}, runtimes: {} };
results.policy.maxSafeInitialSuccess = await retry.retryProviderRequest(async () => 'success', { maxRetries: Number.MAX_SAFE_INTEGER });
for (const status of [400, 503]) {
  let calls = 0;
  const headers = new Headers([['x-should-retry', status === 400 ? 'true' : 'false'], ['x-should-retry', status === 400 ? 'true' : 'false'], ['retry-after-ms', '0']]);
  try { await retry.retryProviderRequest(async () => { calls++; if (calls > 1) return 'success'; throw Object.assign(new Error('synthetic'), { status, headers }); }, { maxRetries: 1 }); } catch {}
  results.policy[`duplicate-${status}`] = { combined: headers.get('x-should-retry'), calls };
}
const watched = ['authorization', 'proxy-authorization', 'cookie', 'host', 'x-api-key', 'content-encoding', 'content-language', 'content-location', 'content-type', 'content-length'];
for (const [label, api] of [['node-global', { fetch: globalThis.fetch, Request, Headers }], ['npm-undici', installed]]) {
  const records = [];
  const rt = { headerGet: {}, responseHeaderGet: {}, methods: {}, validation: {}, sdkInitialFailures: [], cases: [] };
  for (const [family, SDK] of [['openai', require('openai').default], ['anthropic', require('@anthropic-ai/sdk').default]]) {
    for (const [label, baseURL] of [['bad-port','http://127.0.0.1:25'], ['credentials','http://synthetic:synthetic@127.0.0.1'], ['malformed','http://[']]) {
      let calls=0;let dispatches=0;
      const dispatcher={dispatch(){dispatches++;throw new Error('synthetic unexpected dispatch');}};
      const client=new SDK({apiKey:'synthetic',baseURL,maxRetries:0,fetch:(url,init)=>{calls++;return api.fetch(url,{...init,dispatcher});}});
      let result;
      try{await retry.retryProviderRequest(()=>client.post('/probe',{body:{probe:true}}),{maxRetries:1});result='success';}catch(error){result=error.constructor.name;}
      assert.equal(dispatches,0);
      rt.sdkInitialFailures.push({family,label,calls,dispatches,result});
    }
  }
  const joined = new api.Headers([['Cookie', 'a=1'], ['cookie', 'b=2'], ['Location', '/one'], ['LOCATION', '/two'], ['x-should-retry', 'true'], ['X-Should-Retry', 'true']]);
  for (const name of ['cookie', 'location', 'x-should-retry']) rt.headerGet[name] = joined.get(name);
  const responseHeaders = new api.Headers([['Cookie', 'a=1'], ['COOKIE', 'b=2'], ['Set-Cookie', 'a=1'], ['set-cookie', 'b=2']]);
  for (const name of ['CoOkIe', 'SET-cookie']) rt.responseHeaderGet[name] = responseHeaders.get(name);
  for (const method of ['post', 'pOsT', 'patch', 'pAtCh', 'query', 'qUeRy', 'TRACE', 'CONNECT', 'TRACK', 'BAD METHOD', '']) {
    try { rt.methods[method] = new api.Request('http://localhost/', { method }).method; } catch { rt.methods[method] = 'rejected'; }
  }
  for (const [name, url, init] of [['credentials', 'http://synthetic:synthetic@localhost/', {}], ['get-body', 'http://localhost/', {method:'GET',body:new Uint8Array([1])}], ['empty-get-body','http://localhost/',{method:'GET',body:new Uint8Array()}]]) {
    try { new api.Request(url,init); rt.validation[name] = 'accepted'; } catch { rt.validation[name] = 'rejected'; }
  }
  let config;
  const server = http.createServer(async (req,res) => {
    res.setHeader('Cookie', ['a=1', 'b=2']);res.setHeader('Set-Cookie', ['a=1', 'b=2']);
    const parts=[];for await (const part of req) parts.push(part);
    records.push({ target:'A', path:req.url, method:req.method, body:Buffer.concat(parts).toString(), hostOverride:req.headers.host==='synthetic.invalid', headers:watched.filter(name => req.headers[name] !== undefined) });
    if (config.chain) {if(records.length<=config.chain){res.writeHead(302,{Location:`/chain/${records.length}`});}res.end('ok');}
    else if (config.loop || req.url === '/start?old=1') { res.writeHead(config.status, config.location === null ? {} : {Location:config.location});res.end(); }
    else {res.end('ok');}
  });
  const second = http.createServer(async(req,res)=>{const parts=[];for await(const part of req)parts.push(part);records.push({target:'B',path:req.url,method:req.method,body:Buffer.concat(parts).toString(),hostOverride:req.headers.host==='synthetic.invalid',headers:watched.filter(name=>req.headers[name]!==undefined)});res.end('ok');});
  await new Promise(resolve=>server.listen(0,'127.0.0.1',resolve));await new Promise(resolve=>second.listen(0,'127.0.0.1',resolve));
  const base=`http://127.0.0.1:${server.address().port}`;const other=`http://127.0.0.1:${second.address().port}`;
  const cases=[];
  for(const status of [301,302,303,307,308])for(const method of ['POST','PUT','HEAD'])cases.push({name:`${status}-${method}`,status,method,location:'/final',body:method==='HEAD'?undefined:new Uint8Array(Buffer.from('payload'))});
  cases.push(...[
    {name:'twenty-hops',status:302,method:'GET',location:'/final',chain:20},
    {name:'twenty-one-hops',status:302,method:'GET',location:'/final',chain:21},
    {name:'cross-origin',status:307,method:'POST',location:`${other}/final`,body:new Uint8Array(Buffer.from('payload'))},
    {name:'relative',status:302,method:'GET',location:'../final?q=2'},
    {name:'query-fragment',status:302,method:'GET',location:'?new=2#next'},
    {name:'location-latin1',status:302,method:'GET',location:'/caf\xe9'},
    {name:'location-utf8',status:302,method:'GET',location:'/caf\xc3\xa9'},
    {name:'duplicate-location',status:302,method:'GET',location:['/one','/two']},
    {name:'missing-location',status:302,method:'GET',location:null},
    {name:'empty-location',status:302,method:'GET',location:'',loop:true},
    {name:'fragment-location',status:302,method:'GET',location:'#next',loop:true},
    {name:'same-credentials',status:302,method:'GET',location:base.replace('http://','http://synthetic:synthetic@')+'/final'},
    {name:'unsupported-scheme',status:302,method:'GET',location:'ftp://localhost/final'},
    {name:'bad-location',status:302,method:'GET',location:'http://['},
    {name:'cycle',status:307,method:'POST',location:'/start?old=1',loop:true,body:new Uint8Array(Buffer.from('payload'))}
  ]);
  for (const current of cases) {
    config=current;records.length=0;
    const headers={'authorization':'synthetic','proxy-authorization':'synthetic','cookie':'synthetic=1','x-api-key':'synthetic','host':'synthetic.invalid'};
    if(current.body){Object.assign(headers,{'content-encoding':'gzip','content-language':'en','content-location':'/body','content-type':'application/octet-stream','content-length':'7'});}
    let status='error';try{const response=await api.fetch(`${base}/start?old=1#original`,{method:current.method,headers,body:current.body});status=response.status;if(current.name==='missing-location'){for(const name of ['CoOkIe','SET-cookie'])assert.equal(response.headers.get(name),rt.responseHeaderGet[name]);}await response.arrayBuffer();}catch(error){if(error?.code==='ERR_ASSERTION')throw error;}
    const trace=[];for(const record of records){const key=JSON.stringify(record);if(trace.at(-1)?.key===key){trace.at(-1).repeats++;}else{trace.push({...record,key,repeats:1});}}
    rt.cases.push({name:current.name,status,requests:records.length,trace:trace.map(({key,...record})=>record)});
  }
  server.closeAllConnections();second.closeAllConnections();await Promise.all([new Promise(resolve=>server.close(resolve)),new Promise(resolve=>second.close(resolve))]);results.runtimes[label]=rt;
}
if (process.argv.includes('--check')) {
  assert.deepEqual(results, JSON.parse(readFileSync(new URL('./fetch-oracle.json', import.meta.url), 'utf8')));
  process.stdout.write('Pinned Node and installed Undici synthetic Fetch traces match.\n');
} else {
  process.stdout.write(JSON.stringify(results,null,2)+'\n');
}
