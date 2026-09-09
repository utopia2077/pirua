import {createRequire} from 'node:module';
import assert from 'node:assert/strict';
const require=createRequire('upstream/pi-mono/package.json');
const {EventStreamCodec,Int64,getChunkedStream}=require('@smithy/core/event-streams');
const {Crc32}=require('@aws-crypto/crc32');
assert.equal(require('@smithy/core/package.json').version,'3.24.3');
const codec=new EventStreamCodec(b=>Buffer.from(b).toString('utf8'),s=>Buffer.from(s,'utf8'));
const h=(type,value)=>({type,value});
const headers={':message-type':h('string','event'),':event-type':h('string','messageStart'),'true':h('boolean',true),'false':h('boolean',false),'byte':h('byte',-128),'short':h('short',-32768),'integer':h('integer',-2147483648),'long':h('long',new Int64(Uint8Array.from([128,0,0,0,0,0,0,1]))),'binary':h('binary',Uint8Array.from([0,128,255])),'string':h('string','é🙂'),'timestamp':h('timestamp',new Date('2026-01-02T03:04:05Z')),'uuid':h('uuid','12345678-1234-4321-abcd-123456789abc')};
const body=Buffer.from('{"role":"assistant"}');
const good=codec.encode({headers,body});
const describe=message=>({headers:Object.entries(message.headers).map(([name,v])=>[name,v.type,v.type==='long'?Buffer.from(v.value.bytes).toString('hex'):v.type==='binary'?Buffer.from(v.value).toString('base64'):v.type==='timestamp'?v.value.getTime():v.value]),body:Buffer.from(message.body).toString('base64')});
const fixCrc=bytes=>{const b=Buffer.from(bytes);b.writeUInt32BE(new Crc32().update(b.subarray(0,8)).digest(),8);b.writeUInt32BE(new Crc32().update(b.subarray(0,-4)).digest(),b.length-4);return b;};
const rawMessage=(raw,data=body)=>{const b=Buffer.alloc(16+raw.length+data.length);b.writeUInt32BE(b.length);b.writeUInt32BE(raw.length,4);Buffer.from(raw).copy(b,12);data.copy(b,12+raw.length);return fixCrc(b);};
const rows=[];const run=(id,b)=>{try{rows.push({id,wire:Buffer.from(b).toString('base64'),decoded:describe(codec.decode(Uint8Array.from(b)))});}catch(e){rows.push({id,wire:Buffer.from(b).toString('base64'),error:e.name,message:e.message});}};
run('all-header-types',good);
run('empty',codec.encode({headers:{},body:new Uint8Array()}));
const prelude=Buffer.from(good);prelude[8]^=1;run('prelude-crc',prelude);
const message=Buffer.from(good);message[message.length-1]^=1;run('message-crc',message);
const headerOver=Buffer.from(good);headerOver.writeUInt32BE(good.length,4);run('header-length-outside-message',fixCrc(headerOver));
run('unknown-header-type',rawMessage([1,120,10]));
run('duplicate-header-name',rawMessage([1,120,0,1,120,1]));
run('prototype-header-name',rawMessage([9,...Buffer.from('__proto__'),0]));
run('replacement-utf8-name-value',rawMessage([1,255,7,0,1,255]));
run('string-length-crosses-header-section',rawMessage([1,120,7,0,4]));
run('long-length-crosses-header-section',rawMessage([1,120,5]));
run('string-value-ends-at-message-crc',rawMessage([1,120,7,0,body.length+4]));
run('string-value-crosses-owned-message',rawMessage([1,120,7,0,body.length+5]));
run('binary-value-ends-at-message-crc',rawMessage([1,120,6,0,body.length+4]));
run('binary-value-crosses-owned-message',rawMessage([1,120,6,0,body.length+5]));
run('long-value-crosses-owned-message',rawMessage([1,120,5],Buffer.alloc(0)));
run('short-value-crosses-header-section',rawMessage([1,120,3]));
const chunks=[];
for(const [id,wire] of [['one-byte-prefix',good.slice(0,1)],['three-byte-prefix',good.slice(0,3)],['four-byte-prefix',good.slice(0,4)],['truncated-message',good.slice(0,-1)],['complete',good]]) {
 try{const decoded=[];for await(const msg of getChunkedStream((async function*(){yield wire;})()))decoded.push(describe(codec.decode(msg)));chunks.push({id,wire:Buffer.from(wire).toString('base64'),decoded});}
 catch(e){chunks.push({id,wire:Buffer.from(wire).toString('base64'),error:e.name,message:e.message});}
}
let splits=0;for(let i=0;i<=good.length;i++){const decoded=[];for await(const msg of getChunkedStream((async function*(){yield good.slice(0,i);yield good.slice(i);})()))decoded.push(describe(codec.decode(msg)));assert.deepEqual(decoded,[describe(codec.decode(good))]);splits++;}
console.log(JSON.stringify({smithy:'3.24.3',rows,chunks,splits},null,2));
