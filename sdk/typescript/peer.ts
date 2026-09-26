/** Bounded protocol-only tool echo reference peer; no public SDK surface. */
import { readFileSync, readSync, writeSync } from 'node:fs';

const root = new URL('../../', import.meta.url);
const fixture = JSON.parse(readFileSync(new URL('conformance/extension-protocol/echo-v1.json', root), 'utf8'));
const digests = JSON.parse(readFileSync(new URL('digests.json', import.meta.url), 'utf8'));
const args = process.argv.slice(2);
function option(name: string): string { const p=args.indexOf(name); if(p<0 || !args[p+1]) throw Error(name); return args[p+1]; }
const caseId=option('--case'), fault=option('--fault');
const pair=fixture.version_pairs.find((x: any)=>x.id===caseId.split('/')[0]);
const item=fixture.inputs.find((x: any)=>x.id===caseId.split('/')[1]);
if(!pair || !item || !pair.inputs.includes(item.id) || !['normal','manifest_profile_digest_wrong','manifest_dynamic_digest_wrong','terminal_projection_wrong','terminal_digest_wrong','duplicate_terminal'].includes(fault)) throw Error('case/fault');
const core=pair.core_minor, tool=pair.tool_minor;
const bytes=(value: string)=>Buffer.from(value,'utf8');
const hex=(value: string)=>Buffer.from(value,'hex');
function expect(ok: unknown, why: string): asserts ok { if(!ok) throw Error(why); }
function v(n: number): Buffer { expect(Number.isSafeInteger(n)&&n>=0,'integer'); const a=[]; do { let b=n%128; n=Math.floor(n/128); if(n) b|=128; a.push(b); } while(n); return Buffer.from(a); }
function field(n: number, data: Buffer): Buffer { return Buffer.concat([v(n*8+2),v(data.length),data]); }
function num(n: number, value: number): Buffer { return Buffer.concat([v(n*8),v(value)]); }
function str(n: number, value: string): Buffer { return field(n,bytes(value)); }
function msg(...parts: Buffer[]): Buffer { return Buffer.concat(parts); }
function parse(raw: Buffer): Map<number, Buffer[]> {
  const out=new Map<number,Buffer[]>(); let i=0;
  function takeVar(): number { let x=0, shift=0, count=0; while(true) { expect(i<raw.length&&count<10,'truncated varint'); const b=raw[i++]; x+=(b&127)*2**shift; count++; if(b<128) { expect(v(x).length===count,'nonminimal varint'); return x; } shift+=7; } }
  while(i<raw.length) { const tag=takeVar(), n=Math.floor(tag/8), wire=tag%8; expect(n>0,'zero tag'); let value: Buffer;
    if(wire===0) { const start=i; takeVar(); value=raw.subarray(start,i); }
    else if(wire===2) { const len=takeVar(); expect(len<=8192&&i+len<=raw.length,'length'); value=raw.subarray(i,i+len); i+=len; }
    else throw Error('wire type'); const list=out.get(n)??[]; list.push(value); out.set(n,list); }
  return out;
}
function one(map: Map<number,Buffer[]>, n: number): Buffer { const values=map.get(n); expect(values?.length===1,`field ${n}`); return values[0]; }
function optional(map: Map<number,Buffer[]>, n: number): Buffer|undefined { const values=map.get(n); expect(!values||values.length===1,`field ${n}`); return values?.[0]; }
function integer(raw: Buffer): number { const x=parseVar(raw); return x; }
function parseVar(raw: Buffer): number { let x=0,s=0; for(let i=0;i<raw.length;i++) { x+=(raw[i]&127)*2**s; if(raw[i]<128) { expect(i===raw.length-1,'extra varint'); return x; } s+=7; } throw Error('varint'); }
function text(raw: Buffer): string { return raw.toString('utf8'); }
function readExact(n: number): Buffer { expect(n<=8192,'frame cap'); const out=Buffer.alloc(n); let at=0; while(at<n) { const got=readSync(0,out,at,n-at,null); expect(got>0,'EOF'); at+=got; } return out; }
function receive(arm: number): Map<number,Buffer[]> { const prefix: number[]=[]; while(prefix.length<10) { const b=readExact(1)[0]; prefix.push(b); if(b<128) break; } expect(prefix.length<10,'prefix cap'); const len=parseVar(Buffer.from(prefix)); expect(len>0&&len<=8192,'frame length'); const env=parse(readExact(len)); expect(env.size===1,'envelope'); return parse(one(env,arm)); }
function send(arm: number, payload: Buffer): Buffer { const body=field(arm,payload); const raw=msg(v(body.length),body); expect(raw.length<=8192,'outbound cap'); writeSync(1,raw); return raw; }
function profile(minor: number): Buffer { return hex(digests.profile[`1.${minor}`]); }
function schema(minor: number, slot: string): Buffer { return hex(digests.schema[`1.${minor}`][slot]); }
function altered(raw: Buffer): Buffer { const out=Buffer.from(raw); out[0]^=1; return out; }
function descriptor(minor: number): Buffer { return msg(num(1,minor),field(2,(fault==='manifest_profile_digest_wrong'&&minor===tool)?altered(profile(minor)):profile(minor))); }
function projection(minor: number, slot: string, id: string): Buffer { let digest=schema(minor,slot); if(fault==='manifest_dynamic_digest_wrong'&&minor===tool&&slot==='raw_result') digest=altered(digest); return msg(str(1,id),field(2,digest)); }
function schemaSet(minor: number): Buffer { return msg(str(1,'echo'),num(2,minor),field(3,projection(minor,'arguments','tool.arguments:echo')),field(4,projection(minor,'progress','tool.progress:echo')),field(5,projection(minor,'raw_result','tool.result:echo'))); }
function range(name: string): Buffer { return msg(str(1,name),num(2,1),num(3,0),num(4,1)); }
function manifest(): Buffer { const cap=msg(str(1,'tool'),str(2,'echo'),field(3,range('tool.v1')),str(4,'echo'),num(6,1),num(7,2),num(8,1),field(9,descriptor(0)),field(9,descriptor(1)),...(core===1?[str(10,'Echo')]:[]),field(11,schemaSet(0)),field(11,schemaSet(1)));
  return msg(str(1,'peer-a'),str(2,'0.1'),field(3,msg(num(1,1),num(2,0),num(3,1))),field(4,cap)); }
function checkKey(raw: Buffer): void { const k=parse(raw); const names=['session_id','lane','operation_id','effect_kind','step_or_task_id']; names.forEach((name,n)=>expect(text(one(k,n+1))===item.effect_key[name],name)); expect((optional(k,6)?integer(one(k,6)):0)===item.effect_key.logical_attempt,'attempt'); expect(integer(one(k,7))===item.effect_key.item_index,'index'); }
function checkRef(raw: Buffer, granted: Buffer): void { expect(raw.equals(granted),'grant ref'); const r=parse(raw); expect(text(one(r,1))==='peer-a'&&text(one(r,2))==='tool'&&text(one(r,3))==='echo','ref identity'); const p=parse(one(r,4)); expect(text(one(p,1))==='tool.v1'&&integer(one(p,2))===1&&(optional(p,3)?integer(one(p,3)):0)===tool,'ref version'); expect(one(r,5).equals(profile(tool)),'ref digest'); }
function checkRequest(grant: Map<number,Buffer[]>): string { const r=receive(6); expect(integer(one(r,1))===item.request_id,'request id'); expect(!r.has(2)&&!r.has(7),'parent/credit'); expect(text(one(r,5))==='tool.invoke','method'); expect(integer(one(r,6))>0,'deadline'); checkKey(one(r,4)); checkRef(one(r,3),one(grant,3));
  const inner=parse(one(r,8)); expect(text(one(inner,1))==='echo'&&text(one(inner,3))===item.tool_call_id,'inner'); const arg=parse(one(inner,2)); expect(text(one(arg,1))==='tool.arguments:echo'&&one(arg,2).equals(schema(tool,'arguments')),'argument projection'); const wire=parse(one(arg,3)); const entries=parse(one(wire,8)).get(1); expect(entries?.length===1,'argument map'); const e=parse(entries[0]); expect(text(one(e,1))==='text','argument key'); const value=parse(one(e,2)); const actual=text(one(value,5)); expect(actual===item.text,'argument text'); return actual; }
function result(textValue: string): Buffer { const itemMap=field(1,msg(str(1,'text'),field(2,str(5,textValue)))); const typeMap=field(1,msg(str(1,'type'),field(2,str(5,'text')))); const contentValue=field(7,msg(field(1,field(8,msg(itemMap,typeMap))))); const content=field(1,msg(str(1,'content'),field(2,contentValue)));
  const details=field(1,msg(str(1,'details'),field(2,field(8,Buffer.alloc(0))))); const value=field(8,msg(content,details));
  let digest=schema(tool,'raw_result'); if(fault==='terminal_digest_wrong') digest=altered(digest);
  const bound=msg(str(1,fault==='terminal_projection_wrong'?'tool.progress:echo':'tool.result:echo'),field(2,digest),field(3,value)); return field(1,bound); }
function main(): void {
  send(1,msg(field(1,msg(num(1,1),num(2,0),num(3,1))),field(2,range('tool.v1'))));
  const ack=receive(2), version=parse(one(ack,1)); expect(integer(one(version,1))===1&&(optional(version,2)?integer(one(version,2)):0)===core,'ack');
  send(3,manifest()); if(fault.startsWith('manifest_')) return;
  const grant=receive(4); expect(text(one(grant,1))==='peer-a','grant principal'); const selected=parse(one(grant,2)); expect(integer(one(selected,1))===1&&(optional(selected,2)?integer(one(selected,2)):0)===core,'grant core'); expect(integer(one(grant,6))===item.generation,'generation'); expect(grant.get(3)?.length===1,'granted tool');
  send(5,Buffer.alloc(0)); const actual=checkRequest(grant); expect(actual===item.expected_terminal_text,'causal text'); const end=msg(num(1,item.request_id),field(2,result(actual))); const first=send(9,end); if(fault==='duplicate_terminal') writeSync(1,first);
}
try { main(); } catch(error) { process.stderr.write(`peer failure: ${error}\n`); process.exitCode=2; }
