import {execFileSync} from 'node:child_process';
import {readFileSync} from 'node:fs';
import {deepStrictEqual, strictEqual} from 'node:assert';
import {zstdDecompressSync} from 'node:zlib';

const root=new URL('../../../',import.meta.url);
const build=execFileSync('cargo',['test','-p','pi-ai-transport','--test','request_encoding','--locked','--offline','--no-run','--message-format=json'],{cwd:root,encoding:'utf8',maxBuffer:8*1024*1024});
const artifact=build.split('\n').filter(Boolean).map(JSON.parse).find(x=>x.reason==='compiler-artifact'&&x.target.name==='request_encoding'&&x.executable);
if(!artifact)throw new Error('request-encoding test artifact missing');
const result=execFileSync(artifact.executable,['--exact','pinned_level_three_frames_and_cross_decode_vectors','--nocapture'],{encoding:'utf8',maxBuffer:8*1024*1024});
const vectors=JSON.parse(result.split('\n').find(s=>s.startsWith('ZSTD_CROSS_DECODE=')).slice('ZSTD_CROSS_DECODE='.length));
const source=JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8'));
strictEqual(vectors.length,source.cases.length);
for(const vector of vectors){const spec=source.cases.find(c=>c.id===vector.id);const text=spec.count?Array.from({length:spec.count},(_,i)=>JSON.stringify({id:i,text:'the repeated long prefix item '+i%17,group:i%13})+'\n').join(''):spec.length?spec.text.repeat(Math.ceil(spec.length/spec.text.length)).slice(0,spec.length):spec.text;deepStrictEqual(zstdDecompressSync(Buffer.from(vector.encoded,'hex')),Buffer.from(text));}
console.log('Node cross-decoded all '+vectors.length+' Rust-produced frames against source input bytes');
