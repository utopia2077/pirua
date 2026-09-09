import {execFileSync} from 'node:child_process';import {stripTypeScriptTypes} from 'node:module';import {createHash} from 'node:crypto';import {readFileSync} from 'node:fs';import assert from 'node:assert/strict';
const pin='853a80d26c90a14c1886f0ebb8ffaae133ca2185',path='packages/ai/src/api/constrained-sampling.ts';
const source=execFileSync('git',['show',pin+':'+path],{cwd:'upstream/pi-mono',encoding:'utf8'});
const js=stripTypeScriptTypes(source).replace(/^import[\s\S]*?;\s*$/gm,'').replace(/\bexport /g,'');
const {makeStrictJsonSchema}=new Function(js+'\nreturn {makeStrictJsonSchema};')();
const canonical=v=>Array.isArray(v)?'['+v.map(canonical).join(',')+']':v&&typeof v==='object'?'{'+Object.keys(v).sort((a,b)=>Buffer.compare(Buffer.from(a),Buffer.from(b))).map(k=>JSON.stringify(k)+':'+canonical(v[k])).join(',')+'}':JSON.stringify(v);
const cases=[
 ['append-additional',{properties:{x:{type:'string'}},required:['x'],type:'object'}],
 ['existing-additional',{additionalProperties:false,properties:{x:{type:'string'}},required:['x'],type:'object'}],
 ['append-both',{properties:{x:{type:'string'}},type:'object',zzz:7}],
 ['empty-object',{type:'object',zzz:'tail'}],
 ['empty-properties',{properties:{},type:'object',zzz:'tail'}],
 ['nested',{properties:{list:{items:{properties:{z:{type:'string'}},type:'object'},type:'array'},object:{additionalProperties:false,properties:{optional:{description:'d',type:'number'}},type:'object'},union:{anyOf:[{type:'integer'},{type:'null'}]}},type:'object'}],
 ['numeric-names',{properties:{'10':{type:'string'},'2':{type:'number'},b:{type:'boolean'},a:{type:'string'}},required:['b','2'],type:'object'}],
 ['nullable-variants',{properties:{a:{type:['string','null']},b:{const:null},c:{enum:[null,1]},d:{anyOf:[{type:'string'},{type:'null'}]},e:{type:'boolean'}},type:'object'}],
 ['metadata-data',{default:{properties:{bad:true},type:'object',additionalProperties:true},examples:[{items:false}],properties:{x:{default:{$ref:'not-active'},type:'string'}},type:'object'}],
 ['existing-slots-nested',{additionalProperties:false,properties:{x:{additionalProperties:false,properties:{a:{type:'string'}},required:[],type:'object',zzz:9}},required:[],type:'object',zzz:1}],
];
const observations=cases.map(([id,value])=>{const inputJson=canonical(value),input=JSON.parse(inputJson);const before=JSON.stringify(input);const output=makeStrictJsonSchema(input);assert.equal(JSON.stringify(input),before);return{id,inputJson,input,expected:output,expectedJson:JSON.stringify(output)};});
assert.notEqual(observations[0].expectedJson,observations[1].expectedJson);assert.deepEqual(observations[0].expected,observations[1].expected);
const result={pin,path,sha256:createHash('sha256').update(source).digest('hex'),evidence:'Complete pinned helper on canonical DTO inputs; no helper reimplementation in source oracle.',cases:observations};
if(process.argv.includes('--check'))assert.deepEqual(result,JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));else process.stdout.write(JSON.stringify(result,null,2)+'\n');
