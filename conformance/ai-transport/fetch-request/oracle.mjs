import assert from 'node:assert/strict';
import {readFileSync} from 'node:fs';
assert.equal(process.versions.node,'26.8.1');
const cases=[
{id:'record-forward',headers:[['x-alias','lower'],['X-Alias','upper']],text:'{}'},
{id:'record-reverse',headers:[['X-Alias','upper'],['x-alias','lower']],text:'{}'},
{id:'literal',headers:[['x-alias','lower, upper']],text:'{}'},
{id:'text-default',headers:[],text:'text'}, {id:'bytes-default',headers:[],bytes:[116,101,120,116]},
{id:'text-bom',headers:[],text:'\ufeffnull'}, {id:'bytes-bom',headers:[],bytes:[239,187,191,110,117,108,108]},
{id:'text-empty',headers:[],text:''}, {id:'bytes-empty',headers:[],bytes:[]},
{id:'trim',headers:[['x-space',' \r\nkept \t\r\n']],text:'{}'},
{id:'unicode',headers:[['x-text','😀']],text:'{}'},
{id:'bad-name',headers:[['bad name','x']],text:'{}'},
{id:'interior-newline',headers:[['x-text','one\ntwo']],text:'{}'},
{id:'get-text',method:'GET',headers:[],text:''},
{id:'numeric-order',headers:[['z','last'],['2','two'],['01','leading'],['10','ten']],text:'{}'},
];
const output={node:process.versions.node,cases:[]};
for(const spec of cases){let accepted=false,headers,body;try{const request=new Request('https://example.invalid/path',{method:spec.method??'POST',headers:Object.fromEntries(spec.headers),body:spec.bytes?new Uint8Array(spec.bytes):spec.text});headers=Object.fromEntries(request.headers);body=[...new Uint8Array(await request.arrayBuffer())];accepted=true;}catch{}output.cases.push({...spec,accepted,...(accepted?{selectedHeaders:headers,selectedBody:body}:{})});}
if(process.argv.includes('--check'))assert.deepEqual(output,JSON.parse(readFileSync(new URL('./trace.json',import.meta.url),'utf8')));else console.log(JSON.stringify(output,null,2));
