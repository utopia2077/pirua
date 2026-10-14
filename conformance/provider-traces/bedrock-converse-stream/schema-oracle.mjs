import {createRequire} from 'node:module';import {upstreamPath} from '../../upstream.mjs';
const require=createRequire(upstreamPath('package.json'));
const {NormalizedSchema}=require('@smithy/core/schema');
const schemas=require(upstreamPath('node_modules/@aws-sdk/client-bedrock-runtime/dist-cjs/schemas/schemas_0.js'));
const graph={};
function visit(input){const s=NormalizedSchema.of(input);const raw=s.getSchema();const name=typeof raw==='number'?'scalar_'+raw:s.getName();if(graph[name])return name;const row={};graph[name]=row;
 if(s.isStructSchema()){row.kind='struct';row.union=s.isUnionSchema();row.fields=[...s.structIterator()].map(([k,v])=>[k,visit(v)]);}
 else if(s.isListSchema()){row.kind='list';row.sparse=!!s.getMergedTraits().sparse;row.value=visit(s.getValueSchema());}
 else if(s.isMapSchema()){row.kind='map';row.sparse=!!s.getMergedTraits().sparse;row.value=visit(s.getValueSchema());}
 else if(s.isBlobSchema())row.kind='blob';
 else if(s.isDocumentSchema())row.kind='document';
 else if(s.isStringSchema())row.kind='string';
 else if(s.isNumericSchema())row.kind='number';
 else if(s.isBooleanSchema())row.kind='boolean';
 else if(s.isTimestampSchema())row.kind='timestamp';
 else throw Error('unhandled schema '+name);
 return name;
}
const request=visit(schemas.ConverseStreamRequest$),response=visit(schemas.ConverseStreamOutput$),initialResponse=visit(schemas.ConverseStreamResponse$);console.log(JSON.stringify({request,response,initialResponse,graph},null,2));
