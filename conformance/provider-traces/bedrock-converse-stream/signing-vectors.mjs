import {createRequire} from 'node:module';import {upstreamPath} from '../../upstream.mjs';
import assert from 'node:assert/strict';
const require=createRequire(upstreamPath('package.json'));
const {SignatureV4}=require('@smithy/signature-v4');const {HttpRequest}=require('@smithy/core/protocols');const {Hash}=require('@smithy/core/serde');
assert.equal(require('@smithy/signature-v4/package.json').version,'5.4.3');
const credentials={accessKeyId:'AKID_SYNTHETIC',secretAccessKey:'synthetic-secret',sessionToken:'synthetic-session'};
const cases=[
{id:'basic',headers:{host:'synthetic.invalid','content-type':'application/json','x-custom':'first'},body:'{"messages":[]}'},
{id:'changed-custom',headers:{host:'synthetic.invalid','content-type':'application/json','x-custom':'second'},body:'{"messages":[]}'},
{id:'h2-authority',headers:{':authority':'synthetic.invalid:8443','content-type':'application/json','x-z':'one','X-Z':'two','amz-sdk-request':'attempt=1; max=3'},body:'{"messages":[]}'},
{id:'header-spaces-unsigned',headers:{host:'synthetic.invalid','X-Custom':'  a\t b  \n c ','x-custom':'last','expect':'100-continue','user-agent':'synthetic','proxy-note':'omit','sec-test':'omit'},body:'{}'},
{id:'query-escaped-path',path:'/base//x/../model/a%2Fb%20c/./converse-stream',query:{z:['2','1'],a:'x y','X-Amz-Signature':'ignored','empty':'','ö':'é'},headers:{host:'synthetic.invalid'},body:'{"z":1,"a":"é🙂"}'},
{id:'no-session',credentials:{accessKeyId:'AKID_SYNTHETIC',secretAccessKey:'synthetic-secret'},region:'cn-north-1',headers:{host:'bedrock-runtime.cn-north-1.amazonaws.com.cn'},body:''},
{id:'existing-content-hash',headers:{host:'synthetic.invalid','X-Amz-Content-Sha256':'SOURCE_HASH'},body:'ignored-for-hash'},
{id:'clock-corrected',date:'2026-01-02T04:04:05Z',headers:{host:'synthetic.invalid','amz-sdk-request':'attempt=2; max=3'},body:'{}'},
];
const result=[];for(const row of cases){const input={protocol:'https:',hostname:'synthetic.invalid',method:'POST',path:row.path??'/model/synthetic/converse-stream',headers:row.headers,body:row.body,...(row.query?{query:row.query}:{})};const identity=row.credentials??credentials;const signer=new SignatureV4({credentials:identity,region:row.region??'us-east-1',service:'bedrock',sha256:Hash.bind(null,'sha256')});const signed=await signer.sign(new HttpRequest(input),{signingDate:new Date(row.date??'2026-01-02T03:04:05Z')});result.push({id:row.id,input,credentials:identity,region:row.region??'us-east-1',date:row.date??'2026-01-02T03:04:05Z',signed});}
assert.notEqual(result[0].signed.headers.authorization,result[1].signed.headers.authorization);
console.log(JSON.stringify({signatureV4:'5.4.3',cases:result},null,2));
