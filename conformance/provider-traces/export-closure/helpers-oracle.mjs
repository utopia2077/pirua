// Complete pinned helper bodies, finite canonical inputs, no provider services.
import { execFileSync } from 'node:child_process';import {upstreamRoot} from '../../upstream.mjs';
import { stripTypeScriptTypes } from 'node:module';
import { deepStrictEqual, strictEqual, throws } from 'node:assert';
import vm from 'node:vm';
const commit = '853a80d26c90a14c1886f0ebb8ffaae133ca2185';
const context = vm.createContext({ structuredClone });
for (const path of ['utils/estimate.ts', 'api/simple-options.ts', 'api/constrained-sampling.ts', 'api/github-copilot-headers.ts', 'api/openai-prompt-cache.ts']) {
  const text = execFileSync('git', ['show', `${commit}:packages/ai/src/${path}`], {
    cwd: upstreamRoot, encoding: 'utf8', env: { PATH: '/usr/bin:/bin' },
  });
  vm.runInContext(stripTypeScriptTypes(text.replace(/^import[\s\S]*?;\n/gm, '').replace(/^export /gm, '')), context, { filename: path });
}
const run = expression => vm.runInContext(expression, context);
const plain = value => JSON.parse(JSON.stringify(value));
strictEqual(run('OPENAI_PROMPT_CACHE_KEY_MAX_LENGTH'), 64);
strictEqual(context.clampOpenAIPromptCacheKey('😀'.repeat(65)), '😀'.repeat(64));
strictEqual(context.clampOpenAIPromptCacheKey(undefined), undefined);
strictEqual(context.inferCopilotInitiator([]), 'user');
strictEqual(context.inferCopilotInitiator([{role:'toolResult',content:[]}]), 'agent');
strictEqual(context.hasCopilotVisionInput([{role:'toolResult',content:[{type:'image'}]}]), true);
deepStrictEqual(plain(context.buildCopilotDynamicHeaders({messages:[],hasImages:true})), {'X-Initiator':'user','Openai-Intent':'conversation-edits','Copilot-Vision-Request':'true'});
const tool = {name:'grammar',parameters:{type:'object',properties:{input:{type:'string'}},required:['input']},constrainedSampling:{type:'grammar',variants:{openai_lark:'start: /.+/',openai_regex:'.+'}}};
deepStrictEqual(plain(context.resolveGrammarConstrainedSampling(tool,true)), {format:'lark',definition:'start: /.+/',inputProperty:'input'});
strictEqual(context.resolveGrammarConstrainedSampling(tool,false),undefined);
deepStrictEqual(plain([...context.createGrammarToolInputProperties([tool],true)]), [['grammar','input']]);
strictEqual(context.getGrammarToolInput('grammar',{input:'x'},'input'),'x');
throws(()=>context.getGrammarToolInput('grammar',{input:2},'input'));
const buffer={input:'',started:false,closed:false};
strictEqual(context.appendGrammarToolInputJsonDelta(buffer,'input','a"',false),'{"input":"a\\"');
strictEqual(context.appendGrammarToolInputJsonDelta(buffer,'input','a"\n',true),'\\n"}');
strictEqual(context.appendGrammarToolInputJsonDelta(buffer,'input','a"\n',true),undefined);
throws(()=>context.appendGrammarToolInputJsonDelta(buffer,'input','changed',true));
const schema={properties:{x:{type:'string'}},required:[],type:'object'};
const strict=context.makeStrictJsonSchema(schema);
deepStrictEqual(Object.keys(strict),['properties','required','type','additionalProperties']);
deepStrictEqual(plain(strict),{properties:{x:{anyOf:[{type:'string'},{type:'null'}]}},required:['x'],type:'object',additionalProperties:false});
strictEqual(context.getJsonSchemaToolParameters(tool,false),tool.parameters);
deepStrictEqual(plain(context.getJsonSchemaToolParameters(tool,true)),plain(context.makeStrictJsonSchema(tool.parameters)));
strictEqual(context.resolveJsonSchemaStrictSampling({...tool,constrainedSampling:{type:'json_schema',strict:'require'}},true),true);
throws(()=>context.resolveJsonSchemaStrictSampling({...tool,constrainedSampling:{type:'json_schema',strict:'require'}},false));
strictEqual(run('MIN_ANSWER_TOKENS'),1024);
deepStrictEqual(plain(run('DEFAULT_THINKING_BUDGETS')),{minimal:1024,low:2048,medium:8192,high:16384});
strictEqual(context.clampReasoning('xhigh'),'high');
strictEqual(context.thinkingBudgetForLevel('max',{high:7}),7);
strictEqual(context.clampThinkingBudgetToAnswerRoom(100,1050),26);
deepStrictEqual(plain(context.adjustMaxTokensForThinking(undefined,8192,'medium')),{maxTokens:8192,thinkingBudget:7168});
const model={contextWindow:10000,maxTokens:10000,samplingParams:{a:1,b:2}};
const input={messages:[{role:'user',content:'abcd',timestamp:1}]};
strictEqual(context.clampMaxTokensToContext(model,input,10000),5903);
const base=context.buildBaseOptions(model,input,{samplingParams:{b:3},temperature:0.2});
strictEqual(base.maxTokens,5903);strictEqual(base.temperature,0.2);
deepStrictEqual(plain(base.samplingParams),{a:1,b:3});
console.log('PASS complete pinned helper bodies: grammar, strict schema, Copilot, cache and simple options');
