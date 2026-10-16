// Run: node source.mjs /absolute/path/to/pi-mono [--check]
// Only the exact release object is read; the checkout may be ahead of the tag.
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { execFileSync } from 'node:child_process';
import { stripTypeScriptTypes } from 'node:module';
import { createRequire } from 'node:module';
import { createHash } from 'node:crypto';
const commit = '4a080252f06caf63a525d7873c5d94d3c23887e0';
const repo = process.argv[2];
if (!repo) throw new Error('Provide a pi-mono repository containing v1.4.0');
const sources = {};
function load(file, names, context = {}) {
  const source = execFileSync('git', ['-C', repo, 'show', `${commit}:${file}`], {encoding:'utf8'});
  sources[file] = createHash('sha256').update(source).digest('hex');
  const withoutImports = source.replace(/^import\s[\s\S]*?from\s+["'][^"']+["'];?\s*$/gm, '');
  const js = stripTypeScriptTypes(withoutImports).replace(/\bexport\s+/g, '');
  return vm.runInNewContext(`${js}\n;({${names.join(',')}})`, {...context, console, Buffer});
}
const require = createRequire(path.resolve(repo, 'package.json'));
const diffVersion=require('diff/package.json').version;
if(diffVersion!=='8.0.4')throw new Error(`Expected target diff 8.0.4; found ${diffVersion}`);
const base = 'packages/coding-agent/src/core/';
const edit = load(base+'tools/edit-diff.ts', ['applyEditsToNormalizedContent','generateUnifiedPatch','generateDiffString'], {Diff:require('diff')});
const trunc = load(base+'tools/truncate.ts', ['truncateHead','truncateTail']);
const args = load(base+'prompt-templates.ts', ['parseCommandArgs','substituteArgs','expandPromptTemplate']);
const skills = load(base+'skills.ts', ['formatSkillsForPrompt']);
const prompt = load(base+'system-prompt.ts', ['buildSystemPromptSections','buildSystemPromptState','diffSystemPromptSections'], {
  ...skills, getReadmePath:()=>'/pi/README.md', getDocsPath:()=>'/pi/docs', getExamplesPath:()=>'/pi/examples'
});
const resolver = load(base+'model-resolver.ts', ['resolveModelReference','findInitialModel'], {
  DEFAULT_THINKING_LEVEL:'medium',
  isValidThinkingLevel: value => ['off','minimal','low','medium','high','xhigh','max'].includes(value),
});
const cases = JSON.parse(fs.readFileSync(new URL('cases.json',import.meta.url),'utf8'));
const observations = {};
for (const row of cases) {
  if (row.kind === 'truncate') {
    const input = row.repeat ? row.text.repeat(row.repeat) : row.text;
    observations[row.id] = trunc[row.tail?'truncateTail':'truncateHead'](input);
  } else if (row.kind === 'args') {
    const parsed = args.parseCommandArgs(row.arguments);
    observations[row.id] = {parsed, expanded:args.substituteArgs(row.template,parsed)};
  } else if (row.kind === 'skills') {
    observations[row.id] = skills.formatSkillsForPrompt(row.skills,row.reader);
  } else if (row.kind === 'prompt') {
    try {
      const state = prompt.buildSystemPromptState(row.options);
      // Owner scope omits classifier/image generation documentation references.
      if(state.sections?.docs) state.sections.docs = state.sections.docs.replace('codemode scripts and non-LLM models such as classifiers and image models (docs/codemode.md)','codemode scripts (docs/codemode.md)');
      observations[row.id] = state;
    } catch(error) { observations[row.id] = {error:error.message}; }
  } else if (row.kind === 'edit') {
    try {
      const applied = edit.applyEditsToNormalizedContent(row.content,row.edits,'fixture.txt');
      observations[row.id] = {...applied,...edit.generateDiffString(applied.baseContent,applied.newContent),patch:edit.generateUnifiedPatch('fixture.txt',applied.baseContent,applied.newContent)};
    } catch(error) { observations[row.id] = {error:error.message}; }
  } else if (row.kind === 'default-model') {
    observations[row.id] = JSON.parse(JSON.stringify(await resolver.findInitialModel({scopedModels:[],isContinuing:false,modelRuntime:{getAvailableSnapshot:()=>row.models}})));
  } else if (row.kind === 'model') {
    const result = resolver.resolveModelReference(row.request,row.models, provider=>row.usable.includes(provider));
    observations[row.id] = JSON.parse(JSON.stringify(result));
  }
}
const trace = {upstreamCommit:commit, diffVersion, sources, observations};
const output = JSON.stringify(trace,null,2)+'\n';
const target = new URL('trace.json',import.meta.url);
if(process.argv.includes('--check')) {
  if(fs.readFileSync(target,'utf8') !== output) throw new Error('Target source observations changed');
  console.log(`Pinned source fixtures match (${cases.length} cases).`);
} else fs.writeFileSync(target,output);
