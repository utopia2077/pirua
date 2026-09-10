import { readFileSync, writeFileSync, mkdirSync, copyFileSync } from 'node:fs';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

const [textOutput, sourceRoot, pinnedImage, destination] = process.argv.slice(2);
const selected = ['openai', 'google', 'anthropic', 'deepseek', 'openai-codex', 'openrouter'];
const all = JSON.parse(readFileSync(join(textOutput, 'models.json'), 'utf8'));
const put = (name, value) => {
  const path = join(destination, name);
  mkdirSync(join(path, '..'), { recursive: true });
  writeFileSync(path, JSON.stringify(value) + '\n');
};
copyFileSync(join(textOutput, 'models.json'), join(destination, 'source-text.json'));
const groups = {};
for (const provider of Object.keys(all)) {
  groups[provider] = JSON.parse(readFileSync(join(sourceRoot, 'packages/ai/src/providers/data', provider + '.json'), 'utf8'));
}
put('source-grouped.json', groups);
copyFileSync(join(sourceRoot, 'packages/ai/src/providers/data/.manifest.json'), join(destination, 'source-data-manifest.json'));
for (const provider of selected) {
  if (!Object.hasOwn(all, provider)) throw new Error('Missing selected text provider');
  put('text/' + provider + '.json', Object.values(all[provider]));
}
const generatedImage = join(sourceRoot, 'packages/ai/src/image-models.generated.ts');
const { IMAGE_MODELS: captured } = await import(pathToFileURL(generatedImage));
const { IMAGE_MODELS: historical } = await import(pathToFileURL(pinnedImage));
copyFileSync(generatedImage, join(destination, 'captured-image.ts'));
put('source-images.json', captured);
put('images/openrouter.json', Object.values(captured.openrouter));
put('historical-images.json', historical);
