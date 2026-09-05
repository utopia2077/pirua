import { deepStrictEqual } from "node:assert";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { stripTypeScriptTypes } from "node:module";

const commit = "853a80d26c90a14c1886f0ebb8ffaae133ca2185";
const sourcePath = "packages/ai/src/api/openai-responses-shared.ts";
const source = execFileSync("git", ["show", `${commit}:${sourcePath}`], { cwd: "upstream/pi-mono", encoding: "utf8" });
const encode = source.slice(source.indexOf("function encodeTextSignatureV1("), source.indexOf("\nfunction parseTextSignature("));
const processor = source.slice(source.indexOf("export async function processResponsesStream<")).replace(/^export /, "");
const run = new Function("calculateCost", stripTypeScriptTypes(encode + "\n" + processor) + "\nreturn processResponsesStream;")(() => {});
const cases = [
  { id: "integral-decimal-slot", raw: '[{"type":"response.output_item.added","output_index":1.0,"item":{"type":"message","id":"m"}},{"type":"response.output_text.delta","output_index":1e0,"delta":"text"},{"type":"response.output_item.done","output_index":1,"item":{"type":"message","id":"m","content":[{"type":"output_text","text":"text"}]}},{"type":"response.completed","response":{"status":"completed"}}]' },
  { id: "integral-exponent-usage", raw: '[{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":1e2,"output_tokens":3.0,"total_tokens":1.03e2,"input_tokens_details":{"cached_tokens":2.0,"cache_write_tokens":-0.0}}}}]' },
];
for (const entry of cases) {
  const events = JSON.parse(entry.raw);
  const output = { content: [], usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: {} }, stopReason: "pending" };
  async function* iterate() { yield* events; }
  await run(iterate(), output, { push() {} }, {});
  const { cost, ...usage } = output.usage;
  entry.expectedContent = output.content;
  entry.expectedUsage = usage;
  entry.expectedStop = output.stopReason;
}
const result = {
  sourceCommit: commit, sourcePath,
  sourceSha256: createHash("sha256").update(source).digest("hex"),
  evidenceClass: "executed-pinned-processor-numeric-oracle",
  scope: "Exact processor/stop mapper/text signature; calculateCost stubbed and costs excluded; raw numeric spelling retained in inputs",
  cases,
};
if (process.argv.includes("--check")) {
  deepStrictEqual(result, JSON.parse(readFileSync(new URL("./numeric-wire.json", import.meta.url), "utf8")));
} else {
  process.stdout.write(JSON.stringify(result, null, 2) + "\n");
}
