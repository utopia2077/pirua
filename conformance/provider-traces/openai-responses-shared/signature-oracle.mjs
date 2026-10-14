// Bounded signature oracle: execute exact pinned processor bodies, no SDK/network.
import { execFileSync } from "node:child_process";import {upstreamRoot} from '../../upstream.mjs';
import { createHash } from "node:crypto";
import { stripTypeScriptTypes } from "node:module";

const commit = "853a80d26c90a14c1886f0ebb8ffaae133ca2185";
const path = "packages/ai/src/api/openai-responses-shared.ts";
const source = execFileSync("git", ["show", `${commit}:${path}`], {
  cwd: upstreamRoot, encoding: "utf8",
});
const encode = source.slice(source.indexOf("function encodeTextSignatureV1("), source.indexOf("\nfunction parseTextSignature("));
const processor = source.slice(source.indexOf("export async function processResponsesStream<"));
const program = stripTypeScriptTypes(encode + "\n" + processor.replace(/^export /, ""), { mode: "strip" });
const processStream = new Function("calculateCost", program + "\nreturn processResponsesStream;")(() => {});
const doneItem = { type: "reasoning", id: "rs", summary: [{ type: "summary_text", text: "reason" }], status: "completed" };
const events = [
  { type: "response.output_item.added", output_index: 0, item: { type: "reasoning", id: "rs", summary: [] } },
  { type: "response.output_item.done", output_index: 0, item: doneItem },
  { type: "response.output_item.done", output_index: 1, item: { phase: "final_answer", id: "msg", type: "message", content: [{ type: "output_text", text: "answer" }] } },
  { type: "response.completed", response: { status: "completed", output: [{ type: "reasoning", id: "rs", encrypted_content: "opaque" }] } },
];
const output = { content: [], usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0, cost: {} }, stopReason: "pending" };
let thinkingEnd;
let textEnd;
const sink = { push(event) {
  if (event.type === "thinking_end") thinkingEnd = event.partial.content[event.contentIndex].thinkingSignature;
  if (event.type === "text_end") textEnd = event.partial.content[event.contentIndex].textSignature;
}};
async function* iterate() { yield* events; }
await processStream(iterate(), output, sink, {});
process.stdout.write(JSON.stringify({
  sourceCommit: commit, sourcePath: path,
  sourceSha256: createHash("sha256").update(source).digest("hex"),
  evidenceClass: "executed-pinned-processor-signature-oracle",
  scope: "Exact extracted encodeTextSignatureV1/processResponsesStream/mapStopReason; calculateCost stubbed because only signatures are observed",
  wireEvents: JSON.stringify(events), thinkingEnd, textEnd,
  finalThinking: output.content[0].thinkingSignature,
}, null, 2) + "\n");
