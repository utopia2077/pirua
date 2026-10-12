import fs from "node:fs";
import path from "node:path";
import { ConformanceMismatch, compareObservation, firstFailure, validateObservation } from "./comparator.mjs";

const here = path.dirname(new URL(import.meta.url).pathname);
const manifestPath = path.join(here, "manifest.json");
const fixturesPath = path.join(here, "fixtures.json");
const manifestBytes = fs.readFileSync(manifestPath);
const fixtureBytes = fs.readFileSync(fixturesPath);
const manifest = JSON.parse(manifestBytes);
const original = JSON.parse(fixtureBytes);
const fail = (message) => { throw new Error(message); };
if (manifest.schemaVersion !== 1 || manifest.authority !== "853a80d26c90a14c1886f0ebb8ffaae133ca2185") fail("manifest identity mismatch");
if (!Array.isArray(manifest.positiveRows) || manifest.positiveRows.length !== 12) fail("expected exactly 12 active positive rows");
if (!Array.isArray(manifest.negatives) || manifest.negatives.length !== 12) fail("expected exactly 12 active causal negatives");
if (!Array.isArray(original.rows) || original.rows.length !== 12) fail("expected exactly 12 active fixture rows");
const pending = ["branch.fork_atomic_publish", "restart.recover_session", "legacy_import.format3", "model_switch.next_run", "extension.participation_ordering", "lifecycle.builder_replace"];
if (JSON.stringify(manifest.pendingRows) !== JSON.stringify(pending) || JSON.stringify(original.pendingRows) !== JSON.stringify(pending)) fail("exact six pending M6-C rows required");
const active = ["model.credentials_catalog", "prompt.system_resources", "prompt.settings_snapshot", "tools.validate_progress_settle", "tools.coding_operations", "queue.steer_during_turn", "queue.follow_up_after_turn", "retry.overflow_precedence", "retry.provider_recovery", "abort.active_stream_cleanup", "compaction.context_projection", "capability.required_prestart_failure"];
if (JSON.stringify(manifest.positiveRows.map((row) => row.id)) !== JSON.stringify(active) || JSON.stringify(original.rows.map((row) => row.caseId)) !== JSON.stringify(active)) fail("exact twelve active M6-A/M6-B rows required");
const byId = new Map(original.rows.map((row) => [row.caseId, row]));
const positiveIds = new Set();
for (const row of manifest.positiveRows) {
  if (positiveIds.has(row.id) || !byId.has(row.fixtureCaseId) || row.id !== row.fixtureCaseId) fail(`invalid positive row ${row.id}`);
  if (!Array.isArray(row.sourceReferences) || row.sourceReferences.length === 0) fail(`missing source references ${row.id}`);
  if (row.status !== "source-confirmed") fail(`invalid status ${row.id}`);
  positiveIds.add(row.id);
}
const negatives = new Map();
for (const negative of manifest.negatives) {
  if (!positiveIds.has(negative.positiveId) || negatives.has(negative.positiveId)) fail(`negative coverage error ${negative.positiveId}`);
  if (negative.reason === "side_effect_count" && negative.mutation?.target !== "observations.durable_entries") fail(`invalid mutation ${negative.negativeId}`);
  if (negative.reason === "capability_preflight" && negative.mutation?.target !== "input.capability") fail(`invalid mutation ${negative.negativeId}`);
  if (negative.reason === "operation_witness" && (!negative.mutation?.target?.startsWith("observations.") || !["remove_last", "increment", "append"].includes(negative.mutation?.operation))) fail(`invalid operation witness ${negative.negativeId}`);
  if (!["side_effect_count", "capability_preflight", "operation_witness"].includes(negative.reason)) fail(`invalid mutation reason ${negative.reason}`);
  negatives.set(negative.positiveId, negative);
}
if (negatives.size !== 12) fail("every positive needs one negative");
const clone = (value) => JSON.parse(JSON.stringify(value));
let completed = 0;
for (const row of original.rows) {
  const negative = negatives.get(row.caseId);
  compareObservation(row, row);
  const mutated = clone(row);
  if (negative.reason === "side_effect_count") mutated.observations.durable_entries += 1;
  else if (negative.reason === "capability_preflight") mutated.input.capability = "models.resolve.removed";
  else {
    const keys = negative.mutation.path ?? negative.mutation.target.split(".");
    let parent = mutated;
    for (const key of keys.slice(0, -1)) parent = parent[key];
    const key = keys.at(-1);
    if (negative.mutation.operation === "increment") parent[key] += 1;
    if (negative.mutation.operation === "append") parent[key] += "-changed";
    if (negative.mutation.operation === "remove_last") parent[key].pop();
    let semanticFailure = false;
    try { validateObservation(mutated, row.caseId); } catch { semanticFailure = true; }
    if (!semanticFailure) fail(`operation mutation passed semantic validation for ${row.caseId}`);
  }
  if (!firstFailure(row, mutated)) fail(`causal mutation unexpectedly passed for ${row.caseId}`);
  const restored = JSON.parse(fixtureBytes).rows.find((candidate) => candidate.caseId === row.caseId);
  compareObservation(row, restored);
  completed += 1;
}
const unknown = clone(original.rows[0]);
unknown.observations.unexpected = true;
if (!firstFailure(original.rows[0], unknown)) fail("unknown observation field was accepted");
const unknownEvent = clone(original.rows[0]);
unknownEvent.events = [{ index: 0, type: "unknown_agent_event" }];
if (!firstFailure(original.rows[0], unknownEvent)) fail("unknown event type was accepted");
const malformedMessage = clone(original.rows[0]);
malformedMessage.messages = [{ notAMessage: true }];
if (!firstFailure(original.rows[0], malformedMessage)) fail("malformed message object was accepted");
const toolRow = original.rows.find((row) => row.caseId === "tools.validate_progress_settle");
const badPayload = clone(toolRow);
badPayload.events.find((event) => event.type === "tool_end").payload.unexpected = true;
try { validateObservation(badPayload); fail("unknown nested tool payload was accepted"); } catch (error) { if (error.message === "unknown nested tool payload was accepted") throw error; }
const badQueuedMessage = clone(original.rows.find((row) => row.caseId === "queue.steer_during_turn"));
badQueuedMessage.observations.queued_item.message.unexpected = true;
try { validateObservation(badQueuedMessage); fail("unknown nested queued message was accepted"); } catch (error) { if (error.message === "unknown nested queued message was accepted") throw error; }
const codingRow = original.rows.find((row) => row.caseId === "tools.coding_operations");
const firstMessageCall = (row) => row.events.find((event) => event.type === "message_end" && event.payload.message.content?.[0]?.type === "toolCall").payload.message.content[0];
const messageProbes = [
  ["unknown MessageEnd tool argument", (row) => { firstMessageCall(row).arguments.unexpected = { nested: true }; }],
  ["contradictory MessageEnd tool path", (row) => { firstMessageCall(row).arguments.path = "missing.txt"; }],
  ["unknown MessageEnd tool name", (row) => { firstMessageCall(row).name = "unknown_tool"; }],
  ["malformed envelope message content", (row) => { row.messages = [{ role: "assistant", id: "injected", content: { unexpected: true } }]; }],
];
for (const [label, mutate] of messageProbes) {
  const mutated = clone(codingRow);
  mutate(mutated);
  for (const check of [() => validateObservation(mutated, mutated.caseId), () => compareObservation(mutated, mutated)]) {
    let rejected = false;
    try { check(); } catch (error) { if (!(error instanceof ConformanceMismatch)) throw error; rejected = true; }
    if (!rejected) fail(`${label} passed semantic or same-mutated expected/actual validation`);
  }
}
if (Buffer.compare(fixtureBytes, fs.readFileSync(fixturesPath)) !== 0) fail("fixture bytes changed during causal test");
if (Buffer.compare(manifestBytes, fs.readFileSync(manifestPath)) !== 0) fail("manifest bytes changed during causal test");
console.log(`causal self-test passed: ${completed} positives, ${manifest.negatives.length} one-change negatives, fixture bytes restored`);
console.log(`nested message probes passed: ${messageProbes.length} semantic and same-mutated expected/actual rejections`);
