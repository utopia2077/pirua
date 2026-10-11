import fs from "node:fs";
import path from "node:path";
import { compareObservation, firstFailure } from "./comparator.mjs";

const here = path.dirname(new URL(import.meta.url).pathname);
const manifestPath = path.join(here, "manifest.json");
const fixturesPath = path.join(here, "fixtures.json");
const manifestBytes = fs.readFileSync(manifestPath);
const fixtureBytes = fs.readFileSync(fixturesPath);
const manifest = JSON.parse(manifestBytes);
const original = JSON.parse(fixtureBytes);
const fail = (message) => { throw new Error(message); };
if (manifest.schemaVersion !== 1 || manifest.authority !== "853a80d26c90a14c1886f0ebb8ffaae133ca2185") fail("manifest identity mismatch");
if (!Array.isArray(manifest.positiveRows) || manifest.positiveRows.length !== 4) fail("expected exactly 18 positive rows");
if (!Array.isArray(manifest.negatives) || manifest.negatives.length !== 4) fail("expected exactly 18 causal negatives");
if (!Array.isArray(original.rows) || original.rows.length !== 4) fail("expected exactly 18 fixture rows");
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
  if (!["side_effect_count", "capability_preflight"].includes(negative.reason)) fail(`invalid mutation reason ${negative.reason}`);
  negatives.set(negative.positiveId, negative);
}
if (negatives.size !== 4) fail("every positive needs one negative");
const clone = (value) => JSON.parse(JSON.stringify(value));
let completed = 0;
for (const row of original.rows) {
  const negative = negatives.get(row.caseId);
  compareObservation(row, row);
  const mutated = clone(row);
  if (negative.reason === "side_effect_count") mutated.observations.durable_entries += 1;
  else mutated.input.capability = "models.resolve.removed";
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
if (Buffer.compare(fixtureBytes, fs.readFileSync(fixturesPath)) !== 0) fail("fixture bytes changed during causal test");
if (Buffer.compare(manifestBytes, fs.readFileSync(manifestPath)) !== 0) fail("manifest bytes changed during causal test");
console.log(`causal self-test passed: ${completed} positives, ${manifest.negatives.length} one-change negatives, fixture bytes restored`);
