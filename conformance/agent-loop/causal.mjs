import fs from "node:fs";
import path from "node:path";
import { compareTrace, firstFailure } from "./comparator.mjs";

const here = path.dirname(new URL(import.meta.url).pathname);
const manifestPath = path.join(here, "manifest.json");
const fixturesPath = path.join(here, "fixtures.json");
const manifestBytes = fs.readFileSync(manifestPath);
const fixtureBytes = fs.readFileSync(fixturesPath);
const manifest = JSON.parse(manifestBytes);
const originalFixtures = JSON.parse(fixtureBytes);

function fail(message) {
  throw new Error(message);
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

if (manifest.schemaVersion !== 1) fail("manifest schemaVersion must be 1");
if (manifest.authority !== "853a80d26c90a14c1886f0ebb8ffaae133ca2185") fail("manifest authority mismatch");
if (!Array.isArray(manifest.positiveRows) || manifest.positiveRows.length !== 12) fail("expected exactly 12 positive rows");
if (!Array.isArray(manifest.negatives) || manifest.negatives.length !== 12) fail("expected exactly 12 causal negatives");
if (originalFixtures.rows.length !== 12) fail("expected exactly 12 fixture rows");

const fixtureById = new Map(originalFixtures.rows.map((row) => [row.caseId, row]));
const positiveIds = new Set(manifest.positiveRows.map((row) => row.id));
const negativeByPositive = new Map();
for (const row of manifest.positiveRows) {
  if (!fixtureById.has(row.id)) fail(`missing fixture for ${row.id}`);
  if (!Array.isArray(row.sourceReferences) || row.sourceReferences.length === 0) fail(`missing source references for ${row.id}`);
  if (!["source-confirmed", "test-unwitnessed", "non-applicable"].includes(row.status)) fail(`invalid status for ${row.id}`);
}
for (const negative of manifest.negatives) {
  if (!positiveIds.has(negative.positiveId)) fail(`negative points to unknown positive ${negative.positiveId}`);
  if (negativeByPositive.has(negative.positiveId)) fail(`duplicate negative for ${negative.positiveId}`);
  if (!["event_order", "tool_pair", "queue_boundary", "abort_boundary", "provider_error"].includes(negative.reason)) fail(`invalid causal reason ${negative.reason}`);
  if (!negative.name || !negative.mutation?.target || !negative.mutation?.operation) fail(`incomplete negative ${negative.negativeId}`);
  negativeByPositive.set(negative.positiveId, negative);
}
if (negativeByPositive.size !== 12) fail("every positive must have exactly one causal negative");

function mutate(row, negative) {
  const changed = clone(row);
  switch (negative.reason) {
    case "event_order": {
      if (changed.events.length < 2) fail(`${row.caseId} has no event pair to mutate`);
      [changed.events[0], changed.events[1]] = [changed.events[1], changed.events[0]];
      return changed;
    }
    case "tool_pair": {
      const end = changed.events.find((event) => event.type === "tool_execution_end");
      if (!end) fail(`${row.caseId} has no tool end to mutate`);
      end.toolCallId = "mutated-tool-id";
      return changed;
    }
    case "queue_boundary": {
      const delivery = changed.events.find((event) => event.type === "queue_deliver");
      if (!delivery) fail(`${row.caseId} has no queue delivery to mutate`);
      delivery.kind = delivery.kind === "steer" ? "followUp" : "steer";
      return changed;
    }
    case "abort_boundary": {
      const index = changed.events.findIndex((event) => event.type === "abort_signal");
      if (index < 0) fail(`${row.caseId} has no abort boundary to mutate`);
      changed.events.splice(index, 1);
      return changed;
    }
    case "provider_error": {
      if (changed.outcome.stopReason !== "error") fail(`${row.caseId} has no provider error stop reason to mutate`);
      changed.outcome.stopReason = "stop";
      return changed;
    }
    default:
      fail(`unhandled causal reason ${negative.reason}`);
  }
}

let completed = 0;
for (const positive of manifest.positiveRows) {
  const row = fixtureById.get(positive.id);
  compareTrace(row, row);
  const negative = negativeByPositive.get(positive.id);
  const mutated = mutate(row, negative);
  const failure = firstFailure(row, mutated);
  if (!failure) fail(`mutation unexpectedly passed for ${positive.id}`);
  const restored = JSON.parse(fixtureBytes);
  compareTrace(row, restored.rows.find((candidate) => candidate.caseId === row.caseId));
  completed += 1;
}

const first = fixtureById.get(manifest.positiveRows[0].id);
const unknownField = clone(first);
unknownField.events[0].unexpected = true;
if (!firstFailure(first, unknownField)) fail("unknown event field was accepted");
const unknownType = clone(first);
unknownType.events[0].type = "unknown_event";
if (!firstFailure(first, unknownType)) fail("unknown event type was accepted");
const protectedNormalizer = clone(first);
protectedNormalizer.normalizers = [{ path: ["events", "0", "type"], kind: "generatedId" }];
if (!firstFailure(first, protectedNormalizer)) fail("protected normalizer path was accepted");

if (Buffer.compare(fixtureBytes, fs.readFileSync(fixturesPath)) !== 0) fail("causal self-test changed fixture bytes");
if (Buffer.compare(manifestBytes, fs.readFileSync(manifestPath)) !== 0) fail("causal self-test changed manifest bytes");
const reasons = new Set(manifest.negatives.map((negative) => negative.reason));
for (const reason of ["event_order", "tool_pair", "queue_boundary", "abort_boundary", "provider_error"]) {
  if (!reasons.has(reason)) fail(`missing causal reason ${reason}`);
}
console.log(`causal self-test passed: ${completed} positives, ${manifest.negatives.length} one-change negatives, fixture bytes restored`);
