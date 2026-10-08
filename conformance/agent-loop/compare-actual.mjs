import fs from "node:fs";
import path from "node:path";
import { compareObservation } from "./comparator.mjs";

const here = path.dirname(new URL(import.meta.url).pathname);
const fixturePath = path.join(here, "fixtures.json");
const caseId = process.argv[2];
const input = fs.readFileSync(0, "utf8");

function fail(message) {
  console.error(message);
  process.exitCode = 1;
}

let actual;
try {
  actual = JSON.parse(input);
} catch (error) {
  fail(`actual observation is not JSON: ${error.message}`);
  process.exit();
}

const selectedCaseId = caseId ?? actual.caseId;
if (typeof selectedCaseId !== "string" || selectedCaseId.length === 0) {
  fail("usage: compare-actual.mjs <case-id> with an actual observation JSON on stdin");
  process.exit();
}

const fixtures = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const expected = fixtures.rows.find((row) => row.caseId === selectedCaseId);
if (!expected) {
  fail(`fixture row not found: ${selectedCaseId}`);
  process.exit();
}

try {
  compareObservation(expected, actual);
  console.log(`actual observation passed: ${selectedCaseId}`);
} catch (error) {
  fail(error?.message ?? String(error));
}
