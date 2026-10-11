import fs from "node:fs";
import path from "node:path";
import { compareObservation } from "./comparator.mjs";

const here = path.dirname(new URL(import.meta.url).pathname);
const caseId = process.argv[2];
let actual;
try { actual = JSON.parse(fs.readFileSync(0, "utf8")); }
catch (error) { console.error(`actual observation is not JSON: ${error.message}`); process.exit(1); }
if (typeof caseId !== "string" || caseId.length === 0) { console.error("usage: compare-actual.mjs <case-id>"); process.exit(1); }
const fixtures = JSON.parse(fs.readFileSync(path.join(here, "fixtures.json"), "utf8"));
const expected = fixtures.rows.find((row) => row.caseId === caseId);
if (!expected) { console.error(`fixture row not found: ${caseId}`); process.exit(1); }
try { compareObservation(expected, actual); console.log(`actual observation passed: ${caseId}`); }
catch (error) { console.error(error?.message ?? String(error)); process.exit(1); }
