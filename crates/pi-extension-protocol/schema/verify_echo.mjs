import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";

const root = new URL(".", import.meta.url);
const catalog = JSON.parse(readFileSync(new URL("fixtures/host-catalog.json", root), "utf8"));
const entry = catalog.entries[0];

function bounded(value) {
  if (Array.isArray(value)) return value.map(bounded);
  if (value !== null && typeof value === "object") {
    const result = {};
    for (const key of Object.keys(value).sort()) {
      if (!/^[\x20-\x7e]*$/.test(key)) throw new Error("non-ASCII key");
      result[key] = bounded(value[key]);
    }
    return result;
  }
  if (typeof value === "string" && !/^[\x20-\x7e]*$/.test(value)) {
    throw new Error("non-ASCII string");
  }
  if (typeof value === "number" && (!Number.isSafeInteger(value) || !Number.isFinite(value))) {
    throw new Error("non-small-integer number");
  }
  return value;
}

for (const minor of ["1.0", "1.1"]) {
  for (const slot of ["arguments", "progress", "raw_result"]) {
    const approved = entry.selected_minor_schema_sets[minor][slot];
    const bytes = Buffer.from(JSON.stringify(bounded(approved.schema)));
    const digest = createHash("sha256").update(bytes).digest("hex");
    if (digest !== approved.schema_digest_sha256) {
      throw new Error(`digest mismatch ${minor}/${slot}`);
    }
  }
}
if (JSON.stringify(entry.agentTool.parameters) !==
    JSON.stringify(entry.selected_minor_schema_sets["1.0"].arguments.schema)) {
  throw new Error("AgentTool.parameters differs from argument schema");
}
process.stdout.write("PASS Node bounded echo schema digests and parameters\n");
