// JSON.stringify calls used by the pinned Responses processor; finite JSON only.
import { deepStrictEqual } from "node:assert";
import { readFileSync } from "node:fs";
const cases = [
  { id: "numeric-order-and-spelling", raw: String.raw`{"10":1.0,"2":1e20,"z":-0.0}` },
  { id: "index-boundaries-and-nesting", raw: String.raw`{"4294967295":"max","01":"leading","4294967294":"index-max","-0":"minus","0":"zero","a":{"10":1,"2":2,"b":3,"1":4},"1e0":"exponent"}` },
  { id: "all-number-storage-variants", raw: String.raw`[-0.0,1.0,1e20,1e21,1e-6,1e-7,9007199254740993,18446744073709551615,-9223372036854775808,5e-324,1.7976931348623157e308]` },
  { id: "string-escapes-and-unicode", raw: String.raw`{"quoted\"key":"\u0000\b\f\n\r\t\\\/\u2028\u2029\ud83d\ude00","x":"\u0061"}` },
];
const result = {
  sourceCommit: "853a80d26c90a14c1886f0ebb8ffaae133ca2185",
  sourcePath: "packages/ai/src/api/openai-responses-shared.ts",
  evidenceClass: "executed-js-stringify",
  scope: "Native JSON.stringify(JSON.parse(raw)) used by pinned opaque signatures; no undefined, BigInt, nonfinite or lone-surrogate cases",
  cases: cases.map(({ id, raw }) => ({ id, raw, expected: JSON.stringify(JSON.parse(raw)) })),
};
if (process.argv.includes("--check")) {
  deepStrictEqual(result, JSON.parse(readFileSync(new URL("./wire-json.json", import.meta.url), "utf8")));
} else {
  process.stdout.write(JSON.stringify(result, null, 2) + "\n");
}
