# Ordered strict-schema wire projection

`oracle.mjs` executes the complete `makeStrictJsonSchema` implementation from
pi-mono commit `853a80d26c90a14c1886f0ebb8ffaae133ca2185`. The fixture records
the Git source hash, canonical input bytes, semantic output, and exact output
bytes. Run without credentials or network access:

```sh
env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node conformance/provider-traces/strict-schema-wire/oracle.mjs --check
```

Inputs start in the canonical order admitted by `JsonSchema`; the oracle does
not reconstruct order discarded before that boundary. JavaScript then applies
its numeric own-key enumeration and preserves existing field slots while
appending missing `required` and `additionalProperties` fields. Ten cases cover
existing and missing slots, nested properties/items/anyOf, nullable properties,
numeric property names, empty objects, and metadata that remains ordinary data.

`make_strict_wire_json_schema` and `get_wire_json_schema_tool_parameters` retain
this mutation order in `WirePayload`. The existing canonical `JsonSchema` and
borrowed `Cow` helpers remain available and unchanged. The ordered strict helper
first invokes the existing validator/converter, releases its canonical result,
then projects the admitted input. The exact 256 KiB input and depth-128 limits,
supported subset, and error categories are unchanged; converted output may grow
as it already could. Nonstrict copying introduces no earlier cap. Existing
consumer serialization/resource checks still apply.

`tests/strict_schema_wire.rs` compares all ten byte strings, confirms equivalent
schemas can occupy distinct request-byte slots, preserves input immutability,
checks the original eight semantic fixtures and 27 rejection cases, and verifies
both sides of the existing size/depth bounds. Replacing the ordered result with
the canonical helper's output makes the unchanged exact-byte test fail on
`append-additional`; restoring the ordered projection makes it pass. The five
wire consumers use the new helper without changing their policy decisions.

This correction preserves request serialization. It does not change the schema
domain, validation policy, public core DTO representation, or provider behavior
beyond the source-defined wire order. Full/delta continuation remains the Codex
adapter's separate integration proof.
