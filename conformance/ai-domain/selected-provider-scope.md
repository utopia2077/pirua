# Selected provider source scope

The [owner decision](../../docs/scope.md#selected-provider-scope)
selects six text factories and OpenRouter images. The
[scope ledger](selected-provider-scope.json) freezes all 241 source occurrences
previously deferred to S05: 17 selected-specific, 97 shared, 125 owner-excluded,
and two Rust platform replacements. This records source scope, not completion.

`generate_ai_conformance.py` validates the ledger against the exact seven proposal
entries, full upstream provider manifest and original source occurrences.
OpenRouter image provider ID is `openrouter` in its separate image namespace.
All 16 legacy wire aliases and `bedrockProviderModule` remain shared;
`amazonBedrockProvider`, its catalog, and public Faux registration are excluded.
Bun OAuth loader packaging and TypeScript JSON declaration typing have separate
Rust replacement rationales; selected OAuth and catalog behavior remain required.

The export mapping uses explicit occurrence selectors to split formerly broad
surface rules. Every owner exclusion requires the owner decision, rationale,
replacement and revisit trigger, uses no Rust product target and cannot claim a
compile probe. The original 22 platform decisions, all 327 behavior rows and
accepted S04 evidence remain unchanged.

Retained scope permits future `implemented` dispositions only through the
existing target, feature, callable and executed-evidence checks. Updating those
implementation mappings does not rewrite the frozen scope ledger. A new upstream
identity, even for an excluded product, still requires explicit classification;
source inventory additions cannot be silently filtered out. Runtime registration
and catalog value checks belong to their later implementation units.
