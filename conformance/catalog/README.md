# Selected Pi v1.4.0 catalogs

The active runtime projection is the exact Pi `v1.4.0` release at
`4a080252f06caf63a525d7873c5d94d3c23887e0`. It contains 85 chat models:
OpenAI 42, Anthropic 17, Google 24, and DeepSeek 2. `generated/text/{provider}.json`
contains the selected ordered arrays consumed by `pi-ai-providers`.

The tag's generated provider feed inputs are ignored and unavailable from Git.
[v1.4.0/provenance.json](v1.4.0/provenance.json) records the exact published
`@earendil-works/pi-ai@1.4.0` archive integrity, input/output hashes, and model
counts. The retained registry response, SLSA attestation, and archive associate
that hydrated release data with the exact tag commit. This establishes release
catalog identity; it does not reconstruct the generator's raw remote feed inputs.
The original feed generation timestamp is unavailable, so the public catalog
metadata reports no invented generation timestamp.

Run `python3 conformance/catalog/v1.4.0/check.py` for the offline integrity,
source-commit association, archive-member, and selected projection checks.
The `catalog_gen` development binary runs this checker; it does not query feeds.
`python3 conformance/catalog/v1.4.0/check_negative.py` confirms that modified
archive bytes, package inputs, selected outputs, and source identity are rejected
in an isolated temporary copy.

The older 2026-09-07 capture, scripts, and unselected generated observations are
historical evidence only. Their OpenRouter, Codex, and image catalogs are not
runtime assets of this release projection. The release checker and current
selected provider tests supersede the former six-provider/image generator gate.
