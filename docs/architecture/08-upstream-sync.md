# Upstream synchronization

A full commit identifies the selected source baseline. The
[ownership map](../../upstream-map.toml) classifies every path in each watched
package, including exclusions. Route digests bind paths, Git blob identities,
owners and dispositions. Generated manifests are projections of immutable objects.

Assess candidate releases by semantic impact rather than advancing automatically
for every tag. Provider/auth contract changes, runtime/storage changes and changes
to selected coding capabilities warrant explicit review. Releases affecting only
excluded UI/platform/provider products may need no native change. Multiple releases
can be reviewed as one interval while retaining the source baseline and decisions.

The [drift guide](../upstream-drift.md) describes local comparison, acknowledgements
and proof receipts. The tool never fetches, changes the pin or applies its own
candidate patch. Newly added files require intake even if a broad route matches.

## Assembly consistency

The [proof registry](../../conformance/assembly.json) links current source
responsibilities to concrete test targets and evidence anchors. Static assembly
checks validate the join and command applicability; they execute zero proof
commands. Actual acceptance also requires native tests, relevant source oracles,
causal checks for new high-consequence guards and documentation/topology checks.

Historical fixtures retain their source identity. A target-version label must not
be applied to old expected values. Native adaptations and omitted responsibilities
are recorded explicitly, with current crate coverage documents as the API-level
reference. Promote the formal pin once the assembled selected scope is accepted.
