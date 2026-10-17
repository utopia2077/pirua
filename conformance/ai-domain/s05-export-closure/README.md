# Historical export and composition evidence

This directory preserves export observations from the earlier source revision
`853a80d26c90a14c1886f0ebb8ffaae133ca2185`. Its original source occurrence,
feature, callable-probe and composition metadata is historical evidence. Some
references name APIs, provider products or test targets retired by the v1.4.0
upgrade. They are not current proof registrations or a runnable acceptance gate.

The former export-closure generator and its stage-specific policy have been
retired. Their presence in old evidence strings does not imply that those
commands or APIs remain supported. Original captured expectations retain their
source identity; no old value is relabelled to claim target-version parity.

Current coverage is defined by the [selected integration contract](../../../docs/contracts/ai-integration/index.md),
[ownership map](../../../upstream-map.toml) and
[active proof registry](../../assembly.json). Current provider and OAuth behavior
is compared against the [v1.4.0 source/release oracle](../../provider-traces/v1.4.0/README.md).
