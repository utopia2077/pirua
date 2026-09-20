# S12 extension wire schema

The checked-in `FileDescriptorSet` files are the wire authority for core and
seven profile roots at versions 1.0 and 1.1. `methods.json` is checked against
descriptor `MethodOptions`. Run `sh verify.sh` to compile every root with
`libprotoc 3.20.3` into a temporary directory and compare its bytes, then check
the saved fixtures and the bounded echo schemas. The gate does not rewrite the
checked-in files.

The common/tool wire contract and bounded echo model were reviewed for the
first source writer. The hook, provider, event-sink, resource, host-action, and
telemetry-sink wire shells are checked in, but their semantic projections and
activation behavior are incomplete and are not grantable or accepted yet.
The host catalog fixture models one echo tool; it is not a production permission
source. Passing these checks does not execute a live endpoint or prove durable
settlement.

The positive echo progress and result frames encode `details` as a present empty
map and pass selected schema-bound value admission for both minors. The progress
Envelope body is 137 bytes, so the byte-credit edge is 136/137; the complete
terminal body is 131 bytes and consumes zero credit. The older 132/126 frames
are retained only as `MissingValue` negative fixtures in the Rust tests.

The raw request is a synthetic wire vector with `EffectKey.logical_attempt = 1`.
It exercises encoding and selected argument admission, not an actual
`ToolEffectPlan`. A real tool-plan integration witness must use the plan's
`logical_attempt = 0` and prove its live/durable binding separately.
