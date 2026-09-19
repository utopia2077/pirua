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
