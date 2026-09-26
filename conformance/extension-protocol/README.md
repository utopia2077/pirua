# Core/tool echo protocol receipt

`echo-v1.json` owns the expected principal, two independent planner-shaped keys,
request IDs, source indices, call IDs, texts, four core/tool minor combinations,
and peer fault stages. Each case uses a fresh Rust `Endpoint` and a fresh Node
TypeScript or Python process. The peer reads the actual host Request and derives
its terminal text from the selected argument. The Rust test admits that terminal
through `ToolInvocation` and checks the first retained `TerminalReceipt`.

Run the isolated generated DTO/descriptor check, then the focused transcript:

```sh
cargo run --manifest-path crates/pi-extension-protocol/codegen/Cargo.toml --locked --offline
cargo test -p pi-extension-protocol --test cross_language_echo --locked --offline
./scripts/verify.sh
```

The focused test repeats the isolated codegen check before launching peers.
It recomputes the profile digests in `sdk/typescript/digests.json` from the
canonical tool descriptors and recomputes each selected schema digest through
protocol JSON admission. `sdk/python/peer.py --inspect-wire` independently
checks captured outgoing frames against canonical FileDescriptorSets, including
the raw absence of the 1.1-only Manifest label under core 1.0, the selected
opaque tool result, and its present empty details map. The test fails when Node
cannot run `.ts` strip-types or Python cannot import `google.protobuf`.

The six new source files are a bounded reference transcript, not a public
TypeScript SDK or a runtime adapter. Fixture keys have the shape of a planned
`EffectKey`; no real `ToolEffectPlan`, `AgentToolResult`, Session write, durable
settlement, positive stream credit, drain/rebind, or other profile is proved.
The five faults are peer-emitted selected digest/projection mutations and a
repeated terminal. Their checks end at activation or the protocol receipt.
