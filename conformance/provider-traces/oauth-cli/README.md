# Optional OAuth developer CLI evidence

Source authority is pi-mono853a80d26c90a14c1886f0ebb8ffaae133ca2185,
`packages/ai/src/cli.ts`, SHA256
e04e6cd5f0e9da829d10b9b285592d5170d59eff29ae985aecf3f5f9bd15cb2d.
`source.mjs` executes that COMPLETE module after type stripping, replacing only
its fs/readline/builtinProviders service imports, console/process and argv.
The builtin list is explicitly the six selected source factory metadata entries,
with the three selected OAuth capabilities scripted. Source factories/flows are
not falsely claimed as reexecuted by this CLI oracle; their owning S05 proofs
remain separate. No real stdin, credentials or HTTP are used by this oracle.

Reproduce: `env -i PATH=/usr/bin:/bin NODE_NO_WARNINGS=1 node
conformance/provider-traces/oauth-cli/source.mjs`. `observations.json` contains
32 complete command cases, input scenarios and observable output/IO/login traces.
The owning pi-ai test reexecutes and compares the complete JSON artifact.

`crates/pi-ai/tests/oauth_cli.rs` compiles the exact private binary modules and
compares all32 traces, selected IDs, full synthetic credential values/extras and
login/file/finally ordering. Ordinary errors are fixed redacted Rust categories:
raw JS exception messages are not a compatibility promise. Valid nonobject auth
files fail without writing (source array false-success is excluded). Source EOF
is a pending-callback observation; actual stdin EOF becomes a fixed interaction
error. Strict JsonValue numeric/string/duplicate/depth rejection is separate
from JSON syntax failure, which preserves source empty-object fallback. The
private syntax scanner does not decode or supply outgoing data. Serialization
compares values, with two-space pretty output and no final newline.

`oauth_cli_real_binary_no_network_help_errors_pipes_and_eof` invokes the ACTUAL
pi_ai_cli executable in an isolated temporary cwd. It covers help aliases/list,
invalid command/provider, piped invalid provider/method and EOF before network.
The separate provider-owned `oauth_cli_host.rs` path-includes the identical CLI
modules to access existing transport test ports WITHOUT adding a facade edge.
It composes all four actual accepted OAuth login paths with CLI AuthInteraction,
synthetic callback/HTTP services and synthetic exported credentials. Its child
is an internal test executable, NOT the actual pi_ai_cli binary. On Linux the
parent observes the named stdin thread in `/proc/.../wchan` pipe_read or
anon_pipe_read while keeping the pipe open and unwritten. Only then does a
private temporary control file release the synthetic browser callback. Successful
login/export and runtime/process exit must finish while stdin remains blocked.
No native OAuth ports or account endpoint calls occur in these new host tests.

`rust-source-hashes.json` binds that included-body proof to the exact production
CLI modules and entrypoint; update only with an owning implementation change.
The stdin reader is one detached process-lifetime std thread, never a Tokio
blocking-pool task. Request identity and cancellation retire one-shot responses;
a late canceled line cannot satisfy another question. EOF differs from empty
line. Piped input is requested on demand, an explicit host normalization of Node
readline typeahead scheduling. No physical OS-read interruption or reusable
console shutdown guarantee is claimed. The native CLI token-response cap is
1MiB; callback host/limits are127.0.0.1 and existing CallbackLimits::default.

Named causal controls cover decimal-prefix parsing, wrong credential/extras,
lost unrelated values, early file access, omitted finally close, admission
misclassified as syntax, cancellation retirement and joined-reader shutdown.
All old49 selected-flow observations and foundation/native host behavior remain
unchanged. The required feature is dev-oauth-cli, independently selecting only
all-oauth/auth-native/optional existing Tokio; help/list do no native setup.
