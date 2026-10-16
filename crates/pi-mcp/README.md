# pi-mcp

A standalone asynchronous Rust Model Context Protocol client. The protocol
implementation follows `packages/mcp` at Pi v1.4.0, commit
`4a080252f06caf63a525d7873c5d94d3c23887e0`. It has no Agent, CodingSession,
Durable, AI provider, or external MCP SDK dependency.

```rust,no_run
use std::sync::Arc;
use pi_mcp::{ClientOptions, McpClient, RequestOptions};
use pi_mcp::transport::{StdioOptions, StdioTransport};
use serde_json::json;

# async fn example() -> pi_mcp::Result<()> {
let mut options = StdioOptions::new("my-mcp-server");
options.args = vec!["--workspace".into(), "/work".into()];
let client = McpClient::connect(
    ClientOptions::new("my-host", "1.0"),
    Arc::new(StdioTransport::new(options)),
).await?;
let tools = client.list_tools(RequestOptions::default()).await?;
let result = client.call_tool("search", json!({"query": "MCP"}),
    RequestOptions::default()).await?;
// A tool failure is a successful MCP response with is_error == Some(true).
// The host adapts result.content and result.structured_content to its own tools.
client.close().await?;
# Ok(()) }
```

For Streamable HTTP, construct `HttpOptions::new(url)` and pass an
`Arc<StreamableHttpTransport>` instead. `HttpOptions.client` permits explicit
proxy, TLS, and networking configuration. The default HTTP client disables
redirects; an injected client owns its redirect and credential-forwarding policy.
Headers and authentication are specific to that transport instance.

The protocol surface includes initialization and version negotiation
(`2025-11-25`, `2025-06-18`, `2025-03-26`, `2024-11-05`), ping, tool discovery
and calls, resource/template discovery and reads, generic requests and
notifications, and server request handlers. `Tool.extra`, content-block fields,
and result extras preserve MCP annotations and extension fields. Explicit JSON
`null` structured content remains distinct from missing structured content.
The host's runtime adapter owns conversion to model content.

Clone `McpClient` to issue concurrent requests. Each request gets a unique ID;
cancelled or timed-out replies are never reused for another request. A
`RequestOptions.cancellation` signal or a dropped request future settles the
request and sends `notifications/cancelled` (except for `initialize`). Progress
is delivered through an optional unbounded Tokio channel, and valid progress
renews the request's inactivity timeout. Zero disables the timer. Transient
transport diagnostics are observable through `client.subscribe()` without
settling unrelated requests; transport closure settles all outstanding calls.
The broadcast event channel retains 256 events; a slow subscriber receives
Tokio's explicit lag error. A host consuming progress must drain its channel.

Server `ping` is built in. Configuring `ClientOptions.roots = Some(roots)`
advertises roots and installs `roots/list`; hosts can replace that handler for
dynamic roots with `set_request_handler`. Server handlers receive a cancellation
token. Shutdown rejects queued server requests before they can start a host
handler. Unknown server methods receive JSON-RPC `-32601`.

Connections and transport instances are single-use. Call `close().await` for an
awaited shutdown. Dropping the last public client handle initiates shutdown on
the active Tokio runtime. Stdio closes stdin, allows a grace period, sends
SIGTERM, and escalates to SIGKILL. Unix servers run in their own process group;
Windows shutdown uses `taskkill /T /F` for the process tree. Stderr retains a
bounded tail. HTTP close cancels streams and uses a best-effort session DELETE
with the last token, without invoking the authentication provider. DELETE has a
one-second deadline. A closed or expired connection is replaced by a new
client/transport and a fresh initialization; tool calls are never replayed as
part of reconnection.

Streamable HTTP accepts JSON or SSE POST responses, captures session IDs,
sends the negotiated protocol version, and opens the optional GET stream only
after `notifications/initialized`. GET streams reconnect with bounded backoff.
A response stream that drops before answering can resume via GET with its
`Last-Event-ID`; a response stream with no resumable cursor fails only its own
request. SSE ID-only priming events and retry hints are honored, and one initial
UTF-8 BOM is removed even when its bytes arrive in separate chunks. Retries after
authentication recovery check request cancellation before dispatch. Authentication,
session, and protocol errors are not retried as transient stream failures.
Message/event limits default to 16 MiB and apply incrementally.

OAuth is an explicit host boundary:

```rust,no_run
use std::sync::Arc;
use pi_mcp::oauth::{OAuthOptions, OAuthProvider, FlowOptions, AuthorizationResult};
use pi_mcp::transport::{HttpOptions, StreamableHttpTransport};
use pi_mcp::{ClientOptions, McpClient};

# async fn example() -> pi_mcp::Result<()> {
let server = "https://mcp.example.com/mcp".parse().unwrap();
let redirect = "http://127.0.0.1:43210/callback".parse().unwrap();
let oauth = Arc::new(OAuthProvider::new(OAuthOptions::new(
    server, redirect, Default::default(),
))?);
match oauth.authorize(FlowOptions::default()).await? {
    AuthorizationResult::Redirect(url) => {
        // Host opens url, verifies callback state against oauth.state(), then
        // calls authorize again with authorization_code, issuer, and the scope
        // used for the authorization request. The host owns its callback server.
    }
    AuthorizationResult::Authorized => {}
}
let mut http = HttpOptions::new("https://mcp.example.com/mcp".parse().unwrap());
http.auth = Some(oauth);
let client = McpClient::connect(ClientOptions::new("my-host", "1"),
    Arc::new(StreamableHttpTransport::new(http)?)).await?;
client.close().await?;
# Ok(()) }
```

`OAuthProvider` implements `AuthProvider`. It supports protected-resource and
OAuth/OIDC authorization-server discovery, issuer checking, a configured
authorization metadata document, PKCE S256 authorization codes, dynamic client
registration, configured client IDs or client ID metadata documents, token
exchange and refresh, basic/post/public client authentication, and a custom
`TokenAuthenticator`. Token endpoints require HTTPS except loopback HTTP.
Concurrent unauthorized requests share one refresh, including rotating refresh
tokens; an already replaced rejected token uses the current grant. A 403 bearer
`insufficient_scope` challenge starts authorization with the union of granted
and requested scopes. Invalid client/grant errors invalidate the corresponding
stored state before one new flow attempt. The host handles redirects and callback
HTTP, and can inject `OAuthStateStore` for durable credential storage. The default
store is memory only. Persisted state from another exact server URL is ignored.
Attaching this OAuth provider to a different HTTP transport URL is rejected before
credentials are read or dispatched.
Serialize user-driven authorization attempts for one provider; the transport's
automatic recovery coordinates concurrent 401/403 responses itself.

Batch JSON-RPC, legacy HTTP+SSE, production MCP servers, sampling, and tasks are
outside this client surface. Generic requests allow hosts to implement additional
methods without advertising those capabilities in the default client.

Run `cargo test -p pi-mcp --all-features --locked`. All HTTP/auth fixtures bind
loopback; no provider credentials or external services are used. The actual stdio
fixture uses Python 3. Unix process-tree assertions additionally use Linux `/proc`
when it exists. Source-to-test correspondence is recorded in
[upstream-coverage.md](upstream-coverage.md).
