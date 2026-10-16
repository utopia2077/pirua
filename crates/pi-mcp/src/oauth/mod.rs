//! MCP OAuth discovery, PKCE, registration and refresh. The host owns browser
//! interaction and credential storage; persisted credentials bind to one exact URL.
//!
//! OAuth behavior is ported from Pi v1.4.0's subset of the MIT-licensed MCP
//! TypeScript SDK v1.29.0; its MIT notice is retained in the root LICENSE/NOTICE.
mod discovery;
mod provider;
mod types;

pub use discovery::{
    build_discovery_urls, discover_server_info, parse_www_authenticate, select_resource,
};
pub use provider::{AuthorizationResult, FlowOptions, OAuthOptions, OAuthProvider, step_up_scope};
pub use types::*;
