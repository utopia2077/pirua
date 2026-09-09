//! Explicit request policy and incremental network ports.
//! All raw wire values are transient, non-Serde and redacted in Debug.

mod fetch;
mod fetch_content;
mod fetch_request;
mod http;
#[cfg(feature = "native")]
mod native;
mod persistent_websocket;
mod policy;
mod provider_retry;
mod proxy;
mod request_encoding;
mod retry_date;
mod smithy;
#[cfg(feature = "native")]
mod smithy_error;
mod smithy_eventstream;
mod smithy_retry;
mod sse;
mod websocket;

pub use fetch::FetchRedirectMode;
pub use fetch_content::{FetchContentLimits, FetchResponseSource};
pub use fetch_request::*;
pub use http::*;
#[cfg(feature = "native")]
pub use native::*;
pub use persistent_websocket::*;
pub use policy::*;
pub use provider_retry::*;
pub use proxy::*;
pub use request_encoding::{ZstdRequestLimits, compress_zstd_request};
pub use smithy::*;
pub use smithy_eventstream::*;
pub use smithy_retry::*;
pub use sse::*;
pub use websocket::*;

// ECMAScript WhiteSpace plus LineTerminator, not Rust's Unicode White_Space.
fn is_ecma_whitespace(c: char) -> bool {
    matches!(c, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
