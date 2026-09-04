//! Explicit request policy and incremental network ports.
//! All raw wire values are transient, non-Serde and redacted in Debug.

mod http;
mod policy;
mod proxy;
mod retry_date;
mod sse;

pub use http::*;
pub use policy::*;
pub use proxy::*;
pub use sse::*;

// ECMAScript WhiteSpace plus LineTerminator, not Rust's Unicode White_Space.
fn is_ecma_whitespace(c: char) -> bool {
    matches!(c, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
