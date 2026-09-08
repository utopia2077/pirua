//! Explicit Node/Smithy request profile; not Fetch or protocol negotiation.
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SmithyProtocol {
    Http1,
    Http2,
}

#[derive(Clone, Eq, PartialEq)]
pub struct SmithyH2PseudoHeaders {
    pub authority: String,
    pub scheme: Option<String>,
}
impl fmt::Debug for SmithyH2PseudoHeaders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SmithyH2PseudoHeaders([REDACTED])")
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum SmithyRequestProfile {
    Http1 {
        body_admission: SmithyBodyAdmission,
        connection: SmithyH1Connection,
    },
    Http2 {
        pseudo_headers: SmithyH2PseudoHeaders,
        body_admission: SmithyBodyAdmission,
    },
}
/// Derived from the source SDK record before physical header case folding.
/// This is body admission, not a neutral request timeout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SmithyBodyAdmission {
    Immediate,
    WaitForContinue,
}
/// Independent source agent-derived default; explicit physical Connection wins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SmithyH1Connection {
    KeepAlive,
    Close,
}
impl SmithyRequestProfile {
    pub fn protocol(&self) -> SmithyProtocol {
        match self {
            Self::Http1 { .. } => SmithyProtocol::Http1,
            Self::Http2 { .. } => SmithyProtocol::Http2,
        }
    }
}
impl fmt::Debug for SmithyRequestProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SmithyRequestProfile([REDACTED])")
    }
}

impl SmithyProtocol {
    /// Node's finite singleton field set; HTTP/1 and HTTP/2 differ.
    pub fn is_single_value_header(self, name: &str) -> bool {
        let protocol = self;
        let common = matches!(
            name,
            "age"
                | "authorization"
                | "content-length"
                | "content-type"
                | "etag"
                | "expires"
                | "from"
                | "host"
                | "if-modified-since"
                | "if-unmodified-since"
                | "last-modified"
                | "location"
                | "max-forwards"
                | "proxy-authorization"
                | "referer"
                | "retry-after"
                | "user-agent"
        );
        common
            || match protocol {
                SmithyProtocol::Http1 => name == "server",
                SmithyProtocol::Http2 => matches!(
                    name,
                    "access-control-allow-credentials"
                        | "access-control-max-age"
                        | "access-control-request-method"
                        | "content-encoding"
                        | "content-language"
                        | "content-location"
                        | "content-md5"
                        | "content-range"
                        | "date"
                        | "dnt"
                        | "if-match"
                        | "if-none-match"
                        | "if-range"
                        | "range"
                        | "tk"
                        | "upgrade-insecure-requests"
                        | "x-content-type-options"
                ),
            }
    }
}
