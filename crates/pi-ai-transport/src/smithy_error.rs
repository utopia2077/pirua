//! Typed cause projection, used only by explicit native Smithy paths.
use crate::{SmithyRetryDisposition as Disposition, TransportError, TransportErrorKind as Kind};
use std::{error::Error, io};
pub(crate) fn no_retry(kind: Kind) -> TransportError {
    TransportError::new(kind)
        .with_smithy_retry(Disposition::NotRetryable)
        .with_smithy_assistant_retry(false)
}
pub(crate) fn transient() -> TransportError {
    TransportError::new(Kind::Connection).with_smithy_retry(Disposition::Transient)
}
fn io_disposition(error: &io::Error) -> Disposition {
    match error.kind() {
        io::ErrorKind::ConnectionRefused
        | io::ErrorKind::ConnectionReset
        | io::ErrorKind::ConnectionAborted
        | io::ErrorKind::BrokenPipe
        | io::ErrorKind::TimedOut
        | io::ErrorKind::HostUnreachable
        | io::ErrorKind::NetworkUnreachable
        | io::ErrorKind::UnexpectedEof => Disposition::Transient,
        _ => Disposition::NotRetryable,
    }
}
pub(crate) fn io(error: io::Error) -> TransportError {
    TransportError::new(Kind::Connection).with_smithy_retry(io_disposition(&error))
}
pub(crate) fn hyper(error: hyper::Error) -> TransportError {
    if error.is_parse() || error.is_user() {
        return no_retry(Kind::Connection);
    }
    if error.is_incomplete_message() || error.is_timeout() {
        return transient().with_smithy_assistant_retry(true);
    }
    let mut source = error.source();
    for _ in 0..16 {
        let Some(next) = source else { break };
        if let Some(io) = next.downcast_ref::<io::Error>() {
            return TransportError::new(Kind::Connection)
                .with_smithy_retry(io_disposition(io))
                .with_smithy_assistant_retry(matches!(
                    io.kind(),
                    io::ErrorKind::ConnectionReset
                        | io::ErrorKind::UnexpectedEof
                        | io::ErrorKind::TimedOut
                ));
        }
        if let Some(h2) = next.downcast_ref::<h2::Error>() {
            return h2_ref(h2);
        }
        source = next.source();
    }
    no_retry(Kind::Connection)
}
fn h2_ref(error: &h2::Error) -> TransportError {
    if let Some(io) = error.get_io() {
        return TransportError::new(Kind::Connection).with_smithy_retry(io_disposition(io));
    }
    if error.is_remote() && error.reason() == Some(h2::Reason::REFUSED_STREAM) {
        return transient().with_smithy_assistant_retry(false);
    }
    no_retry(Kind::Connection)
}
pub(crate) fn h2(error: h2::Error) -> TransportError {
    h2_ref(&error)
}

/// Node's admitted HTTP/1 response emits ECONNRESET on chunk-decoder failure.
/// Hyper retains the decoder's typed InvalidInput IO cause instead. This
/// projection applies only to response body reads after the final head.
pub(crate) fn hyper_h1_body(error: hyper::Error) -> TransportError {
    let mut source = error.source();
    for _ in 0..16 {
        let Some(next) = source else { break };
        if let Some(io) = next.downcast_ref::<io::Error>() {
            if io.kind() == io::ErrorKind::InvalidInput {
                return transient().with_smithy_assistant_retry(false);
            }
            break;
        }
        source = next.source();
    }
    hyper(error).with_smithy_assistant_retry(false)
}

/// Single resolved destination is the finite source domain established by the
/// Node loopback oracle. Multi-address AggregateError wording stays unknown.
pub(crate) fn connect(error: io::Error, addresses: &[std::net::SocketAddr]) -> TransportError {
    let kind = error.kind();
    let result = io(error);
    if kind == io::ErrorKind::ConnectionRefused
        && let [address] = addresses
    {
        let endpoint = address.to_string();
        let matched = ["429", "500", "502", "503", "504", "524"]
            .iter()
            .any(|digits| endpoint.contains(digits));
        return result.with_smithy_assistant_retry(matched);
    }
    if kind == io::ErrorKind::TimedOut {
        return result.with_smithy_assistant_retry(true);
    }
    result
}
