//! Bounded, synchronous request-body encoding; no request policy or I/O.

use std::fmt;

use crate::{TransportError, TransportErrorKind};

/// Explicit local admission limits, not serialized provider options.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ZstdRequestLimits {
    pub max_input_bytes: usize,
    pub max_output_bytes: usize,
}

impl fmt::Debug for ZstdRequestLimits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ZstdRequestLimits([REDACTED])")
    }
}

/// Encodes one complete body with Zstandard level 3.
///
/// Input is borrowed and admitted before allocation or codec work. Output storage
/// is at most `min(compress_bound(body.len()), max_output_bytes)` logical bytes;
/// only a complete frame is returned. Empty input still needs frame capacity.
/// The codec's level-3 context/workspace is additional implementation-owned
/// storage, released on every return; it is not charged to the output limit.
pub fn compress_zstd_request(
    body: &[u8],
    limits: ZstdRequestLimits,
) -> Result<Vec<u8>, TransportError> {
    if body.len() > limits.max_input_bytes || limits.max_output_bytes == 0 {
        return Err(TransportError::new(TransportErrorKind::LimitExceeded));
    }
    let bound = zstd_safe::compress_bound(body.len());
    if bound == 0 {
        return Err(TransportError::new(TransportErrorKind::LimitExceeded));
    }
    let capacity = bound.min(limits.max_output_bytes);
    let mut output = Vec::new();
    output
        .try_reserve_exact(capacity)
        .map_err(|_| TransportError::new(TransportErrorKind::LimitExceeded))?;
    output.resize(capacity, 0);
    // A slice fixes the codec-visible capacity even if the allocator rounded up.
    let mut context = zstd_safe::CCtx::try_create()
        .ok_or_else(|| TransportError::new(TransportErrorKind::LimitExceeded))?;
    context
        .set_parameter(zstd_safe::CParameter::CompressionLevel(3))
        .map_err(|_| TransportError::new(TransportErrorKind::Protocol))?;
    let mut input = zstd_safe::InBuffer { src: body, pos: 0 };
    let mut destination = zstd_safe::OutBuffer::around(output.as_mut_slice());
    let remaining = context
        .compress_stream2(
            &mut destination,
            &mut input,
            zstd_safe::zstd_sys::ZSTD_EndDirective::ZSTD_e_end,
        )
        .map_err(|code| {
            let kind = match zstd_safe::get_error_name(code) {
                "Destination buffer is too small" | "Allocation error : not enough memory" => {
                    TransportErrorKind::LimitExceeded
                }
                _ => TransportErrorKind::Protocol,
            };
            TransportError::new(kind)
        })?;
    if remaining != 0 || input.pos != body.len() {
        return Err(TransportError::new(TransportErrorKind::LimitExceeded));
    }
    let written = destination.pos();
    output.truncate(written);
    Ok(output)
}
