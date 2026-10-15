//! The same explicit native JSON domain is admitted in both directions before
//! serializing, decoding or transferring ownership of any bridge operation.
use crate::types::{BoundaryError, MAX_JSON_DEPTH, Outcome};
use serde::Deserialize;
use serde_json::Value;

fn invalid(message: impl std::fmt::Display) -> BoundaryError {
    BoundaryError(format!("CodeMode JSON domain: {message}"))
}
fn depth_error() -> BoundaryError {
    invalid(format!("nesting exceeds {MAX_JSON_DEPTH} arrays/objects"))
}

/// Bound structural recursion without decoding values or recursing on the input.
/// Quotes/escapes are tracked so brackets inside strings do not count. The JSON
/// decoder remains responsible for syntax, number and Unicode validity.
fn check_text_depth(bytes: &[u8], limit: usize) -> Result<(), BoundaryError> {
    let (mut depth, mut quoted, mut escaped) = (0usize, false, false);
    for &byte in bytes {
        if quoted {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quoted = false;
            }
        } else {
            match byte {
                b'"' => quoted = true,
                b'[' | b'{' => {
                    depth += 1;
                    if depth > limit {
                        return Err(depth_error());
                    }
                }
                b']' | b'}' => depth = depth.saturating_sub(1),
                _ => {}
            }
        }
    }
    Ok(())
}

pub(crate) fn decode<T: for<'de> Deserialize<'de>>(
    bytes: &[u8],
    limit: usize,
) -> Result<T, BoundaryError> {
    check_text_depth(bytes, limit)?;
    // The incidental Serde limit is 128. Replace it only after the bounded scan;
    // this is not acceptance of unbounded recursion (including Drop/Debug).
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let value = T::deserialize(&mut decoder).map_err(invalid)?;
    decoder.end().map_err(invalid)?;
    Ok(value)
}
pub(crate) fn value(text: &str) -> Result<Value, BoundaryError> {
    decode(text.as_bytes(), MAX_JSON_DEPTH)
}
fn check_values<'a>(
    values: impl IntoIterator<Item = (&'a Value, usize)>,
) -> Result<(), BoundaryError> {
    let mut waiting: Vec<_> = values.into_iter().collect();
    while let Some((value, depth)) = waiting.pop() {
        if (value.is_array() || value.is_object()) && depth >= MAX_JSON_DEPTH {
            return Err(depth_error());
        }
        match value {
            Value::Array(values) => waiting.extend(values.iter().map(|value| (value, depth + 1))),
            Value::Object(values) => {
                waiting.extend(values.values().map(|value| (value, depth + 1)))
            }
            _ => {}
        }
    }
    Ok(())
}
pub(crate) fn check_value(value: &Value) -> Result<(), BoundaryError> {
    check_values([(value, 0)])
}
pub(crate) fn check_outcome(outcome: &Outcome) -> Result<(), BoundaryError> {
    match outcome {
        Outcome::Success { value: Some(value) } => check_value(value),
        Outcome::Error { properties, .. } => {
            check_values(properties.values().map(|value| (value, 1)))
        }
        _ => Ok(()),
    }
}
