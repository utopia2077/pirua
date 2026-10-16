//! Checked JS number and JSON.stringify projections for ordered JSON wire values.
//!
//! JavaScript enumerates array-index object keys numerically before other keys,
//! preserves insertion order for other keys, and converts JSON numbers to f64.
//! This module projects those observations without changing core JsonObject order.
//! It does not model undefined, functions, BigInt, nonfinite numbers, toJSON, or
//! lone UTF-16 surrogates, none of which an ordinary serde_json Value represents.

use std::fmt;

use pi_ai_core::SafeUnsigned;
use serde_json::Value;

const MAX_DEPTH: usize = 128;
const MAX_BYTES: usize = 1024 * 1024;

/// Ordered, transient JSON at the trusted payload middleware boundary.
///
/// Construction does not serialize or validate: middleware may replace or reduce
/// the logical value before the family's existing consuming boundary checks it.
/// This is deliberately not a core DTO and implements neither Serde direction.
///
/// ```compile_fail
/// fn serialized<T: serde::Serialize>() {}
/// serialized::<pi_ai_providers::api::wire_json::WirePayload>();
/// ```
/// ```compile_fail
/// fn deserialized<T: serde::de::DeserializeOwned>() {}
/// deserialized::<pi_ai_providers::api::wire_json::WirePayload>();
/// ```
#[derive(Clone)]
pub struct WirePayload(Value);

impl WirePayload {
    pub fn new(value: Value) -> Self {
        Self(value)
    }
    pub fn as_value(&self) -> &Value {
        &self.0
    }
    pub fn as_value_mut(&mut self) -> &mut Value {
        &mut self.0
    }
    pub fn into_value(self) -> Value {
        self.0
    }
}

impl fmt::Debug for WirePayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WirePayload([REDACTED])")
    }
}

/// Redacted checked wire-serialization failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WireJsonError {
    /// The value exceeds the supported JSON depth.
    DepthLimit,
    /// The projected compact JSON exceeds one MiB.
    ByteLimit,
    /// The numeric value cannot be projected to a finite JavaScript Number.
    InvalidNumber,
}

impl fmt::Display for WireJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::DepthLimit => "wire JSON depth limit",
            Self::ByteLimit => "wire JSON byte limit",
            Self::InvalidNumber => "wire JSON number is not finite",
        })
    }
}
impl std::error::Error for WireJsonError {}

/// A wire value is not a finite nonnegative mathematical safe integer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireNumberError;

impl fmt::Display for WireNumberError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("wire number must be a finite nonnegative safe integer")
    }
}
impl std::error::Error for WireNumberError {}

/// Projects a provider JSON Number by value rather than by its lexical storage variant.
///
/// Integral decimal/exponent values and negative zero are accepted. Fractional,
/// negative nonzero, nonfinite, non-number and above-safe-range values fail.
/// Missing/null handling belongs to each wire field's caller; neither is coerced
/// here. Core durable SafeUnsigned deserialization remains deliberately unchanged.
pub fn project_wire_safe_unsigned(value: &Value) -> Result<SafeUnsigned, WireNumberError> {
    let number = value
        .as_f64()
        .filter(|number| {
            number.is_finite()
                && (0.0..=SafeUnsigned::MAX.get() as f64).contains(number)
                && number.fract() == 0.0
        })
        .ok_or(WireNumberError)?;
    SafeUnsigned::new(number as u64).map_err(|_| WireNumberError)
}

/// Serializes an ordered finite JSON value using JS key ordering and Number spelling.
///
/// Input object insertion order must have been retained by the wire decoder.
/// Integer Number variants intentionally round through f64, including values
/// outside JS's safe-integer range: this matches JSON.parse's Number conversion.
/// Core DTO numeric admission remains independently checked and unchanged.
pub fn stringify_wire_json(value: &Value) -> Result<String, WireJsonError> {
    stringify_with_limits(
        value,
        Limits {
            depth: MAX_DEPTH,
            bytes: MAX_BYTES,
        },
    )
}

#[derive(Clone, Copy)]
struct Limits {
    depth: usize,
    bytes: usize,
}

fn stringify_with_limits(value: &Value, limits: Limits) -> Result<String, WireJsonError> {
    let mut output = String::new();
    write_value(&mut output, value, 0, limits)?;
    Ok(output)
}

fn append(output: &mut String, text: &str, limits: Limits) -> Result<(), WireJsonError> {
    if text.len() > limits.bytes.saturating_sub(output.len()) {
        return Err(WireJsonError::ByteLimit);
    }
    output.push_str(text);
    Ok(())
}

fn write_string(output: &mut String, value: &str, limits: Limits) -> Result<(), WireJsonError> {
    // UTF-8 input is a lower bound on its escaped representation.
    if value.len() > limits.bytes.saturating_sub(output.len()) {
        return Err(WireJsonError::ByteLimit);
    }
    append(output, "\"", limits)?;
    for character in value.chars() {
        match character {
            '"' => append(output, "\\\"", limits)?,
            '\\' => append(output, "\\\\", limits)?,
            '\u{0008}' => append(output, "\\b", limits)?,
            '\u{000c}' => append(output, "\\f", limits)?,
            '\n' => append(output, "\\n", limits)?,
            '\r' => append(output, "\\r", limits)?,
            '\t' => append(output, "\\t", limits)?,
            '\u{0000}'..='\u{001f}' => {
                append(output, &format!("\\u{:04x}", u32::from(character)), limits)?;
            }
            _ => {
                let mut bytes = [0; 4];
                append(output, character.encode_utf8(&mut bytes), limits)?;
            }
        }
    }
    append(output, "\"", limits)
}

fn array_index(key: &str) -> Option<u32> {
    let index = key.parse::<u32>().ok()?;
    (index != u32::MAX && index.to_string() == key).then_some(index)
}

fn write_value(
    output: &mut String,
    value: &Value,
    depth: usize,
    limits: Limits,
) -> Result<(), WireJsonError> {
    if depth > limits.depth {
        return Err(WireJsonError::DepthLimit);
    }
    match value {
        Value::Null => append(output, "null", limits),
        Value::Bool(true) => append(output, "true", limits),
        Value::Bool(false) => append(output, "false", limits),
        Value::Number(number) => {
            let number = number
                .as_f64()
                .filter(|value| value.is_finite())
                .ok_or(WireJsonError::InvalidNumber)?;
            // Explicit zero also covers negative zero independently of formatter internals.
            if number == 0.0 {
                append(output, "0", limits)
            } else {
                append(output, ryu_js::Buffer::new().format_finite(number), limits)
            }
        }
        Value::String(value) => write_string(output, value, limits),
        Value::Array(values) => {
            append(output, "[", limits)?;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    append(output, ",", limits)?;
                }
                write_value(output, value, depth + 1, limits)?;
            }
            append(output, "]", limits)
        }
        Value::Object(values) => {
            // Every member takes at least four bytes; reject impossible output
            // before allocating the borrowed numeric-key ordering vector.
            if values.len() > limits.bytes.saturating_sub(output.len()) / 4 {
                return Err(WireJsonError::ByteLimit);
            }
            let mut indexed: Vec<_> = values
                .iter()
                .filter_map(|(key, value)| array_index(key).map(|index| (index, key, value)))
                .collect();
            indexed.sort_unstable_by_key(|(index, _, _)| *index);
            let entries = indexed
                .iter()
                .map(|(_, key, value)| (*key, *value))
                .chain(values.iter().filter(|(key, _)| array_index(key).is_none()));
            append(output, "{", limits)?;
            for (index, (key, value)) in entries.enumerate() {
                if index != 0 {
                    append(output, ",", limits)?;
                }
                write_string(output, key, limits)?;
                append(output, ":", limits)?;
                write_value(output, value, depth + 1, limits)?;
            }
            append(output, "}", limits)
        }
    }
}
