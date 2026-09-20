//! Bounded saved-argument conversion for the selected `tool.v1` profile.
//!
//! The caller supplies the durably saved `JsonObject` and the active host grant.
//! This module neither sends bytes nor starts an invocation. It measures the
//! closed tree and its protobuf encoding before allocating a wire DTO, then
//! re-admits that DTO against the grant's exact argument projection.

use std::fmt;
use std::mem::size_of;

use pi_ai_core::{JS_SAFE_INTEGER_MAX, JsonNumber, JsonObject, JsonValue};
use pi_extension_protocol::activation::ActiveGrant;
use pi_extension_protocol::jcs;
use pi_extension_protocol::tool_live::SelectedArguments;
use pi_extension_protocol::wire::value::{self, Limits as ValueLimits};
use pi_extension_protocol::wire::{v1_0, v1_1};

const HARD_DEPTH_MAX: usize = 64;

/// Fixed categories with no argument, key, schema, or peer payload text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EncodeError {
    InvalidGrant,
    InvalidProjection,
    InvalidNumber,
    Limit,
    Allocation,
    Value(value::Error),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidGrant => "invalid selected tool grant",
            Self::InvalidProjection => "invalid selected argument projection",
            Self::InvalidNumber => "invalid saved JSON number",
            Self::Limit => "saved argument limit exceeded",
            Self::Allocation => "bounded argument allocation failed",
            Self::Value(_) => "saved arguments do not match approved schema",
        })
    }
}

impl std::error::Error for EncodeError {}

type Result<T> = std::result::Result<T, EncodeError>;

/// Encodes a saved tool argument object for the grant's selected tool minor.
/// The approved argument schema, identity, digest, and finite limits come only
/// from `ActiveGrant`, never from a peer-supplied expected tuple.
pub fn encode_arguments(
    saved: &JsonObject,
    grant: &ActiveGrant,
    tool_name: &str,
) -> Result<SelectedArguments> {
    let wire = grant.wire();
    let protocol = wire.selected_protocol.ok_or(EncodeError::InvalidGrant)?;
    if protocol.major != 1 || protocol.minor > 1 || wire.authenticated_extension_id.is_empty() {
        return Err(EncodeError::InvalidGrant);
    }
    let tool = grant
        .tools()
        .iter()
        .find(|item| item.descriptor().name == tool_name)
        .ok_or(EncodeError::InvalidGrant)?;
    let reference = tool.reference();
    let profile = reference
        .profile
        .as_ref()
        .ok_or(EncodeError::InvalidGrant)?;
    if reference.kind != "tool"
        || reference.name != tool_name
        || reference.authenticated_extension_id != wire.authenticated_extension_id
        || profile.name != "tool.v1"
        || profile.major != 1
        || profile.minor > 1
        || !wire.selected_capabilities.contains(reference)
    {
        return Err(EncodeError::InvalidGrant);
    }
    let projection = tool.projections().arguments();
    if projection.id().strip_prefix("tool.arguments:") != Some(tool_name)
        || projection.digest() != projection.schema().canonical().sha256()
    {
        return Err(EncodeError::InvalidProjection);
    }
    let host = wire.limits.as_ref().ok_or(EncodeError::InvalidGrant)?;
    let caps = Caps::new(host)?;
    if projection.id().len() > caps.value.string_bytes {
        return Err(EncodeError::Limit);
    }
    let mut meter = Meter::new(&caps);
    let value_len = meter.measure_map(saved, 1)?;
    let bound_len = field_len(projection.id().len())?
        .checked_add(field_len(32)?)
        .and_then(|n| n.checked_add(field_len(value_len).ok()?))
        .ok_or(EncodeError::Limit)?;
    if bound_len > caps.frame {
        return Err(EncodeError::Limit);
    }

    let expected = value::Projection {
        id: projection.id(),
        digest_sha256: projection.digest(),
        schema: projection.schema(),
    };
    let selected = if profile.minor == 0 {
        let value = build_v1_0_map(saved)?;
        let bound = v1_0::SchemaBoundValue {
            projection_id: copy_string(projection.id())?,
            schema_digest_sha256: projection.digest().to_vec(),
            value: Some(value),
        };
        value::admit_schema_bound_v1_0(&bound, &expected, caps.value, caps.json)
            .map_err(EncodeError::Value)?;
        SelectedArguments::V1_0(bound)
    } else {
        let value = build_v1_1_map(saved)?;
        let bound = v1_1::SchemaBoundValue {
            projection_id: copy_string(projection.id())?,
            schema_digest_sha256: projection.digest().to_vec(),
            value: Some(value),
        };
        value::admit_schema_bound_v1_1(&bound, &expected, caps.value, caps.json)
            .map_err(EncodeError::Value)?;
        SelectedArguments::V1_1(bound)
    };
    Ok(selected)
}

struct Caps {
    frame: usize,
    value: ValueLimits,
    json: jcs::Limits,
}

impl Caps {
    fn new(host: &v1_1::ProtocolLimits) -> Result<Self> {
        let frame = usize::try_from(host.frame_bytes).map_err(|_| EncodeError::Limit)?;
        let decoded =
            usize::try_from(host.aggregate_decoded_bytes).map_err(|_| EncodeError::Limit)?;
        let elements = usize::try_from(host.value_elements).map_err(|_| EncodeError::Limit)?;
        let string_bytes = usize::try_from(host.string_bytes).map_err(|_| EncodeError::Limit)?;
        let bytes_length = usize::try_from(host.bytes_length).map_err(|_| EncodeError::Limit)?;
        if frame == 0 || decoded == 0 || host.value_depth == 0 || elements == 0 || string_bytes == 0
        {
            return Err(EncodeError::Limit);
        }
        let json_bytes = frame.min(decoded);
        let value = ValueLimits {
            depth: host.value_depth as usize,
            elements,
            string_bytes,
            bytes_length,
            decoded_bytes: decoded,
            json_bytes,
        };
        let json = jcs::Limits {
            input_bytes: frame,
            depth: host.value_depth as usize,
            nodes: elements,
            elements,
            decoded_bytes: decoded,
            output_bytes: json_bytes,
            schema_work: decoded.saturating_mul(8).min(usize::MAX - 1),
            instance_work: decoded.saturating_mul(8).min(usize::MAX - 1),
        };
        Ok(Self { frame, value, json })
    }
}

struct Meter<'a> {
    caps: &'a Caps,
    decoded: usize,
    elements: usize,
}

impl<'a> Meter<'a> {
    fn new(caps: &'a Caps) -> Self {
        Self {
            caps,
            decoded: 0,
            elements: 0,
        }
    }
    fn charge(&mut self, amount: usize) -> Result<()> {
        self.decoded = self.decoded.checked_add(amount).ok_or(EncodeError::Limit)?;
        if self.decoded > self.caps.value.decoded_bytes {
            return Err(EncodeError::Limit);
        }
        Ok(())
    }
    fn count(&mut self, amount: usize) -> Result<()> {
        self.elements = self
            .elements
            .checked_add(amount)
            .ok_or(EncodeError::Limit)?;
        if self.elements > self.caps.value.elements {
            return Err(EncodeError::Limit);
        }
        Ok(())
    }
    fn depth(&self, depth: usize) -> Result<()> {
        if depth > self.caps.value.depth.min(HARD_DEPTH_MAX) {
            return Err(EncodeError::Limit);
        }
        Ok(())
    }
    fn measure_map(&mut self, map: &JsonObject, depth: usize) -> Result<usize> {
        self.depth(depth)?;
        self.charge(size_of::<v1_1::WireValue>())?;
        self.count(map.as_map().len())?;
        let mut inner = 0usize;
        for (key, child) in map.as_map() {
            if key.len() > self.caps.value.string_bytes {
                return Err(EncodeError::Limit);
            }
            self.charge(size_of::<v1_1::WireMapEntry>())?;
            self.charge(key.len())?;
            let child_len = self.measure(child, depth + 1)?;
            let entry_len = field_len(key.len())?
                .checked_add(field_len(child_len)?)
                .ok_or(EncodeError::Limit)?;
            inner = inner
                .checked_add(field_len(entry_len)?)
                .ok_or(EncodeError::Limit)?;
        }
        field_len(inner)
    }
    fn measure(&mut self, value: &JsonValue, depth: usize) -> Result<usize> {
        self.depth(depth)?;
        match value {
            JsonValue::Object(map) => self.measure_map(map, depth),
            _ => {
                self.charge(size_of::<v1_1::WireValue>())?;
                match value {
                    JsonValue::Null | JsonValue::Bool(_) => Ok(2),
                    JsonValue::Number(JsonNumber::Unsigned(number)) => {
                        let n =
                            i64::try_from(number.get()).map_err(|_| EncodeError::InvalidNumber)?;
                        if number.get() > JS_SAFE_INTEGER_MAX {
                            return Err(EncodeError::InvalidNumber);
                        }
                        Ok(1 + varint_len(zigzag(n)))
                    }
                    JsonValue::Number(JsonNumber::Signed(number)) => {
                        let n = number.get();
                        if n.unsigned_abs() > JS_SAFE_INTEGER_MAX {
                            return Err(EncodeError::InvalidNumber);
                        }
                        Ok(1 + varint_len(zigzag(n)))
                    }
                    JsonValue::Number(JsonNumber::Float(number)) => {
                        if !number.get().is_finite() {
                            return Err(EncodeError::InvalidNumber);
                        }
                        Ok(9)
                    }
                    JsonValue::String(text) => {
                        if text.len() > self.caps.value.string_bytes {
                            return Err(EncodeError::Limit);
                        }
                        self.charge(text.len())?;
                        field_len(text.len())
                    }
                    JsonValue::Array(items) => {
                        self.count(items.len())?;
                        let mut inner = 0usize;
                        for item in items {
                            inner = inner
                                .checked_add(field_len(self.measure(item, depth + 1)?)?)
                                .ok_or(EncodeError::Limit)?;
                        }
                        field_len(inner)
                    }
                    JsonValue::Object(_) => unreachable!(),
                }
            }
        }
    }
}

fn varint_len(mut n: u64) -> usize {
    let mut len = 1;
    while n >= 128 {
        n >>= 7;
        len += 1;
    }
    len
}
fn zigzag(n: i64) -> u64 {
    ((n as u64) << 1) ^ ((n >> 63) as u64)
}
fn field_len(payload: usize) -> Result<usize> {
    let length = u64::try_from(payload).map_err(|_| EncodeError::Limit)?;
    1usize
        .checked_add(varint_len(length))
        .and_then(|n| n.checked_add(payload))
        .ok_or(EncodeError::Limit)
}
fn copy_string(source: &str) -> Result<String> {
    let mut out = String::new();
    out.try_reserve_exact(source.len())
        .map_err(|_| EncodeError::Allocation)?;
    out.push_str(source);
    Ok(out)
}

macro_rules! build_version {
    ($module:ident,$map_fn:ident,$value_fn:ident) => {
        fn $map_fn(map: &JsonObject) -> Result<$module::WireValue> {
            let mut entries = Vec::new();
            entries
                .try_reserve_exact(map.as_map().len())
                .map_err(|_| EncodeError::Allocation)?;
            for (key, value) in map.as_map() {
                entries.push($module::WireMapEntry {
                    key: copy_string(key)?,
                    value: Some($value_fn(value)?),
                });
            }
            Ok($module::WireValue {
                value: Some($module::wire_value::Value::MapValue($module::WireMap {
                    entries,
                })),
            })
        }
        fn $value_fn(value: &JsonValue) -> Result<$module::WireValue> {
            use $module::wire_value::Value;
            let arm = match value {
                JsonValue::Null => Value::NullValue($module::NullValue {}),
                JsonValue::Bool(v) => Value::BoolValue(*v),
                JsonValue::Number(JsonNumber::Unsigned(v)) => Value::IntegerValue(
                    i64::try_from(v.get()).map_err(|_| EncodeError::InvalidNumber)?,
                ),
                JsonValue::Number(JsonNumber::Signed(v)) => Value::IntegerValue(v.get()),
                JsonValue::Number(JsonNumber::Float(v)) => Value::FloatValue(v.get()),
                JsonValue::String(v) => Value::TextValue(copy_string(v)?),
                JsonValue::Array(items) => {
                    let mut encoded = Vec::new();
                    encoded
                        .try_reserve_exact(items.len())
                        .map_err(|_| EncodeError::Allocation)?;
                    for item in items {
                        encoded.push($value_fn(item)?);
                    }
                    Value::ListValue($module::WireList { items: encoded })
                }
                JsonValue::Object(map) => return $map_fn(map),
            };
            Ok($module::WireValue { value: Some(arm) })
        }
    };
}
build_version!(v1_0, build_v1_0_map, build_v1_0_value);
build_version!(v1_1, build_v1_1_map, build_v1_1_value);
