//! Bounded admission of decoded wire values and selected JSON projections.
//!
//! A caller must cap the physical frame and protobuf decode before invoking
//! this module. These checks constrain a decoded tree and its JSON projection.

use std::collections::HashSet;
use std::fmt;
use std::io::{self, Write};

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

use super::{v1_0, v1_1};
use crate::jcs::{self, PortableSchema};

const MAX_SAFE_INTEGER: i64 = 9_007_199_254_740_991;
const MAX_WALK_DEPTH: usize = 64;

/// Host-frozen finite ceilings. `decoded_bytes` charges the DTO size of every
/// node and map entry plus UTF-8 key/text and exact byte-arm lengths. It is a
/// logical decoded-size bound; the endpoint must also bound its protobuf frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub depth: usize,
    pub elements: usize,
    pub string_bytes: usize,
    pub bytes_length: usize,
    pub decoded_bytes: usize,
    pub json_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            depth: 64,
            elements: 16_384,
            string_bytes: 1_048_576,
            bytes_length: 1_048_576,
            decoded_bytes: 1_048_576,
            json_bytes: 1_048_576,
        }
    }
}

/// Errors never include a peer value, map key, or projection identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    Limit,
    MissingValue,
    DuplicateKey,
    UnsafeInteger,
    NonFiniteFloat,
    BytesNotJson,
    ProjectionMismatch,
    JsonEncoding,
    Schema(jcs::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Limit => "wire value limit exceeded",
            Self::MissingValue => "wire value missing",
            Self::DuplicateKey => "duplicate wire map key",
            Self::UnsafeInteger => "unsafe wire integer",
            Self::NonFiniteFloat => "nonfinite wire float",
            Self::BytesNotJson => "wire bytes cannot enter JSON projection",
            Self::ProjectionMismatch => "schema projection mismatch",
            Self::JsonEncoding => "wire JSON encoding failed",
            Self::Schema(_) => "wire schema instance rejected",
        })
    }
}

impl std::error::Error for Error {}

/// Host-selected identity and already-admitted schema for one dynamic projection.
pub struct Projection<'a> {
    pub id: &'a str,
    pub digest_sha256: [u8; 32],
    pub schema: &'a PortableSchema,
}

/// Byte arms require an explicit profile decision by the trusted caller.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BytePolicy {
    JsonOnly,
    ReviewedByteProfile,
}

enum Atom<'a, V, E> {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    Text(&'a str),
    Bytes(&'a [u8]),
    List(&'a [V]),
    Map(&'a [E]),
}

trait Entry<V> {
    fn key(&self) -> &str;
    fn value(&self) -> Option<&V>;
}

trait Tree: Sized {
    type Entry: Entry<Self>;
    fn atom(&self) -> Option<Atom<'_, Self, Self::Entry>>;
}

macro_rules! impl_tree {
    ($version:ident) => {
        impl Entry<$version::WireValue> for $version::WireMapEntry {
            fn key(&self) -> &str {
                &self.key
            }
            fn value(&self) -> Option<&$version::WireValue> {
                self.value.as_ref()
            }
        }

        impl Tree for $version::WireValue {
            type Entry = $version::WireMapEntry;
            fn atom(&self) -> Option<Atom<'_, Self, Self::Entry>> {
                use $version::wire_value::Value;
                Some(match self.value.as_ref()? {
                    Value::NullValue(_) => Atom::Null,
                    Value::BoolValue(value) => Atom::Bool(*value),
                    Value::IntegerValue(value) => Atom::Integer(*value),
                    Value::FloatValue(value) => Atom::Float(*value),
                    Value::TextValue(value) => Atom::Text(value),
                    Value::BytesValue(value) => Atom::Bytes(value),
                    Value::ListValue(value) => Atom::List(&value.items),
                    Value::MapValue(value) => Atom::Map(&value.entries),
                })
            }
        }
    };
}

impl_tree!(v1_0);
impl_tree!(v1_1);

struct Meter {
    limits: Limits,
    elements: usize,
    decoded: usize,
    allow_bytes: bool,
}

impl Meter {
    fn charge(&mut self, amount: usize) -> Result<(), Error> {
        self.decoded = self.decoded.checked_add(amount).ok_or(Error::Limit)?;
        if self.decoded > self.limits.decoded_bytes {
            return Err(Error::Limit);
        }
        Ok(())
    }

    fn elements(&mut self, amount: usize) -> Result<(), Error> {
        self.elements = self.elements.checked_add(amount).ok_or(Error::Limit)?;
        if self.elements > self.limits.elements {
            return Err(Error::Limit);
        }
        Ok(())
    }

    fn walk<V: Tree>(&mut self, value: &V, depth: usize) -> Result<(), Error> {
        if depth > self.limits.depth.min(MAX_WALK_DEPTH) {
            return Err(Error::Limit);
        }
        self.charge(std::mem::size_of::<V>())?;
        match value.atom().ok_or(Error::MissingValue)? {
            Atom::Null | Atom::Bool(_) => Ok(()),
            Atom::Integer(number) if (-MAX_SAFE_INTEGER..=MAX_SAFE_INTEGER).contains(&number) => {
                Ok(())
            }
            Atom::Integer(_) => Err(Error::UnsafeInteger),
            Atom::Float(number) if number.is_finite() => Ok(()),
            Atom::Float(_) => Err(Error::NonFiniteFloat),
            Atom::Text(text) => {
                if text.len() > self.limits.string_bytes {
                    return Err(Error::Limit);
                }
                self.charge(text.len())
            }
            Atom::Bytes(bytes) => {
                if !self.allow_bytes {
                    return Err(Error::BytesNotJson);
                }
                if bytes.len() > self.limits.bytes_length {
                    return Err(Error::Limit);
                }
                self.charge(bytes.len())
            }
            Atom::List(items) => {
                self.elements(items.len())?;
                for item in items {
                    self.walk(item, depth + 1)?;
                }
                Ok(())
            }
            Atom::Map(entries) => {
                self.elements(entries.len())?;
                let mut keys = HashSet::new();
                for entry in entries {
                    self.charge(std::mem::size_of::<V::Entry>())?;
                    let key = entry.key();
                    if key.len() > self.limits.string_bytes {
                        return Err(Error::Limit);
                    }
                    self.charge(key.len())?;
                    if !keys.insert(key) {
                        return Err(Error::DuplicateKey);
                    }
                    self.walk(entry.value().ok_or(Error::MissingValue)?, depth + 1)?;
                }
                Ok(())
            }
        }
    }
}

fn admit<V: Tree>(value: &V, limits: Limits, allow_bytes: bool) -> Result<(), Error> {
    Meter {
        limits,
        elements: 0,
        decoded: 0,
        allow_bytes,
    }
    .walk(value, 1)
}

/// Admit the closed wire type under the caller's reviewed byte policy.
pub fn admit_v1_0(value: &v1_0::WireValue, limits: Limits, bytes: BytePolicy) -> Result<(), Error> {
    admit(value, limits, bytes == BytePolicy::ReviewedByteProfile)
}

/// Admit the closed wire type under the caller's reviewed byte policy.
pub fn admit_v1_1(value: &v1_1::WireValue, limits: Limits, bytes: BytePolicy) -> Result<(), Error> {
    admit(value, limits, bytes == BytePolicy::ReviewedByteProfile)
}

struct JsonValue<'a, V>(&'a V);

impl<V: Tree> Serialize for JsonValue<'_, V> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self
            .0
            .atom()
            .ok_or_else(|| serde::ser::Error::custom("missing value"))?
        {
            Atom::Null => serializer.serialize_none(),
            Atom::Bool(value) => serializer.serialize_bool(value),
            Atom::Integer(value) => serializer.serialize_i64(value),
            Atom::Float(value) => serializer.serialize_f64(value),
            Atom::Text(value) => serializer.serialize_str(value),
            Atom::Bytes(_) => Err(serde::ser::Error::custom("bytes not JSON")),
            Atom::List(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(&JsonValue(item))?;
                }
                seq.end()
            }
            Atom::Map(entries) => {
                let mut map = serializer.serialize_map(Some(entries.len()))?;
                for entry in entries {
                    let value = entry
                        .value()
                        .ok_or_else(|| serde::ser::Error::custom("missing value"))?;
                    map.serialize_entry(entry.key(), &JsonValue(value))?;
                }
                map.end()
            }
        }
    }
}

struct BoundedWriter {
    bytes: Vec<u8>,
    max: usize,
    exceeded: bool,
}

impl Write for BoundedWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let next = self.bytes.len().checked_add(buf.len());
        if next.is_none_or(|next| next > self.max) {
            self.exceeded = true;
            return Err(io::Error::other("JSON limit exceeded"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn bound<V: Tree>(
    value: Option<&V>,
    id: &str,
    digest: &[u8],
    expected: &Projection<'_>,
    value_limits: Limits,
    instance_limits: jcs::Limits,
) -> Result<Vec<u8>, Error> {
    if id != expected.id
        || digest != expected.digest_sha256
        || expected.schema.canonical().sha256() != expected.digest_sha256
    {
        return Err(Error::ProjectionMismatch);
    }
    let value = value.ok_or(Error::MissingValue)?;
    admit(value, value_limits, false)?;
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        max: value_limits.json_bytes,
        exceeded: false,
    };
    if serde_json::to_writer(&mut writer, &JsonValue(value)).is_err() {
        return Err(if writer.exceeded {
            Error::Limit
        } else {
            Error::JsonEncoding
        });
    }
    expected
        .schema
        .validate_instance(&writer.bytes, instance_limits)
        .map_err(Error::Schema)?;
    Ok(writer.bytes)
}

/// Check selected 1.0 identity, structure, JSON subset and portable instance.
pub fn admit_schema_bound_v1_0(
    value: &v1_0::SchemaBoundValue,
    expected: &Projection<'_>,
    value_limits: Limits,
    instance_limits: jcs::Limits,
) -> Result<Vec<u8>, Error> {
    bound(
        value.value.as_ref(),
        &value.projection_id,
        &value.schema_digest_sha256,
        expected,
        value_limits,
        instance_limits,
    )
}

/// Check selected 1.1 identity, structure, JSON subset and portable instance.
pub fn admit_schema_bound_v1_1(
    value: &v1_1::SchemaBoundValue,
    expected: &Projection<'_>,
    value_limits: Limits,
    instance_limits: jcs::Limits,
) -> Result<Vec<u8>, Error> {
    bound(
        value.value.as_ref(),
        &value.projection_id,
        &value.schema_digest_sha256,
        expected,
        value_limits,
        instance_limits,
    )
}
