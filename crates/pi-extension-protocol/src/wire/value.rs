//! Bounded admission of decoded wire values and selected JSON projections.
//!
//! A caller must cap the physical frame and protobuf decode before invoking
//! this module. These checks constrain a decoded tree and its JSON projection.

use std::collections::HashSet;
use std::fmt;
use std::io::{self, Write};

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

use super::echo_bounded::{DirectMessages, IndexedMessage, MessageId, SelectedMinor};
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

enum Atom<'a, N, E> {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    Text(&'a str),
    Bytes(&'a [u8]),
    List(N),
    Map(E),
}

trait Cursor {
    type Node<'a>: Copy
    where
        Self: 'a;
    type Entry<'a>: Copy
    where
        Self: 'a;
    type Nodes<'a>: Iterator<Item = Self::Node<'a>> + Clone
    where
        Self: 'a;
    type Entries<'a>: Iterator<Item = Self::Entry<'a>> + Clone
    where
        Self: 'a;

    fn node_size(&self) -> usize;
    fn entry_size(&self) -> usize;
    fn atom<'a>(
        &'a self,
        node: Self::Node<'a>,
    ) -> Option<Atom<'a, Self::Nodes<'a>, Self::Entries<'a>>>;
    fn entry<'a>(&'a self, entry: Self::Entry<'a>) -> (&'a str, Option<Self::Node<'a>>);
}

macro_rules! owned_cursor {
    ($cursor:ident, $version:ident) => {
        struct $cursor;

        impl Cursor for $cursor {
            type Node<'a> = &'a $version::WireValue;
            type Entry<'a> = &'a $version::WireMapEntry;
            type Nodes<'a> = std::slice::Iter<'a, $version::WireValue>;
            type Entries<'a> = std::slice::Iter<'a, $version::WireMapEntry>;

            fn node_size(&self) -> usize {
                std::mem::size_of::<$version::WireValue>()
            }
            fn entry_size(&self) -> usize {
                std::mem::size_of::<$version::WireMapEntry>()
            }
            fn atom<'a>(
                &'a self,
                node: Self::Node<'a>,
            ) -> Option<Atom<'a, Self::Nodes<'a>, Self::Entries<'a>>> {
                use $version::wire_value::Value;
                Some(match node.value.as_ref()? {
                    Value::NullValue(_) => Atom::Null,
                    Value::BoolValue(value) => Atom::Bool(*value),
                    Value::IntegerValue(value) => Atom::Integer(*value),
                    Value::FloatValue(value) => Atom::Float(*value),
                    Value::TextValue(value) => Atom::Text(value),
                    Value::BytesValue(value) => Atom::Bytes(value),
                    Value::ListValue(value) => Atom::List(value.items.iter()),
                    Value::MapValue(value) => Atom::Map(value.entries.iter()),
                })
            }
            fn entry<'a>(&'a self, entry: Self::Entry<'a>) -> (&'a str, Option<Self::Node<'a>>) {
                (&entry.key, entry.value.as_ref())
            }
        }
    };
}

owned_cursor!(Owned10, v1_0);
owned_cursor!(Owned11, v1_1);

struct IndexedCursor<'view, 'body> {
    view: &'view IndexedMessage<'view, 'body>,
    minor: SelectedMinor,
}

impl<'view, 'body> Cursor for IndexedCursor<'view, 'body> {
    type Node<'a>
        = MessageId
    where
        Self: 'a;
    type Entry<'a>
        = MessageId
    where
        Self: 'a;
    type Nodes<'a>
        = DirectMessages<'a, 'view, 'body>
    where
        Self: 'a;
    type Entries<'a>
        = DirectMessages<'a, 'view, 'body>
    where
        Self: 'a;

    fn node_size(&self) -> usize {
        match self.minor {
            SelectedMinor::V1_0 => std::mem::size_of::<v1_0::WireValue>(),
            SelectedMinor::V1_1 => std::mem::size_of::<v1_1::WireValue>(),
        }
    }

    fn entry_size(&self) -> usize {
        match self.minor {
            SelectedMinor::V1_0 => std::mem::size_of::<v1_0::WireMapEntry>(),
            SelectedMinor::V1_1 => std::mem::size_of::<v1_1::WireMapEntry>(),
        }
    }

    fn atom<'a>(
        &'a self,
        node: Self::Node<'a>,
    ) -> Option<Atom<'a, Self::Nodes<'a>, Self::Entries<'a>>> {
        let view = self.view;
        Some(match view.oneof_tag(node)? {
            1 => Atom::Null,
            2 => Atom::Bool(view.scalar(node, 2)? != 0),
            3 => {
                let raw = view.scalar(node, 3)?;
                Atom::Integer(((raw >> 1) as i64) ^ (-((raw & 1) as i64)))
            }
            4 => Atom::Float(f64::from_bits(view.fixed64(node, 4)?)),
            5 => Atom::Text(view.text(node, 5)?),
            6 => Atom::Bytes(view.bytes(node, 6)?),
            7 => Atom::List(view.direct_messages(view.message(node, 7)?, 1)),
            8 => Atom::Map(view.direct_messages(view.message(node, 8)?, 1)),
            _ => return None,
        })
    }

    fn entry<'a>(&'a self, entry: Self::Entry<'a>) -> (&'a str, Option<Self::Node<'a>>) {
        (
            self.view.text(entry, 1).unwrap_or(""),
            self.view.message(entry, 2),
        )
    }
}

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

    fn walk<'a, C: Cursor>(
        &mut self,
        cursor: &'a C,
        value: C::Node<'a>,
        depth: usize,
    ) -> Result<(), Error> {
        if depth > self.limits.depth.min(MAX_WALK_DEPTH) {
            return Err(Error::Limit);
        }
        self.charge(cursor.node_size())?;
        match cursor.atom(value).ok_or(Error::MissingValue)? {
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
                self.elements(items.clone().count())?;
                for item in items {
                    self.walk(cursor, item, depth + 1)?;
                }
                Ok(())
            }
            Atom::Map(entries) => {
                self.elements(entries.clone().count())?;
                let mut keys = HashSet::new();
                for entry in entries {
                    self.charge(cursor.entry_size())?;
                    let (key, value) = cursor.entry(entry);
                    if key.len() > self.limits.string_bytes {
                        return Err(Error::Limit);
                    }
                    self.charge(key.len())?;
                    if !keys.insert(key) {
                        return Err(Error::DuplicateKey);
                    }
                    self.walk(cursor, value.ok_or(Error::MissingValue)?, depth + 1)?;
                }
                Ok(())
            }
        }
    }
}

fn admit<'a, C: Cursor>(
    cursor: &'a C,
    value: C::Node<'a>,
    limits: Limits,
    allow_bytes: bool,
) -> Result<(), Error> {
    Meter {
        limits,
        elements: 0,
        decoded: 0,
        allow_bytes,
    }
    .walk(cursor, value, 1)
}

/// Admit the closed wire type under the caller's reviewed byte policy.
pub fn admit_v1_0(value: &v1_0::WireValue, limits: Limits, bytes: BytePolicy) -> Result<(), Error> {
    admit(
        &Owned10,
        value,
        limits,
        bytes == BytePolicy::ReviewedByteProfile,
    )
}

/// Admit the closed wire type under the caller's reviewed byte policy.
pub fn admit_v1_1(value: &v1_1::WireValue, limits: Limits, bytes: BytePolicy) -> Result<(), Error> {
    admit(
        &Owned11,
        value,
        limits,
        bytes == BytePolicy::ReviewedByteProfile,
    )
}

struct JsonValue<'a, C: Cursor>(&'a C, C::Node<'a>);

impl<C: Cursor> Serialize for JsonValue<'_, C> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self
            .0
            .atom(self.1)
            .ok_or_else(|| serde::ser::Error::custom("missing value"))?
        {
            Atom::Null => serializer.serialize_none(),
            Atom::Bool(value) => serializer.serialize_bool(value),
            Atom::Integer(value) => serializer.serialize_i64(value),
            Atom::Float(value) => serializer.serialize_f64(value),
            Atom::Text(value) => serializer.serialize_str(value),
            Atom::Bytes(_) => Err(serde::ser::Error::custom("bytes not JSON")),
            Atom::List(items) => {
                let mut seq = serializer.serialize_seq(Some(items.clone().count()))?;
                for item in items {
                    seq.serialize_element(&JsonValue(self.0, item))?;
                }
                seq.end()
            }
            Atom::Map(entries) => {
                let mut map = serializer.serialize_map(Some(entries.clone().count()))?;
                for entry in entries {
                    let (key, value) = self.0.entry(entry);
                    let value = value.ok_or_else(|| serde::ser::Error::custom("missing value"))?;
                    map.serialize_entry(key, &JsonValue(self.0, value))?;
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

fn bound<'a, C: Cursor>(
    cursor: &'a C,
    value: Option<C::Node<'a>>,
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
    admit(cursor, value, value_limits, false)?;
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        max: value_limits.json_bytes,
        exceeded: false,
    };
    if serde_json::to_writer(&mut writer, &JsonValue(cursor, value)).is_err() {
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
        &Owned10,
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
        &Owned11,
        value.value.as_ref(),
        &value.projection_id,
        &value.schema_digest_sha256,
        expected,
        value_limits,
        instance_limits,
    )
}

/// Admit a scanner-validated child of ToolProgress or InvokeToolResult.
/// The caller keeps the child ID private to the current inner scan.
pub(super) fn admit_indexed_schema_bound(
    view: &IndexedMessage<'_, '_>,
    bound_id: MessageId,
    minor: SelectedMinor,
    expected: &Projection<'_>,
    value_limits: Limits,
    instance_limits: jcs::Limits,
) -> Result<Vec<u8>, Error> {
    let cursor = IndexedCursor { view, minor };
    bound(
        &cursor,
        view.message(bound_id, 3),
        view.text(bound_id, 1).unwrap_or(""),
        view.bytes(bound_id, 2).unwrap_or(&[]),
        expected,
        value_limits,
        instance_limits,
    )
}
