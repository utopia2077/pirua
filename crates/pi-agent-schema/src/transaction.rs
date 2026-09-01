//! Ordered atomic write transactions and commit results.

use std::collections::BTreeMap;

use pi_ai_core::{JsonObject, JsonValue, SafeUnsigned};
use serde::de;
use serde::ser;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::entry::{NewEntry, NewUsageRow};
use crate::register::{RegisterNamespace, RegisterSet};
use crate::serde_support::from_json_value;
use crate::state::v1::{OperationState, RawOperationState};
use crate::{ProviderHeaderClassificationError, ProviderHeaderPolicy};

/// Register deletion key. Absence after this write is distinct from a null value.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterDelete {
    /// Closed register namespace.
    pub namespace: RegisterNamespace,
    /// Namespace-local key.
    pub key: String,
}

/// One ordered transaction write.
#[derive(Clone, Debug, PartialEq)]
pub enum Write<S = OperationState> {
    /// Insert an immutable conversation entry.
    Entry(NewEntry),
    /// Insert an append-only usage row.
    Usage(NewUsageRow),
    /// Set a namespace/value-coupled register.
    SetRegister(Box<RegisterSet<S>>),
    /// Delete a register by namespace and key.
    DeleteRegister(RegisterDelete),
}

impl<S> Serialize for Write<S>
where
    S: Serialize,
{
    fn serialize<Ser>(&self, serializer: Ser) -> Result<Ser::Ok, Ser::Error>
    where
        Ser: Serializer,
    {
        match self {
            Self::Entry(entry) => EntryWriteRef {
                kind: EntryWriteKind::Entry,
                entry,
            }
            .serialize(serializer),
            Self::Usage(row) => UsageWriteRef {
                kind: UsageWriteKind::Usage,
                row,
            }
            .serialize(serializer),
            Self::SetRegister(register) => serialize_register_write(register, "set", serializer),
            Self::DeleteRegister(register) => {
                let value = serde_json::to_value(register).map_err(ser::Error::custom)?;
                serialize_register_value(value, "delete", serializer)
            }
        }
    }
}

fn serialize_register_write<S, R>(
    register: &RegisterSet<R>,
    operation: &'static str,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    R: Serialize,
{
    let value = serde_json::to_value(register).map_err(ser::Error::custom)?;
    serialize_register_value(value, operation, serializer)
}

fn serialize_register_value<S>(
    value: serde_json::Value,
    operation: &'static str,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let serde_json::Value::Object(mut object) = value else {
        return Err(ser::Error::custom(
            "register write must serialize as an object",
        ));
    };
    object.insert(
        "kind".to_owned(),
        serde_json::Value::String("register".to_owned()),
    );
    object.insert(
        "op".to_owned(),
        serde_json::Value::String(operation.to_owned()),
    );
    object.serialize(serializer)
}

impl<'de, S> Deserialize<'de> for Write<S>
where
    S: serde::de::DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = JsonValue::deserialize(deserializer)?;
        let JsonValue::Object(object) = value else {
            return Err(de::Error::custom("transaction write must be an object"));
        };
        let kind = match object.as_map().get("kind") {
            Some(JsonValue::String(kind)) => kind.as_str(),
            Some(_) => return Err(de::Error::custom("write kind must be a string")),
            None => return Err(de::Error::missing_field("kind")),
        };
        match kind {
            "entry" => from_json_value(JsonValue::Object(object))
                .map(|wire: EntryWriteWire| Write::Entry(wire.entry)),
            "usage" => from_json_value(JsonValue::Object(object))
                .map(|wire: UsageWriteWire| Write::Usage(wire.row)),
            "register" => deserialize_register_write::<D::Error, S>(object),
            _ => Err(de::Error::unknown_variant(
                kind,
                &["entry", "usage", "register"],
            )),
        }
    }
}

fn deserialize_register_write<E, S>(object: JsonObject) -> Result<Write<S>, E>
where
    E: de::Error,
    S: serde::de::DeserializeOwned,
{
    let operation = match object.as_map().get("op") {
        Some(JsonValue::String(operation)) => operation.clone(),
        Some(_) => return Err(E::custom("register operation must be a string")),
        None => return Err(E::missing_field("op")),
    };
    let mut stripped: BTreeMap<String, JsonValue> = object.into_map();
    stripped.remove("kind");
    stripped.remove("op");
    match operation.as_str() {
        "set" => from_json_value::<RegisterSet<S>, E>(JsonValue::Object(JsonObject::new(stripped)))
            .map(Box::new)
            .map(Write::SetRegister),
        "delete" => {
            from_json_value(JsonValue::Object(JsonObject::new(stripped))).map(Write::DeleteRegister)
        }
        _ => Err(E::unknown_variant(&operation, &["set", "delete"])),
    }
}

#[derive(Serialize)]
struct EntryWriteRef<'a> {
    kind: EntryWriteKind,
    entry: &'a NewEntry,
}

#[derive(Clone, Copy, Serialize)]
enum EntryWriteKind {
    #[serde(rename = "entry")]
    Entry,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryWriteWire {
    #[serde(rename = "kind")]
    _kind: EntryWriteKindWire,
    entry: NewEntry,
}

#[derive(Deserialize)]
enum EntryWriteKindWire {
    #[serde(rename = "entry")]
    Entry,
}

#[derive(Serialize)]
struct UsageWriteRef<'a> {
    kind: UsageWriteKind,
    row: &'a NewUsageRow,
}

#[derive(Clone, Copy, Serialize)]
enum UsageWriteKind {
    #[serde(rename = "usage")]
    Usage,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UsageWriteWire {
    #[serde(rename = "kind")]
    _kind: UsageWriteKindWire,
    row: NewUsageRow,
}

#[derive(Deserialize)]
enum UsageWriteKindWire {
    #[serde(rename = "usage")]
    Usage,
}

/// Ordered atomic transaction.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(
    deny_unknown_fields,
    bound(
        serialize = "S: Serialize",
        deserialize = "S: serde::de::DeserializeOwned"
    )
)]
pub struct Transaction<S = OperationState> {
    /// Writes applied in strict sequence order.
    pub writes: Vec<Write<S>>,
}

/// Storage-assigned commit metadata.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CommitResult {
    /// First assigned sequence, including zero for a zero-based store.
    pub first_seq: SafeUnsigned,
    /// Assigned sequences in exact write order.
    pub seqs: Vec<SafeUnsigned>,
    /// Storage-assigned commit timestamp in Unix milliseconds.
    pub timestamp: SafeUnsigned,
}

/// Write root whose operation state still contains raw stream options.
pub type RawWrite = Write<RawOperationState>;

/// Transaction root whose operation state still contains raw stream options.
pub type RawTransaction = Transaction<RawOperationState>;

impl Write<RawOperationState> {
    /// Classifies nested raw operation-state headers under one host policy.
    pub fn into_durable(
        self,
        policy: &ProviderHeaderPolicy,
    ) -> Result<Write, ProviderHeaderClassificationError> {
        match self {
            Self::Entry(entry) => Ok(Write::Entry(entry)),
            Self::Usage(row) => Ok(Write::Usage(row)),
            Self::SetRegister(register) => register
                .into_durable(policy)
                .map(Box::new)
                .map(Write::SetRegister),
            Self::DeleteRegister(register) => Ok(Write::DeleteRegister(register)),
        }
    }
}

impl Transaction<RawOperationState> {
    /// Classifies nested raw operation-state headers under one host policy.
    pub fn into_durable(
        self,
        policy: &ProviderHeaderPolicy,
    ) -> Result<Transaction, ProviderHeaderClassificationError> {
        self.writes
            .into_iter()
            .map(|write| write.into_durable(policy))
            .collect::<Result<Vec<_>, _>>()
            .map(|writes| Transaction { writes })
    }
}
