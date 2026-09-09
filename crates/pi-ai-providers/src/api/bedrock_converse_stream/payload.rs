//! The Bedrock logical command retains bytes until the SDK schema consumes them.
use pi_ai_core::FiniteF64;
use serde_json::Value;
use std::fmt;

#[derive(Clone, PartialEq)]
/// Process-local typed payload; persistence requires an explicit lossy JSON copy.
/// ```compile_fail
/// fn persist<T: serde::Serialize>() {}
/// persist::<pi_ai_providers::api::bedrock_converse_stream::BedrockValue>();
/// ```
/// ```compile_fail
/// fn restore<T: for<'de> serde::Deserialize<'de>>() {}
/// restore::<pi_ai_providers::api::bedrock_converse_stream::BedrockValue>();
/// ```
pub enum BedrockValue {
    Null,
    Bool(bool),
    Number(FiniteF64),
    String(String),
    Array(Vec<BedrockValue>),
    Object(BedrockObject),
    Bytes(Vec<u8>),
}
#[derive(Clone, Default, PartialEq)]
/// Process-local typed payload; persistence requires an explicit lossy JSON copy.
/// ```compile_fail
/// fn persist<T: serde::Serialize>() {}
/// persist::<pi_ai_providers::api::bedrock_converse_stream::BedrockObject>();
/// ```
/// ```compile_fail
/// fn restore<T: for<'de> serde::Deserialize<'de>>() {}
/// restore::<pi_ai_providers::api::bedrock_converse_stream::BedrockObject>();
/// ```
pub struct BedrockObject(Vec<(String, BedrockValue)>);
impl BedrockObject {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn get(&self, name: &str) -> Option<&BedrockValue> {
        self.0.iter().find(|(k, _)| k == name).map(|(_, v)| v)
    }
    pub fn get_mut(&mut self, name: &str) -> Option<&mut BedrockValue> {
        self.0.iter_mut().find(|(k, _)| k == name).map(|(_, v)| v)
    }
    /// Exact own-key replacement preserves its original enumeration position.
    pub fn insert(&mut self, name: String, value: BedrockValue) -> Option<BedrockValue> {
        if let Some(old) = self.get_mut(&name) {
            return Some(std::mem::replace(old, value));
        }
        self.0.push((name, value));
        self.0.sort_by_key(|(name, _)| {
            name.parse::<u32>()
                .ok()
                .filter(|n| *n != u32::MAX && n.to_string() == *name)
                .map_or((1, 0), |n| (0, n))
        });
        None
    }
    pub fn remove(&mut self, name: &str) -> Option<BedrockValue> {
        self.0
            .iter()
            .position(|(k, _)| k == name)
            .map(|i| self.0.remove(i).1)
    }
    pub fn iter(&self) -> impl Iterator<Item = (&str, &BedrockValue)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}
impl BedrockValue {
    pub fn from_json(value: Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(v) => Self::Bool(v),
            Value::Number(v) => Self::Number(
                FiniteF64::new(v.as_f64().expect("finite JSON number"))
                    .expect("finite JSON number"),
            ),
            Value::String(v) => Self::String(v),
            Value::Array(v) => Self::Array(v.into_iter().map(Self::from_json).collect()),
            Value::Object(v) => {
                let mut object = BedrockObject::new();
                for (k, v) in v {
                    object.insert(k, Self::from_json(v));
                }
                Self::Object(object)
            }
        }
    }
    /// Explicit JSON cloning loses the typed byte distinction, as JSON.stringify
    /// does for a source Uint8Array. Numeric objects never restore that provenance.
    pub fn to_json(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Bool(v) => Value::Bool(*v),
            Self::Number(v) => serde_json::json!(v.get()),
            Self::String(v) => Value::String(v.clone()),
            Self::Array(v) => Value::Array(v.iter().map(Self::to_json).collect()),
            Self::Object(v) => {
                Value::Object(v.iter().map(|(k, v)| (k.to_owned(), v.to_json())).collect())
            }
            Self::Bytes(bytes) => Value::Object(
                bytes
                    .iter()
                    .enumerate()
                    .map(|(i, b)| (i.to_string(), Value::from(*b)))
                    .collect(),
            ),
        }
    }
    pub fn as_object(&self) -> Option<&BedrockObject> {
        if let Self::Object(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub fn as_object_mut(&mut self) -> Option<&mut BedrockObject> {
        if let Self::Object(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub fn as_array(&self) -> Option<&[Self]> {
        if let Self::Array(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub fn as_array_mut(&mut self) -> Option<&mut Vec<Self>> {
        if let Self::Array(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub fn as_bytes(&self) -> Option<&[u8]> {
        if let Self::Bytes(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        if let Self::String(v) = self {
            Some(v)
        } else {
            None
        }
    }
    pub fn get(&self, key: &str) -> Option<&Self> {
        self.as_object()?.get(key)
    }
    pub fn get_mut(&mut self, key: &str) -> Option<&mut Self> {
        self.as_object_mut()?.get_mut(key)
    }
}
/// Trusted finite logical values with no pre-middleware serialization/size gate.
#[derive(Clone, PartialEq)]
/// Process-local typed payload; persistence requires an explicit lossy JSON copy.
/// ```compile_fail
/// fn persist<T: serde::Serialize>() {}
/// persist::<pi_ai_providers::api::bedrock_converse_stream::BedrockPayload>();
/// ```
/// ```compile_fail
/// fn restore<T: for<'de> serde::Deserialize<'de>>() {}
/// restore::<pi_ai_providers::api::bedrock_converse_stream::BedrockPayload>();
/// ```
pub struct BedrockPayload(BedrockValue);
impl BedrockPayload {
    pub fn new(value: BedrockValue) -> Self {
        Self(value)
    }
    pub fn from_json(value: Value) -> Self {
        Self(BedrockValue::from_json(value))
    }
    pub fn as_value(&self) -> &BedrockValue {
        &self.0
    }
    pub fn as_value_mut(&mut self) -> &mut BedrockValue {
        &mut self.0
    }
    pub fn into_value(self) -> BedrockValue {
        self.0
    }
    pub fn to_json(&self) -> Value {
        self.0.to_json()
    }
}
macro_rules! redacted {
    ($($ty:ty),*) => {$ (impl fmt::Debug for $ty {
        fn fmt(&self,f:&mut fmt::Formatter<'_>)->fmt::Result { f.write_str(concat!(stringify!($ty),"([REDACTED])")) }
    })*};
}
redacted!(BedrockValue, BedrockObject, BedrockPayload);
