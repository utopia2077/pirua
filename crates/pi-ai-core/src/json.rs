//! Deterministic JSON values, presence markers, and safe JSON numbers.

use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Deserializes a present optional non-null field.
///
/// The containing field must also use `default` so omission becomes `None` and
/// `skip_serializing_if = "Option::is_none"` so omission remains omitted.
pub(crate) fn deserialize_optional_non_null<'de, D, T>(
    deserializer: D,
) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct NonNullOptionVisitor<T>(PhantomData<T>);

    impl<'de, T> Visitor<'de> for NonNullOptionVisitor<T>
    where
        T: Deserialize<'de>,
    {
        type Value = Option<T>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a present non-null optional value")
        }

        fn visit_none<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom(
                "explicit null is not allowed for this optional field",
            ))
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            Err(E::custom(
                "explicit null is not allowed for this optional field",
            ))
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
        {
            T::deserialize(deserializer).map(Some)
        }
    }

    deserializer.deserialize_option(NonNullOptionVisitor(PhantomData))
}

struct StrictMapVisitor<K, V>(PhantomData<(K, V)>);

impl<'de, K, V> Visitor<'de> for StrictMapVisitor<K, V>
where
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    type Value = BTreeMap<K, V>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a map without duplicate keys")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = BTreeMap::new();
        while let Some((key, value)) = map.next_entry()? {
            if values.insert(key, value).is_some() {
                return Err(de::Error::custom("duplicate map key"));
            }
        }
        Ok(values)
    }
}

/// Deserializes an ordered map and rejects duplicate keys.
pub(crate) fn deserialize_btree_map_reject_duplicates<'de, D, K, V>(
    deserializer: D,
) -> Result<BTreeMap<K, V>, D::Error>
where
    D: Deserializer<'de>,
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    deserializer.deserialize_map(StrictMapVisitor(PhantomData))
}

/// Deserializes a present optional ordered map, rejecting null and duplicates.
pub(crate) fn deserialize_optional_btree_map_non_null<'de, D, K, V>(
    deserializer: D,
) -> Result<Option<BTreeMap<K, V>>, D::Error>
where
    D: Deserializer<'de>,
    K: Deserialize<'de> + Ord,
    V: Deserialize<'de>,
{
    deserialize_btree_map_reject_duplicates(deserializer).map(Some)
}

/// The largest integer JavaScript can represent without loss.
pub const JS_SAFE_INTEGER_MAX: u64 = 9_007_199_254_740_991;

/// Error returned when a number cannot cross a JavaScript JSON boundary safely.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NumericError {
    message: &'static str,
}

impl NumericError {
    const fn new(message: &'static str) -> Self {
        Self { message }
    }

    /// Creates the retry-normalization boundary error.
    pub(crate) const fn retry_count_too_large() -> Self {
        Self::new("maxRetries + 1 exceeds JavaScript safe range")
    }
}

impl fmt::Display for NumericError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.message)
    }
}

impl std::error::Error for NumericError {}

/// A non-negative integer that JavaScript can represent exactly.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SafeUnsigned(u64);

impl SafeUnsigned {
    /// The largest accepted value.
    pub const MAX: Self = Self(JS_SAFE_INTEGER_MAX);

    /// Creates a checked safe unsigned integer.
    pub const fn new(value: u64) -> Result<Self, NumericError> {
        if value <= JS_SAFE_INTEGER_MAX {
            Ok(Self(value))
        } else {
            Err(NumericError::new(
                "unsigned integer exceeds JavaScript safe range",
            ))
        }
    }

    /// Returns the primitive value.
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl TryFrom<u64> for SafeUnsigned {
    type Error = NumericError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SafeUnsigned> for u64 {
    fn from(value: SafeUnsigned) -> Self {
        value.get()
    }
}

impl Serialize for SafeUnsigned {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> Deserialize<'de> for SafeUnsigned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SafeUnsignedVisitor;

        impl Visitor<'_> for SafeUnsignedVisitor {
            type Value = SafeUnsigned;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a non-negative JavaScript-safe integer")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                SafeUnsigned::new(value).map_err(E::custom)
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let value = u64::try_from(value)
                    .map_err(|_| E::custom("expected a non-negative integer"))?;
                self.visit_u64(value)
            }
        }

        deserializer.deserialize_u64(SafeUnsignedVisitor)
    }
}

/// A signed integer that JavaScript can represent exactly.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SafeSigned(i64);

impl SafeSigned {
    /// The smallest accepted value.
    pub const MIN: Self = Self(-(JS_SAFE_INTEGER_MAX as i64));

    /// The largest accepted value.
    pub const MAX: Self = Self(JS_SAFE_INTEGER_MAX as i64);

    /// Creates a checked safe signed integer.
    pub const fn new(value: i64) -> Result<Self, NumericError> {
        if value >= Self::MIN.0 && value <= Self::MAX.0 {
            Ok(Self(value))
        } else {
            Err(NumericError::new(
                "signed integer exceeds JavaScript safe range",
            ))
        }
    }

    /// Returns the primitive value.
    pub const fn get(self) -> i64 {
        self.0
    }
}

impl TryFrom<i64> for SafeSigned {
    type Error = NumericError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<SafeSigned> for i64 {
    fn from(value: SafeSigned) -> Self {
        value.get()
    }
}

impl Serialize for SafeSigned {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(self.0)
    }
}

impl<'de> Deserialize<'de> for SafeSigned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SafeSignedVisitor;

        impl Visitor<'_> for SafeSignedVisitor {
            type Value = SafeSigned;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a JavaScript-safe signed integer")
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                SafeSigned::new(value).map_err(E::custom)
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let value =
                    i64::try_from(value).map_err(|_| E::custom("integer exceeds signed range"))?;
                self.visit_i64(value)
            }
        }

        deserializer.deserialize_i64(SafeSignedVisitor)
    }
}

/// A finite IEEE-754 double.
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct FiniteF64(f64);

impl FiniteF64 {
    /// Creates a checked finite double.
    pub fn new(value: f64) -> Result<Self, NumericError> {
        if value.is_finite() {
            Ok(Self(value))
        } else {
            Err(NumericError::new("floating-point value must be finite"))
        }
    }

    /// Returns the primitive value.
    pub const fn get(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for FiniteF64 {
    type Error = NumericError;

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<FiniteF64> for f64 {
    fn from(value: FiniteF64) -> Self {
        value.get()
    }
}

impl Serialize for FiniteF64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_f64(self.0)
    }
}

impl<'de> Deserialize<'de> for FiniteF64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = f64::deserialize(deserializer)?;
        Self::new(value).map_err(de::Error::custom)
    }
}

/// A JSON number that retains whether its source was an integer or a fraction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum JsonNumber {
    /// A non-negative integer.
    Unsigned(SafeUnsigned),
    /// A negative or explicitly signed integer.
    Signed(SafeSigned),
    /// A finite fractional value.
    Float(FiniteF64),
}

impl Serialize for JsonNumber {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Unsigned(value) => value.serialize(serializer),
            Self::Signed(value) => value.serialize(serializer),
            Self::Float(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for JsonNumber {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct JsonNumberVisitor;

        impl Visitor<'_> for JsonNumberVisitor {
            type Value = JsonNumber;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a finite JavaScript-safe JSON number")
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                SafeUnsigned::new(value)
                    .map(JsonNumber::Unsigned)
                    .map_err(E::custom)
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                SafeSigned::new(value)
                    .map(JsonNumber::Signed)
                    .map_err(E::custom)
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                FiniteF64::new(value)
                    .map(JsonNumber::Float)
                    .map_err(E::custom)
            }
        }

        deserializer.deserialize_any(JsonNumberVisitor)
    }
}

/// A deterministic, recursively validated JSON value.
#[derive(Clone, Debug, PartialEq)]
pub enum JsonValue {
    /// JSON null.
    Null,
    /// A JSON boolean.
    Bool(bool),
    /// A safe JSON number.
    Number(JsonNumber),
    /// A JSON string.
    String(String),
    /// A JSON array.
    Array(Vec<Self>),
    /// A JSON object with deterministic key order.
    Object(JsonObject),
}

impl Serialize for JsonValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_none(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Number(value) => value.serialize(serializer),
            Self::String(value) => serializer.serialize_str(value),
            Self::Array(value) => value.serialize(serializer),
            Self::Object(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for JsonValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct JsonValueVisitor;

        impl<'de> Visitor<'de> for JsonValueVisitor {
            type Value = JsonValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a deterministic JSON value with safe numbers")
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E> {
                Ok(JsonValue::Null)
            }

            fn visit_none<E>(self) -> Result<Self::Value, E> {
                Ok(JsonValue::Null)
            }

            fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
                Ok(JsonValue::Bool(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                SafeUnsigned::new(value)
                    .map(JsonNumber::Unsigned)
                    .map(JsonValue::Number)
                    .map_err(E::custom)
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                SafeSigned::new(value)
                    .map(JsonNumber::Signed)
                    .map(JsonValue::Number)
                    .map_err(E::custom)
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                FiniteF64::new(value)
                    .map(JsonNumber::Float)
                    .map(JsonValue::Number)
                    .map_err(E::custom)
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(JsonValue::String(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
                Ok(JsonValue::String(value))
            }

            fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(JsonValue::Array(values))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut values = BTreeMap::new();
                while let Some((key, value)) = map.next_entry()? {
                    if values.insert(key, value).is_some() {
                        return Err(de::Error::custom("duplicate JSON object key"));
                    }
                }
                Ok(JsonValue::Object(JsonObject(values)))
            }
        }

        deserializer.deserialize_any(JsonValueVisitor)
    }
}

/// A deterministic JSON object used where arrays and primitives are invalid.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JsonObject(
    #[serde(deserialize_with = "deserialize_btree_map_reject_duplicates")]
    BTreeMap<String, JsonValue>,
);

impl JsonObject {
    /// Creates an object from an ordered map.
    pub fn new(values: BTreeMap<String, JsonValue>) -> Self {
        Self(values)
    }

    /// Returns the ordered entries.
    pub const fn as_map(&self) -> &BTreeMap<String, JsonValue> {
        &self.0
    }

    /// Consumes the wrapper and returns its entries.
    pub fn into_map(self) -> BTreeMap<String, JsonValue> {
        self.0
    }
}

/// Presence state for an optional field whose explicit JSON null is meaningful.
///
/// A containing serde field must use both `default` and
/// `skip_serializing_if = "Presence::is_missing"`. Without `default`, omission
/// cannot become [`Presence::Missing`]; without the skip predicate, a missing
/// value would serialize as JSON null and lose the distinction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Presence<T> {
    /// The field was absent and must be omitted when serialized.
    #[default]
    Missing,
    /// The field was present as JSON null.
    Null,
    /// The field was present with a value.
    Value(T),
}

impl<T> Presence<T> {
    /// Returns whether the field is absent.
    pub const fn is_missing(&self) -> bool {
        matches!(self, Self::Missing)
    }
}

impl<T> Serialize for Presence<T>
where
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Missing | Self::Null => serializer.serialize_none(),
            Self::Value(value) => value.serialize(serializer),
        }
    }
}

impl<'de, T> Deserialize<'de> for Presence<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Option::<T>::deserialize(deserializer).map(|value| value.map_or(Self::Null, Self::Value))
    }
}

/// A marker that serializes only as the JSON literal `true`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LiteralTrue;

impl Serialize for LiteralTrue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bool(true)
    }
}

impl<'de> Deserialize<'de> for LiteralTrue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if bool::deserialize(deserializer)? {
            Ok(Self)
        } else {
            Err(de::Error::custom("expected literal true"))
        }
    }
}

/// A marker that serializes only as the JSON literal `false`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LiteralFalse;

impl Serialize for LiteralFalse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_bool(false)
    }
}

impl<'de> Deserialize<'de> for LiteralFalse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if bool::deserialize(deserializer)? {
            Err(de::Error::custom("expected literal false"))
        } else {
            Ok(Self)
        }
    }
}

/// A marker that serializes only as the JSON integer `1`.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LiteralOne;

impl Serialize for LiteralOne {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u8(1)
    }
}

impl<'de> Deserialize<'de> for LiteralOne {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        match u8::deserialize(deserializer)? {
            1 => Ok(Self),
            _ => Err(de::Error::custom("expected literal integer 1")),
        }
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::{FiniteF64, JS_SAFE_INTEGER_MAX, JsonValue, Presence, SafeSigned, SafeUnsigned};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct OptionalProbe {
        #[serde(
            default,
            deserialize_with = "super::deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        value: Option<bool>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct JsonValueOptionalProbe {
        #[serde(
            default,
            deserialize_with = "super::deserialize_optional_non_null",
            skip_serializing_if = "Option::is_none"
        )]
        value: Option<JsonValue>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct PresenceProbe {
        #[serde(default, skip_serializing_if = "Presence::is_missing")]
        value: Presence<JsonValue>,
    }

    #[derive(Deserialize)]
    struct PresenceWithoutDefault {
        value: Presence<JsonValue>,
    }

    #[derive(Serialize)]
    struct PresenceWithoutSkip {
        value: Presence<JsonValue>,
    }

    #[test]
    fn safe_integer_constructors_accept_exact_extrema() {
        assert_eq!(
            SafeUnsigned::new(JS_SAFE_INTEGER_MAX).map(SafeUnsigned::get),
            Ok(JS_SAFE_INTEGER_MAX)
        );
        assert_eq!(
            SafeSigned::new(-(JS_SAFE_INTEGER_MAX as i64)).map(SafeSigned::get),
            Ok(-(JS_SAFE_INTEGER_MAX as i64))
        );
        assert_eq!(
            SafeSigned::new(JS_SAFE_INTEGER_MAX as i64).map(SafeSigned::get),
            Ok(JS_SAFE_INTEGER_MAX as i64)
        );
    }

    #[test]
    fn numeric_constructors_reject_unsafe_or_non_finite_values() {
        assert!(SafeUnsigned::new(JS_SAFE_INTEGER_MAX + 1).is_err());
        assert!(SafeSigned::new((JS_SAFE_INTEGER_MAX + 1) as i64).is_err());
        assert!(FiniteF64::new(f64::NAN).is_err());
        assert!(FiniteF64::new(f64::INFINITY).is_err());
        assert!(FiniteF64::new(f64::NEG_INFINITY).is_err());
    }

    #[test]
    fn optional_non_null_helper_distinguishes_omission_from_null() {
        assert_eq!(
            serde_json::from_str::<OptionalProbe>("{}").expect("omission is valid"),
            OptionalProbe { value: None }
        );
        assert!(serde_json::from_str::<OptionalProbe>(r#"{"value":null}"#).is_err());
        let value: OptionalProbe =
            serde_json::from_str(r#"{"value":false}"#).expect("false remains present");
        assert_eq!(value.value, Some(false));
        assert_eq!(
            serde_json::to_string(&value).expect("probe serializes"),
            r#"{"value":false}"#
        );

        let omitted: JsonValueOptionalProbe =
            serde_json::from_str("{}").expect("nullable inner omission is valid");
        assert_eq!(omitted.value, None);
        assert!(serde_json::from_str::<JsonValueOptionalProbe>(r#"{"value":null}"#).is_err());
        let present: JsonValueOptionalProbe = serde_json::from_str(r#"{"value":{"ok":true}}"#)
            .expect("present non-null JsonValue is valid");
        assert!(matches!(present.value, Some(JsonValue::Object(_))));
    }

    #[test]
    fn presence_requires_default_and_missing_skip_on_the_containing_field() {
        let missing: PresenceProbe = serde_json::from_str("{}").expect("omission is valid");
        let null: PresenceProbe =
            serde_json::from_str(r#"{"value":null}"#).expect("explicit null is valid");
        let value: PresenceProbe =
            serde_json::from_str(r#"{"value":false}"#).expect("present value is valid");
        assert_eq!(missing.value, Presence::Missing);
        assert_eq!(null.value, Presence::Null);
        assert_eq!(value.value, Presence::Value(JsonValue::Bool(false)));
        assert_eq!(
            serde_json::to_string(&missing).expect("missing serializes"),
            "{}"
        );

        let collapsed: PresenceWithoutDefault =
            serde_json::from_str("{}").expect("serde invokes the field deserializer for omission");
        assert_eq!(collapsed.value, Presence::Null);
        assert_eq!(
            serde_json::to_string(&PresenceWithoutSkip {
                value: Presence::Missing,
            })
            .expect("unskipped presence serializes"),
            r#"{"value":null}"#
        );
    }
}
