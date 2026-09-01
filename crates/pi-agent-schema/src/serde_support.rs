use std::fmt;
use std::marker::PhantomData;

use pi_ai_core::JsonValue;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};

pub(crate) fn deserialize_required_nullable<'de, D, T>(
    deserializer: D,
) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

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

pub(crate) fn from_json_value<T, E>(value: JsonValue) -> Result<T, E>
where
    T: serde::de::DeserializeOwned,
    E: de::Error,
{
    let value = serde_json::to_value(value).map_err(E::custom)?;
    serde_json::from_value(value).map_err(E::custom)
}
