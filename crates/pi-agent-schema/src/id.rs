//! Validated open Agent and Harness identities.

use std::fmt;
use std::str::FromStr;

use pi_ai_core::{JS_SAFE_INTEGER_MAX, SafeUnsigned};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// Error returned when an open schema identity is empty.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OpenIdentityError;

impl fmt::Display for OpenIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("identity must be non-empty")
    }
}

impl std::error::Error for OpenIdentityError {}

/// Error returned when a one-based safe integer is zero or unsafe.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OneBasedSafeUnsignedError {
    /// Zero is not a valid one-based value.
    Zero,
    /// The value exceeds JavaScript's exact integer range.
    Unsafe,
}

impl fmt::Display for OneBasedSafeUnsignedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => formatter.write_str("one-based safe integer must be at least one"),
            Self::Unsafe => formatter.write_str("integer exceeds JavaScript safe range"),
        }
    }
}

impl std::error::Error for OneBasedSafeUnsignedError {}

/// A positive integer that JavaScript can represent exactly.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OneBasedSafeUnsigned(SafeUnsigned);

impl OneBasedSafeUnsigned {
    /// The largest accepted value.
    pub const MAX: Self = Self(SafeUnsigned::MAX);

    /// Creates a checked one-based safe integer.
    pub fn new(value: u64) -> Result<Self, OneBasedSafeUnsignedError> {
        if value == 0 {
            return Err(OneBasedSafeUnsignedError::Zero);
        }
        if value > JS_SAFE_INTEGER_MAX {
            return Err(OneBasedSafeUnsignedError::Unsafe);
        }
        let value = SafeUnsigned::new(value).map_err(|_| OneBasedSafeUnsignedError::Unsafe)?;
        Ok(Self(value))
    }

    /// Returns the primitive value.
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

impl TryFrom<u64> for OneBasedSafeUnsigned {
    type Error = OneBasedSafeUnsignedError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl Serialize for OneBasedSafeUnsigned {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for OneBasedSafeUnsigned {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = SafeUnsigned::deserialize(deserializer)?;
        Self::new(value.get()).map_err(de::Error::custom)
    }
}

macro_rules! open_identity {
    ($(#[$meta:meta])* $name:ident, $description:literal) => {
        $(#[$meta])*
        #[doc = $description]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(String);

        impl $name {
            /// Creates an identity after checking that it is nonempty.
            pub fn new(value: impl Into<String>) -> Result<Self, OpenIdentityError> {
                let value = value.into();
                if value.is_empty() {
                    Err(OpenIdentityError)
                } else {
                    Ok(Self(value))
                }
            }

            /// Returns the exact wire string.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the identity and returns its exact wire string.
            pub fn into_string(self) -> String {
                self.0
            }
        }

        impl TryFrom<String> for $name {
            type Error = OpenIdentityError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }

        impl FromStr for $name {
            type Err = OpenIdentityError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: Serializer,
            {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::new(String::deserialize(deserializer)?).map_err(de::Error::custom)
            }
        }
    };
}

open_identity!(EntryId, "An open session entry identity.");
open_identity!(UsageId, "An open usage-ledger row identity.");
open_identity!(OperationId, "An open durable operation identity.");
open_identity!(StepId, "An open assistant-generation step identity.");
open_identity!(TaskId, "An open structural task identity.");
open_identity!(LaneId, "An open lane name.");
open_identity!(CustomType, "An open custom-entry type.");
open_identity!(OperationErrorCode, "An open operation error code.");
