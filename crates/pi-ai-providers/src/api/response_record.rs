//! Transient callback records, not physical HTTP headers or durable values.
use pi_ai_core::FiniteF64;
use pi_ai_transport::{TransportError, TransportErrorKind};
use std::fmt;

#[derive(Clone, PartialEq)]
pub enum ResponseHeaderValue {
    Text(String),
    Number(FiniteF64),
}
impl ResponseHeaderValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(text) => Some(text),
            Self::Number(_) => None,
        }
    }
}
impl fmt::Debug for ResponseHeaderValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResponseHeaderValue([REDACTED])")
    }
}

/// Ordered own keys; exact replacement keeps its first slot. Numeric keys use
/// JavaScript array-index enumeration. Limits count UTF-8 payload bytes, excluding
/// Vec/String bookkeeping. Checked construction permits8192 entries and1MiB
/// logical data. Existing Fetch projections inherit their already admitted size.
#[derive(Clone, Default, PartialEq)]
pub struct ResponseHeaderRecord {
    entries: Vec<(String, ResponseHeaderValue)>,
    bytes: usize,
}
impl ResponseHeaderRecord {
    pub const MAX_ENTRIES: usize = 8192;
    pub const MAX_BYTES: usize = 1024 * 1024;
    pub fn new() -> Self {
        Self::default()
    }
    pub fn get(&self, name: &str) -> Option<&ResponseHeaderValue> {
        self.entries
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, v)| v)
    }
    pub fn get_str(&self, name: &str) -> Option<&str> {
        self.get(name)?.as_str()
    }
    pub fn iter(&self) -> impl Iterator<Item = (&str, &ResponseHeaderValue)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }
    /// Checked insertion. Inherited Fetch records may exceed these construction
    /// limits; replacements may preserve or reduce their current excess, but
    /// cannot grow it. Once reduced within a limit, that ordinary limit applies.
    pub fn insert(
        &mut self,
        name: String,
        value: ResponseHeaderValue,
    ) -> Result<(), TransportError> {
        self.insert_inner(name, value, true)
    }
    /// Only the existing Fetch projection inherits admission from HttpHeaders.
    /// This is not available to callers constructing new checked/Smithy records.
    pub(super) fn insert_inherited(
        &mut self,
        name: String,
        value: ResponseHeaderValue,
    ) -> Result<(), TransportError> {
        self.insert_inner(name, value, false)
    }
    fn insert_inner(
        &mut self,
        name: String,
        value: ResponseHeaderValue,
        checked: bool,
    ) -> Result<(), TransportError> {
        let size = |value: &ResponseHeaderValue| match value {
            ResponseHeaderValue::Text(v) => v.len(),
            ResponseHeaderValue::Number(_) => 8,
        };
        let old = self.entries.iter().position(|(key, _)| key == &name);
        let before = old.map_or(0, |i| self.entries[i].0.len() + size(&self.entries[i].1));
        let bytes = self
            .bytes
            .saturating_sub(before)
            .checked_add(name.len())
            .and_then(|v| v.checked_add(size(&value)))
            .ok_or_else(|| TransportError::new(TransportErrorKind::LimitExceeded))?;
        if checked
            && (bytes > Self::MAX_BYTES.max(self.bytes)
                || (old.is_none() && self.entries.len() >= Self::MAX_ENTRIES))
        {
            return Err(TransportError::new(TransportErrorKind::LimitExceeded));
        }
        if let Some(i) = old {
            self.entries[i].1 = value;
        } else {
            self.entries.push((name, value));
            self.entries.sort_by_key(|(key, _)| {
                key.parse::<u32>()
                    .ok()
                    .filter(|v| *v != u32::MAX && v.to_string() == *key)
                    .map_or((1, 0), |v| (0, v))
            });
        }
        self.bytes = bytes;
        Ok(())
    }
}
impl fmt::Debug for ResponseHeaderRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ResponseHeaderRecord([REDACTED])")
    }
}
