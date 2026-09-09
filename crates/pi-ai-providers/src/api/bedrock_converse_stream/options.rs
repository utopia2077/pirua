use pi_ai_core::{SecretString, ThinkingBudgets, ThinkingLevel};
use serde::{Deserialize, Serialize, ser::SerializeMap};
use std::fmt;
fn nonnull<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<T>, D::Error> {
    Option::<T>::deserialize(d)?
        .map(Some)
        .ok_or_else(|| serde::de::Error::custom("expected non-null optional field"))
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BedrockThinkingDisplay {
    Summarized,
    Omitted,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BedrockToolChoice {
    Mode(BedrockToolMode),
    Tool(BedrockNamedTool),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BedrockToolMode {
    Auto,
    Any,
    None,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BedrockNamedTool {
    #[serde(rename = "type")]
    pub kind: BedrockNamedToolType,
    pub name: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BedrockNamedToolType {
    #[serde(rename = "tool")]
    Tool,
}
#[derive(Clone, Default, PartialEq)]
pub struct BedrockRequestMetadata(Vec<(String, String)>);
impl BedrockRequestMetadata {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn insert(&mut self, key: String, value: String) -> Option<String> {
        if let Some((_, v)) = self.0.iter_mut().find(|(k, _)| *k == key) {
            return Some(std::mem::replace(v, value));
        }
        self.0.push((key, value));
        None
    }
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }
}
impl Serialize for BedrockRequestMetadata {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut map = s.serialize_map(Some(self.0.len()))?;
        for (k, v) in &self.0 {
            map.serialize_entry(k, v)?;
        }
        map.end()
    }
}
impl<'de> Deserialize<'de> for BedrockRequestMetadata {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = BedrockRequestMetadata;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("ordered string metadata")
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(
                self,
                mut map: M,
            ) -> Result<Self::Value, M::Error> {
                let mut out = BedrockRequestMetadata::new();
                while let Some((k, v)) = map.next_entry::<String, String>()? {
                    if out.insert(k, v).is_some() {
                        return Err(serde::de::Error::custom("duplicate metadata key"));
                    }
                }
                Ok(out)
            }
        }
        d.deserialize_map(Visitor)
    }
}
impl fmt::Debug for BedrockRequestMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockRequestMetadata([REDACTED])")
    }
}
#[derive(Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BedrockOptions {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub region: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub profile: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub tool_choice: Option<BedrockToolChoice>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub reasoning: Option<ThinkingLevel>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub thinking_budgets: Option<ThinkingBudgets>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub interleaved_thinking: Option<bool>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub thinking_display: Option<BedrockThinkingDisplay>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub request_metadata: Option<BedrockRequestMetadata>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "nonnull"
    )]
    pub bearer_token: Option<SecretString>,
}
impl fmt::Debug for BedrockOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BedrockOptions([REDACTED])")
    }
}
