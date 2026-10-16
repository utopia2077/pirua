//! Redacted JSON repair and bounded streaming partial parsing.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use serde::de::DeserializeOwned;

use crate::{JsonObject, JsonValue};

const MAX_STREAMING_JSON_BYTES: usize = 256 * 1024;
const MAX_PARTIAL_DEPTH: usize = 128;

/// Stable complete-JSON parse failure category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JsonParseErrorCode {
    /// Neither the original nor repaired input was valid JSON.
    InvalidJson,
}

/// Fixed, input-redacted JSON parse failure.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::JsonParseError>();
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JsonParseError {
    code: JsonParseErrorCode,
}

impl JsonParseError {
    /// Returns the stable public category.
    pub const fn code(&self) -> JsonParseErrorCode {
        self.code
    }

    const fn invalid() -> Self {
        Self {
            code: JsonParseErrorCode::InvalidJson,
        }
    }
}

impl fmt::Display for JsonParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("invalid JSON")
    }
}

impl Error for JsonParseError {}

/// Repairs malformed JSON string literals without changing valid escapes.
pub fn repair_json(json: &str) -> String {
    let mut repaired = String::with_capacity(json.len());
    let mut characters = json.chars().peekable();
    let mut in_string = false;

    while let Some(character) = characters.next() {
        if !in_string {
            repaired.push(character);
            if character == '"' {
                in_string = true;
            }
            continue;
        }

        if character == '"' {
            repaired.push(character);
            in_string = false;
            continue;
        }
        if character == '\\' {
            let Some(next) = characters.peek().copied() else {
                repaired.push_str("\\\\");
                continue;
            };
            if next == 'u' {
                let mut lookahead = characters.clone();
                lookahead.next();
                let digits = lookahead.by_ref().take(4).collect::<String>();
                if digits.len() == 4 && digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    repaired.push_str("\\u");
                    repaired.push_str(&digits);
                    for _ in 0..5 {
                        characters.next();
                    }
                    continue;
                }
            }
            if matches!(next, '"' | '\\' | '/' | 'b' | 'f' | 'n' | 'r' | 't' | 'u') {
                repaired.push('\\');
                repaired.push(next);
                characters.next();
                continue;
            }
            repaired.push_str("\\\\");
            continue;
        }
        if character <= '\u{1f}' {
            match character {
                '\u{08}' => repaired.push_str("\\b"),
                '\u{0c}' => repaired.push_str("\\f"),
                '\n' => repaired.push_str("\\n"),
                '\r' => repaired.push_str("\\r"),
                '\t' => repaired.push_str("\\t"),
                value => {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    let value = value as u8;
                    repaired.push_str("\\u00");
                    repaired.push(char::from(HEX[usize::from(value >> 4)]));
                    repaired.push(char::from(HEX[usize::from(value & 0x0f)]));
                }
            }
        } else {
            repaired.push(character);
        }
    }
    repaired
}

/// Parses complete JSON, retrying only when repair changes the input.
pub fn parse_json_with_repair<T: DeserializeOwned>(json: &str) -> Result<T, JsonParseError> {
    if let Ok(value) = serde_json::from_str(json) {
        return Ok(value);
    }
    let repaired = repair_json(json);
    if repaired != json {
        return serde_json::from_str(&repaired).map_err(|_| JsonParseError::invalid());
    }
    Err(JsonParseError::invalid())
}

/// Parses bounded, potentially incomplete streaming JSON with deterministic fallbacks.
pub fn parse_streaming_json(partial_json: Option<&str>) -> JsonValue {
    let Some(json) = partial_json else {
        return empty_object();
    };
    if json.trim_matches(is_ecmascript_whitespace).is_empty()
        || json.len() > MAX_STREAMING_JSON_BYTES
    {
        return empty_object();
    }

    if json.len() <= MAX_STREAMING_JSON_BYTES
        && let Ok(value) = serde_json::from_str::<JsonValue>(json)
    {
        return value;
    }
    let repaired = repair_json(json);
    if repaired != json
        && repaired.len() <= MAX_STREAMING_JSON_BYTES
        && let Ok(value) = serde_json::from_str::<JsonValue>(&repaired)
    {
        return value;
    }
    if json.len() <= MAX_STREAMING_JSON_BYTES
        && let Ok(value) = PartialParser::new(json).parse()
    {
        return non_null_partial(value);
    }
    if repaired.len() <= MAX_STREAMING_JSON_BYTES
        && let Ok(value) = PartialParser::new(&repaired).parse()
    {
        return non_null_partial(value);
    }
    empty_object()
}

fn non_null_partial(value: JsonValue) -> JsonValue {
    if matches!(value, JsonValue::Null) {
        empty_object()
    } else {
        value
    }
}

fn empty_object() -> JsonValue {
    JsonValue::Object(JsonObject::default())
}

fn is_ecmascript_whitespace(character: char) -> bool {
    matches!(
        character,
        '\u{0009}'
            | '\u{000a}'
            | '\u{000b}'
            | '\u{000c}'
            | '\u{000d}'
            | '\u{0020}'
            | '\u{00a0}'
            | '\u{1680}'
            | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PartialFailure {
    Incomplete,
    Malformed,
    Duplicate,
    Depth,
}

struct PartialParser<'a> {
    source: &'a str,
    bytes: &'a [u8],
    index: usize,
}

impl<'a> PartialParser<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            bytes: source.as_bytes(),
            index: 0,
        }
    }

    fn parse(mut self) -> Result<JsonValue, PartialFailure> {
        self.skip_whitespace();
        self.parse_value(0)
    }

    fn parse_value(&mut self, depth: usize) -> Result<JsonValue, PartialFailure> {
        self.skip_whitespace();
        let Some(byte) = self.bytes.get(self.index).copied() else {
            return Err(PartialFailure::Incomplete);
        };
        match byte {
            b'{' => self.parse_object(depth + 1),
            b'[' => self.parse_array(depth + 1),
            b'"' => self.parse_string().map(JsonValue::String),
            b'n' if self.partial_keyword("null") => Ok(JsonValue::Null),
            b't' if self.partial_keyword("true") => Ok(JsonValue::Bool(true)),
            b'f' if self.partial_keyword("false") => Ok(JsonValue::Bool(false)),
            b'-' | b'0'..=b'9' => self.parse_number(),
            _ => Err(PartialFailure::Malformed),
        }
    }

    fn parse_object(&mut self, depth: usize) -> Result<JsonValue, PartialFailure> {
        if depth > MAX_PARTIAL_DEPTH {
            return Err(PartialFailure::Depth);
        }
        self.index += 1;
        let mut values = BTreeMap::new();
        loop {
            self.skip_whitespace();
            match self.bytes.get(self.index).copied() {
                None => return Ok(JsonValue::Object(JsonObject::new(values))),
                Some(b'}') => {
                    self.index += 1;
                    return Ok(JsonValue::Object(JsonObject::new(values)));
                }
                Some(b'"') => {}
                Some(_) => return Ok(JsonValue::Object(JsonObject::new(values))),
            }
            let key = match self.parse_string() {
                Ok(key) => key,
                Err(PartialFailure::Incomplete | PartialFailure::Malformed) => {
                    return Ok(JsonValue::Object(JsonObject::new(values)));
                }
                Err(error) => return Err(error),
            };
            self.skip_whitespace();
            let Some(b':') = self.bytes.get(self.index).copied() else {
                return Ok(JsonValue::Object(JsonObject::new(values)));
            };
            self.index += 1;
            let value = match self.parse_value(depth) {
                Ok(value) => value,
                Err(PartialFailure::Incomplete | PartialFailure::Malformed) => {
                    return Ok(JsonValue::Object(JsonObject::new(values)));
                }
                Err(error) => return Err(error),
            };
            if values.insert(key, value).is_some() {
                return Err(PartialFailure::Duplicate);
            }
            self.skip_whitespace();
            match self.bytes.get(self.index).copied() {
                Some(b',') => self.index += 1,
                Some(b'}') => {
                    self.index += 1;
                    return Ok(JsonValue::Object(JsonObject::new(values)));
                }
                None => return Ok(JsonValue::Object(JsonObject::new(values))),
                Some(_) => continue,
            }
        }
    }

    fn parse_array(&mut self, depth: usize) -> Result<JsonValue, PartialFailure> {
        if depth > MAX_PARTIAL_DEPTH {
            return Err(PartialFailure::Depth);
        }
        self.index += 1;
        let mut values = Vec::new();
        loop {
            self.skip_whitespace();
            match self.bytes.get(self.index).copied() {
                None => return Ok(JsonValue::Array(values)),
                Some(b']') => {
                    self.index += 1;
                    return Ok(JsonValue::Array(values));
                }
                Some(_) => {}
            }
            match self.parse_value(depth) {
                Ok(value) => values.push(value),
                Err(PartialFailure::Incomplete | PartialFailure::Malformed) => {
                    return Ok(JsonValue::Array(values));
                }
                Err(error) => return Err(error),
            }
            self.skip_whitespace();
            match self.bytes.get(self.index).copied() {
                Some(b',') => self.index += 1,
                Some(b']') => {
                    self.index += 1;
                    return Ok(JsonValue::Array(values));
                }
                None => return Ok(JsonValue::Array(values)),
                Some(_) => continue,
            }
        }
    }

    fn parse_string(&mut self) -> Result<String, PartialFailure> {
        let start = self.index;
        self.index += 1;
        let mut escaped = false;
        while self.index < self.bytes.len() {
            let byte = self.bytes[self.index];
            if byte == b'"' && !escaped {
                self.index += 1;
                return serde_json::from_str(&self.source[start..self.index])
                    .map_err(|_| PartialFailure::Malformed);
            }
            escaped = if byte == b'\\' { !escaped } else { false };
            self.index += 1;
        }

        let mut end = self.index - usize::from(escaped);
        let mut candidate = String::with_capacity(end - start + 1);
        candidate.push_str(&self.source[start..end]);
        candidate.push('"');
        if let Ok(value) = serde_json::from_str(&candidate) {
            return Ok(value);
        }
        let Some(backslash) = self.source[start..end].rfind('\\') else {
            return Err(PartialFailure::Incomplete);
        };
        end = start + backslash;
        candidate.clear();
        candidate.push_str(&self.source[start..end]);
        candidate.push('"');
        serde_json::from_str(&candidate).map_err(|_| PartialFailure::Malformed)
    }

    fn parse_number(&mut self) -> Result<JsonValue, PartialFailure> {
        let start = self.index;
        while let Some(byte) = self.bytes.get(self.index).copied() {
            if matches!(byte, b',' | b']' | b'}') {
                break;
            }
            self.index += 1;
        }
        let candidate = self.source[start..self.index].trim_matches([' ', '\n', '\r', '\t']);
        if candidate == "-" {
            return Err(PartialFailure::Incomplete);
        }
        if let Ok(value @ JsonValue::Number(_)) = serde_json::from_str(candidate) {
            return Ok(value);
        }
        let mut trimmed = candidate;
        if matches!(trimmed.as_bytes().last(), Some(b'+' | b'-'))
            && trimmed[..trimmed.len() - 1].ends_with(['e', 'E'])
        {
            trimmed = &trimmed[..trimmed.len() - 1];
        }
        if trimmed.ends_with(['e', 'E']) {
            trimmed = &trimmed[..trimmed.len() - 1];
        }
        if trimmed.is_empty() || trimmed == "-" {
            return Err(PartialFailure::Malformed);
        }
        match serde_json::from_str(trimmed) {
            Ok(value @ JsonValue::Number(_)) => Ok(value),
            _ => Err(PartialFailure::Malformed),
        }
    }

    fn partial_keyword(&mut self, keyword: &str) -> bool {
        let remaining = &self.source[self.index..];
        if remaining.starts_with(keyword) {
            self.index += keyword.len();
            return true;
        }
        if !remaining.is_empty()
            && remaining.len() < keyword.len()
            && keyword.starts_with(remaining)
        {
            self.index = self.bytes.len();
            return true;
        }
        false
    }

    fn skip_whitespace(&mut self) {
        while self
            .bytes
            .get(self.index)
            .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.index += 1;
        }
    }
}

/// Complete model-generated tool arguments and an optional parse diagnostic.
#[derive(Clone, Debug, PartialEq)]
pub struct ParsedToolCallArguments {
    /// Object arguments; empty on malformed complete input.
    pub arguments: JsonObject,
    /// Diagnostic retaining the model's complete received text.
    pub arguments_error: Option<String>,
}
/// Parses a finished call without keeping a partially parsed prefix.
pub fn parse_tool_call_arguments(input: Option<&str>) -> ParsedToolCallArguments {
    let input = input.unwrap_or_default();
    if input.trim().is_empty() {
        return ParsedToolCallArguments {
            arguments: JsonObject::default(),
            arguments_error: None,
        };
    }
    let parsed = parse_json_with_repair::<JsonValue>(input);
    match parsed {
        Ok(JsonValue::Object(arguments)) => ParsedToolCallArguments {
            arguments,
            arguments_error: None,
        },
        result => ParsedToolCallArguments {
            arguments: JsonObject::default(),
            arguments_error: Some(format!(
                "{}\n\nReceived arguments:\n{input}",
                if result.is_ok() {
                    "Tool arguments must be a JSON object"
                } else {
                    "Invalid JSON tool arguments"
                }
            )),
        },
    }
}
