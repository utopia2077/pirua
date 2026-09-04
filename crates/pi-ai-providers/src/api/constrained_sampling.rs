//! Provider strict-schema and grammar projections, separate from argument validation.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;

use pi_ai_core::{
    ConstrainedSampling, ConstrainedSamplingConfig, JsonObject, JsonSchema, JsonSchemaStrictness,
    JsonValue, Tool,
};
use serde::{Deserialize, Serialize};

const MAX_SCHEMA_DEPTH: usize = 128;
const MAX_SCHEMA_BYTES: usize = 256 * 1024;
const UNSUPPORTED_KEYS: [&str; 16] = [
    "$ref",
    "$defs",
    "definitions",
    "allOf",
    "oneOf",
    "patternProperties",
    "dependentSchemas",
    "dependencies",
    "unevaluatedProperties",
    "propertyNames",
    "contains",
    "prefixItems",
    "not",
    "if",
    "then",
    "else",
];

/// Closed strict-subset rejection categories; these never contain source schema text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StrictSchemaIssue {
    /// The root must have literal type object.
    RootType,
    /// Every active schema node must be an object.
    NonObjectNode,
    /// An unsupported keyword is present at an active schema node.
    UnsupportedKeyword,
    /// Any-of must be a nonempty array.
    InvalidAnyOf,
    /// Any-of cannot include structured object/array variants.
    StructuredUnion,
    /// Tuple-style items are unsupported.
    TupleItems,
    /// Properties requires literal type object.
    PropertiesWithoutObject,
    /// Additional properties must be omitted or false.
    AdditionalProperties,
    /// Properties must contain an object map.
    PropertiesMap,
    /// Required must be a string array naming existing properties.
    RequiredProperties,
}

/// Redacted helper failure. Resource exhaustion never becomes optional fallback.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConstrainedSamplingError {
    /// Schema cannot be represented by the provider strict subset.
    UnsupportedSchema(StrictSchemaIssue),
    /// Required strict sampling is unavailable on this provider.
    StrictUnavailable,
    /// No supported nonblank grammar variant was supplied.
    MissingGrammar,
    /// The grammar schema lacks exactly one required string property.
    GrammarInputShape,
    /// A grammar invocation lacks its string input argument.
    GrammarInputValue,
    /// An input update is not an extension of the preceding input.
    NonMonotoneInput,
    /// An input update changes an already closed buffer.
    ClosedInput,
    /// The schema exceeds depth 128 or 256 KiB of canonical JSON.
    ResourceLimit,
}

impl fmt::Display for ConstrainedSamplingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedSchema(_) => "unsupported strict schema",
            Self::StrictUnavailable => "required strict sampling is unavailable",
            Self::MissingGrammar => "supported grammar is missing",
            Self::GrammarInputShape => "invalid grammar input schema",
            Self::GrammarInputValue => "grammar input must be a string",
            Self::NonMonotoneInput => "grammar input changed non-monotonically",
            Self::ClosedInput => "grammar input changed after close",
            Self::ResourceLimit => "constrained sampling schema resource limit",
        })
    }
}

impl std::error::Error for ConstrainedSamplingError {}

fn unsupported(issue: StrictSchemaIssue) -> ConstrainedSamplingError {
    ConstrainedSamplingError::UnsupportedSchema(issue)
}

struct ByteBudget(usize);

impl io::Write for ByteBudget {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(bytes.len())
            .ok_or_else(|| io::Error::other("schema byte limit"))?;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn check_schema_bounds(schema: &JsonSchema) -> Result<(), ConstrainedSamplingError> {
    fn check_depth(value: &JsonValue, depth: usize) -> Result<(), ConstrainedSamplingError> {
        if depth > MAX_SCHEMA_DEPTH {
            return Err(ConstrainedSamplingError::ResourceLimit);
        }
        match value {
            JsonValue::Object(object) => {
                for value in object.as_map().values() {
                    check_depth(value, depth + 1)?;
                }
            }
            JsonValue::Array(array) => {
                for value in array {
                    check_depth(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    for value in schema.0.as_map().values() {
        check_depth(value, 1)?;
    }
    serde_json::to_writer(ByteBudget(MAX_SCHEMA_BYTES), schema)
        .map_err(|_| ConstrainedSamplingError::ResourceLimit)
}

fn has_type(schema: &BTreeMap<String, JsonValue>, name: &str) -> bool {
    matches!(schema.get("type"), Some(JsonValue::String(value)) if value == name)
}

fn is_structured(value: &JsonValue) -> bool {
    let JsonValue::Object(object) = value else {
        return false;
    };
    let map = object.as_map();
    has_type(map, "object")
        || has_type(map, "array")
        || matches!(map.get("type"), Some(JsonValue::Array(types)) if types.iter().any(|value| matches!(value, JsonValue::String(name) if name == "object" || name == "array")))
        || map.contains_key("properties")
        || map.contains_key("items")
}

fn allows_null(value: &JsonValue) -> bool {
    let JsonValue::Object(object) = value else {
        return false;
    };
    let map = object.as_map();
    has_type(map, "null")
        || matches!(map.get("type"), Some(JsonValue::Array(types)) if types.contains(&JsonValue::String("null".to_owned())))
        || matches!(map.get("const"), Some(JsonValue::Null))
        || matches!(map.get("enum"), Some(JsonValue::Array(values)) if values.contains(&JsonValue::Null))
        || matches!(map.get("anyOf"), Some(JsonValue::Array(values)) if values.iter().any(allows_null))
}

fn array_index(key: &str) -> Option<u32> {
    let index = key.parse::<u32>().ok()?;
    (index < u32::MAX && index.to_string() == key).then_some(index)
}

fn strict_node(value: JsonValue) -> Result<JsonValue, ConstrainedSamplingError> {
    let JsonValue::Object(object) = value else {
        return Err(unsupported(StrictSchemaIssue::NonObjectNode));
    };
    let mut schema = object.into_map();
    if UNSUPPORTED_KEYS.iter().any(|key| schema.contains_key(*key)) {
        return Err(unsupported(StrictSchemaIssue::UnsupportedKeyword));
    }
    if let Some(any_of) = schema.remove("anyOf") {
        let JsonValue::Array(variants) = any_of else {
            return Err(unsupported(StrictSchemaIssue::InvalidAnyOf));
        };
        if variants.is_empty() {
            return Err(unsupported(StrictSchemaIssue::InvalidAnyOf));
        }
        let variants = variants
            .into_iter()
            .map(|variant| {
                if is_structured(&variant) {
                    Err(unsupported(StrictSchemaIssue::StructuredUnion))
                } else {
                    strict_node(variant)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        schema.insert("anyOf".to_owned(), JsonValue::Array(variants));
    }
    if let Some(items) = schema.remove("items") {
        if matches!(items, JsonValue::Array(_)) {
            return Err(unsupported(StrictSchemaIssue::TupleItems));
        }
        schema.insert("items".to_owned(), strict_node(items)?);
    }
    let is_object = has_type(&schema, "object");
    if schema.contains_key("properties") && !is_object {
        return Err(unsupported(StrictSchemaIssue::PropertiesWithoutObject));
    }
    if is_object {
        if schema
            .get("additionalProperties")
            .is_some_and(|value| value != &JsonValue::Bool(false))
        {
            return Err(unsupported(StrictSchemaIssue::AdditionalProperties));
        }
        let had_properties = schema.contains_key("properties");
        let mut properties = match schema.remove("properties") {
            Some(JsonValue::Object(properties)) => properties.into_map(),
            None => BTreeMap::new(),
            _ => return Err(unsupported(StrictSchemaIssue::PropertiesMap)),
        };
        let required: BTreeSet<_> = match schema.get("required") {
            Some(JsonValue::Array(values)) => values
                .iter()
                .map(|value| match value {
                    JsonValue::String(name) if properties.contains_key(name) => Ok(name.clone()),
                    _ => Err(unsupported(StrictSchemaIssue::RequiredProperties)),
                })
                .collect::<Result<_, _>>()?,
            None => BTreeSet::new(),
            _ => return Err(unsupported(StrictSchemaIssue::RequiredProperties)),
        };
        let mut names: Vec<_> = properties.keys().cloned().collect();
        names.sort_by(
            |left, right| match (array_index(left), array_index(right)) {
                (Some(left), Some(right)) => left.cmp(&right),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => left.cmp(right),
            },
        );
        for (name, property) in &mut properties {
            let converted = strict_node(std::mem::replace(property, JsonValue::Null))?;
            *property = if !required.contains(name) && !allows_null(&converted) {
                JsonValue::Object(JsonObject::new(BTreeMap::from([(
                    "anyOf".to_owned(),
                    JsonValue::Array(vec![
                        converted,
                        JsonValue::Object(JsonObject::new(BTreeMap::from([(
                            "type".to_owned(),
                            JsonValue::String("null".to_owned()),
                        )]))),
                    ]),
                )])))
            } else {
                converted
            };
        }
        if had_properties {
            schema.insert(
                "properties".to_owned(),
                JsonValue::Object(JsonObject::new(properties)),
            );
        }
        schema.insert(
            "required".to_owned(),
            JsonValue::Array(names.into_iter().map(JsonValue::String).collect()),
        );
        schema.insert("additionalProperties".to_owned(), JsonValue::Bool(false));
    }
    Ok(JsonValue::Object(JsonObject::new(schema)))
}

/// Derives a strict provider schema without mutating the input.
/// Only active properties/items/anyOf nodes are converted. Data under default,
/// enum, const, examples, or unknown keys is preserved without reinterpretation.
/// Required-array order uses JS array-index order, then core canonical key order.
pub fn make_strict_json_schema(
    schema: &JsonSchema,
) -> Result<JsonSchema, ConstrainedSamplingError> {
    check_schema_bounds(schema)?;
    let converted = strict_node(JsonValue::Object(schema.0.clone()))?;
    let JsonValue::Object(object) = converted else {
        return Err(unsupported(StrictSchemaIssue::RootType));
    };
    if !has_type(object.as_map(), "object") {
        return Err(unsupported(StrictSchemaIssue::RootType));
    }
    Ok(JsonSchema(object))
}

/// Borrows unchanged tool parameters unless strict conversion is explicitly true.
pub fn get_json_schema_tool_parameters(
    tool: &Tool,
    strict: Option<bool>,
) -> Result<Cow<'_, JsonSchema>, ConstrainedSamplingError> {
    if strict == Some(true) {
        make_strict_json_schema(&tool.parameters).map(Cow::Owned)
    } else {
        Ok(Cow::Borrowed(&tool.parameters))
    }
}

/// Resolves prefer/require without conflating unsupported schemas and resource limits.
pub fn resolve_json_schema_strict_sampling(
    tool: &Tool,
    supports_strict_mode: bool,
) -> Result<Option<bool>, ConstrainedSamplingError> {
    let Some(ConstrainedSampling::Config(ConstrainedSamplingConfig::JsonSchema { strict })) =
        &tool.constrained_sampling
    else {
        return Ok(None);
    };
    if supports_strict_mode {
        match make_strict_json_schema(&tool.parameters) {
            Ok(_) => return Ok(Some(true)),
            Err(ConstrainedSamplingError::UnsupportedSchema(_))
                if *strict != JsonSchemaStrictness::Require =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
    }
    if *strict == JsonSchemaStrictness::Require {
        Err(ConstrainedSamplingError::StrictUnavailable)
    } else {
        Ok(None)
    }
}

/// Supported OpenAI grammar encodings.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum GrammarFormat {
    /// Lark grammar, preferred when both variants exist.
    Lark,
    /// Regular-expression grammar.
    Regex,
}

/// Resolved grammar definition and its string input property.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct GrammarConstrainedSampling {
    /// Provider grammar encoding.
    pub format: GrammarFormat,
    /// Original definition, preserving whitespace.
    pub definition: String,
    /// Required string property used for grammar input.
    pub input_property: String,
}

impl fmt::Debug for GrammarConstrainedSampling {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GrammarConstrainedSampling")
            .field("format", &self.format)
            .finish_non_exhaustive()
    }
}

/// Transient accumulator for append-only JSON string argument deltas.
#[derive(Clone, Default, Eq, PartialEq)]
pub struct GrammarToolInputJsonBuffer {
    /// Complete raw input observed so far.
    pub input: String,
    /// Whether the opening JSON prefix was emitted.
    pub started: bool,
    /// Whether the closing JSON suffix was emitted.
    pub closed: bool,
}

impl fmt::Debug for GrammarToolInputJsonBuffer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GrammarToolInputJsonBuffer")
            .field("started", &self.started)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}

/// Extracts the grammar argument without copying input into diagnostics.
pub fn get_grammar_tool_input<'a>(
    _tool_name: &str,
    arguments: &'a JsonObject,
    input_property: &str,
) -> Result<&'a str, ConstrainedSamplingError> {
    match arguments.as_map().get(input_property) {
        Some(JsonValue::String(input)) => Ok(input),
        _ => Err(ConstrainedSamplingError::GrammarInputValue),
    }
}

/// Appends escaped JSON for an input extension; rejection leaves the buffer unchanged.
pub fn append_grammar_tool_input_json_delta(
    buffer: &mut GrammarToolInputJsonBuffer,
    input_property: &str,
    next_input: &str,
    close: bool,
) -> Result<Option<String>, ConstrainedSamplingError> {
    if buffer.closed {
        return if close && next_input == buffer.input {
            Ok(None)
        } else {
            Err(ConstrainedSamplingError::ClosedInput)
        };
    }
    let Some(input_delta) = next_input.strip_prefix(&buffer.input) else {
        return Err(ConstrainedSamplingError::NonMonotoneInput);
    };
    if !close && input_delta.is_empty() {
        return Ok(None);
    }
    let mut delta = String::new();
    if !buffer.started {
        delta.push('{');
        delta.push('"');
        append_json_string_contents(&mut delta, input_property);
        delta.push('"');
        delta.push_str(":\"");
    }
    append_json_string_contents(&mut delta, input_delta);
    if close {
        delta.push_str("\"}");
    }
    buffer.started = true;
    buffer.input = next_input.to_owned();
    buffer.closed = close;
    Ok(Some(delta))
}

fn append_json_string_contents(output: &mut String, input: &str) {
    for character in input.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{0008}' => output.push_str("\\b"),
            '\u{000c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{0000}'..='\u{001f}' => output.push_str(&format!("\\u{:04x}", u32::from(character))),
            _ => output.push(character),
        }
    }
}

fn nonblank(value: &str) -> bool {
    value.chars().any(|character| {
        !matches!(character,
            '\u{0009}'..='\u{000d}' | ' ' | '\u{00a0}' | '\u{1680}' |
            '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' |
            '\u{205f}' | '\u{3000}' | '\u{feff}'
        )
    })
}

/// Selects Lark before regex and requires exactly one required string property.
pub fn resolve_grammar_constrained_sampling(
    tool: &Tool,
    supports_openai_grammar_tools: bool,
) -> Result<Option<GrammarConstrainedSampling>, ConstrainedSamplingError> {
    let Some(ConstrainedSampling::Config(ConstrainedSamplingConfig::Grammar { variants })) =
        &tool.constrained_sampling
    else {
        return Ok(None);
    };
    if !supports_openai_grammar_tools {
        return Ok(None);
    }
    let (format, definition) = if let Some(definition) = variants
        .openai_lark
        .as_deref()
        .filter(|value| nonblank(value))
    {
        (GrammarFormat::Lark, definition)
    } else if let Some(definition) = variants
        .openai_regex
        .as_deref()
        .filter(|value| nonblank(value))
    {
        (GrammarFormat::Regex, definition)
    } else {
        return Err(ConstrainedSamplingError::MissingGrammar);
    };
    let schema = tool.parameters.0.as_map();
    if !has_type(schema, "object") {
        return Err(ConstrainedSamplingError::GrammarInputShape);
    }
    let Some(JsonValue::Array(required)) = schema.get("required") else {
        return Err(ConstrainedSamplingError::GrammarInputShape);
    };
    let [JsonValue::String(input_property)] = required.as_slice() else {
        return Err(ConstrainedSamplingError::GrammarInputShape);
    };
    let Some(JsonValue::Object(properties)) = schema.get("properties") else {
        return Err(ConstrainedSamplingError::GrammarInputShape);
    };
    let Some(JsonValue::Object(property)) = properties.as_map().get(input_property) else {
        return Err(ConstrainedSamplingError::GrammarInputShape);
    };
    if !has_type(property.as_map(), "string") {
        return Err(ConstrainedSamplingError::GrammarInputShape);
    }
    Ok(Some(GrammarConstrainedSampling {
        format,
        definition: definition.to_owned(),
        input_property: input_property.clone(),
    }))
}

/// Maps grammar tool names to input properties; later duplicate names replace earlier ones.
pub fn create_grammar_tool_input_properties(
    tools: Option<&[Tool]>,
    supports_openai_grammar_tools: bool,
) -> Result<BTreeMap<String, String>, ConstrainedSamplingError> {
    let mut properties = BTreeMap::new();
    for tool in tools.unwrap_or_default() {
        if let Some(grammar) =
            resolve_grammar_constrained_sampling(tool, supports_openai_grammar_tools)?
        {
            properties.insert(tool.name.as_str().to_owned(), grammar.input_property);
        }
    }
    Ok(properties)
}
