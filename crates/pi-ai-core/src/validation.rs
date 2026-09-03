//! Bounded offline JSON Schema validation for tool calls.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use jsonschema::error::ValidationErrorKind;
use serde_json::{Map as SerdeMap, Number as SerdeNumber, Value as SerdeValue};

use crate::{
    FiniteF64, JS_SAFE_INTEGER_MAX, JsonNumber, JsonObject, JsonValue, SafeSigned, SafeUnsigned,
    Tool, ToolCall,
};

const MAX_CONTAINER_DEPTH: usize = 128;
const MAX_SCHEMA_BYTES: usize = 256 * 1024;
const MAX_ARGUMENT_BYTES: usize = 1024 * 1024;
const MAX_PATTERN_COUNT: usize = 32;
const MAX_PATTERN_BYTES: usize = 8 * 1024;
const MAX_SCHEMA_GRAPH_CONTEXTS: usize = 65_536;
const MAX_REFERENCE_SCOPE_DEPTH: usize = MAX_CONTAINER_DEPTH;
const MAX_RESOLVER_URI_BYTES: usize = 4 * MAX_SCHEMA_BYTES;
const MAX_ISSUES: usize = 16;
const MAX_PATH_BYTES: usize = 256;
const MAX_KEYWORD_BYTES: usize = 64;
const VALIDATOR_CACHE_CAPACITY: usize = 16;

type CacheDigest = [u8; 16];

struct CompiledToolSchema {
    validator: jsonschema::Validator,
    pattern_property_probes: Vec<jsonschema::Validator>,
}

struct CacheEntry<V, K = Vec<u8>> {
    digest: CacheDigest,
    canonical: K,
    value: Arc<V>,
}

struct BoundedCache<V, K = Vec<u8>> {
    capacity: usize,
    entries: VecDeque<CacheEntry<V, K>>,
}

impl<V, K> BoundedCache<V, K>
where
    K: AsRef<[u8]>,
{
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: VecDeque::with_capacity(capacity),
        }
    }

    fn find(&self, digest: CacheDigest, canonical: &[u8]) -> Option<Arc<V>> {
        self.entries
            .iter()
            .find(|entry| entry.digest == digest && entry.canonical.as_ref() == canonical)
            .map(|entry| Arc::clone(&entry.value))
    }
}

static VALIDATOR_CACHE: OnceLock<Mutex<BoundedCache<CompiledToolSchema>>> = OnceLock::new();

/// One bounded, redacted tool-argument validation issue.
///
/// Validation issues intentionally do not cross Serde boundaries.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ToolValidationIssue>();
/// ```
///
/// Its fields are private so callers cannot construct unbounded diagnostics.
///
/// ```compile_fail
/// let _ = pi_ai_core::ToolValidationIssue {
///     path: String::new(),
///     keyword: String::new(),
///     message: "unbounded",
/// };
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolValidationIssue {
    path: String,
    keyword: String,
    message: &'static str,
}

impl ToolValidationIssue {
    /// Returns the dotted argument path, using `root` for the root value.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Returns the schema keyword or fixed validation category.
    pub fn keyword(&self) -> &str {
        &self.keyword
    }

    /// Returns a fixed message that never includes an argument value.
    pub fn message(&self) -> &str {
        self.message
    }
}

impl fmt::Display for ToolValidationIssue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} [{}]: {}",
            self.path, self.keyword, self.message
        )
    }
}

/// A bounded, deterministic collection of tool validation issues.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ToolValidationReport>();
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolValidationReport {
    issues: Vec<ToolValidationIssue>,
    omitted_issue_count: SafeUnsigned,
}

impl ToolValidationReport {
    /// Returns the retained issues in deterministic lexical order.
    pub fn issues(&self) -> &[ToolValidationIssue] {
        &self.issues
    }

    /// Returns how many additional issues were omitted.
    pub const fn omitted_issue_count(&self) -> SafeUnsigned {
        self.omitted_issue_count
    }
}

impl fmt::Display for ToolValidationReport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "tool arguments failed validation ({} retained, {} omitted)",
            self.issues.len(),
            self.omitted_issue_count.get()
        )
    }
}

/// A typed, redacted tool validation failure.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::ToolValidationError>();
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ToolValidationError {
    /// No declaration has the requested exact, case-sensitive tool name.
    ToolNotFound,
    /// The tool schema is invalid or exceeds a validation bound.
    InvalidSchema,
    /// The arguments do not satisfy the schema or exceed an argument bound.
    InvalidArguments(ToolValidationReport),
}

impl fmt::Display for ToolValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::ToolNotFound => "tool not found",
            Self::InvalidSchema => "invalid tool schema",
            Self::InvalidArguments(_) => "tool arguments failed validation",
        })
    }
}

impl std::error::Error for ToolValidationError {}

/// Finds the first exact tool-name match and validates its call arguments.
pub fn validate_tool_call(
    tools: &[Tool],
    tool_call: &ToolCall,
) -> Result<JsonObject, ToolValidationError> {
    let tool = tools
        .iter()
        .find(|tool| tool.name == tool_call.name)
        .ok_or(ToolValidationError::ToolNotFound)?;
    validate_tool_arguments(tool, tool_call)
}

/// Validates one call's arguments against a tool declaration.
///
/// This function intentionally does not require the declaration and call names
/// to match. Use [`validate_tool_call`] when name selection is required.
pub fn validate_tool_arguments(
    tool: &Tool,
    tool_call: &ToolCall,
) -> Result<JsonObject, ToolValidationError> {
    preflight_structure(&tool.parameters.0, MAX_SCHEMA_BYTES)
        .map_err(|()| ToolValidationError::InvalidSchema)?;
    preflight_arguments(&tool_call.arguments, MAX_ARGUMENT_BYTES)
        .map_err(|()| invalid_argument_preflight_report())?;

    // Owned conversion occurs only after both borrowed-input structural
    // preflights have succeeded.
    let canonical_schema = canonical_object_bytes(&tool.parameters.0);
    let schema = object_to_serde(&tool.parameters.0);
    let compiled = cached_tool_schema(
        &schema,
        canonical_schema,
        tool.parameters.0.as_map().contains_key("$schema"),
    )?;

    // Argument cloning occurs only after the schema graph is accepted or an
    // exact canonical cache entry proves it was accepted previously.
    let arguments = tool_call.arguments.clone();
    let mut serialized_arguments = object_to_serde(&arguments);
    normalize_optional_nulls(
        &mut serialized_arguments,
        &schema,
        compiled.validator.draft(),
    )?;
    coerce_with_schema(
        &mut serialized_arguments,
        &schema,
        compiled.validator.draft(),
    )?;

    if let Some(report) = pattern_property_runtime_report(&compiled, &serialized_arguments) {
        return Err(ToolValidationError::InvalidArguments(report));
    }
    let report = report_from_errors(compiled.validator.iter_errors(&serialized_arguments));
    if let Some(report) = report {
        return Err(ToolValidationError::InvalidArguments(report));
    }

    object_from_serde(serialized_arguments).ok_or(ToolValidationError::InvalidSchema)
}

fn cached_tool_schema(
    schema: &SerdeValue,
    canonical: Vec<u8>,
    autodetect_draft: bool,
) -> Result<Arc<CompiledToolSchema>, ToolValidationError> {
    let digest = stable_digest(&canonical);
    cache_get_or_compile(validator_cache(), digest, canonical, || {
        let schema_preflight = inspect_schema_graph(schema, autodetect_draft)?;
        let validator = build_root_validator(schema, autodetect_draft)?;
        let pattern_property_probes = compile_pattern_property_probes(
            &schema_preflight.pattern_property_patterns,
            validator.draft(),
        )?;
        Ok(CompiledToolSchema {
            validator,
            pattern_property_probes,
        })
    })
}

fn validator_cache() -> &'static Mutex<BoundedCache<CompiledToolSchema>> {
    VALIDATOR_CACHE.get_or_init(|| Mutex::new(BoundedCache::new(VALIDATOR_CACHE_CAPACITY)))
}

fn cache_get_or_compile<V, E, K>(
    cache: &Mutex<BoundedCache<V, K>>,
    digest: CacheDigest,
    canonical: K,
    compile: impl FnOnce() -> Result<V, E>,
) -> Result<Arc<V>, E>
where
    K: AsRef<[u8]>,
{
    {
        let cache = lock_recover(cache);
        if let Some(value) = cache.find(digest, canonical.as_ref()) {
            return Ok(value);
        }
    }

    let candidate = Arc::new(compile()?);
    let mut cache = lock_recover(cache);
    if let Some(value) = cache.find(digest, canonical.as_ref()) {
        drop(cache);
        drop(candidate);
        drop(canonical);
        return Ok(value);
    }

    let evicted = if cache.entries.len() == cache.capacity {
        cache.entries.pop_front()
    } else {
        None
    };
    cache.entries.push_back(CacheEntry {
        digest,
        canonical,
        value: Arc::clone(&candidate),
    });
    drop(cache);
    drop(evicted);
    Ok(candidate)
}

fn lock_recover<V>(mutex: &Mutex<V>) -> MutexGuard<'_, V> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn stable_digest(bytes: &[u8]) -> CacheDigest {
    let mut left = 0xcbf2_9ce4_8422_2325_u64;
    let mut right = 0x6c62_272e_07bb_0142_u64;
    for byte in bytes {
        left ^= u64::from(*byte);
        left = left.wrapping_mul(0x0000_0100_0000_01b3);
        right ^= u64::from(*byte).wrapping_add(0x9e37_79b9_7f4a_7c15);
        right = right.rotate_left(13).wrapping_mul(0x9ddf_ea08_eb38_2d69);
    }
    let mut digest = [0_u8; 16];
    digest[..8].copy_from_slice(&left.to_le_bytes());
    digest[8..].copy_from_slice(&right.to_le_bytes());
    digest
}

fn build_root_validator(
    schema: &SerdeValue,
    autodetect_draft: bool,
) -> Result<jsonschema::Validator, ToolValidationError> {
    let pattern_options = jsonschema::PatternOptions::fancy_regex()
        .backtrack_limit(100_000)
        .size_limit(131_072)
        .dfa_size_limit(65_536);
    let options = jsonschema::options()
        .offline()
        .with_pattern_options(pattern_options);
    if autodetect_draft {
        options.build(schema)
    } else {
        options.with_draft(jsonschema::Draft::Draft7).build(schema)
    }
    .map_err(|_| ToolValidationError::InvalidSchema)
}

fn build_subschema_validator(
    schema: &SerdeValue,
    draft: jsonschema::Draft,
) -> Result<jsonschema::Validator, ToolValidationError> {
    let pattern_options = jsonschema::PatternOptions::fancy_regex()
        .backtrack_limit(100_000)
        .size_limit(131_072)
        .dfa_size_limit(65_536);
    jsonschema::options()
        .offline()
        .with_pattern_options(pattern_options)
        .with_draft(draft)
        .build(schema)
        .map_err(|_| ToolValidationError::InvalidSchema)
}

fn compile_pattern_property_probes(
    patterns: &[String],
    draft: jsonschema::Draft,
) -> Result<Vec<jsonschema::Validator>, ToolValidationError> {
    let mut probes = Vec::with_capacity(patterns.len());
    for pattern in patterns {
        let probe_schema = SerdeValue::Object(
            [
                ("type".to_owned(), SerdeValue::String("string".to_owned())),
                ("pattern".to_owned(), SerdeValue::String(pattern.clone())),
            ]
            .into_iter()
            .collect(),
        );
        probes.push(build_subschema_validator(&probe_schema, draft)?);
    }
    Ok(probes)
}

fn pattern_property_runtime_report(
    compiled: &CompiledToolSchema,
    arguments: &SerdeValue,
) -> Option<ToolValidationReport> {
    if compiled.pattern_property_probes.is_empty() {
        return None;
    }
    // jsonschema 0.52.1 converts patternProperties engine failures into a
    // non-match. Conservatively probe every bounded argument object key against
    // every cached patternProperties regex. Ordinary non-matches are ignored;
    // only a resource/runtime engine failure rejects the call.
    let mut stack = vec![arguments];
    while let Some(value) = stack.pop() {
        match value {
            SerdeValue::Array(values) => stack.extend(values),
            SerdeValue::Object(values) => {
                for (key, value) in values {
                    let key = SerdeValue::String(key.clone());
                    for probe in &compiled.pattern_property_probes {
                        if let Err(error) = probe.validate(&key)
                            && matches!(
                                error.kind(),
                                ValidationErrorKind::BacktrackLimitExceeded { .. }
                                    | ValidationErrorKind::RegexEngineFailure { .. }
                            )
                        {
                            return Some(ToolValidationReport {
                                issues: vec![ToolValidationIssue {
                                    path: "root".to_owned(),
                                    keyword: "patternProperties".to_owned(),
                                    message: "pattern evaluation limit exceeded",
                                }],
                                omitted_issue_count: safe_unsigned(0),
                            });
                        }
                    }
                    stack.push(value);
                }
            }
            _ => {}
        }
    }
    None
}

fn normalize_optional_nulls(
    value: &mut SerdeValue,
    schema: &SerdeValue,
    draft: jsonschema::Draft,
) -> Result<(), ToolValidationError> {
    if let SerdeValue::Array(values) = value {
        let Some(items) = schema.as_object().and_then(|schema| schema.get("items")) else {
            return Ok(());
        };
        if let Some(item_schemas) = items.as_array() {
            for (value, item_schema) in values.iter_mut().zip(item_schemas) {
                normalize_optional_nulls(value, item_schema, draft)?;
            }
        } else if items.is_object() || items.is_boolean() {
            for value in values {
                normalize_optional_nulls(value, items, draft)?;
            }
        }
        return Ok(());
    }

    let (SerdeValue::Object(value), Some(schema)) = (value, schema.as_object()) else {
        return Ok(());
    };
    let Some(properties) = schema.get("properties").and_then(SerdeValue::as_object) else {
        return Ok(());
    };
    let required = schema.get("required").and_then(SerdeValue::as_array);
    for (key, property_schema) in properties {
        let is_required = required.is_some_and(|required| {
            required
                .iter()
                .any(|required_key| required_key.as_str() == Some(key))
        });
        let direct_string_ref = property_schema
            .as_object()
            .and_then(|schema| schema.get("$ref"))
            .is_some_and(SerdeValue::is_string);
        let Some(property_value) = value.get_mut(key) else {
            continue;
        };
        if property_value.is_null() && !is_required && !direct_string_ref {
            let accepts_null = build_subschema_validator(property_schema, draft)
                .map_or(true, |validator| validator.is_valid(&SerdeValue::Null));
            if !accepts_null {
                value.remove(key);
                continue;
            }
        }
        normalize_optional_nulls(property_value, property_schema, draft)?;
    }
    Ok(())
}

fn coerce_with_schema(
    value: &mut SerdeValue,
    schema: &SerdeValue,
    draft: jsonschema::Draft,
) -> Result<(), ToolValidationError> {
    let Some(schema) = schema.as_object() else {
        return Ok(());
    };

    if let Some(schemas) = schema.get("allOf").and_then(SerdeValue::as_array) {
        for schema in schemas {
            coerce_with_schema(value, schema, draft)?;
        }
    }
    if let Some(schemas) = schema.get("anyOf").and_then(SerdeValue::as_array) {
        coerce_with_union(value, schemas, draft)?;
    }
    if let Some(schemas) = schema.get("oneOf").and_then(SerdeValue::as_array) {
        coerce_with_union(value, schemas, draft)?;
    }

    let schema_types = schema
        .get("type")
        .map(schema_type_names)
        .unwrap_or_default();
    let preserves_type_array_value = schema.get("type").is_some_and(SerdeValue::is_array)
        && schema_types
            .iter()
            .any(|schema_type| matches_json_type(value, schema_type));
    if !preserves_type_array_value {
        for schema_type in &schema_types {
            if let Some(coerced) = coerce_primitive(value, schema_type) {
                *value = coerced;
                break;
            }
        }
    }

    if schema_types.contains(&"object")
        && let SerdeValue::Object(values) = value
    {
        let properties = schema.get("properties").and_then(SerdeValue::as_object);
        if let Some(properties) = properties {
            for (key, property_schema) in properties {
                if let Some(property_value) = values.get_mut(key) {
                    coerce_with_schema(property_value, property_schema, draft)?;
                }
            }
        }
        if let Some(additional_schema) = schema
            .get("additionalProperties")
            .filter(|schema| schema.is_object() || schema.is_boolean())
        {
            for (key, property_value) in values {
                if !properties.is_some_and(|properties| properties.contains_key(key)) {
                    coerce_with_schema(property_value, additional_schema, draft)?;
                }
            }
        }
    }
    if schema_types.contains(&"array")
        && let SerdeValue::Array(values) = value
        && let Some(items) = schema.get("items")
    {
        if let Some(item_schemas) = items.as_array() {
            for (value, item_schema) in values.iter_mut().zip(item_schemas) {
                coerce_with_schema(value, item_schema, draft)?;
            }
        } else if items.is_object() || items.is_boolean() {
            for value in values {
                coerce_with_schema(value, items, draft)?;
            }
        }
    }
    Ok(())
}

fn coerce_with_union(
    value: &mut SerdeValue,
    schemas: &[SerdeValue],
    draft: jsonschema::Draft,
) -> Result<(), ToolValidationError> {
    for schema in schemas {
        if build_subschema_validator(schema, draft).is_ok_and(|validator| validator.is_valid(value))
        {
            return Ok(());
        }
    }
    for schema in schemas {
        let mut candidate = value.clone();
        coerce_with_schema(&mut candidate, schema, draft)?;
        if build_subschema_validator(schema, draft)
            .is_ok_and(|validator| validator.is_valid(&candidate))
        {
            *value = candidate;
            return Ok(());
        }
    }
    Ok(())
}

fn schema_type_names(value: &SerdeValue) -> Vec<&str> {
    match value {
        SerdeValue::String(value) => vec![value],
        SerdeValue::Array(values) => values.iter().filter_map(SerdeValue::as_str).collect(),
        _ => Vec::new(),
    }
}

fn matches_json_type(value: &SerdeValue, schema_type: &str) -> bool {
    match schema_type {
        "number" => value.is_number(),
        "integer" => value.as_f64().is_some_and(|value| value.fract() == 0.0),
        "boolean" => value.is_boolean(),
        "string" => value.is_string(),
        "null" => value.is_null(),
        "array" => value.is_array(),
        "object" => value.is_object(),
        _ => false,
    }
}

fn coerce_primitive(value: &SerdeValue, schema_type: &str) -> Option<SerdeValue> {
    match schema_type {
        "number" => match value {
            SerdeValue::Null => Some(number_value(0.0)),
            SerdeValue::Bool(value) => Some(number_value(f64::from(u8::from(*value)))),
            SerdeValue::String(value) => parse_finite_number(value).map(number_value),
            _ => None,
        },
        "integer" => match value {
            SerdeValue::Null => Some(number_value(0.0)),
            SerdeValue::Bool(value) => Some(number_value(f64::from(u8::from(*value)))),
            SerdeValue::String(value) => parse_finite_number(value)
                .filter(|value| value.fract() == 0.0)
                .map(number_value),
            _ => None,
        },
        "boolean" => match value {
            SerdeValue::Null => Some(SerdeValue::Bool(false)),
            SerdeValue::String(value) if value == "true" => Some(SerdeValue::Bool(true)),
            SerdeValue::String(value) if value == "false" => Some(SerdeValue::Bool(false)),
            SerdeValue::Number(value) if value.as_f64() == Some(1.0) => {
                Some(SerdeValue::Bool(true))
            }
            SerdeValue::Number(value) if value.as_f64() == Some(0.0) => {
                Some(SerdeValue::Bool(false))
            }
            _ => None,
        },
        "string" => match value {
            SerdeValue::Null => Some(SerdeValue::String(String::new())),
            SerdeValue::Bool(value) => Some(SerdeValue::String(value.to_string())),
            SerdeValue::Number(value) => Some(SerdeValue::String(
                ryu_js::Buffer::new()
                    .format_finite(value.as_f64()?)
                    .to_owned(),
            )),
            _ => None,
        },
        "null" => match value {
            SerdeValue::String(value) if value.is_empty() => Some(SerdeValue::Null),
            SerdeValue::Number(value) if value.as_f64() == Some(0.0) => Some(SerdeValue::Null),
            SerdeValue::Bool(false) => Some(SerdeValue::Null),
            _ => None,
        },
        _ => None,
    }
}

fn parse_finite_number(value: &str) -> Option<f64> {
    let value = value.trim_matches(is_ecmascript_whitespace);
    if value.is_empty() {
        return None;
    }
    for (prefix, radix, bits_per_digit) in [
        ("0x", 16, 4),
        ("0X", 16, 4),
        ("0b", 2, 1),
        ("0B", 2, 1),
        ("0o", 8, 3),
        ("0O", 8, 3),
    ] {
        if let Some(digits) = value.strip_prefix(prefix) {
            return parse_unsigned_radix(digits, radix, bits_per_digit);
        }
    }
    static DECIMAL_NUMBER: OnceLock<regex::Regex> = OnceLock::new();
    let decimal_number = DECIMAL_NUMBER.get_or_init(|| {
        let Ok(pattern) = regex::Regex::new(
            r"^[+-]?(?:(?:[0-9]+(?:\.[0-9]*)?)|(?:\.[0-9]+))(?:[eE][+-]?[0-9]+)?$",
        ) else {
            unreachable!("the built-in decimal-number pattern is valid");
        };
        pattern
    });
    if !decimal_number.is_match(value) {
        return None;
    }
    value.parse::<f64>().ok().filter(|value| value.is_finite())
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

fn parse_unsigned_radix(digits: &str, radix: u8, bits_per_digit: usize) -> Option<f64> {
    if digits.is_empty() {
        return None;
    }
    let mut started = false;
    let mut significant_bits = 0_usize;
    let mut significand = 0_u64;
    let mut guard = false;
    let mut sticky = false;
    for byte in digits.bytes() {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => return None,
        };
        if digit >= radix {
            return None;
        }
        for bit_index in (0..bits_per_digit).rev() {
            let bit = digit & (1 << bit_index) != 0;
            if !started {
                if !bit {
                    continue;
                }
                started = true;
            }
            significant_bits = significant_bits.checked_add(1)?;
            if significant_bits <= 53 {
                significand = (significand << 1) | u64::from(bit);
            } else if significant_bits == 54 {
                guard = bit;
            } else {
                sticky |= bit;
            }
        }
    }
    if !started {
        return Some(0.0);
    }
    if significant_bits <= 53 {
        return Some(significand as f64);
    }
    let mut exponent = significant_bits - 1;
    if guard && (sticky || significand & 1 != 0) {
        significand += 1;
        if significand == 1 << 53 {
            significand >>= 1;
            exponent += 1;
        }
    }
    if exponent > 1023 {
        return None;
    }
    let value = significand as f64 * 2.0_f64.powi(exponent as i32 - 52);
    value.is_finite().then_some(value)
}

fn number_value(value: f64) -> SerdeValue {
    if value == 0.0 && value.is_sign_negative() {
        return finite_number_value(value);
    }
    if value.fract() == 0.0 && value.abs() <= JS_SAFE_INTEGER_MAX as f64 {
        if value.is_sign_negative() {
            return SerdeValue::Number(SerdeNumber::from(value as i64));
        }
        return SerdeValue::Number(SerdeNumber::from(value as u64));
    }
    finite_number_value(value)
}

fn finite_number_value(value: f64) -> SerdeValue {
    let Some(value) = SerdeNumber::from_f64(value) else {
        unreachable!("numeric coercion only creates finite values");
    };
    SerdeValue::Number(value)
}

fn invalid_argument_preflight_report() -> ToolValidationError {
    ToolValidationError::InvalidArguments(ToolValidationReport {
        issues: vec![ToolValidationIssue {
            path: "root".to_owned(),
            keyword: "preflight".to_owned(),
            message: "arguments exceed validation limits",
        }],
        omitted_issue_count: safe_unsigned(0),
    })
}

fn report_from_errors<'a>(
    errors: impl Iterator<Item = jsonschema::ValidationError<'a>>,
) -> Option<ToolValidationReport> {
    let mut total = 0_usize;
    let mut issues = Vec::with_capacity(MAX_ISSUES);
    for error in errors {
        total += 1;
        let issue = issue_from_error(&error);
        if issues.len() < MAX_ISSUES {
            issues.push(issue);
            issues.sort_unstable_by(compare_issues);
        } else if compare_issues(&issue, &issues[MAX_ISSUES - 1]) == Ordering::Less {
            issues[MAX_ISSUES - 1] = issue;
            issues.sort_unstable_by(compare_issues);
        }
    }
    if total == 0 {
        return None;
    }
    Some(ToolValidationReport {
        omitted_issue_count: safe_unsigned(
            u64::try_from(total - issues.len()).expect("bounded arguments limit issue count"),
        ),
        issues,
    })
}

fn compare_issues(left: &ToolValidationIssue, right: &ToolValidationIssue) -> Ordering {
    (&left.path, &left.keyword, left.message).cmp(&(&right.path, &right.keyword, right.message))
}

fn issue_from_error(error: &jsonschema::ValidationError<'_>) -> ToolValidationIssue {
    let mut path = format_instance_path(error.instance_path().as_str());
    if let ValidationErrorKind::Required { property } = error.kind()
        && let Some(property) = property.as_str()
    {
        append_path_component(&mut path, property);
    }
    if path.is_empty() {
        path.push_str("root");
    }

    ToolValidationIssue {
        path,
        keyword: bounded_string(error.kind().keyword(), MAX_KEYWORD_BYTES),
        message: fixed_message(error.kind()),
    }
}

fn fixed_message(kind: &ValidationErrorKind) -> &'static str {
    match kind {
        ValidationErrorKind::Required { .. } => "required property is missing",
        ValidationErrorKind::Type { .. } => "value has an invalid type",
        ValidationErrorKind::Pattern { .. } => "value does not match the pattern",
        ValidationErrorKind::BacktrackLimitExceeded { .. }
        | ValidationErrorKind::RegexEngineFailure { .. } => "pattern evaluation limit exceeded",
        ValidationErrorKind::AdditionalProperties { .. }
        | ValidationErrorKind::UnevaluatedProperties { .. } => {
            "object contains an unsupported property"
        }
        ValidationErrorKind::AdditionalItems { .. }
        | ValidationErrorKind::UnevaluatedItems { .. } => "array contains an unsupported item",
        _ => "value violates a schema constraint",
    }
}

fn append_path_component(path: &mut String, component: &str) {
    if path.len() >= MAX_PATH_BYTES {
        return;
    }
    if !path.is_empty() {
        path.push('.');
    }
    let remaining = MAX_PATH_BYTES - path.len();
    let end = utf8_prefix_len(component, remaining);
    path.push_str(&component[..end]);
}

fn format_instance_path(pointer: &str) -> String {
    let Some(pointer) = pointer.strip_prefix('/') else {
        return String::new();
    };
    let mut output = String::new();
    for (index, token) in pointer.split('/').enumerate() {
        if index != 0 {
            append_path_character(&mut output, '.');
        }
        let mut characters = token.chars();
        while let Some(character) = characters.next() {
            if character == '~' {
                match characters.next() {
                    Some('0') => append_path_character(&mut output, '~'),
                    Some('1') => append_path_character(&mut output, '.'),
                    Some(character) => {
                        append_path_character(&mut output, '~');
                        append_path_character(&mut output, character);
                    }
                    None => append_path_character(&mut output, '~'),
                }
            } else if character == '/' {
                append_path_character(&mut output, '.');
            } else {
                append_path_character(&mut output, character);
            }
        }
    }
    output
}

fn append_path_character(path: &mut String, character: char) {
    if path.len() + character.len_utf8() <= MAX_PATH_BYTES {
        path.push(character);
    }
}

fn bounded_string(value: &str, max_bytes: usize) -> String {
    value[..utf8_prefix_len(value, max_bytes)].to_owned()
}

fn utf8_prefix_len(value: &str, max_bytes: usize) -> usize {
    let mut end = value.len().min(max_bytes);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    end
}

fn safe_unsigned(value: u64) -> SafeUnsigned {
    match SafeUnsigned::new(value) {
        Ok(value) => value,
        Err(_) => unreachable!("bounded validation counts are JavaScript-safe"),
    }
}

fn preflight_arguments(root: &JsonObject, byte_limit: usize) -> Result<(), ()> {
    preflight_structure(root, byte_limit)
}

fn preflight_structure(root: &JsonObject, byte_limit: usize) -> Result<(), ()> {
    let mut state = StructurePreflightState { encoded_len: 0 };
    let mut stack = Vec::new();
    inspect_structure_object(root.as_map(), 1, byte_limit, &mut state, &mut stack)?;

    while let Some((value, parent_depth)) = stack.pop() {
        match value {
            JsonValue::Null => add_encoded_len(&mut state, 4, byte_limit)?,
            JsonValue::Bool(value) => {
                add_encoded_len(&mut state, if *value { 4 } else { 5 }, byte_limit)?;
            }
            JsonValue::Number(value) => {
                add_encoded_len(&mut state, number_len(*value), byte_limit)?;
            }
            JsonValue::String(value) => {
                let len = escaped_string_len(value).ok_or(())?;
                add_encoded_len(&mut state, len, byte_limit)?;
            }
            JsonValue::Array(values) => {
                let depth = parent_depth.checked_add(1).ok_or(())?;
                if depth > MAX_CONTAINER_DEPTH {
                    return Err(());
                }
                let punctuation = 2_usize
                    .checked_add(values.len().saturating_sub(1))
                    .ok_or(())?;
                add_encoded_len(&mut state, punctuation, byte_limit)?;
                stack.extend(values.iter().map(|value| (value, depth)));
            }
            JsonValue::Object(values) => inspect_structure_object(
                values.as_map(),
                parent_depth.checked_add(1).ok_or(())?,
                byte_limit,
                &mut state,
                &mut stack,
            )?,
        }
    }
    Ok(())
}

struct StructurePreflightState {
    encoded_len: usize,
}

fn inspect_structure_object<'a>(
    values: &'a BTreeMap<String, JsonValue>,
    depth: usize,
    byte_limit: usize,
    state: &mut StructurePreflightState,
    stack: &mut Vec<(&'a JsonValue, usize)>,
) -> Result<(), ()> {
    if depth > MAX_CONTAINER_DEPTH {
        return Err(());
    }
    let punctuation = 2_usize
        .checked_add(values.len().saturating_sub(1))
        .ok_or(())?;
    add_encoded_len(state, punctuation, byte_limit)?;
    for (key, value) in values {
        let key_len = escaped_string_len(key).ok_or(())?;
        add_encoded_len(state, key_len.checked_add(1).ok_or(())?, byte_limit)?;
        stack.push((value, depth));
    }
    Ok(())
}

struct SchemaPreflight {
    pattern_count: usize,
    pattern_bytes: usize,
    pattern_property_patterns: Vec<String>,
}

#[derive(Default)]
struct ResolverContextInterner {
    uri_ids: BTreeMap<String, usize>,
    uri_bytes: usize,
    scope_ids: BTreeMap<Vec<usize>, usize>,
}

impl ResolverContextInterner {
    fn intern_uri(&mut self, uri: &str) -> Result<usize, ToolValidationError> {
        if let Some(id) = self.uri_ids.get(uri) {
            return Ok(*id);
        }
        let next_bytes = self
            .uri_bytes
            .checked_add(uri.len())
            .ok_or(ToolValidationError::InvalidSchema)?;
        enforce_schema_graph_limits(0, 0, next_bytes)?;
        let id = self.uri_ids.len();
        self.uri_ids.insert(uri.to_owned(), id);
        self.uri_bytes = next_bytes;
        Ok(id)
    }

    fn intern_scope<'a>(
        &mut self,
        uris: impl IntoIterator<Item = &'a jsonschema::Uri<String>>,
    ) -> Result<(usize, usize), ToolValidationError> {
        let mut uri_ids = Vec::new();
        for uri in uris {
            if uri_ids.len() == MAX_REFERENCE_SCOPE_DEPTH {
                return Err(ToolValidationError::InvalidSchema);
            }
            uri_ids.push(self.intern_uri(uri.as_str())?);
        }
        let depth = uri_ids.len();
        if let Some(id) = self.scope_ids.get(&uri_ids) {
            return Ok((*id, depth));
        }
        let id = self.scope_ids.len();
        self.scope_ids.insert(uri_ids, id);
        Ok((id, depth))
    }
}

struct OfflineSchemaRetriever;

impl jsonschema::Retrieve for OfflineSchemaRetriever {
    fn retrieve(
        &self,
        _uri: &jsonschema::Uri<String>,
    ) -> Result<SerdeValue, Box<dyn std::error::Error + Send + Sync>> {
        Err(Box::new(std::io::Error::other(
            "external schema retrieval is disabled",
        )))
    }
}

#[derive(Clone, Copy)]
enum SchemaResolverState {
    Parent,
    ResolvedTarget,
}

fn inspect_schema_graph(
    schema: &SerdeValue,
    autodetect_draft: bool,
) -> Result<SchemaPreflight, ToolValidationError> {
    const DEFAULT_BASE_URI: &str = "json-schema:///";

    let draft = if autodetect_draft {
        jsonschema::Draft::default().detect(schema)
    } else {
        jsonschema::Draft::Draft7
    };
    let root_resource = draft.create_resource_ref(schema);
    let base_uri = jsonschema::uri::from_str(root_resource.id().unwrap_or(DEFAULT_BASE_URI))
        .map_err(|_| ToolValidationError::InvalidSchema)?;
    let registry = jsonschema::Registry::new()
        .retriever(OfflineSchemaRetriever)
        .draft(draft)
        .add(base_uri.as_str(), root_resource)
        .and_then(|builder| builder.prepare())
        .map_err(|_| ToolValidationError::InvalidSchema)?;
    let base_uri = normalize_registry_base(&registry, base_uri)?;

    let mut state = SchemaPreflight {
        pattern_count: 0,
        pattern_bytes: 0,
        pattern_property_patterns: Vec::new(),
    };
    let mut counted_patterns = BTreeSet::new();
    let mut seen_contexts = HashSet::new();
    let mut resolver_contexts = ResolverContextInterner::default();
    let mut stack = vec![(
        schema,
        registry.resolver(base_uri),
        draft,
        SchemaResolverState::Parent,
    )];

    while let Some((value, resolver, inherited_draft, resolver_state)) = stack.pop() {
        let (resolver, draft) = match resolver_state {
            SchemaResolverState::Parent => {
                let draft = inherited_draft.detect(value);
                let resolver = resolver
                    .in_subresource(draft.create_resource_ref(value))
                    .map_err(|_| ToolValidationError::InvalidSchema)?;
                (resolver, draft)
            }
            SchemaResolverState::ResolvedTarget => (resolver, inherited_draft),
        };
        let base_uri = resolver.base_uri();
        let base_uri_id = resolver_contexts.intern_uri(base_uri.as_str())?;
        let dynamic_scope = resolver.dynamic_scope();
        let (dynamic_scope_id, dynamic_scope_depth) =
            resolver_contexts.intern_scope(&dynamic_scope)?;
        let context = (
            std::ptr::from_ref(value) as usize,
            draft,
            base_uri_id,
            dynamic_scope_id,
        );
        let inserted = seen_contexts.insert(context);
        enforce_schema_graph_limits(
            seen_contexts.len(),
            dynamic_scope_depth,
            resolver_contexts.uri_bytes,
        )?;
        if !inserted {
            continue;
        }

        let Some(object) = value.as_object() else {
            continue;
        };

        for keyword in ["$ref", "$dynamicRef"] {
            if !draft.is_known_keyword(keyword) {
                continue;
            }
            let Some(reference) = object.get(keyword) else {
                continue;
            };
            let reference = reference
                .as_str()
                .filter(|reference| reference.starts_with('#'))
                .ok_or(ToolValidationError::InvalidSchema)?;
            let resolved = resolver
                .lookup(reference)
                .map_err(|_| ToolValidationError::InvalidSchema)?;
            let (target, target_resolver, target_draft) = resolved.into_inner();
            stack.push((
                target,
                target_resolver,
                target_draft,
                SchemaResolverState::ResolvedTarget,
            ));
        }

        if draft.is_known_keyword("$recursiveRef")
            && let Some(reference) = object.get("$recursiveRef")
        {
            reference
                .as_str()
                .filter(|reference| reference.starts_with('#'))
                .ok_or(ToolValidationError::InvalidSchema)?;
            let resolved = resolver
                .lookup_recursive_ref()
                .map_err(|_| ToolValidationError::InvalidSchema)?;
            let (target, target_resolver, target_draft) = resolved.into_inner();
            stack.push((
                target,
                target_resolver,
                target_draft,
                SchemaResolverState::ResolvedTarget,
            ));
        }

        if draft.is_known_keyword("pattern")
            && let Some(pattern_value @ SerdeValue::String(pattern)) = object.get("pattern")
        {
            add_schema_pattern(
                pattern,
                (0, std::ptr::from_ref(pattern_value) as usize),
                false,
                &mut state,
                &mut counted_patterns,
            )?;
        }

        if draft.is_known_keyword("patternProperties")
            && let Some(SerdeValue::Object(patterns)) = object.get("patternProperties")
        {
            for pattern in patterns.keys() {
                add_schema_pattern(
                    pattern,
                    (1, std::ptr::from_ref(pattern) as usize),
                    true,
                    &mut state,
                    &mut counted_patterns,
                )?;
            }
        }

        let mut children = Vec::new();
        collect_schema_children(draft, object, &mut children);
        stack.extend(
            children
                .into_iter()
                .map(|child| (child, resolver.clone(), draft, SchemaResolverState::Parent)),
        );
    }

    Ok(state)
}

fn collect_schema_children<'a>(
    draft: jsonschema::Draft,
    object: &'a SerdeMap<String, SerdeValue>,
    children: &mut Vec<&'a SerdeValue>,
) {
    for keyword in [
        "$defs",
        "definitions",
        "dependentSchemas",
        "patternProperties",
        "properties",
    ] {
        if draft.is_known_keyword(keyword)
            && let Some(SerdeValue::Object(values)) = object.get(keyword)
        {
            children.extend(values.values());
        }
    }

    if draft.is_known_keyword("dependencies")
        && let Some(SerdeValue::Object(values)) = object.get("dependencies")
    {
        children.extend(
            values
                .values()
                .filter(|value| value.is_object() || value.is_boolean()),
        );
    }

    for keyword in ["allOf", "anyOf", "oneOf", "prefixItems"] {
        if draft.is_known_keyword(keyword)
            && let Some(SerdeValue::Array(values)) = object.get(keyword)
        {
            children.extend(values);
        }
    }

    if draft.is_known_keyword("items")
        && let Some(items) = object.get("items")
    {
        if let Some(values) = items.as_array() {
            children.extend(values);
        } else if items.is_object() || items.is_boolean() {
            children.push(items);
        }
    }

    for keyword in [
        "additionalItems",
        "additionalProperties",
        "contains",
        "contentSchema",
        "else",
        "if",
        "not",
        "propertyNames",
        "then",
        "unevaluatedItems",
        "unevaluatedProperties",
    ] {
        if draft.is_known_keyword(keyword)
            && let Some(value) = object
                .get(keyword)
                .filter(|value| value.is_object() || value.is_boolean())
        {
            children.push(value);
        }
    }
}

fn enforce_schema_graph_limits(
    context_count: usize,
    dynamic_scope_depth: usize,
    resolver_uri_bytes: usize,
) -> Result<(), ToolValidationError> {
    if context_count > MAX_SCHEMA_GRAPH_CONTEXTS
        || dynamic_scope_depth > MAX_REFERENCE_SCOPE_DEPTH
        || resolver_uri_bytes > MAX_RESOLVER_URI_BYTES
    {
        return Err(ToolValidationError::InvalidSchema);
    }
    Ok(())
}

fn normalize_registry_base(
    registry: &jsonschema::Registry<'_>,
    base_uri: jsonschema::Uri<String>,
) -> Result<jsonschema::Uri<String>, ToolValidationError> {
    if registry.contains_resource(base_uri.as_str()) {
        return Ok(base_uri);
    }
    if base_uri
        .fragment()
        .is_some_and(|fragment| fragment.as_str().is_empty())
    {
        let mut normalized = base_uri;
        normalized.set_fragment(None);
        if registry.contains_resource(normalized.as_str()) {
            return Ok(normalized);
        }
    }
    Err(ToolValidationError::InvalidSchema)
}

fn add_schema_pattern(
    pattern: &str,
    occurrence: (u8, usize),
    pattern_property: bool,
    state: &mut SchemaPreflight,
    counted_patterns: &mut BTreeSet<(u8, usize)>,
) -> Result<(), ToolValidationError> {
    if !counted_patterns.insert(occurrence) {
        return Ok(());
    }
    state.pattern_count = state
        .pattern_count
        .checked_add(1)
        .ok_or(ToolValidationError::InvalidSchema)?;
    state.pattern_bytes = state
        .pattern_bytes
        .checked_add(pattern.len())
        .ok_or(ToolValidationError::InvalidSchema)?;
    if state.pattern_count > MAX_PATTERN_COUNT || state.pattern_bytes > MAX_PATTERN_BYTES {
        return Err(ToolValidationError::InvalidSchema);
    }
    if pattern_property {
        state.pattern_property_patterns.push(pattern.to_owned());
    }
    Ok(())
}

fn add_encoded_len(
    state: &mut StructurePreflightState,
    additional: usize,
    byte_limit: usize,
) -> Result<(), ()> {
    state.encoded_len = state.encoded_len.checked_add(additional).ok_or(())?;
    if state.encoded_len > byte_limit {
        return Err(());
    }
    Ok(())
}

fn escaped_string_len(value: &str) -> Option<usize> {
    let mut len = 2_usize;
    for character in value.chars() {
        let character_len = match character {
            '"' | '\\' | '\u{08}' | '\t' | '\n' | '\u{0c}' | '\r' => 2,
            '\u{00}'..='\u{1f}' => 6,
            _ => character.len_utf8(),
        };
        len = len.checked_add(character_len)?;
    }
    Some(len)
}

fn number_len(value: JsonNumber) -> usize {
    match value {
        JsonNumber::Unsigned(value) => decimal_digits(value.get()),
        JsonNumber::Signed(value) => {
            usize::from(value.get().is_negative()) + decimal_digits(value.get().unsigned_abs())
        }
        JsonNumber::Float(value) => ryu_js::Buffer::new().format_finite(value.get()).len(),
    }
}

fn decimal_digits(value: u64) -> usize {
    value
        .checked_ilog10()
        .map_or(1, |digits| digits as usize + 1)
}

fn canonical_object_bytes(value: &JsonObject) -> Vec<u8> {
    let mut output = Vec::new();
    write_canonical_object(value, &mut output);
    output
}

fn write_canonical_object(value: &JsonObject, output: &mut Vec<u8>) {
    output.push(b'{');
    for (index, (key, value)) in value.as_map().iter().enumerate() {
        if index != 0 {
            output.push(b',');
        }
        write_canonical_string(key, output);
        output.push(b':');
        write_canonical_value(value, output);
    }
    output.push(b'}');
}

fn write_canonical_value(value: &JsonValue, output: &mut Vec<u8>) {
    match value {
        JsonValue::Null => output.extend_from_slice(b"null"),
        JsonValue::Bool(true) => output.extend_from_slice(b"true"),
        JsonValue::Bool(false) => output.extend_from_slice(b"false"),
        JsonValue::Number(JsonNumber::Unsigned(value)) => {
            output.extend_from_slice(value.get().to_string().as_bytes());
        }
        JsonValue::Number(JsonNumber::Signed(value)) => {
            output.extend_from_slice(value.get().to_string().as_bytes());
        }
        JsonValue::Number(JsonNumber::Float(value)) => {
            output.extend_from_slice(ryu_js::Buffer::new().format_finite(value.get()).as_bytes())
        }
        JsonValue::String(value) => write_canonical_string(value, output),
        JsonValue::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.push(b',');
                }
                write_canonical_value(value, output);
            }
            output.push(b']');
        }
        JsonValue::Object(value) => write_canonical_object(value, output),
    }
}

fn write_canonical_string(value: &str, output: &mut Vec<u8>) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.push(b'"');
    for character in value.chars() {
        match character {
            '"' => output.extend_from_slice(br#"\""#),
            '\\' => output.extend_from_slice(br"\\"),
            '\u{08}' => output.extend_from_slice(br"\b"),
            '\t' => output.extend_from_slice(br"\t"),
            '\n' => output.extend_from_slice(br"\n"),
            '\u{0c}' => output.extend_from_slice(br"\f"),
            '\r' => output.extend_from_slice(br"\r"),
            '\u{00}'..='\u{1f}' => {
                let value = character as u8;
                output.extend_from_slice(br"\u00");
                output.push(HEX[usize::from(value >> 4)]);
                output.push(HEX[usize::from(value & 0x0f)]);
            }
            _ => {
                let mut encoded = [0_u8; 4];
                output.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
            }
        }
    }
    output.push(b'"');
}

fn object_to_serde(value: &JsonObject) -> SerdeValue {
    SerdeValue::Object(
        value
            .as_map()
            .iter()
            .map(|(key, value)| (key.clone(), value_to_serde(value)))
            .collect(),
    )
}

fn value_to_serde(value: &JsonValue) -> SerdeValue {
    match value {
        JsonValue::Null => SerdeValue::Null,
        JsonValue::Bool(value) => SerdeValue::Bool(*value),
        JsonValue::Number(JsonNumber::Unsigned(value)) => {
            SerdeValue::Number(SerdeNumber::from(value.get()))
        }
        JsonValue::Number(JsonNumber::Signed(value)) => {
            SerdeValue::Number(SerdeNumber::from(value.get()))
        }
        JsonValue::Number(JsonNumber::Float(value)) => {
            let Some(number) = SerdeNumber::from_f64(value.get()) else {
                unreachable!("JsonValue float invariants require finite values");
            };
            SerdeValue::Number(number)
        }
        JsonValue::String(value) => SerdeValue::String(value.clone()),
        JsonValue::Array(values) => SerdeValue::Array(values.iter().map(value_to_serde).collect()),
        JsonValue::Object(values) => object_to_serde(values),
    }
}

fn object_from_serde(value: SerdeValue) -> Option<JsonObject> {
    let SerdeValue::Object(values) = value else {
        return None;
    };
    let values = values
        .into_iter()
        .map(|(key, value)| value_from_serde(value).map(|value| (key, value)))
        .collect::<Option<BTreeMap<_, _>>>()?;
    Some(JsonObject::new(values))
}

fn value_from_serde(value: SerdeValue) -> Option<JsonValue> {
    match value {
        SerdeValue::Null => Some(JsonValue::Null),
        SerdeValue::Bool(value) => Some(JsonValue::Bool(value)),
        SerdeValue::Number(value) if value.is_u64() => SafeUnsigned::new(value.as_u64()?)
            .ok()
            .map(JsonNumber::Unsigned)
            .map(JsonValue::Number),
        SerdeValue::Number(value) if value.is_i64() => SafeSigned::new(value.as_i64()?)
            .ok()
            .map(JsonNumber::Signed)
            .map(JsonValue::Number),
        SerdeValue::Number(value) => FiniteF64::new(value.as_f64()?)
            .ok()
            .map(JsonNumber::Float)
            .map(JsonValue::Number),
        SerdeValue::String(value) => Some(JsonValue::String(value)),
        SerdeValue::Array(values) => values
            .into_iter()
            .map(value_from_serde)
            .collect::<Option<Vec<_>>>()
            .map(JsonValue::Array),
        SerdeValue::Object(values) => serde_map_to_json(values).map(JsonValue::Object),
    }
}

fn serde_map_to_json(values: SerdeMap<String, SerdeValue>) -> Option<JsonObject> {
    let values = values
        .into_iter()
        .map(|(key, value)| value_from_serde(value).map(|value| (key, value)))
        .collect::<Option<BTreeMap<_, _>>>()?;
    Some(JsonObject::new(values))
}

#[cfg(test)]
mod cache_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, Condvar, Mutex, Weak};
    use std::thread;

    use super::{
        BoundedCache, CacheDigest, MAX_REFERENCE_SCOPE_DEPTH, MAX_RESOLVER_URI_BYTES,
        MAX_SCHEMA_GRAPH_CONTEXTS, ResolverContextInterner, ToolValidationError,
        cache_get_or_compile, canonical_object_bytes, enforce_schema_graph_limits, lock_recover,
        stable_digest,
    };
    use crate::{FiniteF64, JsonNumber, JsonObject, JsonValue};

    const COLLISION: CacheDigest = [7; 16];

    #[test]
    fn schema_graph_resource_limits_are_exact() {
        assert!(
            enforce_schema_graph_limits(
                MAX_SCHEMA_GRAPH_CONTEXTS,
                MAX_REFERENCE_SCOPE_DEPTH,
                MAX_RESOLVER_URI_BYTES,
            )
            .is_ok()
        );
        assert_eq!(
            enforce_schema_graph_limits(MAX_SCHEMA_GRAPH_CONTEXTS + 1, 0, 0),
            Err(ToolValidationError::InvalidSchema)
        );
        assert_eq!(
            enforce_schema_graph_limits(0, MAX_REFERENCE_SCOPE_DEPTH + 1, 0),
            Err(ToolValidationError::InvalidSchema)
        );
        assert_eq!(
            enforce_schema_graph_limits(0, 0, MAX_RESOLVER_URI_BYTES + 1),
            Err(ToolValidationError::InvalidSchema)
        );

        let first = jsonschema::uri::from_str("https://example.test/first").expect("valid URI");
        let second = jsonschema::uri::from_str("https://example.test/second").expect("valid URI");
        let mut scope_interner = ResolverContextInterner::default();
        let (first_scope, first_depth) = scope_interner
            .intern_scope([&first, &second])
            .expect("scope fits");
        let (same_scope, same_depth) = scope_interner
            .intern_scope([&first, &second])
            .expect("same scope fits");
        assert_eq!((same_scope, same_depth), (first_scope, first_depth));
        assert_eq!(scope_interner.scope_ids.len(), 1);
        assert_eq!(scope_interner.uri_ids.len(), 2);
        assert_eq!(
            scope_interner.intern_scope(std::iter::repeat_n(&first, MAX_REFERENCE_SCOPE_DEPTH + 1)),
            Err(ToolValidationError::InvalidSchema)
        );

        let mut uri_interner = ResolverContextInterner::default();
        let exact = "a".repeat(MAX_RESOLVER_URI_BYTES);
        let exact_id = uri_interner
            .intern_uri(&exact)
            .expect("exact URI budget fits");
        assert_eq!(
            uri_interner
                .intern_uri(&exact)
                .expect("the same effective URI is accounted once"),
            exact_id
        );
        assert_eq!(uri_interner.uri_bytes, MAX_RESOLVER_URI_BYTES);
        assert_eq!(uri_interner.uri_ids.len(), 1);
        assert_eq!(
            uri_interner.intern_uri("b"),
            Err(ToolValidationError::InvalidSchema)
        );
        assert_eq!(uri_interner.uri_bytes, MAX_RESOLVER_URI_BYTES);
        assert_eq!(uri_interner.uri_ids.len(), 1);
    }

    fn get_string(cache: &Mutex<BoundedCache<String>>, key: &[u8], value: &str) -> Arc<String> {
        cache_get_or_compile(cache, COLLISION, key.to_vec(), || {
            Ok::<String, ()>(value.to_owned())
        })
        .expect("test compilation succeeds")
    }

    #[test]
    fn canonical_bytes_and_digest_are_stable_and_fixed_width() {
        let schema = JsonObject::new(
            [
                (
                    "z".to_owned(),
                    JsonValue::Number(JsonNumber::Float(FiniteF64::new(-0.0).expect("finite"))),
                ),
                ("a".to_owned(), JsonValue::String("\n".to_owned())),
            ]
            .into_iter()
            .collect(),
        );
        let bytes = canonical_object_bytes(&schema);
        assert_eq!(bytes, br#"{"a":"\n","z":0}"#);
        assert_eq!(stable_digest(&bytes), stable_digest(&bytes));
        assert_eq!(stable_digest(&bytes).len(), 16);
    }

    #[test]
    fn forced_collisions_coexist_and_fifo_hits_do_not_reorder() {
        let cache = Mutex::new(BoundedCache::new(2));
        let first = get_string(&cache, b"first", "first");
        let second = get_string(&cache, b"second", "second");
        let first_hit = get_string(&cache, b"first", "must-not-compile");
        assert!(Arc::ptr_eq(&first, &first_hit));
        drop(first);
        drop(first_hit);
        drop(second);

        let third = get_string(&cache, b"third", "third");
        assert_eq!(third.as_str(), "third");
        let cache = lock_recover(&cache);
        assert_eq!(cache.entries.len(), 2);
        assert_eq!(cache.entries[0].canonical, b"second");
        assert_eq!(cache.entries[1].canonical, b"third");
    }

    #[test]
    fn equal_and_colliding_different_concurrent_misses_admit_correctly() {
        let equal_cache = Arc::new(Mutex::new(BoundedCache::new(16)));
        let equal_barrier = Arc::new(Barrier::new(2));
        let compile_count = Arc::new(AtomicUsize::new(0));
        let equal_threads = (0..2)
            .map(|_| {
                let cache = Arc::clone(&equal_cache);
                let barrier = Arc::clone(&equal_barrier);
                let compile_count = Arc::clone(&compile_count);
                thread::spawn(move || {
                    cache_get_or_compile(&cache, COLLISION, b"same".to_vec(), || {
                        compile_count.fetch_add(1, Ordering::SeqCst);
                        barrier.wait();
                        Ok::<String, ()>("same".to_owned())
                    })
                    .expect("compile succeeds")
                })
            })
            .collect::<Vec<_>>();
        let equal = equal_threads
            .into_iter()
            .map(|thread| thread.join().expect("thread completes"))
            .collect::<Vec<_>>();
        assert_eq!(compile_count.load(Ordering::SeqCst), 2);
        assert!(Arc::ptr_eq(&equal[0], &equal[1]));
        assert_eq!(lock_recover(&equal_cache).entries.len(), 1);

        let different_cache = Arc::new(Mutex::new(BoundedCache::new(16)));
        let different_barrier = Arc::new(Barrier::new(2));
        let different_threads = [b"left".to_vec(), b"right".to_vec()]
            .into_iter()
            .map(|key| {
                let cache = Arc::clone(&different_cache);
                let barrier = Arc::clone(&different_barrier);
                thread::spawn(move || {
                    let value = String::from_utf8(key.clone()).expect("ASCII fixture");
                    cache_get_or_compile(&cache, COLLISION, key, || {
                        barrier.wait();
                        Ok::<String, ()>(value)
                    })
                    .expect("compile succeeds")
                })
            })
            .collect::<Vec<_>>();
        for thread in different_threads {
            thread.join().expect("thread completes");
        }
        assert_eq!(lock_recover(&different_cache).entries.len(), 2);
    }

    #[test]
    fn compile_occurs_outside_lock_and_racing_admission_evicts_atomically() {
        let cache = Arc::new(Mutex::new(BoundedCache::new(1)));
        drop(get_string(&cache, b"initial", "initial"));
        let compile_state = Arc::new((Mutex::new((false, false)), Condvar::new()));

        let thread_cache = Arc::clone(&cache);
        let thread_state = Arc::clone(&compile_state);
        let delayed = thread::spawn(move || {
            cache_get_or_compile(&thread_cache, COLLISION, b"delayed".to_vec(), || {
                let (state, condition) = &*thread_state;
                let mut state = state.lock().expect("state lock");
                state.0 = true;
                condition.notify_one();
                while !state.1 {
                    state = condition.wait(state).expect("state wait");
                }
                Ok::<String, ()>("delayed".to_owned())
            })
            .expect("compile succeeds")
        });

        let (state, condition) = &*compile_state;
        let mut state = state.lock().expect("state lock");
        while !state.0 {
            state = condition.wait(state).expect("state wait");
        }
        drop(get_string(&cache, b"racer", "racer"));
        state.1 = true;
        condition.notify_one();
        drop(state);
        let delayed = delayed.join().expect("thread completes");
        assert_eq!(delayed.as_str(), "delayed");
        let cache = lock_recover(&cache);
        assert_eq!(cache.entries.len(), 1);
        assert_eq!(cache.entries[0].canonical, b"delayed");
    }

    #[test]
    fn poisoned_lock_recovers_and_invalid_values_are_not_cached() {
        let cache = Arc::new(Mutex::new(BoundedCache::<String>::new(2)));
        let poison = Arc::clone(&cache);
        assert!(
            thread::spawn(move || {
                let _guard = poison.lock().expect("initial lock");
                panic!("poison fixture");
            })
            .join()
            .is_err()
        );
        assert_eq!(get_string(&cache, b"valid", "valid").as_str(), "valid");

        let invalid_compiles = AtomicUsize::new(0);
        for _ in 0..2 {
            let invalid = cache_get_or_compile(&cache, COLLISION, b"invalid".to_vec(), || {
                invalid_compiles.fetch_add(1, Ordering::SeqCst);
                Err::<String, &'static str>("invalid")
            });
            assert_eq!(invalid.expect_err("compile fails"), "invalid");
        }
        assert_eq!(invalid_compiles.load(Ordering::SeqCst), 2);
        assert_eq!(lock_recover(&cache).entries.len(), 1);
    }

    type ReentrantCache = Mutex<BoundedCache<ReentrantValue, ReentrantBytes>>;

    struct ReentryState {
        cache: Weak<ReentrantCache>,
        drops: AtomicUsize,
    }

    struct ReentrantValue(Arc<ReentryState>);

    impl Drop for ReentrantValue {
        fn drop(&mut self) {
            reenter_cache(&self.0);
        }
    }

    struct ReentrantBytes {
        bytes: Vec<u8>,
        state: Arc<ReentryState>,
    }

    impl AsRef<[u8]> for ReentrantBytes {
        fn as_ref(&self) -> &[u8] {
            &self.bytes
        }
    }

    impl Drop for ReentrantBytes {
        fn drop(&mut self) {
            reenter_cache(&self.state);
        }
    }

    fn reenter_cache(state: &ReentryState) {
        if let Some(cache) = state.cache.upgrade() {
            let _guard = cache
                .try_lock()
                .expect("cache entry destruction occurs after unlock");
        }
        state.drops.fetch_add(1, Ordering::SeqCst);
    }

    fn reentrant_parts(
        bytes: &[u8],
        state: &Arc<ReentryState>,
    ) -> (ReentrantBytes, ReentrantValue) {
        (
            ReentrantBytes {
                bytes: bytes.to_vec(),
                state: Arc::clone(state),
            },
            ReentrantValue(Arc::clone(state)),
        )
    }

    #[test]
    fn evicted_and_losing_values_and_bytes_drop_after_unlock() {
        let cache = Arc::new(Mutex::new(BoundedCache::new(1)));
        let state = Arc::new(ReentryState {
            cache: Arc::downgrade(&cache),
            drops: AtomicUsize::new(0),
        });
        let (first_bytes, first_value) = reentrant_parts(b"first", &state);
        let first = cache_get_or_compile(&cache, COLLISION, first_bytes, || {
            Ok::<ReentrantValue, ()>(first_value)
        })
        .expect("compile succeeds");
        drop(first);
        let (second_bytes, second_value) = reentrant_parts(b"second", &state);
        let second = cache_get_or_compile(&cache, COLLISION, second_bytes, || {
            Ok::<ReentrantValue, ()>(second_value)
        })
        .expect("compile succeeds");
        assert_eq!(state.drops.load(Ordering::SeqCst), 2);
        drop(second);

        let loser_cache = Arc::new(Mutex::new(BoundedCache::new(2)));
        let loser_state = Arc::new(ReentryState {
            cache: Arc::downgrade(&loser_cache),
            drops: AtomicUsize::new(0),
        });
        let barrier = Arc::new(Barrier::new(2));
        let threads = (0..2)
            .map(|_| {
                let cache = Arc::clone(&loser_cache);
                let state = Arc::clone(&loser_state);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let (bytes, value) = reentrant_parts(b"same", &state);
                    cache_get_or_compile(&cache, COLLISION, bytes, || {
                        barrier.wait();
                        Ok::<ReentrantValue, ()>(value)
                    })
                    .expect("compile succeeds")
                })
            })
            .collect::<Vec<_>>();
        let winners = threads
            .into_iter()
            .map(|thread| thread.join().expect("thread completes"))
            .collect::<Vec<_>>();
        assert!(Arc::ptr_eq(&winners[0], &winners[1]));
        assert_eq!(loser_state.drops.load(Ordering::SeqCst), 2);
    }
}
