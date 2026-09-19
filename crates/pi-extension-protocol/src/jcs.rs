//! Bounded RFC 8785 JSON and the protocol's deliberately closed portable schema subset.
//!
//! Dynamic schema digests hash these canonical JSON bytes. Profile digests instead
//! hash the selected descriptor with its profile name and version.
//! Schema nodes require one supported `type`, except for an exact `{"const": ...}`
//! node used by the checked-in tool echo schemas. That is the sole type-free
//! schema form; extra keywords, composition, remote refs, and unknown keywords
//! reject before hashing.

use ring::digest::{SHA256, digest};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::cell::Cell;
use std::collections::HashSet;
use std::fmt::{self, Write};
use std::rc::Rc;

const MAX_SAFE_INTEGER: f64 = 9_007_199_254_740_991.0;

/// Finite caller ceilings, applied to both schema documents and instances.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    pub input_bytes: usize,
    pub depth: usize,
    pub nodes: usize,
    pub elements: usize,
    pub decoded_bytes: usize,
    pub output_bytes: usize,
    /// Maximum schema-admission work units, independent of parsed values.
    ///
    /// Admission charges schema nodes and keywords, definition/property lookup
    /// key bytes, and every enum comparison step and compared string byte.
    /// A valid schema may reject with `Error::Limit` under a small budget.
    pub schema_work: usize,
    /// Maximum instance-validation work units, separate from schema admission.
    ///
    /// Validation charges recursive nodes, object/required entries, lookup key
    /// bytes, and const/enum comparison nodes and string bytes.
    pub instance_work: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            input_bytes: 1_048_576,
            depth: 64,
            nodes: 16_384,
            elements: 16_384,
            decoded_bytes: 1_048_576,
            output_bytes: 1_048_576,
            schema_work: 262_144,
            instance_work: 262_144,
        }
    }
}

/// Errors intentionally carry no rejected JSON or property names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidJson,
    DuplicateKey,
    InvalidNumber,
    Limit,
    UnsupportedSchema,
    InvalidSchema,
    InstanceMismatch,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidJson => "invalid JSON",
            Self::DuplicateKey => "duplicate JSON key",
            Self::InvalidNumber => "invalid JSON number",
            Self::Limit => "JSON limit exceeded",
            Self::UnsupportedSchema => "unsupported portable schema",
            Self::InvalidSchema => "invalid portable schema",
            Self::InstanceMismatch => "instance does not match portable schema",
        })
    }
}

impl std::error::Error for Error {}

#[derive(Clone, Debug)]
enum Value {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Value>),
    Object(Vec<(String, Value)>),
}

impl Value {
    fn object(&self) -> Result<&[(String, Value)], Error> {
        match self {
            Self::Object(members) => Ok(members),
            _ => Err(Error::InvalidSchema),
        }
    }

    fn string(&self) -> Result<&str, Error> {
        match self {
            Self::String(value) => Ok(value),
            _ => Err(Error::InvalidSchema),
        }
    }
}

fn member<'a>(members: &'a [(String, Value)], name: &str) -> Option<&'a Value> {
    members
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value)
}

struct Budget {
    limits: Limits,
    nodes: Cell<usize>,
    elements: Cell<usize>,
    decoded: Cell<usize>,
    error: Cell<Option<Error>>,
    safe_numbers: bool,
}

impl Budget {
    fn charge<E: de::Error>(&self, used: &Cell<usize>, amount: usize, max: usize) -> Result<(), E> {
        let Some(next) = used.get().checked_add(amount) else {
            self.error.set(Some(Error::Limit));
            return Err(E::custom("limit"));
        };
        if next > max {
            self.error.set(Some(Error::Limit));
            return Err(E::custom("limit"));
        }
        used.set(next);
        Ok(())
    }

    fn fail<E: de::Error>(&self, error: Error) -> E {
        self.error.set(Some(error));
        E::custom("rejected JSON")
    }
}

struct Seed {
    budget: Rc<Budget>,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for Seed {
    type Value = Value;

    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<Value, D::Error> {
        if self.depth > self.budget.limits.depth {
            return Err(self.budget.fail(Error::Limit));
        }
        self.budget
            .charge(&self.budget.nodes, 1, self.budget.limits.nodes)?;
        decoder.deserialize_any(ValueVisitor {
            budget: self.budget,
            depth: self.depth,
        })
    }
}

struct ValueVisitor {
    budget: Rc<Budget>,
    depth: usize,
}

impl ValueVisitor {
    fn number<E: de::Error>(&self, value: f64) -> Result<Value, E> {
        if !value.is_finite()
            || (self.budget.safe_numbers && value.fract() == 0.0 && value.abs() > MAX_SAFE_INTEGER)
        {
            return Err(self.budget.fail(Error::InvalidNumber));
        }
        Ok(Value::Number(value))
    }

    fn string<E: de::Error>(&self, value: &str) -> Result<Value, E> {
        self.budget.charge(
            &self.budget.decoded,
            value.len(),
            self.budget.limits.decoded_bytes,
        )?;
        Ok(Value::String(value.to_owned()))
    }
}

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("bounded JSON value")
    }

    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        self.number(value as f64)
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        self.number(value as f64)
    }

    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        self.number(value)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        self.string(value)
    }

    fn visit_string<E: de::Error>(self, value: String) -> Result<Value, E> {
        self.string(&value)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element_seed(Seed {
            budget: Rc::clone(&self.budget),
            depth: self.depth + 1,
        })? {
            self.budget
                .charge(&self.budget.elements, 1, self.budget.limits.elements)?;
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
        let mut values = Vec::new();
        let mut seen = HashSet::new();
        while let Some(key) = map.next_key::<String>()? {
            self.budget.charge(
                &self.budget.decoded,
                key.len(),
                self.budget.limits.decoded_bytes,
            )?;
            self.budget
                .charge(&self.budget.elements, 1, self.budget.limits.elements)?;
            if !seen.insert(key.clone()) {
                return Err(self.budget.fail(Error::DuplicateKey));
            }
            let value = map.next_value_seed(Seed {
                budget: Rc::clone(&self.budget),
                depth: self.depth + 1,
            })?;
            values.push((key, value));
        }
        Ok(Value::Object(values))
    }
}

fn parse(input: &[u8], limits: Limits, safe_numbers: bool) -> Result<Value, Error> {
    if input.len() > limits.input_bytes {
        return Err(Error::Limit);
    }
    let budget = Rc::new(Budget {
        limits,
        nodes: Cell::new(0),
        elements: Cell::new(0),
        decoded: Cell::new(0),
        error: Cell::new(None),
        safe_numbers,
    });
    let mut decoder = serde_json::Deserializer::from_slice(input);
    let result = Seed {
        budget: Rc::clone(&budget),
        depth: 0,
    }
    .deserialize(&mut decoder);
    let value = result.map_err(|_| budget.error.get().unwrap_or(Error::InvalidJson))?;
    decoder.end().map_err(|_| Error::InvalidJson)?;
    Ok(value)
}

struct Writer {
    bytes: Vec<u8>,
    max: usize,
}

impl Writer {
    fn append(&mut self, bytes: &[u8]) -> Result<(), Error> {
        if bytes.len() > self.max.saturating_sub(self.bytes.len()) {
            return Err(Error::Limit);
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn string(&mut self, input: &str) -> Result<(), Error> {
        self.append(b"\"")?;
        for ch in input.chars() {
            match ch {
                '"' => self.append(br#"\""#)?,
                '\\' => self.append(br"\\")?,
                '\u{0008}' => self.append(br"\b")?,
                '\t' => self.append(br"\t")?,
                '\n' => self.append(br"\n")?,
                '\u{000c}' => self.append(br"\f")?,
                '\r' => self.append(br"\r")?,
                '\u{0000}'..='\u{001f}' => {
                    let mut escaped = String::new();
                    write!(&mut escaped, "\\u{:04x}", ch as u32).expect("string formatting");
                    self.append(escaped.as_bytes())?;
                }
                _ => {
                    let mut encoded = [0; 4];
                    self.append(ch.encode_utf8(&mut encoded).as_bytes())?;
                }
            }
        }
        self.append(b"\"")
    }

    fn value(&mut self, value: &Value) -> Result<(), Error> {
        match value {
            Value::Null => self.append(b"null"),
            Value::Bool(value) => self.append(if *value { b"true" } else { b"false" }),
            Value::Number(value) => {
                self.append(ryu_js::Buffer::new().format_finite(*value).as_bytes())
            }
            Value::String(value) => self.string(value),
            Value::Array(values) => {
                self.append(b"[")?;
                for (index, value) in values.iter().enumerate() {
                    if index != 0 {
                        self.append(b",")?;
                    }
                    self.value(value)?;
                }
                self.append(b"]")
            }
            Value::Object(values) => {
                let mut sorted: Vec<_> = values.iter().collect();
                sorted.sort_by(|left, right| left.0.encode_utf16().cmp(right.0.encode_utf16()));
                self.append(b"{")?;
                for (index, (key, value)) in sorted.into_iter().enumerate() {
                    if index != 0 {
                        self.append(b",")?;
                    }
                    self.string(key)?;
                    self.append(b":")?;
                    self.value(value)?;
                }
                self.append(b"}")
            }
        }
    }
}

/// Canonical bytes and raw SHA-256 digest for a JSON document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalJson {
    bytes: Vec<u8>,
    digest: [u8; 32],
}

impl CanonicalJson {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn sha256(&self) -> [u8; 32] {
        self.digest
    }

    pub fn sha256_hex(&self) -> String {
        let mut hex = String::with_capacity(64);
        for byte in self.digest {
            write!(&mut hex, "{byte:02x}").expect("string formatting");
        }
        hex
    }
}

fn canonical(value: &Value, limits: Limits) -> Result<CanonicalJson, Error> {
    let mut writer = Writer {
        bytes: Vec::new(),
        max: limits.output_bytes,
    };
    writer.value(value)?;
    let mut hash = [0; 32];
    hash.copy_from_slice(digest(&SHA256, &writer.bytes).as_ref());
    Ok(CanonicalJson {
        bytes: writer.bytes,
        digest: hash,
    })
}

/// Canonicalize valid RFC 8785 JSON, including finite IEEE-754 values.
///
/// Portable schemas and instances additionally reject unsafe integral values.
pub fn canonical_json(input: &[u8], limits: Limits) -> Result<CanonicalJson, Error> {
    canonical(&parse(input, limits, false)?, limits)
}

/// Canonicalize an already decoded finite IEEE-754 number.
pub fn canonical_number(value: f64, limits: Limits) -> Result<CanonicalJson, Error> {
    if !value.is_finite() {
        return Err(Error::InvalidNumber);
    }
    canonical(&Value::Number(value), limits)
}

fn valid_def_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}

fn count(value: &Value) -> Result<usize, Error> {
    match value {
        Value::Number(number)
            if *number >= 0.0 && *number <= MAX_SAFE_INTEGER && number.fract() == 0.0 =>
        {
            Ok(*number as usize)
        }
        _ => Err(Error::InvalidSchema),
    }
}

fn optional_count(members: &[(String, Value)], name: &str) -> Result<Option<usize>, Error> {
    member(members, name).map(count).transpose()
}

fn bounds(members: &[(String, Value)], min: &str, max: &str) -> Result<(), Error> {
    if let (Some(minimum), Some(maximum)) =
        (optional_count(members, min)?, optional_count(members, max)?)
        && minimum > maximum
    {
        return Err(Error::InvalidSchema);
    }
    Ok(())
}

struct Meter {
    remaining: usize,
}

impl Meter {
    fn charge(&mut self, amount: usize) -> Result<(), Error> {
        self.remaining = self.remaining.checked_sub(amount).ok_or(Error::Limit)?;
        Ok(())
    }

    fn lookup<'a>(
        &mut self,
        members: &'a [(String, Value)],
        name: &str,
    ) -> Result<Option<&'a Value>, Error> {
        for (key, value) in members {
            self.charge(key.len().max(name.len()))?;
            if key == name {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

struct Admission<'a> {
    defs: &'a [(String, Value)],
    meter: Meter,
    active: HashSet<String>,
    completed: HashSet<String>,
}

impl Admission<'_> {
    fn charge(&mut self, amount: usize) -> Result<(), Error> {
        self.meter.charge(amount)
    }

    fn lookup<'a>(
        &mut self,
        members: &'a [(String, Value)],
        name: &str,
    ) -> Result<Option<&'a Value>, Error> {
        self.meter.lookup(members, name)
    }

    fn definition(&mut self, name: &str, value: &Value) -> Result<(), Error> {
        self.charge(name.len())?;
        if self.completed.contains(name) {
            return Ok(());
        }
        if member(value.object()?, "$ref").is_some() {
            return Err(Error::UnsupportedSchema);
        }
        if !self.active.insert(name.to_owned()) {
            return Err(Error::InvalidSchema);
        }
        let result = validate_schema_node(value, self, false);
        self.active.remove(name);
        if result.is_ok() {
            self.completed.insert(name.to_owned());
        }
        result
    }
}

fn equal_metered(left: &Value, right: &Value, meter: &mut Meter) -> Result<bool, Error> {
    meter.charge(1)?;
    match (left, right) {
        (Value::Null, Value::Null) => Ok(true),
        (Value::Bool(a), Value::Bool(b)) => Ok(a == b),
        (Value::Number(a), Value::Number(b)) => Ok(a == b),
        (Value::String(a), Value::String(b)) => {
            meter.charge(a.len().max(b.len()))?;
            Ok(a == b)
        }
        (Value::Array(a), Value::Array(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (a, b) in a.iter().zip(b) {
                if !equal_metered(a, b, meter)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (Value::Object(a), Value::Object(b)) => {
            if a.len() != b.len() {
                return Ok(false);
            }
            for (key, value) in a {
                let Some(other) = meter.lookup(b, key)? else {
                    return Ok(false);
                };
                if !equal_metered(value, other, meter)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn kind_matches(kind: &str, value: &Value) -> bool {
    match (kind, value) {
        ("null", Value::Null)
        | ("boolean", Value::Bool(_))
        | ("string", Value::String(_))
        | ("array", Value::Array(_))
        | ("object", Value::Object(_))
        | ("number", Value::Number(_)) => true,
        ("integer", Value::Number(number)) => number.fract() == 0.0,
        _ => false,
    }
}

fn validate_schema_node(
    value: &Value,
    admission: &mut Admission<'_>,
    root: bool,
) -> Result<(), Error> {
    admission.charge(1)?;
    let members = value.object()?;
    for (key, _) in members {
        admission.charge(1)?;
        if !matches!(
            key.as_str(),
            "type"
                | "properties"
                | "required"
                | "additionalProperties"
                | "minProperties"
                | "maxProperties"
                | "items"
                | "minItems"
                | "maxItems"
                | "minLength"
                | "maxLength"
                | "minimum"
                | "maximum"
                | "const"
                | "enum"
                | "$ref"
                | "$defs"
        ) {
            return Err(Error::UnsupportedSchema);
        }
        if key == "$defs" && !root {
            return Err(Error::UnsupportedSchema);
        }
    }
    if let Some(reference) = member(members, "$ref") {
        if members.len()
            != if root && member(members, "$defs").is_some() {
                2
            } else {
                1
            }
        {
            return Err(Error::UnsupportedSchema);
        }
        let name = reference
            .string()?
            .strip_prefix("#/$defs/")
            .ok_or(Error::UnsupportedSchema)?;
        if !valid_def_name(name) {
            return Err(Error::UnsupportedSchema);
        }
        let target = admission
            .lookup(admission.defs, name)?
            .ok_or(Error::InvalidSchema)?;
        return admission.definition(name, target);
    }
    if members.len() == 1 && member(members, "const").is_some() {
        return Ok(());
    }
    let kind = member(members, "type")
        .ok_or(Error::InvalidSchema)?
        .string()?;
    let allowed: &[&str] = match kind {
        "object" => &[
            "type",
            "properties",
            "required",
            "additionalProperties",
            "minProperties",
            "maxProperties",
            "const",
            "enum",
        ],
        "array" => &["type", "items", "minItems", "maxItems", "const", "enum"],
        "string" => &["type", "minLength", "maxLength", "const", "enum"],
        "number" | "integer" => &["type", "minimum", "maximum", "const", "enum"],
        "boolean" | "null" => &["type", "const", "enum"],
        _ => return Err(Error::UnsupportedSchema),
    };
    for (key, _) in members {
        if key != "$defs" && !allowed.contains(&key.as_str()) {
            return Err(Error::UnsupportedSchema);
        }
    }
    if let Some(constant) = member(members, "const")
        && !kind_matches(kind, constant)
    {
        return Err(Error::InvalidSchema);
    }
    if let Some(enumeration) = member(members, "enum") {
        let Value::Array(values) = enumeration else {
            return Err(Error::InvalidSchema);
        };
        if values.is_empty() {
            return Err(Error::InvalidSchema);
        }
        for (index, value) in values.iter().enumerate() {
            admission.charge(1)?;
            if !kind_matches(kind, value) {
                return Err(Error::InvalidSchema);
            }
            for prior in &values[..index] {
                if equal_metered(prior, value, &mut admission.meter)? {
                    return Err(Error::InvalidSchema);
                }
            }
        }
    }
    match kind {
        "object" => {
            bounds(members, "minProperties", "maxProperties")?;
            let properties = member(members, "properties")
                .map(Value::object)
                .transpose()?
                .unwrap_or(&[]);
            for (_, schema) in properties {
                validate_schema_node(schema, admission, false)?;
            }
            if let Some(value) = member(members, "additionalProperties")
                && !matches!(value, Value::Bool(_))
            {
                return Err(Error::InvalidSchema);
            }
            if let Some(value) = member(members, "required") {
                let Value::Array(names) = value else {
                    return Err(Error::InvalidSchema);
                };
                let mut seen = HashSet::new();
                for name in names {
                    let name = name.string()?;
                    if !seen.insert(name) || admission.lookup(properties, name)?.is_none() {
                        return Err(Error::InvalidSchema);
                    }
                }
            }
        }
        "array" => {
            bounds(members, "minItems", "maxItems")?;
            validate_schema_node(
                member(members, "items").ok_or(Error::InvalidSchema)?,
                admission,
                false,
            )?;
        }
        "string" => bounds(members, "minLength", "maxLength")?,
        "number" | "integer" => {
            let number = |name| -> Result<Option<f64>, Error> {
                match member(members, name) {
                    None => Ok(None),
                    Some(Value::Number(value)) if kind != "integer" || value.fract() == 0.0 => {
                        Ok(Some(*value))
                    }
                    _ => Err(Error::InvalidSchema),
                }
            };
            if let (Some(minimum), Some(maximum)) = (number("minimum")?, number("maximum")?)
                && minimum > maximum
            {
                return Err(Error::InvalidSchema);
            }
        }
        _ => {}
    }
    Ok(())
}

fn defs_from_root(root: &Value) -> Result<&[(String, Value)], Error> {
    member(root.object()?, "$defs")
        .map(Value::object)
        .transpose()
        .map(|defs| defs.unwrap_or(&[]))
}

/// A fully admitted portable JSON Schema document and its dynamic digest.
#[derive(Clone, Debug)]
pub struct PortableSchema {
    root: Value,
    canonical: CanonicalJson,
}

impl PortableSchema {
    pub fn canonical(&self) -> &CanonicalJson {
        &self.canonical
    }

    /// Parse and validate an instance under independent finite ceilings.
    /// Recursive validation consumes `Limits::instance_work`.
    pub fn validate_instance(&self, input: &[u8], limits: Limits) -> Result<(), Error> {
        let value = parse(input, limits, true)?;
        let defs = defs_from_root(&self.root)?;
        let mut meter = Meter {
            remaining: limits.instance_work,
        };
        if instance_matches(&self.root, &value, defs, &mut meter)? {
            Ok(())
        } else {
            Err(Error::InstanceMismatch)
        }
    }
}

/// Admit a closed portable schema before computing its canonical dynamic digest.
///
/// Each schema node has one supported `type`, except a sole `const` keyword.
/// Object, array, string, and numeric bounds apply only to their matching type.
/// Definitions may occur only at the root; refs are local and cannot chain.
/// Schema validation consumes `Limits::schema_work` before hashing.
pub fn admit_schema(input: &[u8], limits: Limits) -> Result<PortableSchema, Error> {
    let root = parse(input, limits, true)?;
    let defs = defs_from_root(&root)?;
    let mut admission = Admission {
        defs,
        meter: Meter {
            remaining: limits.schema_work,
        },
        active: HashSet::new(),
        completed: HashSet::new(),
    };
    for (name, value) in defs {
        if !valid_def_name(name) {
            return Err(Error::UnsupportedSchema);
        }
        admission.definition(name, value)?;
    }
    validate_schema_node(&root, &mut admission, true)?;
    let canonical = canonical(&root, limits)?;
    Ok(PortableSchema { root, canonical })
}

fn instance_matches(
    schema: &Value,
    value: &Value,
    defs: &[(String, Value)],
    meter: &mut Meter,
) -> Result<bool, Error> {
    meter.charge(1)?;
    let Ok(members) = schema.object() else {
        return Ok(false);
    };
    if let Some(reference) = member(members, "$ref") {
        let Ok(reference) = reference.string() else {
            return Ok(false);
        };
        let Some(name) = reference.strip_prefix("#/$defs/") else {
            return Ok(false);
        };
        let Some(target) = meter.lookup(defs, name)? else {
            return Ok(false);
        };
        return instance_matches(target, value, defs, meter);
    }
    if members.len() == 1
        && let Some(constant) = member(members, "const")
    {
        return equal_metered(constant, value, meter);
    }
    let Some(kind) = member(members, "type").and_then(|value| value.string().ok()) else {
        return Ok(false);
    };
    if !kind_matches(kind, value) {
        return Ok(false);
    }
    if let Some(constant) = member(members, "const")
        && !equal_metered(constant, value, meter)?
    {
        return Ok(false);
    }
    if let Some(Value::Array(values)) = member(members, "enum") {
        let mut found = false;
        for candidate in values {
            if equal_metered(candidate, value, meter)? {
                found = true;
                break;
            }
        }
        if !found {
            return Ok(false);
        }
    }
    match value {
        Value::Object(properties) => {
            let declared = member(members, "properties")
                .and_then(|value| value.object().ok())
                .unwrap_or(&[]);
            if !within_count(members, properties.len(), "minProperties", "maxProperties") {
                return Ok(false);
            }
            if let Some(Value::Array(required)) = member(members, "required") {
                for name in required {
                    meter.charge(1)?;
                    let Ok(name) = name.string() else {
                        return Ok(false);
                    };
                    if meter.lookup(properties, name)?.is_none() {
                        return Ok(false);
                    }
                }
            }
            for (name, value) in properties {
                meter.charge(1)?;
                match meter.lookup(declared, name)? {
                    Some(schema) => {
                        if !instance_matches(schema, value, defs, meter)? {
                            return Ok(false);
                        }
                    }
                    None if matches!(
                        member(members, "additionalProperties"),
                        Some(Value::Bool(false))
                    ) =>
                    {
                        return Ok(false);
                    }
                    None => {}
                }
            }
            Ok(true)
        }
        Value::Array(values) => {
            if !within_count(members, values.len(), "minItems", "maxItems") {
                return Ok(false);
            }
            let Some(items) = member(members, "items") else {
                return Ok(false);
            };
            for value in values {
                if !instance_matches(items, value, defs, meter)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Value::String(string) => {
            meter.charge(string.len())?;
            Ok(within_count(
                members,
                string.chars().count(),
                "minLength",
                "maxLength",
            ))
        }
        Value::Number(number) => Ok(member(members, "minimum")
            .is_none_or(|minimum| matches!(minimum, Value::Number(bound) if number >= bound))
            && member(members, "maximum")
                .is_none_or(|maximum| matches!(maximum, Value::Number(bound) if number <= bound))),
        Value::Bool(_) | Value::Null => Ok(true),
    }
}

fn within_count(members: &[(String, Value)], count: usize, min: &str, max: &str) -> bool {
    optional_count(members, min)
        .ok()
        .flatten()
        .is_none_or(|limit| count >= limit)
        && optional_count(members, max)
            .ok()
            .flatten()
            .is_none_or(|limit| count <= limit)
}
