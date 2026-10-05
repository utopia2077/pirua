//! Owned telemetry schema metadata and duplicate-name checks.

use crate::{AttributeValue, FiniteTelemetryNumber, SpanAttributes};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write};

/// One of the six telemetry attribute shapes supported by the upstream schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TelemetryAttributeType {
    /// A UTF-8 string.
    String,
    /// A finite number.
    Number,
    /// A boolean.
    Boolean,
    /// A homogeneous list of strings.
    StringList,
    /// A homogeneous list of finite numbers.
    NumberList,
    /// A homogeneous list of booleans.
    BooleanList,
}

impl TelemetryAttributeType {
    /// Returns the schema spelling used by the pinned upstream package.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Number => "number",
            Self::Boolean => "boolean",
            Self::StringList => "string[]",
            Self::NumberList => "number[]",
            Self::BooleanList => "boolean[]",
        }
    }
}

/// Expected cardinality metadata for an attribute.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TelemetryCardinality {
    /// A small bounded set of values.
    Low,
    /// A potentially large set of values such as ids.
    High,
}

impl TelemetryCardinality {
    /// Returns the schema spelling used by the pinned upstream package.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::High => "high",
        }
    }
}

/// Common metadata attached to one telemetry attribute definition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TelemetryAttributeMetadata {
    /// Human-readable description.
    pub description: String,
    /// Whether the attribute requires special handling by an exporter.
    pub sensitive: bool,
    /// Expected value cardinality, when declared.
    pub cardinality: Option<TelemetryCardinality>,
}

impl TelemetryAttributeMetadata {
    /// Creates metadata with no sensitivity or cardinality marker.
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            sensitive: false,
            cardinality: None,
        }
    }

    /// Marks this metadata as sensitive.
    pub fn sensitive(mut self, sensitive: bool) -> Self {
        self.sensitive = sensitive;
        self
    }

    /// Sets the expected cardinality.
    pub fn cardinality(mut self, cardinality: TelemetryCardinality) -> Self {
        self.cardinality = Some(cardinality);
        self
    }
}

/// A serializable telemetry attribute definition.
///
/// `values` is used for scalar closed sets, `element_values` for array closed
/// sets, and `examples` for documentation examples. Constructors keep these
/// fields aligned with [`TelemetryAttributeType`]; the public fields retain the
/// ordinary data shape so schema values remain detached and inspectable.
#[derive(Clone, PartialEq)]
pub struct TelemetryAttributeDefinition {
    /// Attribute value shape.
    pub attribute_type: TelemetryAttributeType,
    /// Human-readable description.
    pub description: String,
    /// Whether the attribute is sensitive.
    pub sensitive: bool,
    /// Expected value cardinality, when declared.
    pub cardinality: Option<TelemetryCardinality>,
    /// Allowed scalar values, when this is a scalar definition.
    pub values: Option<Vec<AttributeValue>>,
    /// Allowed array elements, when this is a list definition.
    pub element_values: Option<Vec<AttributeValue>>,
    /// Documentation examples.
    pub examples: Option<Vec<AttributeValue>>,
}

impl fmt::Debug for TelemetryAttributeDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TelemetryAttributeDefinition")
            .field("attribute_type", &self.attribute_type)
            .field("description", &self.description)
            .field("sensitive", &self.sensitive)
            .field("cardinality", &self.cardinality)
            .field("has_values", &self.values.as_ref().map(Vec::len))
            .field(
                "has_element_values",
                &self.element_values.as_ref().map(Vec::len),
            )
            .field("has_examples", &self.examples.as_ref().map(Vec::len))
            .finish()
    }
}

impl TelemetryAttributeDefinition {
    /// Creates a scalar or list definition with the selected type.
    pub fn new(attribute_type: TelemetryAttributeType, description: impl Into<String>) -> Self {
        Self {
            attribute_type,
            description: description.into(),
            sensitive: false,
            cardinality: None,
            values: None,
            element_values: None,
            examples: None,
        }
    }

    /// Creates a string definition.
    pub fn string(description: impl Into<String>) -> Self {
        Self::new(TelemetryAttributeType::String, description)
    }

    /// Creates a number definition.
    pub fn number(description: impl Into<String>) -> Self {
        Self::new(TelemetryAttributeType::Number, description)
    }

    /// Creates a boolean definition.
    pub fn boolean(description: impl Into<String>) -> Self {
        Self::new(TelemetryAttributeType::Boolean, description)
    }

    /// Creates a string-list definition.
    pub fn strings(description: impl Into<String>) -> Self {
        Self::new(TelemetryAttributeType::StringList, description)
    }

    /// Creates a number-list definition.
    pub fn numbers(description: impl Into<String>) -> Self {
        Self::new(TelemetryAttributeType::NumberList, description)
    }

    /// Creates a boolean-list definition.
    pub fn booleans(description: impl Into<String>) -> Self {
        Self::new(TelemetryAttributeType::BooleanList, description)
    }

    /// Sets scalar closed-set values.
    pub fn with_values(mut self, values: impl Into<Vec<AttributeValue>>) -> Self {
        self.values = Some(values.into());
        self
    }

    /// Sets closed-set values for array elements.
    pub fn with_element_values(mut self, values: impl Into<Vec<AttributeValue>>) -> Self {
        self.element_values = Some(values.into());
        self
    }

    /// Sets documentation examples.
    pub fn with_examples(mut self, examples: impl Into<Vec<AttributeValue>>) -> Self {
        self.examples = Some(examples.into());
        self
    }

    /// Marks this definition as sensitive.
    pub fn sensitive(mut self, sensitive: bool) -> Self {
        self.sensitive = sensitive;
        self
    }

    /// Sets expected value cardinality.
    pub fn cardinality(mut self, cardinality: TelemetryCardinality) -> Self {
        self.cardinality = Some(cardinality);
        self
    }

    /// Returns a detached JSON representation of this definition.
    pub fn to_json(&self) -> String {
        let mut output = String::new();
        write_attribute_definition(&mut output, self);
        output
    }
}

/// A start or event attribute definition with explicit requiredness.
#[derive(Clone, Debug, PartialEq)]
pub struct TelemetryStartAttributeDefinition {
    /// Attribute metadata and value shape.
    pub definition: TelemetryAttributeDefinition,
    /// Whether the attribute is required at span/event start.
    pub required: bool,
}

impl TelemetryStartAttributeDefinition {
    /// Creates a required attribute definition.
    pub fn required(definition: TelemetryAttributeDefinition) -> Self {
        Self {
            definition,
            required: true,
        }
    }

    /// Creates an optional attribute definition.
    pub fn optional(definition: TelemetryAttributeDefinition) -> Self {
        Self {
            definition,
            required: false,
        }
    }
}

/// Event attributes use the same shape and requiredness rule as start attributes.
pub type TelemetryEventAttributeDefinition = TelemetryStartAttributeDefinition;

/// Metadata for one named telemetry event.
#[derive(Clone, Debug, PartialEq)]
pub struct TelemetryEventDefinition {
    /// Human-readable event description.
    pub description: String,
    /// Attribute definitions keyed by event attribute name.
    pub attributes: BTreeMap<String, TelemetryEventAttributeDefinition>,
}

impl TelemetryEventDefinition {
    /// Creates an event with no declared attributes.
    pub fn new(description: impl Into<String>) -> Self {
        Self {
            description: description.into(),
            attributes: BTreeMap::new(),
        }
    }
}

/// Describes which parent spans are valid for a schema span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TelemetryParentDefinition {
    /// A root span or any caller-provided parent.
    Any,
    /// A root span or a parent outside the schema vocabulary.
    RootOrExternal,
    /// A parent whose name is one of the listed schema spans.
    Spans(Vec<String>),
}

/// Default status and failure rule for a schema span.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TelemetryStatusDefinition {
    /// The default status for a newly started span.
    pub default: TelemetryDefaultStatus,
    /// Human-readable description of when failures become error status.
    pub error_when: String,
}

impl TelemetryStatusDefinition {
    /// Creates the standard `ok` default status rule.
    pub fn new(error_when: impl Into<String>) -> Self {
        Self {
            default: TelemetryDefaultStatus::Ok,
            error_when: error_when.into(),
        }
    }
}

/// Status values permitted as a schema default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TelemetryDefaultStatus {
    /// Newly started spans are successful until failure is observed.
    Ok,
}

/// Metadata for one named telemetry span.
#[derive(Clone, Debug, PartialEq)]
pub struct TelemetrySpanDefinition {
    /// Human-readable span description.
    pub description: String,
    /// Allowed parent relationship.
    pub parents: TelemetryParentDefinition,
    /// Attributes supplied when the span starts.
    pub start_attributes: BTreeMap<String, TelemetryStartAttributeDefinition>,
    /// Attributes merged while the span is active.
    pub end_attributes: BTreeMap<String, TelemetryAttributeDefinition>,
    /// Named events emitted by the span.
    pub events: BTreeMap<String, TelemetryEventDefinition>,
    /// Status behavior.
    pub status: TelemetryStatusDefinition,
}

impl TelemetrySpanDefinition {
    /// Creates a span definition with empty attribute and event vocabularies.
    pub fn new(
        description: impl Into<String>,
        parents: TelemetryParentDefinition,
        status: TelemetryStatusDefinition,
    ) -> Self {
        Self {
            description: description.into(),
            parents,
            start_attributes: BTreeMap::new(),
            end_attributes: BTreeMap::new(),
            events: BTreeMap::new(),
            status,
        }
    }
}

/// Top-level serializable telemetry schema metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct TelemetrySchemaDefinition {
    /// Schema version.
    pub version: u32,
    /// Span definitions keyed by span name.
    pub spans: BTreeMap<String, TelemetrySpanDefinition>,
}

impl TelemetrySchemaDefinition {
    /// Creates an empty schema at the supplied version.
    pub fn new(version: u32) -> Self {
        Self {
            version,
            spans: BTreeMap::new(),
        }
    }

    /// Returns a detached JSON representation suitable for conformance files.
    pub fn to_json(&self) -> String {
        let mut output = String::new();
        write_schema(&mut output, self);
        output
    }
}

/// Typed identity helper corresponding to upstream `defineTelemetrySchema`.
pub fn define_telemetry_schema<T>(schema: T) -> T {
    schema
}

/// Duplicate span names found while combining schema vocabularies.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DuplicateTelemetrySpanNames {
    /// Names that occur in more than one supplied schema.
    pub names: Vec<String>,
}

impl fmt::Display for DuplicateTelemetrySpanNames {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "duplicate telemetry span names: {}",
            self.names.join(", ")
        )
    }
}

impl std::error::Error for DuplicateTelemetrySpanNames {}

/// Rejects duplicate span names across independently versioned schemas.
pub fn validate_unique_telemetry_span_names(
    schemas: &[&TelemetrySchemaDefinition],
) -> Result<(), DuplicateTelemetrySpanNames> {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();
    for schema in schemas {
        for name in schema.spans.keys() {
            if !seen.insert(name.clone()) {
                duplicates.insert(name.clone());
            }
        }
    }
    if duplicates.is_empty() {
        Ok(())
    } else {
        Err(DuplicateTelemetrySpanNames {
            names: duplicates.into_iter().collect(),
        })
    }
}

impl AttributeValue {
    /// Returns a detached JSON representation of this attribute value.
    pub fn to_json(&self) -> String {
        let mut output = String::new();
        write_attribute_value(&mut output, self);
        output
    }
}

/// Returns a detached JSON object representation of these attributes.
pub fn span_attributes_to_json(attributes: &SpanAttributes) -> String {
    let mut output = String::new();
    write_attributes(&mut output, attributes);
    output
}

fn write_schema(output: &mut String, schema: &TelemetrySchemaDefinition) {
    output.push_str("{\"version\":");
    output.push_str(&schema.version.to_string());
    output.push_str(",\"spans\":{");
    for (index, (name, span)) in schema.spans.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_json_string(output, name);
        output.push(':');
        write_span_definition(output, span);
    }
    output.push_str("}}");
}

fn write_span_definition(output: &mut String, span: &TelemetrySpanDefinition) {
    output.push_str("{\"description\":");
    write_json_string(output, &span.description);
    output.push_str(",\"parents\":");
    write_parent_definition(output, &span.parents);
    output.push_str(",\"startAttributes\":");
    write_start_attributes(output, &span.start_attributes);
    output.push_str(",\"endAttributes\":");
    write_attribute_definitions(output, &span.end_attributes);
    output.push_str(",\"events\":");
    write_events(output, &span.events);
    output.push_str(",\"status\":{\"default\":\"ok\",\"errorWhen\":");
    write_json_string(output, &span.status.error_when);
    output.push_str("}}");
}

fn write_parent_definition(output: &mut String, parent: &TelemetryParentDefinition) {
    match parent {
        TelemetryParentDefinition::Any => output.push_str("{\"kind\":\"any\"}"),
        TelemetryParentDefinition::RootOrExternal => {
            output.push_str("{\"kind\":\"root_or_external\"}")
        }
        TelemetryParentDefinition::Spans(spans) => {
            output.push_str("{\"kind\":\"spans\",\"spans\":[");
            for (index, span) in spans.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_json_string(output, span);
            }
            output.push_str("]}");
        }
    }
}

fn write_start_attributes(
    output: &mut String,
    definitions: &BTreeMap<String, TelemetryStartAttributeDefinition>,
) {
    output.push('{');
    for (index, (name, definition)) in definitions.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_json_string(output, name);
        output.push(':');
        write_attribute_definition_with_required(
            output,
            &definition.definition,
            definition.required,
        );
    }
    output.push('}');
}

fn write_attribute_definitions(
    output: &mut String,
    definitions: &BTreeMap<String, TelemetryAttributeDefinition>,
) {
    output.push('{');
    for (index, (name, definition)) in definitions.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_json_string(output, name);
        output.push(':');
        write_attribute_definition(output, definition);
    }
    output.push('}');
}

fn write_events(output: &mut String, events: &BTreeMap<String, TelemetryEventDefinition>) {
    output.push('{');
    for (index, (name, event)) in events.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_json_string(output, name);
        output.push(':');
        output.push_str("{\"description\":");
        write_json_string(output, &event.description);
        output.push_str(",\"attributes\":");
        write_start_attributes(output, &event.attributes);
        output.push('}');
    }
    output.push('}');
}

fn write_attribute_definition_with_required(
    output: &mut String,
    definition: &TelemetryAttributeDefinition,
    required: bool,
) {
    output.push('{');
    write_definition_fields(output, definition);
    output.push_str(",\"required\":");
    output.push_str(if required { "true" } else { "false" });
    output.push('}');
}

fn write_attribute_definition(output: &mut String, definition: &TelemetryAttributeDefinition) {
    output.push('{');
    write_definition_fields(output, definition);
    output.push('}');
}

fn write_definition_fields(output: &mut String, definition: &TelemetryAttributeDefinition) {
    output.push_str("\"type\":");
    write_json_string(output, definition.attribute_type.as_str());
    output.push_str(",\"description\":");
    write_json_string(output, &definition.description);
    if definition.sensitive {
        output.push_str(",\"sensitive\":true");
    }
    if let Some(cardinality) = definition.cardinality {
        output.push_str(",\"cardinality\":");
        write_json_string(output, cardinality.as_str());
    }
    if let Some(values) = &definition.values {
        output.push_str(",\"values\":[");
        write_attribute_values(output, values);
        output.push(']');
    }
    if let Some(values) = &definition.element_values {
        output.push_str(",\"elementValues\":[");
        write_attribute_values(output, values);
        output.push(']');
    }
    if let Some(examples) = &definition.examples {
        output.push_str(",\"examples\":[");
        write_attribute_values(output, examples);
        output.push(']');
    }
}

fn write_attributes(output: &mut String, attributes: &SpanAttributes) {
    output.push('{');
    for (index, (name, value)) in attributes.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_json_string(output, name);
        output.push(':');
        write_attribute_value(output, value);
    }
    output.push('}');
}

fn write_attribute_values(output: &mut String, values: &[AttributeValue]) {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_attribute_value(output, value);
    }
}

fn write_attribute_value(output: &mut String, value: &AttributeValue) {
    match value {
        AttributeValue::String(value) => write_json_string(output, value),
        AttributeValue::Number(value) => write_finite_number(output, *value),
        AttributeValue::Boolean(value) => output.push_str(if *value { "true" } else { "false" }),
        AttributeValue::Strings(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_json_string(output, value);
            }
            output.push(']');
        }
        AttributeValue::Numbers(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_finite_number(output, *value);
            }
            output.push(']');
        }
        AttributeValue::Booleans(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(if *value { "true" } else { "false" });
            }
            output.push(']');
        }
    }
}

fn write_finite_number(output: &mut String, number: FiniteTelemetryNumber) {
    let value = number.get();
    if value == 0.0 && value.is_sign_negative() {
        output.push_str("-0");
    } else {
        let _ = write!(output, "{value}");
    }
}

fn write_json_string(output: &mut String, value: &str) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character.is_control() => {
                let _ = write!(output, "\\u{:04x}", character as u32);
            }
            character => output.push(character),
        }
    }
    output.push('"');
}
