//! Frozen six-profile shapes and host-selected exposure contracts.
//!
//! The static descriptor interpreter is deliberately separate from PortableSchema.
//! It validates wire arms before JSON conversion; native codecs and domain validation
//! remain the runtime's responsibility, exercised through `NativeValidator`.

use self::{wire_v1_0 as v1_0, wire_v1_1 as v1_1};
use crate::descriptor::canonical_profile_digest;
use crate::jcs::{self, PortableSchema};
use crate::wire::value;
use prost::Message;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub const STATIC_REGISTRY: &[u8] = include_bytes!("../schema/projections/static-v1.json");
const SAFE: i64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    HostContract,
    Projection,
    Shape,
    Limit,
    Exposure,
    NativeRejected,
    HostLocalFault,
    Method,
    Direction,
    Variant,
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "profile validation: {self:?}")
    }
}
impl std::error::Error for Error {}
type Result<T> = std::result::Result<T, Error>;

/// A local fault is never classified as malformed peer input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeError {
    Rejected,
    LocalFault,
}
/// The callback receives the actual admitted typed payload, not a schema-name assertion.
/// Runtime implementations must construct native DTOs, apply captured codecs and
/// public exposure policy, and validate context-sensitive domain invariants.
pub trait NativeValidator {
    fn validate(&mut self, candidate: &ValidatedPayload) -> std::result::Result<(), NativeError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    CoreToExtension,
    ExtensionToCore,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    Unary,
    Stream,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Request,
    Progress,
    Terminal,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ValidatedPayload {
    pub method: String,
    pub stage: Stage,
    pub projections: BTreeMap<String, Value>,
    pub fields: Value,
}

#[derive(Clone, Debug)]
pub struct Registry {
    document: Arc<Value>,
    digests: BTreeMap<String, [u8; 32]>,
}
impl Registry {
    /// Reconstruct every complete closure and hash it with the existing JCS authority.
    pub fn bundled() -> Result<Self> {
        let document: Value =
            serde_json::from_slice(STATIC_REGISTRY).map_err(|_| Error::HostContract)?;
        let types = document["types"].as_object().ok_or(Error::HostContract)?;
        let roots = document["projections"]
            .as_object()
            .ok_or(Error::HostContract)?;
        let mut digests = BTreeMap::new();
        for (root, entry) in roots {
            let mut closure = BTreeSet::new();
            Self::closure(root, types, &mut closure)?;
            let declared: BTreeSet<_> = entry["type_universe"]
                .as_array()
                .ok_or(Error::HostContract)?
                .iter()
                .map(|v| v.as_str().ok_or(Error::HostContract))
                .collect::<Result<_>>()?;
            if closure.iter().map(String::as_str).collect::<BTreeSet<_>>() != declared {
                return Err(Error::HostContract);
            }
            let selected = closure
                .iter()
                .map(|name| (name.clone(), types[name].clone()))
                .collect::<serde_json::Map<_, _>>();
            let profile = entry["profile"].as_str().ok_or(Error::HostContract)?;
            // Policy selection is frozen by the accepted generator, including null when none.
            let policy = selected_policy(&document, profile);
            let preimage =
                json!({"descriptor_format":1,"root":root,"types":selected,"policy":policy});
            let bytes = serde_json::to_vec(&preimage).map_err(|_| Error::HostContract)?;
            let digest = jcs::canonical_json(
                &bytes,
                jcs::Limits {
                    input_bytes: 2_000_000,
                    output_bytes: 2_000_000,
                    decoded_bytes: 2_000_000,
                    nodes: 100_000,
                    elements: 100_000,
                    schema_work: 16_000_000,
                    instance_work: 16_000_000,
                    depth: 64,
                },
            )
            .map_err(|_| Error::HostContract)?
            .sha256();
            if hex(&digest)
                != entry["proposed_digest_sha256"]
                    .as_str()
                    .ok_or(Error::HostContract)?
            {
                return Err(Error::HostContract);
            }
            digests.insert(root.clone(), digest);
        }
        Ok(Self {
            document: Arc::new(document),
            digests,
        })
    }
    fn closure(
        name: &str,
        types: &serde_json::Map<String, Value>,
        seen: &mut BTreeSet<String>,
    ) -> Result<()> {
        if !seen.insert(name.to_owned()) {
            return Ok(());
        }
        let node = types.get(name).ok_or(Error::HostContract)?;
        fn refs(v: &Value, out: &mut Vec<String>) {
            match v {
                Value::Object(m) => {
                    if let Some(s) = m.get("ref").and_then(Value::as_str) {
                        out.push(s.to_owned());
                    }
                    for v in m.values() {
                        refs(v, out);
                    }
                }
                Value::Array(a) => {
                    for v in a {
                        refs(v, out);
                    }
                }
                _ => {}
            }
        }
        let mut references = Vec::new();
        refs(node, &mut references);
        for reference in references {
            Self::closure(&reference, types, seen)?;
        }
        Ok(())
    }
    pub fn digest(&self, root: &str) -> Option<[u8; 32]> {
        self.digests.get(root).copied()
    }
    pub fn roots(&self) -> impl Iterator<Item = &str> {
        self.digests.keys().map(String::as_str)
    }
    pub fn method(&self, profile: &str, method: &str) -> Option<(Direction, Mode)> {
        self.document["canonical_wire_inventory"][profile]["methods"]
            .as_array()?
            .iter()
            .find_map(|m| {
                if m["method"] != method {
                    return None;
                }
                Some((
                    if m["direction"] == "CORE_TO_EXTENSION" {
                        Direction::CoreToExtension
                    } else {
                        Direction::ExtensionToCore
                    },
                    if m["mode"] == "METHOD_STREAM" {
                        Mode::Stream
                    } else {
                        Mode::Unary
                    },
                ))
            })
    }
    pub fn validate_v1_0(
        &self,
        root: &str,
        bound: &v1_0::SchemaBoundValue,
        limits: value::Limits,
        work: usize,
    ) -> Result<Value> {
        value::admit_v1_0(
            bound.value.as_ref().ok_or(Error::Shape)?,
            limits,
            value::BytePolicy::JsonOnly,
        )
        .map_err(|_| Error::Shape)?;
        let b = v1_1::SchemaBoundValue::decode(bound.encode_to_vec().as_slice())
            .map_err(|_| Error::Shape)?;
        self.validate_v1_1(root, &b, limits, work)
    }
    pub fn validate_v1_1(
        &self,
        root: &str,
        bound: &v1_1::SchemaBoundValue,
        limits: value::Limits,
        work: usize,
    ) -> Result<Value> {
        if bound.projection_id != root
            || self
                .digest(root)
                .is_none_or(|d| bound.schema_digest_sha256 != d)
        {
            return Err(Error::Projection);
        }
        let v = bound.value.as_ref().ok_or(Error::Shape)?;
        value::admit_v1_1(v, limits, value::BytePolicy::JsonOnly).map_err(|_| Error::Shape)?;
        let mut meter = ShapeMeter {
            registry: self,
            remaining: work,
            max_depth: limits.depth.min(64),
        };
        meter.named(root, v, None, 0)?;
        wire_json(v)
    }
    /// Shape-only native-type fixture entry. It does not establish an exposure grant.
    pub fn validate_type(
        &self,
        name: &str,
        v: &v1_1::WireValue,
        limits: value::Limits,
        work: usize,
    ) -> Result<Value> {
        value::admit_v1_1(v, limits, value::BytePolicy::JsonOnly).map_err(|_| Error::Shape)?;
        ShapeMeter {
            registry: self,
            remaining: work,
            max_depth: limits.depth.min(64),
        }
        .named(name, v, None, 0)?;
        wire_json(v)
    }
}

fn selected_policy(_document: &Value, profile: &str) -> Value {
    // The accepted preimage carries the profile identity; its complete type
    // closure already carries the selected policy references and constraints.
    Value::String(profile.to_owned())
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn field<'a>(v: &'a v1_1::WireValue, key: &str) -> Option<&'a v1_1::WireValue> {
    if let Some(v1_1::wire_value::Value::MapValue(m)) = &v.value {
        m.entries.iter().find(|e| e.key == key)?.value.as_ref()
    } else {
        None
    }
}
fn text(v: &v1_1::WireValue) -> Option<&str> {
    if let Some(v1_1::wire_value::Value::TextValue(s)) = &v.value {
        Some(s)
    } else {
        None
    }
}
fn null(v: &v1_1::WireValue) -> bool {
    matches!(v.value, Some(v1_1::wire_value::Value::NullValue(_)))
}
fn wire_json(v: &v1_1::WireValue) -> Result<Value> {
    use v1_1::wire_value::Value as W;
    Ok(match v.value.as_ref().ok_or(Error::Shape)? {
        W::NullValue(_) => Value::Null,
        W::BoolValue(b) => json!(b),
        W::IntegerValue(i) => json!(i),
        W::FloatValue(f) => json!(f),
        W::TextValue(s) => json!(s),
        W::ListValue(l) => Value::Array(l.items.iter().map(wire_json).collect::<Result<_>>()?),
        W::MapValue(m) => Value::Object(
            m.entries
                .iter()
                .map(|e| {
                    Ok((
                        e.key.clone(),
                        wire_json(e.value.as_ref().ok_or(Error::Shape)?)?,
                    ))
                })
                .collect::<Result<_>>()?,
        ),
        W::BytesValue(_) => return Err(Error::Shape),
    })
}
struct ShapeMeter<'a> {
    registry: &'a Registry,
    remaining: usize,
    max_depth: usize,
}
impl ShapeMeter<'_> {
    fn charge(&mut self, depth: usize) -> Result<()> {
        if depth > self.max_depth || self.remaining == 0 {
            return Err(Error::Limit);
        }
        self.remaining -= 1;
        Ok(())
    }
    fn named(
        &mut self,
        name: &str,
        v: &v1_1::WireValue,
        context: Option<&v1_1::WireValue>,
        depth: usize,
    ) -> Result<()> {
        let definition = self.registry.document["types"]
            .get(name)
            .ok_or(Error::HostContract)?;
        self.definition(definition, v, context, depth)?;
        // Mechanical native scalar/container rules; contextual codecs stay at the seam.
        let nonempty = matches!(
            name,
            "ApiId"
                | "CustomType"
                | "DeferredId"
                | "DiagnosticType"
                | "EntryId"
                | "LaneId"
                | "ModelId"
                | "OperationErrorCode"
                | "OperationId"
                | "ProviderId"
                | "RawStopReason"
                | "ResponseId"
                | "ToolCallId"
                | "ToolName"
                | "UsageId"
                | "CustomRole"
        );
        if nonempty && text(v).is_none_or(str::is_empty) {
            return Err(Error::Shape);
        }
        if name == "CustomRole" && matches!(text(v), Some("user" | "assistant" | "toolResult")) {
            return Err(Error::Shape);
        }
        if name == "SettledAssistantMessage" {
            let reason = field(v, "stopReason").and_then(text).ok_or(Error::Shape)?;
            if reason == "pending"
                || (matches!(reason, "error" | "aborted") && field(v, "errorMessage").is_none())
            {
                return Err(Error::Shape);
            }
        }
        if name == "MetadataPatch" {
            let j = wire_json(v)?;
            let a = j["delete"].as_array().ok_or(Error::Shape)?;
            if a.windows(2).any(|w| w[0].as_str() >= w[1].as_str())
                || a.iter()
                    .any(|k| j["set"].get(k.as_str().unwrap_or("")).is_some())
            {
                return Err(Error::Shape);
            }
        }
        if name == "DurableFileOperations" {
            let j = wire_json(v)?;
            for key in ["read", "written", "edited"] {
                let a = j[key].as_array().ok_or(Error::Shape)?;
                if a.windows(2).any(|w| w[0].as_str() >= w[1].as_str()) {
                    return Err(Error::Shape);
                }
            }
        }
        Ok(())
    }
    fn expression(
        &mut self,
        e: &Value,
        v: &v1_1::WireValue,
        c: Option<&v1_1::WireValue>,
        d: usize,
    ) -> Result<()> {
        self.charge(d)?;
        if let Some(name) = e.get("ref").and_then(Value::as_str) {
            return self.named(name, v, c, d + 1);
        }
        if let Some(l) = e.get("literal") {
            return if &wire_json(v)? == l {
                Ok(())
            } else {
                Err(Error::Shape)
            };
        }
        if let Some(a) = e.get("enum").and_then(Value::as_array) {
            return if a.iter().any(|s| s.as_str() == text(v)) {
                Ok(())
            } else {
                Err(Error::Shape)
            };
        }
        if let Some(e) = e.get("nullable") {
            return if null(v) {
                Ok(())
            } else {
                self.expression(e, v, c, d + 1)
            };
        }
        if let Some(e) = e.get("array") {
            return self.array(e, v, c, d + 1);
        }
        if let Some(e) = e.get("map") {
            return self.map(e, v, c, d + 1);
        }
        if let Some(e) = e.get("object") {
            return self.object(e, v, d + 1);
        }
        Err(Error::HostContract)
    }
    fn definition(
        &mut self,
        e: &Value,
        v: &v1_1::WireValue,
        c: Option<&v1_1::WireValue>,
        d: usize,
    ) -> Result<()> {
        self.charge(d)?;
        use v1_1::wire_value::Value as W;
        match e["kind"].as_str().ok_or(Error::HostContract)? {
            "primitive" => {
                let valid = match e["arm"].as_str().ok_or(Error::HostContract)? {
                    "string" => matches!(v.value, Some(W::TextValue(_))),
                    "bool" => matches!(v.value, Some(W::BoolValue(_))),
                    "integer" => {
                        matches!(v.value,Some(W::IntegerValue(i)) if i>=e["min"].as_i64().ok_or(Error::HostContract)? && i<=e["max"].as_i64().ok_or(Error::HostContract)?)
                    }
                    "finite_number" | "safe_integer_or_finite_float" => {
                        matches!(v.value,Some(W::IntegerValue(i)) if (-SAFE..=SAFE).contains(&i))
                            || matches!(v.value,Some(W::FloatValue(f)) if f.is_finite())
                    }
                    _ => return Err(Error::HostContract),
                };
                if valid { Ok(()) } else { Err(Error::Shape) }
            }
            "alias" => self.expression(&e["type"], v, c, d + 1),
            "literal" => {
                if wire_json(v)? == e["value"] {
                    Ok(())
                } else {
                    Err(Error::Shape)
                }
            }
            "enum" => {
                if e["values"]
                    .as_array()
                    .ok_or(Error::HostContract)?
                    .iter()
                    .any(|s| s.as_str() == text(v))
                {
                    Ok(())
                } else {
                    Err(Error::Shape)
                }
            }
            "object" => self.object(e, v, d + 1),
            "array" => self.array(&e["element"], v, c, d + 1),
            "map" => self.map(e, v, c, d + 1),
            "alternatives" => {
                let mut matches = 0;
                for a in e["alternatives"].as_array().ok_or(Error::HostContract)? {
                    match self.expression(&a["type"], v, c, d + 1) {
                        Ok(()) => matches += 1,
                        Err(Error::Shape) => {}
                        Err(err) => return Err(err),
                    }
                }
                if matches == 1 {
                    Ok(())
                } else {
                    Err(Error::Shape)
                }
            }
            "role_selected" => {
                let role = field(v, e["selector"].as_str().ok_or(Error::HostContract)?)
                    .and_then(text)
                    .ok_or(Error::Shape)?;
                self.expression(e["cases"].get(role).unwrap_or(&e["other"]), v, c, d + 1)
            }
            "context_selected" => {
                let api = c
                    .and_then(|c| field(c, "api"))
                    .and_then(text)
                    .ok_or(Error::Shape)?;
                self.expression(e["cases"].get(api).ok_or(Error::Shape)?, v, c, d + 1)
            }
            "native_open_role_object" => {
                let role = field(v, "role").ok_or(Error::Shape)?;
                self.expression(&e["role"], role, c, d + 1)?;
                if matches!(v.value, Some(W::MapValue(_))) {
                    Ok(())
                } else {
                    Err(Error::Shape)
                }
            }
            "native_json_algebra" => Ok(()), // bounded JsonOnly admission already proved every nested arm.
            "protobuf_empty_unit" => Err(Error::Shape),
            _ => Err(Error::HostContract),
        }
    }
    fn array(
        &mut self,
        e: &Value,
        v: &v1_1::WireValue,
        c: Option<&v1_1::WireValue>,
        d: usize,
    ) -> Result<()> {
        let Some(v1_1::wire_value::Value::ListValue(a)) = &v.value else {
            return Err(Error::Shape);
        };
        for v in &a.items {
            self.expression(e, v, c, d + 1)?;
        }
        Ok(())
    }
    fn map(
        &mut self,
        e: &Value,
        v: &v1_1::WireValue,
        c: Option<&v1_1::WireValue>,
        d: usize,
    ) -> Result<()> {
        let Some(v1_1::wire_value::Value::MapValue(m)) = &v.value else {
            return Err(Error::Shape);
        };
        for entry in &m.entries {
            let key = v1_1::WireValue {
                value: Some(v1_1::wire_value::Value::TextValue(entry.key.clone())),
            };
            self.expression(&e["key"], &key, c, d + 1)?;
            self.expression(
                &e["value"],
                entry.value.as_ref().ok_or(Error::Shape)?,
                c,
                d + 1,
            )?;
        }
        Ok(())
    }
    fn object(&mut self, e: &Value, v: &v1_1::WireValue, d: usize) -> Result<()> {
        let Some(v1_1::wire_value::Value::MapValue(m)) = &v.value else {
            return Err(Error::Shape);
        };
        let fields = e["fields"].as_object().ok_or(Error::HostContract)?;
        if e["closed"] == true && m.entries.iter().any(|v| !fields.contains_key(&v.key)) {
            return Err(Error::Shape);
        }
        for (key, spec) in fields {
            self.charge(d)?;
            match field(v, key) {
                None if spec["presence"] == "required" => return Err(Error::Shape),
                None => {}
                Some(value) => {
                    if null(value) && spec["nullable"] == true {
                        continue;
                    }
                    self.expression(&spec["type"], value, Some(v), d + 1)?;
                }
            }
        }
        Ok(())
    }
}

/// Exact public leaf schema contracts, captured by the authenticated host.
/// Keys are root-qualified JSON paths (array positions use `[]`), never wildcards.
#[derive(Clone, Debug)]
pub struct Exposure {
    leaves: BTreeMap<String, PortableSchema>,
    public_headers: BTreeSet<String>,
}
impl Exposure {
    pub fn new(
        leaves: BTreeMap<String, PortableSchema>,
        public_headers: BTreeSet<String>,
    ) -> Result<Self> {
        if leaves.len() > 256
            || public_headers.len() > 256
            || leaves.keys().any(|k| k.is_empty())
            || public_headers
                .iter()
                .any(|h| h.is_empty() || h != &h.to_ascii_lowercase() || forbidden_header(h))
        {
            return Err(Error::HostContract);
        }
        Ok(Self {
            leaves,
            public_headers,
        })
    }
    fn check(
        &self,
        registry: &Registry,
        root: &str,
        json: &Value,
        limits: jcs::Limits,
    ) -> Result<()> {
        fn walk(
            this: &Exposure,
            r: &Registry,
            name: &str,
            node: &Value,
            path: &str,
            limits: jcs::Limits,
            depth: usize,
        ) -> Result<()> {
            if depth > 64 {
                return Err(Error::Limit);
            }
            let t = &r.document["types"][name];
            let fixed = match name {
                "OperationError" => Some(("message", "operation failed")),
                "LaneHookHandlerErrorEvent"
                | "LaneEventHandlerErrorEvent"
                | "GlobalHookHandlerErrorEvent"
                | "GlobalEventHandlerErrorEvent" => Some(("error", "handler failed")),
                "FaultEvent" => Some(("message", "harness fault")),
                "RetryScheduledEvent" => Some(("errorMessage", "request failed")),
                "RetryEndEvent" => Some(("finalError", "request failed")),
                _ => None,
            };
            if let Some((key, value)) = fixed
                && node.get(key).is_some_and(|v| v != value)
            {
                return Err(Error::Exposure);
            }

            if matches!(
                name,
                "JsonValue" | "JsonObject" | "CustomAgentMessage" | "JsonSchema"
            ) {
                let contract = this.leaves.get(path).ok_or(Error::Exposure)?;
                contract
                    .validate_instance(&serde_json::to_vec(node).map_err(|_| Error::Limit)?, limits)
                    .map_err(|_| Error::Exposure)?;
                return Ok(());
            }
            if matches!(
                name,
                "PublicModelHeaders"
                    | "PublicProviderHeaders"
                    | "RawProviderHeaderPatch"
                    | "ResponseHeaders"
            ) {
                let mut seen = BTreeSet::new();
                for key in node.as_object().ok_or(Error::Shape)?.keys() {
                    let k = key.to_ascii_lowercase();
                    if forbidden_header(&k) || !this.public_headers.contains(&k) || !seen.insert(k)
                    {
                        return Err(Error::Exposure);
                    }
                }
            }
            fn expr(
                this: &Exposure,
                r: &Registry,
                e: &Value,
                n: &Value,
                p: &str,
                l: jcs::Limits,
                d: usize,
            ) -> Result<()> {
                if let Some(name) = e["ref"].as_str() {
                    return walk(this, r, name, n, p, l, d + 1);
                }
                if let Some(e) = e.get("nullable")
                    && !n.is_null()
                {
                    return expr(this, r, e, n, p, l, d + 1);
                }
                if let Some(e) = e.get("array") {
                    for n in n.as_array().ok_or(Error::Shape)? {
                        expr(this, r, e, n, &format!("{p}[]"), l, d + 1)?;
                    }
                }
                if let Some(e) = e.get("map") {
                    for (k, n) in n.as_object().ok_or(Error::Shape)? {
                        expr(this, r, &e["value"], n, &format!("{p}.{k}"), l, d + 1)?;
                    }
                }
                if let Some(e) = e.get("object") {
                    object(this, r, e, n, p, l, d + 1)?;
                }
                Ok(())
            }
            fn object(
                this: &Exposure,
                r: &Registry,
                t: &Value,
                n: &Value,
                p: &str,
                l: jcs::Limits,
                d: usize,
            ) -> Result<()> {
                for (key, spec) in t["fields"].as_object().ok_or(Error::HostContract)? {
                    if let Some(n) = n.get(key)
                        && (!n.is_null()
                            || matches!(
                                spec["type"]["ref"].as_str(),
                                Some("JsonValue" | "JsonObject")
                            ))
                    {
                        expr(this, r, &spec["type"], n, &format!("{p}.{key}"), l, d + 1)?;
                    }
                }
                Ok(())
            }
            match t["kind"].as_str().ok_or(Error::HostContract)? {
                "alias" => expr(this, r, &t["type"], node, path, limits, depth + 1),
                "object" => object(this, r, t, node, path, limits, depth + 1),
                "array" => {
                    for n in node.as_array().ok_or(Error::Shape)? {
                        expr(
                            this,
                            r,
                            &t["element"],
                            n,
                            &format!("{path}[]"),
                            limits,
                            depth + 1,
                        )?;
                    }
                    Ok(())
                }
                "role_selected" => {
                    let role = node["role"].as_str().ok_or(Error::Shape)?;
                    expr(
                        this,
                        r,
                        t["cases"].get(role).unwrap_or(&t["other"]),
                        node,
                        path,
                        limits,
                        depth + 1,
                    )
                }
                "alternatives" => {
                    // Already shape-admitted: evaluate only alternatives matching the actual DTO.
                    let wire = json_wire(node)?;
                    for a in t["alternatives"].as_array().ok_or(Error::HostContract)? {
                        let mut meter = ShapeMeter {
                            registry: r,
                            remaining: limits.instance_work,
                            max_depth: 64,
                        };
                        if meter.expression(&a["type"], &wire, None, 0).is_ok() {
                            return expr(this, r, &a["type"], node, path, limits, depth + 1);
                        }
                    }
                    Err(Error::Shape)
                }
                "context_selected" => {
                    let schema = this.leaves.get(path).ok_or(Error::Exposure)?;
                    schema
                        .validate_instance(
                            &serde_json::to_vec(node).map_err(|_| Error::Limit)?,
                            limits,
                        )
                        .map_err(|_| Error::Exposure)
                }
                _ => Ok(()),
            }
        }
        walk(self, registry, root, json, root, limits, 0)?;
        // Diagnostic exposure is safe by default only after omission/fixed replacement.
        fn diagnostics(
            v: &Value,
            path: &str,
            exposure: &Exposure,
            limits: jcs::Limits,
        ) -> Result<()> {
            if let Some(m) = v.as_object() {
                for (k, v) in m {
                    if matches!(
                        k.as_str(),
                        "diagnostics" | "stack" | "rawStopReason" | "responseId"
                    ) {
                        let schema = exposure
                            .leaves
                            .get(&format!("{path}.{k}"))
                            .ok_or(Error::Exposure)?;
                        schema
                            .validate_instance(
                                &serde_json::to_vec(v).map_err(|_| Error::Limit)?,
                                limits,
                            )
                            .map_err(|_| Error::Exposure)?;
                    }
                    if k == "errorMessage" && v != "request failed" && v != "operation failed" {
                        return Err(Error::Exposure);
                    }
                    diagnostics(v, &format!("{path}.{k}"), exposure, limits)?;
                }
            }
            if let Some(a) = v.as_array() {
                for v in a {
                    diagnostics(v, &format!("{path}[]"), exposure, limits)?;
                }
            }
            Ok(())
        }
        diagnostics(json, root, self, limits)
    }
}
fn forbidden_header(h: &str) -> bool {
    matches!(
        h,
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "set-cookie"
            | "api-key"
            | "x-api-key"
            | "x-goog-api-key"
    )
}
/// Fixture/adapter construction preserves integer versus float JSON numbers.
pub fn json_wire(v: &Value) -> Result<v1_1::WireValue> {
    use v1_1::wire_value::Value as W;
    Ok(v1_1::WireValue {
        value: Some(match v {
            Value::Null => W::NullValue(v1_1::NullValue {}),
            Value::Bool(b) => W::BoolValue(*b),
            Value::String(s) => W::TextValue(s.clone()),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    W::IntegerValue(i)
                } else {
                    W::FloatValue(n.as_f64().ok_or(Error::Shape)?)
                }
            }
            Value::Array(a) => W::ListValue(v1_1::WireList {
                items: a.iter().map(json_wire).collect::<Result<_>>()?,
            }),
            Value::Object(m) => W::MapValue(v1_1::WireMap {
                entries: m
                    .iter()
                    .map(|(k, v)| {
                        Ok(v1_1::WireMapEntry {
                            key: k.clone(),
                            value: Some(json_wire(v)?),
                        })
                    })
                    .collect::<Result<_>>()?,
            }),
        }),
    })
}

#[derive(Clone, Debug)]
pub struct ApprovedProfile {
    principal: String,
    kind: String,
    name: String,
    profile: String,
    handler: String,
    minors: BTreeSet<u32>,
    replay: i32,
    execution: i32,
    scope: String,
    permissions: BTreeSet<(String, String)>,
    registry: Registry,
    exposure: Exposure,
}
/// All catalog facts come from host configuration; no peer declaration populates this input.
pub struct ProfileApproval {
    pub principal: String,
    pub kind: String,
    pub name: String,
    pub profile: String,
    pub handler: String,
    pub minors: BTreeSet<u32>,
    pub replay: i32,
    pub execution: i32,
    pub scope: String,
    pub permissions: BTreeSet<(String, String)>,
    pub exposure: Exposure,
}
impl ApprovedProfile {
    pub fn new(input: ProfileApproval) -> Result<Self> {
        if [
            &input.principal,
            &input.kind,
            &input.name,
            &input.profile,
            &input.handler,
            &input.scope,
        ]
        .iter()
        .any(|s| s.is_empty())
            || input.minors.is_empty()
            || input.minors.iter().any(|m| *m > 1)
            || input.permissions.len() > 256
            || input
                .permissions
                .iter()
                .any(|(n, s)| n.is_empty() || s.is_empty())
            || !profile_mode(&input.profile, input.replay, input.execution)
            || input.kind != input.profile.trim_end_matches(".v1")
        {
            return Err(Error::HostContract);
        }
        let mandatory: &[&str] = match input.name.as_str() {
            "before_payload" if input.profile == "hook.v1" => &[
                "hook.input:before_payload.event.payload",
                "hook.output:before_payload.result.payload",
            ],
            "before_tool" if input.profile == "hook.v1" => &["hook.input:before_tool.event.args"],
            "after_tool" if input.profile == "hook.v1" => &["hook.input:after_tool.event.args"],
            _ => &[],
        };
        if mandatory
            .iter()
            .any(|p| !input.exposure.leaves.contains_key(*p))
        {
            return Err(Error::HostContract);
        }
        Ok(Self {
            principal: input.principal,
            kind: input.kind,
            name: input.name,
            profile: input.profile,
            handler: input.handler,
            minors: input.minors,
            replay: input.replay,
            execution: input.execution,
            scope: input.scope,
            permissions: input.permissions,
            registry: Registry::bundled()?,
            exposure: input.exposure,
        })
    }
    pub fn principal(&self) -> &str {
        &self.principal
    }
    pub fn kind(&self) -> &str {
        &self.kind
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn profile(&self) -> &str {
        &self.profile
    }
    pub fn handler(&self) -> &str {
        &self.handler
    }
    pub fn scope(&self) -> &str {
        &self.scope
    }
    pub fn minors(&self) -> &BTreeSet<u32> {
        &self.minors
    }
    pub fn replay(&self) -> i32 {
        self.replay
    }
    pub fn execution(&self) -> i32 {
        self.execution
    }
    pub fn permissions(&self) -> &BTreeSet<(String, String)> {
        &self.permissions
    }
    pub fn registry(&self) -> &Registry {
        &self.registry
    }
    pub fn reference(&self, minor: u32) -> Result<v1_1::CapabilityRef> {
        if !self.minors.contains(&minor) {
            return Err(Error::HostContract);
        }
        Ok(v1_1::CapabilityRef {
            authenticated_extension_id: self.principal.clone(),
            kind: self.kind.clone(),
            name: self.name.clone(),
            profile: Some(v1_1::ProfileVersion {
                name: self.profile.clone(),
                major: 1,
                minor,
            }),
            schema_digest_sha256: canonical_profile_digest(&self.profile, 1, minor)
                .ok_or(Error::HostContract)?
                .to_vec(),
            handler_id: Some(self.handler.clone()),
        })
    }
    fn bound(
        &self,
        root: &str,
        b: Option<&v1_1::SchemaBoundValue>,
        limits: value::Limits,
        json_limits: jcs::Limits,
        budget: &mut PayloadBudget,
        out: &mut BTreeMap<String, Value>,
    ) -> Result<()> {
        let b = b.ok_or(Error::Shape)?;
        budget.charge(b.value.as_ref().ok_or(Error::Shape)?, limits, 1)?;
        let v = self
            .registry
            .validate_v1_1(root, b, limits, json_limits.instance_work)?;
        self.exposure.check(&self.registry, root, &v, json_limits)?;
        out.insert(root.to_owned(), v);
        Ok(())
    }
}
pub fn profile_mode(profile: &str, replay: i32, execution: i32) -> bool {
    match profile {
        "hook.v1" => matches!(replay, 1..=3) && execution == 1,
        "provider.v1" => matches!(replay, 1 | 2) && execution == 2,
        "resource.v1" | "host-action.v1" => matches!(replay, 1..=3) && execution == 1,
        "event-sink.v1" | "telemetry-sink.v1" => matches!(replay, 1..=3) && execution == 3,
        _ => false,
    }
}

impl ApprovedProfile {
    /// Decode through the selected profile DTO before common semantic admission.
    #[allow(clippy::too_many_arguments)]
    pub fn validate_payload(
        &self,
        minor: u32,
        method: &str,
        stage: Stage,
        bytes: &[u8],
        limits: value::Limits,
        json_limits: jcs::Limits,
        native: &mut impl NativeValidator,
    ) -> Result<ValidatedPayload> {
        if !self.minors.contains(&minor) || bytes.len() > limits.bytes_length {
            return Err(Error::Limit);
        }
        let (_, mode) = self
            .registry
            .method(&self.profile, method)
            .ok_or(Error::Method)?;
        if stage == Stage::Progress && mode != Mode::Stream {
            return Err(Error::Direction);
        }
        let mut candidate = ValidatedPayload {
            method: method.into(),
            stage,
            projections: BTreeMap::new(),
            fields: json!({}),
        };
        let mut budget = PayloadBudget {
            elements: 0,
            decoded: bytes.len(),
        };
        // 1.0 and 1.1 profile message fields are identical. We still perform an
        // actual selected-version decode, preserving independent core/profile minors.
        macro_rules! decode {
            ($ty:ident) => {{
                if minor == 0 {
                    let old = v1_0::$ty::decode(bytes).map_err(|_| Error::Shape)?;
                    v1_1::$ty::decode(old.encode_to_vec().as_slice()).map_err(|_| Error::Shape)?
                } else {
                    v1_1::$ty::decode(bytes).map_err(|_| Error::Shape)?
                }
            }};
        }
        macro_rules! bound {
            ($root:expr,$b:expr) => {
                self.bound(
                    $root,
                    $b,
                    limits,
                    json_limits,
                    &mut budget,
                    &mut candidate.projections,
                )?
            };
        }
        if self.profile == "hook.v1" {
            if method != self.name {
                return Err(Error::Method);
            }
            match stage {
                Stage::Request => {
                    let r = decode!(InvokeHookRequest);
                    if r.handler_id != self.handler {
                        return Err(Error::Projection);
                    }
                    bound!(&format!("hook.input:{method}"), r.input.as_ref());
                    candidate.fields = json!({"handler_id":r.handler_id});
                }
                Stage::Terminal if method == "before_resume" => {
                    let _ = decode!(BeforeResumeUnitResult);
                }
                Stage::Terminal => {
                    let r = decode!(InvokeHookResult);
                    if r.output.is_some() {
                        bound!(&format!("hook.output:{method}"), r.output.as_ref());
                    }
                }
                Stage::Progress => return Err(Error::Direction),
            }
        } else {
            match (method, stage) {
                ("provider.invoke", Stage::Request) => {
                    let r = decode!(InvokeProviderRequest);
                    if r.auth_mode != "none" || r.model_id.is_empty() {
                        return Err(Error::Exposure);
                    }
                    bound!("provider.context.v1", r.context.as_ref());
                    bound!("provider.options.v1", r.options.as_ref());
                    bound!("provider.tools.v1", r.tools.as_ref());
                    bound!("provider.model.v1", r.model.as_ref());
                    let model = &candidate.projections["provider.model.v1"];
                    if model.get("headers").is_some() || model["id"] != r.model_id {
                        return Err(Error::Exposure);
                    }
                    candidate.fields = json!({"model_id":r.model_id,"auth_mode":"none"});
                }
                ("provider.invoke", Stage::Progress) => {
                    let r = decode!(ProviderStreamEvent);
                    bound!("provider.assistant_event.v1", r.event.as_ref());
                }
                ("provider.invoke", Stage::Terminal) => {
                    let r = decode!(InvokeProviderResult);
                    bound!(
                        "provider.assistant_message.v1",
                        r.assistant_message.as_ref()
                    );
                    bound!("provider.usage.v1", r.usage.as_ref());
                    if candidate.projections["provider.assistant_message.v1"]["usage"]
                        != candidate.projections["provider.usage.v1"]
                    {
                        return Err(Error::Shape);
                    }
                }
                ("resource.list", Stage::Request) => {
                    let r = decode!(ResourceListRequest);
                    nonempty(&r.scope)?;
                    if r.known_revision.as_ref().is_some_and(String::is_empty) {
                        return Err(Error::Shape);
                    }
                    candidate.fields = json!({"scope":r.scope,"known_revision":r.known_revision});
                }
                ("resource.list", Stage::Terminal) => {
                    let r = decode!(ResourceListResult);
                    nonempty(&r.revision)?;
                    let mut seen = BTreeSet::new();
                    let mut manifests = Vec::new();
                    for m in r.resources {
                        nonempty(&m.id)?;
                        nonempty(&m.version)?;
                        if m.digest_sha256.len() != 32
                            || !seen.insert((m.id.clone(), m.version.clone()))
                            || !matches!(m.kind.as_str(), "skill" | "prompt_template")
                        {
                            return Err(Error::Shape);
                        }
                        let root = format!("resource.metadata:{}", m.kind);
                        bound!(&root, m.metadata.as_ref());
                        manifests.push(json!({"id":m.id,"version":m.version,"kind":m.kind,"digest":hex(&m.digest_sha256),"metadata":candidate.projections[&root]}));
                    }
                    candidate.fields = json!({"revision":r.revision,"resources":manifests});
                }
                ("resource.read", Stage::Request) => {
                    let r = decode!(ResourceReadRequest);
                    nonempty(&r.id)?;
                    nonempty(&r.version)?;
                    if r.expected_digest_sha256.len() != 32 {
                        return Err(Error::Shape);
                    }
                    candidate.fields = json!({"id":r.id,"version":r.version,"digest":hex(&r.expected_digest_sha256)});
                }
                ("resource.read", Stage::Terminal) => {
                    let r = decode!(ResourceReadResult);
                    let Some(v1_1::resource_read_result::Value::Text(t)) = r.value else {
                        return Err(Error::Exposure);
                    };
                    if r.digest_sha256 != sha256(t.as_bytes()) {
                        return Err(Error::Shape);
                    }
                    candidate.fields = json!({"digest":hex(&r.digest_sha256),"text":t});
                }
                ("resource.changed", Stage::Request) => {
                    let r = decode!(ResourceChanged);
                    nonempty(&r.scope)?;
                    candidate.fields = json!({"scope":r.scope});
                }
                ("resource.changed", Stage::Terminal) => {
                    let _ = decode!(ResourceChangedAck);
                }
                ("snapshot.get", Stage::Request) => {
                    let r = decode!(SnapshotGetRequest);
                    nonempty(&r.lane)?;
                    nonempty(&r.visibility)?;
                    candidate.fields = json!({"scope":r.lane,"visibility":r.visibility});
                }
                ("snapshot.get", Stage::Terminal) => {
                    let r = decode!(SnapshotGetResult);
                    bound!("host.filtered_snapshot.v1", r.filtered_snapshot.as_ref());
                }
                ("resource.get", Stage::Request) => {
                    let r = decode!(ResourceGetRequest);
                    nonempty(&r.scope)?;
                    nonempty(&r.id)?;
                    if r.version.as_ref().is_some_and(String::is_empty) {
                        return Err(Error::Shape);
                    }
                    candidate.fields = json!({"scope":r.scope,"id":r.id,"version":r.version});
                }
                ("resource.get", Stage::Terminal) => {
                    let r = decode!(ResourceGetResult);
                    bound!("host.resource_snapshot.v1", r.snapshot.as_ref());
                    let j = &candidate.projections["host.resource_snapshot.v1"];
                    if j["id"].as_str().is_none_or(str::is_empty)
                        || j["version"].as_str().is_none_or(str::is_empty)
                        || j["digest"].as_str()
                            != Some(
                                hex(&sha256(j["value"].as_str().ok_or(Error::Shape)?.as_bytes()))
                                    .as_str(),
                            )
                    {
                        return Err(Error::Shape);
                    }
                }
                ("queue.steer", Stage::Request) => {
                    let r = decode!(QueueSteerRequest);
                    nonempty(&r.lane)?;
                    bound!("host.queue_item.v1", r.item.as_ref());
                    candidate.fields = json!({"scope":r.lane});
                }
                ("queue.follow_up", Stage::Request) => {
                    let r = decode!(QueueFollowUpRequest);
                    nonempty(&r.lane)?;
                    bound!("host.queue_item.v1", r.item.as_ref());
                    candidate.fields = json!({"scope":r.lane});
                }
                ("queue.next_run", Stage::Request) => {
                    let r = decode!(QueueNextRunRequest);
                    nonempty(&r.lane)?;
                    bound!("host.queue_item.v1", r.item.as_ref());
                    candidate.fields = json!({"scope":r.lane});
                }
                ("queue.steer" | "queue.follow_up" | "queue.next_run", Stage::Terminal) => {
                    let r = decode!(QueueResult);
                    if !r.accepted || r.item_id.is_empty() {
                        return Err(Error::Shape);
                    }
                    candidate.fields = json!({"item_id":r.item_id,"accepted":true});
                }
                ("operation.abort", Stage::Request) => {
                    let r = decode!(OperationAbortRequest);
                    nonempty(&r.lane)?;
                    nonempty(&r.operation_id)?;
                    candidate.fields = json!({"scope":r.lane,"operation_id":r.operation_id});
                }
                ("operation.abort", Stage::Terminal) => {
                    let r = decode!(OperationAbortResult);
                    candidate.fields = json!({"aborted":r.aborted});
                }
                ("event.subscribe", Stage::Request) => {
                    let r = decode!(SubscribeRequest);
                    nonempty(&r.scope)?;
                    candidate.fields = json!({"scope":r.scope});
                }
                ("event.subscribe", Stage::Terminal) => {
                    let _ = decode!(SubscribeEnd);
                }
                ("event.subscribe", Stage::Progress) => {
                    let r = decode!(EventSinkItem);
                    match r.item.ok_or(Error::Shape)? {
                        v1_1::event_sink_item::Item::Snapshot(s) => {
                            nonempty(&s.subscription_id)?;
                            bound!("event.filtered_snapshot.v1", s.filtered_snapshot.as_ref());
                            candidate.fields = json!({"subscription_id":s.subscription_id,"snapshot_revision":s.snapshot_revision,"next_event_sequence":s.next_event_sequence});
                        }
                        v1_1::event_sink_item::Item::Event(e) => {
                            nonempty(&e.subscription_id)?;
                            bound!("event.filtered_event.v1", e.event.as_ref());
                            candidate.fields = json!({"subscription_id":e.subscription_id,"event_sequence":e.sequence});
                        }
                        v1_1::event_sink_item::Item::Gap(g) => {
                            nonempty(&g.subscription_id)?;
                            if g.first_dropped > g.last_dropped {
                                return Err(Error::Shape);
                            }
                            candidate.fields = json!({"subscription_id":g.subscription_id,"first_dropped":g.first_dropped,"last_dropped":g.last_dropped});
                        }
                    }
                }
                ("event.resync", Stage::Request) => {
                    let r = decode!(ResyncRequest);
                    nonempty(&r.subscription_id)?;
                    candidate.fields = json!({"subscription_id":r.subscription_id});
                }
                ("event.resync", Stage::Terminal) => {
                    let r = decode!(ResyncResult);
                    let s = r.snapshot.ok_or(Error::Shape)?;
                    nonempty(&s.subscription_id)?;
                    bound!("event.filtered_snapshot.v1", s.filtered_snapshot.as_ref());
                    candidate.fields = json!({"subscription_id":s.subscription_id,"snapshot_revision":s.snapshot_revision,"next_event_sequence":s.next_event_sequence});
                }
                ("telemetry.emit_batch", Stage::Request) => {
                    let r = decode!(TelemetryEmitBatchRequest);
                    if r.schema_version != 1 || r.records.len() > limits.elements {
                        return Err(Error::Shape);
                    }
                    let mut records = Vec::new();
                    for b in r.records {
                        bound!("telemetry.record.v1", Some(&b));
                        let j = &candidate.projections["telemetry.record.v1"];
                        telemetry_rules(j)?;
                        records.push(j.clone());
                    }
                    candidate.fields =
                        json!({"schema_version":1,"sequence":r.sequence,"records":records});
                }
                ("telemetry.emit_batch", Stage::Terminal) => {
                    let r = decode!(TelemetryEmitBatchResult);
                    let count = r.accepted.checked_add(r.dropped).ok_or(Error::Shape)?;
                    candidate.fields =
                        json!({"accepted":r.accepted,"dropped":r.dropped,"count":count});
                }
                ("telemetry.flush", Stage::Request) => {
                    let r = decode!(TelemetryFlushRequest);
                    candidate.fields = json!({"through_sequence":r.through_sequence});
                }
                ("telemetry.flush", Stage::Terminal) => {
                    let r = decode!(TelemetryFlushResult);
                    candidate.fields = json!({"flushed_through":r.flushed_through});
                }
                _ => return Err(Error::Method),
            }
        }
        native.validate(&candidate).map_err(|e| match e {
            NativeError::Rejected => Error::NativeRejected,
            NativeError::LocalFault => Error::HostLocalFault,
        })?;
        Ok(candidate)
    }
}
struct PayloadBudget {
    elements: usize,
    decoded: usize,
}
impl PayloadBudget {
    fn charge(&mut self, v: &v1_1::WireValue, limits: value::Limits, depth: usize) -> Result<()> {
        if depth > limits.depth.min(64) {
            return Err(Error::Limit);
        }
        self.decoded = self
            .decoded
            .checked_add(std::mem::size_of::<v1_1::WireValue>())
            .ok_or(Error::Limit)?;
        use v1_1::wire_value::Value as W;
        match v.value.as_ref().ok_or(Error::Shape)? {
            W::TextValue(s) => {
                self.decoded = self.decoded.checked_add(s.len()).ok_or(Error::Limit)?
            }
            W::ListValue(a) => {
                self.elements = self
                    .elements
                    .checked_add(a.items.len())
                    .ok_or(Error::Limit)?;
                for v in &a.items {
                    self.charge(v, limits, depth + 1)?;
                }
            }
            W::MapValue(m) => {
                self.elements = self
                    .elements
                    .checked_add(m.entries.len())
                    .ok_or(Error::Limit)?;
                for e in &m.entries {
                    self.decoded = self
                        .decoded
                        .checked_add(std::mem::size_of::<v1_1::WireMapEntry>())
                        .and_then(|n| n.checked_add(e.key.len()))
                        .ok_or(Error::Limit)?;
                    self.charge(e.value.as_ref().ok_or(Error::Shape)?, limits, depth + 1)?;
                }
            }
            W::BytesValue(_) => return Err(Error::Shape),
            _ => {}
        }
        if self.decoded > limits.decoded_bytes || self.elements > limits.elements {
            return Err(Error::Limit);
        }
        Ok(())
    }
}
fn nonempty(s: &str) -> Result<()> {
    if s.is_empty() {
        Err(Error::Shape)
    } else {
        Ok(())
    }
}
fn sha256(b: &[u8]) -> [u8; 32] {
    ring::digest::digest(&ring::digest::SHA256, b)
        .as_ref()
        .try_into()
        .expect("sha256")
}
fn telemetry_rules(j: &Value) -> Result<()> {
    if j["spanId"].as_u64().is_none_or(|id| id == 0)
        || j.get("parentSpanId")
            .is_some_and(|p| p.as_u64().is_none_or(|id| id == 0))
    {
        return Err(Error::Shape);
    }
    match j["kind"].as_str() {
        Some("span_finish") if matches!(j["status"].as_str(), Some("ok" | "error")) => Ok(()),
        Some("span_start" | "span_event") if j.get("status").is_none() => Ok(()),
        _ => Err(Error::Shape),
    }
}

// Explicit local wire DTOs mirror only the already-authoritative six profile
// messages. No descriptor, .proto or generated core/tool source is changed.
// Distinct namespaces force actual selected-minor decoding at the boundary.
macro_rules! profile_dtos {
    ($module:ident,$core:ident) => {
        pub mod $module {
            pub use crate::wire::$core::*;
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct InvokeHookRequest {
                #[prost(string, tag = "1")]
                pub handler_id: String,
                #[prost(message, optional, tag = "2")]
                pub input: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct InvokeHookResult {
                #[prost(message, optional, tag = "1")]
                pub output: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct BeforeResumeUnitResult {}
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct InvokeProviderRequest {
                #[prost(string, tag = "1")]
                pub model_id: String,
                #[prost(message, optional, tag = "2")]
                pub context: Option<SchemaBoundValue>,
                #[prost(message, optional, tag = "3")]
                pub options: Option<SchemaBoundValue>,
                #[prost(message, optional, tag = "4")]
                pub tools: Option<SchemaBoundValue>,
                #[prost(string, tag = "5")]
                pub auth_mode: String,
                #[prost(message, optional, tag = "6")]
                pub model: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ProviderStreamEvent {
                #[prost(message, optional, tag = "1")]
                pub event: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct InvokeProviderResult {
                #[prost(message, optional, tag = "1")]
                pub assistant_message: Option<SchemaBoundValue>,
                #[prost(message, optional, tag = "2")]
                pub usage: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResourceListRequest {
                #[prost(string, tag = "1")]
                pub scope: String,
                #[prost(string, optional, tag = "2")]
                pub known_revision: Option<String>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResourceManifest {
                #[prost(string, tag = "1")]
                pub id: String,
                #[prost(string, tag = "2")]
                pub version: String,
                #[prost(string, tag = "3")]
                pub kind: String,
                #[prost(bytes = "vec", tag = "4")]
                pub digest_sha256: Vec<u8>,
                #[prost(message, optional, tag = "5")]
                pub metadata: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResourceListResult {
                #[prost(string, tag = "1")]
                pub revision: String,
                #[prost(message, repeated, tag = "2")]
                pub resources: Vec<ResourceManifest>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResourceReadRequest {
                #[prost(string, tag = "1")]
                pub id: String,
                #[prost(string, tag = "2")]
                pub version: String,
                #[prost(bytes = "vec", tag = "3")]
                pub expected_digest_sha256: Vec<u8>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResourceReadResult {
                #[prost(bytes = "vec", tag = "1")]
                pub digest_sha256: Vec<u8>,
                #[prost(oneof = "resource_read_result::Value", tags = "2,3,4")]
                pub value: Option<resource_read_result::Value>,
            }
            pub mod resource_read_result {
                #[derive(Clone, PartialEq, prost::Oneof)]
                pub enum Value {
                    #[prost(string, tag = "2")]
                    Text(String),
                    #[prost(bytes, tag = "3")]
                    RawBytes(Vec<u8>),
                    #[prost(message, tag = "4")]
                    Portable(super::SchemaBoundValue),
                }
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResourceChanged {
                #[prost(string, tag = "1")]
                pub scope: String,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResourceChangedAck {}
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct SnapshotGetRequest {
                #[prost(string, tag = "1")]
                pub lane: String,
                #[prost(string, tag = "2")]
                pub visibility: String,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct SnapshotGetResult {
                #[prost(message, optional, tag = "1")]
                pub filtered_snapshot: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResourceGetRequest {
                #[prost(string, tag = "1")]
                pub scope: String,
                #[prost(string, tag = "2")]
                pub id: String,
                #[prost(string, optional, tag = "3")]
                pub version: Option<String>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResourceGetResult {
                #[prost(message, optional, tag = "1")]
                pub snapshot: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct QueueSteerRequest {
                #[prost(string, tag = "1")]
                pub lane: String,
                #[prost(message, optional, tag = "2")]
                pub item: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct QueueFollowUpRequest {
                #[prost(string, tag = "1")]
                pub lane: String,
                #[prost(message, optional, tag = "2")]
                pub item: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct QueueNextRunRequest {
                #[prost(string, tag = "1")]
                pub lane: String,
                #[prost(message, optional, tag = "2")]
                pub item: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct QueueResult {
                #[prost(string, tag = "1")]
                pub item_id: String,
                #[prost(bool, tag = "2")]
                pub accepted: bool,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct OperationAbortRequest {
                #[prost(string, tag = "1")]
                pub lane: String,
                #[prost(string, tag = "2")]
                pub operation_id: String,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct OperationAbortResult {
                #[prost(bool, tag = "1")]
                pub aborted: bool,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct SubscribeRequest {
                #[prost(string, tag = "1")]
                pub scope: String,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct SubscriptionSnapshot {
                #[prost(string, tag = "1")]
                pub subscription_id: String,
                #[prost(uint64, tag = "2")]
                pub snapshot_revision: u64,
                #[prost(uint64, tag = "3")]
                pub next_event_sequence: u64,
                #[prost(message, optional, tag = "4")]
                pub filtered_snapshot: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct EventNotification {
                #[prost(string, tag = "1")]
                pub subscription_id: String,
                #[prost(uint64, tag = "2")]
                pub sequence: u64,
                #[prost(message, optional, tag = "3")]
                pub event: Option<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct EventGap {
                #[prost(string, tag = "1")]
                pub subscription_id: String,
                #[prost(uint64, tag = "2")]
                pub first_dropped: u64,
                #[prost(uint64, tag = "3")]
                pub last_dropped: u64,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct EventSinkItem {
                #[prost(oneof = "event_sink_item::Item", tags = "1,2,3")]
                pub item: Option<event_sink_item::Item>,
            }
            pub mod event_sink_item {
                #[derive(Clone, PartialEq, prost::Oneof)]
                pub enum Item {
                    #[prost(message, tag = "1")]
                    Snapshot(super::SubscriptionSnapshot),
                    #[prost(message, tag = "2")]
                    Event(super::EventNotification),
                    #[prost(message, tag = "3")]
                    Gap(super::EventGap),
                }
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct SubscribeEnd {}
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResyncRequest {
                #[prost(string, tag = "1")]
                pub subscription_id: String,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct ResyncResult {
                #[prost(message, optional, tag = "1")]
                pub snapshot: Option<SubscriptionSnapshot>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct TelemetryEmitBatchRequest {
                #[prost(uint32, tag = "1")]
                pub schema_version: u32,
                #[prost(uint64, tag = "2")]
                pub sequence: u64,
                #[prost(message, repeated, tag = "3")]
                pub records: Vec<SchemaBoundValue>,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct TelemetryEmitBatchResult {
                #[prost(uint64, tag = "1")]
                pub accepted: u64,
                #[prost(uint64, tag = "2")]
                pub dropped: u64,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct TelemetryFlushRequest {
                #[prost(uint64, tag = "1")]
                pub through_sequence: u64,
            }
            #[derive(Clone, PartialEq, prost::Message)]
            pub struct TelemetryFlushResult {
                #[prost(uint64, tag = "1")]
                pub flushed_through: u64,
            }
        }
    };
}
profile_dtos!(wire_v1_0, v1_0);
profile_dtos!(wire_v1_1, v1_1);
