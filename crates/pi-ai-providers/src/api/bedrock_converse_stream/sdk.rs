use super::payload::{BedrockObject, BedrockValue};
use pi_ai_core::{ModelsError, ModelsErrorCode};
use serde_json::{Map, Value};

pub(super) enum Shape {
    Object(&'static [(&'static str, &'static str)], bool),
    List(&'static str, bool),
    Map(&'static str, bool),
    Blob,
    Document,
    Scalar,
}
fn invalid() -> ModelsError {
    ModelsError::new(ModelsErrorCode::Stream)
}
fn assigned(out: &mut Map<String, Value>, key: &str, value: Value) {
    if key != "__proto__" {
        out.insert(key.to_owned(), value);
    }
}
/// SDK schema filtering happens after the single typed payload callback.
pub(super) fn serialize(value: &BedrockValue, shape: &str) -> Result<Value, ModelsError> {
    project(value, shape, 0, &mut ProjectionBudget::default())
}
// The post-hook projection owns a finite graph independent of ignored input
// members. Charge before allocating each selected key/scalar/container.
struct ProjectionBudget {
    bytes: usize,
    nodes: usize,
}
impl Default for ProjectionBudget {
    fn default() -> Self {
        Self {
            bytes: 1024 * 1024,
            nodes: 65536,
        }
    }
}
impl ProjectionBudget {
    fn charge(&mut self, bytes: usize) -> Result<(), ModelsError> {
        self.bytes = self.bytes.checked_sub(bytes).ok_or_else(invalid)?;
        Ok(())
    }
    fn node(&mut self) -> Result<(), ModelsError> {
        self.nodes = self.nodes.checked_sub(1).ok_or_else(invalid)?;
        Ok(())
    }
    fn blob(&mut self, n: usize) -> Result<(), ModelsError> {
        self.charge(
            n.checked_add(2)
                .and_then(|n| (n / 3).checked_mul(4))
                .ok_or_else(invalid)?,
        )
    }
}
fn project(
    value: &BedrockValue,
    name: &str,
    depth: usize,
    budget: &mut ProjectionBudget,
) -> Result<Value, ModelsError> {
    if depth > 128 {
        return Err(invalid());
    }
    let shape = super::schema::shape(name).ok_or_else(invalid)?;
    match (&shape, value) {
        (Shape::Object(fields, union), BedrockValue::Object(object)) => {
            budget.node()?;
            let mut out = Map::new();
            for (key, child) in *fields {
                if let Some(v) = object.get(key).filter(|v| !matches!(v, BedrockValue::Null)) {
                    budget.charge(key.len())?;
                    assigned(&mut out, key, project(v, child, depth + 1, budget)?);
                }
            }
            if *union && out.is_empty() {
                if let Some(BedrockValue::Array(pair)) = object.get("$unknown")
                    && let [BedrockValue::String(key), v, ..] = pair.as_slice()
                {
                    budget.charge(key.len())?;
                    assigned(&mut out, key, document(v, depth + 1, budget)?);
                }
            } else if object
                .get("__type")
                .and_then(BedrockValue::as_str)
                .is_some()
            {
                for (key, v) in object.iter() {
                    if !out.contains_key(key)
                        && !matches!(
                            key,
                            "__proto__"
                                | "constructor"
                                | "toString"
                                | "valueOf"
                                | "hasOwnProperty"
                                | "isPrototypeOf"
                                | "propertyIsEnumerable"
                                | "toLocaleString"
                                | "__defineGetter__"
                                | "__defineSetter__"
                                | "__lookupGetter__"
                                | "__lookupSetter__"
                        )
                    {
                        budget.charge(key.len())?;
                        assigned(&mut out, key, document(v, depth + 1, budget)?);
                    }
                }
            }
            Ok(Value::Object(out))
        }
        (Shape::Object(_, _), BedrockValue::Array(_) | BedrockValue::Bytes(_)) => {
            budget.node()?;
            Ok(Value::Object(Map::new()))
        }
        (Shape::List(child, sparse), BedrockValue::Array(values)) => {
            budget.node()?;
            Ok(Value::Array(
                values
                    .iter()
                    .filter(|v| *sparse || !matches!(v, BedrockValue::Null))
                    .map(|v| project(v, child, depth + 1, budget))
                    .collect::<Result<_, _>>()?,
            ))
        }
        (Shape::Map(child, sparse), BedrockValue::Object(values)) => {
            budget.node()?;
            let mut out = Map::new();
            for (k, v) in values.iter() {
                if *sparse || !matches!(v, BedrockValue::Null) {
                    budget.charge(k.len())?;
                    assigned(&mut out, k, project(v, child, depth + 1, budget)?);
                }
            }
            Ok(Value::Object(out))
        }
        (Shape::Blob, BedrockValue::Bytes(v)) => {
            budget.node()?;
            budget.blob(v.len())?;
            Ok(Value::String(base64_encode(v)))
        }
        (Shape::Blob, BedrockValue::String(v)) => {
            budget.node()?;
            budget.blob(v.len())?;
            Ok(Value::String(base64_encode(v.as_bytes())))
        }
        (Shape::Document, _) => document(value, depth, budget),
        _ => json_clone(value, depth, budget),
    }
}
fn document(
    value: &BedrockValue,
    depth: usize,
    budget: &mut ProjectionBudget,
) -> Result<Value, ModelsError> {
    if depth > 128 {
        return Err(invalid());
    }
    budget.node()?;
    match value {
        BedrockValue::Bytes(bytes) => {
            budget.blob(bytes.len())?;
            Ok(Value::String(base64_encode(bytes)))
        }
        BedrockValue::Array(values) => Ok(Value::Array(
            values
                .iter()
                .map(|v| document(v, depth + 1, budget))
                .collect::<Result<_, _>>()?,
        )),
        BedrockValue::Object(values) => {
            let mut out = Map::new();
            for (k, v) in values.iter() {
                budget.charge(k.len())?;
                assigned(&mut out, k, document(v, depth + 1, budget)?);
            }
            Ok(Value::Object(out))
        }
        _ => {
            charge_scalar(value, budget)?;
            Ok(value.to_json())
        }
    }
}
fn json_clone(
    value: &BedrockValue,
    depth: usize,
    budget: &mut ProjectionBudget,
) -> Result<Value, ModelsError> {
    if depth > 128 {
        return Err(invalid());
    }
    budget.node()?;
    match value {
        BedrockValue::Array(values) => Ok(Value::Array(
            values
                .iter()
                .map(|v| json_clone(v, depth + 1, budget))
                .collect::<Result<_, _>>()?,
        )),
        BedrockValue::Object(values) => {
            let mut out = Map::new();
            for (k, v) in values.iter() {
                budget.charge(k.len())?;
                out.insert(k.to_owned(), json_clone(v, depth + 1, budget)?);
            }
            Ok(Value::Object(out))
        }
        _ => {
            charge_scalar(value, budget)?;
            Ok(value.to_json())
        }
    }
}

fn charge_scalar(value: &BedrockValue, budget: &mut ProjectionBudget) -> Result<(), ModelsError> {
    match value {
        BedrockValue::String(s) => budget.charge(s.len()),
        BedrockValue::Bytes(b) => {
            for i in 0..b.len() {
                budget.node()?;
                budget.charge(i.to_string().len() + 1)?;
            }
            Ok(())
        }
        BedrockValue::Number(_) => budget.charge(8),
        BedrockValue::Bool(_) => budget.charge(1),
        _ => Ok(()),
    }
}

pub(super) fn deserialize(value: Value, name: &str) -> Result<BedrockValue, ModelsError> {
    read(value, name, 0)
}
fn read(value: Value, name: &str, depth: usize) -> Result<BedrockValue, ModelsError> {
    if depth > 128 {
        return Err(invalid());
    }
    let shape = super::schema::shape(name).ok_or_else(invalid)?;
    match (shape, value) {
        (Shape::Object(fields, _), Value::Object(mut object)) => {
            let mut out = BedrockObject::new();
            for (k, child) in fields {
                if let Some(v) = object.shift_remove(*k).filter(|v| !v.is_null()) {
                    out.insert((*k).into(), read(v, child, depth + 1)?);
                }
            }
            if object.get("__type").and_then(Value::as_str).is_some() {
                for (k, v) in object {
                    out.insert(k, BedrockValue::from_json(v));
                }
            }
            Ok(BedrockValue::Object(out))
        }
        (Shape::List(child, _), Value::Array(values)) => Ok(BedrockValue::Array(
            values
                .into_iter()
                .map(|v| read(v, child, depth + 1))
                .collect::<Result<_, _>>()?,
        )),
        (Shape::Map(child, _), Value::Object(values)) => {
            let mut out = BedrockObject::new();
            for (k, v) in values {
                out.insert(k, read(v, child, depth + 1)?);
            }
            Ok(BedrockValue::Object(out))
        }
        (Shape::Blob, Value::String(value)) => {
            if value.len() % 4 != 0 {
                return Err(invalid());
            }
            Ok(BedrockValue::Bytes(base64_decode(&value, false)?))
        }
        (_, value) => Ok(BedrockValue::from_json(value)),
    }
}
const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
pub(super) fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3).saturating_mul(4));
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        out.push(B64[((n >> 18) & 63) as usize] as char);
        out.push(B64[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            B64[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            B64[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}
/// pi uses atob for replay/input blobs; SDK response base64 rejects whitespace.
pub(super) fn base64_decode(value: &str, atob: bool) -> Result<Vec<u8>, ModelsError> {
    let mut bytes = value
        .bytes()
        .filter(|b| !atob || !matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0c))
        .collect::<Vec<_>>();
    let full = bytes.len();
    let pads = bytes.iter().rev().take_while(|b| **b == b'=').count();
    if pads > 2 || (pads > 0 && full % 4 != 0) {
        return Err(invalid());
    }
    bytes.truncate(full - pads);
    if bytes.len() % 4 == 1 {
        return Err(invalid());
    }
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    let mut bits = 0u32;
    let mut len = 0;
    for b in bytes {
        let value = B64.iter().position(|c| *c == b).ok_or_else(invalid)? as u32;
        bits = (bits << 6) | value;
        len += 6;
        if len >= 8 {
            len -= 8;
            out.push((bits >> len) as u8);
            bits &= (1 << len) - 1;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod projection_limits {
    use super::*;
    #[test]
    fn selected_storage_and_nodes_are_admitted_before_owned_projection() {
        for (length, success) in [(1024 * 1024, true), (1024 * 1024 + 1, false)] {
            let value = BedrockValue::String("x".repeat(length));
            assert_eq!(
                json_clone(&value, 0, &mut ProjectionBudget::default()).is_ok(),
                success
            );
        }
        for (length, success) in [(65535, true), (65536, false)] {
            let value = BedrockValue::Array(vec![BedrockValue::Null; length]);
            assert_eq!(
                json_clone(&value, 0, &mut ProjectionBudget::default()).is_ok(),
                success
            );
        }
        for (length, success) in [(786432, true), (786433, false)] {
            let value = BedrockValue::Bytes(vec![0; length]);
            assert_eq!(
                document(&value, 0, &mut ProjectionBudget::default()).is_ok(),
                success
            );
        }
        let mut ignored = BedrockObject::new();
        ignored.insert(
            "ignored".into(),
            BedrockValue::String("x".repeat(2 * 1024 * 1024)),
        );
        assert_eq!(
            serialize(&BedrockValue::Object(ignored), "ConverseStreamRequest").unwrap(),
            serde_json::json!({})
        );
        // A failed projection has only local allocation; a new projection starts with
        // the complete budget rather than inheriting a prior failed invocation.
        assert_eq!(
            serialize(
                &BedrockValue::Object(BedrockObject::new()),
                "ConverseStreamRequest"
            )
            .unwrap(),
            serde_json::json!({})
        );
    }
}
