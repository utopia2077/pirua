//! Decoded Chord operations, without wire interning or mutable proxy machinery.
use crate::{Error, Result, records::MAX_SAFE_INTEGER};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Value, json};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Segment {
    Key(String),
    Index(usize),
}
pub type Path = Vec<Segment>;
impl From<&str> for Segment {
    fn from(v: &str) -> Self {
        Self::Key(v.to_owned())
    }
}
impl From<usize> for Segment {
    fn from(v: usize) -> Self {
        Self::Index(v)
    }
}
#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Replace(Value),
    Set(Path, Value),
    Delete(Path),
    Append(Path, String),
    Trim(Path, usize),
    Splice(Path, usize, usize, Vec<Value>),
    Permute(Path, Vec<usize>),
}
impl Serialize for Op {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        let v = match self {
            Self::Replace(v) => json!(["r", v]),
            Self::Set(p, v) => json!(["s", p, v]),
            Self::Delete(p) => json!(["d", p]),
            Self::Append(p, v) => json!(["a", p, v]),
            Self::Trim(p, n) => json!(["t", p, n]),
            Self::Splice(p, i, n, v) => json!(["p", p, i, n, v]),
            Self::Permute(p, v) => json!(["m", p, v]),
        };
        v.serialize(s)
    }
}
impl<'de> Deserialize<'de> for Op {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        Self::decode(Value::deserialize(d)?).map_err(serde::de::Error::custom)
    }
}
impl Op {
    pub fn decode(v: Value) -> Result<Self> {
        let xs = v
            .as_array()
            .ok_or_else(|| Error::validation("op must be a tuple"))?;
        let verb = xs
            .first()
            .and_then(Value::as_str)
            .ok_or_else(|| Error::validation("op verb required"))?;
        let n = match verb {
            "r" | "d" => 2,
            "s" | "a" | "t" | "m" => 3,
            "p" => 5,
            _ => return Err(Error::validation("unknown decoded op")),
        };
        if xs.len() != n {
            return Err(Error::validation("decoded op arity"));
        }
        if verb == "r" {
            return Ok(Self::Replace(xs[1].clone()));
        }
        let path: Path = serde_json::from_value(xs[1].clone())?;
        check_path(&path, !matches!(verb, "p" | "m"))?;
        let count = |v: &Value| -> Result<usize> {
            v.as_u64()
                .filter(|n| *n <= MAX_SAFE_INTEGER)
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| Error::validation("count must be a nonnegative safe integer"))
        };
        Ok(match verb {
            "s" => Self::Set(path, xs[2].clone()),
            "d" => Self::Delete(path),
            "a" => Self::Append(
                path,
                xs[2]
                    .as_str()
                    .ok_or_else(|| Error::validation("append payload must be a string"))?
                    .to_owned(),
            ),
            "t" => Self::Trim(path, count(&xs[2])?),
            "p" => Self::Splice(
                path,
                count(&xs[2])?,
                count(&xs[3])?,
                xs[4]
                    .as_array()
                    .ok_or_else(|| Error::validation("splice items must be an array"))?
                    .clone(),
            ),
            "m" => {
                let p = xs[2]
                    .as_array()
                    .ok_or_else(|| Error::validation("permutation must be an array"))?
                    .iter()
                    .map(count)
                    .collect::<Result<Vec<_>>>()?;
                check_permutation(&p)?;
                Self::Permute(path, p)
            }
            _ => unreachable!(),
        })
    }
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Replace(_) => None,
            Self::Set(p, _)
            | Self::Delete(p)
            | Self::Append(p, _)
            | Self::Trim(p, _)
            | Self::Splice(p, ..)
            | Self::Permute(p, _) => Some(p),
        }
    }
}
fn check_path(path: &[Segment], nonempty: bool) -> Result<()> {
    if nonempty && path.is_empty() {
        return Err(Error::validation("operation cannot target root"));
    }
    for seg in path {
        match seg {
            Segment::Key(s) if matches!(s.as_str(), "__proto__" | "prototype" | "constructor") => {
                return Err(Error::validation("unsafe path segment"));
            }
            Segment::Index(n) if *n as u128 > MAX_SAFE_INTEGER as u128 => {
                return Err(Error::validation("unsafe array index"));
            }
            _ => (),
        }
    }
    Ok(())
}
fn check_permutation(p: &[usize]) -> Result<()> {
    let mut seen = vec![false; p.len()];
    for &i in p {
        if i >= p.len() || seen[i] {
            return Err(Error::validation("invalid permutation"));
        }
        seen[i] = true;
    }
    Ok(())
}
fn resolve_mut<'a>(mut value: &'a mut Value, path: &[Segment]) -> Result<&'a mut Value> {
    for seg in path {
        value = match (value, seg) {
            (Value::Object(m), Segment::Key(k)) => m.get_mut(k),
            (Value::Array(a), Segment::Index(i)) => a.get_mut(*i),
            _ => None,
        }
        .ok_or_else(|| Error::validation("unresolvable path"))?;
    }
    Ok(value)
}
fn trim_utf16(s: &str, count: usize) -> Result<String> {
    let mut units = 0;
    for (offset, ch) in s.char_indices() {
        if units == count {
            return Ok(s[offset..].to_owned());
        }
        units += ch.len_utf16();
        if units > count {
            return Err(Error::validation(
                "UTF-16 prefix cut splits a surrogate pair",
            ));
        }
    }
    Ok(String::new())
}
/// Applies a batch atomically to a detached value; failure preserves the original.
pub fn apply(value: &Value, ops: &[Op]) -> Result<Value> {
    let mut next = value.clone();
    for op in ops {
        apply_one(&mut next, op)?;
    }
    Ok(next)
}
fn apply_one(value: &mut Value, op: &Op) -> Result<()> {
    if let Op::Replace(v) = op {
        *value = v.clone();
        return Ok(());
    }
    let path = op.path().expect("non-replacement path");
    check_path(path, !matches!(op, Op::Splice(..) | Op::Permute(..)))?;
    match op {
        Op::Splice(_, index, remove, items) => {
            let target = resolve_mut(value, path)?
                .as_array_mut()
                .ok_or_else(|| Error::validation("splice target must be an array"))?;
            let start = (*index).min(target.len());
            let end = start.saturating_add(*remove).min(target.len());
            target.splice(start..end, items.clone());
        }
        Op::Permute(_, p) => {
            check_permutation(p)?;
            let target = resolve_mut(value, path)?
                .as_array_mut()
                .ok_or_else(|| Error::validation("permutation target must be an array"))?;
            if target.len() != p.len() {
                return Err(Error::validation("permutation length differs from array"));
            }
            let previous = target.clone();
            for (slot, &i) in target.iter_mut().zip(p) {
                *slot = previous[i].clone();
            }
        }
        _ => {
            let (key, parents) = path
                .split_last()
                .ok_or_else(|| Error::validation("empty non-root path"))?;
            let parent = resolve_mut(value, parents)?;
            match op {
                Op::Set(_, replacement) => match (parent, key) {
                    (Value::Object(m), Segment::Key(k)) => {
                        m.insert(k.clone(), replacement.clone());
                    }
                    (Value::Array(a), Segment::Index(i)) if *i <= a.len() => {
                        if *i == a.len() {
                            a.push(replacement.clone());
                        } else {
                            a[*i] = replacement.clone();
                        }
                    }
                    _ => return Err(Error::validation("invalid set target or sparse index")),
                },
                Op::Delete(_) => match (parent, key) {
                    (Value::Object(m), Segment::Key(k)) => {
                        m.remove(k);
                    }
                    (Value::Array(a), Segment::Index(i)) if *i < a.len() => {
                        a.remove(*i);
                    }
                    _ => return Err(Error::validation("invalid delete target")),
                },
                Op::Append(..) | Op::Trim(..) => {
                    let target = match (parent, key) {
                        (Value::Object(m), Segment::Key(k)) => m.get_mut(k),
                        (Value::Array(a), Segment::Index(i)) => a.get_mut(*i),
                        _ => None,
                    }
                    .ok_or_else(|| Error::validation("missing string target"))?;
                    let s = target
                        .as_str()
                        .ok_or_else(|| Error::validation("string target required"))?;
                    *target = Value::String(match op {
                        Op::Append(_, suffix) => format!("{s}{suffix}"),
                        Op::Trim(_, n) => trim_utf16(s, *n)?,
                        _ => unreachable!(),
                    });
                }
                _ => unreachable!(),
            }
        }
    }
    Ok(())
}

fn at<'a>(value: &'a Value, path: &[Segment]) -> Option<&'a Value> {
    let mut current = value;
    for segment in path {
        current = match (current, segment) {
            (Value::Object(m), Segment::Key(k)) => m.get(k),
            (Value::Array(a), Segment::Index(i)) => a.get(*i),
            _ => None,
        }?;
    }
    Some(current)
}
fn overlap_path(a: &[Segment], b: &[Segment]) -> bool {
    a.starts_with(b) || b.starts_with(a)
}
/// Native scoped preparation. Retains positional removals and operation barriers;
/// it never uses final document equality to erase structural changes.
pub(crate) fn prepare_mutations(base: &Value, ops: &[Op]) -> Result<Vec<Op>> {
    let mut current = base.clone();
    let mut result: Vec<Op> = Vec::new();
    for op in ops {
        let next = apply(&current, std::slice::from_ref(op))?;
        let harmless = match op {
            Op::Set(path, value) => at(&current, path) == Some(value),
            Op::Delete(path) => at(&current, path).is_none(),
            Op::Append(_, s) => s.is_empty(),
            Op::Trim(_, n) => *n == 0,
            Op::Splice(_, _, n, items) => *n == 0 && items.is_empty(),
            Op::Permute(_, p) => p.iter().enumerate().all(|(i, n)| i == *n),
            _ => false,
        };
        if harmless {
            current = next;
            continue;
        }
        let mut normalized = op.clone();
        if let Op::Splice(path, index, remove, items) = &mut normalized {
            let len = at(&current, path)
                .and_then(Value::as_array)
                .expect("validated array")
                .len();
            *index = (*index).min(len);
            *remove = (*remove).min(len - *index);
            let previous = result
                .iter()
                .rposition(|other| other.path().is_none_or(|p| overlap_path(p, path)));
            if let Some(position) = previous
                && let Op::Splice(previous_path, start, deleted, inserted) = &result[position]
                && previous_path == path
                && *index >= *start
                && index.saturating_add(*remove) <= start.saturating_add(inserted.len())
            {
                let mut merged = inserted.clone();
                let offset = *index - *start;
                merged.splice(offset..offset + *remove, items.clone());
                let replacement = Op::Splice(path.clone(), *start, *deleted, merged);
                let no_op = matches!(&replacement,Op::Splice(_,_,0,v) if v.is_empty());
                result.remove(position);
                if !no_op {
                    result.insert(position, replacement);
                }
                current = next;
                continue;
            }
        }
        result.push(normalized);
        current = next;
    }
    // Repeated scalar edits may cancel across independent paths. A structural or
    // ancestor operation is a barrier and is retained even if values compare equal.
    let mut remove = std::collections::BTreeSet::new();
    for (i, op) in result.iter().enumerate() {
        let Some(path) = op.path() else { continue };
        if !matches!(op,Op::Set(_,v) if !v.is_array()&&!v.is_object())
            && !matches!(op, Op::Append(..) | Op::Trim(..))
        {
            continue;
        }
        let mut indices = Vec::new();
        let mut safe = true;
        for (j, other) in result.iter().enumerate() {
            if let Some(other_path) = other.path() {
                if overlap_path(path, other_path) {
                    if other_path == path
                        && (matches!(other,Op::Set(_,v) if !v.is_array()&&!v.is_object())
                            || matches!(other, Op::Append(..) | Op::Trim(..)))
                    {
                        indices.push(j);
                    } else {
                        safe = false;
                        break;
                    }
                }
            } else {
                safe = false;
                break;
            }
        }
        if safe && at(base, path) == at(&current, path) {
            remove.insert(i);
            remove.extend(indices);
        }
    }
    let result = result
        .into_iter()
        .enumerate()
        .filter_map(|(i, op)| (!remove.contains(&i)).then_some(op))
        .collect::<Vec<_>>();
    debug_assert_eq!(apply(base, &result)?, current);
    Ok(result)
}
