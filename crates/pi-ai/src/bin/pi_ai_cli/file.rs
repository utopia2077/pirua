use super::{Error, Files, Selected};
use pi_ai_core::{JsonValue, OAuthCredential};
use std::collections::BTreeMap;
pub struct AuthFile;
impl Files for AuthFile {
    fn exists(&self) -> Result<bool, Error> {
        Ok(std::path::Path::new("auth.json").exists())
    }
    fn read(&self) -> Result<String, Error> {
        std::fs::read_to_string("auth.json").map_err(|_| Error::File)
    }
    fn write(&self, text: &str) -> Result<(), Error> {
        std::fs::write("auth.json", text).map_err(|_| Error::File)
    }
}
pub fn updated(
    previous: Option<&str>,
    provider: Selected,
    credential: &OAuthCredential,
) -> Result<String, Error> {
    let mut object = BTreeMap::new();
    if let Some(text) = previous.filter(|s| syntax(s)) {
        match serde_json::from_str::<JsonValue>(text).map_err(|_| Error::InvalidAuthFile)? {
            JsonValue::Object(value) => object = value.into_map(),
            _ => return Err(Error::InvalidAuthFile),
        }
    }
    let credential = serde_json::to_string(credential).map_err(|_| Error::File)?;
    object.insert(
        provider.id().to_owned(),
        serde_json::from_str::<JsonValue>(&credential).map_err(|_| Error::File)?,
    );
    serde_json::to_string_pretty(&object).map_err(|_| Error::File)
}
// Syntax only: no number conversion, string decoding or recursion limit. In particular,
// valid duplicate keys, huge numbers, lone escaped surrogates and deeply nested values
// reach strict JsonValue admission, never the malformed-file fallback.
#[derive(Clone, Copy)]
enum Expect {
    Value,
    ArrayFirst,
    ArrayNext,
    ObjectFirst,
    ObjectKey,
    ObjectColon,
    ObjectNext,
    End,
}
pub fn syntax(text: &str) -> bool {
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut stack = vec![Expect::End, Expect::Value];
    while let Some(expected) = stack.pop() {
        while bytes
            .get(i)
            .is_some_and(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
        {
            i += 1;
        }
        match expected {
            Expect::End => return i == bytes.len(),
            Expect::ArrayFirst => {
                if bytes.get(i) == Some(&b']') {
                    i += 1;
                } else {
                    stack.extend([Expect::ArrayNext, Expect::Value]);
                }
            }
            Expect::ArrayNext => match bytes.get(i) {
                Some(b']') => i += 1,
                Some(b',') => {
                    i += 1;
                    stack.extend([Expect::ArrayNext, Expect::Value]);
                }
                _ => return false,
            },
            Expect::ObjectFirst => {
                if bytes.get(i) == Some(&b'}') {
                    i += 1;
                } else {
                    stack.push(Expect::ObjectKey);
                }
            }
            Expect::ObjectKey => {
                if !string(bytes, &mut i) {
                    return false;
                }
                stack.push(Expect::ObjectColon);
            }
            Expect::ObjectColon => {
                if bytes.get(i) != Some(&b':') {
                    return false;
                }
                i += 1;
                stack.extend([Expect::ObjectNext, Expect::Value]);
            }
            Expect::ObjectNext => match bytes.get(i) {
                Some(b'}') => i += 1,
                Some(b',') => {
                    i += 1;
                    stack.push(Expect::ObjectKey)
                }
                _ => return false,
            },
            Expect::Value => match bytes.get(i) {
                Some(b'{') => {
                    i += 1;
                    stack.push(Expect::ObjectFirst)
                }
                Some(b'[') => {
                    i += 1;
                    stack.push(Expect::ArrayFirst)
                }
                Some(b'"') => {
                    if !string(bytes, &mut i) {
                        return false;
                    }
                }
                Some(b't') => {
                    if !literal(bytes, &mut i, b"true") {
                        return false;
                    }
                }
                Some(b'f') => {
                    if !literal(bytes, &mut i, b"false") {
                        return false;
                    }
                }
                Some(b'n') => {
                    if !literal(bytes, &mut i, b"null") {
                        return false;
                    }
                }
                Some(b'-' | b'0'..=b'9') => {
                    if !number(bytes, &mut i) {
                        return false;
                    }
                }
                _ => return false,
            },
        }
    }
    false
}
fn literal(bytes: &[u8], i: &mut usize, word: &[u8]) -> bool {
    if bytes.get(*i..*i + word.len()) == Some(word) {
        *i += word.len();
        true
    } else {
        false
    }
}
fn string(bytes: &[u8], i: &mut usize) -> bool {
    if bytes.get(*i) != Some(&b'"') {
        return false;
    }
    *i += 1;
    while let Some(b) = bytes.get(*i) {
        *i += 1;
        match b {
            b'"' => return true,
            b'\\' => match bytes.get(*i) {
                Some(b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't') => *i += 1,
                Some(b'u') => {
                    *i += 1;
                    for _ in 0..4 {
                        if !bytes.get(*i).is_some_and(u8::is_ascii_hexdigit) {
                            return false;
                        }
                        *i += 1;
                    }
                }
                _ => return false,
            },
            0..=31 => return false,
            _ => {}
        }
    }
    false
}
fn digits(bytes: &[u8], i: &mut usize) -> bool {
    let start = *i;
    while bytes.get(*i).is_some_and(u8::is_ascii_digit) {
        *i += 1;
    }
    *i > start
}
fn number(bytes: &[u8], i: &mut usize) -> bool {
    if bytes.get(*i) == Some(&b'-') {
        *i += 1;
    }
    match bytes.get(*i) {
        Some(b'0') => *i += 1,
        Some(b'1'..=b'9') => {
            digits(bytes, i);
        }
        _ => return false,
    }
    if bytes.get(*i) == Some(&b'.') {
        *i += 1;
        if !digits(bytes, i) {
            return false;
        }
    }
    if bytes.get(*i).is_some_and(|b| matches!(b, b'e' | b'E')) {
        *i += 1;
        if bytes.get(*i).is_some_and(|b| matches!(b, b'+' | b'-')) {
            *i += 1;
        }
        if !digits(bytes, i) {
            return false;
        }
    }
    true
}
