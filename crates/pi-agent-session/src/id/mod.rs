//! Checked use of an explicitly supplied host UUIDv7 generator.
use crate::codec::{context, schema};
use pi_agent_schema::session_values::SafeUnsigned;
use pi_agent_schema::{IdGenerator, SessionError};

/// Validate canonical UUIDv7 structure and return its 48-bit timestamp.
pub fn uuid_timestamp(id: &str) -> Result<SafeUnsigned, SessionError> {
    let bytes = id.as_bytes();
    if bytes.len() != 36
        || [8, 13, 18, 23].iter().any(|i| bytes[*i] != b'-')
        || bytes
            .iter()
            .enumerate()
            .any(|(i, b)| ![8, 13, 18, 23].contains(&i) && !b.is_ascii_hexdigit())
        || bytes[14] != b'7'
        || !matches!(bytes[19], b'8' | b'9' | b'a' | b'A' | b'b' | b'B')
    {
        return Err(schema());
    }
    let prefix = id[..8].to_owned() + &id[9..13];
    let timestamp = u64::from_str_radix(&prefix, 16).map_err(|_| schema())?;
    SafeUnsigned::new(timestamp).map_err(|_| schema())
}
/// Mint an ID and enforce the host's UUIDv7/follower timestamp contract.
/// Fresh entropy remains the explicitly supplied generator's responsibility.
pub fn mint(
    generator: &dyn IdGenerator,
    timestamp: Option<SafeUnsigned>,
) -> Result<String, SessionError> {
    if timestamp.is_some_and(|value| value.get() > 0xffff_ffff_ffff) {
        return Err(SessionError::Schema(context(
            "follower timestamp exceeds UUIDv7 range",
        )));
    }
    let id = generator.next(timestamp);
    let actual = uuid_timestamp(&id)?;
    if timestamp.is_some_and(|expected| expected != actual) {
        return Err(schema());
    }
    Ok(id)
}
