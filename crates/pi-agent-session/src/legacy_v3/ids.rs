use super::LegacyError;
use pi_agent_schema::{SessionId, session_values::SafeUnsigned};
use ring::digest::{Context, SHA256};

/// Domain byte in the frozen, length-framed deterministic UUID calculation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemintDomain {
    /// Retained entry; domain byte zero.
    Entry,
    /// Virtual aggregate row; domain byte one and empty old ID.
    Aggregate,
}
/// Explicit alternate remint policy for tests or a trusted custom import host.
/// Returned IDs still undergo format, timestamp and whole-import uniqueness checks.
pub trait LegacyIdReminter: Send + Sync {
    /// Remint a single domain identity at its exact legacy timestamp.
    fn remint(
        &self,
        header_id: &SessionId,
        domain: RemintDomain,
        old_id: &str,
        timestamp: SafeUnsigned,
    ) -> Result<String, LegacyError>;
}
/// Frozen SHA256 deterministic remint policy, without path/content/position inputs.
pub struct Sha256Reminter;
impl LegacyIdReminter for Sha256Reminter {
    fn remint(
        &self,
        header_id: &SessionId,
        domain: RemintDomain,
        old_id: &str,
        timestamp: SafeUnsigned,
    ) -> Result<String, LegacyError> {
        deterministic_id(header_id, domain, old_id, timestamp)
    }
}
/// Compute the frozen SHA256/UUIDv7 mapping; aggregate IDs use an empty old ID.
pub fn deterministic_id(
    header_id: &SessionId,
    domain: RemintDomain,
    old_id: &str,
    timestamp: SafeUnsigned,
) -> Result<String, LegacyError> {
    if timestamp.get() > 0xffff_ffff_ffff
        || (domain == RemintDomain::Aggregate && !old_id.is_empty())
    {
        return Err(LegacyError::Identity);
    }
    let header_length =
        u64::try_from(header_id.as_str().len()).map_err(|_| LegacyError::Identity)?;
    let old_length = u64::try_from(old_id.len()).map_err(|_| LegacyError::Identity)?;
    let mut hash = Context::new(&SHA256);
    hash.update(b"pi-harness/legacy-v3-id\0v1\0");
    hash.update(&header_length.to_be_bytes());
    hash.update(header_id.as_str().as_bytes());
    hash.update(&[match domain {
        RemintDomain::Entry => 0,
        RemintDomain::Aggregate => 1,
    }]);
    hash.update(&old_length.to_be_bytes());
    hash.update(old_id.as_bytes());
    let digest = hash.finish();
    let digest = digest.as_ref();
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&timestamp.get().to_be_bytes()[2..]);
    // Stream the first 74 digest bits into rand_a's 12 and rand_b's 62 bits.
    for bit in 0..74 {
        let target = if bit < 12 { 52 + bit } else { 66 + bit - 12 };
        bytes[target / 8] |= ((digest[bit / 8] >> (7 - bit % 8)) & 1) << (7 - target % 8);
    }
    bytes[6] |= 0x70;
    bytes[8] |= 0x80;
    let mut id = String::with_capacity(36);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (index, byte) in bytes.into_iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            id.push('-');
        }
        id.push(char::from(HEX[usize::from(byte >> 4)]));
        id.push(char::from(HEX[usize::from(byte & 15)]));
    }
    Ok(id)
}
