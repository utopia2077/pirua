//! Fast deterministic UTF-16 short hashes.

/// Returns the pinned two-lane 32-bit hash as lowercase base-36 `h2 || h1`.
pub fn short_hash(text: &str) -> String {
    let mut h1 = 0xdead_beef_u32;
    let mut h2 = 0x41c6_ce57_u32;
    for unit in text.encode_utf16() {
        h1 = (h1 ^ u32::from(unit)).wrapping_mul(2_654_435_761);
        h2 = (h2 ^ u32::from(unit)).wrapping_mul(1_597_334_677);
    }
    h1 = (h1 ^ (h1 >> 16)).wrapping_mul(2_246_822_507)
        ^ (h2 ^ (h2 >> 13)).wrapping_mul(3_266_489_909);
    h2 = (h2 ^ (h2 >> 16)).wrapping_mul(2_246_822_507)
        ^ (h1 ^ (h1 >> 13)).wrapping_mul(3_266_489_909);
    let mut output = base36(h2);
    output.push_str(&base36(h1));
    output
}

fn base36(mut value: u32) -> String {
    const DIGITS: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if value == 0 {
        return "0".to_owned();
    }
    let mut reversed = [0_u8; 7];
    let mut start = reversed.len();
    while value != 0 {
        start -= 1;
        reversed[start] = DIGITS[(value % 36) as usize];
        value /= 36;
    }
    String::from_utf8(reversed[start..].to_vec()).expect("base-36 digits are ASCII")
}
