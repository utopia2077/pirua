//! OpenAI prompt-cache key projection.

/// Maximum number of Unicode scalar values in a prompt-cache key.
pub const OPENAI_PROMPT_CACHE_KEY_MAX_LENGTH: usize = 64;

/// Keeps omission and empty strings distinct and truncates by codepoint, not bytes.
/// Rust strings cannot represent JavaScript's unpaired surrogate code units.
pub fn clamp_openai_prompt_cache_key(key: Option<&str>) -> Option<&str> {
    key.map(|key| {
        let end = key
            .char_indices()
            .nth(OPENAI_PROMPT_CACHE_KEY_MAX_LENGTH)
            .map_or(key.len(), |(index, _)| index);
        &key[..end]
    })
}
