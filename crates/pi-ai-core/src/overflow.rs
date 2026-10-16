//! Context-overflow and recoverable-length classification.

use std::fmt;
use std::sync::LazyLock;

use regex::{Regex, RegexBuilder, RegexSet, RegexSetBuilder};

use crate::{AssistantMessage, SafeUnsigned, StopReason};

const ECMASCRIPT_WHITESPACE_BODY: &str = r"\x09-\x0D\x20\u{00A0}\u{1680}\u{2000}-\u{200A}\u{2028}\u{2029}\u{202F}\u{205F}\u{3000}\u{FEFF}";
const LEGACY_DOT_OPTIONAL: &str =
    r"[\x00-\x09\x0B\x0C\x0E-\u{2027}\u{202A}-\u{D7FF}\u{E000}-\u{FFFF}]?";
const LEGACY_DOT_STAR: &str = r"[^\n\r\u{2028}\u{2029}]*";

pub(crate) const OVERFLOW_PATTERN_SOURCES: [&str; 25] = [
    "prompt is too long",
    "request_too_large",
    "input is too long for requested model",
    "exceeds the context window",
    r"exceeds (?:the )?(?:model'?s )?maximum context length(?: of [\d,]+ tokens?|\s*\([\d,]+\))",
    "input token count.*exceeds the maximum",
    r"maximum prompt length is \d+",
    "reduce the length of the messages",
    r"maximum context length is \d+ tokens",
    r"exceeds (?:the )?maximum allowed input length of [\d,]+ tokens?",
    r"input \(\d+ tokens\) is longer than the model'?s context length \(\d+ tokens\)",
    r"exceeds the limit of \d+",
    "exceeds the available context size",
    "greater than the context length",
    "context window exceeds limit",
    "exceeded model token limit",
    r"too large for model with \d+ maximum context length",
    r"prompt has [\d,]+ tokens?, but the configured context size is [\d,]+ tokens?",
    "model_context_window_exceeded",
    "prompt too long; exceeded (?:max )?context length",
    "range of input length should be",
    r"context[_ ]length[_ ]exceeded",
    "too many tokens",
    "token limit exceeded",
    r"^4(?:00|13)\s*(?:status code)?\s*\(no body\)",
];

const NON_OVERFLOW_PATTERN_SOURCES: [&str; 3] = [
    r"^(Throttling error|Service unavailable):",
    "rate limit",
    "too many requests",
];

static OVERFLOW_PATTERNS: LazyLock<RegexSet> =
    LazyLock::new(|| compile_legacy_regex_set(&OVERFLOW_PATTERN_SOURCES));
static NON_OVERFLOW_PATTERNS: LazyLock<RegexSet> =
    LazyLock::new(|| compile_legacy_regex_set(&NON_OVERFLOW_PATTERN_SOURCES));

/// One opaque compiled upstream overflow pattern.
///
/// Pattern source and regex implementation details are intentionally not part
/// of the public or extension-protocol surface.
///
/// ```compile_fail
/// fn requires_serialize<T: serde::Serialize>() {}
/// requires_serialize::<pi_ai_core::OverflowPattern>();
/// ```
pub struct OverflowPattern {
    compiled: Regex,
}

impl OverflowPattern {
    /// Returns whether this provider pattern matches a message.
    pub fn is_match(&self, message: &str) -> bool {
        self.compiled.is_match(message)
    }
}

impl fmt::Debug for OverflowPattern {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("OverflowPattern")
    }
}

/// Returns fresh opaque values for the 25 pinned overflow patterns.
pub fn get_overflow_patterns() -> Vec<OverflowPattern> {
    OVERFLOW_PATTERN_SOURCES
        .iter()
        .map(|source| OverflowPattern {
            compiled: compile_legacy_regex(source),
        })
        .collect()
}

/// Returns whether an assistant message represents context overflow.
pub fn is_context_overflow(
    message: &AssistantMessage,
    context_window: Option<SafeUnsigned>,
) -> bool {
    if message.stop_reason == StopReason::Error
        && let Some(error_message) = message.error_message.as_deref()
        && !error_message.is_empty()
        && !NON_OVERFLOW_PATTERNS.is_match(error_message)
        && OVERFLOW_PATTERNS.is_match(error_message)
    {
        return true;
    }

    let Some(context_window) = context_window.filter(|value| value.get() != 0) else {
        return false;
    };
    let input_tokens = message.usage.input.get() as f64 + message.usage.cache_read.get() as f64;
    let context_window = context_window.get() as f64;

    if message.stop_reason == StopReason::Stop && input_tokens > context_window {
        return true;
    }
    message.stop_reason == StopReason::Length
        && message.usage.output.get() == 0
        && input_tokens >= context_window * 0.99
}

/// Returns whether a length stop ended below the intended output limit.
pub fn is_recoverable_length(message: &AssistantMessage, desired_max_output: SafeUnsigned) -> bool {
    message.stop_reason == StopReason::Length
        && desired_max_output.get() > 0
        && message.usage.output < desired_max_output
}

pub(crate) fn compile_legacy_regex_set(sources: &[&str]) -> RegexSet {
    let translated = sources
        .iter()
        .map(|source| translate_legacy_regex(source))
        .collect::<Result<Vec<_>, ()>>();
    let Ok(translated) = translated else {
        panic!("frozen JavaScript pattern is outside the audited subset");
    };
    let mut builder = RegexSetBuilder::new(translated);
    builder
        .case_insensitive(false)
        .multi_line(false)
        .dot_matches_new_line(false)
        .unicode(true);
    builder
        .build()
        .unwrap_or_else(|_| panic!("translated classifier pattern must compile"))
}

fn compile_legacy_regex(source: &str) -> Regex {
    let translated = translate_legacy_regex(source)
        .unwrap_or_else(|()| panic!("frozen JavaScript pattern is outside the audited subset"));
    let mut builder = RegexBuilder::new(&translated);
    builder
        .case_insensitive(false)
        .multi_line(false)
        .dot_matches_new_line(false)
        .unicode(true);
    builder
        .build()
        .unwrap_or_else(|_| panic!("translated classifier pattern must compile"))
}

pub(crate) fn translate_legacy_regex(source: &str) -> Result<String, ()> {
    let mut translated = String::with_capacity(source.len() * 2);
    let mut characters = source.chars().peekable();
    let mut in_class = false;

    while let Some(character) = characters.next() {
        match character {
            '[' if !in_class => {
                in_class = true;
                translated.push('[');
            }
            '[' => return Err(()),
            ']' if in_class => {
                in_class = false;
                translated.push(']');
            }
            ']' => return Err(()),
            '\\' => {
                let escaped = characters.next().ok_or(())?;
                match escaped {
                    'd' if in_class => translated.push_str("0-9"),
                    'd' => translated.push_str("[0-9]"),
                    's' if in_class => translated.push_str(ECMASCRIPT_WHITESPACE_BODY),
                    's' => {
                        translated.push('[');
                        translated.push_str(ECMASCRIPT_WHITESPACE_BODY);
                        translated.push(']');
                    }
                    'S' if in_class => return Err(()),
                    'S' => {
                        translated.push_str("[^");
                        translated.push_str(ECMASCRIPT_WHITESPACE_BODY);
                        translated.push(']');
                    }
                    '(' | ')' => {
                        translated.push('\\');
                        translated.push(escaped);
                    }
                    _ => return Err(()),
                }
            }
            '.' if !in_class => match characters.next() {
                Some('?') => translated.push_str(LEGACY_DOT_OPTIONAL),
                Some('*') => translated.push_str(LEGACY_DOT_STAR),
                _ => return Err(()),
            },
            '(' if !in_class && characters.peek() == Some(&'?') => {
                characters.next();
                if characters.next() != Some(':') {
                    return Err(());
                }
                translated.push_str("(?:");
            }
            '{' | '}' if !in_class => return Err(()),
            character if !in_class && character.is_ascii_alphabetic() => {
                translated.push('[');
                translated.push(character.to_ascii_lowercase());
                translated.push(character.to_ascii_uppercase());
                translated.push(']');
            }
            character if in_class && character.is_ascii_alphabetic() => return Err(()),
            character if character.is_ascii() => translated.push(character),
            _ => return Err(()),
        }
    }
    if in_class {
        return Err(());
    }
    Ok(translated)
}

#[cfg(test)]
mod tests {
    use super::{compile_legacy_regex, translate_legacy_regex};

    #[test]
    fn restricted_translator_rejects_unfrozen_constructs() {
        for pattern in [".", "[a-z]", r"\w+", "(?=x)", "a{2}", "é"] {
            assert_eq!(translate_legacy_regex(pattern), Err(()));
        }
    }

    #[test]
    fn restricted_translator_preserves_legacy_dot_boundaries() {
        let optional = compile_legacy_regex("left.?right");
        assert!(optional.is_match("leftright"));
        assert!(optional.is_match("left right"));
        assert!(!optional.is_match("left😀right"));
        for terminator in ['\n', '\r', '\u{2028}', '\u{2029}'] {
            assert!(!optional.is_match(&format!("left{terminator}right")));
        }

        let star = compile_legacy_regex("left.*right");
        assert!(star.is_match("left😀right"));
        assert!(!star.is_match("left\nright"));
    }
}
