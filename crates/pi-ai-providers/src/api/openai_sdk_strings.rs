//! OpenAI 7.19.0 string projections, not general HTTP/header/env normalization.

use pi_ai_core::ProviderEnv;

pub(crate) fn trim(value: &str) -> &str {
    value.trim_matches(|c| matches!(c, '\u{9}'..='\u{d}' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}'))
}

/// SDK readEnv uses the captured ambient value, String.trim, then `|| undefined`.
/// Pi's scoped provider environment and ordinary header trimming are separate.
pub(crate) fn read_env<'a>(env: &'a ProviderEnv, name: &str) -> Option<&'a str> {
    env.as_map()
        .get(name)
        .map(|value| trim(value))
        .filter(|value| !value.is_empty())
}

/// Settle SDK `parsed = {}` before any HTTP name/value validation. Exact-key
/// replacement retains its original slot; string __proto__ assignments create
/// no own entry. This finite string record does not emulate executable objects.
pub(crate) fn custom_headers(value: &str) -> Vec<(&str, &str)> {
    let mut entries: Vec<(&str, &str)> = Vec::new();
    for line in value.split('\n') {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = trim(name);
        let value = trim(value);
        if name == "__proto__" {
            continue;
        }
        if let Some((_, previous)) = entries.iter_mut().find(|(key, _)| *key == name) {
            *previous = value;
        } else {
            entries.push((name, value));
        }
    }
    // Object.entries enumerates array-index keys before other own string keys.
    entries.sort_by_key(|(name, _)| {
        name.parse::<u32>()
            .ok()
            .filter(|index| *index != u32::MAX && index.to_string() == *name)
            .map_or((1, 0), |index| (0, index))
    });
    entries
}

#[cfg(test)]
mod tests {
    #[test]
    fn own_record_enumeration_matches_actual_sdk_parse_stage() {
        let source: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../conformance/provider-traces/openai-sdk-strings/responses-custom-record.json"
        )).unwrap();
        for case in source["cases"].as_array().unwrap() {
            let raw = case["ambient"]["OPENAI_CUSTOM_HEADERS"].as_str().unwrap();
            assert_eq!(
                serde_json::json!(super::custom_headers(super::trim(raw))),
                case["parsedEntries"],
                "{}",
                case["id"]
            );
        }
    }
}
