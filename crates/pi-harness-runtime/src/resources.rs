//! Pure pinned skill formatting and prompt-template substitution.
use pi_agent_schema::Skill;
pub(crate) fn skill(skill: &Skill, additional: Option<&str>) -> String {
    let normalized = skill.file_path.trim_end_matches(['\\', '/']);
    let directory = match normalized.rfind(['\\', '/']) {
        Some(2) if normalized.as_bytes().get(1) == Some(&b':') => &normalized[..3],
        Some(index) if index > 0 => &normalized[..index],
        _ => "/",
    };
    let mut prompt = format!(
        "<skill name=\"{}\" location=\"{}\">\nReferences are relative to {}.\n\n{}\n</skill>",
        skill.name, skill.file_path, directory, skill.content
    );
    if let Some(additional) = additional.filter(|value| !value.is_empty()) {
        prompt.push_str("\n\n");
        prompt.push_str(additional);
    }
    prompt
}
fn positional(text: &str, args: &[String]) -> String {
    let mut result = String::new();
    let mut position = 0;
    while position < text.len() {
        if text.as_bytes()[position] == b'$' {
            let mut end = position + 1;
            while text.as_bytes().get(end).is_some_and(u8::is_ascii_digit) {
                end += 1;
            }
            if end > position + 1 {
                if let Some(arg) = text[position + 1..end]
                    .parse::<usize>()
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .and_then(|index| args.get(index))
                {
                    result.push_str(arg);
                }
                position = end;
                continue;
            }
        }
        let character = text[position..].chars().next().expect("nonempty suffix");
        result.push(character);
        position += character.len_utf8();
    }
    result
}
fn ranges(text: &str, args: &[String]) -> String {
    let mut result = String::new();
    let mut position = 0;
    while position < text.len() {
        if text[position..].starts_with("${@:")
            && let Some(close) = text[position + 4..].find('}')
        {
            let end = position + 4 + close;
            let expression = &text[position + 4..end];
            let parts: Vec<_> = expression.split(':').collect();
            if parts.len() <= 2
                && parts
                    .iter()
                    .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()))
            {
                let start = parts[0]
                    .parse::<usize>()
                    .unwrap_or(usize::MAX)
                    .saturating_sub(1)
                    .min(args.len());
                let end_index = if parts.len() == 2 {
                    start
                        .saturating_add(parts[1].parse::<usize>().unwrap_or(usize::MAX))
                        .min(args.len())
                } else {
                    args.len()
                };
                result.push_str(&args[start..end_index].join(" "));
                position = end + 1;
                continue;
            }
        }
        let character = text[position..].chars().next().expect("nonempty suffix");
        result.push(character);
        position += character.len_utf8();
    }
    result
}
// JavaScript String.replace with a replacement string interprets these four
// tokens, even though the matched global patterns have no capture groups.
fn replacement(text: &str, pattern: &str, value: &str) -> String {
    let mut result = String::new();
    let mut previous = 0;
    for (start, _) in text.match_indices(pattern) {
        result.push_str(&text[previous..start]);
        let end = start + pattern.len();
        let mut chars = value.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '$' {
                match chars.peek() {
                    Some('$') => {
                        chars.next();
                        result.push('$');
                        continue;
                    }
                    Some('&') => {
                        chars.next();
                        result.push_str(pattern);
                        continue;
                    }
                    Some('`') => {
                        chars.next();
                        result.push_str(&text[..start]);
                        continue;
                    }
                    Some('\'') => {
                        chars.next();
                        result.push_str(&text[end..]);
                        continue;
                    }
                    _ => (),
                }
            }
            result.push(c);
        }
        previous = end;
    }
    result.push_str(&text[previous..]);
    result
}
pub(crate) fn template(text: &str, args: &[String]) -> String {
    let result = ranges(&positional(text, args), args);
    let all = args.join(" ");
    replacement(&replacement(&result, "$ARGUMENTS", &all), "$@", &all)
}
