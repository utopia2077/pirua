//! Exact/fuzzy replacements and target display/patch projection.
use unicode_normalization::UnicodeNormalization;

fn whitespace(ch: char) -> bool {
    matches!(ch, '\u{0009}'..='\u{000d}' | '\u{0020}' | '\u{00a0}' | '\u{1680}' | '\u{2000}'..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}' | '\u{feff}')
}
fn normalize_fuzzy(text: &str) -> String {
    text.nfkc()
        .collect::<String>()
        .split('\n')
        .map(|line| line.trim_end_matches(whitespace))
        .collect::<Vec<_>>()
        .join("\n")
        .chars()
        .map(|ch| match ch {
            '\u{2018}' | '\u{2019}' | '\u{201a}' | '\u{201b}' => '\'',
            '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}' => '"',
            '\u{2010}'..='\u{2015}' | '\u{2212}' => '-',
            '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            ch => ch,
        })
        .collect()
}
struct Replacement {
    index: usize,
    length: usize,
    text: String,
    edit_index: usize,
}
fn replace(content: &str, replacements: &[Replacement], offset: usize) -> String {
    let mut result = content.to_owned();
    for replacement in replacements.iter().rev() {
        let start = replacement.index - offset;
        result.replace_range(start..start + replacement.length, &replacement.text);
    }
    result
}

pub(super) fn apply_edits(
    content: &str,
    edits: &[(String, String)],
    path: &str,
) -> Result<(String, String), String> {
    let edits: Vec<_> = edits
        .iter()
        .map(|(old, new)| {
            (
                old.replace("\r\n", "\n").replace('\r', "\n"),
                new.replace("\r\n", "\n").replace('\r', "\n"),
            )
        })
        .collect();
    let single = edits.len() == 1;
    for (index, (old, _)) in edits.iter().enumerate() {
        if old.is_empty() {
            return Err(if single {
                format!("oldText must not be empty in {path}.")
            } else {
                format!("edits[{index}].oldText must not be empty in {path}.")
            });
        }
    }
    let fuzzy_content = normalize_fuzzy(content);
    let fuzzy = edits
        .iter()
        .any(|(old, _)| !content.contains(old) && fuzzy_content.contains(&normalize_fuzzy(old)));
    let base = if fuzzy {
        fuzzy_content.as_str()
    } else {
        content
    };
    let mut replacements = Vec::new();
    for (index, (old, new)) in edits.iter().enumerate() {
        let fuzzy_old = normalize_fuzzy(old);
        let (position, length) = if let Some(position) = base.find(old) {
            (position, old.len())
        } else if let Some(position) = normalize_fuzzy(base).find(&fuzzy_old) {
            (position, fuzzy_old.len())
        } else {
            return Err(if single {
                format!(
                    "Could not find the exact text in {path}. The old text must match exactly including all whitespace and newlines."
                )
            } else {
                format!(
                    "Could not find edits[{index}] in {path}. The oldText must match exactly including all whitespace and newlines."
                )
            });
        };
        let occurrences = if fuzzy_old.is_empty() {
            normalize_fuzzy(base).chars().count().saturating_sub(1)
        } else {
            normalize_fuzzy(base).matches(&fuzzy_old).count()
        };
        if occurrences > 1 {
            return Err(if single {
                format!(
                    "Found {occurrences} occurrences of the text in {path}. The text must be unique. Please provide more context to make it unique."
                )
            } else {
                format!(
                    "Found {occurrences} occurrences of edits[{index}] in {path}. Each oldText must be unique. Please provide more context to make it unique."
                )
            });
        }
        replacements.push(Replacement {
            index: position,
            length,
            text: new.clone(),
            edit_index: index,
        });
    }
    replacements.sort_by_key(|replacement| replacement.index);
    for pair in replacements.windows(2) {
        if pair[0].index + pair[0].length > pair[1].index {
            return Err(format!(
                "edits[{}] and edits[{}] overlap in {path}. Merge them into one edit or target disjoint regions.",
                pair[0].edit_index, pair[1].edit_index
            ));
        }
    }
    let result = if fuzzy {
        let original_lines: Vec<_> = content.split_inclusive('\n').collect();
        let base_lines: Vec<_> = base.split_inclusive('\n').collect();
        let mut spans = Vec::new();
        let mut offset = 0;
        for line in &base_lines {
            spans.push((offset, offset + line.len()));
            offset += line.len();
        }
        let mut groups: Vec<(usize, usize, Vec<Replacement>)> = Vec::new();
        for replacement in replacements {
            let start = spans
                .iter()
                .position(|(start, end)| replacement.index >= *start && replacement.index < *end)
                .ok_or("Replacement range is outside the base content.")?;
            let end = spans
                .iter()
                .position(|(_, end)| *end >= replacement.index + replacement.length)
                .map(|index| index + 1)
                .ok_or("Replacement range is outside the base content.")?;
            if let Some(group) = groups.last_mut().filter(|group| start < group.1) {
                group.1 = group.1.max(end);
                group.2.push(replacement);
            } else {
                groups.push((start, end, vec![replacement]));
            }
        }
        if original_lines.len() != base_lines.len() {
            return Err("Cannot preserve unchanged lines because the base content has a different line count.".into());
        }
        let mut result = String::new();
        let mut original_index = 0;
        for (start, end, replacements) in groups {
            result.push_str(&original_lines[original_index..start].concat());
            result.push_str(&replace(
                &base[spans[start].0..spans[end - 1].1],
                &replacements,
                spans[start].0,
            ));
            original_index = end;
        }
        result.push_str(&original_lines[original_index..].concat());
        result
    } else {
        replace(base, &replacements, 0)
    };
    if result == content {
        return Err(if single {
            format!(
                "No changes made to {path}. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected."
            )
        } else {
            format!("No changes made to {path}. The replacements produced identical content.")
        });
    }
    Ok((content.into(), result))
}

pub(super) fn generate_diff(path: &str, old: &str, new: &str) -> (String, String, Option<usize>) {
    use similar::{ChangeTag, TextDiff};
    let diff = TextDiff::from_lines(old, new);
    let patch = diff
        .unified_diff()
        .context_radius(4)
        .header(path, path)
        .to_string();
    let mut parts: Vec<(ChangeTag, Vec<String>)> = Vec::new();
    for change in diff.iter_all_changes() {
        let line = change
            .value()
            .strip_suffix('\n')
            .unwrap_or(change.value())
            .to_owned();
        if let Some(part) = parts.last_mut().filter(|part| part.0 == change.tag()) {
            part.1.push(line);
        } else {
            parts.push((change.tag(), vec![line]));
        }
    }
    let width = old
        .split('\n')
        .count()
        .max(new.split('\n').count())
        .to_string()
        .len();
    let mut old_line = 1usize;
    let mut new_line = 1usize;
    let mut output = Vec::new();
    let mut first = None;
    for (index, (tag, lines)) in parts.iter().enumerate() {
        if *tag != ChangeTag::Equal {
            first.get_or_insert(new_line);
            for line in lines {
                if *tag == ChangeTag::Delete {
                    output.push(format!("-{:>width$} {line}", old_line));
                    old_line += 1;
                } else {
                    output.push(format!("+{:>width$} {line}", new_line));
                    new_line += 1;
                }
            }
            continue;
        }
        let before = index > 0 && parts[index - 1].0 != ChangeTag::Equal;
        let after = index + 1 < parts.len() && parts[index + 1].0 != ChangeTag::Equal;
        let selected: Vec<_> = if before && after {
            if lines.len() <= 8 {
                (0..lines.len()).map(Some).collect()
            } else {
                (0..4)
                    .map(Some)
                    .chain(std::iter::once(None))
                    .chain((lines.len() - 4..lines.len()).map(Some))
                    .collect()
            }
        } else if before {
            (0..lines.len().min(4))
                .map(Some)
                .chain((lines.len() > 4).then_some(None))
                .collect()
        } else if after {
            (lines.len() > 4)
                .then_some(None)
                .into_iter()
                .chain((lines.len().saturating_sub(4)..lines.len()).map(Some))
                .collect()
        } else {
            old_line += lines.len();
            new_line += lines.len();
            continue;
        };
        let mut consumed = 0;
        for position in selected {
            if let Some(position) = position {
                let skipped = position - consumed;
                old_line += skipped;
                new_line += skipped;
                output.push(format!(" {:>width$} {}", old_line, lines[position]));
                old_line += 1;
                new_line += 1;
                consumed = position + 1;
            } else {
                output.push(format!(" {:>width$} ...", ""));
            }
        }
        old_line += lines.len() - consumed;
        new_line += lines.len() - consumed;
    }
    (output.join("\n"), patch, first)
}
