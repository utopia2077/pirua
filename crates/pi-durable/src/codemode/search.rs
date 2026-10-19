//! Selected pinned Pi BM25 search (k1=1.2, b=0.75); stable ties retain input order.
use std::collections::{BTreeMap, BTreeSet};
fn tokenize(text: &str) -> Vec<String> {
    const STOP: [&str; 21] = [
        "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "in", "is", "it", "of",
        "on", "or", "that", "the", "this", "to", "with",
    ];
    let chars = text.chars().collect::<Vec<_>>();
    let mut separated = String::new();
    for (i, c) in chars.iter().copied().enumerate() {
        if i > 0
            && c.is_ascii_uppercase()
            && (chars[i - 1].is_ascii_lowercase()
                || chars[i - 1].is_ascii_digit()
                || chars[i - 1].is_ascii_uppercase()
                    && chars.get(i + 1).is_some_and(char::is_ascii_lowercase))
        {
            separated.push(' ')
        }
        separated.extend(c.to_lowercase());
    }
    separated
        .split(|c: char| !c.is_ascii_lowercase() && !c.is_ascii_digit())
        .filter(|s| !s.is_empty() && !STOP.contains(s))
        .map(|s| {
            if s.len() > 4 && s.ends_with("ies") {
                format!("{}y", &s[..s.len() - 3])
            } else if s.len() > 4
                && ["ches", "shes", "sses", "xes", "zes"]
                    .iter()
                    .any(|suffix| s.ends_with(suffix))
            {
                s[..s.len() - 2].into()
            } else if s.len() > 3 && s.ends_with('s') && !s.ends_with("ss") {
                s[..s.len() - 1].into()
            } else {
                s.into()
            }
        })
        .collect()
}
pub(super) fn rank(query: &str, documents: &[String], limit: usize) -> Vec<usize> {
    if documents.is_empty() || limit == 0 {
        return Vec::new();
    }
    let mut seen = BTreeSet::new();
    let terms = tokenize(query)
        .into_iter()
        .filter(|t| seen.insert(t.clone()))
        .collect::<Vec<_>>();
    let counts = documents
        .iter()
        .map(|d| {
            let mut map = BTreeMap::<String, usize>::new();
            for t in tokenize(d) {
                *map.entry(t).or_default() += 1;
            }
            map
        })
        .collect::<Vec<_>>();
    let lengths = counts
        .iter()
        .map(|c| c.values().sum::<usize>() as f64)
        .collect::<Vec<_>>();
    let average = lengths.iter().sum::<f64>() / documents.len() as f64;
    let average = if average == 0. { 1. } else { average };
    let mut scores = vec![0.; documents.len()];
    for term in terms {
        let frequency = counts.iter().filter(|c| c.contains_key(&term)).count() as f64;
        let idf = (1. + (documents.len() as f64 - frequency + 0.5) / (frequency + 0.5)).ln();
        for (i, c) in counts.iter().enumerate() {
            if let Some(count) = c.get(&term) {
                let count = *count as f64;
                let norm = 1.2 * (1. - 0.75 + 0.75 * lengths[i] / average);
                scores[i] += idf * count * (1.2 + 1.) / (count + norm);
            }
        }
    }
    let mut indices = (0..scores.len())
        .filter(|i| scores[*i] > 0.)
        .collect::<Vec<_>>();
    indices.sort_by(|a, b| scores[*b].total_cmp(&scores[*a]).then(a.cmp(b)));
    indices.truncate(limit);
    indices
}
