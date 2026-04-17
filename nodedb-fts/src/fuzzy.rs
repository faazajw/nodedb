//! Fuzzy string matching via Levenshtein edit distance.
//!
//! Used for typo-tolerant full-text search. When an exact term match
//! isn't found in the inverted index, fuzzy matching finds terms within
//! a configurable edit distance.
//!
//! Distance thresholds are adaptive based on token length:
//! - 1-3 chars: exact only (no fuzzy — too many false positives)
//! - 4-6 chars: max distance 1
//! - 7+ chars: max distance 2

/// Compute Levenshtein edit distance between two strings, measured in
/// Unicode scalar values (chars) not bytes. Byte iteration would count a
/// single multi-byte codepoint substitution as multiple edits and bucket
/// non-ASCII queries incorrectly.
///
/// Uses the two-row DP: O(min(a,b)) space, O(a*b) time.
pub fn levenshtein(a: &str, b: &str) -> usize {
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let a_len = a_chars.len();
    let b_len = b_chars.len();

    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    // Iterate the shorter string as the inner dimension.
    let (short, long) = if a_len <= b_len {
        (&a_chars[..], &b_chars[..])
    } else {
        (&b_chars[..], &a_chars[..])
    };
    let s_len = short.len();

    let mut prev_row: Vec<usize> = (0..=s_len).collect();
    let mut curr_row: Vec<usize> = vec![0; s_len + 1];

    for (j, l_ch) in long.iter().enumerate() {
        curr_row[0] = j + 1;
        for (i, s_ch) in short.iter().enumerate() {
            let cost = if s_ch == l_ch { 0 } else { 1 };
            curr_row[i + 1] = (prev_row[i + 1] + 1)
                .min(curr_row[i] + 1)
                .min(prev_row[i] + cost);
        }
        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[s_len]
}

/// Maximum allowed edit distance for a given token length.
///
/// Short tokens get no fuzzy matching (too many false positives).
/// Longer tokens allow more edits.
pub fn max_distance_for_length(len: usize) -> usize {
    match len {
        0..=3 => 0, // Exact only — "cat" → "bat" is too different
        4..=6 => 1, // One typo — "database" → "databse"
        _ => 2,     // Two typos — "distributed" → "distrubted"
    }
}

/// Find terms in the index that fuzzy-match the query term.
///
/// Returns `(matched_term, edit_distance)` pairs for all terms within
/// the adaptive distance threshold. Results are sorted by distance
/// (closest matches first).
pub fn fuzzy_match<'a>(
    query_term: &str,
    index_terms: impl Iterator<Item = &'a str>,
) -> Vec<(&'a str, usize)> {
    let q_char_len = query_term.chars().count();
    let max_dist = max_distance_for_length(q_char_len);
    if max_dist == 0 {
        return Vec::new();
    }

    // Length-bucket prefilter: group candidates by character length, then
    // visit only buckets within the distance window. Edit distance is at
    // least |len(a) - len(b)|, so terms outside [q-d .. q+d] cannot match.
    // This bounds the number of expensive O(L²) levenshtein calls to the
    // candidates in the relevant buckets, avoiding whole-dictionary scans
    // on large term indexes.
    use std::collections::HashMap;
    let mut buckets: HashMap<usize, Vec<&'a str>> = HashMap::new();
    for term in index_terms {
        buckets.entry(term.chars().count()).or_default().push(term);
    }

    let low = q_char_len.saturating_sub(max_dist);
    let high = q_char_len.saturating_add(max_dist);
    let mut matches: Vec<(&'a str, usize)> = Vec::new();
    for len in low..=high {
        let Some(bucket) = buckets.get(&len) else {
            continue;
        };
        for term in bucket {
            let dist = levenshtein(query_term, term);
            if dist > 0 && dist <= max_dist {
                matches.push((*term, dist));
            }
        }
    }

    matches.sort_by_key(|&(_, d)| d);
    matches
}

/// Score discount for fuzzy matches based on edit distance.
///
/// Exact match = 1.0 (not handled here — caller checks exact first).
/// Distance 1 = 0.7, Distance 2 = 0.4.
pub fn fuzzy_discount(distance: usize) -> f32 {
    match distance {
        0 => 1.0,
        1 => 0.7,
        2 => 0.4,
        _ => 0.2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn levenshtein_basic() {
        assert_eq!(levenshtein("", ""), 0);
        assert_eq!(levenshtein("abc", ""), 3);
        assert_eq!(levenshtein("", "xyz"), 3);
        assert_eq!(levenshtein("kitten", "sitting"), 3);
        assert_eq!(levenshtein("saturday", "sunday"), 3);
    }

    #[test]
    fn levenshtein_same() {
        assert_eq!(levenshtein("database", "database"), 0);
    }

    #[test]
    fn levenshtein_one_edit() {
        assert_eq!(levenshtein("database", "databse"), 1);
        assert_eq!(levenshtein("index", "indx"), 1);
    }

    #[test]
    fn max_distance_thresholds() {
        assert_eq!(max_distance_for_length(2), 0);
        assert_eq!(max_distance_for_length(3), 0);
        assert_eq!(max_distance_for_length(4), 1);
        assert_eq!(max_distance_for_length(6), 1);
        assert_eq!(max_distance_for_length(7), 2);
        assert_eq!(max_distance_for_length(10), 2);
    }

    #[test]
    fn fuzzy_match_finds_typos() {
        let index = ["database", "distributed", "document", "data", "date"];
        let matches = fuzzy_match("databse", index.iter().copied());
        assert!(!matches.is_empty());
        assert_eq!(matches[0].0, "database");
        assert_eq!(matches[0].1, 1);
    }

    #[test]
    fn fuzzy_match_respects_length_threshold() {
        let index = ["cat", "bat", "car"];
        let matches = fuzzy_match("cat", index.iter().copied());
        assert!(matches.is_empty());
    }

    #[test]
    fn levenshtein_counts_unicode_codepoints_not_bytes() {
        // Spec: edit distance must be measured in characters (scalar values),
        // not UTF-8 bytes. Substituting one 2-byte codepoint is one edit.
        assert_eq!(
            levenshtein("café", "cafe"),
            1,
            "substituting é→e is one edit, not two"
        );
        assert_eq!(
            levenshtein("naïve", "naive"),
            1,
            "substituting ï→i is one edit, not two"
        );
        assert_eq!(
            levenshtein("über", "uber"),
            1,
            "substituting ü→u is one edit, not two"
        );
    }

    #[test]
    fn levenshtein_cjk_single_substitution() {
        // Spec: CJK characters are 3 bytes each in UTF-8. One-character
        // substitution in a 3-char string must be distance 1, not 3.
        assert_eq!(
            levenshtein("日本語", "日本国"),
            1,
            "one CJK substitution is one edit, not three"
        );
    }

    #[test]
    fn fuzzy_match_finds_unicode_one_edit() {
        // Spec: a single-character typo in non-ASCII input must match the
        // canonical term at distance 1 and be returned.
        let index = ["café", "database", "cafeteria"];
        let matches = fuzzy_match("cafe", index.iter().copied());
        // "café" differs from "cafe" by one character → should match at dist 1.
        // `len("cafe")` is 4 chars; `max_distance_for_length(4) == 1`.
        assert!(
            matches.iter().any(|(t, d)| *t == "café" && *d == 1),
            "expected fuzzy_match('cafe') to include ('café', 1), got {matches:?}"
        );
    }
}
