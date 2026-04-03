//! Stop word lists for full-text search filtering.
//!
//! Currently English-only. Multi-language stop word lists (27 languages)
//! will be added as part of the multi-language support batch.

/// Sorted English stop words for binary search.
static STOP_WORDS_EN: &[&str] = &[
    "a", "about", "an", "and", "are", "as", "at", "be", "been", "but", "by", "can", "do", "for",
    "from", "had", "has", "have", "he", "her", "him", "his", "how", "if", "in", "into", "is", "it",
    "its", "just", "me", "my", "no", "not", "of", "on", "or", "our", "out", "own", "say", "she",
    "so", "some", "than", "that", "the", "their", "them", "then", "there", "these", "they", "this",
    "to", "too", "up", "us", "very", "was", "we", "were", "what", "when", "which", "who", "will",
    "with", "would", "you", "your",
];

/// Check if a word is a common English stop word.
///
/// Uses binary search on a sorted static list for O(log n) lookup.
pub fn is_stop_word_en(word: &str) -> bool {
    STOP_WORDS_EN.binary_search(&word).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_stop_words() {
        assert!(is_stop_word_en("the"));
        assert!(is_stop_word_en("and"));
        assert!(is_stop_word_en("is"));
        assert!(is_stop_word_en("a"));
    }

    #[test]
    fn non_stop_words() {
        assert!(!is_stop_word_en("database"));
        assert!(!is_stop_word_en("rust"));
        assert!(!is_stop_word_en("search"));
    }

    #[test]
    fn list_is_sorted() {
        for window in STOP_WORDS_EN.windows(2) {
            assert!(
                window[0] < window[1],
                "{} should come before {}",
                window[0],
                window[1]
            );
        }
    }
}
