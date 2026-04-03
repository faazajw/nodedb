//! BM25 scoring function.
//!
//! Single implementation used by both Origin and Lite, ensuring identical
//! ranking across all deployment tiers.

use crate::posting::Bm25Params;

/// Compute BM25 score for a single term in a single document.
///
/// # Arguments
/// * `tf` — term frequency in the document
/// * `df` — document frequency (number of documents containing the term)
/// * `doc_len` — number of tokens in the document
/// * `total_docs` — total number of documents in the collection
/// * `avg_doc_len` — average document length across the collection
/// * `params` — BM25 k1 and b parameters
pub fn bm25_score(
    tf: u32,
    df: u32,
    doc_len: u32,
    total_docs: u32,
    avg_doc_len: f32,
    params: &Bm25Params,
) -> f32 {
    let tf_f = tf as f32;
    let df_f = df as f32;
    let n = total_docs as f32;
    let dl = doc_len as f32;

    // IDF: log((N - df + 0.5) / (df + 0.5) + 1)
    let idf = ((n - df_f + 0.5) / (df_f + 0.5) + 1.0).ln();

    // TF normalization: (tf * (k1 + 1)) / (tf + k1 * (1 - b + b * dl / avgdl))
    let tf_norm = (tf_f * (params.k1 + 1.0))
        / (tf_f + params.k1 * (1.0 - params.b + params.b * dl / avg_doc_len));

    idf * tf_norm
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bm25_basic() {
        let params = Bm25Params::default();
        let score = bm25_score(2, 5, 100, 1000, 120.0, &params);
        assert!(score > 0.0, "BM25 score should be positive");
    }

    #[test]
    fn bm25_rare_term_scores_higher() {
        let params = Bm25Params::default();
        let common = bm25_score(1, 500, 100, 1000, 100.0, &params);
        let rare = bm25_score(1, 5, 100, 1000, 100.0, &params);
        assert!(
            rare > common,
            "rare term should score higher than common term"
        );
    }

    #[test]
    fn bm25_higher_tf_scores_higher() {
        let params = Bm25Params::default();
        let low_tf = bm25_score(1, 10, 100, 1000, 100.0, &params);
        let high_tf = bm25_score(5, 10, 100, 1000, 100.0, &params);
        assert!(high_tf > low_tf, "higher TF should score higher");
    }

    #[test]
    fn bm25_shorter_doc_scores_higher() {
        let params = Bm25Params::default();
        let short = bm25_score(1, 10, 50, 1000, 100.0, &params);
        let long = bm25_score(1, 10, 200, 1000, 100.0, &params);
        assert!(short > long, "shorter doc should score higher for same TF");
    }

    #[test]
    fn bm25_tf_saturation() {
        let params = Bm25Params::default();
        let tf10 = bm25_score(10, 10, 100, 1000, 100.0, &params);
        let tf100 = bm25_score(100, 10, 100, 1000, 100.0, &params);
        // Score should increase but with diminishing returns (saturation).
        assert!(tf100 > tf10);
        assert!(
            tf100 / tf10 < 2.0,
            "TF saturation should limit score growth"
        );
    }
}
