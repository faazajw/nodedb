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

/// Compute the BM25 upper bound score for a block.
///
/// Uses `block_max_tf` and `block_min_fieldnorm` (decoded to approximate
/// doc length) to compute the maximum possible BM25 score any document
/// in the block could achieve. Used by BMW to skip blocks that can't
/// beat the current threshold.
pub fn bm25_block_upper_bound(
    block_max_tf: u32,
    block_min_fieldnorm: u8,
    df: u32,
    total_docs: u32,
    avg_doc_len: f32,
    params: &Bm25Params,
) -> f32 {
    // Decode the minimum fieldnorm to get the shortest doc length in the block.
    // Shorter docs score higher in BM25 (less length normalization penalty).
    let min_doc_len = crate::codec::smallfloat::decode(block_min_fieldnorm).max(1);
    bm25_score(
        block_max_tf,
        df,
        min_doc_len,
        total_docs,
        avg_doc_len,
        params,
    )
}

/// Compute per-term IDF, factored out for reuse across blocks.
pub fn idf(df: u32, total_docs: u32) -> f32 {
    let df_f = df as f32;
    let n = total_docs as f32;
    ((n - df_f + 0.5) / (df_f + 0.5) + 1.0).ln()
}

/// Compute the maximum possible BM25 score for a term across ALL blocks.
///
/// Uses the global max_tf and min_fieldnorm across the entire posting list.
/// This is the term's contribution to WAND pivot selection.
pub fn term_max_score(
    global_max_tf: u32,
    global_min_fieldnorm: u8,
    df: u32,
    total_docs: u32,
    avg_doc_len: f32,
    params: &Bm25Params,
) -> f32 {
    bm25_block_upper_bound(
        global_max_tf,
        global_min_fieldnorm,
        df,
        total_docs,
        avg_doc_len,
        params,
    )
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
    fn block_upper_bound_is_upper() {
        let params = Bm25Params::default();
        // Upper bound with max_tf=5, min_fieldnorm for length 50.
        let upper = bm25_block_upper_bound(
            5,
            crate::codec::smallfloat::encode(50),
            10,
            1000,
            100.0,
            &params,
        );
        // Actual score with tf=3, doc_len=100 should be ≤ upper bound.
        let actual = bm25_score(3, 10, 100, 1000, 100.0, &params);
        assert!(upper >= actual, "upper bound {upper} < actual {actual}");
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
