//! Multi-source query: merge postings from memtable + all segments,
//! then feed the merged stream to BMW or exhaustive scorer.

use crate::backend::FtsBackend;
use crate::block::{CompactPosting, into_blocks};
use crate::search::bmw::skip_index::TermBlocks;

use super::memtable::Memtable;
use super::merge::merge_term_postings;
use super::segment::reader::SegmentReader;

/// Collect posting lists for a set of query tokens by merging across
/// the active memtable and all immutable segments.
///
/// Returns per-term `TermBlocks` ready for BMW scoring.
pub fn collect_merged_term_blocks<B: FtsBackend>(
    backend: &B,
    collection: &str,
    memtable: &Memtable,
    query_tokens: &[String],
) -> Result<Vec<TermBlocks>, B::Error> {
    // Load all segments for this collection.
    let seg_keys = backend.list_segments(collection)?;
    let mut readers: Vec<SegmentReader> = Vec::new();
    for key in &seg_keys {
        if let Some(data) = backend.read_segment(key)?
            && let Some(reader) = SegmentReader::open(data)
        {
            readers.push(reader);
        }
    }

    let mut term_blocks_list = Vec::with_capacity(query_tokens.len());

    for token in query_tokens {
        // Get memtable postings for this term (scoped by collection).
        let scoped_term = format!("{collection}:{token}");
        let mt_postings = memtable.get_postings(&scoped_term);

        // Get segment postings for this term.
        let seg_postings: Vec<Vec<CompactPosting>> = readers
            .iter()
            .map(|reader| {
                let blocks = reader.read_postings(token);
                // Decompress blocks back to CompactPostings.
                let mut postings = Vec::new();
                for block in blocks {
                    for i in 0..block.doc_ids.len() {
                        postings.push(CompactPosting {
                            doc_id: block.doc_ids[i],
                            term_freq: block.term_freqs[i],
                            fieldnorm: block.fieldnorms[i],
                            positions: block.positions[i].clone(),
                        });
                    }
                }
                postings
            })
            .collect();

        // Merge memtable + segments.
        let merged = merge_term_postings(&mt_postings, &seg_postings);
        if merged.is_empty() {
            term_blocks_list.push(TermBlocks::from_blocks(Vec::new()));
            continue;
        }

        let blocks = into_blocks(merged);
        term_blocks_list.push(TermBlocks::from_blocks(blocks));
    }

    Ok(term_blocks_list)
}

/// Collect all unique term names across memtable + segments for a collection.
///
/// Used by fuzzy matching to scan available terms.
pub fn collect_all_terms<B: FtsBackend>(
    backend: &B,
    collection: &str,
    memtable: &Memtable,
) -> Result<Vec<String>, B::Error> {
    let prefix = format!("{collection}:");
    let mut terms: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Memtable terms (scoped as "{collection}:{term}").
    for key in memtable.terms() {
        if let Some(term) = key.strip_prefix(&prefix) {
            terms.insert(term.to_string());
        }
    }

    // Segment terms.
    let seg_keys = backend.list_segments(collection)?;
    for key in &seg_keys {
        if let Some(data) = backend.read_segment(key)?
            && let Some(reader) = SegmentReader::open(data)
        {
            for term in reader.terms() {
                terms.insert(term);
            }
        }
    }

    Ok(terms.into_iter().collect())
}

/// Compute merged corpus stats from memtable + all segments.
///
/// For now, uses the backend's incremental stats (which track all indexed docs).
/// The memtable stats are a subset of the backend stats (memtable docs were
/// also recorded in the backend).
pub fn merged_collection_stats<B: FtsBackend>(
    backend: &B,
    collection: &str,
) -> Result<(u32, f32), B::Error> {
    let (count, total) = backend.collection_stats(collection)?;
    let avg = if count > 0 {
        total as f32 / count as f32
    } else {
        1.0
    };
    Ok((count, avg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::memory::MemoryBackend;
    use crate::codec::smallfloat;
    use crate::lsm::memtable::{Memtable, MemtableConfig};
    use crate::lsm::segment::writer;
    use std::collections::HashMap;

    fn cp(doc_id: u32, tf: u32) -> CompactPosting {
        CompactPosting {
            doc_id,
            term_freq: tf,
            fieldnorm: smallfloat::encode(100),
            positions: vec![0],
        }
    }

    #[test]
    fn memtable_only() {
        let backend = MemoryBackend::new();
        let mt = Memtable::new(MemtableConfig::default());
        // Use scoped terms: "{collection}:{term}" as FtsIndex does.
        mt.insert("col:hello", cp(0, 2));
        mt.insert("col:hello", cp(1, 1));

        let tokens = vec!["hello".to_string()];
        let term_blocks = collect_merged_term_blocks(&backend, "col", &mt, &tokens).unwrap();

        assert_eq!(term_blocks.len(), 1);
        assert_eq!(term_blocks[0].df, 2);
    }

    #[test]
    fn segment_only() {
        let backend = MemoryBackend::new();
        // Write a segment to the backend.
        let mut postings = HashMap::new();
        postings.insert("hello".to_string(), vec![cp(0, 1), cp(5, 2)]);
        let seg_bytes = writer::flush_to_segment(postings);
        backend
            .write_segment("col:seg:L0:0000000000000001", &seg_bytes)
            .unwrap();

        let mt = Memtable::new(MemtableConfig::default());
        let tokens = vec!["hello".to_string()];
        let term_blocks = collect_merged_term_blocks(&backend, "col", &mt, &tokens).unwrap();

        assert_eq!(term_blocks.len(), 1);
        assert_eq!(term_blocks[0].df, 2);
    }

    #[test]
    fn memtable_plus_segment_merge() {
        let backend = MemoryBackend::new();

        // Segment has docs 0, 5.
        let mut seg_postings = HashMap::new();
        seg_postings.insert("hello".to_string(), vec![cp(0, 1), cp(5, 2)]);
        let seg_bytes = writer::flush_to_segment(seg_postings);
        backend
            .write_segment("col:seg:L0:0000000000000001", &seg_bytes)
            .unwrap();

        // Memtable has docs 0 (updated tf=10) and 3 (new). Scoped terms.
        let mt = Memtable::new(MemtableConfig::default());
        mt.insert("col:hello", cp(0, 10)); // Updated from segment's tf=1.
        mt.insert("col:hello", cp(3, 1)); // New doc.

        let tokens = vec!["hello".to_string()];
        let term_blocks = collect_merged_term_blocks(&backend, "col", &mt, &tokens).unwrap();

        assert_eq!(term_blocks.len(), 1);
        // Should have 3 unique docs: 0 (memtable version), 3, 5.
        assert_eq!(term_blocks[0].df, 3);
    }

    #[test]
    fn missing_term() {
        let backend = MemoryBackend::new();
        let mt = Memtable::new(MemtableConfig::default());

        let tokens = vec!["nonexistent".to_string()];
        let term_blocks = collect_merged_term_blocks(&backend, "col", &mt, &tokens).unwrap();

        assert_eq!(term_blocks.len(), 1);
        assert_eq!(term_blocks[0].df, 0);
    }
}
