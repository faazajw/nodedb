use crate::posting::Posting;

/// Storage backend abstraction for the full-text search engine.
///
/// Origin implements this with redb (persistent). Lite implements with
/// in-memory HashMap. All scoring, BMW, compression, and analysis logic
/// works identically over any backend.
pub trait FtsBackend {
    /// Error type for backend operations.
    type Error: std::fmt::Display;

    /// Read the posting list for a term in a collection.
    fn read_postings(&self, collection: &str, term: &str) -> Result<Vec<Posting>, Self::Error>;

    /// Write/replace the posting list for a term in a collection.
    fn write_postings(
        &mut self,
        collection: &str,
        term: &str,
        postings: &[Posting],
    ) -> Result<(), Self::Error>;

    /// Remove a term's posting list entirely.
    fn remove_postings(&mut self, collection: &str, term: &str) -> Result<(), Self::Error>;

    /// Read the document length (token count) for a document.
    fn read_doc_length(&self, collection: &str, doc_id: &str) -> Result<Option<u32>, Self::Error>;

    /// Write/replace the document length for a document.
    fn write_doc_length(
        &mut self,
        collection: &str,
        doc_id: &str,
        length: u32,
    ) -> Result<(), Self::Error>;

    /// Remove a document's length entry.
    fn remove_doc_length(&mut self, collection: &str, doc_id: &str) -> Result<(), Self::Error>;

    /// Get all term keys for a collection (for fuzzy matching).
    /// Returns terms without the collection prefix.
    fn collection_terms(&self, collection: &str) -> Result<Vec<String>, Self::Error>;

    /// Get total document count and sum of all document lengths for a collection.
    /// Returns `(doc_count, total_token_sum)`.
    fn collection_stats(&self, collection: &str) -> Result<(u32, u64), Self::Error>;

    /// Remove all entries for a collection prefix. Returns count of removed entries.
    fn purge_collection(&mut self, collection: &str) -> Result<usize, Self::Error>;
}
