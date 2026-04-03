//! `CHUNK_TEXT(text, chunk_size, overlap, strategy)` — deterministic text splitting.
//!
//! Splits a text string into overlapping chunks using one of three strategies:
//! - `character`: split at character boundaries, respecting chunk_size and overlap
//! - `sentence`: split at sentence boundaries (`.` `!` `?` followed by whitespace)
//! - `paragraph`: split at double-newline boundaries
//!
//! All operations are UTF-8 safe (split on char boundaries, not byte boundaries).
//! Shared between Origin and Lite.

use std::fmt;

/// A single chunk produced by text splitting.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TextChunk {
    /// Zero-based chunk index in the output sequence.
    pub index: usize,
    /// Start character offset in the original text (inclusive).
    pub start: usize,
    /// End character offset in the original text (exclusive).
    pub end: usize,
    /// The chunk text content.
    pub text: String,
}

/// Chunking strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkStrategy {
    Character,
    Sentence,
    Paragraph,
}

impl ChunkStrategy {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "character" | "char" => Some(Self::Character),
            "sentence" | "sent" => Some(Self::Sentence),
            "paragraph" | "para" => Some(Self::Paragraph),
            _ => None,
        }
    }
}

impl fmt::Display for ChunkStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Character => write!(f, "character"),
            Self::Sentence => write!(f, "sentence"),
            Self::Paragraph => write!(f, "paragraph"),
        }
    }
}

/// Error returned when chunk parameters are invalid.
#[derive(Debug)]
pub enum ChunkError {
    /// chunk_size must be > 0.
    InvalidChunkSize,
    /// overlap must be < chunk_size.
    OverlapTooLarge,
}

impl fmt::Display for ChunkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidChunkSize => write!(f, "chunk_size must be greater than 0"),
            Self::OverlapTooLarge => write!(f, "overlap must be less than chunk_size"),
        }
    }
}

/// Split text into chunks using the specified strategy.
///
/// - `chunk_size`: maximum number of **characters** per chunk.
/// - `overlap`: number of characters shared between consecutive chunks.
/// - `strategy`: splitting strategy.
///
/// Returns a deterministic sequence of chunks. Same input + params → same output.
pub fn chunk_text(
    text: &str,
    chunk_size: usize,
    overlap: usize,
    strategy: ChunkStrategy,
) -> Result<Vec<TextChunk>, ChunkError> {
    if chunk_size == 0 {
        return Err(ChunkError::InvalidChunkSize);
    }
    if overlap >= chunk_size {
        return Err(ChunkError::OverlapTooLarge);
    }
    if text.is_empty() {
        return Ok(Vec::new());
    }

    match strategy {
        ChunkStrategy::Character => chunk_by_characters(text, chunk_size, overlap),
        ChunkStrategy::Sentence => chunk_by_sentences(text, chunk_size, overlap),
        ChunkStrategy::Paragraph => chunk_by_paragraphs(text, chunk_size, overlap),
    }
}

/// Character-based splitting: advance by `chunk_size - overlap` chars each step.
fn chunk_by_characters(
    text: &str,
    chunk_size: usize,
    overlap: usize,
) -> Result<Vec<TextChunk>, ChunkError> {
    let chars: Vec<char> = text.chars().collect();
    let total = chars.len();
    let step = chunk_size - overlap;
    let mut chunks = Vec::new();
    let mut pos = 0usize;
    let mut index = 0usize;

    while pos < total {
        let end = (pos + chunk_size).min(total);
        let chunk_chars = &chars[pos..end];
        let text_content: String = chunk_chars.iter().collect();

        // Compute byte offsets for start/end (character offsets, not byte).
        chunks.push(TextChunk {
            index,
            start: pos,
            end,
            text: text_content,
        });

        index += 1;
        pos += step;

        // If the remaining text is smaller than overlap, we've covered everything.
        if end == total {
            break;
        }
    }

    Ok(chunks)
}

/// Sentence-based splitting: split at sentence boundaries, respecting chunk_size as max.
///
/// Sentence boundaries: `.` `!` `?` followed by whitespace or end-of-string.
/// If a single sentence exceeds chunk_size, it falls back to character split for that piece.
fn chunk_by_sentences(
    text: &str,
    chunk_size: usize,
    overlap: usize,
) -> Result<Vec<TextChunk>, ChunkError> {
    let sentences = split_sentences(text);
    if sentences.is_empty() {
        return Ok(Vec::new());
    }

    build_chunks_from_segments(&sentences, chunk_size, overlap)
}

/// Paragraph-based splitting: split at double-newline boundaries.
///
/// If a single paragraph exceeds chunk_size, it falls back to sentence split,
/// then character split if needed.
fn chunk_by_paragraphs(
    text: &str,
    chunk_size: usize,
    overlap: usize,
) -> Result<Vec<TextChunk>, ChunkError> {
    let paragraphs = split_paragraphs(text);
    if paragraphs.is_empty() {
        return Ok(Vec::new());
    }

    build_chunks_from_segments(&paragraphs, chunk_size, overlap)
}

/// Split text into sentences at `.` `!` `?` followed by whitespace or end-of-string.
///
/// Returns `(start_char_offset, text)` pairs. Preserves all original text (no trimming).
fn split_sentences(text: &str) -> Vec<(usize, String)> {
    let chars: Vec<char> = text.chars().collect();
    let mut segments = Vec::new();
    let mut start = 0usize;

    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        let is_sentence_end = (ch == '.' || ch == '!' || ch == '?')
            && (i + 1 >= chars.len() || chars[i + 1].is_whitespace());

        if is_sentence_end {
            // Include the punctuation and any trailing whitespace.
            let mut end = i + 1;
            while end < chars.len() && chars[end].is_whitespace() && chars[end] != '\n' {
                end += 1;
            }
            let segment: String = chars[start..end].iter().collect();
            segments.push((start, segment));
            start = end;
            i = end;
        } else {
            i += 1;
        }
    }

    // Remaining text (no sentence terminator).
    if start < chars.len() {
        let segment: String = chars[start..].iter().collect();
        segments.push((start, segment));
    }

    segments
}

/// Split text into paragraphs at double-newline (`\n\n`) boundaries.
fn split_paragraphs(text: &str) -> Vec<(usize, String)> {
    let mut segments = Vec::new();
    let mut char_offset = 0usize;

    // Split on \n\n (or \r\n\r\n).
    let mut remaining = text;
    while let Some(pos) = find_paragraph_break(remaining) {
        let para = &remaining[..pos];
        let para_chars: Vec<char> = para.chars().collect();
        if !para_chars.is_empty() {
            segments.push((char_offset, para.to_string()));
        }
        char_offset += para.chars().count();

        // Skip the paragraph break characters.
        let break_str = &remaining[pos..];
        let break_len = if break_str.starts_with("\r\n\r\n") {
            4
        } else {
            2 // \n\n
        };
        let break_chars = remaining[pos..pos + break_len].chars().count();
        char_offset += break_chars;
        remaining = &remaining[pos + break_len..];
    }

    // Remaining text.
    if !remaining.is_empty() {
        segments.push((char_offset, remaining.to_string()));
    }

    segments
}

/// Find the byte position of the next paragraph break (\n\n or \r\n\r\n).
fn find_paragraph_break(text: &str) -> Option<usize> {
    // Check \r\n\r\n first (longer match).
    if let Some(pos) = text.find("\r\n\r\n") {
        let nn_pos = text.find("\n\n");
        // Return whichever comes first.
        match nn_pos {
            Some(nn) if nn < pos => Some(nn),
            _ => Some(pos),
        }
    } else {
        text.find("\n\n")
    }
}

/// Build chunks from pre-split segments, respecting chunk_size and overlap.
///
/// Segments that exceed chunk_size are split by characters as a fallback.
fn build_chunks_from_segments(
    segments: &[(usize, String)],
    chunk_size: usize,
    overlap: usize,
) -> Result<Vec<TextChunk>, ChunkError> {
    let mut chunks = Vec::new();
    let mut current_text = String::new();
    let mut current_start: Option<usize> = None;
    let mut index = 0usize;

    for (seg_offset, seg_text) in segments {
        let seg_chars = seg_text.chars().count();

        // If a single segment exceeds chunk_size, split it by characters.
        if seg_chars > chunk_size {
            // Flush current buffer first.
            if !current_text.is_empty() {
                let start = current_start.unwrap_or(0);
                let end = start + current_text.chars().count();
                chunks.push(TextChunk {
                    index,
                    start,
                    end,
                    text: std::mem::take(&mut current_text),
                });
                index += 1;
                current_start = None;
            }

            // Character-split the oversized segment.
            let sub_chunks = chunk_by_characters(seg_text, chunk_size, overlap)?;
            for sub in sub_chunks {
                chunks.push(TextChunk {
                    index,
                    start: seg_offset + sub.start,
                    end: seg_offset + sub.end,
                    text: sub.text,
                });
                index += 1;
            }
            continue;
        }

        let current_chars = current_text.chars().count();
        // Would adding this segment exceed chunk_size?
        if current_chars + seg_chars > chunk_size && !current_text.is_empty() {
            // Emit current chunk.
            let start = current_start.unwrap_or(0);
            let end = start + current_chars;
            chunks.push(TextChunk {
                index,
                start,
                end,
                text: current_text.clone(),
            });
            index += 1;

            // Apply overlap: keep the last `overlap` characters.
            if overlap > 0 && current_chars > overlap {
                let chars: Vec<char> = current_text.chars().collect();
                let overlap_chars = &chars[current_chars - overlap..];
                current_text = overlap_chars.iter().collect();
                current_start = Some(end - overlap);
            } else {
                current_text.clear();
                current_start = None;
            }
        }

        if current_start.is_none() {
            current_start = Some(*seg_offset);
        }
        current_text.push_str(seg_text);
    }

    // Emit any remaining text.
    if !current_text.is_empty() {
        let start = current_start.unwrap_or(0);
        let end = start + current_text.chars().count();
        chunks.push(TextChunk {
            index,
            start,
            end,
            text: current_text,
        });
    }

    Ok(chunks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn character_basic() {
        let text = "Hello, World! This is a test.";
        let chunks = chunk_text(text, 10, 0, ChunkStrategy::Character).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].text, "Hello, Wor");
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[0].end, 10);
        assert_eq!(chunks[1].text, "ld! This i");
        assert_eq!(chunks[2].text, "s a test.");
    }

    #[test]
    fn character_with_overlap() {
        let text = "abcdefghijklmnop";
        let chunks = chunk_text(text, 8, 3, ChunkStrategy::Character).unwrap();
        // step = 8 - 3 = 5
        // chunk 0: [0..8] = "abcdefgh"
        // chunk 1: [5..13] = "fghijklm"
        // chunk 2: [10..16] = "klmnop"
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].text, "abcdefgh");
        assert_eq!(chunks[1].text, "fghijklm");
        assert_eq!(chunks[1].start, 5);
        assert_eq!(chunks[2].text, "klmnop");
    }

    #[test]
    fn sentence_basic() {
        let text = "First sentence. Second sentence. Third sentence.";
        let chunks = chunk_text(text, 20, 0, ChunkStrategy::Sentence).unwrap();
        // "First sentence. " = 17 chars
        // "Second sentence. " = 18 chars — doesn't fit with first (35), standalone
        // "Third sentence." = 15 chars
        assert!(chunks.len() >= 2);
        assert!(chunks[0].text.contains("First"));
    }

    #[test]
    fn paragraph_basic() {
        let text = "Paragraph one.\n\nParagraph two.\n\nParagraph three.";
        let chunks = chunk_text(text, 20, 0, ChunkStrategy::Paragraph).unwrap();
        assert!(chunks.len() >= 2);
        assert!(chunks[0].text.contains("Paragraph one"));
    }

    #[test]
    fn empty_text() {
        let chunks = chunk_text("", 10, 0, ChunkStrategy::Character).unwrap();
        assert!(chunks.is_empty());
    }

    #[test]
    fn text_smaller_than_chunk() {
        let text = "short";
        let chunks = chunk_text(text, 100, 0, ChunkStrategy::Character).unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "short");
        assert_eq!(chunks[0].start, 0);
        assert_eq!(chunks[0].end, 5);
    }

    #[test]
    fn invalid_params() {
        assert!(chunk_text("text", 0, 0, ChunkStrategy::Character).is_err());
        assert!(chunk_text("text", 5, 5, ChunkStrategy::Character).is_err());
        assert!(chunk_text("text", 5, 10, ChunkStrategy::Character).is_err());
    }

    #[test]
    fn utf8_safety() {
        // Multi-byte characters: each emoji is 4 bytes but 1 char.
        let text = "🌍🌎🌏🌍🌎🌏";
        let chunks = chunk_text(text, 3, 0, ChunkStrategy::Character).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].text, "🌍🌎🌏");
        assert_eq!(chunks[1].text, "🌍🌎🌏");
    }

    #[test]
    fn sentence_fallback_to_character() {
        // A single sentence longer than chunk_size should fall back to character split.
        let text = "This is a very long sentence that exceeds the chunk size limit.";
        let chunks = chunk_text(text, 20, 0, ChunkStrategy::Sentence).unwrap();
        assert!(chunks.len() > 1);
        for chunk in &chunks {
            assert!(chunk.text.chars().count() <= 20);
        }
    }

    #[test]
    fn deterministic() {
        let text = "Deterministic output means same input produces same output every time.";
        let a = chunk_text(text, 15, 3, ChunkStrategy::Character).unwrap();
        let b = chunk_text(text, 15, 3, ChunkStrategy::Character).unwrap();
        assert_eq!(a.len(), b.len());
        for (ca, cb) in a.iter().zip(b.iter()) {
            assert_eq!(ca.text, cb.text);
            assert_eq!(ca.start, cb.start);
            assert_eq!(ca.end, cb.end);
        }
    }

    #[test]
    fn overlap_produces_shared_chars() {
        let text = "0123456789abcdef";
        let chunks = chunk_text(text, 8, 4, ChunkStrategy::Character).unwrap();
        // step = 4. chunk0=[0..8], chunk1=[4..12], chunk2=[8..16]
        assert_eq!(chunks.len(), 3);
        // Overlap: last 4 chars of chunk 0 == first 4 chars of chunk 1.
        let c0_tail: String = chunks[0]
            .text
            .chars()
            .rev()
            .take(4)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        let c1_head: String = chunks[1].text.chars().take(4).collect();
        assert_eq!(c0_tail, c1_head);
    }
}
