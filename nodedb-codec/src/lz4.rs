//! LZ4 block compression codec for string/log columns.
//!
//! Uses `lz4_flex` (pure Rust, WASM-compatible) for fast decompression
//! with reasonable compression ratios (3-5x for typical log text).
//!
//! Data is split into 4KB blocks for random access: to read row N,
//! decompress only the block containing that row, not the entire column.
//!
//! Wire format:
//! ```text
//! [4 bytes] total uncompressed size (LE u32)
//! [4 bytes] block size (LE u32, default 4096)
//! [4 bytes] block count (LE u32)
//! [block_count × 4 bytes] compressed block lengths (LE u32 each)
//! [block_count × N bytes] compressed blocks (concatenated)
//! ```
//!
//! The block length table allows seeking to any block without
//! decompressing preceding blocks.

use crate::error::CodecError;

/// Default block size for LZ4 compression (4 KiB).
const DEFAULT_BLOCK_SIZE: usize = 4096;

// ---------------------------------------------------------------------------
// Public encode / decode API
// ---------------------------------------------------------------------------

/// Compress raw bytes using LZ4 block compression.
///
/// Splits input into `block_size` blocks, compresses each independently.
pub fn encode(data: &[u8]) -> Vec<u8> {
    encode_with_block_size(data, DEFAULT_BLOCK_SIZE)
}

/// Compress with a custom block size (useful for testing or tuning).
pub fn encode_with_block_size(data: &[u8], block_size: usize) -> Vec<u8> {
    let block_size = block_size.max(64); // minimum 64 bytes
    let block_count = if data.is_empty() {
        0
    } else {
        data.len().div_ceil(block_size)
    };

    // Pre-allocate: header(12) + block_lengths(4*N) + compressed_blocks.
    let mut out = Vec::with_capacity(12 + block_count * 4 + data.len());

    // Header.
    out.extend_from_slice(&(data.len() as u32).to_le_bytes());
    out.extend_from_slice(&(block_size as u32).to_le_bytes());
    out.extend_from_slice(&(block_count as u32).to_le_bytes());

    // Reserve space for block length table (filled in after compression).
    let lengths_offset = out.len();
    out.resize(lengths_offset + block_count * 4, 0);

    // Compress each block.
    for (i, chunk) in data.chunks(block_size).enumerate() {
        let compressed = lz4_flex::compress_prepend_size(chunk);
        let compressed_len = compressed.len() as u32;

        // Write block length into the table.
        let table_pos = lengths_offset + i * 4;
        out[table_pos..table_pos + 4].copy_from_slice(&compressed_len.to_le_bytes());

        // Append compressed block.
        out.extend_from_slice(&compressed);
    }

    out
}

/// Decompress LZ4 block-compressed bytes back to raw data.
pub fn decode(data: &[u8]) -> Result<Vec<u8>, CodecError> {
    let header = read_header(data)?;

    if header.block_count == 0 {
        return Ok(Vec::new());
    }

    let mut result = Vec::with_capacity(header.uncompressed_size);
    let mut block_offset = header.data_offset;

    for i in 0..header.block_count {
        let compressed_len = header.block_lengths[i];
        let block_end = block_offset + compressed_len;

        if block_end > data.len() {
            return Err(CodecError::Truncated {
                expected: block_end,
                actual: data.len(),
            });
        }

        let block_data = &data[block_offset..block_end];
        let decompressed = lz4_flex::decompress_size_prepended(block_data).map_err(|e| {
            CodecError::DecompressFailed {
                detail: format!("LZ4 block {i}: {e}"),
            }
        })?;

        result.extend_from_slice(&decompressed);
        block_offset = block_end;
    }

    if result.len() != header.uncompressed_size {
        return Err(CodecError::Corrupt {
            detail: format!(
                "uncompressed size mismatch: header says {}, got {}",
                header.uncompressed_size,
                result.len()
            ),
        });
    }

    Ok(result)
}

/// Decompress a single block by index (for random access).
///
/// Returns the decompressed bytes of just that block.
pub fn decode_block(data: &[u8], block_idx: usize) -> Result<Vec<u8>, CodecError> {
    let header = read_header(data)?;

    if block_idx >= header.block_count {
        return Err(CodecError::Corrupt {
            detail: format!(
                "block index {block_idx} out of range (block_count={})",
                header.block_count
            ),
        });
    }

    // Sum lengths of preceding blocks to find this block's offset.
    let mut block_offset = header.data_offset;
    for i in 0..block_idx {
        block_offset += header.block_lengths[i];
    }

    let compressed_len = header.block_lengths[block_idx];
    let block_end = block_offset + compressed_len;

    if block_end > data.len() {
        return Err(CodecError::Truncated {
            expected: block_end,
            actual: data.len(),
        });
    }

    let block_data = &data[block_offset..block_end];
    lz4_flex::decompress_size_prepended(block_data).map_err(|e| CodecError::DecompressFailed {
        detail: format!("LZ4 block {block_idx}: {e}"),
    })
}

// ---------------------------------------------------------------------------
// Header parsing
// ---------------------------------------------------------------------------

struct Lz4Header {
    uncompressed_size: usize,
    block_count: usize,
    block_lengths: Vec<usize>,
    /// Byte offset where compressed block data starts.
    data_offset: usize,
}

fn read_header(data: &[u8]) -> Result<Lz4Header, CodecError> {
    if data.len() < 12 {
        return Err(CodecError::Truncated {
            expected: 12,
            actual: data.len(),
        });
    }

    let uncompressed_size = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    let _block_size = u32::from_le_bytes([data[4], data[5], data[6], data[7]]) as usize;
    let block_count = u32::from_le_bytes([data[8], data[9], data[10], data[11]]) as usize;

    let lengths_end = 12 + block_count * 4;
    if data.len() < lengths_end {
        return Err(CodecError::Truncated {
            expected: lengths_end,
            actual: data.len(),
        });
    }

    let block_lengths: Vec<usize> = data[12..lengths_end]
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]) as usize)
        .collect();

    Ok(Lz4Header {
        uncompressed_size,
        block_count,
        block_lengths,
        data_offset: lengths_end,
    })
}

// ---------------------------------------------------------------------------
// Streaming encoder / decoder types
// ---------------------------------------------------------------------------

/// Streaming LZ4 encoder. Accumulates data and compresses on `finish()`.
pub struct Lz4Encoder {
    buf: Vec<u8>,
    block_size: usize,
}

impl Lz4Encoder {
    pub fn new() -> Self {
        Self {
            buf: Vec::with_capacity(4096),
            block_size: DEFAULT_BLOCK_SIZE,
        }
    }

    pub fn with_block_size(block_size: usize) -> Self {
        Self {
            buf: Vec::with_capacity(block_size),
            block_size: block_size.max(64),
        }
    }

    pub fn push(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn finish(self) -> Vec<u8> {
        encode_with_block_size(&self.buf, self.block_size)
    }
}

impl Default for Lz4Encoder {
    fn default() -> Self {
        Self::new()
    }
}

/// LZ4 decoder wrapper.
pub struct Lz4Decoder;

impl Lz4Decoder {
    /// Decompress all blocks.
    pub fn decode_all(data: &[u8]) -> Result<Vec<u8>, CodecError> {
        decode(data)
    }

    /// Decompress a single block by index.
    pub fn decode_block(data: &[u8], block_idx: usize) -> Result<Vec<u8>, CodecError> {
        decode_block(data, block_idx)
    }

    /// Number of blocks in the compressed data.
    pub fn block_count(data: &[u8]) -> Result<usize, CodecError> {
        let header = read_header(data)?;
        Ok(header.block_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_roundtrip() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn small_data_roundtrip() {
        let data = b"hello world, this is a log message";
        let encoded = encode(data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn large_data_multiple_blocks() {
        // Generate ~40KB of log-like data (10 blocks of 4KB each).
        let mut data = Vec::new();
        for i in 0..1000 {
            let line = format!(
                "2024-01-15T10:30:{:02}.000Z INFO server.handler request_id={} method=GET path=/api/v1/metrics status=200 duration_ms={}\n",
                i % 60,
                10000 + i,
                i * 3 + 1
            );
            data.extend_from_slice(line.as_bytes());
        }

        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);

        // LZ4 should achieve at least 2x compression on structured logs.
        let ratio = data.len() as f64 / encoded.len() as f64;
        assert!(
            ratio > 2.0,
            "expected >2x compression for structured logs, got {ratio:.1}x"
        );
    }

    #[test]
    fn random_access_block() {
        let data: Vec<u8> = (0..20000).map(|i| (i % 256) as u8).collect();
        let block_size = 4096;
        let encoded = encode_with_block_size(&data, block_size);

        let block_count = Lz4Decoder::block_count(&encoded).unwrap();
        assert_eq!(block_count, data.len().div_ceil(block_size));

        // Decompress each block individually and verify.
        let mut reassembled = Vec::new();
        for i in 0..block_count {
            let block = decode_block(&encoded, i).unwrap();
            reassembled.extend_from_slice(&block);
        }
        assert_eq!(reassembled, data);
    }

    #[test]
    fn out_of_range_block_index() {
        let data = b"some data here";
        let encoded = encode(data);
        assert!(decode_block(&encoded, 999).is_err());
    }

    #[test]
    fn compressible_log_data() {
        // Highly repetitive log lines.
        let line = "2024-01-15 ERROR database connection timeout host=db-prod-01 retry=3\n";
        let data: Vec<u8> = line.as_bytes().repeat(500);
        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);

        let ratio = data.len() as f64 / encoded.len() as f64;
        assert!(
            ratio > 3.0,
            "highly repetitive logs should compress >3x, got {ratio:.1}x"
        );
    }

    #[test]
    fn incompressible_data() {
        // Random bytes — LZ4 may expand slightly but shouldn't fail.
        let mut data = vec![0u8; 10_000];
        let mut rng: u64 = 9999;
        for byte in &mut data {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            *byte = (rng >> 33) as u8;
        }
        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn streaming_encoder() {
        let parts = [b"hello ".as_ref(), b"world".as_ref(), b" test".as_ref()];
        let full: Vec<u8> = parts.iter().flat_map(|p| p.iter().copied()).collect();

        let mut enc = Lz4Encoder::new();
        for part in &parts {
            enc.push(part);
        }
        let encoded = enc.finish();
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, full);
    }

    #[test]
    fn custom_block_size() {
        let data = vec![42u8; 10_000];
        let encoded = encode_with_block_size(&data, 1024);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, data);

        let block_count = Lz4Decoder::block_count(&encoded).unwrap();
        assert_eq!(block_count, 10); // 10000 / 1024 rounded up
    }

    #[test]
    fn truncated_input_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[0; 8]).is_err()); // too short for header
    }
}
