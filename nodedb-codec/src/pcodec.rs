//! Pcodec wrapper for complex numerical sequences.
//!
//! For data where ALP's decimal-to-integer trick doesn't apply (scientific
//! floats, irregular numerical sequences, CRDT operation counters), Pcodec
//! builds a probabilistic model of the data distribution, separates
//! high-order structure from low-order noise, and compresses each
//! independently.
//!
//! Compression: 30-100% better ratio than Zstd on numerical data.
//! Decode: 1-4 GB/s.
//!
//! Wire format: Pcodec's native format with a 5-byte NodeDB header:
//! ```text
//! [1 byte]  type tag (0=f64, 1=i64)
//! [4 bytes] value count (LE u32)
//! [N bytes] pco compressed data
//! ```

use crate::error::CodecError;

/// Type tag for f64 data.
const TAG_F64: u8 = 0;
/// Type tag for i64 data.
const TAG_I64: u8 = 1;

// ---------------------------------------------------------------------------
// f64 encode / decode
// ---------------------------------------------------------------------------

/// Compress f64 values using Pcodec.
pub fn encode_f64(values: &[f64]) -> Result<Vec<u8>, CodecError> {
    let count = values.len() as u32;
    let compressed = pco::standalone::simple_compress(values, &pco::ChunkConfig::default())
        .map_err(|e| CodecError::CompressFailed {
            detail: format!("pcodec f64: {e}"),
        })?;

    let mut out = Vec::with_capacity(5 + compressed.len());
    out.push(TAG_F64);
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Decompress Pcodec f64 data.
pub fn decode_f64(data: &[u8]) -> Result<Vec<f64>, CodecError> {
    if data.len() < 5 {
        return Err(CodecError::Truncated {
            expected: 5,
            actual: data.len(),
        });
    }

    let tag = data[0];
    if tag != TAG_F64 {
        return Err(CodecError::Corrupt {
            detail: format!("pcodec expected f64 tag (0), got {tag}"),
        });
    }

    let count = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    let values: Vec<f64> = pco::standalone::simple_decompress(&data[5..]).map_err(|e| {
        CodecError::DecompressFailed {
            detail: format!("pcodec f64: {e}"),
        }
    })?;

    if values.len() != count {
        return Err(CodecError::Corrupt {
            detail: format!(
                "pcodec f64 count mismatch: header says {count}, got {}",
                values.len()
            ),
        });
    }

    Ok(values)
}

// ---------------------------------------------------------------------------
// i64 encode / decode
// ---------------------------------------------------------------------------

/// Compress i64 values using Pcodec.
pub fn encode_i64(values: &[i64]) -> Result<Vec<u8>, CodecError> {
    let count = values.len() as u32;
    let compressed = pco::standalone::simple_compress(values, &pco::ChunkConfig::default())
        .map_err(|e| CodecError::CompressFailed {
            detail: format!("pcodec i64: {e}"),
        })?;

    let mut out = Vec::with_capacity(5 + compressed.len());
    out.push(TAG_I64);
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Decompress Pcodec i64 data.
pub fn decode_i64(data: &[u8]) -> Result<Vec<i64>, CodecError> {
    if data.len() < 5 {
        return Err(CodecError::Truncated {
            expected: 5,
            actual: data.len(),
        });
    }

    let tag = data[0];
    if tag != TAG_I64 {
        return Err(CodecError::Corrupt {
            detail: format!("pcodec expected i64 tag (1), got {tag}"),
        });
    }

    let count = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    let values: Vec<i64> = pco::standalone::simple_decompress(&data[5..]).map_err(|e| {
        CodecError::DecompressFailed {
            detail: format!("pcodec i64: {e}"),
        }
    })?;

    if values.len() != count {
        return Err(CodecError::Corrupt {
            detail: format!(
                "pcodec i64 count mismatch: header says {count}, got {}",
                values.len()
            ),
        });
    }

    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn f64_empty() {
        let encoded = encode_f64(&[]).unwrap();
        let decoded = decode_f64(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn f64_roundtrip() {
        let values: Vec<f64> = (0..1000).map(|i| std::f64::consts::PI * i as f64).collect();
        let encoded = encode_f64(&values).unwrap();
        let decoded = decode_f64(&encoded).unwrap();
        assert_eq!(decoded.len(), values.len());
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits(), "mismatch");
        }
    }

    #[test]
    fn f64_compression_ratio() {
        // Pcodec should compress numerical data better than raw.
        let mut values = Vec::with_capacity(10_000);
        let mut rng: u64 = 42;
        for _ in 0..10_000 {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            values.push(((rng >> 33) as f64 / (u32::MAX as f64)) * 1000.0);
        }
        let encoded = encode_f64(&values).unwrap();
        let raw_size = values.len() * 8;
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 1.1,
            "pcodec should compress random-ish floats >1.1x, got {ratio:.2}x"
        );
    }

    #[test]
    fn i64_empty() {
        let encoded = encode_i64(&[]).unwrap();
        let decoded = decode_i64(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn i64_roundtrip() {
        let values: Vec<i64> = (0..1000).map(|i| i * i * 7 - 500).collect();
        let encoded = encode_i64(&values).unwrap();
        let decoded = decode_i64(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn i64_compression_ratio() {
        let values: Vec<i64> = (0..10_000)
            .map(|i| 1_700_000_000_000 + i * 10_000)
            .collect();
        let encoded = encode_i64(&values).unwrap();
        let raw_size = values.len() * 8;
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 2.0,
            "pcodec should compress monotonic i64 >2x, got {ratio:.2}x"
        );
    }

    #[test]
    fn f64_special_values() {
        let values = vec![0.0, -0.0, f64::INFINITY, f64::NEG_INFINITY, 1.0, -1.0];
        let encoded = encode_f64(&values).unwrap();
        let decoded = decode_f64(&encoded).unwrap();
        for (a, b) in values.iter().zip(decoded.iter()) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }

    #[test]
    fn truncated_errors() {
        assert!(decode_f64(&[]).is_err());
        assert!(decode_i64(&[]).is_err());
        assert!(decode_f64(&[0, 1, 0, 0, 0]).is_err()); // count=1, no data
    }
}
