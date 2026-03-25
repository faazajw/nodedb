//! Delta codec for monotonic counter columns.
//!
//! Monotonic counters (bytes_sent, request_count) have small positive deltas
//! that compress well with simple delta encoding + varint packing.
//!
//! Wire format:
//! ```text
//! [4 bytes] sample count (LE u32)
//! [8 bytes] first value (LE i64)
//! [N bytes] varint-encoded deltas (ZigZag + LEB128)
//! ```
//!
//! Varint encoding uses ZigZag to handle negative deltas (counter resets)
//! efficiently: small absolute values → 1-2 bytes regardless of sign.
//!
//! Compression: monotonic counters with small increments → ~1-2 bytes/sample.
//! Non-monotonic data → ~2-4 bytes/sample (still better than raw 8 bytes).

use crate::error::CodecError;

// ---------------------------------------------------------------------------
// ZigZag + LEB128 varint encoding
// ---------------------------------------------------------------------------

/// ZigZag-encode a signed i64 into an unsigned u64.
///
/// Maps signed integers to unsigned so small absolute values have small
/// representations: 0→0, -1→1, 1→2, -2→3, 2→4, ...
#[inline]
fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// ZigZag-decode an unsigned u64 back to signed i64.
#[inline]
fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Write a varint (LEB128-encoded u64) to a buffer.
fn write_varint(buf: &mut Vec<u8>, mut value: u64) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buf.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Read a varint (LEB128-encoded u64) from a byte slice.
///
/// Returns `(value, bytes_consumed)`.
fn read_varint(data: &[u8]) -> Result<(u64, usize), CodecError> {
    let mut value: u64 = 0;
    let mut shift: u32 = 0;

    for (i, &byte) in data.iter().enumerate() {
        if shift >= 70 {
            return Err(CodecError::Corrupt {
                detail: "varint too long (>10 bytes)".into(),
            });
        }

        value |= ((byte & 0x7F) as u64) << shift;
        shift += 7;

        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
    }

    Err(CodecError::Truncated {
        expected: data.len() + 1,
        actual: data.len(),
    })
}

// ---------------------------------------------------------------------------
// Public encode / decode API
// ---------------------------------------------------------------------------

/// Encode a slice of i64 values using Delta + ZigZag-varint compression.
pub fn encode(values: &[i64]) -> Vec<u8> {
    let count = values.len() as u32;
    // Estimate: header(4) + first_value(8) + ~2 bytes per delta.
    let mut out = Vec::with_capacity(12 + values.len() * 2);

    out.extend_from_slice(&count.to_le_bytes());

    if values.is_empty() {
        return out;
    }

    out.extend_from_slice(&values[0].to_le_bytes());

    for i in 1..values.len() {
        let delta = values[i].wrapping_sub(values[i - 1]);
        write_varint(&mut out, zigzag_encode(delta));
    }

    out
}

/// Decode Delta-compressed bytes back to i64 values.
pub fn decode(data: &[u8]) -> Result<Vec<i64>, CodecError> {
    if data.len() < 4 {
        return Err(CodecError::Truncated {
            expected: 4,
            actual: data.len(),
        });
    }

    let count = u32::from_le_bytes(data[0..4].try_into().map_err(|_| CodecError::Corrupt {
        detail: "invalid header".into(),
    })?) as usize;

    if count == 0 {
        return Ok(Vec::new());
    }

    if data.len() < 12 {
        return Err(CodecError::Truncated {
            expected: 12,
            actual: data.len(),
        });
    }

    let first_value =
        i64::from_le_bytes(data[4..12].try_into().map_err(|_| CodecError::Corrupt {
            detail: "invalid first value".into(),
        })?);

    let mut values = Vec::with_capacity(count);
    values.push(first_value);

    let mut offset = 12;
    for _ in 1..count {
        if offset >= data.len() {
            return Err(CodecError::Truncated {
                expected: offset + 1,
                actual: data.len(),
            });
        }
        let (encoded_delta, consumed) = read_varint(&data[offset..])?;
        let delta = zigzag_decode(encoded_delta);
        let value = values[values.len() - 1].wrapping_add(delta);
        values.push(value);
        offset += consumed;
    }

    Ok(values)
}

// ---------------------------------------------------------------------------
// Streaming encoder / decoder types
// ---------------------------------------------------------------------------

/// Streaming Delta encoder. Accumulates values and produces compressed
/// bytes on `finish()`.
pub struct DeltaEncoder {
    values: Vec<i64>,
}

impl DeltaEncoder {
    pub fn new() -> Self {
        Self {
            values: Vec::with_capacity(4096),
        }
    }

    pub fn push(&mut self, value: i64) {
        self.values.push(value);
    }

    pub fn push_batch(&mut self, values: &[i64]) {
        self.values.extend_from_slice(values);
    }

    pub fn count(&self) -> usize {
        self.values.len()
    }

    pub fn finish(self) -> Vec<u8> {
        encode(&self.values)
    }
}

impl Default for DeltaEncoder {
    fn default() -> Self {
        Self::new()
    }
}

/// Streaming Delta decoder.
pub struct DeltaDecoder {
    values: Vec<i64>,
    pos: usize,
}

impl DeltaDecoder {
    pub fn new(data: &[u8]) -> Result<Self, CodecError> {
        let values = decode(data)?;
        Ok(Self { values, pos: 0 })
    }

    pub fn decode_all(data: &[u8]) -> Result<Vec<i64>, CodecError> {
        decode(data)
    }

    pub fn next_value(&mut self) -> Option<i64> {
        if self.pos < self.values.len() {
            let v = self.values[self.pos];
            self.pos += 1;
            Some(v)
        } else {
            None
        }
    }

    pub fn remaining(&self) -> usize {
        self.values.len() - self.pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zigzag_roundtrip() {
        for v in [0i64, 1, -1, 2, -2, 63, -63, 127, -128, i64::MAX, i64::MIN] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v, "zigzag failed for {v}");
        }
    }

    #[test]
    fn varint_roundtrip() {
        for v in [0u64, 1, 127, 128, 255, 16383, 16384, u64::MAX / 2, u64::MAX] {
            let mut buf = Vec::new();
            write_varint(&mut buf, v);
            let (decoded, consumed) = read_varint(&buf).unwrap();
            assert_eq!(decoded, v, "varint failed for {v}");
            assert_eq!(consumed, buf.len());
        }
    }

    #[test]
    fn empty_roundtrip() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn single_value() {
        let encoded = encode(&[42i64]);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, vec![42i64]);
        assert_eq!(encoded.len(), 12); // 4 + 8
    }

    #[test]
    fn monotonic_counter() {
        // Bytes sent: monotonically increasing by ~1000 each step.
        let values: Vec<i64> = (0..10_000).map(|i| i * 1000).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        // All deltas are exactly 1000 → zigzag(1000) = 2000 → 2 bytes each.
        let bytes_per_sample = encoded.len() as f64 / values.len() as f64;
        assert!(
            bytes_per_sample < 3.0,
            "monotonic counter should compress to <3 bytes/sample, got {bytes_per_sample:.2}"
        );
    }

    #[test]
    fn counter_with_small_increments() {
        // Request count: increment by 1 each step.
        let values: Vec<i64> = (0..10_000).collect();
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        // Delta = 1 → zigzag(1) = 2 → 1 byte each.
        let bytes_per_sample = encoded.len() as f64 / values.len() as f64;
        assert!(
            bytes_per_sample < 2.0,
            "unit-increment counter should compress to <2 bytes/sample, got {bytes_per_sample:.2}"
        );
    }

    #[test]
    fn counter_reset() {
        // Counter with a reset (wrap-around): monotonic then drops to 0.
        let mut values: Vec<i64> = (0..500).map(|i| i * 100).collect();
        values.push(0); // reset
        values.extend((1..500).map(|i| i * 100));

        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn non_monotonic_gauge() {
        // CPU gauge: fluctuates around a value.
        let mut values = Vec::with_capacity(10_000);
        let mut val = 50i64;
        let mut rng: u64 = 12345;
        for _ in 0..10_000 {
            values.push(val);
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let delta = ((rng >> 33) as i64 % 11) - 5; // -5 to +5
            val += delta;
        }
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);

        // Small deltas → 1 byte each → ~2 bytes/sample.
        let bytes_per_sample = encoded.len() as f64 / values.len() as f64;
        assert!(
            bytes_per_sample < 3.0,
            "small-delta gauge should compress to <3 bytes/sample, got {bytes_per_sample:.2}"
        );
    }

    #[test]
    fn negative_values() {
        let values: Vec<i64> = vec![-1000, -999, -998, -997, -996];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn large_values() {
        let values: Vec<i64> = vec![i64::MAX, i64::MAX - 1, i64::MAX - 2];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn boundary_values() {
        let values: Vec<i64> = vec![i64::MIN, 0, i64::MAX];
        let encoded = encode(&values);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn streaming_encoder_matches_batch() {
        let values: Vec<i64> = (0..1000).map(|i| i * 7).collect();
        let batch = encode(&values);

        let mut enc = DeltaEncoder::new();
        for &v in &values {
            enc.push(v);
        }
        assert_eq!(enc.finish(), batch);
    }

    #[test]
    fn streaming_decoder() {
        let values: Vec<i64> = (0..100).map(|i| i * 10).collect();
        let encoded = encode(&values);
        let mut dec = DeltaDecoder::new(&encoded).unwrap();

        for &expected in &values {
            assert_eq!(dec.next_value(), Some(expected));
        }
        assert_eq!(dec.next_value(), None);
    }

    #[test]
    fn truncated_input_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[1, 0, 0, 0]).is_err()); // count=1, no value
    }

    #[test]
    fn compression_vs_raw() {
        let values: Vec<i64> = (0..100_000).map(|i| i * 1000).collect();
        let encoded = encode(&values);
        let raw_size = values.len() * 8;
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 3.0,
            "expected >3x compression for monotonic counter, got {ratio:.1}x"
        );
    }
}
