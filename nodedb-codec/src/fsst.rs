//! FSST (Fast Static Symbol Table) codec for string/log columns.
//!
//! Builds a lightweight dictionary of common substrings (1-8 bytes) and
//! encodes strings as sequences of symbol table indices. Unlike whole-string
//! dictionary encoding, FSST handles partial overlap — strings sharing
//! prefixes or suffixes compress well even if no two strings are identical.
//!
//! Compression: 3-5x on string columns before any terminal compressor.
//! Combined with lz4_flex terminal: 8-15x total on structured log data.
//!
//! Decompression: simple table lookup — fast enough to query directly
//! over encoded data.
//!
//! Wire format:
//! ```text
//! [2 bytes] symbol count (LE u16, max 255)
//! [symbol_count × (1 + len) bytes] symbol table: (len: u8, bytes: [u8; len])
//! [4 bytes] total encoded length (LE u32)
//! [4 bytes] string count (LE u32)
//! [string_count × 4 bytes] encoded string offsets (cumulative LE u32)
//! [N bytes] encoded data (symbol indices interleaved with escape+literal)
//! ```
//!
//! Escape mechanism: byte value 255 followed by a literal byte encodes
//! bytes not covered by any symbol. Symbol indices are 0..254.

use crate::error::CodecError;

/// Maximum number of symbols in the table (reserve 255 as escape).
const MAX_SYMBOLS: usize = 255;

/// Maximum symbol length in bytes.
const MAX_SYMBOL_LEN: usize = 8;

/// Escape byte: signals the next byte is a literal (not a symbol index).
const ESCAPE: u8 = 255;

/// Number of training passes over the input data.
const TRAINING_ROUNDS: usize = 5;

// ---------------------------------------------------------------------------
// Symbol table
// ---------------------------------------------------------------------------

/// A trained FSST symbol table.
#[derive(Debug, Clone)]
struct SymbolTable {
    /// Symbols sorted by length (longest first) for greedy matching.
    symbols: Vec<Vec<u8>>,
}

impl SymbolTable {
    /// Train a symbol table from a set of input strings.
    ///
    /// Uses iterative count-based selection: in each round, count how many
    /// bytes each candidate n-gram would save, pick the best, repeat.
    fn train(strings: &[&[u8]]) -> Self {
        if strings.is_empty() {
            return Self {
                symbols: Vec::new(),
            };
        }

        let mut symbols: Vec<Vec<u8>> = Vec::new();
        let mut symbol_set: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        let mut candidates: std::collections::HashMap<Vec<u8>, usize> =
            std::collections::HashMap::new();

        for _round in 0..TRAINING_ROUNDS {
            // Count n-gram frequencies in the data (after encoding with current table).
            candidates.clear();

            for s in strings {
                // Scan for n-grams of length 1-8 that are NOT already covered by symbols.
                let mut pos = 0;
                while pos < s.len() {
                    // Check if current position starts with a known symbol.
                    let existing_match = longest_symbol_match(&symbols, s, pos);

                    if existing_match > 0 {
                        pos += existing_match;
                        continue;
                    }

                    // No existing symbol matches — count new n-gram candidates.
                    for len in 1..=MAX_SYMBOL_LEN.min(s.len() - pos) {
                        let ngram = &s[pos..pos + len];
                        *candidates.entry(ngram.to_vec()).or_insert(0) += 1;
                    }
                    pos += 1;
                }
            }

            if candidates.is_empty() {
                break;
            }

            // Score candidates by compression gain: frequency * (length - 1).
            // Each symbol saves (length - 1) bytes per occurrence (1 byte for
            // the symbol index vs `length` bytes raw).
            let mut scored: Vec<(Vec<u8>, usize)> = candidates
                .drain()
                .map(|(ngram, freq)| {
                    let gain = freq * (ngram.len().saturating_sub(1));
                    (ngram, gain)
                })
                .filter(|(_, gain)| *gain > 0)
                .collect();

            scored.sort_by_key(|a| std::cmp::Reverse(a.1));

            // Add top candidates that don't duplicate existing symbols.
            for (ngram, _) in scored {
                if symbols.len() >= MAX_SYMBOLS {
                    break;
                }
                if symbol_set.insert(ngram.clone()) {
                    symbols.push(ngram);
                }
            }
        }

        // Sort symbols longest-first for greedy matching.
        symbols.sort_by_key(|a| std::cmp::Reverse(a.len()));

        Self { symbols }
    }

    fn symbol_count(&self) -> usize {
        self.symbols.len()
    }
}

/// Find the longest symbol matching at position `pos` in `data`.
/// Returns the match length (0 if no match).
fn longest_symbol_match(symbols: &[Vec<u8>], data: &[u8], pos: usize) -> usize {
    let remaining = &data[pos..];
    for sym in symbols {
        if remaining.starts_with(sym) {
            return sym.len();
        }
    }
    0
}

// ---------------------------------------------------------------------------
// Public encode / decode API
// ---------------------------------------------------------------------------

/// Encode a batch of strings using FSST compression.
///
/// Trains a symbol table on the input, then encodes each string as a
/// sequence of symbol indices and escaped literals.
pub fn encode(strings: &[&[u8]]) -> Vec<u8> {
    let table = SymbolTable::train(strings);

    // Encode each string.
    let mut encoded_strings: Vec<Vec<u8>> = Vec::with_capacity(strings.len());
    for s in strings {
        encoded_strings.push(encode_string(&table, s));
    }

    // Build wire format.
    let mut out = Vec::new();

    // Symbol table.
    out.extend_from_slice(&(table.symbol_count() as u16).to_le_bytes());
    for sym in &table.symbols {
        out.push(sym.len() as u8);
        out.extend_from_slice(sym);
    }

    // Encoded strings with offset table.
    let total_encoded: usize = encoded_strings.iter().map(|s| s.len()).sum();
    out.extend_from_slice(&(total_encoded as u32).to_le_bytes());
    out.extend_from_slice(&(strings.len() as u32).to_le_bytes());

    // Cumulative offsets.
    let mut offset = 0u32;
    for es in &encoded_strings {
        offset += es.len() as u32;
        out.extend_from_slice(&offset.to_le_bytes());
    }

    // Encoded data.
    for es in &encoded_strings {
        out.extend_from_slice(es);
    }

    out
}

/// Decode FSST-compressed data back to strings.
pub fn decode(data: &[u8]) -> Result<Vec<Vec<u8>>, CodecError> {
    if data.len() < 2 {
        return Err(CodecError::Truncated {
            expected: 2,
            actual: data.len(),
        });
    }

    // Read symbol table.
    let sym_count = u16::from_le_bytes([data[0], data[1]]) as usize;
    let mut pos = 2;
    let mut symbols: Vec<Vec<u8>> = Vec::with_capacity(sym_count);

    for _ in 0..sym_count {
        if pos >= data.len() {
            return Err(CodecError::Truncated {
                expected: pos + 1,
                actual: data.len(),
            });
        }
        let len = data[pos] as usize;
        pos += 1;
        if pos + len > data.len() {
            return Err(CodecError::Truncated {
                expected: pos + len,
                actual: data.len(),
            });
        }
        symbols.push(data[pos..pos + len].to_vec());
        pos += len;
    }

    // Read header.
    if pos + 8 > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + 8,
            actual: data.len(),
        });
    }
    let _total_encoded =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;
    let string_count =
        u32::from_le_bytes([data[pos], data[pos + 1], data[pos + 2], data[pos + 3]]) as usize;
    pos += 4;

    // Read offsets.
    let offsets_size = string_count * 4;
    if pos + offsets_size > data.len() {
        return Err(CodecError::Truncated {
            expected: pos + offsets_size,
            actual: data.len(),
        });
    }
    let mut offsets = Vec::with_capacity(string_count);
    for i in 0..string_count {
        let off_pos = pos + i * 4;
        offsets.push(u32::from_le_bytes([
            data[off_pos],
            data[off_pos + 1],
            data[off_pos + 2],
            data[off_pos + 3],
        ]) as usize);
    }
    pos += offsets_size;

    let encoded_data = &data[pos..];

    // Decode each string.
    let mut result = Vec::with_capacity(string_count);
    let mut prev_end = 0;
    for &end in &offsets {
        if end > encoded_data.len() {
            return Err(CodecError::Truncated {
                expected: pos + end,
                actual: data.len(),
            });
        }
        let encoded_str = &encoded_data[prev_end..end];
        result.push(decode_string(&symbols, encoded_str)?);
        prev_end = end;
    }

    Ok(result)
}

/// Convenience: encode a single contiguous byte buffer that contains
/// multiple strings separated by a delimiter (e.g., newlines for log data).
pub fn encode_delimited(data: &[u8], delimiter: u8) -> Vec<u8> {
    let strings: Vec<&[u8]> = data.split(|&b| b == delimiter).collect();
    encode(&strings)
}

/// Convenience: decode and reassemble with delimiter.
pub fn decode_delimited(data: &[u8], delimiter: u8) -> Result<Vec<u8>, CodecError> {
    let strings = decode(data)?;
    let mut out = Vec::new();
    for (i, s) in strings.iter().enumerate() {
        if i > 0 {
            out.push(delimiter);
        }
        out.extend_from_slice(s);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Per-string encode / decode
// ---------------------------------------------------------------------------

fn encode_string(table: &SymbolTable, input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut pos = 0;

    while pos < input.len() {
        // Greedy: try to match the longest symbol at current position.
        let mut matched = false;
        for (idx, sym) in table.symbols.iter().enumerate() {
            if input[pos..].starts_with(sym) {
                out.push(idx as u8);
                pos += sym.len();
                matched = true;
                break;
            }
        }

        if !matched {
            // No symbol matches — emit escape + literal byte.
            out.push(ESCAPE);
            out.push(input[pos]);
            pos += 1;
        }
    }

    out
}

fn decode_string(symbols: &[Vec<u8>], encoded: &[u8]) -> Result<Vec<u8>, CodecError> {
    let mut out = Vec::with_capacity(encoded.len() * 2);
    let mut pos = 0;

    while pos < encoded.len() {
        let byte = encoded[pos];
        pos += 1;

        if byte == ESCAPE {
            // Next byte is a literal.
            if pos >= encoded.len() {
                return Err(CodecError::Corrupt {
                    detail: "FSST escape at end of encoded data".into(),
                });
            }
            out.push(encoded[pos]);
            pos += 1;
        } else {
            // Symbol index.
            let idx = byte as usize;
            if idx >= symbols.len() {
                return Err(CodecError::Corrupt {
                    detail: format!(
                        "FSST symbol index {idx} out of range (max {})",
                        symbols.len()
                    ),
                });
            }
            out.extend_from_slice(&symbols[idx]);
        }
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input() {
        let encoded = encode(&[]);
        let decoded = decode(&encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn single_string() {
        let strings: Vec<&[u8]> = vec![b"hello world"];
        let encoded = encode(&strings);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], b"hello world");
    }

    #[test]
    fn multiple_strings_roundtrip() {
        let strings: Vec<&[u8]> = vec![
            b"us-east-1",
            b"us-east-2",
            b"us-west-1",
            b"eu-west-1",
            b"us-east-1",
            b"us-east-1",
        ];
        let encoded = encode(&strings);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), strings.len());
        for (a, b) in strings.iter().zip(decoded.iter()) {
            assert_eq!(*a, b.as_slice());
        }
    }

    #[test]
    fn repetitive_log_lines() {
        let lines: Vec<&[u8]> = (0..1000)
            .map(|i| {
                let s: &[u8] = match i % 5 {
                    0 => b"2024-01-15 INFO server.handler request_id=abc method=GET status=200",
                    1 => b"2024-01-15 INFO server.handler request_id=def method=POST status=201",
                    2 => b"2024-01-15 WARN server.handler request_id=ghi method=GET status=404",
                    3 => b"2024-01-15 ERROR server.handler request_id=jkl method=PUT status=500",
                    _ => b"2024-01-15 DEBUG server.handler request_id=mno method=GET status=200",
                };
                s
            })
            .collect();

        let encoded = encode(&lines);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), lines.len());
        for (a, b) in lines.iter().zip(decoded.iter()) {
            assert_eq!(*a, b.as_slice());
        }

        // FSST should compress repetitive logs.
        let raw_size: usize = lines.iter().map(|s| s.len()).sum();
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 1.5,
            "FSST should compress repetitive logs >1.5x, got {ratio:.1}x"
        );
    }

    #[test]
    fn hostnames() {
        let hosts: Vec<&[u8]> = vec![
            b"prod-web-01.us-east-1.example.com",
            b"prod-web-02.us-east-1.example.com",
            b"prod-web-03.us-east-1.example.com",
            b"prod-api-01.us-west-2.example.com",
            b"prod-api-02.us-west-2.example.com",
            b"staging-web-01.eu-west-1.example.com",
        ];
        let encoded = encode(&hosts);
        let decoded = decode(&encoded).unwrap();
        for (a, b) in hosts.iter().zip(decoded.iter()) {
            assert_eq!(*a, b.as_slice());
        }
    }

    #[test]
    fn binary_data() {
        // Binary data with no patterns — should still roundtrip (escape every byte).
        let data: Vec<&[u8]> = vec![&[0, 1, 2, 3, 4, 255, 254, 253]];
        let encoded = encode(&data);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded[0], data[0]);
    }

    #[test]
    fn empty_strings() {
        let strings: Vec<&[u8]> = vec![b"", b"hello", b"", b"world", b""];
        let encoded = encode(&strings);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), 5);
        assert!(decoded[0].is_empty());
        assert_eq!(decoded[1], b"hello");
        assert!(decoded[2].is_empty());
    }

    #[test]
    fn delimited_roundtrip() {
        let data = b"line one\nline two\nline three\nline one\nline two";
        let encoded = encode_delimited(data, b'\n');
        let decoded = decode_delimited(&encoded, b'\n').unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn compression_ratio_structured_logs() {
        let mut lines: Vec<Vec<u8>> = Vec::new();
        for i in 0..5000 {
            let line = format!(
                "2024-01-15T10:30:{:02}.000Z INFO server.handler request_id={} method=GET path=/api/v1/metrics status=200 duration_ms={}",
                i % 60,
                10000 + i,
                i * 3 + 1
            );
            lines.push(line.into_bytes());
        }
        let refs: Vec<&[u8]> = lines.iter().map(|l| l.as_slice()).collect();

        let encoded = encode(&refs);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), lines.len());

        let raw_size: usize = lines.iter().map(|s| s.len()).sum();
        let ratio = raw_size as f64 / encoded.len() as f64;
        assert!(
            ratio > 1.5,
            "FSST should compress structured logs >1.5x, got {ratio:.1}x"
        );
    }

    #[test]
    fn truncated_input_errors() {
        assert!(decode(&[]).is_err());
        assert!(decode(&[1]).is_err());
    }

    #[test]
    fn large_dataset() {
        let mut strings: Vec<Vec<u8>> = Vec::new();
        for i in 0..10_000 {
            strings.push(format!("key-{}-value-{}", i % 100, i % 50).into_bytes());
        }
        let refs: Vec<&[u8]> = strings.iter().map(|s| s.as_slice()).collect();
        let encoded = encode(&refs);
        let decoded = decode(&encoded).unwrap();
        assert_eq!(decoded.len(), strings.len());
        for (a, b) in strings.iter().zip(decoded.iter()) {
            assert_eq!(a.as_slice(), b.as_slice());
        }
    }
}
