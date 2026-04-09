//! Msgpack serialization for `serde_json::Value` and `nodedb_types::Value`.

use super::json_value::JsonValue;

/// Serialize a `serde_json::Value` to MessagePack bytes.
#[inline]
pub fn json_to_msgpack(value: &serde_json::Value) -> zerompk::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&JsonValue(value.clone()))
}

/// Serialize a `nodedb_types::Value` to standard MessagePack bytes.
///
/// Writes standard msgpack format (fixmap 0x80-0x8F, fixstr 0xA0-0xBF, etc.)
/// directly from `Value` — no zerompk tagged encoding.
pub fn value_to_msgpack(value: &crate::Value) -> zerompk::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(128);
    write_native_value(&mut buf, value);
    Ok(buf)
}

/// Write a `nodedb_types::Value` as standard msgpack bytes.
fn write_native_value(buf: &mut Vec<u8>, value: &crate::Value) {
    match value {
        crate::Value::Null => buf.push(0xC0),
        crate::Value::Bool(false) => buf.push(0xC2),
        crate::Value::Bool(true) => buf.push(0xC3),
        crate::Value::Integer(i) => write_native_int(buf, *i),
        crate::Value::Float(f) => {
            buf.push(0xCB);
            buf.extend_from_slice(&f.to_be_bytes());
        }
        crate::Value::String(s)
        | crate::Value::Uuid(s)
        | crate::Value::Ulid(s)
        | crate::Value::Regex(s) => write_native_str(buf, s),
        crate::Value::Bytes(b) => write_native_bin(buf, b),
        crate::Value::Array(arr) | crate::Value::Set(arr) => {
            write_native_array_header(buf, arr.len());
            for v in arr {
                write_native_value(buf, v);
            }
        }
        crate::Value::Object(map) => {
            write_native_map_header(buf, map.len());
            for (k, v) in map {
                write_native_str(buf, k);
                write_native_value(buf, v);
            }
        }
        crate::Value::DateTime(dt) => write_native_str(buf, &dt.to_string()),
        crate::Value::Duration(d) => write_native_str(buf, &d.to_string()),
        crate::Value::Decimal(d) => write_native_str(buf, &d.to_string()),
        crate::Value::Geometry(g) => {
            if let Ok(s) = serde_json::to_string(g) {
                write_native_str(buf, &s);
            } else {
                buf.push(0xC0);
            }
        }
        crate::Value::Range { .. } | crate::Value::Record { .. } => buf.push(0xC0),
    }
}

fn write_native_int(buf: &mut Vec<u8>, i: i64) {
    if (0..=0x7F).contains(&i) {
        buf.push(i as u8);
    } else if (-32..0).contains(&i) {
        buf.push(i as u8); // negative fixint
    } else if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
        buf.push(0xD0);
        buf.push(i as i8 as u8);
    } else if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
        buf.push(0xD1);
        buf.extend_from_slice(&(i as i16).to_be_bytes());
    } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
        buf.push(0xD2);
        buf.extend_from_slice(&(i as i32).to_be_bytes());
    } else {
        buf.push(0xD3);
        buf.extend_from_slice(&i.to_be_bytes());
    }
}

fn write_native_str(buf: &mut Vec<u8>, s: &str) {
    let len = s.len();
    if len < 32 {
        buf.push(0xA0 | len as u8);
    } else if len <= u8::MAX as usize {
        buf.push(0xD9);
        buf.push(len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDA);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDB);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(s.as_bytes());
}

fn write_native_bin(buf: &mut Vec<u8>, b: &[u8]) {
    let len = b.len();
    if len <= u8::MAX as usize {
        buf.push(0xC4);
        buf.push(len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xC5);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xC6);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(b);
}

fn write_native_array_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x90 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDC);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

fn write_native_map_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDE);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDF);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}
