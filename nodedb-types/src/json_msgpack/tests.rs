//! Roundtrip tests for json_msgpack reader/writer.

use crate::json_msgpack::{
    json_from_msgpack, json_to_msgpack, value_from_msgpack, value_to_msgpack,
};
use serde_json::json;

#[test]
fn roundtrip_null() {
    let val = json!(null);
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_bool() {
    for val in [json!(true), json!(false)] {
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }
}

#[test]
fn roundtrip_integers() {
    for val in [
        json!(0),
        json!(42),
        json!(-1),
        json!(i64::MAX),
        json!(i64::MIN),
    ] {
        let bytes = json_to_msgpack(&val).unwrap();
        let restored = json_from_msgpack(&bytes).unwrap();
        assert_eq!(val, restored);
    }
}

#[test]
fn roundtrip_float() {
    let val = json!(9.81);
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_string() {
    let val = json!("hello world");
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_array() {
    let val = json!([1, "two", true, null, 2.72]);
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_nested_object() {
    let val = json!({"a": 1, "b": {"c": [2, 3]}, "d": null});
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_empty_map() {
    let val = json!({});
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_empty_array() {
    let val = json!([]);
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn roundtrip_large_string() {
    let s = "x".repeat(300);
    let val = json!(s);
    let bytes = json_to_msgpack(&val).unwrap();
    let restored = json_from_msgpack(&bytes).unwrap();
    assert_eq!(val, restored);
}

#[test]
fn native_value_roundtrip() {
    let mut map = std::collections::HashMap::new();
    map.insert("id".to_string(), crate::Value::String("host1".into()));
    map.insert("cpu".to_string(), crate::Value::Float(0.75));
    map.insert("mem".to_string(), crate::Value::Float(0.5));

    let row = crate::Value::Object(map);
    let arr = crate::Value::Array(vec![row]);

    let bytes = value_to_msgpack(&arr).unwrap();
    let decoded = value_from_msgpack(&bytes).unwrap();

    match &decoded {
        crate::Value::Array(items) => {
            assert_eq!(items.len(), 1);
            match &items[0] {
                crate::Value::Object(m) => {
                    assert_eq!(m.len(), 3);
                    assert_eq!(m.get("id"), Some(&crate::Value::String("host1".into())));
                    assert_eq!(m.get("cpu"), Some(&crate::Value::Float(0.75)));
                    assert_eq!(m.get("mem"), Some(&crate::Value::Float(0.5)));
                }
                other => panic!("expected Object, got {other:?}"),
            }
        }
        other => panic!("expected Array, got {other:?}"),
    }
}

#[test]
fn native_value_scalars() {
    let cases: Vec<crate::Value> = vec![
        crate::Value::Null,
        crate::Value::Bool(true),
        crate::Value::Integer(42),
        crate::Value::Float(2.72),
        crate::Value::String("hello".into()),
    ];
    for val in cases {
        let bytes = value_to_msgpack(&val).unwrap();
        let decoded = value_from_msgpack(&bytes).unwrap();
        assert_eq!(val, decoded);
    }
}
