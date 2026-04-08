//! Type-checking scalar functions.

use nodedb_types::Value;

pub(super) fn try_eval(name: &str, args: &[Value]) -> Option<Value> {
    let v = match name {
        "typeof" | "type_of" => {
            let type_name = match args.first() {
                Some(v) => v.type_name(),
                None => "null",
            };
            Value::String(type_name.to_string())
        }
        _ => return None,
    };
    Some(v)
}
