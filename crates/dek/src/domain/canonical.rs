//! One text for one value: compact JSON with object keys in order, whatever
//! order a map keeps. What the associated data of every cipher in this crate
//! is made of, so it must never change for data that exists.

use serde_json::Value;

/// The canonical text of `value`.
pub(crate) fn json(value: &Value) -> String {
    let mut out = String::new();
    write(value, &mut out);
    out
}

fn write(value: &Value, out: &mut String) {
    match value {
        Value::Object(fields) => {
            let mut keys: Vec<&String> = fields.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(&Value::String((*key).clone()), out);
                out.push(':');
                write(&fields[*key], out);
            }
            out.push('}');
        }
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(item, out);
            }
            out.push(']');
        }
        // A scalar has one compact form.
        scalar => out.push_str(&scalar.to_string()),
    }
}
