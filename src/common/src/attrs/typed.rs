//! Codec between the JSON-in-Utf8 attribute carrier used on the Flight/WAL
//! wire and the CBOR residue document used by the typed attribute layout
//! (see [`crate::schema::typed_attributes`]) for anything that has no typed
//! home (off-type scalars, arrays, kvlists, bytes).
//!
//! See [`crate::attrs`] for the legacy single-map/JSON-to-strings decoder
//! this layout replaces.

use serde_json::{Map, Value as JsonValue};

use crate::flight::conversion::conversion_common::{
    bytes_to_carrier, carrier_to_bytes, is_bytes_carrier,
};
use crate::schema::type_authority::ObservedKind;

#[derive(Debug, thiserror::Error)]
pub enum ResidueError {
    #[error("failed to encode residue CBOR: {0}")]
    Encode(#[from] ciborium::ser::Error<std::io::Error>),
    #[error("failed to decode residue CBOR: {0}")]
    Decode(#[from] ciborium::de::Error<std::io::Error>),
    #[error("residue document is not a CBOR map")]
    NotAMap,
}

/// Classify a carrier value's JSON shape for [`crate::schema::type_authority::Placement`]
/// decisions. A bytes carrier whose base64 payload fails to decode is not
/// bytes: it classifies (and encodes) as a `KvList`, matching
/// [`json_to_cbor`].
pub fn observed_kind(value: &JsonValue) -> ObservedKind {
    match value {
        JsonValue::Null => ObservedKind::Empty,
        JsonValue::String(_) => ObservedKind::String,
        JsonValue::Bool(_) => ObservedKind::Bool,
        JsonValue::Number(n) if n.is_i64() => ObservedKind::Int64,
        JsonValue::Number(_) => ObservedKind::Float64,
        JsonValue::Array(_) => ObservedKind::Array,
        JsonValue::Object(obj) if is_bytes_carrier(obj) && carrier_to_bytes(obj).is_some() => {
            ObservedKind::Bytes
        }
        JsonValue::Object(_) => ObservedKind::KvList,
    }
}

fn object_to_cbor_map(obj: &Map<String, JsonValue>) -> ciborium::Value {
    ciborium::Value::Map(
        obj.iter()
            .map(|(k, v)| (ciborium::Value::Text(k.clone()), json_to_cbor(v)))
            .collect(),
    )
}

fn json_to_cbor(value: &JsonValue) -> ciborium::Value {
    match value {
        JsonValue::Null => ciborium::Value::Null,
        JsonValue::Bool(b) => ciborium::Value::Bool(*b),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                ciborium::Value::Integer(i.into())
            } else if let Some(u) = n.as_u64() {
                ciborium::Value::Integer(u.into())
            } else {
                ciborium::Value::Float(n.as_f64().unwrap_or_default())
            }
        }
        JsonValue::String(s) => ciborium::Value::Text(s.clone()),
        JsonValue::Array(arr) => ciborium::Value::Array(arr.iter().map(json_to_cbor).collect()),
        JsonValue::Object(obj) if is_bytes_carrier(obj) => match carrier_to_bytes(obj) {
            Some(bytes) => ciborium::Value::Bytes(bytes),
            None => object_to_cbor_map(obj),
        },
        JsonValue::Object(obj) => object_to_cbor_map(obj),
    }
}

fn cbor_to_json(value: &ciborium::Value) -> JsonValue {
    match value {
        ciborium::Value::Null => JsonValue::Null,
        ciborium::Value::Bool(b) => JsonValue::Bool(*b),
        ciborium::Value::Integer(i) => {
            let i: i128 = (*i).into();
            i64::try_from(i)
                .map(JsonValue::from)
                .unwrap_or_else(|_| JsonValue::from(i as f64))
        }
        ciborium::Value::Float(f) => {
            serde_json::Number::from_f64(*f).map_or(JsonValue::Null, JsonValue::Number)
        }
        ciborium::Value::Text(s) => JsonValue::String(s.clone()),
        ciborium::Value::Bytes(b) => bytes_to_carrier(b),
        ciborium::Value::Array(arr) => JsonValue::Array(arr.iter().map(cbor_to_json).collect()),
        ciborium::Value::Map(map) => JsonValue::Object(
            map.iter()
                .filter_map(|(k, v)| match k {
                    ciborium::Value::Text(key) => Some((key.clone(), cbor_to_json(v))),
                    _ => None,
                })
                .collect(),
        ),
        _ => JsonValue::Null,
    }
}

/// Encode a row's residue entries as one CBOR document: a map from key to its
/// natively-typed CBOR value, with bytes stored as a CBOR byte string rather
/// than base64 text.
pub fn encode_residue<'a>(
    entries: impl IntoIterator<Item = (&'a str, &'a JsonValue)>,
) -> Result<Vec<u8>, ResidueError> {
    let document = ciborium::Value::Map(
        entries
            .into_iter()
            .map(|(k, v)| (ciborium::Value::Text(k.to_string()), json_to_cbor(v)))
            .collect(),
    );
    let mut buf = Vec::new();
    ciborium::into_writer(&document, &mut buf)?;
    Ok(buf)
}

/// Decode a residue document back into the JSON carrier form, the inverse of
/// [`encode_residue`].
pub fn decode_residue(bytes: &[u8]) -> Result<Map<String, JsonValue>, ResidueError> {
    let document: ciborium::Value = ciborium::from_reader(bytes)?;
    let ciborium::Value::Map(entries) = document else {
        return Err(ResidueError::NotAMap);
    };
    entries
        .into_iter()
        .map(|(k, v)| match k {
            ciborium::Value::Text(key) => Ok((key, cbor_to_json(&v))),
            _ => Err(ResidueError::NotAMap),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn residue_round_trips_every_off_type_shape() {
        let row = Map::from_iter([
            ("scalar".to_string(), json!("off-type string")),
            ("arr".to_string(), json!([1, "two", false, null])),
            ("kv".to_string(), json!({"a": 1, "b": "c"})),
            (
                "bytes".to_string(),
                json!({"$otlp_type": "bytes", "base64": "AP9h"}),
            ),
            ("nothing".to_string(), JsonValue::Null),
        ]);

        let encoded = encode_residue(row.iter().map(|(k, v)| (k.as_str(), v))).unwrap();

        let cbor: ciborium::Value = ciborium::from_reader(encoded.as_slice()).unwrap();
        let ciborium::Value::Map(entries) = &cbor else {
            panic!("expected a CBOR map");
        };
        let (_, bytes_value) = entries
            .iter()
            .find(|(k, _)| k == &ciborium::Value::Text("bytes".to_string()))
            .unwrap();
        assert_eq!(bytes_value, &ciborium::Value::Bytes(vec![0, 255, b'a']));

        let decoded = decode_residue(&encoded).unwrap();
        assert_eq!(decoded, row);
    }

    #[test]
    fn invalid_base64_bytes_carrier_stays_a_map_not_bytes() {
        let bad_carrier = json!({"$otlp_type": "bytes", "base64": "not valid base64!!"});
        assert_eq!(observed_kind(&bad_carrier), ObservedKind::KvList);
        assert!(matches!(
            json_to_cbor(&bad_carrier),
            ciborium::Value::Map(_)
        ));
    }
}
