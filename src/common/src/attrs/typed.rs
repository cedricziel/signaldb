//! Codec between the JSON-in-Utf8 attribute carrier used on the Flight/WAL
//! wire and the CBOR residue document used by the typed attribute layout
//! (see [`crate::schema::typed_attributes`]) for anything that has no typed
//! home (off-type scalars, arrays, kvlists, bytes).
//!
//! See [`crate::attrs`] for the legacy single-map/JSON-to-strings decoder
//! this layout replaces.

use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, Float64Array,
    Float64Builder, Int64Array, Int64Builder, MapArray, MapBuilder, MapFieldNames, RecordBatch,
    StringArray, StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field};
use serde_json::{Map, Value as JsonValue};

use crate::flight::conversion::conversion_common::{
    bytes_to_carrier, carrier_to_bytes, is_bytes_carrier,
};
use crate::schema::type_authority::{CanonicalType, ObservedKind, Placement};
use crate::schema::typed_attributes;

#[derive(Debug, thiserror::Error)]
pub enum ResidueError {
    #[error("failed to encode residue CBOR: {0}")]
    Encode(#[from] ciborium::ser::Error<std::io::Error>),
    #[error("failed to decode residue CBOR: {0}")]
    Decode(#[from] ciborium::de::Error<std::io::Error>),
    #[error("residue document is not a CBOR map")]
    NotAMap,
}

/// Errors from [`TypedAttrBuilder`] or [`decode_container`].
#[derive(Debug, thiserror::Error)]
pub enum TypedAttrError {
    #[error("expected a Utf8 column for the typed-attribute source")]
    NotAStringColumn,
    #[error("field '{0}' is not a typed-attribute field (map or residue)")]
    NotATypedAttrField(String),
    #[error("column '{0}' is missing from the batch")]
    MissingColumn(String),
    #[error("column '{0}' has an unexpected Arrow type for its typed-attribute role")]
    UnexpectedColumnType(String),
    #[error(transparent)]
    Residue(#[from] ResidueError),
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
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

fn cast_if_needed(array: ArrayRef, target_type: &DataType) -> Result<ArrayRef, TypedAttrError> {
    if array.data_type() == target_type {
        Ok(array)
    } else {
        Ok(datafusion::arrow::compute::cast(&array, target_type)?)
    }
}

fn map_field_names(field: &Field) -> Result<MapFieldNames, TypedAttrError> {
    let field_err = || TypedAttrError::NotATypedAttrField(field.name().clone());
    let DataType::Map(entry_field, _) = field.data_type() else {
        return Err(field_err());
    };
    let DataType::Struct(kv_fields) = entry_field.data_type() else {
        return Err(field_err());
    };
    Ok(MapFieldNames {
        entry: entry_field.name().clone(),
        key: kv_fields[0].name().clone(),
        value: kv_fields[1].name().clone(),
    })
}

/// Splits attribute rows into the five typed-attribute columns, one row at a time.
pub struct TypedAttrBuilder {
    str_builder: MapBuilder<StringBuilder, StringBuilder>,
    int_builder: MapBuilder<StringBuilder, Int64Builder>,
    double_builder: MapBuilder<StringBuilder, Float64Builder>,
    bool_builder: MapBuilder<StringBuilder, BooleanBuilder>,
    residue_builder: BinaryBuilder,
    // `finish` casts each map array onto these exact types, since the map
    // builders don't carry the target fields' nested Parquet metadata.
    map_types: [DataType; 4],
}

impl TypedAttrBuilder {
    /// `fields` must be the container's five typed-attribute fields, in
    /// [`typed_attributes::typed_fields`] order, from the target table's
    /// real schema.
    pub fn new(fields: &[Field; 5]) -> Result<Self, TypedAttrError> {
        let residue_field = &fields[4];
        if residue_field.data_type() != &DataType::Binary {
            return Err(TypedAttrError::NotATypedAttrField(
                residue_field.name().clone(),
            ));
        }
        Ok(Self {
            str_builder: MapBuilder::new(
                Some(map_field_names(&fields[0])?),
                StringBuilder::new(),
                StringBuilder::new(),
            ),
            int_builder: MapBuilder::new(
                Some(map_field_names(&fields[1])?),
                StringBuilder::new(),
                Int64Builder::new(),
            ),
            double_builder: MapBuilder::new(
                Some(map_field_names(&fields[2])?),
                StringBuilder::new(),
                Float64Builder::new(),
            ),
            bool_builder: MapBuilder::new(
                Some(map_field_names(&fields[3])?),
                StringBuilder::new(),
                BooleanBuilder::new(),
            ),
            residue_builder: BinaryBuilder::new(),
            map_types: [
                fields[0].data_type().clone(),
                fields[1].data_type().clone(),
                fields[2].data_type().clone(),
                fields[3].data_type().clone(),
            ],
        })
    }

    fn append_map_validity(&mut self, valid: bool) -> Result<(), TypedAttrError> {
        self.str_builder.append(valid)?;
        self.int_builder.append(valid)?;
        self.double_builder.append(valid)?;
        self.bool_builder.append(valid)?;
        Ok(())
    }

    /// Appends one row. `row` of `None` produces a null in all five columns.
    /// `place` decides each key's canonical home; a home whose
    /// [`CanonicalType`] doesn't match the value's [`ObservedKind`] is
    /// residue, never coerced.
    pub fn append_row(
        &mut self,
        row: Option<&Map<String, JsonValue>>,
        mut place: impl FnMut(&str, ObservedKind) -> Placement,
    ) -> Result<(), TypedAttrError> {
        let Some(row) = row else {
            self.append_map_validity(false)?;
            self.residue_builder.append_null();
            return Ok(());
        };

        let mut residue = Vec::new();
        for (key, value) in row {
            let Placement::Home(home) = place(key, observed_kind(value)) else {
                residue.push((key.as_str(), value));
                continue;
            };
            match (home, value) {
                (CanonicalType::String, JsonValue::String(s)) => {
                    self.str_builder.keys().append_value(key);
                    self.str_builder.values().append_value(s);
                }
                (CanonicalType::Int64, JsonValue::Number(n)) if n.is_i64() => {
                    self.int_builder.keys().append_value(key);
                    self.int_builder.values().append_option(n.as_i64());
                }
                (CanonicalType::Float64, JsonValue::Number(n)) if !n.is_i64() => {
                    self.double_builder.keys().append_value(key);
                    self.double_builder.values().append_option(n.as_f64());
                }
                (CanonicalType::Bool, JsonValue::Bool(b)) => {
                    self.bool_builder.keys().append_value(key);
                    self.bool_builder.values().append_value(*b);
                }
                _ => residue.push((key.as_str(), value)),
            }
        }
        self.append_map_validity(true)?;
        if residue.is_empty() {
            self.residue_builder.append_null();
        } else {
            self.residue_builder.append_value(encode_residue(residue)?);
        }
        Ok(())
    }

    /// Returns the five typed-attribute arrays, cast onto the exact field
    /// types passed to [`Self::new`].
    pub fn finish(mut self) -> Result<[ArrayRef; 5], TypedAttrError> {
        let maps: [ArrayRef; 4] = [
            Arc::new(self.str_builder.finish()),
            Arc::new(self.int_builder.finish()),
            Arc::new(self.double_builder.finish()),
            Arc::new(self.bool_builder.finish()),
        ];
        let [str_type, int_type, double_type, bool_type] = &self.map_types;
        let [str_array, int_array, double_array, bool_array] = maps;
        Ok([
            cast_if_needed(str_array, str_type)?,
            cast_if_needed(int_array, int_type)?,
            cast_if_needed(double_array, double_type)?,
            cast_if_needed(bool_array, bool_type)?,
            Arc::new(self.residue_builder.finish()),
        ])
    }
}

/// Parses each row of a JSON-in-Utf8 attribute column (the Flight/WAL wire
/// carrier) into its object form. A null, unparseable, or non-object row
/// becomes `None` — the convention every typed-attribute consumer shares.
pub fn parse_json_object_rows(strings: &StringArray) -> Vec<Option<Map<String, JsonValue>>> {
    (0..strings.len())
        .map(|i| {
            if strings.is_null(i) {
                return None;
            }
            match serde_json::from_str::<JsonValue>(strings.value(i)) {
                Ok(JsonValue::Object(map)) => Some(map),
                _ => None,
            }
        })
        .collect()
}

/// Splits a JSON-in-Utf8 attribute column (the Flight/WAL wire carrier)
/// into the five typed-attribute columns. A null, unparseable, or non-object
/// row becomes a null row.
pub fn split_json_column(
    column: &dyn Array,
    fields: &[Field; 5],
    mut place: impl FnMut(&str, ObservedKind) -> Placement,
) -> Result<[ArrayRef; 5], TypedAttrError> {
    let strings = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(TypedAttrError::NotAStringColumn)?;
    let mut builder = TypedAttrBuilder::new(fields)?;
    for row in parse_json_object_rows(strings) {
        builder.append_row(row.as_ref(), &mut place)?;
    }
    builder.finish()
}

fn typed_column<'a, A: Array + 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a A, TypedAttrError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| TypedAttrError::MissingColumn(name.to_string()))?
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| TypedAttrError::UnexpectedColumnType(name.to_string()))
}

/// Merges one row of a typed-home map into `doc`, converting each value to its
/// carrier JSON form. Returns whether the row's map was non-null.
fn merge_typed_row<V: Array + 'static>(
    map: &MapArray,
    name: &str,
    row: usize,
    doc: &mut Map<String, JsonValue>,
    to_json: impl Fn(&V, usize) -> JsonValue,
) -> Result<bool, TypedAttrError> {
    if map.is_null(row) {
        return Ok(false);
    }
    let type_err = || TypedAttrError::UnexpectedColumnType(name.to_string());
    let entries = map.value(row);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(type_err)?;
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<V>()
        .ok_or_else(type_err)?;
    for j in 0..entries.len() {
        if !keys.is_null(j) && !values.is_null(j) {
            doc.insert(keys.value(j).to_string(), to_json(values, j));
        }
    }
    Ok(true)
}

/// Decodes a container's five typed-attribute columns back into per-row
/// carrier documents. A row where all five columns are null decodes to `None`.
pub fn decode_container(
    batch: &RecordBatch,
    container: &str,
) -> Result<Vec<Option<Map<String, JsonValue>>>, TypedAttrError> {
    let [str_name, int_name, double_name, bool_name, residue_name] =
        typed_attributes::typed_columns(container);
    let str_map = typed_column::<MapArray>(batch, &str_name)?;
    let int_map = typed_column::<MapArray>(batch, &int_name)?;
    let double_map = typed_column::<MapArray>(batch, &double_name)?;
    let bool_map = typed_column::<MapArray>(batch, &bool_name)?;
    let residue_col = typed_column::<BinaryArray>(batch, &residue_name)?;

    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut doc = Map::new();
        let mut present =
            merge_typed_row(str_map, &str_name, row, &mut doc, |v: &StringArray, j| {
                JsonValue::String(v.value(j).to_string())
            })?;
        present |= merge_typed_row(int_map, &int_name, row, &mut doc, |v: &Int64Array, j| {
            JsonValue::from(v.value(j))
        })?;
        present |= merge_typed_row(
            double_map,
            &double_name,
            row,
            &mut doc,
            |v: &Float64Array, j| {
                serde_json::Number::from_f64(v.value(j)).map_or(JsonValue::Null, JsonValue::Number)
            },
        )?;
        present |= merge_typed_row(
            bool_map,
            &bool_name,
            row,
            &mut doc,
            |v: &BooleanArray, j| JsonValue::Bool(v.value(j)),
        )?;
        if !residue_col.is_null(row) {
            doc.extend(decode_residue(residue_col.value(row))?);
            present = true;
        }
        out.push(present.then_some(doc));
    }
    Ok(out)
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

    use crate::schema::SCHEMA_DEFINITIONS;
    use datafusion::arrow::datatypes::Fields;

    fn span_attribute_fields() -> [Field; 5] {
        let resolved = SCHEMA_DEFINITIONS
            .resolve_trace_schema("physical-v5")
            .unwrap();
        let iceberg_schema = resolved.to_iceberg_schema().unwrap();
        let arrow_fields: Fields = iceberg_schema.fields().try_into().unwrap();
        typed_attributes::typed_columns("span_attributes").map(|name| {
            arrow_fields
                .iter()
                .find(|f| f.name() == &name)
                .unwrap_or_else(|| panic!("missing field {name}"))
                .as_ref()
                .clone()
        })
    }

    fn home_int(_key: &str, observed: ObservedKind) -> Placement {
        if observed == ObservedKind::Int64 {
            Placement::Home(CanonicalType::Int64)
        } else {
            Placement::Residue { off_type: false }
        }
    }

    fn build_batch(
        fields: &[Field; 5],
        rows: &[Option<Map<String, JsonValue>>],
        mut place: impl FnMut(&str, ObservedKind) -> Placement,
    ) -> RecordBatch {
        let mut builder = TypedAttrBuilder::new(fields).unwrap();
        for row in rows {
            builder.append_row(row.as_ref(), &mut place).unwrap();
        }
        let arrays = builder.finish().unwrap();
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields.to_vec()));
        RecordBatch::try_new(schema, arrays.to_vec()).unwrap()
    }

    #[test]
    fn produced_array_types_match_the_real_schema_fields_exactly() {
        let fields = span_attribute_fields();
        let batch = build_batch(
            &fields,
            &[Some(Map::from_iter([("k".to_string(), json!(1))]))],
            home_int,
        );
        for (i, field) in fields.iter().enumerate() {
            assert_eq!(batch.column(i).data_type(), field.data_type(), "column {i}");
        }
    }

    #[test]
    fn canonical_typed_values_land_in_their_home_and_read_back_typed() {
        let fields = span_attribute_fields();
        let row = Map::from_iter([
            ("i".to_string(), json!(42)),
            ("d".to_string(), json!(1.5)),
            ("b".to_string(), json!(true)),
            ("s".to_string(), json!("hello")),
        ]);
        let place = |key: &str, observed: ObservedKind| match key {
            "i" if observed == ObservedKind::Int64 => Placement::Home(CanonicalType::Int64),
            "d" if observed == ObservedKind::Float64 => Placement::Home(CanonicalType::Float64),
            "b" if observed == ObservedKind::Bool => Placement::Home(CanonicalType::Bool),
            "s" if observed == ObservedKind::String => Placement::Home(CanonicalType::String),
            _ => Placement::Residue { off_type: false },
        };
        let batch = build_batch(&fields, &[Some(row.clone())], place);
        let int_map = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
        assert!(!int_map.is_null(0));
        let decoded = decode_container(&batch, "span_attributes").unwrap();
        assert_eq!(decoded, vec![Some(row)]);
    }

    #[test]
    fn off_type_and_non_scalar_values_round_trip_through_residue() {
        let fields = span_attribute_fields();
        let bytes = json!({"$otlp_type": "bytes", "base64": "AP9h"});
        let row = Map::from_iter([
            ("off_type".to_string(), json!("not-an-int")),
            ("arr".to_string(), json!([1, "two", false])),
            ("kv".to_string(), json!({"a": 1, "b": "c"})),
            ("bytes".to_string(), bytes.clone()),
            ("nothing".to_string(), JsonValue::Null),
        ]);
        let place = |key: &str, _observed: ObservedKind| {
            if key == "off_type" {
                Placement::Home(CanonicalType::Int64)
            } else {
                Placement::Residue { off_type: false }
            }
        };
        let batch = build_batch(&fields, &[Some(row.clone())], place);
        let decoded = decode_container(&batch, "span_attributes").unwrap();
        assert_eq!(decoded, vec![Some(row)]);
    }

    #[test]
    fn mismatched_home_goes_to_residue_never_coerced() {
        let fields = span_attribute_fields();
        // The callback claims an Int64 home, but the value is a string.
        let place = |_key: &str, _observed: ObservedKind| Placement::Home(CanonicalType::Int64);
        let row = Map::from_iter([("k".to_string(), json!("a string"))]);
        let batch = build_batch(&fields, &[Some(row.clone())], place);

        let int_map = batch.column(1).as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(int_map.value_length(0), 0);
        let decoded = decode_container(&batch, "span_attributes").unwrap();
        assert_eq!(decoded, vec![Some(row)]);
    }

    #[test]
    fn null_row_and_row_without_residue_conventions() {
        let fields = span_attribute_fields();
        let rows = vec![None, Some(Map::from_iter([("i".to_string(), json!(1))]))];
        let batch = build_batch(&fields, &rows, home_int);
        assert!((0..5).all(|i| batch.column(i).is_null(0)), "null row");
        let residue_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(residue_col.is_null(1), "typed-home-only row has no residue");

        let decoded = decode_container(&batch, "span_attributes").unwrap();
        assert_eq!(decoded, vec![None, rows[1].clone()]);
    }

    #[test]
    fn unparseable_json_column_rows_become_null_rows() {
        let fields = span_attribute_fields();
        let strings = StringArray::from(vec![
            Some("{\"k\": 1}"),
            Some("not json"),
            Some("[1,2]"),
            None,
        ]);
        let arrays = split_json_column(&strings, &fields, home_int).unwrap();
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(fields.to_vec()));
        let batch = RecordBatch::try_new(schema, arrays.to_vec()).unwrap();

        let decoded = decode_container(&batch, "span_attributes").unwrap();
        assert_eq!(
            decoded,
            vec![
                Some(Map::from_iter([("k".to_string(), json!(1))])),
                None,
                None,
                None,
            ]
        );
    }
}
