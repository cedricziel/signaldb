//! Series label sets (D11): the canonical `__labels` encoding of a metric
//! Series and the `series_labels` UDF that derives it from a point's columns.
//!
//! A label set is a JSON object with string values, keys sorted, compact
//! (`serde_json` over a `BTreeMap`), so equal label sets are equal strings.

use std::collections::BTreeMap;
use std::sync::Arc;

use common::attrs::typed::decode_typed_arrays;
use common::schema::typed_attributes::typed_columns;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, MapArray, StringBuilder, StructArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::functions::core::expr_fn::named_struct;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, lit,
};
use datafusion::prelude::ident;
use datafusion::scalar::ScalarValue;
use serde_json::{Map, Value as JsonValue};

pub(crate) type LabelSet = BTreeMap<String, String>;

pub(crate) const METRIC_NAME: &str = "metric.name";
pub(crate) const SERVICE_NAME: &str = "service.name";
/// The instrumentation scope's name and version, spelled as Prometheus' OTLP
/// translation does (`otel_scope_name`), so two scopes' series stay apart.
pub(crate) const SCOPE_NAME: &str = "otel.scope.name";
pub(crate) const SCOPE_VERSION: &str = "otel.scope.version";
const METRIC_PREFIX: &str = "metric.";
const RESOURCE_PREFIX: &str = "resource.";
/// The metrics source's point-attribute qualifier (its `SourcePlan` prefix).
pub(crate) const POINT_PREFIX: &str = "point.";

pub(crate) fn encode(labels: &LabelSet) -> Result<String> {
    serde_json::to_string(labels).map_err(|e| DataFusionError::External(Box::new(e)))
}

/// A string argument as a `Utf8` array of `rows` rows.
pub(super) fn utf8_array(arg: &ColumnarValue, rows: usize) -> Result<ArrayRef> {
    Ok(cast(&arg.to_array(rows)?, &DataType::Utf8)?)
}

/// The label a point attribute is emitted under: its own key, unless that key
/// would collide with the `metric.*`/`resource.*`/`service.name`/scope
/// namespace (or starts with the qualifier itself), in which case it is
/// scope-qualified.
fn point_label(key: &str) -> String {
    let collides = [SERVICE_NAME, SCOPE_NAME, SCOPE_VERSION].contains(&key)
        || [METRIC_PREFIX, RESOURCE_PREFIX, POINT_PREFIX]
            .iter()
            .any(|p| key.starts_with(p));
    if collides {
        format!("{POINT_PREFIX}{key}")
    } else {
        key.to_string()
    }
}

/// An attribute value as a label value: scalars as their text, structured
/// values (arrays, key-value lists, bytes) as compact JSON; null and the
/// empty string are absent, as an empty label value is in PromQL.
fn render(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::Null => None,
        JsonValue::String(s) if s.is_empty() => None,
        JsonValue::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// What one metric series' label set is derived from.
#[derive(Default)]
pub(crate) struct SeriesIdentity<'a> {
    pub metric_name: Option<&'a str>,
    pub service_name: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub scope_version: Option<&'a str>,
    pub resource: Option<&'a Map<String, JsonValue>>,
    pub attrs: Option<&'a Map<String, JsonValue>>,
}

/// The full label set of one metric series.
pub(crate) fn series_label_set(id: &SeriesIdentity<'_>) -> LabelSet {
    let mut labels = LabelSet::new();
    let mut put = |key: String, value: Option<&str>| {
        if let Some(value) = value.filter(|v| !v.is_empty()) {
            labels.insert(key, value.to_string());
        }
    };
    for (key, value) in id.resource.into_iter().flatten() {
        let label = if key == SERVICE_NAME {
            SERVICE_NAME.to_string()
        } else {
            format!("{RESOURCE_PREFIX}{key}")
        };
        put(label, render(value).as_deref());
    }
    put(SERVICE_NAME.to_string(), id.service_name);
    put(METRIC_NAME.to_string(), id.metric_name);
    put(SCOPE_NAME.to_string(), id.scope_name);
    put(SCOPE_VERSION.to_string(), id.scope_version);
    for (key, value) in id.attrs.into_iter().flatten() {
        put(point_label(key), render(value).as_deref());
    }
    labels
}

/// `named_struct` of `container`'s five typed columns, the bag argument of
/// `series_labels`; a NULL when the scan has no such typed container.
pub(crate) fn bag_arg(container: &str, has_container: bool) -> Expr {
    if !has_container {
        return lit(ScalarValue::Null);
    }
    let names = ["str", "int", "double", "bool", "residue"];
    let args = names
        .iter()
        .zip(typed_columns(container))
        .flat_map(|(name, column)| [lit(*name), ident(column)])
        .collect();
    named_struct(args)
}

/// `series_labels(metric_name, service_name, scope_name, scope_version,
/// resource_bag, attrs_bag)`: the canonical label set of each row's series;
/// bags as built by [`bag_arg`].
pub(crate) fn series_labels_udf() -> ScalarUDF {
    ScalarUDF::new_from_impl(SeriesLabels {
        signature: Signature::any(6, Volatility::Immutable),
    })
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct SeriesLabels {
    signature: Signature,
}

/// Decode one typed attribute bag argument (or a NULL) into per-row objects.
fn decode_bag(arg: &ColumnarValue, rows: usize) -> Result<Vec<Option<Map<String, JsonValue>>>> {
    let array = arg.to_array(rows)?;
    if array.data_type() == &DataType::Null {
        return Ok(vec![None; rows]);
    }
    let bad = |what: &str| DataFusionError::Plan(format!("series_labels: {what}"));
    let bag = array
        .as_any()
        .downcast_ref::<StructArray>()
        .filter(|s| s.num_columns() == 5)
        .ok_or_else(|| bad("expected a typed attribute bag"))?;
    let map = |i: usize| -> Result<&MapArray> {
        bag.column(i)
            .as_map_opt()
            .ok_or_else(|| bad("bag home is not a map"))
    };
    let residue = cast(bag.column(4), &DataType::Binary)?;
    let rows_out = decode_typed_arrays(
        map(0)?,
        map(1)?,
        map(2)?,
        map(3)?,
        residue.as_binary::<i32>(),
    )
    .map_err(|e| DataFusionError::External(Box::new(e)))?;
    if rows_out.len() != rows {
        return Err(DataFusionError::Internal(format!(
            "series_labels: decoded {} bag rows for {rows}",
            rows_out.len()
        )));
    }
    Ok(rows_out)
}

impl ScalarUDFImpl for SeriesLabels {
    fn name(&self) -> &str {
        "series_labels"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let rows = args.number_rows;
        let strings = args.args[..4]
            .iter()
            .map(|arg| utf8_array(arg, rows))
            .collect::<Result<Vec<_>>>()?;
        let text = |i: usize, row: usize| {
            let a = strings[i].as_string::<i32>();
            a.is_valid(row).then(|| a.value(row))
        };
        let resource = decode_bag(&args.args[4], rows)?;
        let attrs = decode_bag(&args.args[5], rows)?;
        let mut out = StringBuilder::new();
        for row in 0..rows {
            let set = series_label_set(&SeriesIdentity {
                metric_name: text(0, row),
                service_name: text(1, row),
                scope_name: text(2, row),
                scope_version: text(3, row),
                resource: resource[row].as_ref(),
                attrs: attrs[row].as_ref(),
            });
            out.append_value(encode(&set)?);
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{RecordBatch, StringArray};
    use datafusion::prelude::SessionContext;
    use serde_json::json;

    use super::*;

    fn obj(v: JsonValue) -> Map<String, JsonValue> {
        v.as_object().cloned().unwrap_or_default()
    }

    fn labels(id: SeriesIdentity<'_>) -> JsonValue {
        serde_json::to_value(series_label_set(&id)).unwrap()
    }

    #[test]
    fn a_series_carries_name_service_scope_resource_and_point_labels_in_key_order() {
        let resource = obj(json!({"host.name": "h1", "service.name": "api"}));
        let attrs = obj(json!({"code": 200, "ok": true, "ratio": 0.5, "tags": ["a", 1]}));
        let set = series_label_set(&SeriesIdentity {
            metric_name: Some("http.requests"),
            service_name: Some("api"),
            scope_name: Some("io.otel.http"),
            scope_version: Some("1.2"),
            resource: Some(&resource),
            attrs: Some(&attrs),
        });
        assert_eq!(
            encode(&set).unwrap(),
            r#"{"code":"200","metric.name":"http.requests","ok":"true","otel.scope.name":"io.otel.http","otel.scope.version":"1.2","ratio":"0.5","resource.host.name":"h1","service.name":"api","tags":"[\"a\",1]"}"#
        );
    }

    #[test]
    fn colliding_point_attributes_are_scope_qualified() {
        let resource = obj(json!({"k": "res"}));
        let attrs = obj(json!({
            "metric.name": "a", "metric.unit": "b", "resource.k": "c",
            "service.name": "d", "point.x": "e", "x": "f", "metrics": "g",
            "otel.scope.name": "h", "null": null
        }));
        let got = labels(SeriesIdentity {
            metric_name: Some("m"),
            service_name: Some("svc"),
            resource: Some(&resource),
            attrs: Some(&attrs),
            ..Default::default()
        });
        let want = json!({
            "metric.name": "m", "service.name": "svc", "resource.k": "res",
            "point.metric.name": "a", "point.metric.unit": "b", "point.resource.k": "c",
            "point.service.name": "d", "point.point.x": "e", "x": "f", "metrics": "g",
            "point.otel.scope.name": "h"
        });
        assert_eq!(got, want);
    }

    /// Decoding every label back recovers exactly the resource and point
    /// inputs, so no two distinct attribute sets share an encoding.
    #[test]
    fn the_encoding_is_injective_over_colliding_keys() {
        let keys = [
            "x",
            "metric.name",
            "metric.x",
            "resource.x",
            "point.x",
            "point.point.x",
            "service.name",
            "otel.scope.name",
        ];
        for (i, key) in keys.iter().enumerate() {
            let resource = obj(json!({ *key: format!("r{i}") }));
            let point = obj(json!({ *key: format!("p{i}") }));
            let set = series_label_set(&SeriesIdentity {
                resource: Some(&resource),
                attrs: Some(&point),
                ..Default::default()
            });
            let (mut back_resource, mut back_point) = (Map::new(), Map::new());
            for (label, value) in set {
                if label == SERVICE_NAME {
                    back_resource.insert(label, json!(value));
                } else if let Some(k) = label.strip_prefix(RESOURCE_PREFIX) {
                    back_resource.insert(k.to_string(), json!(value));
                } else {
                    let k = label.strip_prefix(POINT_PREFIX).unwrap_or(&label);
                    back_point.insert(k.to_string(), json!(value));
                }
            }
            assert_eq!((back_resource, back_point), (resource, point), "{key}");
        }
    }

    #[test]
    fn the_service_name_column_wins_and_empty_values_are_absent() {
        let resource = obj(json!({"service.name": "from-resource", "empty": ""}));
        let attrs = obj(json!({"blank": ""}));
        let id = |service_name| SeriesIdentity {
            service_name,
            scope_name: Some(""),
            resource: Some(&resource),
            attrs: Some(&attrs),
            ..Default::default()
        };
        assert_eq!(labels(id(Some("col"))), json!({"service.name": "col"}));
        assert_eq!(labels(id(None)), json!({"service.name": "from-resource"}));
    }

    #[tokio::test]
    async fn series_labels_reads_typed_bags() {
        let mut columns: Vec<(&str, ArrayRef)> = ["metric_name", "service_name", "scope_name"]
            .iter()
            .map(|c| (*c, Arc::new(StringArray::from(vec![*c])) as ArrayRef))
            .collect();
        let bags = [
            ("resource_attributes", json!({"host": "h", "n": 3})),
            ("attributes", json!({"metric.name": "p", "code": 500})),
        ];
        let names = bags
            .each_ref()
            .map(|(container, _)| typed_columns(container));
        for ((container, row), names) in bags.iter().zip(&names) {
            let (_, arrays) = common::testing::typed_attribute_columns_from(
                "metrics",
                "physical-v4",
                container,
                &[Some(obj(row.clone()))],
            );
            columns.extend(names.iter().map(String::as_str).zip(arrays));
        }
        let ctx = SessionContext::new();
        ctx.register_batch("m", RecordBatch::try_from_iter(columns).unwrap())
            .unwrap();
        let call = |present: bool| {
            series_labels_udf().call(vec![
                ident("metric_name"),
                ident("service_name"),
                ident("scope_name"),
                lit(ScalarValue::Utf8(None)),
                bag_arg("resource_attributes", present),
                bag_arg("attributes", present),
            ])
        };
        let out = ctx.table("m").await.unwrap();
        let out = out.select(vec![call(true), call(false)]).unwrap();
        let out = out.collect().await.unwrap();
        let col = |i: usize| out[0].column(i).as_string::<i32>().value(0).to_string();
        assert_eq!(
            col(0),
            r#"{"code":"500","metric.name":"metric_name","otel.scope.name":"scope_name","point.metric.name":"p","resource.host":"h","resource.n":"3","service.name":"service_name"}"#
        );
        assert_eq!(
            col(1),
            r#"{"metric.name":"metric_name","otel.scope.name":"scope_name","service.name":"service_name"}"#
        );
    }
}
