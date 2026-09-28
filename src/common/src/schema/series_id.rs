//! `series_id`: the stable key of one metric series (metric name and type,
//! resource, scope, record attributes), shared by `metrics` rows and their
//! `metric_exemplars`.

use crate::ingest_dedup::fingerprint;

/// One length-prefixed input to [`fingerprint`]: either borrowed straight
/// from the caller's strings/keys, or owned where a value had to be
/// serialized to get its bytes.
enum Part<'a> {
    Borrowed(&'a [u8]),
    Owned(Vec<u8>),
}

impl Part<'_> {
    fn as_bytes(&self) -> &[u8] {
        match self {
            Part::Borrowed(b) => b,
            Part::Owned(v) => v,
        }
    }
}

/// What identifies a series besides its record attributes. Scope and type
/// are part of it: two libraries on one resource can emit the same metric
/// name, and OTLP allows a gauge and a sum to share one.
pub struct SeriesKey<'a> {
    pub metric_name: &'a str,
    pub metric_type: &'a str,
    pub resource_identity: Option<&'a str>,
    pub scope_name: Option<&'a str>,
    pub scope_version: Option<&'a str>,
}

/// Attributes are hashed as a canonical byte encoding so the id never
/// depends on iteration order: workspace-hack enables serde_json's
/// `preserve_order`, so an object -- top-level or nested inside an
/// attribute's value -- otherwise serializes in insertion order.
pub fn metric_series_id(
    key: &SeriesKey<'_>,
    attributes: &serde_json::Map<String, serde_json::Value>,
) -> String {
    let mut parts = vec![
        Part::Borrowed(key.metric_name.as_bytes()),
        Part::Borrowed(key.metric_type.as_bytes()),
    ];
    for optional in [key.resource_identity, key.scope_name, key.scope_version] {
        push_optional(&mut parts, optional);
    }
    push_canonical_map(&mut parts, attributes);

    let refs: Vec<&[u8]> = parts.iter().map(Part::as_bytes).collect();
    fingerprint(&refs).simple().to_string()
}

/// A presence byte first, so `None` and `Some("")` hash differently.
fn push_optional<'a>(parts: &mut Vec<Part<'a>>, value: Option<&'a str>) {
    match value {
        Some(value) => {
            parts.push(Part::Borrowed(&[1]));
            parts.push(Part::Borrowed(value.as_bytes()));
        }
        None => parts.push(Part::Borrowed(&[0])),
    }
}

/// Pushes `map`'s entries in sorted-key order, recursively canonicalizing
/// each value.
fn push_canonical_map<'a>(
    parts: &mut Vec<Part<'a>>,
    map: &'a serde_json::Map<String, serde_json::Value>,
) {
    let mut entries: Vec<(&str, &serde_json::Value)> =
        map.iter().map(|(k, v)| (k.as_str(), v)).collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    for (key, value) in entries {
        parts.push(Part::Borrowed(key.as_bytes()));
        push_canonical(parts, value);
    }
}

/// Pushes one JSON value's canonical byte encoding: an object's keys sorted
/// at every depth, an array's elements in order, a scalar as its own
/// serialization (the only case that needs an owned copy).
fn push_canonical<'a>(parts: &mut Vec<Part<'a>>, value: &'a serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            parts.push(Part::Borrowed(b"{"));
            push_canonical_map(parts, map);
            parts.push(Part::Borrowed(b"}"));
        }
        serde_json::Value::Array(items) => {
            parts.push(Part::Borrowed(b"["));
            for item in items {
                push_canonical(parts, item);
            }
            parts.push(Part::Borrowed(b"]"));
        }
        scalar => parts.push(Part::Owned(serde_json::to_vec(scalar).unwrap_or_default())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn id(
        metric_name: &str,
        resource_identity: Option<&str>,
        attributes: &serde_json::Map<String, serde_json::Value>,
    ) -> String {
        metric_series_id(
            &SeriesKey {
                metric_name,
                metric_type: "gauge",
                resource_identity,
                scope_name: Some("lib"),
                scope_version: None,
            },
            attributes,
        )
    }

    fn attrs(pairs: &[(&str, serde_json::Value)]) -> serde_json::Map<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn same_inputs_give_the_same_id() {
        let a = attrs(&[("host", json!("a")), ("region", json!("eu"))]);
        assert_eq!(
            id("cpu.usage", Some("res-1"), &a),
            id("cpu.usage", Some("res-1"), &a)
        );
    }

    #[test]
    fn attribute_order_is_irrelevant() {
        let forward = attrs(&[("host", json!("a")), ("region", json!("eu"))]);
        let reversed = attrs(&[("region", json!("eu")), ("host", json!("a"))]);
        assert_eq!(
            id("cpu.usage", Some("res-1"), &forward),
            id("cpu.usage", Some("res-1"), &reversed)
        );
    }

    /// `preserve_order` means a nested object's key order is otherwise
    /// significant to `serde_json::to_vec`; the id must not depend on it.
    #[test]
    fn nested_object_key_order_is_irrelevant() {
        let forward = attrs(&[("k8s", json!({"pod": "a", "namespace": "prod"}))]);
        let reversed = attrs(&[("k8s", json!({"namespace": "prod", "pod": "a"}))]);
        assert_eq!(
            id("cpu.usage", Some("res-1"), &forward),
            id("cpu.usage", Some("res-1"), &reversed)
        );
    }

    #[test]
    fn different_metric_name_gives_a_different_id() {
        let a = attrs(&[("host", json!("a"))]);
        assert_ne!(
            id("cpu.usage", Some("res-1"), &a),
            id("mem.usage", Some("res-1"), &a)
        );
    }

    #[test]
    fn different_attributes_give_a_different_id() {
        let a = attrs(&[("host", json!("a"))]);
        let b = attrs(&[("host", json!("b"))]);
        assert_ne!(
            id("cpu.usage", Some("res-1"), &a),
            id("cpu.usage", Some("res-1"), &b)
        );
    }

    #[test]
    fn different_resource_gives_a_different_id() {
        let a = attrs(&[("host", json!("a"))]);
        assert_ne!(
            id("cpu.usage", Some("res-1"), &a),
            id("cpu.usage", Some("res-2"), &a)
        );
        assert_ne!(
            id("cpu.usage", None, &a),
            id("cpu.usage", Some("res-1"), &a)
        );
    }

    /// A missing resource identity must not hash the same as an explicitly
    /// empty one.
    #[test]
    fn absent_resource_identity_differs_from_an_empty_one() {
        let a = attrs(&[("host", json!("a"))]);
        assert_ne!(id("cpu.usage", None, &a), id("cpu.usage", Some(""), &a));
    }

    #[test]
    fn metric_type_and_scope_are_part_of_the_series() {
        let a = attrs(&[("host", json!("a"))]);
        let key = |metric_type, scope_name, scope_version| SeriesKey {
            metric_name: "requests",
            metric_type,
            resource_identity: Some("res-1"),
            scope_name,
            scope_version,
        };
        let base = metric_series_id(&key("gauge", Some("lib"), None), &a);
        for other in [
            key("sum", Some("lib"), None),
            key("gauge", Some("other-lib"), None),
            key("gauge", Some("lib"), Some("1.0")),
        ] {
            assert_ne!(base, metric_series_id(&other, &a));
        }
    }
}
