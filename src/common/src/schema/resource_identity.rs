//! The `resource.identity` logical field: a stable digest of an OTLP
//! resource's attribute set.
//!
//! An OTLP resource has no id of its own — only an attribute set (plus a
//! schema URL, which describes the attributes' shape, not the resource).
//! `resource_identity` gives every producer (acceptor, writer, and any
//! future entity table) the same digest for "the same resource", so a
//! future join on `resource.identity` sees one row per distinct attribute
//! set rather than per span/log/metric.
//!
//! ## Canonical form
//!
//! Only the attribute map participates — `schema_url` and anything else
//! carried alongside it are not identity. The map is serialised to a
//! canonical JSON string:
//!
//! - object keys are sorted lexicographically (byte-wise `str` ordering),
//!   recursively at every nesting level, so insertion order never affects
//!   the result;
//! - array element order is preserved — arrays are ordered data, not sets;
//! - no whitespace;
//! - strings and numbers use serde_json's default escaping and formatting.
//!
//! The sort is done explicitly (as a `Vec` sort, not by relying on
//! `serde_json::Map`'s default `BTreeMap` backing) so enabling
//! `serde_json`'s `preserve_order` feature anywhere in the workspace can
//! never silently change a digest.
//!
//! ## Digest
//!
//! SHA-256 over the canonical form's UTF-8 bytes, truncated to the first 16
//! bytes, rendered as 32 lowercase hex characters. An empty attribute map is
//! a valid resource (the canonical form is `{}`) and gets a real digest, not
//! a sentinel.
//!
//! This is a contract other producers rely on: changing the canonical form
//! or the digest scheme changes every `resource.identity` value already
//! written.

use sha2::{Digest, Sha256};

/// Physical column name carrying the resource identity on every signal table.
pub const RESOURCE_IDENTITY_COLUMN: &str = "resource_identity";

/// Identity of an OTLP resource: a stable digest of its attribute set.
///
/// See the module documentation for the canonical form and digest scheme.
pub fn resource_identity(attributes: &serde_json::Map<String, serde_json::Value>) -> String {
    let mut canonical = String::new();
    write_canonical_object(attributes, &mut canonical);
    digest_hex(&canonical)
}

/// Same, from the `resource_json` string the acceptor puts in v1 Flight
/// batches. Accepts both shapes that exist in the codebase: a flat
/// attribute object, and the envelope `{"attributes": {...}, "schema_url":
/// "...", "dropped_attributes_count": ...}` — `schema_url` and
/// `dropped_attributes_count` are never part of the identity either way.
///
/// The object is the envelope only when every one of its own keys is one of
/// [`ENVELOPE_KEYS`] and `attributes` holds an object; otherwise it is
/// treated as flat and hashed whole, `attributes` key and all. That
/// disqualification matters: a flat resource can legitimately carry its own
/// `attributes` attribute (e.g. `{"attributes":{"tenant":"a"},
/// "service.name":"checkout"}`) alongside unrelated keys like
/// `service.name`, and only the presence of a sibling key outside
/// [`ENVELOPE_KEYS`] tells the two shapes apart — checking for an
/// `attributes` key alone would treat that flat map as an envelope, drop
/// `service.name`, and collide it with every other flat map sharing the
/// same nested `attributes` value.
///
/// Returns `None` when `resource_json` is not valid JSON or is not a JSON
/// object.
pub fn resource_identity_from_json(resource_json: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(resource_json).ok()?;
    let obj = value.as_object()?;

    let is_envelope = obj.keys().all(|key| ENVELOPE_KEYS.contains(&key.as_str()));
    let envelope_attributes = is_envelope
        .then(|| obj.get("attributes").and_then(|value| value.as_object()))
        .flatten();

    Some(resource_identity(envelope_attributes.unwrap_or(obj)))
}

/// The envelope shape's own keys. An object whose keys are not a subset of
/// these is never the envelope, no matter what it carries under
/// `attributes` — see [`resource_identity_from_json`].
const ENVELOPE_KEYS: [&str; 3] = ["attributes", "schema_url", "dropped_attributes_count"];

/// Appends the canonical form of a JSON object to `out`: keys sorted
/// lexicographically, values canonicalised recursively.
fn write_canonical_object(object: &serde_json::Map<String, serde_json::Value>, out: &mut String) {
    let mut keys: Vec<&String> = object.keys().collect();
    keys.sort();

    out.push('{');
    for (index, key) in keys.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_canonical_string(key, out);
        out.push(':');
        // `key` came from iterating `object`'s own keys, so the lookup
        // always finds an entry.
        if let Some(value) = object.get(*key) {
            write_canonical_value(value, out);
        }
    }
    out.push('}');
}

/// Appends the canonical form of an arbitrary JSON value to `out`: objects
/// go through [`write_canonical_object`] (sorted keys), array elements keep
/// their original order, and scalars use serde_json's default encoding.
fn write_canonical_value(value: &serde_json::Value, out: &mut String) {
    match value {
        serde_json::Value::Object(object) => write_canonical_object(object, out),
        serde_json::Value::Array(elements) => {
            out.push('[');
            for (index, element) in elements.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical_value(element, out);
            }
            out.push(']');
        }
        // `Value`'s scalar variants (null/bool/number/string) serialise to
        // JSON without ever failing, and their `serde_json::to_string`
        // encoding is already whitespace-free and canonical.
        scalar => out.push_str(&serde_json::to_string(scalar).unwrap_or_default()),
    }
}

/// Appends a JSON-escaped, quoted string to `out`, reusing serde_json's
/// escaping so it matches the escaping [`write_canonical_value`] applies to
/// string values.
fn write_canonical_string(value: &str, out: &mut String) {
    out.push_str(&serde_json::to_string(value).unwrap_or_default());
}

/// SHA-256 of `canonical`'s UTF-8 bytes, truncated to 16 bytes and rendered
/// as 32 lowercase hex characters.
fn digest_hex(canonical: &str) -> String {
    let hash = Sha256::digest(canonical.as_bytes());
    hex::encode(&hash[..16])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn map(pairs: &[(&str, serde_json::Value)]) -> serde_json::Map<String, serde_json::Value> {
        let mut map = serde_json::Map::new();
        for (key, value) in pairs {
            map.insert((*key).to_string(), value.clone());
        }
        map
    }

    #[test]
    fn key_order_does_not_change_the_digest() {
        let forward = map(&[
            ("service.name", json!("checkout")),
            ("host.name", json!("ip-10-0-0-1")),
        ]);
        let reversed = map(&[
            ("host.name", json!("ip-10-0-0-1")),
            ("service.name", json!("checkout")),
        ]);

        assert_eq!(resource_identity(&forward), resource_identity(&reversed));
    }

    #[test]
    fn nested_object_key_order_does_not_change_the_digest() {
        let forward = map(&[(
            "k8s",
            json!({"pod.name": "checkout-abc123", "namespace": "prod"}),
        )]);
        let reversed = map(&[(
            "k8s",
            json!({"namespace": "prod", "pod.name": "checkout-abc123"}),
        )]);

        assert_eq!(resource_identity(&forward), resource_identity(&reversed));
    }

    #[test]
    fn a_value_change_changes_the_digest() {
        let a = map(&[("service.name", json!("checkout"))]);
        let b = map(&[("service.name", json!("billing"))]);

        assert_ne!(resource_identity(&a), resource_identity(&b));
    }

    #[test]
    fn a_key_change_changes_the_digest() {
        let a = map(&[("service.name", json!("checkout"))]);
        let b = map(&[("service.id", json!("checkout"))]);

        assert_ne!(resource_identity(&a), resource_identity(&b));
    }

    #[test]
    fn empty_map_yields_a_fixed_digest() {
        let empty = serde_json::Map::new();

        assert_eq!(
            resource_identity(&empty),
            "44136fa355b3678a1146ad16f7e8649e"
        );
    }

    #[test]
    fn digest_is_exactly_32_lowercase_hex_chars() {
        let attributes = map(&[("service.name", json!("checkout"))]);
        let digest = resource_identity(&attributes);

        assert_eq!(digest.len(), 32);
        assert!(
            digest
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
    }

    #[test]
    fn flat_and_wrapped_shapes_give_the_same_digest() {
        let flat = r#"{"service.name":"checkout","host.name":"ip-10-0-0-1"}"#;
        let wrapped = r#"{"attributes":{"service.name":"checkout","host.name":"ip-10-0-0-1"},"schema_url":"https://opentelemetry.io/schemas/1.20.0"}"#;

        assert_eq!(
            resource_identity_from_json(flat),
            resource_identity_from_json(wrapped)
        );
    }

    #[test]
    fn schema_url_is_ignored() {
        let with_schema_url = r#"{"attributes":{"service.name":"checkout"},"schema_url":"https://opentelemetry.io/schemas/1.20.0"}"#;
        let without_schema_url = r#"{"attributes":{"service.name":"checkout"}}"#;

        assert_eq!(
            resource_identity_from_json(with_schema_url),
            resource_identity_from_json(without_schema_url)
        );
    }

    /// A flat resource may legitimately carry its own `attributes` key
    /// alongside other keys. Checking for `attributes` alone would
    /// misdetect this as the envelope shape, drop every sibling key, and
    /// collide two flat resources that only share the nested `attributes`
    /// value.
    #[test]
    fn a_flat_map_with_a_sibling_attributes_key_is_not_mistaken_for_the_envelope() {
        let checkout = r#"{"attributes":{"tenant":"a"},"service.name":"checkout"}"#;
        let billing = r#"{"attributes":{"tenant":"a"},"service.name":"billing"}"#;

        assert_ne!(
            resource_identity_from_json(checkout),
            resource_identity_from_json(billing)
        );

        let checkout_map = map(&[
            ("attributes", json!({"tenant": "a"})),
            ("service.name", json!("checkout")),
        ]);
        let billing_map = map(&[
            ("attributes", json!({"tenant": "a"})),
            ("service.name", json!("billing")),
        ]);
        assert_eq!(
            resource_identity_from_json(checkout),
            Some(resource_identity(&checkout_map))
        );
        assert_eq!(
            resource_identity_from_json(billing),
            Some(resource_identity(&billing_map))
        );
    }

    #[test]
    fn non_object_json_yields_none() {
        assert_eq!(resource_identity_from_json("42"), None);
        assert_eq!(resource_identity_from_json("\"hello\""), None);
        assert_eq!(resource_identity_from_json("[1,2,3]"), None);
        assert_eq!(resource_identity_from_json("null"), None);
    }

    #[test]
    fn unparsable_json_yields_none() {
        assert_eq!(resource_identity_from_json("{not json"), None);
        assert_eq!(resource_identity_from_json(""), None);
    }

    /// Pins the digest of a small, fixed attribute set so a future change to
    /// the canonical form or digest scheme shows up as a deliberate diff
    /// here, rather than silently shifting every `resource.identity` value.
    #[test]
    fn golden_digest_of_a_fixed_attribute_set() {
        let attributes = map(&[
            ("service.name", json!("checkout")),
            ("service.version", json!("1.4.2")),
            ("host.name", json!("ip-10-0-0-1")),
            ("k8s.pod.name", json!("checkout-7d8f9c-abc12")),
        ]);

        assert_eq!(
            resource_identity(&attributes),
            "d8fec1962b04f3bdade57694a6b35a31"
        );
    }
}
