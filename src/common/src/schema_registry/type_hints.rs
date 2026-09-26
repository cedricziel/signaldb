//! [`TypeHints`] over the registries visible to a tenant: the bundled `otel`
//! semconv snapshot (selected by any `https://opentelemetry.io/schemas/...`
//! url, regardless of version) plus every registry's own declared
//! `schema_url` (custom registries, and the bundled `signaldb` one).

use std::collections::HashMap;
use std::sync::Arc;

use schema_model::{AttributeDef, EnumMember, ResolvedRegistry};

use super::{OTEL_NAMESPACE, SchemaResolver, StoreError, Visible};
use crate::schema::type_authority::{CanonicalType, TypeHints};

const OTEL_SCHEMA_URL_PREFIX: &str = "https://opentelemetry.io/schemas/";

/// A sync snapshot of one tenant's visible registries, keyed by schema_url,
/// for hinting canonical attribute types on the ingest hot path.
#[derive(Debug, Clone, Default)]
pub struct SemconvTypeHints {
    otel: Option<Arc<ResolvedRegistry>>,
    by_schema_url: HashMap<String, Arc<ResolvedRegistry>>,
}

impl SemconvTypeHints {
    fn from_visible(visible: &[Visible]) -> Self {
        let mut otel = None;
        let mut by_schema_url = HashMap::new();
        for v in visible {
            if v.resolved.namespace == OTEL_NAMESPACE && otel.is_none() {
                otel = Some(v.resolved.clone());
            }
            if let Some(url) = &v.resolved.schema_url {
                by_schema_url
                    .entry(url.clone())
                    .or_insert_with(|| v.resolved.clone());
            }
        }
        SemconvTypeHints {
            otel,
            by_schema_url,
        }
    }
}

impl TypeHints for SemconvTypeHints {
    fn hint(&self, schema_url: &str, key: &str) -> Option<CanonicalType> {
        let registry = if schema_url.starts_with(OTEL_SCHEMA_URL_PREFIX) {
            self.otel.as_deref()
        } else {
            self.by_schema_url.get(schema_url).map(Arc::as_ref)
        };
        registry
            .and_then(|r| r.attributes.get(key))
            .and_then(canonical_of)
    }
}

/// Weaver type name to canonical type; arrays, templates, `any`, `map`, and
/// `undefined` have no typed home, so `None`.
fn canonical_of(def: &AttributeDef) -> Option<CanonicalType> {
    match def.r#type.as_str() {
        "string" => Some(CanonicalType::String),
        "int" => Some(CanonicalType::Int64),
        "double" => Some(CanonicalType::Float64),
        "boolean" => Some(CanonicalType::Bool),
        "enum" => enum_member_canonical(&def.enum_members),
        _ => None,
    }
}

/// An enum's canonical type is its members' value type when they agree
/// (all string, or all integer); otherwise there is no typed home.
fn enum_member_canonical(members: &[EnumMember]) -> Option<CanonicalType> {
    let mut kind = None;
    for member in members {
        let member_kind = match &member.value {
            serde_json::Value::String(_) => CanonicalType::String,
            serde_json::Value::Number(n) if n.is_i64() || n.is_u64() => CanonicalType::Int64,
            _ => return None,
        };
        match kind {
            None => kind = Some(member_kind),
            Some(k) if k == member_kind => {}
            Some(_) => return None,
        }
    }
    kind
}

impl SchemaResolver {
    /// A sync [`TypeHints`] snapshot of the registries visible to `tenant_id`,
    /// for per-attribute canonical-type hints on the ingest hot path.
    pub async fn type_hints(&self, tenant_id: &str) -> Result<SemconvTypeHints, StoreError> {
        let visible = self.visible(tenant_id).await?;
        Ok(SemconvTypeHints::from_visible(&visible))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attr(r#type: &str, enum_members: Vec<EnumMember>) -> AttributeDef {
        AttributeDef {
            key: "test.key".to_string(),
            group_id: "group".to_string(),
            group_display_name: None,
            brief: "brief".to_string(),
            note: None,
            r#type: r#type.to_string(),
            enum_members,
            examples: Vec::new(),
            stability: None,
            deprecated: None,
            requirement_level: None,
        }
    }

    fn member(id: &str, value: serde_json::Value) -> EnumMember {
        EnumMember {
            id: id.to_string(),
            value,
            brief: None,
            note: None,
            stability: None,
            deprecated: None,
            extra: Default::default(),
        }
    }

    #[test]
    fn scalar_types_map_to_canonical() {
        assert_eq!(
            canonical_of(&attr("string", vec![])),
            Some(CanonicalType::String)
        );
        assert_eq!(
            canonical_of(&attr("int", vec![])),
            Some(CanonicalType::Int64)
        );
        assert_eq!(
            canonical_of(&attr("double", vec![])),
            Some(CanonicalType::Float64)
        );
        assert_eq!(
            canonical_of(&attr("boolean", vec![])),
            Some(CanonicalType::Bool)
        );
    }

    #[test]
    fn arrays_templates_any_and_unknown_have_no_typed_home() {
        for t in [
            "string[]",
            "int[]",
            "template[string]",
            "any",
            "map",
            "undefined",
            "something_unknown",
        ] {
            assert_eq!(canonical_of(&attr(t, vec![])), None, "{t}");
        }
    }

    #[test]
    fn enum_with_all_string_members_is_string() {
        let members = vec![
            member("a", serde_json::Value::String("a".into())),
            member("b", serde_json::Value::String("b".into())),
        ];
        assert_eq!(
            canonical_of(&attr("enum", members)),
            Some(CanonicalType::String)
        );
    }

    #[test]
    fn enum_with_all_int_members_is_int64() {
        let members = vec![
            member("a", serde_json::Value::from(1)),
            member("b", serde_json::Value::from(2)),
        ];
        assert_eq!(
            canonical_of(&attr("enum", members)),
            Some(CanonicalType::Int64)
        );
    }

    #[test]
    fn enum_with_mixed_or_float_members_has_no_typed_home() {
        let mixed = vec![
            member("a", serde_json::Value::String("a".into())),
            member("b", serde_json::Value::from(1)),
        ];
        assert_eq!(canonical_of(&attr("enum", mixed)), None);

        let floats = vec![member("a", serde_json::Value::from(1.5))];
        assert_eq!(canonical_of(&attr("enum", floats)), None);

        let empty: Vec<EnumMember> = vec![];
        assert_eq!(canonical_of(&attr("enum", empty)), None);
    }
}
