//! # Attribute-registry resolver interface
//!
//! Every logical field is resolved to its physical location — a promoted
//! column or an attribute-map extraction — plus its canonical [`ValueType`].
//! This is a **consumer** of the attribute-registry epic (#811): a query-facing
//! *view*, not a re-implementation of the registry.
//!
//! ## Production gating (#811)
//!
//! The registry today is advisory-only (`attribute_stats` carries no canonical
//! type; promoted attribute columns are `String`). Until #811 lands a canonical
//! type source, an attribute's type is `String` unless a column-typed source
//! declares otherwise, and a field the resolver cannot type at all is reported
//! as unresolved so the validator can raise a **defined rejection** rather than
//! letting an untyped literal reach the engine. See the module's
//! [`FieldResolver::resolve`] contract.

use std::collections::HashMap;

use super::value::ValueType;

/// Where a logical field physically lives, with its canonical type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolved {
    /// A promoted physical column, addressed by its physical column name.
    Column { name: String, value_type: ValueType },
    /// An unpromoted attribute served by extracting `key` from an attribute
    /// container column (an Arrow `Map`/JSON column such as `log_attributes`).
    JsonPath {
        container: String,
        key: String,
        value_type: ValueType,
    },
}

impl Resolved {
    /// The canonical [`ValueType`] of the resolved field.
    pub fn value_type(&self) -> &ValueType {
        match self {
            Resolved::Column { value_type, .. } => value_type,
            Resolved::JsonPath { value_type, .. } => value_type,
        }
    }
}

/// Resolves a logical, dotted OTel-native field name to its physical location.
///
/// A resolver returns `None` for a field it cannot resolve **or cannot assign a
/// canonical type to** — the validator turns that into a defined rejection.
/// Resolution MUST be promotion-invariant in meaning: a field resolves to a
/// [`Resolved::JsonPath`] when unpromoted and a [`Resolved::Column`] when
/// promoted, but both denote the same logical field with the same type.
pub trait FieldResolver: Send + Sync {
    fn resolve(&self, source: &str, field: &str) -> Option<Resolved>;

    /// Whether `field` is an actually-known field of `source` (a physical
    /// column or an explicitly-declared attribute) rather than a permissive
    /// fallback. Collision checks (extract shadowing, aggregate output names)
    /// use this so a production resolver that resolves *any* name to a String
    /// attribute does not spuriously flag every derived/output name as a
    /// collision. Defaults to "resolvable" for strict/in-memory resolvers.
    fn is_known(&self, source: &str, field: &str) -> bool {
        self.resolve(source, field).is_some()
    }
}

/// A per-attribute entry in the [`InMemoryResolver`].
#[derive(Debug, Clone)]
enum Entry {
    /// A first-class physical column: resolves to a `Column` with `physical`.
    Column {
        physical: String,
        value_type: ValueType,
    },
    /// An attribute stored in `container`; resolves to a `Column` (the
    /// materialized name) when `promoted`, else to a `JsonPath` extraction.
    Attribute {
        container: String,
        value_type: ValueType,
        promoted: bool,
    },
}

/// A config/in-memory [`FieldResolver`] used for tests and as the fallback
/// before #811 provides a production registry view. Fully functional: it
/// captures the promoted-vs-unpromoted distinction that makes resolution
/// promotion-invariant.
#[derive(Debug, Clone, Default)]
pub struct InMemoryResolver {
    entries: HashMap<(String, String), Entry>,
}

impl InMemoryResolver {
    pub fn new() -> Self {
        Self::default()
    }

    /// Declare a first-class physical column for `source`. `logical` is the
    /// name a query writes; `physical` is the column the planner scans.
    pub fn with_column(
        mut self,
        source: &str,
        logical: &str,
        physical: &str,
        value_type: ValueType,
    ) -> Self {
        self.entries.insert(
            (source.to_string(), logical.to_string()),
            Entry::Column {
                physical: physical.to_string(),
                value_type,
            },
        );
        self
    }

    /// Declare an attribute for `source`, stored in `container`. `promoted`
    /// selects whether it currently resolves to a materialized column or a
    /// JSON-path extraction — the two promotion states of the same field.
    pub fn with_attribute(
        mut self,
        source: &str,
        logical: &str,
        container: &str,
        value_type: ValueType,
        promoted: bool,
    ) -> Self {
        self.entries.insert(
            (source.to_string(), logical.to_string()),
            Entry::Attribute {
                container: container.to_string(),
                value_type,
                promoted,
            },
        );
        self
    }
}

impl FieldResolver for InMemoryResolver {
    fn resolve(&self, source: &str, field: &str) -> Option<Resolved> {
        match self.entries.get(&(source.to_string(), field.to_string()))? {
            Entry::Column {
                physical,
                value_type,
            } => Some(Resolved::Column {
                name: physical.clone(),
                value_type: value_type.clone(),
            }),
            Entry::Attribute {
                container,
                value_type,
                promoted,
            } => {
                if *promoted {
                    // Promoted attributes materialize to `label_<sanitized>`
                    // columns — resolution shifts from JSON path to column,
                    // meaning unchanged (promotion invariance).
                    Some(Resolved::Column {
                        name: crate::schema::materialized_column_name(field),
                        value_type: value_type.clone(),
                    })
                } else {
                    Some(Resolved::JsonPath {
                        container: container.clone(),
                        key: field.to_string(),
                        value_type: value_type.clone(),
                    })
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Task 3.1 — resolving a logical field yields a column ref or a json-path
    // plus the canonical type; unpromoted → json path, promoted → column.
    #[test]
    fn physical_column_resolves_to_column_with_type() {
        let r = InMemoryResolver::new().with_column(
            "logs",
            "severity_number",
            "severity_number",
            ValueType::Int64,
        );
        assert_eq!(
            r.resolve("logs", "severity_number"),
            Some(Resolved::Column {
                name: "severity_number".to_string(),
                value_type: ValueType::Int64,
            })
        );
    }

    #[test]
    fn unpromoted_attribute_resolves_to_json_path() {
        let r = InMemoryResolver::new().with_attribute(
            "logs",
            "deployment.environment",
            "log_attributes",
            ValueType::String,
            false,
        );
        assert_eq!(
            r.resolve("logs", "deployment.environment"),
            Some(Resolved::JsonPath {
                container: "log_attributes".to_string(),
                key: "deployment.environment".to_string(),
                value_type: ValueType::String,
            })
        );
    }

    #[test]
    fn promoted_attribute_resolves_to_column() {
        let r = InMemoryResolver::new().with_attribute(
            "logs",
            "deployment.environment",
            "log_attributes",
            ValueType::String,
            true,
        );
        match r.resolve("logs", "deployment.environment") {
            Some(Resolved::Column { name, value_type }) => {
                assert_eq!(value_type, ValueType::String);
                assert_eq!(
                    name,
                    crate::schema::materialized_column_name("deployment.environment")
                );
            }
            other => panic!("expected a column, got {other:?}"),
        }
    }

    #[test]
    fn unknown_field_is_unresolved() {
        let r = InMemoryResolver::new();
        assert_eq!(r.resolve("logs", "nope"), None);
    }
}
