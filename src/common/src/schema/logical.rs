//! Canonical client-visible schema independent of physical Iceberg layout.

use std::collections::{HashMap, HashSet};

/// The level at which an attribute is attached to an OTel record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AttributeLevel {
    Resource,
    Scope,
    Record,
}

/// The client-visible type of a logical field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LogicalType {
    String,
    Bool,
    Int64,
    Float64,
    TimestampNs,
    DurationNs,
    Bytes,
    AnyValue,
}

/// Whether predicates may address a field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Filterability {
    Filterable,
    RetrievalOnly,
}

/// The semantic role of a logical field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum LogicalFieldKind {
    Attribute,
    RecordMetadata,
    JoinKey,
    SignalDbDefined,
}

/// Stable identity for a logical field.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LogicalFieldId {
    pub source: String,
    pub level: Option<AttributeLevel>,
    pub name: String,
}

/// One client-visible field in the canonical schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalField {
    pub id: LogicalFieldId,
    pub value_type: LogicalType,
    pub filterability: Filterability,
    pub kind: LogicalFieldKind,
    pub non_native: bool,
}

impl LogicalField {
    pub fn attribute(
        source: &str,
        level: AttributeLevel,
        name: &str,
        value_type: LogicalType,
    ) -> Self {
        Self {
            id: LogicalFieldId {
                source: source.to_string(),
                level: Some(level),
                name: name.to_string(),
            },
            value_type,
            filterability: Filterability::Filterable,
            kind: LogicalFieldKind::Attribute,
            non_native: false,
        }
    }

    pub fn signaldb_resource_identity(source: &str) -> Self {
        Self {
            id: LogicalFieldId {
                source: source.to_string(),
                level: None,
                name: "resource.identity".to_string(),
            },
            value_type: LogicalType::String,
            filterability: Filterability::Filterable,
            kind: LogicalFieldKind::SignalDbDefined,
            non_native: true,
        }
    }

    pub fn record_metadata(source: &str, name: &str, value_type: LogicalType) -> Self {
        Self {
            id: LogicalFieldId {
                source: source.to_string(),
                level: None,
                name: name.to_string(),
            },
            value_type,
            filterability: Filterability::Filterable,
            kind: LogicalFieldKind::RecordMetadata,
            non_native: false,
        }
    }

    pub fn join_key(source: &str, name: &str) -> Self {
        Self {
            kind: LogicalFieldKind::JoinKey,
            ..Self::record_metadata(source, name, LogicalType::String)
        }
    }

    pub fn retrieval_only(mut self) -> Self {
        self.filterability = Filterability::RetrievalOnly;
        self
    }
}

/// Resolves logical fields without exposing physical storage names.
#[derive(Debug, Clone, Default)]
pub struct LogicalSchema {
    fields: HashMap<LogicalFieldId, LogicalField>,
    physical_names: HashSet<String>,
}

impl LogicalSchema {
    pub fn new(fields: impl IntoIterator<Item = LogicalField>) -> Self {
        Self {
            fields: fields
                .into_iter()
                .map(|field| (field.id.clone(), field))
                .collect(),
            physical_names: HashSet::new(),
        }
    }

    pub fn with_physical_names(
        mut self,
        names: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.physical_names = names.into_iter().map(Into::into).collect();
        self
    }

    /// Every registered logical field, in no particular order. Admin
    /// tooling iterates this to render the schema; query resolution goes
    /// through [`Self::resolve`] instead.
    pub fn fields(&self) -> impl Iterator<Item = &LogicalField> + '_ {
        self.fields.values()
    }

    pub fn is_physical_name(&self, name: &str) -> bool {
        self.physical_names.contains(name)
    }

    pub fn resolve(&self, source: &str, name: &str) -> Option<&LogicalField> {
        let (level, name) = match name {
            value if let Some(name) = value.strip_prefix("resource.") => {
                (Some(AttributeLevel::Resource), name)
            }
            value if let Some(name) = value.strip_prefix("scope.") => {
                (Some(AttributeLevel::Scope), name)
            }
            value if let Some(name) = value.strip_prefix("record.") => {
                (Some(AttributeLevel::Record), name)
            }
            _ => (None, name),
        };

        if let Some(level) = level {
            return self.fields.get(&LogicalFieldId {
                source: source.to_string(),
                level: Some(level),
                name: name.to_string(),
            });
        }

        [
            AttributeLevel::Record,
            AttributeLevel::Scope,
            AttributeLevel::Resource,
        ]
        .into_iter()
        .find_map(|level| {
            self.fields.get(&LogicalFieldId {
                source: source.to_string(),
                level: Some(level),
                name: name.to_string(),
            })
        })
        .or_else(|| {
            self.fields.get(&LogicalFieldId {
                source: source.to_string(),
                level: None,
                name: name.to_string(),
            })
        })
    }

    /// The initial OTel-native surface shared by query and ingest. Physical
    /// realization is deliberately absent from this declaration.
    pub fn core() -> Self {
        let mut fields = vec![
            LogicalField::record_metadata("logs", "timestamp", LogicalType::TimestampNs),
            LogicalField::record_metadata("logs", "observed_timestamp", LogicalType::TimestampNs),
            LogicalField::record_metadata("logs", "severity_number", LogicalType::Int64),
            LogicalField::record_metadata("logs", "severity_text", LogicalType::String),
            LogicalField::record_metadata("logs", "trace_flags", LogicalType::Int64),
            LogicalField::record_metadata("logs", "event_name", LogicalType::String),
            LogicalField::record_metadata("logs", "dropped_attributes_count", LogicalType::Int64),
            LogicalField::record_metadata("logs", "body", LogicalType::AnyValue).retrieval_only(),
            LogicalField::record_metadata("traces", "dropped_attributes_count", LogicalType::Int64),
            LogicalField::record_metadata("traces", "dropped_events_count", LogicalType::Int64),
            LogicalField::record_metadata("traces", "dropped_links_count", LogicalType::Int64),
            LogicalField::record_metadata("traces", "parent_span_id", LogicalType::String),
            LogicalField::record_metadata(
                "traces",
                "start_time_unix_nano",
                LogicalType::TimestampNs,
            ),
            LogicalField::record_metadata("traces", "end_time_unix_nano", LogicalType::TimestampNs),
            LogicalField::record_metadata("traces", "span_kind", LogicalType::String),
            // Numeric OTel source of truth (issue #1208): span_kind/
            // status.code above remain derived display strings for query
            // ergonomics, computed at write time from these, never the
            // reverse.
            LogicalField::record_metadata("traces", "span_kind_number", LogicalType::Int64),
            LogicalField::record_metadata("traces", "status_code_number", LogicalType::Int64),
            LogicalField::record_metadata("traces", "status_message", LogicalType::String),
            LogicalField::record_metadata("traces", "is_root", LogicalType::Bool),
            LogicalField::record_metadata("traces", "trace_state", LogicalType::String),
            LogicalField::record_metadata("traces", "name", LogicalType::String),
            // The span's events list (name, timestamp, attributes per event),
            // as a JSON array. Retrieval-only like `body`: exception fields
            // are addressed via `exception.*`, not by filtering on the list.
            LogicalField::record_metadata("traces", "span_events", LogicalType::AnyValue)
                .retrieval_only(),
            LogicalField::record_metadata("traces", "span.name", LogicalType::String),
            LogicalField::record_metadata("traces", "duration", LogicalType::DurationNs),
            LogicalField::record_metadata("traces", "duration_nano", LogicalType::DurationNs),
            LogicalField::record_metadata("traces", "status.code", LogicalType::String),
            // Metrics: gauge/sum only (see ir_planner.rs's `metrics`
            // SourcePlan) — a scalar numeric point per row, not an OTel
            // record with resource/scope/record attribute levels the way
            // logs/traces/profiles are, so it isn't part of the shared loop
            // below. `metric.value`'s type must be registered (not left to
            // the alias fallback's naive String default) or a `sum`/`avg`
            // aggregate over it is rejected as non-numeric.
            LogicalField::record_metadata("metrics", "metric.name", LogicalType::String),
            LogicalField::record_metadata("metrics", "metric.value", LogicalType::Float64),
            // metrics_histogram: a whole bucketed histogram per row, not a
            // scalar — only reachable via the `histogram_quantile` stage
            // (ir_planner.rs), which reads bucket_counts/explicit_bounds by
            // physical column name directly rather than through the
            // resolver. Only the fields a `where`/`by` clause can reference
            // need registering here.
            LogicalField::record_metadata("metrics_histogram", "metric.name", LogicalType::String),
        ];
        fields.push(LogicalField::attribute(
            "metrics",
            AttributeLevel::Resource,
            "service.name",
            LogicalType::String,
        ));
        fields.push(LogicalField::attribute(
            "metrics_histogram",
            AttributeLevel::Resource,
            "service.name",
            LogicalType::String,
        ));
        for source in ["logs", "traces"] {
            fields.push(LogicalField::join_key(source, "trace_id"));
            fields.push(LogicalField::join_key(source, "span_id"));
            fields.push(LogicalField::signaldb_resource_identity(source));
            fields.push(LogicalField::attribute(
                source,
                AttributeLevel::Resource,
                "service.name",
                LogicalType::String,
            ));
            fields.push(LogicalField::attribute(
                source,
                AttributeLevel::Resource,
                "schema_url",
                LogicalType::String,
            ));
            fields.push(LogicalField::attribute(
                source,
                AttributeLevel::Scope,
                "name",
                LogicalType::String,
            ));
            fields.push(LogicalField::attribute(
                source,
                AttributeLevel::Scope,
                "version",
                LogicalType::String,
            ));
            fields.push(LogicalField::attribute(
                source,
                AttributeLevel::Scope,
                "schema_url",
                LogicalType::String,
            ));
        }
        Self::new(fields)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unqualified_attributes_shadow_scope_then_resource() {
        let schema = LogicalSchema::new([
            LogicalField::attribute(
                "logs",
                AttributeLevel::Resource,
                "service.name",
                LogicalType::String,
            ),
            LogicalField::attribute(
                "logs",
                AttributeLevel::Scope,
                "service.name",
                LogicalType::Int64,
            ),
            LogicalField::attribute(
                "logs",
                AttributeLevel::Record,
                "service.name",
                LogicalType::Bool,
            ),
        ]);

        assert_eq!(
            schema.resolve("logs", "service.name").unwrap().value_type,
            LogicalType::Bool
        );
        assert_eq!(
            schema
                .resolve("logs", "scope.service.name")
                .unwrap()
                .value_type,
            LogicalType::Int64
        );
        assert_eq!(
            schema
                .resolve("logs", "resource.service.name")
                .unwrap()
                .value_type,
            LogicalType::String
        );
    }

    #[test]
    fn structured_values_are_retrievable_but_not_filterable() {
        let field = LogicalField::attribute(
            "logs",
            AttributeLevel::Record,
            "http.request.header",
            LogicalType::AnyValue,
        )
        .retrieval_only();

        assert_eq!(field.filterability, Filterability::RetrievalOnly);
    }

    #[test]
    fn resource_identity_is_explicitly_non_native() {
        let field = LogicalField::signaldb_resource_identity("logs");

        assert_eq!(field.kind, LogicalFieldKind::SignalDbDefined);
        assert!(field.non_native);
    }

    #[test]
    fn core_schema_exposes_log_metadata_and_trace_join_keys() {
        let schema = LogicalSchema::core();

        assert_eq!(
            schema
                .resolve("logs", "dropped_attributes_count")
                .unwrap()
                .kind,
            LogicalFieldKind::RecordMetadata
        );
        assert_eq!(
            schema.resolve("logs", "body").unwrap().filterability,
            Filterability::RetrievalOnly
        );
        assert_eq!(
            schema.resolve("traces", "trace_id").unwrap().kind,
            LogicalFieldKind::JoinKey
        );
        assert_eq!(
            schema
                .resolve("traces", "span_kind_number")
                .unwrap()
                .value_type,
            LogicalType::Int64
        );
        assert_eq!(
            schema
                .resolve("traces", "status_code_number")
                .unwrap()
                .value_type,
            LogicalType::Int64
        );
        assert_eq!(
            schema.resolve("logs", "service.name").unwrap().value_type,
            LogicalType::String
        );
    }

    #[test]
    fn fields_iterates_every_registered_field_and_agrees_with_resolve() {
        let schema = LogicalSchema::core();
        let all: Vec<_> = schema.fields().collect();

        assert!(
            all.len() > 10,
            "expected many registered fields, got {}",
            all.len()
        );
        // A level-prefixed field must resolve back to itself via its own
        // qualified name — the iterator and the resolver read the same map,
        // and a prefix pins the lookup unambiguously.
        //
        // An unprefixed (`level: None`) field has no such guarantee: bare
        // lookup falls back through Record/Scope/Resource attributes first
        // (see `resolve`), so a same-named attribute at one of those levels
        // shadows it. That's a real ambiguity in the registered schema, not
        // a bug in `fields()` or `resolve()` — e.g. traces registers both a
        // `RecordMetadata` "name" (the span's own name) and a `Scope`-level
        // "name" attribute (the instrumentation scope's name); bare `name`
        // resolves to the latter.
        for field in all {
            let Some(level) = field.id.level else {
                continue;
            };
            let prefix = match level {
                AttributeLevel::Resource => "resource",
                AttributeLevel::Scope => "scope",
                AttributeLevel::Record => "record",
            };
            let name = format!("{prefix}.{}", field.id.name);
            assert_eq!(schema.resolve(&field.id.source, &name), Some(field));
        }
    }

    #[test]
    fn physical_realization_names_are_not_logical_fields() {
        let schema =
            LogicalSchema::core().with_physical_names(["log_attributes", "label_service_name"]);

        assert!(schema.resolve("logs", "log_attributes").is_none());
        assert!(schema.is_physical_name("log_attributes"));
        assert!(schema.is_physical_name("label_service_name"));
    }
}
