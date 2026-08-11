//! Canonical client-visible schema independent of physical Iceberg layout.

use std::collections::{HashMap, HashSet};

/// The level at which an attribute is attached to an OTel record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AttributeLevel {
    Resource,
    Scope,
    Record,
}

/// The client-visible type of a logical field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Filterability {
    Filterable,
    RetrievalOnly,
}

/// The semantic role of a logical field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
            LogicalField::record_metadata("traces", "status_message", LogicalType::String),
            LogicalField::record_metadata("traces", "is_root", LogicalType::Bool),
            LogicalField::record_metadata("traces", "trace_state", LogicalType::String),
            LogicalField::record_metadata("traces", "name", LogicalType::String),
            LogicalField::record_metadata("traces", "span.name", LogicalType::String),
            LogicalField::record_metadata("traces", "duration", LogicalType::DurationNs),
            LogicalField::record_metadata("traces", "duration_nano", LogicalType::DurationNs),
            LogicalField::record_metadata("traces", "status.code", LogicalType::String),
        ];
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
            schema.resolve("logs", "service.name").unwrap().value_type,
            LogicalType::String
        );
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
