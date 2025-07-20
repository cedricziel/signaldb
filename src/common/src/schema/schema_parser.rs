use anyhow::{Result, anyhow};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

/// Schema definitions loaded from TOML
#[derive(Debug, Deserialize)]
pub struct SchemaDefinitions {
    pub metadata: SchemaMetadata,
    pub traces: HashMap<String, TableSchemaDefinition>,
    pub logs: HashMap<String, TableSchemaDefinition>,
    pub metrics_gauge: HashMap<String, TableSchemaDefinition>,
    pub metrics_sum: HashMap<String, TableSchemaDefinition>,
    pub metrics_histogram: HashMap<String, TableSchemaDefinition>,
}

#[derive(Debug, Deserialize)]
pub struct SchemaMetadata {
    pub description: String,
    pub current_trace_version: String,
    pub current_log_version: String,
    pub current_metric_version: String,
}

#[derive(Debug, Deserialize)]
pub struct TableSchemaDefinition {
    pub description: String,
    #[serde(default)]
    pub inherits: Option<String>,
    #[serde(default)]
    pub fields: Vec<FieldDefinition>,
    #[serde(default)]
    pub field_renames: Vec<FieldRename>,
    #[serde(default)]
    pub field_additions: Vec<FieldDefinition>,
    #[serde(default)]
    pub partition_by: Vec<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FieldDefinition {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub required: bool,
    #[serde(default)]
    pub computed: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct FieldRename {
    pub from: String,
    pub to: String,
}

/// A resolved schema with all inheritance applied
#[derive(Debug, Clone)]
pub struct ResolvedSchema {
    pub version: String,
    pub description: String,
    pub fields: Vec<ResolvedField>,
    pub partition_by: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct ResolvedField {
    pub name: String,
    pub field_type: String,
    pub required: bool,
    pub computed: Option<String>,
    pub field_id: usize,
}

impl SchemaDefinitions {
    /// Load schema definitions from TOML string
    pub fn from_toml(toml_str: &str) -> Result<Self> {
        toml::from_str(toml_str).map_err(|e| anyhow!("Failed to parse schema TOML: {}", e))
    }

    /// Get current trace schema version
    pub fn current_trace_version(&self) -> &str {
        &self.metadata.current_trace_version
    }

    /// Resolve a trace schema by version
    pub fn resolve_trace_schema(&self, version: &str) -> Result<ResolvedSchema> {
        self.resolve_table_schema(&self.traces, version)
    }

    /// Resolve a log schema by version
    pub fn resolve_log_schema(&self, version: &str) -> Result<ResolvedSchema> {
        self.resolve_table_schema(&self.logs, version)
    }

    /// Generic schema resolver that handles inheritance
    #[allow(clippy::only_used_in_recursion)]
    fn resolve_table_schema(
        &self,
        schemas: &HashMap<String, TableSchemaDefinition>,
        version: &str,
    ) -> Result<ResolvedSchema> {
        let schema_def = schemas
            .get(version)
            .ok_or_else(|| anyhow!("Schema version {} not found", version))?;

        let mut resolved_fields = Vec::new();
        let mut field_names = HashMap::new();
        let mut field_id = 1;

        // If this schema inherits from another, start with those fields
        if let Some(ref parent_version) = schema_def.inherits {
            let parent_schema = self.resolve_table_schema(schemas, parent_version)?;
            for parent_field in parent_schema.fields {
                field_names.insert(parent_field.name.clone(), resolved_fields.len());
                resolved_fields.push(parent_field);
            }
        } else {
            // Base schema - use fields directly
            for field in &schema_def.fields {
                let resolved = ResolvedField {
                    name: field.name.clone(),
                    field_type: field.field_type.clone(),
                    required: field.required,
                    computed: field.computed.clone(),
                    field_id,
                };
                field_names.insert(field.name.clone(), resolved_fields.len());
                resolved_fields.push(resolved);
                field_id += 1;
            }
        }

        // Apply field renames
        for rename in &schema_def.field_renames {
            if let Some(&idx) = field_names.get(&rename.from) {
                resolved_fields[idx].name = rename.to.clone();
                field_names.remove(&rename.from);
                field_names.insert(rename.to.clone(), idx);
            }
        }

        // Add new fields
        for addition in &schema_def.field_additions {
            let resolved = ResolvedField {
                name: addition.name.clone(),
                field_type: addition.field_type.clone(),
                required: addition.required,
                computed: addition.computed.clone(),
                field_id: resolved_fields.len() + 1,
            };
            field_names.insert(addition.name.clone(), resolved_fields.len());
            resolved_fields.push(resolved);
        }

        Ok(ResolvedSchema {
            version: version.to_string(),
            description: schema_def.description.clone(),
            fields: resolved_fields,
            partition_by: schema_def.partition_by.clone(),
        })
    }
}

impl ResolvedSchema {
    /// Convert to Iceberg Schema
    pub fn to_iceberg_schema(&self) -> Result<Schema> {
        let mut fields = Vec::new();

        for (idx, field) in self.fields.iter().enumerate() {
            let field_type = match field.field_type.as_str() {
                "string" => Type::Primitive(PrimitiveType::String),
                "int32" => Type::Primitive(PrimitiveType::Int),
                "int64" => Type::Primitive(PrimitiveType::Long),
                "uint64" => Type::Primitive(PrimitiveType::Long), // Map uint64 to long
                "double" => Type::Primitive(PrimitiveType::Double),
                "boolean" => Type::Primitive(PrimitiveType::Boolean),
                "timestamp_ns" => Type::Primitive(PrimitiveType::TimestampNs),
                "date" => Type::Primitive(PrimitiveType::Date),
                "list<struct>" => {
                    // For now, use string for complex types
                    // TODO: Properly handle nested structures
                    Type::Primitive(PrimitiveType::String)
                }
                _ => return Err(anyhow!("Unsupported field type: {}", field.field_type)),
            };

            let nested_field = if field.required {
                NestedField::required(idx as i32 + 1, &field.name, field_type)
            } else {
                NestedField::optional(idx as i32 + 1, &field.name, field_type)
            };

            fields.push(Arc::new(nested_field));
        }

        Schema::builder()
            .with_fields(fields)
            .build()
            .map_err(|e| anyhow!("Failed to create Iceberg schema: {}", e))
    }

    /// Get field names that need to be computed
    pub fn computed_fields(&self) -> Vec<&ResolvedField> {
        self.fields
            .iter()
            .filter(|f| f.computed.is_some())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_parsing() {
        let toml = r#"
[metadata]
description = "Test schemas"
current_trace_version = "v2"
current_log_version = "v1"
current_metric_version = "v1"

[traces.v1]
description = "Base schema"
fields = [
    { name = "trace_id", type = "string", required = true },
    { name = "name", type = "string", required = true },
]

[traces.v2]
description = "Extended schema"
inherits = "v1"
field_renames = [
    { from = "name", to = "span_name" },
]
field_additions = [
    { name = "timestamp", type = "timestamp_ns", required = true, computed = "start_time" },
]
partition_by = ["timestamp"]

[logs.v1]
description = "Log schema"
fields = [
    { name = "timestamp", type = "timestamp_ns", required = true },
]
"#;

        let schemas = SchemaDefinitions::from_toml(toml).unwrap();
        assert_eq!(schemas.current_trace_version(), "v2");

        // Test v1 resolution
        let v1 = schemas.resolve_trace_schema("v1").unwrap();
        assert_eq!(v1.fields.len(), 2);
        assert_eq!(v1.fields[0].name, "trace_id");
        assert_eq!(v1.fields[1].name, "name");

        // Test v2 resolution with inheritance and rename
        let v2 = schemas.resolve_trace_schema("v2").unwrap();
        assert_eq!(v2.fields.len(), 3);
        assert_eq!(v2.fields[0].name, "trace_id");
        assert_eq!(v2.fields[1].name, "span_name"); // Renamed from "name"
        assert_eq!(v2.fields[2].name, "timestamp");
        assert_eq!(v2.fields[2].computed, Some("start_time".to_string()));
    }
}
