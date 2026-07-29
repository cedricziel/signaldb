use anyhow::{Result, anyhow};
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::{ListType, MapType, PrimitiveType, StructField, StructType, Type};
use serde::Deserialize;
use std::collections::HashMap;

/// Schema definitions loaded from TOML
#[derive(Debug, Deserialize)]
pub struct SchemaDefinitions {
    pub metadata: SchemaMetadata,
    pub traces: HashMap<String, TableSchemaDefinition>,
    pub logs: HashMap<String, TableSchemaDefinition>,
    #[serde(default)]
    pub metrics_gauge: HashMap<String, TableSchemaDefinition>,
    #[serde(default)]
    pub metrics_sum: HashMap<String, TableSchemaDefinition>,
    #[serde(default)]
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
    /// Convert to Iceberg Schema.
    pub fn to_iceberg_schema(&self) -> Result<Schema> {
        self.to_iceberg_schema_with_labels(&[])
    }

    /// Convert to an Iceberg Schema, appending an optional `label_<key>`
    /// column for each materialized attribute label (see
    /// [`crate::schema::materialized_column_name`]). Duplicate or
    /// base-column-colliding labels are skipped, and field IDs continue
    /// after the base columns.
    pub fn to_iceberg_schema_with_labels(&self, labels: &[String]) -> Result<Schema> {
        self.build_iceberg_schema(labels, false)
    }

    /// Like [`Self::to_iceberg_schema_with_labels`], but also appends the
    /// derived optional `attr_tokens` `List<String>` column (see
    /// [`crate::schema::ATTR_TOKENS_COLUMN`]). Used by the logs schema,
    /// where the writer materializes `key=value` tokens over all attribute
    /// scopes for bloom-filtered containment checks.
    pub fn to_iceberg_schema_with_labels_and_attr_tokens(
        &self,
        labels: &[String],
    ) -> Result<Schema> {
        self.build_iceberg_schema(labels, true)
    }

    fn build_iceberg_schema(&self, labels: &[String], attr_tokens: bool) -> Result<Schema> {
        let mut fields = Vec::new();

        // Nested (map key/value) field IDs must be unique across the whole
        // schema; allocate them after every top-level ID so the top-level
        // numbering stays identical to the historical string-only layout.
        let mut next_nested_id = self.fields.len() as i32 + 1;
        let mut map_slots: Vec<usize> = Vec::new();

        for (idx, field) in self.fields.iter().enumerate() {
            let field_type = match field.field_type.as_str() {
                "string" => Type::Primitive(PrimitiveType::String),
                "int32" => Type::Primitive(PrimitiveType::Int),
                "int64" => Type::Primitive(PrimitiveType::Long),
                "uint64" => Type::Primitive(PrimitiveType::Long), // Map uint64 to long
                "double" => Type::Primitive(PrimitiveType::Double),
                "boolean" => Type::Primitive(PrimitiveType::Boolean),
                "timestamp_ns" => Type::Primitive(PrimitiveType::Timestamp), // No TimestampNs in iceberg-rust
                "date" => Type::Primitive(PrimitiveType::Date),
                // Attribute maps: string keys to string values. Key/value
                // IDs are assigned in a second pass below.
                "map<string,string>" => {
                    map_slots.push(idx);
                    Type::Primitive(PrimitiveType::String) // placeholder
                }
                "list<struct>" => {
                    // For now, use string for complex types
                    // TODO: Properly handle nested structures
                    Type::Primitive(PrimitiveType::String)
                }
                _ => return Err(anyhow!("Unsupported field type: {}", field.field_type)),
            };

            let struct_field = StructField {
                id: idx as i32 + 1,
                name: field.name.clone(),
                required: field.required,
                field_type,
                doc: None,
                initial_default: None,
                write_default: None,
            };

            fields.push(struct_field);
        }

        // Second pass: fill in map types with globally-unique nested IDs.
        for idx in map_slots {
            let key_id = next_nested_id;
            let value_id = next_nested_id + 1;
            next_nested_id += 2;
            fields[idx].field_type = Type::Map(MapType {
                key_id,
                key: Box::new(Type::Primitive(PrimitiveType::String)),
                value_id,
                value_required: false,
                value: Box::new(Type::Primitive(PrimitiveType::String)),
            });
        }

        // Append materialized-label columns after the base fields. They are
        // always optional strings (a row may not carry the attribute).
        let mut next_id = next_nested_id;
        for label in labels {
            let name = crate::schema::materialized_column_name(label);
            if fields.iter().any(|f| f.name == name) {
                continue; // collides with a base column or an earlier label
            }
            fields.push(StructField {
                id: next_id,
                name,
                required: false,
                field_type: Type::Primitive(PrimitiveType::String),
                doc: Some(format!("Materialized attribute label '{label}'")),
                initial_default: None,
                write_default: None,
            });
            next_id += 1;
        }

        // Derived `key=value` token column: an optional List<String> whose
        // element ID follows every other ID in the schema.
        if attr_tokens
            && !fields
                .iter()
                .any(|f| f.name == crate::schema::ATTR_TOKENS_COLUMN)
        {
            fields.push(StructField {
                id: next_id,
                name: crate::schema::ATTR_TOKENS_COLUMN.to_string(),
                required: false,
                field_type: Type::List(ListType {
                    element_id: next_id + 1,
                    element_required: false,
                    element: Box::new(Type::Primitive(PrimitiveType::String)),
                }),
                doc: Some(
                    "Derived `key=value` tokens over resource, scope, and record attributes"
                        .to_string(),
                ),
                initial_default: None,
                write_default: None,
            });
        }

        Ok(Schema::from_struct_type(StructType::new(fields), 0, None))
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
    fn to_iceberg_schema_appends_materialized_label_columns() {
        let base = ResolvedSchema {
            version: "v1".to_string(),
            description: "test".to_string(),
            fields: vec![
                ResolvedField {
                    name: "timestamp".to_string(),
                    field_type: "timestamp_ns".to_string(),
                    required: true,
                    computed: None,
                    field_id: 1,
                },
                ResolvedField {
                    name: "body".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    computed: None,
                    field_id: 2,
                },
            ],
            partition_by: vec![],
        };

        // No labels → base schema unchanged.
        let plain = base.to_iceberg_schema().unwrap();
        assert_eq!(plain.fields().iter().count(), 2);

        // Labels → one optional `label_<key>` column each; dotted keys are
        // sanitized and duplicates collapsed.
        let labels = vec![
            "namespace".to_string(),
            "http.method".to_string(),
            "namespace".to_string(),
        ];
        let s = base.to_iceberg_schema_with_labels(&labels).unwrap();
        let names: Vec<String> = s.fields().iter().map(|f| f.name.clone()).collect();
        assert_eq!(names.len(), 4, "2 base + 2 unique labels, got {names:?}");
        assert!(names.contains(&"label_namespace".to_string()));
        assert!(names.contains(&"label_http_method".to_string()));

        // Materialized columns are optional strings.
        let ns = s
            .fields()
            .iter()
            .find(|f| f.name == "label_namespace")
            .unwrap();
        assert!(!ns.required);
        assert_eq!(ns.field_type, Type::Primitive(PrimitiveType::String));
    }

    #[test]
    fn attr_tokens_variant_appends_optional_list_column() {
        let base = ResolvedSchema {
            version: "v1".to_string(),
            description: "test".to_string(),
            fields: vec![
                ResolvedField {
                    name: "timestamp".to_string(),
                    field_type: "timestamp_ns".to_string(),
                    required: true,
                    computed: None,
                    field_id: 1,
                },
                ResolvedField {
                    name: "log_attributes".to_string(),
                    field_type: "map<string,string>".to_string(),
                    required: false,
                    computed: None,
                    field_id: 2,
                },
            ],
            partition_by: vec![],
        };

        let labels = vec!["namespace".to_string()];
        let schema = base
            .to_iceberg_schema_with_labels_and_attr_tokens(&labels)
            .unwrap();

        let tokens = schema
            .fields()
            .iter()
            .find(|f| f.name == "attr_tokens")
            .expect("attr_tokens column present");
        assert!(!tokens.required);
        let Type::List(list) = &tokens.field_type else {
            panic!("attr_tokens should be a List, got {:?}", tokens.field_type);
        };
        assert_eq!(*list.element, Type::Primitive(PrimitiveType::String));
        assert!(!list.element_required);

        // IDs stay unique across top-level, nested map, label, and list
        // element IDs.
        let label = schema
            .fields()
            .iter()
            .find(|f| f.name == "label_namespace")
            .unwrap();
        let mut ids = vec![1, 2, label.id, tokens.id, list.element_id];
        if let Type::Map(m) = &schema
            .fields()
            .iter()
            .find(|f| f.name == "log_attributes")
            .unwrap()
            .field_type
        {
            ids.push(m.key_id);
            ids.push(m.value_id);
        }
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "duplicate field IDs in {ids:?}");

        // The labels-only variant stays token-free.
        let plain = base.to_iceberg_schema_with_labels(&labels).unwrap();
        assert!(!plain.fields().iter().any(|f| f.name == "attr_tokens"));
    }

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
