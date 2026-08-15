use anyhow::{Result, anyhow};
use iceberg_rust::spec::schema::Schema;
use iceberg_rust::spec::types::{ListType, MapType, PrimitiveType, StructField, StructType, Type};
use serde::{Deserialize, Serialize};
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
    #[serde(default)]
    pub metrics_exponential_histogram: HashMap<String, TableSchemaDefinition>,
    #[serde(default)]
    pub metrics_summary: HashMap<String, TableSchemaDefinition>,
    #[serde(default)]
    pub profiles: HashMap<String, TableSchemaDefinition>,
}

#[derive(Debug, Deserialize)]
pub struct SchemaMetadata {
    pub description: String,
    pub current_trace_version: String,
    pub current_log_version: String,
    pub current_metric_version: String,
    #[serde(default = "default_logical_schema_version")]
    pub logical_schema_version: String,
}

fn default_logical_schema_version() -> String {
    "otel-2026-08".to_string()
}

#[derive(Debug, Deserialize, Clone)]
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
    pub field_removals: Vec<FieldRemoval>,
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
    #[serde(default)]
    pub physical_only: bool,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FieldRename {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FieldRemoval {
    pub name: String,
}

/// A resolved schema with all inheritance applied
#[derive(Debug, Clone, Serialize)]
pub struct ResolvedSchema {
    pub version: String,
    pub description: String,
    pub fields: Vec<ResolvedField>,
    pub partition_by: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ResolvedField {
    pub name: String,
    pub field_type: String,
    pub required: bool,
    pub computed: Option<String>,
    pub physical_only: bool,
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

    /// Version of the client-visible OTel logical schema, independent from
    /// the physical Iceberg table realization selected above.
    pub fn logical_schema_version(&self) -> &str {
        &self.metadata.logical_schema_version
    }

    /// Resolve a trace schema by version
    pub fn resolve_trace_schema(&self, version: &str) -> Result<ResolvedSchema> {
        self.resolve_table_schema(&self.traces, version)
    }

    /// Resolve a log schema by version
    pub fn resolve_log_schema(&self, version: &str) -> Result<ResolvedSchema> {
        self.resolve_table_schema(&self.logs, version)
    }

    /// Generic schema resolver that handles inheritance. Public so callers
    /// with a source-keyed map they don't have a dedicated
    /// `resolve_*_schema` wrapper for (e.g. admin schema introspection over
    /// `metrics_gauge`/`metrics_sum`/`metrics_histogram`) can still resolve
    /// it without duplicating the inheritance/rename/addition logic.
    #[allow(clippy::only_used_in_recursion)]
    pub fn resolve_table_schema(
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
                    physical_only: field.physical_only || field.computed.is_some(),
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
                physical_only: addition.physical_only || addition.computed.is_some(),
                field_id: resolved_fields.len() + 1,
            };
            field_names.insert(addition.name.clone(), resolved_fields.len());
            resolved_fields.push(resolved);
        }

        // Apply field removals
        for removal in &schema_def.field_removals {
            if let Some(idx) = field_names.remove(&removal.name) {
                resolved_fields.remove(idx);
                // Every field after the removed one shifted down by one
                // position in `resolved_fields`; keep `field_names` in sync.
                for stored_idx in field_names.values_mut() {
                    if *stored_idx > idx {
                        *stored_idx -= 1;
                    }
                }
            }
        }

        for partition in &schema_def.partition_by {
            if let Some(field) = resolved_fields
                .iter_mut()
                .find(|field| field.name == *partition)
            {
                field.physical_only = true;
            }
        }

        Ok(ResolvedSchema {
            version: version.to_string(),
            description: schema_def.description.clone(),
            fields: resolved_fields,
            partition_by: schema_def.partition_by.clone(),
        })
    }
}

/// Walks the `inherits` chain backward from `to_version` until it reaches
/// `from_version` (exclusive) or the chain's root (the version with no
/// `inherits`), then reverses the result -- so the returned list is the
/// forward hop order a table recorded at `from_version` (or predating
/// version tracking entirely, when `None`) must walk to reach
/// `to_version`, one version at a time.
///
/// Version names carry no ordering of their own -- `"physical-v3"` is not
/// known to come after `"physical-v2"` by parsing the string. Only
/// `inherits` pointers encode the chain, and they point backward (child to
/// parent), so this walks backward first and reverses.
///
/// Returns `Ok(None)` when `from_version` is `Some` but never appears while
/// walking to the root -- an unrecognized or stale recorded version. Hops
/// carry `field_removals`/renames with `allow_removals` enabled, computed by
/// assuming the table's live schema exactly matches each hop's prior
/// version; a table whose recorded version isn't actually on this chain
/// doesn't satisfy that assumption; walking the full chain from root anyway
/// could delete legacy fields the table genuinely has that this schema's
/// history simply never mentions. Callers must treat `None` the same as an
/// entirely unrecorded baseline (additions-only, direct jump to
/// `to_version`), never as "apply every hop".
pub fn version_chain(
    schemas: &HashMap<String, TableSchemaDefinition>,
    from_version: Option<&str>,
    to_version: &str,
) -> Result<Option<Vec<String>>> {
    let mut chain = Vec::new();
    let mut current = to_version.to_string();
    loop {
        if Some(current.as_str()) == from_version {
            chain.reverse();
            return Ok(Some(chain));
        }
        chain.push(current.clone());
        let def = schemas
            .get(&current)
            .ok_or_else(|| anyhow!("Schema version {} not found", current))?;
        match &def.inherits {
            Some(parent) => current = parent.clone(),
            None => {
                // Reached the root. With no `from_version` to find, the
                // full chain to root is exactly what was asked for.
                if from_version.is_none() {
                    chain.reverse();
                    return Ok(Some(chain));
                }
                return Ok(None);
            }
        }
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

    /// Field IDs here are assigned positionally (`idx as i32 + 1`), recomputed
    /// fresh on every call. That's only safe for a table being created for
    /// the first time — there's no prior Parquet data whose column mapping
    /// could disagree. It is NOT safe to diff this output's field IDs
    /// against an existing table's live schema (e.g. to decide what to add
    /// or remove when evolving that table): a version that removes a field
    /// in the middle of the list shifts every subsequent field's ID here,
    /// which would silently corrupt the ID mapping already burned into that
    /// table's existing Parquet files. Evolving a live table must diff by
    /// field *name* against the table's actual persisted `Schema`, reusing
    /// its existing IDs untouched and minting new ones only for genuine
    /// additions — never regenerate a live table's target schema from this
    /// function.
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
    fn resolve_table_schema_is_public_and_works_for_non_trace_log_tables() {
        // Metrics tables have no dedicated `resolve_*_schema` wrapper (only
        // traces/logs do); admin schema introspection needs the generic
        // resolver directly, so it must be reachable from outside this
        // module.
        let defs = SchemaDefinitions::from_toml(crate::schema::SCHEMA_DEFINITIONS_TOML).unwrap();
        let resolved = defs
            .resolve_table_schema(&defs.metrics_gauge, "physical-v1")
            .unwrap();
        assert!(
            resolved.fields.iter().any(|f| f.name == "metric_name"),
            "expected a metric_name field, got {:?}",
            resolved.fields.iter().map(|f| &f.name).collect::<Vec<_>>()
        );
        assert_eq!(resolved.partition_by, vec!["timestamp".to_string()]);
    }

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
                    physical_only: false,
                    field_id: 1,
                },
                ResolvedField {
                    name: "body".to_string(),
                    field_type: "string".to_string(),
                    required: false,
                    computed: None,
                    physical_only: false,
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
                    physical_only: false,
                    field_id: 1,
                },
                ResolvedField {
                    name: "log_attributes".to_string(),
                    field_type: "map<string,string>".to_string(),
                    required: false,
                    computed: None,
                    physical_only: false,
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
current_trace_version = "physical-v2"
current_log_version = "physical-v1"
current_metric_version = "physical-v1"

[traces.physical-v1]
description = "Base schema"
fields = [
    { name = "trace_id", type = "string", required = true },
    { name = "name", type = "string", required = true },
]

[traces.physical-v2]
description = "Extended schema"
inherits = "physical-v1"
field_renames = [
    { from = "name", to = "span_name" },
]
field_additions = [
    { name = "timestamp", type = "timestamp_ns", required = true, computed = "start_time" },
]
partition_by = ["timestamp"]

[logs.physical-v1]
description = "Log schema"
fields = [
    { name = "timestamp", type = "timestamp_ns", required = true },
]
"#;

        let schemas = SchemaDefinitions::from_toml(toml).unwrap();
        assert_eq!(schemas.current_trace_version(), "physical-v2");

        // Test v1 resolution
        let v1 = schemas.resolve_trace_schema("physical-v1").unwrap();
        assert_eq!(v1.fields.len(), 2);
        assert_eq!(v1.fields[0].name, "trace_id");
        assert_eq!(v1.fields[1].name, "name");

        // Test v2 resolution with inheritance and rename
        let v2 = schemas.resolve_trace_schema("physical-v2").unwrap();
        assert_eq!(v2.fields.len(), 3);
        assert_eq!(v2.fields[0].name, "trace_id");
        assert_eq!(v2.fields[1].name, "span_name"); // Renamed from "name"
        assert_eq!(v2.fields[2].name, "timestamp");
        assert_eq!(v2.fields[2].computed, Some("start_time".to_string()));
        assert!(v2.fields[2].physical_only);
        assert!(
            v2.fields
                .iter()
                .find(|field| field.name == "timestamp")
                .unwrap()
                .physical_only
        );
    }

    #[test]
    fn field_removals_drop_the_named_field_from_the_resolved_schema() {
        let toml = r#"
[metadata]
description = "Test schemas"
current_trace_version = "physical-v3"
current_log_version = "physical-v1"
current_metric_version = "physical-v1"

[traces.physical-v1]
description = "Base schema"
fields = [
    { name = "trace_id", type = "string", required = true },
    { name = "deprecated_field", type = "string", required = false },
    { name = "span_name", type = "string", required = true },
]

[traces.physical-v3]
description = "Removes deprecated_field"
inherits = "physical-v1"
field_removals = [
    { name = "deprecated_field" },
]

[logs.physical-v1]
description = "Log schema"
fields = [
    { name = "timestamp", type = "timestamp_ns", required = true },
]
"#;

        let schemas = SchemaDefinitions::from_toml(toml).unwrap();

        // A version with no field_removals is unaffected.
        let v1 = schemas.resolve_trace_schema("physical-v1").unwrap();
        assert_eq!(v1.fields.len(), 3);
        assert!(v1.fields.iter().any(|f| f.name == "deprecated_field"));

        // The version declaring the removal resolves without the field,
        // and every remaining field survives with its name intact.
        let v3 = schemas.resolve_trace_schema("physical-v3").unwrap();
        assert_eq!(v3.fields.len(), 2);
        assert!(!v3.fields.iter().any(|f| f.name == "deprecated_field"));
        assert_eq!(v3.fields[0].name, "trace_id");
        assert_eq!(v3.fields[1].name, "span_name");
    }

    fn chain_fixture() -> HashMap<String, TableSchemaDefinition> {
        let mut schemas = HashMap::new();
        schemas.insert(
            "physical-v1".to_string(),
            TableSchemaDefinition {
                description: "root".to_string(),
                inherits: None,
                fields: vec![],
                field_renames: vec![],
                field_additions: vec![],
                field_removals: vec![],
                partition_by: vec![],
            },
        );
        schemas.insert(
            "physical-v2".to_string(),
            TableSchemaDefinition {
                description: "hop 1".to_string(),
                inherits: Some("physical-v1".to_string()),
                fields: vec![],
                field_renames: vec![],
                field_additions: vec![],
                field_removals: vec![],
                partition_by: vec![],
            },
        );
        schemas.insert(
            "physical-v3".to_string(),
            TableSchemaDefinition {
                description: "hop 2".to_string(),
                inherits: Some("physical-v2".to_string()),
                fields: vec![],
                field_renames: vec![],
                field_additions: vec![],
                field_removals: vec![],
                partition_by: vec![],
            },
        );
        schemas
    }

    #[test]
    fn version_chain_from_none_walks_the_full_chain_root_first() {
        let schemas = chain_fixture();
        let chain = version_chain(&schemas, None, "physical-v3")
            .unwrap()
            .unwrap();
        assert_eq!(chain, vec!["physical-v1", "physical-v2", "physical-v3"]);
    }

    #[test]
    fn version_chain_from_a_known_version_walks_only_the_remaining_hops() {
        let schemas = chain_fixture();
        let chain = version_chain(&schemas, Some("physical-v1"), "physical-v3")
            .unwrap()
            .unwrap();
        assert_eq!(chain, vec!["physical-v2", "physical-v3"]);
    }

    #[test]
    fn version_chain_already_at_target_is_empty() {
        let schemas = chain_fixture();
        let chain = version_chain(&schemas, Some("physical-v3"), "physical-v3")
            .unwrap()
            .unwrap();
        assert!(chain.is_empty());
    }

    #[test]
    fn version_chain_ordering_does_not_depend_on_name_lexical_order() {
        // Names are arbitrary labels -- only `inherits` encodes order.
        let mut schemas = HashMap::new();
        schemas.insert(
            "alpha".to_string(),
            TableSchemaDefinition {
                description: "root".to_string(),
                inherits: None,
                fields: vec![],
                field_renames: vec![],
                field_additions: vec![],
                field_removals: vec![],
                partition_by: vec![],
            },
        );
        schemas.insert(
            "zeta-but-actually-next".to_string(),
            TableSchemaDefinition {
                description: "hop".to_string(),
                inherits: Some("alpha".to_string()),
                fields: vec![],
                field_renames: vec![],
                field_additions: vec![],
                field_removals: vec![],
                partition_by: vec![],
            },
        );
        let chain = version_chain(&schemas, None, "zeta-but-actually-next")
            .unwrap()
            .unwrap();
        assert_eq!(chain, vec!["alpha", "zeta-but-actually-next"]);
    }

    #[test]
    fn version_chain_returns_none_for_an_unrecognized_recorded_version() {
        // A recorded `signaldb.schema.version` that isn't actually on this
        // table's chain (corrupted property, or a version retired from
        // schemas.toml) must not be trusted as a real hop-by-hop baseline --
        // the caller has to fall back to an untrusted, additions-only
        // migration instead of walking the full chain with removals on.
        let schemas = chain_fixture();
        let chain =
            version_chain(&schemas, Some("physical-v0-does-not-exist"), "physical-v3").unwrap();
        assert!(chain.is_none());
    }
}
