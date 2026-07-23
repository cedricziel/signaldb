//! # Profile Query Service
//!
//! DataFusion-backed queries against the tenant-scoped `profiles` Iceberg
//! table: single-profile lookup, time-range search, and the discovery
//! queries (profile types, label names/values) that back the Pyroscope API.
//!
//! Lookup and search return storage-format RecordBatches so the Flight
//! layer can stream them without re-encoding; discovery queries return
//! plain string lists.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::{
    arrow::array::{Array, RecordBatch, StringArray},
    logical_expr::{col, lit},
    prelude::{DataFrame, SessionContext},
    scalar::ScalarValue,
};

use super::{error::QuerierError, table_ref::build_table_reference};

/// Parameters for single-profile lookup.
#[derive(Debug)]
pub struct FindProfileByIdParams {
    /// Hex-encoded profile ID as stored in the profiles table.
    pub profile_id: String,
}

/// Search parameters carried in the `search_profiles` Flight ticket.
/// Unknown JSON fields in the ticket are ignored on deserialization.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ProfileSearchParams {
    /// Restrict to a service (`service.name` resource attribute).
    pub service_name: Option<String>,
    /// Restrict to a sample type (e.g. "cpu").
    pub sample_type: Option<String>,
    /// Search window start (unix seconds, inclusive).
    pub start: Option<i64>,
    /// Search window end (unix seconds, inclusive).
    pub end: Option<i64>,
    /// Maximum number of profiles to return.
    pub limit: Option<i32>,
}

/// Time-window parameters shared by the discovery queries
/// (profile types, label names, label values).
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct ProfileDiscoveryParams {
    /// Window start (unix seconds, inclusive).
    pub start: Option<i64>,
    /// Window end (unix seconds, inclusive).
    pub end: Option<i64>,
}

/// Upper bound on distinct attribute documents scanned when deriving
/// label names/values from the JSON attribute columns.
const LABEL_SCAN_LIMIT: usize = 1000;

pub struct ProfileService {
    session_context: Arc<SessionContext>,
    /// Upper bound for the client-supplied `limit` on search.
    max_search_limit: usize,
}

impl Debug for ProfileService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProfileService")
            .field("session_context", &"set")
            .field("max_search_limit", &self.max_search_limit)
            .finish()
    }
}

impl Clone for ProfileService {
    fn clone(&self) -> Self {
        Self {
            session_context: Arc::clone(&self.session_context),
            max_search_limit: self.max_search_limit,
        }
    }
}

impl ProfileService {
    pub fn new(session_context: SessionContext) -> Self {
        Self {
            session_context: Arc::new(session_context),
            max_search_limit: common::config::QuerierConfig::default().max_search_limit,
        }
    }

    /// Override the clamp applied to client-supplied search limits.
    pub fn with_max_search_limit(mut self, max_search_limit: usize) -> Self {
        self.max_search_limit = max_search_limit;
        self
    }

    async fn profiles_table(
        &self,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<DataFrame, QuerierError> {
        let table_ref = build_table_reference(tenant_slug, dataset_slug, "profiles")
            .map_err(|e| QuerierError::InvalidInput(e.to_string()))?;
        self.session_context.table(table_ref).await.map_err(|e| {
            log::error!(
                "Failed to access profiles table for tenant_slug={tenant_slug}, dataset_slug={dataset_slug}: {e}"
            );
            QuerierError::QueryFailed(e)
        })
    }

    /// Apply inclusive unix-second window bounds on the `timestamp` column.
    fn apply_time_window(
        df: DataFrame,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<DataFrame, QuerierError> {
        let mut df = df;
        if let Some(start) = start {
            let nanos = start.saturating_mul(1_000_000_000);
            df = df
                .filter(
                    col("timestamp")
                        .gt_eq(lit(ScalarValue::TimestampNanosecond(Some(nanos), None))),
                )
                .map_err(QuerierError::QueryFailed)?;
        }
        if let Some(end) = end {
            let nanos = end.saturating_mul(1_000_000_000);
            df = df
                .filter(
                    col("timestamp")
                        .lt_eq(lit(ScalarValue::TimestampNanosecond(Some(nanos), None))),
                )
                .map_err(QuerierError::QueryFailed)?;
        }
        Ok(df)
    }

    /// Find a profile by its hex ID. Returns storage-format batches;
    /// empty means not found.
    pub async fn find_by_id_with_tenant(
        &self,
        params: FindProfileByIdParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        if params.profile_id.is_empty() || !params.profile_id.chars().all(|c| c.is_ascii_hexdigit())
        {
            return Err(QuerierError::InvalidInput(format!(
                "profile_id must be a hex string, got '{}'",
                params.profile_id
            )));
        }

        let df = self
            .profiles_table(tenant_slug, dataset_slug)
            .await?
            .filter(col("profile_id").eq(lit(&params.profile_id)))
            .map_err(QuerierError::QueryFailed)?;

        let batches = df.collect().await.map_err(QuerierError::QueryFailed)?;
        Ok(batches.into_iter().filter(|b| b.num_rows() > 0).collect())
    }

    /// Search profiles in a time window, newest first. Returns
    /// storage-format batches without the bulky JSON payload columns.
    pub async fn search_with_tenant(
        &self,
        params: ProfileSearchParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<RecordBatch>, QuerierError> {
        let mut df = self.profiles_table(tenant_slug, dataset_slug).await?;

        if let Some(service_name) = &params.service_name {
            df = df
                .filter(col("service_name").eq(lit(service_name)))
                .map_err(QuerierError::QueryFailed)?;
        }
        if let Some(sample_type) = &params.sample_type {
            df = df
                .filter(col("sample_type").eq(lit(sample_type)))
                .map_err(QuerierError::QueryFailed)?;
        }
        df = Self::apply_time_window(df, params.start, params.end)?;

        let limit = params
            .limit
            .and_then(|l| usize::try_from(l).ok())
            .filter(|l| *l > 0)
            .unwrap_or(self.max_search_limit)
            .min(self.max_search_limit);

        let df = df
            .select_columns(&[
                "profile_id",
                "timestamp",
                "duration_nano",
                "sample_type",
                "sample_unit",
                "period",
                "service_name",
                "trace_id",
                "span_id",
            ])
            .map_err(QuerierError::QueryFailed)?
            .sort(vec![col("timestamp").sort(false, true)])
            .map_err(QuerierError::QueryFailed)?
            .limit(0, Some(limit))
            .map_err(QuerierError::QueryFailed)?;

        df.collect().await.map_err(QuerierError::QueryFailed)
    }

    /// Distinct profile types in the window, as `{sample_type}:{sample_unit}`.
    pub async fn profile_types_with_tenant(
        &self,
        params: ProfileDiscoveryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<String>, QuerierError> {
        let df = self.profiles_table(tenant_slug, dataset_slug).await?;
        let df = Self::apply_time_window(df, params.start, params.end)?;
        let batches = df
            .select_columns(&["sample_type", "sample_unit"])
            .map_err(QuerierError::QueryFailed)?
            .distinct()
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        let mut types = BTreeSet::new();
        for batch in &batches {
            let sample_types = string_column(batch, "sample_type")?;
            let sample_units = string_column(batch, "sample_unit")?;
            for i in 0..batch.num_rows() {
                if sample_types.is_null(i) {
                    continue;
                }
                let sample_type = sample_types.value(i);
                if sample_type.is_empty() {
                    continue;
                }
                let unit = if sample_units.is_null(i) {
                    ""
                } else {
                    sample_units.value(i)
                };
                types.insert(format!("{sample_type}:{unit}"));
            }
        }
        Ok(types.into_iter().collect())
    }

    /// Label names available in the window: `service_name` plus every key
    /// observed in the profile attribute documents.
    pub async fn label_names_with_tenant(
        &self,
        params: ProfileDiscoveryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<String>, QuerierError> {
        let df = self.profiles_table(tenant_slug, dataset_slug).await?;
        let df = Self::apply_time_window(df, params.start, params.end)?;
        let batches = df
            .select_columns(&["profile_attributes"])
            .map_err(QuerierError::QueryFailed)?
            .distinct()
            .map_err(QuerierError::QueryFailed)?
            .limit(0, Some(LABEL_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        let mut names: BTreeSet<String> = BTreeSet::new();
        names.insert("service_name".to_string());
        for batch in &batches {
            let attrs = string_column(batch, "profile_attributes")?;
            for i in 0..batch.num_rows() {
                if attrs.is_null(i) {
                    continue;
                }
                if let Ok(serde_json::Value::Object(map)) =
                    serde_json::from_str::<serde_json::Value>(attrs.value(i))
                {
                    names.extend(map.keys().cloned());
                }
            }
        }
        Ok(names.into_iter().collect())
    }

    /// Values for a label in the window. `service_name` reads the dedicated
    /// column; other labels come from the profile attribute documents.
    pub async fn label_values_with_tenant(
        &self,
        label_name: &str,
        params: ProfileDiscoveryParams,
        tenant_slug: &str,
        dataset_slug: &str,
    ) -> Result<Vec<String>, QuerierError> {
        if label_name.is_empty() {
            return Err(QuerierError::InvalidInput(
                "label name must not be empty".to_string(),
            ));
        }

        let df = self.profiles_table(tenant_slug, dataset_slug).await?;
        let df = Self::apply_time_window(df, params.start, params.end)?;

        if label_name == "service_name" {
            let batches = df
                .select_columns(&["service_name"])
                .map_err(QuerierError::QueryFailed)?
                .distinct()
                .map_err(QuerierError::QueryFailed)?
                .collect()
                .await
                .map_err(QuerierError::QueryFailed)?;
            let mut values = BTreeSet::new();
            for batch in &batches {
                let services = string_column(batch, "service_name")?;
                for i in 0..batch.num_rows() {
                    if !services.is_null(i) && !services.value(i).is_empty() {
                        values.insert(services.value(i).to_string());
                    }
                }
            }
            return Ok(values.into_iter().collect());
        }

        let batches = df
            .select_columns(&["profile_attributes"])
            .map_err(QuerierError::QueryFailed)?
            .distinct()
            .map_err(QuerierError::QueryFailed)?
            .limit(0, Some(LABEL_SCAN_LIMIT))
            .map_err(QuerierError::QueryFailed)?
            .collect()
            .await
            .map_err(QuerierError::QueryFailed)?;

        let mut values = BTreeSet::new();
        for batch in &batches {
            let attrs = string_column(batch, "profile_attributes")?;
            for i in 0..batch.num_rows() {
                if attrs.is_null(i) {
                    continue;
                }
                if let Ok(serde_json::Value::Object(map)) =
                    serde_json::from_str::<serde_json::Value>(attrs.value(i))
                    && let Some(value) = map.get(label_name)
                {
                    match value {
                        serde_json::Value::String(s) => {
                            values.insert(s.clone());
                        }
                        other => {
                            values.insert(other.to_string());
                        }
                    }
                }
            }
        }
        Ok(values.into_iter().collect())
    }
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a StringArray, QuerierError> {
    batch
        .column_by_name(name)
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| {
            QuerierError::InvalidInput(format!("column '{name}' missing or not a string column"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_params_deserialize_ignores_unknown_fields() {
        let params: ProfileSearchParams = serde_json::from_str(
            r#"{"service_name":"checkout","start":100,"end":200,"limit":10,"unknown":"x"}"#,
        )
        .expect("deserialize");
        assert_eq!(params.service_name.as_deref(), Some("checkout"));
        assert_eq!(params.start, Some(100));
        assert_eq!(params.limit, Some(10));
    }

    #[tokio::test]
    async fn find_by_id_rejects_non_hex_profile_id() {
        let service = ProfileService::new(SessionContext::new());
        let result = service
            .find_by_id_with_tenant(
                FindProfileByIdParams {
                    profile_id: "not-hex!".to_string(),
                },
                "acme",
                "prod",
            )
            .await;
        assert!(matches!(result, Err(QuerierError::InvalidInput(_))));
    }

    /// Register a profiles MemTable at acme.prod.profiles and exercise the
    /// full query surface against it.
    async fn context_with_profiles() -> SessionContext {
        use datafusion::arrow::array::{
            Date32Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
        };
        use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use datafusion::catalog::{
            CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider,
        };
        use datafusion::datasource::MemTable;

        let schema = Arc::new(Schema::new(vec![
            Field::new("profile_id", DataType::Utf8, false),
            Field::new(
                "timestamp",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("duration_nano", DataType::Int64, false),
            Field::new("sample_type", DataType::Utf8, false),
            Field::new("sample_unit", DataType::Utf8, false),
            Field::new("period_type", DataType::Utf8, true),
            Field::new("period_unit", DataType::Utf8, true),
            Field::new("period", DataType::Int64, true),
            Field::new("service_name", DataType::Utf8, false),
            Field::new("stacktraces_json", DataType::Utf8, false),
            Field::new("samples_json", DataType::Utf8, false),
            Field::new("resource_attributes", DataType::Utf8, true),
            Field::new("scope_attributes", DataType::Utf8, true),
            Field::new("profile_attributes", DataType::Utf8, true),
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("span_id", DataType::Utf8, true),
            Field::new("date_day", DataType::Date32, false),
            Field::new("hour", DataType::Int32, false),
        ]));

        let base_nanos: i64 = 1_700_000_000_000_000_000;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["aa".repeat(16), "bb".repeat(16)])),
                Arc::new(TimestampNanosecondArray::from(vec![
                    base_nanos,
                    base_nanos + 60_000_000_000,
                ])),
                Arc::new(Int64Array::from(vec![10_000_000_000, 10_000_000_000])),
                Arc::new(StringArray::from(vec!["cpu", "alloc_space"])),
                Arc::new(StringArray::from(vec!["nanoseconds", "bytes"])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(Int64Array::from(vec![None::<i64>, None])),
                Arc::new(StringArray::from(vec!["checkout", "billing"])),
                Arc::new(StringArray::from(vec!["[]", "[]"])),
                Arc::new(StringArray::from(vec!["[]", "[]"])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec![None::<&str>, None])),
                Arc::new(StringArray::from(vec![
                    Some(r#"{"host":"web-1"}"#),
                    Some(r#"{"host":"web-2","region":"eu"}"#),
                ])),
                Arc::new(StringArray::from(vec![Some("11".repeat(16)), None])),
                Arc::new(StringArray::from(vec![Some("22".repeat(8)), None])),
                Arc::new(Date32Array::from(vec![19676, 19676])),
                Arc::new(Int32Array::from(vec![8, 8])),
            ],
        )
        .expect("valid batch");

        let ctx = SessionContext::new();
        let catalog = Arc::new(MemoryCatalogProvider::new());
        let schema_provider = Arc::new(MemorySchemaProvider::new());
        schema_provider
            .register_table(
                "profiles".to_string(),
                Arc::new(MemTable::try_new(schema, vec![vec![batch]]).expect("memtable")),
            )
            .expect("register table");
        catalog
            .register_schema("prod", schema_provider)
            .expect("register schema");
        ctx.register_catalog("acme", catalog);
        ctx
    }

    #[tokio::test]
    async fn queries_profiles_from_registered_table() {
        let service = ProfileService::new(context_with_profiles().await);

        // find_by_id returns exactly the requested profile
        let batches = service
            .find_by_id_with_tenant(
                FindProfileByIdParams {
                    profile_id: "aa".repeat(16),
                },
                "acme",
                "prod",
            )
            .await
            .expect("find");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

        // search filters by service and returns summaries
        let batches = service
            .search_with_tenant(
                ProfileSearchParams {
                    service_name: Some("billing".to_string()),
                    ..ProfileSearchParams::default()
                },
                "acme",
                "prod",
            )
            .await
            .expect("search");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

        // profile types are distinct type:unit pairs
        let types = service
            .profile_types_with_tenant(ProfileDiscoveryParams::default(), "acme", "prod")
            .await
            .expect("types");
        assert_eq!(
            types,
            vec![
                "alloc_space:bytes".to_string(),
                "cpu:nanoseconds".to_string()
            ]
        );

        // label names merge service_name with attribute keys
        let names = service
            .label_names_with_tenant(ProfileDiscoveryParams::default(), "acme", "prod")
            .await
            .expect("names");
        assert_eq!(
            names,
            vec![
                "host".to_string(),
                "region".to_string(),
                "service_name".to_string()
            ]
        );

        // label values for a JSON attribute label
        let values = service
            .label_values_with_tenant("host", ProfileDiscoveryParams::default(), "acme", "prod")
            .await
            .expect("values");
        assert_eq!(values, vec!["web-1".to_string(), "web-2".to_string()]);

        // time window filtering excludes the second profile
        let batches = service
            .search_with_tenant(
                ProfileSearchParams {
                    start: Some(1_699_999_999),
                    end: Some(1_700_000_030),
                    ..ProfileSearchParams::default()
                },
                "acme",
                "prod",
            )
            .await
            .expect("windowed search");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    }

    #[tokio::test]
    async fn label_values_rejects_empty_label() {
        let service = ProfileService::new(SessionContext::new());
        let result = service
            .label_values_with_tenant("", ProfileDiscoveryParams::default(), "acme", "prod")
            .await;
        assert!(matches!(result, Err(QuerierError::InvalidInput(_))));
    }
}
