//! Catalog persistence for the attribute type authority (`attribute_types`
//! table): the one canonical type per (tenant, dataset, signal, level, key),
//! established once via an atomic first-seen insert and never retyped.

use sqlx::{Row, query};

use crate::catalog::Catalog;
use crate::schema::logical::{AttributeLevel, LogicalFieldId, LogicalSchema};
use crate::schema::type_authority::{CanonicalType, Resolution, TypeSource};

/// Errors from attribute-type-authority storage.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("field has no attribute level; only attributes have a canonical type")]
    NoLevel,
    #[error("stored attribute type is corrupt: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// The committed canonical type for one field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredType {
    pub canonical: CanonicalType,
    pub source: TypeSource,
    pub hint_schema_url: Option<String>,
    pub schema_version: String,
    pub off_type_count: i64,
}

/// One stored row for an attribute key, scoped to a single dataset, signal,
/// and attribute level within a tenant.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, utoipa::ToSchema)]
pub struct AttributeTypeRecord {
    pub dataset: String,
    pub signal: String,
    pub level: AttributeLevel,
    pub canonical_type: CanonicalType,
    pub source: TypeSource,
    pub hint_schema_url: Option<String>,
    pub off_type_count: i64,
}

impl CanonicalType {
    fn as_str(self) -> &'static str {
        match self {
            CanonicalType::String => "string",
            CanonicalType::Int64 => "int64",
            CanonicalType::Float64 => "float64",
            CanonicalType::Bool => "bool",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "string" => Ok(CanonicalType::String),
            "int64" => Ok(CanonicalType::Int64),
            "float64" => Ok(CanonicalType::Float64),
            "bool" => Ok(CanonicalType::Bool),
            other => Err(StoreError::Corrupt(format!(
                "unknown canonical_type `{other}`"
            ))),
        }
    }
}

impl TypeSource {
    fn as_str(self) -> &'static str {
        match self {
            TypeSource::Config => "config",
            TypeSource::Semconv => "semconv",
            TypeSource::Observed => "observed",
        }
    }

    fn parse(value: &str) -> Result<Self, StoreError> {
        match value {
            "config" => Ok(TypeSource::Config),
            "semconv" => Ok(TypeSource::Semconv),
            "observed" => Ok(TypeSource::Observed),
            other => Err(StoreError::Corrupt(format!("unknown source `{other}`"))),
        }
    }
}

fn level_of(field: &LogicalFieldId) -> Result<&'static str, StoreError> {
    Ok(field.level.ok_or(StoreError::NoLevel)?.as_str())
}

/// How an upsert resolves a conflict on an already-established row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Conflict {
    /// The monotonic first-seen insert: an existing row is left untouched.
    Keep,
    /// The config-override path: an existing row is retyped.
    Override,
}

const SQLITE_UPSERT_KEEP: &str = r#"
INSERT INTO attribute_types
    (tenant_id, dataset_id, signal, level, attr_key,
     canonical_type, source, hint_schema_url, schema_version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tenant_id, dataset_id, signal, level, attr_key)
    DO UPDATE SET attr_key = attribute_types.attr_key
RETURNING canonical_type, source, hint_schema_url, schema_version, off_type_count
"#;

const SQLITE_UPSERT_OVERRIDE: &str = r#"
INSERT INTO attribute_types
    (tenant_id, dataset_id, signal, level, attr_key,
     canonical_type, source, hint_schema_url, schema_version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (tenant_id, dataset_id, signal, level, attr_key)
    DO UPDATE SET
        canonical_type = excluded.canonical_type,
        source = excluded.source,
        hint_schema_url = excluded.hint_schema_url,
        schema_version = excluded.schema_version,
        updated_at = datetime('now')
RETURNING canonical_type, source, hint_schema_url, schema_version, off_type_count
"#;

const POSTGRES_UPSERT_KEEP: &str = r#"
INSERT INTO attribute_types
    (tenant_id, dataset_id, signal, level, attr_key,
     canonical_type, source, hint_schema_url, schema_version)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tenant_id, dataset_id, signal, level, attr_key)
    DO UPDATE SET attr_key = attribute_types.attr_key
RETURNING canonical_type, source, hint_schema_url, schema_version, off_type_count
"#;

const POSTGRES_UPSERT_OVERRIDE: &str = r#"
INSERT INTO attribute_types
    (tenant_id, dataset_id, signal, level, attr_key,
     canonical_type, source, hint_schema_url, schema_version)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tenant_id, dataset_id, signal, level, attr_key)
    DO UPDATE SET
        canonical_type = excluded.canonical_type,
        source = excluded.source,
        hint_schema_url = excluded.hint_schema_url,
        schema_version = excluded.schema_version,
        updated_at = NOW()
RETURNING canonical_type, source, hint_schema_url, schema_version, off_type_count
"#;

fn row_to_stored<R: Row>(row: &R) -> Result<StoredType, StoreError>
where
    for<'a> &'a str: sqlx::ColumnIndex<R>,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    let canonical_type: String = row.get("canonical_type");
    let source: String = row.get("source");
    Ok(StoredType {
        canonical: CanonicalType::parse(&canonical_type)?,
        source: TypeSource::parse(&source)?,
        hint_schema_url: row.get("hint_schema_url"),
        schema_version: row.get("schema_version"),
        off_type_count: row.get("off_type_count"),
    })
}

impl Catalog {
    /// Insert `field`'s canonical type, or apply `conflict`'s resolution if a
    /// row already exists.
    async fn upsert_attribute_type(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        field: &LogicalFieldId,
        resolution: Resolution<'_>,
        conflict: Conflict,
    ) -> Result<StoredType, StoreError> {
        let level = level_of(field)?;
        let canonical = resolution.canonical.as_str();
        let source = resolution.source.as_str();
        let hint_schema_url = resolution.hint_schema_url;
        let schema_version = LogicalSchema::VERSION;

        match self {
            Catalog::Sqlite(pool) => {
                let sql = match conflict {
                    Conflict::Keep => SQLITE_UPSERT_KEEP,
                    Conflict::Override => SQLITE_UPSERT_OVERRIDE,
                };
                let row = query(sql)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(&field.source)
                    .bind(level)
                    .bind(&field.name)
                    .bind(canonical)
                    .bind(source)
                    .bind(hint_schema_url)
                    .bind(schema_version)
                    .fetch_one(pool)
                    .await?;
                row_to_stored(&row)
            }
            Catalog::Postgres(pool) => {
                let sql = match conflict {
                    Conflict::Keep => POSTGRES_UPSERT_KEEP,
                    Conflict::Override => POSTGRES_UPSERT_OVERRIDE,
                };
                let row = query(sql)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(&field.source)
                    .bind(level)
                    .bind(&field.name)
                    .bind(canonical)
                    .bind(source)
                    .bind(hint_schema_url)
                    .bind(schema_version)
                    .fetch_one(pool)
                    .await?;
                row_to_stored(&row)
            }
        }
    }

    /// Commit the canonical type for `field` if none is stored yet, then
    /// return the committed row — the winner of a first-seen race, which may
    /// differ from `resolution` when a concurrent writer won first. Never
    /// updates an existing row (the canonical type is monotonic).
    pub async fn establish_attribute_type(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        field: &LogicalFieldId,
        resolution: Resolution<'_>,
    ) -> Result<StoredType, StoreError> {
        self.upsert_attribute_type(tenant_id, dataset_id, field, resolution, Conflict::Keep)
            .await
    }

    /// Pin `field`'s canonical type to `canonical` by operator config,
    /// creating the row if none exists. The only path that may change an
    /// already-established row's canonical type: `source` becomes `config`,
    /// `hint_schema_url` is cleared, and `off_type_count` is preserved (an
    /// existing row) or starts at zero (a new one).
    pub async fn override_attribute_type(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        field: &LogicalFieldId,
        canonical: CanonicalType,
    ) -> Result<StoredType, StoreError> {
        let resolution = Resolution {
            canonical,
            source: TypeSource::Config,
            hint_schema_url: None,
        };
        self.upsert_attribute_type(tenant_id, dataset_id, field, resolution, Conflict::Override)
            .await
    }

    /// The committed canonical type for `field`, if one has been established.
    pub async fn get_attribute_type(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        field: &LogicalFieldId,
    ) -> Result<Option<StoredType>, StoreError> {
        let level = level_of(field)?;

        match self {
            Catalog::Sqlite(pool) => query(
                "SELECT canonical_type, source, hint_schema_url, schema_version, off_type_count \
                 FROM attribute_types \
                 WHERE tenant_id = ? AND dataset_id = ? AND signal = ? AND level = ? AND attr_key = ?",
            )
            .bind(tenant_id)
            .bind(dataset_id)
            .bind(&field.source)
            .bind(level)
            .bind(&field.name)
            .fetch_optional(pool)
            .await?
            .as_ref()
            .map(row_to_stored)
            .transpose(),
            Catalog::Postgres(pool) => query(
                "SELECT canonical_type, source, hint_schema_url, schema_version, off_type_count \
                 FROM attribute_types \
                 WHERE tenant_id = $1 AND dataset_id = $2 AND signal = $3 AND level = $4 AND attr_key = $5",
            )
            .bind(tenant_id)
            .bind(dataset_id)
            .bind(&field.source)
            .bind(level)
            .bind(&field.name)
            .fetch_optional(pool)
            .await?
            .as_ref()
            .map(row_to_stored)
            .transpose(),
        }
    }

    /// Record `count` more off-type occurrences for `field`. A no-op if the
    /// field has no established canonical type yet.
    pub async fn record_off_type(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        field: &LogicalFieldId,
        count: i64,
    ) -> Result<(), StoreError> {
        let level = level_of(field)?;

        match self {
            Catalog::Sqlite(pool) => {
                query(
                    "UPDATE attribute_types SET off_type_count = off_type_count + ?, \
                     updated_at = datetime('now') \
                     WHERE tenant_id = ? AND dataset_id = ? AND signal = ? AND level = ? AND attr_key = ?",
                )
                .bind(count)
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(&field.source)
                .bind(level)
                .bind(&field.name)
                .execute(pool)
                .await?;
            }
            Catalog::Postgres(pool) => {
                query(
                    "UPDATE attribute_types SET off_type_count = off_type_count + $1, \
                     updated_at = NOW() \
                     WHERE tenant_id = $2 AND dataset_id = $3 AND signal = $4 AND level = $5 AND attr_key = $6",
                )
                .bind(count)
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(&field.source)
                .bind(level)
                .bind(&field.name)
                .execute(pool)
                .await?;
            }
        }

        Ok(())
    }

    /// Every stored row for `attr_key` across the tenant's datasets, signals,
    /// and attribute levels, ordered by (dataset, signal, level).
    pub async fn list_attribute_types(
        &self,
        tenant_id: &str,
        attr_key: &str,
    ) -> Result<Vec<AttributeTypeRecord>, StoreError> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query(
                    "SELECT dataset_id, signal, level, canonical_type, source, \
                     hint_schema_url, schema_version, off_type_count \
                     FROM attribute_types \
                     WHERE tenant_id = ? AND attr_key = ? \
                     ORDER BY dataset_id, signal, level",
                )
                .bind(tenant_id)
                .bind(attr_key)
                .fetch_all(pool)
                .await?;
                rows.iter().map(row_to_record).collect()
            }
            Catalog::Postgres(pool) => {
                let rows = query(
                    "SELECT dataset_id, signal, level, canonical_type, source, \
                     hint_schema_url, schema_version, off_type_count \
                     FROM attribute_types \
                     WHERE tenant_id = $1 AND attr_key = $2 \
                     ORDER BY dataset_id, signal, level",
                )
                .bind(tenant_id)
                .bind(attr_key)
                .fetch_all(pool)
                .await?;
                rows.iter().map(row_to_record).collect()
            }
        }
    }

    /// Every established canonical type for one (tenant, dataset, signal) —
    /// one query per table rather than one per candidate key. Used by the
    /// compactor's typed-table promotion filter (epic #737, change
    /// otel-native-schema).
    pub async fn list_attribute_types_for_table(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
    ) -> Result<Vec<AttributeKeyType>, StoreError> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query(
                    "SELECT attr_key, level, canonical_type FROM attribute_types \
                     WHERE tenant_id = ? AND dataset_id = ? AND signal = ?",
                )
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(signal)
                .fetch_all(pool)
                .await?;
                rows.iter().map(row_to_key_type).collect()
            }
            Catalog::Postgres(pool) => {
                let rows = query(
                    "SELECT attr_key, level, canonical_type FROM attribute_types \
                     WHERE tenant_id = $1 AND dataset_id = $2 AND signal = $3",
                )
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(signal)
                .fetch_all(pool)
                .await?;
                rows.iter().map(row_to_key_type).collect()
            }
        }
    }
}

/// One key's committed canonical type at one attribute level, scoped to a
/// single (tenant, dataset, signal) table — the shape
/// [`Catalog::list_attribute_types_for_table`] needs and
/// [`AttributeTypeRecord`] doesn't carry (it already knows its key).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeKeyType {
    pub attr_key: String,
    pub level: AttributeLevel,
    pub canonical_type: CanonicalType,
}

/// Decodes a stored `level` column, shared by every row decoder that reads
/// one.
fn parse_level(level: &str) -> Result<AttributeLevel, StoreError> {
    AttributeLevel::parse(level)
        .ok_or_else(|| StoreError::Corrupt(format!("unknown level `{level}`")))
}

fn row_to_key_type<R: Row>(row: &R) -> Result<AttributeKeyType, StoreError>
where
    for<'a> &'a str: sqlx::ColumnIndex<R>,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    let canonical_type: String = row.get("canonical_type");
    let level: String = row.get("level");
    Ok(AttributeKeyType {
        attr_key: row.get("attr_key"),
        level: parse_level(&level)?,
        canonical_type: CanonicalType::parse(&canonical_type)?,
    })
}

/// Decodes the same `canonical_type`/`source`/`hint_schema_url`/
/// `off_type_count` columns as [`row_to_stored`], plus the `dataset_id`,
/// `signal`, and `level` a per-key listing also needs.
fn row_to_record<R: Row>(row: &R) -> Result<AttributeTypeRecord, StoreError>
where
    for<'a> &'a str: sqlx::ColumnIndex<R>,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    let stored = row_to_stored(row)?;
    let level: String = row.get("level");
    Ok(AttributeTypeRecord {
        dataset: row.get("dataset_id"),
        signal: row.get("signal"),
        level: parse_level(&level)?,
        canonical_type: stored.canonical,
        source: stored.source,
        hint_schema_url: stored.hint_schema_url,
        off_type_count: stored.off_type_count,
    })
}
