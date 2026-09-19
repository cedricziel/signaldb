//! Catalog persistence for tenant OTTL processors (`processors` table,
//! change: tenant-ottl-processors, design D3).

use sqlx::{Row, query};

use crate::catalog::Catalog;
use crate::processors::{ProcessorRecord, ProcessorSpec};

/// Errors from processor storage and mutation.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("processor `{0}` already exists")]
    Conflict(String),
    #[error("processor `{0}` not found")]
    NotFound(String),
    #[error("dataset `{0}` does not exist for this tenant")]
    UnknownDataset(String),
    #[error("invalid processor: {0}")]
    Invalid(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// `name` must be a slug: lowercase alphanumerics and hyphens, starting with
/// an alphanumeric, 1-63 characters.
fn validate_name(name: &str) -> Result<(), StoreError> {
    let mut chars = name.chars();
    let is_slug = matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit())
        && name.len() <= 63
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-');
    if !is_slug {
        return Err(StoreError::Invalid(format!(
            "name `{name}` must match [a-z0-9][a-z0-9-]{{0,62}}"
        )));
    }
    Ok(())
}

fn validate_signal(signal: &str) -> Result<(), StoreError> {
    match signal {
        "traces" | "logs" | "metrics" => Ok(()),
        other => Err(StoreError::Invalid(format!(
            "signal `{other}` must be one of traces, logs, metrics"
        ))),
    }
}

fn validate_error_mode(error_mode: &str) -> Result<(), StoreError> {
    error_mode
        .parse::<ottl::ErrorMode>()
        .map(|_| ())
        .map_err(|e| StoreError::Invalid(e.to_string()))
}

fn validate_spec(spec: &ProcessorSpec) -> Result<(), StoreError> {
    validate_name(&spec.name)?;
    validate_signal(&spec.signal)?;
    validate_error_mode(&spec.error_mode)?;
    Ok(())
}

fn row_to_record<R: Row>(row: &R) -> Result<ProcessorRecord, StoreError>
where
    for<'a> &'a str: sqlx::ColumnIndex<R>,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> bool: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> i32: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    let statements_json: String = row.get("statements");
    let statements: Vec<String> = serde_json::from_str(&statements_json)
        .map_err(|e| StoreError::Invalid(format!("corrupt statements column: {e}")))?;
    Ok(ProcessorRecord {
        tenant_id: row.get("tenant_id"),
        name: row.get("name"),
        dataset: row.get("dataset"),
        signal: row.get("signal"),
        enabled: row.get("enabled"),
        priority: row.get("priority"),
        error_mode: row.get("error_mode"),
        description: row.get("description"),
        statements,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

const SELECT_COLS: &str = "tenant_id, name, dataset, signal, enabled, priority, error_mode, \
     description, statements, CAST(created_at AS TEXT) AS created_at, \
     CAST(updated_at AS TEXT) AS updated_at";

impl Catalog {
    /// Every processor of a tenant (unordered).
    pub async fn list_processors(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<ProcessorRecord>, StoreError> {
        let sql_sqlite = format!("SELECT {SELECT_COLS} FROM processors WHERE tenant_id = ?");
        let sql_pg = format!("SELECT {SELECT_COLS} FROM processors WHERE tenant_id = $1");
        let rows = match self {
            Catalog::Sqlite(pool) => query(&sql_sqlite)
                .bind(tenant_id)
                .fetch_all(pool)
                .await?
                .iter()
                .map(row_to_record)
                .collect::<Result<Vec<_>, _>>()?,
            Catalog::Postgres(pool) => query(&sql_pg)
                .bind(tenant_id)
                .fetch_all(pool)
                .await?
                .iter()
                .map(row_to_record)
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(rows)
    }

    /// One processor by name, if present.
    pub async fn get_processor(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Option<ProcessorRecord>, StoreError> {
        let sql_sqlite =
            format!("SELECT {SELECT_COLS} FROM processors WHERE tenant_id = ? AND name = ?");
        let sql_pg =
            format!("SELECT {SELECT_COLS} FROM processors WHERE tenant_id = $1 AND name = $2");
        let row = match self {
            Catalog::Sqlite(pool) => query(&sql_sqlite)
                .bind(tenant_id)
                .bind(name)
                .fetch_optional(pool)
                .await?
                .as_ref()
                .map(row_to_record)
                .transpose()?,
            Catalog::Postgres(pool) => query(&sql_pg)
                .bind(tenant_id)
                .bind(name)
                .fetch_optional(pool)
                .await?
                .as_ref()
                .map(row_to_record)
                .transpose()?,
        };
        Ok(row)
    }

    /// Every enabled processor that applies to a request against `dataset`
    /// for `signal`: tenant-wide rows first, then by `priority` ascending,
    /// then by `name` (design D3 ordering).
    pub async fn select_processors_for_request(
        &self,
        tenant_id: &str,
        dataset: &str,
        signal: &str,
    ) -> Result<Vec<ProcessorRecord>, StoreError> {
        let order = "ORDER BY dataset IS NULL DESC, priority ASC, name ASC";
        let sql_sqlite = format!(
            "SELECT {SELECT_COLS} FROM processors \
             WHERE tenant_id = ? AND signal = ? AND enabled = TRUE \
             AND (dataset IS NULL OR dataset = ?) {order}"
        );
        let sql_pg = format!(
            "SELECT {SELECT_COLS} FROM processors \
             WHERE tenant_id = $1 AND signal = $2 AND enabled = TRUE \
             AND (dataset IS NULL OR dataset = $3) {order}"
        );
        let rows = match self {
            Catalog::Sqlite(pool) => query(&sql_sqlite)
                .bind(tenant_id)
                .bind(signal)
                .bind(dataset)
                .fetch_all(pool)
                .await?
                .iter()
                .map(row_to_record)
                .collect::<Result<Vec<_>, _>>()?,
            Catalog::Postgres(pool) => query(&sql_pg)
                .bind(tenant_id)
                .bind(signal)
                .bind(dataset)
                .fetch_all(pool)
                .await?
                .iter()
                .map(row_to_record)
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(rows)
    }

    /// Check that `dataset_name` exists for `tenant_id`, so a bad reference
    /// is rejected with a clear error rather than relying on the FK's
    /// (dialect-specific) error text.
    async fn dataset_exists(
        &self,
        tenant_id: &str,
        dataset_name: &str,
    ) -> Result<bool, sqlx::Error> {
        let datasets = self.get_datasets(tenant_id).await?;
        Ok(datasets.iter().any(|d| d.name == dataset_name))
    }

    /// Insert a new processor. Fails with [`StoreError::Conflict`] on a
    /// duplicate name and [`StoreError::UnknownDataset`] when `dataset` does
    /// not name an existing dataset of this tenant.
    pub async fn insert_processor(
        &self,
        tenant_id: &str,
        spec: &ProcessorSpec,
    ) -> Result<ProcessorRecord, StoreError> {
        validate_spec(spec)?;
        if let Some(dataset) = &spec.dataset
            && !self.dataset_exists(tenant_id, dataset).await?
        {
            return Err(StoreError::UnknownDataset(dataset.clone()));
        }
        let statements_json = serde_json::to_string(&spec.statements)
            .map_err(|e| StoreError::Invalid(format!("failed to serialize statements: {e}")))?;

        let sql_sqlite = "INSERT INTO processors \
            (tenant_id, name, dataset, signal, enabled, priority, error_mode, description, statements) \
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        let sql_pg = "INSERT INTO processors \
            (tenant_id, name, dataset, signal, enabled, priority, error_mode, description, statements) \
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)";
        let result: Result<u64, sqlx::Error> = match self {
            Catalog::Sqlite(pool) => query(sql_sqlite)
                .bind(tenant_id)
                .bind(&spec.name)
                .bind(&spec.dataset)
                .bind(&spec.signal)
                .bind(spec.enabled)
                .bind(spec.priority)
                .bind(&spec.error_mode)
                .bind(&spec.description)
                .bind(&statements_json)
                .execute(pool)
                .await
                .map(|r| r.rows_affected()),
            Catalog::Postgres(pool) => query(sql_pg)
                .bind(tenant_id)
                .bind(&spec.name)
                .bind(&spec.dataset)
                .bind(&spec.signal)
                .bind(spec.enabled)
                .bind(spec.priority)
                .bind(&spec.error_mode)
                .bind(&spec.description)
                .bind(&statements_json)
                .execute(pool)
                .await
                .map(|r| r.rows_affected()),
        };
        match result {
            Ok(_) => {}
            Err(sqlx::Error::Database(db)) if db.is_unique_violation() => {
                return Err(StoreError::Conflict(spec.name.clone()));
            }
            Err(e) => return Err(e.into()),
        }
        self.get_processor(tenant_id, &spec.name)
            .await?
            .ok_or_else(|| StoreError::NotFound(spec.name.clone()))
    }

    /// Replace an existing processor's full document by name. Never
    /// upserts: [`StoreError::NotFound`] if `name` does not already exist.
    /// `spec.name` must equal `name`.
    pub async fn replace_processor(
        &self,
        tenant_id: &str,
        name: &str,
        spec: &ProcessorSpec,
    ) -> Result<ProcessorRecord, StoreError> {
        validate_spec(spec)?;
        if spec.name != name {
            return Err(StoreError::Invalid(format!(
                "spec name `{}` does not match path name `{name}`",
                spec.name
            )));
        }
        if let Some(dataset) = &spec.dataset
            && !self.dataset_exists(tenant_id, dataset).await?
        {
            return Err(StoreError::UnknownDataset(dataset.clone()));
        }
        let statements_json = serde_json::to_string(&spec.statements)
            .map_err(|e| StoreError::Invalid(format!("failed to serialize statements: {e}")))?;

        let sql_sqlite = "UPDATE processors SET dataset = ?, signal = ?, enabled = ?, \
            priority = ?, error_mode = ?, description = ?, statements = ?, \
            updated_at = datetime('now') \
            WHERE tenant_id = ? AND name = ?";
        let sql_pg = "UPDATE processors SET dataset = $1, signal = $2, enabled = $3, \
            priority = $4, error_mode = $5, description = $6, statements = $7, \
            updated_at = NOW() \
            WHERE tenant_id = $8 AND name = $9";
        let affected = match self {
            Catalog::Sqlite(pool) => query(sql_sqlite)
                .bind(&spec.dataset)
                .bind(&spec.signal)
                .bind(spec.enabled)
                .bind(spec.priority)
                .bind(&spec.error_mode)
                .bind(&spec.description)
                .bind(&statements_json)
                .bind(tenant_id)
                .bind(name)
                .execute(pool)
                .await?
                .rows_affected(),
            Catalog::Postgres(pool) => query(sql_pg)
                .bind(&spec.dataset)
                .bind(&spec.signal)
                .bind(spec.enabled)
                .bind(spec.priority)
                .bind(&spec.error_mode)
                .bind(&spec.description)
                .bind(&statements_json)
                .bind(tenant_id)
                .bind(name)
                .execute(pool)
                .await?
                .rows_affected(),
        };
        if affected == 0 {
            return Err(StoreError::NotFound(name.to_string()));
        }
        self.get_processor(tenant_id, name)
            .await?
            .ok_or_else(|| StoreError::NotFound(name.to_string()))
    }

    /// Delete a processor; `Ok(false)` if it did not exist.
    pub async fn delete_processor(&self, tenant_id: &str, name: &str) -> Result<bool, StoreError> {
        let sql_sqlite = "DELETE FROM processors WHERE tenant_id = ? AND name = ?";
        let sql_pg = "DELETE FROM processors WHERE tenant_id = $1 AND name = $2";
        let affected = match self {
            Catalog::Sqlite(pool) => query(sql_sqlite)
                .bind(tenant_id)
                .bind(name)
                .execute(pool)
                .await?
                .rows_affected(),
            Catalog::Postgres(pool) => query(sql_pg)
                .bind(tenant_id)
                .bind(name)
                .execute(pool)
                .await?
                .rows_affected(),
        };
        Ok(affected > 0)
    }
}
