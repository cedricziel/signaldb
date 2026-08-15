//! Catalog persistence for tenant custom schema registries
//! (`schema_registries` table). The uploaded document is the source of truth
//! and is returned verbatim; the resolved form is cached alongside so the
//! resolver can load a tenant's registries without re-resolving.

use schema_model::{RegistryDocument, ResolvedRegistry, ValidationError};
use sqlx::{Row, query};

use crate::catalog::Catalog;

/// Errors from registry storage and mutation.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("namespace `{0}` is reserved for a bundled registry")]
    ReservedNamespace(String),
    #[error("registry {namespace}@{version} is bundled and read-only")]
    ReadOnly { namespace: String, version: String },
    #[error("registry {namespace}@{version} already exists")]
    AlreadyExists { namespace: String, version: String },
    #[error("registry {namespace}@{version} not found")]
    NotFound { namespace: String, version: String },
    #[error("path names {path} but the document is {document}")]
    IdentityMismatch { path: String, document: String },
    #[error("registry document is invalid ({} error(s))", .0.len())]
    Invalid(Vec<ValidationError>),
    #[error("stored registry is corrupt: {0}")]
    Corrupt(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// A custom registry row.
#[derive(Debug, Clone)]
pub struct StoredRegistry {
    pub tenant_id: String,
    pub namespace: String,
    pub version: String,
    pub document: RegistryDocument,
    pub resolved: ResolvedRegistry,
    pub created_at: String,
    pub updated_at: String,
}

fn row_to_stored<R: Row>(row: &R) -> Result<StoredRegistry, StoreError>
where
    for<'a> &'a str: sqlx::ColumnIndex<R>,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    let document: String = row.get("document");
    let resolved: String = row.get("resolved");
    Ok(StoredRegistry {
        tenant_id: row.get("tenant_id"),
        namespace: row.get("namespace"),
        version: row.get("version"),
        document: serde_json::from_str(&document)
            .map_err(|e| StoreError::Corrupt(e.to_string()))?,
        resolved: serde_json::from_str(&resolved)
            .map_err(|e| StoreError::Corrupt(e.to_string()))?,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

const SELECT_COLS: &str = "tenant_id, namespace, version, document, resolved, \
     CAST(created_at AS TEXT) AS created_at, CAST(updated_at AS TEXT) AS updated_at";

impl Catalog {
    /// Every custom registry of a tenant (unordered).
    pub async fn list_schema_registries(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<StoredRegistry>, StoreError> {
        let sql_sqlite = format!("SELECT {SELECT_COLS} FROM schema_registries WHERE tenant_id = ?");
        let sql_pg = format!("SELECT {SELECT_COLS} FROM schema_registries WHERE tenant_id = $1");
        let rows = match self {
            Catalog::Sqlite(pool) => query(&sql_sqlite)
                .bind(tenant_id)
                .fetch_all(pool)
                .await?
                .iter()
                .map(row_to_stored)
                .collect::<Result<Vec<_>, _>>()?,
            Catalog::Postgres(pool) => query(&sql_pg)
                .bind(tenant_id)
                .fetch_all(pool)
                .await?
                .iter()
                .map(row_to_stored)
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(rows)
    }

    /// One custom registry, if present.
    pub async fn get_schema_registry(
        &self,
        tenant_id: &str,
        namespace: &str,
        version: &str,
    ) -> Result<Option<StoredRegistry>, StoreError> {
        let sql_sqlite = format!(
            "SELECT {SELECT_COLS} FROM schema_registries \
             WHERE tenant_id = ? AND namespace = ? AND version = ?"
        );
        let sql_pg = format!(
            "SELECT {SELECT_COLS} FROM schema_registries \
             WHERE tenant_id = $1 AND namespace = $2 AND version = $3"
        );
        let row = match self {
            Catalog::Sqlite(pool) => query(&sql_sqlite)
                .bind(tenant_id)
                .bind(namespace)
                .bind(version)
                .fetch_optional(pool)
                .await?
                .as_ref()
                .map(row_to_stored)
                .transpose()?,
            Catalog::Postgres(pool) => query(&sql_pg)
                .bind(tenant_id)
                .bind(namespace)
                .bind(version)
                .fetch_optional(pool)
                .await?
                .as_ref()
                .map(row_to_stored)
                .transpose()?,
        };
        Ok(row)
    }

    /// Insert a new custom registry. Fails on a duplicate `(namespace, version)`.
    pub async fn insert_schema_registry(
        &self,
        tenant_id: &str,
        document: &RegistryDocument,
        resolved: &ResolvedRegistry,
    ) -> Result<StoredRegistry, StoreError> {
        let doc_json =
            serde_json::to_string(document).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        let res_json =
            serde_json::to_string(resolved).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        let sql_sqlite = "INSERT INTO schema_registries \
            (tenant_id, namespace, version, source, schema_url, description, document, resolved, \
             attribute_count, entity_count, metric_count) \
            VALUES (?, ?, ?, 'custom', ?, ?, ?, ?, ?, ?, ?)";
        let sql_pg = "INSERT INTO schema_registries \
            (tenant_id, namespace, version, source, schema_url, description, document, resolved, \
             attribute_count, entity_count, metric_count) \
            VALUES ($1, $2, $3, 'custom', $4, $5, $6, $7, $8, $9, $10)";
        let result: Result<u64, sqlx::Error> = match self {
            Catalog::Sqlite(pool) => query(sql_sqlite)
                .bind(tenant_id)
                .bind(&document.name)
                .bind(&document.version)
                .bind(&document.schema_url)
                .bind(&document.description)
                .bind(&doc_json)
                .bind(&res_json)
                .bind(resolved.attributes.len() as i64)
                .bind(resolved.entities.len() as i64)
                .bind(resolved.metrics.len() as i64)
                .execute(pool)
                .await
                .map(|r| r.rows_affected()),
            Catalog::Postgres(pool) => query(sql_pg)
                .bind(tenant_id)
                .bind(&document.name)
                .bind(&document.version)
                .bind(&document.schema_url)
                .bind(&document.description)
                .bind(&doc_json)
                .bind(&res_json)
                .bind(resolved.attributes.len() as i64)
                .bind(resolved.entities.len() as i64)
                .bind(resolved.metrics.len() as i64)
                .execute(pool)
                .await
                .map(|r| r.rows_affected()),
        };
        match result {
            Ok(_) => {}
            Err(sqlx::Error::Database(db)) if db.is_unique_violation() => {
                return Err(StoreError::AlreadyExists {
                    namespace: document.name.clone(),
                    version: document.version.clone(),
                });
            }
            Err(e) => return Err(e.into()),
        }
        self.get_schema_registry(tenant_id, &document.name, &document.version)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                namespace: document.name.clone(),
                version: document.version.clone(),
            })
    }

    /// Replace an existing custom registry's document (single statement, so
    /// readers see either the old or the new row).
    pub async fn replace_schema_registry(
        &self,
        tenant_id: &str,
        document: &RegistryDocument,
        resolved: &ResolvedRegistry,
    ) -> Result<StoredRegistry, StoreError> {
        let doc_json =
            serde_json::to_string(document).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        let res_json =
            serde_json::to_string(resolved).map_err(|e| StoreError::Corrupt(e.to_string()))?;
        let sql_sqlite = "UPDATE schema_registries SET schema_url = ?, description = ?, document = ?, \
            resolved = ?, attribute_count = ?, entity_count = ?, metric_count = ?, \
            updated_at = datetime('now') \
            WHERE tenant_id = ? AND namespace = ? AND version = ?";
        let sql_pg = "UPDATE schema_registries SET schema_url = $1, description = $2, document = $3, \
            resolved = $4, attribute_count = $5, entity_count = $6, metric_count = $7, \
            updated_at = NOW() \
            WHERE tenant_id = $8 AND namespace = $9 AND version = $10";
        let affected = match self {
            Catalog::Sqlite(pool) => query(sql_sqlite)
                .bind(&document.schema_url)
                .bind(&document.description)
                .bind(&doc_json)
                .bind(&res_json)
                .bind(resolved.attributes.len() as i64)
                .bind(resolved.entities.len() as i64)
                .bind(resolved.metrics.len() as i64)
                .bind(tenant_id)
                .bind(&document.name)
                .bind(&document.version)
                .execute(pool)
                .await?
                .rows_affected(),
            Catalog::Postgres(pool) => query(sql_pg)
                .bind(&document.schema_url)
                .bind(&document.description)
                .bind(&doc_json)
                .bind(&res_json)
                .bind(resolved.attributes.len() as i64)
                .bind(resolved.entities.len() as i64)
                .bind(resolved.metrics.len() as i64)
                .bind(tenant_id)
                .bind(&document.name)
                .bind(&document.version)
                .execute(pool)
                .await?
                .rows_affected(),
        };
        if affected == 0 {
            return Err(StoreError::NotFound {
                namespace: document.name.clone(),
                version: document.version.clone(),
            });
        }
        self.get_schema_registry(tenant_id, &document.name, &document.version)
            .await?
            .ok_or_else(|| StoreError::NotFound {
                namespace: document.name.clone(),
                version: document.version.clone(),
            })
    }

    /// Delete a custom registry; `Ok(false)` if it did not exist.
    pub async fn delete_schema_registry(
        &self,
        tenant_id: &str,
        namespace: &str,
        version: &str,
    ) -> Result<bool, StoreError> {
        let sql_sqlite =
            "DELETE FROM schema_registries WHERE tenant_id = ? AND namespace = ? AND version = ?";
        let sql_pg = "DELETE FROM schema_registries WHERE tenant_id = $1 AND namespace = $2 AND version = $3";
        let affected = match self {
            Catalog::Sqlite(pool) => query(sql_sqlite)
                .bind(tenant_id)
                .bind(namespace)
                .bind(version)
                .execute(pool)
                .await?
                .rows_affected(),
            Catalog::Postgres(pool) => query(sql_pg)
                .bind(tenant_id)
                .bind(namespace)
                .bind(version)
                .execute(pool)
                .await?
                .rows_affected(),
        };
        Ok(affected > 0)
    }
}
