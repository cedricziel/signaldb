use crate::auth::Authenticator;
use crate::config::AuthConfig;
use crate::flight::transport::ServiceCapability;
use crate::service_bootstrap::ServiceType;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row, SqlitePool, query};
use uuid::Uuid;

/// Helper to parse RFC3339 datetime strings (SQLite stores timestamps as text)
fn parse_rfc3339(s: &str) -> Result<DateTime<Utc>, sqlx::Error> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| sqlx::Error::Decode(Box::new(e)))
}

/// Canonicalize an email address for identity comparison: trim whitespace
/// and lowercase, so the `users.email` UNIQUE constraint applies to the
/// canonical form identically on SQLite and PostgreSQL.
fn canonicalize_email(email: &str) -> String {
    email.trim().to_lowercase()
}

/// Catalog provides an interface to the catalog database (PostgreSQL or SQLite).
#[derive(Clone)]
pub enum Catalog {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl Catalog {
    /// Create an in-memory SQLite catalog for fast tests.
    ///
    /// This is equivalent to `Catalog::new("sqlite::memory:")` and provides
    /// a quick way to create an isolated, ephemeral catalog for testing.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use common::catalog::Catalog;
    ///
    /// let catalog = Catalog::new_in_memory().await?;
    /// ```
    pub async fn new_in_memory() -> Result<Self, sqlx::Error> {
        Self::new("sqlite::memory:").await
    }

    /// Create a new Catalog client and initialize schema.
    pub async fn new(dsn: &str) -> Result<Self, sqlx::Error> {
        tracing::info!(dsn = %crate::config::redact_dsn(dsn), "Connecting to catalog database");

        let catalog = if dsn.starts_with("sqlite:") {
            // Add mode=rwc to create database file if it doesn't exist
            let dsn_with_create = if dsn.contains('?') {
                if dsn.contains("mode=") {
                    dsn.to_string()
                } else {
                    format!("{dsn}&mode=rwc")
                }
            } else {
                format!("{dsn}?mode=rwc")
            };

            let pool = SqlitePool::connect(&dsn_with_create).await.map_err(|e| {
                tracing::error!(
                    dsn = %crate::config::redact_dsn(&dsn_with_create),
                    error = %e,
                    "Failed to connect to SQLite database"
                );
                e
            })?;
            Catalog::Sqlite(pool)
        } else {
            let pool = PgPool::connect(dsn).await.map_err(|e| {
                tracing::error!(dsn = %crate::config::redact_dsn(dsn), error = %e, "Failed to connect to PostgreSQL database");
                e
            })?;
            Catalog::Postgres(pool)
        };

        tracing::info!("Database connection established successfully");
        catalog.init().await.map_err(|e| {
            tracing::error!(error = %e, "Failed to initialize catalog schema");
            e
        })?;
        tracing::info!("Catalog schema initialized successfully");
        Ok(catalog)
    }

    /// Initialize catalog tables if they do not exist.
    async fn init(&self) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                // SQLite schema
                let create_ingesters = r#"
                CREATE TABLE IF NOT EXISTS ingesters (
                    id TEXT PRIMARY KEY,
                    address TEXT NOT NULL,
                    last_seen TEXT NOT NULL,
                    service_type TEXT NOT NULL DEFAULT 'Writer',
                    capabilities TEXT NOT NULL DEFAULT 'TraceIngestion,Storage'
                )"#;
                query(create_ingesters).execute(pool).await?;

                let create_shards = r#"
                CREATE TABLE IF NOT EXISTS shards (
                    id INTEGER PRIMARY KEY,
                    start_range INTEGER NOT NULL,
                    end_range INTEGER NOT NULL
                )"#;
                query(create_shards).execute(pool).await?;

                let create_shard_owners = r#"
                CREATE TABLE IF NOT EXISTS shard_owners (
                    shard_id INTEGER NOT NULL,
                    ingester_id TEXT NOT NULL,
                    PRIMARY KEY (shard_id, ingester_id)
                )"#;
                query(create_shard_owners).execute(pool).await?;

                // Multi-tenancy tables
                let create_tenants = r#"
                CREATE TABLE IF NOT EXISTS tenants (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    default_dataset TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    source TEXT NOT NULL CHECK(source IN ('config', 'database'))
                )"#;
                query(create_tenants).execute(pool).await?;

                let create_api_keys = r#"
                CREATE TABLE IF NOT EXISTS api_keys (
                    id TEXT PRIMARY KEY,
                    key_hash TEXT NOT NULL UNIQUE,
                    tenant_id TEXT NOT NULL,
                    name TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    revoked_at TEXT,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
                    UNIQUE(tenant_id, name)
                )"#;
                query(create_api_keys).execute(pool).await?;

                let create_datasets = r#"
                CREATE TABLE IF NOT EXISTS datasets (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
                    UNIQUE(tenant_id, name)
                )"#;
                query(create_datasets).execute(pool).await?;

                // Indexes for multi-tenancy tables
                query("CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id)")
                    .execute(pool)
                    .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash) WHERE revoked_at IS NULL",
                )
                .execute(pool)
                .await?;
                query("CREATE INDEX IF NOT EXISTS idx_datasets_tenant ON datasets(tenant_id)")
                    .execute(pool)
                    .await?;

                // User accounts, tenant memberships, and login sessions
                let create_users = r#"
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    display_name TEXT,
                    password_hash TEXT NOT NULL,
                    is_instance_admin INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    disabled_at TEXT
                )"#;
                query(create_users).execute(pool).await?;

                let create_tenant_memberships = r#"
                CREATE TABLE IF NOT EXISTS tenant_memberships (
                    user_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('admin', 'member', 'viewer')),
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (user_id, tenant_id),
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
                )"#;
                query(create_tenant_memberships).execute(pool).await?;

                let create_user_sessions = r#"
                CREATE TABLE IF NOT EXISTS user_sessions (
                    id TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL UNIQUE,
                    user_id TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    expires_at TEXT NOT NULL,
                    revoked_at TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                )"#;
                query(create_user_sessions).execute(pool).await?;

                // Indexes for user/membership/session tables
                query(
                    "CREATE INDEX IF NOT EXISTS idx_tenant_memberships_tenant ON tenant_memberships(tenant_id)",
                )
                .execute(pool)
                .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_user_sessions_user ON user_sessions(user_id)",
                )
                .execute(pool)
                .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_user_sessions_hash ON user_sessions(token_hash) WHERE revoked_at IS NULL",
                )
                .execute(pool)
                .await?;

                // Compactor lease table — prevents duplicate work when multiple compactor
                // instances run simultaneously. One row per (tenant, dataset, table, partition).
                let create_compactor_leases = r#"
                CREATE TABLE IF NOT EXISTS compactor_leases (
                    tenant_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    partition_id TEXT NOT NULL,
                    holder_id TEXT NOT NULL,
                    acquired_at TEXT NOT NULL DEFAULT (datetime('now')),
                    expires_at TEXT NOT NULL,
                    renewed_at TEXT,
                    PRIMARY KEY (tenant_id, dataset_id, table_name, partition_id)
                )"#;
                query(create_compactor_leases).execute(pool).await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_compactor_leases_holder ON compactor_leases(holder_id)",
                )
                .execute(pool)
                .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_compactor_leases_expires ON compactor_leases(expires_at)",
                )
                .execute(pool)
                .await?;

                // Advisory attribute statistics (epic #737, #733): per-key
                // presence/cardinality written by the compactor's analyzer
                // and query-demand hit counters written by the querier.
                let create_attribute_stats = r#"
                CREATE TABLE IF NOT EXISTS attribute_stats (
                    tenant_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    signal TEXT NOT NULL,
                    attr_key TEXT NOT NULL,
                    present_rows BIGINT NOT NULL DEFAULT 0,
                    total_rows BIGINT NOT NULL DEFAULT 0,
                    distinct_estimate BIGINT NOT NULL DEFAULT 0,
                    capped INTEGER NOT NULL DEFAULT 0,
                    query_hits BIGINT NOT NULL DEFAULT 0,
                    promote_streak BIGINT NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (tenant_id, dataset_id, signal, attr_key)
                )"#;
                query(create_attribute_stats).execute(pool).await?;
            }
            Catalog::Postgres(pool) => {
                // PostgreSQL schema
                let create_ingesters = r#"
                CREATE TABLE IF NOT EXISTS ingesters (
                    id UUID PRIMARY KEY,
                    address TEXT NOT NULL,
                    last_seen TIMESTAMPTZ NOT NULL,
                    service_type TEXT NOT NULL DEFAULT 'Writer',
                    capabilities TEXT NOT NULL DEFAULT 'TraceIngestion,Storage'
                )"#;
                query(create_ingesters).execute(pool).await?;

                let create_shards = r#"
                CREATE TABLE IF NOT EXISTS shards (
                    id INT PRIMARY KEY,
                    start_range BIGINT NOT NULL,
                    end_range BIGINT NOT NULL
                )"#;
                query(create_shards).execute(pool).await?;

                let create_shard_owners = r#"
                CREATE TABLE IF NOT EXISTS shard_owners (
                    shard_id INT NOT NULL REFERENCES shards(id),
                    ingester_id UUID NOT NULL REFERENCES ingesters(id),
                    PRIMARY KEY (shard_id, ingester_id)
                )"#;
                query(create_shard_owners).execute(pool).await?;

                // Multi-tenancy tables
                let create_tenants = r#"
                CREATE TABLE IF NOT EXISTS tenants (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    default_dataset TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    source TEXT NOT NULL CHECK(source IN ('config', 'database'))
                )"#;
                query(create_tenants).execute(pool).await?;

                let create_api_keys = r#"
                CREATE TABLE IF NOT EXISTS api_keys (
                    id TEXT PRIMARY KEY,
                    key_hash TEXT NOT NULL UNIQUE,
                    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                    name TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    revoked_at TIMESTAMPTZ,
                    UNIQUE(tenant_id, name)
                )"#;
                query(create_api_keys).execute(pool).await?;

                let create_datasets = r#"
                CREATE TABLE IF NOT EXISTS datasets (
                    id TEXT PRIMARY KEY,
                    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                    name TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    UNIQUE(tenant_id, name)
                )"#;
                query(create_datasets).execute(pool).await?;

                // Indexes for multi-tenancy tables
                query("CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id)")
                    .execute(pool)
                    .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash) WHERE revoked_at IS NULL",
                )
                .execute(pool)
                .await?;
                query("CREATE INDEX IF NOT EXISTS idx_datasets_tenant ON datasets(tenant_id)")
                    .execute(pool)
                    .await?;

                // User accounts, tenant memberships, and login sessions
                let create_users = r#"
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    display_name TEXT,
                    password_hash TEXT NOT NULL,
                    is_instance_admin BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    disabled_at TIMESTAMPTZ
                )"#;
                query(create_users).execute(pool).await?;

                let create_tenant_memberships = r#"
                CREATE TABLE IF NOT EXISTS tenant_memberships (
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                    role TEXT NOT NULL CHECK(role IN ('admin', 'member', 'viewer')),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, tenant_id)
                )"#;
                query(create_tenant_memberships).execute(pool).await?;

                let create_user_sessions = r#"
                CREATE TABLE IF NOT EXISTS user_sessions (
                    id TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL UNIQUE,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL,
                    revoked_at TIMESTAMPTZ
                )"#;
                query(create_user_sessions).execute(pool).await?;

                // Indexes for user/membership/session tables
                query(
                    "CREATE INDEX IF NOT EXISTS idx_tenant_memberships_tenant ON tenant_memberships(tenant_id)",
                )
                .execute(pool)
                .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_user_sessions_user ON user_sessions(user_id)",
                )
                .execute(pool)
                .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_user_sessions_hash ON user_sessions(token_hash) WHERE revoked_at IS NULL",
                )
                .execute(pool)
                .await?;

                // Compactor lease table — prevents duplicate work when multiple compactor
                // instances run simultaneously. One row per (tenant, dataset, table, partition).
                let create_compactor_leases = r#"
                CREATE TABLE IF NOT EXISTS compactor_leases (
                    tenant_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    partition_id TEXT NOT NULL,
                    holder_id TEXT NOT NULL,
                    acquired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL,
                    renewed_at TIMESTAMPTZ,
                    PRIMARY KEY (tenant_id, dataset_id, table_name, partition_id)
                )"#;
                query(create_compactor_leases).execute(pool).await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_compactor_leases_holder ON compactor_leases(holder_id)",
                )
                .execute(pool)
                .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_compactor_leases_expires ON compactor_leases(expires_at)",
                )
                .execute(pool)
                .await?;

                // Advisory attribute statistics (epic #737, #733): per-key
                // presence/cardinality written by the compactor's analyzer
                // and query-demand hit counters written by the querier.
                let create_attribute_stats = r#"
                CREATE TABLE IF NOT EXISTS attribute_stats (
                    tenant_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    signal TEXT NOT NULL,
                    attr_key TEXT NOT NULL,
                    present_rows BIGINT NOT NULL DEFAULT 0,
                    total_rows BIGINT NOT NULL DEFAULT 0,
                    distinct_estimate BIGINT NOT NULL DEFAULT 0,
                    capped BOOLEAN NOT NULL DEFAULT FALSE,
                    query_hits BIGINT NOT NULL DEFAULT 0,
                    promote_streak BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (tenant_id, dataset_id, signal, attr_key)
                )"#;
                query(create_attribute_stats).execute(pool).await?;
            }
        }

        Ok(())
    }

    /// Register or update an ingester with its address, service type, capabilities and heartbeat.
    #[tracing::instrument(
        level = "debug",
        skip_all,
        fields(service_id = %id, address = %address, service_type = ?service_type)
    )]
    pub async fn register_ingester(
        &self,
        id: Uuid,
        address: &str,
        service_type: ServiceType,
        capabilities: &[ServiceCapability],
    ) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();
                let id_str = id.to_string();
                let service_type_str = format!("{service_type:?}");
                let capabilities_str = capabilities
                    .iter()
                    .map(|c| format!("{c:?}"))
                    .collect::<Vec<_>>()
                    .join(",");

                // Try insert first, then update if it already exists
                let insert_stmt = r#"
                INSERT INTO ingesters (id, address, last_seen, service_type, capabilities)
                VALUES (?, ?, ?, ?, ?)
                "#;

                let result = query(insert_stmt)
                    .bind(&id_str)
                    .bind(address)
                    .bind(&now)
                    .bind(&service_type_str)
                    .bind(&capabilities_str)
                    .execute(pool)
                    .await;

                if result.is_err() {
                    // If insert failed (likely due to duplicate key), try update
                    let update_stmt = r#"
                    UPDATE ingesters SET address = ?, last_seen = ?, service_type = ?, capabilities = ?
                    WHERE id = ?
                    "#;
                    query(update_stmt)
                        .bind(address)
                        .bind(&now)
                        .bind(&service_type_str)
                        .bind(&capabilities_str)
                        .bind(&id_str)
                        .execute(pool)
                        .await?;
                }
            }
            Catalog::Postgres(pool) => {
                let service_type_str = format!("{service_type:?}");
                let capabilities_str = capabilities
                    .iter()
                    .map(|c| format!("{c:?}"))
                    .collect::<Vec<_>>()
                    .join(",");

                // PostgreSQL with UPSERT
                let stmt = r#"
                INSERT INTO ingesters (id, address, last_seen, service_type, capabilities)
                VALUES ($1, $2, NOW(), $3, $4)
                ON CONFLICT (id) DO UPDATE SET address = $2, last_seen = NOW(), service_type = $3, capabilities = $4
                "#;
                query(stmt)
                    .bind(id)
                    .bind(address)
                    .bind(&service_type_str)
                    .bind(&capabilities_str)
                    .execute(pool)
                    .await?;
            }
        }

        Ok(())
    }

    /// Update heartbeat timestamp for an ingester.
    #[tracing::instrument(level = "debug", skip_all, fields(service_id = %id))]
    pub async fn heartbeat(&self, id: Uuid) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();
                let id_str = id.to_string();
                let stmt = r#"
                UPDATE ingesters SET last_seen = ?
                WHERE id = ?
                "#;
                let result = query(stmt).bind(&now).bind(&id_str).execute(pool).await?;
                if result.rows_affected() == 0 {
                    return Err(sqlx::Error::RowNotFound);
                }
            }
            Catalog::Postgres(pool) => {
                let stmt = r#"
                UPDATE ingesters SET last_seen = NOW()
                WHERE id = $1
                "#;
                let result = query(stmt).bind(id).execute(pool).await?;
                if result.rows_affected() == 0 {
                    return Err(sqlx::Error::RowNotFound);
                }
            }
        }
        Ok(())
    }

    /// List all ingesters in the catalog.
    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn list_ingesters(&self) -> Result<Vec<Ingester>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query(
                    "SELECT id, address, last_seen, service_type, capabilities FROM ingesters",
                )
                .fetch_all(pool)
                .await?;
                let mut ingesters = Vec::with_capacity(rows.len());
                for row in rows {
                    let id_str: String = row.get("id");
                    let last_seen_str: String = row.get("last_seen");
                    let service_type_str: String = row.get("service_type");
                    let capabilities_str: String = row.get("capabilities");

                    let id = Uuid::parse_str(&id_str)
                        .map_err(|_| sqlx::Error::Decode("Invalid UUID format".into()))?;
                    let last_seen = DateTime::parse_from_rfc3339(&last_seen_str)
                        .map_err(|_| sqlx::Error::Decode("Invalid timestamp format".into()))?
                        .with_timezone(&Utc);

                    let service_type = parse_service_type(&service_type_str);
                    let capabilities = parse_capabilities(&capabilities_str);

                    let ing = Ingester {
                        id,
                        address: row.get("address"),
                        last_seen,
                        service_type,
                        capabilities,
                    };
                    ingesters.push(ing);
                }
                Ok(ingesters)
            }
            Catalog::Postgres(pool) => {
                let rows = query(
                    "SELECT id, address, last_seen, service_type, capabilities FROM ingesters",
                )
                .fetch_all(pool)
                .await?;
                let mut ingesters = Vec::with_capacity(rows.len());
                for row in rows {
                    let service_type_str: String = row.get("service_type");
                    let capabilities_str: String = row.get("capabilities");

                    let service_type = parse_service_type(&service_type_str);
                    let capabilities = parse_capabilities(&capabilities_str);

                    let ing = Ingester {
                        id: row.get("id"),
                        address: row.get("address"),
                        last_seen: row.get("last_seen"),
                        service_type,
                        capabilities,
                    };
                    ingesters.push(ing);
                }
                Ok(ingesters)
            }
        }
    }

    /// List all shards in the catalog.
    pub async fn list_shards(&self) -> Result<Vec<Shard>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query("SELECT id, start_range, end_range FROM shards")
                    .fetch_all(pool)
                    .await?;
                let mut shards = Vec::with_capacity(rows.len());
                for row in rows {
                    let shard = Shard {
                        id: row.get("id"),
                        start_range: row.get("start_range"),
                        end_range: row.get("end_range"),
                    };
                    shards.push(shard);
                }
                Ok(shards)
            }
            Catalog::Postgres(pool) => {
                let rows = query("SELECT id, start_range, end_range FROM shards")
                    .fetch_all(pool)
                    .await?;
                let mut shards = Vec::with_capacity(rows.len());
                for row in rows {
                    let shard = Shard {
                        id: row.get("id"),
                        start_range: row.get("start_range"),
                        end_range: row.get("end_range"),
                    };
                    shards.push(shard);
                }
                Ok(shards)
            }
        }
    }

    /// List all shard-owner mappings.
    pub async fn list_shard_owners(&self) -> Result<Vec<ShardOwner>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query("SELECT shard_id, ingester_id FROM shard_owners")
                    .fetch_all(pool)
                    .await?;
                let mut owners = Vec::with_capacity(rows.len());
                for row in rows {
                    let ingester_id_str: String = row.get("ingester_id");
                    let ingester_id = Uuid::parse_str(&ingester_id_str)
                        .map_err(|_| sqlx::Error::Decode("Invalid UUID format".into()))?;

                    let owner = ShardOwner {
                        shard_id: row.get("shard_id"),
                        ingester_id,
                    };
                    owners.push(owner);
                }
                Ok(owners)
            }
            Catalog::Postgres(pool) => {
                let rows = query("SELECT shard_id, ingester_id FROM shard_owners")
                    .fetch_all(pool)
                    .await?;
                let mut owners = Vec::with_capacity(rows.len());
                for row in rows {
                    let owner = ShardOwner {
                        shard_id: row.get("shard_id"),
                        ingester_id: row.get("ingester_id"),
                    };
                    owners.push(owner);
                }
                Ok(owners)
            }
        }
    }

    /// Add a shard definition if not exists.
    pub async fn add_shard(
        &self,
        id: i32,
        start_range: i64,
        end_range: i64,
    ) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                // Try insert, ignore if it already exists
                let stmt = r#"
                INSERT INTO shards (id, start_range, end_range)
                VALUES (?, ?, ?)
                "#;
                let result = query(stmt)
                    .bind(id)
                    .bind(start_range)
                    .bind(end_range)
                    .execute(pool)
                    .await;

                // Ignore duplicate key errors
                if let Err(sqlx::Error::Database(db_err)) = &result
                    && db_err.message().contains("UNIQUE constraint failed")
                {
                    return Ok(());
                }

                result?;
                Ok(())
            }
            Catalog::Postgres(pool) => {
                // PostgreSQL version with ON CONFLICT
                let stmt = r#"
                INSERT INTO shards (id, start_range, end_range)
                VALUES ($1, $2, $3)
                ON CONFLICT (id) DO NOTHING
                "#;
                query(stmt)
                    .bind(id)
                    .bind(start_range)
                    .bind(end_range)
                    .execute(pool)
                    .await?;
                Ok(())
            }
        }
    }

    /// Assign an ingester as owner of a shard.
    pub async fn assign_shard(&self, shard_id: i32, ingester_id: Uuid) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let ingester_id_str = ingester_id.to_string();
                let stmt = r#"
                INSERT INTO shard_owners (shard_id, ingester_id)
                VALUES (?, ?)
                "#;
                let result = query(stmt)
                    .bind(shard_id)
                    .bind(&ingester_id_str)
                    .execute(pool)
                    .await;

                // Ignore duplicate key errors
                if let Err(sqlx::Error::Database(db_err)) = &result
                    && db_err.message().contains("UNIQUE constraint failed")
                {
                    return Ok(());
                }

                result?;
                Ok(())
            }
            Catalog::Postgres(pool) => {
                // PostgreSQL version with ON CONFLICT
                let stmt = r#"
                INSERT INTO shard_owners (shard_id, ingester_id)
                VALUES ($1, $2)
                ON CONFLICT (shard_id, ingester_id) DO NOTHING
                "#;
                query(stmt)
                    .bind(shard_id)
                    .bind(ingester_id)
                    .execute(pool)
                    .await?;
                Ok(())
            }
        }
    }
    /// Discover services that have a specific capability.
    #[tracing::instrument(level = "debug", skip_all, fields(capability = ?capability))]
    pub async fn discover_services_by_capability(
        &self,
        capability: ServiceCapability,
    ) -> Result<Vec<Ingester>, sqlx::Error> {
        let ingesters = self.list_ingesters().await?;

        // Filter ingesters that have the required capability
        let filtered: Vec<Ingester> = ingesters
            .into_iter()
            .filter(|ingester| ingester.capabilities.contains(&capability))
            .collect();

        Ok(filtered)
    }

    /// List ingesters whose heartbeat is fresher than `ttl`.
    ///
    /// Crashed services never deregister (only graceful shutdown does),
    /// so consumers must ignore rows whose `last_seen` is stale or they
    /// will route to dead addresses (issue #555).
    pub async fn list_active_ingesters(
        &self,
        ttl: std::time::Duration,
    ) -> Result<Vec<Ingester>, sqlx::Error> {
        let ttl = chrono::Duration::from_std(ttl).unwrap_or(chrono::Duration::MAX);
        let now = Utc::now();
        let ingesters = self.list_ingesters().await?;
        Ok(ingesters
            .into_iter()
            .filter(|ingester| now.signed_duration_since(ingester.last_seen) <= ttl)
            .collect())
    }

    /// Delete ingester rows whose `last_seen` is older than `cutoff`.
    ///
    /// Returns the number of rows removed. Safe to run from every
    /// service concurrently — the DELETE is idempotent.
    pub async fn reap_stale_ingesters(&self, cutoff: DateTime<Utc>) -> Result<u64, sqlx::Error> {
        let removed = match self {
            Catalog::Sqlite(pool) => {
                // last_seen is stored as chrono RFC3339 text (UTC, +00:00
                // offset), so lexicographic comparison against another
                // RFC3339 UTC timestamp is chronologically correct.
                let stmt = r#"
                DELETE FROM ingesters
                WHERE last_seen < ?
                "#;
                query(stmt)
                    .bind(cutoff.to_rfc3339())
                    .execute(pool)
                    .await?
                    .rows_affected()
            }
            Catalog::Postgres(pool) => {
                let stmt = r#"
                DELETE FROM ingesters
                WHERE last_seen < $1
                "#;
                query(stmt)
                    .bind(cutoff)
                    .execute(pool)
                    .await?
                    .rows_affected()
            }
        };
        if removed > 0 {
            tracing::info!(removed, cutoff = %cutoff, "Reaped stale service registrations");
        }
        Ok(removed)
    }

    /// Deregister an ingester instance, removing it from the catalog.
    #[tracing::instrument(level = "debug", skip_all, fields(service_id = %id))]
    pub async fn deregister_ingester(&self, id: Uuid) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let id_str = id.to_string();
                let stmt = r#"
                DELETE FROM ingesters
                WHERE id = ?
                "#;
                query(stmt).bind(&id_str).execute(pool).await?;
            }
            Catalog::Postgres(pool) => {
                let stmt = r#"
                DELETE FROM ingesters
                WHERE id = $1
                "#;
                query(stmt).bind(id).execute(pool).await?;
            }
        }
        Ok(())
    }
}

/// Extension methods for Catalog to manage heartbeats.
impl Catalog {
    /// Spawn a background task that updates the heartbeat (last_seen) for the given ingester ID
    /// at the specified interval. Returns a JoinHandle for the spawned task.
    pub fn spawn_ingester_heartbeat(
        &self,
        id: Uuid,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let catalog = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                if let Err(e) = catalog.heartbeat(id).await {
                    tracing::error!(service_id = %id, error = %e, "Failed to send heartbeat for ingester");
                }
            }
        })
    }

    /// Spawn a background task that periodically deletes service rows
    /// whose heartbeat stopped more than `reap_after` ago. Every service
    /// runs one; the DELETE is idempotent across instances.
    pub fn spawn_ingester_reaper(
        &self,
        interval: std::time::Duration,
        reap_after: std::time::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let catalog = self.clone();
        tokio::spawn(async move {
            let reap_after =
                chrono::Duration::from_std(reap_after).unwrap_or(chrono::Duration::MAX);
            let mut ticker = tokio::time::interval(interval);
            loop {
                ticker.tick().await;
                let cutoff = Utc::now() - reap_after;
                if let Err(e) = catalog.reap_stale_ingesters(cutoff).await {
                    tracing::error!(error = %e, "Failed to reap stale service registrations");
                }
            }
        })
    }
}

/// Metadata for an ingester node.
#[derive(Debug, Clone)]
pub struct Ingester {
    pub id: Uuid,
    pub address: String,
    pub last_seen: DateTime<Utc>,
    pub service_type: ServiceType,
    pub capabilities: Vec<ServiceCapability>,
}

/// Definition of a shard range.
#[derive(Debug, Clone)]
pub struct Shard {
    pub id: i32,
    pub start_range: i64,
    pub end_range: i64,
}

/// Mapping of shard ownership.
#[derive(Debug, Clone)]
pub struct ShardOwner {
    pub shard_id: i32,
    pub ingester_id: Uuid,
}

/// Helper function to parse service type from string
fn parse_service_type(s: &str) -> ServiceType {
    match s {
        "Acceptor" => ServiceType::Acceptor,
        "Router" => ServiceType::Router,
        "Writer" => ServiceType::Writer,
        "Querier" => ServiceType::Querier,
        _ => ServiceType::Writer, // Default fallback
    }
}

/// Helper function to parse capabilities from comma-separated string
fn parse_capabilities(s: &str) -> Vec<ServiceCapability> {
    if s.is_empty() {
        return vec![];
    }

    s.split(',')
        .filter_map(|cap| match cap.trim() {
            "TraceIngestion" => Some(ServiceCapability::TraceIngestion),
            "QueryExecution" => Some(ServiceCapability::QueryExecution),
            "Routing" => Some(ServiceCapability::Routing),
            "Storage" => Some(ServiceCapability::Storage),
            "KafkaIngestion" => Some(ServiceCapability::KafkaIngestion),
            _ => None,
        })
        .collect()
}

/// Tenant record from database
#[derive(Debug, Clone)]
pub struct TenantRecord {
    pub id: String,
    pub name: String,
    pub default_dataset: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub source: String,
}

/// API Key record from database (without actual key)
#[derive(Debug, Clone)]
pub struct ApiKeyRecord {
    pub id: String,
    pub tenant_id: String,
    pub name: Option<String>,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Dataset record from database
#[derive(Debug, Clone)]
pub struct DatasetRecord {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub created_at: DateTime<Utc>,
}

/// Role a user holds within a tenant.
///
/// Stored as lowercase TEXT in the `tenant_memberships` table, matching
/// the `CHECK(role IN ('admin', 'member', 'viewer'))` constraint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MembershipRole {
    Admin,
    Member,
    Viewer,
}

impl MembershipRole {
    /// The lowercase string form stored in the database.
    pub fn as_str(&self) -> &'static str {
        match self {
            MembershipRole::Admin => "admin",
            MembershipRole::Member => "member",
            MembershipRole::Viewer => "viewer",
        }
    }
}

impl std::fmt::Display for MembershipRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for MembershipRole {
    type Err = sqlx::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "admin" => Ok(MembershipRole::Admin),
            "member" => Ok(MembershipRole::Member),
            "viewer" => Ok(MembershipRole::Viewer),
            other => Err(sqlx::Error::Decode(
                format!("invalid membership role: {other}").into(),
            )),
        }
    }
}

/// User account record from database
#[derive(Debug, Clone)]
pub struct UserRecord {
    pub id: String,
    pub email: String,
    pub display_name: Option<String>,
    pub password_hash: String,
    pub is_instance_admin: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub disabled_at: Option<DateTime<Utc>>,
}

/// Tenant membership record from database
#[derive(Debug, Clone)]
pub struct TenantMembershipRecord {
    pub user_id: String,
    pub tenant_id: String,
    pub role: MembershipRole,
    pub created_at: DateTime<Utc>,
}

/// User session record from database (token stored as hash only)
#[derive(Debug, Clone)]
pub struct UserSessionRecord {
    pub id: String,
    pub token_hash: String,
    pub user_id: String,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Map a SQLite row (RFC3339 text timestamps) to a `UserRecord`.
fn user_from_sqlite_row(r: &sqlx::sqlite::SqliteRow) -> Result<UserRecord, sqlx::Error> {
    let disabled_at: Option<String> = r.get("disabled_at");
    Ok(UserRecord {
        id: r.get("id"),
        email: r.get("email"),
        display_name: r.get("display_name"),
        password_hash: r.get("password_hash"),
        is_instance_admin: r.get("is_instance_admin"),
        created_at: parse_rfc3339(r.get("created_at"))?,
        updated_at: parse_rfc3339(r.get("updated_at"))?,
        disabled_at: disabled_at.map(|s| parse_rfc3339(&s)).transpose()?,
    })
}

/// Map a PostgreSQL row (native TIMESTAMPTZ) to a `UserRecord`.
fn user_from_pg_row(r: &sqlx::postgres::PgRow) -> UserRecord {
    UserRecord {
        id: r.get("id"),
        email: r.get("email"),
        display_name: r.get("display_name"),
        password_hash: r.get("password_hash"),
        is_instance_admin: r.get("is_instance_admin"),
        created_at: r.get("created_at"),
        updated_at: r.get("updated_at"),
        disabled_at: r.get("disabled_at"),
    }
}

/// Map a SQLite row to a `TenantMembershipRecord`.
fn membership_from_sqlite_row(
    r: &sqlx::sqlite::SqliteRow,
) -> Result<TenantMembershipRecord, sqlx::Error> {
    let role: String = r.get("role");
    Ok(TenantMembershipRecord {
        user_id: r.get("user_id"),
        tenant_id: r.get("tenant_id"),
        role: role.parse()?,
        created_at: parse_rfc3339(r.get("created_at"))?,
    })
}

/// Map a PostgreSQL row to a `TenantMembershipRecord`.
fn membership_from_pg_row(
    r: &sqlx::postgres::PgRow,
) -> Result<TenantMembershipRecord, sqlx::Error> {
    let role: String = r.get("role");
    Ok(TenantMembershipRecord {
        user_id: r.get("user_id"),
        tenant_id: r.get("tenant_id"),
        role: role.parse()?,
        created_at: r.get("created_at"),
    })
}

/// Map a SQLite row to a `UserSessionRecord`.
fn session_from_sqlite_row(r: &sqlx::sqlite::SqliteRow) -> Result<UserSessionRecord, sqlx::Error> {
    let revoked_at: Option<String> = r.get("revoked_at");
    Ok(UserSessionRecord {
        id: r.get("id"),
        token_hash: r.get("token_hash"),
        user_id: r.get("user_id"),
        created_at: parse_rfc3339(r.get("created_at"))?,
        expires_at: parse_rfc3339(r.get("expires_at"))?,
        revoked_at: revoked_at.map(|s| parse_rfc3339(&s)).transpose()?,
    })
}

/// Map a PostgreSQL row to a `UserSessionRecord`.
fn session_from_pg_row(r: &sqlx::postgres::PgRow) -> UserSessionRecord {
    UserSessionRecord {
        id: r.get("id"),
        token_hash: r.get("token_hash"),
        user_id: r.get("user_id"),
        created_at: r.get("created_at"),
        expires_at: r.get("expires_at"),
        revoked_at: r.get("revoked_at"),
    }
}

/// A per-attribute-key statistics row (epic #737, #733): scan-side
/// presence/cardinality from the compactor's analyzer plus query-demand
/// hit counters from the querier.
#[derive(Debug, Clone)]
pub struct AttributeStatsRecord {
    pub tenant_id: String,
    pub dataset_id: String,
    pub signal: String,
    pub attr_key: String,
    pub present_rows: i64,
    pub total_rows: i64,
    pub distinct_estimate: i64,
    pub capped: bool,
    pub query_hits: i64,
    /// Consecutive analyzer cycles this key scored above the promotion
    /// threshold (hysteresis state for auto-promotion, #734).
    pub promote_streak: i64,
}

/// Advisory attribute-statistics methods (epic #737, #733).
impl Catalog {
    /// Upsert the scan-side statistics for one attribute key, replacing the
    /// previous presence/cardinality observation (the analyzer sees the
    /// whole rewritten table, so newer observations supersede older ones).
    /// `query_hits` is left untouched.
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_attribute_scan_stats(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
        attr_key: &str,
        present_rows: i64,
        total_rows: i64,
        distinct_estimate: i64,
        capped: bool,
    ) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                query(
                    r#"
                INSERT INTO attribute_stats
                    (tenant_id, dataset_id, signal, attr_key, present_rows,
                     total_rows, distinct_estimate, capped, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT (tenant_id, dataset_id, signal, attr_key) DO UPDATE SET
                    present_rows = excluded.present_rows,
                    total_rows = excluded.total_rows,
                    distinct_estimate = excluded.distinct_estimate,
                    capped = excluded.capped,
                    updated_at = datetime('now')
                "#,
                )
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(signal)
                .bind(attr_key)
                .bind(present_rows)
                .bind(total_rows)
                .bind(distinct_estimate)
                .bind(capped)
                .execute(pool)
                .await?;
            }
            Catalog::Postgres(pool) => {
                query(
                    r#"
                INSERT INTO attribute_stats
                    (tenant_id, dataset_id, signal, attr_key, present_rows,
                     total_rows, distinct_estimate, capped, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                ON CONFLICT (tenant_id, dataset_id, signal, attr_key) DO UPDATE SET
                    present_rows = EXCLUDED.present_rows,
                    total_rows = EXCLUDED.total_rows,
                    distinct_estimate = EXCLUDED.distinct_estimate,
                    capped = EXCLUDED.capped,
                    updated_at = NOW()
                "#,
                )
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(signal)
                .bind(attr_key)
                .bind(present_rows)
                .bind(total_rows)
                .bind(distinct_estimate)
                .bind(capped)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    /// Add query-demand hits for one attribute key (accumulating counter).
    /// Scan-side columns are left untouched.
    pub async fn add_attribute_query_hits(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
        attr_key: &str,
        hits: i64,
    ) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                query(
                    r#"
                INSERT INTO attribute_stats
                    (tenant_id, dataset_id, signal, attr_key, query_hits, updated_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT (tenant_id, dataset_id, signal, attr_key) DO UPDATE SET
                    query_hits = attribute_stats.query_hits + excluded.query_hits,
                    updated_at = datetime('now')
                "#,
                )
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(signal)
                .bind(attr_key)
                .bind(hits)
                .execute(pool)
                .await?;
            }
            Catalog::Postgres(pool) => {
                query(
                    r#"
                INSERT INTO attribute_stats
                    (tenant_id, dataset_id, signal, attr_key, query_hits, updated_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (tenant_id, dataset_id, signal, attr_key) DO UPDATE SET
                    query_hits = attribute_stats.query_hits + EXCLUDED.query_hits,
                    updated_at = NOW()
                "#,
                )
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(signal)
                .bind(attr_key)
                .bind(hits)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    /// Store the new promotion streak for one attribute key (hysteresis
    /// state for auto-promotion, #734).
    pub async fn set_attribute_promote_streak(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
        attr_key: &str,
        streak: i64,
    ) -> Result<(), sqlx::Error> {
        let sql_sqlite = "UPDATE attribute_stats SET promote_streak = ? \
             WHERE tenant_id = ? AND dataset_id = ? AND signal = ? AND attr_key = ?";
        let sql_pg = "UPDATE attribute_stats SET promote_streak = $1 \
             WHERE tenant_id = $2 AND dataset_id = $3 AND signal = $4 AND attr_key = $5";
        match self {
            Catalog::Sqlite(pool) => {
                query(sql_sqlite)
                    .bind(streak)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(signal)
                    .bind(attr_key)
                    .execute(pool)
                    .await?;
            }
            Catalog::Postgres(pool) => {
                query(sql_pg)
                    .bind(streak)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(signal)
                    .bind(attr_key)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// The stored statistics for one (tenant, dataset, signal), sorted by
    /// attribute key.
    pub async fn get_attribute_stats(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
    ) -> Result<Vec<AttributeStatsRecord>, sqlx::Error> {
        let sql_sqlite = r#"
            SELECT tenant_id, dataset_id, signal, attr_key, present_rows,
                   total_rows, distinct_estimate, capped, query_hits,
                   promote_streak
            FROM attribute_stats
            WHERE tenant_id = ? AND dataset_id = ? AND signal = ?
            ORDER BY attr_key
        "#;
        let sql_pg = r#"
            SELECT tenant_id, dataset_id, signal, attr_key, present_rows,
                   total_rows, distinct_estimate, capped, query_hits,
                   promote_streak
            FROM attribute_stats
            WHERE tenant_id = $1 AND dataset_id = $2 AND signal = $3
            ORDER BY attr_key
        "#;
        fn record<R: Row>(row: &R) -> AttributeStatsRecord
        where
            for<'a> &'a str: sqlx::ColumnIndex<R>,
            for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
            for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
            for<'a> bool: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
        {
            AttributeStatsRecord {
                tenant_id: row.get("tenant_id"),
                dataset_id: row.get("dataset_id"),
                signal: row.get("signal"),
                attr_key: row.get("attr_key"),
                present_rows: row.get("present_rows"),
                total_rows: row.get("total_rows"),
                distinct_estimate: row.get("distinct_estimate"),
                capped: row.get("capped"),
                query_hits: row.get("query_hits"),
                promote_streak: row.get("promote_streak"),
            }
        }
        match self {
            Catalog::Sqlite(pool) => Ok(query(sql_sqlite)
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(signal)
                .fetch_all(pool)
                .await?
                .iter()
                .map(record)
                .collect()),
            Catalog::Postgres(pool) => Ok(query(sql_pg)
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(signal)
                .fetch_all(pool)
                .await?
                .iter()
                .map(record)
                .collect()),
        }
    }
}

/// Multi-tenancy catalog methods
impl Catalog {
    /// Upsert a tenant (insert or update if exists)
    pub async fn upsert_tenant(
        &self,
        tenant_id: &str,
        name: &str,
        default_dataset: Option<&str>,
        source: &str,
    ) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();

                // Try insert first
                let insert_stmt = r#"
                INSERT INTO tenants (id, name, default_dataset, created_at, updated_at, source)
                VALUES (?, ?, ?, ?, ?, ?)
                "#;

                let result = query(insert_stmt)
                    .bind(tenant_id)
                    .bind(name)
                    .bind(default_dataset)
                    .bind(&now)
                    .bind(&now)
                    .bind(source)
                    .execute(pool)
                    .await;

                if result.is_err() {
                    // Update if already exists
                    let update_stmt = r#"
                    UPDATE tenants
                    SET name = ?, default_dataset = ?, updated_at = ?, source = ?
                    WHERE id = ?
                    "#;
                    query(update_stmt)
                        .bind(name)
                        .bind(default_dataset)
                        .bind(&now)
                        .bind(source)
                        .bind(tenant_id)
                        .execute(pool)
                        .await?;
                }
            }
            Catalog::Postgres(pool) => {
                let stmt = r#"
                INSERT INTO tenants (id, name, default_dataset, source)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (id) DO UPDATE
                SET name = $2, default_dataset = $3, updated_at = NOW(), source = $4
                "#;
                query(stmt)
                    .bind(tenant_id)
                    .bind(name)
                    .bind(default_dataset)
                    .bind(source)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Get tenant by ID
    pub async fn get_tenant(&self, tenant_id: &str) -> Result<Option<TenantRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let row = query("SELECT id, name, default_dataset, created_at, updated_at, source FROM tenants WHERE id = ?")
                    .bind(tenant_id)
                    .fetch_optional(pool)
                    .await?;

                row.map(|r| {
                    Ok(TenantRecord {
                        id: r.get("id"),
                        name: r.get("name"),
                        default_dataset: r.get("default_dataset"),
                        created_at: parse_rfc3339(r.get("created_at"))?,
                        updated_at: parse_rfc3339(r.get("updated_at"))?,
                        source: r.get("source"),
                    })
                })
                .transpose()
            }
            Catalog::Postgres(pool) => {
                let row = query("SELECT id, name, default_dataset, created_at, updated_at, source FROM tenants WHERE id = $1")
                    .bind(tenant_id)
                    .fetch_optional(pool)
                    .await?;

                Ok(row.map(|r| TenantRecord {
                    id: r.get("id"),
                    name: r.get("name"),
                    default_dataset: r.get("default_dataset"),
                    created_at: r.get("created_at"),
                    updated_at: r.get("updated_at"),
                    source: r.get("source"),
                }))
            }
        }
    }

    /// List all tenants
    pub async fn list_tenants(&self) -> Result<Vec<TenantRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query(
                    "SELECT id, name, default_dataset, created_at, updated_at, source FROM tenants",
                )
                .fetch_all(pool)
                .await?;

                rows.iter()
                    .map(|r| {
                        Ok(TenantRecord {
                            id: r.get("id"),
                            name: r.get("name"),
                            default_dataset: r.get("default_dataset"),
                            created_at: parse_rfc3339(r.get("created_at"))?,
                            updated_at: parse_rfc3339(r.get("updated_at"))?,
                            source: r.get("source"),
                        })
                    })
                    .collect()
            }
            Catalog::Postgres(pool) => {
                let rows = query(
                    "SELECT id, name, default_dataset, created_at, updated_at, source FROM tenants",
                )
                .fetch_all(pool)
                .await?;

                Ok(rows
                    .iter()
                    .map(|r| TenantRecord {
                        id: r.get("id"),
                        name: r.get("name"),
                        default_dataset: r.get("default_dataset"),
                        created_at: r.get("created_at"),
                        updated_at: r.get("updated_at"),
                        source: r.get("source"),
                    })
                    .collect())
            }
        }
    }

    /// Create or update an API key
    pub async fn upsert_api_key(
        &self,
        tenant_id: &str,
        key_hash: &str,
        name: Option<&str>,
    ) -> Result<String, sqlx::Error> {
        let key_id = Uuid::new_v4().to_string();

        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();

                // Check if key_hash already exists
                let existing =
                    query("SELECT id FROM api_keys WHERE key_hash = ? AND revoked_at IS NULL")
                        .bind(key_hash)
                        .fetch_optional(pool)
                        .await?;

                if let Some(row) = existing {
                    // Return existing ID
                    return Ok(row.get("id"));
                }

                // Insert new key
                let stmt = r#"
                INSERT INTO api_keys (id, key_hash, tenant_id, name, created_at)
                VALUES (?, ?, ?, ?, ?)
                "#;
                query(stmt)
                    .bind(&key_id)
                    .bind(key_hash)
                    .bind(tenant_id)
                    .bind(name)
                    .bind(&now)
                    .execute(pool)
                    .await?;
            }
            Catalog::Postgres(pool) => {
                // Check if key_hash already exists
                let existing =
                    query("SELECT id FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL")
                        .bind(key_hash)
                        .fetch_optional(pool)
                        .await?;

                if let Some(row) = existing {
                    return Ok(row.get("id"));
                }

                let stmt = r#"
                INSERT INTO api_keys (id, key_hash, tenant_id, name)
                VALUES ($1, $2, $3, $4)
                "#;
                query(stmt)
                    .bind(&key_id)
                    .bind(key_hash)
                    .bind(tenant_id)
                    .bind(name)
                    .execute(pool)
                    .await?;
            }
        }

        Ok(key_id)
    }

    /// Validate an API key and return tenant_id if valid
    pub async fn validate_api_key(
        &self,
        key_hash: &str,
    ) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let row = query("SELECT tenant_id, name FROM api_keys WHERE key_hash = ? AND revoked_at IS NULL")
                    .bind(key_hash)
                    .fetch_optional(pool)
                    .await?;

                Ok(row.map(|r| (r.get("tenant_id"), r.get("name"))))
            }
            Catalog::Postgres(pool) => {
                let row = query("SELECT tenant_id, name FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL")
                    .bind(key_hash)
                    .fetch_optional(pool)
                    .await?;

                Ok(row.map(|r| (r.get("tenant_id"), r.get("name"))))
            }
        }
    }

    /// Revoke an API key
    pub async fn revoke_api_key(&self, key_id: &str) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();
                query("UPDATE api_keys SET revoked_at = ? WHERE id = ?")
                    .bind(&now)
                    .bind(key_id)
                    .execute(pool)
                    .await?;
            }
            Catalog::Postgres(pool) => {
                query("UPDATE api_keys SET revoked_at = NOW() WHERE id = $1")
                    .bind(key_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Create a dataset for a tenant
    pub async fn create_dataset(
        &self,
        tenant_id: &str,
        dataset_name: &str,
    ) -> Result<String, sqlx::Error> {
        let dataset_id = Uuid::new_v4().to_string();

        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();
                let stmt = r#"
                INSERT INTO datasets (id, tenant_id, name, created_at)
                VALUES (?, ?, ?, ?)
                "#;
                query(stmt)
                    .bind(&dataset_id)
                    .bind(tenant_id)
                    .bind(dataset_name)
                    .bind(&now)
                    .execute(pool)
                    .await?;
            }
            Catalog::Postgres(pool) => {
                let stmt = r#"
                INSERT INTO datasets (id, tenant_id, name)
                VALUES ($1, $2, $3)
                "#;
                query(stmt)
                    .bind(&dataset_id)
                    .bind(tenant_id)
                    .bind(dataset_name)
                    .execute(pool)
                    .await?;
            }
        }

        Ok(dataset_id)
    }

    /// Get datasets for a tenant
    pub async fn get_datasets(&self, tenant_id: &str) -> Result<Vec<DatasetRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query(
                    "SELECT id, tenant_id, name, created_at FROM datasets WHERE tenant_id = ?",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;

                rows.iter()
                    .map(|r| {
                        Ok(DatasetRecord {
                            id: r.get("id"),
                            tenant_id: r.get("tenant_id"),
                            name: r.get("name"),
                            created_at: parse_rfc3339(r.get("created_at"))?,
                        })
                    })
                    .collect()
            }
            Catalog::Postgres(pool) => {
                let rows = query(
                    "SELECT id, tenant_id, name, created_at FROM datasets WHERE tenant_id = $1",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;

                Ok(rows
                    .iter()
                    .map(|r| DatasetRecord {
                        id: r.get("id"),
                        tenant_id: r.get("tenant_id"),
                        name: r.get("name"),
                        created_at: r.get("created_at"),
                    })
                    .collect())
            }
        }
    }

    /// List API keys for a tenant
    pub async fn list_api_keys(&self, tenant_id: &str) -> Result<Vec<ApiKeyRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query(
                    "SELECT id, tenant_id, name, created_at, revoked_at FROM api_keys WHERE tenant_id = ?",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;

                rows.iter()
                    .map(|r| {
                        let revoked_at: Option<String> = r.get("revoked_at");
                        Ok(ApiKeyRecord {
                            id: r.get("id"),
                            tenant_id: r.get("tenant_id"),
                            name: r.get("name"),
                            created_at: parse_rfc3339(r.get("created_at"))?,
                            revoked_at: revoked_at.map(|s| parse_rfc3339(&s)).transpose()?,
                        })
                    })
                    .collect()
            }
            Catalog::Postgres(pool) => {
                let rows = query(
                    "SELECT id, tenant_id, name, created_at, revoked_at FROM api_keys WHERE tenant_id = $1",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;

                Ok(rows
                    .iter()
                    .map(|r| ApiKeyRecord {
                        id: r.get("id"),
                        tenant_id: r.get("tenant_id"),
                        name: r.get("name"),
                        created_at: r.get("created_at"),
                        revoked_at: r.get("revoked_at"),
                    })
                    .collect())
            }
        }
    }

    /// Get a single API key by ID
    pub async fn get_api_key(&self, key_id: &str) -> Result<Option<ApiKeyRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let row = query(
                    "SELECT id, tenant_id, name, created_at, revoked_at FROM api_keys WHERE id = ?",
                )
                .bind(key_id)
                .fetch_optional(pool)
                .await?;

                row.map(|r| {
                    let revoked_at: Option<String> = r.get("revoked_at");
                    Ok(ApiKeyRecord {
                        id: r.get("id"),
                        tenant_id: r.get("tenant_id"),
                        name: r.get("name"),
                        created_at: parse_rfc3339(r.get("created_at"))?,
                        revoked_at: revoked_at.map(|s| parse_rfc3339(&s)).transpose()?,
                    })
                })
                .transpose()
            }
            Catalog::Postgres(pool) => {
                let row = query(
                    "SELECT id, tenant_id, name, created_at, revoked_at FROM api_keys WHERE id = $1",
                )
                .bind(key_id)
                .fetch_optional(pool)
                .await?;

                Ok(row.map(|r| ApiKeyRecord {
                    id: r.get("id"),
                    tenant_id: r.get("tenant_id"),
                    name: r.get("name"),
                    created_at: r.get("created_at"),
                    revoked_at: r.get("revoked_at"),
                }))
            }
        }
    }

    /// Delete a tenant (only database-sourced tenants can be deleted)
    /// Returns true if a row was deleted, false if the tenant was not found
    /// or is config-sourced.
    pub async fn delete_tenant(&self, tenant_id: &str) -> Result<bool, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                // Delete associated api_keys and datasets first
                query("DELETE FROM api_keys WHERE tenant_id = ?")
                    .bind(tenant_id)
                    .execute(pool)
                    .await?;
                query("DELETE FROM datasets WHERE tenant_id = ?")
                    .bind(tenant_id)
                    .execute(pool)
                    .await?;
                let result = query("DELETE FROM tenants WHERE id = ? AND source = 'database'")
                    .bind(tenant_id)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
            Catalog::Postgres(pool) => {
                query("DELETE FROM api_keys WHERE tenant_id = $1")
                    .bind(tenant_id)
                    .execute(pool)
                    .await?;
                query("DELETE FROM datasets WHERE tenant_id = $1")
                    .bind(tenant_id)
                    .execute(pool)
                    .await?;
                let result = query("DELETE FROM tenants WHERE id = $1 AND source = 'database'")
                    .bind(tenant_id)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
        }
    }

    /// Delete a dataset by ID
    /// Returns true if a row was deleted.
    pub async fn delete_dataset(&self, dataset_id: &str) -> Result<bool, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let result = query("DELETE FROM datasets WHERE id = ?")
                    .bind(dataset_id)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
            Catalog::Postgres(pool) => {
                let result = query("DELETE FROM datasets WHERE id = $1")
                    .bind(dataset_id)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
        }
    }

    /// Delete a dataset by ID, enforcing tenant ownership.
    /// Returns true if a row was deleted, false if not found or wrong tenant.
    pub async fn delete_dataset_for_tenant(
        &self,
        tenant_id: &str,
        dataset_id: &str,
    ) -> Result<bool, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let result = query("DELETE FROM datasets WHERE id = ? AND tenant_id = ?")
                    .bind(dataset_id)
                    .bind(tenant_id)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
            Catalog::Postgres(pool) => {
                let result = query("DELETE FROM datasets WHERE id = $1 AND tenant_id = $2")
                    .bind(dataset_id)
                    .bind(tenant_id)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
        }
    }

    pub async fn sync_config_tenants(&self, auth_config: &AuthConfig) -> Result<(), sqlx::Error> {
        for tenant in &auth_config.tenants {
            self.upsert_tenant(
                &tenant.id,
                &tenant.name,
                tenant.default_dataset.as_deref(),
                "config",
            )
            .await?;

            for api_key in &tenant.api_keys {
                let key_hash = Authenticator::hash_api_key(&api_key.key);
                self.upsert_api_key(&tenant.id, &key_hash, api_key.name.as_deref())
                    .await?;
            }

            let existing_datasets = self.get_datasets(&tenant.id).await?;
            for dataset in &tenant.datasets {
                let already_exists = existing_datasets.iter().any(|d| d.name == dataset.id);
                if !already_exists {
                    self.create_dataset(&tenant.id, &dataset.id).await?;
                }
            }
        }

        Ok(())
    }
}

/// User, tenant-membership, and session catalog methods
impl Catalog {
    /// Create a new user account with a random UUID id.
    ///
    /// `password_hash` is the already-hashed PHC string; hashing is the
    /// caller's responsibility. The email is canonicalized (trimmed and
    /// lowercased) before storage so the UNIQUE constraint applies to the
    /// canonical form on both backends. Fails if the email is already taken.
    pub async fn create_user(
        &self,
        email: &str,
        display_name: Option<&str>,
        password_hash: &str,
        is_instance_admin: bool,
    ) -> Result<UserRecord, sqlx::Error> {
        let user_id = Uuid::new_v4().to_string();
        let email = canonicalize_email(email);
        let now = Utc::now();

        match self {
            Catalog::Sqlite(pool) => {
                let now_str = now.to_rfc3339();
                let stmt = r#"
                INSERT INTO users (id, email, display_name, password_hash, is_instance_admin, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                "#;
                query(stmt)
                    .bind(&user_id)
                    .bind(&email)
                    .bind(display_name)
                    .bind(password_hash)
                    .bind(is_instance_admin)
                    .bind(&now_str)
                    .bind(&now_str)
                    .execute(pool)
                    .await?;
            }
            Catalog::Postgres(pool) => {
                let stmt = r#"
                INSERT INTO users (id, email, display_name, password_hash, is_instance_admin, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                "#;
                query(stmt)
                    .bind(&user_id)
                    .bind(&email)
                    .bind(display_name)
                    .bind(password_hash)
                    .bind(is_instance_admin)
                    .bind(now)
                    .bind(now)
                    .execute(pool)
                    .await?;
            }
        }

        Ok(UserRecord {
            id: user_id,
            email,
            display_name: display_name.map(str::to_string),
            password_hash: password_hash.to_string(),
            is_instance_admin,
            created_at: now,
            updated_at: now,
            disabled_at: None,
        })
    }

    /// Get a user by ID
    pub async fn get_user(&self, user_id: &str) -> Result<Option<UserRecord>, sqlx::Error> {
        let columns = "id, email, display_name, password_hash, is_instance_admin, created_at, updated_at, disabled_at";
        match self {
            Catalog::Sqlite(pool) => {
                let row = query(&format!("SELECT {columns} FROM users WHERE id = ?"))
                    .bind(user_id)
                    .fetch_optional(pool)
                    .await?;
                row.map(|r| user_from_sqlite_row(&r)).transpose()
            }
            Catalog::Postgres(pool) => {
                let row = query(&format!("SELECT {columns} FROM users WHERE id = $1"))
                    .bind(user_id)
                    .fetch_optional(pool)
                    .await?;
                Ok(row.map(|r| user_from_pg_row(&r)))
            }
        }
    }

    /// Get a user by email.
    ///
    /// The lookup email is canonicalized (trimmed and lowercased) to match
    /// the form stored by [`Catalog::create_user`].
    pub async fn get_user_by_email(&self, email: &str) -> Result<Option<UserRecord>, sqlx::Error> {
        let email = canonicalize_email(email);
        let columns = "id, email, display_name, password_hash, is_instance_admin, created_at, updated_at, disabled_at";
        match self {
            Catalog::Sqlite(pool) => {
                let row = query(&format!("SELECT {columns} FROM users WHERE email = ?"))
                    .bind(&email)
                    .fetch_optional(pool)
                    .await?;
                row.map(|r| user_from_sqlite_row(&r)).transpose()
            }
            Catalog::Postgres(pool) => {
                let row = query(&format!("SELECT {columns} FROM users WHERE email = $1"))
                    .bind(&email)
                    .fetch_optional(pool)
                    .await?;
                Ok(row.map(|r| user_from_pg_row(&r)))
            }
        }
    }

    /// List all users, ordered by email
    pub async fn list_users(&self) -> Result<Vec<UserRecord>, sqlx::Error> {
        let columns = "id, email, display_name, password_hash, is_instance_admin, created_at, updated_at, disabled_at";
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query(&format!("SELECT {columns} FROM users ORDER BY email"))
                    .fetch_all(pool)
                    .await?;
                rows.iter().map(user_from_sqlite_row).collect()
            }
            Catalog::Postgres(pool) => {
                let rows = query(&format!("SELECT {columns} FROM users ORDER BY email"))
                    .fetch_all(pool)
                    .await?;
                Ok(rows.iter().map(user_from_pg_row).collect())
            }
        }
    }

    /// Disable or re-enable a user account.
    ///
    /// Sets `disabled_at` to now when `disabled` is true, clears it when
    /// false, and bumps `updated_at` either way. Returns
    /// `sqlx::Error::RowNotFound` if the user does not exist.
    pub async fn set_user_disabled(
        &self,
        user_id: &str,
        disabled: bool,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now();
        let rows_affected = match self {
            Catalog::Sqlite(pool) => {
                let disabled_at = disabled.then(|| now.to_rfc3339());
                query("UPDATE users SET disabled_at = ?, updated_at = ? WHERE id = ?")
                    .bind(disabled_at)
                    .bind(now.to_rfc3339())
                    .bind(user_id)
                    .execute(pool)
                    .await?
                    .rows_affected()
            }
            Catalog::Postgres(pool) => {
                let disabled_at = disabled.then_some(now);
                query("UPDATE users SET disabled_at = $1, updated_at = $2 WHERE id = $3")
                    .bind(disabled_at)
                    .bind(now)
                    .bind(user_id)
                    .execute(pool)
                    .await?
                    .rows_affected()
            }
        };
        if rows_affected == 0 {
            return Err(sqlx::Error::RowNotFound);
        }
        Ok(())
    }

    /// Add a user to a tenant, or update their role if already a member
    pub async fn upsert_tenant_membership(
        &self,
        user_id: &str,
        tenant_id: &str,
        role: MembershipRole,
    ) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();
                let stmt = r#"
                INSERT INTO tenant_memberships (user_id, tenant_id, role, created_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (user_id, tenant_id) DO UPDATE SET role = excluded.role
                "#;
                query(stmt)
                    .bind(user_id)
                    .bind(tenant_id)
                    .bind(role.as_str())
                    .bind(&now)
                    .execute(pool)
                    .await?;
            }
            Catalog::Postgres(pool) => {
                let stmt = r#"
                INSERT INTO tenant_memberships (user_id, tenant_id, role)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, tenant_id) DO UPDATE SET role = EXCLUDED.role
                "#;
                query(stmt)
                    .bind(user_id)
                    .bind(tenant_id)
                    .bind(role.as_str())
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Remove a user from a tenant (idempotent)
    pub async fn remove_tenant_membership(
        &self,
        user_id: &str,
        tenant_id: &str,
    ) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                query("DELETE FROM tenant_memberships WHERE user_id = ? AND tenant_id = ?")
                    .bind(user_id)
                    .bind(tenant_id)
                    .execute(pool)
                    .await?;
            }
            Catalog::Postgres(pool) => {
                query("DELETE FROM tenant_memberships WHERE user_id = $1 AND tenant_id = $2")
                    .bind(user_id)
                    .bind(tenant_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Get a single membership for a (user, tenant) pair
    pub async fn get_tenant_membership(
        &self,
        user_id: &str,
        tenant_id: &str,
    ) -> Result<Option<TenantMembershipRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let row = query(
                    "SELECT user_id, tenant_id, role, created_at FROM tenant_memberships WHERE user_id = ? AND tenant_id = ?",
                )
                .bind(user_id)
                .bind(tenant_id)
                .fetch_optional(pool)
                .await?;
                row.map(|r| membership_from_sqlite_row(&r)).transpose()
            }
            Catalog::Postgres(pool) => {
                let row = query(
                    "SELECT user_id, tenant_id, role, created_at FROM tenant_memberships WHERE user_id = $1 AND tenant_id = $2",
                )
                .bind(user_id)
                .bind(tenant_id)
                .fetch_optional(pool)
                .await?;
                row.map(|r| membership_from_pg_row(&r)).transpose()
            }
        }
    }

    /// List all tenant memberships for a user
    pub async fn list_memberships_for_user(
        &self,
        user_id: &str,
    ) -> Result<Vec<TenantMembershipRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query(
                    "SELECT user_id, tenant_id, role, created_at FROM tenant_memberships WHERE user_id = ? ORDER BY tenant_id",
                )
                .bind(user_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(membership_from_sqlite_row).collect()
            }
            Catalog::Postgres(pool) => {
                let rows = query(
                    "SELECT user_id, tenant_id, role, created_at FROM tenant_memberships WHERE user_id = $1 ORDER BY tenant_id",
                )
                .bind(user_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(membership_from_pg_row).collect()
            }
        }
    }

    /// List all user memberships for a tenant
    pub async fn list_members_for_tenant(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<TenantMembershipRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query(
                    "SELECT user_id, tenant_id, role, created_at FROM tenant_memberships WHERE tenant_id = ? ORDER BY user_id",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(membership_from_sqlite_row).collect()
            }
            Catalog::Postgres(pool) => {
                let rows = query(
                    "SELECT user_id, tenant_id, role, created_at FROM tenant_memberships WHERE tenant_id = $1 ORDER BY user_id",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;
                rows.iter().map(membership_from_pg_row).collect()
            }
        }
    }

    /// Create a login session for a user with a random UUID id.
    ///
    /// `token_hash` is the hash of the session token; the plaintext token
    /// is never stored.
    pub async fn create_user_session(
        &self,
        user_id: &str,
        token_hash: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<UserSessionRecord, sqlx::Error> {
        let session_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        match self {
            Catalog::Sqlite(pool) => {
                let stmt = r#"
                INSERT INTO user_sessions (id, token_hash, user_id, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                "#;
                query(stmt)
                    .bind(&session_id)
                    .bind(token_hash)
                    .bind(user_id)
                    .bind(now.to_rfc3339())
                    .bind(expires_at.to_rfc3339())
                    .execute(pool)
                    .await?;
            }
            Catalog::Postgres(pool) => {
                let stmt = r#"
                INSERT INTO user_sessions (id, token_hash, user_id, created_at, expires_at)
                VALUES ($1, $2, $3, $4, $5)
                "#;
                query(stmt)
                    .bind(&session_id)
                    .bind(token_hash)
                    .bind(user_id)
                    .bind(now)
                    .bind(expires_at)
                    .execute(pool)
                    .await?;
            }
        }

        Ok(UserSessionRecord {
            id: session_id,
            token_hash: token_hash.to_string(),
            user_id: user_id.to_string(),
            created_at: now,
            expires_at,
            revoked_at: None,
        })
    }

    /// Look up a session by token hash, returning it only if it is neither
    /// revoked nor expired, and its user account is not disabled.
    pub async fn get_valid_session(
        &self,
        token_hash: &str,
    ) -> Result<Option<UserSessionRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                // expires_at is stored as chrono RFC3339 text (UTC, +00:00
                // offset), so lexicographic comparison against another
                // RFC3339 UTC timestamp is chronologically correct. The
                // join on users cuts off sessions of disabled accounts
                // immediately.
                let row = query(
                    r#"
                    SELECT s.id, s.token_hash, s.user_id, s.created_at, s.expires_at, s.revoked_at
                    FROM user_sessions s
                    JOIN users u ON u.id = s.user_id
                    WHERE s.token_hash = ?
                      AND s.revoked_at IS NULL
                      AND s.expires_at > ?
                      AND u.disabled_at IS NULL
                    "#,
                )
                .bind(token_hash)
                .bind(Utc::now().to_rfc3339())
                .fetch_optional(pool)
                .await?;
                row.map(|r| session_from_sqlite_row(&r)).transpose()
            }
            Catalog::Postgres(pool) => {
                let row = query(
                    r#"
                    SELECT s.id, s.token_hash, s.user_id, s.created_at, s.expires_at, s.revoked_at
                    FROM user_sessions s
                    JOIN users u ON u.id = s.user_id
                    WHERE s.token_hash = $1
                      AND s.revoked_at IS NULL
                      AND s.expires_at > NOW()
                      AND u.disabled_at IS NULL
                    "#,
                )
                .bind(token_hash)
                .fetch_optional(pool)
                .await?;
                Ok(row.map(|r| session_from_pg_row(&r)))
            }
        }
    }

    /// Revoke a session by ID
    pub async fn revoke_session(&self, session_id: &str) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();
                query("UPDATE user_sessions SET revoked_at = ? WHERE id = ?")
                    .bind(&now)
                    .bind(session_id)
                    .execute(pool)
                    .await?;
            }
            Catalog::Postgres(pool) => {
                query("UPDATE user_sessions SET revoked_at = NOW() WHERE id = $1")
                    .bind(session_id)
                    .execute(pool)
                    .await?;
            }
        }
        Ok(())
    }

    /// Delete all sessions whose expiry has passed.
    ///
    /// Returns the number of rows removed. Safe to run concurrently — the
    /// DELETE is idempotent.
    pub async fn delete_expired_sessions(&self) -> Result<u64, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let result = query("DELETE FROM user_sessions WHERE expires_at < ?")
                    .bind(Utc::now().to_rfc3339())
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected())
            }
            Catalog::Postgres(pool) => {
                let result = query("DELETE FROM user_sessions WHERE expires_at < NOW()")
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected())
            }
        }
    }
}

// ── Compactor lease management ────────────────────────────────────────────────

/// A lease record from the `compactor_leases` table.
///
/// A lease represents an exclusive claim on a single compaction work unit
/// (identified by tenant/dataset/table/partition). Only one compactor
/// instance may hold a non-expired lease at a time.
#[derive(Debug, Clone)]
pub struct CompactorLease {
    /// Tenant identifier
    pub tenant_id: String,
    /// Dataset identifier
    pub dataset_id: String,
    /// Table name (e.g. "traces", "logs")
    pub table_name: String,
    /// Partition identifier
    pub partition_id: String,
    /// UUID (as string) of the compactor instance that holds this lease
    pub holder_id: String,
    /// When the lease was first acquired
    pub acquired_at: DateTime<Utc>,
    /// When the lease expires (may be renewed)
    pub expires_at: DateTime<Utc>,
    /// Last renewal time, if any
    pub renewed_at: Option<DateTime<Utc>>,
}

impl Catalog {
    /// Attempt to acquire a compaction lease for a specific work unit.
    ///
    /// Uses an atomic `INSERT … ON CONFLICT DO UPDATE WHERE expires_at < now`
    /// pattern so that only one instance can hold a non-expired lease at a time.
    /// Expired leases are automatically taken over.
    ///
    /// Returns `true` when the lease was acquired (new insert or expired takeover),
    /// `false` when another instance holds a live lease.
    pub async fn try_acquire_compaction_lease(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        partition_id: &str,
        holder_id: &str,
        ttl_ms: i64,
    ) -> Result<bool, sqlx::Error> {
        let now = Utc::now();
        let expires_at = now + chrono::Duration::milliseconds(ttl_ms);
        let now_str = now.to_rfc3339();
        let expires_at_str = expires_at.to_rfc3339();

        match self {
            Catalog::Sqlite(pool) => {
                let stmt = r#"
                INSERT INTO compactor_leases
                    (tenant_id, dataset_id, table_name, partition_id, holder_id, acquired_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (tenant_id, dataset_id, table_name, partition_id) DO UPDATE
                SET holder_id  = excluded.holder_id,
                    acquired_at = excluded.acquired_at,
                    expires_at  = excluded.expires_at,
                    renewed_at  = NULL
                WHERE compactor_leases.expires_at < ?
                "#;
                let result = query(stmt)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(table_name)
                    .bind(partition_id)
                    .bind(holder_id)
                    .bind(&now_str)
                    .bind(&expires_at_str)
                    .bind(&now_str) // WHERE clause: existing lease must be expired
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
            Catalog::Postgres(pool) => {
                // Use the database clock (NOW()) for both the expiry stamp
                // and the takeover comparison so that clock skew between
                // compactor instances cannot steal a live lease.
                let stmt = r#"
                INSERT INTO compactor_leases
                    (tenant_id, dataset_id, table_name, partition_id, holder_id, acquired_at, expires_at)
                VALUES ($1, $2, $3, $4, $5, NOW(), NOW() + $6 * INTERVAL '1 millisecond')
                ON CONFLICT (tenant_id, dataset_id, table_name, partition_id) DO UPDATE
                SET holder_id   = EXCLUDED.holder_id,
                    acquired_at = EXCLUDED.acquired_at,
                    expires_at  = EXCLUDED.expires_at,
                    renewed_at  = NULL
                WHERE compactor_leases.expires_at < NOW()
                "#;
                let result = query(stmt)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(table_name)
                    .bind(partition_id)
                    .bind(holder_id)
                    .bind(ttl_ms)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
        }
    }

    /// Renew an existing lease, extending its expiry by `ttl_ms` from now.
    ///
    /// Only succeeds if `holder_id` matches the current holder (prevents
    /// renewing a lease that was taken over by another instance).
    ///
    /// Returns `true` if renewed, `false` if the lease was stolen or not found.
    pub async fn renew_compaction_lease(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        partition_id: &str,
        holder_id: &str,
        ttl_ms: i64,
    ) -> Result<bool, sqlx::Error> {
        let now = Utc::now();
        let expires_at = now + chrono::Duration::milliseconds(ttl_ms);

        match self {
            Catalog::Sqlite(pool) => {
                let stmt = r#"
                UPDATE compactor_leases
                SET expires_at = ?,
                    renewed_at = ?
                WHERE tenant_id    = ?
                  AND dataset_id   = ?
                  AND table_name   = ?
                  AND partition_id = ?
                  AND holder_id    = ?
                "#;
                let result = query(stmt)
                    .bind(expires_at.to_rfc3339())
                    .bind(now.to_rfc3339())
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(table_name)
                    .bind(partition_id)
                    .bind(holder_id)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
            Catalog::Postgres(pool) => {
                // DB clock, matching try_acquire (clock-skew immunity).
                let stmt = r#"
                UPDATE compactor_leases
                SET expires_at = NOW() + $1 * INTERVAL '1 millisecond',
                    renewed_at = NOW()
                WHERE tenant_id    = $2
                  AND dataset_id   = $3
                  AND table_name   = $4
                  AND partition_id = $5
                  AND holder_id    = $6
                "#;
                let result = query(stmt)
                    .bind(ttl_ms)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(table_name)
                    .bind(partition_id)
                    .bind(holder_id)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
        }
    }

    /// Release a lease explicitly after work completes.
    ///
    /// Only deletes the lease if `holder_id` matches, preventing an instance
    /// from releasing a lease it no longer owns.
    ///
    /// Returns `true` if the lease was deleted, `false` if not found or stolen.
    pub async fn release_compaction_lease(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        table_name: &str,
        partition_id: &str,
        holder_id: &str,
    ) -> Result<bool, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let stmt = r#"
                DELETE FROM compactor_leases
                WHERE tenant_id    = ?
                  AND dataset_id   = ?
                  AND table_name   = ?
                  AND partition_id = ?
                  AND holder_id    = ?
                "#;
                let result = query(stmt)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(table_name)
                    .bind(partition_id)
                    .bind(holder_id)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
            Catalog::Postgres(pool) => {
                let stmt = r#"
                DELETE FROM compactor_leases
                WHERE tenant_id    = $1
                  AND dataset_id   = $2
                  AND table_name   = $3
                  AND partition_id = $4
                  AND holder_id    = $5
                "#;
                let result = query(stmt)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(table_name)
                    .bind(partition_id)
                    .bind(holder_id)
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected() > 0)
            }
        }
    }

    /// Delete all expired leases (where `expires_at < now`).
    ///
    /// Called periodically to garbage-collect leases from crashed instances.
    /// Returns the count of leases removed.
    pub async fn expire_stale_compaction_leases(&self) -> Result<u64, sqlx::Error> {
        let now = Utc::now();
        match self {
            Catalog::Sqlite(pool) => {
                let result = query("DELETE FROM compactor_leases WHERE expires_at < ?")
                    .bind(now.to_rfc3339())
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected())
            }
            Catalog::Postgres(pool) => {
                let result = query("DELETE FROM compactor_leases WHERE expires_at < NOW()")
                    .execute(pool)
                    .await?;
                Ok(result.rows_affected())
            }
        }
    }

    /// List all currently active (non-expired) leases.
    ///
    /// Used for status reporting and observability.
    pub async fn list_active_compaction_leases(&self) -> Result<Vec<CompactorLease>, sqlx::Error> {
        let now = Utc::now();
        match self {
            Catalog::Sqlite(pool) => {
                let rows = sqlx::query(
                    r#"
                    SELECT tenant_id, dataset_id, table_name, partition_id,
                           holder_id, acquired_at, expires_at, renewed_at
                    FROM compactor_leases
                    WHERE expires_at >= ?
                    ORDER BY acquired_at
                    "#,
                )
                .bind(now.to_rfc3339())
                .fetch_all(pool)
                .await?;

                rows.iter()
                    .map(|row| {
                        Ok(CompactorLease {
                            tenant_id: row.try_get("tenant_id")?,
                            dataset_id: row.try_get("dataset_id")?,
                            table_name: row.try_get("table_name")?,
                            partition_id: row.try_get("partition_id")?,
                            holder_id: row.try_get("holder_id")?,
                            acquired_at: parse_rfc3339(row.try_get("acquired_at")?)?,
                            expires_at: parse_rfc3339(row.try_get("expires_at")?)?,
                            renewed_at: row
                                .try_get::<Option<&str>, _>("renewed_at")?
                                .map(parse_rfc3339)
                                .transpose()?,
                        })
                    })
                    .collect()
            }
            Catalog::Postgres(pool) => {
                let rows = sqlx::query(
                    r#"
                    SELECT tenant_id, dataset_id, table_name, partition_id,
                           holder_id, acquired_at, expires_at, renewed_at
                    FROM compactor_leases
                    WHERE expires_at >= $1
                    ORDER BY acquired_at
                    "#,
                )
                .bind(now)
                .fetch_all(pool)
                .await?;

                rows.iter()
                    .map(|row| {
                        Ok(CompactorLease {
                            tenant_id: row.try_get("tenant_id")?,
                            dataset_id: row.try_get("dataset_id")?,
                            table_name: row.try_get("table_name")?,
                            partition_id: row.try_get("partition_id")?,
                            holder_id: row.try_get::<Uuid, _>("holder_id")?.to_string(),
                            acquired_at: row.try_get("acquired_at")?,
                            expires_at: row.try_get("expires_at")?,
                            renewed_at: row.try_get("renewed_at")?,
                        })
                    })
                    .collect()
            }
        }
    }
}

#[cfg(test)]
mod multi_tenancy_tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn hash_api_key(key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        hex::encode(hasher.finalize())
    }

    #[tokio::test]
    async fn test_tenant_upsert_and_get() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        // Upsert a new tenant
        catalog
            .upsert_tenant("test-tenant", "Test Tenant", Some("production"), "config")
            .await
            .unwrap();

        // Retrieve the tenant
        let tenant = catalog.get_tenant("test-tenant").await.unwrap().unwrap();
        assert_eq!(tenant.id, "test-tenant");
        assert_eq!(tenant.name, "Test Tenant");
        assert_eq!(tenant.default_dataset, Some("production".to_string()));
        assert_eq!(tenant.source, "config");

        // Update the tenant
        catalog
            .upsert_tenant("test-tenant", "Updated Tenant", Some("staging"), "database")
            .await
            .unwrap();

        // Verify update
        let updated = catalog.get_tenant("test-tenant").await.unwrap().unwrap();
        assert_eq!(updated.name, "Updated Tenant");
        assert_eq!(updated.default_dataset, Some("staging".to_string()));
        assert_eq!(updated.source, "database");
    }

    #[tokio::test]
    async fn test_list_tenants() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        // Create multiple tenants
        catalog
            .upsert_tenant("tenant1", "Tenant One", Some("prod"), "config")
            .await
            .unwrap();
        catalog
            .upsert_tenant("tenant2", "Tenant Two", None, "database")
            .await
            .unwrap();
        catalog
            .upsert_tenant("tenant3", "Tenant Three", Some("dev"), "config")
            .await
            .unwrap();

        // List all tenants
        let tenants = catalog.list_tenants().await.unwrap();
        assert_eq!(tenants.len(), 3);

        let tenant_ids: Vec<&str> = tenants.iter().map(|t| t.id.as_str()).collect();
        assert!(tenant_ids.contains(&"tenant1"));
        assert!(tenant_ids.contains(&"tenant2"));
        assert!(tenant_ids.contains(&"tenant3"));
    }

    #[tokio::test]
    async fn test_api_key_lifecycle() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        // Create a tenant first
        catalog
            .upsert_tenant("acme", "Acme Corp", Some("production"), "config")
            .await
            .unwrap();

        // Create an API key
        let api_key = "sk_acme_test_1234567890";
        let key_hash = hash_api_key(api_key);
        let key_id = catalog
            .upsert_api_key("acme", &key_hash, Some("test-key"))
            .await
            .unwrap();

        assert!(!key_id.is_empty());

        // Validate the API key
        let validation = catalog.validate_api_key(&key_hash).await.unwrap();
        assert!(validation.is_some());
        let (tenant_id, name) = validation.unwrap();
        assert_eq!(tenant_id, "acme");
        assert_eq!(name, Some("test-key".to_string()));

        // Try to create the same key again (should return existing ID)
        let duplicate_id = catalog
            .upsert_api_key("acme", &key_hash, Some("test-key"))
            .await
            .unwrap();
        assert_eq!(key_id, duplicate_id);

        // Revoke the API key
        catalog.revoke_api_key(&key_id).await.unwrap();

        // Validation should now fail
        let revoked_validation = catalog.validate_api_key(&key_hash).await.unwrap();
        assert!(revoked_validation.is_none());
    }

    #[tokio::test]
    async fn test_api_key_tenant_isolation() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        // Create two tenants
        catalog
            .upsert_tenant("tenant-a", "Tenant A", None, "config")
            .await
            .unwrap();
        catalog
            .upsert_tenant("tenant-b", "Tenant B", None, "config")
            .await
            .unwrap();

        // Create API keys for each tenant
        let key_a = "sk_tenant_a_key";
        let hash_a = hash_api_key(key_a);
        catalog
            .upsert_api_key("tenant-a", &hash_a, Some("key-a"))
            .await
            .unwrap();

        let key_b = "sk_tenant_b_key";
        let hash_b = hash_api_key(key_b);
        catalog
            .upsert_api_key("tenant-b", &hash_b, Some("key-b"))
            .await
            .unwrap();

        // Validate keys return correct tenants
        let (tenant_id_a, _) = catalog.validate_api_key(&hash_a).await.unwrap().unwrap();
        assert_eq!(tenant_id_a, "tenant-a");

        let (tenant_id_b, _) = catalog.validate_api_key(&hash_b).await.unwrap().unwrap();
        assert_eq!(tenant_id_b, "tenant-b");
    }

    #[tokio::test]
    async fn test_dataset_operations() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        // Create a tenant
        catalog
            .upsert_tenant("company", "Company Inc", Some("production"), "config")
            .await
            .unwrap();

        // Create datasets
        let dataset1_id = catalog
            .create_dataset("company", "production")
            .await
            .unwrap();
        let dataset2_id = catalog.create_dataset("company", "staging").await.unwrap();
        let dataset3_id = catalog
            .create_dataset("company", "development")
            .await
            .unwrap();

        assert!(!dataset1_id.is_empty());
        assert!(!dataset2_id.is_empty());
        assert!(!dataset3_id.is_empty());
        assert_ne!(dataset1_id, dataset2_id);

        // Get datasets for tenant
        let datasets = catalog.get_datasets("company").await.unwrap();
        assert_eq!(datasets.len(), 3);

        let dataset_names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();
        assert!(dataset_names.contains(&"production"));
        assert!(dataset_names.contains(&"staging"));
        assert!(dataset_names.contains(&"development"));

        // Verify all datasets belong to the correct tenant
        for dataset in datasets {
            assert_eq!(dataset.tenant_id, "company");
        }
    }

    #[tokio::test]
    async fn test_dataset_tenant_isolation() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        // Create two tenants
        catalog
            .upsert_tenant("org-a", "Organization A", None, "config")
            .await
            .unwrap();
        catalog
            .upsert_tenant("org-b", "Organization B", None, "config")
            .await
            .unwrap();

        // Create datasets for each tenant
        catalog.create_dataset("org-a", "prod").await.unwrap();
        catalog.create_dataset("org-a", "dev").await.unwrap();
        catalog.create_dataset("org-b", "test").await.unwrap();

        // Get datasets for org-a
        let datasets_a = catalog.get_datasets("org-a").await.unwrap();
        assert_eq!(datasets_a.len(), 2);
        for dataset in datasets_a {
            assert_eq!(dataset.tenant_id, "org-a");
        }

        // Get datasets for org-b
        let datasets_b = catalog.get_datasets("org-b").await.unwrap();
        assert_eq!(datasets_b.len(), 1);
        assert_eq!(datasets_b[0].tenant_id, "org-b");
        assert_eq!(datasets_b[0].name, "test");
    }

    #[tokio::test]
    async fn test_get_nonexistent_tenant() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        let result = catalog.get_tenant("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_validate_nonexistent_api_key() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        let fake_hash = "nonexistent_hash";
        let result = catalog.validate_api_key(fake_hash).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn attribute_stats_scan_upsert_and_demand_accumulate() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        catalog
            .upsert_attribute_scan_stats("t", "d", "logs", "namespace", 80, 100, 5, false)
            .await
            .unwrap();
        // Demand accumulates across flushes; scan stats replace.
        catalog
            .add_attribute_query_hits("t", "d", "logs", "namespace", 3)
            .await
            .unwrap();
        catalog
            .add_attribute_query_hits("t", "d", "logs", "namespace", 2)
            .await
            .unwrap();
        catalog
            .upsert_attribute_scan_stats("t", "d", "logs", "namespace", 90, 120, 7, true)
            .await
            .unwrap();
        // A demand-only key exists with zeroed scan stats.
        catalog
            .add_attribute_query_hits("t", "d", "logs", "pod", 1)
            .await
            .unwrap();

        let stats = catalog.get_attribute_stats("t", "d", "logs").await.unwrap();
        assert_eq!(stats.len(), 2);
        let ns = &stats[0];
        assert_eq!(ns.attr_key, "namespace");
        assert_eq!(ns.present_rows, 90);
        assert_eq!(ns.total_rows, 120);
        assert_eq!(ns.distinct_estimate, 7);
        assert!(ns.capped);
        assert_eq!(ns.query_hits, 5);
        let pod = &stats[1];
        assert_eq!(pod.attr_key, "pod");
        assert_eq!(pod.query_hits, 1);
        assert_eq!(pod.present_rows, 0);

        assert!(
            catalog
                .get_attribute_stats("t", "d", "traces")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_get_datasets_for_nonexistent_tenant() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        let datasets = catalog.get_datasets("nonexistent").await.unwrap();
        assert!(datasets.is_empty());
    }
}

#[cfg(test)]
mod user_membership_tests {
    use super::*;
    use chrono::Duration;

    #[test]
    fn membership_role_round_trips_through_lowercase_strings() {
        for (role, s) in [
            (MembershipRole::Admin, "admin"),
            (MembershipRole::Member, "member"),
            (MembershipRole::Viewer, "viewer"),
        ] {
            assert_eq!(role.to_string(), s);
            assert_eq!(s.parse::<MembershipRole>().unwrap(), role);
        }
        assert!("Owner".parse::<MembershipRole>().is_err());
    }

    #[tokio::test]
    async fn create_user_returns_record_retrievable_by_id_and_email() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        let created = catalog
            .create_user("alice@example.com", Some("Alice"), "phc-hash-1", true)
            .await
            .unwrap();
        assert_eq!(created.email, "alice@example.com");
        assert_eq!(created.display_name, Some("Alice".to_string()));
        assert_eq!(created.password_hash, "phc-hash-1");
        assert!(created.is_instance_admin);
        assert!(created.disabled_at.is_none());

        let by_id = catalog.get_user(&created.id).await.unwrap().unwrap();
        assert_eq!(by_id.id, created.id);
        assert_eq!(by_id.email, "alice@example.com");
        assert!(by_id.is_instance_admin);

        let by_email = catalog
            .get_user_by_email("alice@example.com")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(by_email.id, created.id);
    }

    #[tokio::test]
    async fn get_user_returns_none_for_unknown_id_and_email() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        assert!(catalog.get_user("missing").await.unwrap().is_none());
        assert!(
            catalog
                .get_user_by_email("nobody@example.com")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn create_user_with_duplicate_email_returns_error() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        catalog
            .create_user("dup@example.com", None, "hash-a", false)
            .await
            .unwrap();
        let result = catalog
            .create_user("dup@example.com", None, "hash-b", false)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn user_email_identity_is_case_insensitive() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        let created = catalog
            .create_user("Alice@Example.com", None, "hash-a", false)
            .await
            .unwrap();
        // Stored in canonical (lowercase) form
        assert_eq!(created.email, "alice@example.com");

        // Same address in different case hits the UNIQUE constraint
        let duplicate = catalog
            .create_user("alice@example.com", None, "hash-b", false)
            .await;
        assert!(duplicate.is_err());

        // Lookup canonicalizes too
        let found = catalog
            .get_user_by_email("ALICE@example.com")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(found.id, created.id);
    }

    #[tokio::test]
    async fn list_users_returns_all_created_users() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        catalog
            .create_user("b@example.com", None, "hash-b", false)
            .await
            .unwrap();
        catalog
            .create_user("a@example.com", None, "hash-a", false)
            .await
            .unwrap();

        let users = catalog.list_users().await.unwrap();
        assert_eq!(users.len(), 2);
        // Ordered by email
        assert_eq!(users[0].email, "a@example.com");
        assert_eq!(users[1].email, "b@example.com");
    }

    #[tokio::test]
    async fn set_user_disabled_sets_and_clears_disabled_at() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        let user = catalog
            .create_user("flip@example.com", None, "hash", false)
            .await
            .unwrap();

        catalog.set_user_disabled(&user.id, true).await.unwrap();
        let disabled = catalog.get_user(&user.id).await.unwrap().unwrap();
        assert!(disabled.disabled_at.is_some());
        assert!(disabled.updated_at >= user.updated_at);

        catalog.set_user_disabled(&user.id, false).await.unwrap();
        let enabled = catalog.get_user(&user.id).await.unwrap().unwrap();
        assert!(enabled.disabled_at.is_none());
    }

    #[tokio::test]
    async fn set_user_disabled_returns_row_not_found_for_unknown_user() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        let result = catalog.set_user_disabled("missing", true).await;
        assert!(matches!(result, Err(sqlx::Error::RowNotFound)));
    }

    async fn setup_user_and_tenants(catalog: &Catalog) -> String {
        catalog
            .upsert_tenant("acme", "Acme Corp", None, "config")
            .await
            .unwrap();
        catalog
            .upsert_tenant("globex", "Globex", None, "config")
            .await
            .unwrap();
        let user = catalog
            .create_user("member@example.com", None, "hash", false)
            .await
            .unwrap();
        user.id
    }

    #[tokio::test]
    async fn upsert_tenant_membership_inserts_then_updates_role() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let user_id = setup_user_and_tenants(&catalog).await;

        catalog
            .upsert_tenant_membership(&user_id, "acme", MembershipRole::Viewer)
            .await
            .unwrap();
        let membership = catalog
            .get_tenant_membership(&user_id, "acme")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(membership.role, MembershipRole::Viewer);
        assert_eq!(membership.user_id, user_id);
        assert_eq!(membership.tenant_id, "acme");

        // Upserting again changes the role without duplicating the row
        catalog
            .upsert_tenant_membership(&user_id, "acme", MembershipRole::Admin)
            .await
            .unwrap();
        let updated = catalog
            .get_tenant_membership(&user_id, "acme")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.role, MembershipRole::Admin);
        assert_eq!(
            catalog.list_members_for_tenant("acme").await.unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn remove_tenant_membership_deletes_row() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let user_id = setup_user_and_tenants(&catalog).await;

        catalog
            .upsert_tenant_membership(&user_id, "acme", MembershipRole::Member)
            .await
            .unwrap();
        catalog
            .remove_tenant_membership(&user_id, "acme")
            .await
            .unwrap();
        assert!(
            catalog
                .get_tenant_membership(&user_id, "acme")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn membership_lists_work_in_both_directions() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let user_id = setup_user_and_tenants(&catalog).await;
        let other = catalog
            .create_user("other@example.com", None, "hash2", false)
            .await
            .unwrap();

        catalog
            .upsert_tenant_membership(&user_id, "acme", MembershipRole::Admin)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&user_id, "globex", MembershipRole::Viewer)
            .await
            .unwrap();
        catalog
            .upsert_tenant_membership(&other.id, "acme", MembershipRole::Member)
            .await
            .unwrap();

        let for_user = catalog.list_memberships_for_user(&user_id).await.unwrap();
        assert_eq!(for_user.len(), 2);
        let tenant_ids: Vec<&str> = for_user.iter().map(|m| m.tenant_id.as_str()).collect();
        assert_eq!(tenant_ids, vec!["acme", "globex"]);

        let for_tenant = catalog.list_members_for_tenant("acme").await.unwrap();
        assert_eq!(for_tenant.len(), 2);
        assert!(for_tenant.iter().any(|m| m.user_id == user_id));
        assert!(for_tenant.iter().any(|m| m.user_id == other.id));

        assert!(
            catalog
                .list_memberships_for_user("missing")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn membership_insert_fails_for_nonexistent_tenant() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let user = catalog
            .create_user("orphan@example.com", None, "hash", false)
            .await
            .unwrap();

        // sqlx enables PRAGMA foreign_keys by default for SQLite, so the
        // FK to tenants(id) is enforced.
        let result = catalog
            .upsert_tenant_membership(&user.id, "no-such-tenant", MembershipRole::Member)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_user_session_and_get_valid_session_roundtrip() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let user = catalog
            .create_user("session@example.com", None, "hash", false)
            .await
            .unwrap();

        let expires_at = Utc::now() + Duration::hours(1);
        let session = catalog
            .create_user_session(&user.id, "token-hash-1", expires_at)
            .await
            .unwrap();
        assert_eq!(session.user_id, user.id);
        assert_eq!(session.token_hash, "token-hash-1");
        assert!(session.revoked_at.is_none());

        let valid = catalog
            .get_valid_session("token-hash-1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(valid.id, session.id);
        assert_eq!(valid.user_id, user.id);

        assert!(
            catalog
                .get_valid_session("unknown-hash")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn revoked_session_is_not_returned_by_get_valid_session() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let user = catalog
            .create_user("revoke@example.com", None, "hash", false)
            .await
            .unwrap();

        let session = catalog
            .create_user_session(
                &user.id,
                "token-hash-revoke",
                Utc::now() + Duration::hours(1),
            )
            .await
            .unwrap();
        catalog.revoke_session(&session.id).await.unwrap();

        assert!(
            catalog
                .get_valid_session("token-hash-revoke")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn disabled_user_sessions_are_not_returned_until_reenabled() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let user = catalog
            .create_user("locked@example.com", None, "hash", false)
            .await
            .unwrap();

        catalog
            .create_user_session(
                &user.id,
                "token-hash-locked",
                Utc::now() + Duration::hours(1),
            )
            .await
            .unwrap();
        assert!(
            catalog
                .get_valid_session("token-hash-locked")
                .await
                .unwrap()
                .is_some()
        );

        // Disabling the user cuts off the session immediately
        catalog.set_user_disabled(&user.id, true).await.unwrap();
        assert!(
            catalog
                .get_valid_session("token-hash-locked")
                .await
                .unwrap()
                .is_none()
        );

        // Re-enabling restores access for the still-valid session
        catalog.set_user_disabled(&user.id, false).await.unwrap();
        assert!(
            catalog
                .get_valid_session("token-hash-locked")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn expired_session_is_not_returned_by_get_valid_session() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let user = catalog
            .create_user("expired@example.com", None, "hash", false)
            .await
            .unwrap();

        catalog
            .create_user_session(
                &user.id,
                "token-hash-expired",
                Utc::now() - Duration::hours(1),
            )
            .await
            .unwrap();

        assert!(
            catalog
                .get_valid_session("token-hash-expired")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn delete_expired_sessions_removes_only_expired_rows() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let user = catalog
            .create_user("cleanup@example.com", None, "hash", false)
            .await
            .unwrap();

        catalog
            .create_user_session(&user.id, "hash-old", Utc::now() - Duration::hours(2))
            .await
            .unwrap();
        catalog
            .create_user_session(&user.id, "hash-current", Utc::now() + Duration::hours(2))
            .await
            .unwrap();

        let removed = catalog.delete_expired_sessions().await.unwrap();
        assert_eq!(removed, 1);

        // The live session survives cleanup
        assert!(
            catalog
                .get_valid_session("hash-current")
                .await
                .unwrap()
                .is_some()
        );

        // Nothing left to remove on a second pass
        assert_eq!(catalog.delete_expired_sessions().await.unwrap(), 0);
    }
}
