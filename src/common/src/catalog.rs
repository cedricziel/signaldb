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
        log::info!("Connecting to catalog database with DSN: {dsn}");

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
                log::error!(
                    "Failed to connect to SQLite database with DSN '{dsn_with_create}': {e}"
                );
                e
            })?;
            Catalog::Sqlite(pool)
        } else {
            let pool = PgPool::connect(dsn).await.map_err(|e| {
                log::error!("Failed to connect to PostgreSQL database with DSN '{dsn}': {e}");
                e
            })?;
            Catalog::Postgres(pool)
        };

        log::info!("Database connection established successfully");
        catalog.init().await.map_err(|e| {
            log::error!("Failed to initialize catalog schema: {e}");
            e
        })?;
        log::info!("Catalog schema initialized successfully");
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
            }
        }

        Ok(())
    }

    /// Register or update an ingester with its address, service type, capabilities and heartbeat.
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

    /// Deregister an ingester instance, removing it from the catalog.
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
                    log::error!("Failed to send heartbeat for ingester {id}: {e}");
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

#[cfg(test)]
mod multi_tenancy_tests {
    use super::*;
    use sha2::{Digest, Sha256};

    fn hash_api_key(key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        format!("{:x}", hasher.finalize())
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
    async fn test_get_datasets_for_nonexistent_tenant() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        let datasets = catalog.get_datasets("nonexistent").await.unwrap();
        assert!(datasets.is_empty());
    }
}
