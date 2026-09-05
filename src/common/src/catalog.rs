use crate::auth::Authenticator;
use crate::config::AuthConfig;
use crate::flight::transport::ServiceCapability;
use crate::service_bootstrap::ServiceType;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{PgPool, Row, SqlitePool, query};
use std::str::FromStr;
use tracing::Instrument;
use uuid::Uuid;

/// Helper to parse RFC3339 datetime strings (SQLite stores timestamps as text)
fn parse_rfc3339(s: &str) -> Result<DateTime<Utc>, sqlx::Error> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| sqlx::Error::Decode(Box::new(e)))
}

/// Decode a required JSON-array-of-strings column (e.g. OAuth scopes,
/// redirect URIs) into a `Vec<String>`.
fn decode_json_vec(json: String) -> Result<Vec<String>, sqlx::Error> {
    serde_json::from_str(&json).map_err(|e| sqlx::Error::Decode(Box::new(e)))
}

/// Decode an optional JSON-array-of-strings column into `Option<Vec<String>>`.
fn decode_json_vec_opt(json: Option<String>) -> Result<Option<Vec<String>>, sqlx::Error> {
    json.map(decode_json_vec).transpose()
}

/// Add `column TEXT` to `table` if it doesn't already exist, via `PRAGMA
/// table_info` (SQLite has no native `ADD COLUMN IF NOT EXISTS`) — the same
/// gate `api_keys`' `dataset_id`/`scopes`/`created_by_user_id` columns use.
/// `table` and `column` are always compile-time literals from call sites in
/// this module, never user input.
async fn ensure_sqlite_text_column(
    pool: &SqlitePool,
    table: &str,
    column: &str,
) -> Result<(), sqlx::Error> {
    let columns = query(&format!("PRAGMA table_info({table})"))
        .fetch_all(pool)
        .await?;
    let has_column = columns
        .iter()
        .any(|row| row.get::<String, _>("name") == column);
    if !has_column {
        query(&format!("ALTER TABLE {table} ADD COLUMN {column} TEXT"))
            .execute(pool)
            .await?;
    }
    Ok(())
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
    /// Semconv DB CLIENT span for a catalog operation (`db.operation.name`
    /// is the SQL verb; the catalog is one logical namespace).
    fn db_span(&self, operation: &str) -> tracing::Span {
        let system = match self {
            Catalog::Sqlite(_) => "sqlite",
            Catalog::Postgres(_) => "postgresql",
        };
        crate::self_monitoring::spans::db_client_span(system, operation, "signaldb-catalog")
    }

    /// Record the statement text on the current `db.client` span. sqlx
    /// binds values rather than interpolating them, so `stmt` is already a
    /// parameterized template with nothing sensitive in it; sanitizing
    /// anyway is a defensive default (see `db_client_span`'s doc comment)
    /// and a no-op on text that has no literals to strip.
    fn record_query_text(stmt: &str) {
        tracing::Span::current().record(
            "db.query.text",
            crate::self_monitoring::sanitize::sanitize_query_text(stmt).as_str(),
        );
    }

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

            // Enable WAL journaling and a generous busy_timeout on the catalog
            // connection. The default rollback journal takes an exclusive lock
            // for every write, so under concurrent commit volume writers
            // serialize and slow statements pile up (this is what makes the
            // acceptor->writer do_put time out on first-time metric-table
            // creation). WAL lets readers proceed during a write and makes each
            // write cheaper. In-memory databases don't support WAL, so only
            // tune on-disk files.
            let is_memory = dsn.contains(":memory:");
            let mut connect_options = SqliteConnectOptions::from_str(&dsn_with_create)
                .map_err(|e| {
                    tracing::error!(
                        dsn = %crate::config::redact_dsn(&dsn_with_create),
                        error = %e,
                        "Failed to parse SQLite connection string"
                    );
                    e
                })?
                .create_if_missing(true)
                .busy_timeout(std::time::Duration::from_secs(30));
            if !is_memory {
                connect_options = connect_options
                    .journal_mode(SqliteJournalMode::Wal)
                    .synchronous(SqliteSynchronous::Normal);
            }

            let pool = SqlitePoolOptions::new()
                .connect_with(connect_options)
                .await
                .map_err(|e| {
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
                    dataset_id TEXT,
                    scopes TEXT,
                    created_by_user_id TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    revoked_at TEXT,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
                    UNIQUE(tenant_id, name)
                )"#;
                query(create_api_keys).execute(pool).await?;
                let api_key_columns = query("PRAGMA table_info(api_keys)").fetch_all(pool).await?;
                let has_api_key_column = |name: &str| {
                    api_key_columns
                        .iter()
                        .any(|row| row.get::<String, _>("name") == name)
                };
                if !has_api_key_column("dataset_id") {
                    query("ALTER TABLE api_keys ADD COLUMN dataset_id TEXT")
                        .execute(pool)
                        .await?;
                }
                if !has_api_key_column("scopes") {
                    query("ALTER TABLE api_keys ADD COLUMN scopes TEXT")
                        .execute(pool)
                        .await?;
                }
                if !has_api_key_column("created_by_user_id") {
                    query("ALTER TABLE api_keys ADD COLUMN created_by_user_id TEXT")
                        .execute(pool)
                        .await?;
                }
                if !has_api_key_column("dataset_ids") {
                    query("ALTER TABLE api_keys ADD COLUMN dataset_ids TEXT")
                        .execute(pool)
                        .await?;
                }

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

                // Value sketches (change: query-field-discovery): the bounded
                // top values per key the analyzer observed, so discovery can
                // suggest values without reading signal data.
                let create_attribute_value_stats = r#"
                CREATE TABLE IF NOT EXISTS attribute_value_stats (
                    tenant_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    signal TEXT NOT NULL,
                    attr_key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    count BIGINT NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (tenant_id, dataset_id, signal, attr_key, value)
                )"#;
                query(create_attribute_value_stats).execute(pool).await?;

                // Tenant custom schema registries (change: schema-registry).
                // The uploaded Weaver-model document is the source of truth;
                // `resolved` caches the flattened definitions the resolver
                // loads per tenant. Bundled registries never live here.
                query(
                    r#"
                CREATE TABLE IF NOT EXISTS schema_registries (
                    tenant_id TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    version TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'custom',
                    schema_url TEXT,
                    description TEXT,
                    document TEXT NOT NULL,
                    resolved TEXT NOT NULL,
                    attribute_count BIGINT NOT NULL DEFAULT 0,
                    entity_count BIGINT NOT NULL DEFAULT 0,
                    metric_count BIGINT NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
                    PRIMARY KEY (tenant_id, namespace, version)
                )"#,
                )
                .execute(pool)
                .await?;

                // OAuth 2.1 authorization-server tables (change: mcp-oauth-dcr).
                // Dynamically-registered clients, single-use authorization
                // codes, and opaque access/refresh tokens (stored as hashes).
                query(
                    r#"
                CREATE TABLE IF NOT EXISTS oauth_clients (
                    id TEXT PRIMARY KEY,
                    client_name TEXT,
                    redirect_uris TEXT NOT NULL,
                    grant_types TEXT,
                    scope TEXT,
                    token_endpoint_auth_method TEXT NOT NULL DEFAULT 'none',
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )"#,
                )
                .execute(pool)
                .await?;
                query(
                    r#"
                CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
                    code_hash TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    scopes TEXT NOT NULL,
                    dataset_ids TEXT,
                    redirect_uri TEXT NOT NULL,
                    code_challenge TEXT NOT NULL,
                    resource TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    expires_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
                )"#,
                )
                .execute(pool)
                .await?;
                query(
                    r#"
                CREATE TABLE IF NOT EXISTS oauth_access_tokens (
                    id TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL UNIQUE,
                    client_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    scopes TEXT NOT NULL,
                    dataset_ids TEXT,
                    resource TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    expires_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
                )"#,
                )
                .execute(pool)
                .await?;
                query(
                    r#"
                CREATE TABLE IF NOT EXISTS oauth_refresh_tokens (
                    id TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL UNIQUE,
                    client_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    tenant_id TEXT NOT NULL,
                    scopes TEXT NOT NULL,
                    dataset_ids TEXT,
                    resource TEXT,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    expires_at TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
                )"#,
                )
                .execute(pool)
                .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_oauth_access_tokens_hash ON oauth_access_tokens(token_hash)",
                )
                .execute(pool)
                .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_oauth_refresh_tokens_hash ON oauth_refresh_tokens(token_hash)",
                )
                .execute(pool)
                .await?;
                // Idempotent guard for a table created before `dataset_ids`
                // existed (SQLite has no native `ADD COLUMN IF NOT EXISTS`).
                for table in [
                    "oauth_authorization_codes",
                    "oauth_access_tokens",
                    "oauth_refresh_tokens",
                ] {
                    ensure_sqlite_text_column(pool, table, "dataset_ids").await?;
                }
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
                    dataset_id TEXT,
                    scopes TEXT,
                    created_by_user_id TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    revoked_at TIMESTAMPTZ,
                    UNIQUE(tenant_id, name)
                )"#;
                query(create_api_keys).execute(pool).await?;
                query("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS dataset_id TEXT")
                    .execute(pool)
                    .await?;
                query("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS scopes TEXT")
                    .execute(pool)
                    .await?;
                query("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS created_by_user_id TEXT")
                    .execute(pool)
                    .await?;
                query("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS dataset_ids TEXT")
                    .execute(pool)
                    .await?;

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

                // Value sketches (change: query-field-discovery): see the
                // SQLite branch.
                let create_attribute_value_stats = r#"
                CREATE TABLE IF NOT EXISTS attribute_value_stats (
                    tenant_id TEXT NOT NULL,
                    dataset_id TEXT NOT NULL,
                    signal TEXT NOT NULL,
                    attr_key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    count BIGINT NOT NULL DEFAULT 0,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (tenant_id, dataset_id, signal, attr_key, value)
                )"#;
                query(create_attribute_value_stats).execute(pool).await?;

                // Tenant custom schema registries (change: schema-registry).
                query(
                    r#"
                CREATE TABLE IF NOT EXISTS schema_registries (
                    tenant_id TEXT NOT NULL,
                    namespace TEXT NOT NULL,
                    version TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'custom',
                    schema_url TEXT,
                    description TEXT,
                    document TEXT NOT NULL,
                    resolved TEXT NOT NULL,
                    attribute_count BIGINT NOT NULL DEFAULT 0,
                    entity_count BIGINT NOT NULL DEFAULT 0,
                    metric_count BIGINT NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (tenant_id, namespace, version)
                )"#,
                )
                .execute(pool)
                .await?;

                // OAuth 2.1 authorization-server tables (change: mcp-oauth-dcr).
                query(
                    r#"
                CREATE TABLE IF NOT EXISTS oauth_clients (
                    id TEXT PRIMARY KEY,
                    client_name TEXT,
                    redirect_uris TEXT NOT NULL,
                    grant_types TEXT,
                    scope TEXT,
                    token_endpoint_auth_method TEXT NOT NULL DEFAULT 'none',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )"#,
                )
                .execute(pool)
                .await?;
                query(
                    r#"
                CREATE TABLE IF NOT EXISTS oauth_authorization_codes (
                    code_hash TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                    scopes TEXT NOT NULL,
                    dataset_ids TEXT,
                    redirect_uri TEXT NOT NULL,
                    code_challenge TEXT NOT NULL,
                    resource TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL
                )"#,
                )
                .execute(pool)
                .await?;
                query(
                    r#"
                CREATE TABLE IF NOT EXISTS oauth_access_tokens (
                    id TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL UNIQUE,
                    client_id TEXT NOT NULL,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                    scopes TEXT NOT NULL,
                    dataset_ids TEXT,
                    resource TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL
                )"#,
                )
                .execute(pool)
                .await?;
                query(
                    r#"
                CREATE TABLE IF NOT EXISTS oauth_refresh_tokens (
                    id TEXT PRIMARY KEY,
                    token_hash TEXT NOT NULL UNIQUE,
                    client_id TEXT NOT NULL,
                    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
                    scopes TEXT NOT NULL,
                    dataset_ids TEXT,
                    resource TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL
                )"#,
                )
                .execute(pool)
                .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_oauth_access_tokens_hash ON oauth_access_tokens(token_hash)",
                )
                .execute(pool)
                .await?;
                query(
                    "CREATE INDEX IF NOT EXISTS idx_oauth_refresh_tokens_hash ON oauth_refresh_tokens(token_hash)",
                )
                .execute(pool)
                .await?;
                for table in [
                    "oauth_authorization_codes",
                    "oauth_access_tokens",
                    "oauth_refresh_tokens",
                ] {
                    query(&format!(
                        "ALTER TABLE {table} ADD COLUMN IF NOT EXISTS dataset_ids TEXT"
                    ))
                    .execute(pool)
                    .await?;
                }
            }
        }

        // D2's one-time-per-row backfill: safe to run on every boot, since a
        // row already synced by new code never matches its WHERE clause.
        self.backfill_api_key_dataset_ids().await?;

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
        self.register_ingester_inner(id, address, service_type, capabilities)
            .instrument(self.db_span("INSERT"))
            .await
    }

    async fn register_ingester_inner(
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
                let service_type_str = service_type.catalog_name();
                let capabilities_str = capabilities
                    .iter()
                    .map(|c| c.catalog_name())
                    .collect::<Vec<_>>()
                    .join(",");

                // Try insert first, then update if it already exists
                let insert_stmt = r#"
                INSERT INTO ingesters (id, address, last_seen, service_type, capabilities)
                VALUES (?, ?, ?, ?, ?)
                "#;
                Self::record_query_text(insert_stmt);

                let result = query(insert_stmt)
                    .bind(&id_str)
                    .bind(address)
                    .bind(&now)
                    .bind(service_type_str)
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
                        .bind(service_type_str)
                        .bind(&capabilities_str)
                        .bind(&id_str)
                        .execute(pool)
                        .await?;
                }
            }
            Catalog::Postgres(pool) => {
                let service_type_str = service_type.catalog_name();
                let capabilities_str = capabilities
                    .iter()
                    .map(|c| c.catalog_name())
                    .collect::<Vec<_>>()
                    .join(",");

                // PostgreSQL with UPSERT
                let stmt = r#"
                INSERT INTO ingesters (id, address, last_seen, service_type, capabilities)
                VALUES ($1, $2, NOW(), $3, $4)
                ON CONFLICT (id) DO UPDATE SET address = $2, last_seen = NOW(), service_type = $3, capabilities = $4
                "#;
                Self::record_query_text(stmt);
                query(stmt)
                    .bind(id)
                    .bind(address)
                    .bind(service_type_str)
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
        self.heartbeat_inner(id)
            .instrument(self.db_span("UPDATE"))
            .await
    }

    async fn heartbeat_inner(&self, id: Uuid) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();
                let id_str = id.to_string();
                let stmt = r#"
                UPDATE ingesters SET last_seen = ?
                WHERE id = ?
                "#;
                Self::record_query_text(stmt);
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
                Self::record_query_text(stmt);
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
        self.list_ingesters_inner()
            .instrument(self.db_span("SELECT"))
            .await
    }

    async fn list_ingesters_inner(&self) -> Result<Vec<Ingester>, sqlx::Error> {
        Self::record_query_text(
            "SELECT id, address, last_seen, service_type, capabilities FROM ingesters",
        );
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
                    && db_err.is_unique_violation()
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
                    && db_err.is_unique_violation()
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
        self.deregister_ingester_inner(id)
            .instrument(self.db_span("DELETE"))
            .await
    }

    async fn deregister_ingester_inner(&self, id: Uuid) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let id_str = id.to_string();
                let stmt = r#"
                DELETE FROM ingesters
                WHERE id = ?
                "#;
                Self::record_query_text(stmt);
                query(stmt).bind(&id_str).execute(pool).await?;
            }
            Catalog::Postgres(pool) => {
                let stmt = r#"
                DELETE FROM ingesters
                WHERE id = $1
                "#;
                Self::record_query_text(stmt);
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
    ServiceType::from_catalog_name(s).unwrap_or_else(|| {
        // Only reachable on version skew: a peer running a newer binary
        // registered a type this one has no name for. Warn rather than
        // mislabel it silently.
        tracing::warn!("Unknown service type {s:?} in catalog, treating as writer");
        ServiceType::Writer
    })
}

/// Helper function to parse capabilities from comma-separated string
fn parse_capabilities(s: &str) -> Vec<ServiceCapability> {
    if s.is_empty() {
        return vec![];
    }

    s.split(',')
        .filter_map(|cap| {
            let cap = cap.trim();
            ServiceCapability::from_catalog_name(cap).or_else(|| {
                // A dropped capability makes the service invisible to
                // capability-based routing, so make the loss audible.
                tracing::warn!("Unknown service capability {cap:?} in catalog, ignoring");
                None
            })
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
    /// Legacy single-dataset column (D2): kept as a dual-write projection
    /// of `dataset_ids` (`ids[0]` when it has exactly one element, `NULL`
    /// otherwise) so an old-code node's reads stay correct.
    pub dataset_id: Option<String>,
    /// Dataset-set restriction (D1/D2): `None` is unrestricted, `Some` is
    /// the exact set. Dual-read: authoritative from the `dataset_ids`
    /// column when present, else derived from the legacy `dataset_id`
    /// column.
    pub dataset_ids: Option<Vec<String>>,
    pub scopes: Option<Vec<String>>,
    pub created_by_user_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

/// Authentication attributes for an active API key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiKeyAuthRecord {
    pub tenant_id: String,
    pub name: Option<String>,
    /// Legacy single-dataset column; see [`ApiKeyRecord::dataset_id`].
    pub dataset_id: Option<String>,
    /// Dataset-set restriction; see [`ApiKeyRecord::dataset_ids`].
    pub dataset_ids: Option<Vec<String>>,
    /// Explicit scopes, or `None` for a legacy unrestricted key.
    pub scopes: Option<Vec<String>>,
}

/// Tri-state update to a live API key's dataset-set restriction (D1a/D2b).
///
/// The predecessor `dataset_id: Option<&str>` parameter this replaces could
/// only express "leave unchanged" (`None`) or "set" (`Some`) through a
/// `COALESCE`-based `UPDATE` — a `NULL` can never win a `COALESCE`, so
/// there was no way to express "clear an existing restriction."
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetRestrictionUpdate {
    /// Leave the existing restriction (or lack of one) untouched.
    Keep,
    /// Clear any restriction back to unrestricted: both `dataset_ids` and
    /// the legacy `dataset_id` become `NULL`.
    Clear,
    /// Replace the restriction with exactly this set. Validated the same
    /// way as the create path (`upsert_scoped_api_key`): an empty or
    /// duplicate-containing set is rejected.
    Set(Vec<String>),
}

/// Reject an empty or duplicate-containing dataset-id set (D1a). Shared by
/// every path that writes a dataset-id set — the API-key create path
/// (`upsert_scoped_api_key`), the API-key update path
/// (`update_api_key_scopes`'s `Set` variant), and the OAuth grant paths —
/// so none of them can drift into checking a different rule.
fn validate_dataset_id_set(ids: &[String]) -> Result<(), sqlx::Error> {
    if ids.is_empty() {
        return Err(sqlx::Error::Protocol(
            "dataset_ids must not be empty; omit the field (or send null) for an unrestricted key"
                .to_string(),
        ));
    }
    let mut seen = std::collections::HashSet::with_capacity(ids.len());
    for id in ids {
        if !seen.insert(id.as_str()) {
            return Err(sqlx::Error::Protocol(format!(
                "dataset_ids contains duplicate dataset '{id}'"
            )));
        }
    }
    Ok(())
}

/// Validate and JSON-encode a dataset-id set for the `dataset_ids` column
/// (D1).
fn encode_dataset_ids_json(ids: &[String]) -> Result<String, sqlx::Error> {
    validate_dataset_id_set(ids)?;
    serde_json::to_string(ids)
        .map_err(|e| sqlx::Error::Protocol(format!("failed to serialize dataset_ids: {e}")))
}

/// Project a validated, non-empty dataset-id set onto the D2 storage
/// representation: the JSON-array-in-TEXT `dataset_ids` column (always) and
/// the legacy single-value `dataset_id` column (only when the set has
/// exactly one element, `NULL` otherwise — an empty set is unreachable,
/// [`validate_dataset_id_set`] already rejected it).
fn project_dataset_id_set(ids: &[String]) -> Result<(String, Option<String>), sqlx::Error> {
    let json = encode_dataset_ids_json(ids)?;
    let legacy = match ids {
        [single] => Some(single.clone()),
        _ => None,
    };
    Ok((json, legacy))
}

/// Dual-read (D2): the `dataset_ids` column is authoritative when non-NULL;
/// otherwise the set is derived from the legacy `dataset_id` column exactly
/// as before this column existed.
fn decode_dataset_id_set(
    dataset_ids: Option<String>,
    legacy_dataset_id: Option<String>,
) -> Result<Option<Vec<String>>, sqlx::Error> {
    match dataset_ids {
        Some(json) => decode_json_vec(json).map(Some),
        None => Ok(legacy_dataset_id.map(|id| vec![id])),
    }
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
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, utoipa::ToSchema,
)]
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

/// A dynamically-registered OAuth client (RFC 7591). SignalDB registers only
/// public clients (`token_endpoint_auth_method = "none"`) that authenticate
/// with PKCE; no client secret is stored.
#[derive(Debug, Clone)]
pub struct OAuthClientRecord {
    pub id: String,
    pub client_name: Option<String>,
    pub redirect_uris: Vec<String>,
    pub grant_types: Option<Vec<String>>,
    pub scope: Option<String>,
    pub token_endpoint_auth_method: String,
    pub created_at: DateTime<Utc>,
}

/// A redeemed-once authorization code's grant, returned by
/// [`Catalog::consume_authorization_code`]. The raw code is never stored — the
/// row is keyed by the code's hash.
#[derive(Debug, Clone)]
pub struct OAuthAuthorizationCode {
    pub client_id: String,
    pub user_id: String,
    pub tenant_id: String,
    pub scopes: Vec<String>,
    /// Dataset-set restriction chosen at consent (D1/D6). `None` is
    /// unrestricted.
    pub dataset_ids: Option<Vec<String>>,
    pub redirect_uri: String,
    pub code_challenge: String,
    pub resource: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// An opaque OAuth token grant (access or refresh), stored and looked up by
/// hash. Carries the tenant, scopes, and audience the token was minted for.
#[derive(Debug, Clone)]
pub struct OAuthTokenRecord {
    pub id: String,
    pub client_id: String,
    pub user_id: String,
    pub tenant_id: String,
    pub scopes: Vec<String>,
    /// Dataset-set restriction (D1/D6). `None` is unrestricted. There is no
    /// legacy column here — OAuth never had dataset restriction before this
    /// column existed, unlike `api_keys`' `dataset_id`.
    pub dataset_ids: Option<Vec<String>>,
    pub resource: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
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
    /// When the analyzer last wrote this row. Discovery reports it so a
    /// client can see how stale a statistics-derived answer is.
    pub updated_at: String,
}

/// One value of an attribute key, with how often the analyzer saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeValueStat {
    pub value: String,
    pub count: i64,
    pub updated_at: String,
}

impl AttributeStatsRecord {
    /// The fraction of scanned rows carrying this key, or `None` when the
    /// analyzer has seen no rows — the one number every consumer of these
    /// statistics derives, defined once here so they cannot disagree about
    /// what a zero-row observation means.
    pub fn coverage(&self) -> Option<f64> {
        (self.total_rows > 0).then(|| self.present_rows as f64 / self.total_rows as f64)
    }
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
                   promote_streak, CAST(updated_at AS TEXT) AS updated_at
            FROM attribute_stats
            WHERE tenant_id = ? AND dataset_id = ? AND signal = ?
            ORDER BY attr_key
        "#;
        let sql_pg = r#"
            SELECT tenant_id, dataset_id, signal, attr_key, present_rows,
                   total_rows, distinct_estimate, capped, query_hits,
                   promote_streak, CAST(updated_at AS TEXT) AS updated_at
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
                updated_at: row.get("updated_at"),
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

    /// Replace one key's value sketch with the analyzer's latest observation.
    ///
    /// The analyzer sees the whole rewritten partition, so the new sketch
    /// supersedes the old one wholesale; replacing rather than merging keeps a
    /// value that has stopped occurring from lingering as a suggestion
    /// forever. Passing an empty `values` clears the sketch, which is how a
    /// key that grew past the cardinality cap stops being suggested.
    pub async fn replace_attribute_value_stats(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
        attr_key: &str,
        values: &[(String, i64)],
    ) -> Result<(), sqlx::Error> {
        let delete_sqlite = "DELETE FROM attribute_value_stats \
             WHERE tenant_id = ? AND dataset_id = ? AND signal = ? AND attr_key = ?";
        let delete_pg = "DELETE FROM attribute_value_stats \
             WHERE tenant_id = $1 AND dataset_id = $2 AND signal = $3 AND attr_key = $4";
        let insert_sqlite = r#"
            INSERT INTO attribute_value_stats
                (tenant_id, dataset_id, signal, attr_key, value, count, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        "#;
        let insert_pg = r#"
            INSERT INTO attribute_value_stats
                (tenant_id, dataset_id, signal, attr_key, value, count, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, NOW())
        "#;
        match self {
            Catalog::Sqlite(pool) => {
                let mut tx = pool.begin().await?;
                query(delete_sqlite)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(signal)
                    .bind(attr_key)
                    .execute(&mut *tx)
                    .await?;
                for (value, count) in values {
                    query(insert_sqlite)
                        .bind(tenant_id)
                        .bind(dataset_id)
                        .bind(signal)
                        .bind(attr_key)
                        .bind(value)
                        .bind(count)
                        .execute(&mut *tx)
                        .await?;
                }
                tx.commit().await?;
            }
            Catalog::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                query(delete_pg)
                    .bind(tenant_id)
                    .bind(dataset_id)
                    .bind(signal)
                    .bind(attr_key)
                    .execute(&mut *tx)
                    .await?;
                for (value, count) in values {
                    query(insert_pg)
                        .bind(tenant_id)
                        .bind(dataset_id)
                        .bind(signal)
                        .bind(attr_key)
                        .bind(value)
                        .bind(count)
                        .execute(&mut *tx)
                        .await?;
                }
                tx.commit().await?;
            }
        }
        Ok(())
    }

    /// One key's value sketch, most frequent first. Empty when the analyzer
    /// keeps no sketch for the key — which discovery reports as "nothing
    /// covers this field" rather than as "this field has no values".
    pub async fn get_attribute_value_stats(
        &self,
        tenant_id: &str,
        dataset_id: &str,
        signal: &str,
        attr_key: &str,
        limit: i64,
    ) -> Result<Vec<AttributeValueStat>, sqlx::Error> {
        let sql_sqlite = r#"
            SELECT value, count, CAST(updated_at AS TEXT) AS updated_at
            FROM attribute_value_stats
            WHERE tenant_id = ? AND dataset_id = ? AND signal = ? AND attr_key = ?
            ORDER BY count DESC, value ASC
            LIMIT ?
        "#;
        let sql_pg = r#"
            SELECT value, count, CAST(updated_at AS TEXT) AS updated_at
            FROM attribute_value_stats
            WHERE tenant_id = $1 AND dataset_id = $2 AND signal = $3 AND attr_key = $4
            ORDER BY count DESC, value ASC
            LIMIT $5
        "#;
        fn stat<R: Row>(row: &R) -> AttributeValueStat
        where
            for<'a> &'a str: sqlx::ColumnIndex<R>,
            for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
            for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
        {
            AttributeValueStat {
                value: row.get("value"),
                count: row.get("count"),
                updated_at: row.get("updated_at"),
            }
        }
        match self {
            Catalog::Sqlite(pool) => Ok(query(sql_sqlite)
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(signal)
                .bind(attr_key)
                .bind(limit)
                .fetch_all(pool)
                .await?
                .iter()
                .map(stat)
                .collect()),
            Catalog::Postgres(pool) => Ok(query(sql_pg)
                .bind(tenant_id)
                .bind(dataset_id)
                .bind(signal)
                .bind(attr_key)
                .bind(limit)
                .fetch_all(pool)
                .await?
                .iter()
                .map(stat)
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
        self.upsert_scoped_api_key(tenant_id, key_hash, name, None, None, None)
            .await
    }

    /// Create or return an API key with optional dataset-set and scope
    /// restrictions.
    ///
    /// `scopes = None` preserves legacy unrestricted-key behavior. New
    /// user-created keys should always pass an explicit, non-empty scope list.
    /// `dataset_ids = Some(&[])` or a set containing a duplicate name is
    /// rejected (D1a) — omit the argument (or pass `None`) for an
    /// unrestricted key.
    pub async fn upsert_scoped_api_key(
        &self,
        tenant_id: &str,
        key_hash: &str,
        name: Option<&str>,
        dataset_ids: Option<&[String]>,
        scopes: Option<&[String]>,
        created_by_user_id: Option<&str>,
    ) -> Result<String, sqlx::Error> {
        let key_id = Uuid::new_v4().to_string();
        let scopes_json = scopes
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| {
                sqlx::Error::Protocol(format!("failed to serialize API key scopes: {error}"))
            })?;
        let (dataset_ids_json, dataset_id_legacy) = match dataset_ids {
            Some(ids) => {
                let (json, legacy) = project_dataset_id_set(ids)?;
                (Some(json), legacy)
            }
            None => (None, None),
        };

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
                INSERT INTO api_keys (
                    id, key_hash, tenant_id, name, dataset_id, dataset_ids, scopes,
                    created_by_user_id, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                "#;
                query(stmt)
                    .bind(&key_id)
                    .bind(key_hash)
                    .bind(tenant_id)
                    .bind(name)
                    .bind(&dataset_id_legacy)
                    .bind(&dataset_ids_json)
                    .bind(&scopes_json)
                    .bind(created_by_user_id)
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
                INSERT INTO api_keys (
                    id, key_hash, tenant_id, name, dataset_id, dataset_ids, scopes,
                    created_by_user_id
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#;
                query(stmt)
                    .bind(&key_id)
                    .bind(key_hash)
                    .bind(tenant_id)
                    .bind(name)
                    .bind(&dataset_id_legacy)
                    .bind(&dataset_ids_json)
                    .bind(&scopes_json)
                    .bind(created_by_user_id)
                    .execute(pool)
                    .await?;
            }
        }

        Ok(key_id)
    }

    /// Validate an API key and return its authorization attributes.
    pub async fn validate_api_key(
        &self,
        key_hash: &str,
    ) -> Result<Option<ApiKeyAuthRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let row = query("SELECT tenant_id, name, dataset_id, dataset_ids, scopes FROM api_keys WHERE key_hash = ? AND revoked_at IS NULL")
                    .bind(key_hash)
                    .fetch_optional(pool)
                    .await?;

                row.map(|r| {
                    let dataset_id: Option<String> = r.get("dataset_id");
                    let dataset_ids =
                        decode_dataset_id_set(r.get("dataset_ids"), dataset_id.clone())?;
                    Ok(ApiKeyAuthRecord {
                        tenant_id: r.get("tenant_id"),
                        name: r.get("name"),
                        dataset_id,
                        dataset_ids,
                        scopes: decode_json_vec_opt(r.get("scopes"))?,
                    })
                })
                .transpose()
            }
            Catalog::Postgres(pool) => {
                let row = query("SELECT tenant_id, name, dataset_id, dataset_ids, scopes FROM api_keys WHERE key_hash = $1 AND revoked_at IS NULL")
                    .bind(key_hash)
                    .fetch_optional(pool)
                    .await?;

                row.map(|r| {
                    let dataset_id: Option<String> = r.get("dataset_id");
                    let dataset_ids =
                        decode_dataset_id_set(r.get("dataset_ids"), dataset_id.clone())?;
                    Ok(ApiKeyAuthRecord {
                        tenant_id: r.get("tenant_id"),
                        name: r.get("name"),
                        dataset_id,
                        dataset_ids,
                        scopes: decode_json_vec_opt(r.get("scopes"))?,
                    })
                })
                .transpose()
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

    /// Update the scopes and/or dataset-set restriction of a live API key.
    ///
    /// `scopes = None` leaves scopes untouched. `dataset_update` is a
    /// tri-state (D2b): [`DatasetRestrictionUpdate::Keep`] leaves the
    /// restriction untouched, [`DatasetRestrictionUpdate::Clear`] nulls both
    /// the `dataset_ids` and legacy `dataset_id` columns, and
    /// [`DatasetRestrictionUpdate::Set`] replaces the restriction (rejecting
    /// an empty or duplicate-containing set, D1a). Returns `false` when the
    /// key does not exist or is revoked (revoked keys are immutable).
    /// Because the tenant context is rebuilt from the key row on every
    /// request, the change applies to the next request made with the key.
    pub async fn update_api_key_scopes(
        &self,
        key_id: &str,
        scopes: Option<&[String]>,
        dataset_update: DatasetRestrictionUpdate,
    ) -> Result<bool, sqlx::Error> {
        let scopes_json = scopes
            .map(serde_json::to_string)
            .transpose()
            .map_err(|error| {
                sqlx::Error::Protocol(format!("failed to serialize API key scopes: {error}"))
            })?;
        let rows_affected = match (self, dataset_update) {
            (Catalog::Sqlite(pool), DatasetRestrictionUpdate::Keep) => query(
                "UPDATE api_keys SET scopes = COALESCE(?, scopes) WHERE id = ? AND revoked_at IS NULL",
            )
            .bind(&scopes_json)
            .bind(key_id)
            .execute(pool)
            .await?
            .rows_affected(),
            (Catalog::Sqlite(pool), DatasetRestrictionUpdate::Clear) => query(
                "UPDATE api_keys SET scopes = COALESCE(?, scopes), dataset_ids = NULL, dataset_id = NULL \
                     WHERE id = ? AND revoked_at IS NULL",
            )
            .bind(&scopes_json)
            .bind(key_id)
            .execute(pool)
            .await?
            .rows_affected(),
            (Catalog::Sqlite(pool), DatasetRestrictionUpdate::Set(ids)) => {
                let (dataset_ids_json, dataset_id_legacy) = project_dataset_id_set(&ids)?;
                query(
                    "UPDATE api_keys SET scopes = COALESCE(?, scopes), dataset_ids = ?, dataset_id = ? \
                         WHERE id = ? AND revoked_at IS NULL",
                )
                .bind(&scopes_json)
                .bind(&dataset_ids_json)
                .bind(&dataset_id_legacy)
                .bind(key_id)
                .execute(pool)
                .await?
                .rows_affected()
            }
            (Catalog::Postgres(pool), DatasetRestrictionUpdate::Keep) => query(
                "UPDATE api_keys SET scopes = COALESCE($1, scopes) WHERE id = $2 AND revoked_at IS NULL",
            )
            .bind(&scopes_json)
            .bind(key_id)
            .execute(pool)
            .await?
            .rows_affected(),
            (Catalog::Postgres(pool), DatasetRestrictionUpdate::Clear) => query(
                "UPDATE api_keys SET scopes = COALESCE($1, scopes), dataset_ids = NULL, dataset_id = NULL \
                     WHERE id = $2 AND revoked_at IS NULL",
            )
            .bind(&scopes_json)
            .bind(key_id)
            .execute(pool)
            .await?
            .rows_affected(),
            (Catalog::Postgres(pool), DatasetRestrictionUpdate::Set(ids)) => {
                let (dataset_ids_json, dataset_id_legacy) = project_dataset_id_set(&ids)?;
                query(
                    "UPDATE api_keys SET scopes = COALESCE($1, scopes), dataset_ids = $2, dataset_id = $3 \
                         WHERE id = $4 AND revoked_at IS NULL",
                )
                .bind(&scopes_json)
                .bind(&dataset_ids_json)
                .bind(&dataset_id_legacy)
                .bind(key_id)
                .execute(pool)
                .await?
                .rows_affected()
            }
        };
        Ok(rows_affected > 0)
    }

    /// Rows whose legacy `dataset_id` predates the `dataset_ids` column and
    /// have not yet been synced onto it (D2's backfill `SELECT`).
    async fn pending_api_key_dataset_id_backfill(
        &self,
    ) -> Result<Vec<(String, String)>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let rows = query(
                    "SELECT id, dataset_id FROM api_keys WHERE dataset_id IS NOT NULL AND dataset_ids IS NULL",
                )
                .fetch_all(pool)
                .await?;
                Ok(rows
                    .iter()
                    .map(|r| (r.get("id"), r.get("dataset_id")))
                    .collect())
            }
            Catalog::Postgres(pool) => {
                let rows = query(
                    "SELECT id, dataset_id FROM api_keys WHERE dataset_id IS NOT NULL AND dataset_ids IS NULL",
                )
                .fetch_all(pool)
                .await?;
                Ok(rows
                    .iter()
                    .map(|r| (r.get("id"), r.get("dataset_id")))
                    .collect())
            }
        }
    }

    /// Compare-and-swap backfill of a single row (D2): writes `dataset_ids`
    /// derived from `dataset_id_at_read` only if the row's `dataset_id`
    /// still matches that value and `dataset_ids` is still NULL. A
    /// concurrent legacy write to `dataset_id` between the backfill's
    /// `SELECT` and this call makes it a no-op instead of persisting a
    /// `dataset_ids` value derived from data that's already stale — the row
    /// is picked up correctly by the next boot's backfill pass instead.
    /// Returns the number of rows affected (0 or 1).
    async fn apply_api_key_dataset_id_backfill(
        &self,
        id: &str,
        dataset_id_at_read: &str,
    ) -> Result<u64, sqlx::Error> {
        let dataset_ids_json = serde_json::to_string(&[dataset_id_at_read]).map_err(|e| {
            sqlx::Error::Protocol(format!("failed to serialize dataset_ids backfill: {e}"))
        })?;
        let rows_affected = match self {
            Catalog::Sqlite(pool) => query(
                "UPDATE api_keys SET dataset_ids = ? \
                     WHERE id = ? AND dataset_id = ? AND dataset_ids IS NULL",
            )
            .bind(&dataset_ids_json)
            .bind(id)
            .bind(dataset_id_at_read)
            .execute(pool)
            .await?
            .rows_affected(),
            Catalog::Postgres(pool) => query(
                "UPDATE api_keys SET dataset_ids = $1 \
                     WHERE id = $2 AND dataset_id = $3 AND dataset_ids IS NULL",
            )
            .bind(&dataset_ids_json)
            .bind(id)
            .bind(dataset_id_at_read)
            .execute(pool)
            .await?
            .rows_affected(),
        };
        Ok(rows_affected)
    }

    /// One-time-per-row backfill (D2), run at startup from `init()` and safe
    /// to call again on every boot: a row new code has already written is
    /// never selected again (its `dataset_ids` is no longer NULL).
    async fn backfill_api_key_dataset_ids(&self) -> Result<(), sqlx::Error> {
        for (id, dataset_id) in self.pending_api_key_dataset_id_backfill().await? {
            self.apply_api_key_dataset_id_backfill(&id, &dataset_id)
                .await?;
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

    /// Upsert a tenant and materialize its `default_dataset` in one
    /// transaction, so the two rows land together or not at all.
    ///
    /// Writing them separately can strand a tenant: if the dataset insert
    /// fails after the tenant row commits, the tenant exists with a
    /// `default_dataset` that has no row, which fails authentication closed
    /// (`resolve_database_tenant`). Creation rejects an existing id with 409,
    /// so a retry cannot repair it either — the tenant would stay broken
    /// until the next [`Catalog::backfill_default_datasets`] at boot.
    ///
    /// `default_dataset` of `None` writes only the tenant row. Repointing an
    /// existing tenant's default materializes the new dataset and leaves the
    /// previous one in place, since it may still hold data.
    pub async fn upsert_tenant_with_default_dataset(
        &self,
        tenant_id: &str,
        name: &str,
        default_dataset: Option<&str>,
        source: &str,
    ) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();
                let mut tx = pool.begin().await?;

                query(
                    r#"
                    INSERT INTO tenants (id, name, default_dataset, created_at, updated_at, source)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (id) DO UPDATE SET
                        name = excluded.name,
                        default_dataset = excluded.default_dataset,
                        updated_at = excluded.updated_at,
                        source = excluded.source
                    "#,
                )
                .bind(tenant_id)
                .bind(name)
                .bind(default_dataset)
                .bind(&now)
                .bind(&now)
                .bind(source)
                .execute(&mut *tx)
                .await?;

                if let Some(dataset_name) = default_dataset {
                    query(
                        r#"
                        INSERT INTO datasets (id, tenant_id, name, created_at)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT (tenant_id, name) DO NOTHING
                        "#,
                    )
                    .bind(Uuid::new_v4().to_string())
                    .bind(tenant_id)
                    .bind(dataset_name)
                    .bind(&now)
                    .execute(&mut *tx)
                    .await?;
                }

                tx.commit().await?;
            }
            Catalog::Postgres(pool) => {
                let mut tx = pool.begin().await?;

                query(
                    r#"
                    INSERT INTO tenants (id, name, default_dataset, source)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (id) DO UPDATE SET
                        name = EXCLUDED.name,
                        default_dataset = EXCLUDED.default_dataset,
                        updated_at = NOW(),
                        source = EXCLUDED.source
                    "#,
                )
                .bind(tenant_id)
                .bind(name)
                .bind(default_dataset)
                .bind(source)
                .execute(&mut *tx)
                .await?;

                if let Some(dataset_name) = default_dataset {
                    query(
                        r#"
                        INSERT INTO datasets (id, tenant_id, name)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (tenant_id, name) DO NOTHING
                        "#,
                    )
                    .bind(Uuid::new_v4().to_string())
                    .bind(tenant_id)
                    .bind(dataset_name)
                    .execute(&mut *tx)
                    .await?;
                }

                tx.commit().await?;
            }
        }

        Ok(())
    }

    /// Idempotently ensure a tenant has a dataset by this name, returning its
    /// id — the existing one when the row is already there.
    ///
    /// [`Catalog::create_dataset`] is a bare INSERT against a
    /// `UNIQUE(tenant_id, name)` table, so it errors on a second call and
    /// cannot be used on a convergence path. The admin API keeps that
    /// behavior, since a duplicate there is a client error worth reporting;
    /// the internal materialization and backfill paths use this instead.
    ///
    /// Insert-first rather than check-then-insert: the read-then-write form
    /// races between processes, which matters because boot sync runs in every
    /// router and monolith process at once.
    pub async fn ensure_dataset(
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
                ON CONFLICT (tenant_id, name) DO NOTHING
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
                ON CONFLICT (tenant_id, name) DO NOTHING
                "#;
                query(stmt)
                    .bind(&dataset_id)
                    .bind(tenant_id)
                    .bind(dataset_name)
                    .execute(pool)
                    .await?;
            }
        }

        // Read back rather than trusting `dataset_id`: on conflict the insert
        // was a no-op and the authoritative id belongs to the existing row.
        let existing = self
            .get_datasets(tenant_id)
            .await?
            .into_iter()
            .find(|d| d.name == dataset_name)
            .map(|d| d.id);
        Ok(existing.unwrap_or(dataset_id))
    }

    /// Materialize a `datasets` row for every tenant that names a
    /// `default_dataset` without having one, returning how many were written.
    ///
    /// A tenant in that state cannot authenticate — `resolve_database_tenant`
    /// requires a matching row and fails closed with `403 Dataset '<name>' not
    /// found` — and is invisible to every consumer that enumerates dataset
    /// rows. Tenant creation now materializes the row, so this exists to
    /// converge tenants created before that: it runs at boot alongside
    /// `sync_config_tenants` and is a no-op once converged.
    pub async fn backfill_default_datasets(&self) -> Result<usize, sqlx::Error> {
        let tenants = self.list_tenants().await?;
        let mut materialized = 0;
        for tenant in tenants {
            let Some(default_dataset) = tenant.default_dataset.as_deref() else {
                continue;
            };
            let datasets = self.get_datasets(&tenant.id).await?;
            if datasets.iter().any(|d| d.name == default_dataset) {
                continue;
            }
            self.ensure_dataset(&tenant.id, default_dataset).await?;
            materialized += 1;
            tracing::info!(
                tenant_id = %tenant.id,
                dataset = %default_dataset,
                "Materialized a missing default dataset row"
            );
        }
        Ok(materialized)
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
                    "SELECT id, tenant_id, name, dataset_id, dataset_ids, scopes, created_by_user_id, created_at, revoked_at FROM api_keys WHERE tenant_id = ? ORDER BY created_at DESC",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;

                rows.iter()
                    .map(|r| {
                        let revoked_at: Option<String> = r.get("revoked_at");
                        let dataset_id: Option<String> = r.get("dataset_id");
                        let dataset_ids =
                            decode_dataset_id_set(r.get("dataset_ids"), dataset_id.clone())?;
                        Ok(ApiKeyRecord {
                            id: r.get("id"),
                            tenant_id: r.get("tenant_id"),
                            name: r.get("name"),
                            dataset_id,
                            dataset_ids,
                            scopes: decode_json_vec_opt(r.get("scopes"))?,
                            created_by_user_id: r.get("created_by_user_id"),
                            created_at: parse_rfc3339(r.get("created_at"))?,
                            revoked_at: revoked_at.map(|s| parse_rfc3339(&s)).transpose()?,
                        })
                    })
                    .collect()
            }
            Catalog::Postgres(pool) => {
                let rows = query(
                    "SELECT id, tenant_id, name, dataset_id, dataset_ids, scopes, created_by_user_id, created_at, revoked_at FROM api_keys WHERE tenant_id = $1 ORDER BY created_at DESC",
                )
                .bind(tenant_id)
                .fetch_all(pool)
                .await?;

                rows.iter()
                    .map(|r| {
                        let dataset_id: Option<String> = r.get("dataset_id");
                        let dataset_ids =
                            decode_dataset_id_set(r.get("dataset_ids"), dataset_id.clone())?;
                        Ok(ApiKeyRecord {
                            id: r.get("id"),
                            tenant_id: r.get("tenant_id"),
                            name: r.get("name"),
                            dataset_id,
                            dataset_ids,
                            scopes: decode_json_vec_opt(r.get("scopes"))?,
                            created_by_user_id: r.get("created_by_user_id"),
                            created_at: r.get("created_at"),
                            revoked_at: r.get("revoked_at"),
                        })
                    })
                    .collect()
            }
        }
    }

    /// Get a single API key by ID
    pub async fn get_api_key(&self, key_id: &str) -> Result<Option<ApiKeyRecord>, sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let row = query(
                    "SELECT id, tenant_id, name, dataset_id, dataset_ids, scopes, created_by_user_id, created_at, revoked_at FROM api_keys WHERE id = ?",
                )
                .bind(key_id)
                .fetch_optional(pool)
                .await?;

                row.map(|r| {
                    let revoked_at: Option<String> = r.get("revoked_at");
                    let dataset_id: Option<String> = r.get("dataset_id");
                    let dataset_ids =
                        decode_dataset_id_set(r.get("dataset_ids"), dataset_id.clone())?;
                    Ok(ApiKeyRecord {
                        id: r.get("id"),
                        tenant_id: r.get("tenant_id"),
                        name: r.get("name"),
                        dataset_id,
                        dataset_ids,
                        scopes: decode_json_vec_opt(r.get("scopes"))?,
                        created_by_user_id: r.get("created_by_user_id"),
                        created_at: parse_rfc3339(r.get("created_at"))?,
                        revoked_at: revoked_at.map(|s| parse_rfc3339(&s)).transpose()?,
                    })
                })
                .transpose()
            }
            Catalog::Postgres(pool) => {
                let row = query(
                    "SELECT id, tenant_id, name, dataset_id, dataset_ids, scopes, created_by_user_id, created_at, revoked_at FROM api_keys WHERE id = $1",
                )
                .bind(key_id)
                .fetch_optional(pool)
                .await?;

                row.map(|r| {
                    let dataset_id: Option<String> = r.get("dataset_id");
                    let dataset_ids =
                        decode_dataset_id_set(r.get("dataset_ids"), dataset_id.clone())?;
                    Ok(ApiKeyRecord {
                        id: r.get("id"),
                        tenant_id: r.get("tenant_id"),
                        name: r.get("name"),
                        dataset_id,
                        dataset_ids,
                        scopes: decode_json_vec_opt(r.get("scopes"))?,
                        created_by_user_id: r.get("created_by_user_id"),
                        created_at: r.get("created_at"),
                        revoked_at: r.get("revoked_at"),
                    })
                })
                .transpose()
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

            // A tenant may declare `default_dataset` with no matching
            // `[[auth.tenants.datasets]]` block; it still needs a row, or it
            // is invisible to everything that enumerates dataset rows.
            for dataset in tenant
                .datasets
                .iter()
                .map(|d| d.id.as_str())
                .chain(tenant.default_dataset.as_deref())
            {
                self.ensure_dataset(&tenant.id, dataset).await?;
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

    // ----- OAuth 2.1 authorization-server storage (change: mcp-oauth-dcr) -----

    /// Persist a dynamically-registered OAuth client (RFC 7591) and return the
    /// stored record. `client_id` is caller-supplied (a fresh UUID).
    pub async fn register_oauth_client(
        &self,
        client_id: &str,
        client_name: Option<&str>,
        redirect_uris: &[String],
        grant_types: Option<&[String]>,
        scope: Option<&str>,
        token_endpoint_auth_method: &str,
    ) -> Result<OAuthClientRecord, sqlx::Error> {
        let now = Utc::now();
        let redirect_json = serde_json::to_string(redirect_uris).map_err(|e| {
            sqlx::Error::Protocol(format!("failed to serialize redirect_uris: {e}"))
        })?;
        let grant_json = grant_types
            .map(serde_json::to_string)
            .transpose()
            .map_err(|e| sqlx::Error::Protocol(format!("failed to serialize grant_types: {e}")))?;
        match self {
            Catalog::Sqlite(pool) => {
                query(
                    "INSERT INTO oauth_clients (id, client_name, redirect_uris, grant_types, scope, token_endpoint_auth_method, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(client_id)
                .bind(client_name)
                .bind(&redirect_json)
                .bind(&grant_json)
                .bind(scope)
                .bind(token_endpoint_auth_method)
                .bind(now.to_rfc3339())
                .execute(pool)
                .await?;
            }
            Catalog::Postgres(pool) => {
                query(
                    "INSERT INTO oauth_clients (id, client_name, redirect_uris, grant_types, scope, token_endpoint_auth_method, created_at) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                )
                .bind(client_id)
                .bind(client_name)
                .bind(&redirect_json)
                .bind(&grant_json)
                .bind(scope)
                .bind(token_endpoint_auth_method)
                .bind(now)
                .execute(pool)
                .await?;
            }
        }
        Ok(OAuthClientRecord {
            id: client_id.to_string(),
            client_name: client_name.map(str::to_owned),
            redirect_uris: redirect_uris.to_vec(),
            grant_types: grant_types.map(<[String]>::to_vec),
            scope: scope.map(str::to_owned),
            token_endpoint_auth_method: token_endpoint_auth_method.to_string(),
            created_at: now,
        })
    }

    /// Look up a registered OAuth client by `client_id`.
    pub async fn get_oauth_client(
        &self,
        client_id: &str,
    ) -> Result<Option<OAuthClientRecord>, sqlx::Error> {
        let sql = "SELECT id, client_name, redirect_uris, grant_types, scope, token_endpoint_auth_method, created_at FROM oauth_clients WHERE id = ";
        match self {
            Catalog::Sqlite(pool) => {
                let row = query(&format!("{sql}?"))
                    .bind(client_id)
                    .fetch_optional(pool)
                    .await?;
                row.map(|r| {
                    Ok::<_, sqlx::Error>(OAuthClientRecord {
                        id: r.get("id"),
                        client_name: r.get("client_name"),
                        redirect_uris: decode_json_vec(r.get("redirect_uris"))?,
                        grant_types: decode_json_vec_opt(r.get("grant_types"))?,
                        scope: r.get("scope"),
                        token_endpoint_auth_method: r.get("token_endpoint_auth_method"),
                        created_at: parse_rfc3339(r.get("created_at"))?,
                    })
                })
                .transpose()
            }
            Catalog::Postgres(pool) => {
                let row = query(&format!("{sql}$1"))
                    .bind(client_id)
                    .fetch_optional(pool)
                    .await?;
                row.map(|r| {
                    Ok::<_, sqlx::Error>(OAuthClientRecord {
                        id: r.get("id"),
                        client_name: r.get("client_name"),
                        redirect_uris: decode_json_vec(r.get("redirect_uris"))?,
                        grant_types: decode_json_vec_opt(r.get("grant_types"))?,
                        scope: r.get("scope"),
                        token_endpoint_auth_method: r.get("token_endpoint_auth_method"),
                        created_at: r.get("created_at"),
                    })
                })
                .transpose()
            }
        }
    }

    /// Store a single-use authorization code, keyed by its hash.
    ///
    /// `dataset_ids = Some(&[])` or a set containing a duplicate name is
    /// rejected (D1a), same as an API key's.
    #[allow(clippy::too_many_arguments)]
    pub async fn create_authorization_code(
        &self,
        code_hash: &str,
        client_id: &str,
        user_id: &str,
        tenant_id: &str,
        scopes: &[String],
        dataset_ids: Option<&[String]>,
        redirect_uri: &str,
        code_challenge: &str,
        resource: Option<&str>,
        expires_at: DateTime<Utc>,
    ) -> Result<(), sqlx::Error> {
        let scopes_json = serde_json::to_string(scopes)
            .map_err(|e| sqlx::Error::Protocol(format!("failed to serialize scopes: {e}")))?;
        let dataset_ids_json = dataset_ids.map(encode_dataset_ids_json).transpose()?;
        let now = Utc::now();
        match self {
            Catalog::Sqlite(pool) => {
                query(
                    "INSERT INTO oauth_authorization_codes (code_hash, client_id, user_id, tenant_id, scopes, dataset_ids, redirect_uri, code_challenge, resource, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(code_hash)
                .bind(client_id)
                .bind(user_id)
                .bind(tenant_id)
                .bind(&scopes_json)
                .bind(&dataset_ids_json)
                .bind(redirect_uri)
                .bind(code_challenge)
                .bind(resource)
                .bind(now.to_rfc3339())
                .bind(expires_at.to_rfc3339())
                .execute(pool)
                .await?;
            }
            Catalog::Postgres(pool) => {
                query(
                    "INSERT INTO oauth_authorization_codes (code_hash, client_id, user_id, tenant_id, scopes, dataset_ids, redirect_uri, code_challenge, resource, created_at, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
                )
                .bind(code_hash)
                .bind(client_id)
                .bind(user_id)
                .bind(tenant_id)
                .bind(&scopes_json)
                .bind(&dataset_ids_json)
                .bind(redirect_uri)
                .bind(code_challenge)
                .bind(resource)
                .bind(now)
                .bind(expires_at)
                .execute(pool)
                .await?;
            }
        }
        Ok(())
    }

    /// Atomically redeem an authorization code: delete its row and return the
    /// grant. Single-use — a second call with the same hash returns `None`
    /// because the row is already gone. Returns `None` for an unknown code or
    /// one whose stored expiry has passed (the row is still removed).
    pub async fn consume_authorization_code(
        &self,
        code_hash: &str,
    ) -> Result<Option<OAuthAuthorizationCode>, sqlx::Error> {
        let cols = "client_id, user_id, tenant_id, scopes, dataset_ids, redirect_uri, code_challenge, resource, created_at, expires_at";
        let record = match self {
            Catalog::Sqlite(pool) => {
                let row = query(&format!(
                    "DELETE FROM oauth_authorization_codes WHERE code_hash = ? RETURNING {cols}"
                ))
                .bind(code_hash)
                .fetch_optional(pool)
                .await?;
                match row {
                    None => return Ok(None),
                    Some(r) => OAuthAuthorizationCode {
                        client_id: r.get("client_id"),
                        user_id: r.get("user_id"),
                        tenant_id: r.get("tenant_id"),
                        scopes: decode_json_vec(r.get("scopes"))?,
                        dataset_ids: decode_json_vec_opt(r.get("dataset_ids"))?,
                        redirect_uri: r.get("redirect_uri"),
                        code_challenge: r.get("code_challenge"),
                        resource: r.get("resource"),
                        created_at: parse_rfc3339(r.get("created_at"))?,
                        expires_at: parse_rfc3339(r.get("expires_at"))?,
                    },
                }
            }
            Catalog::Postgres(pool) => {
                let row = query(&format!(
                    "DELETE FROM oauth_authorization_codes WHERE code_hash = $1 RETURNING {cols}"
                ))
                .bind(code_hash)
                .fetch_optional(pool)
                .await?;
                match row {
                    None => return Ok(None),
                    Some(r) => OAuthAuthorizationCode {
                        client_id: r.get("client_id"),
                        user_id: r.get("user_id"),
                        tenant_id: r.get("tenant_id"),
                        scopes: decode_json_vec(r.get("scopes"))?,
                        dataset_ids: decode_json_vec_opt(r.get("dataset_ids"))?,
                        redirect_uri: r.get("redirect_uri"),
                        code_challenge: r.get("code_challenge"),
                        resource: r.get("resource"),
                        created_at: r.get("created_at"),
                        expires_at: r.get("expires_at"),
                    },
                }
            }
        };
        if record.expires_at <= Utc::now() {
            return Ok(None);
        }
        Ok(Some(record))
    }

    /// Store an opaque access token, keyed by its hash, and return the grant.
    ///
    /// `dataset_ids = Some(&[])` or a set containing a duplicate name is
    /// rejected (D1a).
    #[allow(clippy::too_many_arguments)]
    pub async fn create_access_token(
        &self,
        token_hash: &str,
        client_id: &str,
        user_id: &str,
        tenant_id: &str,
        scopes: &[String],
        dataset_ids: Option<&[String]>,
        resource: Option<&str>,
        expires_at: DateTime<Utc>,
    ) -> Result<OAuthTokenRecord, sqlx::Error> {
        self.insert_oauth_token(
            "oauth_access_tokens",
            token_hash,
            client_id,
            user_id,
            tenant_id,
            scopes,
            dataset_ids,
            resource,
            expires_at,
        )
        .await
    }

    /// Store an opaque refresh token, keyed by its hash, and return the grant.
    ///
    /// `dataset_ids = Some(&[])` or a set containing a duplicate name is
    /// rejected (D1a).
    #[allow(clippy::too_many_arguments)]
    pub async fn create_refresh_token(
        &self,
        token_hash: &str,
        client_id: &str,
        user_id: &str,
        tenant_id: &str,
        scopes: &[String],
        dataset_ids: Option<&[String]>,
        resource: Option<&str>,
        expires_at: DateTime<Utc>,
    ) -> Result<OAuthTokenRecord, sqlx::Error> {
        self.insert_oauth_token(
            "oauth_refresh_tokens",
            token_hash,
            client_id,
            user_id,
            tenant_id,
            scopes,
            dataset_ids,
            resource,
            expires_at,
        )
        .await
    }

    /// Shared INSERT for the structurally-identical access/refresh token tables.
    #[allow(clippy::too_many_arguments)]
    async fn insert_oauth_token(
        &self,
        table: &str,
        token_hash: &str,
        client_id: &str,
        user_id: &str,
        tenant_id: &str,
        scopes: &[String],
        dataset_ids: Option<&[String]>,
        resource: Option<&str>,
        expires_at: DateTime<Utc>,
    ) -> Result<OAuthTokenRecord, sqlx::Error> {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let scopes_json = serde_json::to_string(scopes)
            .map_err(|e| sqlx::Error::Protocol(format!("failed to serialize scopes: {e}")))?;
        let dataset_ids_json = dataset_ids.map(encode_dataset_ids_json).transpose()?;
        match self {
            Catalog::Sqlite(pool) => {
                query(&format!(
                    "INSERT INTO {table} (id, token_hash, client_id, user_id, tenant_id, scopes, dataset_ids, resource, created_at, expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                ))
                .bind(&id)
                .bind(token_hash)
                .bind(client_id)
                .bind(user_id)
                .bind(tenant_id)
                .bind(&scopes_json)
                .bind(&dataset_ids_json)
                .bind(resource)
                .bind(now.to_rfc3339())
                .bind(expires_at.to_rfc3339())
                .execute(pool)
                .await?;
            }
            Catalog::Postgres(pool) => {
                query(&format!(
                    "INSERT INTO {table} (id, token_hash, client_id, user_id, tenant_id, scopes, dataset_ids, resource, created_at, expires_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"
                ))
                .bind(&id)
                .bind(token_hash)
                .bind(client_id)
                .bind(user_id)
                .bind(tenant_id)
                .bind(&scopes_json)
                .bind(&dataset_ids_json)
                .bind(resource)
                .bind(now)
                .bind(expires_at)
                .execute(pool)
                .await?;
            }
        }
        Ok(OAuthTokenRecord {
            id,
            client_id: client_id.to_string(),
            user_id: user_id.to_string(),
            tenant_id: tenant_id.to_string(),
            scopes: scopes.to_vec(),
            dataset_ids: dataset_ids.map(<[String]>::to_vec),
            resource: resource.map(str::to_owned),
            created_at: now,
            expires_at,
        })
    }

    /// Look up a valid (unexpired) access token by hash.
    pub async fn get_valid_access_token(
        &self,
        token_hash: &str,
    ) -> Result<Option<OAuthTokenRecord>, sqlx::Error> {
        self.get_valid_oauth_token("oauth_access_tokens", token_hash)
            .await
    }

    /// Look up a valid (unexpired) refresh token by hash.
    pub async fn get_valid_refresh_token(
        &self,
        token_hash: &str,
    ) -> Result<Option<OAuthTokenRecord>, sqlx::Error> {
        self.get_valid_oauth_token("oauth_refresh_tokens", token_hash)
            .await
    }

    /// Shared valid-token lookup for the access/refresh token tables.
    async fn get_valid_oauth_token(
        &self,
        table: &str,
        token_hash: &str,
    ) -> Result<Option<OAuthTokenRecord>, sqlx::Error> {
        let cols = "id, client_id, user_id, tenant_id, scopes, dataset_ids, resource, created_at, expires_at";
        match self {
            Catalog::Sqlite(pool) => {
                let row = query(&format!(
                    "SELECT {cols} FROM {table} WHERE token_hash = ? AND expires_at > ?"
                ))
                .bind(token_hash)
                .bind(Utc::now().to_rfc3339())
                .fetch_optional(pool)
                .await?;
                row.map(|r| {
                    Ok::<_, sqlx::Error>(OAuthTokenRecord {
                        id: r.get("id"),
                        client_id: r.get("client_id"),
                        user_id: r.get("user_id"),
                        tenant_id: r.get("tenant_id"),
                        scopes: decode_json_vec(r.get("scopes"))?,
                        dataset_ids: decode_json_vec_opt(r.get("dataset_ids"))?,
                        resource: r.get("resource"),
                        created_at: parse_rfc3339(r.get("created_at"))?,
                        expires_at: parse_rfc3339(r.get("expires_at"))?,
                    })
                })
                .transpose()
            }
            Catalog::Postgres(pool) => {
                let row = query(&format!(
                    "SELECT {cols} FROM {table} WHERE token_hash = $1 AND expires_at > NOW()"
                ))
                .bind(token_hash)
                .fetch_optional(pool)
                .await?;
                row.map(|r| {
                    Ok::<_, sqlx::Error>(OAuthTokenRecord {
                        id: r.get("id"),
                        client_id: r.get("client_id"),
                        user_id: r.get("user_id"),
                        tenant_id: r.get("tenant_id"),
                        scopes: decode_json_vec(r.get("scopes"))?,
                        dataset_ids: decode_json_vec_opt(r.get("dataset_ids"))?,
                        resource: r.get("resource"),
                        created_at: r.get("created_at"),
                        expires_at: r.get("expires_at"),
                    })
                })
                .transpose()
            }
        }
    }

    /// Revoke an access token by deleting its row, so subsequent presentations
    /// fail. Revoking an unknown token is a no-op.
    pub async fn revoke_access_token(&self, token_hash: &str) -> Result<(), sqlx::Error> {
        self.delete_oauth_token("oauth_access_tokens", token_hash)
            .await
    }

    /// Revoke a refresh token by deleting its row.
    pub async fn revoke_refresh_token(&self, token_hash: &str) -> Result<(), sqlx::Error> {
        self.delete_oauth_token("oauth_refresh_tokens", token_hash)
            .await
    }

    async fn delete_oauth_token(&self, table: &str, token_hash: &str) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                query(&format!("DELETE FROM {table} WHERE token_hash = ?"))
                    .bind(token_hash)
                    .execute(pool)
                    .await?;
            }
            Catalog::Postgres(pool) => {
                query(&format!("DELETE FROM {table} WHERE token_hash = $1"))
                    .bind(token_hash)
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

    /// Delete expired OAuth authorization codes, access tokens, and refresh
    /// tokens (change: mcp-oauth-dcr). Expired rows already fail validation, but
    /// nothing removes them; this reaps them so the tables and their `token_hash`
    /// indexes don't grow without bound. Mirrors [`delete_expired_sessions`];
    /// wiring a periodic reaper for both is a tracked follow-up. Returns the
    /// total rows deleted across the three tables.
    pub async fn delete_expired_oauth_grants(&self) -> Result<u64, sqlx::Error> {
        let mut deleted = 0u64;
        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();
                for table in [
                    "oauth_authorization_codes",
                    "oauth_access_tokens",
                    "oauth_refresh_tokens",
                ] {
                    // `table` is one of these three compile-time literals — never
                    // user input — so it is safe to name in the statement; every
                    // value is still bound.
                    let sql = match table {
                        "oauth_authorization_codes" => {
                            "DELETE FROM oauth_authorization_codes WHERE expires_at < ?"
                        }
                        "oauth_access_tokens" => {
                            "DELETE FROM oauth_access_tokens WHERE expires_at < ?"
                        }
                        _ => "DELETE FROM oauth_refresh_tokens WHERE expires_at < ?",
                    };
                    deleted += query(sql).bind(&now).execute(pool).await?.rows_affected();
                }
            }
            Catalog::Postgres(pool) => {
                for sql in [
                    "DELETE FROM oauth_authorization_codes WHERE expires_at < NOW()",
                    "DELETE FROM oauth_access_tokens WHERE expires_at < NOW()",
                    "DELETE FROM oauth_refresh_tokens WHERE expires_at < NOW()",
                ] {
                    deleted += query(sql).execute(pool).await?.rows_affected();
                }
            }
        }
        Ok(deleted)
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

    /// An on-disk SQLite catalog must run in WAL journal mode so that concurrent
    /// writers don't serialize behind an exclusive rollback lock.
    #[tokio::test]
    async fn on_disk_sqlite_catalog_uses_wal_journal_mode() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("signaldb.db");
        let dsn = format!("sqlite://{}", db_path.display());

        let catalog = Catalog::new(&dsn).await.unwrap();
        let Catalog::Sqlite(pool) = catalog else {
            panic!("expected a SQLite catalog");
        };

        let mode: String = sqlx::query("PRAGMA journal_mode")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get(0);
        assert_eq!(mode.to_lowercase(), "wal");
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
        let validation = validation.unwrap();
        assert_eq!(validation.tenant_id, "acme");
        assert_eq!(validation.name, Some("test-key".to_string()));
        assert_eq!(validation.dataset_id, None);
        assert_eq!(validation.scopes, None);

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
        let key_a = catalog.validate_api_key(&hash_a).await.unwrap().unwrap();
        assert_eq!(key_a.tenant_id, "tenant-a");

        let key_b = catalog.validate_api_key(&hash_b).await.unwrap().unwrap();
        assert_eq!(key_b.tenant_id, "tenant-b");
    }

    #[tokio::test]
    async fn list_api_keys_orders_by_created_at_descending() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme Corp", None, "config")
            .await
            .unwrap();

        // Rows are inserted in an order that would come back unchanged if the
        // query had no ORDER BY (ascending insertion order), so the
        // assertion below only passes if the query actually sorts.
        let oldest = catalog
            .upsert_api_key("acme", &hash_api_key("sk_oldest"), Some("oldest"))
            .await
            .unwrap();
        let middle = catalog
            .upsert_api_key("acme", &hash_api_key("sk_middle"), Some("middle"))
            .await
            .unwrap();
        let newest = catalog
            .upsert_api_key("acme", &hash_api_key("sk_newest"), Some("newest"))
            .await
            .unwrap();

        let keys = catalog.list_api_keys("acme").await.unwrap();
        let ids: Vec<&str> = keys.iter().map(|k| k.id.as_str()).collect();
        assert_eq!(ids, vec![newest, middle, oldest]);
    }

    #[tokio::test]
    async fn test_scoped_api_key_attributes_round_trip() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        catalog.create_dataset("acme", "production").await.unwrap();

        let key_hash = hash_api_key("scoped-secret");
        let scopes = vec!["metrics:write".to_string()];
        let key_id = catalog
            .upsert_scoped_api_key(
                "acme",
                &key_hash,
                Some("metrics"),
                Some(&["production".to_string()]),
                Some(&scopes),
                Some("user-1"),
            )
            .await
            .unwrap();

        let auth = catalog.validate_api_key(&key_hash).await.unwrap().unwrap();
        assert_eq!(auth.dataset_id.as_deref(), Some("production"));
        assert_eq!(auth.scopes, Some(scopes.clone()));

        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_id.as_deref(), Some("production"));
        assert_eq!(record.scopes, Some(scopes));
        assert_eq!(record.created_by_user_id.as_deref(), Some("user-1"));
    }

    #[tokio::test]
    async fn update_api_key_scopes_changes_scopes_and_dataset_of_live_key() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let key_hash = hash_api_key("live-secret");
        let key_id = catalog
            .upsert_scoped_api_key(
                "acme",
                &key_hash,
                Some("live"),
                None,
                Some(&["schema:read".to_string()]),
                None,
            )
            .await
            .unwrap();

        // Scopes only: dataset untouched.
        let updated = catalog
            .update_api_key_scopes(
                &key_id,
                Some(&["schema:read".to_string(), "schema:write".to_string()]),
                DatasetRestrictionUpdate::Keep,
            )
            .await
            .unwrap();
        assert!(updated);
        let auth = catalog.validate_api_key(&key_hash).await.unwrap().unwrap();
        assert_eq!(
            auth.scopes,
            Some(vec!["schema:read".to_string(), "schema:write".to_string()])
        );
        assert_eq!(auth.dataset_id, None);

        // Dataset only: scopes untouched.
        let updated = catalog
            .update_api_key_scopes(
                &key_id,
                None,
                DatasetRestrictionUpdate::Set(vec!["production".to_string()]),
            )
            .await
            .unwrap();
        assert!(updated);
        let auth = catalog.validate_api_key(&key_hash).await.unwrap().unwrap();
        assert_eq!(auth.dataset_id.as_deref(), Some("production"));
        assert_eq!(
            auth.scopes,
            Some(vec!["schema:read".to_string(), "schema:write".to_string()])
        );

        // Nothing to change is a no-op success.
        assert!(
            catalog
                .update_api_key_scopes(&key_id, None, DatasetRestrictionUpdate::Keep)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn update_api_key_scopes_rejects_revoked_and_unknown_keys() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let key_hash = hash_api_key("doomed-secret");
        let key_id = catalog
            .upsert_scoped_api_key(
                "acme",
                &key_hash,
                None,
                None,
                Some(&["traces:write".to_string()]),
                None,
            )
            .await
            .unwrap();
        catalog.revoke_api_key(&key_id).await.unwrap();

        let updated = catalog
            .update_api_key_scopes(
                &key_id,
                Some(&["logs:write".to_string()]),
                DatasetRestrictionUpdate::Keep,
            )
            .await
            .unwrap();
        assert!(!updated, "revoked keys must not be updatable");
        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.scopes, Some(vec!["traces:write".to_string()]));

        let updated = catalog
            .update_api_key_scopes(
                "no-such-key",
                Some(&["logs:write".to_string()]),
                DatasetRestrictionUpdate::Keep,
            )
            .await
            .unwrap();
        assert!(!updated);
    }

    #[tokio::test]
    async fn create_with_multi_element_dataset_ids_round_trips() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let key_hash = hash_api_key("multi-dataset-secret");
        let ids = vec!["a".to_string(), "b".to_string()];
        let key_id = catalog
            .upsert_scoped_api_key(
                "acme",
                &key_hash,
                Some("multi"),
                Some(&ids),
                Some(&["traces:read".to_string()]),
                None,
            )
            .await
            .unwrap();

        let auth = catalog.validate_api_key(&key_hash).await.unwrap().unwrap();
        assert_eq!(auth.dataset_ids, Some(ids.clone()));
        // The legacy single-value column can't represent a multi-element
        // set, so it stays NULL (D2's projection).
        assert_eq!(auth.dataset_id, None);

        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_ids, Some(ids));
        assert_eq!(record.dataset_id, None);
    }

    #[tokio::test]
    async fn create_with_empty_dataset_ids_is_rejected() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let result = catalog
            .upsert_scoped_api_key(
                "acme",
                &hash_api_key("empty-dataset-secret"),
                None,
                Some(&[]),
                None,
                None,
            )
            .await;
        assert!(result.is_err(), "an empty dataset_ids set must be rejected");
    }

    #[tokio::test]
    async fn create_with_duplicate_dataset_ids_is_rejected() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let result = catalog
            .upsert_scoped_api_key(
                "acme",
                &hash_api_key("dup-dataset-secret"),
                None,
                Some(&["production".to_string(), "production".to_string()]),
                None,
                None,
            )
            .await;
        assert!(
            result.is_err(),
            "a dataset_ids set with a duplicate name must be rejected"
        );
    }

    /// A key created before this change (legacy `dataset_id` column
    /// populated, `dataset_ids` never written) reads back as a one-element
    /// set (D2's dual-read).
    #[tokio::test]
    async fn legacy_dataset_id_reads_back_as_single_element_set() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let key_id = catalog
            .upsert_api_key("acme", &hash_api_key("legacy-secret"), None)
            .await
            .unwrap();
        let Catalog::Sqlite(pool) = &catalog else {
            panic!("expected a SQLite catalog");
        };
        // Simulate a pre-existing row written before `dataset_ids` existed:
        // only the legacy column is populated.
        query("UPDATE api_keys SET dataset_id = 'legacy-value' WHERE id = ?")
            .bind(&key_id)
            .execute(pool)
            .await
            .unwrap();

        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_ids, Some(vec!["legacy-value".to_string()]));
    }

    #[tokio::test]
    async fn dataset_restriction_update_keep_leaves_both_columns_untouched() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let key_id = catalog
            .upsert_scoped_api_key(
                "acme",
                &hash_api_key("keep-secret"),
                None,
                Some(&["a".to_string()]),
                Some(&["traces:read".to_string()]),
                None,
            )
            .await
            .unwrap();

        assert!(
            catalog
                .update_api_key_scopes(&key_id, None, DatasetRestrictionUpdate::Keep)
                .await
                .unwrap()
        );
        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_ids, Some(vec!["a".to_string()]));
        assert_eq!(record.dataset_id.as_deref(), Some("a"));
    }

    #[tokio::test]
    async fn dataset_restriction_update_clear_nulls_both_columns() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let key_id = catalog
            .upsert_scoped_api_key(
                "acme",
                &hash_api_key("clear-secret"),
                None,
                Some(&["a".to_string()]),
                Some(&["traces:read".to_string()]),
                None,
            )
            .await
            .unwrap();

        assert!(
            catalog
                .update_api_key_scopes(&key_id, None, DatasetRestrictionUpdate::Clear)
                .await
                .unwrap()
        );
        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_ids, None);
        assert_eq!(record.dataset_id, None);
    }

    #[tokio::test]
    async fn dataset_restriction_update_set_projects_legacy_column() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let key_id = catalog
            .upsert_scoped_api_key(
                "acme",
                &hash_api_key("set-secret"),
                None,
                None,
                Some(&["traces:read".to_string()]),
                None,
            )
            .await
            .unwrap();

        // A single-element `Set` projects onto the legacy column too.
        assert!(
            catalog
                .update_api_key_scopes(
                    &key_id,
                    None,
                    DatasetRestrictionUpdate::Set(vec!["a".to_string()]),
                )
                .await
                .unwrap()
        );
        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_ids, Some(vec!["a".to_string()]));
        assert_eq!(record.dataset_id.as_deref(), Some("a"));

        // A multi-element `Set` clears the legacy column (unrepresentable).
        assert!(
            catalog
                .update_api_key_scopes(
                    &key_id,
                    None,
                    DatasetRestrictionUpdate::Set(vec!["a".to_string(), "b".to_string()]),
                )
                .await
                .unwrap()
        );
        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(
            record.dataset_ids,
            Some(vec!["a".to_string(), "b".to_string()])
        );
        assert_eq!(record.dataset_id, None);
    }

    #[tokio::test]
    async fn dataset_restriction_update_set_rejects_empty_and_duplicate() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let key_id = catalog
            .upsert_scoped_api_key(
                "acme",
                &hash_api_key("set-invalid-secret"),
                None,
                None,
                Some(&["traces:read".to_string()]),
                None,
            )
            .await
            .unwrap();

        let empty = catalog
            .update_api_key_scopes(&key_id, None, DatasetRestrictionUpdate::Set(vec![]))
            .await;
        assert!(empty.is_err(), "an empty Set must be rejected");

        let duplicate = catalog
            .update_api_key_scopes(
                &key_id,
                None,
                DatasetRestrictionUpdate::Set(vec!["a".to_string(), "a".to_string()]),
            )
            .await;
        assert!(
            duplicate.is_err(),
            "a duplicate-containing Set must be rejected"
        );
    }

    /// D2's backfill compare-and-swap guard: a concurrent legacy write to
    /// `dataset_id` between the backfill's read and its write must make the
    /// write a no-op rather than persist a `dataset_ids` value derived from
    /// data that's already stale. The row resolves itself on the next pass.
    #[tokio::test]
    async fn backfill_race_with_concurrent_legacy_write_is_a_no_op_then_self_heals() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let key_id = catalog
            .upsert_api_key("acme", &hash_api_key("race-secret"), None)
            .await
            .unwrap();
        let Catalog::Sqlite(pool) = &catalog else {
            panic!("expected a SQLite catalog");
        };
        // Simulate a legacy row: only `dataset_id` populated.
        query("UPDATE api_keys SET dataset_id = 'a' WHERE id = ?")
            .bind(&key_id)
            .execute(pool)
            .await
            .unwrap();

        // Backfill's SELECT reads `dataset_id = 'a'`.
        let pending = catalog.pending_api_key_dataset_id_backfill().await.unwrap();
        let (_, read_value) = pending
            .iter()
            .find(|(id, _)| id == &key_id)
            .expect("row is pending backfill")
            .clone();
        assert_eq!(read_value, "a");

        // Before the backfill's UPDATE runs, an old-code node races in a
        // concurrent legacy write.
        query("UPDATE api_keys SET dataset_id = 'b' WHERE id = ?")
            .bind(&key_id)
            .execute(pool)
            .await
            .unwrap();

        // The backfill's UPDATE, using the now-stale value it read, must
        // affect zero rows and leave the `dataset_ids` *column* untouched
        // (still NULL) — checked directly against the column, since
        // `ApiKeyRecord::dataset_ids` derives from the legacy column
        // whenever the `dataset_ids` column is NULL and would otherwise
        // mask a stale write with a correct-looking read.
        let rows_affected = catalog
            .apply_api_key_dataset_id_backfill(&key_id, &read_value)
            .await
            .unwrap();
        assert_eq!(rows_affected, 0);
        let raw_dataset_ids: Option<String> =
            query("SELECT dataset_ids FROM api_keys WHERE id = ?")
                .bind(&key_id)
                .fetch_one(pool)
                .await
                .unwrap()
                .get("dataset_ids");
        assert_eq!(raw_dataset_ids, None);
        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_id.as_deref(), Some("b"));

        // The next boot's full backfill pass picks up the current value.
        catalog.backfill_api_key_dataset_ids().await.unwrap();
        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_ids, Some(vec!["b".to_string()]));
    }

    /// The tenant row and its default dataset row must land together. A
    /// half-written tenant fails authentication closed, and — because
    /// creation rejects an existing id with 409 — a retry cannot repair it,
    /// so it would stay broken until the next boot backfill.
    #[tokio::test]
    async fn upsert_tenant_with_default_dataset_writes_both_rows() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        catalog
            .upsert_tenant_with_default_dataset("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();

        assert_eq!(
            catalog.get_tenant("acme").await.unwrap().unwrap().id,
            "acme"
        );
        assert_eq!(
            catalog
                .get_datasets("acme")
                .await
                .unwrap()
                .iter()
                .map(|d| d.name.as_str())
                .collect::<Vec<_>>(),
            vec!["production"],
        );
    }

    #[tokio::test]
    async fn upsert_tenant_with_default_dataset_handles_no_default_and_repeats() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();

        catalog
            .upsert_tenant_with_default_dataset("acme", "Acme", None, "database")
            .await
            .unwrap();
        assert!(catalog.get_datasets("acme").await.unwrap().is_empty());

        // Repointing the default (the update path) materializes the new one
        // and leaves the old in place; repeats stay idempotent.
        catalog
            .upsert_tenant_with_default_dataset("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        catalog
            .upsert_tenant_with_default_dataset("acme", "Acme", Some("staging"), "database")
            .await
            .unwrap();
        catalog
            .upsert_tenant_with_default_dataset("acme", "Acme", Some("staging"), "database")
            .await
            .unwrap();

        let mut names: Vec<String> = catalog
            .get_datasets("acme")
            .await
            .unwrap()
            .into_iter()
            .map(|d| d.name)
            .collect();
        names.sort();
        assert_eq!(names, vec!["production", "staging"]);
        assert_eq!(
            catalog
                .get_tenant("acme")
                .await
                .unwrap()
                .unwrap()
                .default_dataset
                .as_deref(),
            Some("staging"),
        );
    }

    #[tokio::test]
    async fn ensure_dataset_is_idempotent() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();

        let first = catalog.ensure_dataset("acme", "production").await.unwrap();
        let second = catalog.ensure_dataset("acme", "production").await.unwrap();

        assert_eq!(first, second, "the existing dataset id must be returned");
        assert_eq!(catalog.get_datasets("acme").await.unwrap().len(), 1);

        // And it adopts a row that `create_dataset` wrote.
        let created = catalog.create_dataset("acme", "staging").await.unwrap();
        assert_eq!(
            catalog.ensure_dataset("acme", "staging").await.unwrap(),
            created
        );
        assert_eq!(catalog.get_datasets("acme").await.unwrap().len(), 2);
    }

    /// Tenants created before the default dataset was materialized at write
    /// time carry `default_dataset` with no matching row, which fails auth
    /// closed. The backfill converges them.
    #[tokio::test]
    async fn backfill_materializes_missing_default_dataset_rows() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        // Pre-existing admin-API tenant: column set, no dataset row.
        catalog
            .upsert_tenant("gamma", "Gamma", Some("production"), "database")
            .await
            .unwrap();
        // A tenant whose default already has a row must be left alone.
        catalog
            .upsert_tenant("delta", "Delta", Some("production"), "database")
            .await
            .unwrap();
        catalog.create_dataset("delta", "production").await.unwrap();
        // A tenant with no default dataset at all must gain nothing.
        catalog
            .upsert_tenant("epsilon", "Epsilon", None, "database")
            .await
            .unwrap();

        let materialized = catalog.backfill_default_datasets().await.unwrap();
        assert_eq!(materialized, 1, "only the missing row is written");

        let gamma = catalog.get_datasets("gamma").await.unwrap();
        assert_eq!(
            gamma.iter().map(|d| d.name.as_str()).collect::<Vec<_>>(),
            vec!["production"],
        );
        assert_eq!(catalog.get_datasets("delta").await.unwrap().len(), 1);
        assert!(catalog.get_datasets("epsilon").await.unwrap().is_empty());

        // Converged: a second pass is a no-op.
        assert_eq!(catalog.backfill_default_datasets().await.unwrap(), 0);
    }

    /// A config tenant may declare `default_dataset` without a matching
    /// `[[auth.tenants.datasets]]` block; boot sync must still give it a row.
    #[tokio::test]
    async fn sync_config_tenants_materializes_an_undeclared_default_dataset() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let auth = crate::config::AuthConfig {
            tenants: vec![crate::config::TenantConfig {
                id: "acme".to_string(),
                slug: "acme".to_string(),
                name: "Acme".to_string(),
                default_dataset: Some("production".to_string()),
                datasets: vec![],
                api_keys: vec![],
                schema_config: None,
                limits: None,
            }],
            ..Default::default()
        };

        catalog.sync_config_tenants(&auth).await.unwrap();
        assert_eq!(
            catalog
                .get_datasets("acme")
                .await
                .unwrap()
                .iter()
                .map(|d| d.name.as_str())
                .collect::<Vec<_>>(),
            vec!["production"],
        );

        // Boot runs this every start; it must stay idempotent.
        catalog.sync_config_tenants(&auth).await.unwrap();
        assert_eq!(catalog.get_datasets("acme").await.unwrap().len(), 1);
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
    async fn attribute_value_sketch_replaces_wholesale() {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        catalog
            .replace_attribute_value_stats(
                "t",
                "d",
                "logs",
                "http.route",
                &[("/orders".to_string(), 9), ("/users".to_string(), 3)],
            )
            .await
            .unwrap();

        let sketch = catalog
            .get_attribute_value_stats("t", "d", "logs", "http.route", 10)
            .await
            .unwrap();
        assert_eq!(sketch.len(), 2);
        assert_eq!(sketch[0].value, "/orders", "most frequent first");
        assert_eq!(sketch[0].count, 9);
        assert!(!sketch[0].updated_at.is_empty());

        // A later pass supersedes the earlier one rather than merging: a
        // value that stopped occurring must stop being suggested.
        catalog
            .replace_attribute_value_stats(
                "t",
                "d",
                "logs",
                "http.route",
                &[("/new".to_string(), 1)],
            )
            .await
            .unwrap();
        let sketch = catalog
            .get_attribute_value_stats("t", "d", "logs", "http.route", 10)
            .await
            .unwrap();
        assert_eq!(sketch.len(), 1);
        assert_eq!(sketch[0].value, "/new");

        // Clearing it is how a key past the cardinality cap stops being
        // suggested at all.
        catalog
            .replace_attribute_value_stats("t", "d", "logs", "http.route", &[])
            .await
            .unwrap();
        assert!(
            catalog
                .get_attribute_value_stats("t", "d", "logs", "http.route", 10)
                .await
                .unwrap()
                .is_empty()
        );

        // Another tenant's sketch is never visible.
        catalog
            .replace_attribute_value_stats(
                "other",
                "d",
                "logs",
                "http.route",
                &[("/secret".to_string(), 5)],
            )
            .await
            .unwrap();
        assert!(
            catalog
                .get_attribute_value_stats("t", "d", "logs", "http.route", 10)
                .await
                .unwrap()
                .is_empty()
        );
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

/// `dataset_ids` behaves identically on Postgres (D2's dual-read/dual-write
/// and the backfill compare-and-swap guard aren't SQLite-specific).
#[cfg(test)]
mod postgres_dataset_ids_tests {
    use super::*;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    async fn postgres_catalog() -> (
        Catalog,
        testcontainers_modules::testcontainers::ContainerAsync<Postgres>,
    ) {
        let container = Postgres::default().start().await.unwrap();
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432).await.unwrap();
        let dsn = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        let catalog = Catalog::new(&dsn).await.unwrap();
        (catalog, container)
    }

    #[tokio::test]
    async fn multi_element_dataset_ids_round_trip_on_postgres() {
        let (catalog, _container) = postgres_catalog().await;
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let ids = vec!["a".to_string(), "b".to_string()];
        let key_id = catalog
            .upsert_scoped_api_key(
                "acme",
                "pg-multi-hash",
                Some("multi"),
                Some(&ids),
                Some(&["traces:read".to_string()]),
                None,
            )
            .await
            .unwrap();

        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_ids, Some(ids));
        assert_eq!(record.dataset_id, None);

        assert!(
            catalog
                .update_api_key_scopes(&key_id, None, DatasetRestrictionUpdate::Clear)
                .await
                .unwrap()
        );
        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_ids, None);
        assert_eq!(record.dataset_id, None);
    }

    /// Same compare-and-swap regression as
    /// `backfill_race_with_concurrent_legacy_write_is_a_no_op_then_self_heals`,
    /// on Postgres.
    #[tokio::test]
    async fn backfill_race_with_concurrent_legacy_write_is_a_no_op_then_self_heals_on_postgres() {
        let (catalog, _container) = postgres_catalog().await;
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        let key_id = catalog
            .upsert_api_key("acme", "pg-race-hash", None)
            .await
            .unwrap();
        let Catalog::Postgres(pool) = &catalog else {
            panic!("expected a Postgres catalog");
        };
        query("UPDATE api_keys SET dataset_id = 'a' WHERE id = $1")
            .bind(&key_id)
            .execute(pool)
            .await
            .unwrap();

        let pending = catalog.pending_api_key_dataset_id_backfill().await.unwrap();
        let (_, read_value) = pending
            .iter()
            .find(|(id, _)| id == &key_id)
            .expect("row is pending backfill")
            .clone();
        assert_eq!(read_value, "a");

        query("UPDATE api_keys SET dataset_id = 'b' WHERE id = $1")
            .bind(&key_id)
            .execute(pool)
            .await
            .unwrap();

        let rows_affected = catalog
            .apply_api_key_dataset_id_backfill(&key_id, &read_value)
            .await
            .unwrap();
        assert_eq!(rows_affected, 0);
        let raw_dataset_ids: Option<String> =
            query("SELECT dataset_ids FROM api_keys WHERE id = $1")
                .bind(&key_id)
                .fetch_one(pool)
                .await
                .unwrap()
                .get("dataset_ids");
        assert_eq!(raw_dataset_ids, None);
        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_id.as_deref(), Some("b"));

        catalog.backfill_api_key_dataset_ids().await.unwrap();
        let record = catalog.get_api_key(&key_id).await.unwrap().unwrap();
        assert_eq!(record.dataset_ids, Some(vec!["b".to_string()]));
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

#[cfg(test)]
mod oauth_storage_tests {
    use super::*;
    use chrono::Duration;

    /// A catalog with one user and one tenant, whose ids the OAuth rows
    /// reference (satisfying the foreign keys).
    async fn catalog_with_principal() -> (Catalog, String, String) {
        let catalog = Catalog::new("sqlite::memory:").await.unwrap();
        let user = catalog
            .create_user("agent@example.com", None, "phc", false)
            .await
            .unwrap();
        catalog
            .upsert_tenant("acme", "Acme", Some("production"), "database")
            .await
            .unwrap();
        (catalog, user.id, "acme".to_string())
    }

    #[tokio::test]
    async fn oauth_client_round_trips_and_unknown_is_none() {
        let (catalog, _user, _tenant) = catalog_with_principal().await;
        let redirects = vec!["https://claude.ai/cb".to_string()];
        let stored = catalog
            .register_oauth_client(
                "client-1",
                Some("Claude"),
                &redirects,
                None,
                Some("traces:read"),
                "none",
            )
            .await
            .unwrap();
        assert_eq!(stored.id, "client-1");

        let fetched = catalog.get_oauth_client("client-1").await.unwrap().unwrap();
        assert_eq!(fetched.client_name.as_deref(), Some("Claude"));
        assert_eq!(fetched.redirect_uris, redirects);
        assert_eq!(fetched.token_endpoint_auth_method, "none");

        assert!(catalog.get_oauth_client("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn authorization_code_is_single_use() {
        let (catalog, user, tenant) = catalog_with_principal().await;
        let scopes = vec!["traces:read".to_string()];
        catalog
            .create_authorization_code(
                "code-hash-1",
                "client-1",
                &user,
                &tenant,
                &scopes,
                None,
                "https://claude.ai/cb",
                "challenge-abc",
                Some("https://mcp.example.com/mcp"),
                Utc::now() + Duration::minutes(1),
            )
            .await
            .unwrap();

        let first = catalog
            .consume_authorization_code("code-hash-1")
            .await
            .unwrap()
            .expect("first consume returns the grant");
        assert_eq!(first.tenant_id, tenant);
        assert_eq!(first.scopes, scopes);
        assert_eq!(first.code_challenge, "challenge-abc");
        assert_eq!(
            first.resource.as_deref(),
            Some("https://mcp.example.com/mcp")
        );

        // Single-use: a second redemption of the same code finds nothing.
        assert!(
            catalog
                .consume_authorization_code("code-hash-1")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn expired_authorization_code_is_not_returned() {
        let (catalog, user, tenant) = catalog_with_principal().await;
        catalog
            .create_authorization_code(
                "code-hash-old",
                "client-1",
                &user,
                &tenant,
                &["traces:read".to_string()],
                None,
                "https://claude.ai/cb",
                "challenge",
                None,
                Utc::now() - Duration::minutes(1),
            )
            .await
            .unwrap();
        assert!(
            catalog
                .consume_authorization_code("code-hash-old")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn access_token_valid_lookup_then_revoke() {
        let (catalog, user, tenant) = catalog_with_principal().await;
        let scopes = vec!["traces:read".to_string(), "logs:read".to_string()];
        catalog
            .create_access_token(
                "at-hash-1",
                "client-1",
                &user,
                &tenant,
                &scopes,
                None,
                Some("https://mcp.example.com/mcp"),
                Utc::now() + Duration::hours(1),
            )
            .await
            .unwrap();

        let found = catalog
            .get_valid_access_token("at-hash-1")
            .await
            .unwrap()
            .expect("valid token is found");
        assert_eq!(found.tenant_id, tenant);
        assert_eq!(found.scopes, scopes);

        catalog.revoke_access_token("at-hash-1").await.unwrap();
        assert!(
            catalog
                .get_valid_access_token("at-hash-1")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn expired_access_token_is_not_valid() {
        let (catalog, user, tenant) = catalog_with_principal().await;
        catalog
            .create_access_token(
                "at-hash-old",
                "client-1",
                &user,
                &tenant,
                &["traces:read".to_string()],
                None,
                None,
                Utc::now() - Duration::seconds(1),
            )
            .await
            .unwrap();
        assert!(
            catalog
                .get_valid_access_token("at-hash-old")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn refresh_token_valid_lookup_then_revoke() {
        let (catalog, user, tenant) = catalog_with_principal().await;
        catalog
            .create_refresh_token(
                "rt-hash-1",
                "client-1",
                &user,
                &tenant,
                &["traces:read".to_string()],
                None,
                Some("https://mcp.example.com/mcp"),
                Utc::now() + Duration::days(30),
            )
            .await
            .unwrap();
        assert!(
            catalog
                .get_valid_refresh_token("rt-hash-1")
                .await
                .unwrap()
                .is_some()
        );
        catalog.revoke_refresh_token("rt-hash-1").await.unwrap();
        assert!(
            catalog
                .get_valid_refresh_token("rt-hash-1")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn oauth_grants_round_trip_dataset_ids() {
        let (catalog, user, tenant) = catalog_with_principal().await;
        let scopes = vec!["traces:read".to_string()];
        let ids = vec!["a".to_string(), "b".to_string()];

        catalog
            .create_authorization_code(
                "code-with-datasets",
                "client-1",
                &user,
                &tenant,
                &scopes,
                Some(&ids),
                "https://claude.ai/cb",
                "challenge",
                None,
                Utc::now() + Duration::minutes(1),
            )
            .await
            .unwrap();
        let code = catalog
            .consume_authorization_code("code-with-datasets")
            .await
            .unwrap()
            .expect("code exists");
        assert_eq!(code.dataset_ids, Some(ids.clone()));

        let access = catalog
            .create_access_token(
                "at-with-datasets",
                "client-1",
                &user,
                &tenant,
                &scopes,
                Some(&ids),
                None,
                Utc::now() + Duration::hours(1),
            )
            .await
            .unwrap();
        assert_eq!(access.dataset_ids, Some(ids.clone()));
        let fetched = catalog
            .get_valid_access_token("at-with-datasets")
            .await
            .unwrap()
            .expect("access token exists");
        assert_eq!(fetched.dataset_ids, Some(ids.clone()));

        let refresh = catalog
            .create_refresh_token(
                "rt-with-datasets",
                "client-1",
                &user,
                &tenant,
                &scopes,
                Some(&ids),
                None,
                Utc::now() + Duration::days(30),
            )
            .await
            .unwrap();
        assert_eq!(refresh.dataset_ids, Some(ids.clone()));
        let fetched = catalog
            .get_valid_refresh_token("rt-with-datasets")
            .await
            .unwrap()
            .expect("refresh token exists");
        assert_eq!(fetched.dataset_ids, Some(ids));
    }

    #[tokio::test]
    async fn create_access_token_rejects_empty_and_duplicate_dataset_ids() {
        let (catalog, user, tenant) = catalog_with_principal().await;
        let scopes = vec!["traces:read".to_string()];

        let empty = catalog
            .create_access_token(
                "at-empty",
                "client-1",
                &user,
                &tenant,
                &scopes,
                Some(&[]),
                None,
                Utc::now() + Duration::hours(1),
            )
            .await;
        assert!(empty.is_err());

        let duplicate = catalog
            .create_access_token(
                "at-dup",
                "client-1",
                &user,
                &tenant,
                &scopes,
                Some(&["a".to_string(), "a".to_string()]),
                None,
                Utc::now() + Duration::hours(1),
            )
            .await;
        assert!(duplicate.is_err());
    }

    /// D6: a refresh reads `dataset_ids` from the presented
    /// `oauth_refresh_tokens` row being redeemed, not from any access
    /// token. The original access token is revoked (gone) before the
    /// "refresh" happens, so a wrong implementation that tries to read it
    /// would fail loudly rather than coincidentally pass.
    #[tokio::test]
    async fn refresh_reads_dataset_ids_from_refresh_token_row_not_access_token() {
        let (catalog, user, tenant) = catalog_with_principal().await;
        let scopes = vec!["traces:read".to_string()];
        let ids = vec!["production".to_string()];

        catalog
            .create_access_token(
                "at-original",
                "client-1",
                &user,
                &tenant,
                &scopes,
                Some(&ids),
                None,
                Utc::now() + chrono::Duration::hours(1),
            )
            .await
            .unwrap();
        catalog
            .create_refresh_token(
                "rt-original",
                "client-1",
                &user,
                &tenant,
                &scopes,
                Some(&ids),
                None,
                Utc::now() + Duration::days(30),
            )
            .await
            .unwrap();

        // The original access token is gone by the time refresh happens.
        catalog.revoke_access_token("at-original").await.unwrap();
        assert!(
            catalog
                .get_valid_access_token("at-original")
                .await
                .unwrap()
                .is_none()
        );

        // Refresh: read dataset_ids from the presented refresh token row...
        let presented = catalog
            .get_valid_refresh_token("rt-original")
            .await
            .unwrap()
            .expect("refresh token is valid");
        assert_eq!(presented.dataset_ids, Some(ids.clone()));

        // ...and propagate it onto BOTH the new access token and the new
        // replacement refresh token the refresh grant mints.
        let new_access = catalog
            .create_access_token(
                "at-refreshed",
                "client-1",
                &user,
                &tenant,
                &presented.scopes,
                presented.dataset_ids.as_deref(),
                None,
                Utc::now() + chrono::Duration::hours(1),
            )
            .await
            .unwrap();
        let new_refresh = catalog
            .create_refresh_token(
                "rt-refreshed",
                "client-1",
                &user,
                &tenant,
                &presented.scopes,
                presented.dataset_ids.as_deref(),
                None,
                Utc::now() + Duration::days(30),
            )
            .await
            .unwrap();

        assert_eq!(new_access.dataset_ids, Some(ids.clone()));
        assert_eq!(new_refresh.dataset_ids, Some(ids));
    }

    #[tokio::test]
    async fn delete_expired_oauth_grants_reaps_only_expired_rows() {
        let (catalog, user, tenant) = catalog_with_principal().await;
        let past = Utc::now() - Duration::hours(1);
        let future = Utc::now() + Duration::hours(1);
        // One expired + one live token in each token table, and one expired code.
        catalog
            .create_access_token("at-old", "c", &user, &tenant, &[], None, None, past)
            .await
            .unwrap();
        catalog
            .create_access_token("at-live", "c", &user, &tenant, &[], None, None, future)
            .await
            .unwrap();
        catalog
            .create_refresh_token("rt-old", "c", &user, &tenant, &[], None, None, past)
            .await
            .unwrap();
        catalog
            .create_authorization_code(
                "code-old",
                "c",
                &user,
                &tenant,
                &[],
                None,
                "https://c/cb",
                "chal",
                None,
                past,
            )
            .await
            .unwrap();

        let removed = catalog.delete_expired_oauth_grants().await.unwrap();
        assert_eq!(removed, 3, "the two expired tokens and the expired code");

        // The live access token survives; a second pass removes nothing.
        assert!(
            catalog
                .get_valid_access_token("at-live")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(catalog.delete_expired_oauth_grants().await.unwrap(), 0);
    }
}
