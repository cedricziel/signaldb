use chrono::{DateTime, Utc};
use sqlx::{query, PgPool, Row, SqlitePool};
use uuid::Uuid;

/// Catalog provides an interface to the catalog database (PostgreSQL or SQLite).
#[derive(Clone)]
pub enum Catalog {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

impl Catalog {
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
                log::error!(
                    "Failed to connect to PostgreSQL database with DSN '{dsn}': {e}"
                );
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
                    last_seen TEXT NOT NULL
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
            }
            Catalog::Postgres(pool) => {
                // PostgreSQL schema
                let create_ingesters = r#"
                CREATE TABLE IF NOT EXISTS ingesters (
                    id UUID PRIMARY KEY,
                    address TEXT NOT NULL,
                    last_seen TIMESTAMPTZ NOT NULL
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
            }
        }

        Ok(())
    }

    /// Register or update an ingester with its address and heartbeat.
    pub async fn register_ingester(&self, id: Uuid, address: &str) -> Result<(), sqlx::Error> {
        match self {
            Catalog::Sqlite(pool) => {
                let now = Utc::now().to_rfc3339();
                let id_str = id.to_string();

                // Try insert first, then update if it already exists
                let insert_stmt = r#"
                INSERT INTO ingesters (id, address, last_seen)
                VALUES (?, ?, ?)
                "#;

                let result = query(insert_stmt)
                    .bind(&id_str)
                    .bind(address)
                    .bind(&now)
                    .execute(pool)
                    .await;

                if result.is_err() {
                    // If insert failed (likely due to duplicate key), try update
                    let update_stmt = r#"
                    UPDATE ingesters SET address = ?, last_seen = ?
                    WHERE id = ?
                    "#;
                    query(update_stmt)
                        .bind(address)
                        .bind(&now)
                        .bind(&id_str)
                        .execute(pool)
                        .await?;
                }
            }
            Catalog::Postgres(pool) => {
                // PostgreSQL with UPSERT
                let stmt = r#"
                INSERT INTO ingesters (id, address, last_seen)
                VALUES ($1, $2, NOW())
                ON CONFLICT (id) DO UPDATE SET address = $2, last_seen = NOW()
                "#;
                query(stmt).bind(id).bind(address).execute(pool).await?;
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
                let rows = query("SELECT id, address, last_seen FROM ingesters")
                    .fetch_all(pool)
                    .await?;
                let mut ingesters = Vec::with_capacity(rows.len());
                for row in rows {
                    let id_str: String = row.get("id");
                    let last_seen_str: String = row.get("last_seen");

                    let id = Uuid::parse_str(&id_str)
                        .map_err(|_| sqlx::Error::Decode("Invalid UUID format".into()))?;
                    let last_seen = DateTime::parse_from_rfc3339(&last_seen_str)
                        .map_err(|_| sqlx::Error::Decode("Invalid timestamp format".into()))?
                        .with_timezone(&Utc);

                    let ing = Ingester {
                        id,
                        address: row.get("address"),
                        last_seen,
                    };
                    ingesters.push(ing);
                }
                Ok(ingesters)
            }
            Catalog::Postgres(pool) => {
                let rows = query("SELECT id, address, last_seen FROM ingesters")
                    .fetch_all(pool)
                    .await?;
                let mut ingesters = Vec::with_capacity(rows.len());
                for row in rows {
                    let ing = Ingester {
                        id: row.get("id"),
                        address: row.get("address"),
                        last_seen: row.get("last_seen"),
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
                if let Err(sqlx::Error::Database(db_err)) = &result {
                    if db_err.message().contains("UNIQUE constraint failed") {
                        return Ok(());
                    }
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
                if let Err(sqlx::Error::Database(db_err)) = &result {
                    if db_err.message().contains("UNIQUE constraint failed") {
                        return Ok(());
                    }
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
