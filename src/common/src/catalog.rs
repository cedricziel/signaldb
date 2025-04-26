use sqlx::{PgPool, query, Row};
use uuid::Uuid;
use chrono::{DateTime, Utc};

/// Catalog provides an interface to the Postgres catalog database.
#[derive(Clone)]
pub struct Catalog {
    pool: PgPool,
}

impl Catalog {
    /// Create a new Catalog client and initialize schema.
    pub async fn new(dsn: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPool::connect(dsn).await?;
        let catalog = Catalog { pool };
        catalog.init().await?;
        Ok(catalog)
    }

    /// Initialize catalog tables if they do not exist.
    async fn init(&self) -> Result<(), sqlx::Error> {
        // ingesters table
        let create_ingesters = r#"
        CREATE TABLE IF NOT EXISTS ingesters (
            id UUID PRIMARY KEY,
            address TEXT NOT NULL,
            last_seen TIMESTAMPTZ NOT NULL
        )"#;
        query(create_ingesters).execute(&self.pool).await?;

        // shards table
        let create_shards = r#"
        CREATE TABLE IF NOT EXISTS shards (
            id INT PRIMARY KEY,
            start_range BIGINT NOT NULL,
            end_range BIGINT NOT NULL
        )"#;
        query(create_shards).execute(&self.pool).await?;

        // shard_owners table
        let create_shard_owners = r#"
        CREATE TABLE IF NOT EXISTS shard_owners (
            shard_id INT NOT NULL REFERENCES shards(id),
            ingester_id UUID NOT NULL REFERENCES ingesters(id),
            PRIMARY KEY (shard_id, ingester_id)
        )"#;
        query(create_shard_owners).execute(&self.pool).await?;

        Ok(())
    }

    /// Register or update an ingester with its address and heartbeat.
    pub async fn register_ingester(&self, id: Uuid, address: &str) -> Result<(), sqlx::Error> {
        let stmt = r#"
        INSERT INTO ingesters (id, address, last_seen)
        VALUES ($1, $2, NOW())
        ON CONFLICT (id) DO UPDATE SET address = $2, last_seen = NOW()
        "#;
        query(stmt)
            .bind(id)
            .bind(address)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Update heartbeat timestamp for an ingester.
    pub async fn heartbeat(&self, id: Uuid) -> Result<(), sqlx::Error> {
        let stmt = r#"
        UPDATE ingesters SET last_seen = NOW()
        WHERE id = $1
        "#;
-       query(stmt)
-           .bind(id)
-           .execute(&self.pool)
-           .await?;
+       let result = query(stmt)
+           .bind(id)
+           .execute(&self.pool)
+           .await?;
+       if result.rows_affected() == 0 {
+           return Err(sqlx::Error::RowNotFound);
+       }
        Ok(())
    }

    /// List all ingesters in the catalog.
    pub async fn list_ingesters(&self) -> Result<Vec<Ingester>, sqlx::Error> {
        let rows = query("SELECT id, address, last_seen FROM ingesters")
            .fetch_all(&self.pool)
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

    /// List all shards in the catalog.
    pub async fn list_shards(&self) -> Result<Vec<Shard>, sqlx::Error> {
        let rows = query("SELECT id, start_range, end_range FROM shards")
            .fetch_all(&self.pool)
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

    /// List all shard-owner mappings.
    pub async fn list_shard_owners(&self) -> Result<Vec<ShardOwner>, sqlx::Error> {
        let rows = query("SELECT shard_id, ingester_id FROM shard_owners")
            .fetch_all(&self.pool)
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

    /// Add a shard definition if not exists.
    pub async fn add_shard(&self, id: i32, start_range: i64, end_range: i64) -> Result<(), sqlx::Error> {
        let stmt = r#"
        INSERT INTO shards (id, start_range, end_range)
        VALUES ($1, $2, $3)
        ON CONFLICT (id) DO NOTHING
        "#;
        query(stmt)
            .bind(id)
            .bind(start_range)
            .bind(end_range)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    /// Assign an ingester as owner of a shard.
    pub async fn assign_shard(&self, shard_id: i32, ingester_id: Uuid) -> Result<(), sqlx::Error> {
        let stmt = r#"
        INSERT INTO shard_owners (shard_id, ingester_id)
        VALUES ($1, $2)
        ON CONFLICT (shard_id, ingester_id) DO NOTHING
        "#;
        query(stmt)
            .bind(shard_id)
            .bind(ingester_id)
            .execute(&self.pool)
            .await?;
        Ok(())
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