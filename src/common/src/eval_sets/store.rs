//! Catalog persistence for eval sets (`eval_sets` and `eval_cases` tables,
//! change: agent-offline-evals, design D7).
//!
//! Every operation is keyed by tenant *and* dataset name, so a set is
//! invisible outside the dataset it was created in. Cases keep their order
//! through an explicit `position` column; mutations that touch both tables
//! run in one transaction.

use std::collections::HashSet;

use chrono::{DateTime, SecondsFormat, SubsecRound, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Row, query};

use crate::catalog::{Catalog, parse_rfc3339};
use crate::eval_sets::{
    EvalCase, EvalCaseSource, EvalCaseSourceCounts, EvalSetListing, EvalSetRecord, EvalSetSpec,
    EvalSetSummary,
};

/// Most cases one set may hold.
pub const MAX_CASES_PER_SET: usize = 10_000;

const MAX_NAME_LEN: usize = 128;
const MAX_CASE_ID_LEN: usize = 128;

/// Rows per multi-row `INSERT` (11 binds each, well under both dialects'
/// bind-parameter limits).
const CASE_INSERT_CHUNK: usize = 500;

/// Errors from eval-set storage and mutation.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("eval set `{0}` already exists")]
    Conflict(String),
    #[error("eval set `{0}` not found")]
    NotFound(String),
    #[error("dataset `{0}` does not exist for this tenant")]
    UnknownDataset(String),
    #[error("invalid eval set: {0}")]
    Invalid(String),
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Result of appending cases to a set: ids already in the set are reported,
/// never overwritten.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct AppendCasesOutcome {
    pub added: u64,
    pub already_present: u64,
    /// Ids appended, in the order they now appear in the set.
    pub added_ids: Vec<String>,
    /// Ids skipped because the set already held them.
    pub already_present_ids: Vec<String>,
}

/// Checks an eval set name: a slug of lowercase letters, digits, `-`, `_`
/// and `.`, starting with a letter or digit, at most 128 characters.
pub fn validate_name(name: &str) -> Result<(), StoreError> {
    let mut chars = name.chars();
    let is_slug = matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit())
        && name.len() <= MAX_NAME_LEN
        && chars
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || matches!(c, '-' | '_' | '.'));
    if !is_slug {
        return Err(StoreError::Invalid(format!(
            "name `{name}` must match [a-z0-9][a-z0-9._-]{{0,127}}"
        )));
    }
    Ok(())
}

/// Validates `cases` and normalises them in place (trace ids lower-cased).
fn validate_cases(mut cases: Vec<EvalCase>) -> Result<Vec<EvalCase>, StoreError> {
    if cases.len() > MAX_CASES_PER_SET {
        return Err(StoreError::Invalid(format!(
            "{} cases exceed the limit of {MAX_CASES_PER_SET} per set",
            cases.len()
        )));
    }
    let mut seen = HashSet::with_capacity(cases.len());
    for case in &mut cases {
        if case.id.is_empty() || case.id.len() > MAX_CASE_ID_LEN {
            return Err(StoreError::Invalid(format!(
                "case id `{}` must be 1-{MAX_CASE_ID_LEN} bytes",
                case.id
            )));
        }
        if !seen.insert(case.id.as_str()) {
            return Err(StoreError::Invalid(format!(
                "case id `{}` appears more than once",
                case.id
            )));
        }
        if let EvalCaseSource::Trace { trace_id } = &mut case.source {
            if trace_id.len() != 32 || !trace_id.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(StoreError::Invalid(format!(
                    "case `{}`: trace_id `{trace_id}` must be 32 hex characters",
                    case.id
                )));
            }
            trace_id.make_ascii_lowercase();
        }
    }
    Ok(cases)
}

/// Validates `spec` and returns it with its cases normalised.
fn validate_spec(mut spec: EvalSetSpec) -> Result<EvalSetSpec, StoreError> {
    validate_name(&spec.name)?;
    if spec.agent.trim().is_empty() {
        return Err(StoreError::Invalid("agent must not be empty".to_string()));
    }
    spec.cases = validate_cases(spec.cases)?;
    Ok(spec)
}

/// A case flattened to its column values, borrowing from the case.
struct CaseRow<'a> {
    case_id: &'a str,
    position: i64,
    input: &'a str,
    expected_tools: String,
    reference: Option<&'a str>,
    tags: String,
    source_kind: &'static str,
    source_trace_id: Option<&'a str>,
}

fn encode_cases(cases: &[EvalCase], first_position: i64) -> Result<Vec<CaseRow<'_>>, StoreError> {
    let json = |v: &Vec<String>| {
        serde_json::to_string(v)
            .map_err(|e| StoreError::Invalid(format!("failed to serialize case: {e}")))
    };
    (first_position..)
        .zip(cases)
        .map(|(position, case)| {
            let (source_kind, source_trace_id) = match &case.source {
                EvalCaseSource::Trace { trace_id } => ("trace", Some(trace_id.as_str())),
                EvalCaseSource::Upload => ("upload", None),
                EvalCaseSource::HandWritten => ("hand_written", None),
            };
            Ok(CaseRow {
                case_id: &case.id,
                position,
                input: &case.input,
                expected_tools: json(&case.expected_tools)?,
                reference: case.reference.as_deref(),
                tags: json(&case.tags)?,
                source_kind,
                source_trace_id,
            })
        })
        .collect()
}

fn decode_case<R: Row>(row: &R) -> Result<EvalCase, StoreError>
where
    for<'a> &'a str: sqlx::ColumnIndex<R>,
    for<'a> String: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
    for<'a> Option<String>: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    let json = |column: &str| -> Result<Vec<String>, StoreError> {
        let raw: String = row.try_get(column)?;
        serde_json::from_str(&raw)
            .map_err(|e| StoreError::Invalid(format!("corrupt {column} column: {e}")))
    };
    let kind: String = row.try_get("source_kind")?;
    let trace_id: Option<String> = row.try_get("source_trace_id")?;
    let source = match (kind.as_str(), trace_id) {
        ("trace", Some(trace_id)) => EvalCaseSource::Trace { trace_id },
        ("trace", None) => {
            return Err(StoreError::Invalid(
                "corrupt source_trace_id column: NULL for a trace source".to_string(),
            ));
        }
        ("upload", _) => EvalCaseSource::Upload,
        ("hand_written", _) => EvalCaseSource::HandWritten,
        (other, _) => {
            return Err(StoreError::Invalid(format!(
                "corrupt source_kind column: `{other}`"
            )));
        }
    };
    Ok(EvalCase {
        id: row.try_get("case_id")?,
        input: row.try_get("input")?,
        expected_tools: json("expected_tools")?,
        reference: row.try_get("reference")?,
        tags: json("tags")?,
        source,
    })
}

/// Set columns plus the case count for one set; callers append the `WHERE`
/// clause.
const SUMMARY_SELECT: &str = "SELECT s.name, s.agent, s.description, s.created_at, s.updated_at, \
     (SELECT COUNT(*) FROM eval_cases c WHERE c.tenant_id = s.tenant_id \
      AND c.dataset = s.dataset AND c.set_name = s.name) AS case_count \
     FROM eval_sets s";

/// Every set of a tenant's dataset with its case count per source kind, in
/// one grouped pass over the dataset's cases, ordered by name. `$1` is the
/// tenant id and `$2` the dataset (numbered parameters work in SQLite too).
const LISTING_SELECT: &str = "SELECT s.name, s.agent, s.description, s.created_at, s.updated_at, \
     COALESCE(c.case_count, 0) AS case_count, \
     COALESCE(c.trace_cases, 0) AS trace_cases, \
     COALESCE(c.upload_cases, 0) AS upload_cases, \
     COALESCE(c.hand_written_cases, 0) AS hand_written_cases \
     FROM eval_sets s LEFT JOIN ( \
       SELECT set_name, COUNT(*) AS case_count, \
         SUM(CASE WHEN source_kind = 'trace' THEN 1 ELSE 0 END) AS trace_cases, \
         SUM(CASE WHEN source_kind = 'upload' THEN 1 ELSE 0 END) AS upload_cases, \
         SUM(CASE WHEN source_kind = 'hand_written' THEN 1 ELSE 0 END) AS hand_written_cases \
       FROM eval_cases WHERE tenant_id = $1 AND dataset = $2 GROUP BY set_name \
     ) c ON c.set_name = s.name \
     WHERE s.tenant_id = $1 AND s.dataset = $2 ORDER BY s.name";

fn source_counts<R: Row>(row: &R) -> Result<EvalCaseSourceCounts, StoreError>
where
    for<'a> &'a str: sqlx::ColumnIndex<R>,
    for<'a> i64: sqlx::Decode<'a, R::Database> + sqlx::Type<R::Database>,
{
    let count = |column: &str| -> Result<u64, StoreError> {
        Ok(u64::try_from(row.try_get::<i64, _>(column)?).unwrap_or(0))
    };
    Ok(EvalCaseSourceCounts {
        trace: count("trace_cases")?,
        upload: count("upload_cases")?,
        hand_written: count("hand_written_cases")?,
    })
}

fn sqlite_summary(row: &sqlx::sqlite::SqliteRow) -> Result<EvalSetSummary, StoreError> {
    Ok(EvalSetSummary {
        name: row.try_get("name")?,
        agent: row.try_get("agent")?,
        description: row.try_get("description")?,
        case_count: u64::try_from(row.try_get::<i64, _>("case_count")?).unwrap_or(0),
        created_at: parse_rfc3339(&row.try_get::<String, _>("created_at")?)?,
        updated_at: parse_rfc3339(&row.try_get::<String, _>("updated_at")?)?,
    })
}

fn pg_summary(row: &sqlx::postgres::PgRow) -> Result<EvalSetSummary, StoreError> {
    Ok(EvalSetSummary {
        name: row.try_get("name")?,
        agent: row.try_get("agent")?,
        description: row.try_get("description")?,
        case_count: u64::try_from(row.try_get::<i64, _>("case_count")?).unwrap_or(0),
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// The record a successful create/replace stored, built from the validated
/// spec rather than read back.
fn stored_record(
    tenant_id: &str,
    dataset: &str,
    spec: EvalSetSpec,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
) -> EvalSetRecord {
    EvalSetRecord {
        tenant_id: tenant_id.to_string(),
        dataset: dataset.to_string(),
        summary: EvalSetSummary {
            name: spec.name,
            agent: spec.agent,
            description: spec.description,
            case_count: spec.cases.len() as u64,
            created_at,
            updated_at,
        },
        cases: spec.cases,
    }
}

/// Current time at the precision both dialects store (Postgres
/// `TIMESTAMPTZ` keeps microseconds), so a returned record compares equal
/// to a later read.
fn now() -> DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

fn sqlite_ts(ts: DateTime<Utc>) -> String {
    ts.to_rfc3339_opts(SecondsFormat::Micros, true)
}

/// Batched `INSERT` of encoded case rows inside a transaction. A macro
/// rather than a generic function because `QueryBuilder`'s bind bounds
/// would otherwise have to be spelled out per database.
macro_rules! insert_case_rows {
    ($db:ty, $tx:expr, $tenant_id:expr, $dataset:expr, $set_name:expr, $rows:expr) => {{
        let (tenant_id, dataset, set_name): (&str, &str, &str) = ($tenant_id, $dataset, $set_name);
        for chunk in $rows.chunks(CASE_INSERT_CHUNK) {
            let mut builder = sqlx::QueryBuilder::<$db>::new(
                "INSERT INTO eval_cases (tenant_id, dataset, set_name, case_id, position, \
                 input, expected_tools, reference, tags, source_kind, source_trace_id) ",
            );
            builder.push_values(chunk, |mut b, row| {
                b.push_bind(tenant_id)
                    .push_bind(dataset)
                    .push_bind(set_name)
                    .push_bind(row.case_id)
                    .push_bind(row.position)
                    .push_bind(row.input)
                    .push_bind(row.expected_tools.as_str())
                    .push_bind(row.reference)
                    .push_bind(row.tags.as_str())
                    .push_bind(row.source_kind)
                    .push_bind(row.source_trace_id);
            });
            builder.build().execute(&mut *$tx).await?;
        }
    }};
}

const CASE_COLS: &str =
    "case_id, input, expected_tools, reference, tags, source_kind, source_trace_id";

impl Catalog {
    /// Every eval set of a tenant's dataset, without cases but with their
    /// case counts per source kind, ordered by name.
    pub async fn list_eval_sets(
        &self,
        tenant_id: &str,
        dataset: &str,
    ) -> Result<Vec<EvalSetListing>, StoreError> {
        match self {
            Catalog::Sqlite(pool) => query(LISTING_SELECT)
                .bind(tenant_id)
                .bind(dataset)
                .fetch_all(pool)
                .await?
                .iter()
                .map(|row| {
                    Ok(EvalSetListing {
                        summary: sqlite_summary(row)?,
                        sources: source_counts(row)?,
                    })
                })
                .collect(),
            Catalog::Postgres(pool) => query(LISTING_SELECT)
                .bind(tenant_id)
                .bind(dataset)
                .fetch_all(pool)
                .await?
                .iter()
                .map(|row| {
                    Ok(EvalSetListing {
                        summary: pg_summary(row)?,
                        sources: source_counts(row)?,
                    })
                })
                .collect(),
        }
    }

    /// One eval set with its cases in order, if present. The set and its
    /// cases are read in one transaction, so they come from one snapshot.
    pub async fn get_eval_set(
        &self,
        tenant_id: &str,
        dataset: &str,
        name: &str,
    ) -> Result<Option<EvalSetRecord>, StoreError> {
        let (summary, cases) = match self {
            Catalog::Sqlite(pool) => {
                let mut tx = pool.begin().await?;
                let sql = format!(
                    "{SUMMARY_SELECT} WHERE s.tenant_id = ? AND s.dataset = ? AND s.name = ?"
                );
                let Some(row) = query(&sql)
                    .bind(tenant_id)
                    .bind(dataset)
                    .bind(name)
                    .fetch_optional(&mut *tx)
                    .await?
                else {
                    return Ok(None);
                };
                let sql = format!(
                    "SELECT {CASE_COLS} FROM eval_cases \
                     WHERE tenant_id = ? AND dataset = ? AND set_name = ? ORDER BY position"
                );
                let cases = query(&sql)
                    .bind(tenant_id)
                    .bind(dataset)
                    .bind(name)
                    .fetch_all(&mut *tx)
                    .await?
                    .iter()
                    .map(decode_case)
                    .collect::<Result<Vec<_>, _>>()?;
                tx.commit().await?;
                (sqlite_summary(&row)?, cases)
            }
            Catalog::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                // READ COMMITTED would give each statement its own snapshot.
                query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
                    .execute(&mut *tx)
                    .await?;
                let sql = format!(
                    "{SUMMARY_SELECT} WHERE s.tenant_id = $1 AND s.dataset = $2 AND s.name = $3"
                );
                let Some(row) = query(&sql)
                    .bind(tenant_id)
                    .bind(dataset)
                    .bind(name)
                    .fetch_optional(&mut *tx)
                    .await?
                else {
                    return Ok(None);
                };
                let sql = format!(
                    "SELECT {CASE_COLS} FROM eval_cases \
                     WHERE tenant_id = $1 AND dataset = $2 AND set_name = $3 ORDER BY position"
                );
                let cases = query(&sql)
                    .bind(tenant_id)
                    .bind(dataset)
                    .bind(name)
                    .fetch_all(&mut *tx)
                    .await?
                    .iter()
                    .map(decode_case)
                    .collect::<Result<Vec<_>, _>>()?;
                tx.commit().await?;
                (pg_summary(&row)?, cases)
            }
        };
        Ok(Some(EvalSetRecord {
            tenant_id: tenant_id.to_string(),
            dataset: dataset.to_string(),
            summary,
            cases,
        }))
    }

    /// Create a set. Fails with [`StoreError::Conflict`] when the name is
    /// taken in this tenant's dataset and [`StoreError::UnknownDataset`]
    /// when the dataset does not exist.
    pub async fn insert_eval_set(
        &self,
        tenant_id: &str,
        dataset: &str,
        spec: EvalSetSpec,
    ) -> Result<EvalSetRecord, StoreError> {
        let spec = validate_spec(spec)?;
        if !self.dataset_exists(tenant_id, dataset).await? {
            return Err(StoreError::UnknownDataset(dataset.to_string()));
        }
        let rows = encode_cases(&spec.cases, 0)?;
        let now = now();
        let conflict = |e: sqlx::Error| match e {
            sqlx::Error::Database(db) if db.is_unique_violation() => {
                StoreError::Conflict(spec.name.clone())
            }
            e => e.into(),
        };
        match self {
            Catalog::Sqlite(pool) => {
                let mut tx = pool.begin().await?;
                query(
                    "INSERT INTO eval_sets \
                     (tenant_id, dataset, name, agent, description, created_at, updated_at) \
                     VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(tenant_id)
                .bind(dataset)
                .bind(&spec.name)
                .bind(&spec.agent)
                .bind(&spec.description)
                .bind(sqlite_ts(now))
                .bind(sqlite_ts(now))
                .execute(&mut *tx)
                .await
                .map_err(conflict)?;
                insert_case_rows!(sqlx::Sqlite, tx, tenant_id, dataset, &spec.name, rows);
                tx.commit().await?;
            }
            Catalog::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                query(
                    "INSERT INTO eval_sets \
                     (tenant_id, dataset, name, agent, description, created_at, updated_at) \
                     VALUES ($1, $2, $3, $4, $5, $6, $7)",
                )
                .bind(tenant_id)
                .bind(dataset)
                .bind(&spec.name)
                .bind(&spec.agent)
                .bind(&spec.description)
                .bind(now)
                .bind(now)
                .execute(&mut *tx)
                .await
                .map_err(conflict)?;
                insert_case_rows!(sqlx::Postgres, tx, tenant_id, dataset, &spec.name, rows);
                tx.commit().await?;
            }
        }
        Ok(stored_record(tenant_id, dataset, spec, now, now))
    }

    /// Replace a set's agent, description and every case. Never upserts:
    /// [`StoreError::NotFound`] if `name` does not exist. `spec.name` must
    /// equal `name`.
    pub async fn replace_eval_set(
        &self,
        tenant_id: &str,
        dataset: &str,
        name: &str,
        spec: EvalSetSpec,
    ) -> Result<EvalSetRecord, StoreError> {
        let spec = validate_spec(spec)?;
        if spec.name != name {
            return Err(StoreError::Invalid(format!(
                "body name `{}` does not match path name `{name}`",
                spec.name
            )));
        }
        let rows = encode_cases(&spec.cases, 0)?;
        let now = now();
        let not_found = || StoreError::NotFound(name.to_string());
        let created_at = match self {
            Catalog::Sqlite(pool) => {
                let mut tx = pool.begin().await?;
                let created_at: String = query(
                    "UPDATE eval_sets SET agent = ?, description = ?, updated_at = ? \
                     WHERE tenant_id = ? AND dataset = ? AND name = ? RETURNING created_at",
                )
                .bind(&spec.agent)
                .bind(&spec.description)
                .bind(sqlite_ts(now))
                .bind(tenant_id)
                .bind(dataset)
                .bind(name)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(not_found)?
                .try_get("created_at")?;
                query(
                    "DELETE FROM eval_cases WHERE tenant_id = ? AND dataset = ? AND set_name = ?",
                )
                .bind(tenant_id)
                .bind(dataset)
                .bind(name)
                .execute(&mut *tx)
                .await?;
                insert_case_rows!(sqlx::Sqlite, tx, tenant_id, dataset, name, rows);
                tx.commit().await?;
                parse_rfc3339(&created_at)?
            }
            Catalog::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                let created_at: DateTime<Utc> = query(
                    "UPDATE eval_sets SET agent = $1, description = $2, updated_at = $3 \
                     WHERE tenant_id = $4 AND dataset = $5 AND name = $6 RETURNING created_at",
                )
                .bind(&spec.agent)
                .bind(&spec.description)
                .bind(now)
                .bind(tenant_id)
                .bind(dataset)
                .bind(name)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or_else(not_found)?
                .try_get("created_at")?;
                query(
                    "DELETE FROM eval_cases WHERE tenant_id = $1 AND dataset = $2 AND set_name = $3",
                )
                .bind(tenant_id)
                .bind(dataset)
                .bind(name)
                .execute(&mut *tx)
                .await?;
                insert_case_rows!(sqlx::Postgres, tx, tenant_id, dataset, name, rows);
                tx.commit().await?;
                created_at
            }
        };
        Ok(stored_record(tenant_id, dataset, spec, created_at, now))
    }

    /// Delete a set and its cases; `Ok(false)` if it did not exist.
    pub async fn delete_eval_set(
        &self,
        tenant_id: &str,
        dataset: &str,
        name: &str,
    ) -> Result<bool, StoreError> {
        let affected = match self {
            Catalog::Sqlite(pool) => {
                query("DELETE FROM eval_sets WHERE tenant_id = ? AND dataset = ? AND name = ?")
                    .bind(tenant_id)
                    .bind(dataset)
                    .bind(name)
                    .execute(pool)
                    .await?
                    .rows_affected()
            }
            Catalog::Postgres(pool) => {
                query("DELETE FROM eval_sets WHERE tenant_id = $1 AND dataset = $2 AND name = $3")
                    .bind(tenant_id)
                    .bind(dataset)
                    .bind(name)
                    .execute(pool)
                    .await?
                    .rows_affected()
            }
        };
        Ok(affected > 0)
    }

    /// Append `cases` after the set's last case, in one transaction. Cases
    /// whose id the set already holds are skipped and reported, never
    /// overwritten; `updated_at` moves only when something was added.
    pub async fn append_eval_cases(
        &self,
        tenant_id: &str,
        dataset: &str,
        name: &str,
        cases: Vec<EvalCase>,
    ) -> Result<AppendCasesOutcome, StoreError> {
        let cases = validate_cases(cases)?;
        let now = now();
        // Runs after the caller has locked the set (and so checked it
        // exists) inside `$tx`, so concurrent appends to one set serialise
        // instead of racing for the same positions.
        macro_rules! append {
            ($db:ty, $tx:ident, $existing:expr, $touch:expr, $ts:expr) => {{
                let mut existing = HashSet::new();
                let mut next_position = 0_i64;
                for row in query($existing)
                    .bind(tenant_id)
                    .bind(dataset)
                    .bind(name)
                    .fetch_all(&mut *$tx)
                    .await?
                {
                    existing.insert(row.try_get::<String, _>("case_id")?);
                    next_position = next_position.max(row.try_get::<i64, _>("position")? + 1);
                }
                let (present, fresh): (Vec<EvalCase>, Vec<EvalCase>) =
                    cases.into_iter().partition(|c| existing.contains(&c.id));
                if existing.len() + fresh.len() > MAX_CASES_PER_SET {
                    return Err(StoreError::Invalid(format!(
                        "appending {} cases to {} would exceed the limit of \
                         {MAX_CASES_PER_SET} per set",
                        fresh.len(),
                        existing.len()
                    )));
                }
                if !fresh.is_empty() {
                    let rows = encode_cases(&fresh, next_position)?;
                    insert_case_rows!($db, $tx, tenant_id, dataset, name, rows);
                    query($touch)
                        .bind($ts)
                        .bind(tenant_id)
                        .bind(dataset)
                        .bind(name)
                        .execute(&mut *$tx)
                        .await?;
                }
                $tx.commit().await?;
                AppendCasesOutcome {
                    added: fresh.len() as u64,
                    already_present: present.len() as u64,
                    added_ids: fresh.into_iter().map(|c| c.id).collect(),
                    already_present_ids: present.into_iter().map(|c| c.id).collect(),
                }
            }};
        }
        let outcome = match self {
            Catalog::Sqlite(pool) => {
                let mut tx = pool.begin().await?;
                // A no-op UPDATE takes SQLite's write lock up front.
                let locked = query(
                    "UPDATE eval_sets SET updated_at = updated_at \
                     WHERE tenant_id = ? AND dataset = ? AND name = ?",
                )
                .bind(tenant_id)
                .bind(dataset)
                .bind(name)
                .execute(&mut *tx)
                .await?
                .rows_affected();
                if locked == 0 {
                    return Err(StoreError::NotFound(name.to_string()));
                }
                append!(
                    sqlx::Sqlite,
                    tx,
                    "SELECT case_id, position FROM eval_cases \
                     WHERE tenant_id = ? AND dataset = ? AND set_name = ?",
                    "UPDATE eval_sets SET updated_at = ? \
                     WHERE tenant_id = ? AND dataset = ? AND name = ?",
                    sqlite_ts(now)
                )
            }
            Catalog::Postgres(pool) => {
                let mut tx = pool.begin().await?;
                let locked = query(
                    "SELECT 1 FROM eval_sets \
                     WHERE tenant_id = $1 AND dataset = $2 AND name = $3 FOR UPDATE",
                )
                .bind(tenant_id)
                .bind(dataset)
                .bind(name)
                .fetch_optional(&mut *tx)
                .await?;
                if locked.is_none() {
                    return Err(StoreError::NotFound(name.to_string()));
                }
                append!(
                    sqlx::Postgres,
                    tx,
                    "SELECT case_id, position FROM eval_cases \
                     WHERE tenant_id = $1 AND dataset = $2 AND set_name = $3",
                    "UPDATE eval_sets SET updated_at = $1 \
                     WHERE tenant_id = $2 AND dataset = $3 AND name = $4",
                    now
                )
            }
        };
        Ok(outcome)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(id: &str) -> EvalCase {
        EvalCase {
            id: id.to_string(),
            input: "hi".to_string(),
            expected_tools: Vec::new(),
            reference: None,
            tags: Vec::new(),
            source: EvalCaseSource::HandWritten,
        }
    }

    #[test]
    fn names_are_slugs_of_at_most_128_characters() {
        for ok in ["a", "0", "refund_edge.cases-40", &"a".repeat(128)] {
            assert!(validate_name(ok).is_ok(), "{ok:?}");
        }
        for bad in ["", "-a", ".a", "_a", "A", "a b", "a/b", &"a".repeat(129)] {
            assert!(validate_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn cases_reject_duplicate_empty_and_overlong_ids() {
        assert!(validate_cases(vec![case("a"), case("b")]).is_ok());
        assert!(validate_cases(vec![case("a"), case("a")]).is_err());
        assert!(validate_cases(vec![case("")]).is_err());
        assert!(validate_cases(vec![case(&"x".repeat(129))]).is_err());
    }

    #[test]
    fn trace_ids_must_be_32_hex_and_are_lower_cased() {
        let mut traced = case("t");
        traced.source = EvalCaseSource::Trace {
            trace_id: "4BF92F3577B34DA6A3CE929D0E0E4736".to_string(),
        };
        let normalised = validate_cases(vec![traced.clone()]).expect("valid");
        assert_eq!(
            normalised[0].source,
            EvalCaseSource::Trace {
                trace_id: "4bf92f3577b34da6a3ce929d0e0e4736".to_string()
            }
        );
        for bad in ["", "4bf9", "g".repeat(32).as_str()] {
            traced.source = EvalCaseSource::Trace {
                trace_id: bad.to_string(),
            };
            assert!(validate_cases(vec![traced.clone()]).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_set_holds_at_most_max_cases() {
        let cases: Vec<EvalCase> = (0..=MAX_CASES_PER_SET)
            .map(|i| case(&format!("c{i}")))
            .collect();
        assert!(validate_cases(cases[..MAX_CASES_PER_SET].to_vec()).is_ok());
        assert!(validate_cases(cases).is_err());
    }

    #[test]
    fn encoded_cases_get_consecutive_positions_from_the_start() {
        let cases = [case("a"), case("b")];
        let rows = encode_cases(&cases, 7).expect("encode");
        let positions: Vec<i64> = rows.iter().map(|r| r.position).collect();
        assert_eq!(positions, vec![7, 8]);
        assert_eq!(rows[0].expected_tools, "[]");
        assert_eq!(rows[0].source_kind, "hand_written");
    }

    #[tokio::test]
    async fn a_trace_case_without_a_trace_id_reports_the_trace_id_column() {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:")
            .await
            .expect("pool");
        let row = query(
            "SELECT 'a' AS case_id, 'hi' AS input, '[]' AS expected_tools, \
             NULL AS reference, '[]' AS tags, 'trace' AS source_kind, \
             NULL AS source_trace_id",
        )
        .fetch_one(&pool)
        .await
        .expect("row");
        let err = decode_case(&row).expect_err("corrupt");
        assert!(err.to_string().contains("source_trace_id"), "{err}");
    }
}
