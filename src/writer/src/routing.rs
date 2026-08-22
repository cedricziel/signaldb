//! Where a batch belongs: the one place that turns the routing metadata a
//! batch carries into an `(tenant, dataset, table)` destination.
//!
//! Both writer paths route the same batch, at different times: `do_put` routes
//! it on arrival to pick its WAL, and the WAL processor routes it again when it
//! commits (live, or on replay after a restart). Any difference between the two
//! rules is silent and only shows up as rows under a tenant nobody asked for —
//! #1319, where ingest trimmed and defaulted the metadata ids and replay did
//! not, so `tenant_id: ""` landed in the `default/default` WAL and committed to
//! Iceberg tenant `""`. Both paths therefore call [`route`], and no path
//! reimplements a piece of it.

use common::auth::validation::{self, ValidationError};
use common::iceberg::schemas::TableSchema;
use common::wal::WalOperation;

/// The table a metrics batch goes to when its metadata names none.
const DEFAULT_METRICS_TABLE: &str = "metrics_gauge";

/// Whether `table_name` is one of the metrics signal tables — the only
/// tables a `WriteMetrics` batch may target.
fn is_known_metrics_table(table_name: &str) -> bool {
    matches!(
        TableSchema::from_table_name(table_name),
        Some(
            TableSchema::MetricsGauge
                | TableSchema::MetricsSum
                | TableSchema::MetricsHistogram
                | TableSchema::MetricsExponentialHistogram
                | TableSchema::MetricsSummary
        )
    )
}

/// A batch's destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTarget {
    pub tenant_id: String,
    pub dataset_id: String,
    pub table_name: String,
}

/// The routing metadata a batch carries, as the sender supplied it: untrimmed,
/// possibly blank, possibly absent. On the ingest path it comes from the Flight
/// app metadata; on the commit path from the same metadata as persisted in the
/// WAL entry.
#[derive(Debug, Default, Clone, Copy)]
pub struct RouteMetadata<'a> {
    pub tenant_id: Option<&'a str>,
    pub dataset_id: Option<&'a str>,
    /// Honoured for metrics only, which fan out across several tables
    /// (`metrics_gauge`, `metrics_sum`, ...). Every other signal has exactly
    /// one table, so a `target_table` there is ignored.
    pub target_table: Option<&'a str>,
}

/// Why a batch could not be routed. Every variant is deterministic in its
/// input, so a retry of the same batch fails identically: on ingest that makes
/// it `invalid_argument` (never the retryable `internal`, see #1060), and on
/// replay it makes the entry poison rather than a transient failure.
#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    #[error("invalid {kind} id {id:?}: {source}")]
    InvalidId {
        kind: &'static str,
        id: String,
        #[source]
        source: ValidationError,
    },
    #[error("Flush operations should not be routed as table writes")]
    NotRoutable,
    #[error("unknown metrics table {0:?}")]
    UnknownTable(String),
}

/// Route one batch.
///
/// `fallback_tenant`/`fallback_dataset` supply the id when the metadata names
/// none or names a blank one. The ingest path passes the deployment defaults;
/// the commit path passes the ids of the WAL the entry lives in, which are
/// themselves what this function returned on ingest — so both paths agree on
/// every input, including ids the metadata never carried.
///
/// The ids become path components (WAL directory, Iceberg namespace) and this
/// Flight surface can be configured without authentication, so they are
/// validated here rather than trusted.
pub fn route(
    operation: &WalOperation,
    metadata: RouteMetadata<'_>,
    fallback_tenant: &str,
    fallback_dataset: &str,
) -> Result<RouteTarget, RoutingError> {
    let table_name = match operation {
        WalOperation::WriteTraces => "traces".to_string(),
        WalOperation::WriteLogs => "logs".to_string(),
        WalOperation::WriteProfiles => "profiles".to_string(),
        WalOperation::WriteMetrics => {
            let table = present(metadata.target_table).unwrap_or(DEFAULT_METRICS_TABLE);
            if !is_known_metrics_table(table) {
                return Err(RoutingError::UnknownTable(table.to_string()));
            }
            table.to_string()
        }
        // A `Flush` marker carries no data; the processor force-commits its
        // scope and marks it processed without ever routing it.
        WalOperation::Flush => return Err(RoutingError::NotRoutable),
    };

    Ok(RouteTarget {
        tenant_id: normalize_id("tenant", metadata.tenant_id, fallback_tenant)?,
        dataset_id: normalize_id("dataset", metadata.dataset_id, fallback_dataset)?,
        table_name,
    })
}

/// The supplied value trimmed, or `None` when absent or blank.
fn present(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|v| !v.is_empty())
}

fn normalize_id(
    kind: &'static str,
    value: Option<&str>,
    fallback: &str,
) -> Result<String, RoutingError> {
    let id = present(value).unwrap_or(fallback);
    validation::validate_id(id).map_err(|source| RoutingError::InvalidId {
        kind,
        id: id.to_string(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::bootstrap::{DEFAULT_DATASET_ID, DEFAULT_TENANT_ID};

    /// Route the way `do_put` does: the sender's metadata against the
    /// deployment defaults.
    fn on_ingest(operation: &WalOperation, metadata: RouteMetadata<'_>) -> RouteTarget {
        route(operation, metadata, DEFAULT_TENANT_ID, DEFAULT_DATASET_ID).unwrap()
    }

    /// Route the way the commit path does: the same metadata, against the ids
    /// of the WAL the ingest path chose for it.
    fn on_replay(
        operation: &WalOperation,
        metadata: RouteMetadata<'_>,
        wal: &RouteTarget,
    ) -> RouteTarget {
        route(operation, metadata, &wal.tenant_id, &wal.dataset_id).unwrap()
    }

    /// #1319: whatever the sender supplied, committing live and replaying
    /// after a restart must reach the same table.
    #[test]
    fn ingest_and_replay_agree_on_every_shape_of_id() {
        let cases = [
            (None, None),
            (Some("acme"), Some("production")),
            // Padded: ingest trims to pick the WAL, so replay must trim too.
            (Some(" acme "), Some("\tproduction\n")),
            // Blank: ingest falls back to the defaults.
            (Some(""), Some("   ")),
            // One supplied, one not.
            (Some("acme"), None),
        ];

        for (tenant_id, dataset_id) in cases {
            let metadata = RouteMetadata {
                tenant_id,
                dataset_id,
                target_table: None,
            };
            let ingest = on_ingest(&WalOperation::WriteTraces, metadata);
            let replay = on_replay(&WalOperation::WriteTraces, metadata, &ingest);
            assert_eq!(
                ingest, replay,
                "disagreement on {tenant_id:?}/{dataset_id:?}"
            );
        }
    }

    #[test]
    fn blank_and_padded_ids_normalize() {
        let target = on_ingest(
            &WalOperation::WriteTraces,
            RouteMetadata {
                tenant_id: Some(" acme "),
                dataset_id: Some(""),
                target_table: None,
            },
        );
        assert_eq!(target.tenant_id, "acme");
        assert_eq!(target.dataset_id, DEFAULT_DATASET_ID);
    }

    #[test]
    fn each_signal_routes_to_its_table() {
        for (operation, table) in [
            (WalOperation::WriteTraces, "traces"),
            (WalOperation::WriteLogs, "logs"),
            (WalOperation::WriteProfiles, "profiles"),
            (WalOperation::WriteMetrics, DEFAULT_METRICS_TABLE),
        ] {
            let target = on_ingest(&operation, RouteMetadata::default());
            assert_eq!(target.table_name, table);
        }
    }

    #[test]
    fn metrics_honour_target_table_and_fall_back_to_gauge() {
        let with_table = on_ingest(
            &WalOperation::WriteMetrics,
            RouteMetadata {
                target_table: Some("metrics_exponential_histogram"),
                ..Default::default()
            },
        );
        assert_eq!(with_table.table_name, "metrics_exponential_histogram");

        // Absent or blank falls back rather than routing to a nameless table.
        for target_table in [None, Some(""), Some("  ")] {
            let target = on_ingest(
                &WalOperation::WriteMetrics,
                RouteMetadata {
                    target_table,
                    ..Default::default()
                },
            );
            assert_eq!(target.table_name, DEFAULT_METRICS_TABLE);
        }
    }

    /// W4: an unknown `target_table` can never commit (`IcebergTableWriter`
    /// fails to open it on every cycle), so it must be rejected at routing
    /// time rather than accepted and dead-lettered later.
    #[test]
    fn unknown_metrics_table_is_rejected() {
        let err = route(
            &WalOperation::WriteMetrics,
            RouteMetadata {
                target_table: Some("metrics_bogus"),
                ..Default::default()
            },
            DEFAULT_TENANT_ID,
            DEFAULT_DATASET_ID,
        )
        .unwrap_err();
        assert!(
            matches!(err, RoutingError::UnknownTable(ref t) if t == "metrics_bogus"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn non_metric_signals_ignore_target_table() {
        let target = on_ingest(
            &WalOperation::WriteLogs,
            RouteMetadata {
                target_table: Some("metrics_gauge"),
                ..Default::default()
            },
        );
        assert_eq!(target.table_name, "logs");
    }

    #[test]
    fn a_malformed_id_is_rejected_not_sanitized() {
        let err = route(
            &WalOperation::WriteTraces,
            RouteMetadata {
                tenant_id: Some("../etc"),
                ..Default::default()
            },
            DEFAULT_TENANT_ID,
            DEFAULT_DATASET_ID,
        )
        .unwrap_err();
        assert!(
            matches!(err, RoutingError::InvalidId { kind: "tenant", .. }),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn flush_markers_are_not_routable() {
        let err = route(
            &WalOperation::Flush,
            RouteMetadata::default(),
            DEFAULT_TENANT_ID,
            DEFAULT_DATASET_ID,
        )
        .unwrap_err();
        assert!(matches!(err, RoutingError::NotRoutable));
    }
}
