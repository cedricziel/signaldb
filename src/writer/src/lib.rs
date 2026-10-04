pub mod storage;
pub use storage::{IcebergTableWriter, RetryConfig};

pub mod cli;
pub mod processor;
pub use processor::{FlushScope, ProcessorStats, WalProcessor};

pub mod flight_iceberg;
pub use flight_iceberg::IcebergWriterFlightService;

pub mod reconcile;
pub use reconcile::{ReconcilePassSummary, TableReconciler};

/// Internal to the writer: both of its paths (Flight ingest, WAL commit) route
/// through it, nothing outside the crate does.
pub(crate) mod routing;

pub mod schema_transform;

/// Shared test helpers for the writer crate.
#[cfg(test)]
pub(crate) mod test_support {
    use datafusion::arrow::array::RecordBatch;
    use std::sync::Arc;

    /// A wire-format gauge metrics batch serialized for WAL append, so a
    /// test can drive a real Iceberg commit through the writer. An alias for
    /// [`metrics_wire_bytes`] kept under its own name where call sites read
    /// better naming the legacy per-type target table they route through
    /// (the in-flight redirect to the wide `metrics` table, #1926-class).
    pub(crate) fn metrics_gauge_bytes(num_rows: usize) -> bytes::Bytes {
        metrics_wire_bytes(num_rows)
    }

    /// A wire-format gauge metrics batch, built from a real OTLP request via
    /// [`common::flight::conversion::otlp_metrics_to_arrow`].
    pub(crate) fn metrics_wire_bytes(num_rows: usize) -> bytes::Bytes {
        let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();
        let request = common::testing::gauge_metrics_request_with_values(
            "test.metric",
            "coalesce-test",
            &values,
        );
        let batch = common::flight::conversion::otlp_metrics_to_arrow(&request).unwrap();
        common::wal::record_batch_to_bytes(&batch).unwrap()
    }

    /// An Arrow-valid batch whose schema matches no target table, so committing
    /// it fails during schema coercion — used to exercise commit-failure paths.
    pub(crate) fn schema_mismatched_bytes() -> bytes::Bytes {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        common::wal::record_batch_to_bytes(&batch).unwrap()
    }
}
