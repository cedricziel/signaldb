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
    use crate::schema_transform::create_metrics_gauge_arrow_schema;
    use datafusion::arrow::array::{
        Date32Array, Float64Array, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray,
    };
    use std::sync::Arc;

    /// A schema-valid `metrics_gauge` batch serialized for WAL append, so a
    /// test can drive a real Iceberg commit through the writer.
    pub(crate) fn metrics_gauge_bytes(num_rows: usize) -> Vec<u8> {
        let schema = create_metrics_gauge_arrow_schema();

        let timestamps: Vec<i64> = (0..num_rows)
            .map(|i| 1_000_000_000 + (i as i64 * 1_000_000))
            .collect();
        let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampNanosecondArray::from(timestamps)),
                Arc::new(TimestampNanosecondArray::from(vec![None; num_rows])),
                Arc::new(StringArray::from(vec!["coalesce-test"; num_rows])),
                Arc::new(StringArray::from(vec!["test.metric"; num_rows])),
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
                Arc::new(Float64Array::from(values)),
                Arc::new(Int32Array::from(vec![None; num_rows])),
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
                Arc::new(Int32Array::from(vec![None; num_rows])),
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
                Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
                Arc::new(Date32Array::from(vec![19000; num_rows])),
                Arc::new(Int32Array::from(vec![10; num_rows])),
            ],
        )
        .unwrap();

        common::wal::record_batch_to_bytes(&batch).unwrap()
    }

    /// An Arrow-valid batch whose schema matches no target table, so committing
    /// it fails during schema coercion — used to exercise commit-failure paths.
    pub(crate) fn schema_mismatched_bytes() -> Vec<u8> {
        use datafusion::arrow::array::Int32Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1, 2, 3]))]).unwrap();
        common::wal::record_batch_to_bytes(&batch).unwrap()
    }
}
