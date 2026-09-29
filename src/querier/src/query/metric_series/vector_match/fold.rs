//! Folds operand input batches into the state [`super::eval`] matches over.

use std::collections::{BTreeMap, HashMap, btree_map};

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Float64Type, TimeUnit, TimestampNanosecondType};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result as DFResult};

use super::eval::{MAX_MATCH_SERIES, MatchSpec, NAME_LABEL, SeriesMeta, Side, canonical, invalid};
use super::{BUCKET, LABELS, VALUE};

/// Folds one Series input into a [`Side`] batch by batch.
#[derive(Default)]
pub(super) struct SideBuilder {
    ids: HashMap<String, usize>,
    side: Side,
}

impl SideBuilder {
    /// Adds `batch`, returning an estimate of the bytes the state grew by.
    pub(super) fn fold(
        &mut self,
        spec: &MatchSpec,
        batch: &RecordBatch,
        side: &str,
    ) -> DFResult<usize> {
        let buckets = column(batch, BUCKET, &bucket_type())?;
        let buckets = buckets.as_primitive::<TimestampNanosecondType>();
        let labels = column(batch, LABELS, &DataType::Utf8)?;
        let labels = labels.as_string::<i32>();
        let values = column(batch, VALUE, &DataType::Float64)?;
        let values = values.as_primitive::<Float64Type>();
        let mut grown = 0;
        for row in 0..batch.num_rows() {
            if buckets.is_null(row) || values.is_null(row) {
                continue;
            }
            let raw = if labels.is_null(row) {
                "{}"
            } else {
                labels.value(row)
            };
            let idx = match self.ids.get(raw) {
                Some(idx) => *idx,
                None => {
                    grown += self.add_series(spec, raw, side)?;
                    self.side.series.len() - 1
                }
            };
            let sample = (idx, values.value(row));
            match self.side.samples.entry(buckets.value(row)) {
                btree_map::Entry::Occupied(mut e) => e.get_mut().push(sample),
                btree_map::Entry::Vacant(e) => {
                    grown += 64;
                    e.insert(vec![sample]);
                }
            }
            grown += size_of::<(usize, f64)>();
        }
        Ok(grown)
    }

    fn add_series(&mut self, spec: &MatchSpec, raw: &str, side: &str) -> DFResult<usize> {
        if self.side.series.len() >= MAX_MATCH_SERIES {
            return Err(invalid(format!(
                "binop {side} operand holds more than {MAX_MATCH_SERIES} series; \
                 narrow the selection"
            )));
        }
        let labels: BTreeMap<String, String> = serde_json::from_str(raw)
            .map_err(|e| DataFusionError::Execution(format!("invalid `{LABELS}` {raw}: {e}")))?;
        let mut unnamed = labels.clone();
        unnamed.remove(NAME_LABEL);
        let meta = SeriesMeta {
            raw: canonical(&labels)?.into(),
            unnamed: canonical(&unnamed)?.into(),
            key: spec.key(&labels)?,
            labels,
        };
        let label_bytes: usize = meta
            .labels
            .iter()
            .map(|(k, v)| k.len() + v.len() + 64)
            .sum();
        let grown = 5 * raw.len() + meta.key.len() + label_bytes + 128;
        self.ids.insert(raw.to_string(), self.side.series.len());
        self.side.series.push(meta);
        Ok(grown)
    }

    /// The folded side; two rows for one series at one bucket is an input bug.
    pub(super) fn finish(mut self, side: &str) -> DFResult<Side> {
        for (bucket, samples) in &mut self.side.samples {
            samples.sort_unstable_by_key(|s| s.0);
            if let Some(dup) = samples.windows(2).find(|w| w[0].0 == w[1].0) {
                return Err(DataFusionError::Internal(format!(
                    "binop {side} input holds two rows for {} at bucket {bucket}",
                    self.side.series[dup[0].0].raw
                )));
            }
        }
        Ok(self.side)
    }
}

/// Folds one Scalar input batch into `out`, returning the bytes it grew by.
pub(super) fn fold_scalar(out: &mut BTreeMap<i64, f64>, batch: &RecordBatch) -> DFResult<usize> {
    let buckets = column(batch, BUCKET, &bucket_type())?;
    let buckets = buckets.as_primitive::<TimestampNanosecondType>();
    let values = column(batch, VALUE, &DataType::Float64)?;
    let values = values.as_primitive::<Float64Type>();
    let mut grown = 0;
    for row in 0..batch.num_rows() {
        if buckets.is_valid(row) && values.is_valid(row) {
            let bucket = buckets.value(row);
            if out.insert(bucket, values.value(row)).is_some() {
                return Err(DataFusionError::Internal(format!(
                    "binop scalar input holds two rows at bucket {bucket}"
                )));
            }
            grown += 64;
        }
    }
    Ok(grown)
}

fn column(batch: &RecordBatch, name: &str, ty: &DataType) -> DFResult<ArrayRef> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| DataFusionError::Internal(format!("binop input lacks `{name}`")))?;
    Ok(cast(col, ty)?)
}

fn bucket_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}
