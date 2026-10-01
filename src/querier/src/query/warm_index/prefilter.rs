//! Reads each candidate file's `attr_index.list.item` bloom filter (through
//! the shared metadata cache; see `spike/warm-index.md` for why DataFusion
//! can't do this itself) to prune [`super::probe::probe_clauses`]' output.
//! Never a false negative: anything unreadable is kept.
#![cfg_attr(not(test), allow(dead_code))]

use std::sync::Arc;

use common::attrs::warm_index::WARM_INDEX_COLUMN;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::ParquetFileReaderFactory;
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::parquet::arrow::ParquetRecordBatchStreamBuilder;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::{StreamExt, stream};
use object_store::ObjectStore;

use super::probe::ProbeClause;

/// Selectivity knobs for [`prefilter_files`].
#[derive(Debug, Clone)]
pub(crate) struct WarmIndexGate {
    /// Fewer candidate files than this and probing isn't worth the I/O.
    pub min_files: usize,
    /// Files to sample before committing to a full probe.
    pub sample_files: usize,
    /// Sample keep ratio above which the predicate isn't selective enough.
    pub max_keep_ratio: f64,
    /// Bound on in-flight per-file probes.
    pub probe_concurrency: usize,
}

impl Default for WarmIndexGate {
    fn default() -> Self {
        Self {
            min_files: 4,
            sample_files: 16,
            max_keep_ratio: 0.5,
            probe_concurrency: 16,
        }
    }
}

/// Summary of one [`prefilter_files`] call, for logging/metrics.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct PrefilterOutcome {
    /// Files actually probed (0 when the gate skipped probing entirely).
    pub probed: usize,
    pub pruned_files: usize,
    pub pruned_row_groups: usize,
    /// Set when the gate kept everything without a full probe.
    pub gated: bool,
}

/// A file that survived the probe. `row_groups` is `None` when kept without
/// row-group detail (gated, no warm-index column, or a read error).
#[derive(Debug, Clone)]
pub(crate) struct KeptFile {
    pub file: PartitionedFile,
    pub row_groups: Option<KeptRowGroups>,
}

/// The row groups of one file that survived the probe, plus the file's total
/// row-group count — needed to size a `ParquetAccessPlan` for the file.
#[derive(Debug, Clone)]
pub(crate) struct KeptRowGroups {
    pub kept: Vec<usize>,
    pub total: usize,
}

impl KeptFile {
    fn unprobed(file: PartitionedFile) -> Self {
        Self {
            file,
            row_groups: None,
        }
    }
}

/// Per-file probe result; `Unknown` is never counted as pruning.
enum FileProbe {
    Unknown,
    Narrowed { kept: Vec<usize>, total: usize },
}

impl FileProbe {
    fn keeps_everything(&self) -> bool {
        !matches!(self, FileProbe::Narrowed { kept, .. } if kept.is_empty())
    }
}

/// Logs why a file couldn't be narrowed and returns the "keep it all" outcome.
fn keep_on_error(
    path: &object_store::path::Path,
    error: impl std::fmt::Display,
    stage: &str,
) -> FileProbe {
    tracing::debug!(%path, %error, "warm-index probe: could not {stage}; keeping the file");
    FileProbe::Unknown
}

/// Reads one file's footer and, per row group, its `attr_index.list.item`
/// bloom filter, keeping a row group unless every clause is provably absent.
async fn probe_file(
    store: &Arc<dyn ObjectStore>,
    metadata_cache: &Arc<FileMetadataCache>,
    file: &PartitionedFile,
    clauses: &[ProbeClause],
) -> FileProbe {
    let path = &file.object_meta.location;
    let factory =
        CachedParquetFileReaderFactory::new(Arc::clone(store), Arc::clone(metadata_cache));
    let metrics = ExecutionPlanMetricsSet::new();
    // Drop the `Send` marker so `ParquetRecordBatchStreamBuilder` picks up
    // the blanket `AsyncFileReader` impl for `Box<dyn AsyncFileReader>`.
    let reader: Box<dyn AsyncFileReader> =
        match factory.create_reader(0, file.clone(), None, &metrics) {
            Ok(reader) => reader,
            Err(error) => return keep_on_error(path, error, "create a reader"),
        };
    let mut builder = match ParquetRecordBatchStreamBuilder::new(reader).await {
        Ok(builder) => builder,
        Err(error) => return keep_on_error(path, error, "read the footer"),
    };
    let leaf_path = format!("{WARM_INDEX_COLUMN}.list.item");
    let Some(leaf_idx) = builder
        .parquet_schema()
        .columns()
        .iter()
        .position(|column| column.path().string() == leaf_path)
    else {
        return FileProbe::Unknown;
    };

    let total = builder.metadata().num_row_groups();
    let mut kept = Vec::with_capacity(total);
    for row_group in 0..total {
        match builder
            .get_row_group_column_bloom_filter(row_group, leaf_idx)
            .await
        {
            Ok(Some(bloom)) => {
                let may_match = clauses
                    .iter()
                    .all(|clause| clause.iter().any(|token| bloom.check(token)));
                if may_match {
                    kept.push(row_group);
                }
            }
            // No bloom on this row group: can't prove absence, keep it.
            Ok(None) => kept.push(row_group),
            Err(error) => {
                let stage = format!("read the bloom filter (row group {row_group})");
                return keep_on_error(path, error, &stage);
            }
        }
    }
    FileProbe::Narrowed { kept, total }
}

async fn probe_many(
    store: &Arc<dyn ObjectStore>,
    metadata_cache: &Arc<FileMetadataCache>,
    files: &[PartitionedFile],
    clauses: &[ProbeClause],
    concurrency: usize,
) -> Vec<FileProbe> {
    // Boxed eagerly (rather than left as the closure's own opaque `impl
    // Future`): the actual cause isn't recursion in the caller — a plain,
    // non-recursive `TableProvider::scan` (`WI-5`'s `WarmIndexTable`,
    // `#[async_trait]`-boxed like every `scan` impl) calling this still
    // trips rustc's HRTB inference on the closure ("implementation of
    // `FnOnce` is not general enough"), because that boxed future needs the
    // closure to be region-polymorphic rather than tied to one concrete
    // lifetime.
    let futures: Vec<std::pin::Pin<Box<dyn std::future::Future<Output = FileProbe> + Send + '_>>> =
        files
            .iter()
            .map(|file| Box::pin(probe_file(store, metadata_cache, file, clauses)) as _)
            .collect();
    stream::iter(futures)
        .buffered(concurrency.max(1))
        .collect()
        .await
}

/// Keeps every file unprobed, for a gate decision that skips probing outright.
fn keep_all(
    files: Vec<PartitionedFile>,
    outcome: PrefilterOutcome,
) -> (Vec<KeptFile>, PrefilterOutcome) {
    (files.into_iter().map(KeptFile::unprobed).collect(), outcome)
}

/// Splits `files` into a `sample_size`-file sample spread evenly across the
/// list (every `total / sample_size`-th file) and the remainder, so a data
/// distribution clustered anywhere in the list — not just at the front —
/// still shows up in the selectivity estimate.
fn stride_sample(
    files: Vec<PartitionedFile>,
    sample_size: usize,
) -> (Vec<PartitionedFile>, Vec<PartitionedFile>) {
    let stride = (files.len() / sample_size.max(1)).max(1);
    let mut sample = Vec::with_capacity(sample_size);
    let mut rest = Vec::with_capacity(files.len().saturating_sub(sample_size));
    for (i, file) in files.into_iter().enumerate() {
        if sample.len() < sample_size && i % stride == 0 {
            sample.push(file);
        } else {
            rest.push(file);
        }
    }
    (sample, rest)
}

/// Prunes `files` against `clauses` via each file's bloom filter, gated by
/// [`WarmIndexGate`]. Never a false negative: an undisprovable file is kept.
pub(crate) async fn prefilter_files(
    store: Arc<dyn ObjectStore>,
    metadata_cache: Arc<FileMetadataCache>,
    files: Vec<PartitionedFile>,
    clauses: &[ProbeClause],
    gate: &WarmIndexGate,
) -> (Vec<KeptFile>, PrefilterOutcome) {
    let total = files.len();
    if clauses.is_empty() || total < gate.min_files {
        return keep_all(
            files,
            PrefilterOutcome {
                gated: !clauses.is_empty(),
                ..Default::default()
            },
        );
    }

    let sample_size = (total.div_ceil(20)).max(gate.sample_files).min(total);
    let (sample, rest) = stride_sample(files, sample_size);
    let concurrency = gate.probe_concurrency;

    let sample_results = probe_many(&store, &metadata_cache, &sample, clauses, concurrency).await;
    let kept_in_sample = sample_results
        .iter()
        .filter(|r| r.keeps_everything())
        .count();
    let keep_ratio = kept_in_sample as f64 / sample_size as f64;

    if keep_ratio > gate.max_keep_ratio {
        let mut everything = sample;
        everything.extend(rest);
        return keep_all(
            everything,
            PrefilterOutcome {
                probed: sample_size,
                gated: true,
                ..Default::default()
            },
        );
    }

    let rest_results = probe_many(&store, &metadata_cache, &rest, clauses, concurrency).await;

    let mut outcome = PrefilterOutcome {
        probed: total,
        ..Default::default()
    };
    let mut kept = Vec::with_capacity(total);
    for (file, probe) in sample
        .iter()
        .chain(rest.iter())
        .zip(sample_results.into_iter().chain(rest_results))
    {
        match probe {
            FileProbe::Unknown => kept.push(KeptFile::unprobed(file.clone())),
            FileProbe::Narrowed { kept: rgs, total } => {
                outcome.pruned_row_groups += total - rgs.len();
                if rgs.is_empty() {
                    outcome.pruned_files += 1;
                } else if rgs.len() == total {
                    kept.push(KeptFile::unprobed(file.clone()));
                } else {
                    kept.push(KeptFile {
                        file: file.clone(),
                        row_groups: Some(KeptRowGroups { kept: rgs, total }),
                    });
                }
            }
        }
    }

    (kept, outcome)
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{
        make_files, write_file_without_warm_index, write_warm_index_file,
    };
    use super::*;
    use common::attrs::typed::HomeValue;
    use common::attrs::warm_index::encode_token;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use object_store::ObjectStore;
    use object_store::memory::InMemory;

    fn test_cache() -> Arc<FileMetadataCache> {
        let runtime = RuntimeEnvBuilder::new().build().unwrap();
        runtime.cache_manager.get_file_metadata_cache()
    }

    /// [`prefilter_files`] with a fresh cache and the default gate.
    async fn run(
        store: &Arc<dyn ObjectStore>,
        files: Vec<PartitionedFile>,
        clauses: &[ProbeClause],
    ) -> (Vec<KeptFile>, PrefilterOutcome) {
        prefilter_files(
            Arc::clone(store),
            test_cache(),
            files,
            clauses,
            &WarmIndexGate::default(),
        )
        .await
    }

    #[tokio::test]
    async fn a_target_value_in_two_of_twenty_files_prunes_the_other_eighteen_with_no_false_negatives()
     {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let target = encode_token("host", HomeValue::Str("a.example.com")).unwrap();
        let other = encode_token("host", HomeValue::Str("b.example.com")).unwrap();

        // Target files sit at the end, not the front, so this only passes if
        // the gate's sample genuinely covers the whole list.
        let files = make_files(&store, 20, |i| {
            let token: &[u8] = if i >= 18 { &target } else { &other };
            write_warm_index_file(&[&[token]], 100)
        })
        .await;
        let target_names: Vec<String> = files[18..]
            .iter()
            .map(|f| f.object_meta.location.to_string())
            .collect();

        let clauses = vec![vec![target.clone()]];
        let (kept, outcome) = run(&store, files, &clauses).await;

        assert!(!outcome.gated);
        assert_eq!(outcome.pruned_files, 18);
        let kept_names: Vec<String> = kept
            .iter()
            .map(|k| k.file.object_meta.location.to_string())
            .collect();
        assert_eq!(kept_names, target_names);
    }

    #[tokio::test]
    async fn a_value_clustered_at_the_front_does_not_falsely_trip_the_gate() {
        // A naive "first N files" sample would see nothing but this value
        // (it fills exactly the first `sample_files` files) and wrongly
        // conclude the predicate isn't selective, gating away the real
        // pruning opportunity in the other 84 files. The stride sample must
        // spread across the whole list and see through the front cluster.
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let common = encode_token("env", HomeValue::Str("shared")).unwrap();
        let rare = encode_token("env", HomeValue::Str("rare")).unwrap();

        let files = make_files(&store, 100, |i| {
            let token: &[u8] = if i < 16 { &common } else { &rare };
            write_warm_index_file(&[&[token]], 100)
        })
        .await;

        let clauses = vec![vec![common]];
        let (kept, outcome) = run(&store, files, &clauses).await;

        assert!(
            !outcome.gated,
            "a front-only cluster must not trip the gate"
        );
        assert_eq!(outcome.pruned_files, 84);
        assert_eq!(kept.len(), 16);
    }

    #[tokio::test]
    async fn a_value_present_in_every_file_trips_the_selectivity_gate() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let token = encode_token("env", HomeValue::Str("prod")).unwrap();

        let files = make_files(&store, 20, |_| {
            write_warm_index_file(&[&[token.as_slice()]], 100)
        })
        .await;
        let total = files.len();

        let clauses = vec![vec![token]];
        let (kept, outcome) = run(&store, files, &clauses).await;

        assert!(outcome.gated);
        assert!(outcome.probed < total);
        assert_eq!(outcome.pruned_files, 0);
        assert_eq!(kept.len(), total);
    }

    #[tokio::test]
    async fn a_file_without_the_warm_index_column_is_always_kept() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let files = make_files(&store, 4, |i| {
            if i == 0 {
                write_file_without_warm_index()
            } else {
                write_warm_index_file(&[&[b"unrelated"]], 10)
            }
        })
        .await;

        let clauses = vec![vec![b"absent-token".to_vec()]];
        let (kept, outcome) = run(&store, files, &clauses).await;

        assert!(!outcome.gated);
        // The column-less file is kept unconditionally; the other three
        // (present bloom, absent token) are pruned.
        assert_eq!(kept.len(), 1);
        assert_eq!(outcome.pruned_files, 3);
    }

    #[tokio::test]
    async fn fewer_than_min_files_skips_probing_entirely() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let files = make_files(&store, 2, |_| write_warm_index_file(&[&[b"x"]], 10)).await;

        let clauses = vec![vec![b"absent".to_vec()]];
        let (kept, outcome) = run(&store, files, &clauses).await;

        assert!(outcome.gated);
        assert_eq!(outcome.probed, 0);
        assert_eq!(kept.len(), 2);
    }
}
