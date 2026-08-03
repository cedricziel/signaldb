//! Does the point index stay fast as it grows to billions of traces?
//!
//! Builds a REAL Parquet-backed `trace_index` (unlike `querier_read_paths`,
//! which uses a MemTable), partitioned by `trace_id` prefix (Hive-style
//! `prefix=XX/` directories, 256 shards) and bloom-filtered on `trace_id`.
//! A point lookup filters `prefix = ? AND trace_id = ?`, so DataFusion prunes
//! to the ONE shard directory and opens ~one file footer — regardless of total
//! size. This benches the index lookup itself across 10k → 1M traces; a flat
//! curve is the O(1)-per-lookup claim.

use std::collections::HashMap;
use std::hint::black_box;
use std::path::Path;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::{RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::prelude::SessionContext;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const TARGET: &str = "88888888888888888888888888888888";
const HOUR_US: i64 = 3_600_000_000;
const BASE_US: i64 = 1_700_000_000_000_000;
const SIZES: [usize; 3] = [10_000, 100_000, 1_000_000];

/// splitmix64 — deterministic, well-spread ids without a rng dependency.
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

fn trace_id(i: usize) -> String {
    let hi = mix(i as u64);
    let lo = mix((i as u64).wrapping_mul(2).wrapping_add(0xDEAD));
    format!("{hi:016x}{lo:016x}")
}

fn index_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new(
            "ts_hour",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
    ]))
}

/// Write one shard's rows (sorted by trace_id, bloom on trace_id) to
/// `<dir>/prefix=XX/data.parquet`. The `prefix` column is NOT in the file — it
/// is derived from the directory name by Hive partitioning.
fn write_shard(dir: &Path, prefix: &str, mut rows: Vec<(String, i64)>) {
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let ids: Vec<&str> = rows.iter().map(|(s, _)| s.as_str()).collect();
    let hours: Vec<i64> = rows.iter().map(|(_, h)| *h).collect();
    let schema = index_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(ids)),
            Arc::new(TimestampMicrosecondArray::from(hours)),
        ],
    )
    .expect("index batch");

    let shard_dir = dir.join(format!("prefix={prefix}"));
    std::fs::create_dir_all(&shard_dir).expect("mkdir shard");
    let file = std::fs::File::create(shard_dir.join("data.parquet")).expect("create parquet");
    let props = WriterProperties::builder()
        .set_bloom_filter_enabled(true)
        .build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).expect("arrow writer");
    writer.write(&batch).expect("write batch");
    writer.close().expect("close writer");
}

/// Build a prefix-sharded index of `n` traces (+ TARGET) and return the temp
/// dir holding it (kept alive by the caller).
fn build_index(n: usize) -> TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut shards: HashMap<String, Vec<(String, i64)>> = HashMap::new();
    for i in 0..n {
        let id = trace_id(i);
        let prefix = id[0..2].to_string();
        let hour = BASE_US + ((i as i64 % 100) * HOUR_US);
        shards.entry(prefix).or_default().push((id, hour));
    }
    shards
        .entry(TARGET[0..2].to_string())
        .or_default()
        .push((TARGET.to_string(), BASE_US));
    for (prefix, rows) in shards {
        write_shard(dir.path(), &prefix, rows);
    }
    dir
}

async fn register(ctx: &SessionContext, dir: &Path) {
    let url = ListingTableUrl::parse(format!("file://{}/", dir.display())).expect("listing url");
    let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension(".parquet")
        .with_table_partition_cols(vec![("prefix".to_string(), DataType::Utf8)]);
    let schema = options
        .infer_schema(&ctx.state(), &url)
        .await
        .expect("infer schema");
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    let table = ListingTable::try_new(config).expect("listing table");
    ctx.register_table("idx", Arc::new(table))
        .expect("register idx");
}

fn bench_index_scaling(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    // Build + register one index per size up front (kept alive via `_dir`).
    let envs: Vec<(usize, SessionContext, TempDir)> = SIZES
        .iter()
        .map(|&n| {
            let dir = build_index(n);
            let ctx = SessionContext::new();
            rt.block_on(register(&ctx, dir.path()));
            (n, ctx, dir)
        })
        .collect();

    let prefix = &TARGET[0..2];
    let mut group = c.benchmark_group("trace_index_scaling");
    for (n, ctx, _dir) in &envs {
        group.bench_with_input(BenchmarkId::from_parameter(n), n, |b, _| {
            b.to_async(&rt).iter(|| async {
                let batches = ctx
                    .sql(&format!(
                        "SELECT ts_hour FROM idx WHERE prefix = '{prefix}' AND trace_id = '{TARGET}'"
                    ))
                    .await
                    .expect("plan index lookup")
                    .collect()
                    .await
                    .expect("run index lookup");
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                black_box(rows);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_index_scaling);
criterion_main!(benches);
