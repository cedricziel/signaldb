//! Would a materialized `ancestor_ids` column beat the `match` stage's
//! per-trace evaluator on `descendant`? (openspec change
//! `materialized-ancestry`.)
//!
//! Each shape is written once to an in-memory ZSTD Parquet file holding both
//! layouts. Per iteration each strategy pays its whole query-time cost from
//! that file: decode its columns, flag the two span-sets, answer
//! `anc descendant desc`.
//!
//! - `evaluator` reads `trace_id, span_id, parent_span_id, span_name`, splits
//!   the rows by trace and runs the real per-trace kernel
//!   (`querier::bench_descendant_masks`). The exec's repartition, sort,
//!   candidate-trace semi-join, `match_max_trace_*` bounds and memory-pool
//!   accounting are NOT included, which favours it.
//! - `ancestry` reads `trace_id, span_id, ancestor_ids, span_name` and answers
//!   with one hash probe per (desc span, ancestor), as a hash join would. It
//!   needs no trace grouping or sort.
//!
//! `materialize` is the write side: computing `ancestor_ids` for complete
//! traces, which a writer or compactor would pay once.
//!
//! Criterion is avoided to keep `querier`'s dependency set unchanged, so this
//! is a plain `harness = false` binary: the strategies run alternately,
//! sample by sample, so machine noise hits them alike, and the median is
//! reported. `--output-format bencher` prints criterion-style `test ...
//! bench:` lines for `scripts/run-benches.sh`.
//! `cargo bench -p querier --features benchmarks --bench structural_ancestry
//! [-- <shape filter>]`

use std::collections::{HashMap, HashSet};
use std::hint::black_box;
use std::sync::Arc;

use bytes::Bytes;
use std::time::{Duration, Instant};

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, ListBuilder, RecordBatch, StringArray, StringBuilder,
};
use datafusion::arrow::compute::kernels::cmp::eq;
use datafusion::arrow::compute::{concat_batches, partition};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::arrow::ProjectionMask;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;

const ANC: &str = "root";
const DESC: &str = "write";

/// splitmix64 — deterministic ids without a rng dependency.
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// One trace: per span, `(parent index, name)`, parents before children.
type Tree = Vec<(Option<usize>, &'static str)>;

/// A chain `root -> hop.. -> write` of `n` spans (a lone `root` when 1).
fn chain(n: usize) -> Tree {
    (0..n)
        .map(|i| match i {
            0 => (None, ANC),
            _ if i == n - 1 => (Some(i - 1), DESC),
            _ => (Some(i - 1), "hop"),
        })
        .collect()
}

/// The span count per trace in `_system/_monitoring` (817 traces, 30 min,
/// 2026-10-01); every trace was a chain (fan-out 1 for 99.6% of parents).
const OBSERVED: [(usize, usize); 8] = [
    (1, 178),
    (2, 274),
    (3, 3),
    (4, 199),
    (5, 153),
    (6, 2),
    (7, 2),
    (8, 6),
];

fn monitoring(traces: usize) -> Vec<Tree> {
    let total: usize = OBSERVED.iter().map(|(_, c)| c).sum();
    (0..traces)
        .map(|t| {
            let mut pick = (mix(t as u64) % total as u64) as usize;
            let (n, _) = OBSERVED
                .iter()
                .find(|(_, c)| {
                    let hit = pick < *c;
                    pick = pick.saturating_sub(*c);
                    hit
                })
                .copied()
                .unwrap_or((1, 0));
            chain(n)
        })
        .collect()
}

/// A root with `n - 1` direct `write` children.
fn wide(n: usize) -> Tree {
    std::iter::once((None, ANC))
        .chain((1..n).map(|_| (Some(0), DESC)))
        .collect()
}

/// A random recursive tree (depth ~ ln n): 5% of spans are `root`-named
/// ancestors-to-match, 10% are `write`.
fn bushy(seed: u64, n: usize) -> Tree {
    (0..n)
        .map(|i| {
            let r = mix(seed.wrapping_mul(1_000_003).wrapping_add(i as u64));
            let parent = (i > 0).then(|| (r % i as u64) as usize);
            let name = match (r >> 32) % 20 {
                0 => ANC,
                1 | 2 => DESC,
                _ => "hop",
            };
            (parent, name)
        })
        .collect()
}

fn shapes() -> Vec<(&'static str, Vec<Tree>)> {
    vec![
        ("monitoring_10k_traces", monitoring(10_000)),
        ("deep_20x500_chain", (0..20).map(|_| chain(500)).collect()),
        (
            "wide_10x10000_fanout",
            (0..10).map(|_| wide(10_000)).collect(),
        ),
        (
            "bushy_20x5000_random",
            (0..20).map(|s| bushy(s, 5_000)).collect(),
        ),
    ]
}

fn hex_id(seed: u64, width: usize) -> String {
    match width {
        16 => format!("{:016x}", mix(seed)),
        _ => format!("{:016x}{:016x}", mix(seed), mix(seed ^ 0xDEAD)),
    }
}

/// Ancestor span ids per span, nearest first, given each span's parent.
fn ancestry(span_ids: &[String], parents: &[Option<usize>]) -> ArrayRef {
    let mut out = ListBuilder::new(StringBuilder::new());
    for parent in parents {
        let mut at = *parent;
        while let Some(p) = at {
            out.values().append_value(&span_ids[p]);
            at = parents[p];
        }
        out.append(true);
    }
    Arc::new(out.finish())
}

struct Rows {
    trace: Vec<String>,
    span: Vec<String>,
    parent_idx: Vec<Option<usize>>,
    name: Vec<&'static str>,
}

/// Every trace's rows, trace-major (the evaluator's required order).
fn rows(traces: &[Tree]) -> Rows {
    let mut r = Rows {
        trace: Vec::new(),
        span: Vec::new(),
        parent_idx: Vec::new(),
        name: Vec::new(),
    };
    let mut seed = 0u64;
    for (t, tree) in traces.iter().enumerate() {
        let tid = hex_id(t as u64 + (1 << 40), 32);
        let base = r.span.len();
        for (parent, name) in tree {
            seed += 1;
            r.trace.push(tid.clone());
            r.span.push(hex_id(seed, 16));
            r.parent_idx.push(parent.map(|p| base + p));
            r.name.push(name);
        }
    }
    r
}

fn schema() -> Arc<Schema> {
    let item = Arc::new(Field::new("item", DataType::Utf8, true));
    Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("span_id", DataType::Utf8, false),
        Field::new("parent_span_id", DataType::Utf8, true),
        Field::new("span_name", DataType::Utf8, false),
        Field::new("ancestor_ids", DataType::List(item), true),
    ]))
}

/// The Parquet file, plus each column's compressed bytes.
fn write_parquet(r: &Rows) -> (Bytes, Vec<(String, i64)>) {
    let parents: StringArray = (r.parent_idx.iter())
        .map(|p| p.map(|p| r.span[p].as_str()))
        .collect();
    let batch = RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(StringArray::from_iter_values(&r.trace)),
            Arc::new(StringArray::from_iter_values(&r.span)),
            Arc::new(parents),
            Arc::new(StringArray::from_iter_values(&r.name)),
            ancestry(&r.span, &r.parent_idx),
        ],
    )
    .expect("batch");
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).expect("zstd")))
        .build();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema(), Some(props)).expect("writer");
    writer.write(&batch).expect("write");
    let meta = writer.close().expect("close");
    let mut sizes: HashMap<String, i64> = HashMap::new();
    for rg in meta.row_groups() {
        for c in rg.columns() {
            let name = c.column_path().parts()[0].clone();
            *sizes.entry(name).or_default() += c.compressed_size();
        }
    }
    let mut sizes: Vec<_> = sizes.into_iter().collect();
    sizes.sort();
    (Bytes::from(buf), sizes)
}

fn read(file: &Bytes, columns: &[&str]) -> Vec<RecordBatch> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(file.clone()).expect("reader");
    let roots = columns
        .iter()
        .map(|c| builder.schema().index_of(c).expect("column"));
    let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
    let reader = builder.with_projection(mask).build().expect("build");
    reader.collect::<Result<_, _>>().expect("decode")
}

fn flag(batch: &RecordBatch, name: &str) -> BooleanArray {
    let names = batch.column_by_name("span_name").expect("span_name");
    eq(names, &StringArray::new_scalar(name)).expect("eq")
}

/// `(anc witnesses, desc witnesses, matching traces)`.
type Answer = (usize, usize, usize);

const EVALUATOR_COLUMNS: [&str; 4] = ["trace_id", "span_id", "parent_span_id", "span_name"];
const ANCESTRY_COLUMNS: [&str; 4] = ["trace_id", "span_id", "ancestor_ids", "span_name"];

/// Decode and flag only: the floor under each strategy.
fn decode(file: &Bytes, columns: &[&str]) -> Answer {
    let batches = read(file, columns);
    let flagged = (batches.iter()).map(|b| flag(b, ANC).true_count() + flag(b, DESC).true_count());
    (flagged.sum(), 0, 0)
}

fn evaluator(file: &Bytes) -> Answer {
    let batches = read(file, &EVALUATOR_COLUMNS);
    let batch = concat_batches(&batches[0].schema(), &batches).expect("concat");
    let anc: ArrayRef = Arc::new(flag(&batch, ANC));
    let desc: ArrayRef = Arc::new(flag(&batch, DESC));
    let column = |name| batch.column_by_name(name).expect("column");
    let (spans, parents) = (column("span_id"), column("parent_span_id"));
    let traces = partition(&[Arc::clone(column("trace_id"))]).expect("partition");
    let mut answer = (0, 0, 0);
    for range in traces.ranges() {
        let slice = |a: &ArrayRef| a.slice(range.start, range.len());
        let masks = querier::bench_descendant_masks(
            &slice(spans),
            &slice(parents),
            &slice(&anc),
            &slice(&desc),
        )
        .expect("evaluate");
        if let Some(masks) = masks {
            answer.0 += masks.iter().filter(|m| *m & 1 != 0).count();
            answer.1 += masks.iter().filter(|m| *m & 2 != 0).count();
            answer.2 += 1;
        }
    }
    answer
}

fn strings<'a>(b: &'a RecordBatch, name: &str) -> &'a StringArray {
    b.column_by_name(name).expect("column").as_string::<i32>()
}

fn ancestry_probe(file: &Bytes) -> Answer {
    let batches = read(file, &ANCESTRY_COLUMNS);
    let flags: Vec<_> = (batches.iter())
        .map(|b| (flag(b, ANC), flag(b, DESC)))
        .collect();
    let mut ancestors: HashMap<(&str, &str), bool> = HashMap::new();
    for (b, (anc, _)) in batches.iter().zip(&flags) {
        let (traces, spans) = (strings(b, "trace_id"), strings(b, "span_id"));
        for i in (0..b.num_rows()).filter(|&i| anc.value(i)) {
            ancestors.insert((traces.value(i), spans.value(i)), false);
        }
    }
    let (mut desc_witnesses, mut matched) = (0, HashSet::new());
    for (b, (_, desc)) in batches.iter().zip(&flags) {
        let traces = strings(b, "trace_id");
        let lists = b.column_by_name("ancestor_ids").expect("ancestor_ids");
        let lists = lists.as_list::<i32>();
        let ids = lists.values().as_string::<i32>();
        let offsets = lists.value_offsets();
        for i in (0..b.num_rows()).filter(|&i| desc.value(i)) {
            let trace = traces.value(i);
            let mut hit = false;
            for a in offsets[i] as usize..offsets[i + 1] as usize {
                if let Some(seen) = ancestors.get_mut(&(trace, ids.value(a))) {
                    *seen = true;
                    hit = true;
                }
            }
            if hit {
                desc_witnesses += 1;
                matched.insert(trace);
            }
        }
    }
    let anc_witnesses = ancestors.values().filter(|seen| **seen).count();
    (anc_witnesses, desc_witnesses, matched.len())
}

const SAMPLES: usize = 30;
const BUDGET: Duration = Duration::from_secs(20);

/// Median, min and max of `samples`, in ms.
fn summary(samples: &mut [Duration]) -> String {
    samples.sort();
    let ms = |d: Duration| d.as_secs_f64() * 1e3;
    format!(
        "median {:>9.3} ms  (min {:.3}, max {:.3}, n={})",
        ms(samples[samples.len() / 2]),
        ms(samples[0]),
        ms(samples[samples.len() - 1]),
        samples.len()
    )
}

type Strategy<'a, T> = (&'a str, &'a dyn Fn(&T) -> Answer);

/// Time `strategies` alternately, one sample each per round, after one
/// warm-up round; stop at `SAMPLES` rounds or `BUDGET`.
fn race<T>(out: &Output, shape: &str, input: &T, strategies: &[Strategy<'_, T>]) {
    for (_, run) in strategies {
        black_box(run(input));
    }
    let mut samples = vec![Vec::new(); strategies.len()];
    let started = Instant::now();
    while samples[0].len() < SAMPLES && started.elapsed() < BUDGET {
        for ((_, run), out) in strategies.iter().zip(&mut samples) {
            let t = Instant::now();
            black_box(run(input));
            out.push(t.elapsed());
        }
    }
    for ((name, _), s) in strategies.iter().zip(&mut samples) {
        match out {
            Output::Human => println!("  {name:<12} {}", summary(s)),
            Output::Bencher => println!("{}", bencher_line(shape, name, s)),
        }
    }
}

/// The line `github-action-benchmark`'s `cargo` tool parses, as criterion's
/// `--output-format bencher` prints it; the spread is half the min..max range.
fn bencher_line(shape: &str, name: &str, samples: &mut [Duration]) -> String {
    samples.sort();
    let ns = |d: Duration| d.as_nanos();
    let median = ns(samples[samples.len() / 2]);
    let spread = (ns(samples[samples.len() - 1]) - ns(samples[0])) / 2;
    format!("test structural_ancestry/{shape}/{name} ... bench: {median} ns/iter (+/- {spread})")
}

enum Output {
    Human,
    Bencher,
}

/// The shape filter and output format from the harness arguments, skipping
/// every `--flag` and the value of `--output-format`.
fn args() -> (String, Output) {
    let (mut filter, mut output) = (String::new(), Output::Human);
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--output-format" {
            if args.next().as_deref() == Some("bencher") {
                output = Output::Bencher;
            }
        } else if !arg.starts_with("--") {
            filter = arg;
        }
    }
    (filter, output)
}

fn main() {
    let (filter, out) = args();
    for (shape, traces) in shapes() {
        if !shape.contains(&filter) {
            continue;
        }
        let r = rows(&traces);
        let (file, sizes) = write_parquet(&r);
        let expected = evaluator(&file);
        assert_eq!(
            expected,
            ancestry_probe(&file),
            "{shape}: strategies disagree"
        );
        // In bencher mode only the `test ...` lines go to stdout.
        eprintln!(
            "{shape}: {} spans, {} traces, {} file bytes, answer (anc, desc, traces) {expected:?}",
            r.span.len(),
            traces.len(),
            file.len()
        );
        eprintln!("  compressed column bytes {sizes:?}");
        let decode_evaluator = |f: &Bytes| decode(f, &EVALUATOR_COLUMNS);
        let decode_ancestry = |f: &Bytes| decode(f, &ANCESTRY_COLUMNS);
        race(
            &out,
            shape,
            &file,
            &[
                ("evaluator", &evaluator),
                ("ancestry", &ancestry_probe),
                ("decode(a)", &decode_evaluator),
                ("decode(b)", &decode_ancestry),
            ],
        );
        let materialize = |r: &Rows| {
            black_box(ancestry(&r.span, &r.parent_idx));
            (0, 0, 0)
        };
        race(&out, shape, &r, &[("materialize", &materialize)]);
    }
}
