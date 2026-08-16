//! `transform_trace_v1_to_v2` benchmark.
//!
//! `compiled-schema-materializer`'s regression gate: the plan-based
//! rewrite must show no regression against this baseline, and is expected
//! to show a measurable improvement by removing the per-field, per-batch
//! `get_column_by_name` string lookups from the hot loop.
//!
//! Pure CPU (no WAL, no object store) -- for relative regression tracking,
//! not absolute production latency.

use std::hint::black_box;

use common::flight::conversion::otlp_traces_to_arrow;
use common::testing::sample_trace_request;
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use writer::schema_transform::transform_trace_v1_to_v2;

const NUM_SPANS: usize = 1_000;

fn bench_transform_trace_v1_to_v2(c: &mut Criterion) {
    let v1_batch =
        otlp_traces_to_arrow(&sample_trace_request(NUM_SPANS)).expect("conversion should succeed");

    let mut group = c.benchmark_group("schema_transform");
    group.throughput(Throughput::Elements(NUM_SPANS as u64));
    group.bench_function("transform_trace_v1_to_v2", |b| {
        b.iter(|| {
            let result = transform_trace_v1_to_v2(black_box(v1_batch.clone()), &[])
                .expect("transform should succeed");
            black_box(result);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_transform_trace_v1_to_v2);
criterion_main!(benches);
