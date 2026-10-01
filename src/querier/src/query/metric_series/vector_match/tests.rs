use std::collections::BTreeMap;
use std::sync::Arc;

use common::query_ir::{Binop, BinopGroup, BinopOp, BinopOperand, GroupSide};
use datafusion::arrow::array::{
    ArrayRef, AsArray, Float64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{Float64Type, TimestampNanosecondType};
use datafusion::dataframe::DataFrame;
use datafusion::prelude::SessionContext;

use super::{Operand, vector_match};
use crate::query::error::QuerierError;

pub(super) type Labels<'a> = &'a [(&'a str, &'a str)];
pub(super) type Row = (i64, String, f64);

pub(super) fn labels(pairs: Labels) -> String {
    let map: BTreeMap<&str, &str> = pairs.iter().copied().collect();
    serde_json::to_string(&map).unwrap()
}

pub(super) fn frame(
    ctx: &SessionContext,
    buckets: Vec<i64>,
    labels: Option<Vec<String>>,
    values: Vec<f64>,
) -> DataFrame {
    let mut columns: Vec<(&str, ArrayRef)> =
        vec![("bucket", Arc::new(TimestampNanosecondArray::from(buckets)))];
    if let Some(labels) = labels {
        columns.push(("__labels", Arc::new(StringArray::from(labels))));
    }
    columns.push(("value", Arc::new(Float64Array::from(values))));
    ctx.read_batch(RecordBatch::try_from_iter(columns).unwrap())
        .unwrap()
}

pub(super) fn series(ctx: &SessionContext, rows: &[(i64, Labels, f64)]) -> Operand {
    let buckets = rows.iter().map(|r| r.0).collect();
    let names = rows.iter().map(|r| labels(r.1)).collect();
    Operand::Series(frame(
        ctx,
        buckets,
        Some(names),
        rows.iter().map(|r| r.2).collect(),
    ))
}

pub(super) fn scalar(ctx: &SessionContext, rows: &[(i64, f64)]) -> Operand {
    let buckets = rows.iter().map(|r| r.0).collect();
    Operand::Scalar(frame(
        ctx,
        buckets,
        None,
        rows.iter().map(|r| r.1).collect(),
    ))
}

pub(super) fn spec(op: BinopOp) -> Binop {
    Binop {
        op,
        right: BinopOperand::Number(0.0),
        reverse: false,
        on: None,
        ignoring: None,
        group: None,
        bool: false,
    }
}

pub(super) fn on(op: BinopOp, labels: &[&str]) -> Binop {
    Binop {
        on: Some(labels.iter().map(|l| l.to_string()).collect()),
        ..spec(op)
    }
}

pub(super) fn grouped(op: BinopOp, side: GroupSide, include: &[&str]) -> Binop {
    Binop {
        group: Some(BinopGroup {
            side,
            include: include.iter().map(|l| l.to_string()).collect(),
        }),
        ..on(op, &["job"])
    }
}

pub(super) async fn run(
    left: Operand,
    right: Operand,
    spec: Binop,
) -> Result<Vec<Row>, QuerierError> {
    let batches = vector_match(left, right, &spec)?.collect().await?;
    let mut rows = Vec::new();
    for batch in batches {
        let buckets = batch.column(0).as_primitive::<TimestampNanosecondType>();
        let values = batch.column(batch.num_columns() - 1);
        let values = values.as_primitive::<Float64Type>();
        let labels = (batch.num_columns() == 3).then(|| batch.column(1).as_string::<i32>());
        for row in 0..batch.num_rows() {
            let l = labels.map_or(String::new(), |l| l.value(row).to_string());
            rows.push((buckets.value(row), l, values.value(row)));
        }
    }
    Ok(rows)
}

pub(super) fn row(bucket: i64, pairs: Labels, value: f64) -> Row {
    (bucket, labels(pairs), value)
}

pub(super) fn assert_invalid(result: Result<Vec<Row>, QuerierError>, needle: &str) {
    match result {
        Err(QuerierError::InvalidInput(msg)) => assert!(msg.contains(needle), "{msg}"),
        other => panic!("expected InvalidInput containing {needle:?}, got {other:?}"),
    }
}

#[tokio::test]
async fn vector_match_one_to_one_default_key_drops_name() {
    let ctx = SessionContext::new();
    let left = series(
        &ctx,
        &[
            (
                1,
                &[("metric.name", "a"), ("job", "x"), ("inst", "1")],
                10.0,
            ),
            (1, &[("metric.name", "a"), ("job", "y")], 20.0),
        ],
    );
    let right = series(
        &ctx,
        &[
            (1, &[("metric.name", "b"), ("job", "x"), ("inst", "1")], 2.0),
            (1, &[("metric.name", "b"), ("job", "y")], 5.0),
            (1, &[("metric.name", "b"), ("job", "z")], 5.0),
        ],
    );
    let rows = run(left, right, spec(BinopOp::Div)).await.unwrap();
    assert_eq!(
        rows,
        vec![
            row(1, &[("inst", "1"), ("job", "x")], 5.0),
            row(1, &[("job", "y")], 4.0),
        ]
    );
}

#[tokio::test]
async fn vector_match_on_keeps_only_matching_labels() {
    let ctx = SessionContext::new();
    let left = series(
        &ctx,
        &[(
            1,
            &[("metric.name", "a"), ("job", "x"), ("inst", "1")],
            10.0,
        )],
    );
    let right = series(
        &ctx,
        &[(1, &[("metric.name", "b"), ("job", "x"), ("env", "p")], 3.0)],
    );
    let rows = run(left, right, on(BinopOp::Sub, &["job"])).await.unwrap();
    assert_eq!(rows, vec![row(1, &[("job", "x")], 7.0)]);
}

#[tokio::test]
async fn vector_match_ignoring_removes_ignored_labels() {
    let ctx = SessionContext::new();
    let left = series(
        &ctx,
        &[(
            1,
            &[("metric.name", "a"), ("job", "x"), ("inst", "1")],
            10.0,
        )],
    );
    let right = series(
        &ctx,
        &[(1, &[("metric.name", "b"), ("job", "x"), ("inst", "2")], 3.0)],
    );
    let binop = Binop {
        ignoring: Some(vec!["inst".to_string()]),
        ..spec(BinopOp::Add)
    };
    let rows = run(left, right, binop).await.unwrap();
    assert_eq!(rows, vec![row(1, &[("job", "x")], 13.0)]);
}

#[tokio::test]
async fn vector_match_group_left_copies_include_labels() {
    let ctx = SessionContext::new();
    let left = series(
        &ctx,
        &[
            (
                1,
                &[("metric.name", "a"), ("job", "x"), ("inst", "1")],
                10.0,
            ),
            (
                1,
                &[("metric.name", "a"), ("job", "x"), ("inst", "2")],
                20.0,
            ),
        ],
    );
    let right = series(
        &ctx,
        &[(
            1,
            &[("metric.name", "info"), ("job", "x"), ("version", "v1")],
            2.0,
        )],
    );
    let binop = grouped(BinopOp::Mul, GroupSide::Left, &["version"]);
    let rows = run(left, right, binop).await.unwrap();
    assert_eq!(
        rows,
        vec![
            row(1, &[("inst", "1"), ("job", "x"), ("version", "v1")], 20.0),
            row(1, &[("inst", "2"), ("job", "x"), ("version", "v1")], 40.0),
        ]
    );
}

#[tokio::test]
async fn vector_match_group_right_keeps_operand_order() {
    let ctx = SessionContext::new();
    let left = series(&ctx, &[(1, &[("metric.name", "cap"), ("job", "x")], 100.0)]);
    let right = series(
        &ctx,
        &[
            (
                1,
                &[("metric.name", "a"), ("job", "x"), ("inst", "1")],
                10.0,
            ),
            (
                1,
                &[("metric.name", "a"), ("job", "x"), ("inst", "2")],
                30.0,
            ),
        ],
    );
    let rows = run(left, right, grouped(BinopOp::Sub, GroupSide::Right, &[]))
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            row(1, &[("inst", "1"), ("job", "x")], 90.0),
            row(1, &[("inst", "2"), ("job", "x")], 70.0),
        ]
    );
}

#[tokio::test]
async fn vector_match_many_to_many_is_invalid_input() {
    let ctx = SessionContext::new();
    let two = |name: &'static str| {
        series(
            &ctx,
            &[
                (
                    1,
                    &[("metric.name", name), ("job", "x"), ("inst", "1")],
                    1.0,
                ),
                (
                    1,
                    &[("metric.name", name), ("job", "x"), ("inst", "2")],
                    2.0,
                ),
            ],
        )
    };
    let one_to_one = run(two("a"), two("b"), on(BinopOp::Add, &["job"])).await;
    assert_invalid(one_to_one, "many-to-many");
    let group_left = grouped(BinopOp::Add, GroupSide::Left, &[]);
    assert_invalid(run(two("a"), two("b"), group_left).await, "many-to-many");
}

#[tokio::test]
async fn vector_match_duplicate_output_label_set_is_invalid_input() {
    let ctx = SessionContext::new();
    let left = series(
        &ctx,
        &[
            (1, &[("metric.name", "a"), ("job", "x"), ("inst", "1")], 1.0),
            (1, &[("metric.name", "c"), ("job", "x"), ("inst", "1")], 2.0),
        ],
    );
    let right = series(&ctx, &[(1, &[("metric.name", "b"), ("job", "x")], 1.0)]);
    let binop = grouped(BinopOp::Add, GroupSide::Left, &[]);
    assert_invalid(run(left, right, binop).await, "more than once");
}

#[tokio::test]
async fn vector_match_set_ops_allow_many_series_per_key() {
    let ctx = SessionContext::new();
    let left_rows: &[(i64, Labels, f64)] = &[
        (1, &[("metric.name", "a"), ("job", "x"), ("inst", "1")], 1.0),
        (1, &[("metric.name", "a"), ("job", "x"), ("inst", "2")], 2.0),
        (1, &[("metric.name", "a"), ("job", "y")], 3.0),
    ];
    let right_rows: &[(i64, Labels, f64)] = &[
        (1, &[("metric.name", "b"), ("job", "x"), ("z", "1")], 9.0),
        (1, &[("metric.name", "b"), ("job", "x"), ("z", "2")], 9.0),
        (1, &[("metric.name", "b"), ("job", "w")], 9.0),
    ];
    let eval = |op| {
        run(
            series(&ctx, left_rows),
            series(&ctx, right_rows),
            on(op, &["job"]),
        )
    };
    let x1 = row(1, &[("inst", "1"), ("job", "x"), ("metric.name", "a")], 1.0);
    let x2 = row(1, &[("inst", "2"), ("job", "x"), ("metric.name", "a")], 2.0);
    let y = row(1, &[("job", "y"), ("metric.name", "a")], 3.0);
    let w = row(1, &[("job", "w"), ("metric.name", "b")], 9.0);
    let and = eval(BinopOp::And).await.unwrap();
    assert_eq!(and, vec![x1.clone(), x2.clone()]);
    assert_eq!(eval(BinopOp::Unless).await.unwrap(), vec![y.clone()]);
    assert_eq!(eval(BinopOp::Or).await.unwrap(), vec![x1, x2, w, y]);
}

#[tokio::test]
async fn vector_match_comparison_filters_or_yields_bool() {
    let ctx = SessionContext::new();
    let left_rows: &[(i64, Labels, f64)] = &[
        (1, &[("metric.name", "a"), ("job", "x")], 5.0),
        (1, &[("metric.name", "a"), ("job", "y")], 1.0),
    ];
    let right_rows: &[(i64, Labels, f64)] = &[
        (1, &[("metric.name", "b"), ("job", "x")], 3.0),
        (1, &[("metric.name", "b"), ("job", "y")], 3.0),
    ];
    let filtered = run(
        series(&ctx, left_rows),
        series(&ctx, right_rows),
        spec(BinopOp::Gt),
    )
    .await
    .unwrap();
    assert_eq!(
        filtered,
        vec![row(1, &[("job", "x"), ("metric.name", "a")], 5.0)]
    );
    let binop = Binop {
        bool: true,
        ..spec(BinopOp::Gt)
    };
    let as_bool = run(series(&ctx, left_rows), series(&ctx, right_rows), binop)
        .await
        .unwrap();
    assert_eq!(
        as_bool,
        vec![row(1, &[("job", "x")], 1.0), row(1, &[("job", "y")], 0.0)]
    );
}

#[tokio::test]
async fn vector_match_scalar_broadcasts_and_reverses() {
    let ctx = SessionContext::new();
    let x: &[(i64, Labels, f64)] = &[
        (1, &[("metric.name", "a"), ("job", "x")], 10.0),
        (2, &[("metric.name", "a"), ("job", "x")], 3.0),
    ];
    let job = &[("job", "x")][..];
    let sub = run(series(&ctx, x), Operand::Number(2.0), spec(BinopOp::Sub)).await;
    assert_eq!(sub.unwrap(), vec![row(1, job, 8.0), row(2, job, 1.0)]);
    let reverse = Binop {
        reverse: true,
        ..spec(BinopOp::Sub)
    };
    let rsub = run(series(&ctx, x), Operand::Number(2.0), reverse.clone()).await;
    assert_eq!(rsub.unwrap(), vec![row(1, job, -8.0), row(2, job, -1.0)]);
    // `5 > x` keeps the series' own value, name included.
    let gt = Binop {
        op: BinopOp::Gt,
        ..reverse
    };
    let kept = run(series(&ctx, x), Operand::Number(5.0), gt)
        .await
        .unwrap();
    assert_eq!(
        kept,
        vec![row(2, &[("job", "x"), ("metric.name", "a")], 3.0)]
    );
    let per_bucket = scalar(&ctx, &[(1, 2.0), (2, 10.0)]);
    let mul = run(series(&ctx, x), per_bucket, spec(BinopOp::Mul)).await;
    assert_eq!(mul.unwrap(), vec![row(1, job, 20.0), row(2, job, 30.0)]);
}

#[tokio::test]
async fn vector_match_aligns_buckets_present_on_some_sides_only() {
    let ctx = SessionContext::new();
    let a = &[("metric.name", "a"), ("job", "x")][..];
    let b = &[("metric.name", "b"), ("job", "x")][..];
    let left: &[(i64, Labels, f64)] = &[(1, a, 1.0), (2, a, 2.0)];
    let right: &[(i64, Labels, f64)] = &[(2, b, 20.0), (3, b, 30.0)];
    let add = run(series(&ctx, left), series(&ctx, right), spec(BinopOp::Add)).await;
    assert_eq!(add.unwrap(), vec![row(2, &[("job", "x")], 22.0)]);
    let or = run(series(&ctx, left), series(&ctx, right), spec(BinopOp::Or)).await;
    assert_eq!(
        or.unwrap(),
        vec![row(1, a, 1.0), row(2, a, 2.0), row(3, b, 30.0)]
    );
}

#[tokio::test]
async fn vector_match_division_by_zero_follows_ieee() {
    let ctx = SessionContext::new();
    let x: &[(i64, Labels, f64)] = &[
        (1, &[("job", "p")], 1.0),
        (1, &[("job", "n")], -1.0),
        (1, &[("job", "z")], 0.0),
    ];
    let rows = run(series(&ctx, x), Operand::Number(0.0), spec(BinopOp::Div))
        .await
        .unwrap();
    let values: Vec<f64> = rows.iter().map(|r| r.2).collect();
    assert_eq!(values[0], f64::NEG_INFINITY);
    assert_eq!(values[1], f64::INFINITY);
    assert!(values[2].is_nan());
}

#[tokio::test]
async fn vector_match_scalar_operands_yield_a_scalar_frame() {
    let ctx = SessionContext::new();
    let rows = run(
        scalar(&ctx, &[(1, 4.0), (2, 6.0)]),
        Operand::Number(2.0),
        spec(BinopOp::Pow),
    )
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![(1, String::new(), 16.0), (2, String::new(), 36.0)]
    );
    let cmp = run(
        scalar(&ctx, &[(1, 4.0)]),
        Operand::Number(2.0),
        spec(BinopOp::Gt),
    )
    .await;
    assert_invalid(cmp, "need `bool`");
    let set = run(
        scalar(&ctx, &[(1, 4.0)]),
        Operand::Number(2.0),
        spec(BinopOp::And),
    )
    .await;
    assert_invalid(set, "series on both sides");
}

#[test]
fn vector_match_rejects_two_numbers() {
    let (one, add) = (Operand::Number(1.0), spec(BinopOp::Add));
    let two = vector_match(one, Operand::Number(2.0), &add);
    assert!(matches!(two, Err(QuerierError::InvalidInput(_))));
}

#[tokio::test]
async fn vector_match_collects_every_input_partition() {
    let ctx = SessionContext::new_with_config(
        datafusion::prelude::SessionConfig::new().with_target_partitions(4),
    );
    let Operand::Series(df) = series(
        &ctx,
        &[(1, &[("job", "x")], 1.0), (1, &[("job", "y")], 2.0)],
    ) else {
        unreachable!()
    };
    let batch = df.collect().await.unwrap().remove(0);
    let parts = vec![vec![batch.slice(0, 1)], vec![batch.slice(1, 1)]];
    let table = datafusion::datasource::MemTable::try_new(batch.schema(), parts).unwrap();
    let left = Operand::Series(ctx.read_table(Arc::new(table)).unwrap());
    let rows = run(left, Operand::Number(1.0), spec(BinopOp::Add))
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![row(1, &[("job", "x")], 2.0), row(1, &[("job", "y")], 3.0)]
    );
}
