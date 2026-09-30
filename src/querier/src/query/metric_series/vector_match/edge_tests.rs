use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use common::query_ir::{Binop, BinopOp, GroupSide};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

use super::tests::{Labels, grouped, on, row, run, scalar, series, spec};
use super::{Operand, vector_match};
use crate::query::correlate_cap::wrap_with_cap;
use crate::query::error::QuerierError;

fn ignoring(op: BinopOp, labels: &[&str]) -> Binop {
    Binop {
        ignoring: Some(labels.iter().map(|l| l.to_string()).collect()),
        ..spec(op)
    }
}

fn as_bool(op: BinopOp) -> Binop {
    Binop {
        bool: true,
        ..spec(op)
    }
}

fn assert_invalid(result: Result<Vec<super::tests::Row>, QuerierError>, needle: &str) {
    match result {
        Err(QuerierError::InvalidInput(e)) => assert!(e.contains(needle), "{e}"),
        other => panic!("expected invalid input containing {needle:?}, got {other:?}"),
    }
}

const X: Labels = &[("metric.name", "a"), ("job", "x")];
const JOB_X: Labels = &[("job", "x")];

#[tokio::test]
async fn vector_match_missing_scalar_bucket_is_nan() {
    let ctx = SessionContext::new();
    let x = &[(1, X, 10.0), (2, X, 3.0)];
    let add = run(
        series(&ctx, x),
        scalar(&ctx, &[(1, 2.0)]),
        spec(BinopOp::Add),
    )
    .await;
    let add = add.unwrap();
    assert_eq!(add[0], row(1, JOB_X, 12.0));
    assert_eq!((add[1].0, add.len()), (2, 2));
    assert!(add[1].2.is_nan());
    let gt = run(
        series(&ctx, x),
        scalar(&ctx, &[(1, 2.0)]),
        spec(BinopOp::Gt),
    )
    .await;
    assert_eq!(gt.unwrap(), vec![row(1, X, 10.0)]);
}

#[tokio::test]
async fn vector_match_series_against_scalar_bool() {
    let ctx = SessionContext::new();
    let x = &[(1, X, 10.0), (2, X, 3.0)];
    let rows = run(series(&ctx, x), Operand::Number(5.0), as_bool(BinopOp::Gt)).await;
    assert_eq!(rows.unwrap(), vec![row(1, JOB_X, 1.0), row(2, JOB_X, 0.0)]);
}

#[tokio::test]
async fn vector_match_group_right_takes_include_from_the_left() {
    let ctx = SessionContext::new();
    let left = series(
        &ctx,
        &[(
            1,
            &[("metric.name", "info"), ("job", "x"), ("version", "v1")],
            1.0,
        )],
    );
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
                20.0,
            ),
        ],
    );
    let binop = grouped(BinopOp::Mul, GroupSide::Right, &["version"]);
    assert_eq!(
        run(left, right, binop).await.unwrap(),
        vec![
            row(1, &[("inst", "1"), ("job", "x"), ("version", "v1")], 10.0),
            row(1, &[("inst", "2"), ("job", "x"), ("version", "v1")], 20.0),
        ]
    );
}

#[tokio::test]
async fn vector_match_group_right_filter_keeps_the_left_value() {
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
                200.0,
            ),
        ],
    );
    let rows = run(left, right, grouped(BinopOp::Gt, GroupSide::Right, &[])).await;
    let kept = &[("metric.name", "a"), ("job", "x"), ("inst", "1")];
    assert_eq!(rows.unwrap(), vec![row(1, kept, 100.0)]);
}

#[tokio::test]
async fn vector_match_on_metric_name() {
    let ctx = SessionContext::new();
    let left = series(&ctx, &[(1, X, 1.0)]);
    let right = series(&ctx, &[(1, &[("metric.name", "a"), ("job", "y")], 2.0)]);
    let rows = run(left, right, on(BinopOp::Add, &["metric.name"])).await;
    assert_eq!(rows.unwrap(), vec![row(1, &[], 3.0)]);
}

#[tokio::test]
async fn vector_match_set_ops_honour_ignoring() {
    let ctx = SessionContext::new();
    let l: &[(i64, Labels, f64)] =
        &[(1, &[("metric.name", "a"), ("job", "x"), ("inst", "1")], 1.0)];
    let r: &[(i64, Labels, f64)] =
        &[(1, &[("metric.name", "b"), ("job", "x"), ("inst", "2")], 2.0)];
    let eval = |binop| run(series(&ctx, l), series(&ctx, r), binop);
    let and = eval(ignoring(BinopOp::And, &["inst"])).await.unwrap();
    assert_eq!(and, vec![row(l[0].0, l[0].1, 1.0)]);
    assert!(
        eval(ignoring(BinopOp::Unless, &["inst"]))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(eval(spec(BinopOp::And)).await.unwrap().is_empty());
}

#[tokio::test]
async fn vector_match_and_unless_with_one_side_empty_at_a_bucket() {
    let ctx = SessionContext::new();
    let l = &[(1, X, 1.0), (2, X, 2.0)];
    let r: &[(i64, Labels, f64)] = &[(1, &[("metric.name", "b"), ("job", "x")], 9.0)];
    let and = run(series(&ctx, l), series(&ctx, r), spec(BinopOp::And)).await;
    assert_eq!(and.unwrap(), vec![row(1, X, 1.0)]);
    let unless = run(series(&ctx, l), series(&ctx, r), spec(BinopOp::Unless)).await;
    assert_eq!(unless.unwrap(), vec![row(2, X, 2.0)]);
}

#[tokio::test]
async fn vector_match_scalars_with_disjoint_buckets_are_nan() {
    let ctx = SessionContext::new();
    let rows = run(
        scalar(&ctx, &[(1, 4.0)]),
        scalar(&ctx, &[(2, 6.0)]),
        spec(BinopOp::Add),
    )
    .await
    .unwrap();
    assert_eq!(rows.iter().map(|r| r.0).collect::<Vec<_>>(), vec![1, 2]);
    assert!(rows.iter().all(|r| r.2.is_nan()));
}

#[tokio::test]
async fn vector_match_comparisons_against_nan() {
    let ctx = SessionContext::new();
    let x = &[(1, X, f64::NAN)];
    let gt = run(series(&ctx, x), Operand::Number(1.0), spec(BinopOp::Gt)).await;
    assert!(gt.unwrap().is_empty());
    let ne = run(series(&ctx, x), Operand::Number(1.0), spec(BinopOp::Ne)).await;
    assert!(ne.unwrap()[0].2.is_nan());
    let eq = run(
        series(&ctx, x),
        Operand::Number(f64::NAN),
        as_bool(BinopOp::Eq),
    )
    .await;
    assert_eq!(eq.unwrap(), vec![row(1, JOB_X, 0.0)]);
}

#[tokio::test]
async fn vector_match_one_to_one_counts_only_pairs_that_pass_the_filter() {
    let ctx = SessionContext::new();
    let left = series(
        &ctx,
        &[
            (1, &[("metric.name", "a"), ("job", "x"), ("inst", "1")], 5.0),
            (1, &[("metric.name", "a"), ("job", "x"), ("inst", "2")], 1.0),
        ],
    );
    let right = series(&ctx, &[(1, &[("metric.name", "b"), ("job", "x")], 3.0)]);
    let rows = run(left, right, on(BinopOp::Gt, &["job"])).await;
    assert_eq!(rows.unwrap(), vec![row(1, JOB_X, 5.0)]);
}

#[tokio::test]
async fn vector_match_skips_cardinality_checks_where_a_side_is_empty() {
    let ctx = SessionContext::new();
    let left = series(&ctx, &[(2, X, 1.0)]);
    let r1 = &[("metric.name", "b"), ("job", "x"), ("inst", "1")][..];
    let r2 = &[("metric.name", "b"), ("job", "x"), ("inst", "2")][..];
    let right = series(&ctx, &[(1, r1, 1.0), (1, r2, 1.0), (2, r1, 1.0)]);
    let rows = run(left, right, on(BinopOp::Add, &["job"])).await;
    assert_eq!(rows.unwrap(), vec![row(2, JOB_X, 2.0)]);
}

#[tokio::test]
async fn vector_match_rejects_malformed_frames_and_specs() {
    let ctx = SessionContext::new();
    let dup = run(
        series(&ctx, &[(1, X, 1.0), (1, X, 2.0)]),
        Operand::Number(1.0),
        spec(BinopOp::Add),
    );
    assert_invalid(dup.await, "same labelset");
    let both = Binop {
        on: Some(vec!["job".to_string()]),
        ..ignoring(BinopOp::Add, &["inst"])
    };
    let both = run(
        series(&ctx, &[(1, X, 1.0)]),
        series(&ctx, &[(1, X, 1.0)]),
        both,
    )
    .await;
    assert_invalid(both, "mutually exclusive");
}

#[tokio::test]
async fn vector_match_empty_label_value_is_absent_from_the_key() {
    let ctx = SessionContext::new();
    let left = series(
        &ctx,
        &[(1, &[("metric.name", "a"), ("job", "x"), ("env", "")], 1.0)],
    );
    let right = series(&ctx, &[(1, &[("metric.name", "b"), ("job", "x")], 2.0)]);
    let rows = run(left, right, spec(BinopOp::Add)).await;
    assert_eq!(
        rows.unwrap(),
        vec![row(1, &[("env", ""), ("job", "x")], 3.0)]
    );
}

#[tokio::test]
async fn vector_match_composes_with_the_correlate_cap() {
    let ctx = SessionContext::new();
    let rows = &[(1, X, 1.0), (2, X, 2.0), (3, X, 3.0)];
    let Operand::Series(df) = series(&ctx, rows) else {
        unreachable!()
    };
    let truncated = Arc::new(AtomicBool::new(false));
    let capped = wrap_with_cap(df, 2, Arc::clone(&truncated)).unwrap();
    let out = run(
        Operand::Series(capped),
        Operand::Number(1.0),
        spec(BinopOp::Add),
    )
    .await;
    assert_eq!(out.unwrap(), vec![row(1, JOB_X, 2.0), row(2, JOB_X, 3.0)]);
    assert!(truncated.load(Ordering::Relaxed));
}

#[tokio::test]
async fn vector_match_reserves_its_state_against_the_memory_pool() {
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(512, 1.0)
        .build_arc()
        .unwrap();
    let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
    let names: Vec<String> = (0..50).map(|i| format!("i{i}")).collect();
    let pairs: Vec<[(&str, &str); 1]> = names.iter().map(|n| [("inst", n.as_str())]).collect();
    let rows: Vec<(i64, Labels, f64)> = pairs.iter().map(|p| (1, &p[..], 1.0)).collect();
    let binop = spec(BinopOp::Add);
    let result = vector_match(series(&ctx, &rows), Operand::Number(1.0), &binop)
        .unwrap()
        .collect()
        .await;
    let err = result.unwrap_err().to_string();
    assert!(err.contains("esources exhausted"), "{err}");
}
