//! Label-set UDFs over a Series' canonical `__labels`: keep / drop /
//! drop the name, and PromQL's `label_replace` / `label_join` (D11).

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::scalar::ScalarValue;
use regex::Regex;

use super::labels::{LabelSet, METRIC_NAME, decode, encode, utf8_array};
use crate::query::metric_ops::instants::invalid;

/// `label_replace`: when `regex` fully matches `src`'s value (absent = ""),
/// set `dst` to the expanded `replacement`; an empty result removes `dst`.
pub(crate) fn label_replace(
    labels: &mut LabelSet,
    dst: &str,
    replacement: &str,
    src: &str,
    regex: &Regex,
) {
    let value = labels.get(src).map_or("", String::as_str);
    let Some(caps) = regex.captures(value) else {
        return;
    };
    let mut out = String::new();
    caps.expand(replacement, &mut out);
    set_or_remove(labels, dst, out);
}

/// `label_join`: `dst` = the `srcs` values (absent = "") joined by `sep`;
/// an empty result removes `dst`.
pub(crate) fn label_join(labels: &mut LabelSet, dst: &str, sep: &str, srcs: &[String]) {
    let joined = srcs
        .iter()
        .map(|s| labels.get(s).map_or("", String::as_str))
        .collect::<Vec<_>>()
        .join(sep);
    set_or_remove(labels, dst, joined);
}

fn set_or_remove(labels: &mut LabelSet, key: &str, value: String) {
    if value.is_empty() {
        labels.remove(key);
    } else {
        labels.insert(key.to_string(), value);
    }
}

/// A PromQL `label_replace` regex: anchored at both ends.
pub(crate) fn anchored_regex(pattern: &str) -> Result<Regex> {
    Regex::new(&format!("^(?s:{pattern})$"))
        .map_err(|e| invalid(format!("label_replace: invalid regex: {e}")))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Op {
    Keep,
    Drop,
    Replace,
    Join,
    DropName,
}

impl Op {
    fn name(self) -> &'static str {
        match self {
            Op::Keep => "labels_keep",
            Op::Drop => "labels_drop",
            Op::Replace => "label_replace",
            Op::Join => "label_join",
            Op::DropName => "labels_drop_name",
        }
    }

    /// The constant operands after `labels`: exactly, or at least (variadic).
    fn operands(self) -> (usize, bool) {
        match self {
            Op::Keep | Op::Drop => (0, true),
            Op::Replace => (4, false),
            Op::Join => (2, true),
            Op::DropName => (0, false),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct LabelOp {
    op: Op,
    signature: Signature,
}

fn udf(op: Op) -> ScalarUDF {
    ScalarUDF::new_from_impl(LabelOp {
        op,
        signature: Signature::variadic_any(Volatility::Immutable),
    })
}

/// `labels_keep(labels, key…)`: only the named labels.
pub(crate) fn labels_keep_udf() -> ScalarUDF {
    udf(Op::Keep)
}
/// `labels_drop(labels, key…)`: every label but the named ones.
pub(crate) fn labels_drop_udf() -> ScalarUDF {
    udf(Op::Drop)
}
/// `label_replace(labels, dst, replacement, src, regex)`, PromQL semantics.
pub(crate) fn label_replace_udf() -> ScalarUDF {
    udf(Op::Replace)
}
/// `label_join(labels, dst, sep, src…)`, PromQL semantics.
pub(crate) fn label_join_udf() -> ScalarUDF {
    udf(Op::Join)
}
/// `labels_drop_name(labels)`: the label set without `metric.name`.
pub(crate) fn labels_drop_name_udf() -> ScalarUDF {
    udf(Op::DropName)
}

/// A constant string operand (a label name, separator, regex, …).
fn literal(op: Op, arg: &ColumnarValue) -> Result<String> {
    match arg {
        ColumnarValue::Scalar(
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)),
        ) => Ok(s.clone()),
        _ => Err(invalid(format!(
            "{}: label operands must be non-null string literals",
            op.name()
        ))),
    }
}

impl ScalarUDFImpl for LabelOp {
    fn name(&self) -> &str {
        self.op.name()
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let (n, variadic) = self.op.operands();
        let got = arg_types.len().saturating_sub(1);
        if arg_types.is_empty() || got < n || (!variadic && got > n) {
            return Err(invalid(format!(
                "{} takes labels and {}{n} operands, got {got}",
                self.op.name(),
                if variadic { "at least " } else { "" },
            )));
        }
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let consts = args.args[1..]
            .iter()
            .map(|a| literal(self.op, a))
            .collect::<Result<Vec<_>>>()?;
        let regex = match self.op {
            Op::Replace => Some(anchored_regex(&consts[3])?),
            _ => None,
        };
        let rewrite = |labels: &str| -> Result<String> {
            let mut set = decode(labels)?;
            match (self.op, &regex) {
                (Op::Keep, _) => set.retain(|k, _| consts.contains(k)),
                (Op::Drop, _) => set.retain(|k, _| !consts.contains(k)),
                (Op::DropName, _) => {
                    set.remove(METRIC_NAME);
                }
                (Op::Replace, Some(re)) => {
                    label_replace(&mut set, &consts[0], &consts[1], &consts[2], re)
                }
                (Op::Replace, None) => {
                    return Err(DataFusionError::Internal("regex not compiled".into()));
                }
                (Op::Join, _) => label_join(&mut set, &consts[0], &consts[1], &consts[2..]),
            }
            encode(&set)
        };
        let labels = utf8_array(&args.args[0], args.number_rows)?;
        let labels = labels.as_string::<i32>();
        // A batch repeats few label sets many times (one per instant).
        let mut seen: HashMap<&str, String> = HashMap::new();
        let mut out = StringBuilder::new();
        for row in 0..labels.len() {
            if labels.is_null(row) {
                out.append_null();
                continue;
            }
            let set = labels.value(row);
            if !seen.contains_key(set) {
                seen.insert(set, rewrite(set)?);
            }
            out.append_option(seen.get(set));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{RecordBatch, StringArray};
    use datafusion::logical_expr::{Expr, lit};
    use datafusion::prelude::{SessionContext, ident};

    use super::*;

    fn set(pairs: &[(&str, &str)]) -> LabelSet {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn get<'a>(s: &'a LabelSet, key: &str) -> Option<&'a str> {
        s.get(key).map(String::as_str)
    }

    #[test]
    fn replace_follows_promql() {
        let re = |r: &str| anchored_regex(r).unwrap();
        let mut s = set(&[("service.name", "api-1")]);
        label_replace(&mut s, "svc", "$1", "service.name", &re("(.*)-\\d"));
        assert_eq!(get(&s, "svc"), Some("api"));
        // A partial match is no match: unchanged.
        label_replace(&mut s, "svc", "x", "service.name", &re("api"));
        assert_eq!(get(&s, "svc"), Some("api"));
        // An empty replacement removes the destination.
        label_replace(&mut s, "svc", "", "service.name", &re(".*"));
        assert_eq!(get(&s, "svc"), None);
        // An absent source matches as "".
        label_replace(&mut s, "new", "y", "missing", &re(""));
        assert_eq!(get(&s, "new"), Some("y"));
    }

    #[test]
    fn join_reads_absent_sources_as_empty() {
        let mut s = set(&[("a", "1"), ("b", "2")]);
        label_join(
            &mut s,
            "ab",
            "-",
            &["a".into(), "missing".into(), "b".into()],
        );
        assert_eq!(get(&s, "ab"), Some("1--2"));
        label_join(&mut s, "a", ",", &["missing".into()]);
        assert_eq!(get(&s, "a"), None);
    }

    async fn eval(expr: Expr) -> datafusion::error::Result<Vec<Option<String>>> {
        let rows = StringArray::from(vec![
            Some(r#"{"a":"1","metric.name":"m","service.name":"s"}"#),
            None,
        ]);
        let batch = RecordBatch::try_from_iter([("l", Arc::new(rows) as _)])?;
        let ctx = SessionContext::new();
        ctx.register_batch("t", batch)?;
        let out = ctx.table("t").await?.select(vec![expr])?.collect().await?;
        let col = out[0].column(0).as_string::<i32>();
        Ok((0..col.len())
            .map(|i| col.is_valid(i).then(|| col.value(i).to_string()))
            .collect())
    }

    #[tokio::test]
    async fn the_udfs_rewrite_canonical_label_sets() {
        let l = || ident("l");
        let cases = [
            (
                labels_keep_udf().call(vec![l(), lit("a"), lit("service.name")]),
                r#"{"a":"1","service.name":"s"}"#,
            ),
            (
                labels_drop_udf().call(vec![l(), lit("a")]),
                r#"{"metric.name":"m","service.name":"s"}"#,
            ),
            (
                labels_drop_name_udf().call(vec![l()]),
                r#"{"a":"1","service.name":"s"}"#,
            ),
            (
                label_replace_udf().call(vec![l(), lit("b"), lit("x$1"), lit("a"), lit("(.)")]),
                r#"{"a":"1","b":"x1","metric.name":"m","service.name":"s"}"#,
            ),
            (
                label_join_udf().call(vec![l(), lit("j"), lit("/"), lit("a"), lit("service.name")]),
                r#"{"a":"1","j":"1/s","metric.name":"m","service.name":"s"}"#,
            ),
        ];
        for (expr, want) in cases {
            assert_eq!(eval(expr).await.unwrap(), [Some(want.to_string()), None]);
        }
    }

    #[tokio::test]
    async fn bad_operands_are_rejected() {
        let bad_regex =
            label_replace_udf().call(vec![ident("l"), lit("a"), lit(""), lit("b"), lit("(")]);
        let arity = label_join_udf().call(vec![ident("l"), lit("a")]);
        let not_literal = labels_keep_udf().call(vec![ident("l"), ident("l")]);
        for (expr, want) in [
            (bad_regex, "invalid regex"),
            (arity, "label_join takes"),
            (not_literal, "string literals"),
        ] {
            let err = crate::query::error::QuerierError::from(eval(expr).await.unwrap_err());
            assert!(
                matches!(&err, crate::query::error::QuerierError::InvalidInput(m) if m.contains(want)),
                "{err}"
            );
        }
    }
}
