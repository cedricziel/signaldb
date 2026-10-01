//! # Formulas — arithmetic across the named queries of one request (D5)
//!
//! A [`MultiDocument`] carries several named [`Document`]s (each required to
//! yield a `series` result) plus a list of [`Formula`]s: `+ - * /` over
//! numeric constants and query names, with parentheses. A formula is
//! evaluated after every inner query has run, joining series on an
//! identical label set and timestamp — a series present in one operand but
//! absent from another contributes nothing to the join rather than erroring,
//! and a point whose divisor is zero is dropped rather than erroring.
//!
//! Parsing/validation lives here (leaf crate, no query engine); evaluation
//! over already-computed series ([`evaluate`]) is engine-agnostic too, so the
//! querier only has to run the inner queries and hand the results here.

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use super::document::{Document, ResultEnvelope};

/// One named formula: `name` is the output series' identity in the
/// response, `expr` is the arithmetic expression source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Formula {
    pub name: String,
    pub expr: String,
}

/// A multi-query document: several named queries plus formulas over their
/// `series` results. `result` is carried for symmetry with [`Document`] but
/// is always `series` — a formula document has no other shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MultiDocument {
    pub queries: BTreeMap<String, Document>,
    pub formulas: Vec<Formula>,
    pub result: ResultEnvelope,
}

/// Errors raised while parsing or validating a formula expression or a
/// [`MultiDocument`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FormulaError {
    #[error("formula '{name}': {reason}")]
    Invalid { name: String, reason: String },

    #[error("multi-query document requires at least one query")]
    NoQueries,

    #[error("multi-query document requires at least one formula")]
    NoFormulas,

    #[error("multi-query document result must be 'series', got '{found}'")]
    NotSeries { found: &'static str },

    #[error("duplicate formula name '{name}'")]
    DuplicateFormulaName { name: String },

    #[error("formula name '{name}' collides with a query name")]
    FormulaNameCollidesWithQuery { name: String },
}

/// A parsed arithmetic expression over query names and numeric constants.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Num(f64),
    /// A reference to one of the document's named queries.
    Query(String),
    Add(Box<Expr>, Box<Expr>),
    Sub(Box<Expr>, Box<Expr>),
    Mul(Box<Expr>, Box<Expr>),
    Div(Box<Expr>, Box<Expr>),
}

impl Expr {
    /// The set of query names this expression references.
    fn query_refs(&self, out: &mut std::collections::HashSet<String>) {
        match self {
            Expr::Num(_) => {}
            Expr::Query(name) => {
                out.insert(name.clone());
            }
            Expr::Add(a, b) | Expr::Sub(a, b) | Expr::Mul(a, b) | Expr::Div(a, b) => {
                a.query_refs(out);
                b.query_refs(out);
            }
        }
    }
}

/// Parse a formula expression: `+ - * /`, numeric constants, bare
/// identifiers (query names), and parentheses — standard precedence
/// (`*`/`/` bind tighter than `+`/`-`), left-associative.
pub fn parse_expr(src: &str) -> Result<Expr, String> {
    let tokens = tokenize(src)?;
    if tokens.is_empty() {
        return Err("empty expression".to_string());
    }
    let mut parser = ExprParser { tokens, pos: 0 };
    let expr = parser.parse_add_sub()?;
    if parser.pos != parser.tokens.len() {
        return Err(format!("unexpected trailing input at token {}", parser.pos));
    }
    Ok(expr)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Num(f64),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
}

fn tokenize(src: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = src.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            ' ' | '\t' | '\n' | '\r' => i += 1,
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            c if c.is_ascii_digit() || c == '.' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                let n: f64 = text
                    .parse()
                    .map_err(|_| format!("invalid number '{text}'"))?;
                tokens.push(Token::Num(n));
            }
            c if c.is_alphanumeric() || c == '_' || c == '.' || c == '-' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                {
                    i += 1;
                }
                let text: String = chars[start..i].iter().collect();
                tokens.push(Token::Ident(text));
            }
            other => return Err(format!("unexpected character '{other}'")),
        }
    }
    Ok(tokens)
}

struct ExprParser {
    tokens: Vec<Token>,
    pos: usize,
}

impl ExprParser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn parse_add_sub(&mut self) -> Result<Expr, String> {
        let mut lhs = self.parse_mul_div()?;
        loop {
            match self.peek() {
                Some(Token::Plus) => {
                    self.pos += 1;
                    let rhs = self.parse_mul_div()?;
                    lhs = Expr::Add(Box::new(lhs), Box::new(rhs));
                }
                Some(Token::Minus) => {
                    self.pos += 1;
                    let rhs = self.parse_mul_div()?;
                    lhs = Expr::Sub(Box::new(lhs), Box::new(rhs));
                }
                _ => break,
            }
        }
        Ok(lhs)
    }

    fn parse_mul_div(&mut self) -> Result<Expr, String> {
        let mut lhs = self.parse_atom()?;
        loop {
            match self.peek() {
                Some(Token::Star) => {
                    self.pos += 1;
                    let rhs = self.parse_atom()?;
                    lhs = Expr::Mul(Box::new(lhs), Box::new(rhs));
                }
                Some(Token::Slash) => {
                    self.pos += 1;
                    let rhs = self.parse_atom()?;
                    lhs = Expr::Div(Box::new(lhs), Box::new(rhs));
                }
                _ => break,
            }
        }
        Ok(lhs)
    }

    fn parse_atom(&mut self) -> Result<Expr, String> {
        match self.tokens.get(self.pos).cloned() {
            Some(Token::Num(n)) => {
                self.pos += 1;
                Ok(Expr::Num(n))
            }
            Some(Token::Ident(name)) => {
                self.pos += 1;
                Ok(Expr::Query(name))
            }
            Some(Token::Minus) => {
                self.pos += 1;
                let inner = self.parse_atom()?;
                Ok(Expr::Sub(Box::new(Expr::Num(0.0)), Box::new(inner)))
            }
            Some(Token::LParen) => {
                self.pos += 1;
                let inner = self.parse_add_sub()?;
                match self.tokens.get(self.pos) {
                    Some(Token::RParen) => {
                        self.pos += 1;
                        Ok(inner)
                    }
                    _ => Err("expected ')'".to_string()),
                }
            }
            other => Err(format!("unexpected token {other:?}")),
        }
    }
}

/// Validate a [`MultiDocument`]: every referenced query exists and yields a
/// `series` result, formula names are unique and don't collide with a query
/// name, and every formula expression parses.
///
/// `series_results` maps each query name to the [`ResultEnvelope`] its own
/// (already-validated) document declares — the caller validates each inner
/// [`Document`] with [`super::validate::validate`] itself, since that needs a
/// per-source resolver this crate does not have.
pub fn validate_multi(
    doc: &MultiDocument,
    inner_envelopes: &HashMap<String, ResultEnvelope>,
) -> Result<(), FormulaError> {
    if doc.queries.is_empty() {
        return Err(FormulaError::NoQueries);
    }
    if doc.formulas.is_empty() {
        return Err(FormulaError::NoFormulas);
    }
    if doc.result != ResultEnvelope::Series {
        return Err(FormulaError::NotSeries {
            found: doc.result.as_str(),
        });
    }
    for (name, envelope) in inner_envelopes {
        if *envelope != ResultEnvelope::Series {
            return Err(FormulaError::Invalid {
                name: name.clone(),
                reason: "inner queries must yield a `series` result".to_string(),
            });
        }
    }

    let mut seen_formula_names = std::collections::HashSet::new();
    for formula in &doc.formulas {
        if doc.queries.contains_key(&formula.name) {
            return Err(FormulaError::FormulaNameCollidesWithQuery {
                name: formula.name.clone(),
            });
        }
        if !seen_formula_names.insert(formula.name.clone()) {
            return Err(FormulaError::DuplicateFormulaName {
                name: formula.name.clone(),
            });
        }
        let expr = parse_expr(&formula.expr).map_err(|reason| FormulaError::Invalid {
            name: formula.name.clone(),
            reason,
        })?;
        let mut refs = std::collections::HashSet::new();
        expr.query_refs(&mut refs);
        for r in refs {
            if !doc.queries.contains_key(&r) {
                return Err(FormulaError::Invalid {
                    name: formula.name.clone(),
                    reason: format!("references unknown query '{r}'"),
                });
            }
        }
    }
    Ok(())
}

/// One series' identity: its sorted label set. Two series with the same
/// labels but from different queries are the same join key.
pub type LabelKey = Vec<(String, String)>;

fn label_key(labels: &BTreeMap<String, String>) -> LabelKey {
    labels.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

/// One evaluated series: its label set plus `timestamp -> value` points.
#[derive(Debug, Clone, PartialEq)]
pub struct EvalSeries {
    pub labels: BTreeMap<String, String>,
    pub points: BTreeMap<i64, f64>,
}

/// An expression's intermediate value while evaluating: either a scalar
/// constant (broadcasts over every series it meets) or a set of series keyed
/// by label identity.
enum Value {
    Scalar(f64),
    Series(HashMap<LabelKey, EvalSeries>),
}

/// Evaluate a formula expression against already-computed named query
/// results. Returns one [`EvalSeries`] per label set common to every query
/// name the expression references (a label set only one side has yields no
/// output series); a point whose divisor is zero is dropped, not an error.
pub fn evaluate(expr: &Expr, inputs: &HashMap<String, Vec<EvalSeries>>) -> Vec<EvalSeries> {
    match eval(expr, inputs) {
        Value::Scalar(_) => Vec::new(),
        Value::Series(map) => map.into_values().collect(),
    }
}

fn eval(expr: &Expr, inputs: &HashMap<String, Vec<EvalSeries>>) -> Value {
    match expr {
        Expr::Num(n) => Value::Scalar(*n),
        Expr::Query(name) => {
            let series = inputs.get(name).cloned().unwrap_or_default();
            let map = series
                .into_iter()
                .map(|s| (label_key(&s.labels), s))
                .collect();
            Value::Series(map)
        }
        Expr::Add(a, b) => combine(eval(a, inputs), eval(b, inputs), |x, y| Some(x + y)),
        Expr::Sub(a, b) => combine(eval(a, inputs), eval(b, inputs), |x, y| Some(x - y)),
        Expr::Mul(a, b) => combine(eval(a, inputs), eval(b, inputs), |x, y| Some(x * y)),
        Expr::Div(a, b) => combine(eval(a, inputs), eval(b, inputs), |x, y| {
            (y != 0.0).then_some(x / y)
        }),
    }
}

/// Apply a binary op over two evaluated values, broadcasting a scalar over
/// every point of the other side's series and inner-joining two series sets
/// on label identity then timestamp. `op` returning `None` (division by
/// zero) drops that point rather than producing one.
fn combine(a: Value, b: Value, op: impl Fn(f64, f64) -> Option<f64>) -> Value {
    match (a, b) {
        (Value::Scalar(x), Value::Scalar(y)) => match op(x, y) {
            Some(v) => Value::Scalar(v),
            None => Value::Scalar(f64::NAN),
        },
        (Value::Scalar(x), Value::Series(s)) => Value::Series(map_points(s, |v| op(x, v))),
        (Value::Series(s), Value::Scalar(y)) => Value::Series(map_points(s, |v| op(v, y))),
        (Value::Series(a), Value::Series(b)) => {
            let mut out = HashMap::new();
            for (key, sa) in &a {
                let Some(sb) = b.get(key) else { continue };
                let mut points = BTreeMap::new();
                for (t, va) in &sa.points {
                    let Some(vb) = sb.points.get(t) else { continue };
                    if let Some(v) = op(*va, *vb) {
                        points.insert(*t, v);
                    }
                }
                if !points.is_empty() {
                    out.insert(
                        key.clone(),
                        EvalSeries {
                            labels: sa.labels.clone(),
                            points,
                        },
                    );
                }
            }
            Value::Series(out)
        }
    }
}

fn map_points(
    series: HashMap<LabelKey, EvalSeries>,
    f: impl Fn(f64) -> Option<f64>,
) -> HashMap<LabelKey, EvalSeries> {
    series
        .into_iter()
        .filter_map(|(key, s)| {
            let points: BTreeMap<i64, f64> = s
                .points
                .into_iter()
                .filter_map(|(t, v)| f(v).map(|v| (t, v)))
                .collect();
            (!points.is_empty()).then_some((
                key,
                EvalSeries {
                    labels: s.labels,
                    points,
                },
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn series(labels: &[(&str, &str)], points: &[(i64, f64)]) -> EvalSeries {
        EvalSeries {
            labels: labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            points: points.iter().copied().collect(),
        }
    }

    #[test]
    fn parses_precedence_and_parens() {
        let expr = parse_expr("a + b * c").unwrap();
        assert_eq!(
            expr,
            Expr::Add(
                Box::new(Expr::Query("a".into())),
                Box::new(Expr::Mul(
                    Box::new(Expr::Query("b".into())),
                    Box::new(Expr::Query("c".into()))
                ))
            )
        );
        let expr = parse_expr("(a + b) * c").unwrap();
        assert_eq!(
            expr,
            Expr::Mul(
                Box::new(Expr::Add(
                    Box::new(Expr::Query("a".into())),
                    Box::new(Expr::Query("b".into()))
                )),
                Box::new(Expr::Query("c".into()))
            )
        );
    }

    #[test]
    fn parses_numeric_constants() {
        assert_eq!(parse_expr("2.5").unwrap(), Expr::Num(2.5));
    }

    #[test]
    fn rejects_an_invalid_expression() {
        for bad in ["a +", "a ** b", "(a + b", "a % b"] {
            assert!(parse_expr(bad).is_err(), "{bad} should be rejected");
        }
    }

    // Task 5.1 — error ratio: a/b, one service missing from `a`.
    #[test]
    fn error_ratio_drops_a_service_missing_from_the_numerator() {
        let expr = parse_expr("errors / total").unwrap();
        let mut inputs = HashMap::new();
        inputs.insert(
            "errors".to_string(),
            vec![series(&[("service", "api")], &[(0, 5.0), (60, 10.0)])],
        );
        inputs.insert(
            "total".to_string(),
            vec![
                series(&[("service", "api")], &[(0, 50.0), (60, 100.0)]),
                series(&[("service", "worker")], &[(0, 20.0)]),
            ],
        );
        let out = evaluate(&expr, &inputs);
        assert_eq!(out.len(), 1, "only 'api' is common to both queries");
        let api = &out[0];
        assert_eq!(api.labels.get("service"), Some(&"api".to_string()));
        assert_eq!(api.points.get(&0), Some(&0.1));
        assert_eq!(api.points.get(&60), Some(&0.1));
    }

    // Task 5.1 — divide by zero: the point is dropped, not an error.
    #[test]
    fn division_by_zero_drops_the_point() {
        let expr = parse_expr("a / b").unwrap();
        let mut inputs = HashMap::new();
        inputs.insert(
            "a".to_string(),
            vec![series(&[("service", "api")], &[(0, 5.0), (60, 10.0)])],
        );
        inputs.insert(
            "b".to_string(),
            vec![series(&[("service", "api")], &[(0, 0.0), (60, 2.0)])],
        );
        let out = evaluate(&expr, &inputs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].points.get(&0), None, "t=0 divides by zero");
        assert_eq!(out[0].points.get(&60), Some(&5.0));
    }

    // Task 5.1 — missing series entirely (a name with no matching label set
    // on either side) yields no output series.
    #[test]
    fn a_series_present_on_only_one_side_yields_nothing() {
        let expr = parse_expr("a / b").unwrap();
        let mut inputs = HashMap::new();
        inputs.insert(
            "a".to_string(),
            vec![series(&[("service", "api")], &[(0, 5.0)])],
        );
        inputs.insert("b".to_string(), vec![]);
        let out = evaluate(&expr, &inputs);
        assert!(out.is_empty());
    }

    #[test]
    fn a_constant_broadcasts_over_every_series() {
        let expr = parse_expr("a * 100").unwrap();
        let mut inputs = HashMap::new();
        inputs.insert(
            "a".to_string(),
            vec![series(&[("service", "api")], &[(0, 0.5)])],
        );
        let out = evaluate(&expr, &inputs);
        assert_eq!(out[0].points.get(&0), Some(&50.0));
    }

    // Task 5.1 — invalid expression is rejected at validation.
    #[test]
    fn validate_multi_rejects_an_invalid_expression() {
        let doc = MultiDocument {
            queries: BTreeMap::from([(
                "a".to_string(),
                serde_json::from_value(json!({
                    "irVersion": 1, "from": "traces",
                    "range": { "from": "now-1h", "to": "now" }, "result": "series",
                    "pipeline": [{ "aggregate": { "by": [], "aggs": [{ "fn": "count", "as": "n" }], "step": "1m" } }]
                }))
                .unwrap(),
            )]),
            formulas: vec![Formula {
                name: "f".to_string(),
                expr: "a +".to_string(),
            }],
            result: ResultEnvelope::Series,
        };
        let err = validate_multi(
            &doc,
            &HashMap::from([("a".to_string(), ResultEnvelope::Series)]),
        )
        .unwrap_err();
        assert!(matches!(err, FormulaError::Invalid { .. }), "got {err:?}");
    }

    #[test]
    fn validate_multi_rejects_a_non_series_inner_query() {
        let doc = MultiDocument {
            queries: BTreeMap::from([(
                "a".to_string(),
                serde_json::from_value(json!({
                    "irVersion": 1, "from": "traces",
                    "range": { "from": "now-1h", "to": "now" }, "result": "rows",
                    "pipeline": []
                }))
                .unwrap(),
            )]),
            formulas: vec![Formula {
                name: "f".to_string(),
                expr: "a * 2".to_string(),
            }],
            result: ResultEnvelope::Series,
        };
        let err = validate_multi(
            &doc,
            &HashMap::from([("a".to_string(), ResultEnvelope::Rows)]),
        )
        .unwrap_err();
        assert!(matches!(err, FormulaError::Invalid { .. }), "got {err:?}");
    }

    #[test]
    fn validate_multi_rejects_a_reference_to_an_unknown_query() {
        let doc = MultiDocument {
            queries: BTreeMap::from([(
                "a".to_string(),
                serde_json::from_value(json!({
                    "irVersion": 1, "from": "traces",
                    "range": { "from": "now-1h", "to": "now" }, "result": "series",
                    "pipeline": [{ "aggregate": { "by": [], "aggs": [{ "fn": "count", "as": "n" }], "step": "1m" } }]
                }))
                .unwrap(),
            )]),
            formulas: vec![Formula {
                name: "f".to_string(),
                expr: "a / b".to_string(),
            }],
            result: ResultEnvelope::Series,
        };
        let err = validate_multi(
            &doc,
            &HashMap::from([("a".to_string(), ResultEnvelope::Series)]),
        )
        .unwrap_err();
        assert!(
            matches!(err, FormulaError::Invalid { ref reason, .. } if reason.contains("unknown query 'b'")),
            "got {err:?}"
        );
    }
}
