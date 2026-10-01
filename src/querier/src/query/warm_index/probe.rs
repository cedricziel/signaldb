//! Recognizes an equality-shaped filter over a typed-attribute home as a set
//! of warm-index containment tokens ([`probe_clauses`]) for
//! [`super::prefilter::prefilter_files`] to check against each file's bloom
//! filter. Anything unrecognized is dropped rather than guessed at, so every
//! returned clause is safe to AND against a file without risking a false
//! negative.
#![cfg_attr(not(test), allow(dead_code))]

use common::attrs::typed::HomeValue;
use common::attrs::warm_index::encode_token;
use common::schema::type_authority::CanonicalType;
use common::schema::typed_attributes::canonical_of_home_column;
use datafusion::arrow::array::{Array, Float64Array, Int64Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};

/// One recognized equality's candidate tokens: a row's warm index contains a
/// match for this clause if it contains *any* of these tokens (an OR).
/// [`probe_clauses`] returns one clause per recognized filter, ANDed
/// together — a file/row-group must satisfy every clause to survive.
pub(crate) type ProbeClause = Vec<Vec<u8>>;

/// Recognizes, at the top level of each of `filters` (already split into
/// conjuncts by the caller), an equality shape the warm index can answer:
/// a direct or coalesced typed-home `= literal`, an `IN` list, or an `OR` of
/// recognized leaves. Anything else — `!=`, ranges, regex, `label_*`
/// columns, an unrecognized shape — is dropped rather than guessed at, so
/// the returned clauses are always safe to AND against a file's bloom
/// filters without risking a false negative.
pub(crate) fn probe_clauses(filters: &[Expr]) -> Vec<ProbeClause> {
    filters.iter().filter_map(recognize_conjunct).collect()
}

fn recognize_conjunct(expr: &Expr) -> Option<ProbeClause> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => recognize_eq(left, right),
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Or,
            right,
        }) => {
            let mut clause = recognize_conjunct(left)?;
            clause.extend(recognize_conjunct(right)?);
            Some(clause)
        }
        Expr::InList(in_list) => recognize_in_list(in_list),
        _ => None,
    }
}

fn recognize_eq(left: &Expr, right: &Expr) -> Option<ProbeClause> {
    match (left, right) {
        (_, Expr::Literal(lit, _)) => recognize_field_value(left, lit),
        (Expr::Literal(lit, _), _) => recognize_field_value(right, lit),
        _ => None,
    }
}

fn recognize_in_list(in_list: &InList) -> Option<ProbeClause> {
    if in_list.negated {
        return None;
    }
    let mut tokens = Vec::new();
    for item in &in_list.list {
        let Expr::Literal(lit, _) = item else {
            return None;
        };
        tokens.extend(recognize_field_value(&in_list.expr, lit)?);
    }
    Some(tokens)
}

/// Dispatches a field-side expression to the single-home or coalesced-home
/// recognizer, given the literal it was compared against.
fn recognize_field_value(field_expr: &Expr, lit: &ScalarValue) -> Option<ProbeClause> {
    if let Some((home, key)) = as_get_field(field_expr) {
        return single_home_token(home, key, lit).map(|token| vec![token]);
    }
    let args = as_scalar_call(field_expr, "coalesce")?;
    recognize_same_typed_homes(args, lit).or_else(|| recognize_compat_coalesce(args, lit))
}

/// `get_field(<column>, <string literal key>)`, the shape every typed-home
/// accessor lowers to.
fn as_get_field(expr: &Expr) -> Option<(&str, &str)> {
    let args = as_scalar_call(expr, "get_field")?;
    let [container, key] = args else { return None };
    let Expr::Column(column) = container else {
        return None;
    };
    let Expr::Literal(ScalarValue::Utf8(Some(key)), _) = key else {
        return None;
    };
    Some((column.name.as_str(), key.as_str()))
}

fn as_scalar_call<'a>(expr: &'a Expr, name: &str) -> Option<&'a [Expr]> {
    let Expr::ScalarFunction(func) = expr else {
        return None;
    };
    (func.func.name() == name).then_some(func.args.as_slice())
}

/// The [`CanonicalType`] a typed-home column name implies.
fn canonical_from_suffix(column: &str) -> Option<CanonicalType> {
    canonical_of_home_column(column).map(|(_container, canonical)| canonical)
}

fn as_utf8_literal(lit: &ScalarValue) -> Option<&str> {
    match lit {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
        ScalarValue::Utf8View(Some(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// The value actually written to `canonical`'s home, from a literal already
/// known (by construction) to compare against it directly — no cast, no
/// stringification. `None` when the literal's type doesn't match, which
/// callers treat as "no candidate", never a coercion.
fn home_value_from_literal<'a>(
    canonical: CanonicalType,
    lit: &'a ScalarValue,
) -> Option<HomeValue<'a>> {
    match (canonical, lit) {
        (CanonicalType::String, _) => as_utf8_literal(lit).map(HomeValue::Str),
        (CanonicalType::Int64, ScalarValue::Int64(Some(i))) => Some(HomeValue::Int(*i)),
        (CanonicalType::Float64, ScalarValue::Float64(Some(d))) if !d.is_nan() => {
            Some(HomeValue::Double(*d))
        }
        (CanonicalType::Bool, ScalarValue::Boolean(Some(b))) => Some(HomeValue::Bool(*b)),
        _ => None,
    }
}

fn single_home_token(home_col: &str, key: &str, lit: &ScalarValue) -> Option<Vec<u8>> {
    let canonical = canonical_from_suffix(home_col)?;
    encode_token(key, home_value_from_literal(canonical, lit)?)
}

/// IR typed mode's coalesce of same-canonical homes across containers
/// (`typed_attribute_expr` in `ir_planner`): every arg is a plain
/// `get_field(home, key)` (no cast), all sharing one canonical type and key
/// — which all encode to the very same token, so one candidate suffices.
fn recognize_same_typed_homes(args: &[Expr], lit: &ScalarValue) -> Option<ProbeClause> {
    let mut canonical = None;
    let mut shared_key = None;
    for arg in args {
        let (home, key) = as_get_field(arg)?;
        let this_canonical = canonical_from_suffix(home)?;
        match canonical.get_or_insert(this_canonical) {
            c if *c == this_canonical => {}
            _ => return None,
        }
        match shared_key.get_or_insert(key) {
            k if *k == key => {}
            _ => return None,
        }
    }
    let token = encode_token(shared_key?, home_value_from_literal(canonical?, lit)?)?;
    Some(vec![token])
}

/// `common::attrs::expr::typed_compat_attr_expr`'s shape: `coalesce(str
/// home, CAST(int home AS Utf8), CAST(double home AS Utf8), CAST(bool home
/// AS Utf8))`, compared to a string literal. Yields up to four candidates —
/// the string reading always, plus each numeric/bool home whose value would
/// have stringified to exactly this literal.
fn recognize_compat_coalesce(args: &[Expr], lit: &ScalarValue) -> Option<ProbeClause> {
    let [str_arg, int_arg, double_arg, bool_arg] = args else {
        return None;
    };
    let literal = as_utf8_literal(lit)?;
    let (str_home, key) = as_get_field(str_arg)?;
    if canonical_from_suffix(str_home)? != CanonicalType::String {
        return None;
    }
    for (arg, expected) in [
        (int_arg, CanonicalType::Int64),
        (double_arg, CanonicalType::Float64),
        (bool_arg, CanonicalType::Bool),
    ] {
        let Expr::Cast(cast_expr) = arg else {
            return None;
        };
        if cast_expr.field.data_type() != &DataType::Utf8 {
            return None;
        }
        let (home, cast_key) = as_get_field(&cast_expr.expr)?;
        if cast_key != key || canonical_from_suffix(home)? != expected {
            return None;
        }
    }

    let mut tokens = vec![encode_token(key, HomeValue::Str(literal))?];
    if let Ok(i) = literal.parse::<i64>()
        && utf8_cast_round_trips(&Int64Array::from(vec![i]), literal)
        && let Some(token) = encode_token(key, HomeValue::Int(i))
    {
        tokens.push(token);
    }
    if let Ok(d) = literal.parse::<f64>()
        && !d.is_nan()
        && utf8_cast_round_trips(&Float64Array::from(vec![d]), literal)
        && let Some(token) = encode_token(key, HomeValue::Double(d))
    {
        tokens.push(token);
    }
    match literal {
        "true" => tokens.extend(encode_token(key, HomeValue::Bool(true))),
        "false" => tokens.extend(encode_token(key, HomeValue::Bool(false))),
        _ => {}
    }
    Some(tokens)
}

/// Whether Arrow's own `CAST(.. AS Utf8)` of `array`'s single value renders
/// exactly `literal` — the same rendering the legacy writer produced on the
/// wire, so a match here means the compat literal really could have come
/// from this numeric/bool home.
fn utf8_cast_round_trips(array: &dyn Array, literal: &str) -> bool {
    let Ok(casted) = cast(array, &DataType::Utf8) else {
        return false;
    };
    let Some(strings) = casted.as_any().downcast_ref::<StringArray>() else {
        return false;
    };
    !strings.is_null(0) && strings.value(0) == literal
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::schema::typed_attributes::home_column;
    use datafusion::functions::core::expr_fn::get_field;
    use datafusion::logical_expr::expr_fn::in_list;
    use datafusion::logical_expr::{cast, col, lit};
    use datafusion::prelude::ident;

    fn direct_home_eq(container: &str, canonical: CanonicalType, key: &str, value: Expr) -> Expr {
        get_field(ident(home_column(container, canonical)), key).eq(value)
    }

    fn int_home_field(container: &str, key: &str) -> Expr {
        get_field(ident(home_column(container, CanonicalType::Int64)), key)
    }

    // --- recognized shapes ---

    #[test]
    fn single_home_equality_yields_one_token() {
        let expr = direct_home_eq(
            "span_attributes",
            CanonicalType::Int64,
            "status",
            lit(200i64),
        );
        let clauses = probe_clauses(&[expr]);
        assert_eq!(clauses.len(), 1);
        assert_eq!(
            clauses[0],
            vec![encode_token("status", HomeValue::Int(200)).unwrap()]
        );
    }

    #[test]
    fn coalesce_of_same_typed_homes_across_containers_collapses_to_one_token() {
        let homes = vec![
            home_column("span_attributes", CanonicalType::Int64),
            home_column("resource_attributes", CanonicalType::Int64),
        ];
        let expr = common::attrs::expr::typed_home_expr(&homes, &[None, None], "status", "")
            .eq(lit(200i64));
        let clauses = probe_clauses(&[expr]);
        assert_eq!(clauses.len(), 1);
        assert_eq!(
            clauses[0],
            vec![encode_token("status", HomeValue::Int(200)).unwrap()]
        );
    }

    #[test]
    fn typed_home_expr_with_a_promoted_label_is_rejected() {
        // `typed_home_expr` puts the promoted `label_<key>` ident first when
        // present; that ident isn't a `get_field`, so the coalesce doesn't
        // match `recognize_same_typed_homes` and the predicate is dropped
        // rather than guessed at.
        let homes = vec![home_column("span_attributes", CanonicalType::String)];
        let promoted = vec![Some("label_host".to_string())];
        let expr = common::attrs::expr::typed_home_expr(&homes, &promoted, "host", "").eq(lit("a"));
        assert!(probe_clauses(&[expr]).is_empty());
    }

    #[test]
    fn compat_coalesce_of_int_string_matches_its_own_generator() {
        let expr =
            common::attrs::expr::typed_compat_attr_expr("span_attributes", "status").eq(lit("200"));
        let clauses = probe_clauses(&[expr]);
        assert_eq!(clauses.len(), 1);
        let candidates = &clauses[0];
        assert!(candidates.contains(&encode_token("status", HomeValue::Str("200")).unwrap()));
        assert!(candidates.contains(&encode_token("status", HomeValue::Int(200)).unwrap()));
        assert_eq!(
            candidates.len(),
            2,
            "1.5/true never round-trip from \"200\""
        );
    }

    #[test]
    fn compat_coalesce_recognizes_a_double_literal() {
        let expr =
            common::attrs::expr::typed_compat_attr_expr("span_attributes", "ratio").eq(lit("1.5"));
        let clauses = probe_clauses(&[expr]);
        let candidates = &clauses[0];
        assert!(candidates.contains(&encode_token("ratio", HomeValue::Str("1.5")).unwrap()));
        assert!(candidates.contains(&encode_token("ratio", HomeValue::Double(1.5)).unwrap()));
    }

    #[test]
    fn compat_coalesce_recognizes_a_bool_literal() {
        let expr =
            common::attrs::expr::typed_compat_attr_expr("span_attributes", "ok").eq(lit("true"));
        let clauses = probe_clauses(&[expr]);
        let candidates = &clauses[0];
        assert!(candidates.contains(&encode_token("ok", HomeValue::Str("true")).unwrap()));
        assert!(candidates.contains(&encode_token("ok", HomeValue::Bool(true)).unwrap()));
    }

    #[test]
    fn compat_coalesce_rejects_a_non_numeric_non_bool_literal_down_to_its_string_token() {
        let expr =
            common::attrs::expr::typed_compat_attr_expr("span_attributes", "host").eq(lit("abc"));
        let clauses = probe_clauses(&[expr]);
        assert_eq!(
            clauses[0],
            vec![encode_token("host", HomeValue::Str("abc")).unwrap()]
        );
    }

    #[test]
    fn in_list_unions_every_values_tokens() {
        let expr = in_list(
            int_home_field("span_attributes", "status"),
            vec![lit(200i64), lit(404i64)],
            false,
        );
        let clauses = probe_clauses(&[expr]);
        assert_eq!(clauses.len(), 1);
        assert!(clauses[0].contains(&encode_token("status", HomeValue::Int(200)).unwrap()));
        assert!(clauses[0].contains(&encode_token("status", HomeValue::Int(404)).unwrap()));
    }

    #[test]
    fn or_of_recognized_leaves_unions_their_candidates() {
        let a = direct_home_eq(
            "span_attributes",
            CanonicalType::Int64,
            "status",
            lit(200i64),
        );
        let b = direct_home_eq(
            "span_attributes",
            CanonicalType::Int64,
            "status",
            lit(404i64),
        );
        let clauses = probe_clauses(&[a.or(b)]);
        assert_eq!(clauses.len(), 1);
        assert!(clauses[0].contains(&encode_token("status", HomeValue::Int(200)).unwrap()));
        assert!(clauses[0].contains(&encode_token("status", HomeValue::Int(404)).unwrap()));
    }

    // --- rejections: every case must yield no clause at all ---

    #[test]
    fn unrecognized_shapes_are_all_rejected() {
        let regex = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(get_field(
                ident(home_column("span_attributes", CanonicalType::String)),
                "host",
            )),
            op: Operator::RegexMatch,
            right: Box::new(lit("^a.*")),
        });
        let cases: [(&str, Expr); 6] = [
            (
                "not equal",
                int_home_field("span_attributes", "status").not_eq(lit(200i64)),
            ),
            (
                "a range",
                int_home_field("span_attributes", "status").gt(lit(200i64)),
            ),
            ("regex match", regex),
            ("a label column", col("label_http_method").eq(lit("GET"))),
            (
                "a negated IN list",
                in_list(
                    int_home_field("span_attributes", "status"),
                    vec![lit(200i64)],
                    true,
                ),
            ),
            (
                "an unrelated cast shape",
                cast(
                    int_home_field("span_attributes", "status"),
                    DataType::Float64,
                )
                .eq(lit(200.0f64)),
            ),
        ];
        for (name, expr) in cases {
            assert!(probe_clauses(&[expr]).is_empty(), "{name} must be rejected");
        }
    }
}
