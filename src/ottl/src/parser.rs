//! `pest`-backed parser: source text -> [`crate::ast::Statement`].

use pest::Parser;
use pest::iterators::Pair;
use pest_derive::Parser;

use crate::ast::{Call, CmpOp, Condition, Expr, Literal, Path, PathSegment, Statement};
use crate::error::ParseError;

#[derive(Parser)]
#[grammar = "ottl.pest"]
struct OttlParser;

/// Parses a single OTTL statement of the form `editor(args...) [where condition]`.
pub fn parse(src: &str) -> Result<Statement, ParseError> {
    let mut pairs =
        OttlParser::parse(Rule::statement, src).map_err(|err| ParseError::from_pest(src, err))?;
    let statement_pair = pairs.next().ok_or_else(|| ParseError {
        message: "empty statement".to_string(),
        column: 1,
        token: String::new(),
    })?;

    let mut inner = statement_pair.into_inner();
    let call_pair = inner.next().ok_or_else(|| ParseError {
        message: "missing editor call".to_string(),
        column: 1,
        token: String::new(),
    })?;
    let call = build_call(call_pair)?;

    let mut condition = None;
    for pair in inner {
        if pair.as_rule() == Rule::condition {
            condition = Some(build_condition(pair)?);
        }
    }

    Ok(Statement { call, condition })
}

fn build_call(pair: Pair<Rule>) -> Result<Call, ParseError> {
    let mut inner = pair.into_inner();
    let name = inner
        .next()
        .ok_or_else(|| ParseError {
            message: "missing call name".to_string(),
            column: 1,
            token: String::new(),
        })?
        .as_str()
        .to_string();
    let mut args = Vec::new();
    for expr_pair in inner {
        args.push(build_expr(expr_pair)?);
    }
    Ok(Call { name, args })
}

fn build_expr(pair: Pair<Rule>) -> Result<Expr, ParseError> {
    // `expr` wraps exactly one of call | list_literal | literal | path.
    let inner = pair.into_inner().next().ok_or_else(|| ParseError {
        message: "empty expression".to_string(),
        column: 1,
        token: String::new(),
    })?;
    match inner.as_rule() {
        Rule::call => Ok(Expr::Call(build_call(inner)?)),
        Rule::list_literal => {
            let mut items = Vec::new();
            for expr_pair in inner.into_inner() {
                items.push(build_expr(expr_pair)?);
            }
            Ok(Expr::List(items))
        }
        Rule::literal => Ok(Expr::Literal(build_literal(inner)?)),
        Rule::path => Ok(Expr::Path(build_path(inner)?)),
        rule => Err(ParseError {
            message: format!("unexpected expression node {rule:?}"),
            column: 1,
            token: inner.as_str().to_string(),
        }),
    }
}

fn build_path(pair: Pair<Rule>) -> Result<Path, ParseError> {
    let mut inner = pair.into_inner();
    let root = inner
        .next()
        .ok_or_else(|| ParseError {
            message: "missing path root".to_string(),
            column: 1,
            token: String::new(),
        })?
        .as_str()
        .to_string();
    let mut segments = vec![PathSegment::Field(root)];
    for seg_pair in inner {
        let inner_seg = seg_pair.into_inner().next().ok_or_else(|| ParseError {
            message: "empty path segment".to_string(),
            column: 1,
            token: String::new(),
        })?;
        match inner_seg.as_rule() {
            Rule::ident => segments.push(PathSegment::Field(inner_seg.as_str().to_string())),
            Rule::string => segments.push(PathSegment::Index(unescape_string(inner_seg.as_str()))),
            rule => {
                return Err(ParseError {
                    message: format!("unexpected path segment {rule:?}"),
                    column: 1,
                    token: inner_seg.as_str().to_string(),
                });
            }
        }
    }
    Ok(Path { segments })
}

fn build_literal(pair: Pair<Rule>) -> Result<Literal, ParseError> {
    let inner = pair.into_inner().next().ok_or_else(|| ParseError {
        message: "empty literal".to_string(),
        column: 1,
        token: String::new(),
    })?;
    match inner.as_rule() {
        Rule::int => {
            let value: i64 = inner.as_str().parse().map_err(|_| ParseError {
                message: format!("invalid integer literal `{}`", inner.as_str()),
                column: 1,
                token: inner.as_str().to_string(),
            })?;
            Ok(Literal::Int(value))
        }
        Rule::float => {
            let value: f64 = inner.as_str().parse().map_err(|_| ParseError {
                message: format!("invalid float literal `{}`", inner.as_str()),
                column: 1,
                token: inner.as_str().to_string(),
            })?;
            Ok(Literal::Float(value))
        }
        Rule::bool_lit => Ok(Literal::Bool(inner.as_str() == "true")),
        Rule::nil_lit => Ok(Literal::Nil),
        Rule::string => Ok(Literal::String(unescape_string(inner.as_str()))),
        rule => Err(ParseError {
            message: format!("unexpected literal node {rule:?}"),
            column: 1,
            token: inner.as_str().to_string(),
        }),
    }
}

/// Strips the surrounding quotes and resolves `\"`, `\\`, `\n`, `\t`, `\r` escapes.
fn unescape_string(raw: &str) -> String {
    let inner = raw
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(raw);
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                // Supported escapes per spec: `\\`, `\"`, `\n`, `\t`. Anything else is
                // passed through literally (the backslash is dropped).
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('"') => out.push('"'),
                Some('\\') => out.push('\\'),
                Some(other) => out.push(other),
                None => {}
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn build_condition(pair: Pair<Rule>) -> Result<Condition, ParseError> {
    // condition = { or_expr }
    let or_expr = pair.into_inner().next().ok_or_else(|| ParseError {
        message: "empty condition".to_string(),
        column: 1,
        token: String::new(),
    })?;
    build_or(or_expr)
}

fn build_or(pair: Pair<Rule>) -> Result<Condition, ParseError> {
    // or_expr = { and_expr ~ (kw_or ~ and_expr)* }; kw_or is atomic (still
    // produces a pair) so it must be filtered out here, not treated as an
    // operand.
    let mut and_exprs = pair.into_inner().filter(|p| p.as_rule() == Rule::and_expr);
    let mut acc = build_and(and_exprs.next().ok_or_else(|| ParseError {
        message: "empty or-expression".to_string(),
        column: 1,
        token: String::new(),
    })?)?;
    for next in and_exprs {
        let rhs = build_and(next)?;
        acc = Condition::Or(Box::new(acc), Box::new(rhs));
    }
    Ok(acc)
}

fn build_and(pair: Pair<Rule>) -> Result<Condition, ParseError> {
    // and_expr = { not_expr ~ (kw_and ~ not_expr)* }; same filtering as build_or.
    let mut not_exprs = pair.into_inner().filter(|p| p.as_rule() == Rule::not_expr);
    let mut acc = build_not(not_exprs.next().ok_or_else(|| ParseError {
        message: "empty and-expression".to_string(),
        column: 1,
        token: String::new(),
    })?)?;
    for next in not_exprs {
        let rhs = build_not(next)?;
        acc = Condition::And(Box::new(acc), Box::new(rhs));
    }
    Ok(acc)
}

fn build_not(pair: Pair<Rule>) -> Result<Condition, ParseError> {
    // not_expr = { negated | cond_atom }; negated = { kw_not ~ cond_atom }
    let inner = pair.into_inner().next().ok_or_else(|| ParseError {
        message: "empty not-expression".to_string(),
        column: 1,
        token: String::new(),
    })?;
    match inner.as_rule() {
        Rule::negated => {
            // negated = { kw_not ~ cond_atom }; kw_not is atomic (still produces a
            // pair), so skip it rather than treating it as the operand.
            let atom = inner
                .into_inner()
                .find(|p| p.as_rule() == Rule::cond_atom)
                .ok_or_else(|| ParseError {
                    message: "`not` without operand".to_string(),
                    column: 1,
                    token: String::new(),
                })?;
            Ok(Condition::Not(Box::new(build_cond_atom(atom)?)))
        }
        Rule::cond_atom => build_cond_atom(inner),
        rule => Err(ParseError {
            message: format!("unexpected not-expression node {rule:?}"),
            column: 1,
            token: inner.as_str().to_string(),
        }),
    }
}

fn build_cond_atom(pair: Pair<Rule>) -> Result<Condition, ParseError> {
    // cond_atom = { "(" ~ condition ~ ")" | comparison }
    let inner = pair.into_inner().next().ok_or_else(|| ParseError {
        message: "empty condition atom".to_string(),
        column: 1,
        token: String::new(),
    })?;
    match inner.as_rule() {
        Rule::condition => build_condition(inner),
        Rule::comparison => build_comparison(inner),
        rule => Err(ParseError {
            message: format!("unexpected condition node {rule:?}"),
            column: 1,
            token: inner.as_str().to_string(),
        }),
    }
}

fn build_comparison(pair: Pair<Rule>) -> Result<Condition, ParseError> {
    let mut inner = pair.into_inner();
    let lhs_pair = inner.next().ok_or_else(|| ParseError {
        message: "empty comparison".to_string(),
        column: 1,
        token: String::new(),
    })?;
    let lhs = build_expr(lhs_pair)?;
    match (inner.next(), inner.next()) {
        (Some(op_pair), Some(rhs_pair)) => {
            let op = match op_pair.as_str() {
                "==" => CmpOp::Eq,
                "!=" => CmpOp::Ne,
                "<=" => CmpOp::Le,
                ">=" => CmpOp::Ge,
                "<" => CmpOp::Lt,
                ">" => CmpOp::Gt,
                other => {
                    return Err(ParseError {
                        message: format!("unknown comparison operator `{other}`"),
                        column: 1,
                        token: other.to_string(),
                    });
                }
            };
            let rhs = build_expr(rhs_pair)?;
            Ok(Condition::Compare(lhs, op, rhs))
        }
        _ => Ok(Condition::Bare(lhs)),
    }
}
