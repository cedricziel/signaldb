//! Compiles parsed statements into a [`CompiledProgram`]: path legality per
//! signal, regex precompilation, and editor/converter resolution.

use regex::{Regex, RegexBuilder};

use crate::ast::{self, CmpOp, Condition, Expr, Literal, Path, PathSegment};
use crate::error::CompileError;
use crate::value::Value;

/// The telemetry signal a program is compiled for; selects which paths are legal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Traces,
    Logs,
    Metrics,
}

/// Compile-time limits, also enforced defensively at evaluation time.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    /// Maximum number of statements in one program.
    pub max_statements: usize,
    /// Maximum source length of a single regex pattern.
    pub max_regex_len: usize,
    /// `regex::RegexBuilder::size_limit` applied to every compiled regex.
    pub regex_size_limit: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_statements: 200,
            max_regex_len: 2048,
            regex_size_limit: 1 << 20,
        }
    }
}

/// A field or attribute reachable on the resource/scope/leaf items visible to `signal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScalarTarget {
    ScopeName,
    ScopeVersion,
    SpanName,
    SpanKind,
    StatusCode,
    StatusMessage,
    LogBody,
    LogSeverityText,
    LogSeverityNumber,
    MetricName,
    MetricDescription,
    MetricUnit,
}

/// An attribute map reachable on the resource/scope/leaf items visible to `signal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MapTarget {
    Resource,
    Scope,
    /// `span.attributes` / `log.attributes` / `datapoint.attributes`, depending on signal.
    Leaf,
}

/// Something an editor can read and/or overwrite.
#[derive(Debug, Clone, PartialEq)]
pub enum AssignTarget {
    Scalar(ScalarTarget),
    AttrValue(MapTarget, String),
}

/// A compiled expression: literals, list literals, target reads, and converters.
#[derive(Debug, Clone)]
pub enum CExpr {
    Literal(Value),
    List(Vec<CExpr>),
    Target(AssignTarget),
    Converter(Converter),
}

/// A compiled converter call.
#[derive(Debug, Clone)]
pub enum Converter {
    IsMatch(Box<CExpr>, Box<Regex>),
    IsString(Box<CExpr>),
    Concat(Vec<CExpr>, String),
    String(Box<CExpr>),
    Int(Box<CExpr>),
    Double(Box<CExpr>),
    Len(Box<CExpr>),
    Sha256(Box<CExpr>),
    Substring(Box<CExpr>, i64, i64),
    ToLowerCase(Box<CExpr>),
    ToUpperCase(Box<CExpr>),
    Truncate(Box<CExpr>, usize),
}

/// Which side of a `KeyValue` `replace_all_patterns` rewrites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyOrValue {
    Key,
    Value,
}

/// A simplified glob supporting only `*` and `?`, pre-translated to a `Regex`.
#[derive(Debug, Clone)]
pub struct Glob(pub Box<Regex>);

/// A compiled editor: the only statement-level operation.
#[derive(Debug, Clone)]
pub enum Editor {
    Set(AssignTarget, CExpr),
    DeleteKey(MapTarget, String),
    DeleteMatchingKeys(MapTarget, Box<Regex>),
    KeepKeys(MapTarget, Vec<String>),
    TruncateAll(MapTarget, usize),
    Limit(MapTarget, usize, Vec<String>),
    ReplacePattern(AssignTarget, Box<Regex>, String),
    ReplaceAllPatterns(MapTarget, KeyOrValue, Box<Regex>, String),
    ReplaceMatch(AssignTarget, Glob, String),
    ReplaceAllMatches(MapTarget, Glob, String),
}

/// A compiled comparison/boolean condition.
#[derive(Debug, Clone)]
pub enum CCondition {
    Or(Box<CCondition>, Box<CCondition>),
    And(Box<CCondition>, Box<CCondition>),
    Not(Box<CCondition>),
    Bare(CExpr),
    Compare(CExpr, CmpOp, CExpr),
}

/// One compiled statement: its editor and optional `where` guard.
#[derive(Debug, Clone)]
pub struct CompiledStatement {
    pub editor: Editor,
    pub condition: Option<CCondition>,
}

/// The result of [`crate::compile`]: a signal-bound, ready-to-run program.
#[derive(Debug, Clone)]
pub struct CompiledProgram {
    signal: Signal,
    pub(crate) statements: Vec<CompiledStatement>,
}

impl CompiledProgram {
    /// The signal this program was compiled for.
    pub fn signal(&self) -> Signal {
        self.signal
    }
}

/// Parses and compiles every statement for `signal`, collecting every error rather
/// than stopping at the first one.
pub fn compile(
    signal: Signal,
    statements: &[String],
    limits: &Limits,
) -> Result<CompiledProgram, Vec<CompileError>> {
    let mut errors = Vec::new();
    if statements.len() > limits.max_statements {
        errors.push(CompileError {
            statement: statements.len(),
            column: None,
            message: format!(
                "program has {} statements, exceeding the limit of {}",
                statements.len(),
                limits.max_statements
            ),
        });
        return Err(errors);
    }

    let mut compiled = Vec::with_capacity(statements.len());
    for (index, source) in statements.iter().enumerate() {
        match crate::parse(source) {
            Ok(ast) => match compile_statement(signal, &ast, limits) {
                Ok(stmt) => compiled.push(stmt),
                Err(mut errs) => {
                    for err in &mut errs {
                        err.statement = index;
                    }
                    errors.extend(errs);
                }
            },
            Err(parse_err) => errors.push(CompileError {
                statement: index,
                column: Some(parse_err.column),
                message: parse_err.message,
            }),
        }
    }

    if errors.is_empty() {
        Ok(CompiledProgram {
            signal,
            statements: compiled,
        })
    } else {
        Err(errors)
    }
}

fn err(message: impl Into<String>) -> CompileError {
    CompileError {
        statement: 0,
        column: None,
        message: message.into(),
    }
}

fn compile_statement(
    signal: Signal,
    stmt: &ast::Statement,
    limits: &Limits,
) -> Result<CompiledStatement, Vec<CompileError>> {
    let editor = compile_editor(signal, &stmt.call, limits).map_err(|e| vec![e])?;
    let condition = match &stmt.condition {
        Some(cond) => Some(compile_condition(signal, cond, limits).map_err(|e| vec![e])?),
        None => None,
    };
    Ok(CompiledStatement { editor, condition })
}

fn compile_regex(pattern: &str, limits: &Limits) -> Result<Regex, CompileError> {
    if pattern.len() > limits.max_regex_len {
        return Err(err(format!(
            "regex `{pattern}` exceeds the maximum length of {} bytes",
            limits.max_regex_len
        )));
    }
    RegexBuilder::new(pattern)
        .size_limit(limits.regex_size_limit)
        .build()
        .map_err(|e| err(format!("invalid regex `{pattern}`: {e}")))
}

/// Translates a `*`/`?` glob into an anchored regex.
fn compile_glob(pattern: &str, limits: &Limits) -> Result<Glob, CompileError> {
    let mut regex_src = String::with_capacity(pattern.len() * 2 + 2);
    regex_src.push('^');
    for c in pattern.chars() {
        match c {
            '*' => regex_src.push_str(".*"),
            '?' => regex_src.push('.'),
            other => regex_src.push_str(&regex::escape(&other.to_string())),
        }
    }
    regex_src.push('$');
    Ok(Glob(Box::new(compile_regex(&regex_src, limits)?)))
}

fn expect_string_literal(expr: &Expr, what: &str) -> Result<String, CompileError> {
    match expr {
        Expr::Literal(Literal::String(s)) => Ok(s.clone()),
        _ => Err(err(format!("{what} must be a string literal"))),
    }
}

fn expect_int_literal(expr: &Expr, what: &str) -> Result<i64, CompileError> {
    match expr {
        Expr::Literal(Literal::Int(i)) => Ok(*i),
        _ => Err(err(format!("{what} must be an integer literal"))),
    }
}

fn expect_list_of_strings(expr: &Expr, what: &str) -> Result<Vec<String>, CompileError> {
    match expr {
        Expr::List(items) => items
            .iter()
            .map(|item| expect_string_literal(item, what))
            .collect::<Result<Vec<_>, _>>(),
        _ => Err(err(format!("{what} must be a list of string literals"))),
    }
}

fn expect_path<'a>(expr: &'a Expr, what: &str) -> Result<&'a Path, CompileError> {
    match expr {
        Expr::Path(p) => Ok(p),
        _ => Err(err(format!("{what} must be a path"))),
    }
}

fn resolve_assign_target(signal: Signal, path: &Path) -> Result<AssignTarget, CompileError> {
    match resolve_path(signal, path)? {
        Resolved::Scalar(s) => Ok(AssignTarget::Scalar(s)),
        Resolved::AttrValue(m, k) => Ok(AssignTarget::AttrValue(m, k)),
        Resolved::Map(_) => Err(err(format!(
            "`{}` names a map, not a scalar or attribute value",
            render_path(path)
        ))),
    }
}

fn resolve_map_target(signal: Signal, path: &Path) -> Result<MapTarget, CompileError> {
    match resolve_path(signal, path)? {
        Resolved::Map(m) => Ok(m),
        _ => Err(err(format!(
            "`{}` does not name an attribute map",
            render_path(path)
        ))),
    }
}

fn render_path(path: &Path) -> String {
    let mut out = String::new();
    for (i, seg) in path.segments.iter().enumerate() {
        match seg {
            PathSegment::Field(name) => {
                if i > 0 {
                    out.push('.');
                }
                out.push_str(name);
            }
            PathSegment::Index(key) => {
                out.push_str(&format!("[\"{key}\"]"));
            }
        }
    }
    out
}

enum Resolved {
    Scalar(ScalarTarget),
    Map(MapTarget),
    AttrValue(MapTarget, String),
}

/// Resolves a source path to a target, validating it is legal for `signal` (D1/spec:
/// per-signal path legality).
fn resolve_path(signal: Signal, path: &Path) -> Result<Resolved, CompileError> {
    let root = path.root();
    let rest = &path.segments[1..];

    let map_or_attr = |map: MapTarget, rest: &[PathSegment]| -> Result<Resolved, CompileError> {
        match rest {
            [] => Ok(Resolved::Map(map)),
            [PathSegment::Index(key)] => Ok(Resolved::AttrValue(map, key.clone())),
            _ => Err(err(format!(
                "`{}` is not a valid attribute path",
                render_path(path)
            ))),
        }
    };

    match root {
        "resource" => match rest {
            [PathSegment::Field(f), tail @ ..] if f == "attributes" => {
                map_or_attr(MapTarget::Resource, tail)
            }
            _ => Err(err(format!("unknown path `{}`", render_path(path)))),
        },
        "instrumentation_scope" => match rest {
            [PathSegment::Field(f), tail @ ..] if f == "attributes" => {
                map_or_attr(MapTarget::Scope, tail)
            }
            [PathSegment::Field(f)] if f == "name" => Ok(Resolved::Scalar(ScalarTarget::ScopeName)),
            [PathSegment::Field(f)] if f == "version" => {
                Ok(Resolved::Scalar(ScalarTarget::ScopeVersion))
            }
            _ => Err(err(format!("unknown path `{}`", render_path(path)))),
        },
        "span" if signal == Signal::Traces => match rest {
            [PathSegment::Field(f)] if f == "name" => Ok(Resolved::Scalar(ScalarTarget::SpanName)),
            [PathSegment::Field(f)] if f == "kind" => Ok(Resolved::Scalar(ScalarTarget::SpanKind)),
            [PathSegment::Field(f), PathSegment::Field(g)] if f == "status" && g == "code" => {
                Ok(Resolved::Scalar(ScalarTarget::StatusCode))
            }
            [PathSegment::Field(f), PathSegment::Field(g)] if f == "status" && g == "message" => {
                Ok(Resolved::Scalar(ScalarTarget::StatusMessage))
            }
            [PathSegment::Field(f), tail @ ..] if f == "attributes" => {
                map_or_attr(MapTarget::Leaf, tail)
            }
            _ => Err(err(format!(
                "unknown path `{}` for traces",
                render_path(path)
            ))),
        },
        "log" if signal == Signal::Logs => match rest {
            [PathSegment::Field(f)] if f == "body" => Ok(Resolved::Scalar(ScalarTarget::LogBody)),
            [PathSegment::Field(f)] if f == "severity_text" => {
                Ok(Resolved::Scalar(ScalarTarget::LogSeverityText))
            }
            [PathSegment::Field(f)] if f == "severity_number" => {
                Ok(Resolved::Scalar(ScalarTarget::LogSeverityNumber))
            }
            [PathSegment::Field(f), tail @ ..] if f == "attributes" => {
                map_or_attr(MapTarget::Leaf, tail)
            }
            _ => Err(err(format!(
                "unknown path `{}` for logs",
                render_path(path)
            ))),
        },
        "metric" if signal == Signal::Metrics => match rest {
            [PathSegment::Field(f)] if f == "name" => {
                Ok(Resolved::Scalar(ScalarTarget::MetricName))
            }
            [PathSegment::Field(f)] if f == "description" => {
                Ok(Resolved::Scalar(ScalarTarget::MetricDescription))
            }
            [PathSegment::Field(f)] if f == "unit" => {
                Ok(Resolved::Scalar(ScalarTarget::MetricUnit))
            }
            _ => Err(err(format!(
                "unknown path `{}` for metrics",
                render_path(path)
            ))),
        },
        "datapoint" if signal == Signal::Metrics => match rest {
            [PathSegment::Field(f), tail @ ..] if f == "attributes" => {
                map_or_attr(MapTarget::Leaf, tail)
            }
            _ => Err(err(format!(
                "unknown path `{}` for metrics",
                render_path(path)
            ))),
        },
        // Bare roots: `attributes[...]`/`attributes`, `name`, `body`.
        "attributes" => map_or_attr(MapTarget::Leaf, rest),
        "name" if signal == Signal::Traces => match rest {
            [] => Ok(Resolved::Scalar(ScalarTarget::SpanName)),
            _ => Err(err(format!("unknown path `{}`", render_path(path)))),
        },
        "name" if signal == Signal::Metrics => match rest {
            [] => Ok(Resolved::Scalar(ScalarTarget::MetricName)),
            _ => Err(err(format!("unknown path `{}`", render_path(path)))),
        },
        "body" if signal == Signal::Logs => match rest {
            [] => Ok(Resolved::Scalar(ScalarTarget::LogBody)),
            _ => Err(err(format!("unknown path `{}`", render_path(path)))),
        },
        other => Err(err(format!("unknown path root `{other}` for this signal"))),
    }
}

fn compile_expr(signal: Signal, expr: &Expr, limits: &Limits) -> Result<CExpr, CompileError> {
    match expr {
        Expr::Literal(Literal::String(s)) => Ok(CExpr::Literal(Value::String(s.clone()))),
        Expr::Literal(Literal::Int(i)) => Ok(CExpr::Literal(Value::Int(*i))),
        Expr::Literal(Literal::Float(f)) => Ok(CExpr::Literal(Value::Double(*f))),
        Expr::Literal(Literal::Bool(b)) => Ok(CExpr::Literal(Value::Bool(*b))),
        Expr::Literal(Literal::Nil) => Ok(CExpr::Literal(Value::Nil)),
        Expr::List(items) => Ok(CExpr::List(
            items
                .iter()
                .map(|item| compile_expr(signal, item, limits))
                .collect::<Result<_, _>>()?,
        )),
        Expr::Path(path) => Ok(CExpr::Target(resolve_assign_target(signal, path)?)),
        Expr::Call(call) => Ok(CExpr::Converter(compile_converter(signal, call, limits)?)),
    }
}

fn arity_error(name: &str, expected: &str, got: usize) -> CompileError {
    err(format!(
        "`{name}` expects {expected} argument(s), got {got}"
    ))
}

fn compile_converter(
    signal: Signal,
    call: &ast::Call,
    limits: &Limits,
) -> Result<Converter, CompileError> {
    let args = &call.args;
    match call.name.as_str() {
        "IsMatch" => {
            if args.len() != 2 {
                return Err(arity_error("IsMatch", "2", args.len()));
            }
            let target = compile_expr(signal, &args[0], limits)?;
            let pattern = expect_string_literal(&args[1], "IsMatch regex")?;
            Ok(Converter::IsMatch(
                Box::new(target),
                Box::new(compile_regex(&pattern, limits)?),
            ))
        }
        "IsString" => Ok(Converter::IsString(Box::new(compile_one(
            signal, args, "IsString", limits,
        )?))),
        "Concat" => {
            if args.len() != 2 {
                return Err(arity_error("Concat", "2", args.len()));
            }
            let items = match &args[0] {
                Expr::List(items) => items
                    .iter()
                    .map(|item| compile_expr(signal, item, limits))
                    .collect::<Result<Vec<_>, _>>()?,
                _ => return Err(err("Concat's first argument must be a list")),
            };
            let delimiter = expect_string_literal(&args[1], "Concat delimiter")?;
            Ok(Converter::Concat(items, delimiter))
        }
        "String" => Ok(Converter::String(Box::new(compile_one(
            signal, args, "String", limits,
        )?))),
        "Int" => Ok(Converter::Int(Box::new(compile_one(
            signal, args, "Int", limits,
        )?))),
        "Double" => Ok(Converter::Double(Box::new(compile_one(
            signal, args, "Double", limits,
        )?))),
        "Len" => Ok(Converter::Len(Box::new(compile_one(
            signal, args, "Len", limits,
        )?))),
        "SHA256" => Ok(Converter::Sha256(Box::new(compile_one(
            signal, args, "SHA256", limits,
        )?))),
        "Substring" => {
            if args.len() != 3 {
                return Err(arity_error("Substring", "3", args.len()));
            }
            let value = compile_expr(signal, &args[0], limits)?;
            let start = expect_int_literal(&args[1], "Substring start")?;
            let len = expect_int_literal(&args[2], "Substring len")?;
            Ok(Converter::Substring(Box::new(value), start, len))
        }
        "ToLowerCase" => Ok(Converter::ToLowerCase(Box::new(compile_one(
            signal,
            args,
            "ToLowerCase",
            limits,
        )?))),
        "ToUpperCase" => Ok(Converter::ToUpperCase(Box::new(compile_one(
            signal,
            args,
            "ToUpperCase",
            limits,
        )?))),
        "Truncate" => {
            if args.len() != 2 {
                return Err(arity_error("Truncate", "2", args.len()));
            }
            let value = compile_expr(signal, &args[0], limits)?;
            let len = expect_int_literal(&args[1], "Truncate len")?;
            if len < 0 {
                return Err(err("Truncate len must be non-negative"));
            }
            Ok(Converter::Truncate(Box::new(value), len as usize))
        }
        other => Err(err(format!("unknown converter `{other}`"))),
    }
}

fn compile_one(
    signal: Signal,
    args: &[Expr],
    name: &str,
    limits: &Limits,
) -> Result<CExpr, CompileError> {
    if args.len() != 1 {
        return Err(arity_error(name, "1", args.len()));
    }
    compile_expr(signal, &args[0], limits)
}

fn compile_editor(
    signal: Signal,
    call: &ast::Call,
    limits: &Limits,
) -> Result<Editor, CompileError> {
    let args = &call.args;
    match call.name.as_str() {
        "set" => {
            if args.len() != 2 {
                return Err(arity_error("set", "2", args.len()));
            }
            let target = resolve_assign_target(signal, expect_path(&args[0], "set target")?)?;
            let value = compile_expr(signal, &args[1], limits)?;
            Ok(Editor::Set(target, value))
        }
        "delete_key" => {
            if args.len() != 2 {
                return Err(arity_error("delete_key", "2", args.len()));
            }
            let map = resolve_map_target(signal, expect_path(&args[0], "delete_key map")?)?;
            let key = expect_string_literal(&args[1], "delete_key key")?;
            Ok(Editor::DeleteKey(map, key))
        }
        "delete_matching_keys" => {
            if args.len() != 2 {
                return Err(arity_error("delete_matching_keys", "2", args.len()));
            }
            let map =
                resolve_map_target(signal, expect_path(&args[0], "delete_matching_keys map")?)?;
            let pattern = expect_string_literal(&args[1], "delete_matching_keys regex")?;
            Ok(Editor::DeleteMatchingKeys(
                map,
                Box::new(compile_regex(&pattern, limits)?),
            ))
        }
        "keep_keys" => {
            if args.len() != 2 {
                return Err(arity_error("keep_keys", "2", args.len()));
            }
            let map = resolve_map_target(signal, expect_path(&args[0], "keep_keys map")?)?;
            let keys = expect_list_of_strings(&args[1], "keep_keys keys")?;
            Ok(Editor::KeepKeys(map, keys))
        }
        "truncate_all" => {
            if args.len() != 2 {
                return Err(arity_error("truncate_all", "2", args.len()));
            }
            let map = resolve_map_target(signal, expect_path(&args[0], "truncate_all map")?)?;
            let limit = expect_int_literal(&args[1], "truncate_all limit")?;
            if limit < 0 {
                return Err(err("truncate_all limit must be non-negative"));
            }
            Ok(Editor::TruncateAll(map, limit as usize))
        }
        "limit" => {
            if args.len() != 3 {
                return Err(arity_error("limit", "3", args.len()));
            }
            let map = resolve_map_target(signal, expect_path(&args[0], "limit map")?)?;
            let limit = expect_int_literal(&args[1], "limit count")?;
            if limit < 0 {
                return Err(err("limit count must be non-negative"));
            }
            let priority = expect_list_of_strings(&args[2], "limit priority_keys")?;
            Ok(Editor::Limit(map, limit as usize, priority))
        }
        "replace_pattern" => {
            if args.len() != 3 {
                return Err(arity_error("replace_pattern", "3", args.len()));
            }
            let target =
                resolve_assign_target(signal, expect_path(&args[0], "replace_pattern target")?)?;
            let pattern = expect_string_literal(&args[1], "replace_pattern regex")?;
            let replacement = expect_string_literal(&args[2], "replace_pattern replacement")?;
            Ok(Editor::ReplacePattern(
                target,
                Box::new(compile_regex(&pattern, limits)?),
                replacement,
            ))
        }
        "replace_all_patterns" => {
            if args.len() != 4 {
                return Err(arity_error("replace_all_patterns", "4", args.len()));
            }
            let map =
                resolve_map_target(signal, expect_path(&args[0], "replace_all_patterns map")?)?;
            let which = expect_string_literal(&args[1], "replace_all_patterns key|value selector")?;
            let which = match which.as_str() {
                "key" => KeyOrValue::Key,
                "value" => KeyOrValue::Value,
                other => {
                    return Err(err(format!(
                        "replace_all_patterns selector must be \"key\" or \"value\", got `{other}`"
                    )));
                }
            };
            let pattern = expect_string_literal(&args[2], "replace_all_patterns regex")?;
            let replacement = expect_string_literal(&args[3], "replace_all_patterns replacement")?;
            Ok(Editor::ReplaceAllPatterns(
                map,
                which,
                Box::new(compile_regex(&pattern, limits)?),
                replacement,
            ))
        }
        "replace_match" => {
            if args.len() != 3 {
                return Err(arity_error("replace_match", "3", args.len()));
            }
            let target =
                resolve_assign_target(signal, expect_path(&args[0], "replace_match target")?)?;
            let glob = expect_string_literal(&args[1], "replace_match glob")?;
            let replacement = expect_string_literal(&args[2], "replace_match replacement")?;
            Ok(Editor::ReplaceMatch(
                target,
                compile_glob(&glob, limits)?,
                replacement,
            ))
        }
        "replace_all_matches" => {
            if args.len() != 3 {
                return Err(arity_error("replace_all_matches", "3", args.len()));
            }
            let map =
                resolve_map_target(signal, expect_path(&args[0], "replace_all_matches map")?)?;
            let glob = expect_string_literal(&args[1], "replace_all_matches glob")?;
            let replacement = expect_string_literal(&args[2], "replace_all_matches replacement")?;
            Ok(Editor::ReplaceAllMatches(
                map,
                compile_glob(&glob, limits)?,
                replacement,
            ))
        }
        other => Err(err(format!("unknown editor `{other}`"))),
    }
}

fn compile_condition(
    signal: Signal,
    cond: &Condition,
    limits: &Limits,
) -> Result<CCondition, CompileError> {
    Ok(match cond {
        Condition::Or(a, b) => CCondition::Or(
            Box::new(compile_condition(signal, a, limits)?),
            Box::new(compile_condition(signal, b, limits)?),
        ),
        Condition::And(a, b) => CCondition::And(
            Box::new(compile_condition(signal, a, limits)?),
            Box::new(compile_condition(signal, b, limits)?),
        ),
        Condition::Not(a) => CCondition::Not(Box::new(compile_condition(signal, a, limits)?)),
        Condition::Bare(e) => CCondition::Bare(compile_expr(signal, e, limits)?),
        Condition::Compare(a, op, b) => CCondition::Compare(
            compile_expr(signal, a, limits)?,
            *op,
            compile_expr(signal, b, limits)?,
        ),
    })
}
