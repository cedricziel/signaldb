//! Shared evaluation engine: reads/writes a [`Frame`] according to compiled
//! expressions, conditions, and editors. One engine serves traces, logs, and
//! metrics; only the `Frame` implementation differs per signal.

use sha2::{Digest, Sha256};

use crate::compile::{
    AssignTarget, CCondition, CExpr, CompiledStatement, Converter, Editor, KeyOrValue, MapTarget,
};
use crate::value::Value;
use opentelemetry_proto::tonic::common::v1::KeyValue;

/// A live view over the resource/scope/leaf item a statement runs against.
///
/// Implementations map [`crate::compile::ScalarTarget`]/[`MapTarget`] onto the
/// concrete `opentelemetry_proto` fields available for one signal.
pub trait Frame {
    fn get_scalar(&self, target: crate::compile::ScalarTarget) -> Value;
    fn set_scalar(
        &mut self,
        target: crate::compile::ScalarTarget,
        value: Value,
    ) -> Result<(), String>;
    fn map_mut(&mut self, target: MapTarget) -> &mut Vec<KeyValue>;
}

/// Runs one compiled statement against `frame`. Returns whether the `where`
/// guard (if any) matched and the editor ran.
pub fn run_statement(frame: &mut dyn Frame, stmt: &CompiledStatement) -> Result<bool, String> {
    if let Some(cond) = &stmt.condition
        && !eval_condition(frame, cond)?
    {
        return Ok(false);
    }
    apply_editor(frame, &stmt.editor)?;
    Ok(true)
}

fn get_attr(map: &[KeyValue], key: &str) -> Value {
    map.iter()
        .find(|kv| kv.key == key)
        .and_then(|kv| kv.value.as_ref())
        .map(Value::from_any_value)
        .unwrap_or(Value::Nil)
}

fn set_attr(map: &mut Vec<KeyValue>, key: &str, value: Value) {
    if value.is_nil() {
        map.retain(|kv| kv.key != key);
        return;
    }
    if let Some(existing) = map.iter_mut().find(|kv| kv.key == key) {
        existing.value = Some(value.into_any_value());
    } else {
        map.push(KeyValue {
            key: key.to_string(),
            value: Some(value.into_any_value()),
            ..Default::default()
        });
    }
}

pub fn eval_expr(frame: &mut dyn Frame, expr: &CExpr) -> Result<Value, String> {
    match expr {
        CExpr::Literal(v) => Ok(v.clone()),
        CExpr::List(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(eval_expr(frame, item)?);
            }
            Ok(Value::List(out))
        }
        CExpr::Target(target) => Ok(read_target(frame, target)),
        CExpr::Converter(conv) => eval_converter(frame, conv),
    }
}

fn eval_converter(frame: &mut dyn Frame, conv: &Converter) -> Result<Value, String> {
    match conv {
        Converter::IsMatch(target, regex) => {
            let value = eval_expr(frame, target)?;
            let text = value
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| value.to_display_string());
            Ok(Value::Bool(regex.is_match(&text)))
        }
        Converter::IsString(v) => Ok(Value::Bool(matches!(
            eval_expr(frame, v)?,
            Value::String(_)
        ))),
        Converter::Concat(items, delimiter) => {
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                parts.push(eval_expr(frame, item)?.to_display_string());
            }
            Ok(Value::String(parts.join(delimiter)))
        }
        Converter::String(v) => Ok(Value::String(eval_expr(frame, v)?.to_display_string())),
        Converter::Int(v) => {
            let value = eval_expr(frame, v)?;
            match &value {
                Value::Int(i) => Ok(Value::Int(*i)),
                Value::Double(d) => Ok(Value::Int(*d as i64)),
                Value::Bool(b) => Ok(Value::Int(i64::from(*b))),
                Value::String(s) => s
                    .trim()
                    .parse::<i64>()
                    .map(Value::Int)
                    .map_err(|_| "Int(): string value is not an integer".to_string()),
                other => Err(format!(
                    "Int(): cannot convert a {} to int",
                    other.type_name()
                )),
            }
        }
        Converter::Double(v) => {
            let value = eval_expr(frame, v)?;
            match &value {
                Value::Double(d) => Ok(Value::Double(*d)),
                Value::Int(i) => Ok(Value::Double(*i as f64)),
                Value::String(s) => s
                    .trim()
                    .parse::<f64>()
                    .map(Value::Double)
                    .map_err(|_| "Double(): string value is not a number".to_string()),
                other => Err(format!(
                    "Double(): cannot convert a {} to double",
                    other.type_name()
                )),
            }
        }
        Converter::Len(v) => match eval_expr(frame, v)? {
            Value::String(s) => Ok(Value::Int(s.chars().count() as i64)),
            Value::List(items) => Ok(Value::Int(items.len() as i64)),
            Value::Bytes(b) => Ok(Value::Int(b.len() as i64)),
            other => Err(format!(
                "Len(): unsupported value of type {}",
                other.type_name()
            )),
        },
        Converter::Sha256(v) => {
            let text = eval_expr(frame, v)?.to_display_string();
            let mut hasher = Sha256::new();
            hasher.update(text.as_bytes());
            let digest = hasher.finalize();
            Ok(Value::String(
                digest.iter().map(|b| format!("{b:02x}")).collect(),
            ))
        }
        Converter::Substring(v, start, len) => {
            let text = eval_expr(frame, v)?.to_display_string();
            Ok(Value::String(substring(&text, *start, *len)))
        }
        Converter::ToLowerCase(v) => Ok(Value::String(
            eval_expr(frame, v)?.to_display_string().to_lowercase(),
        )),
        Converter::ToUpperCase(v) => Ok(Value::String(
            eval_expr(frame, v)?.to_display_string().to_uppercase(),
        )),
        Converter::Truncate(v, len) => {
            let text = eval_expr(frame, v)?.to_display_string();
            Ok(Value::String(text.chars().take(*len).collect()))
        }
    }
}

fn substring(text: &str, start: i64, len: i64) -> String {
    if start < 0 || len < 0 {
        return String::new();
    }
    text.chars()
        .skip(start as usize)
        .take(len as usize)
        .collect()
}

fn write_target(frame: &mut dyn Frame, target: &AssignTarget, value: Value) -> Result<(), String> {
    match target {
        AssignTarget::Scalar(s) => frame.set_scalar(*s, value),
        AssignTarget::AttrValue(map, key) => {
            set_attr(frame.map_mut(*map), key, value);
            Ok(())
        }
    }
}

/// Expands `$1`/`${1}` capture-group references and `$$` (literal `$`) in a
/// replacement template. Mirrors `regex`/Go `regexp` replacement syntax so
/// Collector configs (including `$$1`) port verbatim.
pub fn expand_replacement(template: &str, caps: &regex::Captures) -> String {
    let mut out = String::new();
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        match chars.peek().copied() {
            Some('$') => {
                chars.next();
                out.push('$');
            }
            Some('{') => {
                chars.next();
                let mut num = String::new();
                for c2 in chars.by_ref() {
                    if c2 == '}' {
                        break;
                    }
                    num.push(c2);
                }
                if let Ok(idx) = num.parse::<usize>()
                    && let Some(m) = caps.get(idx)
                {
                    out.push_str(m.as_str());
                }
            }
            Some(c2) if c2.is_ascii_digit() => {
                let mut num = String::new();
                while let Some(&c2) = chars.peek() {
                    if c2.is_ascii_digit() {
                        num.push(c2);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if let Ok(idx) = num.parse::<usize>()
                    && let Some(m) = caps.get(idx)
                {
                    out.push_str(m.as_str());
                }
            }
            _ => out.push('$'),
        }
    }
    out
}

fn replace_pattern_str(text: &str, regex: &regex::Regex, replacement: &str) -> String {
    regex
        .replace_all(text, |caps: &regex::Captures| {
            expand_replacement(replacement, caps)
        })
        .into_owned()
}

fn replace_glob_str(text: &str, glob: &crate::compile::Glob, replacement: &str) -> String {
    if glob.0.is_match(text) {
        replacement.to_string()
    } else {
        text.to_string()
    }
}

fn apply_editor(frame: &mut dyn Frame, editor: &Editor) -> Result<(), String> {
    match editor {
        Editor::Set(target, value_expr) => {
            let value = eval_expr(frame, value_expr)?;
            write_target(frame, target, value)
        }
        Editor::DeleteKey(map, key) => {
            frame.map_mut(*map).retain(|kv| &kv.key != key);
            Ok(())
        }
        Editor::DeleteMatchingKeys(map, regex) => {
            frame.map_mut(*map).retain(|kv| !regex.is_match(&kv.key));
            Ok(())
        }
        Editor::KeepKeys(map, keys) => {
            frame.map_mut(*map).retain(|kv| keys.contains(&kv.key));
            Ok(())
        }
        Editor::TruncateAll(map, limit) => {
            for kv in frame.map_mut(*map).iter_mut() {
                if let Some(any) = &mut kv.value
                    && let Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) = &mut any.value
                    && s.chars().count() > *limit
                {
                    *s = s.chars().take(*limit).collect();
                }
            }
            Ok(())
        }
        Editor::Limit(map, limit, priority_keys) => {
            let entries = frame.map_mut(*map);
            if entries.len() <= *limit {
                return Ok(());
            }
            let mut kept: Vec<KeyValue> = Vec::with_capacity(*limit);
            for key in priority_keys {
                if kept.len() >= *limit {
                    break;
                }
                if let Some(pos) = entries.iter().position(|kv| &kv.key == key) {
                    kept.push(entries[pos].clone());
                }
            }
            for kv in entries.iter() {
                if kept.len() >= *limit {
                    break;
                }
                if !kept.iter().any(|k| k.key == kv.key) {
                    kept.push(kv.clone());
                }
            }
            *entries = kept;
            Ok(())
        }
        Editor::ReplacePattern(target, regex, replacement) => {
            let value = read_target(frame, target);
            match value.as_str() {
                Some(text) => {
                    let replaced = replace_pattern_str(text, regex, replacement);
                    write_target(frame, target, Value::String(replaced))
                }
                None => Ok(()),
            }
        }
        Editor::ReplaceAllPatterns(map, which, regex, replacement) => {
            for kv in frame.map_mut(*map).iter_mut() {
                match which {
                    KeyOrValue::Key => {
                        kv.key = replace_pattern_str(&kv.key, regex, replacement);
                    }
                    KeyOrValue::Value => {
                        if let Some(any) = &mut kv.value
                            && let Some(opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s)) = &mut any.value
                        {
                            *s = replace_pattern_str(s, regex, replacement);
                        }
                    }
                }
            }
            Ok(())
        }
        Editor::ReplaceMatch(target, glob, replacement) => {
            let value = read_target(frame, target);
            match value.as_str() {
                Some(text) => {
                    let replaced = replace_glob_str(text, glob, replacement);
                    write_target(frame, target, Value::String(replaced))
                }
                None => Ok(()),
            }
        }
        Editor::ReplaceAllMatches(map, glob, replacement) => {
            for kv in frame.map_mut(*map).iter_mut() {
                if let Some(any) = &mut kv.value
                    && let Some(
                        opentelemetry_proto::tonic::common::v1::any_value::Value::StringValue(s),
                    ) = &mut any.value
                {
                    *s = replace_glob_str(s, glob, replacement);
                }
            }
            Ok(())
        }
    }
}

fn read_target(frame: &mut dyn Frame, target: &AssignTarget) -> Value {
    match target {
        AssignTarget::Scalar(s) => frame.get_scalar(*s),
        AssignTarget::AttrValue(map, key) => get_attr(frame.map_mut(*map), key),
    }
}

fn eval_condition(frame: &mut dyn Frame, cond: &CCondition) -> Result<bool, String> {
    Ok(match cond {
        CCondition::Or(a, b) => eval_condition(frame, a)? || eval_condition(frame, b)?,
        CCondition::And(a, b) => eval_condition(frame, a)? && eval_condition(frame, b)?,
        CCondition::Not(a) => !eval_condition(frame, a)?,
        CCondition::Bare(e) => matches!(eval_expr(frame, e)?, Value::Bool(true)),
        CCondition::Compare(a, op, b) => {
            let lhs = eval_expr(frame, a)?;
            let rhs = eval_expr(frame, b)?;
            compare(&lhs, *op, &rhs)
        }
    })
}

fn compare(lhs: &Value, op: crate::ast::CmpOp, rhs: &Value) -> bool {
    use crate::ast::CmpOp;
    // A missing path (Nil) compared with anything but Nil is false, except
    // `!=` which is exactly the presence test.
    if lhs.is_nil() || rhs.is_nil() {
        return match op {
            CmpOp::Eq => lhs.is_nil() && rhs.is_nil(),
            CmpOp::Ne => !(lhs.is_nil() && rhs.is_nil()),
            _ => false,
        };
    }
    match op {
        CmpOp::Eq => values_eq(lhs, rhs),
        CmpOp::Ne => !values_eq(lhs, rhs),
        CmpOp::Lt => numeric_cmp(lhs, rhs).is_some_and(|o| o.is_lt()),
        CmpOp::Le => numeric_cmp(lhs, rhs).is_some_and(|o| o.is_le()),
        CmpOp::Gt => numeric_cmp(lhs, rhs).is_some_and(|o| o.is_gt()),
        CmpOp::Ge => numeric_cmp(lhs, rhs).is_some_and(|o| o.is_ge()),
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Int(i) => Some(*i as f64),
        Value::Double(d) => Some(*d),
        _ => None,
    }
}

fn numeric_cmp(lhs: &Value, rhs: &Value) -> Option<std::cmp::Ordering> {
    if let (Value::Int(a), Value::Int(b)) = (lhs, rhs) {
        return Some(a.cmp(b));
    }
    as_f64(lhs)
        .zip(as_f64(rhs))
        .and_then(|(a, b)| a.partial_cmp(&b))
}

fn values_eq(lhs: &Value, rhs: &Value) -> bool {
    match (lhs, rhs) {
        (Value::Int(a), Value::Int(b)) => a == b,
        (Value::Int(_) | Value::Double(_), Value::Int(_) | Value::Double(_)) => {
            as_f64(lhs).zip(as_f64(rhs)).is_some_and(|(a, b)| a == b)
        }
        _ => lhs == rhs,
    }
}
