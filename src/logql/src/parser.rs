//! # LogQL Parser
//!
//! Recursive-descent parser over the [`crate::lexer`] token stream.
//! Covers stream selectors and log pipelines (line filters, parser
//! stages, label filters, formatters, unwrap); metric queries follow in
//! a later phase of the epic.

use crate::ast::{
    FilterOp, FilterValue, LabelExtraction, LabelFilterExpr, LabelFilterPred, LabelFormat,
    LabelFormatValue, LabelMatcher, LineFilter, LineFilterOp, LogQuery, MatchOp, PipelineStage,
    StreamSelector, Unwrap,
};
use crate::lexer::{LexError, tokenize};
use crate::token::{SpannedToken, Token};

/// Parse error with 1-based source position.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{message} at line {line}, column {col}")]
pub struct ParseError {
    pub message: String,
    pub line: u32,
    pub col: u32,
}

impl From<LexError> for ParseError {
    fn from(e: LexError) -> Self {
        Self {
            message: e.message,
            line: e.line,
            col: e.col,
        }
    }
}

/// Parse a full log query: a stream selector followed by an optional
/// pipeline. Metric-query wrappers are not parsed here.
pub fn parse_query(input: &str) -> Result<LogQuery, ParseError> {
    let mut parser = Parser::new(input)?;
    let selector = parser.parse_selector()?;
    let pipeline = parser.parse_pipeline()?;
    parser.expect_eof()?;
    Ok(LogQuery { selector, pipeline })
}

/// Parse a bare stream selector, as sent by the metadata endpoints'
/// `match[]` parameter.
pub fn parse_selector(input: &str) -> Result<StreamSelector, ParseError> {
    let mut parser = Parser::new(input)?;
    let selector = parser.parse_selector()?;
    parser.expect_eof()?;
    Ok(selector)
}

pub(crate) struct Parser {
    tokens: Vec<SpannedToken>,
    pos: usize,
    /// Position just past the last token, for end-of-input errors.
    end: (u32, u32),
}

impl Parser {
    pub(crate) fn new(input: &str) -> Result<Self, ParseError> {
        let tokens = tokenize(input)?;
        let end = tokens
            .last()
            .map(|t| (t.line, t.col + t.token.to_string().len() as u32))
            .unwrap_or((1, 1));
        Ok(Self {
            tokens,
            pos: 0,
            end,
        })
    }

    fn peek(&self) -> Option<&SpannedToken> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<SpannedToken> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn error_at(&self, message: impl Into<String>, at: Option<&SpannedToken>) -> ParseError {
        let (line, col) = at.map(|t| (t.line, t.col)).unwrap_or(self.end);
        ParseError {
            message: message.into(),
            line,
            col,
        }
    }

    fn expect(&mut self, expected: &Token, what: &str) -> Result<SpannedToken, ParseError> {
        match self.next() {
            Some(t) if &t.token == expected => Ok(t),
            Some(t) => Err(ParseError {
                message: format!("expected {what}, found '{}'", t.token),
                line: t.line,
                col: t.col,
            }),
            None => Err(self.error_at(format!("expected {what}, found end of input"), None)),
        }
    }

    pub(crate) fn expect_eof(&mut self) -> Result<(), ParseError> {
        match self.peek() {
            None => Ok(()),
            Some(t) => Err(ParseError {
                message: format!("unexpected trailing token '{}'", t.token),
                line: t.line,
                col: t.col,
            }),
        }
    }

    /// Parse zero or more pipeline stages following a stream selector.
    ///
    /// `|=`, `!=`, `|~`, `!~` introduce line filters; `|` introduces a
    /// parser stage, formatter, `unwrap`, or label filter, resolved by
    /// the token that follows.
    pub(crate) fn parse_pipeline(&mut self) -> Result<Vec<PipelineStage>, ParseError> {
        let mut stages = Vec::new();
        loop {
            match self.peek().map(|t| &t.token) {
                Some(Token::PipeExact) => {
                    self.next();
                    stages.push(PipelineStage::LineFilter(
                        self.parse_line_filter_value(LineFilterOp::Contains)?,
                    ));
                }
                Some(Token::PipeRegex) => {
                    self.next();
                    stages.push(PipelineStage::LineFilter(
                        self.parse_line_filter_value(LineFilterOp::Regex)?,
                    ));
                }
                Some(Token::Neq) => {
                    self.next();
                    stages.push(PipelineStage::LineFilter(
                        self.parse_line_filter_value(LineFilterOp::NotContains)?,
                    ));
                }
                Some(Token::Nre) => {
                    self.next();
                    stages.push(PipelineStage::LineFilter(
                        self.parse_line_filter_value(LineFilterOp::NotRegex)?,
                    ));
                }
                Some(Token::Pipe) => {
                    self.next();
                    stages.push(self.parse_pipe_stage()?);
                }
                _ => break,
            }
        }
        Ok(stages)
    }

    /// The string operand of a line filter.
    fn parse_line_filter_value(&mut self, op: LineFilterOp) -> Result<LineFilter, ParseError> {
        match self.next() {
            Some(t) => match t.token {
                Token::String(value) => Ok(LineFilter { op, value }),
                ref other => Err(self.error_at(
                    format!("expected quoted string after line filter, found '{other}'"),
                    Some(&t),
                )),
            },
            None => Err(self.error_at("expected quoted string after line filter", None)),
        }
    }

    /// Parse the stage that follows a bare `|`.
    fn parse_pipe_stage(&mut self) -> Result<PipelineStage, ParseError> {
        // `unwrap` is a keyword; every other stage head is an identifier.
        if let Some(t) = self.peek()
            && t.token == Token::Unwrap
        {
            self.next();
            return Ok(PipelineStage::Unwrap(self.parse_unwrap()?));
        }

        let head = match self.peek() {
            Some(t) => match &t.token {
                Token::Ident(name) => name.clone(),
                other => {
                    return Err(self.error_at(
                        format!("expected a pipeline stage after '|', found '{other}'"),
                        Some(t),
                    ));
                }
            },
            None => return Err(self.error_at("expected a pipeline stage after '|'", None)),
        };

        match head.as_str() {
            "json" => {
                self.next();
                Ok(PipelineStage::Json(self.parse_label_extractions()?))
            }
            "logfmt" => {
                self.next();
                Ok(PipelineStage::Logfmt(self.parse_label_extractions()?))
            }
            "regexp" => {
                self.next();
                Ok(PipelineStage::Regexp(
                    self.expect_string("a regexp pattern")?,
                ))
            }
            "pattern" => {
                self.next();
                Ok(PipelineStage::Pattern(
                    self.expect_string("a pattern string")?,
                ))
            }
            "unpack" => {
                self.next();
                Ok(PipelineStage::Unpack)
            }
            "line_format" => {
                self.next();
                Ok(PipelineStage::LineFormat(
                    self.expect_string("a line_format template")?,
                ))
            }
            "label_format" => {
                self.next();
                Ok(PipelineStage::LabelFormat(self.parse_label_formats()?))
            }
            "drop" => {
                self.next();
                Ok(PipelineStage::Drop(self.parse_label_name_list()?))
            }
            "keep" => {
                self.next();
                Ok(PipelineStage::Keep(self.parse_label_name_list()?))
            }
            // Anything else is a label filter expression (it starts with
            // a label name, which is an identifier).
            _ => Ok(PipelineStage::LabelFilter(self.parse_label_filter_expr()?)),
        }
    }

    fn expect_string(&mut self, what: &str) -> Result<String, ParseError> {
        match self.next() {
            Some(t) => match t.token {
                Token::String(value) => Ok(value),
                ref other => {
                    Err(self.error_at(format!("expected {what}, found '{other}'"), Some(&t)))
                }
            },
            None => Err(self.error_at(format!("expected {what}, found end of input"), None)),
        }
    }

    /// An identifier or a keyword used as an identifier.
    fn expect_ident(&mut self, what: &str) -> Result<String, ParseError> {
        match self.next() {
            Some(t) => match &t.token {
                Token::Ident(name) => Ok(name.clone()),
                other if Token::keyword_text(other).is_some() => {
                    Ok(Token::keyword_text(other).expect("checked").to_string())
                }
                other => Err(self.error_at(format!("expected {what}, found '{other}'"), Some(&t))),
            },
            None => Err(self.error_at(format!("expected {what}, found end of input"), None)),
        }
    }

    /// Optional comma-separated label extractions for `json`/`logfmt`.
    /// Each is `name` or `name="expression"`. Stops at the next stage.
    fn parse_label_extractions(&mut self) -> Result<Vec<LabelExtraction>, ParseError> {
        let mut out = Vec::new();
        // No extraction params when the next token isn't an identifier.
        if !matches!(self.peek().map(|t| &t.token), Some(Token::Ident(_))) {
            return Ok(out);
        }
        loop {
            let name = self.expect_ident("a label name")?;
            let expr = if matches!(self.peek().map(|t| &t.token), Some(Token::Eq)) {
                self.next();
                Some(self.expect_string("an extraction expression")?)
            } else {
                None
            };
            out.push(LabelExtraction { name, expr });
            if matches!(self.peek().map(|t| &t.token), Some(Token::Comma)) {
                self.next();
            } else {
                break;
            }
        }
        Ok(out)
    }

    /// Comma-separated `label_format` assignments: `dst=src` (rename) or
    /// `dst="template"`.
    fn parse_label_formats(&mut self) -> Result<Vec<LabelFormat>, ParseError> {
        let mut out = Vec::new();
        loop {
            let dst = self.expect_ident("a destination label")?;
            self.expect(&Token::Eq, "'=' in label_format assignment")?;
            let value = match self.next() {
                Some(t) => match t.token {
                    Token::String(tmpl) => LabelFormatValue::Template(tmpl),
                    Token::Ident(src) => LabelFormatValue::Rename(src),
                    ref other => {
                        return Err(self.error_at(
                            format!("expected a source label or template string, found '{other}'"),
                            Some(&t),
                        ));
                    }
                },
                None => {
                    return Err(self.error_at("expected a source label or template string", None));
                }
            };
            out.push(LabelFormat { dst, value });
            if matches!(self.peek().map(|t| &t.token), Some(Token::Comma)) {
                self.next();
            } else {
                break;
            }
        }
        Ok(out)
    }

    /// Comma-separated bare label names for `drop`/`keep`.
    fn parse_label_name_list(&mut self) -> Result<Vec<String>, ParseError> {
        let mut out = vec![self.expect_ident("a label name")?];
        while matches!(self.peek().map(|t| &t.token), Some(Token::Comma)) {
            self.next();
            out.push(self.expect_ident("a label name")?);
        }
        Ok(out)
    }

    /// `unwrap <label>` or `unwrap <conversion>(<label>)`.
    fn parse_unwrap(&mut self) -> Result<Unwrap, ParseError> {
        let first = self.expect_ident("a label to unwrap")?;
        if matches!(self.peek().map(|t| &t.token), Some(Token::LParen)) {
            self.next();
            let label = self.expect_ident("a label inside the conversion")?;
            self.expect(&Token::RParen, "')' to close the unwrap conversion")?;
            Ok(Unwrap {
                label,
                conversion: Some(first),
            })
        } else {
            Ok(Unwrap {
                label: first,
                conversion: None,
            })
        }
    }

    /// Label-filter expression with `or` (loosest), then `and` / `,`.
    fn parse_label_filter_expr(&mut self) -> Result<LabelFilterExpr, ParseError> {
        let mut left = self.parse_label_filter_and()?;
        while matches!(self.peek().map(|t| &t.token), Some(Token::Or)) {
            self.next();
            let right = self.parse_label_filter_and()?;
            left = LabelFilterExpr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_label_filter_and(&mut self) -> Result<LabelFilterExpr, ParseError> {
        let mut left = self.parse_label_filter_pred()?;
        // Both `and` and a bare comma mean conjunction.
        while matches!(
            self.peek().map(|t| &t.token),
            Some(Token::And) | Some(Token::Comma)
        ) {
            self.next();
            let right = self.parse_label_filter_pred()?;
            left = LabelFilterExpr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_label_filter_pred(&mut self) -> Result<LabelFilterExpr, ParseError> {
        let name = self.expect_ident("a label name")?;
        let (op_token, op) = match self.next() {
            Some(t) => {
                let op = match t.token {
                    Token::Eq => FilterOp::Eq,
                    Token::Neq => FilterOp::Neq,
                    Token::Re => FilterOp::Re,
                    Token::Nre => FilterOp::Nre,
                    Token::CmpEq => FilterOp::CmpEq,
                    Token::Gt => FilterOp::Gt,
                    Token::Gte => FilterOp::Gte,
                    Token::Lt => FilterOp::Lt,
                    Token::Lte => FilterOp::Lte,
                    ref other => {
                        return Err(self.error_at(
                            format!(
                                "expected a comparison operator in label filter, found '{other}'"
                            ),
                            Some(&t),
                        ));
                    }
                };
                (t, op)
            }
            None => {
                return Err(self.error_at("expected a comparison operator in label filter", None));
            }
        };

        let value = self.parse_filter_value()?;
        self.check_op_value(op, &value, &op_token)?;
        Ok(LabelFilterExpr::Pred(LabelFilterPred { name, op, value }))
    }

    fn parse_filter_value(&mut self) -> Result<FilterValue, ParseError> {
        match self.next() {
            Some(t) => match t.token {
                Token::String(s) => Ok(FilterValue::String(s)),
                Token::Number(n) => Ok(FilterValue::Number(n)),
                Token::Duration(d) => Ok(FilterValue::Duration(d)),
                ref other => Err(self.error_at(
                    format!(
                        "expected a string, number, or duration in label filter, found '{other}'"
                    ),
                    Some(&t),
                )),
            },
            None => Err(self.error_at("expected a value in label filter", None)),
        }
    }

    /// Reject operator/value combinations that don't type-check: regex
    /// matchers require a string; ordered comparisons require a number
    /// or duration.
    fn check_op_value(
        &self,
        op: FilterOp,
        value: &FilterValue,
        op_token: &SpannedToken,
    ) -> Result<(), ParseError> {
        let ok = match op {
            FilterOp::Re | FilterOp::Nre => matches!(value, FilterValue::String(_)),
            FilterOp::Gt | FilterOp::Gte | FilterOp::Lt | FilterOp::Lte => {
                matches!(value, FilterValue::Number(_) | FilterValue::Duration(_))
            }
            FilterOp::Eq | FilterOp::Neq | FilterOp::CmpEq => true,
        };
        if ok {
            Ok(())
        } else {
            Err(self.error_at(
                format!(
                    "operator '{}' is not valid for this value type",
                    op_token.token
                ),
                Some(op_token),
            ))
        }
    }

    /// `{` [matcher {`,` matcher} [`,`]] `}`
    pub(crate) fn parse_selector(&mut self) -> Result<StreamSelector, ParseError> {
        self.expect(&Token::LBrace, "'{' to start a stream selector")?;

        let mut matchers = Vec::new();
        loop {
            match self.peek() {
                Some(t) if t.token == Token::RBrace => {
                    self.next();
                    break;
                }
                Some(_) => {
                    matchers.push(self.parse_matcher()?);
                    match self.next() {
                        Some(t) if t.token == Token::Comma => continue,
                        Some(t) if t.token == Token::RBrace => break,
                        Some(t) => {
                            return Err(self.error_at(
                                format!(
                                    "expected ',' or '}}' in stream selector, found '{}'",
                                    t.token
                                ),
                                Some(&t),
                            ));
                        }
                        None => {
                            return Err(
                                self.error_at("unclosed stream selector (missing '}')", None)
                            );
                        }
                    }
                }
                None => {
                    return Err(self.error_at("unclosed stream selector (missing '}')", None));
                }
            }
        }

        Ok(StreamSelector { matchers })
    }

    /// `name (= | != | =~ | !~) "value"`
    fn parse_matcher(&mut self) -> Result<LabelMatcher, ParseError> {
        let name = match self.next() {
            // Grammar keywords are valid label names inside a selector
            // (`{by="x"}` selects the label "by").
            Some(t) => match &t.token {
                Token::Ident(name) => name.clone(),
                other if Token::keyword_text(other).is_some() => {
                    Token::keyword_text(other).expect("checked").to_string()
                }
                other => {
                    return Err(
                        self.error_at(format!("expected label name, found '{other}'"), Some(&t))
                    );
                }
            },
            None => return Err(self.error_at("expected label name, found end of input", None)),
        };

        let op = match self.next() {
            Some(t) => match t.token {
                Token::Eq => MatchOp::Eq,
                Token::Neq => MatchOp::Neq,
                Token::Re => MatchOp::Re,
                Token::Nre => MatchOp::Nre,
                ref other => {
                    return Err(self.error_at(
                        format!("expected matcher operator (=, !=, =~, !~), found '{other}'"),
                        Some(&t),
                    ));
                }
            },
            None => {
                return Err(self.error_at("expected matcher operator, found end of input", None));
            }
        };

        let value = match self.next() {
            Some(t) => match t.token {
                Token::String(value) => value,
                ref other => {
                    return Err(self.error_at(
                        format!("expected quoted label value, found '{other}'"),
                        Some(&t),
                    ));
                }
            },
            None => return Err(self.error_at("expected label value, found end of input", None)),
        };

        Ok(LabelMatcher { name, op, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matcher(name: &str, op: MatchOp, value: &str) -> LabelMatcher {
        LabelMatcher {
            name: name.to_string(),
            op,
            value: value.to_string(),
        }
    }

    #[test]
    fn parses_single_matcher() {
        assert_eq!(
            parse_selector(r#"{job="api"}"#).unwrap().matchers,
            vec![matcher("job", MatchOp::Eq, "api")]
        );
    }

    #[test]
    fn parses_multiple_matchers_with_all_operators() {
        assert_eq!(
            parse_selector(
                r#"{service_name="frontend", level!="debug", namespace=~"prod-.*", env!~"dev.*"}"#
            )
            .unwrap()
            .matchers,
            vec![
                matcher("service_name", MatchOp::Eq, "frontend"),
                matcher("level", MatchOp::Neq, "debug"),
                matcher("namespace", MatchOp::Re, "prod-.*"),
                matcher("env", MatchOp::Nre, "dev.*"),
            ]
        );
    }

    #[test]
    fn parses_empty_selector_and_trailing_comma() {
        assert!(parse_selector("{}").unwrap().matchers.is_empty());
        assert_eq!(parse_selector(r#"{job="api",}"#).unwrap().matchers.len(), 1);
    }

    #[test]
    fn keywords_are_valid_label_names() {
        assert_eq!(
            parse_selector(r#"{by="x", offset!="y"}"#).unwrap().matchers,
            vec![
                matcher("by", MatchOp::Eq, "x"),
                matcher("offset", MatchOp::Neq, "y"),
            ]
        );
    }

    #[test]
    fn parse_query_returns_empty_pipeline() {
        let query = parse_query(r#"{job="api"}"#).unwrap();
        assert_eq!(query.selector.matchers.len(), 1);
        assert!(query.pipeline.is_empty());
    }

    #[test]
    fn parse_query_parses_a_line_filter_pipeline() {
        let query = parse_query(r#"{job="api"} |= "error""#).unwrap();
        assert_eq!(query.selector.matchers.len(), 1);
        assert_eq!(
            query.pipeline,
            vec![PipelineStage::LineFilter(LineFilter {
                op: LineFilterOp::Contains,
                value: "error".to_string(),
            })]
        );
    }

    #[test]
    fn rejects_missing_brace() {
        let err = parse_selector(r#"{job="api""#).unwrap_err();
        assert!(err.message.contains("missing '}'"), "{err}");
    }

    #[test]
    fn rejects_missing_operator_and_value() {
        let err = parse_selector(r#"{job}"#).unwrap_err();
        assert!(err.message.contains("matcher operator"), "{err}");

        let err = parse_selector(r#"{job=}"#).unwrap_err();
        assert!(err.message.contains("quoted label value"), "{err}");

        let err = parse_selector(r#"{job=api}"#).unwrap_err();
        assert!(err.message.contains("quoted label value"), "{err}");
    }

    #[test]
    fn rejects_missing_selector() {
        let err = parse_selector(r#"job="api""#).unwrap_err();
        assert!(err.message.contains("'{'"), "{err}");
    }

    #[test]
    fn error_positions_point_at_offending_token() {
        let err = parse_selector("{job =\n  5}").unwrap_err();
        assert_eq!((err.line, err.col), (2, 3));
    }

    #[test]
    fn lex_errors_propagate_with_position() {
        let err = parse_selector(r#"{job="unterminated}"#).unwrap_err();
        assert!(err.message.contains("unterminated"), "{err}");
    }

    // ---- pipeline stages (#371) ----

    fn pipeline(input: &str) -> Vec<PipelineStage> {
        parse_query(input).expect("parse").pipeline
    }

    fn line(op: LineFilterOp, value: &str) -> PipelineStage {
        PipelineStage::LineFilter(LineFilter {
            op,
            value: value.to_string(),
        })
    }

    #[test]
    fn parses_all_line_filter_operators() {
        assert_eq!(
            pipeline(r#"{a="b"} |= "keep" != "drop" |~ "re" !~ "nre""#),
            vec![
                line(LineFilterOp::Contains, "keep"),
                line(LineFilterOp::NotContains, "drop"),
                line(LineFilterOp::Regex, "re"),
                line(LineFilterOp::NotRegex, "nre"),
            ]
        );
    }

    #[test]
    fn parses_json_and_logfmt_with_and_without_extractions() {
        assert_eq!(
            pipeline(r#"{a="b"} | json"#),
            vec![PipelineStage::Json(vec![])]
        );
        assert_eq!(
            pipeline(r#"{a="b"} | json status="response.status", method"#),
            vec![PipelineStage::Json(vec![
                LabelExtraction {
                    name: "status".into(),
                    expr: Some("response.status".into()),
                },
                LabelExtraction {
                    name: "method".into(),
                    expr: None,
                },
            ])]
        );
        assert_eq!(
            pipeline(r#"{a="b"} | logfmt"#),
            vec![PipelineStage::Logfmt(vec![])]
        );
    }

    #[test]
    fn parses_regexp_pattern_unpack() {
        assert_eq!(
            pipeline(r#"{a="b"} | regexp "(?P<method>\\w+)""#),
            vec![PipelineStage::Regexp(r"(?P<method>\w+)".into())]
        );
        assert_eq!(
            pipeline(r#"{a="b"} | pattern "<method> <path>""#),
            vec![PipelineStage::Pattern("<method> <path>".into())]
        );
        assert_eq!(pipeline(r#"{a="b"} | unpack"#), vec![PipelineStage::Unpack]);
    }

    #[test]
    fn parses_formatters() {
        assert_eq!(
            pipeline(r#"{a="b"} | line_format "{{.msg}}""#),
            vec![PipelineStage::LineFormat("{{.msg}}".into())]
        );
        assert_eq!(
            pipeline(r#"{a="b"} | label_format dst="{{.src}}", renamed=original"#),
            vec![PipelineStage::LabelFormat(vec![
                LabelFormat {
                    dst: "dst".into(),
                    value: LabelFormatValue::Template("{{.src}}".into()),
                },
                LabelFormat {
                    dst: "renamed".into(),
                    value: LabelFormatValue::Rename("original".into()),
                },
            ])]
        );
    }

    #[test]
    fn parses_drop_and_keep() {
        assert_eq!(
            pipeline(r#"{a="b"} | drop level, source"#),
            vec![PipelineStage::Drop(vec!["level".into(), "source".into()])]
        );
        assert_eq!(
            pipeline(r#"{a="b"} | keep pod"#),
            vec![PipelineStage::Keep(vec!["pod".into()])]
        );
    }

    #[test]
    fn parses_unwrap_forms() {
        assert_eq!(
            pipeline(r#"{a="b"} | unwrap duration_ms"#),
            vec![PipelineStage::Unwrap(Unwrap {
                label: "duration_ms".into(),
                conversion: None,
            })]
        );
        assert_eq!(
            pipeline(r#"{a="b"} | unwrap duration(latency)"#),
            vec![PipelineStage::Unwrap(Unwrap {
                label: "latency".into(),
                conversion: Some("duration".into()),
            })]
        );
    }

    #[test]
    fn parses_label_filter_string_and_numeric() {
        assert_eq!(
            pipeline(r#"{a="b"} | level="error""#),
            vec![PipelineStage::LabelFilter(LabelFilterExpr::Pred(
                LabelFilterPred {
                    name: "level".into(),
                    op: FilterOp::Eq,
                    value: FilterValue::String("error".into()),
                }
            ))]
        );
        assert_eq!(
            pipeline(r#"{a="b"} | status >= 500"#),
            vec![PipelineStage::LabelFilter(LabelFilterExpr::Pred(
                LabelFilterPred {
                    name: "status".into(),
                    op: FilterOp::Gte,
                    value: FilterValue::Number(500.0),
                }
            ))]
        );
    }

    #[test]
    fn label_filter_and_or_precedence() {
        // `a and b or c` parses as `(a and b) or c`.
        let stages = pipeline(r#"{x="y"} | a="1" and b="2" or c="3""#);
        let PipelineStage::LabelFilter(expr) = &stages[0] else {
            panic!("expected label filter");
        };
        match expr {
            LabelFilterExpr::Or(left, right) => {
                assert!(matches!(**left, LabelFilterExpr::And(_, _)));
                assert!(matches!(**right, LabelFilterExpr::Pred(_)));
            }
            other => panic!("expected top-level Or, got {other:?}"),
        }
    }

    #[test]
    fn label_filter_comma_is_conjunction() {
        let stages = pipeline(r#"{x="y"} | a="1", b="2""#);
        let PipelineStage::LabelFilter(expr) = &stages[0] else {
            panic!("expected label filter");
        };
        assert!(matches!(expr, LabelFilterExpr::And(_, _)));
    }

    #[test]
    fn parses_full_pipeline_end_to_end() {
        let query = parse_query(
            r#"{app="api"} |= "error" | json | status >= 500 | line_format "{{.msg}}""#,
        )
        .unwrap();
        assert_eq!(query.pipeline.len(), 4);
        assert!(matches!(query.pipeline[0], PipelineStage::LineFilter(_)));
        assert!(matches!(query.pipeline[1], PipelineStage::Json(_)));
        assert!(matches!(query.pipeline[2], PipelineStage::LabelFilter(_)));
        assert!(matches!(query.pipeline[3], PipelineStage::LineFormat(_)));
    }

    #[test]
    fn duration_label_filter() {
        assert_eq!(
            pipeline(r#"{a="b"} | logfmt | latency > 1s"#),
            vec![
                PipelineStage::Logfmt(vec![]),
                PipelineStage::LabelFilter(LabelFilterExpr::Pred(LabelFilterPred {
                    name: "latency".into(),
                    op: FilterOp::Gt,
                    value: FilterValue::Duration(std::time::Duration::from_secs(1)),
                })),
            ]
        );
    }

    #[test]
    fn rejects_regex_operator_against_number() {
        let err = parse_query(r#"{a="b"} | status =~ 500"#).unwrap_err();
        assert!(
            err.message.contains("not valid for this value type"),
            "{err}"
        );
    }

    #[test]
    fn rejects_ordered_comparison_against_string() {
        let err = parse_query(r#"{a="b"} | level > "error""#).unwrap_err();
        assert!(
            err.message.contains("not valid for this value type"),
            "{err}"
        );
    }

    #[test]
    fn rejects_line_filter_without_string() {
        let err = parse_query(r#"{a="b"} |= 5"#).unwrap_err();
        assert!(err.message.contains("quoted string"), "{err}");
    }

    #[test]
    fn rejects_dangling_pipe() {
        let err = parse_query(r#"{a="b"} |"#).unwrap_err();
        assert!(err.message.contains("pipeline stage after '|'"), "{err}");
    }
}
