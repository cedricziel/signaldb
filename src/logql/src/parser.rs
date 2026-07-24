//! # LogQL Parser
//!
//! Recursive-descent parser over the [`crate::lexer`] token stream.
//! Covers stream selectors and log pipelines (line filters, parser
//! stages, label filters, formatters, unwrap); metric queries follow in
//! a later phase of the epic.

use crate::ast::{
    FilterOp, FilterValue, Grouping, LabelExtraction, LabelFilterExpr, LabelFilterPred,
    LabelFormat, LabelFormatValue, LabelMatcher, LineFilter, LineFilterOp, LogQuery, MatchOp,
    PipelineStage, StreamSelector, Unwrap,
};
use crate::lexer::{LexError, tokenize};
use crate::metric::{
    AggregationFunction, BinOp, BinaryExpr, MetricQuery, RangeAggregation, RangeFunction,
    VectorAggregation,
};
use crate::token::{SpannedToken, Token};

/// A top-level LogQL expression: either a log query (returns log lines)
/// or a metric query (returns numeric samples).
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Log(LogQuery),
    Metric(MetricQuery),
}

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

/// Parse a top-level LogQL expression, dispatching between a log query
/// and a metric query. A query beginning with `{` is a log query; any
/// other start (a range/vector function, a number, or a parenthesis) is
/// a metric query.
pub fn parse(input: &str) -> Result<Expr, ParseError> {
    let mut parser = Parser::new(input)?;
    let expr = if matches!(parser.peek().map(|t| &t.token), Some(Token::LBrace)) {
        let selector = parser.parse_selector()?;
        let pipeline = parser.parse_pipeline()?;
        Expr::Log(LogQuery { selector, pipeline })
    } else {
        Expr::Metric(parser.parse_metric_expr()?)
    };
    parser.expect_eof()?;
    Ok(expr)
}

/// Parse a metric query (rejects a bare log query).
pub fn parse_metric_query(input: &str) -> Result<MetricQuery, ParseError> {
    let mut parser = Parser::new(input)?;
    let query = parser.parse_metric_expr()?;
    parser.expect_eof()?;
    Ok(query)
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

    // ---- metric queries (#372) ----

    /// Metric expression with full binary-operator precedence.
    fn parse_metric_expr(&mut self) -> Result<MetricQuery, ParseError> {
        self.parse_binary(0)
    }

    /// Precedence-climbing binary-operator parser. `min_prec` is the
    /// lowest binding power this call will consume.
    ///
    /// Tiers, loosest to tightest: `or` (1); `and`/`unless` (2);
    /// comparisons (3); `+`/`-` (4); `*`/`/`/`%` (5); `^` (6,
    /// right-associative). Unary and atoms bind tighter still.
    fn parse_binary(&mut self, min_prec: u8) -> Result<MetricQuery, ParseError> {
        let mut left = self.parse_metric_unary()?;
        while let Some((op, prec, right_assoc)) = self.peek_binary_op() {
            if prec < min_prec {
                break;
            }
            self.next(); // consume operator

            // Optional `bool` modifier on comparison operators.
            let bool_modifier = if is_comparison(op)
                && matches!(self.peek().map(|t| &t.token), Some(Token::Bool))
            {
                self.next();
                true
            } else {
                false
            };

            let next_min = if right_assoc { prec } else { prec + 1 };
            let right = self.parse_binary(next_min)?;
            left = MetricQuery::Binary(Box::new(BinaryExpr {
                op,
                left,
                right,
                bool_modifier,
            }));
        }
        Ok(left)
    }

    /// Map the upcoming token to a binary operator and its precedence.
    fn peek_binary_op(&self) -> Option<(BinOp, u8, bool)> {
        let op = match self.peek().map(|t| &t.token)? {
            Token::Or => (BinOp::Or, 1, false),
            Token::And => (BinOp::And, 2, false),
            Token::Unless => (BinOp::Unless, 2, false),
            Token::CmpEq => (BinOp::Eq, 3, false),
            Token::Neq => (BinOp::Neq, 3, false),
            Token::Gt => (BinOp::Gt, 3, false),
            Token::Gte => (BinOp::Gte, 3, false),
            Token::Lt => (BinOp::Lt, 3, false),
            Token::Lte => (BinOp::Lte, 3, false),
            Token::Add => (BinOp::Add, 4, false),
            Token::Sub => (BinOp::Sub, 4, false),
            Token::Mul => (BinOp::Mul, 5, false),
            Token::Div => (BinOp::Div, 5, false),
            Token::Mod => (BinOp::Mod, 5, false),
            Token::Pow => (BinOp::Pow, 6, true),
            _ => return None,
        };
        Some(op)
    }

    /// A unary-prefixed metric atom (`-x` negates a scalar/vector).
    fn parse_metric_unary(&mut self) -> Result<MetricQuery, ParseError> {
        if matches!(self.peek().map(|t| &t.token), Some(Token::Sub)) {
            self.next();
            let inner = self.parse_metric_unary()?;
            // Represent `-x` as `0 - x`.
            return Ok(MetricQuery::Binary(Box::new(BinaryExpr {
                op: BinOp::Sub,
                left: MetricQuery::Literal(0.0),
                right: inner,
                bool_modifier: false,
            })));
        }
        if matches!(self.peek().map(|t| &t.token), Some(Token::Add)) {
            self.next();
            return self.parse_metric_unary();
        }
        self.parse_metric_atom()
    }

    /// A metric atom: parenthesized expr, scalar literal, range
    /// aggregation, or vector aggregation.
    fn parse_metric_atom(&mut self) -> Result<MetricQuery, ParseError> {
        match self.peek().map(|t| &t.token) {
            Some(Token::LParen) => {
                self.next();
                let inner = self.parse_metric_expr()?;
                self.expect(&Token::RParen, "')' to close a parenthesized expression")?;
                Ok(inner)
            }
            Some(Token::Number(n)) => {
                let n = *n;
                self.next();
                Ok(MetricQuery::Literal(n))
            }
            Some(Token::Ident(name)) => {
                let name = name.clone();
                if RangeFunction::from_name(&name).is_some() {
                    self.parse_range_aggregation()
                } else if AggregationFunction::from_name(&name).is_some() {
                    self.parse_vector_aggregation()
                } else {
                    let t = self.peek().cloned();
                    Err(self.error_at(format!("unknown metric function '{name}'"), t.as_ref()))
                }
            }
            _ => {
                let t = self.peek().cloned();
                Err(self.error_at("expected a metric query", t.as_ref()))
            }
        }
    }

    /// `func ( [param,] logRangeExpr )` where `logRangeExpr` is a log
    /// selector/pipeline carrying a `[range]`, plus optional `offset`
    /// and `unwrap`; unwrapped forms may carry a trailing grouping.
    fn parse_range_aggregation(&mut self) -> Result<MetricQuery, ParseError> {
        let name = self.expect_ident("a range function")?;
        let function = RangeFunction::from_name(&name).expect("checked by caller");
        self.expect(&Token::LParen, "'(' after a range function")?;

        let param = if function.takes_param() {
            let p = self.parse_number("a scalar parameter")?;
            self.expect(&Token::Comma, "',' after the range function parameter")?;
            Some(p)
        } else {
            None
        };

        let (log_query, range, offset, unwrap) = self.parse_log_range_expr()?;
        self.expect(&Token::RParen, "')' to close the range aggregation")?;

        // A grouping may trail an unwrapped range aggregation.
        let grouping = self.parse_optional_grouping()?;

        Ok(MetricQuery::Range(Box::new(RangeAggregation {
            function,
            log_query,
            range,
            offset,
            unwrap,
            param,
            grouping,
        })))
    }

    /// Parse the `{selector} pipeline [range] [offset d] [| unwrap ...]`
    /// core of a range aggregation. The `[range]` may appear before or
    /// after the unwrap stage, matching Loki's grammar.
    fn parse_log_range_expr(
        &mut self,
    ) -> Result<
        (
            LogQuery,
            std::time::Duration,
            Option<std::time::Duration>,
            Option<Unwrap>,
        ),
        ParseError,
    > {
        let selector = self.parse_selector()?;
        let mut pipeline = self.parse_pipeline()?;

        // Range window `[Nd]`.
        let range = self.parse_range_window()?;
        let offset = self.parse_optional_offset()?;

        // In `{..} | unwrap x [5m]`, the unwrap is the last pipeline
        // stage and the range follows it; lift it into the aggregation.
        let unwrap = match pipeline.last() {
            Some(PipelineStage::Unwrap(_)) => {
                let Some(PipelineStage::Unwrap(u)) = pipeline.pop() else {
                    unreachable!()
                };
                Some(u)
            }
            _ => None,
        };

        Ok((
            LogQuery {
                selector: selector.clone(),
                pipeline,
            },
            range,
            offset,
            unwrap,
        ))
    }

    /// `[ <duration> ]`
    fn parse_range_window(&mut self) -> Result<std::time::Duration, ParseError> {
        self.expect(&Token::LBracket, "'[' to start a range window")?;
        let range = match self.next() {
            Some(t) => match t.token {
                Token::Duration(d) => d,
                ref other => {
                    return Err(self.error_at(
                        format!("expected a range duration, found '{other}'"),
                        Some(&t),
                    ));
                }
            },
            None => return Err(self.error_at("expected a range duration", None)),
        };
        self.expect(&Token::RBracket, "']' to close a range window")?;
        Ok(range)
    }

    /// Optional `offset <duration>`.
    fn parse_optional_offset(&mut self) -> Result<Option<std::time::Duration>, ParseError> {
        if !matches!(self.peek().map(|t| &t.token), Some(Token::Offset)) {
            return Ok(None);
        }
        self.next();
        match self.next() {
            Some(t) => match t.token {
                Token::Duration(d) => Ok(Some(d)),
                ref other => Err(self.error_at(
                    format!("expected a duration after 'offset', found '{other}'"),
                    Some(&t),
                )),
            },
            None => Err(self.error_at("expected a duration after 'offset'", None)),
        }
    }

    /// `agg [by/without (labels)] ( [param,] inner )` — grouping may also
    /// trail the parenthesized inner query.
    fn parse_vector_aggregation(&mut self) -> Result<MetricQuery, ParseError> {
        let name = self.expect_ident("an aggregation function")?;
        let function = AggregationFunction::from_name(&name).expect("checked by caller");

        // Grouping may precede the argument list.
        let mut grouping = self.parse_optional_grouping()?;

        self.expect(&Token::LParen, "'(' after an aggregation function")?;
        let param = if function.takes_param() {
            let p = self.parse_number("a scalar parameter")?;
            self.expect(&Token::Comma, "',' after the aggregation parameter")?;
            Some(p)
        } else {
            None
        };
        let inner = self.parse_metric_expr()?;
        self.expect(&Token::RParen, "')' to close the aggregation")?;

        // ...or after it (`sum(...) by (label)`).
        if grouping.is_none() {
            grouping = self.parse_optional_grouping()?;
        }

        Ok(MetricQuery::Vector(Box::new(VectorAggregation {
            function,
            param,
            grouping,
            inner,
        })))
    }

    /// Optional `by (...)` / `without (...)` clause.
    fn parse_optional_grouping(&mut self) -> Result<Option<Grouping>, ParseError> {
        let without = match self.peek().map(|t| &t.token) {
            Some(Token::By) => false,
            Some(Token::Without) => true,
            _ => return Ok(None),
        };
        self.next();
        self.expect(&Token::LParen, "'(' after by/without")?;
        let mut labels = Vec::new();
        if !matches!(self.peek().map(|t| &t.token), Some(Token::RParen)) {
            loop {
                labels.push(self.expect_ident("a grouping label")?);
                if matches!(self.peek().map(|t| &t.token), Some(Token::Comma)) {
                    self.next();
                } else {
                    break;
                }
            }
        }
        self.expect(&Token::RParen, "')' to close by/without")?;
        Ok(Some(Grouping { without, labels }))
    }

    /// A bare numeric literal.
    fn parse_number(&mut self, what: &str) -> Result<f64, ParseError> {
        match self.next() {
            Some(t) => match t.token {
                Token::Number(n) => Ok(n),
                ref other => {
                    Err(self.error_at(format!("expected {what}, found '{other}'"), Some(&t)))
                }
            },
            None => Err(self.error_at(format!("expected {what}, found end of input"), None)),
        }
    }
}

/// Whether an operator is a comparison (may carry a `bool` modifier).
fn is_comparison(op: BinOp) -> bool {
    matches!(
        op,
        BinOp::Eq | BinOp::Neq | BinOp::Gt | BinOp::Gte | BinOp::Lt | BinOp::Lte
    )
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

    // ---- metric queries (#372) ----

    use std::time::Duration;

    fn metric(input: &str) -> MetricQuery {
        parse_metric_query(input).expect("parse metric")
    }

    fn as_range(q: &MetricQuery) -> &RangeAggregation {
        match q {
            MetricQuery::Range(r) => r,
            other => panic!("expected range aggregation, got {other:?}"),
        }
    }

    fn as_vector(q: &MetricQuery) -> &VectorAggregation {
        match q {
            MetricQuery::Vector(v) => v,
            other => panic!("expected vector aggregation, got {other:?}"),
        }
    }

    fn as_binary(q: &MetricQuery) -> &BinaryExpr {
        match q {
            MetricQuery::Binary(b) => b,
            other => panic!("expected binary expression, got {other:?}"),
        }
    }

    #[test]
    fn parses_bare_range_aggregation() {
        let q = metric(r#"rate({job="api"}[5m])"#);
        let r = as_range(&q);
        assert_eq!(r.function, RangeFunction::Rate);
        assert_eq!(r.range, Duration::from_secs(300));
        assert_eq!(r.log_query.selector.matchers.len(), 1);
        assert!(r.log_query.pipeline.is_empty());
        assert!(r.offset.is_none() && r.unwrap.is_none() && r.param.is_none());
    }

    #[test]
    fn parses_range_aggregation_over_a_pipeline() {
        let q = metric(r#"count_over_time({job="api"} |= "error" [1h])"#);
        let r = as_range(&q);
        assert_eq!(r.function, RangeFunction::CountOverTime);
        assert_eq!(r.range, Duration::from_secs(3600));
        assert_eq!(r.log_query.pipeline.len(), 1);
        assert!(matches!(
            r.log_query.pipeline[0],
            PipelineStage::LineFilter(_)
        ));
    }

    #[test]
    fn parses_every_range_function_name() {
        for (name, func) in [
            ("rate", RangeFunction::Rate),
            ("rate_counter", RangeFunction::RateCounter),
            ("count_over_time", RangeFunction::CountOverTime),
            ("bytes_rate", RangeFunction::BytesRate),
            ("bytes_over_time", RangeFunction::BytesOverTime),
            ("first_over_time", RangeFunction::FirstOverTime),
            ("last_over_time", RangeFunction::LastOverTime),
            ("absent_over_time", RangeFunction::AbsentOverTime),
        ] {
            let q = metric(&format!(r#"{name}({{a="b"}}[5m])"#));
            assert_eq!(as_range(&q).function, func, "{name}");
        }
    }

    #[test]
    fn parses_offset_modifier() {
        let q = metric(r#"rate({a="b"}[5m] offset 1h)"#);
        let r = as_range(&q);
        assert_eq!(r.offset, Some(Duration::from_secs(3600)));
    }

    #[test]
    fn parses_unwrap_range_aggregation() {
        let q = metric(r#"avg_over_time({job="api"} | json | unwrap duration [5m])"#);
        let r = as_range(&q);
        assert_eq!(r.function, RangeFunction::AvgOverTime);
        assert_eq!(
            r.unwrap,
            Some(Unwrap {
                label: "duration".into(),
                conversion: None,
            })
        );
        // The unwrap stage is lifted out of the pipeline.
        assert_eq!(r.log_query.pipeline.len(), 1);
        assert!(matches!(r.log_query.pipeline[0], PipelineStage::Json(_)));
    }

    #[test]
    fn parses_quantile_over_time_param() {
        let q = metric(r#"quantile_over_time(0.99, {a="b"} | unwrap latency [5m])"#);
        let r = as_range(&q);
        assert_eq!(r.function, RangeFunction::QuantileOverTime);
        assert_eq!(r.param, Some(0.99));
        assert_eq!(r.unwrap.as_ref().unwrap().label, "latency");
    }

    #[test]
    fn parses_vector_aggregation_with_grouping() {
        let q = metric(r#"sum by (level) (rate({job="api"}[5m]))"#);
        let v = as_vector(&q);
        assert_eq!(v.function, AggregationFunction::Sum);
        assert_eq!(
            v.grouping,
            Some(Grouping {
                without: false,
                labels: vec!["level".into()],
            })
        );
        assert!(matches!(v.inner, MetricQuery::Range(_)));
    }

    #[test]
    fn parses_trailing_grouping() {
        let q = metric(r#"sum(rate({a="b"}[5m])) without (pod, ns)"#);
        let v = as_vector(&q);
        assert_eq!(
            v.grouping,
            Some(Grouping {
                without: true,
                labels: vec!["pod".into(), "ns".into()],
            })
        );
    }

    #[test]
    fn parses_topk_with_param_and_nested_aggregation() {
        let q = metric(r#"topk(10, sum by (path) (rate({job="api"}[5m])))"#);
        let v = as_vector(&q);
        assert_eq!(v.function, AggregationFunction::Topk);
        assert_eq!(v.param, Some(10.0));
        let inner = as_vector(&v.inner);
        assert_eq!(inner.function, AggregationFunction::Sum);
    }

    #[test]
    fn parses_empty_grouping() {
        let q = metric(r#"sum by () (rate({a="b"}[5m]))"#);
        assert_eq!(
            as_vector(&q).grouping.as_ref().unwrap().labels,
            Vec::<String>::new()
        );
    }

    #[test]
    fn parses_binary_arithmetic_with_scalar() {
        let q = metric(r#"rate({a="b"}[5m]) * 60"#);
        let b = as_binary(&q);
        assert_eq!(b.op, BinOp::Mul);
        assert!(matches!(b.left, MetricQuery::Range(_)));
        assert_eq!(b.right, MetricQuery::Literal(60.0));
    }

    #[test]
    fn binary_operator_precedence() {
        // `a + b * c` → `a + (b * c)`
        let q = metric(r#"rate({a="b"}[1m]) + rate({a="c"}[1m]) * 2"#);
        let b = as_binary(&q);
        assert_eq!(b.op, BinOp::Add);
        assert_eq!(as_binary(&b.right).op, BinOp::Mul);
    }

    #[test]
    fn pow_is_right_associative() {
        // `2 ^ 3 ^ 2` → `2 ^ (3 ^ 2)`
        let q = metric("2 ^ 3 ^ 2");
        let b = as_binary(&q);
        assert_eq!(b.op, BinOp::Pow);
        assert_eq!(b.left, MetricQuery::Literal(2.0));
        assert_eq!(as_binary(&b.right).op, BinOp::Pow);
    }

    #[test]
    fn logical_operators_bind_loosest() {
        // `a > 1 and b > 2 or c > 3` → `((a>1) and (b>2)) or (c>3)`
        let q =
            metric(r#"rate({a="1"}[1m]) > 1 and rate({a="2"}[1m]) > 2 or rate({a="3"}[1m]) > 3"#);
        let top = as_binary(&q);
        assert_eq!(top.op, BinOp::Or);
        assert_eq!(as_binary(&top.left).op, BinOp::And);
    }

    #[test]
    fn parses_bool_modifier_on_comparison() {
        let q = metric(r#"rate({a="b"}[5m]) > bool 5"#);
        let b = as_binary(&q);
        assert_eq!(b.op, BinOp::Gt);
        assert!(b.bool_modifier);
        assert_eq!(b.right, MetricQuery::Literal(5.0));
    }

    #[test]
    fn parses_parenthesized_and_unary_minus() {
        let q = metric(r#"(rate({a="b"}[5m]))"#);
        assert!(matches!(q, MetricQuery::Range(_)));

        let neg = metric(r#"- rate({a="b"}[5m])"#);
        let b = as_binary(&neg);
        assert_eq!(b.op, BinOp::Sub);
        assert_eq!(b.left, MetricQuery::Literal(0.0));
    }

    #[test]
    fn parse_dispatches_log_vs_metric() {
        assert!(matches!(parse(r#"{job="api"}"#).unwrap(), Expr::Log(_)));
        assert!(matches!(
            parse(r#"{job="api"} |= "x""#).unwrap(),
            Expr::Log(_)
        ));
        assert!(matches!(
            parse(r#"rate({job="api"}[5m])"#).unwrap(),
            Expr::Metric(_)
        ));
        assert!(matches!(
            parse(r#"sum(rate({a="b"}[1m]))"#).unwrap(),
            Expr::Metric(_)
        ));
    }

    #[test]
    fn rejects_missing_range_window() {
        let err = parse_metric_query(r#"rate({a="b"})"#).unwrap_err();
        assert!(
            err.message.contains("range window") || err.message.contains("'['"),
            "{err}"
        );
    }

    #[test]
    fn rejects_unknown_metric_function() {
        let err = parse_metric_query(r#"frobnicate({a="b"}[5m])"#).unwrap_err();
        assert!(err.message.contains("unknown metric function"), "{err}");
    }

    #[test]
    fn rejects_missing_quantile_param() {
        let err = parse_metric_query(r#"quantile_over_time({a="b"} | unwrap x [5m])"#).unwrap_err();
        assert!(
            err.message.contains("scalar parameter") || err.message.contains("','"),
            "{err}"
        );
    }

    #[test]
    fn rejects_unclosed_aggregation_paren() {
        let err = parse_metric_query(r#"sum(rate({a="b"}[5m])"#).unwrap_err();
        assert!(err.message.contains("')'"), "{err}");
    }
}
