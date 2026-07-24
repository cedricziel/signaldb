//! # LogQL Parser
//!
//! Recursive-descent parser over the [`crate::lexer`] token stream.
//! This module currently covers stream selectors; pipeline stages and
//! metric queries follow in later phases of the epic.

use crate::ast::{LabelMatcher, LogQuery, MatchOp, StreamSelector};
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

/// Parse a full log query. Pipelines are not parsed yet, so any input
/// beyond the stream selector is rejected (#371).
pub fn parse_query(input: &str) -> Result<LogQuery, ParseError> {
    let mut parser = Parser::new(input)?;
    let selector = parser.parse_selector()?;
    parser.expect_eof()?;
    Ok(LogQuery {
        selector,
        pipeline: Vec::new(),
    })
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
                message: format!("unexpected '{}' after stream selector", t.token),
                line: t.line,
                col: t.col,
            }),
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
    fn rejects_pipeline_input_for_now() {
        let err = parse_query(r#"{job="api"} |= "error""#).unwrap_err();
        assert!(err.message.contains("after stream selector"), "{err}");
        assert_eq!((err.line, err.col), (1, 13));
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
}
