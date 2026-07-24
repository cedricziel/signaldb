//! # LogQL Lexer
//!
//! Hand-rolled tokenizer for LogQL. Produces [`SpannedToken`]s with
//! 1-based line/column positions; every error carries the position it
//! occurred at.
//!
//! Notable rules:
//!
//! - A number immediately followed by a letter is a duration literal
//!   (`5m`, `1h30m`, `1.5h`); an invalid unit is an error rather than
//!   two adjacent tokens.
//! - Double-quoted strings process escapes; backtick strings are raw
//!   (the form used for regex patterns).
//! - `#` starts a comment running to end of line.

use crate::token::{SpannedToken, Token};
use std::time::Duration;

/// Lexing error with 1-based source position.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{message} at line {line}, column {col}")]
pub struct LexError {
    pub message: String,
    pub line: u32,
    pub col: u32,
}

/// Tokenize a LogQL query.
pub fn tokenize(input: &str) -> Result<Vec<SpannedToken>, LexError> {
    Lexer::new(input).run()
}

struct Lexer<'a> {
    chars: std::iter::Peekable<std::str::Chars<'a>>,
    line: u32,
    col: u32,
}

impl<'a> Lexer<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            chars: input.chars().peekable(),
            line: 1,
            col: 1,
        }
    }

    fn error(&self, message: impl Into<String>, line: u32, col: u32) -> LexError {
        LexError {
            message: message.into(),
            line,
            col,
        }
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.chars.next()?;
        if c == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(c)
    }

    /// Consume the next char when it equals `expected`.
    fn eat(&mut self, expected: char) -> bool {
        if self.chars.peek() == Some(&expected) {
            self.bump();
            true
        } else {
            false
        }
    }

    fn run(mut self) -> Result<Vec<SpannedToken>, LexError> {
        let mut tokens = Vec::new();
        loop {
            // Skip whitespace and comments.
            match self.chars.peek() {
                Some(c) if c.is_whitespace() => {
                    self.bump();
                    continue;
                }
                Some('#') => {
                    while let Some(&c) = self.chars.peek() {
                        if c == '\n' {
                            break;
                        }
                        self.bump();
                    }
                    continue;
                }
                None => break,
                _ => {}
            }

            let (line, col) = (self.line, self.col);
            let c = self.bump().expect("peeked above");
            let token = match c {
                '{' => Token::LBrace,
                '}' => Token::RBrace,
                '(' => Token::LParen,
                ')' => Token::RParen,
                '[' => Token::LBracket,
                ']' => Token::RBracket,
                ',' => Token::Comma,
                '+' => Token::Add,
                '-' => {
                    // `--flag` is a parser-stage flag; a single `-` is
                    // subtraction / unary minus.
                    if self.eat('-') {
                        self.lex_flag(line, col)?
                    } else {
                        Token::Sub
                    }
                }
                '*' => Token::Mul,
                '/' => Token::Div,
                '%' => Token::Mod,
                '^' => Token::Pow,
                '|' => {
                    if self.eat('=') {
                        Token::PipeExact
                    } else if self.eat('~') {
                        Token::PipeRegex
                    } else {
                        Token::Pipe
                    }
                }
                '=' => {
                    if self.eat('=') {
                        Token::CmpEq
                    } else if self.eat('~') {
                        Token::Re
                    } else {
                        Token::Eq
                    }
                }
                '!' => {
                    if self.eat('=') {
                        Token::Neq
                    } else if self.eat('~') {
                        Token::Nre
                    } else {
                        return Err(self.error(
                            "unexpected '!' (expected '!=' or '!~')",
                            line,
                            col,
                        ));
                    }
                }
                '>' => {
                    if self.eat('=') {
                        Token::Gte
                    } else {
                        Token::Gt
                    }
                }
                '<' => {
                    if self.eat('=') {
                        Token::Lte
                    } else {
                        Token::Lt
                    }
                }
                '"' => self.lex_string(line, col)?,
                '`' => self.lex_raw_string(line, col)?,
                c if c.is_ascii_digit() => self.lex_number_or_duration(c, line, col)?,
                c if c.is_alphabetic() || c == '_' => {
                    let mut word = String::from(c);
                    while let Some(&next) = self.chars.peek() {
                        if next.is_alphanumeric() || next == '_' {
                            word.push(next);
                            self.bump();
                        } else {
                            break;
                        }
                    }
                    Token::keyword(&word).unwrap_or(Token::Ident(word))
                }
                other => {
                    return Err(self.error(format!("unexpected character {other:?}"), line, col));
                }
            };
            tokens.push(SpannedToken { token, line, col });
        }
        Ok(tokens)
    }

    /// Double-quoted string; the opening quote is already consumed.
    fn lex_string(&mut self, line: u32, col: u32) -> Result<Token, LexError> {
        let mut value = String::new();
        loop {
            let (c_line, c_col) = (self.line, self.col);
            let Some(c) = self.bump() else {
                return Err(self.error("unterminated string literal", line, col));
            };
            match c {
                '"' => return Ok(Token::String(value)),
                '\\' => {
                    let Some(esc) = self.bump() else {
                        return Err(self.error("unterminated string literal", line, col));
                    };
                    match esc {
                        '"' => value.push('"'),
                        '\\' => value.push('\\'),
                        '/' => value.push('/'),
                        'n' => value.push('\n'),
                        't' => value.push('\t'),
                        'r' => value.push('\r'),
                        'u' => {
                            let mut code = String::new();
                            for _ in 0..4 {
                                match self.bump() {
                                    Some(h) if h.is_ascii_hexdigit() => code.push(h),
                                    _ => {
                                        return Err(self.error(
                                            "invalid \\u escape (expected 4 hex digits)",
                                            c_line,
                                            c_col,
                                        ));
                                    }
                                }
                            }
                            let cp = u32::from_str_radix(&code, 16).expect("hex digits");
                            match char::from_u32(cp) {
                                Some(ch) => value.push(ch),
                                None => {
                                    return Err(self.error(
                                        format!("invalid unicode code point \\u{code}"),
                                        c_line,
                                        c_col,
                                    ));
                                }
                            }
                        }
                        other => {
                            return Err(self.error(
                                format!("invalid escape sequence '\\{other}'"),
                                c_line,
                                c_col,
                            ));
                        }
                    }
                }
                other => value.push(other),
            }
        }
    }

    /// Backtick raw string (no escape processing); the opening backtick
    /// is already consumed.
    fn lex_raw_string(&mut self, line: u32, col: u32) -> Result<Token, LexError> {
        let mut value = String::new();
        loop {
            let Some(c) = self.bump() else {
                return Err(self.error("unterminated raw string literal", line, col));
            };
            if c == '`' {
                return Ok(Token::String(value));
            }
            value.push(c);
        }
    }

    /// Read the digits (with optional fraction/exponent) of one number
    /// starting with `first`.
    fn read_number(&mut self, first: char, line: u32, col: u32) -> Result<f64, LexError> {
        let mut text = String::from(first);
        while let Some(&c) = self.chars.peek() {
            if c.is_ascii_digit() {
                text.push(c);
                self.bump();
            } else {
                break;
            }
        }
        if self.chars.peek() == Some(&'.') {
            text.push('.');
            self.bump();
            let mut has_fraction = false;
            while let Some(&c) = self.chars.peek() {
                if c.is_ascii_digit() {
                    text.push(c);
                    self.bump();
                    has_fraction = true;
                } else {
                    break;
                }
            }
            if !has_fraction {
                return Err(self.error("expected digits after decimal point", line, col));
            }
        }
        // Exponent: only when followed by digits (or sign + digits), so
        // `5e` in a duration position is not misread. Peek-ahead here is
        // single-char, so clone the iterator to look past the 'e'.
        if matches!(self.chars.peek(), Some('e' | 'E')) {
            let mut lookahead = self.chars.clone();
            lookahead.next();
            let next = lookahead.peek().copied();
            let is_exponent = match next {
                Some(d) if d.is_ascii_digit() => true,
                Some('+') | Some('-') => {
                    lookahead.next();
                    matches!(lookahead.peek(), Some(d) if d.is_ascii_digit())
                }
                _ => false,
            };
            if is_exponent {
                text.push(self.bump().expect("peeked 'e'"));
                if matches!(self.chars.peek(), Some('+') | Some('-')) {
                    text.push(self.bump().expect("peeked sign"));
                }
                while let Some(&c) = self.chars.peek() {
                    if c.is_ascii_digit() {
                        text.push(c);
                        self.bump();
                    } else {
                        break;
                    }
                }
            }
        }
        text.parse::<f64>()
            .map_err(|_| self.error(format!("invalid number '{text}'"), line, col))
    }

    /// A number, or a duration when a unit letter follows the number.
    fn lex_number_or_duration(
        &mut self,
        first: char,
        line: u32,
        col: u32,
    ) -> Result<Token, LexError> {
        let value = self.read_number(first, line, col)?;

        // 'µ' (as in µs) is alphabetic, so this also covers it.
        if !matches!(self.chars.peek(), Some(c) if c.is_alphabetic()) {
            return Ok(Token::Number(value));
        }

        // Read the first unit. A bytes unit (`KB`, `MiB`, ...) yields a
        // bytes literal; otherwise fall through to duration parsing.
        let (u_line, u_col) = (self.line, self.col);
        let unit = self.read_unit();
        if let Some(factor) = bytes_factor(&unit) {
            if value < 0.0 {
                return Err(self.error("negative bytes literal", line, col));
            }
            let bytes = (value * factor as f64).round() as u64;
            return Ok(Token::Bytes(bytes));
        }

        // Duration: one or more (number, unit) pairs, e.g. `1h30m`. The
        // first unit was already read above.
        let mut total_secs = 0.0_f64;
        let mut pending = value;
        let mut unit = unit;
        let (mut u_line, mut u_col) = (u_line, u_col);
        loop {
            let factor = match unit.as_str() {
                "ns" => 1e-9,
                "us" | "µs" => 1e-6,
                "ms" => 1e-3,
                "s" => 1.0,
                "m" => 60.0,
                "h" => 3600.0,
                "d" => 86400.0,
                "w" => 604800.0,
                "y" => 31536000.0,
                _ => {
                    return Err(self.error(
                        format!("invalid duration unit '{unit}'"),
                        u_line,
                        u_col,
                    ));
                }
            };
            total_secs += pending * factor;

            // Another number directly attached continues the duration.
            match self.chars.peek() {
                Some(&c) if c.is_ascii_digit() => {
                    let first = self.bump().expect("peeked digit");
                    pending = self.read_number(first, self.line, self.col)?;
                    (u_line, u_col) = (self.line, self.col);
                    unit = self.read_unit();
                }
                _ => break,
            }
        }
        if total_secs < 0.0 {
            return Err(self.error("negative duration", line, col));
        }
        Ok(Token::Duration(Duration::from_secs_f64(total_secs)))
    }

    /// Read a run of unit letters (`ms`, `KiB`, `µs`, ...).
    fn read_unit(&mut self) -> String {
        let mut unit = String::new();
        while let Some(&c) = self.chars.peek() {
            if c.is_alphabetic() || c == 'µ' {
                unit.push(c);
                self.bump();
            } else {
                break;
            }
        }
        unit
    }

    /// A parser-stage flag: `--<name>`. The two dashes are already
    /// consumed.
    fn lex_flag(&mut self, line: u32, col: u32) -> Result<Token, LexError> {
        let mut name = String::new();
        while let Some(&c) = self.chars.peek() {
            if c.is_alphanumeric() || c == '-' || c == '_' {
                name.push(c);
                self.bump();
            } else {
                break;
            }
        }
        if name.is_empty() {
            return Err(self.error("expected a flag name after '--'", line, col));
        }
        Ok(Token::Flag(name))
    }
}

/// Bytes-per-unit factor for a byte-size unit, or `None` if `unit` is
/// not a bytes unit. Decimal units are powers of 1000; `*iB` units and
/// bare `KB`/`MB`/... follow Loki's `humanize` convention of powers of
/// 1024 for the `i` forms.
fn bytes_factor(unit: &str) -> Option<u64> {
    let f = match unit {
        "B" => 1,
        "kB" | "KB" => 1_000,
        "MB" => 1_000_000,
        "GB" => 1_000_000_000,
        "TB" => 1_000_000_000_000,
        "PB" => 1_000_000_000_000_000,
        "KiB" => 1_024,
        "MiB" => 1_024 * 1_024,
        "GiB" => 1_024 * 1_024 * 1_024,
        "TiB" => 1_024_u64.pow(4),
        "PiB" => 1_024_u64.pow(5),
        _ => return None,
    };
    Some(f)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tokens(input: &str) -> Vec<Token> {
        tokenize(input)
            .expect("tokenize")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn lexes_stream_selector() {
        assert_eq!(
            tokens(r#"{job="api", env!="prod", pod=~"web-.*", ns!~"kube.*"}"#),
            vec![
                Token::LBrace,
                Token::Ident("job".into()),
                Token::Eq,
                Token::String("api".into()),
                Token::Comma,
                Token::Ident("env".into()),
                Token::Neq,
                Token::String("prod".into()),
                Token::Comma,
                Token::Ident("pod".into()),
                Token::Re,
                Token::String("web-.*".into()),
                Token::Comma,
                Token::Ident("ns".into()),
                Token::Nre,
                Token::String("kube.*".into()),
                Token::RBrace,
            ]
        );
    }

    #[test]
    fn lexes_line_filters_and_pipeline() {
        assert_eq!(
            tokens(
                r#"{app="foo"} |= "error" != "timeout" |~ `exc.*` | json | line_format "{{.msg}}""#
            ),
            vec![
                Token::LBrace,
                Token::Ident("app".into()),
                Token::Eq,
                Token::String("foo".into()),
                Token::RBrace,
                Token::PipeExact,
                Token::String("error".into()),
                Token::Neq,
                Token::String("timeout".into()),
                Token::PipeRegex,
                Token::String("exc.*".into()),
                Token::Pipe,
                Token::Ident("json".into()),
                Token::Pipe,
                Token::Ident("line_format".into()),
                Token::String("{{.msg}}".into()),
            ]
        );
    }

    #[test]
    fn lexes_metric_query() {
        assert_eq!(
            tokens(r#"sum by (level) (rate({app="foo"}[5m]))"#),
            vec![
                Token::Ident("sum".into()),
                Token::By,
                Token::LParen,
                Token::Ident("level".into()),
                Token::RParen,
                Token::LParen,
                Token::Ident("rate".into()),
                Token::LParen,
                Token::LBrace,
                Token::Ident("app".into()),
                Token::Eq,
                Token::String("foo".into()),
                Token::RBrace,
                Token::LBracket,
                Token::Duration(Duration::from_secs(300)),
                Token::RBracket,
                Token::RParen,
                Token::RParen,
            ]
        );
    }

    #[test]
    fn lexes_unwrap_and_comparison() {
        assert_eq!(
            tokens(
                r#"{app="foo"} | logfmt | unwrap duration_ms | __error__ = "" and latency > 1.5"#
            ),
            vec![
                Token::LBrace,
                Token::Ident("app".into()),
                Token::Eq,
                Token::String("foo".into()),
                Token::RBrace,
                Token::Pipe,
                Token::Ident("logfmt".into()),
                Token::Pipe,
                Token::Unwrap,
                Token::Ident("duration_ms".into()),
                Token::Pipe,
                Token::Ident("__error__".into()),
                Token::Eq,
                Token::String("".into()),
                Token::And,
                Token::Ident("latency".into()),
                Token::Gt,
                Token::Number(1.5),
            ]
        );
    }

    #[test]
    fn lexes_compound_and_fractional_durations() {
        assert_eq!(
            tokens("[1h30m]"),
            vec![
                Token::LBracket,
                Token::Duration(Duration::from_secs(5400)),
                Token::RBracket,
            ]
        );
        assert_eq!(
            tokens("[1.5h]"),
            vec![
                Token::LBracket,
                Token::Duration(Duration::from_secs(5400)),
                Token::RBracket,
            ]
        );
        assert_eq!(
            tokens("[250ms]"),
            vec![
                Token::LBracket,
                Token::Duration(Duration::from_millis(250)),
                Token::RBracket,
            ]
        );
    }

    #[test]
    fn lexes_numbers_including_scientific_notation() {
        assert_eq!(tokens("42"), vec![Token::Number(42.0)]);
        assert_eq!(tokens("0.25"), vec![Token::Number(0.25)]);
        assert_eq!(tokens("1e3"), vec![Token::Number(1000.0)]);
        assert_eq!(tokens("2.5e-2"), vec![Token::Number(0.025)]);
    }

    #[test]
    fn lexes_bytes_literals() {
        assert_eq!(tokens("20KB"), vec![Token::Bytes(20_000)]);
        assert_eq!(tokens("5MB"), vec![Token::Bytes(5_000_000)]);
        assert_eq!(tokens("1GB"), vec![Token::Bytes(1_000_000_000)]);
        assert_eq!(tokens("1KiB"), vec![Token::Bytes(1_024)]);
        assert_eq!(tokens("2MiB"), vec![Token::Bytes(2 * 1_024 * 1_024)]);
        assert_eq!(tokens("512B"), vec![Token::Bytes(512)]);
        // Bytes and durations are distinguished by unit.
        assert_eq!(
            tokens("20ms"),
            vec![Token::Duration(Duration::from_millis(20))]
        );
    }

    #[test]
    fn lexes_parser_stage_flags() {
        assert_eq!(
            tokens("logfmt --strict --keep-empty"),
            vec![
                Token::Ident("logfmt".into()),
                Token::Flag("strict".into()),
                Token::Flag("keep-empty".into()),
            ]
        );
        // A lone '-' is still subtraction.
        assert_eq!(
            tokens("5 - 2"),
            vec![Token::Number(5.0), Token::Sub, Token::Number(2.0),]
        );
    }

    #[test]
    fn lexes_arithmetic_and_comparison_operators() {
        assert_eq!(
            tokens("+ - * / % ^ == >= <= > < = != =~ !~ |= |~ |"),
            vec![
                Token::Add,
                Token::Sub,
                Token::Mul,
                Token::Div,
                Token::Mod,
                Token::Pow,
                Token::CmpEq,
                Token::Gte,
                Token::Lte,
                Token::Gt,
                Token::Lt,
                Token::Eq,
                Token::Neq,
                Token::Re,
                Token::Nre,
                Token::PipeExact,
                Token::PipeRegex,
                Token::Pipe,
            ]
        );
    }

    #[test]
    fn lexes_string_escapes() {
        assert_eq!(
            tokens(r#""a\"b\\c\n\tA""#),
            vec![Token::String("a\"b\\c\n\tA".into())]
        );
    }

    #[test]
    fn raw_strings_keep_backslashes() {
        assert_eq!(
            tokens(r"`\d+(\.\d+)?`"),
            vec![Token::String(r"\d+(\.\d+)?".into())]
        );
    }

    #[test]
    fn skips_comments_and_whitespace() {
        assert_eq!(
            tokens("{a=\"b\"} # trailing comment\n | json"),
            vec![
                Token::LBrace,
                Token::Ident("a".into()),
                Token::Eq,
                Token::String("b".into()),
                Token::RBrace,
                Token::Pipe,
                Token::Ident("json".into()),
            ]
        );
    }

    #[test]
    fn reports_position_of_tokens_and_errors() {
        let spanned = tokenize("{job=\"a\"}\n  |= @").unwrap_err();
        assert_eq!(spanned.line, 2);
        assert_eq!(spanned.col, 6);
        assert!(spanned.to_string().contains("line 2, column 6"));

        let ok = tokenize("{job=\"a\"}\n  |= \"x\"").unwrap();
        let pipe = ok.iter().find(|t| t.token == Token::PipeExact).unwrap();
        assert_eq!((pipe.line, pipe.col), (2, 3));
    }

    #[test]
    fn rejects_malformed_input() {
        assert!(tokenize(r#"{job="unterminated"#).is_err());
        assert!(tokenize(r"`unterminated").is_err());
        assert!(tokenize(r#""bad \q escape""#).is_err());
        assert!(tokenize("!x").is_err());
        assert!(tokenize("5x").is_err());
        assert!(tokenize("1.").is_err());
        assert!(tokenize("@").is_err());
    }

    #[test]
    fn keywords_are_case_sensitive() {
        assert_eq!(
            tokens("by BY By"),
            vec![
                Token::By,
                Token::Ident("BY".into()),
                Token::Ident("By".into()),
            ]
        );
    }
}
