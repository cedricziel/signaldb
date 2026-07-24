//! # LogQL Tokens
//!
//! Token type produced by the [`crate::lexer`]. Function names
//! (`rate`, `count_over_time`, `sum`, `json`, `line_format`, ...) are
//! deliberately lexed as [`Token::Ident`] — LogQL treats them as
//! context-dependent identifiers, so the parser resolves them where the
//! grammar expects a function or parser stage. Only words that are
//! reserved in the grammar itself become keyword tokens.

use std::time::Duration;

/// A single LogQL token.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Literals
    /// Double-quoted (escaped) or backtick-quoted (raw) string.
    String(String),
    /// Numeric literal, including floats and scientific notation.
    Number(f64),
    /// Duration literal such as `5m`, `1h30m`, or `1.5h`.
    Duration(Duration),
    /// Identifier: label names, function names, parser stage names.
    Ident(String),

    // Label matchers / equality
    /// `=`
    Eq,
    /// `!=` (label matcher or line filter, depending on position)
    Neq,
    /// `=~`
    Re,
    /// `!~` (label matcher or line filter, depending on position)
    Nre,

    // Pipeline
    /// `|=`
    PipeExact,
    /// `|~`
    PipeRegex,
    /// `|`
    Pipe,

    // Comparison (metric queries and label filter expressions)
    /// `==`
    CmpEq,
    /// `>`
    Gt,
    /// `>=`
    Gte,
    /// `<`
    Lt,
    /// `<=`
    Lte,

    // Arithmetic
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `^`
    Pow,

    // Brackets
    /// `{`
    LBrace,
    /// `}`
    RBrace,
    /// `(`
    LParen,
    /// `)`
    RParen,
    /// `[`
    LBracket,
    /// `]`
    RBracket,

    // Punctuation
    /// `,`
    Comma,

    // Keywords (reserved words in the LogQL grammar)
    /// `by`
    By,
    /// `without`
    Without,
    /// `on`
    On,
    /// `ignoring`
    Ignoring,
    /// `group_left`
    GroupLeft,
    /// `group_right`
    GroupRight,
    /// `unwrap`
    Unwrap,
    /// `offset`
    Offset,
    /// `bool`
    Bool,
    /// `and`
    And,
    /// `or`
    Or,
    /// `unless`
    Unless,
}

impl Token {
    /// Map a lexed word to its keyword token, if it is one.
    pub(crate) fn keyword(word: &str) -> Option<Token> {
        Some(match word {
            "by" => Token::By,
            "without" => Token::Without,
            "on" => Token::On,
            "ignoring" => Token::Ignoring,
            "group_left" => Token::GroupLeft,
            "group_right" => Token::GroupRight,
            "unwrap" => Token::Unwrap,
            "offset" => Token::Offset,
            "bool" => Token::Bool,
            "and" => Token::And,
            "or" => Token::Or,
            "unless" => Token::Unless,
            _ => return None,
        })
    }
}

impl std::fmt::Display for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Token::String(s) => write!(f, "{s:?}"),
            Token::Number(n) => write!(f, "{n}"),
            Token::Duration(d) => write!(f, "{d:?}"),
            Token::Ident(i) => write!(f, "{i}"),
            Token::Eq => f.write_str("="),
            Token::Neq => f.write_str("!="),
            Token::Re => f.write_str("=~"),
            Token::Nre => f.write_str("!~"),
            Token::PipeExact => f.write_str("|="),
            Token::PipeRegex => f.write_str("|~"),
            Token::Pipe => f.write_str("|"),
            Token::CmpEq => f.write_str("=="),
            Token::Gt => f.write_str(">"),
            Token::Gte => f.write_str(">="),
            Token::Lt => f.write_str("<"),
            Token::Lte => f.write_str("<="),
            Token::Add => f.write_str("+"),
            Token::Sub => f.write_str("-"),
            Token::Mul => f.write_str("*"),
            Token::Div => f.write_str("/"),
            Token::Mod => f.write_str("%"),
            Token::Pow => f.write_str("^"),
            Token::LBrace => f.write_str("{"),
            Token::RBrace => f.write_str("}"),
            Token::LParen => f.write_str("("),
            Token::RParen => f.write_str(")"),
            Token::LBracket => f.write_str("["),
            Token::RBracket => f.write_str("]"),
            Token::Comma => f.write_str(","),
            Token::By => f.write_str("by"),
            Token::Without => f.write_str("without"),
            Token::On => f.write_str("on"),
            Token::Ignoring => f.write_str("ignoring"),
            Token::GroupLeft => f.write_str("group_left"),
            Token::GroupRight => f.write_str("group_right"),
            Token::Unwrap => f.write_str("unwrap"),
            Token::Offset => f.write_str("offset"),
            Token::Bool => f.write_str("bool"),
            Token::And => f.write_str("and"),
            Token::Or => f.write_str("or"),
            Token::Unless => f.write_str("unless"),
        }
    }
}

/// A token with its 1-based source position (position of the token's
/// first character).
#[derive(Debug, Clone, PartialEq)]
pub struct SpannedToken {
    pub token: Token,
    pub line: u32,
    pub col: u32,
}
