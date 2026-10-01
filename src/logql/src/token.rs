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
#[non_exhaustive]
pub enum Token {
    // Literals
    /// Double-quoted (escaped) or backtick-quoted (raw) string.
    String(String),
    /// Numeric literal, including floats and scientific notation.
    Number(f64),
    /// Duration literal such as `5m`, `1h30m`, or `1.5h`.
    Duration(Duration),
    /// Bytes literal such as `20KB`, `5MB`, or `1GiB`, in bytes.
    Bytes(u64),
    /// Identifier: label names, function names, parser stage names.
    Ident(String),
    /// A parser-stage flag such as `--strict` or `--keep-empty` (stored
    /// without the leading dashes).
    Flag(String),

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

    /// The source text of a keyword token, if the token is one. Inverse
    /// of [`Token::keyword`], used where the grammar allows keywords as
    /// plain identifiers (e.g. label names).
    pub(crate) fn keyword_text(token: &Token) -> Option<&'static str> {
        Some(match token {
            Token::By => "by",
            Token::Without => "without",
            Token::On => "on",
            Token::Ignoring => "ignoring",
            Token::GroupLeft => "group_left",
            Token::GroupRight => "group_right",
            Token::Unwrap => "unwrap",
            Token::Offset => "offset",
            Token::Bool => "bool",
            Token::And => "and",
            Token::Or => "or",
            Token::Unless => "unless",
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
            Token::Bytes(b) => write!(f, "{b}B"),
            Token::Ident(i) => write!(f, "{i}"),
            Token::Flag(name) => write!(f, "--{name}"),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every keyword the lexer recognises, paired with the token it produces.
    /// Written as one table so a keyword added to `keyword` without a matching
    /// `keyword_text` arm (or vice versa) fails the round-trip below.
    const KEYWORDS: &[(&str, Token)] = &[
        ("by", Token::By),
        ("without", Token::Without),
        ("on", Token::On),
        ("ignoring", Token::Ignoring),
        ("group_left", Token::GroupLeft),
        ("group_right", Token::GroupRight),
        ("unwrap", Token::Unwrap),
        ("offset", Token::Offset),
        ("bool", Token::Bool),
        ("and", Token::And),
        ("or", Token::Or),
        ("unless", Token::Unless),
    ];

    #[test]
    fn every_keyword_maps_to_its_token() {
        for (word, expected) in KEYWORDS {
            assert_eq!(
                Token::keyword(word).as_ref(),
                Some(expected),
                "keyword({word:?})"
            );
        }
    }

    /// `keyword_text` is documented as the inverse of `keyword`, and the
    /// grammar relies on it where a keyword may appear as a plain label name.
    /// An arm missing from either direction breaks that.
    #[test]
    fn keyword_text_inverts_keyword() {
        for (word, token) in KEYWORDS {
            assert_eq!(
                Token::keyword_text(token),
                Some(*word),
                "keyword_text({token:?})"
            );
        }
    }

    #[test]
    fn non_keywords_are_identifiers() {
        for word in ["service_name", "rate", "json", "By", "AND", ""] {
            assert_eq!(Token::keyword(word), None, "{word:?} is not a keyword");
        }
    }

    #[test]
    fn only_keyword_tokens_have_keyword_text() {
        for token in [
            Token::Eq,
            Token::Pipe,
            Token::LBrace,
            Token::Ident("service_name".to_string()),
            Token::Number(1.0),
        ] {
            assert_eq!(Token::keyword_text(&token), None, "{token:?}");
        }
    }

    /// Display is what error messages show a user, so every variant needs to
    /// render as the source text it was lexed from — not as a Debug dump.
    #[test]
    fn every_token_displays_as_its_source_text() {
        let cases: &[(Token, &str)] = &[
            (Token::Number(2.5), "2.5"),
            (Token::Bytes(1024), "1024B"),
            (Token::Ident("service_name".to_string()), "service_name"),
            (Token::Flag("strict".to_string()), "--strict"),
            (Token::Eq, "="),
            (Token::Neq, "!="),
            (Token::Re, "=~"),
            (Token::Nre, "!~"),
            (Token::PipeExact, "|="),
            (Token::PipeRegex, "|~"),
            (Token::Pipe, "|"),
            (Token::CmpEq, "=="),
            (Token::Gt, ">"),
            (Token::Gte, ">="),
            (Token::Lt, "<"),
            (Token::Lte, "<="),
            (Token::Add, "+"),
            (Token::Sub, "-"),
            (Token::Mul, "*"),
            (Token::Div, "/"),
            (Token::Mod, "%"),
            (Token::Pow, "^"),
            (Token::LBrace, "{"),
            (Token::RBrace, "}"),
            (Token::LParen, "("),
            (Token::RParen, ")"),
            (Token::LBracket, "["),
            (Token::RBracket, "]"),
            (Token::Comma, ","),
            (Token::By, "by"),
            (Token::Without, "without"),
            (Token::On, "on"),
            (Token::Ignoring, "ignoring"),
            (Token::GroupLeft, "group_left"),
            (Token::GroupRight, "group_right"),
            (Token::Unwrap, "unwrap"),
            (Token::Offset, "offset"),
            (Token::Bool, "bool"),
            (Token::And, "and"),
            (Token::Or, "or"),
            (Token::Unless, "unless"),
        ];
        for (token, expected) in cases {
            assert_eq!(&token.to_string(), expected, "Display for {token:?}");
        }
    }

    /// A string renders quoted so an error message can show where a literal
    /// began and ended; a duration renders in its Debug form, which is the
    /// spelled-out unit rather than a raw nanosecond count.
    #[test]
    fn strings_and_durations_render_readably() {
        assert_eq!(Token::String("api".to_string()).to_string(), "\"api\"");
        assert_eq!(
            Token::Duration(std::time::Duration::from_secs(300)).to_string(),
            "300s"
        );
    }
}
