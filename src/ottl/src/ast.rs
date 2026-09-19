//! Abstract syntax tree produced by [`crate::parse`].

/// A single parsed OTTL statement: `editor(args...) [where condition]`.
#[derive(Debug, Clone, PartialEq)]
pub struct Statement {
    pub call: Call,
    pub condition: Option<Condition>,
}

/// A function call: either a top-level editor or a nested converter.
#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub name: String,
    pub args: Vec<Expr>,
}

/// One segment of a dotted/bracketed path.
#[derive(Debug, Clone, PartialEq)]
pub enum PathSegment {
    /// `.field`
    Field(String),
    /// `["key"]`
    Index(String),
}

/// A path such as `resource.attributes["k"]` or `span.name`.
#[derive(Debug, Clone, PartialEq)]
pub struct Path {
    pub segments: Vec<PathSegment>,
}

impl Path {
    /// The leading identifier (e.g. `span`, `resource`, `attributes`).
    pub fn root(&self) -> &str {
        match self.segments.first() {
            Some(PathSegment::Field(name)) => name,
            _ => "",
        }
    }
}

/// A literal value in source syntax.
#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    String(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Nil,
}

/// An expression: a call, list literal, literal, or path.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Call(Call),
    List(Vec<Expr>),
    Literal(Literal),
    Path(Path),
}

/// A comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A boolean condition used in `where` clauses.
#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    Or(Box<Condition>, Box<Condition>),
    And(Box<Condition>, Box<Condition>),
    Not(Box<Condition>),
    /// A bare expression used as a boolean (must evaluate to a bool).
    Bare(Expr),
    Compare(Expr, CmpOp, Expr),
}
