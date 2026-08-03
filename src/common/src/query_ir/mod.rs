//! # Query IR — the versioned, structured query contract
//!
//! SignalDB's native query surface. A [`Document`] is a **versioned** JSON
//! query over a **registered source** with a defined type system: value types
//! and coercion ([`value`]), three-valued absent semantics ([`value::Truth`]),
//! relation types that flow between stages ([`relation`]), a shared predicate
//! grammar ([`predicate`]), structured stage operands ([`stage`]), and
//! registry-mediated field resolution ([`resolver`]).
//!
//! The IR's meaning is defined by this type system and its denotational
//! semantics — **not** by the DataFusion plan it lowers to. The plan (built in
//! the `querier` crate) is correct iff it evaluates to this denotation, so the
//! contract is stable across DataFusion upgrades and is a sound lowering target
//! for later sibling changes (correlate, structural traces, metrics) and future
//! front-ends.
//!
//! See `openspec/changes/query-ir-core` for the full specification.

pub mod document;
pub mod predicate;
pub mod relation;
pub mod resolver;
pub mod source;
pub mod stage;
pub mod validate;
pub mod value;
pub mod version;

pub use document::{Document, Range, ResultEnvelope};
pub use predicate::{ComparisonOp, Leaf, Predicate, Record};
pub use relation::{Column, Grain, RelationType, RowSet, Series};
pub use resolver::{FieldResolver, InMemoryResolver, Resolved};
pub use source::{SourceDef, SourceRegistry};
pub use stage::{
    Agg, AggFn, Aggregate, DerivedField, Direction, Extract, Order, Parser, Rank, Stage,
};
pub use validate::{IrError, Validated, parse_document, validate};
pub use value::{
    CoercionError, Literal, RelativeTime, TimestampLiteral, Truth, ValueType, coerce,
    parse_duration_ns, parse_relative_time, parse_timestamp_literal,
};
pub use version::{MAX_IR_VERSION, MIN_IR_VERSION, OperatorRegistry, is_supported};
