//! # Vector matching for the `binop` stage
//!
//! Combines two operands step by step (`bucket`), following Prometheus'
//! vector-matching rules. A Series operand is a frame of `bucket`,
//! `__labels` (canonical JSON) and `value`; a Scalar operand is `bucket` and
//! `value`, or a number.

#![expect(dead_code, reason = "not wired into a planner yet")]

mod eval;

const BUCKET: &str = "bucket";
const LABELS: &str = "__labels";
const VALUE: &str = "value";
