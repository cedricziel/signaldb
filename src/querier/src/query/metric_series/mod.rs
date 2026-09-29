//! Metric Series planning (D11): the Series frame `(bucket, __labels, value)`
//! and the label-set UDFs its stages group, match and rewrite by.

#![cfg_attr(not(test), expect(dead_code, reason = "not wired into a planner yet"))]

pub mod labels;
pub(crate) mod vector_match;
