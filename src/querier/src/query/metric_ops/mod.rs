//! Windowed metric operators: range functions, histogram math and their aggregate UDFs.

#![cfg_attr(not(test), expect(dead_code, reason = "not wired into a planner yet"))]

pub mod exp_histogram;
#[cfg(test)]
pub(crate) mod fixtures;
pub mod hist;
pub mod hist_math;
pub(crate) mod hist_plan;
mod hist_state;
pub mod instants;
pub mod range;
pub mod range_math;
