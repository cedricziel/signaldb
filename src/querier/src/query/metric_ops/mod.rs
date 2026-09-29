//! Windowed metric operators: range functions, histogram math and their aggregate UDFs.

#![cfg_attr(not(test), expect(dead_code, reason = "not wired into a planner yet"))]

pub mod exp_histogram;
pub mod hist;
pub mod hist_math;
mod hist_state;
pub mod instants;
pub mod range;
pub mod range_math;
