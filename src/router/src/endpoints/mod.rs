pub mod admin;
pub mod api_error;
pub mod flight;
mod flight_decode;
pub mod logql;
pub mod management;
pub mod oauth;
pub mod ops;
pub mod promql;
pub mod pyroscope;
pub mod query;
pub mod schema;
pub mod session;
pub mod tempo;
pub mod tenant;

/// Current time as unix-epoch nanoseconds.
pub(crate) fn now_ns() -> i64 {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .unwrap_or_else(|| chrono::Utc::now().timestamp_millis() * 1_000_000)
}
