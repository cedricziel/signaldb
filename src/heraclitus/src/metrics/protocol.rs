use crate::error::Result;
use prometheus::{HistogramVec, IntCounterVec, Opts, Registry};

pub struct ProtocolMetrics {
    pub request_count: IntCounterVec,
    pub request_duration: HistogramVec,
    pub request_errors: IntCounterVec,
}

impl ProtocolMetrics {
    pub fn new(registry: &Registry, prefix: &str) -> Result<Self> {
        let request_count = IntCounterVec::new(
            Opts::new("requests_total", "Total number of Kafka protocol requests")
                .namespace(prefix),
            &["api_key", "api_name", "api_version"],
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create request_count metric: {e}"
            ))
        })?;

        let request_duration = HistogramVec::new(
            prometheus::HistogramOpts::new(
                "request_duration_seconds",
                "Request processing duration in seconds",
            )
            .namespace(prefix)
            .buckets(vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0]),
            &["api_key", "api_name", "api_version"],
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create request_duration metric: {e}"
            ))
        })?;

        let request_errors = IntCounterVec::new(
            Opts::new("request_errors_total", "Total number of request errors").namespace(prefix),
            &["api_key", "api_name", "error_code"],
        )
        .map_err(|e| {
            crate::error::HeraclitusError::Initialization(format!(
                "Failed to create request_errors metric: {e}"
            ))
        })?;

        // Register all metrics
        registry
            .register(Box::new(request_count.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register request_count: {e}"
                ))
            })?;
        registry
            .register(Box::new(request_duration.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register request_duration: {e}"
                ))
            })?;
        registry
            .register(Box::new(request_errors.clone()))
            .map_err(|e| {
                crate::error::HeraclitusError::Initialization(format!(
                    "Failed to register request_errors: {e}"
                ))
            })?;

        Ok(Self {
            request_count,
            request_duration,
            request_errors,
        })
    }

    /// Get the API name for a given API key
    pub fn api_name(api_key: i16) -> &'static str {
        match api_key {
            0 => "Produce",
            1 => "Fetch",
            2 => "ListOffsets",
            3 => "Metadata",
            8 => "OffsetCommit",
            9 => "OffsetFetch",
            10 => "FindCoordinator",
            11 => "JoinGroup",
            12 => "Heartbeat",
            13 => "LeaveGroup",
            14 => "SyncGroup",
            17 => "SaslHandshake",
            18 => "ApiVersions",
            36 => "SaslAuthenticate",
            _ => "Unknown",
        }
    }
}
