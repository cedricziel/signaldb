//! Classification of OTLP ingest-handler failures.
//!
//! Every OTLP handler (`otlp_grpc`, `otlp_log_handler`,
//! `otlp_metrics_handler`, `otlp_profiles_handler`) used to return a bare
//! `anyhow::Result<()>`, which the gRPC services and the OTLP/HTTP dispatch
//! helper flattened into a single retryable status
//! (`UNAVAILABLE`/`503`) regardless of *why* the export failed. A payload
//! that deterministically fails OTLP→Arrow conversion would then be
//! retried forever by a well-behaved client, backing up its export queue
//! on data that can never succeed.
//!
//! [`IngestError`] splits the two cases the transport layer actually needs
//! to distinguish: a malformed payload (retrying without a client-side fix
//! is pointless) versus a backend/durability hiccup (retrying may
//! succeed).

use std::fmt;

/// An OTLP ingest handler failure, classified by whether retrying the same
/// bytes without a client-side change could succeed.
#[derive(Debug)]
pub enum IngestError {
    /// The payload could not be processed — most commonly an OTLP→Arrow
    /// conversion failure. Deterministic: maps to `400 Bad Request` /
    /// `INVALID_ARGUMENT`.
    Invalid(anyhow::Error),
    /// A backend/durability failure: the WAL could not be opened, written,
    /// flushed, or the record batch could not be serialized. Transient:
    /// maps to `503 Service Unavailable` / `UNAVAILABLE`.
    Unavailable(anyhow::Error),
}

impl fmt::Display for IngestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(e) | Self::Unavailable(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for IngestError {}
