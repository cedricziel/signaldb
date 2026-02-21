//! Compactor Flight Service
//!
//! Exposes Arrow Flight DoAction endpoints for on-demand compaction management
//! and operational visibility. Allows operators and automation to:
//!
//! - Trigger an immediate compaction cycle (`compact_now`)
//! - Inspect active distributed leases and metrics (`compact_status`)
//! - Dry-run planning without executing (`compact_dry_run`)
//!
//! ## Usage
//!
//! ```no_run
//! use std::sync::Arc;
//! use compactor::flight::CompactorFlightService;
//! use compactor::planner::{CompactionPlanner, PlannerConfig};
//! use compactor::executor::{CompactionExecutor, ExecutorConfig};
//! use compactor::lease::LeaseManager;
//! use compactor::metrics::CompactionMetrics;
//!
//! # async fn example() -> anyhow::Result<()> {
//! # let planner: Arc<CompactionPlanner> = todo!();
//! # let executor: Arc<CompactionExecutor> = todo!();
//! # let lease_manager: LeaseManager = todo!();
//! let service = CompactorFlightService::new(
//!     planner,
//!     executor,
//!     lease_manager,
//!     CompactionMetrics::new(),
//! );
//! # Ok(())
//! # }
//! ```

use crate::executor::CompactionExecutor;
use crate::lease::LeaseManager;
use crate::metrics::CompactionMetrics;
use crate::planner::CompactionPlanner;
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    PutResult, SchemaResult, Ticket, flight_service_server::FlightService,
};
use bytes::Bytes;
use futures::stream::{self, BoxStream};
use std::sync::Arc;
use tonic::{Request, Response, Status, Streaming};

/// JSON shape for lease entries in `compact_status` responses.
#[derive(serde::Serialize)]
struct ActiveLeaseInfo {
    tenant_id: String,
    dataset_id: String,
    table_name: String,
    partition_id: String,
    holder_id: String,
    expires_at: String,
}

/// JSON shape for candidates in `compact_dry_run` / `compact_now` responses.
#[derive(serde::Serialize)]
struct CandidateInfo {
    tenant_id: String,
    dataset_id: String,
    table_name: String,
    partition_id: String,
    file_count: usize,
    total_size_bytes: u64,
}

/// Arrow Flight service for the compactor, providing on-demand control actions.
///
/// All three `do_action` commands can be invoked concurrently with the
/// background compaction loop — distributed leases prevent duplicate work.
pub struct CompactorFlightService {
    planner: Arc<CompactionPlanner>,
    executor: Arc<CompactionExecutor>,
    lease_manager: LeaseManager,
    metrics: CompactionMetrics,
}

impl CompactorFlightService {
    /// Create a new `CompactorFlightService`.
    ///
    /// All arguments are cheap to clone (`Arc` or atomic counters).
    pub fn new(
        planner: Arc<CompactionPlanner>,
        executor: Arc<CompactionExecutor>,
        lease_manager: LeaseManager,
        metrics: CompactionMetrics,
    ) -> Self {
        Self {
            planner,
            executor,
            lease_manager,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl FlightService for CompactorFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<arrow_flight::ActionType, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("handshake not implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights not implemented"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not implemented"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get not implemented"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("do_put not implemented"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    /// Handle compactor management actions.
    ///
    /// | `action_type`      | Description |
    /// |--------------------|-------------|
    /// | `compact_now`      | Run a full plan → lease → execute cycle immediately |
    /// | `compact_status`   | Return active leases + cumulative metrics as JSON |
    /// | `compact_dry_run`  | Plan candidates without executing, return JSON list |
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();

        match action.r#type.as_str() {
            "compact_now" => {
                let candidates =
                    self.planner.plan().await.map_err(|e| {
                        Status::internal(format!("Compaction planning failed: {e:#}"))
                    })?;

                let total = candidates.len();
                let mut started = 0usize;
                let mut skipped = 0usize;

                for candidate in candidates {
                    match self.lease_manager.try_acquire_default(&candidate).await {
                        Ok(Some(lease)) => {
                            match self.executor.execute_candidate(candidate).await {
                                Ok(_) => started += 1,
                                Err(e) => {
                                    log::error!("compact_now execution failed: {e:#}");
                                }
                            }
                            if let Err(e) = self.lease_manager.release(&lease).await {
                                log::warn!("compact_now lease release failed: {e:#}");
                            }
                        }
                        Ok(None) => skipped += 1,
                        Err(e) => {
                            log::warn!("compact_now lease acquisition failed: {e:#}");
                            skipped += 1;
                        }
                    }
                }

                let body = serde_json::json!({
                    "candidates_found": total,
                    "jobs_started": started,
                    "jobs_skipped_leased": skipped,
                });
                let result = arrow_flight::Result {
                    body: Bytes::from(body.to_string()),
                };
                Ok(Response::new(Box::pin(stream::once(
                    async move { Ok(result) },
                ))))
            }

            "compact_status" => {
                let leases = self
                    .lease_manager
                    .list_active()
                    .await
                    .map_err(|e| Status::internal(format!("Failed to list leases: {e:#}")))?;

                let lease_infos: Vec<ActiveLeaseInfo> = leases
                    .into_iter()
                    .map(|l| ActiveLeaseInfo {
                        tenant_id: l.tenant_id,
                        dataset_id: l.dataset_id,
                        table_name: l.table_name,
                        partition_id: l.partition_id,
                        holder_id: l.holder_id,
                        expires_at: l.expires_at.to_rfc3339(),
                    })
                    .collect();

                let summary = self.metrics.summary();
                let body = serde_json::json!({
                    "active_leases": lease_infos,
                    "metrics": {
                        "jobs_started": summary.jobs_started,
                        "jobs_succeeded": summary.jobs_succeeded,
                        "jobs_failed": summary.jobs_failed,
                        "conflicts_detected": summary.conflicts_detected,
                        "total_input_files": summary.total_input_files,
                        "total_output_files": summary.total_output_files,
                        "bytes_before_compaction": summary.bytes_before_compaction,
                        "bytes_after_compaction": summary.bytes_after_compaction,
                        "compression_ratio": summary.compression_ratio,
                        "avg_duration_ms": summary.avg_duration_ms,
                    }
                });
                let result = arrow_flight::Result {
                    body: Bytes::from(body.to_string()),
                };
                Ok(Response::new(Box::pin(stream::once(
                    async move { Ok(result) },
                ))))
            }

            "compact_dry_run" => {
                let candidates =
                    self.planner.plan().await.map_err(|e| {
                        Status::internal(format!("Compaction planning failed: {e:#}"))
                    })?;

                let candidate_infos: Vec<CandidateInfo> = candidates
                    .into_iter()
                    .map(|c| CandidateInfo {
                        tenant_id: c.tenant_id,
                        dataset_id: c.dataset_id,
                        table_name: c.table_name,
                        partition_id: c.partition_id,
                        file_count: c.stats.file_count,
                        total_size_bytes: c.stats.total_size_bytes,
                    })
                    .collect();

                let body = serde_json::json!({ "candidates": candidate_infos });
                let result = arrow_flight::Result {
                    body: Bytes::from(body.to_string()),
                };
                Ok(Response::new(Box::pin(stream::once(
                    async move { Ok(result) },
                ))))
            }

            other => Err(Status::invalid_argument(format!(
                "Unknown action: {other:?}. Valid actions: compact_now, compact_status, compact_dry_run"
            ))),
        }
    }

    /// List available DoAction commands supported by this service.
    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            arrow_flight::ActionType {
                r#type: "compact_now".to_string(),
                description: "Trigger an immediate compaction cycle (plan → lease → execute)"
                    .to_string(),
            },
            arrow_flight::ActionType {
                r#type: "compact_status".to_string(),
                description: "Return active distributed leases and cumulative compaction metrics"
                    .to_string(),
            },
            arrow_flight::ActionType {
                r#type: "compact_dry_run".to_string(),
                description: "Plan compaction candidates without executing them".to_string(),
            },
        ];
        Ok(Response::new(Box::pin(stream::iter(
            actions.into_iter().map(Ok),
        ))))
    }
}
