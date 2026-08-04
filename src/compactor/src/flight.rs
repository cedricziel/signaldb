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
        use tracing::Instrument;

        let metadata = request.metadata().clone();
        let action = request.into_inner();

        // RPC SERVER boundary span, named by the fully-qualified Flight
        // method plus the low-cardinality action type, joined to the
        // caller's trace context.
        let span = common::self_monitoring::spans::rpc_server_span(
            common::self_monitoring::spans::FLIGHT_DO_ACTION,
            Some(action.r#type.as_str()),
        );
        common::flight::trace_context::set_parent_from_metadata(&span, &metadata);

        let record_span = span.clone();
        let result = async move {
        let out: Result<Response<Self::DoActionStream>, Status> = match action.r#type.as_str() {
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
                            // Keep the lease alive for jobs longer than the TTL.
                            let renewal = self.lease_manager.spawn_renewal(lease.clone());
                            match self.executor.execute_candidate(candidate).await {
                                Ok(_) => started += 1,
                                Err(e) => {
                                    tracing::error!("compact_now execution failed: {e:#}");
                                }
                            }
                            drop(renewal);
                            if let Err(e) = self.lease_manager.release(&lease).await {
                                tracing::warn!("compact_now lease release failed: {e:#}");
                            }
                        }
                        Ok(None) => skipped += 1,
                        Err(e) => {
                            tracing::warn!("compact_now lease acquisition failed: {e:#}");
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
        };
        out
        }
        .instrument(span)
        .await;
        let code = result
            .as_ref()
            .err()
            .map(|s| s.code())
            .unwrap_or(tonic::Code::Ok);
        common::self_monitoring::spans::record_rpc_result(
            &record_span,
            common::self_monitoring::spans::RpcBoundary::Server,
            code,
        );
        result
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{CompactionExecutor, ExecutorConfig};
    use crate::lease::LeaseManager;
    use crate::metrics::CompactionMetrics;
    use crate::planner::{CompactionPlanner, PlannerConfig};
    use common::catalog::Catalog;
    use common::catalog_manager::CatalogManager;
    use std::time::Duration;
    use uuid::Uuid;

    async fn make_service() -> CompactorFlightService {
        let catalog_manager = Arc::new(CatalogManager::new_in_memory().await.unwrap());
        let planner = Arc::new(CompactionPlanner::new(
            catalog_manager.clone(),
            PlannerConfig {
                file_count_threshold: 10,
                min_input_file_size_bytes: 1024 * 1024,
                max_files_per_job: 50,
                target_file_size_bytes: 128 * 1024 * 1024,
            },
        ));
        let metrics = CompactionMetrics::new();
        let executor = Arc::new(CompactionExecutor::new(
            catalog_manager,
            ExecutorConfig::default(),
            metrics.clone(),
        ));
        let catalog = Arc::new(Catalog::new_in_memory().await.unwrap());
        let lease_manager = LeaseManager::new(catalog, Uuid::new_v4(), Duration::from_secs(300));
        CompactorFlightService::new(planner, executor, lease_manager, metrics)
    }

    /// `do_action` is an RPC boundary: it emits a semconv SERVER span named
    /// by the fully-qualified Flight method plus the low-cardinality action
    /// type.
    #[tokio::test]
    async fn do_action_emits_semconv_rpc_server_span() {
        use opentelemetry::trace::{SpanKind, Status as OtelStatus, TracerProvider as _};
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::prelude::*;

        let service = make_service().await;

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        async {
            let action = Action {
                r#type: "compact_dry_run".to_string(),
                body: Default::default(),
            };
            let result = service.do_action(Request::new(action)).await;
            assert!(result.is_ok(), "dry run should succeed");
        }
        .with_subscriber(subscriber)
        .await;

        provider.force_flush().unwrap();
        let spans = exporter.get_finished_spans().unwrap();
        let names: Vec<_> = spans.iter().map(|s| s.name.to_string()).collect();
        let span = spans
            .iter()
            .find(|s| {
                s.name
                    .starts_with("arrow.flight.protocol.FlightService/DoAction")
            })
            .unwrap_or_else(|| panic!("no RPC server span; exported = {names:?}"));

        assert_eq!(
            span.name,
            "arrow.flight.protocol.FlightService/DoAction compact_dry_run"
        );
        assert_eq!(span.span_kind, SpanKind::Server);
        let attr = |key: &str| {
            span.attributes
                .iter()
                .find(|kv| kv.key.as_str() == key)
                .map(|kv| kv.value.as_str().to_string())
        };
        assert_eq!(attr("rpc.system.name").as_deref(), Some("grpc"));
        assert_eq!(attr("rpc.response.status_code").as_deref(), Some("OK"));
        assert_eq!(span.status, OtelStatus::Unset);
    }
}
