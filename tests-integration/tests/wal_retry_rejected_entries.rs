//! # Acceptor WAL Retry: Rejected-Entry Handling
//!
//! Regression tests for issue #1060. The retry consumer treated every forward
//! failure as "the writer may be down" and `break`ed out of the WAL's entire
//! entry loop. On hive that let a single batch the writer refuses
//! deterministically — a `metrics_sum` batch whose non-nullable `value` column
//! contained nulls — shadow ~101 500 entries queued behind it. The WAL never
//! drained, and the poison handling added in #1059 was never reached because
//! the pass stopped before walking to it.
//!
//! The distinction that matters is *why* the forward failed:
//! - the writer refused the batch → deterministic, retrying cannot help
//! - the writer was unreachable → transient, retrying is the whole point
//!
//! These tests pin both halves against a real Flight server, because the
//! classification depends on the tonic status surviving the wire.

use acceptor::handler::{WalManager, WalRetryConsumer};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use common::catalog::Catalog;
use common::flight::transport::{InMemoryFlightTransport, ServiceCapability};
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::{WalConfig, WalOperation, record_batch_to_bytes};
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

/// Flight server that fails every `do_put` with a fixed status, standing in
/// for a writer that either refuses batches or is unreachable.
#[derive(Clone)]
struct FailingFlightService {
    code: tonic::Code,
    message: &'static str,
}

#[tonic::async_trait]
impl FlightService for FailingFlightService {
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
        Err(Status::unimplemented("handshake"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("list_flights"))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::new(self.code, self.message))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions"))
    }
}

fn wal_config(base_dir: &Path) -> WalConfig {
    let mut config = WalConfig::with_defaults(base_dir.to_path_buf());
    config.max_segment_size = 1024 * 1024;
    config.max_buffer_entries = 100;
    config.flush_interval_secs = 3600;
    config
}

fn test_manager(base_dir: &Path) -> WalManager {
    WalManager::new(
        wal_config(base_dir),
        wal_config(base_dir),
        wal_config(base_dir),
        wal_config(base_dir),
    )
}

/// A valid Arrow IPC payload, so entries reach the forward stage rather than
/// being retired by the deserialization path added in #1059.
fn valid_payload(value: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let column: ArrayRef = Arc::new(Int64Array::from(vec![value]));
    let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
    record_batch_to_bytes(&batch).unwrap()
}

/// Serve `service` and register it as a Storage-capable writer, returning a
/// transport that discovers it.
async fn transport_to(service: FailingFlightService) -> Arc<InMemoryFlightTransport> {
    let temp = TempDir::new().unwrap();
    let catalog_dsn = format!("sqlite://{}?mode=rwc", temp.path().join("cat.db").display());
    let catalog = Catalog::new(&catalog_dsn).await.unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            // The real writer's builder, so the stub negotiates compression
            // identically to production.
            .add_service(common::flight::flight_service_server(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .ok();
    });

    catalog
        .register_ingester(
            Uuid::new_v4(),
            &addr.to_string(),
            ServiceType::Writer,
            &[ServiceCapability::Storage],
        )
        .await
        .expect("writer registered");

    // Leak the temp dir: the catalog file must outlive this function.
    std::mem::forget(temp);

    let bootstrap =
        ServiceBootstrap::new_for_test_with_catalog(catalog, ServiceType::Acceptor, "127.0.0.1:0")
            .await
            .unwrap();
    Arc::new(InMemoryFlightTransport::new(bootstrap))
}

async fn append_entries(manager: &WalManager, count: i64) {
    let wal = manager
        .get_wal("acme", "production", "metrics")
        .await
        .unwrap();
    for i in 0..count {
        wal.append(WalOperation::WriteMetrics, valid_payload(i), None)
            .await
            .unwrap();
    }
    wal.flush().await.unwrap();
}

#[tokio::test]
async fn rejected_entries_are_dead_lettered_and_the_pass_continues() {
    // The #1060 wedge: the writer refuses each batch deterministically. Every
    // entry must be retired, not just the first — a pass that stops at entry
    // one leaves the rest pinned forever.
    let temp_dir = TempDir::new().unwrap();
    let manager = Arc::new(test_manager(temp_dir.path()));
    append_entries(&manager, 3).await;

    let transport = transport_to(FailingFlightService {
        code: tonic::Code::InvalidArgument,
        message: "Schema transformation failed: Column 'value' is declared as non-nullable",
    })
    .await;

    let mut consumer = WalRetryConsumer::new(manager.clone(), transport)
        .with_timing(Duration::from_secs(1), Duration::from_secs(0));

    let stats = consumer.run_once().await.unwrap();

    assert_eq!(
        stats.dead_lettered, 3,
        "every rejected entry must be retired in a single pass, not just the first"
    );
    assert_eq!(stats.retried, 0);

    let wal = manager
        .get_wal("acme", "production", "metrics")
        .await
        .unwrap();
    assert!(
        wal.get_unprocessed_entries().await.unwrap().is_empty(),
        "rejected entries must stop pinning their segment"
    );

    // The payloads are intact and replayable, so they are preserved with a
    // reason marker rather than discarded.
    let dead_letter_dir = temp_dir
        .path()
        .join("acme")
        .join("production")
        .join("metrics")
        .join("dead-letter");
    let mut markers = 0;
    let mut dir = tokio::fs::read_dir(&dead_letter_dir).await.unwrap();
    while let Some(entry) = dir.next_entry().await.unwrap() {
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with(".rejected.json")
        {
            markers += 1;
        }
    }
    assert_eq!(
        markers, 3,
        "each rejected payload must record why it failed"
    );
}

#[tokio::test]
async fn unreachable_writer_still_stops_the_pass_without_discarding_anything() {
    // The other half, and the one that must not regress: when the writer is
    // merely down the entries are good. Retrying them later is the point of
    // the WAL, so nothing may be dead-lettered.
    let temp_dir = TempDir::new().unwrap();
    let manager = Arc::new(test_manager(temp_dir.path()));
    append_entries(&manager, 3).await;

    let transport = transport_to(FailingFlightService {
        code: tonic::Code::Unavailable,
        message: "connection refused",
    })
    .await;

    let mut consumer = WalRetryConsumer::new(manager.clone(), transport)
        .with_timing(Duration::from_secs(1), Duration::from_secs(0));

    let stats = consumer.run_once().await.unwrap();

    assert_eq!(
        stats.dead_lettered, 0,
        "a transient failure must never discard data"
    );
    assert_eq!(
        stats.failed, 1,
        "the pass should stop at the first failure rather than hammering a down writer"
    );

    let wal = manager
        .get_wal("acme", "production", "metrics")
        .await
        .unwrap();
    assert_eq!(
        wal.get_unprocessed_entries().await.unwrap().len(),
        3,
        "all entries must remain pending for a later retry"
    );
}
