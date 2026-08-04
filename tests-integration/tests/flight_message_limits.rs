//! # Flight gRPC Message-Size Limit Tests
//!
//! Regression tests for issue #944: without explicit limits, tonic's 4 MiB
//! receive default applies to every Flight endpoint. One OTLP export becomes
//! one FlightData message on the acceptor → writer `do_put` path, and Arrow
//! encoding inflates OTLP, so a ~4 MB export could exceed the default —
//! `do_put` then fails deterministically and the WAL entry retries the same
//! oversized batch forever.
//!
//! These tests run a minimal in-process Flight server and verify:
//! - a >4 MiB FlightData message is rejected by a default-configured server
//!   (the wedge), and
//! - the same payload round-trips once server and client apply
//!   `common::flight::MAX_GRPC_MESSAGE_SIZE`.

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    Action, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use common::flight::MAX_GRPC_MESSAGE_SIZE;
use datafusion::arrow::array::{ArrayRef, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint, Server};
use tonic::{Request, Response, Status, Streaming};

/// Minimal Flight service that decodes `do_put` payloads and counts the
/// rows it received, so tests can assert the payload survived the wire.
#[derive(Clone, Default)]
struct RowCountingFlightService {
    rows_received: Arc<AtomicUsize>,
    messages_received: Arc<AtomicUsize>,
}

#[tonic::async_trait]
impl FlightService for RowCountingFlightService {
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
        _request: Request<Criteria>,
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
    ) -> Result<Response<PollInfo>, Status> {
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
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let stream = request
            .into_inner()
            .map_err(arrow_flight::error::FlightError::from);
        let mut batches = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(
            stream.inspect_ok({
                let messages = self.messages_received.clone();
                move |_| {
                    messages.fetch_add(1, Ordering::SeqCst);
                }
            }),
        );
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|e| Status::internal(format!("decode failed: {e}")))?;
            self.rows_received
                .fetch_add(batch.num_rows(), Ordering::SeqCst);
        }
        Ok(Response::new(Box::pin(stream::once(async {
            Ok(PutResult::default())
        }))))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not implemented"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not implemented"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("list_actions not implemented"))
    }
}

/// Start an in-process Flight server; when `with_limits` is set, apply the
/// production message-size limits exactly like the real services do.
async fn start_server(
    service: RowCountingFlightService,
    with_limits: bool,
) -> anyhow::Result<SocketAddr> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpListenerStream::new(listener);

    let mut server = FlightServiceServer::new(service);
    if with_limits {
        server = server
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE);
    }
    tokio::spawn(async move {
        if let Err(e) = Server::builder()
            .add_service(server)
            .serve_with_incoming(incoming)
            .await
        {
            eprintln!("test Flight server exited with error: {e}");
        }
    });
    Ok(addr)
}

async fn connect(
    addr: SocketAddr,
    with_limits: bool,
) -> anyhow::Result<FlightServiceClient<Channel>> {
    let channel = Endpoint::from_shared(format!("http://{addr}"))?
        .connect()
        .await?;
    let client = FlightServiceClient::new(channel);
    Ok(if with_limits {
        client
            .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
    } else {
        client
    })
}

/// A batch of `rows` rows, each carrying ~2 KiB of string payload plus an
/// i64 sequence number.
fn synthetic_batch(rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("seq", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
    ]));
    let seq: ArrayRef = Arc::new(Int64Array::from_iter_values(0..rows as i64));
    let payload: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..rows).map(|_| "x".repeat(2048)),
    ));
    RecordBatch::try_new(schema, vec![seq, payload]).expect("valid batch")
}

/// A batch whose single FlightData message encodes to well over tonic's
/// 4 MiB default: 4096 rows x 2 KiB payload = ~8 MiB.
fn oversized_batch() -> RecordBatch {
    synthetic_batch(4096)
}

/// Send FlightData via do_put and drain the response stream; an error at
/// either stage counts as a failed put (tonic may surface a mid-stream
/// rejection on the response stream rather than the initial call).
async fn do_put_all(
    client: &mut FlightServiceClient<Channel>,
    flight_data: Vec<FlightData>,
) -> Result<(), tonic::Status> {
    let response = client.do_put(stream::iter(flight_data)).await?;
    let mut response_stream = response.into_inner();
    while let Some(result) = response_stream.next().await {
        result?;
    }
    Ok(())
}

/// The failure mode behind issue #944: against tonic's defaults, a single
/// >4 MiB FlightData message can never be delivered — retrying it (as the
/// WAL retry loop does) can only fail again.
#[tokio::test]
async fn oversized_message_is_rejected_by_default_limits() -> anyhow::Result<()> {
    let service = RowCountingFlightService::default();
    let addr = start_server(service.clone(), false).await?;
    let mut client = connect(addr, false).await?;

    let batch = oversized_batch();
    let flight_data = batches_to_flight_data(&batch.schema(), vec![batch])?;

    let result = do_put_all(&mut client, flight_data).await;

    assert!(
        result.is_err(),
        "a >4 MiB message must exceed tonic's default receive limit; \
         if this starts passing, the default changed and the regression \
         premise should be re-checked"
    );
    assert_eq!(service.rows_received.load(Ordering::SeqCst), 0);
    Ok(())
}

/// With `MAX_GRPC_MESSAGE_SIZE` applied on both ends (as every SignalDB
/// Flight server and pooled client now does), the same oversized message
/// round-trips.
#[tokio::test]
async fn oversized_message_round_trips_with_explicit_limits() -> anyhow::Result<()> {
    let service = RowCountingFlightService::default();
    let addr = start_server(service.clone(), true).await?;
    let mut client = connect(addr, true).await?;

    let batch = oversized_batch();
    let expected_rows = batch.num_rows();
    let flight_data = batches_to_flight_data(&batch.schema(), vec![batch])?;

    do_put_all(&mut client, flight_data)
        .await
        .map_err(|e| anyhow::anyhow!("do_put failed despite explicit limits: {e}"))?;

    assert_eq!(service.rows_received.load(Ordering::SeqCst), expected_rows);
    Ok(())
}
