//! Shared fixtures for the Flight unit tests.

use arrow_flight::FlightData;
use datafusion::arrow::array::{Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use futures::StreamExt;
use std::sync::{Arc, Mutex};

/// A 64-row batch with a nullable JSON-ish string column, the shape the
/// acceptor ingests.
pub(crate) fn batch_with_nulls() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let ids = Int64Array::from_iter_values(0..64);
    let bodies: StringArray = (0..64)
        .map(|i| (i % 3 != 0).then(|| format!("{{\"resource\":\"checkout\",\"n\":{i}}}")))
        .collect();
    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(bodies)]).unwrap()
}

/// Bare-minimum `FlightService` that answers every RPC with
/// `unimplemented` — enough to let a client complete its connection
/// handshake against a real listener. With `received` set, `do_put`
/// instead accepts the stream and records every `FlightData` message.
#[derive(Clone, Default)]
pub(crate) struct NoopFlightService {
    pub(crate) received: Option<Arc<Mutex<Vec<FlightData>>>>,
}

#[tonic::async_trait]
impl arrow_flight::flight_service_server::FlightService for NoopFlightService {
    type HandshakeStream =
        futures::stream::BoxStream<'static, Result<arrow_flight::HandshakeResponse, tonic::Status>>;
    type ListFlightsStream =
        futures::stream::BoxStream<'static, Result<arrow_flight::FlightInfo, tonic::Status>>;
    type DoGetStream =
        futures::stream::BoxStream<'static, Result<arrow_flight::FlightData, tonic::Status>>;
    type DoPutStream =
        futures::stream::BoxStream<'static, Result<arrow_flight::PutResult, tonic::Status>>;
    type DoExchangeStream =
        futures::stream::BoxStream<'static, Result<arrow_flight::FlightData, tonic::Status>>;
    type DoActionStream =
        futures::stream::BoxStream<'static, Result<arrow_flight::Result, tonic::Status>>;
    type ListActionsStream =
        futures::stream::BoxStream<'static, Result<arrow_flight::ActionType, tonic::Status>>;

    async fn handshake(
        &self,
        _request: tonic::Request<tonic::Streaming<arrow_flight::HandshakeRequest>>,
    ) -> Result<tonic::Response<Self::HandshakeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("handshake"))
    }
    async fn list_flights(
        &self,
        _request: tonic::Request<arrow_flight::Criteria>,
    ) -> Result<tonic::Response<Self::ListFlightsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("list_flights"))
    }
    async fn get_flight_info(
        &self,
        _request: tonic::Request<arrow_flight::FlightDescriptor>,
    ) -> Result<tonic::Response<arrow_flight::FlightInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_flight_info"))
    }
    async fn poll_flight_info(
        &self,
        _request: tonic::Request<arrow_flight::FlightDescriptor>,
    ) -> Result<tonic::Response<arrow_flight::PollInfo>, tonic::Status> {
        Err(tonic::Status::unimplemented("poll_flight_info"))
    }
    async fn get_schema(
        &self,
        _request: tonic::Request<arrow_flight::FlightDescriptor>,
    ) -> Result<tonic::Response<arrow_flight::SchemaResult>, tonic::Status> {
        Err(tonic::Status::unimplemented("get_schema"))
    }
    async fn do_get(
        &self,
        _request: tonic::Request<arrow_flight::Ticket>,
    ) -> Result<tonic::Response<Self::DoGetStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_get"))
    }
    async fn do_put(
        &self,
        request: tonic::Request<tonic::Streaming<arrow_flight::FlightData>>,
    ) -> Result<tonic::Response<Self::DoPutStream>, tonic::Status> {
        let Some(received) = &self.received else {
            return Err(tonic::Status::unimplemented("do_put"));
        };
        let mut stream = request.into_inner();
        while let Some(message) = stream.next().await {
            received.lock().unwrap().push(message?);
        }
        Ok(tonic::Response::new(futures::stream::empty().boxed()))
    }
    async fn do_exchange(
        &self,
        _request: tonic::Request<tonic::Streaming<arrow_flight::FlightData>>,
    ) -> Result<tonic::Response<Self::DoExchangeStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_exchange"))
    }
    async fn do_action(
        &self,
        _request: tonic::Request<arrow_flight::Action>,
    ) -> Result<tonic::Response<Self::DoActionStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("do_action"))
    }
    async fn list_actions(
        &self,
        _request: tonic::Request<arrow_flight::Empty>,
    ) -> Result<tonic::Response<Self::ListActionsStream>, tonic::Status> {
        Err(tonic::Status::unimplemented("list_actions"))
    }
}
