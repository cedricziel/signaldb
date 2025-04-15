use std::sync::Arc;

use arrow_flight::flight_service_server::FlightService;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use bytes::Bytes;
use common::flight::schema::FlightSchemas;
use futures::stream::BoxStream;
use futures::{stream, Stream, StreamExt};
use tonic::{async_trait, Request, Response, Status, Streaming};

use crate::RouterState;

/// SignalDBFlightService is a Flight service implementation for SignalDB
pub struct SignalDBFlightService<S: RouterState> {
    state: S,
    schemas: FlightSchemas,
}

impl<S: RouterState> SignalDBFlightService<S> {
    /// Create a new SignalDBFlightService with the given state
    pub fn new(state: S) -> Self {
        Self {
            state,
            schemas: FlightSchemas::new(),
        }
    }
}

#[async_trait]
impl<S: RouterState> FlightService for SignalDBFlightService<S> {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // Simple handshake implementation - no authentication for now
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Bytes::new(),
        };

        let output = stream::once(async move { Ok(response) }).boxed();
        Ok(Response::new(output))
    }

    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // For now, return an empty list of flights
        let output = stream::empty().boxed();
        Ok(Response::new(output))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("get_flight_info is not implemented"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("poll_flight_info is not implemented"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("get_schema is not implemented"))
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("do_get is not implemented"))
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("do_put is not implemented"))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("do_exchange is not implemented"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        // Not implemented for now
        Err(Status::unimplemented("do_action is not implemented"))
    }

    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        // Return an empty list of actions for now
        let output = stream::empty().boxed();
        Ok(Response::new(output))
    }
}
