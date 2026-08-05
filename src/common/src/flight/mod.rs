//! # Flight
//!
//! Shared Apache Arrow Flight infrastructure: wire schemas, OTLP
//! conversions, per-request auth, trace-context propagation, and the pooled
//! inter-service transport.
//!
//! This module also centralizes the compression policy for Flight traffic
//! (#945): IPC record-batch buffers are lz4-compressed (fast, latency
//! friendly), and the tonic channel negotiates zstd/gzip message
//! compression on top. Servers keep accepting uncompressed messages, and
//! Arrow IPC readers auto-detect per-message buffer compression, so both
//! layers stay wire-compatible with older peers' payloads.

use arrow_flight::FlightData;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::error::ArrowError;
use datafusion::arrow::ipc::CompressionType;
use datafusion::arrow::ipc::writer::{
    CompressionContext, DictionaryTracker, IpcDataGenerator, IpcWriteOptions,
};
use datafusion::arrow::record_batch::RecordBatch;
use tonic::codec::CompressionEncoding;
use tonic::service::interceptor::InterceptedService;

pub mod auth;
pub mod chunk;
pub mod conversion;
pub mod decode;
pub mod schema;
pub mod trace_context;
pub mod transport;

#[cfg(test)]
mod integration_tests;

/// Maximum gRPC message size (encoding and decoding) for all Flight
/// servers and clients, in bytes.
///
/// tonic's default receive limit is 4 MiB. One OTLP export becomes one
/// FlightData message on the acceptor → writer `do_put` path, and Arrow
/// encoding inflates OTLP (resource JSON repeated per row, IDs as hex
/// text), so a ~4 MB OTLP payload can exceed the default. When that
/// happens the writer rejects `do_put`, and the acceptor's WAL entry
/// retries the same oversized batch forever — a permanently wedged entry.
///
/// Every `FlightServiceServer` and `FlightServiceClient` must apply this
/// limit via `max_decoding_message_size` / `max_encoding_message_size`.
/// The sender additionally chunks batches (see [`chunk`]) so a single
/// message stays well below this bound.
pub const MAX_GRPC_MESSAGE_SIZE: usize = 64 * 1024 * 1024;

/// IPC write options for Flight payloads: lz4-frame buffer compression.
///
/// lz4 keeps encode/decode cheap on the latency-sensitive query path while
/// still collapsing the highly repetitive JSON attribute columns that
/// dominate SignalDB payloads. Decoders auto-detect compression from
/// per-message metadata, so uncompressed peers keep working.
pub fn flight_ipc_write_options() -> IpcWriteOptions {
    // LZ4_FRAME is a supported codec and the lz4 feature is compiled in
    // (workspace `arrow/ipc_compression`), so this cannot fail; fall back
    // to uncompressed options rather than panic if it ever does.
    IpcWriteOptions::default()
        .try_with_compression(Some(CompressionType::LZ4_FRAME))
        .unwrap_or_default()
}

/// Encode record batches as Flight data with lz4 IPC buffer compression.
///
/// Drop-in replacement for [`arrow_flight::utils::batches_to_flight_data`]
/// that compresses record-batch buffers. The first message carries the
/// schema; dictionary batches (if any) precede each data batch.
///
/// The schema message is built through [`IpcDataGenerator::schema_to_bytes_with_dictionary_tracker`]
/// against the *same* [`DictionaryTracker`] used to encode the batches below
/// — not `arrow_flight::SchemaAsIpc`'s `Into<FlightData>`, which builds and
/// discards its own internal tracker. `IpcDataGenerator::encode` assigns each dictionary
/// column's IPC `dict_id` from `dictionary_tracker.dict_ids`, a sequence
/// populated only while walking the schema; encoding the schema through a
/// throwaway tracker leaves that sequence empty, so the first dictionary
/// column in any batch fails to encode ("no dict id for field ...") rather
/// than silently dropping data. This has no effect on schemas without a
/// dictionary-typed column (SignalDB's own schemas, today): the emitted
/// schema bytes are identical either way.
pub fn batches_to_compressed_flight_data(
    schema: &Schema,
    batches: Vec<RecordBatch>,
) -> Result<Vec<FlightData>, ArrowError> {
    let options = flight_ipc_write_options();
    let data_gen = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);
    let mut compression_context = CompressionContext::default();

    let schema_bytes =
        data_gen.schema_to_bytes_with_dictionary_tracker(schema, &mut dictionary_tracker, &options);
    let schema_flight_data = FlightData {
        data_header: schema_bytes.ipc_message.into(),
        data_body: vec![].into(),
        app_metadata: vec![].into(),
        flight_descriptor: None,
    };
    let mut flight_data = vec![schema_flight_data];
    for batch in batches {
        let (encoded_dictionaries, encoded_batch) = data_gen.encode(
            &batch,
            &mut dictionary_tracker,
            &options,
            &mut compression_context,
        )?;
        flight_data.extend(encoded_dictionaries.into_iter().map(Into::into));
        flight_data.push(encoded_batch.into());
    }
    Ok(flight_data)
}

/// Construct a `FlightServiceServer` with the shared transport policy.
///
/// Every Flight server in SignalDB must be built through this function so
/// no construction site can diverge from either policy:
///
/// - **Compression**: accept zstd- and gzip-compressed requests
///   (uncompressed always works) and compress responses with zstd when the
///   client advertises support.
/// - **Message-size limits**: [`MAX_GRPC_MESSAGE_SIZE`] on both directions,
///   because tonic's 4 MiB receive default wedges oversized `do_put`
///   batches in the WAL retry loop (#944).
///
/// For authenticated endpoints use
/// [`flight_service_server_with_interceptor`] — the size setters are not
/// exposed once the interceptor wrapper is applied, so the policy must be
/// attached to the inner server first.
pub fn flight_service_server<T: FlightService>(service: T) -> FlightServiceServer<T> {
    FlightServiceServer::new(service)
        .accept_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Zstd)
        .max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(MAX_GRPC_MESSAGE_SIZE)
}

/// [`flight_service_server`] wrapped with a per-request interceptor,
/// mirroring `FlightServiceServer::with_interceptor`.
pub fn flight_service_server_with_interceptor<T, F>(
    service: T,
    interceptor: F,
) -> InterceptedService<FlightServiceServer<T>, F>
where
    T: FlightService,
    F: tonic::service::Interceptor,
{
    InterceptedService::new(flight_service_server(service), interceptor)
}

#[cfg(test)]
mod compression_tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field};
    use std::sync::Arc;

    /// A batch shaped like real trace payloads: the same resource_json
    /// string repeated on every row.
    fn repetitive_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("resource_json", DataType::Utf8, false),
        ]));
        let resource = "{\"service.name\":\"checkout\",\"deployment.environment\":\"prod\",\
                        \"host.name\":\"node-1\",\"telemetry.sdk.language\":\"rust\"}";
        let ids = Int64Array::from((0..1024).collect::<Vec<i64>>());
        let resources = StringArray::from(vec![resource; 1024]);
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(resources)])
            .expect("valid test batch")
    }

    #[test]
    fn compressed_flight_data_round_trips_through_standard_decoder() {
        let batch = repetitive_batch();
        let schema = batch.schema();
        let flight_data = batches_to_compressed_flight_data(&schema, vec![batch.clone()])
            .expect("encode with lz4 compression");
        let decoded =
            arrow_flight::utils::flight_data_to_batches(&flight_data).expect("decode lz4 payload");
        assert_eq!(decoded, vec![batch]);
    }

    #[test]
    fn compressed_flight_data_is_smaller_than_uncompressed() {
        let batch = repetitive_batch();
        let schema = batch.schema();
        let compressed: usize = batches_to_compressed_flight_data(&schema, vec![batch.clone()])
            .expect("compressed encode")
            .iter()
            .map(|d| d.data_body.len())
            .sum();
        let uncompressed: usize = arrow_flight::utils::batches_to_flight_data(&schema, vec![batch])
            .expect("uncompressed encode")
            .iter()
            .map(|d| d.data_body.len())
            .sum();
        assert!(
            compressed < uncompressed,
            "lz4-compressed payload ({compressed} bytes) should be smaller than \
             uncompressed ({uncompressed} bytes) for repetitive data"
        );
    }
}
