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
    DictionaryTracker, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
};
use datafusion::arrow::record_batch::RecordBatch;
use tonic::codec::CompressionEncoding;
use tonic::service::interceptor::InterceptedService;

pub mod auth;
pub mod chunk;
pub mod conversion;
pub mod decode;
pub mod forward;
pub mod schema;

/// `app_metadata` prefix for the trailing, data-free `FlightData` message
/// the querier appends after a Query IR `correlate` stage
/// (`openspec/changes/query-ir-span-join`, `otel-native-schema`).
///
/// The report it carries — [`QueryReport`], as JSON — is only known
/// once the join(s) have actually streamed to completion (row/fanout caps
/// are detected by streaming operators such as `CorrelateCapExec`
/// (`querier::query::correlate_cap`), and the target scan window is only
/// resolved during planning) — too late for the one schema message, which
/// Flight sends first. It rides as a final trailer message instead:
/// [`FlightData::app_metadata`] is otherwise unused on this path, so a
/// message starting with this prefix (empty header and body) is
/// unambiguous. The router reads it while draining the stream (see
/// `router::endpoints::query`) and excludes it from batch decoding.
pub const CORRELATE_REPORT_APP_METADATA_PREFIX: &[u8] = b"correlate_report:";

/// The trailer an older querier sends for a truncated join, and the form a
/// report carrying only `row_limit` still takes, so a router and querier of
/// adjacent releases understand each other in either rollout order.
const LEGACY_CORRELATE_TRUNCATED_APP_METADATA: &[u8] = br#"{"correlate_truncated":true}"#;

/// What a query's stages reported while running — a `correlate` stage's
/// join outcome and a `match` stage's incomplete traces — sent by the
/// querier to the router in a Flight trailer (see
/// [`CORRELATE_REPORT_APP_METADATA_PREFIX`]) so the router can surface it
/// as `QueryWarning`s. Every field defaults to "nothing to report", and
/// unknown members are ignored, so adjacent releases read each other's
/// reports; [`Self::is_empty`] says whether the trailer is worth sending.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryReport {
    /// A `correlate` stage's joined row count reached
    /// `[querier].correlate_max_rows`; the result was truncated.
    #[serde(default)]
    pub row_limit: bool,
    /// A `correlate` stage matched more target rows per source row than its
    /// `fanout` cap; the earliest matches were kept.
    #[serde(default)]
    pub fanout_limit: bool,
    /// The target scan window a signal `correlate` stage actually used.
    #[serde(default)]
    pub window: Option<CorrelateWindowReport>,
    /// Traces a relational `match` stage evaluated whose hierarchy the
    /// query range visibly cut.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_incomplete: Option<MatchIncompleteReport>,
    /// What a paged query emitted (`query-result-pagination`). An older
    /// router ignores it, but never sends the `page` ticket that sets it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page: Option<crate::query_cursor::PageReport>,
}

/// The pre-`match` name of [`QueryReport`].
pub type CorrelateReport = QueryReport;

/// Evaluated traces with a span whose parent is not among the evaluated
/// spans, or a span that ends after the range: `matched` ones may be
/// missing witness spans, `unmatched` ones may have matched over a wider
/// range. `sample_trace_ids` holds up to three, matched first.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MatchIncompleteReport {
    #[serde(default)]
    pub matched: u64,
    #[serde(default)]
    pub unmatched: u64,
    #[serde(default)]
    pub sample_trace_ids: Vec<String>,
}

impl QueryReport {
    /// Whether every field is at its default — nothing worth reporting.
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    fn row_limit_only() -> Self {
        Self {
            row_limit: true,
            ..Self::default()
        }
    }
}

/// The `[start, end]` bound (unix epoch nanoseconds) a signal `correlate`
/// stage scanned its target table over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CorrelateWindowReport {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Build the trailing `FlightData` message reporting a `correlate` stage's
/// outcome (see [`CORRELATE_REPORT_APP_METADATA_PREFIX`]), or `None` when
/// `report` [`QueryReport::is_empty`] or (never expected in practice)
/// it fails to serialize.
pub fn correlate_report_trailer(report: &QueryReport) -> Option<FlightData> {
    if report.is_empty() {
        return None;
    }
    let app_metadata = if *report == QueryReport::row_limit_only() {
        LEGACY_CORRELATE_TRUNCATED_APP_METADATA.to_vec()
    } else {
        let mut app_metadata = CORRELATE_REPORT_APP_METADATA_PREFIX.to_vec();
        if let Err(e) = serde_json::to_writer(&mut app_metadata, report) {
            tracing::error!(error = %e, "failed to serialize QueryReport for the Flight trailer");
            return None;
        }
        app_metadata
    };
    Some(FlightData {
        data_header: vec![].into(),
        data_body: vec![].into(),
        app_metadata: app_metadata.into(),
        flight_descriptor: None,
    })
}

/// Parse a trailer's `app_metadata` back into a [`QueryReport`], or
/// `None` if it doesn't carry [`CORRELATE_REPORT_APP_METADATA_PREFIX`].
/// `Some(Err(_))` means the prefix matched but the JSON payload didn't
/// parse — a malformed trailer, not one from a differently-shaped message.
pub fn parse_correlate_report_trailer(
    app_metadata: &[u8],
) -> Option<Result<QueryReport, serde_json::Error>> {
    if app_metadata == LEGACY_CORRELATE_TRUNCATED_APP_METADATA {
        return Some(Ok(QueryReport::row_limit_only()));
    }
    let payload = app_metadata.strip_prefix(CORRELATE_REPORT_APP_METADATA_PREFIX)?;
    Some(serde_json::from_slice(payload))
}

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

/// Encode a stream of record batches as Flight data, one batch at a time,
/// with the same lz4 compression as [`batches_to_compressed_flight_data`].
///
/// Dictionaries are resent rather than hydrated, so dictionary columns keep
/// their type on the wire. An empty input encodes to no messages at all,
/// not even a schema.
pub fn encode_batch_stream<S>(batches: S) -> arrow_flight::encode::FlightDataEncoder
where
    S: futures::Stream<Item = Result<RecordBatch, arrow_flight::error::FlightError>>
        + Send
        + 'static,
{
    arrow_flight::encode::FlightDataEncoderBuilder::new()
        .with_options(flight_ipc_write_options())
        .with_dictionary_handling(arrow_flight::encode::DictionaryHandling::Resend)
        .build(batches)
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
    let mut compression_context = IpcWriteContext::default();

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
mod correlate_report_tests {
    use super::*;

    #[test]
    fn empty_report_has_no_trailer() {
        assert!(correlate_report_trailer(&QueryReport::default()).is_none());
    }

    #[test]
    fn non_empty_report_round_trips_through_the_trailer() {
        let report = QueryReport {
            row_limit: true,
            fanout_limit: true,
            window: Some(CorrelateWindowReport {
                start_ns: 1,
                end_ns: 2,
            }),
            ..match_incomplete_report()
        };
        let trailer = correlate_report_trailer(&report).expect("non-empty report has a trailer");
        let decoded = parse_correlate_report_trailer(&trailer.app_metadata)
            .expect("trailer carries the report prefix")
            .expect("trailer JSON parses");
        assert_eq!(decoded, report);
    }

    #[test]
    fn row_limit_only_report_uses_the_legacy_trailer_both_ways() {
        let report = QueryReport::row_limit_only();
        let trailer = correlate_report_trailer(&report).expect("non-empty report has a trailer");
        assert_eq!(
            trailer.app_metadata.as_ref(),
            LEGACY_CORRELATE_TRUNCATED_APP_METADATA
        );
        let decoded = parse_correlate_report_trailer(&trailer.app_metadata)
            .expect("legacy trailer is recognized")
            .expect("legacy trailer parses");
        assert_eq!(decoded, report);
    }

    fn match_incomplete_report() -> QueryReport {
        QueryReport {
            match_incomplete: Some(MatchIncompleteReport {
                matched: 3,
                unmatched: 1,
                sample_trace_ids: vec!["5b8e".into(), "a1f0".into()],
            }),
            ..QueryReport::default()
        }
    }

    #[test]
    fn match_incomplete_round_trips_through_the_trailer() {
        let report = match_incomplete_report();
        let trailer = correlate_report_trailer(&report).expect("non-empty report has a trailer");
        let decoded = parse_correlate_report_trailer(&trailer.app_metadata)
            .expect("trailer carries the report prefix")
            .expect("trailer JSON parses");
        assert_eq!(decoded, report);
    }

    #[test]
    fn unknown_report_members_are_ignored() {
        let mut app_metadata = CORRELATE_REPORT_APP_METADATA_PREFIX.to_vec();
        app_metadata.extend_from_slice(br#"{"fanoutLimit":true,"fromTheFuture":{"x":1}}"#);
        let decoded = parse_correlate_report_trailer(&app_metadata)
            .expect("prefix matched")
            .expect("unknown members are ignored");
        assert!(decoded.fanout_limit);
    }

    #[test]
    fn a_pre_match_report_parses_without_match_incomplete() {
        let mut app_metadata = CORRELATE_REPORT_APP_METADATA_PREFIX.to_vec();
        app_metadata.extend_from_slice(br#"{"rowLimit":false,"fanoutLimit":true,"window":null}"#);
        let decoded = parse_correlate_report_trailer(&app_metadata)
            .expect("prefix matched")
            .expect("old report parses");
        assert_eq!(decoded.match_incomplete, None);
    }

    #[test]
    fn a_page_report_round_trips_through_the_trailer() {
        use crate::query_cursor::{KeyPart, KeyValue, PageReport};
        let report = CorrelateReport {
            page: Some(PageReport {
                last_key: Some(vec![KeyPart {
                    field: "timestamp".into(),
                    value: KeyValue::I64(7),
                }]),
                has_more: true,
                emitted: 3,
            }),
            ..CorrelateReport::default()
        };
        let trailer = correlate_report_trailer(&report).expect("non-empty report has a trailer");
        let decoded = parse_correlate_report_trailer(&trailer.app_metadata)
            .expect("trailer carries the report prefix")
            .expect("trailer JSON parses");
        assert_eq!(decoded, report);
    }

    #[test]
    fn a_trailer_without_page_still_parses() {
        let mut app_metadata = CORRELATE_REPORT_APP_METADATA_PREFIX.to_vec();
        app_metadata.extend_from_slice(br#"{"rowLimit":false,"fanoutLimit":true,"window":null}"#);
        let decoded = parse_correlate_report_trailer(&app_metadata)
            .expect("prefix matched")
            .expect("parses");
        assert!(decoded.fanout_limit);
        assert_eq!(decoded.page, None);
    }

    #[test]
    fn unrelated_app_metadata_is_not_a_report_trailer() {
        assert!(parse_correlate_report_trailer(b"something else").is_none());
    }

    #[test]
    fn malformed_report_payload_is_reported_as_an_error() {
        let mut app_metadata = CORRELATE_REPORT_APP_METADATA_PREFIX.to_vec();
        app_metadata.extend_from_slice(b"not json");
        assert!(
            parse_correlate_report_trailer(&app_metadata)
                .expect("prefix matched")
                .is_err()
        );
    }
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
