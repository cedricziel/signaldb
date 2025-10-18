// New protocol implementation using kafka-protocol crate
mod connection_handler;
mod protocol_handler;

pub use connection_handler::ConnectionHandler;
pub use protocol_handler::{ProtocolConfig, ProtocolHandler};

use crate::error::{HeraclitusError, Result};
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{
    ApiKey, ApiVersionsRequest, ApiVersionsResponse, CreateTopicsRequest, FetchRequest,
    FindCoordinatorRequest, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest,
    ListOffsetsRequest, MetadataRequest, OffsetCommitRequest, OffsetFetchRequest, ProduceRequest,
    RequestHeader, RequestKind, ResponseHeader, ResponseKind, SyncGroupRequest,
};
use kafka_protocol::protocol::{Decodable, Encodable};
use tracing::{error, info};

pub struct KafkaProtocolHandler;

impl KafkaProtocolHandler {
    /// Parse a Kafka request from the wire
    pub async fn parse_request(data: &[u8]) -> Result<(RequestHeader, RequestKind)> {
        info!("Parsing request, total bytes: {}", data.len());

        if data.len() < 4 {
            return Err(HeraclitusError::Protocol("Request too short".to_string()));
        }

        // Create a buffer for parsing
        let mut buf = Bytes::from(data.to_vec());

        // Peek at the API key and version to determine the correct header version
        let api_key = i16::from_be_bytes([data[0], data[1]]);
        let api_version = i16::from_be_bytes([data[2], data[3]]);

        info!("Request: api_key={}, api_version={}", api_key, api_version);

        // Determine the correct header version using kafka-protocol's built-in logic
        let header_version = match ApiKey::try_from(api_key) {
            Ok(api_key_enum) => api_key_enum.request_header_version(api_version),
            Err(_) => {
                return Err(HeraclitusError::Protocol(format!(
                    "Unknown API key: {api_key}"
                )));
            }
        };

        info!("Using header version: {}", header_version);

        // Decode the request header with the correct version
        let header = match RequestHeader::decode(&mut buf, header_version) {
            Ok(h) => h,
            Err(e) => {
                error!(
                    "Failed to decode header with version {}: {}",
                    header_version, e
                );
                error!("Remaining bytes in buffer: {}", buf.len());
                return Err(HeraclitusError::Protocol(format!(
                    "Failed to decode header: {e}"
                )));
            }
        };

        info!(
            "Parsed header: correlation_id={}, client_id={:?}",
            header.correlation_id, header.client_id
        );

        // Decode the request body based on API key
        let request = match ApiKey::try_from(api_key) {
            Ok(ApiKey::ApiVersions) => {
                // ApiVersionsRequest is special - clients may send versions we don't support yet
                // Try to decode with the requested version, but fall back to version 3 if it fails
                let safe_version = api_version.min(3);
                let req = ApiVersionsRequest::decode(&mut buf, safe_version).map_err(|e| {
                    HeraclitusError::Protocol(format!(
                        "Failed to decode ApiVersions (version {api_version}, tried {safe_version}): {e}"
                    ))
                })?;
                RequestKind::ApiVersions(req)
            }
            Ok(ApiKey::Metadata) => {
                let req = MetadataRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode Metadata: {e}"))
                })?;
                RequestKind::Metadata(req)
            }
            Ok(ApiKey::Produce) => {
                let req = ProduceRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode Produce: {e}"))
                })?;
                RequestKind::Produce(req)
            }
            Ok(ApiKey::Fetch) => {
                let req = FetchRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode Fetch: {e}"))
                })?;
                RequestKind::Fetch(req)
            }
            Ok(ApiKey::FindCoordinator) => {
                let req = FindCoordinatorRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode FindCoordinator: {e}"))
                })?;
                RequestKind::FindCoordinator(req)
            }
            Ok(ApiKey::JoinGroup) => {
                let req = JoinGroupRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode JoinGroup: {e}"))
                })?;
                RequestKind::JoinGroup(req)
            }
            Ok(ApiKey::SyncGroup) => {
                let req = SyncGroupRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode SyncGroup: {e}"))
                })?;
                RequestKind::SyncGroup(req)
            }
            Ok(ApiKey::Heartbeat) => {
                let req = HeartbeatRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode Heartbeat: {e}"))
                })?;
                RequestKind::Heartbeat(req)
            }
            Ok(ApiKey::LeaveGroup) => {
                let req = LeaveGroupRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode LeaveGroup: {e}"))
                })?;
                RequestKind::LeaveGroup(req)
            }
            Ok(ApiKey::OffsetCommit) => {
                let req = OffsetCommitRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode OffsetCommit: {e}"))
                })?;
                RequestKind::OffsetCommit(req)
            }
            Ok(ApiKey::OffsetFetch) => {
                let req = OffsetFetchRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode OffsetFetch: {e}"))
                })?;
                RequestKind::OffsetFetch(req)
            }
            Ok(ApiKey::ListOffsets) => {
                let req = ListOffsetsRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode ListOffsets: {e}"))
                })?;
                RequestKind::ListOffsets(req)
            }
            Ok(ApiKey::CreateTopics) => {
                let req = CreateTopicsRequest::decode(&mut buf, api_version).map_err(|e| {
                    HeraclitusError::Protocol(format!("Failed to decode CreateTopics: {e}"))
                })?;
                RequestKind::CreateTopics(req)
            }
            _ => {
                return Err(HeraclitusError::Protocol(format!(
                    "Unsupported API key: {api_key} version: {api_version}"
                )));
            }
        };

        Ok((header, request))
    }

    /// Encode a Kafka response to wire format
    pub async fn encode_response(
        header: &RequestHeader,
        response: ResponseKind,
    ) -> Result<Vec<u8>> {
        let mut buf = BytesMut::new();

        // Write response header with correlation ID
        let resp_header = ResponseHeader::default().with_correlation_id(header.correlation_id);

        // Determine the correct response header version using kafka-protocol's built-in logic
        let header_version = match ApiKey::try_from(header.request_api_key) {
            Ok(api_key_enum) => api_key_enum.response_header_version(header.request_api_version),
            Err(_) => 0, // Default to version 0 for unknown API keys
        };

        resp_header
            .encode(&mut buf, header_version)
            .map_err(|e| HeraclitusError::Protocol(format!("Failed to encode header: {e}")))?;

        // Encode the response body
        match response {
            ResponseKind::ApiVersions(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode ApiVersions: {e}"))
                    })?;
            }
            ResponseKind::Metadata(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode Metadata: {e}"))
                    })?;
            }
            ResponseKind::Produce(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode Produce: {e}"))
                    })?;
            }
            ResponseKind::Fetch(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode Fetch: {e}"))
                    })?;
            }
            ResponseKind::FindCoordinator(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode FindCoordinator: {e}"))
                    })?;
            }
            ResponseKind::JoinGroup(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode JoinGroup: {e}"))
                    })?;
            }
            ResponseKind::SyncGroup(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode SyncGroup: {e}"))
                    })?;
            }
            ResponseKind::Heartbeat(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode Heartbeat: {e}"))
                    })?;
            }
            ResponseKind::LeaveGroup(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode LeaveGroup: {e}"))
                    })?;
            }
            ResponseKind::OffsetCommit(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode OffsetCommit: {e}"))
                    })?;
            }
            ResponseKind::OffsetFetch(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode OffsetFetch: {e}"))
                    })?;
            }
            ResponseKind::ListOffsets(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode ListOffsets: {e}"))
                    })?;
            }
            ResponseKind::CreateTopics(resp) => {
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        HeraclitusError::Protocol(format!("Failed to encode CreateTopics: {e}"))
                    })?;
            }
            _ => {
                return Err(HeraclitusError::Protocol(
                    "Unsupported response type".to_string(),
                ));
            }
        }

        Ok(buf.to_vec())
    }

    /// Create ApiVersions response with all supported APIs
    pub fn create_api_versions_response() -> ApiVersionsResponse {
        let api_versions = vec![
            // Produce
            ApiVersion::default()
                .with_api_key(0)
                .with_min_version(0)
                .with_max_version(11),
            // Fetch
            ApiVersion::default()
                .with_api_key(1)
                .with_min_version(0)
                .with_max_version(16),
            // ListOffsets
            ApiVersion::default()
                .with_api_key(2)
                .with_min_version(0)
                .with_max_version(8),
            // Metadata
            ApiVersion::default()
                .with_api_key(3)
                .with_min_version(0)
                .with_max_version(12),
            // OffsetCommit
            ApiVersion::default()
                .with_api_key(8)
                .with_min_version(0)
                .with_max_version(9),
            // OffsetFetch
            ApiVersion::default()
                .with_api_key(9)
                .with_min_version(0)
                .with_max_version(9),
            // FindCoordinator
            ApiVersion::default()
                .with_api_key(10)
                .with_min_version(0)
                .with_max_version(5),
            // JoinGroup
            ApiVersion::default()
                .with_api_key(11)
                .with_min_version(0)
                .with_max_version(9),
            // Heartbeat
            ApiVersion::default()
                .with_api_key(12)
                .with_min_version(0)
                .with_max_version(4),
            // LeaveGroup
            ApiVersion::default()
                .with_api_key(13)
                .with_min_version(0)
                .with_max_version(5),
            // SyncGroup
            ApiVersion::default()
                .with_api_key(14)
                .with_min_version(0)
                .with_max_version(5),
            // DescribeGroups
            ApiVersion::default()
                .with_api_key(15)
                .with_min_version(0)
                .with_max_version(5),
            // ListGroups
            ApiVersion::default()
                .with_api_key(16)
                .with_min_version(0)
                .with_max_version(5),
            // SaslHandshake
            ApiVersion::default()
                .with_api_key(17)
                .with_min_version(0)
                .with_max_version(1),
            // ApiVersions
            ApiVersion::default()
                .with_api_key(18)
                .with_min_version(0)
                .with_max_version(3),
            // CreateTopics
            ApiVersion::default()
                .with_api_key(19)
                .with_min_version(0)
                .with_max_version(7),
            // DeleteTopics
            ApiVersion::default()
                .with_api_key(20)
                .with_min_version(0)
                .with_max_version(6),
            // DeleteRecords
            ApiVersion::default()
                .with_api_key(21)
                .with_min_version(0)
                .with_max_version(2),
            // InitProducerId
            ApiVersion::default()
                .with_api_key(22)
                .with_min_version(0)
                .with_max_version(5),
            // OffsetForLeaderEpoch
            ApiVersion::default()
                .with_api_key(23)
                .with_min_version(0)
                .with_max_version(4),
            // AddPartitionsToTxn
            ApiVersion::default()
                .with_api_key(24)
                .with_min_version(0)
                .with_max_version(5),
            // AddOffsetsToTxn
            ApiVersion::default()
                .with_api_key(25)
                .with_min_version(0)
                .with_max_version(4),
            // EndTxn
            ApiVersion::default()
                .with_api_key(26)
                .with_min_version(0)
                .with_max_version(4),
            // WriteTxnMarkers
            ApiVersion::default()
                .with_api_key(27)
                .with_min_version(0)
                .with_max_version(1),
            // TxnOffsetCommit
            ApiVersion::default()
                .with_api_key(28)
                .with_min_version(0)
                .with_max_version(4),
            // DescribeAcls
            ApiVersion::default()
                .with_api_key(29)
                .with_min_version(0)
                .with_max_version(3),
            // CreateAcls
            ApiVersion::default()
                .with_api_key(30)
                .with_min_version(0)
                .with_max_version(3),
            // DeleteAcls
            ApiVersion::default()
                .with_api_key(31)
                .with_min_version(0)
                .with_max_version(3),
            // DescribeConfigs
            ApiVersion::default()
                .with_api_key(32)
                .with_min_version(0)
                .with_max_version(4),
            // AlterConfigs
            ApiVersion::default()
                .with_api_key(33)
                .with_min_version(0)
                .with_max_version(2),
            // AlterReplicaLogDirs
            ApiVersion::default()
                .with_api_key(34)
                .with_min_version(0)
                .with_max_version(2),
            // DescribeLogDirs
            ApiVersion::default()
                .with_api_key(35)
                .with_min_version(0)
                .with_max_version(4),
            // SaslAuthenticate
            ApiVersion::default()
                .with_api_key(36)
                .with_min_version(0)
                .with_max_version(2),
            // CreatePartitions
            ApiVersion::default()
                .with_api_key(37)
                .with_min_version(0)
                .with_max_version(3),
        ];

        ApiVersionsResponse::default()
            .with_error_code(0)
            .with_api_keys(api_versions)
            .with_throttle_time_ms(0)
    }
}
