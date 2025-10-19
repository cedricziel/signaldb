// New protocol implementation using kafka-protocol crate
mod compression_proof;
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
use kafka_protocol::protocol::{Decodable, Encodable, decode_request_header_from_buffer};
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

        // Peek at the API key and version for logging
        let api_key = i16::from_be_bytes([data[0], data[1]]);
        let api_version = i16::from_be_bytes([data[2], data[3]]);

        info!("Request: api_key={}, api_version={}", api_key, api_version);

        info!("Before decoding header: buffer has {} bytes", buf.len());

        // Decode the request header using kafka-protocol's helper function
        // This automatically determines the correct header version based on API key and version
        let header = decode_request_header_from_buffer(&mut buf).map_err(|e| {
            error!("Failed to decode header: {}", e);
            error!("Remaining bytes in buffer: {}", buf.len());
            HeraclitusError::Protocol(format!("Failed to decode header: {e}"))
        })?;

        info!(
            "Parsed header: correlation_id={}, client_id={:?}, remaining buffer: {} bytes",
            header.correlation_id,
            header.client_id,
            buf.len()
        );

        // Decode the request body based on API key
        let request = match ApiKey::try_from(api_key) {
            Ok(ApiKey::ApiVersions) => {
                // ApiVersionsRequest is special - clients may send versions we don't support yet
                // Try to decode with the requested version, but fall back to version 3 if it fails
                let safe_version = api_version.min(3);
                info!(
                    "Decoding ApiVersionsRequest with version {}, buffer has {} bytes",
                    safe_version,
                    buf.len()
                );
                let req = ApiVersionsRequest::decode(&mut buf, safe_version).map_err(|e| {
                    error!("Failed to decode ApiVersionsRequest body: {}", e);
                    error!("Buffer has {} bytes remaining", buf.len());
                    HeraclitusError::Protocol(format!(
                        "Failed to decode ApiVersions (version {api_version}, tried {safe_version}): {e}"
                    ))
                })?;
                info!(
                    "ApiVersionsRequest decoded successfully, buffer has {} bytes remaining",
                    buf.len()
                );

                // For flex versions (v3+), check if we correctly consumed all bytes
                if safe_version >= 3 && !buf.is_empty() {
                    error!(
                        "WARNING: Buffer has {} leftover bytes after decoding ApiVersionsRequest v{}",
                        buf.len(),
                        safe_version
                    );
                    error!("This indicates a parsing issue with flex encoding");
                }

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
        info!(
            "encode_response: Starting for api_key={}, api_version={}",
            header.request_api_key, header.request_api_version
        );
        let mut buf = BytesMut::new();

        // Write response header with correlation ID
        let resp_header = ResponseHeader::default().with_correlation_id(header.correlation_id);

        // Determine the correct response header version using kafka-protocol's built-in logic
        let header_version = match ApiKey::try_from(header.request_api_key) {
            Ok(api_key_enum) => api_key_enum.response_header_version(header.request_api_version),
            Err(_) => 0, // Default to version 0 for unknown API keys
        };

        info!(
            "encode_response: Response header version determined as {}",
            header_version
        );

        resp_header.encode(&mut buf, header_version).map_err(|e| {
            error!("Failed to encode response header: {}", e);
            HeraclitusError::Protocol(format!("Failed to encode header: {e}"))
        })?;

        info!(
            "encode_response: Response header encoded, buffer now has {} bytes",
            buf.len()
        );

        // Encode the response body
        match response {
            ResponseKind::ApiVersions(resp) => {
                info!(
                    "encode_response: Encoding ApiVersions response body with version {}",
                    header.request_api_version
                );
                resp.encode(&mut buf, header.request_api_version)
                    .map_err(|e| {
                        error!("Failed to encode ApiVersions response body: {}", e);
                        error!("Buffer size before error: {} bytes", buf.len());
                        HeraclitusError::Protocol(format!("Failed to encode ApiVersions: {e}"))
                    })?;
                info!(
                    "encode_response: ApiVersions response body encoded, total buffer size: {} bytes",
                    buf.len()
                );
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

        let result = buf.to_vec();
        info!(
            "encode_response: Completed successfully, returning {} bytes",
            result.len()
        );
        Ok(result)
    }

    /// Create ApiVersions response with all supported APIs
    /// This response works for all versions (0-3), as the kafka-protocol crate
    /// handles version-specific encoding automatically
    pub fn create_api_versions_response() -> ApiVersionsResponse {
        let api_versions = vec![
            // Produce
            ApiVersion::default()
                .with_api_key(ApiKey::Produce as i16)
                .with_min_version(0)
                .with_max_version(11),
            // Fetch
            ApiVersion::default()
                .with_api_key(ApiKey::Fetch as i16)
                .with_min_version(0)
                .with_max_version(16),
            // ListOffsets
            ApiVersion::default()
                .with_api_key(ApiKey::ListOffsets as i16)
                .with_min_version(0)
                .with_max_version(8),
            // Metadata
            ApiVersion::default()
                .with_api_key(ApiKey::Metadata as i16)
                .with_min_version(0)
                .with_max_version(12),
            // OffsetCommit
            ApiVersion::default()
                .with_api_key(ApiKey::OffsetCommit as i16)
                .with_min_version(0)
                .with_max_version(9),
            // OffsetFetch
            ApiVersion::default()
                .with_api_key(ApiKey::OffsetFetch as i16)
                .with_min_version(0)
                .with_max_version(5),
            // FindCoordinator
            ApiVersion::default()
                .with_api_key(ApiKey::FindCoordinator as i16)
                .with_min_version(0)
                .with_max_version(5),
            // JoinGroup
            ApiVersion::default()
                .with_api_key(ApiKey::JoinGroup as i16)
                .with_min_version(0)
                .with_max_version(9),
            // Heartbeat
            ApiVersion::default()
                .with_api_key(ApiKey::Heartbeat as i16)
                .with_min_version(0)
                .with_max_version(4),
            // LeaveGroup
            ApiVersion::default()
                .with_api_key(ApiKey::LeaveGroup as i16)
                .with_min_version(0)
                .with_max_version(5),
            // SyncGroup
            ApiVersion::default()
                .with_api_key(ApiKey::SyncGroup as i16)
                .with_min_version(0)
                .with_max_version(5),
            // DescribeGroups
            ApiVersion::default()
                .with_api_key(ApiKey::DescribeGroups as i16)
                .with_min_version(0)
                .with_max_version(5),
            // ListGroups
            ApiVersion::default()
                .with_api_key(ApiKey::ListGroups as i16)
                .with_min_version(0)
                .with_max_version(5),
            // SaslHandshake
            ApiVersion::default()
                .with_api_key(ApiKey::SaslHandshake as i16)
                .with_min_version(0)
                .with_max_version(1),
            // ApiVersions
            ApiVersion::default()
                .with_api_key(ApiKey::ApiVersions as i16)
                .with_min_version(0)
                .with_max_version(3),
            // CreateTopics
            ApiVersion::default()
                .with_api_key(ApiKey::CreateTopics as i16)
                .with_min_version(0)
                .with_max_version(7),
            // DeleteTopics
            ApiVersion::default()
                .with_api_key(ApiKey::DeleteTopics as i16)
                .with_min_version(0)
                .with_max_version(6),
            // DeleteRecords
            ApiVersion::default()
                .with_api_key(ApiKey::DeleteRecords as i16)
                .with_min_version(0)
                .with_max_version(2),
            // InitProducerId
            ApiVersion::default()
                .with_api_key(ApiKey::InitProducerId as i16)
                .with_min_version(0)
                .with_max_version(5),
            // OffsetForLeaderEpoch
            ApiVersion::default()
                .with_api_key(ApiKey::OffsetForLeaderEpoch as i16)
                .with_min_version(0)
                .with_max_version(4),
            // AddPartitionsToTxn
            ApiVersion::default()
                .with_api_key(ApiKey::AddPartitionsToTxn as i16)
                .with_min_version(0)
                .with_max_version(5),
            // AddOffsetsToTxn
            ApiVersion::default()
                .with_api_key(ApiKey::AddOffsetsToTxn as i16)
                .with_min_version(0)
                .with_max_version(4),
            // EndTxn
            ApiVersion::default()
                .with_api_key(ApiKey::EndTxn as i16)
                .with_min_version(0)
                .with_max_version(4),
            // WriteTxnMarkers
            ApiVersion::default()
                .with_api_key(ApiKey::WriteTxnMarkers as i16)
                .with_min_version(0)
                .with_max_version(1),
            // TxnOffsetCommit
            ApiVersion::default()
                .with_api_key(ApiKey::TxnOffsetCommit as i16)
                .with_min_version(0)
                .with_max_version(4),
            // DescribeAcls
            ApiVersion::default()
                .with_api_key(ApiKey::DescribeAcls as i16)
                .with_min_version(0)
                .with_max_version(3),
            // CreateAcls
            ApiVersion::default()
                .with_api_key(ApiKey::CreateAcls as i16)
                .with_min_version(0)
                .with_max_version(3),
            // DeleteAcls
            ApiVersion::default()
                .with_api_key(ApiKey::DeleteAcls as i16)
                .with_min_version(0)
                .with_max_version(3),
            // DescribeConfigs
            ApiVersion::default()
                .with_api_key(ApiKey::DescribeConfigs as i16)
                .with_min_version(0)
                .with_max_version(4),
            // AlterConfigs
            ApiVersion::default()
                .with_api_key(ApiKey::AlterConfigs as i16)
                .with_min_version(0)
                .with_max_version(2),
            // AlterReplicaLogDirs
            ApiVersion::default()
                .with_api_key(ApiKey::AlterReplicaLogDirs as i16)
                .with_min_version(0)
                .with_max_version(2),
            // DescribeLogDirs
            ApiVersion::default()
                .with_api_key(ApiKey::DescribeLogDirs as i16)
                .with_min_version(0)
                .with_max_version(4),
            // SaslAuthenticate
            ApiVersion::default()
                .with_api_key(ApiKey::SaslAuthenticate as i16)
                .with_min_version(0)
                .with_max_version(2),
            // CreatePartitions
            ApiVersion::default()
                .with_api_key(ApiKey::CreatePartitions as i16)
                .with_min_version(0)
                .with_max_version(3),
        ];

        ApiVersionsResponse::default()
            .with_error_code(0)
            .with_api_keys(api_versions)
            .with_throttle_time_ms(0)
    }
}
