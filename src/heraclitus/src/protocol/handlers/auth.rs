use super::{ApiHandler, HandlerContext};
use crate::{
    error::Result,
    protocol::{
        api_versions::{ApiVersion, ApiVersionsRequest, ApiVersionsResponse},
        kafka_protocol::*,
        request::KafkaRequest,
        sasl_authenticate::{SaslAuthenticateRequest, SaslAuthenticateResponse},
        sasl_handshake::{SaslHandshakeRequest, SaslHandshakeResponse},
    },
};
use tracing::{debug, info, warn};

pub struct ApiVersionsHandler;

#[async_trait::async_trait]
impl ApiHandler for ApiVersionsHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!(
            "Handling API versions request v{}, correlation_id={}, body_len={}",
            request.api_version,
            request.correlation_id,
            request.body.len()
        );

        // CRITICAL: ApiVersions has special handling in Kafka protocol
        // If client sends unsupported version, we MUST respond with v0 format
        let response_version = if request.api_version > 3 {
            warn!(
                "Client requested ApiVersions v{} which we don't support, will respond with v0",
                request.api_version
            );
            0
        } else {
            request.api_version
        };

        info!(
            "ApiVersionsHandler: Will encode response for version {} (requested: {})",
            response_version, request.api_version
        );

        // Track API versions requests
        let api_key_str = request.api_key.to_string();
        let api_version_str = request.api_version.to_string();
        let api_name =
            crate::metrics::protocol::ProtocolMetrics::api_name(request.api_key).to_string();
        context
            .metrics
            .protocol
            .request_count
            .with_label_values(&[&api_key_str, &api_name, &api_version_str])
            .inc();

        // Parse request (minimal parsing needed)
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let api_versions_request = ApiVersionsRequest::parse(&mut cursor, request.api_version)?;

        info!(
            "ApiVersions request parsed: client_software_name={:?}, client_software_version={:?}",
            api_versions_request.client_software_name, api_versions_request.client_software_version
        );

        // List all supported APIs with updated versions for rdkafka 2.10.0+ compatibility
        let api_versions = vec![
            ApiVersion {
                api_key: 0,
                min_version: 0,
                max_version: 11, // Produce - updated from 9 to 11
            },
            ApiVersion {
                api_key: 1,
                min_version: 0,
                max_version: 16, // Fetch - updated from 12 to 16
            },
            ApiVersion {
                api_key: 2,
                min_version: 0,
                max_version: 8, // ListOffsets - updated from 7 to 8
            },
            ApiVersion {
                api_key: 3,
                min_version: 0,
                max_version: 12, // Metadata - kept at 12
            },
            ApiVersion {
                api_key: 8,
                min_version: 0,
                max_version: 9, // OffsetCommit - updated from 8 to 9
            },
            ApiVersion {
                api_key: 9,
                min_version: 0,
                max_version: 9, // OffsetFetch - updated from 8 to 9
            },
            ApiVersion {
                api_key: 10,
                min_version: 0,
                max_version: 5, // FindCoordinator - updated from 4 to 5
            },
            ApiVersion {
                api_key: 11,
                min_version: 0,
                max_version: 9, // JoinGroup - updated from 7 to 9
            },
            ApiVersion {
                api_key: 12,
                min_version: 0,
                max_version: 4, // Heartbeat - kept at 4
            },
            ApiVersion {
                api_key: 13,
                min_version: 0,
                max_version: 5, // LeaveGroup - updated from 4 to 5
            },
            ApiVersion {
                api_key: 14,
                min_version: 0,
                max_version: 5, // SyncGroup - kept at 5
            },
            ApiVersion {
                api_key: 15,
                min_version: 0,
                max_version: 5, // DescribeGroups - kept at 5
            },
            ApiVersion {
                api_key: 16,
                min_version: 0,
                max_version: 5, // ListGroups - updated from 4 to 5
            },
            ApiVersion {
                api_key: 17,
                min_version: 0,
                max_version: 1, // SaslHandshake - kept at 1
            },
            ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 3, // ApiVersions - kept at 3 for now (will update later if needed)
            },
            ApiVersion {
                api_key: 19,
                min_version: 0,
                max_version: 7, // CreateTopics - kept at 7
            },
            ApiVersion {
                api_key: 20,
                min_version: 0,
                max_version: 6, // DeleteTopics - kept at 6
            },
            // Add missing API keys that modern Kafka clients expect
            ApiVersion {
                api_key: 21,
                min_version: 0,
                max_version: 2, // DeleteRecords
            },
            ApiVersion {
                api_key: 22,
                min_version: 0,
                max_version: 5, // InitProducerId - updated from 4 to 5
            },
            ApiVersion {
                api_key: 23,
                min_version: 0,
                max_version: 4, // OffsetForLeaderEpoch
            },
            ApiVersion {
                api_key: 24,
                min_version: 0,
                max_version: 5, // AddPartitionsToTxn
            },
            ApiVersion {
                api_key: 25,
                min_version: 0,
                max_version: 4, // AddOffsetsToTxn
            },
            ApiVersion {
                api_key: 26,
                min_version: 0,
                max_version: 4, // EndTxn
            },
            ApiVersion {
                api_key: 27,
                min_version: 0,
                max_version: 1, // WriteTxnMarkers
            },
            ApiVersion {
                api_key: 28,
                min_version: 0,
                max_version: 4, // TxnOffsetCommit
            },
            ApiVersion {
                api_key: 29,
                min_version: 0,
                max_version: 3, // DescribeAcls
            },
            ApiVersion {
                api_key: 30,
                min_version: 0,
                max_version: 3, // CreateAcls
            },
            ApiVersion {
                api_key: 31,
                min_version: 0,
                max_version: 3, // DeleteAcls
            },
            ApiVersion {
                api_key: 32,
                min_version: 0,
                max_version: 4, // DescribeConfigs
            },
            ApiVersion {
                api_key: 33,
                min_version: 0,
                max_version: 2, // AlterConfigs
            },
            ApiVersion {
                api_key: 34,
                min_version: 0,
                max_version: 2, // AlterReplicaLogDirs
            },
            ApiVersion {
                api_key: 35,
                min_version: 0,
                max_version: 4, // DescribeLogDirs
            },
            ApiVersion {
                api_key: 36,
                min_version: 0,
                max_version: 2, // SaslAuthenticate - kept at 2
            },
            ApiVersion {
                api_key: 37,
                min_version: 0,
                max_version: 3, // CreatePartitions
            },
            ApiVersion {
                api_key: 38,
                min_version: 0,
                max_version: 3, // CreateDelegationToken
            },
            ApiVersion {
                api_key: 39,
                min_version: 0,
                max_version: 2, // RenewDelegationToken
            },
            ApiVersion {
                api_key: 40,
                min_version: 0,
                max_version: 2, // ExpireDelegationToken
            },
            ApiVersion {
                api_key: 41,
                min_version: 0,
                max_version: 3, // DescribeDelegationToken
            },
            ApiVersion {
                api_key: 42,
                min_version: 0,
                max_version: 2, // DeleteGroups
            },
            ApiVersion {
                api_key: 43,
                min_version: 0,
                max_version: 2, // ElectLeaders
            },
            ApiVersion {
                api_key: 44,
                min_version: 0,
                max_version: 1, // IncrementalAlterConfigs
            },
            ApiVersion {
                api_key: 45,
                min_version: 0,
                max_version: 0, // AlterPartitionReassignments
            },
            ApiVersion {
                api_key: 46,
                min_version: 0,
                max_version: 0, // ListPartitionReassignments
            },
            ApiVersion {
                api_key: 47,
                min_version: 0,
                max_version: 0, // OffsetDelete
            },
            ApiVersion {
                api_key: 48,
                min_version: 0,
                max_version: 1, // DescribeClientQuotas
            },
            ApiVersion {
                api_key: 49,
                min_version: 0,
                max_version: 1, // AlterClientQuotas
            },
            ApiVersion {
                api_key: 50,
                min_version: 0,
                max_version: 0, // DescribeUserScramCredentials
            },
            ApiVersion {
                api_key: 51,
                min_version: 0,
                max_version: 0, // AlterUserScramCredentials
            },
            ApiVersion {
                api_key: 55,
                min_version: 0,
                max_version: 1, // DescribeQuorum
            },
            ApiVersion {
                api_key: 57,
                min_version: 0,
                max_version: 1, // AlterPartition
            },
            ApiVersion {
                api_key: 60,
                min_version: 0,
                max_version: 1, // DescribeCluster
            },
            ApiVersion {
                api_key: 61,
                min_version: 0,
                max_version: 0, // DescribeProducers
            },
            ApiVersion {
                api_key: 64,
                min_version: 0,
                max_version: 0, // DescribeTransactions
            },
            ApiVersion {
                api_key: 65,
                min_version: 0,
                max_version: 0, // ListTransactions
            },
            ApiVersion {
                api_key: 66,
                min_version: 0,
                max_version: 1, // AllocateProducerIds
            },
            ApiVersion {
                api_key: 68,
                min_version: 0,
                max_version: 0, // ConsumerGroupHeartbeat
            },
            ApiVersion {
                api_key: 69,
                min_version: 0,
                max_version: 0, // ConsumerGroupDescribe
            },
            ApiVersion {
                api_key: 74,
                min_version: 0,
                max_version: 0, // DescribeTopicPartitions
            },
            ApiVersion {
                api_key: 75,
                min_version: 0,
                max_version: 0, // GetTelemetrySubscriptions
            },
        ];

        let response = ApiVersionsResponse {
            error_code: ERROR_NONE,
            api_versions,
            throttle_time_ms: 0,
        };

        // Encode response
        info!(
            "About to encode ApiVersionsResponse with api_version={}, should_use_flexible={}",
            response_version,
            crate::protocol::kafka_protocol::uses_flexible_version_for_api(18, response_version)
        );
        let encoded = response.encode(response_version)?;
        info!(
            "Encoded {} bytes, first 32 bytes: {:02x?}",
            encoded.len(),
            &encoded[..32.min(encoded.len())]
        );

        // If we downgraded the version, we should return an error in the response
        if response_version != request.api_version {
            // For unsupported versions, return v0 format with UNSUPPORTED_VERSION error
            let error_response = ApiVersionsResponse {
                error_code: crate::protocol::kafka_protocol::ERROR_UNSUPPORTED_VERSION,
                api_versions: vec![], // Empty on error
                throttle_time_ms: 0,
            };
            let encoded = error_response.encode(0)?; // Always v0 for errors
            info!("Returning UNSUPPORTED_VERSION error in v0 format");
            return Ok(encoded);
        }

        Ok(encoded)
    }

    fn api_key(&self) -> i16 {
        18 // ApiVersions
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=3).contains(&version)
    }
}

pub struct SaslHandshakeHandler;

#[async_trait::async_trait]
impl ApiHandler for SaslHandshakeHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling SASL handshake request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let handshake_request = SaslHandshakeRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "SASL handshake request: mechanism={}",
            handshake_request.mechanism
        );

        // Check if the requested mechanism is supported
        let supported_mechanisms = vec![context.auth_config.mechanism.clone()];
        let (error_code, mechanisms) =
            if handshake_request.mechanism == context.auth_config.mechanism {
                (ERROR_NONE, supported_mechanisms.clone())
            } else {
                warn!(
                    "Unsupported SASL mechanism: {}",
                    handshake_request.mechanism
                );
                (ERROR_UNSUPPORTED_SASL_MECHANISM, supported_mechanisms)
            };

        let response = SaslHandshakeResponse {
            error_code,
            enabled_mechanisms: mechanisms,
        };

        // Encode response
        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        17 // SaslHandshake
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=1).contains(&version)
    }
}

pub struct SaslAuthenticateHandler;

#[async_trait::async_trait]
impl ApiHandler for SaslAuthenticateHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!(
            "Handling SASL authenticate request v{}",
            request.api_version
        );

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let auth_request = SaslAuthenticateRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "SASL authenticate request: {} bytes",
            auth_request.auth_bytes.len()
        );

        // Parse PLAIN mechanism auth bytes: [0] username [0] password
        let auth_str = String::from_utf8_lossy(&auth_request.auth_bytes);
        let parts: Vec<&str> = auth_str.split('\0').collect();

        if parts.len() < 3 {
            warn!("Invalid SASL PLAIN auth format");
            let response = SaslAuthenticateResponse {
                error_code: ERROR_SASL_AUTHENTICATION_FAILED,
                error_message: Some("Invalid authentication format".to_string()),
                auth_bytes: vec![],
                session_lifetime_ms: 0,
            };
            return response.encode(request.api_version);
        }

        let username = parts[1];
        let password = parts[2];

        // Validate credentials
        if let (Some(expected_username), Some(expected_password)) = (
            &context.auth_config.plain_username,
            &context.auth_config.plain_password,
        ) {
            if username == expected_username && password == expected_password {
                info!("SASL authentication successful for user: {}", username);
                context.authenticated = true;
                context.username = Some(username.to_string());

                let response = SaslAuthenticateResponse {
                    error_code: ERROR_NONE,
                    error_message: None,
                    auth_bytes: vec![],
                    session_lifetime_ms: 0,
                };
                return response.encode(request.api_version);
            }
        }

        warn!("SASL authentication failed for user: {}", username);
        let response = SaslAuthenticateResponse {
            error_code: ERROR_SASL_AUTHENTICATION_FAILED,
            error_message: Some("Authentication failed".to_string()),
            auth_bytes: vec![],
            session_lifetime_ms: 0,
        };

        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        36 // SaslAuthenticate
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=2).contains(&version)
    }
}
