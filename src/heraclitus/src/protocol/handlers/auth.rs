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
        info!("Handling API versions request v{}", request.api_version);

        // Track API versions requests
        context
            .metrics
            .protocol
            .request_count
            .with_label_values(&["api_versions"])
            .inc();

        // Parse request (minimal parsing needed)
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let _api_versions_request = ApiVersionsRequest::parse(&mut cursor, request.api_version)?;

        // List all supported APIs
        let api_versions = vec![
            ApiVersion {
                api_key: 0,
                min_version: 0,
                max_version: 9,
            }, // Produce
            ApiVersion {
                api_key: 1,
                min_version: 0,
                max_version: 12,
            }, // Fetch
            ApiVersion {
                api_key: 2,
                min_version: 0,
                max_version: 7,
            }, // ListOffsets
            ApiVersion {
                api_key: 3,
                min_version: 0,
                max_version: 12,
            }, // Metadata
            ApiVersion {
                api_key: 8,
                min_version: 0,
                max_version: 8,
            }, // OffsetCommit
            ApiVersion {
                api_key: 9,
                min_version: 0,
                max_version: 8,
            }, // OffsetFetch
            ApiVersion {
                api_key: 10,
                min_version: 0,
                max_version: 4,
            }, // FindCoordinator
            ApiVersion {
                api_key: 11,
                min_version: 0,
                max_version: 7,
            }, // JoinGroup
            ApiVersion {
                api_key: 12,
                min_version: 0,
                max_version: 4,
            }, // Heartbeat
            ApiVersion {
                api_key: 13,
                min_version: 0,
                max_version: 4,
            }, // LeaveGroup
            ApiVersion {
                api_key: 14,
                min_version: 0,
                max_version: 5,
            }, // SyncGroup
            ApiVersion {
                api_key: 15,
                min_version: 0,
                max_version: 5,
            }, // DescribeGroups
            ApiVersion {
                api_key: 16,
                min_version: 0,
                max_version: 4,
            }, // ListGroups
            ApiVersion {
                api_key: 17,
                min_version: 0,
                max_version: 1,
            }, // SaslHandshake
            ApiVersion {
                api_key: 18,
                min_version: 0,
                max_version: 3,
            }, // ApiVersions
            ApiVersion {
                api_key: 19,
                min_version: 0,
                max_version: 7,
            }, // CreateTopics
            ApiVersion {
                api_key: 20,
                min_version: 0,
                max_version: 6,
            }, // DeleteTopics
            ApiVersion {
                api_key: 22,
                min_version: 0,
                max_version: 4,
            }, // InitProducerId
            ApiVersion {
                api_key: 36,
                min_version: 0,
                max_version: 2,
            }, // SaslAuthenticate
        ];

        let response = ApiVersionsResponse {
            error_code: ERROR_NONE,
            api_versions,
            throttle_time_ms: 0,
        };

        // Encode response
        response.encode(request.api_version)
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
