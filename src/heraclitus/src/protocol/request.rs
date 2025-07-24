#[derive(Debug, Clone)]
pub enum RequestType {
    Produce,
    Fetch,
    ListOffsets,
    Metadata,
    OffsetCommit,
    OffsetFetch,
    FindCoordinator,
    JoinGroup,
    Heartbeat,
    LeaveGroup,
    SyncGroup,
    ApiVersions,
    SaslHandshake,
    SaslAuthenticate,
}

impl std::fmt::Display for RequestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RequestType::Produce => write!(f, "produce"),
            RequestType::Fetch => write!(f, "fetch"),
            RequestType::ListOffsets => write!(f, "list_offsets"),
            RequestType::Metadata => write!(f, "metadata"),
            RequestType::OffsetCommit => write!(f, "offset_commit"),
            RequestType::OffsetFetch => write!(f, "offset_fetch"),
            RequestType::FindCoordinator => write!(f, "find_coordinator"),
            RequestType::JoinGroup => write!(f, "join_group"),
            RequestType::Heartbeat => write!(f, "heartbeat"),
            RequestType::LeaveGroup => write!(f, "leave_group"),
            RequestType::SyncGroup => write!(f, "sync_group"),
            RequestType::ApiVersions => write!(f, "api_versions"),
            RequestType::SaslHandshake => write!(f, "sasl_handshake"),
            RequestType::SaslAuthenticate => write!(f, "sasl_authenticate"),
        }
    }
}

#[derive(Debug)]
pub struct KafkaRequest {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
    pub request_type: RequestType,
    pub body: Vec<u8>,
}

impl KafkaRequest {
    pub fn new(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<String>,
        body: Vec<u8>,
    ) -> Result<Self, crate::error::HeraclitusError> {
        let request_type = match api_key {
            0 => RequestType::Produce,
            1 => RequestType::Fetch,
            2 => RequestType::ListOffsets,
            3 => RequestType::Metadata,
            8 => RequestType::OffsetCommit,
            9 => RequestType::OffsetFetch,
            10 => RequestType::FindCoordinator,
            11 => RequestType::JoinGroup,
            12 => RequestType::Heartbeat,
            13 => RequestType::LeaveGroup,
            14 => RequestType::SyncGroup,
            17 => RequestType::SaslHandshake,
            18 => RequestType::ApiVersions,
            36 => RequestType::SaslAuthenticate,
            _ => {
                return Err(crate::error::HeraclitusError::Protocol(format!(
                    "Unsupported API key: {api_key}"
                )));
            }
        };

        Ok(Self {
            api_key,
            api_version,
            correlation_id,
            client_id,
            request_type,
            body,
        })
    }
}
