use std::fmt;

#[derive(Debug)]
pub enum HeraclitusError {
    Initialization(String),
    Storage(String),
    Protocol(String),
    Network(String),
    Serialization(String),
    NotFound(String),
    InvalidState(String),
    Timeout(String),
    InvalidRequest(String),
    UnsupportedVersion(i16, i16), // api_key, api_version
    UnsupportedApiKey(i16),
}

impl fmt::Display for HeraclitusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Initialization(e) => write!(f, "Initialization error: {e}"),
            Self::Storage(e) => write!(f, "Storage error: {e}"),
            Self::Protocol(e) => write!(f, "Protocol error: {e}"),
            Self::Network(e) => write!(f, "Network error: {e}"),
            Self::Serialization(e) => write!(f, "Serialization error: {e}"),
            Self::NotFound(e) => write!(f, "Not found: {e}"),
            Self::InvalidState(e) => write!(f, "Invalid state: {e}"),
            Self::Timeout(e) => write!(f, "Timeout: {e}"),
            Self::InvalidRequest(e) => write!(f, "Invalid request: {e}"),
            Self::UnsupportedVersion(api_key, version) => {
                write!(f, "Unsupported version {version} for API key {api_key}")
            }
            Self::UnsupportedApiKey(api_key) => write!(f, "Unsupported API key: {api_key}"),
        }
    }
}

impl std::error::Error for HeraclitusError {}

impl From<std::io::Error> for HeraclitusError {
    fn from(err: std::io::Error) -> Self {
        HeraclitusError::Network(err.to_string())
    }
}

impl From<object_store::Error> for HeraclitusError {
    fn from(err: object_store::Error) -> Self {
        HeraclitusError::Storage(err.to_string())
    }
}

impl From<serde_json::Error> for HeraclitusError {
    fn from(err: serde_json::Error) -> Self {
        HeraclitusError::Serialization(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, HeraclitusError>;
