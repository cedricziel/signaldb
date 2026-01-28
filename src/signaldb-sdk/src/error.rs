/// Errors from the SignalDB SDK
#[derive(Debug, thiserror::Error)]
pub enum SdkError {
    /// HTTP transport error
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    /// API returned an error response
    #[error("API error ({status}): {message}")]
    Api {
        /// HTTP status code
        status: u16,
        /// Error message from the API
        message: String,
    },
    /// JSON deserialization error
    #[error("Deserialization error: {0}")]
    Deserialize(#[from] serde_json::Error),
}
