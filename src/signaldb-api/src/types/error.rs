use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Standard API error response
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiError {
    /// Error category
    pub error: String,
    /// Human-readable error description
    pub message: String,
}

impl ApiError {
    /// Create a new API error
    pub fn new(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            message: message.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_error_serde() {
        let err = ApiError::new("not_found", "Tenant not found");
        let json = serde_json::to_string(&err).unwrap();
        let deserialized: ApiError = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.error, "not_found");
        assert_eq!(deserialized.message, "Tenant not found");
    }
}
