#[derive(Debug)]
pub struct KafkaResponse {
    pub correlation_id: i32,
    pub body: Vec<u8>,
}

impl KafkaResponse {
    pub fn new(correlation_id: i32, body: Vec<u8>) -> Self {
        Self {
            correlation_id,
            body,
        }
    }

    pub fn error(correlation_id: i32, error_code: i16, error_message: &str) -> Self {
        // TODO: Properly encode error response according to Kafka protocol
        let body = format!("Error {error_code}: {error_message}").into_bytes();
        Self::new(correlation_id, body)
    }
}
