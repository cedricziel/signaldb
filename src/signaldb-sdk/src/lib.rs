mod admin;
mod client;
mod error;

pub use client::SignalDbClient;
pub use error::SdkError;

// Re-export API types for convenience
pub use signaldb_api;
