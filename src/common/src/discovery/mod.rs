//! Service discovery interfaces
pub mod nats;

// Re-export common discovery functions and types
pub use nats::{register, deregister, lookup, watch, Instance};