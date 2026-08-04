//! Middleware modules for the acceptor service

pub mod auth;
pub mod grpc_auth;
pub mod grpc_trace;

pub use auth::{TenantContextExtractor, auth_middleware};
pub use grpc_auth::{get_tenant_context, grpc_auth_interceptor};
pub use grpc_trace::GrpcTraceLayer;
