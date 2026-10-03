use std::sync::Arc;

use common::auth::{TenantContext, TenantSource};
use common::catalog::Catalog;
use common::flight::transport::InMemoryFlightTransport;
use common::processors::ProcessorRegistry;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use common::wal::WalConfig;

use super::WalManager;

pub(crate) fn test_tenant_context() -> TenantContext {
    TenantContext {
        tenant_id: "acme".to_string(),
        dataset_id: "production".to_string(),
        tenant_slug: "acme".to_string(),
        dataset_slug: "production".to_string(),
        api_key_name: Some("test-key".to_string()),
        api_key_scopes: None,
        api_key_dataset_ids: None,
        oauth_tenant_grants: None,
        api_key_allowed_origins: None,
        user_id: None,
        role: None,
        is_instance_admin: false,
        session_id: None,
        source: TenantSource::Config,
    }
}

pub(crate) fn test_wal_manager(base_dir: &std::path::Path) -> WalManager {
    let config = WalConfig::with_defaults(base_dir.to_path_buf());
    WalManager::new(config.clone(), config.clone(), config.clone(), config)
}

/// A transport with no writer registered, so every forward fails and each
/// entry a handler appends stays unprocessed, plus an empty processor
/// registry.
pub(crate) async fn transport_without_writer()
-> (Arc<InMemoryFlightTransport>, Arc<ProcessorRegistry>) {
    let catalog = Catalog::new_in_memory().await.unwrap();
    let processor_registry = Arc::new(ProcessorRegistry::new(
        Arc::new(catalog.clone()),
        &common::config::ProcessorsConfig::default(),
    ));
    let bootstrap =
        ServiceBootstrap::new_for_test_with_catalog(catalog, ServiceType::Acceptor, "127.0.0.1:0")
            .await
            .unwrap();
    (
        Arc::new(InMemoryFlightTransport::new(bootstrap)),
        processor_registry,
    )
}

/// The payload of the single unprocessed WAL entry for `signal`.
pub(crate) async fn only_wal_entry_bytes(
    wal_manager: &WalManager,
    tenant_context: &TenantContext,
    signal: &str,
) -> Vec<u8> {
    let wal = wal_manager
        .get_wal(
            &tenant_context.tenant_id,
            &tenant_context.dataset_id,
            signal,
        )
        .await
        .unwrap();
    let entries = wal.get_unprocessed_entries().await.unwrap();
    assert_eq!(entries.len(), 1);
    wal.read_entry_data(&entries[0]).await.unwrap()
}

/// `n` string attributes `k0..k{n-1}`, each with value `"v"`.
pub(crate) fn string_attrs(n: usize) -> Vec<opentelemetry_proto::tonic::common::v1::KeyValue> {
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value};
    (0..n)
        .map(|i| KeyValue {
            key: format!("k{i}"),
            value: Some(AnyValue {
                value: Some(Value::StringValue("v".to_string())),
            }),
            ..Default::default()
        })
        .collect()
}

/// Column `name` of `batch`, downcast to `A`.
pub(crate) fn column_as<'a, A: 'static>(
    batch: &'a datafusion::arrow::record_batch::RecordBatch,
    name: &str,
) -> &'a A {
    batch
        .column_by_name(name)
        .unwrap()
        .as_any()
        .downcast_ref::<A>()
        .unwrap()
}
