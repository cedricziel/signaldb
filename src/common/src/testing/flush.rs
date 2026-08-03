//! Read-your-writes helper for integration tests.
//!
//! The writer commits ingested data to Iceberg asynchronously (coalesced by its
//! background loop), so a test that ingests and then immediately queries would
//! race the commit. [`flush_storage_writers`] drives the writer's
//! `do_action("flush")` to force an immediate commit, giving tests a
//! deterministic read-your-writes barrier instead of sleeping.

use arrow_flight::Action;
use futures::StreamExt;

use crate::flight::transport::{InMemoryFlightTransport, ServiceCapability};

/// The writer Flight `do_action` type that forces a commit of all pending
/// writes. Kept in sync with `writer::flight_iceberg::FLUSH_ACTION` (duplicated
/// here to avoid a `common -> writer` dependency).
const FLUSH_ACTION: &str = "flush";

/// Force every Storage-capable writer reachable through `transport` to commit
/// the pending writes for `tenant_id` (and, when `dataset_id` is `Some`, that
/// dataset only), so subsequently-issued queries observe the just-ingested
/// data. The flush is tenant-scoped: it does not disturb other tenants'
/// coalescing. Flushes each Storage instance individually (not a round-robin
/// pick), so it is correct for multi-writer topologies. Returns an error if no
/// writer is reachable or any flush fails.
pub async fn flush_storage_writers(
    transport: &InMemoryFlightTransport,
    tenant_id: &str,
    dataset_id: Option<&str>,
) -> anyhow::Result<()> {
    let services = transport
        .discover_services_by_capability(ServiceCapability::Storage)
        .await;
    if services.is_empty() {
        anyhow::bail!("no Storage writer to flush");
    }

    for service in services {
        let mut client = transport
            .get_flight_client(service.service_id)
            .await
            .map_err(|e| anyhow::anyhow!("flush client for {}: {e}", service.service_id))?;

        // The writer derives the flush scope from the request's tenant metadata
        // (matching the ingest path), not the action body.
        let mut request = tonic::Request::new(Action {
            r#type: FLUSH_ACTION.to_string(),
            body: Default::default(),
        });
        request
            .metadata_mut()
            .insert("x-tenant-id", tenant_id.parse()?);
        if let Some(dataset_id) = dataset_id {
            request
                .metadata_mut()
                .insert("x-dataset-id", dataset_id.parse()?);
        }

        let mut stream = client.do_action(request).await?.into_inner();

        // Drain the (empty) result stream so the flush completes before moving on.
        while let Some(result) = stream.next().await {
            result?;
        }
    }
    Ok(())
}
