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

/// Force **every** Storage-capable writer reachable through `transport` to
/// commit its pending writes, so subsequently-issued queries observe the
/// just-ingested data. Flushes each Storage instance individually (not a
/// round-robin pick), so it is correct for multi-writer topologies. Returns an
/// error if no writer is reachable or any flush fails.
pub async fn flush_storage_writers(transport: &InMemoryFlightTransport) -> anyhow::Result<()> {
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

        let mut stream = client
            .do_action(Action {
                r#type: FLUSH_ACTION.to_string(),
                body: Default::default(),
            })
            .await?
            .into_inner();

        // Drain the (empty) result stream so the flush completes before moving on.
        while let Some(result) = stream.next().await {
            result?;
        }
    }
    Ok(())
}
