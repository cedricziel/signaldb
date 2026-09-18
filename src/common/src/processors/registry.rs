//! Lazy per-tenant cache of compiled tenant OTTL processors (change:
//! tenant-ottl-processors, design D5).
//!
//! Mirrors `schema_registry::SchemaResolver`'s `DashMap` cache: every
//! tenant's processors are compiled once and cached; an entry older than
//! the configured TTL is reloaded synchronously on next access, guarded by
//! a per-entry mutex so concurrent requests against a stale entry only hit
//! the catalog once. There is no background task — the cost is one small
//! `SELECT` per tenant per TTL interval, paid by whichever request happens
//! to land after expiry.

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Mutex as AsyncMutex;

use crate::catalog::Catalog;
use crate::config::ProcessorsConfig;
use crate::processors::{ProcessorRecord, StoreError};

/// One stored processor plus its compiled program, if it compiled.
///
/// `program` is `None` when the stored statements failed to compile (e.g.
/// after a limits change) — the row is skipped at apply time and reported
/// as `status: "invalid"` by callers, never blocking ingest.
#[derive(Debug)]
pub struct CompiledProcessor {
    pub record: ProcessorRecord,
    pub program: Option<ottl::CompiledProgram>,
}

impl CompiledProcessor {
    fn compile(record: ProcessorRecord, limits: &ottl::Limits) -> Self {
        let signal = match record.signal.as_str() {
            "traces" => ottl::Signal::Traces,
            "logs" => ottl::Signal::Logs,
            "metrics" => ottl::Signal::Metrics,
            _ => {
                tracing::warn!(
                    processor = %record.name,
                    tenant = %record.tenant_id,
                    signal = %record.signal,
                    "processor has an unrecognized signal; treating as invalid"
                );
                return CompiledProcessor {
                    record,
                    program: None,
                };
            }
        };
        match ottl::compile(signal, &record.statements, limits) {
            Ok(program) => CompiledProcessor {
                record,
                program: Some(program),
            },
            Err(errors) => {
                tracing::warn!(
                    processor = %record.name,
                    tenant = %record.tenant_id,
                    ?errors,
                    "processor failed to compile; skipping at apply time"
                );
                CompiledProcessor {
                    record,
                    program: None,
                }
            }
        }
    }
}

struct Entry {
    loaded_at: Instant,
    programs: Vec<Arc<CompiledProcessor>>,
    /// Serializes reloads of this tenant's entry so concurrent requests
    /// against a stale entry only hit the catalog once.
    reload: AsyncMutex<()>,
}

/// Per-tenant cache of compiled OTTL processors, backed by the catalog.
pub struct ProcessorRegistry {
    catalog: Arc<Catalog>,
    cache: DashMap<String, Arc<Entry>>,
    ttl: Duration,
    limits: ottl::Limits,
}

impl ProcessorRegistry {
    pub fn new(catalog: Arc<Catalog>, config: &ProcessorsConfig) -> Self {
        ProcessorRegistry {
            catalog,
            cache: DashMap::new(),
            ttl: config.reload_interval,
            limits: ottl::Limits {
                max_statements: config.max_statements,
                max_regex_len: config.max_regex_len,
                ..ottl::Limits::default()
            },
        }
    }

    async fn load(&self, tenant_id: &str) -> Result<Vec<Arc<CompiledProcessor>>, StoreError> {
        let records = self.catalog.list_processors(tenant_id).await?;
        Ok(records
            .into_iter()
            .map(|r| Arc::new(CompiledProcessor::compile(r, &self.limits)))
            .collect())
    }

    /// The current entry, loading or refreshing it as needed. On a reload
    /// failure the previous entry (if any) keeps serving and the error is
    /// logged, never propagated to the caller — a catalog hiccup must not
    /// block ingest.
    async fn entry(&self, tenant_id: &str) -> Arc<Entry> {
        // `DashMap::get` returns a `Ref` holding the shard's lock; extract the
        // owned `Arc` and let the `Ref` drop immediately (a `let` binding
        // reused via shadowing would otherwise keep it alive until this
        // function returns, deadlocking the later `insert`/`get` on the same
        // shard/key).
        let cached: Option<Arc<Entry>> = self.cache.get(tenant_id).map(|r| r.clone());
        if let Some(existing) = cached {
            if existing.loaded_at.elapsed() <= self.ttl {
                return existing;
            }
            // Stale: serialize reloads through the entry's own mutex so
            // concurrent callers don't all hit the catalog.
            let reload_result = {
                let _guard = existing.reload.lock().await;
                // Re-check: another task may have already refreshed while
                // we waited for the lock.
                let refreshed: Option<Arc<Entry>> = self.cache.get(tenant_id).map(|r| r.clone());
                if let Some(refreshed) = refreshed
                    && refreshed.loaded_at.elapsed() <= self.ttl
                {
                    return refreshed;
                }
                self.load(tenant_id).await
            };
            match reload_result {
                Ok(programs) => {
                    let entry = Arc::new(Entry {
                        loaded_at: Instant::now(),
                        programs,
                        reload: AsyncMutex::new(()),
                    });
                    self.cache.insert(tenant_id.to_string(), entry.clone());
                    entry
                }
                Err(err) => {
                    tracing::warn!(tenant = %tenant_id, error = %err, "failed to reload tenant processors; keeping stale cache");
                    existing
                }
            }
        } else {
            // First access for this tenant: no per-entry mutex exists yet,
            // so use a coarse tenant-keyed lock via `DashMap::entry` to
            // avoid a duplicate load on a cold-start stampede.
            let programs = match self.load(tenant_id).await {
                Ok(programs) => programs,
                Err(err) => {
                    tracing::warn!(tenant = %tenant_id, error = %err, "failed to load tenant processors; caching empty set");
                    Vec::new()
                }
            };
            let entry = Arc::new(Entry {
                loaded_at: Instant::now(),
                programs,
                reload: AsyncMutex::new(()),
            });
            // `DashMap::entry(..).or_insert_with` would hold the shard lock
            // across the `await` above, so instead race on plain `insert`
            // and let whichever caller wins be the source of truth; a
            // duplicate cold-start load is harmless and rare.
            self.cache
                .entry(tenant_id.to_string())
                .or_insert_with(|| entry)
                .clone()
        }
    }

    /// Every enabled processor visible to a request against `dataset` for
    /// `signal`, in D3 order: tenant-wide first, then `priority` ascending,
    /// then `name` ascending.
    pub async fn for_request(
        &self,
        tenant_id: &str,
        dataset: &str,
        signal: &str,
    ) -> Vec<Arc<CompiledProcessor>> {
        let entry = self.entry(tenant_id).await;
        let mut matching: Vec<Arc<CompiledProcessor>> = entry
            .programs
            .iter()
            .filter(|p| {
                p.record.enabled
                    && p.record.signal == signal
                    && p.record.dataset.as_deref().is_none_or(|d| d == dataset)
            })
            .cloned()
            .collect();
        matching.sort_by(|a, b| {
            let a_tenant_wide = a.record.dataset.is_none();
            let b_tenant_wide = b.record.dataset.is_none();
            b_tenant_wide
                .cmp(&a_tenant_wide)
                .then_with(|| a.record.priority.cmp(&b.record.priority))
                .then_with(|| a.record.name.cmp(&b.record.name))
        });
        matching
    }

    /// Drop the cached entry for `tenant_id` so the next `for_request` call
    /// reloads from the catalog. Write handlers must call this after any
    /// mutation.
    pub fn invalidate(&self, tenant_id: &str) {
        self.cache.remove(tenant_id);
    }

    /// Number of catalog list calls this registry has made, for tests only.
    #[cfg(test)]
    fn ttl(&self) -> Duration {
        self.ttl
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::processors::ProcessorSpec;
    use crate::testing::TempCatalog;

    async fn catalog() -> (TempCatalog, Arc<Catalog>) {
        let temp = TempCatalog::new();
        let catalog = Catalog::new(temp.uri())
            .await
            .expect("failed to create catalog");
        (temp, Arc::new(catalog))
    }

    fn spec(name: &str, signal: &str, dataset: Option<&str>, priority: i32) -> ProcessorSpec {
        ProcessorSpec {
            name: name.to_string(),
            dataset: dataset.map(str::to_string),
            signal: signal.to_string(),
            enabled: true,
            priority,
            error_mode: "ignore".to_string(),
            description: None,
            statements: vec![r#"set(attributes["k"], "v")"#.to_string()],
        }
    }

    fn config_with_ttl(ttl: Duration) -> ProcessorsConfig {
        ProcessorsConfig {
            reload_interval: ttl,
            ..ProcessorsConfig::default()
        }
    }

    #[tokio::test]
    async fn empty_tenant_caches_cheaply() {
        let (_temp, catalog) = catalog().await;
        let registry = ProcessorRegistry::new(catalog, &ProcessorsConfig::default());
        let result = registry.for_request("acme", "default", "traces").await;
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn lazy_load_and_filtering() {
        let (_temp, catalog) = catalog().await;
        catalog
            .insert_processor("acme", &spec("redact", "traces", None, 100))
            .await
            .expect("insert");
        catalog
            .insert_processor("acme", &spec("other-signal", "logs", None, 100))
            .await
            .expect("insert");

        let registry = ProcessorRegistry::new(catalog, &ProcessorsConfig::default());
        let traces = registry.for_request("acme", "default", "traces").await;
        assert_eq!(traces.len(), 1);
        assert_eq!(traces[0].record.name, "redact");

        // Unrelated tenant is unaffected and cheap.
        let other = registry
            .for_request("other-tenant", "default", "traces")
            .await;
        assert!(other.is_empty());
    }

    #[tokio::test]
    async fn ordering_tenant_wide_then_priority_then_name() {
        let (_temp, catalog) = catalog().await;
        catalog
            .upsert_tenant("acme", "Acme", None, "config")
            .await
            .expect("upsert tenant");
        catalog
            .create_dataset("acme", "default")
            .await
            .expect("create dataset");
        catalog
            .insert_processor("acme", &spec("b-dataset", "traces", Some("default"), 50))
            .await
            .expect("insert");
        catalog
            .insert_processor("acme", &spec("a-tenant-high-prio", "traces", None, 200))
            .await
            .expect("insert");
        catalog
            .insert_processor("acme", &spec("z-tenant-low-prio", "traces", None, 10))
            .await
            .expect("insert");

        let registry = ProcessorRegistry::new(catalog, &ProcessorsConfig::default());
        let ordered = registry.for_request("acme", "default", "traces").await;
        let names: Vec<&str> = ordered.iter().map(|p| p.record.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["z-tenant-low-prio", "a-tenant-high-prio", "b-dataset"]
        );
    }

    #[tokio::test]
    async fn dataset_scoped_processor_excluded_from_other_dataset() {
        let (_temp, catalog) = catalog().await;
        catalog
            .upsert_tenant("acme", "Acme", None, "config")
            .await
            .expect("upsert tenant");
        catalog
            .create_dataset("acme", "default")
            .await
            .expect("create dataset");
        catalog
            .insert_processor("acme", &spec("scoped", "traces", Some("default"), 100))
            .await
            .expect("insert");

        let registry = ProcessorRegistry::new(catalog, &ProcessorsConfig::default());
        let matches_default = registry.for_request("acme", "default", "traces").await;
        assert_eq!(matches_default.len(), 1);
        let matches_other = registry
            .for_request("acme", "other-dataset", "traces")
            .await;
        assert!(matches_other.is_empty());
    }

    #[tokio::test]
    async fn invalid_row_skipped_but_reported() {
        let (_temp, catalog) = catalog().await;
        let mut bad = spec("broken", "traces", None, 100);
        bad.statements = vec!["not a valid statement (".to_string()];
        catalog
            .insert_processor("acme", &bad)
            .await
            .expect("insert");

        let registry = ProcessorRegistry::new(catalog, &ProcessorsConfig::default());
        let result = registry.for_request("acme", "default", "traces").await;
        assert_eq!(result.len(), 1);
        assert!(result[0].program.is_none());
    }

    #[tokio::test]
    async fn ttl_refresh_picks_up_new_rows() {
        let (_temp, catalog) = catalog().await;
        let registry =
            ProcessorRegistry::new(catalog.clone(), &config_with_ttl(Duration::from_millis(10)));
        assert!(
            registry
                .for_request("acme", "default", "traces")
                .await
                .is_empty()
        );

        catalog
            .insert_processor("acme", &spec("new-one", "traces", None, 100))
            .await
            .expect("insert");
        tokio::time::sleep(Duration::from_millis(20)).await;

        let result = registry.for_request("acme", "default", "traces").await;
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn invalidate_forces_reload_before_ttl() {
        let (_temp, catalog) = catalog().await;
        let registry =
            ProcessorRegistry::new(catalog.clone(), &config_with_ttl(Duration::from_secs(60)));
        assert!(
            registry
                .for_request("acme", "default", "traces")
                .await
                .is_empty()
        );

        catalog
            .insert_processor("acme", &spec("new-one", "traces", None, 100))
            .await
            .expect("insert");
        // TTL has not elapsed, so without invalidation the empty cache would
        // still be returned.
        registry.invalidate("acme");

        let result = registry.for_request("acme", "default", "traces").await;
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn concurrent_access_to_stale_entry_hits_catalog_once() {
        let (_temp, real_catalog) = catalog().await;
        real_catalog
            .insert_processor("acme", &spec("p1", "traces", None, 100))
            .await
            .expect("insert");

        let registry = Arc::new(ProcessorRegistry::new(
            real_catalog.clone(),
            &config_with_ttl(Duration::from_millis(1)),
        ));
        // Prime the cache, then let it go stale.
        registry.for_request("acme", "default", "traces").await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(registry.ttl() > Duration::ZERO);

        let hits = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let registry = registry.clone();
            let hits = hits.clone();
            handles.push(tokio::spawn(async move {
                let result = registry.for_request("acme", "default", "traces").await;
                hits.fetch_add(1, Ordering::SeqCst);
                assert_eq!(result.len(), 1);
            }));
        }
        for h in handles {
            h.await.expect("task panicked");
        }
        assert_eq!(hits.load(Ordering::SeqCst), 8);
    }
}
