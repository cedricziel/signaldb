//! # Per-Tenant Rate Limiting
//!
//! Token-bucket rate limiting keyed by tenant id, used by the ingest
//! paths (OTLP gRPC, Prometheus remote_write) to keep one tenant from
//! exhausting shared acceptor throughput.
//!
//! Limits come from `[auth].default_limits` with optional per-tenant
//! overrides (`[[auth.tenants]].limits`). Unset limits mean unlimited,
//! so deployments without limit configuration are unaffected.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

use dashmap::DashMap;

use crate::config::{AuthConfig, TenantLimits};

/// A single token bucket: `rate` tokens per second, holding at most
/// `burst` tokens.
#[derive(Debug)]
struct TokenBucket {
    rate: f64,
    burst: f64,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(rate: f64, burst: f64, now: Instant) -> Self {
        Self {
            rate,
            burst,
            tokens: burst,
            last_refill: now,
        }
    }

    /// Refill for elapsed time, then try to take `cost` tokens.
    fn try_acquire(&mut self, cost: f64, now: Instant) -> bool {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate).min(self.burst);
        self.last_refill = now;
        if self.tokens >= cost {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }
}

/// Buckets for one tenant; `None` means that dimension is unlimited.
#[derive(Debug, Default)]
struct TenantBuckets {
    requests: Option<TokenBucket>,
    bytes: Option<TokenBucket>,
}

/// The dimension that rejected a request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimitKind {
    /// The per-tenant requests/second limit.
    Requests,
    /// The per-tenant ingest bytes/second limit.
    Bytes,
}

/// Error returned when a tenant exceeds an ingest limit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RateLimitExceeded {
    pub tenant_id: String,
    pub kind: RateLimitKind,
}

impl std::fmt::Display for RateLimitExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = match self.kind {
            RateLimitKind::Requests => "request rate",
            RateLimitKind::Bytes => "ingest byte rate",
        };
        write!(
            f,
            "tenant '{}' exceeded its {what} limit; retry later or raise the tenant's limits",
            self.tenant_id
        )
    }
}

impl std::error::Error for RateLimitExceeded {}

/// Per-tenant ingest rate limiter.
///
/// Cheap to check on the hot path: one `DashMap` shard lock plus a
/// short bucket mutex; no async and no allocation after the first
/// request per tenant.
pub struct TenantRateLimiter {
    defaults: TenantLimits,
    overrides: HashMap<String, TenantLimits>,
    buckets: DashMap<String, Mutex<TenantBuckets>>,
}

impl TenantRateLimiter {
    /// Build a limiter from auth configuration: global `default_limits`
    /// plus per-tenant overrides.
    pub fn from_auth_config(auth: &AuthConfig) -> Self {
        let overrides = auth
            .tenants
            .iter()
            .filter_map(|t| t.limits.clone().map(|l| (t.id.clone(), l)))
            .collect();
        Self {
            defaults: auth.default_limits.clone(),
            overrides,
            buckets: DashMap::new(),
        }
    }

    /// Limits that apply to `tenant_id`.
    fn limits_for(&self, tenant_id: &str) -> &TenantLimits {
        self.overrides.get(tenant_id).unwrap_or(&self.defaults)
    }

    /// Record one ingest request of `bytes` payload bytes for the tenant,
    /// rejecting it if either the request-rate or byte-rate budget is
    /// exhausted.
    pub fn check_ingest(&self, tenant_id: &str, bytes: usize) -> Result<(), RateLimitExceeded> {
        self.check_ingest_at(tenant_id, bytes, Instant::now())
    }

    /// [`Self::check_ingest`] with an injectable clock for tests.
    fn check_ingest_at(
        &self,
        tenant_id: &str,
        bytes: usize,
        now: Instant,
    ) -> Result<(), RateLimitExceeded> {
        let limits = self.limits_for(tenant_id);
        if limits.max_ingest_requests_per_sec.is_none() && limits.max_ingest_bytes_per_sec.is_none()
        {
            return Ok(());
        }

        let entry = self
            .buckets
            .entry(tenant_id.to_string())
            .or_insert_with(|| {
                let burst_secs = limits.burst_seconds.max(1.0);
                Mutex::new(TenantBuckets {
                    requests: limits.max_ingest_requests_per_sec.map(|rps| {
                        let rate = f64::from(rps);
                        TokenBucket::new(rate, rate * burst_secs, now)
                    }),
                    bytes: limits.max_ingest_bytes_per_sec.map(|bps| {
                        let rate = bps as f64;
                        TokenBucket::new(rate, rate * burst_secs, now)
                    }),
                })
            });
        let mut buckets = entry
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        if let Some(bucket) = buckets.requests.as_mut()
            && !bucket.try_acquire(1.0, now)
        {
            return Err(RateLimitExceeded {
                tenant_id: tenant_id.to_string(),
                kind: RateLimitKind::Requests,
            });
        }
        if let Some(bucket) = buckets.bytes.as_mut()
            && !bucket.try_acquire(bytes as f64, now)
        {
            return Err(RateLimitExceeded {
                tenant_id: tenant_id.to_string(),
                kind: RateLimitKind::Bytes,
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TenantConfig;
    use std::time::Duration;

    fn limiter(defaults: TenantLimits, overrides: Vec<TenantConfig>) -> TenantRateLimiter {
        let auth = AuthConfig {
            default_limits: defaults,
            tenants: overrides,
            ..Default::default()
        };
        TenantRateLimiter::from_auth_config(&auth)
    }

    fn tenant_with_limits(id: &str, limits: TenantLimits) -> TenantConfig {
        TenantConfig {
            id: id.to_string(),
            slug: id.to_string(),
            name: id.to_string(),
            default_dataset: None,
            datasets: vec![],
            api_keys: vec![],
            schema_config: None,
            limits: Some(limits),
        }
    }

    #[test]
    fn unlimited_by_default() {
        let limiter = limiter(TenantLimits::default(), vec![]);
        for _ in 0..10_000 {
            assert!(limiter.check_ingest("acme", 1_000_000).is_ok());
        }
    }

    #[test]
    fn request_rate_is_enforced_and_refills() {
        let limiter = limiter(
            TenantLimits {
                max_ingest_requests_per_sec: Some(10),
                burst_seconds: 1.0,
                ..Default::default()
            },
            vec![],
        );

        let start = Instant::now();
        for _ in 0..10 {
            assert!(limiter.check_ingest_at("acme", 0, start).is_ok());
        }
        let denied = limiter.check_ingest_at("acme", 0, start).unwrap_err();
        assert_eq!(denied.kind, RateLimitKind::Requests);

        // Half a second refills five tokens.
        let later = start + Duration::from_millis(500);
        for _ in 0..5 {
            assert!(limiter.check_ingest_at("acme", 0, later).is_ok());
        }
        assert!(limiter.check_ingest_at("acme", 0, later).is_err());
    }

    #[test]
    fn byte_rate_is_enforced() {
        let limiter = limiter(
            TenantLimits {
                max_ingest_bytes_per_sec: Some(1_000),
                burst_seconds: 1.0,
                ..Default::default()
            },
            vec![],
        );

        let start = Instant::now();
        assert!(limiter.check_ingest_at("acme", 900, start).is_ok());
        let denied = limiter.check_ingest_at("acme", 200, start).unwrap_err();
        assert_eq!(denied.kind, RateLimitKind::Bytes);
    }

    #[test]
    fn tenants_have_independent_budgets() {
        let limiter = limiter(
            TenantLimits {
                max_ingest_requests_per_sec: Some(1),
                burst_seconds: 1.0,
                ..Default::default()
            },
            vec![],
        );

        let start = Instant::now();
        assert!(limiter.check_ingest_at("acme", 0, start).is_ok());
        assert!(limiter.check_ingest_at("acme", 0, start).is_err());
        // A different tenant still has its own budget.
        assert!(limiter.check_ingest_at("globex", 0, start).is_ok());
    }

    #[test]
    fn per_tenant_override_beats_default() {
        let limiter = limiter(
            TenantLimits {
                max_ingest_requests_per_sec: Some(1),
                burst_seconds: 1.0,
                ..Default::default()
            },
            vec![tenant_with_limits(
                "vip",
                TenantLimits {
                    max_ingest_requests_per_sec: Some(100),
                    burst_seconds: 1.0,
                    ..Default::default()
                },
            )],
        );

        let start = Instant::now();
        for _ in 0..100 {
            assert!(limiter.check_ingest_at("vip", 0, start).is_ok());
        }
        assert!(limiter.check_ingest_at("vip", 0, start).is_err());

        // Non-override tenants use the default.
        assert!(limiter.check_ingest_at("acme", 0, start).is_ok());
        assert!(limiter.check_ingest_at("acme", 0, start).is_err());
    }
}
